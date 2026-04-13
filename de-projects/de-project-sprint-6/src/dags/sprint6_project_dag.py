"""
Airflow DAG для проекта Sprint 6: Анализ конверсии групп в социальной сети

Этот DAG выполняет следующие задачи:
1. Скачивает файл group_log.csv из S3
2. Создает необходимые таблицы в Vertica (staging и DWH слои)
3. Загружает данные в staging таблицу с обработкой NULL значений
4. Мигрирует данные в DWH слой (линк и сателлит)
"""

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.decorators import dag
import pendulum

import os
import boto3
import pandas as pd


def fetch_s3_file(bucket: str, key: str, local_dir: str = "/data"):
    """
    Download a single file from Yandex Cloud S3-compatible storage to local_dir.
    Credentials are read from environment if set.
    """
    # Ensure destination directory exists
    os.makedirs(local_dir, exist_ok=True)

    # Create S3 client for Yandex Cloud
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    )

    local_path = os.path.join(local_dir, os.path.basename(key))
    s3_client.download_file(Bucket=bucket, Key=key, Filename=local_path)
    print(f"File {key} downloaded to {local_path}")


def prepare_group_log_data(input_path: str = "/data/group_log.csv", output_path: str = "/data/group_log_prepared.csv"):
    """
    Обрабатывает group_log.csv для корректной загрузки в Vertica.
    Преобразует поле user_id_from в Int64 для правильной обработки NULL значений.
    """
    # Читаем CSV файл
    df = pd.read_csv(input_path)
    
    # Преобразуем user_id_from в Int64 для поддержки NULL значений
    # Это необходимо, т.к. обычный int не поддерживает NULL в pandas
    df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
    
    # Сохраняем подготовленный файл
    df.to_csv(output_path, index=False)
    print(f"Prepared data saved to {output_path}")
    print(f"Total rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"Sample data:\n{df.head()}")



# Bash template: print the first 10 lines of the file
bash_command_tmpl = r"""
set -e
echo "===== $FILE ====="
if [ -f "$FILE" ]; then
  head -n 10 "$FILE"
else
  echo "File not found: $FILE"
fi
"""


@dag(
    schedule_interval=None,
    start_date=pendulum.parse("2022-07-13"),
    catchup=False,
    tags=["sprint6", "project", "group_conversion"],
)
def sprint6_project_group_log():
    """
    DAG для загрузки и обработки данных о группах социальной сети
    """
    
    bucket = "sprint6"
    filename = "group_log.csv"
    
    # Старт пайплайна
    start = DummyOperator(task_id="start")
    
    # Шаг 1: Скачиваем файл из S3
    fetch_group_log = PythonOperator(
        task_id="fetch_group_log",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": bucket, "key": filename, "local_dir": "/data"},
    )
    
    # Шаг 2: Обрабатываем данные (конвертируем user_id_from в Int64)
    prepare_data = PythonOperator(
        task_id="prepare_group_log_data",
        python_callable=prepare_group_log_data,
        op_kwargs={
            "input_path": "/data/group_log.csv",
            "output_path": "/data/group_log_prepared.csv"
        },
    )
    
    # Шаг 3: Выводим первые 10 строк для проверки
    print_sample = BashOperator(
        task_id="print_sample",
        bash_command=bash_command_tmpl,
        env={"FILE": "/data/group_log_prepared.csv"},
    )
    
    # Шаг 4: Создаем таблицу в STAGING
    create_staging_table = VerticaOperator(
        task_id="create_staging_table",
        vertica_conn_id="vertica",
        sql="""
        CREATE TABLE IF NOT EXISTS VT251126648744__STAGING.group_log (
            group_id      INT,
            user_id       INT,
            user_id_from  INT,
            event         VARCHAR(10),
            datetime      TIMESTAMP
        );
        """
    )
    
    # Шаг 5: Создаем линк в DWH
    create_link_table = VerticaOperator(
        task_id="create_link_table",
        vertica_conn_id="vertica",
        sql="""
        CREATE TABLE IF NOT EXISTS VT251126648744__DWH.l_user_group_activity (
            hk_l_user_group_activity INT PRIMARY KEY,
            hk_user_id               INT NOT NULL,
            hk_group_id              INT NOT NULL,
            load_dt                  TIMESTAMP,
            load_src                 VARCHAR(20),
            CONSTRAINT fk_l_user_group_activity_user 
                FOREIGN KEY (hk_user_id) REFERENCES VT251126648744__DWH.h_users(hk_user_id),
            CONSTRAINT fk_l_user_group_activity_group 
                FOREIGN KEY (hk_group_id) REFERENCES VT251126648744__DWH.h_groups(hk_group_id)
        );
        """
    )
    
    # Шаг 6: Создаем сателлит в DWH
    create_satellite_table = VerticaOperator(
        task_id="create_satellite_table",
        vertica_conn_id="vertica",
        sql="""
        CREATE TABLE IF NOT EXISTS VT251126648744__DWH.s_auth_history (
            hk_l_user_group_activity INT NOT NULL,
            user_id_from             INT,
            event                    VARCHAR(10),
            event_dt                 TIMESTAMP,
            load_dt                  TIMESTAMP,
            load_src                 VARCHAR(20),
            CONSTRAINT fk_s_auth_history_link 
                FOREIGN KEY (hk_l_user_group_activity) 
                REFERENCES VT251126648744__DWH.l_user_group_activity(hk_l_user_group_activity)
        );
        """
    )
    
    # Шаг 7: Загружаем данные в staging таблицу
    load_staging = VerticaOperator(
        task_id="load_staging",
        vertica_conn_id="vertica",
        sql="""
        TRUNCATE TABLE VT251126648744__STAGING.group_log;
        
        COPY VT251126648744__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
        FROM LOCAL '/data/group_log_prepared.csv'
        DELIMITER ','
        SKIP 1
        NULL AS ''
        REJECTED DATA AS TABLE group_log_rejected;
        """
    )
    
    # Шаг 8: Мигрируем данные в линк (идемпотентно, без дубликатов)
    load_link = VerticaOperator(
        task_id="load_link",
        vertica_conn_id="vertica",
        sql="""
        INSERT INTO VT251126648744__DWH.l_user_group_activity (
            hk_l_user_group_activity, 
            hk_user_id,
            hk_group_id,
            load_dt,
            load_src
        )
        SELECT DISTINCT
            HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity,
            hu.hk_user_id,
            hg.hk_group_id,
            NOW() AS load_dt,
            'group_log' AS load_src
        FROM VT251126648744__STAGING.group_log AS gl
        LEFT JOIN VT251126648744__DWH.h_users AS hu ON gl.user_id = hu.user_id
        LEFT JOIN VT251126648744__DWH.h_groups AS hg ON gl.group_id = hg.group_id
        WHERE hu.hk_user_id IS NOT NULL 
          AND hg.hk_group_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 
              FROM VT251126648744__DWH.l_user_group_activity AS luga
              WHERE luga.hk_user_id = hu.hk_user_id 
                AND luga.hk_group_id = hg.hk_group_id
          );
        """
    )
    
    # Шаг 9: Мигрируем данные в сателлит (идемпотентно, только новые события)
    load_satellite = VerticaOperator(
        task_id="load_satellite",
        vertica_conn_id="vertica",
        sql="""
        INSERT INTO VT251126648744__DWH.s_auth_history (
            hk_l_user_group_activity,
            user_id_from,
            event,
            event_dt,
            load_dt,
            load_src
        )
        SELECT 
            luga.hk_l_user_group_activity,
            gl.user_id_from,
            gl.event,
            gl.datetime AS event_dt,
            NOW() AS load_dt,
            'group_log' AS load_src
        FROM VT251126648744__STAGING.group_log AS gl
        LEFT JOIN VT251126648744__DWH.h_groups AS hg ON gl.group_id = hg.group_id
        LEFT JOIN VT251126648744__DWH.h_users AS hu ON gl.user_id = hu.user_id
        LEFT JOIN VT251126648744__DWH.l_user_group_activity AS luga 
            ON hg.hk_group_id = luga.hk_group_id 
            AND hu.hk_user_id = luga.hk_user_id
        WHERE luga.hk_l_user_group_activity IS NOT NULL
          -- Prevent duplicates: only insert if this exact event doesn't exist
          AND NOT EXISTS (
              SELECT 1 
              FROM VT251126648744__DWH.s_auth_history AS sah
              WHERE sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
                AND sah.event = gl.event
                AND sah.event_dt = gl.datetime
          );
        """
    )
    
    # Шаг 10: Проверяем результаты - выполняем аналитический запрос
    check_results = VerticaOperator(
        task_id="check_results",
        vertica_conn_id="vertica",
        sql="""
        -- Проверка: количество записей в каждой таблице
        SELECT 'staging.group_log' AS table_name, COUNT(*) AS row_count 
        FROM VT251126648744__STAGING.group_log
        UNION ALL
        SELECT 'dwh.l_user_group_activity', COUNT(*) 
        FROM VT251126648744__DWH.l_user_group_activity
        UNION ALL
        SELECT 'dwh.s_auth_history', COUNT(*) 
        FROM VT251126648744__DWH.s_auth_history;
        """
    )
    
    # Завершение пайплайна
    end = DummyOperator(task_id="end")
    
    # Определяем зависимости между задачами
    start >> fetch_group_log >> prepare_data >> print_sample
    
    # Создание таблиц может идти параллельно после подготовки данных
    print_sample >> [create_staging_table, create_link_table, create_satellite_table]
    
    # Сначала создаем линк, потом сателлит (FK зависимость)
    create_link_table >> create_satellite_table
    
    # Загрузка в staging после создания всех таблиц
    [create_staging_table, create_satellite_table] >> load_staging
    
    # Сначала загружаем линк, потом сателлит (FK зависимость)
    load_staging >> load_link >> load_satellite
    
    # Проверка результатов и завершение
    load_satellite >> check_results >> end


# Создаем экземпляр DAG
_ = sprint6_project_group_log()
