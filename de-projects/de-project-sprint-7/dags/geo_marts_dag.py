"""
DAG для автоматизации обновления geo-витрин с использованием ODS слоя.

Архитектура:
1. create_ods_layer - Создание ODS слоя (события с городами)
2. user_geo_report - Геоаналитика пользователей (читает из ODS)
3. zone_mart - Агрегация событий по зонам (читает из ODS)
4. friend_recommendations - Рекомендации друзей (читает из ODS)

Преимущества ODS слоя:
- Однократное выполнение дорогой операции определения городов
- 3x ускорение (вместо 3 раз определяем города 1 раз)
- Переиспользование обогащенных данных во всех витринах

Расписание: ежедневно в 00:00 UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

def log_start(**context):
    """Логирует начало обновления витрин."""
    execution_date = context['execution_date']
    print(f"Начало обновления geo-витрин для даты: {execution_date}")
    return f"Started at {datetime.now()}"

def log_end(**context):
    """Логирует успешное завершение обновления."""
    execution_date = context['execution_date']
    print(f"Успешно обновлены все geo-витрины для даты: {execution_date}")
    return f"Completed at {datetime.now()}"

# Параметры по умолчанию для всех задач
default_args = {
    'owner': 'ajdaral1ev',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['ajdaral1ev@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG
dag = DAG(
    'geo_marts_update',
    default_args=default_args,
    description='Daily update of geo-based data marts',
    schedule_interval='0 0 * * *',  # Ежедневно в 00:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=['geo', 'marts', 'daily'],
)

# Получаем sample_fraction из Airflow Variables (по умолчанию 1.0 = все данные)
try:
    sample_fraction = float(Variable.get("geo_marts_sample_fraction", default_var="1.0"))
except Exception:
    sample_fraction = 1.0

# Формируем application_args для режима выборки
sample_args = ['--sample', str(sample_fraction)] if sample_fraction < 1.0 else []

# Начало DAG - логирование
start = PythonOperator(
    task_id='start',
    python_callable=log_start,
    provide_context=True,
    dag=dag,
)

# Задача 1: Создание ODS слоя (события с городами)
create_ods_layer = SparkSubmitOperator(
    task_id='create_ods_layer',
    application='/lessons/scripts/create_ods_layer.py',
    conn_id='yarn_spark',
    name='create_ods_layer',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': '20',
        'spark.sql.parquet.compression.codec': 'snappy',  # ОПТИМИЗАЦИЯ: Snappy compression
    },
    application_args=sample_args,  # Поддержка режима выборки через Airflow Variables
    py_files='/lessons/scripts/geo_utils.py',
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Задача 2: Обновление user_geo_report
update_user_geo_report = SparkSubmitOperator(
    task_id='update_user_geo_report',
    application='/lessons/scripts/user_geo_mart.py',
    conn_id='yarn_spark',
    name='user_geo_mart_update',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': '20',  # Reduced from 200 for 1.5GB dataset
        'spark.sql.parquet.compression.codec': 'snappy',  # ОПТИМИЗАЦИЯ: Snappy compression
    },
    application_args=sample_args,  # Поддержка режима выборки через Airflow Variables
    py_files='/lessons/scripts/geo_utils.py',
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Задача 3: Обновление zone_mart
update_zone_mart = SparkSubmitOperator(
    task_id='update_zone_mart',
    application='/lessons/scripts/zone_mart.py',
    conn_id='yarn_spark',
    name='zone_mart_update',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': '20',  # Reduced from 200 for 1.5GB dataset
        'spark.sql.parquet.compression.codec': 'snappy',  # ОПТИМИЗАЦИЯ: Snappy compression
    },
    application_args=sample_args,  # Поддержка режима выборки через Airflow Variables
    py_files='/lessons/scripts/geo_utils.py',
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Задача 4: Обновление friend_recommendations
update_friend_recommendations = SparkSubmitOperator(
    task_id='update_friend_recommendations',
    application='/lessons/scripts/friend_recommendations.py',
    conn_id='yarn_spark',
    name='friend_recommendations_update',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': '20',  # Reduced from 200 for 1.5GB dataset
        'spark.sql.parquet.compression.codec': 'snappy',  # ОПТИМИЗАЦИЯ: Snappy compression
    },
    application_args=sample_args,  # Поддержка режима выборки через Airflow Variables
    py_files='/lessons/scripts/geo_utils.py',
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Конец DAG - логирование
end = PythonOperator(
    task_id='end',
    python_callable=log_end,
    provide_context=True,
    dag=dag,
)

# Определяем последовательность выполнения
# ОПТИМИЗАЦИЯ: Сначала создаем ODS слой (события с городами),
# затем все витрины читают из ODS вместо RAW
# start → create_ods_layer → [user_geo_report, zone_mart, friend_recommendations] → end
#
# Витрины теперь могут выполняться параллельно, т.к. читают из ODS,
# но выполняются последовательно для оптимизации ресурсов кластера
start >> create_ods_layer >> update_user_geo_report >> update_zone_mart >> update_friend_recommendations >> end
