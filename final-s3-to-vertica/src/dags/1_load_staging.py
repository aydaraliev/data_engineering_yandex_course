from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
import boto3
import vertica_python
from io import StringIO
import csv
import logging

STAGING_SCHEMA = 'VT251126648744__STAGING'
S3_BUCKET = 'final-project'


def get_s3_client():
    conn = BaseHook.get_connection('yandex_s3')
    extra = conn.extra_dejson
    return boto3.client(
        's3',
        endpoint_url=extra.get('endpoint_url'),
        aws_access_key_id=extra.get('aws_access_key_id'),
        aws_secret_access_key=extra.get('aws_secret_access_key')
    )


def get_vertica_connection():
    conn = BaseHook.get_connection('vertica_conn')
    return vertica_python.connect(
        host=conn.host,
        port=conn.port,
        database=conn.schema,
        user=conn.login,
        password=conn.password,
        autocommit=False
    )


def load_currencies(**context):
    execution_date = context['ds']
    logging.info(f"Loading currencies for date: {execution_date}")

    s3 = get_s3_client()
    response = s3.get_object(Bucket=S3_BUCKET, Key='currencies_history.csv')
    content = response['Body'].read().decode('utf-8')

    reader = csv.DictReader(StringIO(content))
    rows_to_insert = []

    for row in reader:
        date_update = row.get('date_update', '')
        if date_update.startswith(execution_date):
            rows_to_insert.append([
                row.get('date_update'),
                row.get('currency_code', '0'),
                row.get('currency_code_with', '0'),
                row.get('currency_code_div', '1.0')
            ])

    logging.info(f"Found {len(rows_to_insert)} currency records for {execution_date}")

    if not rows_to_insert:
        logging.info("No currency data for this date")
        return

    buffer = StringIO()
    writer = csv.writer(buffer, delimiter='|')
    writer.writerows(rows_to_insert)
    buffer.seek(0)

    conn = get_vertica_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            f"DELETE FROM {STAGING_SCHEMA}.currencies WHERE date_update::DATE = %s",
            (execution_date,)
        )

        copy_query = f"""
            COPY {STAGING_SCHEMA}.currencies
            (date_update, currency_code, currency_code_with, currency_code_div)
            FROM STDIN DELIMITER '|'
            REJECTED DATA AS TABLE {STAGING_SCHEMA}.currencies_rejected
        """
        cur.copy(copy_query, buffer)
        conn.commit()
        logging.info(f"Loaded {len(rows_to_insert)} currency records")
    except Exception as e:
        conn.rollback()
        logging.error(f"Currency load error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def load_transactions(**context):
    execution_date = context['ds']
    logging.info(f"Loading transactions for date: {execution_date}")

    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='transactions')

    transaction_files = [
        obj['Key'] for obj in response.get('Contents', [])
        if obj['Key'].endswith('.csv')
    ]

    logging.info(f"Found {len(transaction_files)} transaction files")

    all_rows = []

    for file_key in transaction_files:
        logging.info(f"Processing file: {file_key}")
        response = s3.get_object(Bucket=S3_BUCKET, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        reader = csv.DictReader(StringIO(content))

        for row in reader:
            transaction_dt = row.get('transaction_dt', '')
            if transaction_dt.startswith(execution_date):
                all_rows.append([
                    row.get('operation_id', ''),
                    row.get('account_number_from', ''),
                    row.get('account_number_to', ''),
                    row.get('currency_code', ''),
                    row.get('country', ''),
                    row.get('status', ''),
                    row.get('transaction_type', ''),
                    row.get('amount', ''),
                    row.get('transaction_dt', '')
                ])

    logging.info(f"Found {len(all_rows)} transactions for {execution_date}")

    if not all_rows:
        logging.info("No transaction data for this date")
        return

    buffer = StringIO()
    writer = csv.writer(buffer, delimiter='|')
    writer.writerows(all_rows)
    buffer.seek(0)

    conn = get_vertica_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            f"DELETE FROM {STAGING_SCHEMA}.transactions WHERE transaction_dt::DATE = %s",
            (execution_date,)
        )

        copy_query = f"""
            COPY {STAGING_SCHEMA}.transactions
            (operation_id, account_number_from, account_number_to, currency_code,
             country, status, transaction_type, amount, transaction_dt)
            FROM STDIN DELIMITER '|'
            REJECTED DATA AS TABLE {STAGING_SCHEMA}.transactions_rejected
        """
        cur.copy(copy_query, buffer)
        conn.commit()
        logging.info(f"Loaded {len(all_rows)} transactions")
    except Exception as e:
        conn.rollback()
        logging.error(f"Transaction load error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    '1_load_staging',
    default_args=default_args,
    description='S3 -> Vertica Staging',
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['staging', 's3', 'vertica']
) as dag:

    load_currencies_task = PythonOperator(
        task_id='load_currencies',
        python_callable=load_currencies
    )

    load_transactions_task = PythonOperator(
        task_id='load_transactions',
        python_callable=load_transactions
    )

    trigger_dwh = TriggerDagRunOperator(
        task_id='trigger_dwh_load',
        trigger_dag_id='2_load_dwh',
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=False
    )

    [load_currencies_task, load_transactions_task] >> trigger_dwh
