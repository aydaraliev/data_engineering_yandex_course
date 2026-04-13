from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import vertica_python
import logging

STAGING_SCHEMA = 'VT251126648744__STAGING'
DWH_SCHEMA = 'VT251126648744__DWH'


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


def load_global_metrics(**context):
    execution_date = context['ds']
    logging.info(f"Loading metrics for date: {execution_date}")

    conn = get_vertica_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            f"DELETE FROM {DWH_SCHEMA}.global_metrics WHERE date_update = %s",
            (execution_date,)
        )

        insert_query = f"""
            INSERT INTO {DWH_SCHEMA}.global_metrics
            (date_update, currency_from, amount_total, cnt_transactions,
             avg_transactions_per_account, cnt_accounts_make_transactions)
            WITH ranked_transactions AS (
                SELECT
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    amount,
                    transaction_dt,
                    status,
                    ROW_NUMBER() OVER (
                        PARTITION BY operation_id
                        ORDER BY transaction_dt DESC
                    ) AS rn
                FROM {STAGING_SCHEMA}.transactions
                WHERE transaction_dt::DATE = %s
            ),
            unique_transactions AS (
                SELECT * FROM ranked_transactions WHERE rn = 1
            )
            SELECT
                t.transaction_dt::DATE AS date_update,
                t.currency_code AS currency_from,
                SUM(t.amount * COALESCE(c.currency_code_div, 1))::NUMERIC(18,2) AS amount_total,
                COUNT(DISTINCT t.operation_id) AS cnt_transactions,
                (COUNT(DISTINCT t.operation_id)::NUMERIC /
                 NULLIF(COUNT(DISTINCT t.account_number_from), 0))::NUMERIC(18,2)
                    AS avg_transactions_per_account,
                COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
            FROM unique_transactions t
            LEFT JOIN {STAGING_SCHEMA}.currencies c
                ON t.currency_code = c.currency_code
                AND c.currency_code_with = 420
                AND c.date_update::DATE = t.transaction_dt::DATE
            WHERE
                t.account_number_from > 0
                AND t.account_number_to > 0
                AND t.amount IS NOT NULL
                AND t.amount > 0
            GROUP BY 1, 2
        """
        cur.execute(insert_query, (execution_date,))

        cur.execute(f"""
            SELECT COUNT(*) FROM {DWH_SCHEMA}.global_metrics WHERE date_update = %s
        """, (execution_date,))
        row_count = cur.fetchone()[0]

        conn.commit()
        logging.info(f"Loaded {row_count} rows into the mart for {execution_date}")

    except Exception as e:
        conn.rollback()
        logging.error(f"DWH load error: {e}")
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
    '2_load_dwh',
    default_args=default_args,
    description='Vertica Staging -> DWH',
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['dwh', 'vertica', 'mart']
) as dag:

    load_metrics_task = PythonOperator(
        task_id='load_global_metrics',
        python_callable=load_global_metrics
    )
