from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 5),
    'retries': 1
}

dag = DAG(
    'dds_init_srv_wf_settings',
    default_args=default_args,
    description='Initialize DDS workflow settings table',
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=['sprint5', 'dds', 'init']
)

create_table_task = PostgresOperator(
    task_id='create_dds_srv_wf_settings_table',
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="""
        DROP TABLE IF EXISTS dds.srv_wf_settings;
        CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
            id SERIAL PRIMARY KEY,
            workflow_key VARCHAR(255) UNIQUE NOT NULL,
            workflow_settings JSON NOT NULL
        );
    """,
    dag=dag
)

create_table_task
