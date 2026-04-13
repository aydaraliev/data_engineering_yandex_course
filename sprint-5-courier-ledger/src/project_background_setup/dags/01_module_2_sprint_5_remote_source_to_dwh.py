from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_ranks_and_users',
    default_args=default_args,
    description='Load ranks and users tables from origin to DWH',
    schedule_interval='*/15 * * * *',
    catchup=False,
)

def load_ranks():
    # Connect to the source
    origin_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    # Connect to the DWH
    dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    # Read data from the source
    origin_conn = origin_hook.get_conn()
    origin_cursor = origin_conn.cursor()
    origin_cursor.execute("SELECT * FROM ranks")
    rows = origin_cursor.fetchall()

    # Fetch column names
    colnames = [desc[0] for desc in origin_cursor.description]

    origin_cursor.close()
    origin_conn.close()

    # Write into the DWH
    dwh_conn = dwh_hook.get_conn()
    dwh_cursor = dwh_conn.cursor()

    # Truncate the table before loading
    dwh_cursor.execute("TRUNCATE TABLE stg.bonussystem_ranks")

    # Insert the data
    if rows:
        placeholders = ','.join(['%s'] * len(colnames))
        insert_query = f"INSERT INTO stg.bonussystem_ranks ({','.join(colnames)}) VALUES ({placeholders})"
        dwh_cursor.executemany(insert_query, rows)

    dwh_conn.commit()
    dwh_cursor.close()
    dwh_conn.close()

def load_users():
    # Connect to the source
    origin_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    # Connect to the DWH
    dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    # Read data from the source
    origin_conn = origin_hook.get_conn()
    origin_cursor = origin_conn.cursor()
    origin_cursor.execute("SELECT * FROM users")
    rows = origin_cursor.fetchall()

    # Fetch column names
    colnames = [desc[0] for desc in origin_cursor.description]

    origin_cursor.close()
    origin_conn.close()

    # Write into the DWH
    dwh_conn = dwh_hook.get_conn()
    dwh_cursor = dwh_conn.cursor()

    # Truncate the table before loading
    dwh_cursor.execute("TRUNCATE TABLE stg.bonussystem_users")

    # Insert the data
    if rows:
        placeholders = ','.join(['%s'] * len(colnames))
        insert_query = f"INSERT INTO stg.bonussystem_users ({','.join(colnames)}) VALUES ({placeholders})"
        dwh_cursor.executemany(insert_query, rows)

    dwh_conn.commit()
    dwh_cursor.close()
    dwh_conn.close()

def load_events():
    # Connect to the source
    origin_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    # Connect to the DWH
    dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    # Obtain a DWH connection
    dwh_conn = dwh_hook.get_conn()
    dwh_cursor = dwh_conn.cursor()

    try:
        # 1. Read the load state from stg.srv_wf_settings
        workflow_key = 'bonussystem_events_load'
        dwh_cursor.execute("""
            SELECT workflow_settings
            FROM stg.srv_wf_settings
            WHERE workflow_key = %s
        """, (workflow_key,))

        result = dwh_cursor.fetchone()
        if result:
            raw_settings = result[0]
            # workflow_settings is stored as json, so psycopg already returns a dict.
            # After migrations it may arrive as a string, so normalize both cases.
            if isinstance(raw_settings, str):
                raw_settings = json.loads(raw_settings)
            settings = raw_settings or {}
            last_loaded_id = settings.get('last_loaded_id', 0)
        else:
            last_loaded_id = 0

        # 2. Read new records from the source
        origin_conn = origin_hook.get_conn()
        origin_cursor = origin_conn.cursor()
        origin_cursor.execute("""
            SELECT id, event_ts, event_type, event_value
            FROM outbox
            WHERE id > %s
            ORDER BY id
        """, (last_loaded_id,))
        rows = origin_cursor.fetchall()

        origin_cursor.close()
        origin_conn.close()

        if rows:
            # 3. Insert data into stg.bonussystem_events
            insert_query = """
                INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """
            dwh_cursor.executemany(insert_query, rows)

            # 4. Update the load state (in the same transaction!)
            new_last_id = rows[-1][0]  # id of the last record
            new_settings = json.dumps({'last_loaded_id': new_last_id})

            # Check whether the record already exists
            dwh_cursor.execute("""
                SELECT id FROM stg.srv_wf_settings WHERE workflow_key = %s
            """, (workflow_key,))

            if dwh_cursor.fetchone():
                # Update the existing record
                dwh_cursor.execute("""
                    UPDATE stg.srv_wf_settings
                    SET workflow_settings = %s
                    WHERE workflow_key = %s
                """, (new_settings, workflow_key))
            else:
                # Insert a new record
                dwh_cursor.execute("""
                    INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES (%s, %s)
                """, (workflow_key, new_settings))

        # Commit the transaction (steps 3 and 4 run atomically)
        dwh_conn.commit()

    except Exception as e:
        # Roll back the transaction on error
        dwh_conn.rollback()
        raise e
    finally:
        dwh_cursor.close()
        dwh_conn.close()

load_ranks_task = PythonOperator(
    task_id='load_ranks',
    python_callable=load_ranks,
    dag=dag,
)

load_users_task = PythonOperator(
    task_id='load_users',
    python_callable=load_users,
    dag=dag,
)

load_events_task = PythonOperator(
    task_id='events_load',
    python_callable=load_events,
    dag=dag,
)

load_ranks_task >> load_users_task >> load_events_task
