"""
DAG for automated refresh of geo marts using the ODS layer.

Architecture:
1. create_ods_layer - builds the ODS layer (events with cities)
2. user_geo_report - user geo analytics (reads from ODS)
3. zone_mart - event aggregation by zones (reads from ODS)
4. friend_recommendations - friend recommendations (reads from ODS)

Benefits of the ODS layer:
- The expensive city resolution operation runs only once
- 3x speed-up (city resolution done once instead of three times)
- Enriched data is reused across every mart

Schedule: daily at 00:00 UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

def log_start(**context):
    """Logs the start of the mart refresh."""
    execution_date = context['execution_date']
    print(f"Starting geo mart refresh for date: {execution_date}")
    return f"Started at {datetime.now()}"

def log_end(**context):
    """Logs a successful completion of the refresh."""
    execution_date = context['execution_date']
    print(f"All geo marts refreshed successfully for date: {execution_date}")
    return f"Completed at {datetime.now()}"

# Default parameters for every task
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'geo_marts_update',
    default_args=default_args,
    description='Daily update of geo-based data marts',
    schedule_interval='0 0 * * *',  # Daily at 00:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=['geo', 'marts', 'daily'],
)

# Fetch sample_fraction from Airflow Variables (defaults to 1.0 = all data)
try:
    sample_fraction = float(Variable.get("geo_marts_sample_fraction", default_var="1.0"))
except Exception:
    sample_fraction = 1.0

# Build application_args for sampling mode
sample_args = ['--sample', str(sample_fraction)] if sample_fraction < 1.0 else []

# DAG start - logging
start = PythonOperator(
    task_id='start',
    python_callable=log_start,
    provide_context=True,
    dag=dag,
)

# Task 1: Build the ODS layer (events with cities)
create_ods_layer = SparkSubmitOperator(
    task_id='create_ods_layer',
    application='/lessons/scripts/create_ods_layer.py',
    conn_id='yarn_spark',
    name='create_ods_layer',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': '20',
        'spark.sql.parquet.compression.codec': 'snappy',  # OPTIMIZATION: Snappy compression
    },
    application_args=sample_args,  # Support sample mode via Airflow Variables
    py_files='/lessons/scripts/geo_utils.py',
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Task 2: Refresh user_geo_report
update_user_geo_report = SparkSubmitOperator(
    task_id='update_user_geo_report',
    application='/lessons/scripts/user_geo_mart.py',
    conn_id='yarn_spark',
    name='user_geo_mart_update',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': '20',  # Reduced from 200 for 1.5GB dataset
        'spark.sql.parquet.compression.codec': 'snappy',  # OPTIMIZATION: Snappy compression
    },
    application_args=sample_args,  # Support sample mode via Airflow Variables
    py_files='/lessons/scripts/geo_utils.py',
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Task 3: Refresh zone_mart
update_zone_mart = SparkSubmitOperator(
    task_id='update_zone_mart',
    application='/lessons/scripts/zone_mart.py',
    conn_id='yarn_spark',
    name='zone_mart_update',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': '20',  # Reduced from 200 for 1.5GB dataset
        'spark.sql.parquet.compression.codec': 'snappy',  # OPTIMIZATION: Snappy compression
    },
    application_args=sample_args,  # Support sample mode via Airflow Variables
    py_files='/lessons/scripts/geo_utils.py',
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Task 4: Refresh friend_recommendations
update_friend_recommendations = SparkSubmitOperator(
    task_id='update_friend_recommendations',
    application='/lessons/scripts/friend_recommendations.py',
    conn_id='yarn_spark',
    name='friend_recommendations_update',
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': '20',  # Reduced from 200 for 1.5GB dataset
        'spark.sql.parquet.compression.codec': 'snappy',  # OPTIMIZATION: Snappy compression
    },
    application_args=sample_args,  # Support sample mode via Airflow Variables
    py_files='/lessons/scripts/geo_utils.py',
    driver_memory='4g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# DAG end - logging
end = PythonOperator(
    task_id='end',
    python_callable=log_end,
    provide_context=True,
    dag=dag,
)

# Define execution order
# OPTIMIZATION: first build the ODS layer (events with cities),
# then every mart reads from ODS instead of RAW
# start -> create_ods_layer -> [user_geo_report, zone_mart, friend_recommendations] -> end
#
# The marts could now run in parallel since they read from ODS,
# but they execute sequentially to optimize cluster resources
start >> create_ods_layer >> update_user_geo_report >> update_zone_mart >> update_friend_recommendations >> end
