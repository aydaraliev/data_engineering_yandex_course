"""
────────────────────────────── DAG: ETL_full_pipeline_alchemy ───────────────────────────────

Stage 1:
    - Pull data from the API (/generate_report, /get_report).
    - Load CSV files into stage.user_order_log and the other stage tables.
    - Handle shipped / refunded statuses.
    - Rebuild the transactional mart mart.f_sales (refunded rows get negative amounts).

Stage 2:
    - Build the weekly mart mart.f_customer_retention:
        * new, returning and refunded customers;
        * revenue per group.

Stage 3:
    - Idempotency:
        * Stage tables are rebuilt on every batch (pandas.to_sql(if_exists="replace")).
        * mart.f_sales and mart.f_customer_retention are cleared for the dates / weeks
          being recomputed.
        * Re-running the DAG never introduces duplicates.

SQLAlchemy is used for every database operation:
    - PostgresHook.get_sqlalchemy_engine()
    - pandas.to_sql for the stage layer
    - engine.begin() + text() for DML and DDL
"""

import datetime as dt
import os
import time
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# ─────────────────────────────── 1. Constants ───────────────────────────────
PG_CONN_ID = "pg_connection"                    # Airflow connection to Postgres
LOCAL_DIR = Path("/opt/airflow/data")
LOCAL_DIR.mkdir(exist_ok=True, parents=True)

API_HOST  = "d5dg1j9kt695d30blp03.apigw.yandexcloud.net"
API_TOKEN = os.environ["PRACTICUM_API_TOKEN"]

NICKNAME  = "Ajdar"
COHORT    = "40"

HEADERS = {
    "X-API-KEY": API_TOKEN,
    "X-Nickname": NICKNAME,
    "X-Cohort": COHORT,
    "X-Project": "True",
}

# ─────────────────────────────── 2. DDL ───────────────────────────────
DDL_STAGE = """
CREATE SCHEMA IF NOT EXISTS stage;

CREATE TABLE IF NOT EXISTS stage.user_order_log (
    order_id        BIGINT,
    date_time       TIMESTAMP,
    item_id         INT,
    customer_id     INT,
    quantity        INT,
    payment_amount  NUMERIC,
    status          VARCHAR(10) DEFAULT 'shipped'
);

CREATE TABLE IF NOT EXISTS stage.user_activity_log (
    action_id    INT,
    date_time    TIMESTAMP,
    quantity     INT
);

CREATE TABLE IF NOT EXISTS stage.customer_research (
    date_id     INT,
    customer_id INT,
    item_id     INT,
    quantity    INT
);
"""

DDL_MART = """
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.f_sales (
    order_id        BIGINT,
    order_status    VARCHAR(10),
    order_date      DATE,
    item_id         INT,
    customer_id     INT,
    price           NUMERIC,
    quantity        INT,
    payment_amount  NUMERIC,
    sign            SMALLINT,
    PRIMARY KEY (order_id, order_status)
);

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    period_name                  TEXT,
    period_id                    DATE,
    item_id                      INT,
    new_customers_count          INT,
    returning_customers_count    INT,
    refunded_customer_count      INT,
    new_customers_revenue        NUMERIC,
    returning_customers_revenue  NUMERIC,
    customers_refunded           NUMERIC,
    PRIMARY KEY (period_id, item_id)
);
"""

# ─────────────────────────────── 3. SQLAlchemy helpers ───────────────────────────────
def get_engine():
    """Return a SQLAlchemy Engine bound to the Postgres connection."""
    return PostgresHook(postgres_conn_id=PG_CONN_ID).get_sqlalchemy_engine()

def exec_sql(sql: str):
    """Execute multi-statement SQL inside a single transaction."""
    engine = get_engine()
    with engine.begin() as conn:
        for stmt in sql.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))

# ─────────────────────────────── 4. Tasks ───────────────────────────────

# ───── Stage 1: API
def create_report(**context):
    """
    POST /generate_report - kick off report generation.
    Push the returned task_id into XCom.
    """
    r = requests.post(f"https://{API_HOST}/generate_report", headers=HEADERS, timeout=10)
    r.raise_for_status()
    context["ti"].xcom_push(key="task_id", value=r.json()["task_id"])

def wait_report(**context):
    """
    GET /get_report - poll until status == SUCCESS.
    Push the report_id needed to download the CSV files.
    """
    task_id = context["ti"].xcom_pull(key="task_id")
    for _ in range(10):
        resp = requests.get(
            f"https://{API_HOST}/get_report",
            headers=HEADERS,
            params={"task_id": task_id},
            timeout=10,
        ).json()
        if resp["status"] == "SUCCESS":
            context["ti"].xcom_push(key="report_id", value=resp["data"]["report_id"])
            return
        time.sleep(30)
    raise TimeoutError("Report not ready after 10 attempts")

# ───── Stage 1: load into stage
def load_stage(**context):
    """
    Download CSV files and load them into stage.*.
    - If the status column is missing (legacy format) default it to 'shipped'.
    - pandas.to_sql(if_exists='replace') rebuilds each stage table per batch,
      which underpins the Stage 3 idempotency guarantee.
    """
    report_id = context["ti"].xcom_pull(key="report_id")
    base = (
        f"https://storage.yandexcloud.net/s3-sprint3/"
        f"cohort_{COHORT}/{NICKNAME}/project/{report_id}/{{}}"
    )
    files = {
        "user_order_log.csv": "user_order_log",
        "user_activity_log.csv": "user_activity_log",
        "customer_research.csv": "customer_research",
    }

    engine = get_engine()

    for fname, table in files.items():
        df = pd.read_csv(base.format(fname))
        if table == "user_order_log" and "status" not in df.columns:
            df["status"] = "shipped"

        df.to_csv(LOCAL_DIR / fname, index=False)  # keep a local copy

        df.to_sql(
            name=table,
            con=engine,
            schema="stage",
            if_exists="replace",
            index=False,
            method="multi"
        )

# ───── Stage 1: mart.f_sales
def refresh_f_sales(**_):
    """
    Rebuild the transactional mart mart.f_sales:
    - shipped → +, refunded → - (recorded in the sign column).
    - Only the dates present in stage.user_order_log are deleted before insert
      (Stage 3 idempotency).
    """
    sql = """
          DELETE FROM mart.f_sales
           WHERE order_date IN (SELECT DISTINCT date_time::date FROM stage.user_order_log);
          INSERT INTO mart.f_sales
            (order_id, order_status, order_date,
             item_id, customer_id, price, quantity, payment_amount, sign)
          -- A dedicated id-mapping table would be cleaner, but md5-derived ids are fine
          -- for catalogues up to ~1M orders. At 100M orders the collision probability is
          -- p ≈ (10^16) / (3.7×10^19) ≈ 0.00027 (~1 in 3.7k). Acceptable unless we are Amazon.
          SELECT DISTINCT ON (uniq_id, status)
                 ('x' || substr(md5(uniq_id), 1, 16))::bit(64)::bigint AS order_id,
                 status,
                 date_time::date,
                 item_id,
                 customer_id,
                 payment_amount/NULLIF(quantity,0),
                 quantity,
                 payment_amount * CASE WHEN status='refunded' THEN -1 ELSE 1 END,
                 CASE WHEN status='refunded' THEN -1 ELSE 1 END
          FROM stage.user_order_log
          ORDER BY uniq_id, status, date_time DESC
          ON CONFLICT (order_id, order_status) DO UPDATE
          SET order_date      = EXCLUDED.order_date,
              item_id         = EXCLUDED.item_id,
              customer_id     = EXCLUDED.customer_id,
              price           = EXCLUDED.price,
              quantity        = EXCLUDED.quantity,
              payment_amount  = EXCLUDED.payment_amount,
              sign            = EXCLUDED.sign;
          """
    exec_sql(sql)

# ───── Stage 2: mart.f_customer_retention
def refresh_f_retention(**_):
    """
    Build the weekly customer-retention mart:
    - new, returning, refunded customers;
    - revenue per group;
    - only the weeks in the current increment are deleted before insert
      (Stage 3 idempotency).
    """
    sql = """
          WITH sales AS (
              SELECT order_date, item_id, customer_id, payment_amount, order_status
              FROM mart.f_sales
          )
          , by_week AS (
              SELECT date_trunc('week', order_date)::date AS week_start,
                     item_id,
                     customer_id,
                     SUM(CASE WHEN order_status='shipped'  THEN 1 END) AS shipped_cnt,
                     SUM(CASE WHEN order_status='refunded' THEN 1 END) AS refunded_cnt,
                     SUM(payment_amount) AS gm
              FROM sales
              GROUP BY 1,2,3
          )
          , flags AS (
              SELECT bw.*,
                     EXISTS (SELECT 1 FROM sales s2
                             WHERE s2.customer_id=bw.customer_id
                               AND s2.order_status='shipped'
                               AND s2.order_date < bw.week_start) AS has_history
              FROM by_week bw
          )
          , agg AS (
              SELECT week_start AS period_id,
                     item_id,
                     COUNT(*) FILTER (WHERE shipped_cnt=1 AND NOT has_history)         AS new_customers_count,
                     COUNT(*) FILTER (WHERE shipped_cnt>1 OR has_history)             AS returning_customers_count,
                     COUNT(*) FILTER (WHERE refunded_cnt>0)                           AS refunded_customer_count,
                     SUM(gm) FILTER (WHERE shipped_cnt=1 AND NOT has_history)         AS new_customers_revenue,
                     SUM(gm) FILTER (WHERE shipped_cnt>1 OR has_history)              AS returning_customers_revenue,
                     SUM(ABS(gm)) FILTER (WHERE refunded_cnt>0)                       AS customers_refunded
              FROM flags
              GROUP BY period_id,item_id
          )
          , deleted AS (
                DELETE FROM mart.f_customer_retention
                WHERE period_id IN (SELECT DISTINCT period_id FROM agg)
          )
          INSERT INTO mart.f_customer_retention
                (period_name, period_id, item_id,
                 new_customers_count, returning_customers_count, refunded_customer_count,
                 new_customers_revenue, returning_customers_revenue, customers_refunded)
          SELECT 'weekly', *
          FROM agg;
          """
    exec_sql(sql)

# ─────────────────────────────── 5. DAG ───────────────────────────────
with DAG(
    dag_id="ETL_full_pipeline_alchemy",
    start_date=dt.datetime(2021, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=dt.timedelta(hours=3),
) as dag:

    # Stage 3: init_db (idempotent schema and table creation)
    with TaskGroup("init_db") as init_db:
        ddl_stage = PostgresOperator(
            task_id="ddl_stage",
            postgres_conn_id=PG_CONN_ID,
            sql=DDL_STAGE,
        )
        ddl_mart = PostgresOperator(
            task_id="ddl_mart",
            postgres_conn_id=PG_CONN_ID,
            sql=DDL_MART,
        )

    # Stage 1: API → Stage → mart.f_sales
    t_create = PythonOperator(task_id="create_report", python_callable=create_report)
    t_wait   = PythonOperator(task_id="wait_report",   python_callable=wait_report)
    t_load   = PythonOperator(task_id="load_stage",    python_callable=load_stage)
    t_f_sales   = PythonOperator(task_id="refresh_f_sales",     python_callable=refresh_f_sales)

    # Stage 2: mart.f_customer_retention
    t_retention = PythonOperator(task_id="refresh_f_retention", python_callable=refresh_f_retention)

    # Task dependencies
    init_db >> t_create >> t_wait >> t_load >> t_f_sales >> t_retention
