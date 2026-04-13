# Sprint 3 Project: Customer Retention ETL

A single-DAG ETL pipeline that lifts order data from an HTTP report API into PostgreSQL and builds two marts:

- `mart.f_sales` — a transactional sales fact table that models both shipped and refunded orders.
- `mart.f_customer_retention` — a weekly customer-retention mart with new, returning, and refunded customer counts plus revenue per group.

The whole pipeline runs in one Airflow DAG (`ETL_full_pipeline_alchemy`) and uses SQLAlchemy for every database interaction.

## Repository Layout

```
data_engineering/
├── migrations/                       # SQL migration files (empty placeholder)
├── project_sprint_3/
│   ├── ETL_full_pipeline_alchemy.py  # The full DAG
│   └── db_connect.txt                # Connection notes
├── src/
│   └── dags/                         # DAG placeholder directory
└── README.md                         # Boilerplate course instructions
```

## Local container

The starter README documents the training image, which packages VS Code, Airflow, and PostgreSQL in a single container:

```bash
docker run -d --rm \
    -p 3000:3000 -p 15432:5432 \
    --name=de-project-sprint-3-server \
    cr.yandex/crp1r8pht0n0gl25aug1/project-sprint-3:latest
```

After the container starts the following services are exposed:

1. Visual Studio Code.
2. Airflow.
3. PostgreSQL database.

## Pipeline Stages

The DAG is divided into three logical stages that match the translated docstring in `ETL_full_pipeline_alchemy.py`:

### Stage 1 — API ingest and staging

- Calls the training API:
  - `POST /generate_report` to kick off a report build; stores the returned `task_id` in XCom.
  - `GET /get_report` in a loop (up to 10 polls, 30-second delay) until the status is `SUCCESS` and a `report_id` is produced.
- Downloads three CSV files for the report:
  - `user_order_log.csv` → `stage.user_order_log`
  - `user_activity_log.csv` → `stage.user_activity_log`
  - `customer_research.csv` → `stage.customer_research`
- Normalises the order log: if the historical format lacks a `status` column, `shipped` is added as the default.
- Loads each file with `pandas.to_sql(if_exists="replace")` so the staging tables are rebuilt on every run, keeping the pipeline idempotent.

### Stage 2 — Fact table `mart.f_sales`

- Recomputes the transactional mart from `stage.user_order_log`.
- Shipped rows are signed `+1`, refunded rows `-1`, and the `payment_amount` is mirrored with the sign.
- Deduplication is achieved with `DISTINCT ON (uniq_id, status)` and the most recent `date_time`.
- A synthetic `order_id` is derived from `md5(uniq_id)` and cast to `bigint` via `('x' || substr(...))::bit(64)::bigint`; a comment in the SQL notes that for catalogues up to a million orders collision risk is negligible.
- The incremental window is guarded by `DELETE FROM mart.f_sales WHERE order_date IN (...)` followed by an `INSERT ... ON CONFLICT DO UPDATE`.

### Stage 3 — Weekly mart `mart.f_customer_retention`

A CTE pipeline computes, per `(week_start, item_id)`:

- **New customers** — customers whose only `shipped` order that week has no earlier `shipped` history.
- **Returning customers** — customers with prior `shipped` history or multiple shipments in the same week.
- **Refunded customers** — customers with at least one `refunded` row that week.
- Matching revenue metrics for each of the three groups (refunded revenue is reported as `SUM(ABS(gm))`).

Before inserting the new rows the DAG deletes any existing rows for the weeks being rebuilt, which keeps the mart idempotent on re-runs.

## Idempotency Summary

- Staging tables are rebuilt from scratch on every load (`pandas.to_sql(if_exists="replace")`).
- `mart.f_sales` and `mart.f_customer_retention` clear the date/week slice that is being recomputed before inserting.
- `init_db` uses `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS`, so rerunning the DAG does not fail and does not introduce duplicates.

## DAG Topology

```
init_db
  ├── ddl_stage      (PostgresOperator: DDL for stage.*)
  └── ddl_mart       (PostgresOperator: DDL for mart.*)
        ↓
create_report → wait_report → load_stage → refresh_f_sales → refresh_f_retention
```

Scheduling and runtime parameters:

- `schedule_interval="0 3 * * *"` — daily at 03:00.
- `catchup=False`.
- `max_active_runs=1`.
- `dagrun_timeout=3 hours`.

## DDL

```sql
-- stage.*
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
    action_id  INT,
    date_time  TIMESTAMP,
    quantity   INT
);

CREATE TABLE IF NOT EXISTS stage.customer_research (
    date_id     INT,
    customer_id INT,
    item_id     INT,
    quantity    INT
);
```

```sql
-- mart.*
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
    period_name                 TEXT,
    period_id                   DATE,
    item_id                     INT,
    new_customers_count         INT,
    returning_customers_count   INT,
    refunded_customer_count     INT,
    new_customers_revenue       NUMERIC,
    returning_customers_revenue NUMERIC,
    customers_refunded          NUMERIC,
    PRIMARY KEY (period_id, item_id)
);
```

## Configuration

- Airflow connection id: `pg_connection`.
- Local cache directory for CSV copies: `/opt/airflow/data`.
- External API: `https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net` with `X-API-KEY`, `X-Nickname`, `X-Cohort`, and `X-Project` headers. The training credentials live directly in the script.

## Notes on Course Workflow

The boilerplate README expects a repository named `de-project-sprint-3` that is linked automatically by the Practicum platform, with the usual `git add` / `commit` / `push` workflow on `main`. No separate `sprint 4` project is present in this workspace — the `sprint_4_data_quality` folder inside `data_engineering/` is empty.
