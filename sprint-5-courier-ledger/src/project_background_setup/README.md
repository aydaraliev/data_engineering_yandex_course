# Project background setup

This directory bundles the DAGs and SQL scripts that stand up the data mart and load the source data before the main sprint tasks can run.

## Contents
- `dags/` - Airflow DAGs for incremental loading.
- `sql_scripts/` - DDL/DML for creating the STG/DDS/CDM schemas and populating aggregates.

## Prerequisites
- Container brought up from the repository root (`./start_docker.sh`), with Postgres reachable on `localhost:15432/de` (user/pass `jovyan`).
- Airflow connections: `PG_WAREHOUSE_CONNECTION` (DWH), `PG_ORIGIN_BONUS_SYSTEM_CONNECTION` (Postgres source), Mongo connection configured via variables (`MONGO_DB_*`) and the TLS certificate (`MONGO_DB_CERTIFICATE_PATH`).
- The DAGs need to be copied into the Airflow folder (`/lessons/dags` inside the container) or the project has to be mounted into `DAGS_FOLDER`.

## DAG execution order
1. `load_ranks_and_users` (01_*) - every 15 minutes pulls `ranks`, `users`, and `outbox` from the source Postgres into `stg.bonussystem_*` and tracks progress in `stg.srv_wf_settings`.
2. `stg_load_users_dag` (02_*) - every 15 minutes loads the `users` collection from MongoDB into `stg.ordersystem_users` with an `update_ts` checkpoint.
3. `stg_load_orders_dag` (03_*) - similarly pulls `orders` and `order_events` from MongoDB into `stg.ordersystem_orders` / `stg.ordersystem_order_events`.
4. `dds_init_srv_wf_settings` (04_*) - manual one-off run: creates `dds.srv_wf_settings` to hold the state of the DDS pipelines.
5. `dds_fill_snowflake` (05_*) - drives the DDS layer load (SCD tables `dm_users`, `dm_restaurants`, `dm_products`, `dm_timestamps`, plus facts/links) and records the state in `dds.srv_wf_settings`.
6. `dm_settlement_report` (06_*) - builds the settlement mart / reporting (DM/CDM) from the final DDS data.

## SQL script order
The file numbering reflects the recommended sequence:
- 01-22 (directory `src/project_background_setup/sql_scripts/`): baseline STG/DDS/CDM structures for the study example.

Run the scripts with `psql -f <script>.sql postgresql://jovyan:jovyan@localhost:15432/de` or through a `PostgresOperator` inside your own DAGs.

## Handy tips
- Airflow variables (`MONGO_DB_*`) and the TLS certificate have to be set manually; if you need connections to additional external databases, flag it up.
- When restarting the container, keep the state in `srv_wf_settings` and mount a Postgres volume so data is not lost.
