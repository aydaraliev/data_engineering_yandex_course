# Sprint 5 Project: Courier Payout Data Mart

## Goal and Result

Build a multi-layer DWH and a data mart `cdm.dm_courier_ledger` that collects courier payouts for the previous month: number of orders, total order and tip amounts, average rating, and the final payout.

## Data Sources

- Order subsystem (PostgreSQL) — orders, line items, payments, tips.
- Courier service (REST API) — couriers, deliveries/trips, ratings.
- Educational sources (PostgreSQL / MongoDB) from the `src/project_background_setup` folder.

## DWH Layers

- **STG**: raw data from the courier API stored as JSON (`stg.couriers_raw`, `stg.courier_deliveries_raw`) plus the educational tables.
- **DDS**: order dimensions and facts (already present) plus new `dds.dm_couriers` (SCD2), `dds.dm_deliveries`, and a `courier_id` column on `dds.dm_orders`.
- **CDM**: the `cdm.dm_courier_ledger` mart with monthly aggregation and fee/tip calculations.

## Data Requirements and Entities Coming From the API

- Mart fields: `courier_id`, `courier_name`, `settlement_year`, `settlement_month`, `orders_count`, `orders_total_sum`, `rate_avg`, `order_processing_fee`, `courier_order_sum`, `courier_tips_sum`, `courier_reward_sum`.
- DDS tables feeding the mart: the existing `dds.dm_orders`, `dds.dm_users`, `dds.dm_restaurants`, `dds.dm_products`, `dds.dm_timestamps`, `dds.fct_product_sales`; the new `dds.dm_couriers`, `dds.dm_deliveries`, and the new `courier_id` column on `dds.dm_orders`.
- Data loaded from the API: couriers (`courier_id`, name, `updated_at`); deliveries/trips (`order_id`, `courier_id`, `delivery_ts`, `sum`, `tip_sum`, `rate`, `updated_at`); individual ratings when they are available separately.

## SQL Scripts

- Baseline (educational) — `src/project_background_setup/sql_scripts/01-22_*`.
- Courier layer — `src/sql_courier/01_courier_stg_ddl.sql`, `02_courier_dds_ddl.sql`, `03_cdm_courier_ledger.sql`.
- To apply the courier scripts, create a `.env.local` from `.env.example` (variables `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` / `PGDATABASE`) and run `bash src/sql_courier/setup_courier_tables.sh`. The script invokes `psql` through `docker exec` inside the `de-pg-cr-af` container (override via the `CONTAINER` variable).

## Environment Deployment

1. Start the container stack: `./start_docker.sh` (Airflow at `localhost:3000/airflow`, PostgreSQL at `jovyan:jovyan@localhost:15432/de`).
2. Configure Airflow connections and variables:
   - `PG_WAREHOUSE_CONNECTION` → `postgresql://jovyan:jovyan@localhost:5432/de?sslmode=disable`
   - `PG_ORIGIN_BONUS_SYSTEM_CONNECTION` → the order source (Yandex Cloud example).
   - `MONGO_DB_*` for the educational examples.
   - Courier API: `COURIER_API_BASE_URL` (default `https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net`), `COURIER_API_KEY` (required), `COURIER_API_NICKNAME` (default `Aydar`), `COURIER_API_COHORT` (default `40`).

## DAGs and Calculations

- Example educational DAGs: `project_background_setup/01-06_*` — loads from PostgreSQL and MongoDB, population of DDS and DM.
- Courier DAG: `src/dags/courier_etl.py` — pulls the API (restaurants, couriers, deliveries for the last 7 days), populates STG (`stg.couriers_raw`, `stg.courier_deliveries_raw`, `stg.courier_restaurants_raw`), DDS (`dds.dm_couriers`, `dds.dm_deliveries`), and the `cdm.dm_courier_ledger` mart. Default schedule is daily at 04:00 UTC with `limit=50` pagination by offset.

## The `cdm.dm_courier_ledger` Mart

- Aggregation is keyed by order date (if delivery happens on the next day, the order date still drives the bucket).
- Formulas: `order_processing_fee = orders_total_sum * 0.25`; `courier_reward_sum = courier_order_sum + courier_tips_sum * 0.95`.
- Rating threshold applied to `courier_order_sum` per order and then aggregated:
  - r < 4 → max(5% × order_sum, 100)
  - 4 ≤ r < 4.5 → max(7% × order_sum, 150)
  - 4.5 ≤ r < 4.9 → max(8% × order_sum, 175)
  - r ≥ 4.9 → max(10% × order_sum, 200)
