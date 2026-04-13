# Итоговый проект: витрина выплат курьерам

## Цель и результат
Построить многослойный DWH и витрину `cdm.dm_courier_ledger`, где собираются выплаты курьерам за прошлый месяц: количество заказов, суммы заказов и чаевых, средний рейтинг и итог к выплате.

## Источники данных
- Подсистема заказов (Postgres) — заказы, позиции, платежи, чаевые.
- Курьерская служба (API) — курьеры, доставки/рейсы, рейтинги.
- Учебные источники (Postgres/Mongo) из каталога `src/project_background_setup`.

## Слои DWH
- **STG**: сырые данные из API курьеров в JSON (`stg.couriers_raw`, `stg.courier_deliveries_raw`) + учебные таблицы.
- **DDS**: измерения/факты заказов (уже есть) + новые `dds.dm_couriers` (SCD2), `dds.dm_deliveries`, `courier_id` в `dds.dm_orders`.
- **CDM**: витрина `cdm.dm_courier_ledger` с месячной агрегацией и расчётом комиссий/чаевых.

## Требования к данным и сущности из API
- Поля витрины: `courier_id`, `courier_name`, `settlement_year`, `settlement_month`, `orders_count`, `orders_total_sum`, `rate_avg`, `order_processing_fee`, `courier_order_sum`, `courier_tips_sum`, `courier_reward_sum`.
- DDS для витрины: существующие `dds.dm_orders`, `dds.dm_users`, `dds.dm_restaurants`, `dds.dm_products`, `dds.dm_timestamps`, `dds.fct_product_sales`; новые `dds.dm_couriers`, `dds.dm_deliveries`, поле `courier_id` в `dds.dm_orders`.
- Что грузить из API: курьеры (`courier_id`, имя, updated_at), доставки/рейсы (`order_id`, `courier_id`, `delivery_ts`, `sum`, `tip_sum`, `rate`, `updated_at`), при наличии — отдельные оценки.

## SQL-скрипты
- Базовые (учебные) — `src/project_background_setup/sql_scripts/01-22_*`.
- Курьерский слой — `src/sql_courier/01_courier_stg_ddl.sql`, `02_courier_dds_ddl.sql`, `03_cdm_courier_ledger.sql`.
- Применение курьерских скриптов: создайте `.env.local` по образцу `.env.example` (переменные PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE), затем запустите `bash src/sql_courier/setup_courier_tables.sh`. Скрипт запускает `psql` через `docker exec` в контейнере `de-pg-cr-af` (можно сменить через переменную `CONTAINER`).

## Развёртывание окружения
1. Поднимите контейнер: `./start_docker.sh` (Airflow: `localhost:3000/airflow`, Postgres: `jovyan:jovyan@localhost:15432/de`).
2. В Airflow задайте connections/variables:
   - `PG_WAREHOUSE_CONNECTION` → `postgresql://jovyan:jovyan@localhost:5432/de?sslmode=disable`
   - `PG_ORIGIN_BONUS_SYSTEM_CONNECTION` → источник заказов (Yandex Cloud пример)
   - `MONGO_DB_*` для примеров.
   - API курьеров: `COURIER_API_BASE_URL` (по умолчанию `https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net`), `COURIER_API_KEY` (обязателен), `COURIER_API_NICKNAME` (по умолчанию `Aydar`), `COURIER_API_COHORT` (по умолчанию `40`).

## DAG’и и расчёты
- Примерные DAG’и (учебные): `project_background_setup/01-06_*` — загрузки из Postgres/Mongo, заполнение DDS и DM.
- Курьерский DAG: `src/dags/courier_etl.py` — выкачивает API (рестораны/курьеры/доставки за 7 дней), заполняет STG (`stg.couriers_raw`, `stg.courier_deliveries_raw`, `stg.courier_restaurants_raw`), DDS (`dds.dm_couriers`, `dds.dm_deliveries`) и витрину `cdm.dm_courier_ledger`. По умолчанию расписание ежедневно в 04:00 UTC, пагинация limit=50 с offset.

## Витрина `cdm.dm_courier_ledger`
- Расчёт по дате заказа (если доставка на следующий день — использовать дату заказа).
- Формулы: `order_processing_fee = orders_total_sum * 0.25`; `courier_reward_sum = courier_order_sum + courier_tips_sum * 0.95`.
- Порог по рейтингу для `courier_order_sum` (применяется к каждому заказу, затем агрегируется):  
  - r < 4 → max(5% * order_sum, 100)  
  - 4 ≤ r < 4.5 → max(7% * order_sum, 150)  
  - 4.5 ≤ r < 4.9 → max(8% * order_sum, 175)  
  - r ≥ 4.9 → max(10% * order_sum, 200)
