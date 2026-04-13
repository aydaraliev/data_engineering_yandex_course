# Подготовка фона проекта

В этом каталоге собраны DAG’и и SQL-скрипты, которые разворачивают витрину и загружают исходные данные перед основными заданиями спринта.

## Состав
- `dags/` — DAG’и Airflow для инкрементальной загрузки.
- `sql_scripts/` — DDL/DML для создания схем STG/DDS/CDM и заполнения агрегатов.

## Предварительные требования
- Поднятый контейнер из корня (`./start_docker.sh`), Postgres доступен на `localhost:15432/de` (user/pass `jovyan`).
- Соединения Airflow: `PG_WAREHOUSE_CONNECTION` (DWH), `PG_ORIGIN_BONUS_SYSTEM_CONNECTION` (источник Postgres), Mongo соединение через переменные (`MONGO_DB_*`) и TLS сертификат (`MONGO_DB_CERTIFICATE_PATH`).
- DAG’и нужно скопировать в папку Airflow (`/lessons/dags` в контейнере) или смонтировать проект в `DAGS_FOLDER`.

## Порядок запуска DAG’ов
1. `load_ranks_and_users` (01_*) — каждые 15 минут тянет `ranks`, `users`, `outbox` из исходного Postgres в `stg.bonussystem_*` и ведёт прогресс в `stg.srv_wf_settings`.
2. `stg_load_users_dag` (02_*) — каждые 15 минут загружает коллекцию `users` из MongoDB в `stg.ordersystem_users` с чекпоинтом по `update_ts`.
3. `stg_load_orders_dag` (03_*) — аналогично тянет `orders` и `order_events` из MongoDB в `stg.ordersystem_orders`/`stg.ordersystem_order_events`.
4. `dds_init_srv_wf_settings` (04_*) — ручной однократный запуск: создаёт `dds.srv_wf_settings` для хранения состояний DDS-пайплайнов.
5. `dds_fill_snowflake` (05_*) — прогоняет загрузку слоёв DDS (SCD таблицы `dm_users`, `dm_restaurants`, `dm_products`, `dm_timestamps`, фкты/линки) и фиксирует состояние в `dds.srv_wf_settings`.
6. `dm_settlement_report` (06_*) — формирует витрину расчётов/отчётность (DM/CDM) по итоговым данным DDS.

## Порядок применения SQL-скриптов
Нумерация файлов отражает рекомендуемую последовательность:
- 01–22 (каталог `src/project_background_setup/sql_scripts/`): базовые структуры STG/DDS/CDM для учебного примера.

Выполняйте скрипты через `psql -f <script>.sql postgresql://jovyan:jovyan@localhost:15432/de` или через `PostgresOperator` внутри своих DAG’ов.

## Полезные подсказки
- Переменные Airflow (`MONGO_DB_*`) и TLS сертификат нужно задать вручную; при необходимости подключений к внешним БД дайте знать.
- Если перезапускаете контейнер, храните состояния в `srv_wf_settings` и монтируйте volume для Postgres, чтобы не потерять данные.***
