# ETL Pipeline: S3 → Vertica

Финальный проект курса Data Engineer. ETL-пайплайн для загрузки данных о транзакциях из S3 в аналитическое хранилище Vertica.

## Архитектура

```
┌─────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
│   Yandex S3     │     │   Vertica STAGING       │     │   Vertica DWH           │
│                 │     │                         │     │                         │
│ transactions_*  │────▶│ transactions (сырые)    │────▶│ global_metrics (витрина)│
│ currencies_*    │     │ currencies (сырые)      │     │                         │
└─────────────────┘     └─────────────────────────┘     └─────────────────────────┘
        │                         │                               │
        └─────────────────────────┴───────────────────────────────┘
                              Airflow DAGs
```

## Структура проекта

```
├── docker-compose.yaml          # Docker-конфигурация
├── credentials.env              # Credentials (в .gitignore)
├── src/
│   ├── dags/
│   │   ├── 1_load_staging.py    # DAG: S3 → Staging
│   │   └── 2_load_dwh.py        # DAG: Staging → DWH
│   ├── sql/
│   │   ├── ddl_staging.sql      # DDL таблиц staging
│   │   └── ddl_dwh.sql          # DDL витрины
│   └── img/                     # Скриншоты дашборда
```

## Слои данных

### STAGING (сырые данные)

#### transactions — история транзакций

| Поле                 | Тип         | Описание           |
|:---------------------|:------------|:-------------------|
| operation_id         | VARCHAR(60) | ID операции        |
| account_number_from  | INT         | Счёт отправителя   |
| account_number_to    | INT         | Счёт получателя    |
| currency_code        | INT         | Код валюты         |
| country              | VARCHAR(50) | Страна             |
| status               | VARCHAR(30) | Статус             |
| transaction_type     | VARCHAR(30) | Тип транзакции     |
| amount               | INT         | Сумма              |
| transaction_dt       | TIMESTAMP   | Дата/время         |

#### currencies — курсы валют

| Поле               | Тип           | Описание                     |
|:-------------------|:--------------|:-----------------------------|
| date_update        | TIMESTAMP     | Дата курса                   |
| currency_code      | INT           | Код валюты                   |
| currency_code_with | INT           | Валюта конвертации (420=USD) |
| currency_code_div  | NUMERIC(10,5) | Коэффициент                  |

### DWH (витрина)

#### global_metrics — агрегированные метрики

| Поле                           | Тип           | Описание              |
|:-------------------------------|:--------------|:----------------------|
| date_update                    | DATE          | Дата                  |
| currency_from                  | INT           | Код валюты            |
| amount_total                   | NUMERIC(18,2) | Сумма в USD           |
| cnt_transactions               | INT           | Кол-во транзакций     |
| avg_transactions_per_account   | NUMERIC(18,2) | Среднее на аккаунт    |
| cnt_accounts_make_transactions | INT           | Уникальных аккаунтов  |

## DAG Pipeline

```
DAG 1: 1_load_staging                    DAG 2: 2_load_dwh
┌─────────────────────────┐              ┌─────────────────────┐
│ load_currencies         │              │                     │
│         ↓               │──trigger────▶│ load_global_metrics │
│ load_transactions       │              │                     │
└─────────────────────────┘              └─────────────────────┘
```

**1_load_staging:**
- Загружает данные из S3 в Vertica Staging
- Использует COPY для bulk-загрузки
- Инкрементальная загрузка по дате

**2_load_dwh:**
- Агрегирует данные из Staging в витрину
- Дедупликация по operation_id
- Конвертация сумм в USD
- Фильтрация тестовых аккаунтов

## Качество данных

| Проблема               | Решение                       |
|:-----------------------|:------------------------------|
| Дубликаты транзакций   | ROW_NUMBER() по operation_id  |
| Тестовые аккаунты      | Фильтр account_number > 0     |
| Пустые суммы           | Фильтр amount > 0             |
| Идемпотентность        | DELETE + INSERT по дате       |

## Дашборд

Скриншоты дашборда и SQL-запросы для визуализаций: [`src/img/`](src/img/)

## Запуск

### 1. Запуск Docker

```bash
docker-compose up -d
```

### 2. Создание таблиц в Vertica

```sql
-- Выполнить:
-- src/sql/ddl_staging.sql
-- src/sql/ddl_dwh.sql
```

### 3. Airflow UI

- URL: http://localhost:8280/airflow/
- Включить DAG `1_load_staging`
- DAG `2_load_dwh` запустится автоматически

## Подключения Airflow

Credentials хранятся в Airflow Connections (не в коде).

### Файл credentials.env

Credentials хранятся в `credentials.env` (добавлен в .gitignore):

```bash
# S3 (Yandex Cloud)
S3_ENDPOINT_URL=https://storage.yandexcloud.net
S3_ACCESS_KEY_ID=<YOUR_KEY>
S3_SECRET_ACCESS_KEY=<YOUR_SECRET>
S3_BUCKET=final-project

# Vertica
VERTICA_HOST=vertica.data-engineer.education-services.ru
VERTICA_PORT=5433
VERTICA_DATABASE=dwh
VERTICA_USER=<YOUR_USER>
VERTICA_PASSWORD=<YOUR_PASSWORD>
```

### Создание подключений через CLI

```bash
source credentials.env

docker exec -it de-final-prj-local bash -c "
airflow connections add 'yandex_s3' \
    --conn-type 'aws' \
    --conn-extra '{
        \"endpoint_url\": \"$S3_ENDPOINT_URL\",
        \"aws_access_key_id\": \"$S3_ACCESS_KEY_ID\",
        \"aws_secret_access_key\": \"$S3_SECRET_ACCESS_KEY\"
    }'

airflow connections add 'vertica_conn' \
    --conn-type 'generic' \
    --conn-host '$VERTICA_HOST' \
    --conn-port $VERTICA_PORT \
    --conn-schema '$VERTICA_DATABASE' \
    --conn-login '$VERTICA_USER' \
    --conn-password '$VERTICA_PASSWORD'
"
```

### Параметры подключений

**yandex_s3**
- Endpoint: https://storage.yandexcloud.net
- Bucket: final-project

**vertica_conn**
- Host: vertica.data-engineer.education-services.ru
- Port: 5433
- Database: dwh
- Schema Staging: VT251126648744__STAGING
- Schema DWH: VT251126648744__DWH
