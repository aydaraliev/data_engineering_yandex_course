# Final Project: S3 → Vertica ETL

Final project for the Data Engineer course. An ETL pipeline that loads transaction data from S3 into the Vertica analytical warehouse.

## Architecture

```
┌─────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
│   Yandex S3     │     │   Vertica STAGING       │     │   Vertica DWH           │
│                 │     │                         │     │                         │
│ transactions_*  │────▶│ transactions (raw)      │────▶│ global_metrics (mart)   │
│ currencies_*    │     │ currencies   (raw)      │     │                         │
└─────────────────┘     └─────────────────────────┘     └─────────────────────────┘
        │                         │                               │
        └─────────────────────────┴───────────────────────────────┘
                              Airflow DAGs
```

## Project Layout

```
final-s3-to-vertica/
├── docker-compose.yaml          # Docker configuration
├── credentials.env              # Credentials (kept in .gitignore)
├── src/
│   ├── dags/
│   │   ├── 1_load_staging.py    # DAG: S3 → Staging
│   │   └── 2_load_dwh.py        # DAG: Staging → DWH
│   ├── sql/
│   │   ├── ddl_staging.sql      # DDL for staging tables
│   │   └── ddl_dwh.sql          # DDL for the mart
│   └── img/                     # Dashboard screenshots
```

## Data Layers

### STAGING (raw data)

#### `transactions` — transaction history

| Field                 | Type         | Description      |
|:----------------------|:-------------|:-----------------|
| operation_id          | VARCHAR(60)  | Operation ID     |
| account_number_from   | INT          | Sender account   |
| account_number_to     | INT          | Receiver account |
| currency_code         | INT          | Currency code    |
| country               | VARCHAR(50)  | Country          |
| status                | VARCHAR(30)  | Status           |
| transaction_type      | VARCHAR(30)  | Transaction type |
| amount                | INT          | Amount           |
| transaction_dt        | TIMESTAMP    | Date / time      |

#### `currencies` — exchange rates

| Field               | Type          | Description                       |
|:--------------------|:--------------|:----------------------------------|
| date_update         | TIMESTAMP     | Rate date                         |
| currency_code       | INT           | Currency code                     |
| currency_code_with  | INT           | Conversion currency (420 = USD)   |
| currency_code_div   | NUMERIC(10,5) | Conversion factor                 |

### DWH (mart)

#### `global_metrics` — aggregated metrics

| Field                            | Type           | Description          |
|:---------------------------------|:---------------|:---------------------|
| date_update                      | DATE           | Date                 |
| currency_from                    | INT            | Source currency      |
| amount_total                     | NUMERIC(18,2)  | Amount in USD        |
| cnt_transactions                 | INT            | Transaction count    |
| avg_transactions_per_account     | NUMERIC(18,2)  | Average per account  |
| cnt_accounts_make_transactions   | INT            | Unique accounts      |

## DAG Pipeline

```
DAG 1: 1_load_staging                    DAG 2: 2_load_dwh
┌─────────────────────────┐              ┌─────────────────────┐
│ load_currencies         │              │                     │
│         ↓               │──trigger────▶│ load_global_metrics │
│ load_transactions       │              │                     │
└─────────────────────────┘              └─────────────────────┘
```

**`1_load_staging`:**
- Loads data from S3 into Vertica staging.
- Uses `COPY` for bulk loading.
- Incremental loads keyed by date.

**`2_load_dwh`:**
- Aggregates data from staging into the mart.
- Deduplicates on `operation_id`.
- Converts amounts into USD.
- Filters out test accounts.

## Data Quality

| Issue                  | Solution                      |
|:-----------------------|:------------------------------|
| Duplicate transactions | `ROW_NUMBER()` over `operation_id` |
| Test accounts          | Filter `account_number > 0`   |
| Empty amounts          | Filter `amount > 0`           |
| Idempotency            | `DELETE` + `INSERT` per date  |

## Dashboard

Dashboard screenshots and SQL queries for the visualizations live in [`src/img/`](src/img/).

## Running

### 1. Start Docker

```bash
docker-compose up -d
```

### 2. Create the Vertica tables

```sql
-- Execute:
-- src/sql/ddl_staging.sql
-- src/sql/ddl_dwh.sql
```

### 3. Airflow UI

- URL: http://localhost:8280/airflow/
- Enable DAG `1_load_staging`.
- DAG `2_load_dwh` is triggered automatically.

## Airflow Connections

Credentials are stored in Airflow Connections, not in the code.

### `credentials.env`

Credentials live in `credentials.env` (which is listed in `.gitignore`):

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

### Create the connections via CLI

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

### Connection parameters

**yandex_s3**
- Endpoint: `https://storage.yandexcloud.net`
- Bucket: `final-project`

**vertica_conn**
- Host: `vertica.data-engineer.education-services.ru`
- Port: `5433`
- Database: `dwh`
- Staging schema: `VT251126648744__STAGING`
- DWH schema: `VT251126648744__DWH`
