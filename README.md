# Yandex Practicum Data Engineer — Project Portfolio

English-language overview and translations of the project work completed during the Yandex Practicum Data Engineer course. This is a single portfolio repository: the English translations and short descriptions live at the top level (`sprint-*/` and `final-*/`), while the original project source code — snapshotted from each GitHub repo — is embedded under [`de-projects/`](de-projects/) as example material. The source READMEs are in Russian; the translations and overviews here are in English.

Sprint coverage: 3, 5, 6, 7, 8, 9, and the final capstone. The course's sprint 4 (data quality) did not produce a standalone project.

## Repository layout

- `README.md` — this overview.
- `sprint-3-customer-retention/` … `sprint-9-guest-tagging/`, `final-s3-to-vertica/` — English write-ups for each deliverable.
- `de-projects/` — snapshotted source code for every deliverable.
- `certificates/` — course-completion certificates ([English](certificates/certificate_Aliev_Aydar_DE_ENG.pdf), [Russian](certificates/certificate_Aliev_Aydar_DE_RUS.pdf)).

## Project index

| # | Description | Title | Source code | Stack | Theme |
|---|-------------|-------|-------------|-------|-------|
| 1 | [sprint-3-customer-retention](sprint-3-customer-retention/README.md) | Customer Retention ETL | [de-projects/de-project-sprint-3](de-projects/de-project-sprint-3/) | Airflow, PostgreSQL, SQLAlchemy, pandas | API → stage → `mart.f_sales` + weekly retention mart |
| 2 | [sprint-5-courier-ledger](sprint-5-courier-ledger/README.md) | Courier Payout Data Mart | [de-projects/de-project-sprint-5](de-projects/de-project-sprint-5/) | Airflow, PostgreSQL, REST API | Multi-layer DWH (STG → DDS → CDM) |
| 3 | [sprint-6-group-conversion](sprint-6-group-conversion/README.md) | Social Network Group Conversion Analysis | [de-projects/de-project-sprint-6](de-projects/de-project-sprint-6/) | Airflow, Vertica, S3, pandas | Data Vault extension, analytical SQL |
| 4 | [sprint-7-geo-recommendations](sprint-7-geo-recommendations/README.md) | Geo Recommendations Data Lake | [de-projects/de-project-sprint-7](de-projects/de-project-sprint-7/) | PySpark, HDFS, YARN, Airflow | Three-layer Data Lake (RAW → ODS → MART) |
| 5 | [sprint-8-streaming-notifications](sprint-8-streaming-notifications/README.md) | Restaurant Subscription Streaming Service | [de-projects/de-project-sprint-8](de-projects/de-project-sprint-8/) | Spark Structured Streaming, Kafka, PostgreSQL | Real-time join of stream and reference data |
| 6 | [sprint-9-guest-tagging](sprint-9-guest-tagging/README.md) | Guest Tagging DWH Pipeline | [de-projects/de-project-sprint-9](de-projects/de-project-sprint-9/) | Python, Kafka, Redis, PostgreSQL, Docker, Kubernetes, Helm | Microservices STG/DDS/CDM over Data Vault 2.0 |
| 7 | [final-s3-to-vertica](final-s3-to-vertica/README.md) | Final Project: S3 → Vertica ETL | [de-projects/de-project-final](de-projects/de-project-final/) | Airflow, Vertica, S3 | End-to-end ETL with staging and reporting mart |

## Short descriptions (English)

### Sprint 3 — Customer Retention ETL

Single-DAG Airflow pipeline (`ETL_full_pipeline_alchemy`) that pulls a daily training-API report, materialises three staging tables from CSV with pandas, and rebuilds two marts in PostgreSQL: a transactional `mart.f_sales` fact (with a signed `sign` column that models refunds as negative amounts) and a weekly `mart.f_customer_retention` mart computing new, returning, and refunded customer counts together with revenue per group. All database work goes through SQLAlchemy; idempotency is enforced by `to_sql(if_exists="replace")` on staging and by targeted `DELETE` + `INSERT` windows on the marts.

### Sprint 5 — Courier Payout Data Mart

Extends an existing food-delivery DWH with a monthly courier payout mart `cdm.dm_courier_ledger`. The pipeline pulls raw courier, delivery, and rating data from a REST API into the staging layer, normalises them into DDS (including an SCD2 `dm_couriers` dimension and a new `dm_deliveries` fact), and computes the final mart with order-processing fees, tips, rating-based reward tiers, and net reward per courier. Orchestrated with Airflow, backed by PostgreSQL, and deployed via Docker Compose.

### Sprint 6 — Social Network Group Conversion Analysis

Extends a Vertica-based Data Vault warehouse with new staging, link, and satellite tables describing user activity inside social-network groups. An Airflow DAG ingests a `group_log.csv` dump from Yandex S3, handles nullable inviter IDs with pandas `Int64`, and performs idempotent incremental loads using `NOT EXISTS` guards and Vertica's `HASH()` function for surrogate keys. A companion analytical query ranks the ten oldest groups by conversion from join to first message, highlighting the best targets for ad placement.

### Sprint 7 — Geo Recommendations Data Lake

Production-grade Data Lake on HDFS that powers geo-aware analytics for an Australian social network. Three layers (RAW → ODS → MART) are implemented in PySpark: an ODS layer performs one-time Haversine-based city resolution with a broadcast join on a 24-row city dictionary, and three downstream marts compute per-user geography, weekly/monthly zone aggregates, and friend-recommendation candidates (subscribed to the same channel, never messaged before, within 1 km, in the same city). The pipeline is scheduled sequentially by Airflow with adaptive query execution and configurable sampling for fast iteration.

### Sprint 8 — Restaurant Subscription Streaming Service

Spark Structured Streaming service that joins a Kafka stream of restaurant promotional campaigns with a static PostgreSQL table of subscribers, producing two sinks: an enriched `subscribers_feedback` table (with a `feedback` column for later reviews) and a Kafka `topic_out` payload consumed by a push-notification service. The job runs over SASL/SSL Kafka, uses `foreachBatch` for the dual write, and was hardened after code review with an explicit Java truststore, checkpointed state, typed casts, and a unique index that guarantees idempotency across retries.

### Sprint 9 — Guest Tagging DWH Pipeline

Three-microservice pipeline (STG, DDS, CDM) that tags restaurant guests based on their order history. Each service is a Flask application driven by APScheduler, consumes from and produces to Kafka, persists to PostgreSQL, and — in the STG service — enriches order events with user and restaurant data from Redis. DDS follows Data Vault 2.0 (hubs, links, satellites keyed by deterministic MD5-derived UUIDs); CDM maintains per-user product and category counters guarded by a `srv_processed_orders` table for exact-once semantics. Deployed via Docker Compose locally and via Helm charts to Yandex Managed Kubernetes using images in Yandex Container Registry.

### Final Project — S3 → Vertica ETL

Capstone ETL that moves transaction and currency files from Yandex S3 into Vertica. One Airflow DAG bulk-loads raw rows into a staging schema via `COPY`; a second, chained DAG dedupes on `operation_id`, filters out test accounts and zero amounts, converts amounts to USD using the `currencies` table (code 420 = USD), and writes the aggregated `global_metrics` mart (daily volume, transaction count, average per account, unique accounts). Idempotent by day via `DELETE + INSERT`, with a dashboard built on top of the mart.
