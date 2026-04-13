import json
from datetime import timedelta
from typing import Any, Dict, Iterable, List

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_LIMIT = 50


def _get_api_params() -> Dict[str, str]:
    def _from_env(name: str, default: str = "") -> str:
        return Variable.get(name, default_var=default) or default

    base_url = _from_env("COURIER_API_BASE_URL", "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net")
    api_key = _from_env("COURIER_API_KEY")
    nickname = _from_env("COURIER_API_NICKNAME", "Aydar")
    cohort = _from_env("COURIER_API_COHORT", "40")

    if not api_key:
        raise ValueError("COURIER_API_KEY is not set in Airflow Variables or environment")

    headers = {
        "X-Nickname": nickname,
        "X-Cohort": cohort,
        "X-API-KEY": api_key,
    }
    return {"base_url": base_url, "headers": headers}


def _fetch_paginated(endpoint: str, extra_params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    api = _get_api_params()
    base_url = api["base_url"]
    headers = api["headers"]

    offset = 0
    limit = DEFAULT_LIMIT

    while True:
        params = {"limit": limit, "offset": offset, **extra_params}
        resp = requests.get(f"{base_url}{endpoint}", headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        items = resp.json()
        if not items:
            break
        for item in items:
            yield item
        if len(items) < limit:
            break
        offset += limit


def _upsert_raw(table: str, items: List[Dict[str, Any]], dwh: PostgresHook, ts_field: str) -> None:
    if not items:
        return

    rows = []
    for obj in items:
        object_id = str(obj.get("_id") or obj.get("id") or obj.get("order_id") or obj.get("delivery_id"))
        if not object_id:
            continue
        update_ts = obj.get(ts_field) or pendulum.now("UTC").to_datetime_string()
        rows.append((object_id, json.dumps(obj), update_ts))

    if not rows:
        return

    with dwh.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {table} (object_id, object_value, update_ts)
                VALUES (%s, %s, %s)
                ON CONFLICT (object_id) DO UPDATE
                SET object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts,
                    load_ts = now();
                """,
                rows,
            )
        conn.commit()


@dag(
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["couriers", "stg", "dds", "cdm"],
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)},
)
def courier_etl():
    dwh = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    @task
    def extract_restaurants():
        items = list(_fetch_paginated("/restaurants", {"sort_field": "id", "sort_direction": "asc"}))
        _upsert_raw("stg.courier_restaurants_raw", items, dwh, ts_field="updated_at")

    @task
    def extract_couriers():
        items = list(_fetch_paginated("/couriers", {"sort_field": "id", "sort_direction": "asc"}))
        _upsert_raw("stg.couriers_raw", items, dwh, ts_field="updated_at")

    @task
    def extract_deliveries():
        now = pendulum.now("UTC")
        date_from = now.subtract(days=7).strftime("%Y-%m-%d %H:%M:%S")
        date_to = now.strftime("%Y-%m-%d %H:%M:%S")
        items = list(
            _fetch_paginated(
                "/deliveries",
                {
                    "sort_field": "_id",
                    "sort_direction": "asc",
                    "from": date_from,
                    "to": date_to,
                },
            )
        )
        _upsert_raw("stg.courier_deliveries_raw", items, dwh, ts_field="delivery_ts")

    @task
    def load_dds_couriers():
        sql = """
        WITH src AS (
            SELECT object_id AS courier_bk,
                   (object_value->>'name') AS courier_name,
                   COALESCE((object_value->>'updated_at')::timestamp, timestamp '2020-01-01') AS updated_at
            FROM stg.couriers_raw
        ),
        closed AS (
            UPDATE dds.dm_couriers dc
               SET active_to = NOW()
             FROM src s
            WHERE dc.courier_id = s.courier_bk
              AND dc.active_to = timestamp '2099-12-31'
              AND dc.courier_name IS DISTINCT FROM s.courier_name
            RETURNING dc.courier_id
        )
        INSERT INTO dds.dm_couriers (courier_id, courier_name, active_from, active_to)
        SELECT s.courier_bk, s.courier_name, s.updated_at, timestamp '2099-12-31'
        FROM src s
        WHERE NOT EXISTS (
            SELECT 1 FROM dds.dm_couriers dc
            WHERE dc.courier_id = s.courier_bk
              AND dc.active_to = timestamp '2099-12-31'
              AND dc.courier_name = s.courier_name
        );
        """
        dwh.run(sql)

    @task
    def load_dds_deliveries():
        sql = """
        WITH src AS (
            SELECT
                (object_value->>'delivery_id') AS delivery_bk,
                (object_value->>'order_id') AS order_bk,
                (object_value->>'courier_id') AS courier_bk,
                COALESCE((object_value->>'delivery_ts')::timestamp, NOW()) AS delivery_ts,
                COALESCE((object_value->>'order_ts')::timestamp, COALESCE((object_value->>'delivery_ts')::timestamp, NOW())) AS order_ts,
                COALESCE((object_value->>'rate')::numeric, NULL) AS rate,
                COALESCE((object_value->>'tip_sum')::numeric, 0) AS tip_sum,
                COALESCE((object_value->>'sum')::numeric, 0) AS order_sum
            FROM stg.courier_deliveries_raw
        ),
        order_map AS (
            SELECT order_key, id AS order_id, timestamp_id FROM dds.dm_orders
        ),
        courier_map AS (
            SELECT courier_id AS courier_bk, id AS courier_sk, active_from, active_to
            FROM dds.dm_couriers
        ),
        payload AS (
            SELECT
                s.delivery_bk,
                om.order_id,
                cm.courier_sk,
                s.delivery_ts,
                s.order_sum,
                s.tip_sum,
                s.rate
            FROM src s
            JOIN order_map om ON om.order_key = s.order_bk
            JOIN courier_map cm ON cm.courier_bk = s.courier_bk
               AND s.delivery_ts >= cm.active_from AND s.delivery_ts < cm.active_to
        )
        INSERT INTO dds.dm_deliveries (delivery_id, order_id, courier_id, delivery_ts, order_sum, tip_sum, rate)
        SELECT delivery_bk, order_id, courier_sk, delivery_ts, order_sum, tip_sum, rate
        FROM payload
        ON CONFLICT (delivery_id) DO UPDATE
            SET order_id = EXCLUDED.order_id,
                courier_id = EXCLUDED.courier_id,
                delivery_ts = EXCLUDED.delivery_ts,
                order_sum = EXCLUDED.order_sum,
                tip_sum = EXCLUDED.tip_sum,
                rate = EXCLUDED.rate;
        """
        dwh.run(sql)

    @task
    def load_cdm_ledger():
        sql = """
        WITH agg AS (
            SELECT
                dc.courier_id,
                dc.courier_name,
                dt.year AS settlement_year,
                dt.month AS settlement_month,
                COUNT(*) AS orders_count,
                SUM(d.order_sum) AS orders_total_sum,
                AVG(d.rate) AS rate_avg,
                SUM(
                    GREATEST(
                        CASE
                            WHEN COALESCE(d.rate, 0) < 4 THEN d.order_sum * 0.05
                            WHEN d.rate < 4.5 THEN d.order_sum * 0.07
                            WHEN d.rate < 4.9 THEN d.order_sum * 0.08
                            ELSE d.order_sum * 0.10
                        END,
                        CASE
                            WHEN COALESCE(d.rate, 0) < 4 THEN 100
                            WHEN d.rate < 4.5 THEN 150
                            WHEN d.rate < 4.9 THEN 175
                            ELSE 200
                        END
                    )
                ) AS courier_order_sum,
                SUM(d.tip_sum) AS courier_tips_sum
            FROM dds.dm_deliveries d
            JOIN dds.dm_orders o ON o.id = d.order_id
            JOIN dds.dm_timestamps dt ON dt.id = o.timestamp_id
            JOIN dds.dm_couriers dc ON dc.id = d.courier_id AND dt.ts >= dc.active_from AND dt.ts < dc.active_to
            GROUP BY dc.courier_id, dc.courier_name, dt.year, dt.month
        )
        INSERT INTO cdm.dm_courier_ledger (
            courier_id, courier_name, settlement_year, settlement_month,
            orders_count, orders_total_sum, rate_avg,
            order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum, updated_at
        )
        SELECT
            courier_id,
            courier_name,
            settlement_year,
            settlement_month,
            orders_count,
            orders_total_sum,
            rate_avg,
            orders_total_sum * 0.25 AS order_processing_fee,
            courier_order_sum,
            courier_tips_sum,
            courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum,
            NOW()
        FROM agg
        ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
            SET orders_count = EXCLUDED.orders_count,
                orders_total_sum = EXCLUDED.orders_total_sum,
                rate_avg = EXCLUDED.rate_avg,
                order_processing_fee = EXCLUDED.order_processing_fee,
                courier_order_sum = EXCLUDED.courier_order_sum,
                courier_tips_sum = EXCLUDED.courier_tips_sum,
                courier_reward_sum = EXCLUDED.courier_reward_sum,
                updated_at = EXCLUDED.updated_at;
        """
        dwh.run(sql)

    r = extract_restaurants()
    c = extract_couriers()
    d = extract_deliveries()
    dds_c = load_dds_couriers()
    dds_d = load_dds_deliveries()
    cdm = load_cdm_ledger()

    [r, c, d] >> dds_c >> dds_d >> cdm


dag_instance = courier_etl()
