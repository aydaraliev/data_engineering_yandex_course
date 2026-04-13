import json
import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import DictCursor


class CdmEtlSettingsRepository:
    def get_setting(self, conn, workflow_key: str):
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT workflow_settings
                FROM cdm.srv_wf_settings
                WHERE workflow_key = %(key)s;
                """,
                {"key": workflow_key},
            )
            row = cur.fetchone()
            if not row:
                return None
            settings = row["workflow_settings"]
            if isinstance(settings, str):
                try:
                    return json.loads(settings)
                except Exception:
                    return None
            return settings

    def save_setting(self, conn, workflow_key: str, workflow_settings) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%(key)s, %(settings)s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "key": workflow_key,
                    "settings": json.dumps(workflow_settings),
                },
            )


def ensure_cdm_service_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS cdm;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cdm.srv_wf_settings (
                id                SERIAL PRIMARY KEY,
                workflow_key      VARCHAR NOT NULL UNIQUE,
                workflow_settings JSONB   NOT NULL DEFAULT '{}'::JSONB
            );
            """
        )


def ensure_dm_settlement_report_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS cdm;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
                id                          SERIAL PRIMARY KEY,
                restaurant_id               VARCHAR     NOT NULL,
                restaurant_name             VARCHAR     NOT NULL,
                settlement_date             DATE        NOT NULL,
                orders_count                INTEGER     NOT NULL DEFAULT 0,
                orders_total_sum            NUMERIC(19, 5) NOT NULL DEFAULT 0,
                orders_bonus_payment_sum    NUMERIC(19, 5) NOT NULL DEFAULT 0,
                orders_bonus_granted_sum    NUMERIC(19, 5) NOT NULL DEFAULT 0,
                order_processing_fee        NUMERIC(19, 5) NOT NULL DEFAULT 0,
                restaurant_reward_sum       NUMERIC(19, 5) NOT NULL DEFAULT 0
            );
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_dm_settlement_report_restaurant_period
                ON cdm.dm_settlement_report (restaurant_id, settlement_date);
            """
        )


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "cdm", "dm_settlement_report"],
    is_paused_upon_creation=False,
)
def load_dm_settlement_report():
    @task()
    def run():
        log = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        conn = hook.get_conn()
        conn.autocommit = False

        settings_repo = CdmEtlSettingsRepository()
        workflow_key = "cdm_dm_settlement_report"

        try:
            ensure_cdm_service_tables(conn)
            ensure_dm_settlement_report_table(conn)

            wf_settings = settings_repo.get_setting(conn, workflow_key) or {
                "last_loaded_ts": "1970-01-01T00:00:00"
            }
            last_loaded_ts = datetime.fromisoformat(wf_settings["last_loaded_ts"])
            # Reprocess a sliding window to keep recent days consistent and allow logic changes
            window_start = last_loaded_ts - timedelta(days=31)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        MIN(dt.ts) AS min_ts,
                        MAX(dt.ts) AS max_ts
                    FROM dds.dm_orders o
                    JOIN dds.dm_timestamps dt ON dt.id = o.timestamp_id
                    WHERE dt.ts >= %(window_start)s
                      AND UPPER(COALESCE(o.order_status, '')) = 'CLOSED';
                    """,
                    {"window_start": window_start},
                )
                min_ts, max_ts = cur.fetchone()

            if not min_ts or not max_ts:
                conn.commit()
                log.info("No new CLOSED orders found for dm_settlement_report.")
                return

            # Recompute from the earliest relevant day to refresh the full daily slice
            start_from = datetime(min_ts.year, min_ts.month, min_ts.day)

            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    """
                    WITH aggregated AS (
                        SELECT
                            dr.restaurant_id,
                            dr.restaurant_name,
                            dt.ts::date                       AS settlement_date,
                            COUNT(DISTINCT o.id)              AS orders_count,
                            COALESCE(SUM(fps.total_sum), 0)::NUMERIC(19,5)         AS orders_total_sum,
                            COALESCE(SUM(fps.bonus_payment), 0)::NUMERIC(19,5)    AS orders_bonus_payment_sum,
                            COALESCE(SUM(fps.bonus_grant), 0)::NUMERIC(19,5)      AS orders_bonus_granted_sum
                        FROM dds.dm_orders o
                        JOIN dds.dm_restaurants dr ON dr.id = o.restaurant_id
                        JOIN dds.dm_timestamps dt ON dt.id = o.timestamp_id
                        JOIN dds.fct_product_sales fps ON fps.order_id = o.id
                        WHERE dt.ts >= %(start_dt)s
                          AND UPPER(COALESCE(o.order_status, '')) = 'CLOSED'
                        GROUP BY dr.restaurant_id, dr.restaurant_name, dt.ts::date
                    )
                    SELECT
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        orders_total_sum * 0.25 AS order_processing_fee,
                        orders_total_sum - orders_bonus_payment_sum - (orders_total_sum * 0.25)
                            AS restaurant_reward_sum
                    FROM aggregated;
                    """,
                    {"start_dt": start_from},
                )
                rows = cur.fetchall()

            if not rows:
                conn.commit()
                log.info("No settlement aggregates produced for the period.")
                return

            insert_sql = """
                INSERT INTO cdm.dm_settlement_report (
                    restaurant_id,
                    restaurant_name,
                    settlement_date,
                    orders_count,
                    orders_total_sum,
                    orders_bonus_payment_sum,
                    orders_bonus_granted_sum,
                    order_processing_fee,
                    restaurant_reward_sum
                )
                VALUES (
                    %(restaurant_id)s,
                    %(restaurant_name)s,
                    %(settlement_date)s,
                    %(orders_count)s,
                    %(orders_total_sum)s,
                    %(orders_bonus_payment_sum)s,
                    %(orders_bonus_granted_sum)s,
                    %(order_processing_fee)s,
                    %(restaurant_reward_sum)s
                )
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                SET restaurant_name          = EXCLUDED.restaurant_name,
                    orders_count             = EXCLUDED.orders_count,
                    orders_total_sum         = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee     = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum    = EXCLUDED.restaurant_reward_sum;
            """

            with conn.cursor() as cur:
                # Clean overlapping slice to avoid stale aggregates when logic changes
                cur.execute(
                    """
                    DELETE FROM cdm.dm_settlement_report
                    WHERE settlement_date >= %(start_from)s
                      AND settlement_date <= %(end_date)s;
                    """,
                    {"start_from": start_from.date(), "end_date": max_ts.date()},
                )

                for row in rows:
                    cur.execute(insert_sql, row)

            wf_settings["last_loaded_ts"] = max_ts.replace(tzinfo=None).isoformat()
            settings_repo.save_setting(conn, workflow_key, wf_settings)
            conn.commit()

            log.info(
                "dm_settlement_report updated for periods starting from %s; last ts %s.",
                start_from,
                wf_settings["last_loaded_ts"],
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    run()


dm_settlement_report_dag = load_dm_settlement_report()
