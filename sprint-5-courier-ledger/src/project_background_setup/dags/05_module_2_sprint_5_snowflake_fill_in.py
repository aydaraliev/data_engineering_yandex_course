import json
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any, Tuple

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import DictCursor

END_OF_TIME = datetime(2099, 12, 31)


class DdsEtlSettingsRepository:
    def get_setting(self, conn, workflow_key: str) -> Optional[Dict[str, Any]]:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT workflow_settings
                FROM dds.srv_wf_settings
                WHERE workflow_key = %(key)s;
                """,
                {"key": workflow_key},
            )
            row = cur.fetchone()
            if not row:
                return None
            ws = row["workflow_settings"]
            if isinstance(ws, str):
                try:
                    return json.loads(ws)
                except Exception:
                    return None
            if isinstance(ws, dict):
                return ws
            return None

    def save_setting(self, conn, workflow_key: str, workflow_settings: Dict[str, Any]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%(key)s, %(settings)s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "key": workflow_key,
                    "settings": json.dumps(workflow_settings),
                },
            )


def ensure_dm_users_table(conn) -> None:
    # dm_users is a regular dimension; uniqueness on the business id is fine
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dds.dm_users (
                id         SERIAL PRIMARY KEY,
                user_id    VARCHAR NOT NULL,
                user_name  VARCHAR NOT NULL,
                user_login VARCHAR NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_dm_users_user_id
                ON dds.dm_users (user_id);
            """
        )


def ensure_dm_restaurants_table(conn) -> None:
    # dm_restaurants is SCD2 - a unique index on restaurant_id must NOT exist
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
                id              SERIAL PRIMARY KEY,
                restaurant_id   VARCHAR   NOT NULL,
                restaurant_name VARCHAR   NOT NULL,
                active_from     TIMESTAMP NOT NULL,
                active_to       TIMESTAMP NOT NULL
            );
            """
        )
        # drop any previously created unique index
        cur.execute("DROP INDEX IF EXISTS dds.ux_dm_restaurants_restaurant_id;")


def ensure_dm_products_table(conn) -> None:
    # dm_products is SCD2 - a unique index on product_id must NOT exist
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dds.dm_products (
                id            SERIAL PRIMARY KEY,
                product_id    VARCHAR                  NOT NULL,
                product_name  VARCHAR                  NOT NULL,
                product_price NUMERIC(14, 2) DEFAULT 0 NOT NULL
                    CONSTRAINT dm_products_product_price_check
                        CHECK (product_price >= (0)::NUMERIC),
                restaurant_id INTEGER                  NOT NULL
                    REFERENCES dds.dm_restaurants,
                active_from   TIMESTAMP                NOT NULL,
                active_to     TIMESTAMP                NOT NULL
            );
            """
        )
        # drop any previously created unique index
        cur.execute("DROP INDEX IF EXISTS dds.ux_dm_products_product_id;")


def ensure_dm_timestamps_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
                id    SERIAL PRIMARY KEY,
                ts    TIMESTAMP NOT NULL,
                year  SMALLINT  NOT NULL
                    CONSTRAINT dm_timestamps_year_check
                        CHECK ((year >= 2022) AND (year < 2500)),
                month SMALLINT  NOT NULL
                    CONSTRAINT dm_timestamps_month_check
                        CHECK ((month >= 1) AND (month <= 12)),
                day   SMALLINT  NOT NULL
                    CONSTRAINT dm_timestamps_day_check
                        CHECK ((day >= 1) AND (day <= 31)),
                time  TIME      NOT NULL,
                date  DATE      NOT NULL
            );
            """
        )
        # convenient for timestamp upserts
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_dm_timestamps_ts
                ON dds.dm_timestamps (ts);
            """
        )


def ensure_dm_orders_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dds.dm_orders (
                id            SERIAL PRIMARY KEY,
                user_id       INTEGER NOT NULL REFERENCES dds.dm_users,
                restaurant_id INTEGER NOT NULL REFERENCES dds.dm_restaurants,
                timestamp_id  INTEGER NOT NULL REFERENCES dds.dm_timestamps,
                order_key     VARCHAR NOT NULL UNIQUE,
                order_status  VARCHAR NOT NULL
            );
            """
        )


def ensure_fct_product_sales_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
                id            SERIAL PRIMARY KEY,
                product_id    INTEGER                   NOT NULL REFERENCES dds.dm_products(id),
                order_id      INTEGER                   NOT NULL REFERENCES dds.dm_orders(id),
                count         INTEGER         DEFAULT 0 NOT NULL
                    CONSTRAINT fct_product_sales_count_check
                        CHECK (count >= 0),
                price         NUMERIC(19, 5)  DEFAULT 0 NOT NULL
                    CONSTRAINT fct_product_sales_price_check
                        CHECK (price >= (0)::NUMERIC),
                total_sum     NUMERIC(19, 5)  DEFAULT 0 NOT NULL
                    CONSTRAINT fct_product_sales_total_sum_check
                        CHECK (total_sum >= (0)::NUMERIC),
                bonus_payment NUMERIC(19, 5) DEFAULT 0 NOT NULL
                    CONSTRAINT fct_product_sales_bonus_payment_check
                        CHECK (bonus_payment >= (0)::NUMERIC),
                bonus_grant   NUMERIC(19, 5) DEFAULT 0 NOT NULL
                    CONSTRAINT fct_product_sales_bonus_grant_check
                        CHECK (bonus_grant >= (0)::NUMERIC)
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE dds.fct_product_sales
                ALTER COLUMN price         TYPE NUMERIC(19, 5),
                ALTER COLUMN total_sum     TYPE NUMERIC(19, 5),
                ALTER COLUMN bonus_payment TYPE NUMERIC(19, 5),
                ALTER COLUMN bonus_grant   TYPE NUMERIC(19, 5);
            """
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'dds'
                      AND table_name = 'fct_product_sales'
                      AND constraint_name = 'fct_product_sales_product_id_fkey'
                ) THEN
                    ALTER TABLE dds.fct_product_sales
                        ADD CONSTRAINT fct_product_sales_product_id_fkey
                            FOREIGN KEY (product_id) REFERENCES dds.dm_products(id);
                END IF;

                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'dds'
                      AND table_name = 'fct_product_sales'
                      AND constraint_name = 'fct_product_sales_order_id_fkey'
                ) THEN
                    ALTER TABLE dds.fct_product_sales
                        ADD CONSTRAINT fct_product_sales_order_id_fkey
                            FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
                END IF;
            END$$;
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_product_sales_order_product
                ON dds.fct_product_sales(order_id, product_id);
            """
        )


def ensure_stg_update_ts_indexes(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ordersystem_orders_update_ts "
            "ON stg.ordersystem_orders(update_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ordersystem_restaurants_update_ts "
            "ON stg.ordersystem_restaurants(update_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ordersystem_users_update_ts "
            "ON stg.ordersystem_users(update_ts);"
        )


def _parse_id(raw_id: Any) -> str:
    if isinstance(raw_id, dict):
        return raw_id.get("$oid") or raw_id.get("oid") or raw_id.get("$id") or ""
    return str(raw_id or "")


def extract_users_from_stg_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    for r in rows:
        try:
            payload = r["object_value"]
            doc = json.loads(payload) if isinstance(payload, str) else payload
            user_id = _parse_id(doc.get("_id") or doc.get("id"))
            user_name = doc.get("name")
            user_login = doc.get("login")
            if not user_id or user_name is None or user_login is None:
                continue
            extracted.append(
                {
                    "user_id": user_id,
                    "user_name": user_name,
                    "user_login": user_login,
                    "update_ts": r["update_ts"],
                }
            )
        except Exception:
            continue
    return extracted


def extract_restaurants_from_stg_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    for r in rows:
        try:
            payload = r["object_value"]
            doc = json.loads(payload) if isinstance(payload, str) else payload

            restaurant_id = _parse_id(doc.get("_id") or doc.get("id"))
            restaurant_name = doc.get("name")

            update_ts = r["update_ts"]
            if isinstance(update_ts, datetime):
                update_ts = update_ts.replace(microsecond=0)

            if not restaurant_id or not restaurant_name or not update_ts:
                continue

            extracted.append(
                {
                    "restaurant_id": restaurant_id,
                    "restaurant_name": restaurant_name,
                    "active_from": update_ts,
                    "active_to": END_OF_TIME,
                    "update_ts": update_ts,
                }
            )
        except Exception:
            continue
    return extracted


def extract_products_from_restaurant_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    for r in rows:
        try:
            payload = r["object_value"]
            doc = json.loads(payload) if isinstance(payload, str) else payload

            restaurant_id = _parse_id(doc.get("_id") or doc.get("id"))
            update_ts = r["update_ts"]
            if isinstance(update_ts, datetime):
                update_ts = update_ts.replace(microsecond=0)

            if not restaurant_id or not update_ts:
                continue

            menu = doc.get("menu") or []
            if not isinstance(menu, list):
                continue

            for item in menu:
                if not isinstance(item, dict):
                    continue

                product_id = _parse_id(item.get("_id") or item.get("id"))
                product_name = item.get("name")
                product_price = item.get("price")

                if not product_id or product_name is None or product_price is None:
                    continue

                extracted.append(
                    {
                        "product_id": product_id,
                        "product_name": product_name,
                        "product_price": product_price,
                        "active_from": update_ts,
                        "active_to": END_OF_TIME,
                        "restaurant_src_id": restaurant_id,
                        "update_ts": update_ts,
                    }
                )
        except Exception:
            continue
    return extracted


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)):
        try:
            ts_val = float(value)
            # Mongo often stores milliseconds since epoch — normalize to seconds for datetime.fromtimestamp.
            if ts_val > 1e12:
                ts_val /= 1000.0
            return datetime.fromtimestamp(ts_val)
        except Exception:
            return None
    if isinstance(value, dict):
        if "$numberLong" in value:
            return _parse_dt(value.get("$numberLong"))
        inner = value.get("$date") or value.get("date") or value.get("$datetime")
        return _parse_dt(inner)
    if isinstance(value, str):
        if value.isdigit():
            return _parse_dt(int(value))
        val = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            dt = datetime.fromisoformat(val)
            return dt.replace(tzinfo=None)
        except Exception:
            return None
    return None


def extract_final_status(doc: Dict[str, Any]) -> Tuple[Optional[str], Optional[datetime]]:
    status_field = doc.get("final_status")
    status_name = None
    ts = None

    if isinstance(status_field, dict):
        status_name = (
            status_field.get("status")
            or status_field.get("state")
            or status_field.get("final_status")
            or status_field.get("name")
        )
        ts = _parse_dt(
            status_field.get("date")
            or status_field.get("datetime")
            or status_field.get("ts")
        )
    else:
        status_name = status_field

    if not status_name:
        status_name = doc.get("status") or doc.get("state")
    if ts is None:
        ts = _parse_dt(doc.get("final_status_ts") or doc.get("date"))
    return status_name, ts


def extract_orders_from_stg_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    for r in rows:
        try:
            payload = r["object_value"]
            doc = json.loads(payload) if isinstance(payload, str) else payload

            order_key = _parse_id(doc.get("_id") or doc.get("id") or doc.get("order_id"))
            order_status, ts = extract_final_status(doc)
            if ts:
                ts = ts.replace(microsecond=0)

            user_obj = doc.get("user")
            user_src_id = (
                _parse_id(user_obj.get("_id") or user_obj.get("id") or user_obj.get("user_id"))
                if isinstance(user_obj, dict)
                else _parse_id(user_obj or doc.get("user_id"))
            )

            restaurant_obj = doc.get("restaurant")
            restaurant_src_id = (
                _parse_id(restaurant_obj.get("_id") or restaurant_obj.get("id") or restaurant_obj.get("restaurant_id"))
                if isinstance(restaurant_obj, dict)
                else _parse_id(restaurant_obj or doc.get("restaurant_id"))
            )

            update_ts = r["update_ts"]
            if isinstance(update_ts, datetime):
                update_ts = update_ts.replace(microsecond=0)

            if not order_key or not ts or not user_src_id or not restaurant_src_id:
                continue

            extracted.append(
                {
                    "order_key": order_key,
                    "order_status": order_status,
                    "user_src_id": user_src_id,
                    "restaurant_src_id": restaurant_src_id,
                    "ts": ts,
                    "update_ts": update_ts,
                }
            )
        except Exception:
            continue
    return extracted


def extract_timestamps_from_orders(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    for r in rows:
        try:
            payload = r["object_value"]
            doc = json.loads(payload) if isinstance(payload, str) else payload
            _, ts = extract_final_status(doc)
            if not ts:
                continue
            ts = ts.replace(microsecond=0)

            extracted.append(
                {
                    "ts": ts,
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "date": ts.date(),
                    "time": ts.time(),
                    "update_ts": r["update_ts"],
                }
            )
        except Exception:
            continue
    return extracted


def upsert_dm_users(conn, users: List[Dict[str, Any]]) -> None:
    if not users:
        return
    with conn.cursor() as cur:
        for u in users:
            cur.execute(
                """
                INSERT INTO dds.dm_users (user_id, user_name, user_login)
                VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                ON CONFLICT (user_id) DO UPDATE
                SET user_name = EXCLUDED.user_name,
                    user_login = EXCLUDED.user_login;
                """,
                u,
            )


def scd2_upsert_dm_restaurants(conn, restaurants: List[Dict[str, Any]]) -> None:
    if not restaurants:
        return

    with conn.cursor() as cur:
        for r in restaurants:
            rid = r["restaurant_id"]
            rname = r["restaurant_name"]
            af = r["active_from"]

            cur.execute(
                """
                SELECT id, restaurant_name, active_from, active_to
                FROM dds.dm_restaurants
                WHERE restaurant_id = %(rid)s
                  AND active_to = %(eot)s
                ORDER BY active_from DESC
                LIMIT 1;
                """,
                {"rid": rid, "eot": END_OF_TIME},
            )
            active = cur.fetchone()

            if not active:
                cur.execute(
                    """
                    INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(rid)s, %(rname)s, %(af)s, %(eot)s);
                    """,
                    {"rid": rid, "rname": rname, "af": af, "eot": END_OF_TIME},
                )
                continue

            active_id, active_name, active_from, active_to = active

            if active_name != rname:
                cur.execute(
                    """
                    UPDATE dds.dm_restaurants
                    SET active_to = %(af)s
                    WHERE id = %(id)s;
                    """,
                    {"af": af, "id": active_id},
                )
                cur.execute(
                    """
                    INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(rid)s, %(rname)s, %(af)s, %(eot)s);
                    """,
                    {"rid": rid, "rname": rname, "af": af, "eot": END_OF_TIME},
                )
            # if the name is unchanged, do not write a new version


def scd2_upsert_dm_products(conn, products: List[Dict[str, Any]]) -> None:
    if not products:
        return

    with conn.cursor() as cur:
        # Pull ALL restaurant versions for time-based mapping
        rest_ids = list({p["restaurant_src_id"] for p in products})
        cur.execute(
            """
            SELECT restaurant_id, id, active_from, active_to
            FROM dds.dm_restaurants
            WHERE restaurant_id = ANY(%(ids)s);
            """,
            {"ids": rest_ids},
        )
        rest_versions: Dict[str, List[Dict[str, Any]]] = {}
        for rid, pk, af, at in cur.fetchall():
            rest_versions.setdefault(rid, []).append(
                {"id": pk, "active_from": af, "active_to": at}
            )

        def pick_rest_pk(rest_src_id: str, ts_point: datetime) -> Optional[int]:
            for v in rest_versions.get(rest_src_id, []):
                if v["active_from"] <= ts_point < v["active_to"]:
                    return v["id"]
            # fall back to the latest known version if timestamp is out of range
            versions = rest_versions.get(rest_src_id, [])
            if versions:
                return sorted(versions, key=lambda x: x["active_from"], reverse=True)[0]["id"]
            return None

        for p in products:
            pid = p["product_id"]
            pname = p["product_name"]
            pprice = p["product_price"]
            af = p["active_from"]
            rest_src_id = p["restaurant_src_id"]

            rest_pk = pick_rest_pk(rest_src_id, af)
            if not rest_pk:
                continue

            cur.execute(
                """
                SELECT id, product_name, product_price, restaurant_id, active_from, active_to
                FROM dds.dm_products
                WHERE product_id = %(pid)s
                  AND active_to = %(eot)s
                ORDER BY active_from DESC
                LIMIT 1;
                """,
                {"pid": pid, "eot": END_OF_TIME},
            )
            active = cur.fetchone()

            if not active:
                cur.execute(
                    """
                    INSERT INTO dds.dm_products (
                        product_id, product_name, product_price,
                        restaurant_id, active_from, active_to
                    )
                    VALUES (%(pid)s, %(pname)s, %(pprice)s,
                            %(rest_pk)s, %(af)s, %(eot)s);
                    """,
                    {
                        "pid": pid, "pname": pname, "pprice": pprice,
                        "rest_pk": rest_pk, "af": af, "eot": END_OF_TIME
                    },
                )
                continue

            active_id, active_name, active_price, active_rest_pk, active_from, active_to = active

            if (
                active_name != pname
                or float(active_price) != float(pprice)
                or int(active_rest_pk) != int(rest_pk)
            ):
                cur.execute(
                    """
                    UPDATE dds.dm_products
                    SET active_to = %(af)s
                    WHERE id = %(id)s;
                    """,
                    {"af": af, "id": active_id},
                )
                cur.execute(
                    """
                    INSERT INTO dds.dm_products (
                        product_id, product_name, product_price,
                        restaurant_id, active_from, active_to
                    )
                    VALUES (%(pid)s, %(pname)s, %(pprice)s,
                            %(rest_pk)s, %(af)s, %(eot)s);
                    """,
                    {
                        "pid": pid, "pname": pname, "pprice": pprice,
                        "rest_pk": rest_pk, "af": af, "eot": END_OF_TIME
                    },
                )


def upsert_dm_timestamps(conn, timestamps: List[Dict[str, Any]]) -> None:
    if not timestamps:
        return
    with conn.cursor() as cur:
        for t in timestamps:
            ts = t["ts"].replace(tzinfo=None, microsecond=0)
            cur.execute(
                """
                INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                ON CONFLICT (ts) DO UPDATE
                SET year  = EXCLUDED.year,
                    month = EXCLUDED.month,
                    day   = EXCLUDED.day,
                    date  = EXCLUDED.date,
                    time  = EXCLUDED.time;
                """,
                {
                    "ts": ts,
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "date": ts.date(),
                    "time": ts.time(),
                },
            )


def upsert_dm_orders(conn, orders: List[Dict[str, Any]]) -> None:
    if not orders:
        return

    with conn.cursor() as cur:
        user_ids = list({o["user_src_id"] for o in orders})
        rest_ids = list({o["restaurant_src_id"] for o in orders})
        ts_vals = list({o["ts"].replace(tzinfo=None, microsecond=0) for o in orders})

        cur.execute(
            "SELECT user_id, id FROM dds.dm_users WHERE user_id = ANY(%(ids)s);",
            {"ids": user_ids},
        )
        user_map = {uid: pk for uid, pk in cur.fetchall()}

        cur.execute(
            """
            SELECT restaurant_id, id, active_from, active_to
            FROM dds.dm_restaurants
            WHERE restaurant_id = ANY(%(ids)s);
            """,
            {"ids": rest_ids},
        )
        rest_versions: Dict[str, List[Dict[str, Any]]] = {}
        for rid, pk, af, at in cur.fetchall():
            rest_versions.setdefault(rid, []).append(
                {"id": pk, "active_from": af, "active_to": at}
            )

        cur.execute(
            "SELECT ts, id FROM dds.dm_timestamps WHERE ts = ANY(%(ts)s);",
            {"ts": ts_vals},
        )
        ts_map = {ts.replace(tzinfo=None): pk for ts, pk in cur.fetchall()}

        def pick_rest_pk(rest_src_id: str, ts_point: datetime) -> Optional[int]:
            for v in rest_versions.get(rest_src_id, []):
                if v["active_from"] <= ts_point < v["active_to"]:
                    return v["id"]
            versions = rest_versions.get(rest_src_id, [])
            if versions:
                return sorted(versions, key=lambda x: x["active_from"], reverse=True)[0]["id"]
            return None

        for o in orders:
            ts = o["ts"].replace(tzinfo=None, microsecond=0)
            user_pk = user_map.get(o["user_src_id"])
            rest_pk = pick_rest_pk(o["restaurant_src_id"], ts)
            ts_pk = ts_map.get(ts)

            if not user_pk or not rest_pk or not ts_pk:
                continue

            status_val = o.get("order_status") or "UNKNOWN"
            if not isinstance(status_val, str):
                status_val = str(status_val)

            cur.execute(
                """
                INSERT INTO dds.dm_orders (
                    user_id, restaurant_id, timestamp_id,
                    order_key, order_status
                )
                VALUES (
                    %(user_id)s, %(restaurant_id)s, %(timestamp_id)s,
                    %(order_key)s, %(order_status)s
                )
                ON CONFLICT (order_key) DO UPDATE
                SET user_id       = EXCLUDED.user_id,
                    restaurant_id = EXCLUDED.restaurant_id,
                    timestamp_id  = EXCLUDED.timestamp_id,
                    order_status  = EXCLUDED.order_status;
                """,
                {
                    "user_id": user_pk,
                    "restaurant_id": rest_pk,
                    "timestamp_id": ts_pk,
                    "order_key": o["order_key"],
                    "order_status": status_val,
                },
            )


def _reset_checkpoint_if_table_empty(
    conn,
    target_table: str,
    settings_repo: "DdsEtlSettingsRepository",
    workflow_key: str,
    workflow_state: Dict[str, Any],
    reset_value: str = "1970-01-01T00:00:00",
) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {target_table};")
        cnt = cur.fetchone()[0]
    if cnt == 0 and workflow_state.get("last_loaded_ts") != reset_value:
        workflow_state["last_loaded_ts"] = reset_value
        settings_repo.save_setting(conn, workflow_key, workflow_state)
        conn.commit()
    return workflow_state


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=[
        "sprint5",
        "dds",
        "dm_users",
        "dm_restaurants",
        "dm_products",
        "dm_orders",
        "dm_timestamps",
        "fct_product_sales",
    ],
    is_paused_upon_creation=False,
)
def load_stg_to_dds():
    @task()
    def run():
        log = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        conn = hook.get_conn()
        conn.autocommit = False

        settings_repo = DdsEtlSettingsRepository()
        users_key = "dds_dm_users_load"
        rest_key = "dds_dm_restaurants_load"
        prod_key = "dds_dm_products_load"
        orders_key = "dds_dm_orders_load"
        ts_key = "dds_dm_timestamps_load"
        batch_size = 10000

        try:
            ensure_stg_update_ts_indexes(conn)

            # USERS
            ensure_dm_users_table(conn)
            wf = settings_repo.get_setting(conn, users_key) or {
                "last_loaded_ts": "1970-01-01T00:00:00"
            }
            wf = _reset_checkpoint_if_table_empty(
                conn, "dds.dm_users", settings_repo, users_key, wf
            )
            last_ts = datetime.fromisoformat(wf["last_loaded_ts"])

            while True:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(
                        """
                        SELECT object_value::TEXT AS object_value, update_ts
                        FROM stg.ordersystem_users
                        WHERE update_ts > %(ts)s
                        ORDER BY update_ts
                        LIMIT %(lim)s;
                        """,
                        {"ts": last_ts, "lim": batch_size},
                    )
                    rows = cur.fetchall()
                if not rows:
                    break

                extracted = extract_users_from_stg_rows(rows)
                if extracted:
                    upsert_dm_users(conn, extracted)

                last_ts = rows[-1]["update_ts"]
                wf["last_loaded_ts"] = last_ts.isoformat()
                settings_repo.save_setting(conn, users_key, wf)
                conn.commit()
                log.info(f"Loaded users up to {last_ts}")

            # RESTAURANTS (SCD2)
            ensure_dm_restaurants_table(conn)
            wf = settings_repo.get_setting(conn, rest_key) or {
                "last_loaded_ts": "1970-01-01T00:00:00"
            }
            wf = _reset_checkpoint_if_table_empty(
                conn, "dds.dm_restaurants", settings_repo, rest_key, wf
            )
            last_ts = datetime.fromisoformat(wf["last_loaded_ts"])

            while True:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(
                        """
                        SELECT object_value::TEXT AS object_value, update_ts
                        FROM stg.ordersystem_restaurants
                        WHERE update_ts > %(ts)s
                        ORDER BY update_ts
                        LIMIT %(lim)s;
                        """,
                        {"ts": last_ts, "lim": batch_size},
                    )
                    rows = cur.fetchall()
                if not rows:
                    break

                extracted = extract_restaurants_from_stg_rows(rows)
                if extracted:
                    scd2_upsert_dm_restaurants(conn, extracted)

                last_ts = rows[-1]["update_ts"]
                wf["last_loaded_ts"] = last_ts.isoformat()
                settings_repo.save_setting(conn, rest_key, wf)
                conn.commit()
                log.info(f"Loaded restaurants up to {last_ts}")

            # PRODUCTS (SCD2)
            ensure_dm_products_table(conn)
            wf = settings_repo.get_setting(conn, prod_key) or {
                "last_loaded_ts": "1970-01-01T00:00:00"
            }
            wf = _reset_checkpoint_if_table_empty(
                conn, "dds.dm_products", settings_repo, prod_key, wf
            )
            last_ts = datetime.fromisoformat(wf["last_loaded_ts"])

            while True:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(
                        """
                        SELECT object_value::TEXT AS object_value, update_ts
                        FROM stg.ordersystem_restaurants
                        WHERE update_ts > %(ts)s
                        ORDER BY update_ts
                        LIMIT %(lim)s;
                        """,
                        {"ts": last_ts, "lim": batch_size},
                    )
                    rows = cur.fetchall()
                if not rows:
                    break

                extracted = extract_products_from_restaurant_rows(rows)
                if extracted:
                    scd2_upsert_dm_products(conn, extracted)

                last_ts = rows[-1]["update_ts"]
                wf["last_loaded_ts"] = last_ts.isoformat()
                settings_repo.save_setting(conn, prod_key, wf)
                conn.commit()
                log.info(f"Loaded products up to {last_ts}")

            # TIMESTAMPS
            ensure_dm_timestamps_table(conn)
            wf = settings_repo.get_setting(conn, ts_key) or {
                "last_loaded_ts": "1970-01-01T00:00:00"
            }
            wf = _reset_checkpoint_if_table_empty(
                conn, "dds.dm_timestamps", settings_repo, ts_key, wf
            )
            last_ts = datetime.fromisoformat(wf["last_loaded_ts"])

            while True:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(
                        """
                        SELECT object_value::TEXT AS object_value, update_ts
                        FROM stg.ordersystem_orders
                        WHERE update_ts > %(ts)s
                        ORDER BY update_ts
                        LIMIT %(lim)s;
                        """,
                        {"ts": last_ts, "lim": batch_size},
                    )
                    rows = cur.fetchall()
                if not rows:
                    break

                extracted = extract_timestamps_from_orders(rows)
                if extracted:
                    upsert_dm_timestamps(conn, extracted)

                last_ts = rows[-1]["update_ts"]
                wf["last_loaded_ts"] = last_ts.isoformat()
                settings_repo.save_setting(conn, ts_key, wf)
                conn.commit()
                log.info(f"Loaded timestamps up to {last_ts}")

            # ORDERS
            ensure_dm_orders_table(conn)
            wf = settings_repo.get_setting(conn, orders_key) or {
                "last_loaded_ts": "1970-01-01T00:00:00"
            }
            wf = _reset_checkpoint_if_table_empty(
                conn, "dds.dm_orders", settings_repo, orders_key, wf
            )
            last_ts = datetime.fromisoformat(wf["last_loaded_ts"])

            while True:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(
                        """
                        SELECT object_value::TEXT AS object_value, update_ts
                        FROM stg.ordersystem_orders
                        WHERE update_ts > %(ts)s
                        ORDER BY update_ts
                        LIMIT %(lim)s;
                        """,
                        {"ts": last_ts, "lim": batch_size},
                    )
                    rows = cur.fetchall()
                if not rows:
                    break

                extracted = extract_orders_from_stg_rows(rows)
                if extracted:
                    upsert_dm_orders(conn, extracted)

                last_ts = rows[-1]["update_ts"]
                wf["last_loaded_ts"] = last_ts.isoformat()
                settings_repo.save_setting(conn, orders_key, wf)
                conn.commit()
                log.info(f"Loaded orders up to {last_ts}")

            # FACT: fct_product_sales
            ensure_fct_product_sales_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.fct_product_sales (
                        product_id, order_id, count,
                        price, total_sum, bonus_payment, bonus_grant
                    )
                    WITH src AS (
                        SELECT ev.event_value::jsonb AS evj, ev.event_ts
                        FROM stg.bonussystem_events ev
                        WHERE ev.event_type = 'bonus_transaction'
                    ),
                    q AS (
                        SELECT
                            COALESCE(evj->'order_id'->>'$oid', evj->>'order_id')        AS order_id,
                            COALESCE(pay->'product_id'->>'$oid', pay->>'product_id')    AS product_id,
                            -- Parse order_date from ISO strings or Mongo-style epoch millis
                            CASE
                                WHEN jsonb_typeof(evj->'order_date') = 'object' THEN
                                    CASE
                                        WHEN evj->'order_date' ? '$date' THEN
                                            to_timestamp(
                                                COALESCE(
                                                    evj->'order_date'->'$date'->>'$numberLong',
                                                    evj->'order_date'->>'$date'
                                                )::numeric / 1000
                                            )
                                        WHEN evj->'order_date' ? '$numberLong' THEN
                                            to_timestamp((evj->'order_date'->>'$numberLong')::numeric / 1000)
                                        ELSE NULL
                                    END
                                WHEN jsonb_typeof(evj->'order_date') = 'string' AND (evj->>'order_date') ~ '^[0-9]+$' THEN
                                    to_timestamp((evj->>'order_date')::numeric / 1000)
                                WHEN (evj->>'order_date') ~ '^[0-9]+$' THEN
                                    to_timestamp((evj->>'order_date')::numeric / 1000)
                                WHEN jsonb_typeof(evj->'order_date') = 'string' THEN
                                    (evj->>'order_date')::timestamptz
                                ELSE
                                    NULL
                            END                                           AS order_date_raw,
                            -- Parse update_ts (fallback for product versioning)
                            CASE
                                WHEN jsonb_typeof(evj->'update_ts') = 'object' THEN
                                    CASE
                                        WHEN evj->'update_ts' ? '$date' THEN
                                            to_timestamp(
                                                COALESCE(
                                                    evj->'update_ts'->'$date'->>'$numberLong',
                                                    evj->'update_ts'->>'$date'
                                                )::numeric / 1000
                                            )
                                        WHEN evj->'update_ts' ? '$numberLong' THEN
                                            to_timestamp((evj->'update_ts'->>'$numberLong')::numeric / 1000)
                                        ELSE NULL
                                    END
                                WHEN jsonb_typeof(evj->'update_ts') = 'string' AND (evj->>'update_ts') ~ '^[0-9]+$' THEN
                                    to_timestamp((evj->>'update_ts')::numeric / 1000)
                                WHEN (evj->>'update_ts') ~ '^[0-9]+$' THEN
                                    to_timestamp((evj->>'update_ts')::numeric / 1000)
                                WHEN jsonb_typeof(evj->'update_ts') = 'string' THEN
                                    (evj->>'update_ts')::timestamptz
                                ELSE
                                    NULL
                            END                                           AS update_ts_raw,
                            event_ts::timestamptz                         AS event_ts,
                            (pay->>'quantity')::int                       AS quantity,
                            (pay->>'price')::numeric(19,5)                AS price,
                            (pay->>'product_cost')::numeric(19,5)         AS product_cost,
                            (pay->>'bonus_payment')::numeric(19,5)        AS bonus_payment,
                            (pay->>'bonus_grant')::numeric(19,5)          AS bonus_grant
                        FROM src
                        CROSS JOIN LATERAL jsonb_array_elements(evj->'product_payments') AS pay
                    ),
                    q_norm AS (
                        SELECT
                            order_id,
                            product_id,
                            COALESCE(order_date_raw, update_ts_raw, event_ts) AS order_dt,
                            quantity,
                            price,
                            product_cost,
                            bonus_payment,
                            bonus_grant
                        FROM q
                    )
                    SELECT
                        dp_pick.product_pk         AS product_id,
                        o.id                       AS order_id,
                        COALESCE(qn.quantity, 0)   AS count,
                        COALESCE(qn.price, 0)      AS price,
                        COALESCE(qn.price, 0) * COALESCE(qn.quantity, 0) AS total_sum,
                        COALESCE(qn.bonus_payment, 0) AS bonus_payment,
                        COALESCE(qn.bonus_grant, 0)   AS bonus_grant
                    FROM q_norm qn
                    JOIN dds.dm_orders o
                      ON o.order_key = qn.order_id
                    LEFT JOIN LATERAL (
                        SELECT dp.id
                        FROM dds.dm_products dp
                        WHERE dp.product_id = qn.product_id
                          AND qn.order_dt >= dp.active_from
                          AND qn.order_dt <  dp.active_to
                        ORDER BY dp.active_from DESC
                        LIMIT 1
                    ) dp_match ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT dp.id
                        FROM dds.dm_products dp
                        WHERE dp.product_id = qn.product_id
                        ORDER BY dp.active_from DESC
                        LIMIT 1
                    ) dp_any ON TRUE
                    JOIN LATERAL (
                        SELECT COALESCE(dp_match.id, dp_any.id) AS product_pk
                    ) dp_pick ON TRUE
                    WHERE qn.order_id IS NOT NULL
                      AND qn.product_id IS NOT NULL
                      AND qn.order_dt IS NOT NULL
                      AND dp_pick.product_pk IS NOT NULL
                    ON CONFLICT (order_id, product_id) DO UPDATE
                    SET
                        count         = EXCLUDED.count,
                        price         = EXCLUDED.price,
                        total_sum     = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant   = EXCLUDED.bonus_grant;
                    """
                )

            conn.commit()
            log.info("Loaded fct_product_sales.")

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    run()


dds_load_dimensions_dag = load_stg_to_dds()
