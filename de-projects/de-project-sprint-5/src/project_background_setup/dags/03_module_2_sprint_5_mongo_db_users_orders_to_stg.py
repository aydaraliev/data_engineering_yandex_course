import json
import logging
from datetime import datetime
from typing import List, Optional, Dict

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bson.json_util import dumps
from psycopg2.extras import DictCursor
from pymongo import MongoClient
from urllib.parse import quote_plus as quote


# --- Helper Classes ---

class MongoConnect:
    def __init__(self, cert_path: str, user: str, pw: str, host: str, rs: str, auth_db: str, main_db: str):
        self.user = user
        self.pw = pw
        self.hosts = [host]
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=','.join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db)

    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]


class UsersReader:
    def __init__(self, mc: MongoConnect):
        self.dbs = mc.client()

    def get_users(self, load_threshold: datetime, limit=100) -> List[Dict]:
        filter_criteria = {'update_ts': {'$gt': load_threshold}}
        sort_order = [('update_ts', 1)]
        docs = list(self.dbs.get_collection('users').find(
            filter=filter_criteria,
            sort=sort_order,
            limit=limit
        ))
        return docs


class OrdersReader:
    def __init__(self, mc: MongoConnect):
        self.dbs = mc.client()

    def get_orders(self, load_threshold: datetime, limit=100) -> List[Dict]:
        filter_criteria = {'update_ts': {'$gt': load_threshold}}
        sort_order = [('update_ts', 1)]
        docs = list(self.dbs.get_collection('orders').find(
            filter=filter_criteria,
            sort=sort_order,
            limit=limit
        ))
        return docs


class PgSaver:
    def save_users(self, conn, users: List[Dict]):
        with conn.cursor() as cur:
            for user in users:
                object_id = str(user['_id'])
                object_value = dumps(user)
                update_ts = user['update_ts']

                cur.execute(
                    """
                    INSERT INTO stg.ordersystem_users(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(ts)s) ON CONFLICT (object_id) DO
                    UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": object_id,
                        "val": object_value,
                        "ts": update_ts
                    }
                )

    def save_orders(self, conn, orders: List[Dict]):
        with conn.cursor() as cur:
            for order in orders:
                object_id = str(order['_id'])
                object_value = dumps(order)
                update_ts = order['update_ts']

                cur.execute(
                    """
                    INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(ts)s) ON CONFLICT (object_id) DO
                    UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": object_id,
                        "val": object_value,
                        "ts": update_ts
                    }
                )


class StgEtlSettingsRepository:
    def get_setting(self, conn, etl_key: str) -> Optional[Dict]:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                """
                SELECT workflow_settings
                FROM stg.srv_wf_settings
                WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

            if obj:
                settings = obj['workflow_settings']
                if isinstance(settings, str):
                    return json.loads(settings)
                return settings
            return None

    def save_setting(self, conn, workflow_key: str, workflow_settings: Dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                VALUES (%(etl_key)s, %(etl_setting)s) ON CONFLICT (workflow_key) DO
                UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": json.dumps(workflow_settings)
                },
            )


class UsersLoader:
    _LOG_THRESHOLD = 10
    _SESSION_LIMIT = 10000
    _WORKFLOW_KEY = "stg_ordersystem_users_copy"

    def __init__(self, collection_loader: UsersReader, pg_dest_conn, pg_saver: PgSaver, logger):
        self.collection_loader = collection_loader
        self.pg_dest = pg_dest_conn
        self.pg_saver = pg_saver
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self):
        wf_setting = self.settings_repository.get_setting(self.pg_dest, self._WORKFLOW_KEY)
        if not wf_setting:
            wf_setting = {"last_loaded_ts": datetime.min.isoformat()}

        last_loaded_ts = datetime.fromisoformat(wf_setting["last_loaded_ts"])
        self.log.info(f"Starting users load from: {last_loaded_ts}")

        docs = self.collection_loader.get_users(last_loaded_ts, self._SESSION_LIMIT)
        self.log.info(f"Found {len(docs)} users to load.")

        if not docs:
            self.log.info("No new users to load.")
            return

        self.pg_saver.save_users(self.pg_dest, docs)
        self.log.info(f"Saved {len(docs)} users to PostgreSQL.")

        new_checkpoint = docs[-1]['update_ts']
        self.settings_repository.save_setting(
            self.pg_dest,
            self._WORKFLOW_KEY,
            {"last_loaded_ts": new_checkpoint.isoformat()}
        )
        self.log.info(f"Updated users checkpoint to: {new_checkpoint}")


class OrdersLoader:
    _LOG_THRESHOLD = 10
    _SESSION_LIMIT = 10000
    _WORKFLOW_KEY = "stg_ordersystem_orders_copy"

    def __init__(self, collection_loader: OrdersReader, pg_dest_conn, pg_saver: PgSaver, logger):
        self.collection_loader = collection_loader
        self.pg_dest = pg_dest_conn
        self.pg_saver = pg_saver
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self):
        wf_setting = self.settings_repository.get_setting(self.pg_dest, self._WORKFLOW_KEY)
        if not wf_setting:
            wf_setting = {"last_loaded_ts": datetime.min.isoformat()}

        last_loaded_ts = datetime.fromisoformat(wf_setting["last_loaded_ts"])
        self.log.info(f"Starting orders load from: {last_loaded_ts}")

        docs = self.collection_loader.get_orders(last_loaded_ts, self._SESSION_LIMIT)
        self.log.info(f"Found {len(docs)} orders to load.")

        if not docs:
            self.log.info("No new orders to load.")
            return

        self.pg_saver.save_orders(self.pg_dest, docs)
        self.log.info(f"Saved {len(docs)} orders to PostgreSQL.")

        new_checkpoint = docs[-1]['update_ts']
        self.settings_repository.save_setting(
            self.pg_dest,
            self._WORKFLOW_KEY,
            {"last_loaded_ts": new_checkpoint.isoformat()}
        )
        self.log.info(f"Updated orders checkpoint to: {new_checkpoint}")


# --- DAG Definition ---

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'ordersystem'],
    is_paused_upon_creation=False
)
def sprint5_stg_ordersystem():
    # Variables from Airflow
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_users():
        dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        conn = dwh_hook.get_conn()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        reader = UsersReader(mongo_connect)
        saver = PgSaver()
        log = logging.getLogger(__name__)

        loader = UsersLoader(reader, conn, saver, log)
        loader.run_copy()

        conn.commit()
        conn.close()

    @task()
    def load_orders():
        dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        conn = dwh_hook.get_conn()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        reader = OrdersReader(mongo_connect)
        saver = PgSaver()
        log = logging.getLogger(__name__)

        loader = OrdersLoader(reader, conn, saver, log)
        loader.run_copy()

        conn.commit()
        conn.close()

    users_task = load_users()
    orders_task = load_orders()


stg_ordersystem_dag = sprint5_stg_ordersystem()