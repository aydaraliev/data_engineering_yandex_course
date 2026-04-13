import uuid
import hashlib
from typing import List

from lib.pg import PgConnect


# Helper function for generating a UUID from a string
def generate_uuid(value: str) -> uuid.UUID:
    """Generate a deterministic UUID from the MD5 hash of the input string."""
    return uuid.UUID(hashlib.md5(value.encode()).hexdigest())


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self._ensure_processed_orders_table()

    def _ensure_processed_orders_table(self) -> None:
        """Create the table that tracks processed orders if it does not already exist."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cdm.srv_processed_orders (
                        order_id INT PRIMARY KEY,
                        processed_dttm TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """)

    def is_order_processed(self, order_id: int) -> bool:
        """Check whether the order has already been processed."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM cdm.srv_processed_orders WHERE order_id = %(order_id)s
                """, {"order_id": order_id})
                return cur.fetchone() is not None

    def mark_order_processed(self, order_id: int) -> None:
        """Mark the order as processed."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.srv_processed_orders (order_id)
                    VALUES (%(order_id)s)
                    ON CONFLICT (order_id) DO NOTHING
                """, {"order_id": order_id})

    def user_product_counters_upsert(self,
                                     user_id: uuid.UUID,
                                     product_id: uuid.UUID,
                                     product_name: str) -> None:
        """
        Upsert the product order counter for the user.
        On conflict (user_id, product_id) increment order_cnt by 1.
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                    VALUES (%(user_id)s, %(product_id)s, %(product_name)s, 1)
                    ON CONFLICT (user_id, product_id)
                    DO UPDATE SET
                        order_cnt = cdm.user_product_counters.order_cnt + 1,
                        product_name = EXCLUDED.product_name
                """, {
                    "user_id": user_id,
                    "product_id": product_id,
                    "product_name": product_name
                })

    def user_category_counters_upsert(self,
                                      user_id: uuid.UUID,
                                      category_id: uuid.UUID,
                                      category_name: str) -> None:
        """
        Upsert the category order counter for the user.
        On conflict (user_id, category_id) increment order_cnt by 1.
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                    VALUES (%(user_id)s, %(category_id)s, %(category_name)s, 1)
                    ON CONFLICT (user_id, category_id)
                    DO UPDATE SET
                        order_cnt = cdm.user_category_counters.order_cnt + 1,
                        category_name = EXCLUDED.category_name
                """, {
                    "user_id": user_id,
                    "category_id": category_id,
                    "category_name": category_name
                })
