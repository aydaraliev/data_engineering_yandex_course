import uuid
import hashlib
from typing import List

from lib.pg import PgConnect


# Вспомогательная функция для генерации UUID из строки
def generate_uuid(value: str) -> uuid.UUID:
    """Генерирует детерминированный UUID на основе MD5-хеша строки."""
    return uuid.UUID(hashlib.md5(value.encode()).hexdigest())


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self._ensure_processed_orders_table()

    def _ensure_processed_orders_table(self) -> None:
        """Создаёт таблицу для отслеживания обработанных заказов, если её нет."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cdm.srv_processed_orders (
                        order_id INT PRIMARY KEY,
                        processed_dttm TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """)

    def is_order_processed(self, order_id: int) -> bool:
        """Проверяет, был ли заказ уже обработан."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM cdm.srv_processed_orders WHERE order_id = %(order_id)s
                """, {"order_id": order_id})
                return cur.fetchone() is not None

    def mark_order_processed(self, order_id: int) -> None:
        """Помечает заказ как обработанный."""
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
        Обновляет или вставляет счётчик заказов продукта для пользователя.
        При конфликте (user_id, product_id) увеличивает order_cnt на 1.
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
        Обновляет или вставляет счётчик заказов категории для пользователя.
        При конфликте (user_id, category_id) увеличивает order_cnt на 1.
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
