import uuid
import hashlib
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


# Вспомогательная функция для генерации UUID из строки
def generate_uuid(value: str) -> uuid.UUID:
    """Генерирует детерминированный UUID на основе MD5-хеша строки."""
    return uuid.UUID(hashlib.md5(value.encode()).hexdigest())


# ==================== Модели для хабов ====================

class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str


class H_Category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str


class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str


class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str


# ==================== Модели для линков ====================

class L_Order_User(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_Order_Product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_Product_Restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_Product_Category(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str


# ==================== Модели для сателлитов ====================

class S_User_Names(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: uuid.UUID


class S_Product_Names(BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: uuid.UUID


class S_Restaurant_Names(BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: uuid.UUID


class S_Order_Cost(BaseModel):
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: uuid.UUID


class S_Order_Status(BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: uuid.UUID


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    # ==================== Методы вставки в хабы ====================

    def h_user_insert(self, user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                    VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_user_pk) DO NOTHING
                """, {
                    "h_user_pk": user.h_user_pk,
                    "user_id": user.user_id,
                    "load_dt": user.load_dt,
                    "load_src": user.load_src
                })

    def h_product_insert(self, product: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                    VALUES (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_product_pk) DO NOTHING
                """, {
                    "h_product_pk": product.h_product_pk,
                    "product_id": product.product_id,
                    "load_dt": product.load_dt,
                    "load_src": product.load_src
                })

    def h_category_insert(self, category: H_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                    VALUES (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_category_pk) DO NOTHING
                """, {
                    "h_category_pk": category.h_category_pk,
                    "category_name": category.category_name,
                    "load_dt": category.load_dt,
                    "load_src": category.load_src
                })

    def h_restaurant_insert(self, restaurant: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                    VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_restaurant_pk) DO NOTHING
                """, {
                    "h_restaurant_pk": restaurant.h_restaurant_pk,
                    "restaurant_id": restaurant.restaurant_id,
                    "load_dt": restaurant.load_dt,
                    "load_src": restaurant.load_src
                })

    def h_order_insert(self, order: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                    VALUES (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_order_pk) DO NOTHING
                """, {
                    "h_order_pk": order.h_order_pk,
                    "order_id": order.order_id,
                    "order_dt": order.order_dt,
                    "load_dt": order.load_dt,
                    "load_src": order.load_src
                })

    # ==================== Методы вставки в линки ====================

    def l_order_user_insert(self, link: L_Order_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                    VALUES (%(hk_order_user_pk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_order_user_pk) DO NOTHING
                """, {
                    "hk_order_user_pk": link.hk_order_user_pk,
                    "h_order_pk": link.h_order_pk,
                    "h_user_pk": link.h_user_pk,
                    "load_dt": link.load_dt,
                    "load_src": link.load_src
                })

    def l_order_product_insert(self, link: L_Order_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                    VALUES (%(hk_order_product_pk)s, %(h_order_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_order_product_pk) DO NOTHING
                """, {
                    "hk_order_product_pk": link.hk_order_product_pk,
                    "h_order_pk": link.h_order_pk,
                    "h_product_pk": link.h_product_pk,
                    "load_dt": link.load_dt,
                    "load_src": link.load_src
                })

    def l_product_restaurant_insert(self, link: L_Product_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                    VALUES (%(hk_product_restaurant_pk)s, %(h_product_pk)s, %(h_restaurant_pk)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_product_restaurant_pk) DO NOTHING
                """, {
                    "hk_product_restaurant_pk": link.hk_product_restaurant_pk,
                    "h_product_pk": link.h_product_pk,
                    "h_restaurant_pk": link.h_restaurant_pk,
                    "load_dt": link.load_dt,
                    "load_src": link.load_src
                })

    def l_product_category_insert(self, link: L_Product_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                    VALUES (%(hk_product_category_pk)s, %(h_product_pk)s, %(h_category_pk)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (hk_product_category_pk) DO NOTHING
                """, {
                    "hk_product_category_pk": link.hk_product_category_pk,
                    "h_product_pk": link.h_product_pk,
                    "h_category_pk": link.h_category_pk,
                    "load_dt": link.load_dt,
                    "load_src": link.load_src
                })

    # ==================== Методы вставки в сателлиты ====================

    def s_user_names_insert(self, sat: S_User_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                    VALUES (%(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s, %(hk_user_names_hashdiff)s)
                    ON CONFLICT (h_user_pk, load_dt) DO NOTHING
                """, {
                    "h_user_pk": sat.h_user_pk,
                    "username": sat.username,
                    "userlogin": sat.userlogin,
                    "load_dt": sat.load_dt,
                    "load_src": sat.load_src,
                    "hk_user_names_hashdiff": sat.hk_user_names_hashdiff
                })

    def s_product_names_insert(self, sat: S_Product_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                    VALUES (%(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_product_names_hashdiff)s)
                    ON CONFLICT (h_product_pk, load_dt) DO NOTHING
                """, {
                    "h_product_pk": sat.h_product_pk,
                    "name": sat.name,
                    "load_dt": sat.load_dt,
                    "load_src": sat.load_src,
                    "hk_product_names_hashdiff": sat.hk_product_names_hashdiff
                })

    def s_restaurant_names_insert(self, sat: S_Restaurant_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
                    VALUES (%(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_restaurant_names_hashdiff)s)
                    ON CONFLICT (h_restaurant_pk, load_dt) DO NOTHING
                """, {
                    "h_restaurant_pk": sat.h_restaurant_pk,
                    "name": sat.name,
                    "load_dt": sat.load_dt,
                    "load_src": sat.load_src,
                    "hk_restaurant_names_hashdiff": sat.hk_restaurant_names_hashdiff
                })

    def s_order_cost_insert(self, sat: S_Order_Cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
                    VALUES (%(h_order_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s, %(hk_order_cost_hashdiff)s)
                    ON CONFLICT (h_order_pk, load_dt) DO NOTHING
                """, {
                    "h_order_pk": sat.h_order_pk,
                    "cost": sat.cost,
                    "payment": sat.payment,
                    "load_dt": sat.load_dt,
                    "load_src": sat.load_src,
                    "hk_order_cost_hashdiff": sat.hk_order_cost_hashdiff
                })

    def s_order_status_insert(self, sat: S_Order_Status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                    VALUES (%(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s, %(hk_order_status_hashdiff)s)
                    ON CONFLICT (h_order_pk, load_dt) DO NOTHING
                """, {
                    "h_order_pk": sat.h_order_pk,
                    "status": sat.status,
                    "load_dt": sat.load_dt,
                    "load_src": sat.load_src,
                    "hk_order_status_hashdiff": sat.hk_order_status_hashdiff
                })
