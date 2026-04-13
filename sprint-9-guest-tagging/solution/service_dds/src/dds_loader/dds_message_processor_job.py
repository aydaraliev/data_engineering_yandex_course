from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import (
    DdsRepository,
    generate_uuid,
    H_User, H_Product, H_Category, H_Restaurant, H_Order,
    L_Order_User, L_Order_Product, L_Product_Restaurant, L_Product_Category,
    S_User_Names, S_Product_Names, S_Restaurant_Names, S_Order_Cost, S_Order_Status
)


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None:
                self._logger.info("Сообщений больше нет")
                break

            self._logger.info(f"Получено сообщение: {msg.get('object_id')}")

            # Извлекаем payload
            payload = msg.get('payload', {})
            order_id = msg.get('object_id')

            # Парсим данные из payload
            user_data = payload.get('user', {})
            restaurant_data = payload.get('restaurant', {})
            products_data = payload.get('products', [])

            order_dt = datetime.strptime(payload.get('date'), '%Y-%m-%d %H:%M:%S')
            load_dt = datetime.utcnow()
            load_src = 'stg-service-orders'

            # Генерируем UUID для хабов
            h_user_pk = generate_uuid(user_data.get('id'))
            h_restaurant_pk = generate_uuid(restaurant_data.get('id'))
            h_order_pk = generate_uuid(str(order_id))

            # ==================== Вставка в хабы ====================

            # h_user
            self._dds_repository.h_user_insert(H_User(
                h_user_pk=h_user_pk,
                user_id=user_data.get('id'),
                load_dt=load_dt,
                load_src=load_src
            ))

            # h_restaurant
            self._dds_repository.h_restaurant_insert(H_Restaurant(
                h_restaurant_pk=h_restaurant_pk,
                restaurant_id=restaurant_data.get('id'),
                load_dt=load_dt,
                load_src=load_src
            ))

            # h_order
            self._dds_repository.h_order_insert(H_Order(
                h_order_pk=h_order_pk,
                order_id=order_id,
                order_dt=order_dt,
                load_dt=load_dt,
                load_src=load_src
            ))

            # ==================== Вставка в линк l_order_user ====================
            hk_order_user_pk = generate_uuid(f"{order_id}_{user_data.get('id')}")
            self._dds_repository.l_order_user_insert(L_Order_User(
                hk_order_user_pk=hk_order_user_pk,
                h_order_pk=h_order_pk,
                h_user_pk=h_user_pk,
                load_dt=load_dt,
                load_src=load_src
            ))

            # ==================== Вставка в сателлиты для user, restaurant, order ====================

            # s_user_names (name используется для обоих полей)
            user_name = user_data.get('name', '')
            hk_user_names_hashdiff = generate_uuid(f"{user_name}_{user_name}")
            self._dds_repository.s_user_names_insert(S_User_Names(
                h_user_pk=h_user_pk,
                username=user_name,
                userlogin=user_name,
                load_dt=load_dt,
                load_src=load_src,
                hk_user_names_hashdiff=hk_user_names_hashdiff
            ))

            # s_restaurant_names
            restaurant_name = restaurant_data.get('name', '')
            hk_restaurant_names_hashdiff = generate_uuid(restaurant_name)
            self._dds_repository.s_restaurant_names_insert(S_Restaurant_Names(
                h_restaurant_pk=h_restaurant_pk,
                name=restaurant_name,
                load_dt=load_dt,
                load_src=load_src,
                hk_restaurant_names_hashdiff=hk_restaurant_names_hashdiff
            ))

            # s_order_cost
            cost = payload.get('cost', 0)
            payment = payload.get('payment', 0)
            hk_order_cost_hashdiff = generate_uuid(f"{cost}_{payment}")
            self._dds_repository.s_order_cost_insert(S_Order_Cost(
                h_order_pk=h_order_pk,
                cost=cost,
                payment=payment,
                load_dt=load_dt,
                load_src=load_src,
                hk_order_cost_hashdiff=hk_order_cost_hashdiff
            ))

            # s_order_status
            status = payload.get('status', '')
            hk_order_status_hashdiff = generate_uuid(status)
            self._dds_repository.s_order_status_insert(S_Order_Status(
                h_order_pk=h_order_pk,
                status=status,
                load_dt=load_dt,
                load_src=load_src,
                hk_order_status_hashdiff=hk_order_status_hashdiff
            ))

            # ==================== Обработка продуктов ====================
            output_products = []

            for product in products_data:
                product_id = product.get('id')
                product_name = product.get('name', '')
                category_name = product.get('category', '')

                h_product_pk = generate_uuid(product_id)
                h_category_pk = generate_uuid(category_name)

                # h_product
                self._dds_repository.h_product_insert(H_Product(
                    h_product_pk=h_product_pk,
                    product_id=product_id,
                    load_dt=load_dt,
                    load_src=load_src
                ))

                # h_category
                self._dds_repository.h_category_insert(H_Category(
                    h_category_pk=h_category_pk,
                    category_name=category_name,
                    load_dt=load_dt,
                    load_src=load_src
                ))

                # l_order_product
                hk_order_product_pk = generate_uuid(f"{order_id}_{product_id}")
                self._dds_repository.l_order_product_insert(L_Order_Product(
                    hk_order_product_pk=hk_order_product_pk,
                    h_order_pk=h_order_pk,
                    h_product_pk=h_product_pk,
                    load_dt=load_dt,
                    load_src=load_src
                ))

                # l_product_restaurant
                hk_product_restaurant_pk = generate_uuid(f"{product_id}_{restaurant_data.get('id')}")
                self._dds_repository.l_product_restaurant_insert(L_Product_Restaurant(
                    hk_product_restaurant_pk=hk_product_restaurant_pk,
                    h_product_pk=h_product_pk,
                    h_restaurant_pk=h_restaurant_pk,
                    load_dt=load_dt,
                    load_src=load_src
                ))

                # l_product_category
                hk_product_category_pk = generate_uuid(f"{product_id}_{category_name}")
                self._dds_repository.l_product_category_insert(L_Product_Category(
                    hk_product_category_pk=hk_product_category_pk,
                    h_product_pk=h_product_pk,
                    h_category_pk=h_category_pk,
                    load_dt=load_dt,
                    load_src=load_src
                ))

                # s_product_names
                hk_product_names_hashdiff = generate_uuid(product_name)
                self._dds_repository.s_product_names_insert(S_Product_Names(
                    h_product_pk=h_product_pk,
                    name=product_name,
                    load_dt=load_dt,
                    load_src=load_src,
                    hk_product_names_hashdiff=hk_product_names_hashdiff
                ))

                # Добавляем продукт в выходной список
                output_products.append({
                    "id": product_id,
                    "name": product_name,
                    "category": category_name
                })

            # ==================== Формируем выходное сообщение для CDM ====================
            output_msg = {
                "object_id": order_id,
                "object_type": "order",
                "payload": {
                    "user_id": user_data.get('id'),
                    "products": output_products
                }
            }

            self._producer.produce(output_msg)

            # Фиксируем offset после успешной обработки
            self._consumer.commit()
            self._logger.info(f"Сообщение {order_id} обработано и отправлено в Kafka")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
