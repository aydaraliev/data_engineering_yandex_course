from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository, generate_uuid


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None:
                self._logger.info("Сообщений больше нет")
                break

            order_id = msg.get('object_id')
            self._logger.info(f"Получено сообщение: {order_id}")

            # Проверяем, был ли заказ уже обработан (идемпотентность)
            if self._cdm_repository.is_order_processed(order_id):
                self._logger.info(f"Заказ {order_id} уже обработан, пропускаем")
                self._consumer.commit()
                continue

            # Извлекаем payload
            payload = msg.get('payload', {})
            user_id_str = payload.get('user_id')
            products = payload.get('products', [])

            # Генерируем UUID пользователя
            h_user_pk = generate_uuid(user_id_str)

            # Множество для отслеживания уникальных категорий в заказе
            processed_categories = set()

            # Обрабатываем каждый продукт
            for product in products:
                product_id_str = product.get('id')
                product_name = product.get('name')
                category_name = product.get('category')

                # Генерируем UUID продукта и категории
                h_product_pk = generate_uuid(product_id_str)
                h_category_pk = generate_uuid(category_name)

                # Обновляем счётчик продуктов пользователя
                self._cdm_repository.user_product_counters_upsert(
                    user_id=h_user_pk,
                    product_id=h_product_pk,
                    product_name=product_name
                )

                # Обновляем счётчик категорий (только для уникальных категорий в заказе)
                if category_name not in processed_categories:
                    self._cdm_repository.user_category_counters_upsert(
                        user_id=h_user_pk,
                        category_id=h_category_pk,
                        category_name=category_name
                    )
                    processed_categories.add(category_name)

            # Помечаем заказ как обработанный
            self._cdm_repository.mark_order_processed(order_id)

            # Фиксируем offset после успешной обработки
            self._consumer.commit()

            self._logger.info(f"Сообщение {order_id} обработано")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
