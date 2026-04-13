import json
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            # Получаем сообщение из Kafka
            msg = self._consumer.consume()

            # Если сообщений больше нет, прекращаем обработку
            if msg is None:
                break

            # Сохраняем сообщение в STG
            self._stg_repository.order_events_insert(
                object_id=msg['object_id'],
                object_type=msg['object_type'],
                sent_dttm=datetime.strptime(msg['sent_dttm'], '%Y-%m-%d %H:%M:%S'),
                payload=json.dumps(msg['payload'])
            )

            # Достаём user_id и получаем информацию о пользователе из Redis
            user_id = msg['payload']['user']['id']
            user = self._redis.get(user_id)

            # Достаём restaurant_id и получаем информацию о ресторане из Redis
            restaurant_id = msg['payload']['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)

            # Создаём словарь продуктов из меню ресторана для быстрого поиска категории
            menu_products = {p['_id']: p for p in restaurant['menu']}

            # Для каждого product получаем категорию из меню ресторана
            products = []
            for item in msg['payload']['order_items']:
                product_id = item['id']
                product_menu = menu_products.get(product_id, {})
                products.append({
                    'id': product_id,
                    'name': item['name'],
                    'price': item['price'],
                    'quantity': item['quantity'],
                    'category': product_menu.get('category', '')
                })

            # Формируем выходное сообщение
            output_msg = {
                'object_id': msg['object_id'],
                'object_type': msg['object_type'],
                'payload': {
                    'id': msg['object_id'],
                    'date': msg['payload']['date'],
                    'cost': msg['payload']['cost'],
                    'payment': msg['payload']['payment'],
                    'status': msg['payload']['final_status'],
                    'restaurant': {
                        'id': restaurant['_id'],
                        'name': restaurant['name']
                    },
                    'user': {
                        'id': user['_id'],
                        'name': user['name']
                    },
                    'products': products
                }
            }

            # Отправляем сообщение в producer
            self._producer.produce(output_msg)

            # Фиксируем offset после успешной обработки
            self._consumer.commit()

            self._logger.info(f"Message {msg['object_id']} processed")

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
