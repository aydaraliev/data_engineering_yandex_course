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

    # Function that will be invoked on schedule.
    def run(self) -> None:
        # Log that the job has started.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            # Consume a message from Kafka
            msg = self._consumer.consume()

            # If there are no more messages, stop processing
            if msg is None:
                break

            # Persist the message into STG
            self._stg_repository.order_events_insert(
                object_id=msg['object_id'],
                object_type=msg['object_type'],
                sent_dttm=datetime.strptime(msg['sent_dttm'], '%Y-%m-%d %H:%M:%S'),
                payload=json.dumps(msg['payload'])
            )

            # Extract user_id and look up user info from Redis
            user_id = msg['payload']['user']['id']
            user = self._redis.get(user_id)

            # Extract restaurant_id and look up restaurant info from Redis
            restaurant_id = msg['payload']['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)

            # Build a product dictionary from the restaurant menu for quick category lookup
            menu_products = {p['_id']: p for p in restaurant['menu']}

            # For each product, fetch its category from the restaurant menu
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

            # Build the outbound message
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

            # Send the message to the producer
            self._producer.produce(output_msg)

            # Commit the offset after successful processing
            self._consumer.commit()

            self._logger.info(f"Message {msg['object_id']} processed")

        # Log that the job has completed successfully.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
