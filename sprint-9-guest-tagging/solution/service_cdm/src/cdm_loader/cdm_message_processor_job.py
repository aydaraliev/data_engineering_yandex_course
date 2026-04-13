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
                self._logger.info("No more messages")
                break

            order_id = msg.get('object_id')
            self._logger.info(f"Received message: {order_id}")

            # Check whether the order has already been processed (idempotency)
            if self._cdm_repository.is_order_processed(order_id):
                self._logger.info(f"Order {order_id} has already been processed, skipping")
                self._consumer.commit()
                continue

            # Extract the payload
            payload = msg.get('payload', {})
            user_id_str = payload.get('user_id')
            products = payload.get('products', [])

            # Generate the user UUID
            h_user_pk = generate_uuid(user_id_str)

            # Set used to track unique categories within the order
            processed_categories = set()

            # Process each product
            for product in products:
                product_id_str = product.get('id')
                product_name = product.get('name')
                category_name = product.get('category')

                # Generate product and category UUIDs
                h_product_pk = generate_uuid(product_id_str)
                h_category_pk = generate_uuid(category_name)

                # Update the user's product counter
                self._cdm_repository.user_product_counters_upsert(
                    user_id=h_user_pk,
                    product_id=h_product_pk,
                    product_name=product_name
                )

                # Update the category counter (only for unique categories within the order)
                if category_name not in processed_categories:
                    self._cdm_repository.user_category_counters_upsert(
                        user_id=h_user_pk,
                        category_id=h_category_pk,
                        category_name=category_name
                    )
                    processed_categories.add(category_name)

            # Mark the order as processed
            self._cdm_repository.mark_order_processed(order_id)

            # Commit the offset after successful processing
            self._consumer.commit()

            self._logger.info(f"Message {order_id} processed")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
