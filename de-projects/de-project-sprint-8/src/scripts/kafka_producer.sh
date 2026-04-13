#!/bin/bash
# Скрипт для отправки сообщения в Kafka топик

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

# Топик по умолчанию - входной, но можно передать другой как аргумент
TOPIC="${1:-$KAFKA_TOPIC_IN}"

# Тестовое сообщение с актуальными временными метками
CURRENT_TS=$(date +%s)
START_TS=$CURRENT_TS
END_TS=$((CURRENT_TS + 86400))  # +24 часа

MESSAGE="{\"restaurant_id\": \"123e4567-e89b-12d3-a456-426614174000\",\"adv_campaign_id\": \"123e4567-e89b-12d3-a456-426614174003\",\"adv_campaign_content\": \"first campaign\",\"adv_campaign_owner\": \"Ivanov Ivan Ivanovich\",\"adv_campaign_owner_contact\": \"iiivanov@restaurant.ru\",\"adv_campaign_datetime_start\": $START_TS,\"adv_campaign_datetime_end\": $END_TS,\"datetime_created\": $CURRENT_TS}"

echo "Sending message to topic: $TOPIC"
echo "Message: $MESSAGE"
echo "---"

# Сохраняем сообщение во временный файл на удалённом хосте, чтобы избежать проблем с экранированием
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "echo 'test_key:$MESSAGE' > /tmp/kafka_msg.txt && \
    docker cp /tmp/kafka_msg.txt $DOCKER_CONTAINER:/tmp/kafka_msg.txt && \
    docker exec $DOCKER_CONTAINER kafkacat \
        -b $KAFKA_BOOTSTRAP_SERVER \
        -X security.protocol=SASL_SSL \
        -X sasl.mechanisms=SCRAM-SHA-512 \
        -X sasl.username=$KAFKA_USERNAME \
        -X sasl.password=$KAFKA_PASSWORD \
        -X ssl.ca.location=$KAFKA_SSL_CA_LOCATION \
        -t $TOPIC \
        -K: \
        -P -l /tmp/kafka_msg.txt && \
    rm /tmp/kafka_msg.txt"

echo "Message sent!"
