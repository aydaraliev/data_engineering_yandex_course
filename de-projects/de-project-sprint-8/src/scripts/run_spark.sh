#!/bin/bash
# Скрипт для запуска Spark Streaming задачи на удалённом сервере

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

REMOTE_SCRIPTS_DIR="/scripts"
SPARK_SCRIPT="${1:-streaming.py}"

echo "=== Running Spark job: $SPARK_SCRIPT ==="

# Экспортируем переменные окружения и запускаем spark-submit
# Используем unbuffer для получения output в реальном времени
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "docker exec $DOCKER_CONTAINER bash -c '\
    export KAFKA_BOOTSTRAP_SERVER=\"$KAFKA_BOOTSTRAP_SERVER\" && \
    export KAFKA_USERNAME=\"$KAFKA_USERNAME\" && \
    export KAFKA_PASSWORD=\"$KAFKA_PASSWORD\" && \
    export KAFKA_TOPIC_IN=\"$KAFKA_TOPIC_IN\" && \
    export KAFKA_TOPIC_OUT=\"$KAFKA_TOPIC_OUT\" && \
    export KAFKA_SSL_TRUSTSTORE_LOCATION=\"$KAFKA_SSL_TRUSTSTORE_LOCATION\" && \
    export KAFKA_SSL_TRUSTSTORE_PASSWORD=\"$KAFKA_SSL_TRUSTSTORE_PASSWORD\" && \
    export CHECKPOINT_LOCATION=\"$CHECKPOINT_LOCATION\" && \
    export PG_SOURCE_HOST=\"$PG_SOURCE_HOST\" && \
    export PG_SOURCE_PORT=\"$PG_SOURCE_PORT\" && \
    export PG_SOURCE_DB=\"$PG_SOURCE_DB\" && \
    export PG_SOURCE_USER=\"$PG_SOURCE_USER\" && \
    export PG_SOURCE_PASSWORD=\"$PG_SOURCE_PASSWORD\" && \
    export PG_DEST_HOST=\"$PG_DEST_HOST\" && \
    export PG_DEST_PORT=\"$PG_DEST_PORT\" && \
    export PG_DEST_DB=\"$PG_DEST_DB\" && \
    export PG_DEST_USER=\"$PG_DEST_USER\" && \
    export PG_DEST_PASSWORD=\"$PG_DEST_PASSWORD\" && \
    mkdir -p \"$CHECKPOINT_LOCATION\" && \
    stdbuf -oL -eL spark-submit \
        --master local[2] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.0 \
        $REMOTE_SCRIPTS_DIR/$SPARK_SCRIPT 2>&1'"