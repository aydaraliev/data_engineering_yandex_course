#!/bin/bash
# Script for reading messages from a Kafka topic
# Usage: ./kafka_consumer.sh [topic] [--follow]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

TOPIC="${1:-$KAFKA_TOPIC_IN}"
EXIT_FLAG="-e"  # By default, exit after reading all messages

# If the --follow flag is passed, keep waiting for new messages
if [[ "$2" == "--follow" ]]; then
    EXIT_FLAG=""
    echo "Starting Kafka consumer for topic: $TOPIC (following new messages)"
    echo "Press Ctrl+C to stop"
else
    echo "Reading all messages from topic: $TOPIC"
fi
echo "---"

ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "docker exec $DOCKER_CONTAINER kafkacat \
    -b $KAFKA_BOOTSTRAP_SERVER \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username=\"$KAFKA_USERNAME\" \
    -X sasl.password=\"$KAFKA_PASSWORD\" \
    -X ssl.ca.location=\"$KAFKA_SSL_CA_LOCATION\" \
    -t $TOPIC \
    -C \
    -o beginning \
    $EXIT_FLAG"
