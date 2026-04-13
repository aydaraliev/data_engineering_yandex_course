#!/bin/bash
# Скрипт для деплоя файлов на удалённый сервер в Docker контейнер

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

REMOTE_SCRIPTS_DIR="/scripts"

echo "=== Deploying to $REMOTE_HOST ==="

# Копируем Python скрипты на удалённый хост
echo "Copying files to remote host..."
scp -i "$SSH_KEY" "$SCRIPT_DIR/streaming.py" "$REMOTE_USER@$REMOTE_HOST:/tmp/"

# Копируем файлы внутрь Docker контейнера
echo "Copying files to Docker container..."
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "\
    docker exec $DOCKER_CONTAINER mkdir -p $REMOTE_SCRIPTS_DIR && \
    docker cp /tmp/streaming.py $DOCKER_CONTAINER:$REMOTE_SCRIPTS_DIR/ && \
    rm /tmp/streaming.py"

echo "=== Deploy complete ==="
echo "Files deployed to container:$REMOTE_SCRIPTS_DIR/"

# Показываем содержимое директории
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "docker exec $DOCKER_CONTAINER ls -la $REMOTE_SCRIPTS_DIR/"
