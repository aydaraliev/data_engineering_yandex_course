#!/bin/bash
# Script for deploying files to a remote server's Docker container

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

REMOTE_SCRIPTS_DIR="/scripts"

echo "=== Deploying to $REMOTE_HOST ==="

# Copy Python scripts to the remote host
echo "Copying files to remote host..."
scp -i "$SSH_KEY" "$SCRIPT_DIR/streaming.py" "$REMOTE_USER@$REMOTE_HOST:/tmp/"

# Copy files into the Docker container
echo "Copying files to Docker container..."
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "\
    docker exec $DOCKER_CONTAINER mkdir -p $REMOTE_SCRIPTS_DIR && \
    docker cp /tmp/streaming.py $DOCKER_CONTAINER:$REMOTE_SCRIPTS_DIR/ && \
    rm /tmp/streaming.py"

echo "=== Deploy complete ==="
echo "Files deployed to container:$REMOTE_SCRIPTS_DIR/"

# Show directory contents
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "docker exec $DOCKER_CONTAINER ls -la $REMOTE_SCRIPTS_DIR/"
