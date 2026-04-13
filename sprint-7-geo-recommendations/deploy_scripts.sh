#!/bin/bash
set -e

# Connection constants
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="cluster-user"
SSH_HOST="10.0.0.10"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

echo "=============================================="
echo "Deploying Python scripts to $SSH_HOST"
echo "=============================================="

# Check SSH connection
echo "-> Checking SSH connection..."
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} "echo 'SSH connection works'" || {
    echo "SSH connection failed"
    echo "  Verify:"
    echo "  - SSH key exists: $SSH_KEY"
    echo "  - Host reachable: $SSH_HOST"
    echo "  - User permissions: $SSH_USER"
    exit 1
}
echo "SSH connection successful"

# Scripts to deploy
SCRIPTS=(
    "src/scripts/create_ods_layer.py"
    "src/scripts/geo_utils.py"
    "src/scripts/user_geo_mart.py"
    "src/scripts/zone_mart.py"
    "src/scripts/friend_recommendations.py"
)

# Check files exist locally
echo ""
echo "-> Checking files are present..."
for script in "${SCRIPTS[@]}"; do
    if [ ! -f "$script" ]; then
        echo "File not found: $script"
        exit 1
    fi
done
echo "All files found locally"

# Copy scripts
echo ""
echo "-> Creating the scripts directory in the container (if missing)..."
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
     mkdir -p /lessons/scripts" || {
    echo "Failed to create the scripts directory"
    exit 1
}
echo "Directory /lessons/scripts is ready"

echo ""
echo "-> Copying scripts..."
for script in "${SCRIPTS[@]}"; do
    script_name=$(basename $script)
    echo "  Copying $script_name..."

    # Copy the file to the server's temp directory
    scp $SSH_OPTS "$script" ${SSH_USER}@${SSH_HOST}:/tmp/ || {
        echo "Failed to copy $script_name to the server"
        exit 1
    }

    # Copy from the temp directory into the Docker container
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker cp /tmp/$script_name \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1):/lessons/scripts/" || {
        echo "Failed to copy $script_name into the Docker container"
        exit 1
    }

    echo "  $script_name deployed"
done

# Verify deployment
echo ""
echo "-> Verifying deployment inside the Docker container..."
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
     sh -c 'ls -lh /lessons/scripts/*.py'" || {
    echo "Failed to verify files in the container"
    exit 1
}

echo ""
echo "=============================================="
echo "All scripts deployed successfully!"
echo "=============================================="
echo ""
echo "Deployed scripts:"
for script in "${SCRIPTS[@]}"; do
    echo "  - $(basename $script)"
done
echo ""
echo "Next step: ./setup_hdfs.sh"
