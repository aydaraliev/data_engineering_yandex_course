#!/bin/bash
set -e

# Connection constants
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="cluster-user"
SSH_HOST="10.0.0.10"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

DAG_FILE="dags/geo_marts_dag.py"
DAG_NAME="geo_marts_update"

echo "================================================"
echo "Deploying Airflow DAG: $DAG_NAME"
echo "================================================"

# Check the DAG file exists locally
echo ""
echo "-> Checking the DAG file is present..."
if [ ! -f "$DAG_FILE" ]; then
    echo "DAG file not found: $DAG_FILE"
    exit 1
fi
echo "DAG file found: $DAG_FILE"

# Local DAG validation (basic Python syntax check)
echo ""
echo "-> Validating DAG Python syntax..."
python3 -m py_compile "$DAG_FILE" || {
    echo "Syntax error in the DAG file"
    exit 1
}
echo "DAG syntax is correct"

# Copy DAG onto the server
echo ""
echo "-> Copying DAG to the server..."
scp $SSH_OPTS "$DAG_FILE" ${SSH_USER}@${SSH_HOST}:/tmp/geo_marts_dag.py || {
    echo "Failed to copy DAG to the server"
    exit 1
}
echo "DAG copied to the server"

# Copy DAG into the Docker container
echo ""
echo "-> Copying DAG into Airflow..."
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker cp /tmp/geo_marts_dag.py \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1):/lessons/dags/" || {
    echo "Failed to copy DAG into Airflow"
    exit 1
}
echo "DAG deployed to /lessons/dags/"

# Verify DAG in Airflow
echo ""
echo "-> Verifying DAG import in Airflow..."
echo "  (waiting 10 seconds for Airflow to refresh...)"
sleep 10

ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
     airflow dags list | grep $DAG_NAME" && echo "  DAG imported into Airflow successfully" || {
    echo "  WARNING: DAG not yet visible in Airflow (may take a bit longer)"
    echo "    Check manually via the Airflow UI or with: airflow dags list"
}

echo ""
echo "================================================"
echo "DAG deployed successfully!"
echo "================================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Verify the DAG in the Airflow UI:"
echo "   - Open the Airflow web interface"
echo "   - Locate the DAG: $DAG_NAME"
echo "   - Confirm there are no import errors"
echo ""
echo "2. Set the Airflow Variable for test mode (optional):"
echo "   airflow variables set geo_marts_sample_fraction 0.1"
echo "   (enables 10% sample mode for every task)"
echo ""
echo "3. Activate the DAG:"
echo "   - Via UI: toggle the DAG '$DAG_NAME' on"
echo "   - Or via CLI: airflow dags unpause $DAG_NAME"
echo ""
echo "4. Trigger manually (optional):"
echo "   airflow dags trigger $DAG_NAME"
echo ""
echo "5. Revert to 100% data:"
echo "   airflow variables set geo_marts_sample_fraction 1.0"
echo "   (or delete the variable: airflow variables delete geo_marts_sample_fraction)"
echo ""
