#!/bin/bash
set -e

# Connection constants
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="cluster-user"
SSH_HOST="10.0.0.10"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

DAG_NAME="geo_marts_update"
LOG_FILE="dag_run_full_$(date +%Y%m%d_%H%M%S).log"

echo "==================================================" | tee $LOG_FILE
echo "Running DAG '$DAG_NAME' on 100% of the data" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE

# Function for running Airflow commands inside Docker
exec_airflow() {
    local cmd="$1"
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
         $cmd"
}

echo "" | tee -a $LOG_FILE
echo "-> Checking connection..." | tee -a $LOG_FILE
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
     echo 'Connection successful'" || {
    echo "ERROR: Unable to connect to the cluster" | tee -a $LOG_FILE
    exit 1
}
echo "Connection established" | tee -a $LOG_FILE

# Verify the DAG exists
echo "" | tee -a $LOG_FILE
echo "-> Checking DAG in Airflow..." | tee -a $LOG_FILE
exec_airflow "airflow dags list | grep $DAG_NAME" &>/dev/null || {
    echo "ERROR: DAG '$DAG_NAME' not found in Airflow" | tee -a $LOG_FILE
    echo "  Run ./deploy_dag.sh first" | tee -a $LOG_FILE
    exit 1
}
echo "DAG found: $DAG_NAME" | tee -a $LOG_FILE

# Configure full-data mode (100%)
echo "" | tee -a $LOG_FILE
echo "-> Configuring full-data mode (100%)..." | tee -a $LOG_FILE

# Remove the sample_fraction variable if it exists (this restores the 100% default)
exec_airflow "airflow variables delete geo_marts_sample_fraction" 2>/dev/null || true

# Or set it explicitly to 1.0
exec_airflow "airflow variables set geo_marts_sample_fraction 1.0" 2>&1 | tee -a $LOG_FILE

# Verify the configured value
SAMPLE_VALUE=$(exec_airflow "airflow variables get geo_marts_sample_fraction 2>/dev/null" || echo "1.0")
echo "Sample mode: ${SAMPLE_VALUE} (100% of data)" | tee -a $LOG_FILE

# Activate the DAG (if not active)
echo "" | tee -a $LOG_FILE
echo "-> Activating DAG..." | tee -a $LOG_FILE
exec_airflow "airflow dags unpause $DAG_NAME" 2>&1 | tee -a $LOG_FILE
echo "DAG activated" | tee -a $LOG_FILE

# Trigger the DAG
echo "" | tee -a $LOG_FILE
echo "-> Triggering DAG '$DAG_NAME'..." | tee -a $LOG_FILE
RUN_ID=$(exec_airflow "airflow dags trigger $DAG_NAME --conf '{\"sample_fraction\": 1.0}' 2>&1" | grep -o "Created <DagRun.*>" | grep -o "manual__[^>]*" || echo "manual_$(date +%Y-%m-%dT%H:%M:%S)")
echo "DAG triggered" | tee -a $LOG_FILE
echo "  Run ID: $RUN_ID" | tee -a $LOG_FILE

# Monitor the run
echo "" | tee -a $LOG_FILE
echo "-> Monitoring execution..." | tee -a $LOG_FILE
echo "  (Refresh every 30 seconds)" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# Wait for completion with periodic checks
MAX_WAIT_MINUTES=120  # Up to 2 hours
WAIT_SECONDS=30
ITERATIONS=$((MAX_WAIT_MINUTES * 60 / WAIT_SECONDS))

for i in $(seq 1 $ITERATIONS); do
    # Fetch DAG run status
    DAG_STATE=$(exec_airflow "airflow dags state $DAG_NAME $RUN_ID 2>/dev/null" || echo "unknown")

    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] Status: $DAG_STATE" | tee -a $LOG_FILE

    # Check final states
    if [[ "$DAG_STATE" == "success" ]]; then
        echo "" | tee -a $LOG_FILE
        echo "DAG executed successfully!" | tee -a $LOG_FILE
        break
    elif [[ "$DAG_STATE" == "failed" ]]; then
        echo "" | tee -a $LOG_FILE
        echo "DAG finished with errors" | tee -a $LOG_FILE
        echo "" | tee -a $LOG_FILE
        echo "-> Most recent failed tasks:" | tee -a $LOG_FILE
        exec_airflow "airflow tasks states-for-dag-run $DAG_NAME $RUN_ID 2>/dev/null | grep failed" | tee -a $LOG_FILE || true
        exit 1
    fi

    # Wait before the next check
    if [ $i -lt $ITERATIONS ]; then
        sleep $WAIT_SECONDS
    fi
done

# Check whether the timeout was exceeded
if [[ "$DAG_STATE" != "success" ]]; then
    echo "" | tee -a $LOG_FILE
    echo "WARNING: wait timeout exceeded ($MAX_WAIT_MINUTES minutes)" | tee -a $LOG_FILE
    echo "  DAG is still running: $DAG_STATE" | tee -a $LOG_FILE
    echo "  Check the status in the Airflow UI" | tee -a $LOG_FILE
fi

# Show HDFS statistics
echo "" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "HDFS data statistics" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "-> ODS layer (events_with_cities):" | tee -a $LOG_FILE
exec_airflow "hdfs dfs -du -s -h /user/student/project/geo/ods/events_with_cities" 2>/dev/null | awk '{print "  Size: " $1}' | tee -a $LOG_FILE
exec_airflow "hdfs dfs -ls /user/student/project/geo/ods/events_with_cities | grep '^d' | wc -l" 2>/dev/null | awk '{print "  Partitions: " $1}' | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "-> MART: user_geo_report:" | tee -a $LOG_FILE
exec_airflow "hdfs dfs -du -s -h /user/student/project/geo/mart/user_geo_report" 2>/dev/null | awk '{print "  Size: " $1}' | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "-> MART: zone_mart:" | tee -a $LOG_FILE
exec_airflow "hdfs dfs -du -s -h /user/student/project/geo/mart/zone_mart" 2>/dev/null | awk '{print "  Size: " $1}' | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "-> MART: friend_recommendations:" | tee -a $LOG_FILE
exec_airflow "hdfs dfs -du -s -h /user/student/project/geo/mart/friend_recommendations" 2>/dev/null | awk '{print "  Size: " $1}' | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "Execution complete" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Log saved to: $LOG_FILE" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "To view DAG logs in Airflow:" | tee -a $LOG_FILE
echo "  airflow dags show $DAG_NAME" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
