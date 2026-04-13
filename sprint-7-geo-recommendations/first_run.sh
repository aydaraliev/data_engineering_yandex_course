#!/bin/bash
set -e

# Connection constants
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="cluster-user"
SSH_HOST="10.0.0.10"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

# Sampling parameters
SAMPLE_FRACTION="${SAMPLE_FRACTION:-0.1}"  # Default: 10%
LOG_FILE="first_run_$(date +%Y%m%d_%H%M%S).log"

echo "==================================================" | tee $LOG_FILE
echo "First test run of the geo-analytics pipeline" | tee -a $LOG_FILE
echo "Sample mode: ${SAMPLE_FRACTION} ($(echo "$SAMPLE_FRACTION * 100" | bc)%)" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE

# Function for running a Spark job via Docker
exec_spark_job() {
    local script_name="$1"
    local step_name="$2"

    echo "" | tee -a $LOG_FILE
    echo "-> [$step_name] Starting $script_name..." | tee -a $LOG_FILE
    start_time=$(date +%s)

    # Run spark-submit inside the Docker container
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
         spark-submit --master yarn \
         --conf spark.sql.adaptive.enabled=true \
         --conf spark.sql.shuffle.partitions=20 \
         --conf spark.sql.parquet.compression.codec=snappy \
         --py-files /lessons/scripts/geo_utils.py \
         --driver-memory 4g \
         --executor-memory 4g \
         --executor-cores 2 \
         --num-executors 4 \
         /lessons/scripts/$script_name --sample $SAMPLE_FRACTION" \
        2>&1 | tee -a $LOG_FILE

    local exit_code=${PIPESTATUS[0]}
    end_time=$(date +%s)
    duration=$((end_time - start_time))

    if [ $exit_code -eq 0 ]; then
        echo "[$step_name] Completed successfully in ${duration}s" | tee -a $LOG_FILE
    else
        echo "[$step_name] Execution error (code: $exit_code)" | tee -a $LOG_FILE
        echo "" | tee -a $LOG_FILE
        echo "See the log for details: $LOG_FILE" | tee -a $LOG_FILE
        exit $exit_code
    fi
}

# Check connection
echo "" | tee -a $LOG_FILE
echo "-> Checking cluster connection..." | tee -a $LOG_FILE
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
     echo 'Connection successful'" || {
    echo "Failed to connect to the cluster" | tee -a $LOG_FILE
    exit 1
}
echo "Connection established" | tee -a $LOG_FILE

# Record overall start time
overall_start=$(date +%s)

# Sequentially run every pipeline stage
exec_spark_job "create_ods_layer.py" "1/4 ODS Layer"
exec_spark_job "user_geo_mart.py" "2/4 User Geo Mart"
exec_spark_job "zone_mart.py" "3/4 Zone Mart"
exec_spark_job "friend_recommendations.py" "4/4 Friend Recommendations"

# Compute total duration
overall_end=$(date +%s)
overall_duration=$((overall_end - overall_start))

echo "" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "Test run completed successfully!" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Total execution time: ${overall_duration}s ($(echo "scale=2; $overall_duration / 60" | bc) minutes)" | tee -a $LOG_FILE
echo "Log saved to: $LOG_FILE" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Next steps:" | tee -a $LOG_FILE
echo "1. Check the results: ./monitor_pipeline.sh" | tee -a $LOG_FILE
echo "2. If tests pass, deploy the DAG: ./deploy_dag.sh" | tee -a $LOG_FILE
echo "3. Switch the DAG to 100% data or keep test mode" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
