#!/bin/bash
set -e

# Connection constants
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="cluster-user"
SSH_HOST="10.0.0.10"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

REPORT_FILE="pipeline_status_$(date +%Y%m%d_%H%M%S).txt"

echo "==================================================" | tee $REPORT_FILE
echo "geo-analytics pipeline monitoring" | tee -a $REPORT_FILE
echo "==================================================" | tee -a $REPORT_FILE

# Function for running commands inside Docker
exec_in_docker() {
    local cmd="$1"
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) bash -c \"$cmd\""
}

# Function for running HDFS commands
exec_hdfs() {
    local cmd="$1"
    exec_in_docker "hdfs dfs $cmd"
}

echo "" | tee -a $REPORT_FILE
echo "-> Checking Airflow DAG..." | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE

# Check DAG in Airflow
exec_in_docker "airflow dags list" | grep geo_marts_update && {
    echo "DAG found in Airflow" | tee -a $REPORT_FILE

    # Check DAG status
    dag_status=$(exec_in_docker "airflow dags list" | grep geo_marts_update | awk '{print $3}')
    echo "  Status: $dag_status" | tee -a $REPORT_FILE

    # Latest runs
    echo "" | tee -a $REPORT_FILE
    echo "  Latest runs:" | tee -a $REPORT_FILE
    exec_in_docker "airflow dags list-runs -d geo_marts_update --limit 5" | tee -a $REPORT_FILE || true
} || {
    echo "WARNING: DAG not found in Airflow" | tee -a $REPORT_FILE
}

echo "" | tee -a $REPORT_FILE
echo "-> Checking HDFS structure..." | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE

# Check ODS layer
echo "ODS layer (events_with_cities):" | tee -a $REPORT_FILE
if exec_hdfs "-test -d /user/student/project/geo/ods/events_with_cities" 2>/dev/null; then
    echo "  Directory exists" | tee -a $REPORT_FILE
    ods_size=$(exec_hdfs "-du -s -h /user/student/project/geo/ods/events_with_cities" | awk '{print $1}')
    echo "  Size: $ods_size" | tee -a $REPORT_FILE

    # Number of partitions
    partitions=$(exec_hdfs "-ls /user/student/project/geo/ods/events_with_cities" | grep "date=" | wc -l || echo "0")
    echo "  Partitions (by date): $partitions" | tee -a $REPORT_FILE
else
    echo "  Directory not found" | tee -a $REPORT_FILE
fi

echo "" | tee -a $REPORT_FILE
echo "MART layer:" | tee -a $REPORT_FILE

# Check marts
MARTS=(
    "user_geo_report"
    "zone_mart"
    "friend_recommendations"
)

for mart in "${MARTS[@]}"; do
    echo "" | tee -a $REPORT_FILE
    echo "Mart: $mart" | tee -a $REPORT_FILE
    mart_path="/user/student/project/geo/mart/$mart"

    if exec_hdfs "-test -d $mart_path" 2>/dev/null; then
        echo "  Directory exists" | tee -a $REPORT_FILE

        # Mart size
        mart_size=$(exec_hdfs "-du -s -h $mart_path" | awk '{print $1}')
        echo "  Size: $mart_size" | tee -a $REPORT_FILE

        # Number of files
        file_count=$(exec_hdfs "-ls -R $mart_path" | grep "\.parquet$" | wc -l || echo "0")
        echo "  Parquet files: $file_count" | tee -a $REPORT_FILE

        # Partitioning (for zone_mart)
        if [ "$mart" == "zone_mart" ]; then
            month_partitions=$(exec_hdfs "-ls $mart_path" | grep "month=" | wc -l || echo "0")
            echo "  Partitions (by month): $month_partitions" | tee -a $REPORT_FILE
        fi
    else
        echo "  Directory not found" | tee -a $REPORT_FILE
    fi
done

echo "" | tee -a $REPORT_FILE
echo "-> Checking data quality..." | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE

# Data quality checks via PySpark
exec_in_docker "python3 -c \"
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DataQualityCheck').getOrCreate()

print('ODS layer check:')
try:
    ods = spark.read.parquet('/user/student/project/geo/ods/events_with_cities')
    total = ods.count()
    null_cities = ods.filter('city IS NULL').count()
    print(f'  Total events: {total:,}')
    print(f'  NULL cities: {null_cities} ({null_cities/total*100:.2f}%)')
    print(f'  Unique cities: {ods.select(\"city\").distinct().count()}')
except Exception as e:
    print(f'  Error: {e}')

print('')
print('User Geo Mart check:')
try:
    user_mart = spark.read.parquet('/user/student/project/geo/mart/user_geo_report')
    total = user_mart.count()
    null_act = user_mart.filter('act_city IS NULL').count()
    print(f'  Total users: {total:,}')
    print(f'  NULL act_city: {null_act}')
    print(f'  With home_city: {user_mart.filter(\"home_city IS NOT NULL\").count():,}')
except Exception as e:
    print(f'  Error: {e}')

print('')
print('Zone Mart check:')
try:
    zone_mart = spark.read.parquet('/user/student/project/geo/mart/zone_mart')
    print(f'  Total records: {zone_mart.count():,}')
    print(f'  Unique zones: {zone_mart.select(\"zone_id\").distinct().count()}')
    print(f'  Unique weeks: {zone_mart.select(\"week\").distinct().count()}')
except Exception as e:
    print(f'  Error: {e}')

print('')
print('Friend Recommendations check:')
try:
    friends = spark.read.parquet('/user/student/project/geo/mart/friend_recommendations')
    total = friends.count()
    print(f'  Total recommendations: {total:,}')
    print(f'  Unique users (left): {friends.select(\"user_left\").distinct().count():,}')
    print(f'  Unique zones: {friends.select(\"zone_id\").distinct().count()}')
    # Correctness check: user_left < user_right
    invalid = friends.filter('user_left >= user_right').count()
    print(f'  Invalid pairs (left >= right): {invalid}')
except Exception as e:
    print(f'  Error: {e}')

spark.stop()
\"" 2>&1 | tee -a $REPORT_FILE || echo "WARNING: data quality check failed" | tee -a $REPORT_FILE

echo "" | tee -a $REPORT_FILE
echo "==================================================" | tee -a $REPORT_FILE
echo "Monitoring complete" | tee -a $REPORT_FILE
echo "==================================================" | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE
echo "Report saved to: $REPORT_FILE" | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE
