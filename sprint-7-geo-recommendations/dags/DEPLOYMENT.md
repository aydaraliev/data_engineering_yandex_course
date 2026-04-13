# Deploying the DAG in Airflow

## Prerequisites

### 1. Installed components
- Apache Airflow 2.0+
- Python 3.7+
- Apache Spark 3.3+
- Provider: `apache-airflow-providers-apache-spark`

### 2. Installing the Spark provider (if missing)
```bash
pip install apache-airflow-providers-apache-spark
```

### 3. Project structure
```
/lessons/
dags/
    geo_marts_dag.py          # DAG file
scripts/
    user_geo_mart.py          # User mart script
    zone_mart.py              # Zone mart script
    friend_recommendations.py # Recommendations script
    geo_utils.py              # Utilities (Haversine, etc.)
```

## Step 1: Prepare the scripts

### 1.1. Copy the scripts onto the server

```bash
# If scripts are in a local directory
LOCAL_SCRIPTS_DIR="src/scripts"
REMOTE_USER="cluster-user"
REMOTE_HOST="10.0.0.11"
SSH_KEY="~/.ssh/ssh_private_key"

# Copy to the server
scp -i $SSH_KEY \
    $LOCAL_SCRIPTS_DIR/user_geo_mart.py \
    $LOCAL_SCRIPTS_DIR/zone_mart.py \
    $LOCAL_SCRIPTS_DIR/friend_recommendations.py \
    $LOCAL_SCRIPTS_DIR/geo_utils.py \
    $REMOTE_USER@$REMOTE_HOST:/lessons/scripts/
```

### 1.2. Verify script availability

```bash
# Connect to the server
ssh -i $SSH_KEY $REMOTE_USER@$REMOTE_HOST

# Check for files
ls -la /lessons/scripts/

# Expected output:
# user_geo_mart.py
# zone_mart.py
# friend_recommendations.py
# geo_utils.py
```

### 1.3. Validate script syntax

```bash
# Python syntax check
python3 /lessons/scripts/user_geo_mart.py --help
python3 /lessons/scripts/zone_mart.py --help
python3 /lessons/scripts/friend_recommendations.py --help
```

## Step 2: Configure the Airflow Connection

### 2.1. Via the Airflow UI

1. Open the Airflow Web UI (usually http://localhost:8080)
2. Navigate to **Admin -> Connections**
3. Click the **"+"** button (Add Connection)
4. Fill out the form:

```
Connection Id: yarn_spark
Connection Type: Spark
Host: yarn://master-host:8032
Port: 8032
Extra: {"queue": "default"}
```

5. Click **Save**

### 2.2. Via the Airflow CLI

```bash
airflow connections add yarn_spark \
    --conn-type spark \
    --conn-host yarn://master-host:8032 \
    --conn-port 8032 \
    --conn-extra '{"queue": "default"}'
```

### 2.3. Via environment variables

```bash
export AIRFLOW_CONN_YARN_SPARK='spark://yarn://master-host:8032'
```

## Step 3: Deploy the DAG

### 3.1. Copy the DAG file

```bash
# Determine the Airflow dags folder path
AIRFLOW_DAGS_DIR=$(airflow config get-value core dags_folder)

# Or use the standard path
AIRFLOW_DAGS_DIR="/opt/airflow/dags"  # Or ~/airflow/dags

# Copy the DAG
cp dags/geo_marts_dag.py $AIRFLOW_DAGS_DIR/

# Verify
ls -la $AIRFLOW_DAGS_DIR/geo_marts_dag.py
```

### 3.2. Alternative: create a symbolic link

```bash
ln -s /path/to/project/dags/geo_marts_dag.py $AIRFLOW_DAGS_DIR/geo_marts_dag.py
```

## Step 4: Validate the DAG

### 4.1. Run the validation script

```bash
cd dags/
python3 validate_dag.py
```

Expected output:
```
======================================================================
DAG VALIDATION: geo_marts_dag.py
======================================================================

1. Checking the file...
   File exists: /path/to/geo_marts_dag.py

2. Checking Python syntax...
   Python syntax is correct

3. Importing the DAG...
   DAG imported successfully

4. Checking DAG attributes...
   DAG ID: geo_marts_update
   Schedule: 0 0 * * *
   Default args: 7 parameters

5. Checking tasks...
   Number of tasks: 5
   Task found: start
   Task found: update_user_geo_report
   Task found: update_zone_mart
   Task found: update_friend_recommendations
   Task found: end

6. Checking dependencies...
   Dependency: start -> update_user_geo_report
   Dependency: update_user_geo_report -> update_zone_mart
   Dependency: update_zone_mart -> update_friend_recommendations
   Dependency: update_friend_recommendations -> end

7. Checking operator types...
   - start: PythonOperator
   - update_user_geo_report: SparkSubmitOperator
     Application: /lessons/scripts/user_geo_mart.py
     Py files: /lessons/scripts/geo_utils.py
   - update_zone_mart: SparkSubmitOperator
     Application: /lessons/scripts/zone_mart.py
     Py files: /lessons/scripts/geo_utils.py
   - update_friend_recommendations: SparkSubmitOperator
     Application: /lessons/scripts/friend_recommendations.py
     Py files: /lessons/scripts/geo_utils.py
   - end: PythonOperator

8. Checking for cyclic dependencies...
   No cyclic dependencies

======================================================================
VALIDATION SUMMARY
======================================================================
DAG is fully valid! Ready for deployment.
======================================================================
```

### 4.2. Check via the Airflow CLI

```bash
# List all DAGs
airflow dags list | grep geo_marts

# Check for import errors
airflow dags list-import-errors

# Show DAG structure
airflow dags show geo_marts_update

# List tasks
airflow tasks list geo_marts_update
```

## Step 5: Activate the DAG

### 5.1. Via the Airflow UI

1. Open the Airflow Web UI
2. Find the DAG **geo_marts_update** in the list
3. Toggle the switch on the left of the DAG name
4. The DAG becomes active

### 5.2. Via the Airflow CLI

```bash
# Unpause the DAG
airflow dags unpause geo_marts_update

# Check status
airflow dags state geo_marts_update $(date +%Y-%m-%d)
```

## Step 6: Test run

### 6.1. Manual run through the UI

1. Locate the DAG **geo_marts_update** in the Airflow UI
2. Click **"Trigger DAG"**
3. Confirm the trigger
4. Watch execution in **Graph View** or **Tree View**

### 6.2. Manual run through the CLI

```bash
# Trigger the DAG
airflow dags trigger geo_marts_update

# Trigger for a specific date
airflow dags trigger geo_marts_update --exec-date 2024-01-15
```

### 6.3. Test a single task

```bash
# Dry-run test
airflow tasks test geo_marts_update start 2024-01-15

# Inspect task parameters
airflow tasks render geo_marts_update update_user_geo_report 2024-01-15
```

## Step 7: Monitoring

### 7.1. View logs

```bash
# Logs of a specific task
airflow tasks logs geo_marts_update update_user_geo_report $(date +%Y-%m-%d)

# Last 100 lines
airflow tasks logs geo_marts_update update_user_geo_report $(date +%Y-%m-%d) | tail -100
```

### 7.2. Check execution status

```bash
# DAG run status
airflow dags state geo_marts_update $(date +%Y-%m-%d)

# List all runs
airflow dags list-runs -d geo_marts_update
```

### 7.3. Airflow UI Views

- **Graph View**: task flow visualization
- **Tree View**: execution history by date
- **Gantt**: execution timeline
- **Task Duration**: task duration chart

## Step 8: Inspect the results

### 8.1. Check HDFS

```bash
# Connect to the server
ssh -i $SSH_KEY $REMOTE_USER@$REMOTE_HOST

# Enter the container
CONTAINER=$(docker ps --format '{{.Names}}' | grep student | head -1)
docker exec -it $CONTAINER bash

# Inspect marts in HDFS
hdfs dfs -ls /user/student/project/geo/mart/

# Expected output:
# /user/student/project/geo/mart/user_geo_report
# /user/student/project/geo/mart/zone_mart
# /user/student/project/geo/mart/friend_recommendations
```

### 8.2. Inspect mart contents

```bash
# Record counts
hdfs dfs -count /user/student/project/geo/mart/user_geo_report
hdfs dfs -count /user/student/project/geo/mart/zone_mart
hdfs dfs -count /user/student/project/geo/mart/friend_recommendations
```

## Troubleshooting

### Problem: DAG does not appear in the UI

**Solutions**:
1. Check the dags folder path:
   ```bash
   airflow config get-value core dags_folder
   ```

2. Check permissions:
   ```bash
   chmod 644 $AIRFLOW_DAGS_DIR/geo_marts_dag.py
   ```

3. Check for import errors:
   ```bash
   airflow dags list-import-errors
   ```

4. Restart the Airflow scheduler:
   ```bash
   airflow scheduler restart
   # Or
   systemctl restart airflow-scheduler
   ```

### Problem: Connection 'yarn_spark' not found

**Solution**: Create the connection (see Step 2)

### Problem: Task fails with an error

**Solutions**:
1. Check task logs:
   ```bash
   airflow tasks logs geo_marts_update <task_id> <date>
   ```

2. Check script availability:
   ```bash
   ls -la /lessons/scripts/
   ```

3. Check HDFS paths:
   ```bash
   hdfs dfs -ls /user/master/data/geo/events
   hdfs dfs -ls /user/student/project/geo/raw/geo_csv/
   ```

### Problem: OutOfMemoryError in Spark

**Solution**: increase resources in the DAG:
```python
driver_memory='8g'
executor_memory='8g'
num_executors=8
```

### Problem: Task is hanging

**Solution**: kill the task and restart:
```bash
# Find active Spark jobs
yarn application -list

# Kill the hung job
yarn application -kill <application_id>

# Clear the task in Airflow
airflow tasks clear geo_marts_update <task_id> -t <date>
```

## Rolling back changes

### Disable the DAG

```bash
# Via CLI
airflow dags pause geo_marts_update

# Via UI: toggle the switch off
```

### Delete the DAG

```bash
# Remove the file
rm $AIRFLOW_DAGS_DIR/geo_marts_dag.py

# Remove from the database
airflow dags delete geo_marts_update
```

## Automated deployment (CI/CD)

### Example shell script for deployment

```bash
#!/bin/bash
# deploy_dag.sh

set -e

AIRFLOW_DAGS_DIR="/opt/airflow/dags"
PROJECT_DIR="/path/to/project"

echo "Deploying DAG for geo marts..."

# 1. Validation
echo "1. Validating DAG..."
cd $PROJECT_DIR/dags
python3 validate_dag.py
if [ $? -ne 0 ]; then
    echo "Validation failed!"
    exit 1
fi

# 2. Copy scripts
echo "2. Copying scripts..."
cp $PROJECT_DIR/src/scripts/*.py /lessons/scripts/

# 3. Copy DAG
echo "3. Copying DAG..."
cp $PROJECT_DIR/dags/geo_marts_dag.py $AIRFLOW_DAGS_DIR/

# 4. Verify in Airflow
echo "4. Verifying in Airflow..."
sleep 5
airflow dags list | grep geo_marts_update
if [ $? -ne 0 ]; then
    echo "DAG not found in Airflow!"
    exit 1
fi

# 5. Activate
echo "5. Activating DAG..."
airflow dags unpause geo_marts_update

echo "Deployment completed successfully!"
```

## Useful commands

```bash
# List all DAGs
airflow dags list

# DAG info
airflow dags show geo_marts_update

# List tasks
airflow tasks list geo_marts_update

# Run history
airflow dags list-runs -d geo_marts_update --state success
airflow dags list-runs -d geo_marts_update --state failed

# Clear tasks for re-run
airflow tasks clear geo_marts_update -s 2024-01-01 -e 2024-01-31

# Backfill (run for past dates)
airflow dags backfill geo_marts_update -s 2024-01-01 -e 2024-01-10
```

## Contacts

**Project**: Data Lake Sprint 7 - Geo Recommendations
**Owner**: student
**Created**: 2024
