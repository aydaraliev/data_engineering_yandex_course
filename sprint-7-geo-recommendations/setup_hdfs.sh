#!/bin/bash
set -e

# Connection constants
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="cluster-user"
SSH_HOST="10.0.0.10"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

echo "================================================"
echo "Setting up HDFS structure for geo-analytics"
echo "================================================"

# Function for running HDFS commands via Docker
exec_hdfs() {
    local cmd="$1"
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
         hdfs dfs $cmd"
}

# Function for creating a directory with existence check (IDEMPOTENT)
create_hdfs_dir_safe() {
    local dir_path="$1"

    # Check whether the directory already exists
    if exec_hdfs "-test -d $dir_path" 2>/dev/null; then
        echo "  Directory already exists: $dir_path"
        # Set correct permissions
        exec_hdfs "-chmod 775 $dir_path" 2>/dev/null || true
        # Set owner (required for the Airflow user student)
        ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
            "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
             bash -c 'HADOOP_USER_NAME=hdfs hdfs dfs -chown student:hadoop $dir_path'" 2>/dev/null || true
    else
        echo "  -> Creating directory: $dir_path"
        exec_hdfs "-mkdir -p $dir_path" || {
            echo "  Failed to create directory: $dir_path"
            return 1
        }
        # Set permissions for the hadoop group
        exec_hdfs "-chmod 775 $dir_path" || true
        # Set owner (required for the Airflow user student)
        ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
            "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
             bash -c 'HADOOP_USER_NAME=hdfs hdfs dfs -chown student:hadoop $dir_path'" || true
        echo "  Created: $dir_path"
    fi
}

# Function for uploading a file with existence check (IDEMPOTENT)
upload_file_safe() {
    local local_file="$1"
    local hdfs_path="$2"

    # Check whether the file already exists in HDFS
    if exec_hdfs "-test -e $hdfs_path" 2>/dev/null; then
        echo "  File already exists in HDFS: $hdfs_path"
        echo "    Skipping upload (delete the file in HDFS to re-upload)"
    else
        echo "  -> Uploading file: $local_file -> $hdfs_path"

        # Verify the local file exists
        if [ ! -f "$local_file" ]; then
            echo "  Local file not found: $local_file"
            return 1
        fi

        # Copy the file onto the server
        scp $SSH_OPTS "$local_file" ${SSH_USER}@${SSH_HOST}:/tmp/ || {
            echo "  Failed to copy file to the server"
            return 1
        }

        # Upload it to HDFS via Docker
        ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
            "docker exec \$(docker ps --filter 'name=student' --format '{{.Names}}' | head -1) \
             hdfs dfs -put /tmp/$(basename $local_file) $hdfs_path" || {
            echo "  Failed to upload file to HDFS"
            return 1
        }
        echo "  File uploaded: $hdfs_path"
    fi
}

echo ""
echo "-> Creating HDFS directory structure..."
echo ""

# RAW layer
echo "RAW layer:"
create_hdfs_dir_safe "/user/student/project/geo/raw"
create_hdfs_dir_safe "/user/student/project/geo/raw/geo_csv"

echo ""
echo "ODS layer:"
create_hdfs_dir_safe "/user/student/project/geo/ods"
create_hdfs_dir_safe "/user/student/project/geo/ods/events_with_cities"

echo ""
echo "MART layer:"
create_hdfs_dir_safe "/user/student/project/geo/mart"
create_hdfs_dir_safe "/user/student/project/geo/mart/user_geo_report"
create_hdfs_dir_safe "/user/student/project/geo/mart/zone_mart"
create_hdfs_dir_safe "/user/student/project/geo/mart/friend_recommendations"

echo ""
echo "-> Uploading reference data..."
echo ""
upload_file_safe "geo.csv" "/user/student/project/geo/raw/geo_csv/geo.csv"

echo ""
echo "-> Verifying the created structure..."
echo ""
exec_hdfs "-ls -R /user/student/project/geo" || true

echo ""
echo "================================================"
echo "HDFS structure successfully set up!"
echo "================================================"
echo ""
echo "Created directories:"
echo "  RAW:  /user/student/project/geo/raw"
echo "  ODS:  /user/student/project/geo/ods"
echo "  MART: /user/student/project/geo/mart"
echo ""
echo "Next step: ./first_run.sh (test run with 10% sample)"
echo "      or:  ./deploy_dag.sh (deploy the Airflow DAG)"
