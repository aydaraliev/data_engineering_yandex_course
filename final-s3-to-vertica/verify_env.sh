#!/bin/bash

echo "=== 1. Checking Docker Container Status ==="
docker ps --filter "name=de-final-prj-local" --format "table {{.ID}}\t{{.Status}}\t{{.Ports}}"

echo -e "\n=== 2. Checking Airflow (Web UI) ==="
# Checks if the login page is accessible (HTTP 200 or 302 Redirect)
status_code=$(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8280/airflow/login/)
echo "Airflow Response Code: $status_code (Expected: 200 or 302)"

echo -e "\n=== 3. Checking Metabase (BI Tool) ==="
# Checks if the setup/login page is accessible
status_code=$(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8998)
echo "Metabase Response Code: $status_code (Expected: 200 or 302)"

echo -e "\n=== 4. Checking PostgreSQL (Database) ==="
# Executes a query inside the container to verify the DB is accepting connections
docker exec -i de-final-prj-local su - postgres -c "psql -c 'SELECT version() as postgres_version;'"

echo -e "\n=== Verification Complete ==="
echo "If PostgreSQL version printed above, the database is ready."
