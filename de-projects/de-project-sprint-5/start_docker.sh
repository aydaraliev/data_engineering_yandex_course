#!/usr/bin/env bash
set -euo pipefail

IMAGE="cr.yandex/crp1r8pht0n0gl25aug1/de-pg-cr-af:latest"
CONTAINER_NAME="de-pg-cr-af"
PORTS=(3000 3002 15432)

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not installed or not available in PATH. Install Docker first." >&2
  exit 1
fi

echo "Pulling image ${IMAGE}..."
docker pull "${IMAGE}"

for PORT in "${PORTS[@]}"; do
  conflict_ids=$(docker ps --filter "publish=${PORT}" --format '{{.ID}}')
  if [ -n "${conflict_ids}" ]; then
    echo "Stopping containers using port ${PORT}: ${conflict_ids}"
    docker stop ${conflict_ids} >/dev/null
  fi
done

if docker ps -a --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}$"; then
  echo "Removing existing container ${CONTAINER_NAME}..."
  docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

echo "Starting container ${CONTAINER_NAME}..."
docker run -d \
  --name "${CONTAINER_NAME}" \
  -p 3000:3000 \
  -p 3002:3002 \
  -p 15432:5432 \
  "${IMAGE}"

echo "Airflow UI: http://localhost:3000/airflow"
echo "Postgres: jovyan:jovyan@localhost:15432/de"
