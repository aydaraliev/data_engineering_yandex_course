#!/bin/bash

# =============================================================================
# Kubernetes Deployment Script for STG, DDS, and CDM Services
# =============================================================================
#
# Usage: ./deploy_k8s.sh
#
# Required environment variables (set before running):
#   KAFKA_HOST, KAFKA_CONSUMER_USERNAME, KAFKA_CONSUMER_PASSWORD
#   REDIS_HOST, REDIS_PASSWORD (for STG service)
#   PG_WAREHOUSE_HOST, PG_WAREHOUSE_DBNAME, PG_WAREHOUSE_USER, PG_WAREHOUSE_PASSWORD
#
# Or create a .env file with these variables in the solution/ directory.
# =============================================================================

set -e

REGISTRY_ID="crp5jrq3v9u1b9npu54p"
REGISTRY_URL="cr.yandex/${REGISTRY_ID}"
VERSION="2026.01.17"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Add yc tools to PATH
export PATH="$HOME/.local/bin:$HOME/yandex-cloud/bin:$PATH"

# Load environment variables from .env if exists
if [ -f "${SCRIPT_DIR}/.env" ]; then
    echo "Loading environment variables from .env"
    export $(grep -v '^#' "${SCRIPT_DIR}/.env" | xargs)
fi

# Check required environment variables
check_required_vars() {
    local missing=()
    for var in KAFKA_HOST KAFKA_CONSUMER_USERNAME KAFKA_CONSUMER_PASSWORD \
               REDIS_HOST REDIS_PASSWORD \
               PG_WAREHOUSE_HOST PG_WAREHOUSE_DBNAME PG_WAREHOUSE_USER PG_WAREHOUSE_PASSWORD; do
        if [ -z "${!var}" ]; then
            missing+=("$var")
        fi
    done
    if [ ${#missing[@]} -ne 0 ]; then
        echo "ERROR: Missing required environment variables:"
        printf '  %s\n' "${missing[@]}"
        echo ""
        echo "Set them in environment or create .env file in ${SCRIPT_DIR}/"
        exit 1
    fi
}

check_required_vars

echo "=== Step 1: Configure Docker authentication ==="
yc container registry configure-docker

echo ""
echo "=== Step 2: Build and push STG service ==="
cd "${SCRIPT_DIR}/service_stg"
docker build -t ${REGISTRY_URL}/stg-service:${VERSION} .
docker push ${REGISTRY_URL}/stg-service:${VERSION}

echo ""
echo "=== Step 3: Build and push DDS service ==="
cd "${SCRIPT_DIR}/service_dds"
docker build -t ${REGISTRY_URL}/dds-service:${VERSION} .
docker push ${REGISTRY_URL}/dds-service:${VERSION}

echo ""
echo "=== Step 4: Build and push CDM service ==="
cd "${SCRIPT_DIR}/service_cdm"
docker build -t ${REGISTRY_URL}/cdm-service:${VERSION} .
docker push ${REGISTRY_URL}/cdm-service:${VERSION}

echo ""
echo "=== Step 5: Create Kubernetes secrets ==="

# STG service secrets (includes Redis)
kubectl create secret generic stg-service-secrets \
  --from-literal=KAFKA_HOST="${KAFKA_HOST}" \
  --from-literal=KAFKA_CONSUMER_USERNAME="${KAFKA_CONSUMER_USERNAME}" \
  --from-literal=KAFKA_CONSUMER_PASSWORD="${KAFKA_CONSUMER_PASSWORD}" \
  --from-literal=REDIS_HOST="${REDIS_HOST}" \
  --from-literal=REDIS_PASSWORD="${REDIS_PASSWORD}" \
  --from-literal=PG_WAREHOUSE_HOST="${PG_WAREHOUSE_HOST}" \
  --from-literal=PG_WAREHOUSE_DBNAME="${PG_WAREHOUSE_DBNAME}" \
  --from-literal=PG_WAREHOUSE_USER="${PG_WAREHOUSE_USER}" \
  --from-literal=PG_WAREHOUSE_PASSWORD="${PG_WAREHOUSE_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

# DDS service secrets
kubectl create secret generic dds-service-secrets \
  --from-literal=KAFKA_HOST="${KAFKA_HOST}" \
  --from-literal=KAFKA_CONSUMER_USERNAME="${KAFKA_CONSUMER_USERNAME}" \
  --from-literal=KAFKA_CONSUMER_PASSWORD="${KAFKA_CONSUMER_PASSWORD}" \
  --from-literal=PG_WAREHOUSE_HOST="${PG_WAREHOUSE_HOST}" \
  --from-literal=PG_WAREHOUSE_DBNAME="${PG_WAREHOUSE_DBNAME}" \
  --from-literal=PG_WAREHOUSE_USER="${PG_WAREHOUSE_USER}" \
  --from-literal=PG_WAREHOUSE_PASSWORD="${PG_WAREHOUSE_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

# CDM service secrets
kubectl create secret generic cdm-service-secrets \
  --from-literal=KAFKA_HOST="${KAFKA_HOST}" \
  --from-literal=KAFKA_CONSUMER_USERNAME="${KAFKA_CONSUMER_USERNAME}" \
  --from-literal=KAFKA_CONSUMER_PASSWORD="${KAFKA_CONSUMER_PASSWORD}" \
  --from-literal=PG_WAREHOUSE_HOST="${PG_WAREHOUSE_HOST}" \
  --from-literal=PG_WAREHOUSE_DBNAME="${PG_WAREHOUSE_DBNAME}" \
  --from-literal=PG_WAREHOUSE_USER="${PG_WAREHOUSE_USER}" \
  --from-literal=PG_WAREHOUSE_PASSWORD="${PG_WAREHOUSE_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo ""
echo "=== Step 6: Deploy with Helm ==="
cd "${SCRIPT_DIR}"

helm upgrade --install stg-service service_stg/helm/stg-service \
  --set image.tag=${VERSION}

helm upgrade --install dds-service service_dds/helm/dds-service \
  --set image.tag=${VERSION}

helm upgrade --install cdm-service service_cdm/helm/cdm-service \
  --set image.tag=${VERSION}

echo ""
echo "=== Step 7: Verify deployment ==="
kubectl get pods -l app=stg-service
kubectl get pods -l app=dds-service
kubectl get pods -l app=cdm-service

echo ""
echo "=== Done! ==="
echo "STG: ${REGISTRY_URL}/stg-service:${VERSION}"
echo "DDS: ${REGISTRY_URL}/dds-service:${VERSION}"
echo "CDM: ${REGISTRY_URL}/cdm-service:${VERSION}"
echo ""
echo "Check logs with:"
echo "  kubectl logs -l app=stg-service --tail=50"
echo "  kubectl logs -l app=dds-service --tail=50"
echo "  kubectl logs -l app=cdm-service --tail=50"
