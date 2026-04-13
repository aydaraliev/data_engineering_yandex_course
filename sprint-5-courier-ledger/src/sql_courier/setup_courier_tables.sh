#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${ENV_FILE:-$REPO_ROOT/.env.local}"
CONTAINER="${CONTAINER:-de-pg-cr-af}"

# Load credentials from .env.local if present (not tracked by git)
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-jovyan}"
PGPASSWORD="${PGPASSWORD:-jovyan}"
PGDATABASE="${PGDATABASE:-de}"

if [[ ! -d "$SQL_DIR" ]]; then
  echo "SQL directory '$SQL_DIR' not found." >&2
  exit 1
fi

export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

echo "Applying courier SQL scripts inside container '${CONTAINER}' to ${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}..."

shopt -s nullglob
for sql_file in "$SQL_DIR"/*.sql; do
  echo "Running $sql_file"
  docker exec -i "$CONTAINER" psql -v ON_ERROR_STOP=1 -f - <<EOF
$(cat "$sql_file")
EOF
done
shopt -u nullglob

echo "Done."
