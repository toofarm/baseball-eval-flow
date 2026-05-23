#!/usr/bin/env bash
# Daily backup of the Airflow Postgres metadata DB to S3-compatible object storage
# (DigitalOcean Spaces by default). Intended to run from cron on the production droplet.
#
# What this backs up:
#   - The `airflow` Postgres database (DAG history, XCom, variables, connections, FAB users).
#
# What this does NOT back up (and why):
#   - `.env` and `rsa_key.p8` — held in the operator's password manager out-of-band.
#   - Snowflake data — covered by Snowflake's Time Travel + Fail-Safe.
#   - `airflow-logs` volume — task logs older than what's in Postgres are not load-bearing.
#
# Restoring:
#   aws --endpoint-url "$S3_ENDPOINT" s3 cp "$BUCKET/postgres/<file>.dump" - \
#     | docker compose ... exec -T postgres pg_restore -U airflow -d airflow --clean --if-exists
#   Then restart the Airflow services so they pick up the restored metadata.
#
# Prerequisites on the droplet:
#   - `awscli` (or `s3cmd`) installed and on PATH for the cron user.
#   - Credentials configured in /root/.aws/credentials (or via env vars) for a Spaces access key
#     with write access to BACKUP_BUCKET only. Use a least-privilege key, not the account-wide one.
#   - A lifecycle rule on the bucket to expire objects older than BACKUP_RETENTION_DAYS
#     (set via the Spaces UI or `aws s3api put-bucket-lifecycle-configuration`).

set -euo pipefail

# ----- Config (override via environment) -----------------------------------
PROJECT_DIR="${PROJECT_DIR:-/home/common/opt/baseball-eval-flow}"
COMPOSE_FILES=(
    "-f" "${PROJECT_DIR}/docker-compose.yaml"
    "-f" "${PROJECT_DIR}/docker-compose.prod.yaml"
)
POSTGRES_USER="${POSTGRES_USER:-airflow}"
POSTGRES_DB="${POSTGRES_DB:-airflow}"

# S3-compatible destination. Defaults are placeholders — set these in cron's environment
# or in /etc/baseball-eval-backup.env before scheduling.
BACKUP_BUCKET="${BACKUP_BUCKET:?set BACKUP_BUCKET, e.g. s3://baseball-eval-backups}"
S3_ENDPOINT="${S3_ENDPOINT:?set S3_ENDPOINT, e.g. https://sfo3.digitaloceanspaces.com}"
BACKUP_RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-30}"

# ----- Run -----------------------------------------------------------------
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OBJECT_KEY="postgres/airflow-${STAMP}.dump"
LOG_PREFIX="[backup.sh ${STAMP}]"

echo "${LOG_PREFIX} starting"

# Stream pg_dump straight into S3 — no intermediate file on disk. `--format=custom`
# produces a compressed binary archive that `pg_restore` can selectively replay.
docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
    pg_dump -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" --format=custom \
    | aws --endpoint-url "${S3_ENDPOINT}" s3 cp - "${BACKUP_BUCKET}/${OBJECT_KEY}"

# Sanity: confirm the object landed and report its size.
SIZE_BYTES="$(aws --endpoint-url "${S3_ENDPOINT}" s3 ls "${BACKUP_BUCKET}/${OBJECT_KEY}" \
    | awk '{print $3}')"

if [[ -z "${SIZE_BYTES}" || "${SIZE_BYTES}" == "0" ]]; then
    echo "${LOG_PREFIX} ERROR upload missing or empty at ${BACKUP_BUCKET}/${OBJECT_KEY}" >&2
    exit 1
fi

echo "${LOG_PREFIX} uploaded ${OBJECT_KEY} (${SIZE_BYTES} bytes)"
echo "${LOG_PREFIX} done (lifecycle policy on bucket handles ${BACKUP_RETENTION_DAYS}-day retention)"
