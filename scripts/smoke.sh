#!/usr/bin/env bash
# Phase 1.1 scaffold smoke: enqueue a job via HTTP, watch worker finalize it.
# Assumes `docker compose up -d` has been run and services are healthy.
set -euo pipefail

SERVER="${SERVER:-http://localhost:8080}"
DB_URL="${DATABASE_URL:-postgres://rustyq:rustyq@localhost:5432/rustyq}"

echo "==> POST /jobs"
RESP=$(curl -sS -X POST "$SERVER/jobs" \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","kind":"noop","payload":{"hello":"world"}}')
echo "    response: $RESP"

JOB_ID=$(echo "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')
echo "    job id: $JOB_ID"

echo "==> wait for worker to finalize (state=done)"
for i in $(seq 1 20); do
  STATE=$(docker compose exec -T postgres psql -U rustyq -d rustyq -tA \
    -c "SELECT state FROM rustyq_jobs WHERE id='$JOB_ID'" | tr -d '[:space:]')
  echo "    [$i] state=$STATE"
  if [ "$STATE" = "done" ]; then
    echo "==> SUCCESS"
    exit 0
  fi
  sleep 0.5
done

echo "==> FAILED: job never reached state=done"
docker compose exec -T postgres psql -U rustyq -d rustyq \
  -c "SELECT id,state,attempts,locked_by,last_error FROM rustyq_jobs WHERE id='$JOB_ID'"
exit 1
