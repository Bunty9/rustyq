#!/usr/bin/env bash
# End-to-end proof for the order-pipeline example. Used by CI.
#
#   examples/order-pipeline/demo.sh                 # starts its own Postgres in Docker
#   DATABASE_URL=postgres://u:p@host:5432/postgres examples/order-pipeline/demo.sh
#   PYTHON_CLIENT=1 examples/order-pipeline/demo.sh # also run producer.py (needs `rustyq` wheel)
#
# Env: DATABASE_URL (server URL; the demo uses its own database `order_pipeline`),
#      DATABASE_URL must end in a database name; anything after the last '/' (incl. ?params) is replaced.
#      PG_CONTAINER (container with psql, used when host psql is missing),
#      APP_PORT (3000), METRICS_PORT (9464), KEEP_DB (keep container we started).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export PATH="$HOME/.cargo/bin:$PATH"

APP_PORT="${APP_PORT:-3000}"
METRICS_PORT="${METRICS_PORT:-9464}"
API="http://127.0.0.1:${APP_PORT}"
WORKER_METRICS="http://127.0.0.1:${METRICS_PORT}/metrics"
PG_CONTAINER="${PG_CONTAINER:-rustyq-example-pg}"
LOGDIR="$ROOT/target/order-pipeline-demo"
BIN="${CARGO_TARGET_DIR:-$ROOT/target}/debug"
mkdir -p "$LOGDIR"

STARTED_CONTAINER=0
API_PID=""
WORKER_PID=""

cleanup() {
  for pid in $API_PID $WORKER_PID; do
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
  done
  if [ "$STARTED_CONTAINER" = 1 ] && [ -z "${KEEP_DB:-}" ]; then
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# ---- 1. database ----------------------------------------------------------
if [ -z "${DATABASE_URL:-}" ]; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$PG_CONTAINER"; then
    docker start "$PG_CONTAINER" >/dev/null   # exists but stopped (no-op if running)
  else
    echo "starting Postgres container $PG_CONTAINER on :55433"
    docker run -d --name "$PG_CONTAINER" -p 55433:5432 -e POSTGRES_PASSWORD=postgres \
      postgres:16-alpine >/dev/null
    STARTED_CONTAINER=1
  fi
  DATABASE_URL="postgres://postgres:postgres@127.0.0.1:55433/postgres"
fi
SERVER_URL="${DATABASE_URL%/*}"            # strip the database name

# sql <db> <psql args...>: host psql if present, else psql inside the container.
sql() {
  local db="$1"; shift
  if command -v psql >/dev/null 2>&1; then
    psql "$SERVER_URL/$db" -v ON_ERROR_STOP=1 -qtAX "$@"
  else
    docker exec -i "$PG_CONTAINER" psql -U postgres -d "$db" -v ON_ERROR_STOP=1 -qtAX "$@"
  fi
}
q() { sql order_pipeline -c "$1"; }          # scalar query against the demo database

for _ in $(seq 60); do sql postgres -c 'select 1' >/dev/null 2>&1 && break; sleep 1; done
sql postgres -c 'select 1' >/dev/null

# Clean slate every run: this is what makes the demo repeatable.
sql postgres -c 'DROP DATABASE IF EXISTS order_pipeline WITH (FORCE)'
sql postgres -c 'CREATE DATABASE order_pipeline'
export DATABASE_URL="$SERVER_URL/order_pipeline"

# ---- 2. build + start api and worker ---------------------------------------
cargo build -q -p order-pipeline --bins

APP_BIND="127.0.0.1:${APP_PORT}" "$BIN/api" >"$LOGDIR/api.log" 2>&1 &
API_PID=$!
RUSTYQ_METRICS_BIND="127.0.0.1:${METRICS_PORT}" "$BIN/worker" >"$LOGDIR/worker.log" 2>&1 &
WORKER_PID=$!

for _ in $(seq 120); do
  curl -fs "$API/queue/healthz" >/dev/null 2>&1 && break
  kill -0 "$API_PID" 2>/dev/null || { echo "api died:"; tail -n 30 "$LOGDIR/api.log"; exit 1; }
  kill -0 "$WORKER_PID" 2>/dev/null || { echo "worker died:"; tail -n 30 "$LOGDIR/worker.log"; exit 1; }
  sleep 0.5
done
curl -fs "$API/queue/healthz" >/dev/null

# ---- results bookkeeping ---------------------------------------------------
RESULTS=()
FAILED=0
record() {   # record <name> <ok: 0|1>
  if [ "$2" = 1 ]; then RESULTS+=("PASS  $1"); else RESULTS+=("FAIL  $1"); FAILED=1; fi
}
check_eq() { # check_eq <name> <actual> <expected>
  if [ "$2" = "$3" ]; then record "$1" 1; else record "$1 (got '$2', want '$3')" 0; fi
}

# ---- 3. create orders -------------------------------------------------------
ORDERS=()
for i in 1 2 3; do
  resp=$(curl -fs -X POST "$API/orders" -H 'content-type: application/json' \
    -d "{\"email\":\"user$i@example.com\",\"amount_cents\":$((i * 1000))}")
  ORDERS+=("$(echo "$resp" | sed -E 's/.*"order_id":"([^"]+)".*/\1/')")
done
echo "orders: ${ORDERS[*]}"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/orders" \
  -H 'content-type: application/json' -d '{"email":"nope","amount_cents":5}')
check_eq "invalid order rejected with 400" "$code" 400

# ---- 4. Rust producer -------------------------------------------------------
if cargo run -q -p order-pipeline --bin producer -- --url "$API/queue"; then
  record "rust producer: delayed report.daily done after its 2s delay" 1
else
  record "rust producer: delayed report.daily done after its 2s delay" 0
fi

# ---- 5. Python producer -----------------------------------------------------
if [ "${PYTHON_CLIENT:-}" = 1 ]; then
  if RUSTYQ_URL="$API/queue" python3 examples/order-pipeline/producer.py; then
    record "python producer: expectations held" 1
  else
    record "python producer: expectations held" 0
  fi
else
  echo "skipping producer.py: install the wheel (maturin develop -m crates/pybind/Cargo.toml) and set PYTHON_CLIENT=1"
fi

# A malformed fraud.review straight over the HTTP API: always covers permanent().
FRAUD_ID=$(curl -fs -X POST "$API/queue/jobs" -H 'content-type: application/json' \
  -d '{"queue":"default","kind":"fraud.review","payload":{"order_id":"not-a-uuid"},"max_attempts":3}' \
  | sed -E 's/.*"id":"([^"]+)".*/\1/')

# ---- 6. wait for the pipeline to drain -------------------------------------
# Paid orders AND no job left queued/running (the done-path is batched, so job
# rows can lag the order update by a moment).
settled=0
for _ in $(seq 120); do
  paid=$(q "SELECT count(*) FROM orders WHERE status='paid'")
  open=$(q "SELECT count(*) FROM jobs WHERE state NOT IN ('done','dead')")
  if [ "$paid" = 3 ] && [ "$open" = 0 ]; then settled=1; break; fi
  sleep 0.5
done
record "all 3 orders paid and all jobs terminal within 60s" "$settled"

# GET /orders/{id}: 200 and both jobs listed.
body=$(curl -fs "$API/orders/${ORDERS[0]}" || true)
check_eq "GET /orders/{id}: order with its 2 jobs" \
  "$(grep -o '"max_attempts"' <<<"$body" | wc -l | tr -d ' ')" 2
# ---- 7. SQL assertions ------------------------------------------------------
check_eq "sent_emails: 3 rows"                      "$(q "SELECT count(*) FROM sent_emails")" 3
check_eq "sent_emails: every order exactly once"    "$(q "SELECT count(*) FROM orders o WHERE (SELECT count(*) FROM sent_emails s WHERE s.order_id=o.id)=1")" 3
check_eq "charges: 3 rows"                          "$(q "SELECT count(*) FROM charges")" 3
check_eq "orders: 3 paid"                           "$(q "SELECT count(*) FROM orders WHERE status='paid'")" 3
check_eq "payment.charge: done after 2 attempts, gateway timeout recorded" \
  "$(q "SELECT count(*) FROM jobs WHERE kind='payment.charge' AND state='done' AND attempts=2 AND last_error LIKE '%gateway timeout%'")" 3
check_eq "email.order_confirmation: done on attempt 1" \
  "$(q "SELECT count(*) FROM jobs WHERE kind='email.order_confirmation' AND state='done' AND attempts=1")" 3
check_eq "report.daily: a daily_reports row exists" \
  "$(q "SELECT (count(*) > 0)::int FROM daily_reports")" 1
check_eq "report.daily: scheduled with a 2s delay (run_at > created_at)" \
  "$(q "SELECT (count(*) > 0)::int FROM jobs WHERE kind='report.daily' AND state='done' AND run_at >= created_at + interval '1.9 seconds'")" 1
check_eq "fraud.review (curl): dead on attempt 1 with last_error" \
  "$(q "SELECT count(*) FROM jobs WHERE id='$FRAUD_ID' AND state='dead' AND attempts=1 AND last_error LIKE '%bad fraud payload%'")" 1
if [ "${PYTHON_CLIENT:-}" = 1 ]; then
  check_eq "fraud.review (python + curl): 2 dead jobs, each attempts=1" \
    "$(q "SELECT count(*) FROM jobs WHERE kind='fraud.review' AND state='dead' AND attempts=1")" 2
fi

# ---- 8. metrics -------------------------------------------------------------
wm=$(curl -fs "$WORKER_METRICS" || true)
am=$(curl -fs "$API/queue/metrics" || true)
has() { if grep -Eq "$2" <<<"$1"; then echo 1; else echo 0; fi; }
check_eq "worker metrics: rustyq_jobs_finished_total{state=done} > 0" "$(has "$wm" '^rustyq_jobs_finished_total\{state="done"\} [1-9]')" 1
check_eq "worker metrics: rustyq_jobs_claimed_total present"         "$(has "$wm" '^rustyq_jobs_claimed_total\{')" 1
check_eq "api metrics: rustyq_jobs_enqueued_total present"           "$(has "$am" '^rustyq_jobs_enqueued_total\{')" 1

# ---- 9. graceful shutdown ---------------------------------------------------
kill -TERM "$WORKER_PID" "$API_PID" || true
wrc=0; arc=0
wait "$WORKER_PID" || wrc=$?
wait "$API_PID" || arc=$?
check_eq "worker exited 0 on SIGTERM" "$wrc" 0
check_eq "api exited 0 on SIGTERM"    "$arc" 0
check_eq "no job left running"        "$(q "SELECT count(*) FROM jobs WHERE state='running'")" 0

# ---- 10. summary ------------------------------------------------------------
echo
echo "================ order-pipeline demo ================"
printf '%s\n' "${RESULTS[@]}"
echo "====================================================="
if [ "$FAILED" = 0 ]; then echo "ALL PASS"; else echo "SOME CHECKS FAILED (logs: $LOGDIR)"; exit 1; fi
