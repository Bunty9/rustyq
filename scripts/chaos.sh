#!/usr/bin/env bash
# Chaos test: SIGKILL half the workers mid-drain, verify zero job loss.
#
#   scripts/chaos.sh [JOBS] [WORKERS]      # default: 20000 jobs, 4 workers
#   KILL=0 scripts/chaos.sh 20000 4        # no kill: plain drain benchmark
#
# 1. Boots the compose stack with workers scaled to 0 and a short lock
#    timeout, bulk-inserts JOBS `sleep` jobs (MS ms each) straight into
#    Postgres.
# 2. Scales to WORKERS, waits until ~25% are done, then `docker kill -s KILL`
#    half the worker containers (no graceful shutdown: their in-flight rows
#    stay `running` with a lock nobody will release).
# 3. Restarts the killed replicas and waits for the queue to drain.
# 4. Asserts every job is `done`: none lost, none stuck in `running`
#    (the reaper recovered them), none `dead`.
#
# Exit code 0 = invariant held. Leaves the stack up for inspection; tear down
# with `docker compose down -v`.
set -euo pipefail

JOBS="${1:-20000}"
WORKERS="${2:-4}"
KILL="${KILL:-$((WORKERS / 2))}"
MS="${MS:-5}"   # per-job sleep; MS=0 for a pure-overhead drain benchmark
TIMEOUT_SECS="${TIMEOUT_SECS:-600}"
COMPOSE="${COMPOSE:-docker compose}"
# Short lock timeout so the reaper recovers killed workers' jobs quickly.
export RUSTYQ_LOCK_TIMEOUT_SECS="${RUSTYQ_LOCK_TIMEOUT_SECS:-10}"

cd "$(git rev-parse --show-toplevel)"

psql() { $COMPOSE exec -T postgres psql -U rustyq -d rustyq -tAq "$@"; }
count() { psql -c "SELECT count(*) FROM jobs WHERE state IN ($1)"; }

echo "==> booting stack (workers scaled to 0, lock timeout ${RUSTYQ_LOCK_TIMEOUT_SECS}s)"
$COMPOSE up -d --build --wait --scale rustyq-worker=0

echo "==> inserting $JOBS jobs"
psql -c "TRUNCATE jobs"
psql -c "INSERT INTO jobs (id, queue, kind, payload, state)
         SELECT gen_random_uuid(), 'default', 'sleep', '{\"ms\": $MS}', 'queued'
         FROM generate_series(1, $JOBS)"

echo "==> starting $WORKERS workers"
$COMPOSE up -d --scale rustyq-worker="$WORKERS" --no-recreate rustyq-worker

# Clock starts at the first finished job, excluding container boot — same
# basis as bench/celery/run.sh.
until [ "$(count "'done'")" -gt 0 ]; do sleep 0.1; done
start=$(date +%s.%N)

if [ "$KILL" -gt 0 ]; then
  until [ "$(count "'done'")" -ge $((JOBS / 4)) ]; do sleep 0.2; done

  mapfile -t victims < <($COMPOSE ps -q rustyq-worker | shuf -n "$KILL")
  echo "==> SIGKILL $KILL workers at $(count "'done'")/$JOBS done, $(count "'running'") running"
  docker kill -s KILL "${victims[@]}" >/dev/null

  sleep 2
  echo "==> restarting killed workers"
  $COMPOSE up -d --scale rustyq-worker="$WORKERS" --no-recreate rustyq-worker
fi

mem=""
deadline=$(( $(date +%s) + TIMEOUT_SECS ))
while [ "$(count "'queued','running'")" -gt 0 ]; do
  if [ "$(date +%s)" -gt "$deadline" ]; then
    echo "FAIL: queue not drained after ${TIMEOUT_SECS}s"
    psql -c "SELECT state, count(*) FROM jobs GROUP BY state"
    exit 1
  fi
  if [ -z "$mem" ] && [ "$(count "'done'")" -ge $((JOBS / 2)) ]; then
    mem=$(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep rustyq-worker || true)
  fi
  sleep 0.2
done
end=$(date +%s.%N)

total=$(psql -c "SELECT count(*) FROM jobs")
done_=$(count "'done'")
dead=$(count "'dead'")
rerun=$(psql -c "SELECT count(*) FROM jobs WHERE attempts > 1")
awk -v n="$JOBS" -v w="$WORKERS" -v s="$start" -v e="$end" -v k="$KILL" \
  'BEGIN { t = e - s; printf "==> drain: %d jobs, %d workers (%d killed): %.2fs = %.0f jobs/s\n", n, w, k, t, n / t }'
echo "==> total=$total done=$done_ dead=$dead re-run(attempts>1)=$rerun"
echo "==> worker memory at ~50%:"
echo "${mem:-  (not sampled)}"

if [ "$total" -eq "$JOBS" ] && [ "$done_" -eq "$JOBS" ] && [ "$dead" -eq 0 ]; then
  echo "PASS: zero job loss"
else
  echo "FAIL: expected $JOBS done, 0 dead"
  exit 1
fi
