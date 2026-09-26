#!/usr/bin/env bash
# Celery drain baseline: pre-enqueue N noop tasks with no worker running,
# start WORKERS x 16-process prefork workers, time the drain.
#
#   ./run.sh [N] [WORKERS]      # default: 20000 tasks, 4 workers
#
# The clock starts at the first completed task, not at `docker compose up`,
# so container/prefork boot time is excluded — the same steady-state basis
# as rustyq's in-process bench (crates/core/benches/drain.rs).
set -euo pipefail

N="${1:-20000}"
WORKERS="${2:-4}"
cd "$(dirname "${BASH_SOURCE[0]}")"

done_count() { docker compose exec -T redis redis-cli -n 1 GET bench:done | tr -dc 0-9; }

docker compose up -d --build --wait redis tools
docker compose up -d --scale worker=0
docker compose exec -T tools python enqueue.py "$N"

docker compose up -d --scale worker="$WORKERS" worker
deadline=$(( $(date +%s) + 900 ))
until [ "$(done_count)" -gt 0 ]; do sleep 0.1; done
start=$(date +%s.%N)
mem=""
while [ "$(done_count)" -lt "$N" ]; do
  if [ -z "$mem" ] && [ "$(done_count)" -ge $((N / 2)) ]; then
    mem=$(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep worker || true)
  fi
  [ "$(date +%s)" -gt "$deadline" ] && { echo "FAIL: timeout at $(done_count)/$N"; exit 1; }
  sleep 0.2
done
end=$(date +%s.%N)

awk -v n="$N" -v w="$WORKERS" -v s="$start" -v e="$end" \
  'BEGIN { t = e - s; printf "celery drain: %d jobs, %d workers x 16 prefork: %.2fs = %.0f jobs/s\n", n, w, t, n / t }'
echo "worker memory at ~50%:"
echo "${mem:-  (not sampled)}"
echo "teardown: docker compose -p rustyq-celery-bench down -v"
