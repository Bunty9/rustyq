#!/usr/bin/env bash
# Local throughput benchmark. Brings up the compose stack (if needed),
# enqueues N jobs in parallel, samples /metrics until the finished counter
# stops growing, and prints a drain-rate summary.
#
# Usage:
#   bench/local/throughput.sh [N] [CONCURRENCY]
#   bench/local/throughput.sh 1000 32     # default: 1000 jobs, 32 concurrent POSTs
#
# Required: docker compose stack already built (`docker compose build`).
# This script is a smoke / sanity baseline — Phase 4 will replace it with a
# Criterion harness that controls the harness process and avoids HTTP/curl
# overhead in the measurement.
set -euo pipefail

N="${1:-1000}"
CONCURRENCY="${2:-32}"
SERVER="${SERVER:-http://localhost:8080}"
COMPOSE="${COMPOSE:-docker compose}"
KIND="${KIND:-noop}"
QUEUE="${QUEUE:-default}"

cd "$(git rev-parse --show-toplevel)"

bring_up=0
if ! $COMPOSE ps --status running --format '{{.Name}}' | grep -q rustyq-server; then
  echo "==> bringing up compose stack"
  $COMPOSE up -d --wait
  bring_up=1
fi

# Wait until /metrics is reachable.
echo "==> waiting for $SERVER/metrics"
for i in $(seq 1 30); do
  if curl -fsS -m 1 "$SERVER/metrics" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS -m 2 "$SERVER/metrics" >/dev/null || { echo "server unreachable"; exit 1; }

# Helper: read a single counter value from /metrics (sum across label sets).
counter() {
  local name="$1"
  curl -fsS "$SERVER/metrics" \
    | awk -v n="$name" '
        $0 !~ /^#/ && $1 ~ "^"n {
          v=$NF; gsub(/[^0-9.\-]/, "", v); sum += v
        }
        END { printf("%.0f\n", sum+0) }
      '
}

before_enq=$(counter rustyq_jobs_enqueued_total)
before_fin=$(counter rustyq_jobs_finished_total)
echo "==> baseline enqueued=$before_enq finished=$before_fin"

payload='{"queue":"'"$QUEUE"'","kind":"'"$KIND"'","payload":{}}'

echo "==> enqueueing $N jobs ($CONCURRENCY concurrent)"
enq_start=$(date +%s.%N)
seq "$N" | xargs -P "$CONCURRENCY" -I {} \
  curl -fsS -m 5 -o /dev/null -X POST "$SERVER/jobs" \
       -H 'content-type: application/json' \
       -d "$payload"
enq_end=$(date +%s.%N)
enq_dur=$(awk -v a="$enq_start" -v b="$enq_end" 'BEGIN{printf "%.3f", b-a}')
echo "==> enqueue done in ${enq_dur}s"

target=$((before_fin + N))
echo "==> waiting for finished counter to reach $target"
poll_start=$(date +%s.%N)
last_fin=$before_fin
stable_iters=0
for i in $(seq 1 600); do
  cur=$(counter rustyq_jobs_finished_total)
  if [ "$cur" -ge "$target" ]; then
    poll_end=$(date +%s.%N)
    drain_dur=$(awk -v a="$enq_end" -v b="$poll_end" 'BEGIN{printf "%.3f", b-a}')
    rate=$(awk -v n="$N" -v d="$drain_dur" 'BEGIN{printf "%.1f", n/d}')
    echo
    echo "==> SUMMARY"
    printf "    %-25s %s\n" "jobs enqueued"     "$N"
    printf "    %-25s %s\n" "enqueue concurrency" "$CONCURRENCY"
    printf "    %-25s %ss\n" "enqueue wall time" "$enq_dur"
    printf "    %-25s %ss\n" "drain wall time"   "$drain_dur"
    printf "    %-25s %s jobs/s\n" "drain throughput" "$rate"
    printf "    %-25s %s\n" "finished_total delta" "$((cur - before_fin))"
    if [ "$bring_up" -eq 1 ]; then
      echo
      echo "==> $COMPOSE down -v  # (compose was started by this script)"
      $COMPOSE down -v >/dev/null
    fi
    exit 0
  fi
  if [ "$cur" = "$last_fin" ]; then
    stable_iters=$((stable_iters + 1))
    if [ "$stable_iters" -ge 20 ]; then
      echo "FAILED: counter stalled at $cur (target $target)"
      exit 2
    fi
  else
    stable_iters=0
    last_fin=$cur
  fi
  sleep 0.25
done
echo "FAILED: timed out before reaching $target finished jobs"
exit 1
