---
title: rustyq Bench Write-up — 2026-09-26 measurement session
status: final (numbers), draft (re-run pending on quiet hardware)
date: 2026-09-26
related:
    - ../plans/2026-06-02-rustyq-execution-plan.md
    - ../../PROGRESS.md
    - ../../README.md
---

# rustyq bench write-up — 2026-09-26

This is the methodology and full numbers behind the Benchmarks section of
`README.md` and the Bench numbers table in `PROGRESS.md`. Every number here
was measured on 2026-09-26, except Celery-comparison runs 2 and 3
(2026-09-28). None of it is invented or
extrapolated — where something was not measured, it says so.

## Environment

- Host: 8 vCPU / 39 GB RAM Linux laptop.
- Heavily shared during most runs: host load average 25–50 from unrelated
  builds running concurrently (this is a personal dev box, not a dedicated
  bench rig).
- Postgres 16 (`postgres:16-alpine`) running inside Docker Desktop's Linux
  VM (qemu), not natively on the host.
- Raw ceiling on this box, measured directly against the same Postgres
  container:
  ```bash
  docker compose exec postgres pgbench -i -s 10 -U rustyq rustyq
  docker compose exec postgres pgbench -N -c16 -T10 -U rustyq rustyq
  ```
  Result: **438 tps** with default `synchronous_commit=on`, **1,239 tps**
  with `synchronous_commit=off`. Every absolute jobs/s number in this
  document is bounded above by roughly this ceiling — a fair reading of
  the numbers below treats them as *relative* evidence (did a fix help)
  rather than *absolute* capacity claims. Even the rustyq-vs-Celery
  comparison on the identical box turned out too noisy for a ratio (§5).

Because of this, the design targets from `docs/plans/2026-06-02-rustyq-execution-plan.md`
Phase 4 (>= 5,000 jobs/s drain, p99 dispatch < 50 ms) are evaluated here but
**not treated as met or missed for real** — they need a re-run on quiet,
dedicated hardware. See "What to re-run" at the end.

## 1. The claim-query index bug

### Symptom

Early drain runs plateaued far below expectations and got *worse* as the
queue grew — a classic O(n²) signature.

### Root cause

The original dispatch index (`migrations/0001_init.sql`) was:

```sql
CREATE INDEX idx_jobs_dispatch ON jobs (queue, state, priority DESC, run_at)
  WHERE state = 'queued';
```

but the claim query (`Worker::claim_batch`, `crates/core/src/lib.rs`) filters
with:

```sql
WHERE state='queued' AND run_at <= now() AND queue = ANY($2)
ORDER BY priority DESC, run_at
FOR UPDATE SKIP LOCKED
LIMIT $3
```

`queue = ANY($2)` (an array parameter, since a worker can drain multiple
queues) cannot be satisfied as an index-leading equality condition the way a
single `queue = $n` could — Postgres's planner fell back to a sequential
scan of every `queued` row, followed by a full in-memory sort to satisfy
`ORDER BY priority DESC, run_at`, on *every single claim*. With the queue
backlog large, each claim got slower as more jobs piled up.

Measured before the fix, `EXPLAIN (ANALYZE)` on the claim query at 20,000
queued rows: **82 ms per claim**. Overall drain: **129 jobs/s**.

### Fix

`migrations/0002_dispatch_index.sql`:

```sql
DROP INDEX IF EXISTS idx_jobs_dispatch;
CREATE INDEX idx_jobs_dispatch ON jobs (priority DESC, run_at)
  WHERE state = 'queued';
```

Dropping `queue` from the index and leading with the `ORDER BY` columns
instead lets the planner walk the index in the exact order the query wants
and stop at `LIMIT`, filtering `queue = ANY($2)` as a cheap row-level check
along the way. Measured after the fix: **0.8 ms per claim**, drain
**304 jobs/s**.

This trades away one thing deliberately (documented as a `ponytail:` comment
in the migration file): a worker draining a rare queue behind a much larger
unrelated queue now scans past foreign-queue rows instead of skipping them
via an index prefix. Add a per-queue index back if that workload shows up.

### Reproduce

```bash
eval "$(./scripts/test-pg.sh up | tail -1)"   # exports TEST_DATABASE_URL
BENCH_DATABASE_URL=$TEST_DATABASE_URL BENCH_JOBS=20000 BENCH_WORKERS=4 \
  BENCH_CONCURRENCY=16 BENCH_LATENCY_JOBS=0 \
  cargo bench -p rustyq-core --bench drain
```

This runs current code (both migrations, batched finalize), so it
reproduces the latest row of §3, not the 129 / 304 jobs/s steps. To see
the `EXPLAIN` difference directly, apply only `0001_init.sql`, insert
20k queued rows, and run `EXPLAIN ANALYZE` on the claim `UPDATE` by hand;
then apply `0002_dispatch_index.sql` and repeat.

## 2. Batched success finalize

### Problem

Even after the index fix, every successfully completed job did its own
`UPDATE jobs SET state='done' ... WHERE id=$1 ...` — one network round trip
plus one `fdatasync`'d commit per job. At high concurrency this dominates:
Postgres inside a Docker Desktop VM pays real fsync latency per commit.

### Fix

`crates/core/src/lib.rs`: a bounded channel (`DONE_BATCH_MAX = 512`)
collects `(job.id, job.attempts)` from every successful job task. A single
finalizer task drains the channel with `recv_many` and issues one statement
per batch:

```sql
UPDATE jobs SET state='done', locked_at=NULL
FROM UNNEST($2::uuid[], $3::int4[]) AS f(id, attempts)
WHERE jobs.id=f.id AND jobs.attempts=f.attempts
  AND jobs.state='running' AND jobs.locked_by=$1
```

fenced the same way as the single-job `finalize` path (see §6). Under load this turns N commits into `ceil(N/512)` commits;
when idle, a "batch" is just one job, so no latency is added to a lone
finalize.

Measured after this fix (same 20k-job, 4×16 setup, index fix already
applied): **798 jobs/s**.

### Reproduce

Same bench command as above; the fix is unconditional in the current code,
so this number is simply "current `main`, 4 workers × 16 concurrency."

## 3. Drain throughput progression

All runs: `cargo bench -p rustyq-core --bench drain`, 20,000 `noop` jobs,
Postgres in Docker, same host/load conditions described above (load average
25–50 throughout).

| Configuration                                                      | Throughput  | Claim latency |
|----------------------------------------------------------------------|-------------|---------------|
| Before fix (seq scan + full sort per claim, `0001_init.sql` index only) | 129 jobs/s  | 82 ms/claim   |
| + dispatch index (`migrations/0002_dispatch_index.sql`)              | 304 jobs/s  | 0.8 ms/claim  |
| + batched success finalize (`finalize_done_batch`)                   | 798 jobs/s  | —             |
| 4 workers × 64 concurrency (index + batching both in place)          | 1,579 jobs/s | —            |

```bash
# reproduce the last row:
BENCH_DATABASE_URL=$TEST_DATABASE_URL BENCH_JOBS=20000 BENCH_WORKERS=4 \
  BENCH_CONCURRENCY=64 BENCH_LATENCY_JOBS=0 \
  cargo bench -p rustyq-core --bench drain
```

## 4. Dispatch latency (enqueue -> handler start)

Same bench binary's second phase: with workers idle on `LISTEN`, enqueue
jobs one at a time via `INSERT + pg_notify` in a single statement (matching
what the HTTP server does), at a steady trickle (sequential enqueues with a
2 ms pause, i.e. at most ~500/s — a deliberate contrast with the drain phase's burst load), and record
`created_at -> handler start` for each.

Measured (default `BENCH_LATENCY_JOBS=2000`, or 500 in the CI smoke job):
**p50 61–89 ms, p99 188–917 ms, on this loaded host.** For scale,
`pgbench`'s average latency on the same Postgres container during this
session was **36 ms** — dispatch latency here is most likely dominated by
fsync'd commits on a contended host rather than rustyq's own dispatch
logic, though the two were not measured separately.

**The design target of p99 < 50 ms was not met on this host.** This needs
re-measurement on quiet hardware — see the closing section.

```bash
BENCH_DATABASE_URL=$TEST_DATABASE_URL BENCH_JOBS=0 BENCH_WORKERS=4 \
  BENCH_CONCURRENCY=16 BENCH_LATENCY_JOBS=2000 \
  cargo bench -p rustyq-core --bench drain
```

(`BENCH_JOBS=0` skips the drain phase and goes straight to latency
measurement; the bench binary supports this via `env_or`.)

## 5. rustyq vs. Celery, in-Docker comparison

### Setup

Both queues ran in the *same* Docker Desktop VM, same host, 20,000 trivial
jobs, 4 workers × 16 concurrency (rustyq: `RUSTYQ_CONCURRENCY=16`; Celery:
`--concurrency=16 --pool=prefork`). Both use the "clock starts at first
completed job" convention (excludes container/prefork boot time), enforced
identically in `scripts/chaos.sh` and `bench/celery/run.sh`.

- **Celery**: 5.4, Redis broker (`bench/celery/tasks.py`,
  `worker_prefetch_multiplier=16`, `task_acks_late=True`), no result
  backend (`task_ignore_result=True`) — a completion counter is a plain
  Redis `INCR` from the task body, not a Celery result.
- **rustyq**: `scripts/chaos.sh` run with `KILL=0 MS=0
  RUSTYQ_CONCURRENCY=16` — the same chaos harness with the kill step
  disabled, used as a pure drain benchmark.

### Results

| Run (2026-09-26 / 09-28)                        | Celery 5.4 (Redis, prefork 16, `acks_late`) | rustyq, durable commits | rustyq, `PG_SYNCHRONOUS_COMMIT=off` |
| ------------------------------------------------ | ------------------------------------------- | ----------------------- | ----------------------------------- |
| 1 (load ~25–30, not back-to-back)                | 279 jobs/s                                  | 386 jobs/s              | —                                   |
| 2 (fresh boot; load rose 10 → 38 mid-run)        | 1,104 jobs/s                                | 394 jobs/s              | —                                   |
| 3 (back-to-back, load ~20–31)                    | 168 jobs/s                                  | 694 jobs/s              | 849 jobs/s                          |
| RSS per worker container (every run)             | ~340 MiB                                    | 2.3–9.4 MiB             | —                                   |

The same harness on the same box swung 6.6x for Celery (168–1,104 jobs/s)
and 1.8x for rustyq (386–694 jobs/s) depending on what else the shared
host was doing, so **no throughput ratio can be claimed from this host** —
in one run Celery was 2.8x faster, in another rustyq was 4.1x faster. The
memory difference (~340 MiB vs 2.3–9.4 MiB, roughly 35–150x) held in every
run. Celery's Redis broker does not
fsync; `PG_SYNCHRONOUS_COMMIT=off` (compose knob, benchmark-only: a
Postgres crash can then lose acknowledged enqueues) removes that
difference for rustyq and was worth ~20% in run 3.

### Fairness caveats — read before quoting the throughput number

- **Host noise dominates throughput.** Three runs gave three different
  winners-by-margin (see table); only a quiet, dedicated box can produce a
  quotable ratio.
- **The memory difference (~35–150x) is robust** across runs — it reflects an
  architectural difference (one multi-threaded Rust process with async
  tasks vs. 16 forked CPython processes per worker container), not host
  load noise.
- **Celery's broker (Redis) does not fsync by default; rustyq pays a
  durable Postgres commit per job.** rustyq is doing strictly more work per
  job (crash-safe persistence with `FOR UPDATE SKIP LOCKED` claim
  semantics) and still came out ahead in 2 of 3 runs — but the comparison
  is not apples-to-apples on durability, and a Postgres-brokered Celery
  (mentioned as an alternative in the original phase plan's risk register)
  was not built or measured this session.
- No run pinned CPU affinity or isolated the containers from the rest
  of the Docker VM's scheduling.

### Reproduce

```bash
# rustyq side:
KILL=0 MS=0 RUSTYQ_CONCURRENCY=16 scripts/chaos.sh 20000 4

# Celery side:
bench/celery/run.sh 20000 4
```

## 6. Chaos test — zero job loss under SIGKILL

### Setup

`scripts/chaos.sh 20000 4`: boots the compose stack with workers scaled to
0 and `RUSTYQ_LOCK_TIMEOUT_SECS=10` (deliberately short so the reaper
recovers killed workers' jobs quickly rather than waiting out the 300s
production default), bulk-inserts 20,000 `sleep` jobs (5 ms each), scales up
to 4 workers, waits until ~25% are done, then `docker kill -s KILL`s 2 of the
4 worker containers (no graceful shutdown — SIGKILL leaves their in-flight
rows `running` with a lock nobody will release), restarts the killed
replicas, and polls until the queue drains.

### Result

- 2 of 4 workers `SIGKILL`ed at **6,311/20,000** done, with **27** jobs
  `running` at kill time.
- Final state: **20,000/20,000 done, 0 dead, 0 stuck** in `queued`/
  `running`.
- **59 jobs re-run** (`attempts > 1`) — i.e. 59 jobs were reaped and
  restarted by a surviving/restarted worker.
- **PASS: zero job loss.**

### The lesson

Under the loaded host, Postgres itself stalled for up to **18 seconds** at
some point during the run (observed via worker logs / reaper behavior). With
`RUSTYQ_LOCK_TIMEOUT_SECS=10`, the reaper requeued some jobs whose original
worker was still alive and still running them — the lock simply looked stale
because the DB round trip that would have refreshed/finalized it was
stalled. This is exactly the race the `(locked_by, attempts)` fence in
`finalize`/`finalize_done_batch` (`crates/core/src/lib.rs`) exists to handle:
when the original (slow, not dead) worker eventually tried to finalize a job
that had already been reaped and re-claimed, its `UPDATE ... WHERE
attempts=$job.attempts` matched zero rows (the re-claim had already bumped
`attempts`), so the finalize was skipped and logged (`"lost lock, finalize
skipped"`) instead of clobbering the new owner's `done` row or corrupting the
retry count. The job re-ran once under its new owner and finished cleanly —
never double-finalized, never lost.

**Operational takeaway:** `lock_timeout` must exceed the worst realistic
Postgres stall plus handler runtime on the deployment target, or the reaper
will requeue jobs that are still legitimately in flight (harmless thanks to
fencing, but wasteful — the job runs twice). That is why the shipped
default (`RUSTYQ_LOCK_TIMEOUT_SECS=300`) is 30x the value used here; 10s was
chosen only to make this particular test converge in a reasonable wall-clock
time, not as an operational recommendation.

### Reproduce

```bash
scripts/chaos.sh 20000 4
# or, to change how many workers get killed:
KILL=1 scripts/chaos.sh 20000 4
```

Leaves the stack up for inspection; tear down with `docker compose down -v`.

## What to re-run on quiet hardware

Every number above was taken on a shared, loaded laptop with Postgres
running inside a Docker Desktop VM. Before treating any target as
definitively met or missed, re-run on a quiet (ideally bare-metal or a
dedicated cloud VM, native Postgres, no other load) 4-vCPU-class box:

1. `cargo bench -p rustyq-core --bench drain` with `BENCH_JOBS=100000
   BENCH_WORKERS=4 BENCH_CONCURRENCY=16` (and again at higher concurrency)
   — check against the >= 5,000 jobs/s design target.
2. The same bench's latency phase (`BENCH_LATENCY_JOBS=2000`) — check p99
   against the < 50 ms design target.
3. `scripts/chaos.sh 20000 4` five consecutive times (only one run was
   performed this session) to build confidence in the zero-loss claim
   beyond a single pass.
4. The Celery comparison (`bench/celery/run.sh` vs.
   `KILL=0 MS=0 scripts/chaos.sh`), several interleaved runs on a quiet,
   dedicated box, to replace the inconclusive range with a quotable ratio.
5. `pgbench -N -c16 -T10` against the target Postgres to record the new
   ceiling, so future numbers on that box can be read the same way this
   document reads the laptop's.
