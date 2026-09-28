# rustyq

> Durable Rust-native job queue with Postgres + PyO3 client. A Celery
> alternative for Python services that need predictable throughput, retries,
> and observability without giving up their existing Postgres footprint.

[![ci](https://img.shields.io/badge/ci-pending-lightgrey.svg)](./.github/workflows/ci.yml)
[![crates.io](https://img.shields.io/badge/crates.io-pending-lightgrey.svg)](#)
[![pypi](https://img.shields.io/badge/pypi-pending-lightgrey.svg)](#)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

## The problem

Many Python services dispatch background work via ad-hoc cron + Postgres
polling. A real job queue (Celery / RQ / Sidekiq) solves retries, scheduling,
and observability — but those workers are Python/Ruby and the hot loop is
what bottlenecks. **rustyq** is a Rust-native durable queue with Python
bindings that replaces the Celery workers: Postgres stays the source of
truth, Python keeps enqueueing, Rust does the claim/dispatch/retry loop. It
is not a drop-in: task bodies must be rewritten as Rust `Handler`s
registered by job `kind` in the worker binary.

## Architecture

```
+-----------------------------------+
| Python agent (PyO3-bound client)  |
| rustyq.Client(url).enqueue(...)   |
+----------------+-------------------+
                 |
                 v
+----------------+-------------------+        +----------------------+
|   axum HTTP API (rustyq-server)    | <----> |  Postgres jobs table |
|   POST /jobs   GET /jobs/{id}      |        |  FOR UPDATE          |
|   GET /healthz GET /metrics        |        |  SKIP LOCKED         |
+----------------+-------------------+        +----------+-----------+
                 |                                        |
                 |  INSERT + pg_notify in one statement    |
                 |  (LISTEN/NOTIFY, 1s poll fallback)      |
                 v                                        v
+----------------+-------------------+        +----------+-----------+
|   rustyq-worker (N replicas)       | <----> |  batched claim_batch |
|  - up to `concurrency` jobs        |        |  UPDATE...RETURNING  |
|    in flight per worker (tokio     |        |  batched success     |
|    tasks gated by a semaphore)     |        |  finalize; fenced on |
|  - exponential backoff retries     |        |  (locked_by,attempts)|
|  - stale-lock reaper (own task)    |        +----------------------+
+-------------------------------------+
```

There is no gRPC API and no per-queue concurrency cap — a worker's
`--concurrency` bounds total in-flight jobs across all of its `--queues`.
Idempotency is the caller's responsibility (see [Features &
semantics](#features--semantics)).

## Features & semantics

rustyq delivers jobs **at least once**: a handler that legitimately runs
longer than `lock_timeout` gets reaped and re-run by another worker while the
original run may still be in flight. Handlers should be idempotent, or should
use `job.attempts`/the job id to detect duplicate execution.

- **Claim** — `claim_batch` does one `UPDATE jobs ... WHERE state='queued'
  ... FOR UPDATE SKIP LOCKED LIMIT n RETURNING ...` per drain cycle, ordered
  by `priority DESC, run_at`. `SKIP LOCKED` makes concurrent claims from many
  workers safe without an app-level lock. (`crates/core/src/lib.rs`)
- **Dispatch** — claimed rows are handed to per-job tokio tasks gated by a
  semaphore sized to `--concurrency`; kind -> handler routing goes through a
  `Registry` built from `Handler` impls. (`crates/core/src/handler.rs`)
- **Reaper** — a periodic task requeues any `running` job whose `locked_at`
  is older than `lock_timeout` (worker presumed dead) back to `queued`, or to
  `dead` if its attempt budget is exhausted. (`reap_stale` in
  `crates/core/src/lib.rs`)
- **Fencing** — every finalize (`done`, `dead`, or requeue) is conditioned on
  `state='running' AND locked_by=$worker_id AND attempts=$job.attempts`.
  Because every claim increments `attempts`, it doubles as a fencing token:
  if the reaper already handed the job to a new owner, the stale finalize
  touches zero rows, is logged (`"lost lock, finalize skipped"`), and is
  dropped — it can never clobber the new owner's row or double-finalize.
  Successful jobs are finalized in batches of up to 512 via one
  `UPDATE ... FROM UNNEST(...)`, fenced the same way. (`finalize`,
  `finalize_done_batch` in `crates/core/src/lib.rs`)
- **Retries / backoff / dead** — on handler error, a job is requeued with
  delay `2^attempts` seconds (capped at 1 hour) until `attempts >=
  max_attempts` (default 5), at which point it is marked `dead` with
  `last_error` preserved.
- **Priorities and delays** — `priority` (higher runs first) and `delay_secs`
  (`run_at = now() + delay_secs`) are set per job at enqueue time.
- **Low-latency dispatch** — `POST /jobs` does the `INSERT` and
  `pg_notify('rustyq_new', '')` in a single statement, so the notification is
  delivered at commit and a listening worker sees the row immediately.
  Workers also poll every 1s as a fallback (missed notify, reconnect).
- **Graceful shutdown** — SIGTERM (Docker stop) or SIGINT/Ctrl-C (Fly's
  configured `kill_signal`) cancels the
  dispatch loop; the worker waits up to `shutdown_grace` (30s, not currently
  configurable via flag/env) for in-flight jobs to finish, then gives the
  batch finalizer up to 10s to flush before exiting.
- **Migrations** — `rustyq-server --migrate` (or `RUSTYQ_MIGRATE=true`) runs
  `sqlx::migrate!` against `DATABASE_URL` before serving. Opt-in and meant for
  the server process only — never run it from every worker replica against a
  shared database.
- **Tracing** — `tracing` + optional OTLP export via
  `tracing-opentelemetry` when `OTEL_EXPORTER_OTLP_ENDPOINT` is set; falls
  back to plain JSON logs otherwise and never fails to start because of
  tracing setup. `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317 docker
  compose --profile otel up` adds a Jaeger UI on `:16686` and points both
  binaries at it.
- **Metrics** — each process exports its own series. `rustyq-server` serves
  `rustyq_jobs_enqueued_total` on its `/metrics` route; every
  `rustyq-worker` runs a Prometheus listener on `RUSTYQ_METRICS_BIND`
  (default `0.0.0.0:9091`) with the claimed/finished/reaped counters and the
  dispatch-latency and run-duration histograms. Scrape both.

**Not implemented** (do not assume these): gRPC API, per-queue concurrency
caps, idempotency keys, cron/recurring scheduling, auth on the HTTP API.

## Stack

| Layer               | Crate / Tool                                                        |
| ------------------- | ------------------------------------------------------------------- |
| Async runtime       | `tokio` 1.47 (full)                                                 |
| HTTP server         | `axum` 0.8 + `tower` + `tower-http`                                 |
| HTTP client (Rust)  | `reqwest` 0.12 (rustls-tls)                                         |
| Postgres            | `sqlx` 0.8                                                          |
| Cancellation        | `tokio-util` `CancellationToken`                                    |
| IDs                 | `uuid` v7 (sortable)                                                |
| Python bindings     | `pyo3` 0.29 + `maturin`                                             |
| Observability       | `tracing` + `tracing-opentelemetry` + `metrics-exporter-prometheus` |
| CLI                 | `clap` 4                                                            |
| Container build     | `cargo-chef` multi-stage, distroless final                          |
| Local orchestration | `docker-compose`                                                    |
| Deploy              | Fly.io (region `sin`) + Neon Postgres — config ready, not deployed  |
| CI                  | GitHub Actions (stable + beta) + `cargo-deny` + `cargo-nextest`     |

Full pinned versions live in [`Cargo.toml`](./Cargo.toml). Schema:
[`migrations/`](./migrations/).

## Quick start (docker-compose)

```bash
git clone <your-fork-url> rustyq
cd rustyq
docker compose up --build
# Postgres (internal only), server on :8080, two workers attached.

# Enqueue a job:
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue": "default", "kind": "sleep", "payload": {"ms": 100}}'
# => {"id":"01..."}
```

The stock worker only registers the built-in handlers `noop`, `sleep`
(`payload.ms`) and `fail_once`. Any other `kind` fails with `no handler for
kind '...'`, is retried with backoff, and ends `dead` after `max_attempts`
(5); register your own `Handler`s in `crates/worker/src/main.rs`.

## Quick start (Python client)

```bash
cd crates/pybind
maturin develop --release   # builds the wheel into your venv
```

```python
import rustyq

client = rustyq.Client("http://localhost:8080", timeout_secs=10.0)
job_id = client.enqueue("default", "sleep", {"ms": 100}, priority=0, delay_secs=0)
print(client.status(job_id))  # -> {"id": ..., "state": "queued", "attempts": 0, ...}
```

`payload` is any JSON-serialisable Python object (dict/list/str/number/None) —
it is serialised with `json.dumps`, not passed through as a JSON string.
Non-2xx responses raise `RuntimeError`; `status()` of an unknown job id raises
`KeyError` (a malformed, non-UUID id gets a `400` and raises `RuntimeError`).

## Configuration

### `rustyq-server`

| Flag               | Env               | Default        | Description                                          |
| ------------------ | ----------------- | -------------- | ----------------------------------------------------- |
| `--database-url`    | `DATABASE_URL`     | *(required)*   | Postgres connection string                            |
| `--bind`            | `RUSTYQ_BIND`      | `0.0.0.0:8080` | HTTP bind address                                      |
| `--pg-max`          | `RUSTYQ_PG_MAX`    | `16`           | Max Postgres pool connections                          |
| `--migrate`         | `RUSTYQ_MIGRATE`   | `false`        | Run pending migrations before serving (server only)    |

### `rustyq-worker`

| Flag                   | Env                        | Default             | Description                                                          |
| ----------------------- | --------------------------- | -------------------- | ---------------------------------------------------------------------- |
| `--database-url`        | `DATABASE_URL`               | *(required)*         | Postgres connection string                                            |
| `--queues`               | `RUSTYQ_QUEUES`              | `default`             | Comma-separated list of queues this worker drains                    |
| `--concurrency`          | `RUSTYQ_CONCURRENCY`         | `8`                   | Max jobs this worker runs concurrently                               |
| `--id`                   | `RUSTYQ_WORKER_ID`           | `<hostname>:<uuidv7>` | Identity written to `locked_by`                                       |
| `--lock-timeout-secs`    | `RUSTYQ_LOCK_TIMEOUT_SECS`   | `300`                 | Seconds a `running` lock may age before the reaper presumes it dead   |
| `--metrics-bind`         | `RUSTYQ_METRICS_BIND`        | `0.0.0.0:9091`        | Address of the worker's Prometheus `/metrics` listener                |

### Both binaries

| Env                             | Default | Description                                                        |
| -------------------------------- | ------- | -------------------------------------------------------------------- |
| `RUST_LOG`                       | `info`  | `tracing-subscriber` `EnvFilter` string                              |
| `OTEL_EXPORTER_OTLP_ENDPOINT`    | unset   | If set, exports spans over OTLP/gRPC; unset means logs-only, no cost |

### `docker-compose.yml` only

| Env                          | Default | Description                                     |
| ----------------------------- | ------- | -------------------------------------------------- |
| `RUSTYQ_HTTP_PORT`            | `8080`  | Host port mapped to the server's container port 8080 |
| `PG_SYNCHRONOUS_COMMIT`       | `on`    | Postgres `synchronous_commit`; `off` is a benchmark knob only (a Postgres crash can lose acknowledged enqueues) |
| `RUSTYQ_CONCURRENCY`          | `8`     | Passed through to every `rustyq-worker` replica       |
| `RUSTYQ_LOCK_TIMEOUT_SECS`    | `300`   | Passed through to every `rustyq-worker` replica       |

## HTTP API

| Method | Path         | Body / params                                                                 | Response                                                                    |
| ------ | ------------ | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `POST` | `/jobs`      | `{"queue","kind","payload","priority"?:0,"delay_secs"?:0}`. Validates: `queue`/`kind` non-empty, `0 <= delay_secs <= i32::MAX`. | `200 {"id": "<uuid>"}`; `400` plain-text on a failed validation; axum's JSON-extractor rejection (`400`/`415`/`422`) on a malformed body or missing field; `500` on a DB error |
| `GET`  | `/jobs/{id}` | —                                                                              | `200` `JobStatus` (`id, state, attempts, max_attempts, run_at, locked_by, last_error`), `404` if unknown, `400` if `{id}` is not a UUID |
| `GET`  | `/healthz`   | —                                                                              | `200 "ok"` if `SELECT 1` succeeds against Postgres, else `503`                |
| `GET`  | `/metrics`   | —                                                                              | Prometheus text exposition of `rustyq_jobs_enqueued_total{queue,kind}`; worker-side series are on each worker's own listener (see Features → Metrics) |

`payload` is not returned by `/jobs/{id}` — the status shape is deliberately
narrow so it stays stable as internal columns change.

## Benchmarks

All numbers below were measured 2026-09-26 (Celery comparison runs
2026-09-26 to 09-28) on an 8 vCPU / 39 GB Linux laptop with Postgres 16 running
inside Docker Desktop's VM, under a heavily loaded host (load average
25–50 from unrelated builds). The raw `pgbench -N -c16 -T10` ceiling on
this box was 438 tps (1,239 tps with `synchronous_commit=off`), so every
absolute number here is pessimistic. Treat the *relative* deltas (before
vs. after a fix, measured back to back) as the useful signal, not the
absolute jobs/s — and even rustyq-vs-Celery on the same box proved too noisy
for a ratio (below). Full methodology, `EXPLAIN` timings, and exact
commands are in
[`docs/specs/2026-09-26-rustyq-bench-writeup.md`](./docs/specs/2026-09-26-rustyq-bench-writeup.md).

In-process drain, `cargo bench -p rustyq-core --bench drain`, 20,000 `noop`
jobs, Postgres in Docker:

| Configuration                                   | Throughput     |
| ------------------------------------------------ | -------------- |
| Before fix (seq scan + full sort per claim)      | 129 jobs/s     |
| + dispatch index (`priority DESC, run_at WHERE state='queued'`, migration 0002) | 304 jobs/s |
| + batched success finalize (one `UPDATE...FROM UNNEST` per batch) | 798 jobs/s |
| 4 workers x 64 concurrency                        | 1,579 jobs/s   |

Dispatch latency (enqueue -> handler start, sequential enqueues with a 2 ms
pause between them, i.e. at most ~500/s, idle workers on LISTEN): p50 61–89 ms, p99 188–917 ms on this loaded host, dominated by
fsync'd commits (pgbench avg latency was 36 ms on the same box during this
session). The design target of p99 < 50 ms was **not met on this host** and
needs re-measurement on quiet hardware.

In-Docker comparison vs. Celery, same Docker VM, 20,000 trivial jobs, 4
workers x 16 concurrency each, clock started at first completed job:

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

| Bench target (from the original design)       | Status                                    |
| ------------------------------------------------ | -------------------------------------------- |
| >= 5,000 jobs/s drain, 4 vCPU                    | Nearly: 4,900 jobs/s on a GitHub-hosted 4-vCPU runner (10k jobs, 4x16, CI bench job, 2026-09-28); 1,579 best on the loaded dev host |
| p99 enqueue -> pickup < 50 ms                    | Met on the CI runner: p50 1.6 ms, p99 4.3 ms, p99.9 9.3 ms; 188–917 ms on the loaded dev host |
| Memory / in-flight job < 2 MB                    | Met (2.3–9.4 MiB **per worker process**, not per job) |
| vs. Celery: 3–5x throughput, much lower memory   | Throughput inconclusive on this host (runs ranged from Celery 2.8x faster to rustyq 4.1x faster); ~35–150x less memory — met on memory only |

## Chaos testing

`scripts/chaos.sh [JOBS] [WORKERS]` (default 20,000 jobs, 4 workers) scales
workers to 0, bulk-inserts sleep jobs, starts the workers, `docker kill -s
KILL`s half of them mid-drain (no graceful shutdown — their in-flight rows
stay `running` with a lock nobody releases), restarts them, and asserts
every job ends up `done` with none `dead` and none stuck.

Latest run (`scripts/chaos.sh 20000 4`, 5 ms sleep jobs,
`RUSTYQ_LOCK_TIMEOUT_SECS=10`): 2 of 4 workers `SIGKILL`ed at 6,311/20,000
done with 27 jobs `running`; result: **20,000/20,000 done, 0 dead, 0 stuck,
59 jobs re-run** (`attempts > 1`). Zero job loss.

The lesson from that run: under the loaded host, Postgres itself stalled for
up to 18 s. With a 10 s lock timeout the reaper requeued some jobs that were
actually still alive and running — but the `attempts` fence
(see [Features & semantics](#features--semantics)) made the original run's
eventual finalize a no-op (`"lost lock, finalize skipped"`) instead of a
double-finalize; the job simply re-ran and completed once. The operational
takeaway: `lock_timeout` must exceed the worst realistic DB stall plus
handler runtime, which is why the shipped default is 300 s, not the 10 s
used to make this test converge quickly.

## Running tests

```bash
# 1. Bring up a throwaway Postgres for integration tests (port 55432):
eval "$(./scripts/test-pg.sh up | tail -1)"   # exports TEST_DATABASE_URL

# 2. Run the suite. Integration tests share the `rustyq_new` LISTEN/NOTIFY
#    channel across isolated schemas, so cross-binary parallelism races —
#    --test-threads=1 is required until that channel is scoped per schema
#    (tracked in PROGRESS.md).
cargo nextest run --workspace --no-fail-fast --test-threads=1
# or, without nextest:
SQLX_OFFLINE=true cargo test --workspace -- --test-threads=1

# 3. Tear down:
./scripts/test-pg.sh down
```

CI (`.github/workflows/ci.yml`) builds with `SQLX_OFFLINE=true` against the
committed `.sqlx/` query metadata, so it needs no live `DATABASE_URL` at
compile time; a separate `sqlx-check` job applies the migrations to a fresh
Postgres and runs `cargo sqlx prepare --workspace --check -- --tests` to
catch metadata drift. After changing a `sqlx::query!`/`query_as!` call,
regenerate it locally with:

```bash
# against the test-pg.sh container (compose's Postgres publishes no host port)
docker exec rustyq-test-pg psql -U postgres -c 'DROP DATABASE IF EXISTS rustyq_prepare' -c 'CREATE DATABASE rustyq_prepare'
for f in migrations/*.sql; do docker exec -i rustyq-test-pg psql -U postgres -d rustyq_prepare < "$f"; done
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/rustyq_prepare cargo sqlx prepare --workspace -- --tests
```

## Deploy (Fly)

**Config ready, not yet deployed** — this needs a Fly account and a
provisioned Neon database, neither of which was available this session.

[`fly.toml`](./fly.toml) defines app `rustyq`, region `sin`, and two process
groups from one image: `server = "rustyq-server --migrate"` (runs
migrations on boot) and `worker = "rustyq-worker"`. `kill_timeout` is 40s,
deliberately longer than the worker's 30s shutdown grace, so Fly's stop
signal gives in-flight jobs time to finish before the process is killed
outright. The HTTP service checks `GET /healthz` every 15s.

To deploy once credentials are available:

```bash
fly secrets set DATABASE_URL='postgres://...neon.tech/rustyq?sslmode=require'
fly deploy
```

## Repository layout

```
rustyq/
  Cargo.toml                # workspace
  crates/
    core/                   # Job, JobState, Worker, claim_batch, finalize, reap_stale
    server/                 # axum HTTP server (POST /jobs, status, healthz, metrics)
    worker/                 # worker daemon (boots Worker, built-in handlers)
    pybind/                 # PyO3 client + pyproject.toml (maturin)
    client/                 # async Rust client (reqwest)
  migrations/               # jobs table + dispatch indexes (sqlx migrate)
  bench/
    celery/                 # Celery + Redis baseline rig for comparison
  scripts/
    chaos.sh                # SIGKILL-mid-drain zero-loss test
    test-pg.sh              # throwaway Postgres for `cargo test`
    smoke.sh                # enqueue-and-wait smoke test against compose
  Dockerfile                # cargo-chef multi-stage, distroless final
  docker-compose.yml        # postgres + server + N workers (+ optional Jaeger)
  fly.toml                  # Fly.io app, region sin, Neon-attached (not yet deployed)
  deny.toml                 # cargo-deny config
  rust-toolchain.toml       # stable channel
  .github/workflows/ci.yml  # nextest + clippy + fmt + deny + sqlx-check + bench smoke
  docs/
    specs/                  # design spec + 2026-09-26 bench writeup
    plans/                  # phase plans
  PROGRESS.md               # per-sprint tracker
```

## License <a id="license"></a>

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](./LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.
