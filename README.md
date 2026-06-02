# rustyq

> Durable Rust-native job queue with Postgres + PyO3 client. Drop-in Celery
> replacement for Python services that need predictable throughput, retries,
> and observability without giving up their existing Postgres footprint.

[![ci](https://img.shields.io/badge/ci-pending-lightgrey.svg)](./.github/workflows/ci.yml)
[![crates.io](https://img.shields.io/badge/crates.io-pending-lightgrey.svg)](#)
[![pypi](https://img.shields.io/badge/pypi-pending-lightgrey.svg)](#)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

## The problem

Many Python services dispatch background work via ad-hoc cron + Postgres
polling. A real job queue (Celery / RQ / Sidekiq) solves retries, scheduling,
observability, and concurrency control — but those are Python/Ruby and the
hot loop is what bottlenecks. **rustyq** is a Rust-native durable queue with
Python bindings that drops into a Celery deployment as a workers-only
replacement.

## Architecture

```
+-----------------------------------+
| Python agent (PyO3-bound client)  |
| rustyq.enqueue("send_email", ...) |
+----------------+------------------+
                 |
                 v
+----------------+------------------+        +--------------------+
|   axum REST + gRPC enqueue API    | <----> |  Postgres jobs tbl |
|   POST /jobs   GET /jobs/{id}     |        |  FOR UPDATE        |
|   /metrics (Prometheus)           |        |  SKIP LOCKED       |
+----------------+------------------+        +---------+----------+
                 |                                     |
                 |        LISTEN/NOTIFY                |
                 |        (low-latency dispatch)       |
                 v                                     v
+----------------+------------------+        +---------+----------+
|        Worker pool (N x CPU)      | <----> |  Each worker holds |
|  - polls SKIP LOCKED              |        |  one job at a time |
|  - exponential backoff retries    |        |  Idempotency key   |
|  - per-queue concurrency caps     |        |  in business table |
+-----------------------------------+        +--------------------+
```

## Stack

| Layer               | Crate / Tool                                                        |
| ------------------- | ------------------------------------------------------------------- |
| Async runtime       | `tokio` 1.47 (full)                                                 |
| HTTP server         | `axum` 0.8 + `tower` + `tower-http`                                 |
| HTTP client (Rust)  | `reqwest` 0.12 (rustls-tls)                                         |
| Postgres            | `sqlx` 0.8                                                          |
| Cancellation        | `tokio-util` `CancellationToken`                                    |
| IDs                 | `uuid` v7 (sortable)                                                |
| Python bindings     | `pyo3` 0.22 + `maturin`                                             |
| Observability       | `tracing` + `tracing-opentelemetry` + `metrics-exporter-prometheus` |
| CLI                 | `clap` 4                                                            |
| Container build     | `cargo-chef` multi-stage, distroless final                          |
| Local orchestration | `docker-compose`                                                    |
| Deploy              | Fly.io (region `sin`) + Neon Postgres                               |
| CI                  | GitHub Actions (stable + beta) + `cargo-deny` + `cargo-nextest`     |

Full pinned versions live in [`Cargo.toml`](./Cargo.toml). Schema:
[`migrations/0001_init.sql`](./migrations/0001_init.sql).

## Quick start (docker-compose)

```bash
git clone <your-fork-url> rustyq
cd rustyq
docker compose up --build
# Postgres on :5432, server on :8080, two workers attached.

# Enqueue a job:
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue": "default", "kind": "send_email", "payload": {"to": "a@b"}}'
# => {"id":"01..."}
```

## Quick start (Python client)

```bash
cd crates/pybind
maturin develop --release   # builds the wheel into your venv
```

```python
import rustyq
client = rustyq.Client("http://localhost:8080")
job_id = client.enqueue("default", "send_email", '{"to": "a@b"}')
print(job_id)
```

## Bench targets

| Metric                                     | Target          | Notes                                   |
| ------------------------------------------ | --------------- | --------------------------------------- |
| Throughput (drain rate, 4 vCPU)            | >= 5,000 jobs/s | 100k jobs enqueued, drained             |
| p99 enqueue -> first worker pickup         | < 50 ms         | via LISTEN/NOTIFY (vs ~1s on pure poll) |
| Chaos: `kill -9` 2 of 4 workers mid-burst  | zero job loss   | every job runs >= 1 time                |
| Memory / job in-flight                     | < 2 MB          | per-worker RSS                          |
| vs Celery on identical Postgres + hardware | 3-5x throughput | dramatically lower memory               |

Run benches (once `cargo bench` targets exist):

```bash
cargo bench --workspace
```

## Repository layout

```
rustyq/
  Cargo.toml                # workspace
  crates/
    core/                   # Job, JobState, Worker, claim_one, finalize
    server/                 # axum HTTP server (POST /jobs)
    worker/                 # worker daemon (boots Worker, Ctrl-C cancels)
    pybind/                 # PyO3 client + pyproject.toml (maturin)
    client/                 # async Rust client (reqwest)
  migrations/0001_init.sql  # jobs table + dispatch indexes
  Dockerfile                # cargo-chef multi-stage, distroless final
  docker-compose.yml        # postgres + server + 2 workers
  fly.toml                  # Fly.io app, region sin, Neon-attached
  deny.toml                 # cargo-deny config
  rust-toolchain.toml       # stable channel
  .github/workflows/ci.yml  # nextest + clippy + fmt + deny + bench
  docs/
    specs/2026-05-28-rustyq-design.md     # full design spec
    plans/2026-05-28-rustyq-phase-1-scaffold.md
  PROGRESS.md               # per-sprint tracker
```

## Roadmap

Phase 1 (scaffold + compile) is the current sprint — see
[`docs/plans/2026-05-28-rustyq-phase-1-scaffold.md`](./docs/plans/2026-05-28-rustyq-phase-1-scaffold.md).
Subsequent phases (real job dispatch, Prometheus `/metrics`, retries +
dead-letter, PyPI publish, Fly.io demo) are tracked in
[`PROGRESS.md`](./PROGRESS.md).

## License <a id="license"></a>

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](./LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.
