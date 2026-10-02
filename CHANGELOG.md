# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-02

First public release.

### Added

- Durable, at-least-once job queue on Postgres: jobs survive worker and
  database restarts; handlers are registered by job `kind`.
- Batched claim with `FOR UPDATE SKIP LOCKED`, ordered by priority then
  `run_at`, so many workers claim concurrently without an application lock.
- Low-latency dispatch: enqueue does `INSERT` and `pg_notify` in one
  statement (LISTEN/NOTIFY, with a 1 s poll fallback).
- Retries with exponential backoff, a `dead` state once attempts are
  exhausted, per-job priorities and delays, and permanent (non-retried)
  errors.
- Reaper that requeues jobs whose worker died, with fenced finalize
  (`state`, `locked_by`, `attempts`) so a reaped-and-reclaimed job is never
  clobbered by a stale worker.
- Batched finalize for successful jobs, and graceful shutdown that drains
  in-flight jobs.
- HTTP API (`POST /jobs`, `GET /jobs/{id}`, `/healthz`, `/metrics`) in
  `rustyq-server`, and the `rustyq-worker` daemon with built-in handlers.
- Prometheus metrics (per process) and optional OTLP tracing export.
- Rust embedder API in `rustyq-core`: `migrate`, `NewJob`, `enqueue` (works
  inside your own transaction), `permanent` errors and `job_status`;
  migrations ship inside the crate.
- Async Rust client (`rustyq-client`) and Python client (`rustyq` on PyPI,
  one `abi3` wheel per platform for CPython 3.9+).
- Chaos test harness (SIGKILL half the workers, assert zero job loss),
  drain/latency benchmarks, an end-to-end reference app
  (`examples/order-pipeline`), and a Docker image at `ghcr.io/bunty9/rustyq`.

- Jobs live in the table `rustyq_jobs` (indexes `idx_rustyq_jobs_*`), in the
  connection's current `search_path` schema, so it cannot collide with an
  application's own `jobs` table; `max_attempts >= 1` is enforced by a CHECK.
- `rustyq-server`, `rustyq-worker` and the Docker image support TLS Postgres
  (`sslmode=require`) via sqlx `tls-rustls`; library crates leave TLS to the
  embedder.
- `rustyq-core` feature `telemetry` (default on) gates `rustyq_core::telemetry`
  and its tracing-subscriber/OpenTelemetry dependencies.
- `JobStatus` is `#[non_exhaustive]`; the client's `run_at` is a
  `chrono::DateTime<Utc>`; `NewJob::max_attempts` clamps to at least 1.

### Fixed

- `rustyq_server::router()` now installs the Prometheus recorder when the
  router is built; previously `rustyq_jobs_enqueued_total` increments made
  before the first `/metrics` scrape were dropped. If the embedding app has
  already installed a global `metrics` recorder, `router()` logs a warning
  and `/metrics` renders without rustyq's series instead of panicking.

### API notes

- `rustyq_client::Client::enqueue_with` takes an `EnqueueOptions` struct
  (`priority`, `delay_secs`, `max_attempts`) instead of positional
  arguments. This differs from earlier unreleased snapshots; there was no
  prior published version.

[0.1.0]: https://github.com/Bunty9/rustyq/releases/tag/v0.1.0
