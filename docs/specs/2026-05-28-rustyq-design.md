---
title: rustyq — Durable Job Queue with PyO3 bindings (P1)
status: draft
date: 2026-05-28
related:
    - ../../backend-cloud-roadmap.md
    - ../../projects-l3-l4.md
---

# rustyq — Design Spec

> Companion spec lifted from `projects-l3-l4.md` § "P1 — Durable Job Queue
> with PyO3 bindings (rustyq)". Code blocks are the authoritative
> implementation reference for the scaffold; downstream phases extend, they
> do not contradict. Default stack pins live in `backend-cloud-roadmap.md`
> § 3 and are mirrored verbatim in [`Cargo.toml`](../../Cargo.toml).

## 1. Problem

Many Python services dispatch background work via ad-hoc cron + Postgres
polling. A real job queue (Celery / RQ / Sidekiq) solves retries, scheduling,
observability, and concurrency control — but those are Python/Ruby and the
hot loop is what bottlenecks. Build a Rust-native durable queue, expose
Python bindings, drop in as a Celery replacement. **Interview pitch:** "I
drained my own Python services with a Rust worker pool."

## 2. Architecture

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

## 3. Stack

- `tokio` 1.47, `axum` 0.8, `sqlx` 0.8 (Postgres), `tower`, `tower-http`.
- `pyo3` 0.22 + `maturin` for the Python wheel.
- `tracing` + `tracing-opentelemetry` + `metrics-exporter-prometheus`.
- `serde`, `serde_json`, `anyhow`, `thiserror`, `clap`, `uuid` v7 (sortable IDs).
- Optional: `cron` crate for scheduling, `argon2` for API key hashing.

## 4. Key Rust code

### 4.1 Schema (`migrations/0001_init.sql`)

```sql
CREATE TABLE jobs (
  id            UUID PRIMARY KEY,
  queue         TEXT NOT NULL,
  kind          TEXT NOT NULL,
  payload       JSONB NOT NULL,
  state         TEXT NOT NULL CHECK (state IN ('queued','running','done','failed','dead')),
  priority      SMALLINT NOT NULL DEFAULT 0,
  attempts      INT NOT NULL DEFAULT 0,
  max_attempts  INT NOT NULL DEFAULT 5,
  run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at     TIMESTAMPTZ,
  locked_by     TEXT,
  last_error    TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_jobs_dispatch ON jobs (queue, state, priority DESC, run_at)
  WHERE state = 'queued';
CREATE INDEX idx_jobs_locked   ON jobs (locked_by, state) WHERE state = 'running';
```

### 4.2 Worker dispatch loop (`crates/core/src/lib.rs`)

```rust
use sqlx::{PgPool, postgres::PgListener};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

pub struct Worker {
    pool: PgPool,
    id: String,           // hostname:pid
    queues: Vec<String>,
    concurrency: usize,
}

impl Worker {
    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("rustyq_new").await?;

        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(self.concurrency));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = listener.recv() => {}, // wake on enqueue notify
                _ = sleep(Duration::from_secs(1)) => {}, // fallback poll
            }
            // Drain as many jobs as concurrency allows
            while sem.available_permits() > 0 {
                let permit = sem.clone().acquire_owned().await?;
                let Some(job) = self.claim_one().await? else {
                    drop(permit);
                    break;
                };
                let pool = self.pool.clone();
                let id = self.id.clone();
                tokio::spawn(async move {
                    let result = run_job(&job).await;
                    finalize(&pool, &id, &job, result).await.ok();
                    drop(permit);
                });
            }
        }
        Ok(())
    }

    async fn claim_one(&self) -> sqlx::Result<Option<Job>> {
        // SKIP LOCKED is the magic — multi-worker safe, no row contention
        let row = sqlx::query_as::<_, Job>(
            r#"
            UPDATE jobs SET state='running', locked_at=now(), locked_by=$1, attempts=attempts+1
            WHERE id = (
              SELECT id FROM jobs
              WHERE state='queued' AND run_at <= now() AND queue = ANY($2)
              ORDER BY priority DESC, run_at
              FOR UPDATE SKIP LOCKED
              LIMIT 1
            )
            RETURNING *
            "#,
        )
        .bind(&self.id)
        .bind(&self.queues)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }
}

async fn finalize(pool: &PgPool, _id: &str, job: &Job, res: anyhow::Result<()>) -> sqlx::Result<()> {
    match res {
        Ok(()) => {
            sqlx::query!("UPDATE jobs SET state='done', locked_at=NULL WHERE id=$1", job.id)
                .execute(pool).await?;
        }
        Err(e) if job.attempts >= job.max_attempts => {
            sqlx::query!(
                "UPDATE jobs SET state='dead', last_error=$2, locked_at=NULL WHERE id=$1",
                job.id, e.to_string()
            ).execute(pool).await?;
        }
        Err(e) => {
            // Exponential backoff: 2^attempts seconds, capped at 1 hour
            let delay = (1u64 << job.attempts.min(12)).min(3600) as i64;
            sqlx::query!(
                "UPDATE jobs SET state='queued', last_error=$2, run_at=now()+ make_interval(secs => $3::int), locked_at=NULL WHERE id=$1",
                job.id, e.to_string(), delay as i32
            ).execute(pool).await?;
        }
    }
    Ok(())
}
```

> **Phase-1 note:** the scaffold replaces `sqlx::query!` (compile-time
> checked) with `sqlx::query` (runtime), so `cargo check` works without a
> live `DATABASE_URL`. Switch back to `query!` once `sqlx prepare` is in
> the CI loop.

### 4.3 Enqueue API (`crates/server/src/api.rs`)

```rust
use axum::{routing::post, extract::State, Json, Router};
use uuid::Uuid;

#[derive(serde::Deserialize)]
pub struct EnqueueReq {
    pub queue: String,
    pub kind: String,
    pub payload: serde_json::Value,
    #[serde(default)] pub priority: i16,
    #[serde(default)] pub delay_secs: i64,
}

async fn enqueue(State(pool): State<PgPool>, Json(req): Json<EnqueueReq>)
    -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)>
{
    let id = Uuid::now_v7();
    sqlx::query!(
        r#"INSERT INTO jobs (id, queue, kind, payload, state, priority, run_at)
           VALUES ($1, $2, $3, $4, 'queued', $5, now() + make_interval(secs => $6::int))"#,
        id, req.queue, req.kind, req.payload, req.priority, req.delay_secs as i32
    )
    .execute(&pool).await
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    sqlx::query!("NOTIFY rustyq_new").execute(&pool).await.ok();
    Ok(Json(serde_json::json!({ "id": id })))
}

pub fn router(pool: PgPool) -> Router {
    Router::new()
        .route("/jobs", post(enqueue))
        .with_state(pool)
}
```

### 4.4 PyO3 client (`crates/pybind/src/lib.rs`)

```rust
use pyo3::prelude::*;

#[pyclass]
struct Client { base_url: String, client: reqwest::blocking::Client }

#[pymethods]
impl Client {
    #[new]
    fn new(base_url: String) -> Self {
        Self { base_url, client: reqwest::blocking::Client::new() }
    }

    fn enqueue(&self, queue: &str, kind: &str, payload: &str) -> PyResult<String> {
        let resp: serde_json::Value = self.client
            .post(format!("{}/jobs", self.base_url))
            .json(&serde_json::json!({ "queue": queue, "kind": kind, "payload": serde_json::from_str::<serde_json::Value>(payload).unwrap() }))
            .send().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .json().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(resp["id"].as_str().unwrap_or("").to_string())
    }
}

#[pymodule]
fn rustyq(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?; Ok(())
}
```

## 5. Deployment

- `Dockerfile` with `cargo-chef` for layer caching. Multi-stage, distroless
  final image.
- `fly.toml` with single machine + attached Neon Postgres.
- Python wheel published to TestPyPI via `maturin publish`.
- Compose for local: postgres + rustyq-server + 2× rustyq-worker.

## 6. Eval / benchmarks

- Throughput: enqueue 100k jobs, drain rate target ≥ **5k jobs/sec** on a
  4-vCPU machine.
- Latency: enqueue → first worker pick-up, target **p99 < 50 ms** via
  LISTEN/NOTIFY (vs ~1 s for pure poll).
- Chaos: `kill -9` 2 of 4 workers mid-burst, verify zero job loss + every
  job runs ≥ 1 time.
- Comparison: Celery on identical Postgres at same hardware — expect 3–5×
  higher throughput, dramatically lower memory.

## 7. Stretch to L4

- Replace Postgres backend with the LSM engine from **P5** (driftdb) —
  owned storage, prove the API is portable.
- Add work-stealing across nodes via a heartbeat table + gossip.
- Per-tenant fairness scheduler with weighted round-robin.

## 8. Source references

- `apalis` (https://github.com/geofmureithi/apalis) for prior art.
- `faktory` for the Sidekiq-Rust angle.
- Postgres docs on `FOR UPDATE SKIP LOCKED`.
