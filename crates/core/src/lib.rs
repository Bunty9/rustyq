//! rustyq-core — types, worker dispatch loop, claim/finalize transitions.
//!
//! The worker is the heart of rustyq. It claims one job at a time from
//! Postgres via `FOR UPDATE SKIP LOCKED`, runs it, and finalizes the row
//! based on outcome (done / requeue with exponential backoff / dead).

pub mod handler;
pub use handler::{Handler, HandlerFut, Registry, RegistryBuilder};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{postgres::PgListener, FromRow, PgPool};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Job lifecycle states. Matches the `state` column CHECK constraint in
/// `migrations/0001_init.sql`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Queued,
    Running,
    Done,
    Failed,
    Dead,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Done => "done",
            JobState::Failed => "failed",
            JobState::Dead => "dead",
        }
    }
}

/// Row representation of a job, returned from `claim_one`.
#[derive(Debug, Clone, FromRow)]
pub struct Job {
    pub id: Uuid,
    pub queue: String,
    pub kind: String,
    pub payload: Json<serde_json::Value>,
    pub state: String,
    pub priority: i16,
    pub attempts: i32,
    pub max_attempts: i32,
    pub run_at: chrono::DateTime<chrono::Utc>,
    pub locked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub locked_by: Option<String>,
    pub last_error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// A long-running worker. Listens on `rustyq_new` LISTEN/NOTIFY and falls
/// back to a 1s poll. Each claimed job runs on a tokio task gated by a
/// semaphore sized to `concurrency`.
pub struct Worker {
    pub pool: PgPool,
    /// Identifier written to `locked_by` — typically `hostname:pid` or
    /// `hostname:uuid`.
    pub id: String,
    pub queues: Vec<String>,
    pub concurrency: usize,
    pub registry: Arc<Registry>,
}

impl Worker {
    pub fn new(
        pool: PgPool,
        id: String,
        queues: Vec<String>,
        concurrency: usize,
        registry: Arc<Registry>,
    ) -> Self {
        Self {
            pool,
            id,
            queues,
            concurrency,
            registry,
        }
    }

    /// Main dispatch loop. Returns on `cancel.cancelled()`.
    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("rustyq_new").await?;

        let sem = Arc::new(Semaphore::new(self.concurrency));
        let registry = self.registry.clone();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = listener.recv() => {}, // wake on enqueue notify
                _ = sleep(Duration::from_secs(1)) => {}, // fallback poll
            }
            // Drain greedily: claim_batch reads up to `available_permits()`
            // rows in a single UPDATE, then we hand each row to its own task
            // gated by a semaphore permit. The inner loop terminates when
            // the batch is shorter than requested (queue empty) — no need
            // for a separate "empty?" round-trip.
            loop {
                let n = sem.available_permits();
                if n == 0 {
                    break;
                }
                let batch = self.claim_batch(n).await?;
                if batch.is_empty() {
                    break;
                }
                let batch_len = batch.len();
                for job in batch {
                    // Acquire one permit per job in the batch. `claim_batch`
                    // bounded its query by available_permits() before the
                    // batch was claimed, so acquire is uncontended in the
                    // steady state.
                    let permit = sem.clone().acquire_owned().await?;
                    let claimed_at = Utc::now();
                    let pool = self.pool.clone();
                    let id = self.id.clone();
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        metrics::counter!(
                            "rustyq_jobs_claimed_total",
                            "queue" => job.queue.clone(),
                            "worker" => id.clone()
                        )
                        .increment(1);

                        let latency_secs = (claimed_at - job.created_at)
                            .num_milliseconds() as f64
                            / 1000.0;
                        metrics::histogram!(
                            "rustyq_dispatch_latency_seconds",
                            "queue" => job.queue.clone()
                        )
                        .record(latency_secs.max(0.0));

                        let started = Instant::now();
                        let result = registry.dispatch(&job).await;
                        let run_secs = started.elapsed().as_secs_f64();

                        metrics::histogram!(
                            "rustyq_job_run_duration_seconds",
                            "queue" => job.queue.clone(),
                            "kind"  => job.kind.clone()
                        )
                        .record(run_secs);

                        if let Err(e) = finalize(&pool, &id, &job, result).await {
                            tracing::error!(?e, job_id = %job.id, "finalize failed");
                        }
                        drop(permit);
                    });
                }
                // If we got fewer than asked, queue is drained — stop the
                // greedy loop and go back to LISTEN/NOTIFY waiting.
                if batch_len < n {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Claim up to `n` runnable jobs in a single round-trip. `SKIP LOCKED`
    /// makes this safe across many parallel workers. Returns an empty `Vec`
    /// when `n == 0` without hitting Postgres.
    ///
    /// Prefer this over a loop of `claim_one`: each `UPDATE … RETURNING`
    /// trip is at least one network round-trip plus a `fdatasync`, so
    /// batching keeps per-job overhead bounded as concurrency grows.
    pub async fn claim_batch(&self, n: usize) -> sqlx::Result<Vec<Job>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as!(
            Job,
            r#"
            UPDATE jobs SET state='running', locked_at=now(), locked_by=$1, attempts=attempts+1
            WHERE id IN (
              SELECT id FROM jobs
              WHERE state='queued' AND run_at <= now() AND queue = ANY($2)
              ORDER BY priority DESC, run_at
              FOR UPDATE SKIP LOCKED
              LIMIT $3
            )
            RETURNING
              id, queue, kind,
              payload as "payload: Json<serde_json::Value>",
              state, priority, attempts, max_attempts,
              run_at, locked_at, locked_by, last_error, created_at
            "#,
            self.id,
            &self.queues,
            n as i64,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Claim exactly one runnable job. Thin wrapper around
    /// [`Worker::claim_batch`] kept for callers that only need a single row.
    pub async fn claim_one(&self) -> sqlx::Result<Option<Job>> {
        Ok(self.claim_batch(1).await?.into_iter().next())
    }
}

/// Finalize a claimed job. On success → `done`. On error past the attempt
/// budget → `dead`. Otherwise → requeue with exponential backoff
/// (2^attempts, capped at 1 hour).
///
/// Returns the terminal state label for metrics (`Some("done")`,
/// `Some("dead")`) or `None` when the job was requeued (not yet finished).
pub async fn finalize(
    pool: &PgPool,
    _id: &str,
    job: &Job,
    res: anyhow::Result<()>,
) -> sqlx::Result<Option<&'static str>> {
    match res {
        Ok(()) => {
            sqlx::query!(
                "UPDATE jobs SET state='done', locked_at=NULL WHERE id=$1",
                job.id,
            )
            .execute(pool)
            .await?;
            metrics::counter!(
                "rustyq_jobs_finished_total",
                "state" => "done"
            )
            .increment(1);
            Ok(Some("done"))
        }
        Err(e) if job.attempts >= job.max_attempts => {
            sqlx::query!(
                "UPDATE jobs SET state='dead', last_error=$2, locked_at=NULL WHERE id=$1",
                job.id,
                e.to_string(),
            )
            .execute(pool)
            .await?;
            metrics::counter!(
                "rustyq_jobs_finished_total",
                "state" => "dead"
            )
            .increment(1);
            Ok(Some("dead"))
        }
        Err(e) => {
            // Exponential backoff: 2^attempts seconds, capped at 1 hour.
            let shift = job.attempts.min(12) as u32;
            let delay = (1u64 << shift).min(3600) as i32;
            sqlx::query!(
                "UPDATE jobs SET state='queued', last_error=$2, \
                 run_at = now() + make_interval(secs => $3::int), locked_at=NULL \
                 WHERE id=$1",
                job.id,
                e.to_string(),
                delay,
            )
            .execute(pool)
            .await?;
            // Job requeued — not yet finished; caller may track separately if needed.
            Ok(None)
        }
    }
}
