//! rustyq-core — types, worker dispatch loop, claim/finalize transitions.
//!
//! The worker is the heart of rustyq. It claims one job at a time from
//! Postgres via `FOR UPDATE SKIP LOCKED`, runs it, and finalizes the row
//! based on outcome (done / requeue with exponential backoff / dead).

use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{postgres::PgListener, FromRow, PgPool};
use std::sync::Arc;
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
}

impl Worker {
    pub fn new(pool: PgPool, id: String, queues: Vec<String>, concurrency: usize) -> Self {
        Self {
            pool,
            id,
            queues,
            concurrency,
        }
    }

    /// Main dispatch loop. Returns on `cancel.cancelled()`.
    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("rustyq_new").await?;

        let sem = Arc::new(Semaphore::new(self.concurrency));
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
                    if let Err(e) = finalize(&pool, &id, &job, result).await {
                        tracing::error!(?e, job_id = %job.id, "finalize failed");
                    }
                    drop(permit);
                });
            }
        }
        Ok(())
    }

    /// Claim exactly one runnable job. `SKIP LOCKED` makes this safe under
    /// many parallel workers — no contention, no row locks held across the
    /// network round-trip.
    pub async fn claim_one(&self) -> sqlx::Result<Option<Job>> {
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

/// Placeholder job runner — Phase 1 has no real job dispatch yet. Returning
/// `Ok(())` lets the finalize machinery be exercised end-to-end with
/// `enqueue → claim → done`.
async fn run_job(_job: &Job) -> anyhow::Result<()> {
    // TODO(phase-2): dispatch by `job.kind` to a registered handler.
    Ok(())
}

/// Finalize a claimed job. On success → `done`. On error past the attempt
/// budget → `dead`. Otherwise → requeue with exponential backoff
/// (2^attempts, capped at 1 hour).
pub async fn finalize(
    pool: &PgPool,
    _id: &str,
    job: &Job,
    res: anyhow::Result<()>,
) -> sqlx::Result<()> {
    match res {
        Ok(()) => {
            sqlx::query("UPDATE jobs SET state='done', locked_at=NULL WHERE id=$1")
                .bind(job.id)
                .execute(pool)
                .await?;
        }
        Err(e) if job.attempts >= job.max_attempts => {
            sqlx::query(
                "UPDATE jobs SET state='dead', last_error=$2, locked_at=NULL WHERE id=$1",
            )
            .bind(job.id)
            .bind(e.to_string())
            .execute(pool)
            .await?;
        }
        Err(e) => {
            // Exponential backoff: 2^attempts seconds, capped at 1 hour.
            let shift = job.attempts.min(12) as u32;
            let delay = (1u64 << shift).min(3600) as i32;
            sqlx::query(
                "UPDATE jobs SET state='queued', last_error=$2, \
                 run_at = now() + make_interval(secs => $3::int), locked_at=NULL \
                 WHERE id=$1",
            )
            .bind(job.id)
            .bind(e.to_string())
            .bind(delay)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}
