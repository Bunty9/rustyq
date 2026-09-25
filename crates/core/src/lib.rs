//! rustyq-core — types, worker dispatch loop, claim/finalize transitions.
//!
//! The worker is the heart of rustyq. It claims one job at a time from
//! Postgres via `FOR UPDATE SKIP LOCKED`, runs it, and finalizes the row
//! based on outcome (done / requeue with exponential backoff / dead).

pub mod handler;
pub mod telemetry;
pub use handler::{Handler, HandlerFut, Registry, RegistryBuilder};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{postgres::PgListener, FromRow, PgPool};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::time::{interval, sleep, timeout, Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
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
    /// How long `run()` waits, after `cancel` fires, for in-flight jobs to
    /// finish before returning. Public so callers can override the 30s
    /// default. A warning is logged if the grace period elapses with jobs
    /// still running.
    pub shutdown_grace: Duration,
    /// A `running` job whose lock is older than this is presumed to belong
    /// to a dead worker and is reaped back to `queued` (or `dead`, if its
    /// attempt budget is exhausted). NOTE: this gives rustyq at-least-once
    /// semantics — a handler that legitimately runs longer than
    /// `lock_timeout` will be reaped and re-run by another worker while the
    /// original run is still in flight.
    pub lock_timeout: Duration,
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
            shutdown_grace: Duration::from_secs(30),
            lock_timeout: Duration::from_secs(300),
        }
    }

    /// Main dispatch loop. Returns once all in-flight jobs have drained
    /// after `cancel.cancelled()` (bounded by `shutdown_grace`).
    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("rustyq_new").await?;

        let sem = Arc::new(Semaphore::new(self.concurrency));

        // Successful jobs are finalized in batches: job tasks hand
        // `(id, attempts)` to this task, which marks everything that has
        // piled up `done` in one UPDATE. Under load that turns one
        // statement + commit per job into one per batch; when idle a batch
        // is a single job, so no latency is added. The bounded channel
        // backpressures handlers if Postgres falls behind.
        let (done_tx, mut done_rx) = mpsc::channel::<(Uuid, i32)>(DONE_BATCH_MAX);
        let finalizer = {
            let pool = self.pool.clone();
            let id = self.id.clone();
            tokio::spawn(async move {
                let mut batch = Vec::with_capacity(DONE_BATCH_MAX);
                while done_rx.recv_many(&mut batch, DONE_BATCH_MAX).await > 0 {
                    // Retry transient errors a few times; if the batch still
                    // fails the rows stay `running` and the reaper requeues
                    // them (at-least-once: they re-run).
                    for attempt in 1..=3 {
                        match finalize_done_batch(&pool, &id, &batch).await {
                            Ok(_) => break,
                            Err(e) => {
                                tracing::warn!(
                                    ?e,
                                    attempt,
                                    n = batch.len(),
                                    "batch finalize failed"
                                );
                                sleep(Duration::from_millis(100 * attempt)).await;
                            }
                        }
                    }
                    batch.clear();
                }
            })
        };
        let registry = self.registry.clone();

        // Periodic stale-lock reaper on its own task: the dispatch loop can
        // sit in the inner drain loop for as long as the queue has work, so
        // reaping from there would starve exactly when it matters.
        let reaper = {
            let pool = self.pool.clone();
            let lock_timeout = self.lock_timeout;
            let cancel = cancel.clone();
            let mut tick = interval((lock_timeout / 2).max(Duration::from_secs(1)));
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tick.tick() => {}
                    }
                    match reap_stale(&pool, lock_timeout).await {
                        Ok(0) => {}
                        Ok(n) => tracing::info!(reaped = n, "reaped stale locks"),
                        Err(e) => tracing::warn!(?e, "reap_stale failed"),
                    }
                }
            })
        };

        'outer: loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                // Wake on enqueue notify. PgListener reconnects on the next
                // recv(), so on error just back off instead of spinning.
                res = listener.recv() => if let Err(e) = res {
                    tracing::warn!(?e, "LISTEN connection error");
                    sleep(Duration::from_secs(1)).await;
                },
                _ = sleep(Duration::from_secs(1)) => {}, // fallback poll
            }
            // Drain greedily: claim_batch reads up to the available job
            // slots in a single UPDATE, then we hand each row to its own
            // task gated by a semaphore permit. The inner loop terminates
            // when the batch is shorter than requested (queue empty) — no
            // need for a separate "empty?" round-trip.
            loop {
                let n = sem.available_permits();
                // When every permit is busy, don't idle until the next
                // NOTIFY/poll — wait right here for a job to finish so the
                // queue keeps draining. Hold that permit for the first job
                // in the next batch so it isn't handed out from under us.
                let (claim_n, mut held_permit): (usize, Option<OwnedSemaphorePermit>) = if n == 0 {
                    tokio::select! {
                        _ = cancel.cancelled() => break 'outer,
                        acquired = sem.clone().acquire_owned() => {
                            let permit = acquired?;
                            (1 + sem.available_permits(), Some(permit))
                        }
                    }
                } else {
                    (n, None)
                };
                // A transient DB error must not kill the worker: log it and
                // fall back to the outer wait (NOTIFY or 1 s poll) to retry.
                let batch = match self.claim_batch(claim_n).await {
                    Ok(batch) => batch,
                    Err(e) => {
                        tracing::warn!(?e, "claim_batch failed");
                        break;
                    }
                };
                if batch.is_empty() {
                    // `held_permit` (if any) drops here, releasing it back
                    // to the semaphore — no leak.
                    break;
                }
                let batch_len = batch.len();
                for job in batch {
                    // Reuse the permit we already hold for the first job;
                    // acquire fresh ones for the rest. `claim_batch` bounded
                    // its query by the available slots before the batch was
                    // claimed, so these are uncontended in the steady state.
                    let permit = match held_permit.take() {
                        Some(p) => p,
                        None => sem.clone().acquire_owned().await?,
                    };
                    let claimed_at = Utc::now();
                    let pool = self.pool.clone();
                    let id = self.id.clone();
                    let registry = registry.clone();
                    let done_tx = done_tx.clone();
                    let delay_ms = (claimed_at - job.created_at).num_milliseconds();
                    let span = tracing::info_span!(
                        "run_job",
                        "job.id" = %job.id,
                        "job.kind" = %job.kind,
                        "job.queue" = %job.queue,
                        "job.attempts" = job.attempts,
                        "job.delay_ms" = delay_ms,
                    );
                    tokio::spawn(
                        async move {
                            metrics::counter!(
                                "rustyq_jobs_claimed_total",
                                "queue" => job.queue.clone(),
                                "worker" => id.clone()
                            )
                            .increment(1);

                            let latency_secs = delay_ms as f64 / 1000.0;
                            metrics::histogram!(
                                "rustyq_dispatch_latency_seconds",
                                "queue" => job.queue.clone()
                            )
                            .record(latency_secs.max(0.0));

                            tracing::debug!("dispatching");
                            let started = Instant::now();
                            let result = registry.dispatch(&job).await;
                            let run_secs = started.elapsed().as_secs_f64();

                            metrics::histogram!(
                                "rustyq_job_run_duration_seconds",
                                "queue" => job.queue.clone(),
                                "kind"  => job.kind.clone()
                            )
                            .record(run_secs);

                            if result.is_ok() {
                                // Only fails if the finalizer is gone, i.e.
                                // shutdown outran the grace period; the row
                                // stays `running` for the reaper.
                                let _ = done_tx.send((job.id, job.attempts)).await;
                            } else {
                                let final_result = finalize(&pool, &id, &job, result).await;
                                match &final_result {
                                    Ok(state) => tracing::debug!(?state, "finalized"),
                                    Err(e) => {
                                        tracing::error!(?e, job_id = %job.id, "finalize failed")
                                    }
                                }
                            }
                            drop(permit);
                        }
                        .instrument(span),
                    );
                }
                // If we got fewer than asked, queue is drained — stop the
                // greedy loop and go back to LISTEN/NOTIFY waiting.
                if batch_len < claim_n {
                    break;
                }
            }
        }

        // Graceful shutdown: process exit (Docker/Fly send SIGTERM, not just
        // Ctrl-C) would otherwise kill spawned job tasks mid-flight, leaving
        // their rows stuck in `running`. Wait for every permit to free up —
        // i.e. every in-flight job to finish — bounded by `shutdown_grace`.
        if timeout(
            self.shutdown_grace,
            sem.acquire_many(self.concurrency as u32),
        )
        .await
        .is_err()
        {
            tracing::warn!(
                grace = ?self.shutdown_grace,
                "shutdown grace period elapsed with jobs still in flight"
            );
        }
        // Every job task that finished has queued its result; closing our
        // sender lets the finalizer flush the last batch and exit.
        drop(done_tx);
        if timeout(Duration::from_secs(10), finalizer).await.is_err() {
            tracing::warn!("finalizer did not flush within 10s");
        }
        let _ = reaper.await;
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

/// Requeue jobs whose lock is older than `lock_timeout` (their worker is
/// presumed dead). Jobs that already used their attempt budget go to `dead`
/// instead. Returns the number of rows touched.
pub async fn reap_stale(pool: &PgPool, lock_timeout: Duration) -> sqlx::Result<u64> {
    let result = sqlx::query!(
        r#"
        UPDATE jobs
        SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
            locked_at = NULL,
            last_error = COALESCE(last_error, 'lock expired: worker presumed dead')
        WHERE state='running' AND locked_at < now() - make_interval(secs => $1)
        "#,
        lock_timeout.as_secs_f64(),
    )
    .execute(pool)
    .await?;
    let n = result.rows_affected();
    metrics::counter!("rustyq_jobs_reaped_total").increment(n);
    Ok(n)
}

/// Upper bound on jobs marked `done` per batched UPDATE (and the capacity of
/// the worker's done channel).
const DONE_BATCH_MAX: usize = 512;

/// Mark a batch of successfully handled jobs `done` in one statement. Each
/// `(id, attempts)` pair is fenced like [`finalize`]: rows reaped and
/// re-claimed since (by any worker, including this one) are skipped. Returns
/// the number of rows marked done.
pub async fn finalize_done_batch(
    pool: &PgPool,
    worker_id: &str,
    jobs: &[(Uuid, i32)],
) -> sqlx::Result<u64> {
    let (ids, attempts): (Vec<Uuid>, Vec<i32>) = jobs.iter().copied().unzip();
    let n = sqlx::query!(
        r#"
        UPDATE jobs SET state='done', locked_at=NULL
        FROM UNNEST($2::uuid[], $3::int4[]) AS f(id, attempts)
        WHERE jobs.id=f.id AND jobs.attempts=f.attempts
          AND jobs.state='running' AND jobs.locked_by=$1
        "#,
        worker_id,
        &ids,
        &attempts,
    )
    .execute(pool)
    .await?
    .rows_affected();
    if (n as usize) < jobs.len() {
        tracing::warn!(
            lost = jobs.len() - n as usize,
            worker_id,
            "lost lock, finalize skipped"
        );
    }
    metrics::counter!("rustyq_jobs_finished_total", "state" => "done").increment(n);
    Ok(n)
}

/// Finalize a claimed job. On success → `done`. On error past the attempt
/// budget → `dead`. Otherwise → requeue with exponential backoff
/// (2^attempts, capped at 1 hour).
///
/// Every update is fenced on `state='running' AND locked_by=$worker_id AND
/// attempts=$job.attempts`: if the job was reaped (see [`reap_stale`]) and
/// re-claimed — by another worker, or by this same worker id, since every
/// claim bumps `attempts` — while this run was still (slowly) going, the
/// update touches zero rows and this returns `Ok(None)` without recording a terminal-state
/// metric — the new owner's row must not be clobbered.
///
/// Returns the terminal state label for metrics (`Some("done")`,
/// `Some("dead")`) or `None` when the job was requeued (not yet finished) or
/// its lock was lost to another worker.
pub async fn finalize(
    pool: &PgPool,
    worker_id: &str,
    job: &Job,
    res: anyhow::Result<()>,
) -> sqlx::Result<Option<&'static str>> {
    match res {
        Ok(()) => {
            let result = sqlx::query!(
                "UPDATE jobs SET state='done', locked_at=NULL \
                 WHERE id=$1 AND state='running' AND locked_by=$2 AND attempts=$3",
                job.id,
                worker_id,
                job.attempts,
            )
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                tracing::warn!(job_id = %job.id, worker_id, "lost lock, finalize skipped");
                return Ok(None);
            }
            metrics::counter!(
                "rustyq_jobs_finished_total",
                "state" => "done"
            )
            .increment(1);
            Ok(Some("done"))
        }
        Err(e) if job.attempts >= job.max_attempts => {
            let result = sqlx::query!(
                "UPDATE jobs SET state='dead', last_error=$2, locked_at=NULL \
                 WHERE id=$1 AND state='running' AND locked_by=$3 AND attempts=$4",
                job.id,
                e.to_string(),
                worker_id,
                job.attempts,
            )
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                tracing::warn!(job_id = %job.id, worker_id, "lost lock, finalize skipped");
                return Ok(None);
            }
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
            let result = sqlx::query!(
                "UPDATE jobs SET state='queued', last_error=$2, \
                 run_at = now() + make_interval(secs => $3::int), locked_at=NULL \
                 WHERE id=$1 AND state='running' AND locked_by=$4 AND attempts=$5",
                job.id,
                e.to_string(),
                delay,
                worker_id,
                job.attempts,
            )
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                tracing::warn!(job_id = %job.id, worker_id, "lost lock, finalize skipped");
            }
            // Job requeued — not yet finished; caller may track separately if needed.
            Ok(None)
        }
    }
}
