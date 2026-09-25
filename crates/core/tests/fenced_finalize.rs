//! Integration test: `finalize` is fenced on `state='running' AND
//! locked_by=$worker_id AND attempts=$attempts`. If a job is reaped from a slow-but-not-dead
//! worker and re-claimed by another, the original worker's finalize call
//! must not clobber the new owner's row. Skipped when `TEST_DATABASE_URL`
//! is unset.

mod common;

use rustyq_core::{finalize, finalize_done_batch, reap_stale, Registry, Worker};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn finalize_skips_when_lock_was_stolen() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let job_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state, max_attempts) \
         VALUES ($1, 'default', 'noop', '{}', 'queued', 5)",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("insert");

    let worker_a = Worker::new(
        pool.clone(),
        "worker-a".to_string(),
        vec!["default".to_string()],
        4,
        Arc::new(Registry::default()),
    );
    let claimed = worker_a.claim_batch(1).await.expect("claim by A");
    let job = claimed.into_iter().next().expect("A claimed the job");

    // Simulate A stalling long enough to be presumed dead: backdate its
    // lock past the reap threshold, then reap and let B re-claim it.
    sqlx::query("UPDATE jobs SET locked_at = now() - interval '600 seconds' WHERE id=$1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("backdate");

    let reaped = reap_stale(&pool, Duration::from_secs(300))
        .await
        .expect("reap_stale");
    assert_eq!(reaped, 1);

    let worker_b = Worker::new(
        pool.clone(),
        "worker-b".to_string(),
        vec!["default".to_string()],
        4,
        Arc::new(Registry::default()),
    );
    let reclaimed = worker_b.claim_batch(1).await.expect("claim by B");
    assert_eq!(reclaimed.len(), 1, "B should reclaim the reaped job");

    // A is not actually dead — it finishes its (stale) run and calls
    // finalize with its own id. This must be a no-op: B now owns the row.
    let result = finalize(&pool, "worker-a", &job, Ok(()))
        .await
        .expect("finalize");
    assert!(
        result.is_none(),
        "finalize must report no terminal state once the lock was lost"
    );

    let row = sqlx::query("SELECT state, locked_by FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("query");
    let state: String = row.get(0);
    let locked_by: Option<String> = row.get(1);
    assert_eq!(state, "running", "row must still be running, owned by B");
    assert_eq!(locked_by.as_deref(), Some("worker-b"));
}

/// Same scenario, but the job is re-claimed by the *same* worker id — the
/// common case for single-worker deployments or a fixed `RUSTYQ_WORKER_ID`.
/// `locked_by` alone cannot tell the two runs apart; `attempts` can.
#[tokio::test]
async fn finalize_skips_when_same_worker_reclaimed() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let job_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state, max_attempts) \
         VALUES ($1, 'default', 'noop', '{}', 'queued', 5)",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("insert");

    let worker = Worker::new(
        pool.clone(),
        "worker-a".to_string(),
        vec!["default".to_string()],
        4,
        Arc::new(Registry::default()),
    );
    let stale = worker.claim_batch(1).await.expect("first claim");
    let stale = stale.into_iter().next().expect("claimed the job");

    sqlx::query("UPDATE jobs SET locked_at = now() - interval '600 seconds' WHERE id=$1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("backdate");
    assert_eq!(
        reap_stale(&pool, Duration::from_secs(300)).await.unwrap(),
        1
    );
    let fresh = worker.claim_batch(1).await.expect("second claim");
    assert_eq!(fresh.len(), 1, "same worker should reclaim the reaped job");

    // The stale run finishing must not mark the row done under the fresh run.
    let result = finalize(&pool, "worker-a", &stale, Ok(()))
        .await
        .expect("finalize");
    assert!(result.is_none(), "stale run must lose the fence");

    let state: String = sqlx::query("SELECT state FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("query")
        .get(0);
    assert_eq!(state, "running");

    // The fresh run still finalizes normally.
    let fresh = fresh.into_iter().next().unwrap();
    let result = finalize(&pool, "worker-a", &fresh, Ok(()))
        .await
        .expect("finalize");
    assert_eq!(result, Some("done"));
}

/// The batched success path applies the same fence per row: a stale entry
/// in the batch is skipped while the fresh ones are marked done.
#[tokio::test]
async fn batch_finalize_skips_stale_entries() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let ids: Vec<Uuid> = (0..3).map(|_| Uuid::now_v7()).collect();
    sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state) \
         SELECT id, 'default', 'noop', '{}', 'queued' FROM UNNEST($1::uuid[]) AS id",
    )
    .bind(&ids)
    .execute(&pool)
    .await
    .expect("insert");

    let worker = Worker::new(
        pool.clone(),
        "worker-a".to_string(),
        vec!["default".to_string()],
        4,
        Arc::new(Registry::default()),
    );
    let claimed = worker.claim_batch(3).await.expect("claim");
    assert_eq!(claimed.len(), 3);
    let mut batch: Vec<(Uuid, i32)> = claimed.iter().map(|j| (j.id, j.attempts)).collect();
    // Pretend the first entry comes from an earlier, since-reaped run.
    batch[0].1 -= 1;

    let n = finalize_done_batch(&pool, "worker-a", &batch)
        .await
        .expect("batch finalize");
    assert_eq!(n, 2);

    let running: i64 = sqlx::query("SELECT count(*) FROM jobs WHERE state='running'")
        .fetch_one(&pool)
        .await
        .expect("query")
        .get(0);
    assert_eq!(running, 1, "stale entry must leave its row untouched");
}
