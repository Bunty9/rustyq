//! Integration tests for `reap_stale`: requeues jobs whose lock is older
//! than `lock_timeout` (their worker is presumed dead), or moves them to
//! `dead` if their attempt budget is already exhausted. Skipped when
//! `TEST_DATABASE_URL` is unset.

mod common;

use rustyq_core::{reap_stale, Registry, Worker};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn stale_lock_is_reaped_to_queued() {
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
    let claimed = worker.claim_batch(1).await.expect("claim");
    assert_eq!(claimed.len(), 1);

    // Backdate the lock so it looks like the claiming worker died.
    sqlx::query("UPDATE jobs SET locked_at = now() - interval '120 seconds' WHERE id=$1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("backdate");

    let n = reap_stale(&pool, Duration::from_secs(60))
        .await
        .expect("reap_stale");
    assert_eq!(n, 1, "exactly one stale row should be reaped");

    let row = sqlx::query("SELECT state, locked_at IS NULL AS lock_cleared FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("query");
    let state: String = row.get(0);
    let lock_cleared: bool = row.get(1);
    assert_eq!(state, "queued", "reaped job should go back to queued");
    assert!(lock_cleared, "locked_at should be cleared");
}

#[tokio::test]
async fn fresh_lock_is_not_reaped() {
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
    let claimed = worker.claim_batch(1).await.expect("claim");
    assert_eq!(claimed.len(), 1, "lock is fresh, just claimed");

    let n = reap_stale(&pool, Duration::from_secs(60))
        .await
        .expect("reap_stale");
    assert_eq!(n, 0, "a fresh lock must not be reaped");

    let state: String = sqlx::query("SELECT state FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("query")
        .get(0);
    assert_eq!(state, "running");
}

#[tokio::test]
async fn stale_exhausted_job_goes_dead() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    // max_attempts=1: the single claim below already exhausts the budget.
    let job_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state, max_attempts) \
         VALUES ($1, 'default', 'noop', '{}', 'queued', 1)",
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
    let claimed = worker.claim_batch(1).await.expect("claim");
    assert_eq!(claimed[0].attempts, 1, "attempts should equal max_attempts");

    sqlx::query("UPDATE jobs SET locked_at = now() - interval '120 seconds' WHERE id=$1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("backdate");

    let n = reap_stale(&pool, Duration::from_secs(60))
        .await
        .expect("reap_stale");
    assert_eq!(n, 1);

    let state: String = sqlx::query("SELECT state FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("query")
        .get(0);
    assert_eq!(
        state, "dead",
        "exhausted stale job should go dead, not queued"
    );
}
