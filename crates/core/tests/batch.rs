//! Integration test: `Worker::claim_batch(N)` returns up to N rows in a
//! single round-trip, leaves the rest queued, and transitions the claimed
//! rows to `state='running'`.
//!
//! Skipped when `TEST_DATABASE_URL` is unset.

mod common;

use rustyq_core::{Registry, Worker};
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn claim_batch_drains_up_to_n_rows() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    // Insert 10 ready-to-run jobs.
    for _ in 0..10 {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO jobs (id, queue, kind, payload, state) \
             VALUES ($1, 'default', 'noop', '{}', 'queued')",
        )
        .bind(id)
        .execute(&pool)
        .await
        .expect("insert");
    }

    let worker = Worker::new(
        pool.clone(),
        "test-batcher".to_string(),
        vec!["default".to_string()],
        4,
        Arc::new(Registry::default()),
    );

    // Single batch claim of 4 should return exactly 4 rows.
    let batch = worker.claim_batch(4).await.expect("claim_batch");
    assert_eq!(batch.len(), 4, "should claim exactly 4 rows in one batch");

    // 4 should be running, 6 still queued.
    let running: i64 = sqlx::query("SELECT COUNT(*) FROM jobs WHERE state='running'")
        .fetch_one(&pool)
        .await
        .expect("count running")
        .get(0);
    let queued: i64 = sqlx::query("SELECT COUNT(*) FROM jobs WHERE state='queued'")
        .fetch_one(&pool)
        .await
        .expect("count queued")
        .get(0);
    assert_eq!(running, 4, "exactly 4 jobs should be in state=running");
    assert_eq!(queued, 6, "exactly 6 jobs should remain queued");

    // Asking for 20 with only 6 left returns 6.
    let rest = worker.claim_batch(20).await.expect("claim_batch rest");
    assert_eq!(rest.len(), 6, "should claim all remaining 6 rows");

    // Asking again with nothing queued returns empty.
    let empty = worker.claim_batch(4).await.expect("claim_batch empty");
    assert!(
        empty.is_empty(),
        "should return empty Vec when queue is drained"
    );
}

#[tokio::test]
async fn claim_batch_zero_returns_empty_without_query() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    // Even if jobs exist, asking for 0 should short-circuit.
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state) \
         VALUES ($1, 'default', 'noop', '{}', 'queued')",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("insert");

    let worker = Worker::new(
        pool.clone(),
        "test-batcher-zero".to_string(),
        vec!["default".to_string()],
        4,
        Arc::new(Registry::default()),
    );

    let batch = worker.claim_batch(0).await.expect("claim_batch 0");
    assert!(batch.is_empty());

    // Job must remain queued, untouched.
    let queued: i64 = sqlx::query("SELECT COUNT(*) FROM jobs WHERE state='queued'")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    assert_eq!(queued, 1);
}
