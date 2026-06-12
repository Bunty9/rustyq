//! Integration test: happy-path dispatch — 5 jobs all reach state=done.
//!
//! Skipped when `TEST_DATABASE_URL` is unset. Otherwise applies the schema
//! migration to an isolated PG schema and verifies that a Worker with a
//! counting handler processes all enqueued jobs.

mod common;

use rustyq_core::{HandlerFut, Job, Registry, Worker};
use sqlx::Row;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn dispatch_happy_path() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let counter = Arc::new(AtomicUsize::new(0));
    let counter2 = counter.clone();
    let registry = Registry::builder()
        .register("count", move |_job: &Job| -> HandlerFut {
            let c = counter2.clone();
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })
        .build();

    for _ in 0..5 {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO jobs (id, queue, kind, payload, state) \
             VALUES ($1, 'default', 'count', '{}', 'queued')",
        )
        .bind(id)
        .execute(&pool)
        .await
        .expect("insert");
    }

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let pool_clone = pool.clone();
    let handle = tokio::spawn(async move {
        Worker::new(
            pool_clone,
            "test-worker".to_string(),
            vec!["default".to_string()],
            4,
            Arc::new(registry),
        )
        .run(cancel_clone)
        .await
        .expect("worker run");
    });

    // Poll the DB until all rows are finalized. Polling the counter alone
    // races finalize(): the handler increments the counter *before* finalize
    // writes state='done', so cancelling on counter==5 can leave rows in
    // state='running'.
    let done_count = timeout(Duration::from_secs(10), async {
        loop {
            let n: i64 = sqlx::query("SELECT COUNT(*) FROM jobs WHERE state='done'")
                .fetch_one(&pool)
                .await
                .expect("query")
                .get(0);
            if n >= 5 {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("5 jobs must reach state=done within 10s");

    cancel.cancel();
    handle.await.expect("worker join");

    assert_eq!(counter.load(Ordering::SeqCst), 5);
    assert_eq!(done_count, 5, "all 5 jobs should be done");
}
