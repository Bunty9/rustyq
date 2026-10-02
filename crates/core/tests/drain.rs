//! Integration test: a concurrency-limited worker must keep draining a
//! queue that's already full of work, without idling a full poll interval
//! between batches while permits free up. Regression test for the
//! "break when permits exhausted" starvation bug. Skipped when
//! `TEST_DATABASE_URL` is unset.

mod common;

use rustyq_core::{HandlerFut, Job, Registry, Worker};
use sqlx::Row;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn concurrency_limited_drain_does_not_idle_between_batches() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let registry = Registry::builder()
        .register("drain_sleep", |_job: &Job| -> HandlerFut {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            })
        })
        .build();

    // Single INSERT, no NOTIFY: the worker only learns about this work from
    // its 1s poll fallback, then must keep draining as permits free up
    // rather than idling again for another full poll interval.
    let ids: Vec<Uuid> = (0..20).map(|_| Uuid::now_v7()).collect();
    sqlx::query(
        "INSERT INTO rustyq_jobs (id, queue, kind, payload, state) \
         SELECT id, 'default', 'drain_sleep', '{}', 'queued' FROM UNNEST($1::uuid[]) AS id",
    )
    .bind(&ids)
    .execute(&pool)
    .await
    .expect("insert");

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let pool_clone = pool.clone();
    let handle = tokio::spawn(async move {
        Worker::new(
            pool_clone,
            "test-drain".to_string(),
            vec!["default".to_string()],
            2,
            Arc::new(registry),
        )
        .run(cancel_clone)
        .await
        .expect("worker run");
    });

    // 20 jobs at ~50ms each with concurrency=2 need ~0.5s of processing
    // once the worker notices the queue (~1s poll, since there's no
    // NOTIFY). The old "break when permits==0" loop idled a full 1s poll
    // interval between every pair of jobs — ~10s total. The 6s bound leaves
    // generous headroom for a loaded CI box while still catching that.
    let done = timeout(Duration::from_secs(6), async {
        loop {
            let n: i64 = sqlx::query("SELECT COUNT(*) FROM rustyq_jobs WHERE state='done'")
                .fetch_one(&pool)
                .await
                .expect("query")
                .get(0);
            if n >= 20 {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all 20 jobs must drain within 6s under the fixed dispatch loop");

    cancel.cancel();
    handle.await.expect("worker join");

    assert_eq!(done, 20);
}
