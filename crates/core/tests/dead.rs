//! Integration test: always-fail handler — exhausts max_attempts and reaches
//! state=dead. Skipped when `TEST_DATABASE_URL` is unset.

mod common;

use rustyq_core::{HandlerFut, Job, Registry, Worker};
use sqlx::Row;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn always_fail_reaches_dead() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let registry = Registry::builder()
        .register("always_fail", |_job: &Job| -> HandlerFut {
            Box::pin(async move { Err(anyhow::anyhow!("nope")) })
        })
        .build();

    // max_attempts=2: first attempt -> fail -> requeue (backoff 2^1 = 2s),
    // second attempt -> fail -> dead.
    let job_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state, max_attempts) \
         VALUES ($1, 'default', 'always_fail', '{}', 'queued', 2)",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("insert");

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let pool_clone = pool.clone();
    let handle = tokio::spawn(async move {
        Worker::new(
            pool_clone,
            "test-worker".to_string(),
            vec!["default".to_string()],
            2,
            Arc::new(registry),
        )
        .run(cancel_clone)
        .await
        .expect("worker run");
    });

    let final_state = timeout(Duration::from_secs(15), async {
        loop {
            let state: String = sqlx::query("SELECT state FROM jobs WHERE id=$1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("query")
                .get(0);
            match state.as_str() {
                "dead" | "failed" => return state,
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("job must reach dead within 15s");

    cancel.cancel();
    handle.await.expect("worker join");

    assert_eq!(final_state, "dead", "exhausted job must be dead");

    let row = sqlx::query("SELECT attempts, last_error FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("query");
    let attempts: i32 = row.get(0);
    let last_error: Option<String> = row.get(1);

    assert_eq!(attempts, 2, "dead job must have 2 attempts");
    assert!(
        last_error.as_deref().unwrap_or("").contains("nope"),
        "last_error must contain the error message"
    );
}
