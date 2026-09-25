//! Integration test: cancelling `Worker::run` must wait for in-flight jobs
//! to finish before returning, instead of abandoning them mid-flight.
//! Skipped when `TEST_DATABASE_URL` is unset.

mod common;

use rustyq_core::{HandlerFut, Job, Registry, Worker};
use sqlx::Row;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn cancel_waits_for_in_flight_job_to_finish() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let registry = Registry::builder()
        .register("shutdown_sleep", |_job: &Job| -> HandlerFut {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(())
            })
        })
        .build();

    let job_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state) \
         VALUES ($1, 'default', 'shutdown_sleep', '{}', 'queued')",
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
            "test-shutdown".to_string(),
            vec!["default".to_string()],
            2,
            Arc::new(registry),
        )
        .run(cancel_clone)
        .await
        .expect("worker run");
    });

    // Wait until the job is picked up and running before cancelling.
    timeout(Duration::from_secs(5), async {
        loop {
            let state: String = sqlx::query("SELECT state FROM jobs WHERE id=$1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("query")
                .get(0);
            if state == "running" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("job must start running within 5s");

    // Cancel while the job is still mid-flight (~500ms sleep). run() must
    // wait for it to finish before returning.
    cancel.cancel();
    timeout(Duration::from_secs(5), handle)
        .await
        .expect("worker run() must return within 5s of cancel")
        .expect("worker join");

    let state: String = sqlx::query("SELECT state FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("query")
        .get(0);
    assert_eq!(
        state, "done",
        "in-flight job must finish before shutdown completes"
    );
}
