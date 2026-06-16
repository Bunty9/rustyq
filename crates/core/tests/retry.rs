//! Integration test: fail-once handler — first attempt errors, second succeeds.
//!
//! Verifies that the Worker correctly retries a failed job and that the final
//! state is `done` with attempts == 2. Skipped when `TEST_DATABASE_URL` is
//! unset.

mod common;

use rustyq_core::{Job, Registry, Worker};
use sqlx::Row;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// A handler that fails on the first call for a given job id, succeeds on
/// subsequent calls. Behaviour matches the `fail_once` built-in handler.
#[derive(Clone)]
struct FailOnce {
    seen: Arc<Mutex<HashMap<Uuid, u8>>>,
}

impl FailOnce {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl rustyq_core::Handler for FailOnce {
    fn call(&self, job: &Job) -> rustyq_core::HandlerFut {
        let seen = self.seen.clone();
        let id = job.id;
        Box::pin(async move {
            let mut map = seen.lock().unwrap();
            let count = map.entry(id).or_insert(0);
            if *count == 0 {
                *count = 1;
                Err(anyhow::anyhow!("synthetic fail_once"))
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn fail_once_retries_to_done() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let handler = FailOnce::new();
    let registry = Registry::builder().register("fail_once", handler).build();

    // max_attempts=3 leaves room for retry. After claim, attempts becomes 1,
    // so finalize's backoff = 2^1 = 2s — fits within the 10s budget.
    let job_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, queue, kind, payload, state, max_attempts) \
         VALUES ($1, 'default', 'fail_once', '{}', 'queued', 3)",
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

    let final_state = timeout(Duration::from_secs(10), async {
        loop {
            let state: String = sqlx::query("SELECT state FROM jobs WHERE id=$1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("query")
                .get(0);
            match state.as_str() {
                "done" | "failed" | "dead" => return state,
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("job must settle within 10s");

    cancel.cancel();
    handle.await.expect("worker join");

    assert_eq!(final_state, "done", "fail_once should retry to done");

    let row = sqlx::query("SELECT attempts, last_error FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("query");
    let attempts: i32 = row.get(0);
    let last_error: Option<String> = row.get(1);

    assert_eq!(attempts, 2, "should have taken exactly 2 attempts");
    assert!(
        last_error.as_deref().unwrap_or("").contains("fail_once"),
        "last_error should record the first-attempt failure"
    );
}
