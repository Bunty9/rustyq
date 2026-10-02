//! `Permanent` errors skip the retry budget. Skipped when
//! `TEST_DATABASE_URL` is unset.

mod common;

use rustyq_core::{enqueue, finalize, job_status, permanent, NewJob, Registry, Worker};
use std::sync::Arc;

async fn run_once(err: anyhow::Error) -> Option<(String, i32, Option<String>)> {
    let pool = common::setup_pool().await?;
    let id = enqueue(
        &pool,
        &NewJob::new("default", "k", serde_json::json!({})).max_attempts(5),
    )
    .await
    .unwrap();
    let worker = Worker::new(
        pool.clone(),
        "w".into(),
        vec!["default".into()],
        1,
        Arc::new(Registry::builder().build()),
    );
    let job = worker.claim_one().await.unwrap().expect("claimed");
    finalize(&pool, "w", &job, Err(err)).await.unwrap();
    let st = job_status(&pool, id).await.unwrap().unwrap();
    Some((st.state, st.attempts, st.last_error))
}

#[tokio::test]
async fn permanent_goes_dead_after_one_attempt() {
    let Some((state, attempts, err)) = run_once(permanent(anyhow::anyhow!("bad input"))).await
    else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    assert_eq!((state.as_str(), attempts), ("dead", 1));
    assert!(err.unwrap().contains("bad input"));
}

#[tokio::test]
async fn permanent_seen_through_context() {
    let e = permanent(anyhow::anyhow!("bad input")).context("while sending");
    let Some((state, attempts, err)) = run_once(e).await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    assert_eq!((state.as_str(), attempts), ("dead", 1));
    let err = err.unwrap();
    assert!(
        err.contains("while sending") && err.contains("bad input"),
        "{err}"
    );
}

#[tokio::test]
async fn ordinary_error_is_requeued_with_chain() {
    let e = anyhow::anyhow!("boom").context("outer");
    let Some((state, attempts, err)) = run_once(e).await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    assert_eq!((state.as_str(), attempts), ("queued", 1));
    assert_eq!(err.as_deref(), Some("outer: boom"));
}
