//! Embedder API: `enqueue` with pool / transaction, `NewJob` options,
//! `Job::payload_as`. Skipped when `TEST_DATABASE_URL` is unset.

mod common;

use rustyq_core::{enqueue, job_status, NewJob, Worker};
use serde::Deserialize;
use sqlx::postgres::PgListener;
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn commit_stores_row_and_notifies() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let mut listener = PgListener::connect_with(&pool).await.expect("listener");
    listener.listen("rustyq_new").await.expect("listen");

    let mut tx = pool.begin().await.expect("begin");
    let id = enqueue(
        &mut *tx,
        &NewJob::new("default", "noop", serde_json::json!({})),
    )
    .await
    .expect("enqueue");
    tx.commit().await.expect("commit");

    tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("notification within 5s")
        .expect("recv");
    let st = job_status(&pool, id).await.expect("status").expect("row");
    assert_eq!(st.state, "queued");
}

#[tokio::test]
async fn rollback_leaves_no_row() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let mut tx = pool.begin().await.expect("begin");
    let id = enqueue(
        &mut *tx,
        &NewJob::new("default", "noop", serde_json::json!({})),
    )
    .await
    .expect("enqueue");
    tx.rollback().await.expect("rollback");
    assert!(job_status(&pool, id).await.expect("status").is_none());
}

#[tokio::test]
async fn options_are_stored() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let id = enqueue(
        &pool,
        &NewJob::new("q", "k", serde_json::json!({ "a": 1 }))
            .priority(7)
            .delay(Duration::from_millis(90_500))
            .max_attempts(9),
    )
    .await
    .expect("enqueue");
    let row = sqlx::query(
        "SELECT priority, max_attempts, run_at > now() + interval '60 seconds' AS later, \
         run_at < now() + interval '120 seconds' AS sooner FROM jobs WHERE id=$1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .expect("row");
    assert_eq!(row.get::<i16, _>("priority"), 7);
    assert_eq!(row.get::<i32, _>("max_attempts"), 9);
    assert!(row.get::<bool, _>("later") && row.get::<bool, _>("sooner"));

    let id = enqueue(&pool, &NewJob::new("q", "k", serde_json::json!(null)))
        .await
        .expect("enqueue defaults");
    let st = job_status(&pool, id).await.unwrap().unwrap();
    assert_eq!(st.max_attempts, 5);
}

#[derive(Debug, Deserialize, PartialEq)]
struct Payload {
    to: String,
    n: u32,
}

#[tokio::test]
async fn payload_as_round_trips() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    enqueue(
        &pool,
        &NewJob::new("default", "k", serde_json::json!({ "to": "a@b", "n": 3 })),
    )
    .await
    .expect("enqueue");
    let worker = Worker::new(
        pool.clone(),
        "w".into(),
        vec!["default".into()],
        1,
        Arc::new(rustyq_core::Registry::builder().build()),
    );
    let job = worker.claim_one().await.unwrap().expect("claimed");
    assert_eq!(
        job.payload_as::<Payload>().unwrap(),
        Payload {
            to: "a@b".into(),
            n: 3
        }
    );
    assert!(job.payload_as::<Vec<u8>>().is_err());
}
