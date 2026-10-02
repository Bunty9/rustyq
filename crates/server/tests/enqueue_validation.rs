//! Integration tests for `POST /jobs` input validation.
//!
//! Skipped when `TEST_DATABASE_URL` is unset (mirrors the pattern in
//! `crates/server/tests/status.rs`).

mod common;

use axum::http::{Request, StatusCode};
use rustyq_server::router;
use serde_json::json;
use tower::ServiceExt;

async fn post_jobs(app: axum::Router, body: serde_json::Value) -> StatusCode {
    let req = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .expect("build request");
    app.oneshot(req).await.expect("oneshot").status()
}

#[tokio::test]
async fn rejects_empty_queue() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let app = router(pool);
    let status = post_jobs(app, json!({ "queue": "", "kind": "noop", "payload": {} })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_empty_kind() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let app = router(pool);
    let status = post_jobs(
        app,
        json!({ "queue": "default", "kind": "", "payload": {} }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_negative_delay_secs() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let app = router(pool);
    let status = post_jobs(
        app,
        json!({ "queue": "default", "kind": "noop", "payload": {}, "delay_secs": -1 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_oversized_delay_secs() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let app = router(pool);
    let status = post_jobs(
        app,
        json!({
            "queue": "default",
            "kind": "noop",
            "payload": {},
            "delay_secs": i64::from(i32::MAX) + 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_out_of_range_max_attempts() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    for bad in [0, -1, 1001] {
        let status = post_jobs(
            router(pool.clone()),
            json!({ "queue": "default", "kind": "noop", "payload": {}, "max_attempts": bad }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "max_attempts={bad}");
    }
    let status = post_jobs(
        router(pool),
        json!({ "queue": "default", "kind": "noop", "payload": {}, "max_attempts": 1000 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn accepts_valid_request() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let app = router(pool);
    let status = post_jobs(
        app,
        json!({ "queue": "default", "kind": "noop", "payload": {}, "delay_secs": 5 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}
