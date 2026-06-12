//! Integration test for `GET /jobs/:id` status endpoint.
//!
//! Skipped when `TEST_DATABASE_URL` is unset.

mod common;

use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use rustyq_server::router;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn status_returns_job_state() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    // Enqueue via POST /jobs so the row matches what a real client sees.
    let app = router(pool.clone());
    let enqueue = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({
                "queue": "default",
                "kind": "noop",
                "payload": {"hi": 1}
            })
            .to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(enqueue).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    let id: Uuid = parsed["id"]
        .as_str()
        .expect("id field")
        .parse()
        .expect("uuid parse");

    // Now GET /jobs/:id.
    let status_req = Request::builder()
        .method("GET")
        .uri(format!("/jobs/{id}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(status_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let status: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["id"], id.to_string());
    assert_eq!(status["state"], "queued");
    assert_eq!(status["attempts"], 0);
    assert!(status["last_error"].is_null());
    assert!(status["locked_by"].is_null());
    assert!(status["run_at"].is_string());
}

#[tokio::test]
async fn status_404_for_unknown_id() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let app = router(pool);
    let unknown = Uuid::now_v7();
    let req = Request::builder()
        .method("GET")
        .uri(format!("/jobs/{unknown}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
