//! Integration test for `GET /healthz`.
//!
//! Skipped when `TEST_DATABASE_URL` is unset (mirrors the pattern in
//! `crates/server/tests/status.rs`).

mod common;

use axum::http::{Request, StatusCode};
use rustyq_server::router;
use tower::ServiceExt;

#[tokio::test]
async fn healthz_returns_200_ok() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    let app = router(pool);
    let req = Request::builder()
        .method("GET")
        .uri("/healthz")
        .body(axum::body::Body::empty())
        .expect("build request");

    let resp = app.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
}
