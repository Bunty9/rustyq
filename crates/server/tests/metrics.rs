//! Integration tests for the Prometheus `/metrics` endpoint.
//!
//! Skipped when `TEST_DATABASE_URL` is unset (mirrors the pattern in
//! `crates/server/tests/status.rs`).

mod common;

use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use rustyq_server::router;
use serde_json::json;
use tower::ServiceExt;

// The metrics recorder is global. Tests rely on the router itself triggering
// `metrics_handle()` on the first /metrics call, but installing it eagerly
// here ensures the `describe_*` HELP strings are registered before any other
// metric is touched.
fn ensure_recorder() {
    let _ = rustyq_server::metrics_handle();
}

#[tokio::test]
async fn metrics_endpoint_returns_200() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    ensure_recorder();
    let app = router(pool);

    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(axum::body::Body::empty())
        .expect("build request");

    let resp = app.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    // The body may be empty here: the exporter only renders series (and
    // their HELP lines) once something was recorded, and under nextest this
    // test runs in its own process. `enqueue_increments_counter` covers the
    // body.
    assert_eq!(resp.headers()["content-type"], "text/plain; version=0.0.4");
}

#[tokio::test]
async fn enqueue_increments_counter() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };

    ensure_recorder();
    let app = router(pool);

    // POST /jobs twice — both should succeed.
    for _ in 0..2_u8 {
        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({
                    "queue": "default",
                    "kind":  "noop",
                    "payload": {}
                })
                .to_string(),
            ))
            .expect("build request");

        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::OK, "enqueue must succeed");
    }

    // GET /metrics — counter should be >= 2.
    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(axum::body::Body::empty())
        .expect("build request");

    let resp = app.oneshot(req).await.expect("oneshot");
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let text = std::str::from_utf8(&body).expect("utf8");

    assert!(
        text.contains("# HELP rustyq_jobs_enqueued_total"),
        "expected HELP line in metrics body, got:\n{text}"
    );
    assert!(
        text.contains("rustyq_jobs_enqueued_total"),
        "expected 'rustyq_jobs_enqueued_total' in metrics body, got:\n{text}"
    );

    // Extract the numeric sample and verify it is >= 2.
    // Prometheus text format line example:
    //   rustyq_jobs_enqueued_total{queue="default",kind="noop"} 2
    let value: f64 = text
        .lines()
        .filter(|l| l.starts_with("rustyq_jobs_enqueued_total") && !l.starts_with('#'))
        .filter_map(|l| l.split_whitespace().last())
        .filter_map(|s| s.parse::<f64>().ok())
        .sum();

    assert!(
        value >= 2.0,
        "expected rustyq_jobs_enqueued_total >= 2, got {value}\nfull body:\n{text}"
    );
}
