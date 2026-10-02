//! Regression: enqueues made before the first /metrics scrape must be counted.
//! Own test binary (one test) so nothing else installs the recorder first.

mod common;

use axum::http::Request;
use http_body_util::BodyExt as _;
use rustyq_server::router;
use tower::ServiceExt;

#[tokio::test]
async fn enqueue_before_first_scrape_is_counted() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    let app = router(pool);

    let post = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            r#"{"queue":"default","kind":"noop","payload":{}}"#,
        ))
        .expect("build request");
    let resp = app.clone().oneshot(post).await.expect("oneshot");
    assert!(resp.status().is_success(), "enqueue must succeed");

    let get = Request::builder()
        .uri("/metrics")
        .body(axum::body::Body::empty())
        .expect("build request");
    let resp = app.oneshot(get).await.expect("oneshot");
    let body = resp.into_body().collect().await.expect("body").to_bytes();
    let text = std::str::from_utf8(&body).expect("utf8");
    assert!(
        text.lines()
            .any(|l| l.starts_with("rustyq_jobs_enqueued_total") && l.ends_with(" 1")),
        "enqueue before first scrape was dropped:\n{text}"
    );
}
