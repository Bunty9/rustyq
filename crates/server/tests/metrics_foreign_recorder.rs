//! `router()` must not panic when the embedding app already installed a
//! global `metrics` recorder. Own test binary: the recorder is process-wide.

mod common;

use axum::http::Request;
use rustyq_server::router;
use tower::ServiceExt;

#[tokio::test]
async fn router_survives_preinstalled_recorder() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("install foreign recorder");

    let app = router(pool);
    let get = Request::builder()
        .uri("/metrics")
        .body(axum::body::Body::empty())
        .expect("build request");
    let resp = app.oneshot(get).await.expect("oneshot");
    assert_eq!(resp.status(), 200);
}
