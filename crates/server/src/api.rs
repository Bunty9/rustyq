//! Enqueue + status + metrics HTTP API for rustyq-server.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    routing::{get, post},
    Json, Router,
};
use rustyq_core::NewJob;
use sqlx::PgPool;
use std::time::Duration;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

#[derive(serde::Deserialize)]
pub struct EnqueueReq {
    pub queue: String,
    pub kind: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub priority: i16,
    #[serde(default)]
    pub delay_secs: i64,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
}

fn default_max_attempts() -> i32 {
    5
}

async fn enqueue(
    State(pool): State<PgPool>,
    Json(req): Json<EnqueueReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if req.queue.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "queue must not be empty".into()));
    }
    if req.kind.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "kind must not be empty".into()));
    }
    if req.delay_secs < 0 || req.delay_secs > i32::MAX as i64 {
        return Err((
            StatusCode::BAD_REQUEST,
            "delay_secs must be between 0 and i32::MAX".into(),
        ));
    }

    if !(1..=1000).contains(&req.max_attempts) {
        return Err((
            StatusCode::BAD_REQUEST,
            "max_attempts must be between 1 and 1000".into(),
        ));
    }

    let job = NewJob::new(req.queue.clone(), req.kind.clone(), req.payload)
        .priority(req.priority)
        .delay(Duration::from_secs(req.delay_secs as u64))
        .max_attempts(req.max_attempts);
    let id = rustyq_core::enqueue(&pool, &job)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Increment enqueue counter.
    metrics::counter!(
        "rustyq_jobs_enqueued_total",
        "queue" => req.queue.clone(),
        "kind"  => req.kind.clone()
    )
    .increment(1);

    Ok(Json(serde_json::json!({ "id": id })))
}

async fn status(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<Json<rustyq_core::JobStatus>, (StatusCode, String)> {
    let row = rustyq_core::job_status(&pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    row.map(Json)
        .ok_or((StatusCode::NOT_FOUND, "no such job".to_string()))
}

async fn healthz(State(pool): State<PgPool>) -> (StatusCode, String) {
    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => (StatusCode::OK, "ok".to_string()),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
}

async fn metrics_handler() -> (StatusCode, [(header::HeaderName, &'static str); 1], String) {
    let body = crate::metrics::handle().render();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

/// Build the HTTP API (`/jobs`, `/jobs/{id}`, `/healthz`, `/metrics`).
///
/// There is no authentication: mount it behind your own auth middleware or on
/// an internal-only listener.
///
/// This installs rustyq's Prometheus recorder as the process-global `metrics`
/// recorder. If one is already installed (the embedding app's own), a warning
/// is logged and `/metrics` renders without rustyq's series.
pub fn router(pool: PgPool) -> Router {
    // Install the recorder now so enqueues before the first scrape are counted.
    crate::metrics::handle();
    Router::new()
        .route("/jobs", post(enqueue))
        .route("/jobs/{id}", get(status))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(pool)
}
