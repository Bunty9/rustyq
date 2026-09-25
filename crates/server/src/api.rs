//! Enqueue + status + metrics HTTP API for rustyq-server.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    routing::{get, post},
    Json, Router,
};
use sqlx::PgPool;
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

    let id = Uuid::now_v7();
    // INSERT and NOTIFY in one statement: one round trip, and the
    // notification is delivered at commit, so a woken worker always sees the
    // row. Workers also fall back to a 1s poll.
    sqlx::query!(
        r#"WITH ins AS (
             INSERT INTO jobs (id, queue, kind, payload, state, priority, run_at)
             VALUES ($1, $2, $3, $4, 'queued', $5, now() + make_interval(secs => $6::int))
           )
           SELECT pg_notify('rustyq_new', '')::text AS "notified""#,
        id,
        req.queue,
        req.kind,
        req.payload,
        req.priority,
        req.delay_secs as i32,
    )
    .fetch_one(&pool)
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

/// Status row returned by `GET /jobs/:id`. Mirrors the columns most relevant
/// to a Python caller waiting on a background job: state, retry progress, the
/// scheduled next run, and any preserved error text. Internal columns like
/// `payload` and `locked_at` are deliberately omitted to keep the surface
/// stable.
#[derive(serde::Serialize, sqlx::FromRow)]
pub struct JobStatus {
    pub id: Uuid,
    pub state: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub run_at: chrono::DateTime<chrono::Utc>,
    pub locked_by: Option<String>,
    pub last_error: Option<String>,
}

async fn status(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobStatus>, (StatusCode, String)> {
    let row = sqlx::query_as!(
        JobStatus,
        r#"SELECT id, state, attempts, max_attempts, run_at, locked_by, last_error
           FROM jobs WHERE id = $1"#,
        id,
    )
    .fetch_optional(&pool)
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

pub fn router(pool: PgPool) -> Router {
    Router::new()
        .route("/jobs", post(enqueue))
        .route("/jobs/{id}", get(status))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(pool)
}
