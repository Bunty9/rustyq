//! Enqueue + status HTTP API for rustyq-server.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use sqlx::PgPool;
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
    let id = Uuid::now_v7();
    sqlx::query(
        r#"INSERT INTO jobs (id, queue, kind, payload, state, priority, run_at)
           VALUES ($1, $2, $3, $4, 'queued', $5, now() + make_interval(secs => $6::int))"#,
    )
    .bind(id)
    .bind(&req.queue)
    .bind(&req.kind)
    .bind(&req.payload)
    .bind(req.priority)
    .bind(req.delay_secs as i32)
    .execute(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Fire-and-forget — workers also fall back to a 1s poll.
    let _ = sqlx::query("NOTIFY rustyq_new").execute(&pool).await;
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
    let row = sqlx::query_as::<_, JobStatus>(
        "SELECT id, state, attempts, max_attempts, run_at, locked_by, last_error \
         FROM jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    row.map(Json)
        .ok_or((StatusCode::NOT_FOUND, "no such job".to_string()))
}

pub fn router(pool: PgPool) -> Router {
    Router::new()
        .route("/jobs", post(enqueue))
        .route("/jobs/{id}", get(status))
        .with_state(pool)
}
