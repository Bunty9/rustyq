//! Enqueue + status HTTP API for rustyq-server.

use axum::{extract::State, routing::post, Json, Router};
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
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
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
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Fire-and-forget — workers also fall back to a 1s poll.
    let _ = sqlx::query("NOTIFY rustyq_new").execute(&pool).await;
    Ok(Json(serde_json::json!({ "id": id })))
}

pub fn router(pool: PgPool) -> Router {
    Router::new().route("/jobs", post(enqueue)).with_state(pool)
}
