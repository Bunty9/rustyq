//! The application's HTTP service: business endpoints plus rustyq's own API
//! nested under `/queue`.

use std::time::Duration;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use order_pipeline::{
    connect_and_migrate, shutdown_signal, ChargePayload, EmailPayload, KIND_CHARGE, KIND_EMAIL,
    KIND_REPORT, QUEUE_DEFAULT, QUEUE_PAYMENTS,
};
use rustyq_core::{enqueue, job_status, telemetry, JobStatus, NewJob};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "APP_BIND", default_value = "127.0.0.1:3000")]
    bind: std::net::SocketAddr,
}

type ApiError = (StatusCode, String);

fn internal(e: impl std::fmt::Display) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("order-pipeline-api")?;
    let args = Args::parse();
    let pool = connect_and_migrate(&args.database_url, 10).await?;

    // rustyq-server counts enqueues through the global `metrics` recorder,
    // which `metrics_handle()` installs on first call. Call it now, otherwise
    // enqueues before the first scrape of /queue/metrics would go uncounted.
    rustyq_server::metrics_handle();

    let app = Router::new()
        .route("/orders", post(create_order))
        .route("/orders/{id}", get(get_order))
        .route("/reports/daily", post(enqueue_report))
        .with_state(pool.clone())
        // Embedding rustyq's HTTP API (POST /queue/jobs, GET /queue/jobs/{id},
        // /queue/healthz, /queue/metrics) in the app's own server. This is how
        // non-Rust producers (the Python client) reach the queue without a
        // separate rustyq-server deployment.
        .nest("/queue", rustyq_server::router(pool));

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    tracing::info!(addr = %args.bind, "order-pipeline api listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    telemetry::shutdown();
    Ok(())
}

#[derive(Deserialize)]
struct NewOrder {
    email: String,
    amount_cents: i32,
}

async fn create_order(
    State(pool): State<PgPool>,
    Json(req): Json<NewOrder>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    if req.email.trim().is_empty() || !req.email.contains('@') {
        return Err((StatusCode::BAD_REQUEST, "email must contain '@'".into()));
    }
    if req.amount_cents <= 0 {
        return Err((StatusCode::BAD_REQUEST, "amount_cents must be > 0".into()));
    }

    let order_id = Uuid::now_v7();

    // Transactional enqueue. The order row and both jobs commit together or
    // not at all, so the "dual write" problem cannot happen: there is never an
    // order without its jobs (customer never charged / emailed), nor jobs for
    // an order that was rolled back. rustyq's NOTIFY is delivered at commit,
    // so a woken worker always sees the committed rows. With a separate
    // queue (Redis, SQS) you would need an outbox table to get this.
    let mut tx = pool.begin().await.map_err(internal)?;
    sqlx::query("INSERT INTO orders (id, email, amount_cents) VALUES ($1, $2, $3)")
        .bind(order_id)
        .bind(&req.email)
        .bind(req.amount_cents)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;

    // Default queue, default priority.
    let email_job = NewJob::new(QUEUE_DEFAULT, KIND_EMAIL, json!(EmailPayload { order_id }));
    let email_id = enqueue(&mut *tx, &email_job).await.map_err(internal)?;

    // Own queue (so payments can be scaled or isolated), higher priority than
    // the default 0 (claimed first when both are waiting), and an explicit
    // retry budget: up to 5 attempts before the job is parked as `dead`.
    let charge_job = NewJob::new(
        QUEUE_PAYMENTS,
        KIND_CHARGE,
        json!(ChargePayload {
            order_id,
            amount_cents: req.amount_cents
        }),
    )
    .priority(10)
    .max_attempts(5);
    let charge_id = enqueue(&mut *tx, &charge_job).await.map_err(internal)?;

    tx.commit().await.map_err(internal)?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "order_id": order_id,
            "job_ids": { "email": email_id, "charge": charge_id },
        })),
    ))
}

#[derive(Serialize, sqlx::FromRow)]
struct OrderRow {
    id: Uuid,
    email: String,
    amount_cents: i32,
    status: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

async fn get_order(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let order = sqlx::query_as::<_, OrderRow>(
        "SELECT id, email, amount_cents, status, created_at FROM orders WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&pool)
    .await
    .map_err(internal)?
    .ok_or((StatusCode::NOT_FOUND, "no such order".to_string()))?;

    // Demo-grade lookup: `payload->>'order_id'` is a sequential scan of `jobs`
    // plus one status query per job (N+1). In a real app store the job ids on
    // the order row (or add an expression index on the payload field).
    // The `jobs` table is ordinary SQL: find this order's jobs by payload,
    // then use rustyq's `job_status` (the same view GET /queue/jobs/{id} gives).
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM jobs WHERE payload->>'order_id' = $1::text ORDER BY created_at",
    )
    .bind(id)
    .fetch_all(&pool)
    .await
    .map_err(internal)?;
    let mut jobs: Vec<JobStatus> = Vec::new();
    for job_id in ids {
        if let Some(s) = job_status(&pool, job_id).await.map_err(internal)? {
            jobs.push(s);
        }
    }

    Ok(Json(json!({ "order": order, "jobs": jobs })))
}

#[derive(Deserialize)]
struct ReportQuery {
    #[serde(default)]
    delay_secs: u64,
}

async fn enqueue_report(
    State(pool): State<PgPool>,
    Query(q): Query<ReportQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Delayed job: `run_at` is now() + delay, workers ignore it until then.
    // This is how you schedule "do X in N seconds" (rustyq has no cron).
    let job =
        NewJob::new(QUEUE_DEFAULT, KIND_REPORT, json!({})).delay(Duration::from_secs(q.delay_secs));
    let id = enqueue(&pool, &job).await.map_err(internal)?;
    Ok(Json(json!({ "job_id": id })))
}
