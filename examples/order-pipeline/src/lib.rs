//! Shared code for the order-pipeline example: job kinds, queues, typed
//! payloads, and the database bootstrap used by both the api and the worker.

pub mod handlers;

use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

// Queues. Workers pick which queues they drain (`--queues`), so you can give
// slow or critical work its own queue and dedicated workers.
pub const QUEUE_DEFAULT: &str = "default";
pub const QUEUE_PAYMENTS: &str = "payments";

// Job kinds: the string a producer enqueues and a handler is registered under.
pub const KIND_EMAIL: &str = "email.order_confirmation";
pub const KIND_CHARGE: &str = "payment.charge";
pub const KIND_FRAUD: &str = "fraud.review";
pub const KIND_REPORT: &str = "report.daily";

// Typed payloads: producers build these (or plain JSON), handlers read them
// back with `Job::payload_as::<T>()`. Keeping them in one shared crate means
// producer and consumer cannot drift apart silently.

#[derive(Debug, Serialize, Deserialize)]
pub struct EmailPayload {
    pub order_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChargePayload {
    pub order_id: Uuid,
    pub amount_cents: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FraudPayload {
    pub order_id: Uuid,
}

/// `report.daily` takes no arguments; the empty struct accepts `{}`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReportPayload {}

/// Connect and bring the schema up to date. Called at boot by both binaries,
/// so whichever starts first migrates; the migrator takes a lock, so
/// concurrent starts are safe.
///
/// `pool_size` should be at least the worker's concurrency + 2 (each running
/// job may hold a connection, plus the LISTEN connection and the finalizer).
pub async fn connect_and_migrate(database_url: &str, pool_size: u32) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(pool_size)
        .connect(database_url)
        .await?;

    // 1. rustyq's own tables (the `jobs` table), versions 1 and 2.
    rustyq_core::migrate(&pool).await?;

    // 2. The application's tables. They share rustyq's `_sqlx_migrations`
    //    table, so this migrator must tolerate versions it does not know
    //    about (rustyq's), and our files use timestamp versions.
    let mut app = sqlx::migrate!("./migrations");
    app.set_ignore_missing(true);
    app.run(&pool).await?;

    Ok(pool)
}

/// Resolve when Ctrl-C or (on unix) SIGTERM arrives. Docker, Kubernetes and
/// systemd all stop processes with SIGTERM.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
    tracing::info!("shutdown signal received");
}
