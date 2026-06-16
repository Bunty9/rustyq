//! rustyq-worker entrypoint — connects to Postgres, boots a `Worker`,
//! cancels on Ctrl-C.

mod handlers;

use clap::Parser;
use handlers::{FailOnce, Noop, Sleep};
use rustyq_core::{telemetry, Registry, Worker};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "rustyq-worker", about = "rustyq worker daemon")]
struct Args {
    /// Postgres connection string.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Comma-separated list of queues this worker drains.
    #[arg(long, env = "RUSTYQ_QUEUES", default_value = "default")]
    queues: String,

    /// Per-worker concurrency cap.
    #[arg(long, env = "RUSTYQ_CONCURRENCY", default_value_t = 8)]
    concurrency: usize,

    /// Worker identity written to `locked_by`. Defaults to `host:uuid`.
    #[arg(long, env = "RUSTYQ_WORKER_ID")]
    id: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("rustyq-worker")?;

    let args = Args::parse();

    let pool = PgPoolOptions::new()
        .max_connections((args.concurrency as u32).max(4) + 2)
        .connect(&args.database_url)
        .await?;

    let id = args.id.unwrap_or_else(|| {
        let host = hostname_or_unknown();
        format!("{host}:{}", Uuid::now_v7())
    });
    let queues = args
        .queues
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();

    tracing::info!(%id, queues = ?queues, concurrency = args.concurrency, "starting worker");

    // Build the handler registry with all built-in handlers.
    let registry = Registry::builder()
        .register("noop", Noop)
        .register("sleep", Sleep)
        .register("fail_once", FailOnce::new())
        .build();

    let cancel = CancellationToken::new();
    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Ctrl-C received, cancelling");
            cancel_for_signal.cancel();
        }
    });

    let worker = Worker::new(pool, id, queues, args.concurrency, Arc::new(registry));
    worker.run(cancel).await?;

    telemetry::shutdown();
    Ok(())
}

fn hostname_or_unknown() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string())
}
