//! rustyq-worker entrypoint — connects to Postgres, boots a `Worker`,
//! cancels on Ctrl-C.

mod handlers;

use clap::Parser;
use handlers::{FailOnce, Noop, Sleep};
use rustyq_core::{telemetry, Registry, Worker};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
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

    /// Seconds a `running` lock may age before its job is presumed
    /// abandoned (worker died) and reaped back to `queued`/`dead`.
    #[arg(long, env = "RUSTYQ_LOCK_TIMEOUT_SECS", default_value_t = 300)]
    lock_timeout_secs: u64,

    /// Address for this worker's Prometheus `/metrics` listener (claimed,
    /// finished, reaped counters; dispatch-latency and run-duration
    /// histograms — they are recorded here, not in the server).
    #[arg(long, env = "RUSTYQ_METRICS_BIND", default_value = "0.0.0.0:9091")]
    metrics_bind: std::net::SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("rustyq-worker")?;

    let args = Args::parse();

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(args.metrics_bind)
        .set_buckets(&[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0])?
        .install()?;

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
        .register("fail_once", FailOnce)
        .build();

    let cancel = CancellationToken::new();
    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Ctrl-C received, cancelling");
            cancel_for_signal.cancel();
        }
    });
    // Docker/Fly send SIGTERM on stop/redeploy, not Ctrl-C — without this,
    // the process would be killed outright, leaving in-flight jobs stuck in
    // `running`.
    #[cfg(unix)]
    {
        let cancel_for_sigterm = cancel.clone();
        tokio::spawn(async move {
            if let Ok(mut sigterm) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                sigterm.recv().await;
                tracing::info!("SIGTERM received, cancelling");
                cancel_for_sigterm.cancel();
            }
        });
    }

    let mut worker = Worker::new(pool, id, queues, args.concurrency, Arc::new(registry));
    worker.lock_timeout = Duration::from_secs(args.lock_timeout_secs);
    worker.run(cancel).await?;

    telemetry::shutdown();
    Ok(())
}

fn hostname_or_unknown() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string())
}
