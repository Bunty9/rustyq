//! The application's worker process: registers the handlers and drains the
//! `default` and `payments` queues until SIGTERM.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use order_pipeline::handlers::{ChargeCustomer, DailyReport, FraudReview, SendConfirmation};
use order_pipeline::{
    connect_and_migrate, shutdown_signal, KIND_CHARGE, KIND_EMAIL, KIND_FRAUD, KIND_REPORT,
};
use rustyq_core::{telemetry, Registry, Worker};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Comma-separated queues this worker drains.
    #[arg(long, env = "RUSTYQ_QUEUES", default_value = "default,payments")]
    queues: String,
    /// Max jobs running at once in this process.
    #[arg(long, env = "RUSTYQ_CONCURRENCY", default_value_t = 8)]
    concurrency: usize,
    /// Prometheus listener. Worker-side metrics (claimed, finished, latency)
    /// are recorded in this process, so they are served from here.
    #[arg(long, env = "RUSTYQ_METRICS_BIND", default_value = "127.0.0.1:9464")]
    metrics_bind: std::net::SocketAddr,
    /// A `running` job older than this is presumed orphaned (worker died) and
    /// requeued. Must exceed your slowest handler plus DB stalls.
    #[arg(long, env = "RUSTYQ_LOCK_TIMEOUT_SECS", default_value_t = 300)]
    lock_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("order-pipeline-worker")?;
    let args = Args::parse();

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(args.metrics_bind)
        .set_buckets(&[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0])?
        .install()?;

    // Pool size: each running job can hold a connection, plus the worker's
    // LISTEN connection and its batched finalizer.
    let pool = connect_and_migrate(&args.database_url, args.concurrency as u32 + 2).await?;

    // One handler per job kind; a job whose kind has no handler fails.
    let registry = Registry::builder()
        .register(KIND_EMAIL, SendConfirmation { pool: pool.clone() })
        .register(KIND_CHARGE, ChargeCustomer { pool: pool.clone() })
        .register(KIND_FRAUD, FraudReview)
        .register(KIND_REPORT, DailyReport { pool: pool.clone() })
        .build();

    let queues: Vec<String> = args
        .queues
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let id = format!("order-pipeline:{}", Uuid::now_v7());
    tracing::info!(%id, ?queues, concurrency = args.concurrency, "starting worker");

    let mut worker = Worker::new(pool, id, queues, args.concurrency, Arc::new(registry));
    worker.lock_timeout = Duration::from_secs(args.lock_timeout_secs);
    // How long to wait for in-flight jobs after SIGTERM before giving up (they
    // would then be reaped by another worker after `lock_timeout`).
    worker.shutdown_grace = Duration::from_secs(10);

    // Graceful shutdown: on SIGTERM/Ctrl-C the worker stops claiming, lets
    // running handlers finish and their results be recorded, then returns.
    let cancel = CancellationToken::new();
    let on_signal = cancel.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        on_signal.cancel();
    });
    worker.run(cancel).await?;

    telemetry::shutdown();
    Ok(())
}
