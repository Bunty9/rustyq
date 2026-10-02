//! rustyq-server entrypoint — boots axum, connects to Postgres, serves the
//! HTTP API. The router itself lives in the library crate so tests can drive
//! it without spinning up a TCP listener.

use clap::Parser;
use rustyq_core::telemetry;
use rustyq_server::router;
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
#[command(name = "rustyq-server", about = "rustyq HTTP server")]
struct Args {
    /// Postgres connection string.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Bind address.
    #[arg(long, env = "RUSTYQ_BIND", default_value = "0.0.0.0:8080")]
    bind: SocketAddr,

    /// Maximum Postgres pool connections.
    #[arg(long, env = "RUSTYQ_PG_MAX", default_value_t = 16)]
    pg_max: u32,

    /// Run pending migrations against `--database-url` before serving.
    /// Opt-in: only the server process should migrate against a shared
    /// database, never every process in a fly.io/compose deployment.
    #[arg(long, env = "RUSTYQ_MIGRATE", default_value_t = false)]
    migrate: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("rustyq-server")?;

    let args = Args::parse();

    let pool = PgPoolOptions::new()
        .max_connections(args.pg_max)
        .connect(&args.database_url)
        .await?;

    if args.migrate {
        tracing::info!("running pending migrations");
        rustyq_core::migrate(&pool).await?;
    }

    let app = router(pool);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "rustyq-server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    telemetry::shutdown();
    Ok(())
}

/// Resolves on Ctrl-C or, on unix, SIGTERM — whichever comes first.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
