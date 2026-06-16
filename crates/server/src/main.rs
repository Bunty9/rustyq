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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("rustyq-server")?;

    let args = Args::parse();

    let pool = PgPoolOptions::new()
        .max_connections(args.pg_max)
        .connect(&args.database_url)
        .await?;

    let app = router(pool);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "rustyq-server listening");
    axum::serve(listener, app).await?;

    telemetry::shutdown();
    Ok(())
}
