//! Shared test helper: sets up a `PgPool` against a Postgres reachable via
//! `TEST_DATABASE_URL`, runs the schema migration, and returns the pool.
//!
//! Tests using this helper are skipped (early return) when `TEST_DATABASE_URL`
//! is unset, so `cargo test` stays green on machines without a test Postgres.
//!
//! Recommended local setup:
//!
//! ```bash
//! eval "$(./scripts/test-pg.sh up | tail -1)"   # exports TEST_DATABASE_URL
//! cargo test --workspace -- --test-threads=1
//! ./scripts/test-pg.sh down                     # when finished
//! ```
//!
//! Note: `--test-threads=1` is required until the LISTEN/NOTIFY channel name
//! is scoped per-schema; otherwise workers across test binaries wake each
//! other on shared `rustyq_new` notifications. Tracked in PROGRESS.md.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

/// Try to set up a pool against `TEST_DATABASE_URL`. Returns `None` when the
/// env var is unset — the caller should early-return so the test reports as
/// passing without a live Postgres.
///
/// Each call creates a fresh, isolated schema (named after a random UUID) so
/// tests do not interfere with each other and can run in parallel.
pub async fn setup_pool() -> Option<PgPool> {
    let url = std::env::var("TEST_DATABASE_URL").ok()?;
    let schema = format!("rustyq_test_{}", Uuid::now_v7().simple());
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .after_connect({
            let schema = schema.clone();
            move |conn, _| {
                let schema = schema.clone();
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute(format!("SET search_path TO \"{schema}\"").as_str())
                        .await?;
                    Ok(())
                })
            }
        })
        .connect(&url)
        .await
        .expect("connect to TEST_DATABASE_URL");

    sqlx::raw_sql(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("create schema");
    rustyq_core::migrate(&pool).await.expect("migration");
    Some(pool)
}
