//! Shared test helper for `rustyq-server` integration tests. Sets up a PgPool
//! against `TEST_DATABASE_URL` in an isolated schema, mirroring the helper in
//! `crates/core/tests/common/mod.rs`. Skipped when the env var is unset.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

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
    sqlx::raw_sql(concat!(
        include_str!("../../../../migrations/0001_init.sql"),
        include_str!("../../../../migrations/0002_dispatch_index.sql"),
    ))
    .execute(&pool)
    .await
    .expect("migration");
    Some(pool)
}
