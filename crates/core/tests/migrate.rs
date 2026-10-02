//! `rustyq_core::migrate` is idempotent and tolerates an application's own
//! migrations in the same `_sqlx_migrations` table. Skipped when
//! `TEST_DATABASE_URL` is unset.

mod common;

#[tokio::test]
async fn migrate_twice_is_noop() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    rustyq_core::migrate(&pool).await.expect("second migrate");
}

#[tokio::test]
async fn migrate_ignores_unrelated_applied_versions() {
    let Some(pool) = common::setup_pool().await else {
        eprintln!("skipping: TEST_DATABASE_URL unset");
        return;
    };
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (20990101000000, 'app', true, '\\x00', 0)",
    )
    .execute(&pool)
    .await
    .expect("fake app migration");
    rustyq_core::migrate(&pool)
        .await
        .expect("migrate with foreign version");
}
