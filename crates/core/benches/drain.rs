//! In-process drain + dispatch-latency benchmark (`cargo bench -p rustyq-core`).
//!
//! Plain `harness = false` binary rather than Criterion: a 100k-job drain is
//! one long measurement, not a micro-benchmark Criterion can iterate.
//!
//! 1. **Drain**: bulk-insert `BENCH_JOBS` noop jobs with no worker running,
//!    start `BENCH_WORKERS` workers × `BENCH_CONCURRENCY`, time until every
//!    row is `done`.
//! 2. **Latency**: with workers idle on LISTEN, enqueue `BENCH_LATENCY_JOBS`
//!    one at a time (INSERT + NOTIFY in one statement, like the server) and record
//!    `created_at -> handler start` for each; print p50/p99/p99.9.
//!
//! Needs `BENCH_DATABASE_URL` (falls back to `TEST_DATABASE_URL`); runs in a
//! throwaway schema that is dropped afterwards. Exits 0 without a database so
//! `cargo bench --workspace` stays green on machines without Postgres.
//! `BENCH_MIN_JOBS_PER_SEC` turns the drain rate into a pass/fail floor.

use rustyq_core::{HandlerFut, Job, Registry, Worker};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Some(url) = std::env::var("BENCH_DATABASE_URL")
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .ok()
    else {
        eprintln!("skipping: BENCH_DATABASE_URL / TEST_DATABASE_URL unset");
        return Ok(());
    };
    let jobs: usize = env_or("BENCH_JOBS", 100_000);
    let workers: usize = env_or("BENCH_WORKERS", 4);
    let concurrency: usize = env_or("BENCH_CONCURRENCY", 16);
    let latency_jobs: usize = env_or("BENCH_LATENCY_JOBS", 2_000);
    let min_rate: f64 = env_or("BENCH_MIN_JOBS_PER_SEC", 0.0);

    let schema = format!("rustyq_bench_{}", Uuid::now_v7().simple());
    let pool = PgPoolOptions::new()
        // Each worker's in-flight jobs finalize on their own connection.
        .max_connections((workers * (concurrency + 2)) as u32 + 4)
        .after_connect({
            let schema = schema.clone();
            move |conn, _| {
                let sql = format!("SET search_path TO \"{schema}\"");
                Box::pin(async move {
                    conn.execute(sql.as_str()).await?;
                    Ok(())
                })
            }
        })
        .connect(&url)
        .await?;
    sqlx::raw_sql(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await?;
    rustyq_core::migrate(&pool).await?;

    let result = run(&pool, jobs, workers, concurrency, latency_jobs).await;
    sqlx::raw_sql(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(&pool)
        .await?;
    let rate = result?;

    if rate < min_rate {
        anyhow::bail!("drain rate {rate:.0} jobs/s below floor {min_rate:.0}");
    }
    Ok(())
}

/// Returns the drain rate in jobs/s.
async fn run(
    pool: &PgPool,
    jobs: usize,
    workers: usize,
    concurrency: usize,
    latency_jobs: usize,
) -> anyhow::Result<f64> {
    let handled = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Vec::<f64>::new()));
    let record_latency = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let registry = {
        let handled = handled.clone();
        let latencies = latencies.clone();
        let record_latency = record_latency.clone();
        Arc::new(
            Registry::builder()
                .register("noop", move |job: &Job| -> HandlerFut {
                    if record_latency.load(Ordering::Relaxed) {
                        let ms = (chrono::Utc::now() - job.created_at).num_microseconds();
                        latencies
                            .lock()
                            .unwrap()
                            .push(ms.unwrap_or(0) as f64 / 1000.0);
                    }
                    handled.fetch_add(1, Ordering::Relaxed);
                    Box::pin(async { Ok(()) })
                })
                .build(),
        )
    };

    // ---- 1. drain ---------------------------------------------------------
    sqlx::query(
        "INSERT INTO rustyq_jobs (id, queue, kind, payload, state) \
         SELECT gen_random_uuid(), 'default', 'noop', '{}', 'queued' \
         FROM generate_series(1, $1)",
    )
    .bind(jobs as i64)
    .execute(pool)
    .await?;
    sqlx::query("ANALYZE rustyq_jobs").execute(pool).await?;

    let cancel = CancellationToken::new();
    let started = Instant::now();
    let handles: Vec<_> = (0..workers)
        .map(|i| {
            let w = Worker::new(
                pool.clone(),
                format!("bench-{i}"),
                vec!["default".into()],
                concurrency,
                registry.clone(),
            );
            tokio::spawn(w.run(cancel.clone()))
        })
        .collect();

    // Handler count is a cheap progress signal; the DB count confirms every
    // row was actually finalized.
    while handled.load(Ordering::Relaxed) < jobs {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    loop {
        let (done,): (i64,) = sqlx::query_as("SELECT count(*) FROM rustyq_jobs WHERE state='done'")
            .fetch_one(pool)
            .await?;
        if done as usize >= jobs {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let secs = started.elapsed().as_secs_f64();
    let rate = jobs as f64 / secs;
    println!(
        "drain: {jobs} jobs, {workers} workers x {concurrency} concurrency: \
         {secs:.2}s = {rate:.0} jobs/s"
    );

    // ---- 2. dispatch latency ---------------------------------------------
    if latency_jobs == 0 {
        cancel.cancel();
        for h in handles {
            h.await??;
        }
        return Ok(rate);
    }
    // Let workers settle back into their LISTEN wait.
    tokio::time::sleep(Duration::from_millis(500)).await;
    record_latency.store(true, Ordering::Relaxed);
    for _ in 0..latency_jobs {
        sqlx::query(
            "WITH ins AS (INSERT INTO rustyq_jobs (id, queue, kind, payload, state) \
             VALUES ($1, 'default', 'noop', '{}', 'queued')) \
             SELECT pg_notify('rustyq_new', '')::text",
        )
        .bind(Uuid::now_v7())
        .fetch_one(pool)
        .await?;
        // ~500 enqueues/s: a steady trickle, not a burst the drain phase
        // already covers.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    while latencies.lock().unwrap().len() < latency_jobs {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut l = latencies.lock().unwrap().clone();
    l.sort_by(|a, b| a.total_cmp(b));
    let pct = |p: f64| l[((l.len() as f64 * p) as usize).min(l.len() - 1)];
    println!(
        "latency (enqueue -> handler start, {latency_jobs} jobs): \
         p50 {:.1}ms  p99 {:.1}ms  p99.9 {:.1}ms  max {:.1}ms",
        pct(0.50),
        pct(0.99),
        pct(0.999),
        l[l.len() - 1],
    );

    cancel.cancel();
    for h in handles {
        h.await??;
    }
    Ok(rate)
}
