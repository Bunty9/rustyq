//! Prometheus recorder installation and handle cache.
//!
//! `metrics-exporter-prometheus` installs a single *global* recorder.
//! A process may only call `install()` once; subsequent calls panic.
//! `handle()` uses a `OnceLock` to install exactly once and hand back the
//! same `PrometheusHandle` on every subsequent call — safe to call from
//! multiple test binaries or server threads.

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

const DURATION_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0];

/// Install the Prometheus recorder on first call; return the cached handle
/// on all subsequent calls.
pub fn handle() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            let h = PrometheusBuilder::new()
                .set_buckets_for_metric(
                    Matcher::Full("rustyq_job_run_duration_seconds".to_string()),
                    DURATION_BUCKETS,
                )
                .expect("set buckets for rustyq_job_run_duration_seconds")
                .set_buckets_for_metric(
                    Matcher::Full("rustyq_dispatch_latency_seconds".to_string()),
                    DURATION_BUCKETS,
                )
                .expect("set buckets for rustyq_dispatch_latency_seconds")
                .install_recorder()
                .expect("install prometheus recorder");

            // Register HELP strings so the text exposition has # HELP rustyq* lines.
            metrics::describe_counter!(
                "rustyq_jobs_enqueued_total",
                "Total number of jobs enqueued via the HTTP API."
            );
            metrics::describe_counter!(
                "rustyq_jobs_claimed_total",
                "Total number of jobs claimed by a worker for execution."
            );
            metrics::describe_counter!(
                "rustyq_jobs_finished_total",
                "Total number of jobs that reached a terminal state (done, dead). \
                 Jobs requeued after a transient failure do not increment this counter."
            );
            metrics::describe_counter!(
                "rustyq_jobs_reaped_total",
                "Total number of jobs reclaimed from workers presumed dead (stale lock)."
            );
            metrics::describe_histogram!(
                "rustyq_dispatch_latency_seconds",
                "Time between job creation and when the worker claimed it, in seconds."
            );
            metrics::describe_histogram!(
                "rustyq_job_run_duration_seconds",
                "Time taken by the handler to process a job, in seconds."
            );

            h
        })
        .clone()
}
