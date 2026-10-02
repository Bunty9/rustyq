//! Library surface for `rustyq-server`. Exposes the axum `Router` so
//! integration tests (and downstream embedders) can drive the HTTP API
//! without booting the binary.

mod api;
pub mod metrics;

pub use api::router;
pub use metrics::handle as metrics_handle;
// Note: `metrics_handle()` returns `metrics_exporter_prometheus::PrometheusHandle`,
// so that type is part of our public signature and this crate's API is
// semver-coupled to metrics-exporter-prometheus 0.16.
