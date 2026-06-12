//! Library surface for `rustyq-server`. Exposes the axum `Router` so
//! integration tests (and downstream embedders) can drive the HTTP API
//! without booting the binary.

mod api;
pub mod metrics;

pub use api::router;
pub use metrics::handle as metrics_handle;
// `metrics_exporter_prometheus::PrometheusHandle` is intentionally NOT
// re-exported — callers should use the `metrics_handle()` function and call
// `.render()` on the returned handle. Keeping the third-party type private
// stops a downstream `metrics-exporter-prometheus` semver bump from breaking
// our public API.
