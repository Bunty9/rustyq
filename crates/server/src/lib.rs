//! Library surface for `rustyq-server`. Exposes the axum `Router` so
//! integration tests (and downstream embedders) can drive the HTTP API
//! without booting the binary.

mod api;
pub mod metrics;

pub use api::router;
pub use metrics::handle as metrics_handle;
pub use metrics_exporter_prometheus::PrometheusHandle;
