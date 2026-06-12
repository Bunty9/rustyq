//! Library surface for `rustyq-server`. Exposes the axum `Router` so
//! integration tests (and downstream embedders) can drive the HTTP API
//! without booting the binary.

mod api;

pub use api::router;
