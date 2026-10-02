#![cfg(feature = "telemetry")]
//! Unit test: `telemetry::init` is idempotent and succeeds without an OTLP
//! endpoint. The second call must not panic even though
//! `tracing_subscriber::set_global_default` can only be called once per
//! process — `init` must guard that internally.

#[test]
fn init_without_endpoint_is_idempotent_and_succeeds() {
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    rustyq_core::telemetry::init("rustyq-test").expect("init #1");
    // Calling twice must not panic. tracing_subscriber::set_global_default
    // can be called once per process; init() should detect and skip on retry.
    rustyq_core::telemetry::init("rustyq-test").expect("init #2");
    rustyq_core::telemetry::shutdown();
}
