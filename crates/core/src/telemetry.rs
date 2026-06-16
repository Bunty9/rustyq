//! Unified observability bootstrap for rustyq binaries.
//!
//! # No-endpoint contract
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` is unset (the default for local
//! development and CI), only a JSON `fmt` layer is installed. No gRPC
//! connection is attempted and no external dependency is required. The binary
//! starts and runs normally.
//!
//! When the env var is set, an additional `tracing_opentelemetry` layer is
//! installed, routing spans to the configured OTLP/gRPC collector. If that
//! setup fails (bad URL, unreachable collector, etc.) a `warn!` is emitted and
//! execution continues with the `fmt` layer only. The binary never fails to
//! start because of tracing.
//!
//! # Idempotency
//!
//! `init` is safe to call multiple times in the same process (e.g. across
//! integration tests). A `OnceLock` guards the global subscriber install; the
//! second and subsequent calls are no-ops that return `Ok(())`.
//!
//! # Shutdown
//!
//! Call `shutdown()` before process exit so the SDK can flush any in-flight
//! spans. This is a no-op when no OTLP exporter was installed.

use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Guards the global tracing subscriber so `init` is safe to call multiple
/// times in the same process (important for integration-test binaries).
static INIT: OnceLock<()> = OnceLock::new();

/// Initialise the global tracing subscriber.
///
/// Always installs a JSON `fmt` layer gated by `RUST_LOG` (defaults to
/// `"info"`). Adds a `tracing_opentelemetry` span layer if and only if
/// `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Returns `Ok(())` in both cases.
///
/// Safe to call more than once: subsequent calls are no-ops.
pub fn init(service: &'static str) -> anyhow::Result<()> {
    INIT.get_or_init(|| {
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));

        if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            match build_otlp_tracer(service) {
                Ok(tracer) => {
                    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
                    tracing_subscriber::registry()
                        .with(env_filter)
                        .with(tracing_subscriber::fmt::layer().json())
                        .with(otel_layer)
                        .init();
                }
                Err(e) => {
                    // Can't use tracing yet (subscriber not yet initialised).
                    // After init() returns the fmt layer records the warning.
                    tracing_subscriber::registry()
                        .with(env_filter)
                        .with(tracing_subscriber::fmt::layer().json())
                        .init();
                    tracing::warn!(
                        error = %e,
                        "OTLP exporter setup failed; continuing with fmt layer only"
                    );
                }
            }
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
    });
    Ok(())
}

/// Flush and shut down the global tracer provider. Should be called before
/// process exit when an OTLP exporter was installed. Safe to call when no
/// provider was installed — the SDK no-ops in that case.
pub fn shutdown() {
    opentelemetry::global::shutdown_tracer_provider();
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build an OTLP/gRPC batch exporter and return the [`Tracer`] that drives it.
/// The endpoint and transport settings are picked up from the standard
/// OpenTelemetry SDK environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`,
/// `OTEL_EXPORTER_OTLP_HEADERS`, etc.).
fn build_otlp_tracer(service: &'static str) -> anyhow::Result<opentelemetry_sdk::trace::Tracer> {
    use opentelemetry::trace::TracerProvider as _;

    let resource = Resource::new([KeyValue::new("service.name", service)]);

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());

    Ok(provider.tracer(service))
}
