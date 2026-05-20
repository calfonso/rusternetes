// Basic tracing initialization.
//
// Distributed tracing via OpenTelemetry (Jaeger/OTLP) was removed: it was unused
// by every binary, the `opentelemetry-*` crates pinned in `Cargo.toml` had drifted
// to incompatible major versions, and Kubernetes conformance does not require
// trace export.

use anyhow::Result;
use tracing_subscriber::EnvFilter;

/// Initialize the global tracing subscriber with stdout fmt output.
///
/// `RUST_LOG` overrides `log_level` when set.
pub fn init_basic_tracing(service_name: &str, log_level: &str) -> Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    tracing::info!("Initialized basic tracing for service '{}'", service_name);

    Ok(())
}
