// Distributed tracing with OpenTelemetry
//
// This module provides OpenTelemetry-based distributed tracing for Rusternetes components.
// It supports multiple exporters: Jaeger, OTLP, and stdout.

use anyhow::Result;
use tracing_subscriber::EnvFilter;

/// Tracing configuration
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Service name for tracing
    pub service_name: String,

    /// Tracing exporter type
    pub exporter: TracingExporter,

    /// Jaeger agent endpoint (for Jaeger exporter)
    pub jaeger_endpoint: Option<String>,

    /// OTLP endpoint (for OTLP exporter)
    pub otlp_endpoint: Option<String>,

    /// Sampling rate (0.0 to 1.0, where 1.0 = 100%)
    pub sample_rate: f64,

    /// Log level filter
    pub log_level: String,
}

/// Tracing exporter types
#[derive(Debug, Clone, PartialEq)]
pub enum TracingExporter {
    /// Export to Jaeger
    Jaeger,
    /// Export via OTLP (OpenTelemetry Protocol)
    Otlp,
    /// Export to stdout (for debugging)
    Stdout,
    /// No tracing
    None,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "rusternetes".to_string(),
            exporter: TracingExporter::None,
            jaeger_endpoint: None,
            otlp_endpoint: Some("http://localhost:4317".to_string()),
            sample_rate: 1.0,
            log_level: "info".to_string(),
        }
    }
}

impl TracingConfig {
    /// Create a new tracing configuration
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            ..Default::default()
        }
    }

    /// Set the exporter type
    pub fn with_exporter(mut self, exporter: TracingExporter) -> Self {
        self.exporter = exporter;
        self
    }

    /// Set the Jaeger endpoint
    pub fn with_jaeger_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.jaeger_endpoint = Some(endpoint.into());
        self
    }

    /// Set the OTLP endpoint
    pub fn with_otlp_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = Some(endpoint.into());
        self
    }

    /// Set the sampling rate
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.sample_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set the log level
    pub fn with_log_level(mut self, level: impl Into<String>) -> Self {
        self.log_level = level.into();
        self
    }
}

/// Initialize distributed tracing
#[cfg(feature = "tracing-full")]
pub fn init_tracing(config: TracingConfig) -> Result<()> {
    match config.exporter {
        TracingExporter::Jaeger => {
            eprintln!("Warning: Jaeger exporter is deprecated. Use OTLP instead. Falling back to basic tracing.");
            init_basic_tracing(&config)
        }
        TracingExporter::Otlp => {
            eprintln!("Warning: OTLP exporter not yet configured. Falling back to basic tracing.");
            init_basic_tracing(&config)
        }
        TracingExporter::Stdout | TracingExporter::None => init_basic_tracing(&config),
    }
}

/// Initialize distributed tracing (fallback without features)
#[cfg(not(feature = "tracing-full"))]
pub fn init_tracing(config: TracingConfig) -> Result<()> {
    init_basic_tracing(&config)
}

/// Initialize basic tracing without OpenTelemetry
fn init_basic_tracing(config: &TracingConfig) -> Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    tracing::info!(
        "Initialized basic tracing for service '{}'",
        config.service_name
    );

    Ok(())
}

/// Shutdown tracing gracefully
#[cfg(feature = "tracing-full")]
pub fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}

/// Shutdown tracing gracefully (no-op without features)
#[cfg(not(feature = "tracing-full"))]
pub fn shutdown_tracing() {
    // No-op when OpenTelemetry is not enabled
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracing_config_builder() {
        let config = TracingConfig::new("test-service")
            .with_exporter(TracingExporter::Jaeger)
            .with_jaeger_endpoint("http://localhost:14268")
            .with_sample_rate(0.5)
            .with_log_level("debug");

        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.exporter, TracingExporter::Jaeger);
        assert_eq!(
            config.jaeger_endpoint,
            Some("http://localhost:14268".to_string())
        );
        assert_eq!(config.sample_rate, 0.5);
        assert_eq!(config.log_level, "debug");
    }

    #[test]
    fn test_sample_rate_clamping() {
        let config1 = TracingConfig::default().with_sample_rate(1.5);
        assert_eq!(config1.sample_rate, 1.0);

        let config2 = TracingConfig::default().with_sample_rate(-0.5);
        assert_eq!(config2.sample_rate, 0.0);
    }
}
