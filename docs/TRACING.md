# Distributed Tracing with OpenTelemetry

This document describes how to use distributed tracing in Rusternetes with OpenTelemetry.

## Overview

Rusternetes supports distributed tracing using OpenTelemetry, allowing you to trace requests across all components:
- API Server
- Scheduler
- Controller Manager
- Kubelet
- Kube-proxy
- DNS Server

Traces can be exported to:
- **Jaeger** - Popular open-source tracing backend
- **OTLP** - OpenTelemetry Protocol (works with many backends)
- **Stdout** - Debug output to console

## Quick Start

### 1. Start Jaeger (Recommended)

The easiest way to get started is with Jaeger using Docker/Podman:

```bash
# Start Jaeger all-in-one (includes UI, collector, and storage)
podman run -d --name jaeger \
  -p 6831:6831/udp \
  -p 16686:16686 \
  -p 14268:14268 \
  jaegertracing/all-in-one:latest

# Access Jaeger UI at http://localhost:16686
```

### 2. Build Rusternetes with Tracing Support

Rebuild components with the `jaeger` feature enabled:

```bash
# Build all components with Jaeger support
cargo build --release --features jaeger

# Or build individual components
cargo build --release -p rusternetes-api-server --features jaeger
cargo build --release -p rusternetes-scheduler --features jaeger
```

### 3. Run Components with Tracing

Components automatically detect and use tracing when built with the feature flag:

```bash
# API Server will automatically export traces to Jaeger
./target/release/api-server \
  --etcd-servers http://localhost:2379 \
  --tracing-exporter jaeger \
  --jaeger-endpoint http://localhost:14268/api/traces

# Scheduler with tracing
./target/release/scheduler \
  --etcd-servers http://localhost:2379 \
  --tracing-exporter jaeger
```

### 4. View Traces

Open http://localhost:16686 in your browser to see the Jaeger UI.

## Configuration

### Environment Variables

Tracing can be configured via environment variables:

```bash
# Tracing exporter type (jaeger, otlp, stdout, none)
export RUSTERNETES_TRACING_EXPORTER=jaeger

# Jaeger endpoint
export JAEGER_ENDPOINT=http://localhost:14268/api/traces

# OTLP endpoint (for OTLP exporter)
export OTLP_ENDPOINT=http://localhost:4317

# Sampling rate (0.0 to 1.0, where 1.0 = 100%)
export RUSTERNETES_TRACING_SAMPLE_RATE=1.0

# Service name (defaults to component name)
export RUSTERNETES_SERVICE_NAME=api-server
```

### Command-Line Flags

Most components support these tracing flags:

```bash
--tracing-exporter <TYPE>       # Exporter type: jaeger, otlp, stdout, none
--jaeger-endpoint <URL>         # Jaeger collector endpoint
--otlp-endpoint <URL>           # OTLP collector endpoint
--tracing-sample-rate <RATE>   # Sampling rate (0.0 - 1.0)
```

### Programmatic Configuration

```rust
use rusternetes_common::tracing::{TracingConfig, TracingExporter, init_tracing};

// Create tracing configuration
let config = TracingConfig::new("api-server")
    .with_exporter(TracingExporter::Jaeger)
    .with_jaeger_endpoint("http://localhost:14268/api/traces")
    .with_sample_rate(1.0)
    .with_log_level("info");

// Initialize tracing
init_tracing(config)?;

// Your application code here...

// Shutdown tracing on exit
rusternetes_common::tracing::shutdown_tracing();
```

## Tracing Backends

### Jaeger

Jaeger is the recommended backend for local development and testing.

**Setup:**
```bash
# Start Jaeger all-in-one
podman run -d --name jaeger \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 14269:14269 \
  -p 9411:9411 \
  jaegertracing/all-in-one:latest
```

**Build with Jaeger support:**
```bash
cargo build --release --features jaeger
```

**Configuration:**
```bash
./target/release/api-server \
  --tracing-exporter jaeger \
  --jaeger-endpoint http://localhost:14268/api/traces
```

**Ports:**
- **16686** - Jaeger UI
- **14268** - Jaeger collector HTTP
- **6831** - Jaeger agent UDP (compact thrift)
- **4317** - OTLP gRPC
- **4318** - OTLP HTTP

### OTLP (OpenTelemetry Protocol)

OTLP is a vendor-neutral protocol that works with many backends:
- Jaeger (via OTLP receiver)
- Grafana Tempo
- Honeycomb
- Lightstep
- New Relic
- Datadog

**Build with OTLP support:**
```bash
cargo build --release --features otlp
```

**Configuration:**
```bash
./target/release/api-server \
  --tracing-exporter otlp \
  --otlp-endpoint http://localhost:4317
```

### Stdout (Debug)

Stdout exporter prints traces to the console for debugging.

**Build:**
```bash
cargo build --release --features tracing-full
```

**Configuration:**
```bash
./target/release/api-server --tracing-exporter stdout
```

## Sampling

Sampling controls what percentage of traces are collected. This is useful for high-traffic environments.

**Sampling Rates:**
- `1.0` = 100% (sample all traces) - Default
- `0.5` = 50% (sample half of traces)
- `0.1` = 10% (sample 10% of traces)
- `0.0` = 0% (disable tracing)

**Configuration:**
```bash
# Sample 10% of traces
./target/release/api-server \
  --tracing-exporter jaeger \
  --tracing-sample-rate 0.1
```

## Trace Context Propagation

Rusternetes automatically propagates trace context across HTTP requests using the W3C Trace Context standard.

**Headers:**
- `traceparent` - Contains trace ID, span ID, and sampling decision
- `tracestate` - Vendor-specific trace information

This allows you to trace a request from kubectl → API Server → Scheduler → Kubelet.

## Integration with Existing Tools

### Kubernetes kubectl

When using the real `kubectl` with Rusternetes, traces will show:
1. kubectl HTTP request to API server
2. API server processing
3. Storage (etcd) operations
4. Controller/Scheduler processing

### Prometheus

Tracing works alongside Prometheus metrics. You can correlate metrics with traces using exemplars.

## Docker Compose Setup

Add Jaeger to your `docker-compose.yml`:

```yaml
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "6831:6831/udp"
      - "16686:16686"
      - "14268:14268"
      - "4317:4317"
    networks:
      - rusternetes-network

  api-server:
    build:
      context: .
      dockerfile: Dockerfile.services
      target: api-server
      args:
        CARGO_FEATURES: "--features jaeger"
    environment:
      - RUSTERNETES_TRACING_EXPORTER=jaeger
      - JAEGER_ENDPOINT=http://jaeger:14268/api/traces
    depends_on:
      - jaeger
      - etcd
    networks:
      - rusternetes-network
```

## Production Deployment

### Recommendations

1. **Use OTLP exporter** for vendor flexibility
2. **Set appropriate sampling rate** (0.01 - 0.1 for production)
3. **Use a dedicated tracing backend** (not all-in-one Jaeger)
4. **Enable TLS** for trace data transmission
5. **Configure retention policies** for trace storage

### Example Production Setup

```bash
# Production API server with OTLP and sampling
./target/release/api-server \
  --etcd-servers https://etcd-1:2379,https://etcd-2:2379,https://etcd-3:2379 \
  --tracing-exporter otlp \
  --otlp-endpoint https://tempo.monitoring.svc.cluster.local:4317 \
  --tracing-sample-rate 0.05 \
  --tls-cert /etc/certs/server.crt \
  --tls-key /etc/certs/server.key
```

### Cloud Providers

Most cloud providers support OpenTelemetry:

**AWS X-Ray:**
```bash
# Use OTLP with AWS Distro for OpenTelemetry Collector
--tracing-exporter otlp \
--otlp-endpoint http://localhost:4317
```

**Google Cloud Trace:**
```bash
# Use OTLP with Google Cloud Trace exporter
--tracing-exporter otlp \
--otlp-endpoint https://cloudtrace.googleapis.com
```

**Azure Monitor:**
```bash
# Use OTLP with Azure Monitor OpenTelemetry exporter
--tracing-exporter otlp \
--otlp-endpoint https://dc.services.visualstudio.com/v2/track
```

## Troubleshooting

### No traces appearing in Jaeger

1. **Check Jaeger is running:**
   ```bash
   curl http://localhost:14268
   ```

2. **Verify component is built with tracing:**
   ```bash
   cargo build --release --features jaeger
   ```

3. **Check component is configured:**
   ```bash
   # Look for "Initialized OpenTelemetry tracing" in logs
   podman logs rusternetes-api-server | grep -i tracing
   ```

4. **Verify network connectivity:**
   ```bash
   # From inside component container
   podman exec rusternetes-api-server curl http://jaeger:14268
   ```

### Traces are incomplete

This usually means trace context isn't being propagated. Check that:
1. All components are built with tracing features
2. HTTP clients are configured to propagate trace headers
3. Sampling rate isn't too low (try 1.0 for testing)

### High overhead

Tracing adds minimal overhead (~1-5%), but if you're seeing performance issues:
1. Lower the sampling rate (try 0.1 or 0.01)
2. Check your tracing backend isn't overloaded
3. Use batch exporting (enabled by default)

## Examples

### Tracing a kubectl Request

```bash
# 1. Start Jaeger
podman run -d --name jaeger -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one

# 2. Start components with tracing
./target/release/api-server --tracing-exporter jaeger &
./target/release/scheduler --tracing-exporter jaeger &
./target/release/controller-manager --tracing-exporter jaeger &

# 3. Make a request
./target/release/kubectl --server https://localhost:6443 \
  --insecure-skip-tls-verify get pods

# 4. View trace in Jaeger UI
# Open http://localhost:16686
# Select service: api-server
# Click "Find Traces"
```

### Tracing with Custom Spans

```rust
use tracing::{info, instrument};

#[instrument]
async fn process_pod(pod: &Pod) -> Result<()> {
    info!("Processing pod: {}", pod.metadata.name);

    // Your code here...

    Ok(())
}
```

The `#[instrument]` macro automatically creates a span and adds it to the current trace.

## Further Reading

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [Tracing Crate Documentation](https://docs.rs/tracing/)

## Summary

Distributed tracing with OpenTelemetry provides:
✅ Request tracing across all components
✅ Performance profiling and bottleneck identification
✅ Debugging distributed systems issues
✅ Integration with popular observability platforms
✅ Production-ready sampling and export capabilities

Enable tracing in your Rusternetes deployment to gain deep insights into request flows and system behavior!
