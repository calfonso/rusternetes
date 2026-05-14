# Rusternetes Documentation

## Start Here

| I want to... | Read this |
|---|---|
| Get a cluster running in 5 minutes | [QUICKSTART.md](QUICKSTART.md) |
| Use the web console | [CONSOLE_USER_GUIDE.md](CONSOLE_USER_GUIDE.md) |
| Understand the architecture | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Set up a development environment | [DEVELOPMENT.md](DEVELOPMENT.md) |
| Deploy to production | [DEPLOYMENT.md](DEPLOYMENT.md) |
| Secure the cluster | [AUTHENTICATION.md](AUTHENTICATION.md) |

## Web Console

- **[CONSOLE_USER_GUIDE.md](CONSOLE_USER_GUIDE.md)** — Complete guide with screenshots: topology, metrics, resource management, live logs, multi-cluster

## Getting Started

- **[QUICKSTART.md](QUICKSTART.md)** — Get a cluster running in minutes
- **[GETTING_STARTED.md](GETTING_STARTED.md)** — Development setup guide
- **[DEVELOPMENT.md](DEVELOPMENT.md)** — Build commands, crate structure, testing

## Deployment

- **[DEPLOYMENT.md](DEPLOYMENT.md)** — Production deployment (Docker Compose, all-in-one)
- **[HIGH_AVAILABILITY.md](HIGH_AVAILABILITY.md)** — HA setup with etcd clustering and leader election
- **[AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md)** — AWS deployment with EC2, ELB, EBS
- **[FEDORA_SETUP.md](FEDORA_SETUP.md)** — Complete Fedora Linux setup
- **[PODMAN_TIPS.md](PODMAN_TIPS.md)** — Podman-specific setup and troubleshooting
- **[DOCKER_MIGRATION.md](DOCKER_MIGRATION.md)** — Migrating from Podman to Docker Desktop

## Security & Authentication

- **[AUTHENTICATION.md](AUTHENTICATION.md)** — Authentication setup: JWT tokens, RBAC, mTLS client certificates
- **[SECURITY.md](SECURITY.md)** — Admission controllers, Pod Security Standards, encryption at rest, audit logging
- **[TLS_GUIDE.md](TLS_GUIDE.md)** — TLS/HTTPS configuration and certificate management
- **[WEBHOOK_INTEGRATION.md](WEBHOOK_INTEGRATION.md)** — Validating and mutating admission webhooks
- **[Service Account Tokens](security/service-account-tokens.md)** — RS256 JWT token signing

## Networking

- **[CNI_GUIDE.md](CNI_GUIDE.md)** — CNI networking guide: default config, third-party plugins (Calico, Cilium, Flannel), troubleshooting
- **[CNI_INTEGRATION.md](CNI_INTEGRATION.md)** — CNI framework architecture and implementation
- **[LOADBALANCER.md](LOADBALANCER.md)** — LoadBalancer services and cloud providers
- **[METALLB_INTEGRATION.md](METALLB_INTEGRATION.md)** — MetalLB for local LoadBalancer services
- **[Network Policies](networking/network-policies.md)** — NetworkPolicy implementation and usage

## Storage

- **[Storage Backends](storage/STORAGE_BACKENDS.md)** — etcd, SQLite, Redis (via Rhino), memory backends
- **[DYNAMIC_PROVISIONING.md](DYNAMIC_PROVISIONING.md)** — Dynamic volume provisioning with StorageClasses
- **[VOLUME_SNAPSHOTS.md](VOLUME_SNAPSHOTS.md)** — Volume snapshot lifecycle
- **[VOLUME_EXPANSION.md](VOLUME_EXPANSION.md)** — Online PVC resize
- **[CSI Integration](storage/csi-integration.md)** — Container Storage Interface drivers

## API & Extensibility

- **[ADVANCED_API_FEATURES.md](ADVANCED_API_FEATURES.md)** — PATCH, field selectors, Server-Side Apply
- **[CRD_IMPLEMENTATION.md](CRD_IMPLEMENTATION.md)** — Custom Resource Definitions with watch, status/scale, schema validation

## Operations

- **[KUBELET_CONFIGURATION.md](KUBELET_CONFIGURATION.md)** — Kubelet configuration options
- **[TRACING.md](TRACING.md)** — Distributed tracing with OpenTelemetry
- **[Custom Metrics](metrics/metrics-integration.md)** — Prometheus metrics and custom metrics API
- **[BOOTSTRAP.md](BOOTSTRAP.md)** — Cluster bootstrap process

## Architecture & Design

- **[ARCHITECTURE.md](ARCHITECTURE.md)** — System architecture and component design
- **[WEBSOCKET_EXEC_IMPLEMENTATION.md](WEBSOCKET_EXEC_IMPLEMENTATION.md)** — WebSocket exec/attach implementation
- **[Runtime Abstraction Plan](planning/RUNTIME_ABSTRACTION.md)** — Future: pluggable container runtimes (Docker, Podman, process, Wasm)

## Testing & Conformance

- **[Testing Guide](testing/TESTING.md)** — How to run tests
- **[Test Status](testing/TEST_STATUS.md)** — Test coverage report
- **[CONFORMANCE.md](CONFORMANCE.md)** — Kubernetes v1.35 conformance tracking (90.2% pass rate)
- **[CONFORMANCE_FAILURES.md](CONFORMANCE_FAILURES.md)** — Active failure tracker

## Contributing

- **[CONTRIBUTING.md](CONTRIBUTING.md)** — Contribution guidelines and pre-commit checks

## HTML Documentation Site

The **[full documentation site](guide/index.html)** has 30 themed HTML pages with search, navigation, and screenshots.
