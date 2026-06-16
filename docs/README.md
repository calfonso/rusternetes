# Rūsternetes Documentation

A ground-up reimplementation of Kubernetes in Rust. This is the index of the
current documentation set. The [HTML documentation site](guide/index.html) has
themed pages with search, navigation, and console screenshots.

## Getting Started

- [Quick Start](QUICKSTART.md) — get a cluster running in a few minutes
- [Console User Guide](CONSOLE_USER_GUIDE.md) — the built-in web console

## Architecture & Development

- [Architecture](ARCHITECTURE.md) — components, data flow, and internal design
- [Development Guide](DEVELOPMENT.md) — local setup and build workflow
- [Contributing](CONTRIBUTING.md) — crate map and contribution guidelines

## Networking

- [CNI Integration](CNI_INTEGRATION.md) — third-party CNI plugins
- [MetalLB Integration](METALLB_INTEGRATION.md) — bare-metal LoadBalancer
- [Network Policies](networking/network-policies.md) — pod network policies

## Storage

- [Storage Backends](storage/STORAGE_BACKENDS.md) — etcd, SQLite, and Redis

## Security & Auth

- [Authentication](AUTHENTICATION.md) — authentication, RBAC, authorization
- [Security](SECURITY.md) — security features and hardening
- [TLS Guide](TLS_GUIDE.md) — TLS/mTLS configuration
- [Webhook Integration](WEBHOOK_INTEGRATION.md) — admission webhooks

## Operations

- [High Availability](HIGH_AVAILABILITY.md) — multi-replica control plane
- [AWS Deployment](AWS_DEPLOYMENT.md) — production deployment on AWS
- [Kubelet Configuration](KUBELET_CONFIGURATION.md) — node agent configuration
- [Tracing](TRACING.md) — distributed tracing

## API Extension

- [CRD Implementation](CRD_IMPLEMENTATION.md) — custom resource definitions

Conformance results are tracked in the repository's GitHub Projects
(Node Conformance and Conformance).
