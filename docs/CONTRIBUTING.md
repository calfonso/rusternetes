# Contributing to Rūsternetes

Thank you for your interest in contributing to Rusternetes. This is a Rust reimplementation of Kubernetes spanning 10 crates, 216,000+ lines of code, and 3,100+ tests. This document covers everything you need to get started.

## Prerequisites

- Rust (stable toolchain)
- Docker and Docker Compose (for integration testing)
- `make` (for convenience targets)

## Pre-Commit Checks

Run these before every commit:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Or run all three at once:

```bash
make pre-commit
```

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` -- New feature
- `fix:` -- Bug fix
- `docs:` -- Documentation changes
- `test:` -- Test changes
- `refactor:` -- Code refactoring
- `chore:` -- Maintenance tasks

## Development Workflow

1. Fork and clone the repository.
2. Create a feature branch from `main`.
3. Make your changes, add tests, and run `make pre-commit`.
4. Push and open a Pull Request.

## Testing

All async tests use `#[tokio::test]`. Unit tests should use `MemoryStorage` rather than etcd.

When tests must run sequentially, annotate them with `#[serial_test::serial]`.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_feature_works() {
        let storage = MemoryStorage::new();

        let result = your_function(&storage).await;

        assert!(result.is_ok());
    }
}
```

To run tests for a single crate (note: use underscores in the package name):

```bash
cargo test -p rusternetes-api-server
```

To run a single test with output:

```bash
cargo test test_name -- --nocapture
```

## Serialization Conventions

These are critical for Kubernetes API compatibility. Every resource struct must follow them:

- `#[serde(rename_all = "camelCase")]` on all resource structs
- `#[serde(skip_serializing_if = "Option::is_none")]` on optional fields
- `#[serde(flatten)]` on the `TypeMeta` field
- Kubernetes-style camelCase abbreviations: `podIP` (not `podIp`), `hostIP` (not `hostIp`), `containerID` (not `containerId`)

## Adding a New Resource Type

1. Define the struct in `crates/common/src/resources/{type}.rs`
2. Add handlers in `crates/api-server/src/handlers/{type}.rs`
3. Register routes in `crates/api-server/src/router.rs`
4. Add a controller in `crates/controller-manager/src/controllers/` if the resource needs reconciliation

## Controller Pattern

Controllers follow a standard reconciliation loop pattern:

```rust
pub struct FooController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
}

impl<S: Storage> FooController<S> {
    pub async fn run(&self) -> Result<()> {
        loop {
            self.reconcile_all().await?;
            tokio::time::sleep(self.interval).await;
        }
    }
}
```

New controllers go in `crates/controller-manager/src/controllers/` and are registered in the controller manager startup.

## Running the Cluster

For integration testing against a full cluster:

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
podman compose build        # or: docker compose build
podman compose up -d        # or: docker compose up -d
bash scripts/bootstrap-cluster.sh
```

Then interact with it:

```bash
export KUBECONFIG=~/.kube/rusternetes-config
kubectl get pods -A
```

The cluster runs etcd, the API server (port 6443 with TLS), a scheduler, a controller manager, two kubelets, and kube-proxy.

## Running Conformance Tests

```bash
bash scripts/run-conformance.sh
bash scripts/conformance-progress.sh
```

The e2e log is written to `/tmp/sonobuoy/results/e2e.log` inside the e2e container.

## Crate Overview

| Crate | Purpose |
|---|---|
| `common` | Shared resource types, error types, utilities |
| `api-server` | Axum-based REST API with per-resource handler files |
| `storage` | Pluggable storage: etcd, SQLite/Redis (rhino), and in-memory backends |
| `controller-manager` | 31 controllers following the reconciliation loop pattern |
| `kubelet` | Container runtime via bollard, pod lifecycle, volumes, probes |
| `kube-proxy` | iptables-based service routing |
| `scheduler` | Pod scheduling with affinity, taints, tolerations, preemption |
| `kubectl` | CLI tool |
| `rusternetes` | All-in-one binary (all components as tokio tasks, embedded SQLite or Redis) |
| `cloud-providers` | AWS, GCP, and Azure integrations |

## Code Style

- Follow Rust standard naming conventions.
- Use `cargo fmt` for formatting and `cargo clippy` for linting.
- Write clear, self-documenting code. Add comments for complex logic.
- Keep functions focused. Prefer small, testable units.
- Avoid `unsafe` code unless absolutely necessary.

## Pull Request Process

1. Ensure `make pre-commit` passes with no errors.
2. Provide a clear description of the changes and any related issues.
3. Include the testing you performed.
4. Address review feedback promptly.

## License

By contributing to Rusternetes, you agree that your contributions will be licensed under the Apache-2.0 License.
