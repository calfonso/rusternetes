# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
# Build
cargo build                    # Debug build (fast iteration)
cargo build --release          # Release build

# Test
cargo test                     # All workspace tests
cargo test -p rusternetes-api-server  # Single crate
cargo test test_name           # Single test by name
cargo test test_name -- --nocapture  # With stdout

# Lint & Format
cargo fmt --all                # Format (REQUIRED before every commit — see below)
cargo fmt --all -- --check     # Verify formatting without writing (CI uses this)
cargo clippy --all-targets --all-features -- -D warnings  # Lint
make pre-commit                # Format + clippy + test (run before commits)

# Cluster (Podman or Docker)
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
podman compose build           # Build images (~1 hour first build, faster with cache)
podman compose up -d           # Start cluster
podman compose down            # Stop cluster
bash scripts/bootstrap-cluster.sh  # Create CoreDNS, services, SA tokens

# Conformance testing
bash scripts/run-conformance.sh       # Full conformance lifecycle
bash scripts/conformance-progress.sh  # Monitor pass/fail progress
# e2e output is in /tmp/sonobuoy/results/e2e.log inside the e2e container
```

KUBECONFIG: `~/.kube/rusternetes-config`

## Architecture

Rust reimplementation of Kubernetes. Workspace with 10 crates:

- **`common`** - Shared resource types (Pod, Deployment, Service, etc.), error types, utilities. All resource structs live in `src/resources/`. Error type in `src/error.rs` maps to Kubernetes StatusReason and implements Axum's `IntoResponse`.
- **`api-server`** - Axum-based REST API. Routes in `src/router.rs` (~9400 lines). One handler file per resource type in `src/handlers/`. State in `src/state.rs` holds storage, auth, IP allocator, webhook manager, watch cache.
- **`storage`** - `Storage` trait in `src/lib.rs` with etcd backend (`src/etcd.rs`), SQLite backend via rhino (`src/rhino.rs`, behind `sqlite` feature), and memory backend (`src/memory.rs`). `StorageBackend` enum dispatches to the selected backend. Keys follow `/registry/{resource_type}/{namespace}/{name}`. Resource versions map to backend revision numbers.
- **`controller-manager`** - 31 controllers in `src/controllers/`. Each has a struct with `storage: Arc<S>` + `interval: Duration`, an infinite `run()` loop, and `reconcile_one()` per resource.
- **`kubelet`** - Container runtime via bollard (Docker API). Manages pod lifecycle, volumes, probes, networking.
- **`kube-proxy`** - iptables-based service routing. Runs in host network mode. Reads both Endpoints and EndpointSlices.
- **`scheduler`** - Pod scheduling with affinity, taints/tolerations, priority/preemption. Plugins in `src/plugins/`.
- **`kubectl`** - CLI tool. Commands in `src/commands/`.
- **`cloud-providers`** - AWS/GCP/Azure integrations.
- **`rusternetes`** - All-in-one binary. Spawns api-server, scheduler, controller-manager, kubelet, and kube-proxy as concurrent tokio tasks sharing one `StorageBackend`. Defaults to embedded SQLite.

## Key Conventions

### Serialization (critical for K8s API compatibility)
- All resource structs use `#[serde(rename_all = "camelCase")]`
- Optional fields use `#[serde(skip_serializing_if = "Option::is_none")]`
- TypeMeta is flattened: `#[serde(flatten)] pub type_meta: TypeMeta`
- camelCase abbreviations follow K8s style: `podIP` not `podIp`, `hostIP` not `hostIp`, `containerID` not `containerId`

### Adding a New Resource
1. Define struct in `crates/common/src/resources/{type}.rs`
2. Add handlers in `crates/api-server/src/handlers/{type}.rs`
3. Register route in `crates/api-server/src/router.rs`
4. Add controller in `crates/controller-manager/src/controllers/` if needed

### Controller Pattern
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

### Testing
- Async tests: `#[tokio::test]`
- Use `MemoryStorage` (not etcd) for unit tests
- Serial tests when needed: `#[serial_test::serial]`

### Commit Messages
Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`.

Keep the subject line ≤72 characters. The GitHub PR-creation page truncates longer titles, and CI / commitlint flags them. If you exceed 72, amend before pushing.

### Pre-push checklist (REQUIRED)
The `.github/workflows/fmt.yml` action runs `cargo fmt --all -- --check` on every PR. If formatting drift exists, the PR is red and unmergeable. **Before pushing any branch, run:**

```bash
cargo fmt --all
```

This applies the canonical style in-place. Then verify with:

```bash
cargo fmt --all -- --check    # must exit 0
```

If you forget and push a branch with formatting drift, the `Check formatting` job goes red. Recovery: run `cargo fmt --all` locally, `git add -A`, `git commit --amend --no-edit`, `git push --force-with-lease`. Prefer to catch it before pushing — `make pre-commit` runs fmt + clippy + test in one shot.

When working across multiple branches (rebases, cherry-picks, parallel work), re-run `cargo fmt --all` on **every** branch before pushing, not just the one you happen to be on. Multi-line `if let` bindings and method chains are the most common drift cases.

## Cluster Details

Services: etcd, api-server (port 6443 with TLS), scheduler, controller-manager, 2x kubelet (node-1, node-2), kube-proxy (host network).

- TLS certs in `.rusternetes/certs/`, generated by `scripts/generate-certs.sh`
- Cert SANs must include bridge IPs (172.18.0.2-5)
- kube-proxy needs `CAP_NET_ADMIN` for iptables; runs host network mode
- `KUBERNETES_SERVICE_HOST_OVERRIDE` env var sets the API server address for pods
- CoreDNS ClusterIP pinned to 10.96.0.10
