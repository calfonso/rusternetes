# Rūsternetes Development Guide

How to build, test, and run Rusternetes locally.

## Prerequisites

- **Rust** (latest stable, via [rustup](https://rustup.rs))
- **Container runtime** — Docker or Podman (see [Container Runtime Setup](#container-runtime-setup) below)
- **Compose tool** — `docker-compose` and/or `podman-compose` for orchestrating the cluster

On macOS, install everything with:

```bash
make install-deps
```

This installs `podman`, `podman-compose`, and `docker-compose` via Homebrew.

## Project Structure

Rusternetes is a Cargo workspace with 10 crates (216,000+ lines of Rust, 3,100+ tests):

| Crate | Purpose |
|-------|---------|
| `crates/common` | Shared resource types (Pod, Service, Deployment, etc.), errors, utilities |
| `crates/api-server` | Axum-based REST API with 75+ handler files and router.rs |
| `crates/storage` | Pluggable storage: etcd, SQLite/Redis (rhino), and in-memory backends |
| `crates/controller-manager` | 31 reconciliation controllers |
| `crates/kubelet` | Node agent, Docker container runtime via bollard |
| `crates/kube-proxy` | iptables-based service routing (host network mode) |
| `crates/scheduler` | Pod scheduling with affinity, taints, priority/preemption plugins |
| `crates/kubectl` | CLI tool |
| `crates/cloud-providers` | AWS/GCP/Azure integrations |
| `crates/rusternetes` | All-in-one binary (all components as concurrent tokio tasks) |

## Build and Test

```bash
# Build
cargo build                    # Debug build (fast iteration)
cargo build --release          # Release build

# Test
cargo test                     # All workspace tests
cargo test -p rusternetes_api_server  # Single crate (use underscores in package name)
cargo test test_name           # Single test by name
cargo test test_name -- --nocapture  # With stdout

# Lint and format
cargo fmt --all                # Format all code
cargo clippy --all-targets --all-features -- -D warnings  # Lint

# Pre-commit (format + clippy + test)
make pre-commit
```

## Container Runtime Setup

The cluster runs 7 services: etcd, api-server (port 6443 with TLS), scheduler,
controller-manager, two kubelets (node-1, node-2), and kube-proxy. kube-proxy
requires `CAP_NET_ADMIN` for iptables, which means rootful container execution.

### macOS -- Docker Desktop

Allocate at least **8GB of memory** to Docker Desktop (Settings > Resources).
Rust compilation will fail or be killed with the default 2GB.

```bash
# Install Docker Desktop
brew install --cask docker
# Then start Docker Desktop from Applications

# Install compose tool
brew install docker-compose

# Verify
docker info
docker compose version
```

### macOS -- Podman Machine

Podman Machine works on macOS with Apple Silicon (M1+) and Intel Macs.

> **Important:** On Apple Silicon, you must use the ARM-native Homebrew
> (`/opt/homebrew/bin/brew`), not the Intel/Rosetta one (`/usr/local/bin/brew`).
> An x86_64 `vfkit` binary cannot start VMs via Apple's Virtualization Framework
> on ARM hardware.

```bash
# Install via Homebrew (use /opt/homebrew/bin/brew on Apple Silicon)
brew install podman podman-compose docker-compose

# Initialize and start the VM in rootful mode (required for kube-proxy iptables)
# At least 8GB of memory is required — Rust compilation will fail with the 2GB default
podman machine init --memory 8192 --cpus 4
podman machine set --rootful
podman machine start

# Verify
podman info
podman-compose version
```

Podman uses a dedicated compose file (`compose.yml`) that mounts the
Podman socket (`/run/podman/podman.sock`) instead of the Docker socket:

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
podman-compose -f compose.yml build
podman-compose -f compose.yml up -d
```

The Makefile auto-detects the runtime and selects the right compose file:

```bash
make build-images   # uses compose.yml if podman-compose is found
make dev-up
```

### Linux -- Docker Engine

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in for group change to take effect

# Verify
docker info
docker compose version
```

### Linux -- Podman (rootful mode required)

Podman works on Linux but must run in rootful mode for kube-proxy iptables access.
All `docker compose` commands below become `sudo podman-compose -f compose.yml` commands.

```bash
# Fedora/RHEL/CentOS
sudo dnf install podman podman-compose docker-compose

# Ubuntu/Debian
sudo apt-get install podman podman-compose docker-compose

# All commands must run as root and use the podman compose file
sudo KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes podman-compose -f compose.yml build
sudo KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes podman-compose -f compose.yml up -d
```

The rest of this guide uses `podman compose` syntax. If you are using Docker,
substitute `docker compose` wherever you see `podman compose`.

## Running the Cluster

### Start

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes

podman compose build           # Build images (~1 hour first build, faster with cache, requires 8GB+ RAM)
podman compose up -d           # Start all services
bash scripts/bootstrap-cluster.sh  # Create CoreDNS, services, SA tokens
```

### KUBECONFIG

```bash
export KUBECONFIG=~/.kube/rusternetes-config
kubectl get nodes
kubectl get pods -A
```

### Logs and Status

```bash
podman compose ps              # List running containers
podman compose logs -f         # All service logs
podman compose logs -f api-server  # Single service
```

### Rebuild a Single Service

```bash
podman compose build api-server
podman compose up -d api-server
```

### Stop

```bash
podman compose down            # Stop services
podman compose down -v         # Stop and remove volumes (clean slate)
```

## Development Workflow

1. Make code changes
2. `cargo build` to verify compilation
3. `cargo test` to run tests
4. `podman compose build <service>` to rebuild the image for the changed crate
5. `podman compose up -d <service>` to restart it
6. `podman compose logs -f <service>` to verify

## Conformance Testing

```bash
bash scripts/run-conformance.sh       # Full conformance lifecycle
bash scripts/conformance-progress.sh  # Monitor pass/fail progress
```

E2e output is at `/tmp/sonobuoy/results/e2e.log` inside the e2e container. Save logs before cleanup:

```bash
podman cp "$E2E_CONTAINER:/tmp/sonobuoy/results/e2e.log" /tmp/e2e-roundXXX.log
```

## Adding a New Resource

1. Define the struct in `crates/common/src/resources/{type}.rs`
2. Add handlers in `crates/api-server/src/handlers/{type}.rs`
3. Register routes in `crates/api-server/src/router.rs`
4. Add a controller in `crates/controller-manager/src/controllers/` if needed
5. Add tests

## Key Conventions

### Serialization (critical for Kubernetes API compatibility)

- All resource structs use `#[serde(rename_all = "camelCase")]`
- Optional fields use `#[serde(skip_serializing_if = "Option::is_none")]`
- TypeMeta is flattened: `#[serde(flatten)] pub type_meta: TypeMeta`
- camelCase abbreviations follow Kubernetes style: `podIP`, `hostIP`, `containerID`

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

- Async tests use `#[tokio::test]`
- Use `MemoryStorage` (not etcd) for unit tests
- Use `#[serial_test::serial]` when tests share mutable state

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):
`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`

## Cluster Architecture

```
Container Network (rusternetes-network)

  etcd          api-server (6443, TLS)    scheduler
  controller-manager    node-1 (kubelet)    node-2 (kubelet)

Host Network:
  kube-proxy (CAP_NET_ADMIN for iptables)
```

- TLS certs are in `.rusternetes/certs/`, generated by `scripts/generate-certs.sh`
- Cert SANs include bridge IPs (172.18.0.2-5)
- CoreDNS ClusterIP is pinned to 10.96.0.10
- Pods use container bridge networking; containers use `container:pause` network mode
- kube-proxy requires `CAP_NET_ADMIN` for iptables rules

## Troubleshooting

### Port Already in Use

```bash
lsof -i :6443
lsof -i :2379
```

Stop conflicting services or change ports in the compose file.

### Container Build Fails

```bash
# Podman
podman system prune -a
podman compose build

# Docker
docker system prune -a
docker compose build
```

### Podman: kube-proxy Permission Denied

If kube-proxy logs show `Permission denied (you must be root)`, you are not
running in rootful mode. On macOS, run `podman machine set --rootful && podman machine stop && podman machine start`. On Linux, prefix compose commands with `sudo`.

### Podman Machine Fails on macOS

If you see `VZErrorDomain Code=1` or `vfkit exited unexpectedly`:

1. **Apple Silicon with Intel Homebrew:** If you have `/usr/local/bin/brew`
   (Intel) and `/opt/homebrew/bin/brew` (ARM), make sure podman was installed
   via the ARM Homebrew. An x86_64 `vfkit` binary cannot use Apple's
   Virtualization Framework on ARM. Reinstall:
   ```bash
   /opt/homebrew/bin/brew install podman podman-compose
   podman machine rm -f
   podman machine init
   podman machine set --rootful
   podman machine start
   ```
2. **Rosetta error:** If the log contains `rosetta is unsupported on non-arm64
   platforms`, the same Intel/ARM mismatch is the cause — see above.
3. **Other `VZErrorDomain` errors:** Try removing and recreating the machine:
   ```bash
   podman machine rm -f
   podman machine init
   podman machine start
   ```

### etcd Issues

```bash
podman compose logs etcd
podman compose restart etcd

# If persistent, clean and restart
podman compose down -v
podman compose up -d
```

### API Server Unreachable

```bash
podman compose ps api-server
podman compose logs api-server
curl -k https://localhost:6443/healthz
```

### Code Changes Not Reflected

Container images cache built binaries. Rebuild after changes:

```bash
podman compose build api-server
podman compose up -d --force-recreate api-server
```

### Clean Slate

```bash
podman compose down -v
podman system prune -a
podman compose build
podman compose up -d
bash scripts/bootstrap-cluster.sh
```
