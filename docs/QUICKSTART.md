# Rūsternetes Quick Start

Get a Rust-based Kubernetes cluster running locally with a built-in web console.

## Prerequisites

- **Container runtime** — Podman or Docker
- **kubectl** (optional) — standard `kubectl` works against the cluster

### macOS — Podman

```bash
brew install podman podman-compose docker-compose
podman machine init --memory 8192 --cpus 4
podman machine set --rootful
podman machine start
```

### macOS — Docker Desktop

```bash
brew install --cask docker
# Start Docker Desktop from Applications
```

### Linux — Podman (rootful mode required)

```bash
# Fedora/RHEL/CentOS
sudo dnf install podman podman-compose docker-compose

# Ubuntu/Debian
sudo apt-get install podman podman-compose docker-compose
```

On Linux, Podman must run in rootful mode because kube-proxy needs `CAP_NET_ADMIN` for iptables. Prefix compose commands with `sudo`.

### Linux — Docker Engine

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in for the group change to take effect
```

## Start the Cluster

```bash
git clone https://github.com/calfonso/rusternetes.git
cd rusternetes

export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes

# Podman
podman compose build           # ~1 hour first build, faster with cache
podman compose up -d

# Or Docker
docker compose build
docker compose up -d

bash scripts/bootstrap-cluster.sh
```

## Open the Console

The web console deploys automatically with the cluster:

```
https://localhost:6443/console/
```

Accept the self-signed certificate warning. You'll see the cluster overview with health rings, live metrics, and a topology map.

See the [Console User Guide](CONSOLE_USER_GUIDE.md) for a full walkthrough of every feature.

## Use kubectl

```bash
export KUBECONFIG=~/.kube/rusternetes-config

kubectl get nodes
kubectl get pods -A
kubectl create deployment nginx --image=nginx
kubectl get pods -w    # watch pods start
```

Standard `kubectl` works because rusternetes implements the same REST API as upstream Kubernetes.

## What You Get

| Component | Details |
|---|---|
| API Server | Port 6443, HTTPS, REST API + Watch + RBAC + Webhooks + **Web Console** |
| Scheduler | Affinity, taints/tolerations, priority, preemption |
| Controller Manager | 31 controllers (Deployment, StatefulSet, Job, DaemonSet, HPA, etc.) |
| Kubelet (node-1, node-2) | Container runtime via Docker/bollard, probes, volumes |
| Kube-Proxy | iptables ClusterIP/NodePort/LoadBalancer routing |
| CoreDNS | Kubernetes service discovery |
| Storage | etcd (default), SQLite, or Redis via [Rhino](https://github.com/calfonso/rhino) |
| Default StorageClass | `standard` with `rusternetes.io/hostpath` provisioner |

TLS certificates are auto-generated in `.rusternetes/certs/`.

## Alternative: SQLite Instead of etcd

Same cluster, but [Rhino](https://github.com/calfonso/rhino) replaces etcd with SQLite:

```bash
# Podman
podman compose -f compose.sqlite.yml build
podman compose -f compose.sqlite.yml up -d

# Or Docker
docker compose -f docker-compose.sqlite.yml build
docker compose -f docker-compose.sqlite.yml up -d

bash scripts/bootstrap-cluster.sh
```

## Alternative: Redis Instead of etcd

Same cluster, but [Rhino](https://github.com/calfonso/rhino) replaces etcd with Redis:

```bash
# Podman
podman compose -f compose.redis.yml build
podman compose -f compose.redis.yml up -d

bash scripts/bootstrap-cluster.sh
```

## Alternative: All-in-One Binary

Full Kubernetes in a single process with embedded SQLite -- no Docker Compose, no etcd:

```bash
cargo build -p rusternetes --release
./target/release/rusternetes --data-dir ./cluster.db --console-dir ./console/dist
```

Or with Redis instead of SQLite:

```bash
cargo build -p rusternetes --features redis --release
./target/release/rusternetes --storage-backend redis --redis-url redis://localhost:6379 --console-dir ./console/dist
```

Open `https://localhost:6443/console/` for the web console.

**Note:** The all-in-one binary still needs Podman or Docker running on the host for the kubelet to create containers.

## Common Operations

### View logs

```bash
podman compose logs -f api-server    # or: docker compose logs -f api-server
podman compose logs -f kubelet
```

### Rebuild after code changes

```bash
podman compose build api-server      # rebuild one component
podman compose up -d api-server      # redeploy it
```

### Stop the cluster

```bash
podman compose down                  # or: docker compose down
```

### Run tests

```bash
cargo test                                    # all tests
cargo test -p rusternetes-api-server          # single crate
make pre-commit                               # format + clippy + test
```

### Run conformance tests

```bash
bash scripts/run-conformance.sh
bash scripts/conformance-progress.sh   # monitor pass/fail
```

## Troubleshooting

### "Port already in use"

```bash
lsof -i :6443    # find what's using the API server port
lsof -i :2379    # find what's using the etcd port
# Kill the conflicting process or stop other K8s clusters
```

### "Cannot connect to API server"

```bash
podman compose ps            # check all services are running
podman compose logs api-server | tail -20   # check for errors
```

### "Build is taking too long"

The first build compiles all Rust crates inside the container (~1 hour without cache). Subsequent builds use the layer cache and are much faster. For local iteration, use `cargo build` directly.

### Console shows no data

The console needs the cluster to be bootstrapped. Run `bash scripts/bootstrap-cluster.sh` if you haven't.

## What's Different from Real Kubernetes

Rusternetes is a ground-up reimplementation — not a fork. Every component is written from scratch in Rust.

- 216,000+ lines of Rust across 10 crates
- 90% conformance pass rate (398/441 tests) across 149 rounds of testing
- Uses Podman or Docker as the container runtime via the bollard library
- Standard `kubectl` works against it (same REST API)
- Built-in web console with topology visualization, live metrics, and pod log streaming
- Supports CNI plugins (Calico, Cilium, Flannel) on Linux

## Next Steps

- **[Console User Guide](CONSOLE_USER_GUIDE.md)** — topology, metrics, resource management, live logs
- **[Authentication Guide](AUTHENTICATION.md)** — secure the cluster with JWT tokens and RBAC
- **[CNI Networking Guide](CNI_GUIDE.md)** — configure third-party CNI plugins
- **[Deployment Guide](DEPLOYMENT.md)** — production deployment options
- **[Full Documentation Site](guide/index.html)** — 30 themed pages covering every feature
