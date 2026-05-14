# Getting Started — Running Components Individually

This guide shows how to run each rusternetes component as a separate process on your local machine. This is useful for development, debugging, and understanding how the components interact.

**For the fastest path to a running cluster, see [QUICKSTART.md](QUICKSTART.md)** which uses Docker Compose.

## Prerequisites

- **Rust toolchain** — install via [rustup.rs](https://rustup.rs/)
- **etcd** — for cluster state storage (or use SQLite/Redis with the all-in-one binary)
- **Docker** — for the kubelet to create containers

### Start etcd

```bash
docker run -d --name etcd -p 2379:2379 -p 2380:2380 \
  -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:latest
```

## Build

```bash
cargo build --release
```

This produces binaries in `target/release/`: `api-server`, `scheduler`, `controller-manager`, `kubelet`, `kube-proxy`, `kubectl`, and `rusternetes` (all-in-one).

## Start the Control Plane

Open separate terminals for each component:

### 1. API Server

```bash
./target/release/api-server \
  --bind-address 0.0.0.0:6443 \
  --etcd-servers http://localhost:2379 \
  --tls --tls-self-signed \
  --skip-auth \
  --console-dir ./console/dist   # optional: enables web console
```

The API server is now at `https://localhost:6443`. If you built the console, open `https://localhost:6443/console/`.

### 2. Scheduler

```bash
./target/release/scheduler \
  --etcd-servers http://localhost:2379
```

### 3. Controller Manager

```bash
./target/release/controller-manager \
  --etcd-servers http://localhost:2379
```

## Start Node Components

### 4. Kubelet

```bash
./target/release/kubelet \
  --node-name node-1 \
  --etcd-servers http://localhost:2379 \
  --cluster-dns 10.96.0.10
```

### 5. Kube-Proxy (optional, Linux only)

```bash
sudo ./target/release/kube-proxy \
  --node-name node-1 \
  --etcd-servers http://localhost:2379
```

Kube-proxy requires `CAP_NET_ADMIN` for iptables rules. Skip it if you don't need ClusterIP/NodePort routing.

## Use the Cluster

```bash
# Using standard kubectl
export KUBECONFIG=~/.kube/rusternetes-config
kubectl get nodes
kubectl create deployment nginx --image=nginx
kubectl get pods -w

# Or using the built-in kubectl
./target/release/kubectl \
  --server https://localhost:6443 \
  --insecure-skip-tls-verify \
  get pods
```

## All-in-One Alternative

Instead of running 5 separate processes, run everything in one:

```bash
./target/release/rusternetes \
  --data-dir ./cluster.db \
  --console-dir ./console/dist
```

This starts the API server, scheduler, controller manager, kubelet, and kube-proxy as concurrent tokio tasks with embedded SQLite. No etcd needed.

For Redis instead of SQLite:

```bash
cargo build -p rusternetes --features redis --release
./target/release/rusternetes --storage-backend redis --redis-url redis://localhost:6379
```

## Next Steps

- **[QUICKSTART.md](QUICKSTART.md)** — Docker Compose cluster (faster, no manual setup)
- **[Console User Guide](CONSOLE_USER_GUIDE.md)** — web console features
- **[DEVELOPMENT.md](DEVELOPMENT.md)** — build commands, testing, crate structure
