# Rūsternetes — Project Overview

A complete, from-scratch reimplementation of **Kubernetes in Rust**. Every
component — API server, scheduler, controller manager, kubelet, kube-proxy,
DNS — is written in Rust against the real Kubernetes API surface, wire format,
and behavioral semantics. It is not a wrapper around the Go codebase or a mock.

This document is the single starting point: what Rusternetes is, how it is put
together, every binary and its flags, the container images we publish, and how
to deploy it — on a laptop in Docker, as a single binary on bare metal, or as a
multi-node cluster.

> New here and just want to run something? Jump to
> [Deployment → Run locally in Docker](#run-locally-in-docker-or-podman) or
> [the all-in-one binary](#the-all-in-one-binary).

---

## Table of contents

- [What it is](#what-it-is)
- [Why it exists (the north star)](#why-it-exists-the-north-star)
- [How it works (architecture)](#how-it-works-architecture)
- [Storage backends](#storage-backends)
- [The binaries and their CLI options](#the-binaries-and-their-cli-options)
  - [api-server](#api-server)
  - [scheduler](#scheduler)
  - [controller-manager](#controller-manager)
  - [kubelet](#kubelet)
  - [kube-proxy](#kube-proxy)
  - [dns](#dns)
  - [kubectl](#kubectl)
- [The all-in-one binary](#the-all-in-one-binary)
- [Container images](#container-images)
- [Deployment](#deployment)
  - [Prerequisites](#prerequisites)
  - [Run locally in Docker or Podman](#run-locally-in-docker-or-podman)
  - [The all-in-one container](#the-all-in-one-container)
  - [Bare metal](#bare-metal)
  - [High availability](#high-availability)
  - [Bootstrapping and certificates](#bootstrapping-and-certificates)
- [Web console](#web-console)
- [Conformance](#conformance)
- [Roadmap](#roadmap)
- [Development](#development)
- [Further documentation](#further-documentation)
- [License](#license)

---

## What it is

Rūsternetes implements the Kubernetes control plane and node components as
native Rust binaries:

| Component | Crate | Role |
|---|---|---|
| **API server** | `api-server` | Axum HTTPS server: the full REST + Watch API, RBAC, admission webhooks, CRDs, Server-Side Apply, and the embedded web console |
| **Scheduler** | `scheduler` | Filter/score plugin scheduling: affinity, taints/tolerations, priority/preemption, topology spread |
| **Controller manager** | `controller-manager` | 31 reconciliation control loops (Deployment, ReplicaSet, Job, Endpoints, PV binding, HPA, GC, …) |
| **Kubelet** | `kubelet` | Pod lifecycle via CRI (CRI v1 gRPC → containerd → Youki): probes, volumes, init/sidecar containers, exec/attach |
| **Kube-proxy** | `kube-proxy` | iptables service routing (ClusterIP / NodePort / LoadBalancer) in host-network mode |
| **DNS** | `dns` | Cluster DNS for Services and Pods |
| **kubectl** | `kubectl` | A from-scratch `kubectl` CLI |
| **All-in-one** | `rusternetes` | Every component above as concurrent async tasks in one process, with embedded storage |

Shared resource types, error mapping, and utilities live in the `common` crate;
storage backends live in `storage`; cloud integrations in `cloud-providers`.

The same binaries run in every deployment mode — full multi-container cluster,
SQLite/Redis-backed cluster, or single-process all-in-one. Only the wiring and
the storage backend change.

## Why it exists (the north star)

Rusternetes exists to answer one concrete question: **can a fully conformant
Kubernetes run on hardware this small?**

The north star is a **four-node cluster of Raspberry Pi 3A+ boards** — 512 MB of
RAM each, a quad-core Cortex-A53, wired over USB-ethernet, a single micro-SD card
per node holding OS, binary, and all cluster state. No etcd quorum, no
gigabyte-per-node control plane, no external dependencies.

That constraint drives every design decision:

- **Rust, from scratch** — a control plane measured in hundreds of MB, not
  gigabytes, leaving a 512 MB node room for real workloads.
- **Embedded SQLite storage, no etcd** — the all-in-one binary keeps cluster
  state in one SQLite file, shedding etcd's RAM cost and the write amplification
  that is brutal on flash.
- **One process, every component** — the control plane and node agents run as
  concurrent tokio tasks in a single binary you can power off a USB port.

Positioning: *"k3s without melting your laptop."* Conformance on commodity x86 is
the proving ground; the small-board cluster is the destination.

## How it works (architecture)

```
┌───────────────────────────────────────────────────────────────┐
│                        Control Plane                          │
│                                                               │
│  ┌──────────────────┐  ┌──────────────┐  ┌────────────────┐   │
│  │  API Server      │  │  Scheduler   │  │  Controller    │   │
│  │  Axum + TLS      │  │  Affinity    │  │  Manager       │   │
│  │  REST + Watch    │  │  Taints      │  │  31 control    │   │
│  │  RBAC + Webhooks │  │  Preemption  │  │  loops         │   │
│  │  Web Console     │  │              │  │                │   │
│  └────────┬─────────┘  └──────────────┘  └────────────────┘   │
│           │                                                   │
│  ┌────────▼───────────────────┐                              │
│  │ Storage trait              │                              │
│  │ etcd | SQLite | Redis | mem│                              │
│  └────────────────────────────┘                              │
├───────────────────────────────────────────────────────────────┤
│                        Node Components                        │
│                                                               │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────┐  │
│  │  Kubelet         │  │  Kube-Proxy      │  │  DNS       │  │
│  │  CRI → containerd│  │  iptables routing│  │  Services  │  │
│  │  Probes+Volumes  │  │  ClusterIP/NP/LB │  │  + Pods    │  │
│  └──────────────────┘  └──────────────────┘  └────────────┘  │
└───────────────────────────────────────────────────────────────┘
```

Components communicate through the storage layer and the API server exactly as
upstream Kubernetes does. Resource keys follow
`/registry/{resource_type}/{namespace}/{name}`; resource versions map to the
backend's revision numbers.

## Storage backends

Storage is pluggable behind a single `Storage` trait, selected at runtime with
`--storage-backend`:

| Backend | Selector | Notes |
|---|---|---|
| **etcd** | `--storage-backend etcd` (default for the per-component binaries) | Standard production backend with optimistic concurrency (CAS via `mod_revision`). Talk to it with `--etcd-servers`. |
| **SQLite via Rhino** | `--storage-backend sqlite` | [Rhino](https://github.com/calfonso/rhino) is an etcd-v3-gRPC server written in Rust, backed by SQLite. Either run it as a container (the components keep their `--etcd-servers` flag, pointed at Rhino) or embed it in-process in the all-in-one binary. |
| **Redis via Rhino** | `--storage-backend redis` | Same Rhino server, Redis-backed. Build the all-in-one with `--features redis`. |
| **Memory** | (used by unit tests) | In-process, non-persistent. |

Because Rhino speaks the etcd v3 gRPC API, **the same binaries work against etcd
or Rhino with no recompilation** — you only change the compose file or the
`--storage-backend`/`--etcd-servers` flags.

## The binaries and their CLI options

All flags below are taken from each crate's argument parser. Every per-component
binary shares a common set of storage/logging flags
(`--etcd-servers`, `--storage-backend`, `--data-dir`, `--log-level`).

### api-server

```
rusternetes-api-server [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `--bind-address` | `0.0.0.0:6443` | Address to bind |
| `--etcd-servers` | `http://localhost:2379` | etcd/Rhino endpoints (comma-separated) |
| `--storage-backend` | `etcd` | `etcd` or `sqlite` |
| `--data-dir` | `./data/rusternetes.db` | SQLite path (when `--storage-backend=sqlite`) |
| `--log-level` | `info` | Log level |
| `--jwt-secret` | `rusternetes-secret-change-in-production` | ServiceAccount token signing secret |
| `--tls` | `false` | Enable TLS/HTTPS |
| `--tls-cert-file` | — | TLS certificate (PEM) |
| `--tls-key-file` | — | TLS private key (PEM) |
| `--tls-self-signed` | `false` | Generate a self-signed cert if no files given |
| `--tls-san` | `localhost,127.0.0.1` | SANs for the self-signed cert (comma-separated) |
| `--client-ca-file` | — | Client CA for mTLS client-certificate auth |
| `--skip-auth` | `false` | **INSECURE** — skip authn/authz (development only) |
| `--prometheus-url` | — | Prometheus URL for custom-metrics HPA |
| `--console-dir` | — | Path to the console SPA build; enables the web console at `/console/` |

### scheduler

```
rusternetes-scheduler [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `--etcd-servers` | `http://localhost:2379` | etcd/Rhino endpoints |
| `--storage-backend` | `etcd` | `etcd` or `sqlite` |
| `--data-dir` | `./data/rusternetes.db` | SQLite path |
| `--log-level` | `info` | Log level |
| `--interval` | `2` | Scheduling interval (seconds) |
| `--metrics-port` | `8081` | Metrics server port |
| `--enable-leader-election` | `false` | Enable leader election (HA) |
| `--leader-election-identity` | — | Unique identity per instance |
| `--leader-election-lock-key` | `/rusternetes/scheduler/leader` | Lock key |
| `--leader-election-lease-duration` | `15` | Lease duration (seconds) |

### controller-manager

```
rusternetes-controller-manager [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `--etcd-servers` | `http://localhost:2379` | etcd/Rhino endpoints |
| `--storage-backend` | `etcd` | `etcd` or `sqlite` |
| `--data-dir` | `./data/rusternetes.db` | SQLite path |
| `--log-level` | `info` | Log level |
| `--sync-interval` | `5` | Controller sync interval (seconds) |
| `--cloud-provider` | — | `aws`, `gcp`, `azure`, or none |
| `--cluster-name` | `rusternetes` | Cluster name for cloud resources |
| `--cloud-region` | — | Cloud provider region |
| `--enable-leader-election` | `false` | Enable leader election (HA) |
| `--leader-election-identity` | — | Unique identity per instance |
| `--leader-election-lock-key` | `/rusternetes/controller-manager/leader` | Lock key |
| `--leader-election-lease-duration` | `15` | Lease duration (seconds) |

### kubelet

```
rusternetes-kubelet --node-name <NAME> [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `--node-name` | *(required)* | Node name |
| `--etcd-servers` | `http://localhost:2379` | etcd/Rhino endpoints |
| `--storage-backend` | `etcd` | `etcd` or `sqlite` |
| `--data-dir` | `./data/rusternetes.db` | SQLite path |
| `--log-level` | — | Log level (trace/debug/info/warn/error) |
| `--config` | — | Path to a kubelet config file |
| `--root-dir` | — | Root dir for kubelet files (volume data, plugin state) |
| `--volume-dir` | — | Directory for volume data |
| `--volume-plugin-dir` | — | Directory where volume plugins are installed |
| `--sync-interval` | — | Sync interval (seconds) |
| `--metrics-port` | — | Metrics/read-only server port (`:10250` in compose) |
| `--cluster-dns` | *(auto-discovered)* | Cluster DNS Service IP |
| `--cluster-domain` | `cluster.local` | Cluster domain suffix |
| `--network` | `rusternetes-network` | Container network pods attach to |
| `--eviction-hard` | — | Hard eviction thresholds (`<signal><op><value>`; empty disables) |
| `--eviction-soft` | — | Soft eviction thresholds (same format) |
| `--eviction-soft-grace-period` | — | Soft-eviction grace periods (`<signal>=<duration>`) |
| `--eviction-minimum-reclaim` | — | Minimum reclaim per pass (accepted for parity) |
| `--eviction-pressure-transition-period` | `5m` | Time held in a pressure state after recovery |

Eviction signals: `memory.available`, `nodefs.available`, `nodefs.inodesFree`,
`imagefs.available`, `imagefs.inodesFree`, `pid.available` (only the `<`
operator is supported).

### kube-proxy

```
rusternetes-kube-proxy --node-name <NAME> [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `--node-name` | *(required)* | Node name |
| `--etcd-servers` | `http://localhost:2379` | etcd/Rhino endpoints |
| `--storage-backend` | `etcd` | `etcd` or `sqlite` |
| `--data-dir` | `./data/rusternetes.db` | SQLite path |
| `--log-level` | `info` | Log level |
| `--sync-interval` | `1` | Sync interval (seconds) |
| `--cluster-cidr` | *(built-in default)* | ClusterIP CIDR; must match the apiserver's `--service-cluster-ip-range`. Scopes the POSTROUTING MASQUERADE rule. |
| `--node-port-range` | *(built-in default)* | NodePort range as `start:end` (hyphen form accepted and normalized). |

Kube-proxy runs in host-network mode and needs `CAP_NET_ADMIN` (see
[Bare metal](#bare-metal)).

### dns

```
rusternetes-dns [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `--etcd-servers` | `http://localhost:2379` | etcd/Rhino endpoints |
| `--storage-backend` | `etcd` | `etcd` or `sqlite` |
| `--data-dir` | `./data/rusternetes.db` | SQLite path |
| `--log-level` | `info` | Log level |
| `--cluster-zone` | `cluster.local` | Cluster zone suffix |
| `--udp-bind` | `0.0.0.0:53` | UDP bind address |
| `--tcp-bind` | `0.0.0.0:53` | TCP bind address |
| `--resync-interval` | `30` | Full-resync interval (seconds) — safety net for missed watches |

### kubectl

A from-scratch `kubectl`. Global flags apply to every subcommand:
`--kubeconfig`, `--context`, `--server`, `--insecure-skip-tls-verify`,
`--token`.

It implements the full familiar surface, including: `get`, `describe`, `create`,
`apply`, `delete`, `replace`, `edit`, `patch`, `run`, `expose`, `scale`,
`autoscale`, `rollout` (status/history/undo/restart/pause/resume), `logs`,
`exec`, `attach`, `port-forward`, `cp`, `proxy`, `top` (node/pod), `label`,
`annotate`, `taint`, `cordon`/`uncordon`/`drain`, `wait`, `diff`, `explain`,
`auth` (can-i/whoami), `api-resources`, `api-versions`, `config`,
`cluster-info`, `events`, `certificate` (approve/deny), `debug`, `set`,
`completion`, and `version`.

## The all-in-one binary

The `rusternetes` binary runs the **entire cluster in one process** — API
server, scheduler, controller manager, kubelet, kube-proxy, and DNS as
concurrent tokio tasks sharing one storage backend. It defaults to **embedded
SQLite**, so there is no etcd and no external infrastructure: the whole cluster
state lives in a single file.

This is the mode built for environments where a full multi-container cluster is
overkill: **edge devices, IoT gateways, CI/CD pipelines, local development,
embedded systems, and air-gapped hosts** — and it is the path toward the
Raspberry-Pi north star.

```bash
# Build (SQLite default)
cargo build --release -p rusternetes
./target/release/rusternetes --data-dir ./cluster.db

# With Redis instead of SQLite
cargo build --release -p rusternetes --features redis
./target/release/rusternetes --storage-backend redis --redis-url redis://localhost:6379
```

Selected flags (run `rusternetes --help` for the complete list):

| Flag | Default | Description |
|---|---|---|
| `--storage-backend` | `sqlite` | `sqlite`, `etcd`, or `redis` |
| `--data-dir` | `./data/rusternetes.db` | SQLite database path |
| `--etcd-servers` | `http://localhost:2379` | etcd endpoints (when `etcd`) |
| `--redis-url` | `redis://localhost:6379` | Redis URL (when `redis`) |
| `--bind-address` | `0.0.0.0:6443` | API server bind address |
| `--node-name` | `node-1` | Embedded kubelet node name |
| `--volume-dir` | `./data/volumes` | Pod volume directory |
| `--cluster-dns` | `10.96.0.10` | Cluster DNS ClusterIP |
| `--network` | `rusternetes-network` | Container network name |
| `--tls` / `--tls-cert-file` / `--tls-key-file` / `--tls-san` | `false` / — / — / `localhost,127.0.0.1` | TLS controls |
| `--client-ca-file` | — | Client CA for mTLS |
| `--skip-auth` | `true` | Skip authn/authz (development default for this binary) |
| `--console-dir` | — | Enable the web console at `/console/` |
| `--kubernetes-service-host` | *(env `KUBERNETES_SERVICE_HOST_OVERRIDE`)* | API host injected into pod env |
| `--disable-proxy` | `false` | Disable the in-process kube-proxy (when iptables is unavailable) |
| `--disable-dns` | `false` | Disable the in-process DNS server |
| `--dns-bind` | `0.0.0.0:53` | DNS server bind (UDP+TCP) |
| `--cluster-cidr` | `10.96.0.0/12` | Service ClusterIP CIDR (kube-proxy scope) |
| `--node-port-range` | `30000:32767` | NodePort range (`start:end`) |
| `--pod-network-mode` | `cni` | `cni`, `netstack`/`netstack-shadow`, or `netstack-active` |
| `--netstack-pod-cidr` | `10.244.0.0/16` | Pod CIDR for the embedded netstack allocator |
| `--netstack-service-cidr` | `10.96.0.0/12` | Service CIDR the netstack treats as on-link |
| `--sync-interval` / `--scheduler-interval` / `--kubelet-sync-interval` / `--proxy-sync-interval` | `5` / `2` / `3` / `1` | Per-component loop intervals (seconds) |
| `--log-level` | `info` | Log level |

## Container images

All runtime images are built **`FROM debian:sid-slim`**. Rust builder stages
pin `rust:1.95`. The two Dockerfiles that matter for deployment are
`Dockerfile.services` (per-component) and `Dockerfile.all-in-one` (single
binary).

| Dockerfile | Build targets | Produces |
|---|---|---|
| **`Dockerfile.services`** | `api-server`, `kubelet`, `scheduler`, `controller-manager`, `kube-proxy`, `dns` | One image per component. A shared `cargo-builder` stage compiles all binaries once; a `console-builder` stage bundles the web console into the `api-server` image. |
| **`Dockerfile.all-in-one`** | *(single stage)* | The `rusternetes` all-in-one image, built with `CARGO_FEATURES=sqlite` or `redis`, console bundled in. |
| **`Dockerfile.rhino`** | *(single stage)* | The `rhino-server` image — etcd-compatible gRPC over SQLite/Redis. |
| **`Dockerfile.dns`** | *(single stage)* | Standalone `rusternetes-dns`. |
| **`Dockerfile.kubectl`** | *(single stage)* | Standalone `kubectl`. |
| **`Dockerfile.local-binary.services`** | `api-server`, `kubelet`, `scheduler`, `controller-manager`, `kube-proxy` | Thin images that **bind a host-built `target/release/<bin>`** instead of compiling — fast local iteration. |

> Compose stacks build these images locally via `docker compose build` /
> `podman compose build`. CI builds them the same way; the only image published
> to a registry today is the self-hosted Actions runner image
> (`ghcr.io/indyjonesnl/rusternetes-arc-runner`), which is infrastructure, not a
> Rusternetes component.

## Deployment

### Prerequisites

- **Rust** (stable toolchain — see `rust-toolchain.toml`) to build from source.
- **A CRI runtime (containerd)** on every node that runs a kubelet — the kubelet
  drives it over CRI v1 gRPC at `CONTAINER_RUNTIME_ENDPOINT`. Docker or Podman is
  still used to run the compose stack of node containers.
- **The Rhino submodule**, required for SQLite/Redis builds:
  ```bash
  git clone --recurse-submodules https://github.com/calfonso/rusternetes.git
  # or, in an existing checkout:
  git submodule update --init
  ```
- A copy of `kubectl` (this project's, or upstream — the API is compatible).

### Run locally in Docker or Podman

The fastest way to a real multi-container cluster on one machine. Pick a storage
backend; the components are identical across them.

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes

# Docker
docker compose build
docker compose up -d

# Podman
podman compose build
podman compose up -d

# Then, regardless of runtime:
bash scripts/bootstrap-cluster.sh
export KUBECONFIG=~/.kube/rusternetes-config
kubectl get nodes
```

Available stacks:

| Compose file | Storage | Brings up |
|---|---|---|
| `compose.yml` / `docker-compose.yml` | etcd | etcd, api-server, scheduler, controller-manager, 2× kubelet, kube-proxy, dns |
| `compose.sqlite.yml` / `docker-compose.sqlite.yml` | Rhino + SQLite | same components, Rhino instead of etcd |
| `compose.redis.yml` | Rhino + Redis | same components, Redis-backed Rhino |
| `compose.all-in-one.yml` | embedded SQLite | a single `rusternetes` container |
| `compose.all-in-one-redis.yml` | Redis | `rusternetes` + a Redis container |
| `compose.ha.yml` / `docker-compose.ha.yml` | 3-node etcd | 3× api-server behind HAProxy, 2× scheduler, 2× controller-manager |

Override files compose on top of the base stack:

- **`compose.dind.all-in-one.yml`** repoints the container-runtime socket from
  Podman's `/run/podman/podman.sock` to Docker's `/var/run/docker.sock`. Apply
  it on Docker-only hosts for the all-in-one stack, which still runs the
  kubelet's runtime client against a host runtime socket:
  ```bash
  docker compose -f compose.all-in-one.yml -f compose.dind.all-in-one.yml up -d
  ```
  The multi-container stacks (`compose.yml` / `compose.sqlite.yml` /
  `compose.redis.yml`) and the **node-conformance** stack
  (`compose.node-conformance.yml`) no longer need a socket override — they run
  pods via the bundled `containerd` service over the shared `containerd-run`
  volume (`CONTAINER_RUNTIME_ENDPOINT=unix:///run/containerd/containerd.sock`),
  so they
  work unchanged on Docker or Podman. (The old `compose.dind.yml` override was
  removed once those stacks migrated to containerd.)
- **`compose.local-binary.*`** bind host-built release binaries into the images
  to skip the in-container cargo build.

### The all-in-one container

A whole cluster in one container, embedded SQLite, no etcd:

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
docker compose -f compose.all-in-one.yml -f compose.dind.all-in-one.yml up -d
bash scripts/bootstrap-cluster.sh
```

The container runs privileged (kube-proxy needs `CAP_NET_ADMIN`/`NET_RAW` for
iptables) and mounts the runtime socket plus the volume directory. Use
`--disable-proxy` where iptables is unavailable.

### Bare metal

**Single node, all-in-one (simplest).** One binary, one SQLite file, the
container runtime, and root (for kube-proxy's iptables):

```bash
sudo apt-get install -y docker.io iptables iproute2     # or the distro equivalent
cargo build --release -p rusternetes
npm --prefix console install && npm --prefix console run build   # optional: web console → console/dist

sudo ./target/release/rusternetes \
  --data-dir /var/lib/rusternetes/cluster.db \
  --volume-dir /var/lib/rusternetes/volumes \
  --tls --console-dir ./console/dist
# API + console on https://<host>:6443/console/
```

If the host has no iptables (or you don't want NodePort/LoadBalancer routing),
add `--disable-proxy`.

**Multi-node, per-component.** Run the control plane on one host and a
kubelet + kube-proxy on each worker, all pointed at the same storage endpoint
(etcd or a Rhino server):

```bash
# control-plane host
./target/release/api-server --bind-address 0.0.0.0:6443 --etcd-servers http://STORE:2379 --tls --tls-cert-file ... --tls-key-file ...
./target/release/scheduler           --etcd-servers http://STORE:2379
./target/release/controller-manager  --etcd-servers http://STORE:2379
./target/release/rusternetes-dns     --etcd-servers http://STORE:2379

# each worker (kube-proxy needs root or CAP_NET_ADMIN)
export CONTAINER_RUNTIME_ENDPOINT=unix:///run/containerd/containerd.sock
./target/release/kubelet --node-name worker-1 --etcd-servers http://STORE:2379
sudo ./target/release/kube-proxy --node-name worker-1 --etcd-servers http://STORE:2379
```

Node requirements:

- **CRI runtime socket** reachable by the kubelet
  (`CONTAINER_RUNTIME_ENDPOINT=unix:///run/containerd/containerd.sock` for containerd).
- **Kube-proxy** needs `CAP_NET_ADMIN` + `CAP_NET_RAW`, host networking, and the
  `iptables`/`iproute2` tools. Run it as root, or grant caps:
  `sudo setcap cap_net_admin,cap_net_raw=ep ./kube-proxy`.
- **TLS certificates** whose SANs cover every address clients and pods use to
  reach the API server (see below). `KUBERNETES_SERVICE_HOST_OVERRIDE` sets the
  API address injected into pods when it isn't the in-cluster `10.96.0.1`.

The installer scripts under `scripts/installers/` (e.g. a Fedora systemd
installer) are a useful reference for wiring the components as services.

### High availability

`compose.ha.yml` demonstrates the HA topology: a 3-node etcd cluster, three
api-servers behind HAProxy (`:6443`, stats on `:8404`), and two each of the
scheduler and controller-manager with **leader election** enabled
(`--enable-leader-election`). See [docs/HIGH_AVAILABILITY.md](docs/HIGH_AVAILABILITY.md).

### Bootstrapping and certificates

Two scripts handle one-time setup; both write under `.rusternetes/`:

- **`scripts/generate-certs.sh`** — generates the API-server TLS cert/key
  (EC P-256), the ServiceAccount signing keypair (`sa.key`/`sa.pub`), and the CA
  copy pods mount. SANs automatically include `localhost`, `127.0.0.1`,
  `10.96.0.1`, the `kubernetes.default.svc*` names, and the container-network
  IPs. Delete the cert files and re-run to regenerate.
- **`scripts/bootstrap-cluster.sh`** — after the stack is up, creates the
  `default` and `kube-system` namespaces, the `kubernetes` (`10.96.0.1`) and
  `kube-dns` (`10.96.0.10`) Services, default ServiceAccounts + tokens, and
  wires DNS to the `rusternetes-dns` container (or a CoreDNS Pod). Honors
  `KUBELET_VOLUMES_PATH`, `CONTAINER_RUNTIME`, and
  `KUBERNETES_SERVICE_HOST_OVERRIDE`.

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
bash scripts/generate-certs.sh          # first run only
bash scripts/bootstrap-cluster.sh       # after `compose up`
```

## Web console

The API server embeds a web console (cluster topology, live metrics, pod log
streaming, full resource management) served at `/console/` when started with
`--console-dir`. The component images bundle it automatically. See
[docs/CONSOLE_USER_GUIDE.md](docs/CONSOLE_USER_GUIDE.md).

## Conformance

Rusternetes is continuously tested against the **official Kubernetes v1.35
conformance suite** (via Sonobuoy/Hydrophone), plus a kubelet-scoped
`[NodeConformance]` job. Current pass rates and the per-test breakdown are
tracked in the repository's GitHub Projects (Node Conformance and Conformance).

```bash
bash scripts/run-conformance.sh        # full conformance lifecycle
bash scripts/conformance-progress.sh   # monitor pass/fail progress
```

## Roadmap

The guiding goal is a lightweight, conformant distribution for small hardware —
*"k3s without melting your laptop"* — with the four-node Raspberry-Pi cluster as
the destination. Themes in flight:

- **Footprint** — measure and shrink idle RAM, binary size, and time-to-cluster;
  the embedded SQLite all-in-one is the lever.
- **`arm64` + static builds** — `musl`/`mimalloc` static binaries and ARM
  cross-compilation for small-board nodes.
- **One-command install** — pre-built release binaries and a `curl | sh`-style
  installer for parity with k3s.
- **Networking** — the embedded `netstack` data plane
  (`--pod-network-mode netstack-active`) as an alternative to the Docker-bridge
  CNI path.
- **Conformance** — closing the remaining gaps toward 100% on the SQLite/Rhino
  backend.

## Development

```bash
cargo build                                   # debug build
cargo test                                    # all workspace tests
cargo test -p rusternetes-api-server          # one crate
cargo clippy --all-targets --all-features -- -D warnings
make pre-commit                               # fmt + clippy + test
```

Unit tests use the in-memory storage backend; `#[tokio::test]` for async. See
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) and
[docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).

## Further documentation

- [README.md](README.md) — project intro and screenshots
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — internal design
- [docs/QUICKSTART.md](docs/QUICKSTART.md) — first-cluster walkthrough and deployment
- [docs/storage/STORAGE_BACKENDS.md](docs/storage/STORAGE_BACKENDS.md) — storage modes
- [docs/AUTHENTICATION.md](docs/AUTHENTICATION.md), [docs/HIGH_AVAILABILITY.md](docs/HIGH_AVAILABILITY.md), [docs/CONSOLE_USER_GUIDE.md](docs/CONSOLE_USER_GUIDE.md)

## License

Apache-2.0
