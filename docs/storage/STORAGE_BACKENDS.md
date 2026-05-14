# Storage Backends


> **Tip:** Storage configuration, classes, and PVCs are manageable through the [web console](CONSOLE_USER_GUIDE.md) Storage page.
Rusternetes supports three deployment modes with different storage backends.
The same component binaries work in all modes — the only difference is
configuration flags and which compose file you use.

| Mode | Storage | Compose file | External deps | Use case |
|------|---------|-------------|---------------|----------|
| Normal | etcd | `compose.yml` | etcd cluster | Production, HA, multi-node |
| SQLite (normal) | rhino (gRPC) | `compose.sqlite.yml` | rhino container | Multi-container without etcd |
| Redis (normal) | rhino (gRPC) | `compose.redis.yml` | rhino + Redis | Multi-container with Redis |
| SQLite (embedded) | rhino (in-process) | `compose.all-in-one.yml` | None | All-in-one single binary |
| Redis (embedded) | rhino (in-process) | `compose.all-in-one-redis.yml` | Redis | All-in-one + Redis |

---

## Deployment Modes

### 1. Normal mode with etcd (default)

The standard deployment. Each component runs in its own container. etcd
provides distributed consensus-based storage.

```bash
podman compose build
podman compose up -d
bash scripts/bootstrap-cluster.sh
```

### 2. Normal mode with SQLite via rhino

Same multi-container architecture, but rhino replaces etcd. Rhino is an
etcd-compatible gRPC server backed by SQLite — it speaks the exact same
etcd v3 API, so components use their existing `--etcd-servers` flag pointed
at `http://rhino:2379`. **No recompilation or feature flags needed.**

```bash
podman compose -f compose.sqlite.yml build
podman compose -f compose.sqlite.yml up -d
bash scripts/bootstrap-cluster.sh
```

This mode uses `Dockerfile.rhino` to build the rhino-server container and
requires the [rhino](https://github.com/calfonso/rhino) repo adjacent to
this one:

```
dev/
  rusternetes/   (this repo)
  rhino/         (https://github.com/calfonso/rhino)
```

### 3. Normal mode with Redis via rhino

Same as the SQLite mode, but rhino uses Redis as its backing store instead
of SQLite. Redis provides in-memory performance with optional persistence.

```bash
podman compose -f compose.redis.yml build
podman compose -f compose.redis.yml up -d
bash scripts/bootstrap-cluster.sh
```

### 4. All-in-one binary (embedded SQLite)

All components run as concurrent tokio tasks in a single process with
rhino's `SqliteBackend` embedded directly — no gRPC, no network, no
containers, pure in-process Rust calls. The `rusternetes` binary in
`crates/rusternetes/` orchestrates everything.

```bash
cargo build -p rusternetes
./target/debug/rusternetes
```

By default it uses SQLite at `./data/rusternetes.db`. No etcd, no Docker
Compose, no containers — just run the binary.

You can also point the all-in-one at etcd if you prefer:

```bash
./target/debug/rusternetes --storage-backend etcd --etcd-servers http://etcd:2379
```

### 5. All-in-one binary with Redis

Same single-binary approach but with Redis for storage. Rhino's
`RedisBackend` is embedded directly — no rhino gRPC server needed.
Requires a Redis server.

```bash
# Build with redis feature
cargo build -p rusternetes --features redis --release

# Run with a local Redis
./target/release/rusternetes --storage-backend redis --redis-url redis://localhost:6379

# Or use the compose file (builds and runs everything)
podman compose -f compose.all-in-one-redis.yml build
podman compose -f compose.all-in-one-redis.yml up -d
bash scripts/bootstrap-cluster.sh
```

---

## Architecture

All storage access flows through the `Storage` trait defined in `crates/storage/src/lib.rs`.

**Normal mode (etcd or rhino gRPC):**

```
    Component  --etcd-servers-->  EtcdStorage  --gRPC-->  etcd (or rhino)
```

Components use the `etcd-client` crate to talk to either real etcd or rhino
over gRPC. From the component's perspective, there is no difference.

**Embedded mode (in-process SQLite or Redis):**

```
    Component  --storage-backend=sqlite-->  RhinoStorage<SqliteBackend>  --direct-->  SQLite file
    Component  --storage-backend=redis-->   RhinoStorage<RedisBackend>   --direct-->  Redis server
```

The `StorageBackend` enum dispatches to the concrete implementation:

```
    +-----------------+     +------------------+     +------------------+
    | StorageBackend  |     | StorageBackend   |     | StorageBackend   |
    |   ::Etcd        |     |   ::Sqlite       |     |   ::Redis        |
    |                 |     |                  |     |                  |
    | EtcdStorage     |     | RhinoStorage     |     | RhinoStorage     |
    |   etcd-client   |     |   SqliteBackend  |     |   RedisBackend   |
    |   gRPC          |     |   in-process     |     |   in-process     |
    +-----------------+     +------------------+     +------------------+
           |                        |                        |
           v                        v                        v
     etcd or rhino         SQLite file on disk          Redis server
     (network, :2379)      (./data/cluster.db)          (redis://host:6379)
```

---

## Feature Flags

SQLite and Redis support are behind Cargo features to keep the default build
lean. When disabled, the rhino dependency and related code are excluded.

```bash
# Build with SQLite support (default for all-in-one)
cargo build --features sqlite

# Build with Redis support
cargo build --features redis

# Build with both
cargo build --features sqlite,redis

# Build without either (etcd only)
cargo build
```

The features propagate through the crate graph:

```
rusternetes-api-server/sqlite  -> rusternetes-storage/sqlite  -> dep:rhino
rusternetes-api-server/redis   -> rusternetes-storage/redis   -> dep:rhino
```

Every binary crate (api-server, scheduler, controller-manager, kubelet,
kube-proxy) defines both `sqlite` and `redis` features that forward to
`rusternetes-storage`.

The `rusternetes` all-in-one crate enables the `sqlite` feature **by
default** — no extra flags needed when building it. For Redis, pass
`--features redis`.

---

## Usage

### etcd (default)

No changes from the existing deployment model. etcd is the default backend
when `--storage-backend` is omitted.

```bash
# Docker Compose
podman compose up -d

# Or run binaries directly
api-server --etcd-servers http://etcd:2379
scheduler --etcd-servers http://etcd:2379
controller-manager --etcd-servers http://etcd:2379
kubelet --node-name node-1 --etcd-servers http://etcd:2379
kube-proxy --node-name node-1 --etcd-servers http://etcd:2379
```

### SQLite via rhino (normal multi-container)

Use `compose.sqlite.yml` to swap etcd for rhino. Components point
their `--etcd-servers` flag at rhino instead of etcd. Same binaries, no
recompilation.

```bash
podman compose -f compose.sqlite.yml build
podman compose -f compose.sqlite.yml up -d
bash scripts/bootstrap-cluster.sh
```

Or run rhino and the binaries directly:

```bash
# Start rhino (from the rhino repo)
rhino-server --listen-address 0.0.0.0:2379 --endpoint ./cluster.db

# Point components at rhino
api-server --etcd-servers http://localhost:2379
scheduler --etcd-servers http://localhost:2379
# ... etc
```

### Redis via rhino (normal multi-container)

Use `compose.redis.yml` to swap etcd for rhino+Redis. Rhino translates the
etcd gRPC API to Redis operations. Components use their existing
`--etcd-servers` flag pointed at rhino.

```bash
podman compose -f compose.redis.yml build
podman compose -f compose.redis.yml up -d
bash scripts/bootstrap-cluster.sh
```

Or run rhino with Redis directly:

```bash
# Start Redis
redis-server

# Start rhino pointed at Redis (from the rhino repo)
rhino-server --listen-address 0.0.0.0:2379 --endpoint redis://localhost:6379

# Point components at rhino
api-server --etcd-servers http://localhost:2379
scheduler --etcd-servers http://localhost:2379
# ... etc
```

### All-in-one binary

The `rusternetes` binary spawns all components as tokio tasks in one process.
The `sqlite` feature is enabled by default for this crate.

```bash
# Build and run with defaults (SQLite, localhost:6443, node-1)
cargo build -p rusternetes
./target/debug/rusternetes

# With Redis instead
cargo build -p rusternetes --features redis
./target/debug/rusternetes --storage-backend redis --redis-url redis://localhost:6379

# Custom configuration
./target/debug/rusternetes \
    --data-dir /var/lib/rusternetes.db \
    --node-name my-node \
    --bind-address 0.0.0.0:6443 \
    --tls \
    --tls-san localhost,127.0.0.1,my-node

# Disable kube-proxy (e.g. when iptables is not available)
./target/debug/rusternetes --disable-proxy
```

The database file is created automatically if it does not exist.

---

## CLI Flags

### Individual binaries

Every component binary accepts these storage flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--storage-backend` | `etcd` | `etcd`, `sqlite`, or `redis` |
| `--data-dir` | `./data/rusternetes.db` | SQLite database file path (sqlite backend) |
| `--etcd-servers` | `http://localhost:2379` | etcd endpoints, comma-separated (etcd backend) |
| `--redis-url` | `redis://localhost:6379` | Redis connection URL (redis backend) |

### All-in-one binary (`rusternetes`)

| Flag | Default | Description |
|------|---------|-------------|
| `--storage-backend` | `sqlite` | `sqlite`, `etcd`, or `redis` |
| `--data-dir` | `./data/rusternetes.db` | SQLite database file path |
| `--etcd-servers` | `http://localhost:2379` | etcd endpoints (when `--storage-backend=etcd`) |
| `--redis-url` | `redis://localhost:6379` | Redis URL (when `--storage-backend=redis`) |
| `--kubernetes-service-host` | `127.0.0.1` | API server address injected into pods (or `KUBERNETES_SERVICE_HOST_OVERRIDE` env var) |
| `--bind-address` | `0.0.0.0:6443` | API server listen address |
| `--node-name` | `node-1` | Node name for the embedded kubelet |
| `--volume-dir` | `./data/volumes` | Pod volume directory |
| `--cluster-dns` | `10.96.0.10` | Cluster DNS IP |
| `--network` | `rusternetes-network` | Container network name |
| `--tls` | `false` | Enable TLS with self-signed certs |
| `--tls-cert-file` | — | TLS certificate file (PEM) |
| `--tls-key-file` | — | TLS private key file (PEM) |
| `--tls-san` | `localhost,127.0.0.1` | TLS Subject Alternative Names |
| `--sync-interval` | `5` | Controller sync interval (seconds) |
| `--scheduler-interval` | `2` | Scheduler interval (seconds) |
| `--kubelet-sync-interval` | `3` | Kubelet sync interval (seconds) |
| `--proxy-sync-interval` | `1` | Kube-proxy sync interval (seconds) |
| `--skip-auth` | `true` | Skip authentication (insecure) |
| `--disable-proxy` | `false` | Disable kube-proxy |
| `--log-level` | `info` | Log level (trace/debug/info/warn/error) |

---

## How It Works

### Key mapping

Both backends use the same key schema: `/registry/{resource_type}/{namespace}/{name}`.
Resource versions map to monotonically increasing revision numbers — etcd's
`mod_revision` for the etcd backend, SQLite row IDs for the sqlite backend.
This is important: resource versions are **backend-specific integers**, not
portable across backends.

### Watch support

Both backends support the full watch API including `watch_from_revision`.

- **etcd**: Uses etcd's native gRPC watch streams with `prev_kv` for delete
  events.
- **SQLite**: Rhino's poll loop detects new rows in the `kine` table and
  broadcasts events via per-subscriber mpsc channels. Historical replay
  is supported by querying rows with `id > revision`.
- **Redis**: Same poll loop architecture as SQLite, but backed by Redis
  sorted sets and Lua scripts for atomic operations. Events are broadcast
  via the same subscriber pattern.

### Optimistic concurrency

Both backends enforce the Kubernetes resource version contract. Updates with a
stale `resourceVersion` are rejected with a 409 Conflict. The mechanism
differs:

- **etcd**: Compare-and-swap transactions checking `mod_revision`.
- **SQLite**: The `kine` table's `(name, prev_revision)` unique index
  prevents concurrent updates to the same key at the same revision.
- **Redis**: Atomic Lua scripts ensure create/update/delete operations
  are serialized per key.

### Compaction

- **etcd**: Managed by etcd's built-in compaction.
- **SQLite**: Rhino runs a background compaction loop (default every 300s)
  that removes superseded revisions, keeping at least the most recent 1000.
  After compaction, a `PRAGMA wal_checkpoint(FULL)` reclaims disk space.
- **Redis**: Same compaction loop, removing old revision entries from
  sorted sets and hash keys.

---

## Crate Structure

```
crates/
  storage/
    src/
      lib.rs          # Storage trait, StorageConfig, StorageBackend enum
      etcd.rs         # EtcdStorage — etcd-client gRPC implementation
      rhino.rs        # RhinoStorage<B> — generic over rhino::Backend (sqlite/redis features)
      memory.rs       # MemoryStorage — in-memory for unit tests
      concurrency.rs  # resourceVersion <-> mod_revision conversion
    Cargo.toml        # rhino = { optional = true }, [features] sqlite/redis = ["dep:rhino"]

  rusternetes/        # All-in-one meta-crate
    src/main.rs       # Spawns all components as tokio tasks
    Cargo.toml        # Depends on all component crates, sqlite enabled by default

  api-server/
    src/lib.rs        # pub async fn run(storage, config) + all modules
    src/main.rs       # CLI wrapper — parses args, calls run()

  scheduler/
    src/lib.rs        # pub async fn run(storage, config)
    src/main.rs       # CLI wrapper

  controller-manager/
    src/lib.rs        # pub async fn run(storage, config) — spawns 28 controllers
    src/main.rs       # CLI wrapper

  kubelet/
    src/lib.rs        # pub async fn run(storage, config)
    src/main.rs       # CLI wrapper

  kube-proxy/
    src/lib.rs        # pub async fn run(storage, config)
    src/main.rs       # CLI wrapper
```

### Library `run()` pattern

Each component exposes a `pub async fn run()` in its `lib.rs` that takes
`Arc<StorageBackend>` and a component-specific config struct. The standalone
binary's `main()` parses CLI args and calls `run()`. The all-in-one binary
spawns each `run()` as a tokio task.

```rust
// Example: crates/scheduler/src/lib.rs
pub struct SchedulerConfig { pub interval: u64 }

pub async fn run(storage: Arc<StorageBackend>, config: SchedulerConfig) -> Result<()> {
    let scheduler = scheduler::Scheduler::new(storage, config.interval);
    scheduler.run().await
}
```

### StorageConfig

```rust
pub enum StorageConfig {
    Etcd { endpoints: Vec<String> },
    #[cfg(feature = "sqlite")]
    Sqlite { path: String },
    #[cfg(feature = "redis")]
    Redis { url: String },
}
```

### StorageBackend

```rust
pub enum StorageBackend {
    Etcd(EtcdStorage),
    #[cfg(feature = "sqlite")]
    Sqlite(RhinoStorage),          // RhinoStorage<SqliteBackend>
    #[cfg(feature = "redis")]
    Redis(RhinoRedisStorage),      // RhinoStorage<RedisBackend>
}

impl Storage for StorageBackend { /* dispatches to inner */ }
impl AuthzStorage for StorageBackend { /* dispatches to inner */ }
```

Components use `Arc<StorageBackend>` which implements both `Storage` and
`AuthzStorage`.

---

## Leader Election

Leader election (used by the scheduler and controller-manager for HA) uses etcd
leases directly and is **independent of the storage backend**. When running in
all-in-one mode with SQLite, leader election is typically disabled
(`--enable-leader-election` is not set) since there is only one instance.

If leader election is needed, the `--etcd-servers` flag is still parsed to
connect the `LeaderElector` to an etcd cluster, even when the storage backend
is SQLite.

---

## Rhino gRPC Mode (compose.sqlite.yml / compose.redis.yml)

The simplest way to use SQLite or Redis in a normal multi-container deployment.
Rhino replaces etcd as a drop-in: same gRPC API, backed by the chosen database.

**Files:**
- `Dockerfile.rhino` — builds the rhino-server binary from the adjacent repo
- `compose.sqlite.yml` — full cluster with rhino backed by SQLite
- `compose.redis.yml` — full cluster with rhino backed by Redis

**How it works:** Components use their existing `--etcd-servers` flag pointed
at `http://rhino:2379`. The `etcd-client` crate in `EtcdStorage` connects to
rhino's tonic gRPC server, which translates operations to the chosen backend.
Watch streams work via rhino's poll loop (1-second intervals).

**Advantages over the embedded approach:**
- No feature flags or recompilation needed — same binaries as etcd mode
- Watches work correctly across process boundaries via gRPC streaming
- Can inspect cluster state with `sqlite3` or `redis-cli`

**Trade-off:** Extra container(s) (rhino + optionally Redis) vs. fewer for embedded.

---

## Rhino Library Dependency (embedded mode)

Rhino is included as a local path dependency in the storage crate. This
requires the following directory layout:

```
dev/
  rusternetes/    # this repo
  rhino/          # https://github.com/calfonso/rhino
```

Rhino provides four database backends (SQLite, Redis, PostgreSQL, MySQL) behind
its `Backend` trait. Rusternetes uses `SqliteBackend` and `RedisBackend`.
`RhinoStorage<B>` is generic over the `Backend` trait, so adding PostgreSQL
or MySQL would only require a new constructor and `StorageConfig`/`StorageBackend`
variant.

### Key rhino details

- **Schema**: Log-structured `kine` table with monotonic row IDs as revisions
- **WAL mode**: SQLite runs in WAL journal mode for concurrent reads during writes
- **Busy timeout**: 30 seconds (handles brief lock contention gracefully)
- **Connection pool**: 5 connections via sqlx
- **Gap filling**: Revision gaps (from rolled-back transactions) are detected
  and filled with placeholder records to maintain sequential ordering

---

## Limitations

- **No cross-backend migration**: Data cannot be moved between backends
  without a manual export/import process. Resource versions are not portable.
- **Single-writer for SQLite**: SQLite supports concurrent reads but serializes
  writes. This is fine for single-node and small multi-container deployments
  but would bottleneck under heavy multi-node write load.
- **Redis persistence**: Redis is in-memory by default. Enable RDB or AOF
  persistence in your Redis config to survive restarts. Without persistence,
  restarting Redis loses all cluster state.
- **Leader election requires etcd**: Even with embedded SQLite storage, leader
  election still needs an etcd cluster. This is a non-issue for the primary
  use case (single-node, no HA). In rhino gRPC mode, leader election could
  use rhino's etcd-compatible lease API.
- **Embedded watch latency**: When multiple processes share a SQLite file
  directly (embedded mode across containers), watch notifications rely on
  rhino's 1-second poll interval rather than instant gRPC streaming. Use
  the rhino gRPC mode (`compose.sqlite.yml`) for multi-container
  deployments to get proper streaming watches.

---

## Related

- [Rhino](https://github.com/calfonso/rhino) — the SQLite/Redis/SQL-backed etcd shim
- [kine](https://github.com/k3s-io/kine) — the Go project rhino is inspired by
- [CSI Integration](csi-integration.md) — volume plugin storage (separate concern)
