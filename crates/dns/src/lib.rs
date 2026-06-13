//! Rusternetes cluster DNS server.
//!
//! Standalone authoritative DNS server for the `cluster.local` zone (and
//! its `in-addr.arpa`/`ip6.arpa` reverse counterparts). Replaces the
//! CoreDNS pod that ships in `bootstrap-cluster.yaml`; runs as a separate
//! container that watches `Service`, `EndpointSlice`, and `Pod` resources
//! from the rusternetes storage backend and serves DNS responses over
//! UDP+TCP/53.
//!
//! ### Crate layout
//!
//! - [`zone`] — pure, wire-agnostic record construction and lookup logic.
//!   100% unit-tested without any tokio sockets.
//! - [`server`] — `hickory-server` integration that translates the
//!   in-memory zone into DNS responses.
//! - [`watcher`] — subscribes to list/watch events (storage backend or
//!   api-server REST) and rebuilds the zone snapshot on every change.
//!
//! ### Integration shapes
//!
//! - **Standalone binary** (`bin/rusternetes-dns`, see `src/main.rs`):
//!   one container per cluster, the original delivery vehicle.
//! - **In-process library** ([`run`] called from
//!   `crates/rusternetes/src/main.rs`): the all-in-one binary spawns the
//!   DNS server as a tokio task that shares the storage backend with
//!   apiserver / kubelet / kube-proxy. No extra container.
//!
//! Both shapes share the same [`zone`], [`watcher`], and [`server`]
//! modules — the only difference is whether [`run`] is invoked from a
//! dedicated `main` or from inside the all-in-one binary.
//!
//! ### What this does NOT do
//!
//! - DNSSEC, DNS-over-TLS, DNS-over-HTTPS.
//! - Recursion / upstream forwarding (kubelet writes upstream resolvers
//!   directly into pod `/etc/resolv.conf`, so the cluster DNS only needs
//!   to answer cluster-local zones).
//! - Prometheus metrics (TODO follow-up).
//! - Dynamic zone updates (RFC 2136) — zone is rebuilt from storage on
//!   each watch event.

pub mod server;
pub mod watcher;
pub mod zone;

use anyhow::Result;
use rusternetes_storage::StorageBackend;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

/// Configuration for the DNS server.
pub struct DnsConfig {
    /// Cluster zone suffix served authoritatively. Defaults to
    /// `cluster.local`.
    pub cluster_zone: String,
    /// UDP bind address (typically `0.0.0.0:53`).
    pub udp_bind: SocketAddr,
    /// TCP bind address (typically `0.0.0.0:53`).
    pub tcp_bind: SocketAddr,
    /// How often (seconds) to do a full resync from storage as a safety
    /// net even when no watch events have fired.
    pub resync_interval: u64,
}

impl Default for DnsConfig {
    fn default() -> Self {
        Self {
            cluster_zone: zone::CLUSTER_ZONE.to_string(),
            udp_bind: "0.0.0.0:53".parse().expect("valid bind addr"),
            tcp_bind: "0.0.0.0:53".parse().expect("valid bind addr"),
            resync_interval: 30,
        }
    }
}

/// Run the DNS server until the process is signalled, reading cluster
/// state directly from the storage backend.
///
/// Wires the storage watcher into the in-memory zone and serves DNS over
/// both UDP and TCP on the configured bind addresses.
pub async fn run(storage: Arc<StorageBackend>, config: DnsConfig) -> Result<()> {
    run_with_source(watcher::DnsSource::Storage(storage), config).await
}

/// Run the DNS server until the process is signalled, reading cluster
/// state from the api-server (list + watch over REST). This is the mode
/// the in-cluster `rusternetes-dns` Deployment uses.
pub async fn run_with_api(
    client: Arc<rusternetes_client::http::ApiClient>,
    config: DnsConfig,
) -> Result<()> {
    run_with_source(watcher::DnsSource::Api(client), config).await
}

/// Shared server wiring for both data sources.
async fn run_with_source(source: watcher::DnsSource, config: DnsConfig) -> Result<()> {
    info!(
        "Starting rusternetes-dns serving zone {} on UDP {} / TCP {}",
        config.cluster_zone, config.udp_bind, config.tcp_bind
    );

    let shared = server::SharedZone::new(zone::Zone::empty(&config.cluster_zone));

    // Start the watcher task that keeps the zone in sync.
    let watcher_zone = shared.clone();
    let watcher_cfg = watcher::WatcherConfig {
        cluster_zone: config.cluster_zone.clone(),
        resync_interval_secs: config.resync_interval,
    };
    tokio::spawn(async move {
        if let Err(e) = watcher::run_with_source(source, watcher_zone, watcher_cfg).await {
            tracing::error!("watcher exited with error: {e:?}");
        }
    });

    // Run the DNS server until shutdown.
    server::serve(shared, config.udp_bind, config.tcp_bind).await
}
