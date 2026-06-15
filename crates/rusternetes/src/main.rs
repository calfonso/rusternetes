//! Rusternetes — all-in-one Kubernetes in a single binary.
//!
//! Runs the API server, scheduler, controller manager, kubelet, kube-proxy,
//! and cluster DNS as concurrent tokio tasks sharing a single storage backend.
//!
//! Usage:
//!   rusternetes                                   # SQLite at ./data/rusternetes.db
//!   rusternetes --data-dir /var/lib/rusternetes.db # custom path
//!   rusternetes --etcd-servers http://etcd:2379    # use etcd instead

use anyhow::Result;
use clap::Parser;
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::sync::Arc;
use tracing::{error, info};

// Heap-profiling allocator (off by default; `--features dhat-heap`). Attributes
// the all-in-one's idle RAM by backtrace for #1138. Dumps `dhat-heap.json` when
// the profiler guard in `main` drops on a clean (SIGINT) shutdown.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Parser, Debug)]
#[command(name = "rusternetes")]
#[command(about = "Rusternetes — all-in-one Kubernetes in a single binary")]
#[command(version)]
struct Args {
    /// Storage backend: "sqlite", "etcd", or "redis"
    #[arg(long, default_value = "sqlite")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Enable the in-process watch event bus (all-in-one fast path, #1039).
    /// On by default; pass `--in-process-bus false` to use native backend
    /// watches (e.g. for A/B benchmarking).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    in_process_bus: bool,

    /// Etcd endpoints, comma-separated (only used when --storage-backend=etcd)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Redis URL (only used when --storage-backend=redis)
    #[arg(long, default_value = "redis://localhost:6379")]
    redis_url: String,

    /// API server bind address
    #[arg(long, default_value = "0.0.0.0:6443")]
    bind_address: String,

    /// Node name for the embedded kubelet
    #[arg(long, default_value = "node-1")]
    node_name: String,

    /// Volume directory for pod volumes
    #[arg(long, default_value = "./data/volumes")]
    volume_dir: String,

    /// Cluster DNS IP
    #[arg(long, default_value = "10.96.0.10")]
    cluster_dns: String,

    /// Container network name
    #[arg(long, default_value = "rusternetes-network")]
    network: String,

    /// Enable TLS with self-signed certificates
    #[arg(long)]
    tls: bool,

    /// TLS certificate file
    #[arg(long)]
    tls_cert_file: Option<String>,

    /// TLS private key file
    #[arg(long)]
    tls_key_file: Option<String>,

    /// TLS Subject Alternative Names (comma-separated)
    #[arg(long, default_value = "localhost,127.0.0.1")]
    tls_san: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Controller sync interval in seconds
    #[arg(long, default_value = "5")]
    sync_interval: u64,

    /// Scheduler interval in seconds
    #[arg(long, default_value = "2")]
    scheduler_interval: u64,

    /// Kubelet sync interval in seconds
    #[arg(long, default_value = "3")]
    kubelet_sync_interval: u64,

    /// Kube-proxy sync interval in seconds
    #[arg(long, default_value = "1")]
    proxy_sync_interval: u64,

    /// ClusterIP CIDR — must match the apiserver's
    /// `--service-cluster-ip-range`. Used by kube-proxy to scope its
    /// POSTROUTING MASQUERADE rule.
    #[arg(long, default_value = rusternetes_kube_proxy::iptables::DEFAULT_CLUSTER_CIDR)]
    cluster_cidr: String,

    /// NodePort range in iptables `start:end` form — must match the
    /// apiserver's `--service-node-port-range`. Hyphen also accepted.
    #[arg(long, default_value = rusternetes_kube_proxy::iptables::DEFAULT_NODEPORT_RANGE)]
    node_port_range: String,

    /// Skip authentication (insecure, for development)
    #[arg(long, default_value = "true")]
    skip_auth: bool,

    /// Disable kube-proxy (useful when iptables is not available)
    #[arg(long)]
    disable_proxy: bool,

    /// Pod networking mode. Three values:
    ///
    ///   - `cni` (default): legacy CNI / Docker-bridge path that's
    ///     been carrying production conformance for months.
    ///   - `netstack-shadow` (or its alias `netstack`): every pod is
    ///     also registered with the embedded netstack (IP allocated,
    ///     TAP opened, runtime notified) but pod traffic still rides
    ///     the legacy path. Used to validate the netstack data plane
    ///     in staging without breaking pod networking.
    ///   - `netstack-active`: pod traffic actually routes through the
    ///     embedded netstack. Kubelet calls
    ///     `netstack.start_pod_in_netns` instead of the CNI plugin /
    ///     Docker bridge. Requires CAP_NET_ADMIN, a working netstack
    ///     runtime, and the Service-watcher populating VIPs.
    #[arg(long, default_value = "cni",
          value_parser = ["cni", "netstack", "netstack-shadow", "netstack-active"])]
    pod_network_mode: String,

    /// Pod CIDR for the embedded netstack's IP allocator (only used
    /// when `--pod-network-mode=netstack`). Must not overlap with the
    /// host network or `--cluster-cidr`. `10.244.0.0/16` matches the
    /// Flannel default and gives ~65k pod IPs per node.
    #[arg(long, default_value = "10.244.0.0/16")]
    netstack_pod_cidr: String,

    /// Service CIDR the embedded netstack treats as on-link. Should
    /// match `--cluster-cidr` (kube-proxy's POSTROUTING MASQUERADE
    /// scope). The first usable address (typically `10.96.0.1`) is
    /// claimed as the gateway IP for smoltcp's routing decisions.
    #[arg(long, default_value = "10.96.0.0/12")]
    netstack_service_cidr: String,

    /// Disable the in-process DNS server (fall back to the standalone
    /// rusternetes-dns container, or to the CoreDNS Pod from
    /// bootstrap-cluster.yaml when USE_RUSTERNETES_DNS=0).
    #[arg(long)]
    disable_dns: bool,

    /// Bind address for the in-process DNS server (UDP+TCP). Pods reach
    /// it via the kube-dns Service ClusterIP; kube-proxy DNATs to this
    /// address. Default binds all interfaces inside the container.
    #[arg(long, default_value = "0.0.0.0:53")]
    dns_bind: String,

    /// Path to the console SPA build directory (enables web console at /console/)
    #[arg(long)]
    console_dir: Option<String>,

    /// Kubernetes service host override for pods (e.g. "api-server" in containerized deployments).
    /// Falls back to KUBERNETES_SERVICE_HOST_OVERRIDE env var if not set.
    #[arg(long)]
    kubernetes_service_host: Option<String>,

    /// Client CA certificate file for mTLS client certificate authentication
    #[arg(long)]
    client_ca_file: Option<String>,
}

fn main() -> Result<()> {
    // Hold the dhat profiler for the whole process; it writes dhat-heap.json
    // when dropped (after block_on returns on a SIGINT shutdown).
    #[cfg(feature = "dhat-heap")]
    let _dhat = dhat::Profiler::new_heap();

    // The all-in-one packs the api-server, scheduler, controller-manager,
    // kubelet, and kube-proxy into one tokio runtime. The api-server's request
    // path (routing → admission → mutating/validating webhooks → watch fan-out)
    // produces deep async call chains, and creating pods overflows tokio's
    // default 2 MiB worker-thread stack (`fatal runtime error: stack overflow`).
    // Multi-container compose runs each component as its own process and isn't
    // affected; only the single-runtime all-in-one is. Give workers an 8 MiB
    // stack — virtual reservation only (Linux commits stack pages lazily), so
    // no idle-RAM cost. See #1135.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    let args = Args::parse();

    rusternetes_common::tracing::init_basic_tracing("rusternetes", &args.log_level)?;

    rusternetes_common::dump::install_panic_hook("rusternetes");

    info!("Starting Rusternetes (all-in-one)");

    // Initialize storage — all components share one instance
    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Storage: SQLite at {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir,
            }
        }
        "etcd" => {
            let endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Storage: etcd at {:?}", endpoints);
            StorageConfig::Etcd { endpoints }
        }
        #[cfg(feature = "redis")]
        "redis" => {
            info!("Storage: Redis at {}", args.redis_url);
            StorageConfig::Redis {
                url: args.redis_url,
            }
        }
        other => {
            anyhow::bail!(
                "Unknown storage backend: {}. Use 'sqlite', 'etcd', or 'redis'.",
                other
            );
        }
    };
    let mut storage = StorageBackend::new(storage_config).await?;
    if args.in_process_bus {
        storage.enable_event_bus();
        info!("In-process watch event bus: ENABLED (internal consumers use the fast path)");
    } else {
        info!("In-process watch event bus: disabled (native backend watches)");
    }
    let storage = Arc::new(storage);

    info!("Storage initialized, starting components...");

    // --- API Server ---
    let api_storage = storage.clone();
    let api_config = rusternetes_api_server::ApiServerConfig {
        bind_address: args.bind_address.clone(),
        tls: args.tls,
        tls_cert_file: args.tls_cert_file.clone(),
        tls_key_file: args.tls_key_file.clone(),
        tls_self_signed: args.tls,
        tls_san: args.tls_san.clone(),
        skip_auth: args.skip_auth,
        console_dir: args.console_dir.map(std::path::PathBuf::from),
        client_ca_file: args.client_ca_file.clone(),
        ..Default::default()
    };
    let api_handle = tokio::spawn(async move {
        if let Err(e) = rusternetes_api_server::run(api_storage, api_config).await {
            error!("API server error: {}", e);
        }
    });

    // Give API server a moment to bind before starting clients
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Embedded clients (scheduler, controller-manager, DNS) reach the
    // api-server over loopback, matching the trust boundary the compose path
    // enforces: only the api-server touches storage (#1128). The self-signed
    // loopback cert is skipped when TLS is on; skip_auth is the all-in-one
    // default, so no bearer token is needed (real authn tracked in #1129). The
    // port is derived from the api-server bind address; fall back to 6443.
    let api_port = args
        .bind_address
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6443);
    let loopback_scheme = if args.tls { "https" } else { "http" };
    let loopback_url = format!("{loopback_scheme}://127.0.0.1:{api_port}");

    // --- Scheduler ---
    let sched_config = rusternetes_scheduler::SchedulerConfig {
        interval: args.scheduler_interval,
    };
    let sched_client = Arc::new(rusternetes_client::http::ApiClient::new(
        &loopback_url,
        args.tls,
        None,
    )?);
    info!("Scheduler reading cluster state from embedded api-server at {loopback_url}");
    tokio::spawn(async move {
        if let Err(e) = rusternetes_scheduler::run_with_api(sched_client, sched_config).await {
            error!("Scheduler error: {}", e);
        }
    });

    // --- Controller Manager ---
    // The HPA metrics client talks to the in-process api-server over loopback
    // using client-cert mTLS (only when cert+key are configured).
    let metrics_config = args
        .tls_cert_file
        .as_ref()
        .zip(args.tls_key_file.as_ref())
        .map(|(cert, key)| {
            rusternetes_controller_manager::controllers::hpa_metrics_client::HttpMetricsConfig {
                api_server_url: format!("https://127.0.0.1:{api_port}"),
                ca_cert_path: args.client_ca_file.clone().unwrap_or_else(|| cert.clone()),
                client_cert_path: cert.clone(),
                client_key_path: key.clone(),
                insecure_skip_tls_verify: true,
            }
        });
    // CA cert PEM for the namespace controller's kube-root-ca.crt publisher.
    // Read from the configured CA file (falling back to the serving cert).
    let cm_ca_pem = args
        .client_ca_file
        .as_ref()
        .or(args.tls_cert_file.as_ref())
        .and_then(|p| std::fs::read_to_string(p).ok());
    let cm_config = rusternetes_controller_manager::ControllerManagerConfig {
        sync_interval: args.sync_interval,
        metrics_config,
        ca_cert_pem: cm_ca_pem,
    };
    // The controller-manager reaches cluster state through the embedded
    // api-server over the same loopback client as scheduler/DNS (#1128),
    // rather than the StorageBackend directly.
    let cm_client = Arc::new(rusternetes_client::http::ApiClient::new(
        &loopback_url,
        args.tls,
        None,
    )?);
    info!("Controller manager reading cluster state from embedded api-server at {loopback_url}");
    tokio::spawn(async move {
        if let Err(e) = rusternetes_controller_manager::run_with_api(cm_client, cm_config).await {
            error!("Controller manager error: {}", e);
        }
    });

    // --- Embedded netstack (Phase 3 shadow mode) ---
    //
    // When `--pod-network-mode=netstack`, instantiate the embedded
    // netstack and pass a handle to the kubelet. The netstack
    // runs in shadow mode — every pod is registered with it but
    // pod traffic still rides the legacy Docker/CNI path. Flip the
    // flag to validate the data plane end-to-end in staging; default
    // stays `cni` until the netns wiring lands and conformance
    // passes on the netstack path.
    let pod_network_mode = match args.pod_network_mode.as_str() {
        "cni" => rusternetes_kubelet::PodNetworkMode::Cni,
        // `netstack` is the legacy alias for `netstack-shadow`.
        "netstack" | "netstack-shadow" => rusternetes_kubelet::PodNetworkMode::NetstackShadow,
        "netstack-active" => rusternetes_kubelet::PodNetworkMode::NetstackActive,
        other => anyhow::bail!(
            "unknown --pod-network-mode {other:?}; expected cni / netstack-shadow / netstack-active"
        ),
    };
    let want_netstack = !matches!(pod_network_mode, rusternetes_kubelet::PodNetworkMode::Cni);
    let netstack_handle: Option<std::sync::Arc<dyn rusternetes_netstack::manager::NetstackHandle>> =
        if want_netstack {
            info!(
                "Pod networking: {} — pod CIDR {}, service CIDR {}",
                args.pod_network_mode, args.netstack_pod_cidr, args.netstack_service_cidr
            );
            match build_netstack(&args.netstack_pod_cidr, &args.netstack_service_cidr) {
                Ok(ns) => Some(ns),
                Err(e) => {
                    error!(
                        "Failed to start embedded netstack: {} — falling back to CNI/Docker for this run",
                        e
                    );
                    None
                }
            }
        } else {
            info!("Pod networking: cni (legacy Docker/CNI path)");
            None
        };
    // If the operator asked for an active netstack but we couldn't
    // build one, demote to CNI so pods aren't stranded with no
    // network. Logged loudly above.
    let effective_pod_network_mode = if netstack_handle.is_none() {
        rusternetes_kubelet::PodNetworkMode::Cni
    } else {
        pod_network_mode
    };

    // --- Service-VIP watcher ---
    //
    // When the embedded netstack is up, also spawn the Service-VIP
    // watcher that reconciles `Service` + `EndpointSlice` cluster
    // state into `Netstack::bind_tcp_service` / `unbind_tcp_service`
    // calls. Without this, the netstack's listener pools stay
    // empty and no Service-VIP TCP can route — even in shadow mode
    // operators wouldn't see realistic bindings.
    if let Some(ns) = netstack_handle.clone() {
        let watcher_storage = storage.clone();
        let watcher_cancel = std::sync::Arc::new(tokio::sync::Notify::new());
        // No `cancel.notify` wired up yet — the watcher exits when
        // the tokio runtime drops on process shutdown. Acceptable
        // since shutdown is process-wide anyway.
        let _ = &watcher_cancel;
        let watcher_interval = std::time::Duration::from_secs(args.sync_interval);
        info!(
            sync_interval_secs = args.sync_interval,
            "Netstack: spawning Service-VIP watcher"
        );
        tokio::spawn(async move {
            rusternetes_netstack::service_watcher::run(
                watcher_storage,
                ns,
                watcher_interval,
                watcher_cancel,
            )
            .await;
        });
    }

    // --- Kubelet ---
    let kubelet_storage = storage.clone();
    let kubelet_config = rusternetes_kubelet::KubeletConfig {
        node_name: args.node_name.clone(),
        volume_dir: args.volume_dir,
        cluster_dns: args.cluster_dns,
        cluster_domain: "cluster.local".to_string(),
        network: args.network,
        sync_interval: args.kubelet_sync_interval,
        metrics_port: 10250,
        kubernetes_service_host: args
            .kubernetes_service_host
            .clone()
            .or_else(|| std::env::var("KUBERNETES_SERVICE_HOST_OVERRIDE").ok())
            .unwrap_or_else(|| "10.96.0.1".to_string()),
        netstack: netstack_handle,
        pod_network_mode: effective_pod_network_mode,
    };
    tokio::spawn(async move {
        if let Err(e) = rusternetes_kubelet::run(kubelet_storage, kubelet_config).await {
            error!("Kubelet error: {}", e);
        }
    });

    // --- Kube-proxy ---
    if !args.disable_proxy {
        let proxy_storage = storage.clone();
        let proxy_config = rusternetes_kube_proxy::KubeProxyConfig {
            node_name: args.node_name,
            sync_interval: args.proxy_sync_interval,
            cluster_cidr: args.cluster_cidr,
            // Accept hyphen form as a convenience; iptables wants colon.
            nodeport_range: args.node_port_range.replace('-', ":"),
        };
        tokio::spawn(async move {
            if let Err(e) = rusternetes_kube_proxy::run(proxy_storage, proxy_config).await {
                error!("Kube-proxy error: {}", e);
            }
        });
    } else {
        info!("Kube-proxy disabled");
    }

    // --- DNS ---
    if !args.disable_dns {
        let dns_bind: std::net::SocketAddr = args
            .dns_bind
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid --dns-bind {:?}: {}", args.dns_bind, e))?;
        let dns_config = rusternetes_dns::DnsConfig {
            udp_bind: dns_bind,
            tcp_bind: dns_bind,
            ..rusternetes_dns::DnsConfig::default()
        };
        // DNS reaches cluster state through the embedded api-server over
        // loopback rather than the StorageBackend directly (see the loopback
        // client note above the scheduler block, #1128).
        let dns_client = Arc::new(rusternetes_client::http::ApiClient::new(
            &loopback_url,
            args.tls,
            None,
        )?);
        info!("DNS reading cluster state from embedded api-server at {loopback_url}");
        tokio::spawn(async move {
            if let Err(e) = rusternetes_dns::run_with_api(dns_client, dns_config).await {
                error!("DNS server error: {}", e);
            }
        });
    } else {
        info!("In-process DNS disabled");
    }

    info!("All components started");

    // The API server task blocks on its listener — wait for it or ctrl-c
    tokio::select! {
        result = api_handle => {
            if let Err(e) = result {
                error!("API server task panicked: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, stopping rusternetes");
        }
    }

    Ok(())
}

/// Build the embedded netstack from `--pod-network-mode=netstack`
/// flag inputs. Parses pod and service CIDRs, instantiates the
/// `Netstack<ProductionTapFactory>`, returns an `Arc<dyn NetstackHandle>`
/// the kubelet can hold without going generic over `TapFactory`.
fn build_netstack(
    pod_cidr: &str,
    service_cidr: &str,
) -> Result<std::sync::Arc<dyn rusternetes_netstack::manager::NetstackHandle>> {
    use rusternetes_netstack::manager::{Netstack, NetstackConfig, ProductionTapFactory};
    use rusternetes_netstack::wire::{IpAddress, IpCidr};

    let (pod_base, pod_prefix) = parse_cidr(pod_cidr)?;
    let (svc_base, svc_prefix) = parse_cidr(service_cidr)?;

    // The netstack claims the first usable address of the service
    // CIDR as its gateway IP (e.g., 10.96.0.1 for `10.96.0.0/12`) —
    // that's the address `kubernetes.default` resolves to. Smoltcp's
    // routing table treats the whole CIDR as on-link via this entry,
    // so packets to every Service ClusterIP route through the
    // netstack.
    let gateway = std::net::Ipv4Addr::new(
        svc_base.octets()[0],
        svc_base.octets()[1],
        svc_base.octets()[2],
        svc_base.octets()[3] | 1,
    );
    let host_ips = vec![IpCidr::new(
        IpAddress::v4(
            gateway.octets()[0],
            gateway.octets()[1],
            gateway.octets()[2],
            gateway.octets()[3],
        ),
        svc_prefix,
    )];

    let cfg = NetstackConfig {
        pod_cidr_base: pod_base,
        pod_cidr_prefix: pod_prefix,
        host_ips,
    };
    let ns = Netstack::new(cfg, ProductionTapFactory)?;
    Ok(std::sync::Arc::new(ns))
}

fn parse_cidr(cidr: &str) -> Result<(std::net::Ipv4Addr, u8)> {
    let (addr, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("CIDR {cidr:?} missing `/prefix`"))?;
    let addr: std::net::Ipv4Addr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("CIDR {cidr:?} address not IPv4: {e}"))?;
    let prefix: u8 = prefix
        .parse()
        .map_err(|e| anyhow::anyhow!("CIDR {cidr:?} prefix not a number: {e}"))?;
    Ok((addr, prefix))
}
