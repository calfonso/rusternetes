mod controllers;

use anyhow::Result;
use clap::Parser;
use controllers::{
    apiservice::APIServiceAvailabilityController,
    certificate_signing_request::CertificateSigningRequestController,
    crd::CRDController,
    cronjob::CronJobController,
    daemonset::DaemonSetController,
    deployment::DeploymentController,
    dynamic_provisioner::DynamicProvisionerController,
    endpoints::EndpointsController,
    endpointslice::EndpointSliceController,
    events::EventsController,
    garbage_collector::GarbageCollector,
    hpa::HorizontalPodAutoscalerController,
    hpa_metrics_client::HttpMetricsConfig,
    ingress::IngressController,
    job::JobController,
    loadbalancer::LoadBalancerController,
    namespace::NamespaceController,
    network_policy::NetworkPolicyController,
    node::NodeController,
    pod_disruption_budget::{PodDisruptionBudgetController, StalePodDisruptionController},
    priorityclass::PriorityClassController,
    pv_binder::PVBinderController,
    replicaset::ReplicaSetController,
    replicationcontroller::ReplicationControllerController,
    resource_quota::ResourceQuotaController,
    service::ServiceController,
    serviceaccount::ServiceAccountController,
    statefulset::StatefulSetController,
    storage_class::StorageClassController,
    ttl_controller::TTLController,
    volume_expansion::VolumeExpansionController,
    volume_snapshot::VolumeSnapshotController,
    vpa::VerticalPodAutoscalerController,
};
use rusternetes_common::cloud_provider::CloudProvider;
use rusternetes_common::leader_election::{LeaderElectionConfig, LeaderElector};
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "rusternetes-controller-manager")]
#[command(about = "Rusternetes Controller Manager - Runs controller loops")]
struct Args {
    /// Etcd endpoints (comma-separated)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Storage backend: "etcd" or "sqlite"
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Controller sync interval in seconds
    #[arg(long, default_value = "5")]
    sync_interval: u64,

    /// Cloud provider (aws, gcp, azure, or none)
    #[arg(long)]
    cloud_provider: Option<String>,

    /// Cluster name for cloud provider resources
    #[arg(long, default_value = "rusternetes")]
    cluster_name: String,

    /// Cloud provider region
    #[arg(long)]
    cloud_region: Option<String>,

    /// Enable leader election (for HA)
    #[arg(long)]
    enable_leader_election: bool,

    /// Leader election identity (unique for each instance)
    #[arg(long)]
    leader_election_identity: Option<String>,

    /// Leader election lock key
    #[arg(long, default_value = "/rusternetes/controller-manager/leader")]
    leader_election_lock_key: String,

    /// Leader election lease duration in seconds
    #[arg(long, default_value = "15")]
    leader_election_lease_duration: u64,

    /// API server base URL for the HPA metrics client
    #[arg(long, default_value = "https://api-server:6443")]
    api_server_url: String,

    /// Directory holding the controller-manager client cert/key + CA for the metrics client
    #[arg(long, default_value = "/etc/kubernetes/pki")]
    pki_dir: String,

    /// Skip api-server cert verification for the HPA metrics client. Defaults to
    /// true because the cluster ships a self-signed api-server cert.
    #[arg(long, default_value_t = true)]
    metrics_insecure_skip_tls_verify: bool,

    /// Path to a kubeconfig for API mode (CA + server URL). When set, the
    /// controller-manager runs as an api-server client (every controller's
    /// `Storage` calls proxy to the api-server via `ApiStorage`) with no direct
    /// storage handle — the in-cluster static-pod path. When unset, the storage
    /// backend is used (all-in-one / legacy compose path).
    #[arg(long)]
    kubeconfig: Option<String>,

    /// Skip TLS verification for the api-server data-plane connection in API
    /// mode (the kubeconfig CA normally validates the self-signed cert).
    #[arg(long)]
    insecure_skip_tls_verify: bool,
}

/// Run the controller-manager as an api-server client (in-cluster static pod).
/// Resolves the api-server CA from `--kubeconfig` and the URL from
/// `--api-server-url`, builds an [`ApiClient`], and delegates to the library's
/// `run_with_api`, which drives every controller through `ApiStorage`.
async fn run_api_mode(args: Args) -> Result<()> {
    use rusternetes_client::http::ApiClient;
    use rusternetes_client::kubeconfig::KubeConfig;

    let (ca_pem, kube_insecure) = if let Some(path) = args.kubeconfig.as_deref() {
        let cfg = KubeConfig::load_from_file(&std::path::PathBuf::from(path))?;
        let ca = cfg.get_ca_cert_pem().ok().flatten();
        let insecure = cfg.should_skip_tls_verify().unwrap_or(false);
        (ca, insecure)
    } else {
        (None, false)
    };
    let insecure = args.insecure_skip_tls_verify || kube_insecure;
    info!(
        "Controller-manager API mode: api-server={}, ca={}, insecure={}",
        args.api_server_url,
        ca_pem.is_some(),
        insecure
    );

    // The api-server runs with --skip-auth in compose, so the CA only validates
    // the server's TLS cert; no client credentials are needed (authn is #1129).
    let client = Arc::new(ApiClient::with_tls(
        &args.api_server_url,
        insecure,
        ca_pem.clone(),
        None,
    )?);
    let config = rusternetes_controller_manager::ControllerManagerConfig {
        sync_interval: args.sync_interval,
        // HPA metric fetches over the api-server in API mode are a follow-up
        // (need a client-cert/token path under the static pod).
        metrics_config: None,
        // The namespace controller can't read the CA from the api-server's
        // cert paths in its own container — hand it the kubeconfig-resolved CA
        // so it can (re)create kube-root-ca.crt in every namespace.
        ca_cert_pem: ca_pem.and_then(|b| String::from_utf8(b).ok()),
    };
    rusternetes_controller_manager::run_with_api(client, config).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    rusternetes_common::tracing::init_basic_tracing("controller-manager", &args.log_level)?;

    // In-cluster static-pod path: run as an api-server client, no storage handle.
    if args.kubeconfig.is_some() {
        info!("Starting Rusternetes Controller Manager (API mode)");
        return run_api_mode(args).await;
    }

    info!("Starting Rusternetes Controller Manager");

    // Initialize storage
    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Using SQLite storage backend at: {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir.clone(),
            }
        }
        _ => {
            let etcd_endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Connecting to etcd: {:?}", etcd_endpoints);
            StorageConfig::Etcd {
                endpoints: etcd_endpoints,
            }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    // Initialize cloud provider if configured
    #[cfg(feature = "cloud-providers")]
    let cloud_provider: Option<Arc<dyn CloudProvider>> = {
        if let Some(provider_str) = &args.cloud_provider {
            use rusternetes_common::cloud_provider::CloudProviderType;

            let provider_type = CloudProviderType::from_str(provider_str)
                .ok_or_else(|| anyhow::anyhow!("Invalid cloud provider: {}", provider_str))?;

            if provider_type != CloudProviderType::None {
                let mut config = std::collections::HashMap::new();
                if let Some(region) = &args.cloud_region {
                    config.insert("region".to_string(), region.clone());
                }

                info!("Initializing {} cloud provider", provider_type.as_str());
                let provider = rusternetes_cloud_providers::create_provider(
                    provider_type,
                    args.cluster_name.clone(),
                    config,
                )
                .await?;
                Some(provider)
            } else {
                None
            }
        } else {
            // Auto-detect cloud provider
            let detected = rusternetes_cloud_providers::detect_cloud_provider();
            if detected != rusternetes_common::cloud_provider::CloudProviderType::None {
                info!("Auto-detected cloud provider: {}", detected.as_str());
                let mut config = std::collections::HashMap::new();
                if let Some(region) = &args.cloud_region {
                    config.insert("region".to_string(), region.clone());
                }
                let provider = rusternetes_cloud_providers::create_provider(
                    detected,
                    args.cluster_name.clone(),
                    config,
                )
                .await
                .ok();
                provider
            } else {
                None
            }
        }
    };

    #[cfg(not(feature = "cloud-providers"))]
    let cloud_provider: Option<Arc<dyn CloudProvider>> = None;

    // Initialize leader election if enabled
    let leader_elector = if args.enable_leader_election {
        let etcd_endpoints: Vec<String> = args
            .etcd_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let identity = args
            .leader_election_identity
            .unwrap_or_else(|| format!("controller-manager-{}", uuid::Uuid::new_v4()));

        let config = LeaderElectionConfig {
            identity: identity.clone(),
            lock_key: args.leader_election_lock_key.clone(),
            lease_duration: args.leader_election_lease_duration,
            renew_interval: args.leader_election_lease_duration / 3,
            retry_interval: 2,
        };

        info!(
            identity = %identity,
            "Leader election enabled - starting in follower mode"
        );

        let elector = Arc::new(LeaderElector::new(etcd_endpoints, config).await?);

        // Start leader election in background
        let elector_clone = elector.clone();
        tokio::spawn(async move {
            if let Err(e) = elector_clone.run().await {
                tracing::error!("Leader election error: {}", e);
            }
        });

        Some(elector)
    } else {
        warn!("Leader election disabled - running in single-instance mode");
        None
    };

    // Macro to spawn controllers with leader election support
    macro_rules! spawn_controller {
        ($name:expr, $elector:expr, $fut:expr) => {{
            let elector_clone = $elector.clone();
            tokio::spawn(async move {
                // Wait until we're the leader (if leader election is enabled)
                if let Some(ref elector) = elector_clone {
                    loop {
                        while !elector.is_leader().await {
                            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                        }
                        info!("{} starting (leader acquired)", $name);

                        // Run controller
                        $fut.await;

                        // Check if we're still the leader
                        if !elector.is_leader().await {
                            warn!("{} stopped (lost leadership)", $name);
                            continue;
                        }
                        break;
                    }
                } else {
                    // No leader election, run directly
                    $fut.await;
                }
            })
        }};
    }

    // Start LoadBalancer controller
    let lb_controller = Arc::new(LoadBalancerController::new(
        storage.clone(),
        cloud_provider.clone(),
        args.cluster_name.clone(),
        args.sync_interval,
    ));
    spawn_controller!("LoadBalancer controller", leader_elector, {
        let controller = lb_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("LoadBalancer controller error: {}", e);
            }
        }
    });

    // Start deployment controller
    let deployment_controller = Arc::new(DeploymentController::new(
        storage.clone(),
        args.sync_interval,
    ));
    spawn_controller!("Deployment controller", leader_elector, {
        let controller = deployment_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Deployment controller error: {}", e);
            }
        }
    });

    // Start replicationcontroller controller
    let rc_controller = Arc::new(ReplicationControllerController::new(
        storage.clone(),
        args.sync_interval,
    ));
    spawn_controller!("ReplicationController controller", leader_elector, {
        let controller = rc_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ReplicationController controller error: {}", e);
            }
        }
    });

    // Start ReplicaSet controller
    let replicaset_controller = Arc::new(ReplicaSetController::new(
        storage.clone(),
        args.sync_interval,
    ));
    spawn_controller!("ReplicaSet controller", leader_elector, {
        let controller = replicaset_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ReplicaSet controller error: {}", e);
            }
        }
    });

    // Start StatefulSet controller
    let statefulset_controller = Arc::new(StatefulSetController::new(storage.clone()));
    spawn_controller!("StatefulSet controller", leader_elector, {
        let controller = statefulset_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("StatefulSet controller error: {}", e);
            }
        }
    });

    // Start DaemonSet controller
    let daemonset_controller = Arc::new(DaemonSetController::new(storage.clone()));
    spawn_controller!("DaemonSet controller", leader_elector, {
        let controller = daemonset_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("DaemonSet controller error: {}", e);
            }
        }
    });

    // Start Job controller
    let job_controller = Arc::new(JobController::new(storage.clone()));
    spawn_controller!("Job controller", leader_elector, {
        let controller = job_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Job controller error: {}", e);
            }
        }
    });

    // Start CronJob controller
    let cronjob_controller = Arc::new(CronJobController::new(storage.clone()));
    spawn_controller!("CronJob controller", leader_elector, {
        let controller = cronjob_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("CronJob controller error: {}", e);
            }
        }
    });

    // Start PV/PVC Binder controller
    let pv_binder_controller = Arc::new(PVBinderController::new(storage.clone()));
    spawn_controller!("PV/PVC Binder controller", leader_elector, {
        let controller = pv_binder_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("PV/PVC Binder controller error: {}", e);
            }
        }
    });

    // Start Dynamic Provisioner controller
    let dynamic_provisioner_controller =
        Arc::new(DynamicProvisionerController::new(storage.clone()));
    spawn_controller!("Dynamic Provisioner controller", leader_elector, {
        let controller = dynamic_provisioner_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Dynamic Provisioner controller error: {}", e);
            }
        }
    });

    // Start Volume Snapshot controller
    let volume_snapshot_controller = Arc::new(VolumeSnapshotController::new(storage.clone()));
    spawn_controller!("Volume Snapshot controller", leader_elector, {
        let controller = volume_snapshot_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Volume Snapshot controller error: {}", e);
            }
        }
    });

    // Start Volume Expansion controller
    let volume_expansion_controller = Arc::new(VolumeExpansionController::new(storage.clone()));
    spawn_controller!("Volume Expansion controller", leader_elector, {
        let controller = volume_expansion_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Volume Expansion controller error: {}", e);
            }
        }
    });

    // Start StorageClass controller
    let storage_class_controller = Arc::new(StorageClassController::new(storage.clone()));
    spawn_controller!("StorageClass controller", leader_elector, {
        let controller = storage_class_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("StorageClass controller error: {}", e);
            }
        }
    });

    // Start Endpoints controller (watch-based)
    let endpoints_controller = Arc::new(EndpointsController::new(storage.clone()));
    spawn_controller!("Endpoints controller", leader_elector, {
        let controller = endpoints_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Endpoints controller error: {}", e);
            }
        }
    });

    // Start EndpointSlice controller (watch-based)
    let endpointslice_controller = Arc::new(EndpointSliceController::new(storage.clone()));
    spawn_controller!("EndpointSlice controller", leader_elector, {
        let controller = endpointslice_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("EndpointSlice controller error: {}", e);
            }
        }
    });

    // Start Events controller
    let events_controller = Arc::new(EventsController::new(storage.clone(), args.sync_interval));
    spawn_controller!("Events controller", leader_elector, {
        let controller = events_controller.clone();
        async move {
            controller.run().await;
        }
    });

    // Start ResourceQuota controller
    let resource_quota_controller = Arc::new(ResourceQuotaController::new(storage.clone()));
    spawn_controller!("ResourceQuota controller", leader_elector, {
        let controller = resource_quota_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ResourceQuota controller error: {}", e);
            }
        }
    });

    // Start Garbage Collector
    let garbage_collector = Arc::new(GarbageCollector::new(storage.clone()));
    spawn_controller!("Garbage Collector", leader_elector, {
        let controller = garbage_collector.clone();
        async move {
            controller.run().await;
        }
    });

    // Start HPA controller
    let hpa_metrics_cfg = HttpMetricsConfig {
        api_server_url: args.api_server_url.clone(),
        ca_cert_path: format!("{}/ca.crt", args.pki_dir),
        client_cert_path: format!("{}/api-server.crt", args.pki_dir),
        client_key_path: format!("{}/api-server.key", args.pki_dir),
        insecure_skip_tls_verify: args.metrics_insecure_skip_tls_verify,
    };
    let hpa_controller = Arc::new(HorizontalPodAutoscalerController::with_config(
        storage.clone(),
        hpa_metrics_cfg,
    ));
    spawn_controller!("HPA controller", leader_elector, {
        let controller = hpa_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                error!("HPA controller error: {}", e);
            }
        }
    });

    // Start VPA controller
    let vpa_controller = Arc::new(VerticalPodAutoscalerController::new(Arc::clone(&storage)));
    spawn_controller!("VPA controller", leader_elector, {
        let controller = vpa_controller.clone();
        async move {
            controller.run().await;
        }
    });

    // Start TTL controller
    let ttl_controller = Arc::new(TTLController::new(storage.clone()));
    spawn_controller!("TTL controller", leader_elector, {
        let controller = ttl_controller.clone();
        async move {
            controller.run().await;
        }
    });

    // Start PodDisruptionBudget controller
    let pdb_controller = Arc::new(PodDisruptionBudgetController::new(storage.clone()));
    spawn_controller!("PodDisruptionBudget controller", leader_elector, {
        let controller = pdb_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                error!("PodDisruptionBudget controller error: {}", e);
            }
        }
    });

    // Start StalePodDisruption sub-controller
    let stale_pdb_controller = Arc::new(StalePodDisruptionController::new(storage.clone()));
    spawn_controller!("StalePodDisruption controller", leader_elector, {
        let controller = stale_pdb_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                error!("StalePodDisruption controller error: {}", e);
            }
        }
    });

    // Start NetworkPolicy controller
    let network_policy_controller = Arc::new(NetworkPolicyController::new(storage.clone()));
    spawn_controller!("NetworkPolicy controller", leader_elector, {
        let controller = network_policy_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("NetworkPolicy controller error: {}", e);
            }
        }
    });

    // Start Ingress controller
    let ingress_controller = Arc::new(IngressController::new(storage.clone()));
    spawn_controller!("Ingress controller", leader_elector, {
        let controller = ingress_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Ingress controller error: {}", e);
            }
        }
    });

    // Start CertificateSigningRequest controller, enabling the in-process signer
    // when a cluster CA is provided via env (RUSTERNETES_CA_CERT_FILE/KEY_FILE).
    let csr_controller = {
        let mut controller = CertificateSigningRequestController::new(storage.clone());
        if let Some(ca) = controllers::cert_authority::load_cluster_ca_from_env() {
            controller = controller.with_certificate_authority(ca);
        }
        Arc::new(controller)
    };
    spawn_controller!("CertificateSigningRequest controller", leader_elector, {
        let controller = csr_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("CertificateSigningRequest controller error: {}", e);
            }
        }
    });

    // Start CRD controller
    let crd_controller = Arc::new(CRDController::new(storage.clone()));
    spawn_controller!("CRD controller", leader_elector, {
        let controller = crd_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("CRD controller error: {}", e);
            }
        }
    });

    // Start Namespace controller (watch-based). Hand it the CA cert so it can
    // (re)create kube-root-ca.crt in every namespace (read from pki_dir, the
    // same dir the metrics/cert paths use above).
    let ns_ca = std::fs::read_to_string(format!("{}/ca.crt", args.pki_dir)).ok();
    let namespace_controller =
        Arc::new(NamespaceController::new(storage.clone()).with_ca_cert(ns_ca));
    spawn_controller!("Namespace controller", leader_elector, {
        let controller = namespace_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Namespace controller error: {}", e);
            }
        }
    });

    // Start TaintEviction controller (watch-based)
    let taint_eviction_controller =
        Arc::new(crate::controllers::taint_eviction::TaintEvictionController::new(storage.clone()));
    spawn_controller!("TaintEviction controller", leader_elector, {
        let controller = taint_eviction_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("TaintEviction controller error: {}", e);
            }
        }
    });

    // Start ServiceAccount controller (watch-based)
    let serviceaccount_controller = Arc::new(ServiceAccountController::new(storage.clone()));
    spawn_controller!("ServiceAccount controller", leader_elector, {
        let controller = serviceaccount_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ServiceAccount controller error: {}", e);
            }
        }
    });

    // Start Service controller (watch-based)
    let service_controller = Arc::new(ServiceController::new(storage.clone()));
    spawn_controller!("Service controller", leader_elector, {
        let controller = service_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Service controller error: {}", e);
            }
        }
    });

    // Start Node controller (watch-based)
    let node_controller = Arc::new(NodeController::new(storage.clone()));
    spawn_controller!("Node controller", leader_elector, {
        let controller = node_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Node controller error: {}", e);
            }
        }
    });

    // Start PriorityClass controller
    let priorityclass_controller = Arc::new(PriorityClassController::new(storage.clone()));
    spawn_controller!("PriorityClass controller", leader_elector, {
        let controller = priorityclass_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("PriorityClass controller error: {}", e);
            }
        }
    });

    // Start APIService availability controller
    let apiservice_controller = Arc::new(APIServiceAvailabilityController::new(storage.clone()));
    spawn_controller!("APIService availability controller", leader_elector, {
        let controller = apiservice_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("APIService availability controller error: {}", e);
            }
        }
    });

    info!("All controllers started successfully");

    // Keep the main thread alive
    tokio::signal::ctrl_c().await?;
    info!("Shutting down controller manager");

    Ok(())
}
