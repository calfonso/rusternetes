// Library interface for controller-manager
pub mod controllers;
pub use controllers::*;

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
use rusternetes_client::http::ApiClient;
use rusternetes_storage::api_storage::ApiStorage;
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;
use tracing::{error, info};

/// Configuration for the controller-manager component.
pub struct ControllerManagerConfig {
    pub sync_interval: u64,
    /// Metrics client config for the HPA controller. When `None`,
    /// `HttpMetricsConfig::default()` is used (api-server:6443 + /etc/kubernetes/pki).
    pub metrics_config: Option<HttpMetricsConfig>,
    /// Cluster CA cert PEM, threaded to the namespace controller so it can
    /// (re)create `kube-root-ca.crt` in every namespace. `None` falls back to
    /// the legacy cert-file paths.
    pub ca_cert_pem: Option<String>,
    /// Node-IPAM config (pod-CIDR allocation). `None` disables it, matching
    /// upstream `--allocate-node-cidrs=false`.
    pub node_ipam: Option<crate::controllers::node_ipam::NodeIpamConfig>,
}

/// Run the controller-manager against a storage backend directly (all-in-one
/// binary's storage mode, and the standalone binary).
pub async fn run(
    storage: Arc<StorageBackend>,
    config: ControllerManagerConfig,
) -> anyhow::Result<()> {
    run_controllers(storage, config).await
}

/// Run the controller-manager as an api-server client: every controller's
/// `Storage` calls are proxied to the api-server over REST via [`ApiStorage`],
/// with no direct storage handle. This is the in-process counterpart of an
/// in-cluster controller-manager — the all-in-one binary calls this with an
/// [`ApiClient`] pointed at its embedded api-server over loopback, so
/// dns/scheduler/controller-manager all share the same trust boundary (only
/// the api-server touches storage).
pub async fn run_with_api(
    client: Arc<ApiClient>,
    config: ControllerManagerConfig,
) -> anyhow::Result<()> {
    run_controllers(Arc::new(ApiStorage::new(client)), config).await
}

/// Spawn all controllers as tokio tasks and wait for ctrl-c. Generic over the
/// storage seam so the SAME controllers run against either a real
/// `StorageBackend` or [`ApiStorage`].
async fn run_controllers<S: Storage + Send + Sync + 'static>(
    storage: Arc<S>,
    config: ControllerManagerConfig,
) -> anyhow::Result<()> {
    info!("Starting Rusternetes Controller Manager");

    let interval = config.sync_interval;
    let hpa_metrics_cfg = config.metrics_config.unwrap_or_default();

    // No leader election in all-in-one mode — single instance
    let cloud_provider: Option<Arc<dyn rusternetes_common::cloud_provider::CloudProvider>> = None;

    // Spawn all controllers
    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(LoadBalancerController::new(
            s,
            cloud_provider,
            "rusternetes".to_string(),
            interval,
        ));
        if let Err(e) = c.run().await {
            error!("LoadBalancer controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(DeploymentController::new(s, interval));
        if let Err(e) = c.run().await {
            error!("Deployment controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(ReplicationControllerController::new(s, interval));
        if let Err(e) = c.run().await {
            error!("ReplicationController controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(ReplicaSetController::new(s, interval));
        if let Err(e) = c.run().await {
            error!("ReplicaSet controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(StatefulSetController::new(s));
        if let Err(e) = c.run().await {
            error!("StatefulSet controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(DaemonSetController::new(s));
        if let Err(e) = c.run().await {
            error!("DaemonSet controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(JobController::new(s));
        if let Err(e) = c.run().await {
            error!("Job controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(CronJobController::new(s));
        if let Err(e) = c.run().await {
            error!("CronJob controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(PVBinderController::new(s));
        if let Err(e) = c.run().await {
            error!("PV/PVC Binder controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(DynamicProvisionerController::new(s));
        if let Err(e) = c.run().await {
            error!("Dynamic Provisioner controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(VolumeSnapshotController::new(s));
        if let Err(e) = c.run().await {
            error!("Volume Snapshot controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(VolumeExpansionController::new(s));
        if let Err(e) = c.run().await {
            error!("Volume Expansion controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(StorageClassController::new(s));
        if let Err(e) = c.run().await {
            error!("StorageClass controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(EndpointsController::new(s));
        if let Err(e) = c.run().await {
            error!("Endpoints controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(EndpointSliceController::new(s));
        if let Err(e) = c.run().await {
            error!("EndpointSlice controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(EventsController::new(s, interval));
        c.run().await;
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(ResourceQuotaController::new(s));
        if let Err(e) = c.run().await {
            error!("ResourceQuota controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = GarbageCollector::new(s);
        c.run().await;
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(HorizontalPodAutoscalerController::with_config(
            s,
            hpa_metrics_cfg,
        ));
        if let Err(e) = c.run().await {
            error!("HPA controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(VerticalPodAutoscalerController::new(s));
        c.run().await;
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(TTLController::new(s));
        c.run().await;
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(PodDisruptionBudgetController::new(s));
        if let Err(e) = c.run().await {
            error!("PodDisruptionBudget controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(StalePodDisruptionController::new(s));
        if let Err(e) = c.run().await {
            error!("StalePodDisruption controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(NetworkPolicyController::new(s));
        if let Err(e) = c.run().await {
            error!("NetworkPolicy controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(IngressController::new(s));
        if let Err(e) = c.run().await {
            error!("Ingress controller error: {}", e);
        }
    });

    let s = storage.clone();
    let csr_ca = controllers::cert_authority::load_cluster_ca_from_env();
    tokio::spawn(async move {
        let mut controller = CertificateSigningRequestController::new(s);
        if let Some(ca) = csr_ca {
            controller = controller.with_certificate_authority(ca);
        }
        let c = Arc::new(controller);
        if let Err(e) = c.run().await {
            error!("CertificateSigningRequest controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(CRDController::new(s));
        if let Err(e) = c.run().await {
            error!("CRD controller error: {}", e);
        }
    });

    let s = storage.clone();
    let ns_ca = config.ca_cert_pem.clone();
    tokio::spawn(async move {
        let c = Arc::new(NamespaceController::new(s).with_ca_cert(ns_ca));
        if let Err(e) = c.run().await {
            error!("Namespace controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(controllers::taint_eviction::TaintEvictionController::new(s));
        if let Err(e) = c.run().await {
            error!("TaintEviction controller error: {}", e);
        }
    });

    let s = storage.clone();
    let sa_ca = config.ca_cert_pem.clone();
    tokio::spawn(async move {
        let c = Arc::new(ServiceAccountController::new(s).with_ca_cert(sa_ca));
        if let Err(e) = c.run().await {
            error!("ServiceAccount controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(ServiceController::new(s));
        if let Err(e) = c.run().await {
            error!("Service controller error: {}", e);
        }
    });

    let s = storage.clone();
    let node_ipam = config.node_ipam.clone();
    tokio::spawn(async move {
        let mut nc = NodeController::new(s);
        if let Some(ipam) = node_ipam {
            nc = nc.with_node_ipam(ipam);
        }
        let c = Arc::new(nc);
        if let Err(e) = c.run().await {
            error!("Node controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(PriorityClassController::new(s));
        if let Err(e) = c.run().await {
            error!("PriorityClass controller error: {}", e);
        }
    });

    let s = storage.clone();
    tokio::spawn(async move {
        let c = Arc::new(APIServiceAvailabilityController::new(s));
        if let Err(e) = c.run().await {
            error!("APIService availability controller error: {}", e);
        }
    });

    info!("All controllers started successfully");

    // Keep alive until shutdown
    tokio::signal::ctrl_c().await?;
    info!("Shutting down controller manager");

    Ok(())
}
