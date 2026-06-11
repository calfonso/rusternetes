use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, State},
    Extension, Json,
};
use chrono::Utc;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{
        ContainerMetrics, NodeMetrics, NodeMetricsMetadata, PodMetrics, PodMetricsMetadata,
    },
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::debug;

/// Build per-container usage from a pod's resource requests/limits.
///
/// Fallback only: used when the kubelet has not yet published live
/// `PodMetrics` to storage. Reports `requests` (then `limits`, then a small
/// default) as a stand-in so `kubectl top` / HPA see a non-zero value during
/// the startup window.
fn fabricate_containers(pod: &rusternetes_common::resources::Pod) -> Vec<ContainerMetrics> {
    let mut containers = Vec::new();
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            let mut usage = BTreeMap::new();
            let cpu = container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .and_then(|req| req.get("cpu"))
                .or_else(|| {
                    container
                        .resources
                        .as_ref()
                        .and_then(|r| r.limits.as_ref())
                        .and_then(|lim| lim.get("cpu"))
                })
                .cloned()
                .unwrap_or_else(|| "100m".to_string());
            let memory = container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .and_then(|req| req.get("memory"))
                .or_else(|| {
                    container
                        .resources
                        .as_ref()
                        .and_then(|r| r.limits.as_ref())
                        .and_then(|lim| lim.get("memory"))
                })
                .cloned()
                .unwrap_or_else(|| "128Mi".to_string());
            usage.insert("cpu".to_string(), cpu);
            usage.insert("memory".to_string(), memory);
            containers.push(ContainerMetrics {
                name: container.name.clone(),
                usage,
            });
        }
    }
    containers
}

/// Return live `PodMetrics` published by the kubelet, or synthesize from the
/// pod spec when none have been published yet.
async fn pod_metrics_or_fallback<S: Storage>(
    storage: &S,
    namespace: &str,
    pod: &rusternetes_common::resources::Pod,
) -> PodMetrics {
    let name = &pod.metadata.name;
    let key = format!("/registry/metrics.k8s.io/pods/{}/{}", namespace, name);
    if let Ok(metrics) = storage.get::<PodMetrics>(&key).await {
        return metrics;
    }
    PodMetrics {
        api_version: "metrics.k8s.io/v1beta1".to_string(),
        kind: "PodMetrics".to_string(),
        metadata: PodMetricsMetadata {
            name: name.clone(),
            namespace: namespace.to_string(),
            creation_timestamp: Some(Utc::now()),
        },
        timestamp: Utc::now(),
        window: "30s".to_string(),
        containers: fabricate_containers(pod),
    }
}

/// Get metrics for a specific node.
/// Reads metrics published to storage by the kubelet.
pub async fn get_node_metrics(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<NodeMetrics>> {
    debug!("Getting node metrics: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "nodes")
        .with_api_group("metrics.k8s.io")
        .with_subresource("metrics")
        .with_name(&name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Check if node exists
    let node_key = build_key("nodes", None, &name);
    let _node: rusternetes_common::resources::Node = state.storage.as_ref().get(&node_key).await?;

    // Read metrics from storage (written by the kubelet)
    let metrics_key = format!("/registry/metrics.k8s.io/nodes/{}", name);
    match state
        .storage
        .as_ref()
        .get::<NodeMetrics>(&metrics_key)
        .await
    {
        Ok(metrics) => Ok(Json(metrics)),
        Err(_) => {
            // No metrics yet — return zeros
            let mut usage = BTreeMap::new();
            usage.insert("cpu".to_string(), "0m".to_string());
            usage.insert("memory".to_string(), "0Mi".to_string());
            Ok(Json(NodeMetrics {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "NodeMetrics".to_string(),
                metadata: NodeMetricsMetadata {
                    name,
                    creation_timestamp: Some(Utc::now()),
                },
                timestamp: Utc::now(),
                window: "30s".to_string(),
                usage,
            }))
        }
    }
}

/// List metrics for all nodes.
/// Reads metrics published to storage by the kubelets.
pub async fn list_node_metrics(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
) -> Result<Json<List<NodeMetrics>>> {
    debug!("Listing node metrics");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "nodes")
        .with_api_group("metrics.k8s.io")
        .with_subresource("metrics");

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Read all node metrics from storage
    let metrics: Vec<NodeMetrics> = state
        .storage
        .as_ref()
        .list("/registry/metrics.k8s.io/nodes/")
        .await
        .unwrap_or_default();

    // If no metrics in storage yet, return empty entries for each node
    if metrics.is_empty() {
        let nodes_prefix = build_prefix("nodes", None);
        let nodes: Vec<rusternetes_common::resources::Node> =
            state.storage.as_ref().list(&nodes_prefix).await?;
        let mut metrics_list = Vec::new();
        for node in nodes {
            let mut usage = BTreeMap::new();
            usage.insert("cpu".to_string(), "0m".to_string());
            usage.insert("memory".to_string(), "0Mi".to_string());
            metrics_list.push(NodeMetrics {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "NodeMetrics".to_string(),
                metadata: NodeMetricsMetadata {
                    name: node.metadata.name.clone(),
                    creation_timestamp: Some(Utc::now()),
                },
                timestamp: Utc::now(),
                window: "30s".to_string(),
                usage,
            });
        }
        let mut list = List::new("NodeMetricsList", "metrics.k8s.io/v1beta1", metrics_list);
        list.metadata.resource_version = Some(
            crate::handlers::list_collection_resource_version(&state.storage, &list.items).await,
        );
        return Ok(Json(list));
    }

    let mut list = List::new("NodeMetricsList", "metrics.k8s.io/v1beta1", metrics);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list))
}

/// Get metrics for a specific pod
pub async fn get_pod_metrics(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<PodMetrics>> {
    debug!("Getting pod metrics: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "pods")
        .with_api_group("metrics.k8s.io")
        .with_namespace(&namespace)
        .with_subresource("metrics")
        .with_name(&name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Check if pod exists
    let pod_key = build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.as_ref().get(&pod_key).await?;

    let metrics = pod_metrics_or_fallback(state.storage.as_ref(), &namespace, &pod).await;
    Ok(Json(metrics))
}

/// List metrics for all pods in a namespace
pub async fn list_pod_metrics(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
) -> Result<Json<List<PodMetrics>>> {
    debug!("Listing pod metrics in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "pods")
        .with_api_group("metrics.k8s.io")
        .with_namespace(&namespace)
        .with_subresource("metrics");

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Get all pods in namespace
    let pods_prefix = build_prefix("pods", Some(&namespace));
    let pods: Vec<rusternetes_common::resources::Pod> =
        state.storage.as_ref().list(&pods_prefix).await?;
    let mut metrics_list = Vec::new();

    for pod in pods {
        let metrics = pod_metrics_or_fallback(state.storage.as_ref(), &namespace, &pod).await;
        metrics_list.push(metrics);
    }

    let mut list = List::new("PodMetricsList", "metrics.k8s.io/v1beta1", metrics_list);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list))
}

/// List metrics for all pods across all namespaces
pub async fn list_all_pod_metrics(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
) -> Result<Json<List<PodMetrics>>> {
    debug!("Listing pod metrics across all namespaces");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "pods")
        .with_api_group("metrics.k8s.io")
        .with_subresource("metrics");

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Get all namespaces first
    let ns_prefix = build_prefix("namespaces", None);
    let namespaces: Vec<rusternetes_common::resources::Namespace> =
        state.storage.as_ref().list(&ns_prefix).await?;
    let mut metrics_list = Vec::new();

    for ns in namespaces {
        // Get all pods in this namespace
        let pods_prefix = build_prefix("pods", Some(&ns.metadata.name));
        let pods: Vec<rusternetes_common::resources::Pod> =
            state.storage.as_ref().list(&pods_prefix).await?;

        for pod in pods {
            let metrics =
                pod_metrics_or_fallback(state.storage.as_ref(), &ns.metadata.name, &pod).await;
            metrics_list.push(metrics);
        }
    }

    let mut list = List::new("PodMetricsList", "metrics.k8s.io/v1beta1", metrics_list);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list))
}

#[cfg(test)]
#[cfg(feature = "integration-tests")] // Disable tests that require full setup
mod tests {
    use super::*;
    use crate::state::ApiServerState;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysAllowAuthorizer;
    use rusternetes_common::resources::{Namespace, Node, NodeSpec, Pod, PodSpec};
    use rusternetes_common::storage::MemoryStorage;
    use rusternetes_common::types::ObjectMeta;

    async fn create_test_state() -> Arc<ApiServerState> {
        use rusternetes_common::auth::{BootstrapTokenManager, TokenManager};
        use rusternetes_common::observability::MetricsRegistry;
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let token_manager = Arc::new(TokenManager::new(b"test-key"));
        let bootstrap_token_manager = Arc::new(BootstrapTokenManager::new());
        let authorizer = Arc::new(AlwaysAllowAuthorizer);
        let metrics = Arc::new(MetricsRegistry::new());

        // Create a test node
        let node = Node {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-node"),
            spec: Some(NodeSpec {
                pod_cidr: Some("10.244.0.0/24".to_string()),
                pod_cidrs: None,
                provider_id: None,
                taints: None,
                unschedulable: None,
            }),
            status: None,
        };
        storage
            .create("/registry/nodes/test-node", &node)
            .await
            .unwrap();

        // Create a test namespace
        let ns = Namespace::new("default");
        storage
            .create("/registry/namespaces/default", &ns)
            .await
            .unwrap();

        // Create a test pod
        let pod = Pod::new(
            "test-pod",
            PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "nginx".to_string(),
                    image: "nginx:latest".to_string(),
                    command: None,
                    args: None,
                    working_dir: None,
                    ports: None,
                    env: None,
                    resources: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                }],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: Some("Always".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
            },
        );
        storage
            .create("/registry/pods/default/test-pod", &pod)
            .await
            .unwrap();

        Arc::new(ApiServerState::new(
            storage,
            token_manager,
            bootstrap_token_manager,
            authorizer,
            metrics,
            true, // skip_auth for tests
            None, // ca_cert_pem
        ))
    }

    #[tokio::test]
    async fn test_get_node_metrics() {
        let state = create_test_state().await;
        let auth_ctx = AuthContext {
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "test-uid".to_string(),
                groups: vec![],
                extra: Default::default(),
            },
        };

        let result = get_node_metrics(
            State(state),
            Extension(auth_ctx),
            Path("test-node".to_string()),
        )
        .await;

        assert!(result.is_ok());
        let metrics = result.unwrap().0;
        assert_eq!(metrics.metadata.name, "test-node");
        assert!(metrics.usage.contains_key("cpu"));
        assert!(metrics.usage.contains_key("memory"));
    }

    #[tokio::test]
    async fn test_list_node_metrics() {
        let state = create_test_state().await;
        let auth_ctx = AuthContext {
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "test-uid".to_string(),
                groups: vec![],
                extra: Default::default(),
            },
        };

        let result = list_node_metrics(State(state), Extension(auth_ctx)).await;

        assert!(result.is_ok());
        let list = result.unwrap().0;
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name, "test-node");
    }

    #[tokio::test]
    async fn test_get_pod_metrics() {
        let state = create_test_state().await;
        let auth_ctx = AuthContext {
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "test-uid".to_string(),
                groups: vec![],
                extra: Default::default(),
            },
        };

        let result = get_pod_metrics(
            State(state),
            Extension(auth_ctx),
            Path(("default".to_string(), "test-pod".to_string())),
        )
        .await;

        assert!(result.is_ok());
        let metrics = result.unwrap().0;
        assert_eq!(metrics.metadata.name, "test-pod");
        assert_eq!(metrics.metadata.namespace, "default");
        assert_eq!(metrics.containers.len(), 1);
        assert_eq!(metrics.containers[0].name, "nginx");
    }

    #[tokio::test]
    async fn test_list_pod_metrics() {
        let state = create_test_state().await;
        let auth_ctx = AuthContext {
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "test-uid".to_string(),
                groups: vec![],
                extra: Default::default(),
            },
        };

        let result = list_pod_metrics(
            State(state),
            Extension(auth_ctx),
            Path("default".to_string()),
        )
        .await;

        assert!(result.is_ok());
        let list = result.unwrap().0;
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name, "test-pod");
    }
}
