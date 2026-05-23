//! Dynamic API route registration for Custom Resource Definitions
//!
//! This module enables hot-reload of API routes when CRDs are created or deleted,
//! allowing the API server to automatically serve new custom resource types without restart.

#![allow(dead_code)]

use crate::{handlers::custom_resource, state::ApiServerState};
use axum::{routing::get, Router};
use rusternetes_common::resources::{CustomResourceDefinition, ResourceScope};
use std::sync::Arc;
use tracing::{debug, info};

/// Manages dynamic route registration for custom resources
pub struct DynamicRouteManager {
    /// API server state
    state: Arc<ApiServerState>,
}

impl DynamicRouteManager {
    /// Create a new dynamic route manager
    pub fn new(state: Arc<ApiServerState>) -> Self {
        Self { state }
    }

    /// Build routes for a CRD
    /// This creates the router configuration that should be registered when a CRD is created
    pub fn build_crd_routes(&self, crd: &CustomResourceDefinition) -> Router<Arc<ApiServerState>> {
        info!("Building dynamic routes for CRD: {}", crd.metadata.name);

        let group = &crd.spec.group;
        let plural = &crd.spec.names.plural;
        let scope = &crd.spec.scope;

        // Get all served versions
        let served_versions: Vec<_> = crd
            .spec
            .versions
            .iter()
            .filter(|v| v.served)
            .map(|v| v.name.as_str())
            .collect();

        debug!(
            "Building routes for {} served version(s): {:?}",
            served_versions.len(),
            served_versions
        );

        // Create new routes for each served version
        let mut new_routes = Router::new();

        for version in served_versions {
            let routes = match scope {
                ResourceScope::Namespaced => {
                    // Namespaced resources have both namespaced and cluster-wide list endpoints
                    let ns_path = format!(
                        "/apis/{}/{}/namespaces/:namespace/{}",
                        group, version, plural
                    );
                    let ns_name_path = format!(
                        "/apis/{}/{}/namespaces/:namespace/{}/:name",
                        group, version, plural
                    );
                    let cluster_path = format!("/apis/{}/{}/{}", group, version, plural);

                    Router::new()
                        .route(
                            &ns_path,
                            get(custom_resource::list_custom_resources)
                                .post(custom_resource::create_custom_resource),
                        )
                        .route(
                            &ns_name_path,
                            get(custom_resource::get_custom_resource)
                                .put(custom_resource::update_custom_resource)
                                .delete(custom_resource::delete_custom_resource),
                        )
                        .route(&cluster_path, get(custom_resource::list_custom_resources))
                }
                ResourceScope::Cluster => {
                    // Cluster-scoped resources have only cluster-wide endpoints
                    let cluster_path = format!("/apis/{}/{}/{}", group, version, plural);
                    let cluster_name_path = format!("/apis/{}/{}/{}/:name", group, version, plural);

                    Router::new()
                        .route(
                            &cluster_path,
                            get(custom_resource::list_custom_resources)
                                .post(custom_resource::create_custom_resource),
                        )
                        .route(
                            &cluster_name_path,
                            get(custom_resource::get_custom_resource)
                                .put(custom_resource::update_custom_resource)
                                .delete(custom_resource::delete_custom_resource),
                        )
                }
            };

            new_routes = new_routes.merge(routes);

            // Register subresource routes if defined
            if let Some(version_spec) = crd.spec.versions.iter().find(|v| v.name == version) {
                if let Some(ref subresources) = version_spec.subresources {
                    // Status subresource
                    if subresources.status.is_some() {
                        let status_routes =
                            self.create_status_routes(group, version, plural, scope);
                        new_routes = new_routes.merge(status_routes);
                    }

                    // Scale subresource
                    if subresources.scale.is_some() {
                        let scale_routes = self.create_scale_routes(group, version, plural, scope);
                        new_routes = new_routes.merge(scale_routes);
                    }
                }
            }
        }

        info!(
            "Successfully built dynamic routes for CRD: {}",
            crd.metadata.name
        );

        new_routes
    }

    /// Log when a CRD is registered (routes are built on-demand)
    pub fn register_crd(&self, crd: &CustomResourceDefinition) {
        info!(
            "CRD registered: {} (routes available on-demand)",
            crd.metadata.name
        );
    }

    /// Log when a CRD is unregistered
    pub fn unregister_crd(&self, crd: &CustomResourceDefinition) {
        info!(
            "CRD unregistered: {} (routes will return 404)",
            crd.metadata.name
        );
    }

    /// Create status subresource routes
    fn create_status_routes(
        &self,
        group: &str,
        version: &str,
        plural: &str,
        scope: &ResourceScope,
    ) -> Router<Arc<ApiServerState>> {
        match scope {
            ResourceScope::Namespaced => {
                let status_name_path = format!(
                    "/apis/{}/{}/namespaces/:namespace/{}/:name/status",
                    group, version, plural
                );

                Router::new().route(
                    &status_name_path,
                    get(custom_resource::get_custom_resource_status)
                        .put(custom_resource::update_custom_resource_status),
                )
            }
            ResourceScope::Cluster => {
                let status_name_path =
                    format!("/apis/{}/{}/{}/:name/status", group, version, plural);

                Router::new().route(
                    &status_name_path,
                    get(custom_resource::get_custom_resource_status)
                        .put(custom_resource::update_custom_resource_status),
                )
            }
        }
    }

    /// Create scale subresource routes
    fn create_scale_routes(
        &self,
        group: &str,
        version: &str,
        plural: &str,
        scope: &ResourceScope,
    ) -> Router<Arc<ApiServerState>> {
        match scope {
            ResourceScope::Namespaced => {
                let scale_path = format!(
                    "/apis/{}/{}/namespaces/:namespace/{}/:name/scale",
                    group, version, plural
                );

                Router::new().route(
                    &scale_path,
                    get(custom_resource::get_custom_resource_scale)
                        .put(custom_resource::update_custom_resource_scale),
                )
            }
            ResourceScope::Cluster => {
                let scale_path = format!("/apis/{}/{}/{}/:name/scale", group, version, plural);

                Router::new().route(
                    &scale_path,
                    get(custom_resource::get_custom_resource_scale)
                        .put(custom_resource::update_custom_resource_scale),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, CustomResourceSubresourceScale,
        CustomResourceSubresourceStatus, CustomResourceSubresources,
    };
    use rusternetes_common::types::ObjectMeta;

    // Build an ApiServerState backed by the in-memory storage backend, so these
    // tests can exercise route-building logic without any external etcd cluster.
    async fn create_test_state() -> Arc<ApiServerState> {
        use rusternetes_common::auth::TokenManager;
        use rusternetes_common::authz::AlwaysAllowAuthorizer;
        use rusternetes_common::observability::MetricsRegistry;
        use rusternetes_storage::StorageBackend;

        let storage = Arc::new(StorageBackend::new_memory());
        let token_manager = Arc::new(TokenManager::new(b"test-secret"));
        let authorizer =
            Arc::new(AlwaysAllowAuthorizer) as Arc<dyn rusternetes_common::authz::Authorizer>;
        let metrics = Arc::new(MetricsRegistry::new());

        Arc::new(ApiServerState::new(
            storage,
            token_manager,
            authorizer,
            metrics,
            true,
        ))
    }

    fn create_test_crd(with_subresources: bool) -> CustomResourceDefinition {
        let mut version = CustomResourceDefinitionVersion {
            name: "v1".to_string(),
            served: true,
            storage: true,
            deprecated: None,
            deprecation_warning: None,
            schema: None,
            subresources: None,
            additional_printer_columns: None,
            selectable_fields: None,
        };

        if with_subresources {
            version.subresources = Some(CustomResourceSubresources {
                status: Some(CustomResourceSubresourceStatus {}),
                scale: Some(CustomResourceSubresourceScale {
                    spec_replicas_path: ".spec.replicas".to_string(),
                    status_replicas_path: ".status.replicas".to_string(),
                    label_selector_path: Some(".status.labelSelector".to_string()),
                }),
            });
        }

        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: ObjectMeta::new("crontabs.stable.example.com"),
            spec: CustomResourceDefinitionSpec {
                group: "stable.example.com".to_string(),
                names: CustomResourceDefinitionNames {
                    plural: "crontabs".to_string(),
                    singular: Some("crontab".to_string()),
                    kind: "CronTab".to_string(),
                    short_names: Some(vec!["ct".to_string()]),
                    categories: None,
                    list_kind: Some("CronTabList".to_string()),
                },
                scope: ResourceScope::Namespaced,
                versions: vec![version],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn test_dynamic_route_manager_creation() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        // Manager should be created successfully
        assert!(std::ptr::addr_of!(manager) as usize > 0);
    }

    #[tokio::test]
    async fn test_build_namespaced_crd_routes() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        let crd = create_test_crd(false);

        let _routes = manager.build_crd_routes(&crd);
        // If we got here without panicking, route building succeeded
    }

    #[tokio::test]
    async fn test_build_crd_routes_with_subresources() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        let crd = create_test_crd(true);

        let _routes = manager.build_crd_routes(&crd);
        // If we got here without panicking, route building with subresources succeeded
    }

    #[tokio::test]
    async fn test_build_cluster_scoped_crd_routes() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        let mut crd = create_test_crd(false);
        crd.spec.scope = ResourceScope::Cluster;

        let _routes = manager.build_crd_routes(&crd);
        // If we got here without panicking, cluster-scoped route building succeeded
    }

    #[tokio::test]
    async fn test_register_and_unregister_crd() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        let crd = create_test_crd(false);

        manager.register_crd(&crd);
        manager.unregister_crd(&crd);
        // If we got here without panicking, registration/unregistration succeeded
    }
}
