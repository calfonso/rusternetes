use crate::{handlers::watch, middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    auth::ServiceAccountClaims,
    authz::{Decision, RequestAttributes},
    resources::{Namespace, Secret, ServiceAccount},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

// Removed - using HashMap<String, String> for query params

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut namespace): DumpingJson<Namespace>,
) -> Result<(StatusCode, Json<Namespace>)> {
    info!("Creating namespace: {}", namespace.metadata.name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "namespaces").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::require_object_name(&namespace.metadata)?;

    // Enrich metadata with system fields. `ensure_uid()` also resolves
    // `generateName` -> `metadata.name` (via `ensure_name()`).
    namespace.metadata.ensure_uid();
    namespace.metadata.ensure_creation_timestamp();

    // Auto-attach the `kubernetes.io/metadata.name=<name>` label.
    // Required by upstream PR kubernetes/kubernetes#96968: every namespace
    // must carry this label so cluster-scoped selectors (e.g. NetworkPolicies)
    // can target a namespace by name without relying on user-set labels.
    // Mirrors the constant `corev1.LabelMetadataName` and the registry-level
    // mutator that stamps it on every create.
    {
        let ns_name = namespace.metadata.name.clone();
        let labels = namespace.metadata.labels.get_or_insert_with(HashMap::new);
        labels.insert("kubernetes.io/metadata.name".to_string(), ns_name);
    }

    // Ensure namespace has Active status (always set phase even if status exists but phase is None)
    match &mut namespace.status {
        None => {
            namespace.status = Some(rusternetes_common::resources::NamespaceStatus {
                phase: Some(rusternetes_common::types::Phase::Active),
                conditions: None,
            });
        }
        Some(status) if status.phase.is_none() => {
            status.phase = Some(rusternetes_common::types::Phase::Active);
        }
        _ => {}
    }

    // Add kubernetes finalizer (prevents immediate deletion; namespace controller cleans up)
    let finalizers = namespace.metadata.finalizers.get_or_insert_with(Vec::new);
    if !finalizers.contains(&"kubernetes".to_string()) {
        finalizers.push("kubernetes".to_string());
    }

    // Ensure kind/apiVersion
    namespace.type_meta.kind = "Namespace".to_string();
    namespace.type_meta.api_version = "v1".to_string();

    let key = build_key("namespaces", None, &namespace.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Namespace {} validated successfully (not created)",
            namespace.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(namespace)));
    }

    let created = state.storage.create(&key, &namespace).await?;

    // Automatically create default ServiceAccount in the new namespace
    let ns_name = created.metadata.name.clone();
    info!("Creating default ServiceAccount for namespace: {}", ns_name);

    match create_default_service_account(&state, &ns_name).await {
        Ok(_) => {
            info!(
                "Successfully created default ServiceAccount for namespace: {}",
                ns_name
            );
        }
        Err(e) => {
            tracing::warn!(
                "Failed to create default ServiceAccount for namespace {}: {}",
                ns_name,
                e
            );
            // Don't fail namespace creation if default SA creation fails
        }
    }

    // Create kube-root-ca.crt ConfigMap (required by Kubernetes conformance).
    // Best-effort: try the well-known filesystem cert paths, then fall back to
    // `state.ca_cert_pem` (only populated when TLS is enabled). If neither is
    // present — e.g. a non-TLS deployment — the cert is empty and ConfigMap
    // creation is skipped below.
    let ca_cert = match tokio::fs::read_to_string("/etc/kubernetes/pki/ca.crt").await {
        Ok(s) => s,
        Err(_) => match tokio::fs::read_to_string("/etc/kubernetes/pki/api-server.crt").await {
            Ok(s) => s,
            Err(_) => match tokio::fs::read_to_string("/root/.rusternetes/certs/ca.crt").await {
                Ok(s) => s,
                Err(_) => state.ca_cert_pem.clone().unwrap_or_default(),
            },
        },
    };

    info!(
        "kube-root-ca.crt for namespace {}: cert_len={}, ns_name={}",
        ns_name,
        ca_cert.len(),
        namespace.metadata.name
    );

    if !ca_cert.is_empty() {
        let ca_cm = rusternetes_common::resources::ConfigMap {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "ConfigMap".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("kube-root-ca.crt")
                .with_namespace(ns_name.clone()),
            data: Some(std::collections::HashMap::from([(
                "ca.crt".to_string(),
                ca_cert,
            )])),
            binary_data: None,
            immutable: None,
        };
        let cm_key = build_key("configmaps", Some(&ns_name), "kube-root-ca.crt");
        match state.storage.create(&cm_key, &ca_cm).await {
            Ok(_) => info!("Created kube-root-ca.crt in namespace {}", ns_name),
            Err(e) => warn!(
                "Failed to create kube-root-ca.crt in namespace {}: {}",
                ns_name, e
            ),
        }
    } else {
        warn!(
            "CA cert is empty, skipping kube-root-ca.crt for namespace {}",
            ns_name
        );
    }

    // Create extension-apiserver-authentication ConfigMap in kube-system.
    // K8s creates this via the ClusterAuthenticationTrust controller.
    // Extension API servers (like sample-apiserver) read this on startup
    // to configure authentication delegation. Without it they crash.
    if ns_name == "kube-system" {
        // Same best-effort cert resolution as kube-root-ca.crt above:
        // filesystem paths first, then the injected `state.ca_cert_pem`.
        let ca_cert_for_auth = std::fs::read_to_string("/etc/kubernetes/pki/ca.crt")
            .or_else(|_| std::fs::read_to_string("/etc/kubernetes/pki/api-server.crt"))
            .or_else(|_| std::fs::read_to_string("/root/.rusternetes/certs/ca.crt"))
            .unwrap_or_else(|_| state.ca_cert_pem.clone().unwrap_or_default());

        let auth_cm = rusternetes_common::resources::ConfigMap {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "ConfigMap".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new(
                "extension-apiserver-authentication",
            )
            .with_namespace("kube-system".to_string()),
            data: Some(std::collections::HashMap::from([
                ("client-ca-file".to_string(), ca_cert_for_auth.clone()),
                ("requestheader-client-ca-file".to_string(), ca_cert_for_auth),
                (
                    "requestheader-username-headers".to_string(),
                    "[\"x-remote-user\"]".to_string(),
                ),
                (
                    "requestheader-group-headers".to_string(),
                    "[\"x-remote-group\"]".to_string(),
                ),
                (
                    "requestheader-extra-headers-prefix".to_string(),
                    "[\"x-remote-extra-\"]".to_string(),
                ),
                ("requestheader-allowed-names".to_string(), "[]".to_string()),
            ])),
            binary_data: None,
            immutable: None,
        };
        let auth_cm_key = build_key(
            "configmaps",
            Some("kube-system"),
            "extension-apiserver-authentication",
        );
        match state.storage.create(&auth_cm_key, &auth_cm).await {
            Ok(_) => info!("Created extension-apiserver-authentication ConfigMap in kube-system"),
            Err(e) => debug!(
                "extension-apiserver-authentication already exists or error: {}",
                e
            ),
        }

        // Create the Role that allows extension API servers to read this ConfigMap
        let reader_role = serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {
                "name": "extension-apiserver-authentication-reader",
                "namespace": "kube-system"
            },
            "rules": [{
                "apiGroups": [""],
                "resources": ["configmaps"],
                "resourceNames": ["extension-apiserver-authentication"],
                "verbs": ["get", "list", "watch"]
            }]
        });
        let role_key = build_key(
            "roles",
            Some("kube-system"),
            "extension-apiserver-authentication-reader",
        );
        match state.storage.create(&role_key, &reader_role).await {
            Ok(_) => info!("Created extension-apiserver-authentication-reader Role"),
            Err(e) => debug!(
                "extension-apiserver-authentication-reader already exists or error: {}",
                e
            ),
        }
    }

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<Namespace>> {
    debug!("Getting namespace: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "namespaces")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("namespaces", None, &name);
    let mut namespace: Namespace = state.storage.get(&key).await?;

    // Ensure status is present (old namespaces may not have it)
    if namespace.status.is_none() {
        namespace.status = Some(rusternetes_common::resources::NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        });
    }
    namespace.type_meta.kind = "Namespace".to_string();
    namespace.type_meta.api_version = "v1".to_string();

    Ok(Json(namespace))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut namespace): DumpingJson<Namespace>,
) -> Result<Json<Namespace>> {
    info!("Updating namespace: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "namespaces")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    namespace.metadata.name = name.clone();

    let key = build_key("namespaces", None, &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Namespace {} validated successfully (not updated)",
            name
        );
        return Ok(Json(namespace));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &namespace).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &namespace).await?
        }
        Err(e) => return Err(e),
    };

    // Namespace finalization removal — mirrors upstream
    // `ShouldDeleteNamespaceDuringUpdate`
    // (pkg/registry/core/namespace/storage/storage.go, release-1.35):
    //   len(ns.Spec.Finalizers) == 0 && ShouldDeleteDuringUpdate(...)
    // i.e. once the finalizer list drains AND the namespace is already
    // Terminating (deletionTimestamp set), the object is removed from storage.
    // This route also serves the `/api/v1/namespaces/:name/finalize`
    // subresource, which is exactly how the namespace controller completes
    // deletion. Without this, every namespace leaked `Terminating` forever and
    // the controller spun re-deleting them on false `Ok` (#1161). rusternetes
    // keeps the lifecycle finalizer in `metadata.finalizers`, so check there.
    let terminating = result.metadata.deletion_timestamp.is_some();
    let no_finalizers = result
        .metadata
        .finalizers
        .as_ref()
        .is_none_or(|f| f.is_empty());
    if terminating && no_finalizers {
        match state.storage.delete(&key).await {
            Ok(_) => info!(
                "Namespace {} finalized (finalizers drained) — removed from storage",
                name
            ),
            Err(e) => warn!("Failed to remove finalized namespace {}: {}", name, e),
        }
    }

    Ok(Json(result))
}

pub async fn delete_ns(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Namespace>> {
    info!("Deleting namespace: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "namespaces")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("namespaces", None, &name);

    // Get the namespace to check for finalizers
    let mut namespace: Namespace = state.storage.get(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=namespace).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "Namespace",
        "namespaces",
        None,
        &name,
        &namespace,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Namespace {} validated successfully (not deleted)",
            name
        );
        return Ok(Json(namespace));
    }

    // Set namespace phase to Terminating and add deletionTimestamp
    // (Kubernetes sets Terminating phase before cascade-deleting resources)
    if let Some(ref mut status) = namespace.status {
        status.phase = Some(rusternetes_common::types::Phase::Terminating);
    } else {
        namespace.status = Some(rusternetes_common::resources::NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Terminating),
            conditions: None,
        });
    }
    if namespace.metadata.deletion_timestamp.is_none() {
        namespace.metadata.deletion_timestamp = Some(chrono::Utc::now());
    }
    // Save the Terminating state
    let _ = state.storage.update(&key, &namespace).await;

    // Don't cascade-delete synchronously. Return the Terminating namespace
    // immediately and let the namespace controller handle cleanup asynchronously.
    // This matches real K8s behavior where DELETE returns quickly and the
    // namespace controller observes the deletionTimestamp and does cleanup.
    //
    // Handle finalizers: if the namespace has finalizers, keep it in storage
    // (the controller will remove finalizers after cleanup). If no finalizers,
    // also keep it — the namespace controller will delete it after cleanup.
    let has_finalizers = namespace
        .metadata
        .finalizers
        .as_ref()
        .map(|f| !f.is_empty())
        .unwrap_or(false);

    if !has_finalizers {
        // Add the kubernetes finalizer so the namespace stays in storage
        // until the controller finishes cleanup
        namespace
            .metadata
            .finalizers
            .get_or_insert_with(Vec::new)
            .push("kubernetes".to_string());
        let _ = state.storage.update(&key, &namespace).await;
    }

    info!("Namespace {} marked for deletion (Terminating)", name);
    // Return the namespace in Terminating state
    let updated: Namespace = state.storage.get(&key).await.unwrap_or(namespace);
    Ok(Json(updated))
}

/// Cascade delete all resources in a namespace
/// This ensures proper cleanup when a namespace is deleted
#[allow(dead_code)]
async fn cascade_delete_namespace_resources(
    storage: &rusternetes_storage::StorageBackend,
    namespace: &str,
) -> Result<()> {
    use serde_json::Value;
    use tracing::warn;

    // List of resource types that are namespace-scoped and should be deleted
    // Order matters: delete child resources before parent resources
    // Delete controllers FIRST so they don't recreate pods during cleanup.
    // Then delete pods last.
    let resource_types = vec![
        // 1. Delete controllers first (they recreate pods if deleted after pods)
        "cronjobs",
        "jobs",
        "deployments",
        "statefulsets",
        "daemonsets",
        "replicasets",
        "replicationcontrollers",
        // 2. Delete pods (now safe since controllers are gone)
        "pods",
        // 3. Delete supporting resources
        "services",
        "endpoints",
        "endpointslices",
        "configmaps",
        "secrets",
        "serviceaccounts",
        "persistentvolumeclaims",
        "ingresses",
        "networkpolicies",
        "poddisruptionbudgets",
        "resourcequotas",
        "limitranges",
        "horizontalpodautoscalers",
        "controllerrevisions",
        "podtemplates",
        "resourceclaims",
        "volumesnapshots",
        "leases",
        "rolebindings",
        "roles",
        "events",
    ];

    let mut total_deleted = 0;
    for resource_type in resource_types {
        let prefix = build_prefix(resource_type, Some(namespace));

        // List all resources with this prefix, then delete each one
        match storage.list::<Value>(&prefix).await {
            Ok(resources) => {
                let count = resources.len();
                for resource in resources {
                    // Extract the resource name from the metadata
                    if let Some(metadata) = resource.get("metadata") {
                        if let Some(name) = metadata.get("name").and_then(|n| n.as_str()) {
                            let key = build_key(resource_type, Some(namespace), name);
                            match storage.delete(&key).await {
                                Ok(_) => {
                                    total_deleted += 1;
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to delete {} {}/{}: {}",
                                        resource_type, namespace, name, e
                                    );
                                }
                            }
                        }
                    }
                }
                if count > 0 {
                    info!(
                        "Deleted {} {} resources in namespace {}",
                        count, resource_type, namespace
                    );
                }
            }
            Err(e) => {
                // Log warning but continue - resource type might not exist or have no resources
                warn!(
                    "Failed to list {} in namespace {}: {}",
                    resource_type, namespace, e
                );
            }
        }
    }

    info!(
        "Cascade deletion completed for namespace {}: {} resources deleted",
        namespace, total_deleted
    );
    Ok(())
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        debug!("Watching namespaces");
        // Parse WatchParams from the query parameters
        let watch_params = watch::WatchParams {
            resource_version: crate::handlers::watch::normalize_resource_version(
                params.get("resourceVersion").cloned(),
            ),
            timeout_seconds: params
                .get("timeoutSeconds")
                .and_then(|v| v.parse::<u64>().ok()),
            label_selector: params.get("labelSelector").cloned(),
            field_selector: params.get("fieldSelector").cloned(),
            watch: Some(true),
            allow_watch_bookmarks: params
                .get("allowWatchBookmarks")
                .and_then(|v| v.parse::<bool>().ok()),

            send_initial_events: params
                .get("sendInitialEvents")
                .and_then(|v| v.parse::<bool>().ok()),
        };
        return watch::watch_cluster_scoped::<Namespace>(
            state,
            auth_ctx,
            "namespaces",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing namespaces");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "namespaces").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("namespaces", None);
    let mut namespaces = state.storage.list::<Namespace>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut namespaces, &params)?;

    let mut list = List::new("NamespaceList", "v1", namespaces);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// Helper function to create the default ServiceAccount in a namespace
/// This replicates what Kubernetes does automatically when a namespace is created
async fn create_default_service_account(
    state: &Arc<ApiServerState>,
    namespace: &str,
) -> Result<()> {
    let sa_name = "default";

    // Create default ServiceAccount
    let mut service_account = ServiceAccount::new(sa_name, namespace);
    service_account.metadata.ensure_uid();
    service_account.metadata.ensure_creation_timestamp();

    let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);
    let created_sa = state.storage.create(&sa_key, &service_account).await?;

    // Generate ServiceAccount token and store it in a Secret
    let sa_uid = created_sa.metadata.uid.clone();

    // Generate JWT token (valid for 10 years - Kubernetes default for static tokens)
    let claims = ServiceAccountClaims::new(
        sa_name.to_string(),
        namespace.to_string(),
        sa_uid.clone(),
        87600, // 10 years in hours
    );

    let token = state.token_manager.generate_token(claims)?;

    // Create Secret to store the token
    let secret_name = format!("{}-token", sa_name);
    let mut string_data = HashMap::new();
    string_data.insert("token".to_string(), token);
    string_data.insert("namespace".to_string(), namespace.to_string());

    let mut secret =
        Secret::new(&secret_name, namespace).with_type("kubernetes.io/service-account-token");

    // Add labels and annotations
    secret.metadata.labels = Some({
        let mut labels = HashMap::new();
        labels.insert(
            "kubernetes.io/service-account.name".to_string(),
            sa_name.to_string(),
        );
        labels
    });
    secret.metadata.annotations = Some({
        let mut annotations = HashMap::new();
        annotations.insert("kubernetes.io/service-account.uid".to_string(), sa_uid);
        annotations
    });
    secret.string_data = Some(string_data);

    // Normalize: convert stringData to base64-encoded data before storing
    secret.normalize();

    // Store the secret
    let secret_key = build_key("secrets", Some(namespace), &secret_name);
    state.storage.create(&secret_key, &secret).await?;

    info!(
        "Created default ServiceAccount and token secret in namespace: {}",
        namespace
    );
    Ok(())
}

// Use the macro to create a PATCH handler for cluster-scoped namespace
crate::patch_handler_cluster!(patch, Namespace, "namespaces", "");

pub async fn deletecollection_namespaces(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection namespaces with params: {:?}", params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs =
        RequestAttributes::new(auth_ctx.user, "deletecollection", "namespaces").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Namespace collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all namespaces
    let prefix = build_prefix("namespaces", None);
    let mut items = state.storage.list::<Namespace>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("namespaces", None, &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "Namespace",
            "namespaces",
            None,
            &item.metadata.name,
            &item,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &item,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} namespaces deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
