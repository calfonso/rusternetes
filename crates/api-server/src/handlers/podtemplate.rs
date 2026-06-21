use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::PodTemplate,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_podtemplate(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut podtemplate): DumpingJson<PodTemplate>,
) -> Result<(StatusCode, Json<PodTemplate>)> {
    info!(
        "Creating podtemplate: {} in namespace: {}",
        podtemplate.metadata.name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "podtemplates")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &podtemplate.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Ensure namespace is set from the URL path
    podtemplate.metadata.namespace = Some(namespace.clone());

    // Validate the embedded template (upstream ValidatePodTemplateSpec): labels,
    // annotations, and the pod spec.
    let errs = rusternetes_common::validation::podtemplate::validate_pod_template(&podtemplate);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Enrich metadata with system fields
    podtemplate.metadata.ensure_uid();
    podtemplate.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PodTemplate validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(podtemplate)));
    }

    let key = build_key("podtemplates", Some(&namespace), &podtemplate.metadata.name);
    let created = state.storage.create(&key, &podtemplate).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_podtemplate(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<PodTemplate>> {
    debug!("Getting podtemplate: {} in namespace: {}", name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "podtemplates")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("podtemplates", Some(&namespace), &name);
    let podtemplate = state.storage.get(&key).await?;

    Ok(Json(podtemplate))
}

pub async fn update_podtemplate(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut podtemplate): DumpingJson<PodTemplate>,
) -> Result<Json<PodTemplate>> {
    info!("Updating podtemplate: {} in namespace: {}", name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "podtemplates")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    podtemplate.metadata.name = name.clone();
    podtemplate.metadata.namespace = Some(namespace.clone());

    // Field validation on update (upstream ValidatePodTemplateUpdate re-runs
    // ValidatePodTemplateSpec on the new object). The create path validated but
    // the update path previously persisted PUTs unchecked.
    {
        let errs = rusternetes_common::validation::podtemplate::validate_pod_template(&podtemplate);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PodTemplate validated successfully (not updated)");
        return Ok(Json(podtemplate));
    }

    let key = build_key("podtemplates", Some(&namespace), &name);

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &podtemplate).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &podtemplate).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_podtemplate(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PodTemplate>> {
    info!("Deleting podtemplate: {} in namespace: {}", name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "podtemplates")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("podtemplates", Some(&namespace), &name);

    // Get the resource for finalizer handling
    let podtemplate: PodTemplate = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PodTemplate validated successfully (not deleted)");
        return Ok(Json(podtemplate));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &podtemplate,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(podtemplate))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: PodTemplate = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_podtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;

    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        let watch_params = crate::handlers::watch::WatchParams {
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
        return crate::handlers::watch::watch_namespaced::<PodTemplate>(
            state,
            auth_ctx,
            namespace,
            "podtemplates",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing podtemplates in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "podtemplates")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("podtemplates", Some(&namespace));
    let mut podtemplates: Vec<PodTemplate> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut podtemplates, &params)?;

    // Deterministic sort by (namespace, name) so `?continue` chains are
    // stable regardless of underlying storage iteration order. The offset
    // helper below relies on a total, repeatable order across pages.
    podtemplates.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    // Apply pagination
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();
    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };
    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    let paginated =
        match rusternetes_common::paginate(podtemplates, pagination_params, &resource_version) {
            Ok(p) => p,
            Err(e) => {
                if e.message.contains("410 Gone") {
                    let mut status =
                        rusternetes_common::Status::failure(&e.message, "Expired", 410);
                    if let Some(token) = e.fresh_continue_token {
                        status.metadata = Some(rusternetes_common::ListMeta {
                            resource_version: Some(resource_version),
                            continue_token: Some(token),
                            remaining_item_count: None,
                        });
                    }
                    return Ok((axum::http::StatusCode::GONE, Json(status)).into_response());
                }
                return Err(rusternetes_common::Error::InvalidResource(e.message));
            }
        };

    let mut list = List::new("PodTemplateList", "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);
    Ok(Json(list).into_response())
}

/// List all podtemplates across all namespaces
pub async fn list_all_podtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;

    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        let watch_params = crate::handlers::watch::WatchParams {
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
        return crate::handlers::watch::watch_cluster_scoped::<PodTemplate>(
            state,
            auth_ctx,
            "podtemplates",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all podtemplates");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "podtemplates").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("podtemplates", None);
    let mut podtemplates: Vec<PodTemplate> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut podtemplates, &params)?;

    // Deterministic sort by (namespace, name) so `?continue` chains are
    // stable regardless of underlying storage iteration order. The offset
    // helper below relies on a total, repeatable order across pages.
    podtemplates.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    // Apply pagination
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();
    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };
    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    let paginated =
        match rusternetes_common::paginate(podtemplates, pagination_params, &resource_version) {
            Ok(p) => p,
            Err(e) => {
                if e.message.contains("410 Gone") {
                    let mut status =
                        rusternetes_common::Status::failure(&e.message, "Expired", 410);
                    if let Some(token) = e.fresh_continue_token {
                        status.metadata = Some(rusternetes_common::ListMeta {
                            resource_version: Some(resource_version),
                            continue_token: Some(token),
                            remaining_item_count: None,
                        });
                    }
                    return Ok((axum::http::StatusCode::GONE, Json(status)).into_response());
                }
                return Err(rusternetes_common::Error::InvalidResource(e.message));
            }
        };

    let mut list = List::new("PodTemplateList", "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch_podtemplate, PodTemplate, "podtemplates", "");

#[cfg(test)]
#[cfg(feature = "integration-tests")] // Disable tests that require full setup
mod tests {
    use super::*;
    use crate::state::ApiServerState;
    use rusternetes_common::{
        authz::Authorizer,
        resources::{PodSpec, PodTemplateSpec},
        types::ObjectMeta,
        User,
    };
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_podtemplate_create() {
        use rusternetes_common::auth::{BootstrapTokenManager, TokenManager};
        use rusternetes_common::authz::AlwaysAllowAuthorizer;
        use rusternetes_common::observability::MetricsRegistry;

        let storage = Arc::new(MemoryStorage::new());
        let token_manager = Arc::new(TokenManager::new(b"test-key"));
        let bootstrap_token_manager = Arc::new(BootstrapTokenManager::new());
        let authorizer = Arc::new(AlwaysAllowAuthorizer);
        let metrics = Arc::new(MetricsRegistry::new());

        let state = Arc::new(ApiServerState::new(
            storage,
            token_manager,
            bootstrap_token_manager,
            authorizer,
            metrics,
            true, // skip_auth for tests
            None, // ca_cert_pem
        ));

        let pod_template_spec = PodTemplateSpec {
            metadata: Some(ObjectMeta::new("test-pod")),
            spec: PodSpec {
                containers: vec![],
                init_containers: None,
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
                ephemeral_containers: None,
                overhead: None,
                scheduler_name: None,
                topology_spread_constraints: None,
                resource_claims: None,
            },
        };

        let podtemplate = PodTemplate::new("test-template", "default", pod_template_spec);

        let auth_ctx = AuthContext {
            user: User::system(),
        };

        let result = create_podtemplate(
            State(state),
            Extension(auth_ctx),
            Path("default".to_string()),
            Json(podtemplate.clone()),
        )
        .await;

        assert!(result.is_ok());
        let (status, created) = result.unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created.0.metadata.name, "test-template");
    }
}

pub async fn deletecollection_podtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection podtemplates in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "podtemplates")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PodTemplate collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all podtemplates in the namespace
    let prefix = build_prefix("podtemplates", Some(&namespace));
    let mut items = state.storage.list::<PodTemplate>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("podtemplates", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} podtemplates deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
