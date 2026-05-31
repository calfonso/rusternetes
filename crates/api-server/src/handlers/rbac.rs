use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{ClusterRole, ClusterRoleBinding, Role, RoleBinding},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

// Role handlers
pub async fn create_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut role): DumpingJson<Role>,
) -> Result<(StatusCode, Json<Role>)> {
    info!("Creating role: {}/{}", namespace, role.metadata.name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    role.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    role.metadata.ensure_uid();
    role.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Role validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(role)));
    }

    let key = build_key("roles", Some(&namespace), &role.metadata.name);
    match state.storage.create(&key, &role).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(rusternetes_common::Error::AlreadyExists(_)) => {
            Err(rusternetes_common::Error::AlreadyExists(format!(
                "roles \"{}\" already exists",
                role.metadata.name
            )))
        }
        Err(e) => Err(e),
    }
}

pub async fn get_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Role>> {
    debug!("Getting role: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("roles", Some(&namespace), &name);
    let role = state.storage.get(&key).await?;

    Ok(Json(role))
}

pub async fn update_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut role): DumpingJson<Role>,
) -> Result<Json<Role>> {
    info!("Updating role: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    role.metadata.name = name.clone();
    role.metadata.namespace = Some(namespace.clone());

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Role validated successfully (not updated)");
        return Ok(Json(role));
    }

    let key = build_key("roles", Some(&namespace), &name);
    let updated = state.storage.update(&key, &role).await?;

    Ok(Json(updated))
}

pub async fn delete_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Role>> {
    info!("Deleting role: {}/{}", namespace, name);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("roles", Some(&namespace), &name);

    // Get the resource for finalizer handling
    let role: Role = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating admission webhooks for DELETE (object=nil, oldObject=role).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "rbac.authorization.k8s.io",
        "v1",
        "Role",
        "roles",
        Some(&namespace),
        &name,
        &role,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    if is_dry_run {
        info!("Dry-run: Role validated successfully (not deleted)");
        return Ok(Json(role));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &role)
            .await?;

    if deleted_immediately {
        Ok(Json(role))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Role = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<Role>(
            state,
            auth_ctx,
            namespace,
            "roles",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing roles in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("roles", Some(&namespace));
    let mut roles = state.storage.list::<Role>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut roles, &params)?;

    let list = List::new("RoleList", "rbac.authorization.k8s.io/v1", roles);
    Ok(Json(list).into_response())
}

/// List all roles across all namespaces
pub async fn list_all_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<Role>(
            state,
            auth_ctx,
            "roles",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all roles");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "roles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("roles", None);
    let mut roles = state.storage.list::<Role>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut roles, &params)?;

    let list = List::new("RoleList", "rbac.authorization.k8s.io/v1", roles);
    Ok(Json(list).into_response())
}

// RoleBinding handlers
pub async fn create_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut rolebinding): DumpingJson<RoleBinding>,
) -> Result<(StatusCode, Json<RoleBinding>)> {
    info!(
        "Creating rolebinding: {}/{}",
        namespace, rolebinding.metadata.name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    rolebinding.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    rolebinding.metadata.ensure_uid();
    rolebinding.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: RoleBinding validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(rolebinding)));
    }

    let key = build_key("rolebindings", Some(&namespace), &rolebinding.metadata.name);
    match state.storage.create(&key, &rolebinding).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(rusternetes_common::Error::AlreadyExists(_)) => {
            Err(rusternetes_common::Error::AlreadyExists(format!(
                "rolebindings \"{}\" already exists",
                rolebinding.metadata.name
            )))
        }
        Err(e) => Err(e),
    }
}

pub async fn get_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<RoleBinding>> {
    debug!("Getting rolebinding: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("rolebindings", Some(&namespace), &name);
    let rolebinding = state.storage.get(&key).await?;

    Ok(Json(rolebinding))
}

pub async fn update_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut rolebinding): DumpingJson<RoleBinding>,
) -> Result<Json<RoleBinding>> {
    info!("Updating rolebinding: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    rolebinding.metadata.name = name.clone();
    rolebinding.metadata.namespace = Some(namespace.clone());

    let key = build_key("rolebindings", Some(&namespace), &name);

    // Validate immutable roleRef — mirrors upstream
    // `pkg/registry/rbac/rolebinding/strategy.go::ValidateUpdate` which calls
    // `apivalidation.ValidateImmutableField(newRoleBinding.RoleRef, …)`.
    if let Ok(existing) = state.storage.get::<RoleBinding>(&key).await {
        if existing.role_ref != rolebinding.role_ref {
            return Err(rusternetes_common::Error::InvalidResource(
                "RoleBinding.roleRef: Invalid value: field is immutable".to_string(),
            ));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: RoleBinding validated successfully (not updated)");
        return Ok(Json(rolebinding));
    }

    let updated = state.storage.update(&key, &rolebinding).await?;

    Ok(Json(updated))
}

pub async fn delete_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<RoleBinding>> {
    info!("Deleting rolebinding: {}/{}", namespace, name);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("rolebindings", Some(&namespace), &name);

    // Get the resource for finalizer handling
    let rolebinding: RoleBinding = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating admission webhooks for DELETE (object=nil, oldObject=rolebinding).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "rbac.authorization.k8s.io",
        "v1",
        "RoleBinding",
        "rolebindings",
        Some(&namespace),
        &name,
        &rolebinding,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    if is_dry_run {
        info!("Dry-run: RoleBinding validated successfully (not deleted)");
        return Ok(Json(rolebinding));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &rolebinding,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(rolebinding))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: RoleBinding = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<RoleBinding>(
            state,
            auth_ctx,
            namespace,
            "rolebindings",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing rolebindings in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("rolebindings", Some(&namespace));
    let mut rolebindings = state.storage.list::<RoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rolebindings, &params)?;

    let list = List::new(
        "RoleBindingList",
        "rbac.authorization.k8s.io/v1",
        rolebindings,
    );
    Ok(Json(list).into_response())
}

/// List all rolebindings across all namespaces
pub async fn list_all_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Honor `?watch=true` on the all-namespaces collection (informer/Lens path).
    // Mirrors list_all_roles: the all-NS watch is just the no-namespace prefix,
    // so it routes through watch_cluster_scoped.
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<RoleBinding>(
            state,
            auth_ctx,
            "rolebindings",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    debug!("Listing all rolebindings");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "rolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("rolebindings", None);
    let mut rolebindings = state.storage.list::<RoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rolebindings, &params)?;

    let list = List::new(
        "RoleBindingList",
        "rbac.authorization.k8s.io/v1",
        rolebindings,
    );
    Ok(Json(list).into_response())
}

// ClusterRole handlers
pub async fn create_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut clusterrole): DumpingJson<ClusterRole>,
) -> Result<(StatusCode, Json<ClusterRole>)> {
    info!("Creating clusterrole: {}", clusterrole.metadata.name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Enrich metadata with system fields
    clusterrole.metadata.ensure_uid();
    clusterrole.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRole validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(clusterrole)));
    }

    let key = build_key("clusterroles", None, &clusterrole.metadata.name);

    match state.storage.create(&key, &clusterrole).await {
        Ok(created) => {
            info!(
                "ClusterRole created successfully: {}",
                clusterrole.metadata.name
            );
            Ok((StatusCode::CREATED, Json(created)))
        }
        Err(e) => {
            tracing::warn!(
                "Failed to create ClusterRole {}: {}",
                clusterrole.metadata.name,
                e
            );
            Err(e)
        }
    }
}

pub async fn get_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ClusterRole>> {
    debug!("Getting clusterrole: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("clusterroles", None, &name);
    let clusterrole = state.storage.get(&key).await?;

    Ok(Json(clusterrole))
}

pub async fn update_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut clusterrole): DumpingJson<ClusterRole>,
) -> Result<Json<ClusterRole>> {
    info!("Updating clusterrole: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    clusterrole.metadata.name = name.clone();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRole validated successfully (not updated)");
        return Ok(Json(clusterrole));
    }

    let key = build_key("clusterroles", None, &name);
    let updated = state.storage.update(&key, &clusterrole).await?;

    Ok(Json(updated))
}

pub async fn delete_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ClusterRole>> {
    info!("Deleting clusterrole: {}", name);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("clusterroles", None, &name);

    // Get the resource for finalizer handling
    let clusterrole: ClusterRole = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating admission webhooks for DELETE (object=nil, oldObject=clusterrole).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "rbac.authorization.k8s.io",
        "v1",
        "ClusterRole",
        "clusterroles",
        None,
        &name,
        &clusterrole,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    if is_dry_run {
        info!("Dry-run: ClusterRole validated successfully (not deleted)");
        return Ok(Json(clusterrole));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &clusterrole,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(clusterrole))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ClusterRole = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_clusterroles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing clusterroles");

    // Honor `?watch=true` on the collection endpoint (what Lens / client-go
    // informers use). Without this the legacy `/watch/clusterroles` subpath is
    // the only way to stream, so list-then-watch clients never see updates.
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ClusterRole>(
            state,
            auth_ctx,
            "clusterroles",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("clusterroles", None);
    let mut clusterroles = state.storage.list::<ClusterRole>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterroles, &params)?;

    let list = List::new(
        "ClusterRoleList",
        "rbac.authorization.k8s.io/v1",
        clusterroles,
    );
    Ok(Json(list).into_response())
}

// ClusterRoleBinding handlers
pub async fn create_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut clusterrolebinding): DumpingJson<ClusterRoleBinding>,
) -> Result<(StatusCode, Json<ClusterRoleBinding>)> {
    info!(
        "Creating clusterrolebinding: {}",
        clusterrolebinding.metadata.name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Enrich metadata with system fields
    clusterrolebinding.metadata.ensure_uid();
    clusterrolebinding.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRoleBinding validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(clusterrolebinding)));
    }

    let key = build_key(
        "clusterrolebindings",
        None,
        &clusterrolebinding.metadata.name,
    );

    match state.storage.create(&key, &clusterrolebinding).await {
        Ok(created) => {
            info!(
                "ClusterRoleBinding created successfully: {}",
                clusterrolebinding.metadata.name
            );
            Ok((StatusCode::CREATED, Json(created)))
        }
        Err(e) => {
            tracing::warn!(
                "Failed to create ClusterRoleBinding {}: {}",
                clusterrolebinding.metadata.name,
                e
            );
            Err(e)
        }
    }
}

pub async fn get_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ClusterRoleBinding>> {
    debug!("Getting clusterrolebinding: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("clusterrolebindings", None, &name);
    let clusterrolebinding = state.storage.get(&key).await?;

    Ok(Json(clusterrolebinding))
}

pub async fn update_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut clusterrolebinding): DumpingJson<ClusterRoleBinding>,
) -> Result<Json<ClusterRoleBinding>> {
    info!("Updating clusterrolebinding: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    clusterrolebinding.metadata.name = name.clone();

    let key = build_key("clusterrolebindings", None, &name);

    // Validate immutable roleRef — mirrors upstream
    // `pkg/registry/rbac/clusterrolebinding/strategy.go::ValidateUpdate` which
    // calls `apivalidation.ValidateImmutableField(newCRB.RoleRef, …)`.
    if let Ok(existing) = state.storage.get::<ClusterRoleBinding>(&key).await {
        if existing.role_ref != clusterrolebinding.role_ref {
            return Err(rusternetes_common::Error::InvalidResource(
                "ClusterRoleBinding.roleRef: Invalid value: field is immutable".to_string(),
            ));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRoleBinding validated successfully (not updated)");
        return Ok(Json(clusterrolebinding));
    }

    let updated = state.storage.update(&key, &clusterrolebinding).await?;

    Ok(Json(updated))
}

pub async fn delete_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ClusterRoleBinding>> {
    info!("Deleting clusterrolebinding: {}", name);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("clusterrolebindings", None, &name);

    // Get the resource for finalizer handling
    let clusterrolebinding: ClusterRoleBinding = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating admission webhooks for DELETE (object=nil, oldObject=clusterrolebinding).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "rbac.authorization.k8s.io",
        "v1",
        "ClusterRoleBinding",
        "clusterrolebindings",
        None,
        &name,
        &clusterrolebinding,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    if is_dry_run {
        info!("Dry-run: ClusterRoleBinding validated successfully (not deleted)");
        return Ok(Json(clusterrolebinding));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &clusterrolebinding,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(clusterrolebinding))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ClusterRoleBinding = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_clusterrolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing clusterrolebindings");

    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ClusterRoleBinding>(
            state,
            auth_ctx,
            "clusterrolebindings",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("clusterrolebindings", None);
    let mut clusterrolebindings = state.storage.list::<ClusterRoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterrolebindings, &params)?;

    let list = List::new(
        "ClusterRoleBindingList",
        "rbac.authorization.k8s.io/v1",
        clusterrolebindings,
    );
    Ok(Json(list).into_response())
}

// DeleteCollection handlers for RBAC resources
pub async fn deletecollection_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection roles in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Role collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all roles in the namespace
    let prefix = build_prefix("roles", Some(&namespace));
    let mut roles = state.storage.list::<Role>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut roles, &params)?;

    // Delete each matching role
    let mut deleted_count = 0;
    for role in roles {
        let key = build_key("roles", Some(&namespace), &role.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "rbac.authorization.k8s.io",
            "v1",
            "Role",
            "roles",
            Some(&namespace),
            &role.metadata.name,
            &role,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &role,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} roles deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection rolebindings in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: RoleBinding collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all rolebindings in the namespace
    let prefix = build_prefix("rolebindings", Some(&namespace));
    let mut rolebindings = state.storage.list::<RoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rolebindings, &params)?;

    // Delete each matching rolebinding
    let mut deleted_count = 0;
    for rolebinding in rolebindings {
        let key = build_key("rolebindings", Some(&namespace), &rolebinding.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "rbac.authorization.k8s.io",
            "v1",
            "RoleBinding",
            "rolebindings",
            Some(&namespace),
            &rolebinding.metadata.name,
            &rolebinding,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &rolebinding,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} rolebindings deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_clusterroles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection clusterroles with params: {:?}", params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRole collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all clusterroles
    let prefix = build_prefix("clusterroles", None);
    let mut clusterroles = state.storage.list::<ClusterRole>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterroles, &params)?;

    // Delete each matching clusterrole
    let mut deleted_count = 0;
    for clusterrole in clusterroles {
        let key = build_key("clusterroles", None, &clusterrole.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "rbac.authorization.k8s.io",
            "v1",
            "ClusterRole",
            "clusterroles",
            None,
            &clusterrole.metadata.name,
            &clusterrole,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &clusterrole,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} clusterroles deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_clusterrolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection clusterrolebindings with params: {:?}",
        params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRoleBinding collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all clusterrolebindings
    let prefix = build_prefix("clusterrolebindings", None);
    let mut clusterrolebindings = state.storage.list::<ClusterRoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterrolebindings, &params)?;

    // Delete each matching clusterrolebinding
    let mut deleted_count = 0;
    for clusterrolebinding in clusterrolebindings {
        let key = build_key(
            "clusterrolebindings",
            None,
            &clusterrolebinding.metadata.name,
        );

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "rbac.authorization.k8s.io",
            "v1",
            "ClusterRoleBinding",
            "clusterrolebindings",
            None,
            &clusterrolebinding.metadata.name,
            &clusterrolebinding,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &clusterrolebinding,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} clusterrolebindings deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

// Use macros to create PATCH handlers for RBAC resources
crate::patch_handler_namespaced!(patch_role, Role, "roles", "rbac.authorization.k8s.io");
crate::patch_handler_namespaced!(
    patch_rolebinding,
    RoleBinding,
    "rolebindings",
    "rbac.authorization.k8s.io"
);
crate::patch_handler_cluster!(
    patch_clusterrole,
    ClusterRole,
    "clusterroles",
    "rbac.authorization.k8s.io"
);
crate::patch_handler_cluster!(
    patch_clusterrolebinding,
    ClusterRoleBinding,
    "clusterrolebindings",
    "rbac.authorization.k8s.io"
);
