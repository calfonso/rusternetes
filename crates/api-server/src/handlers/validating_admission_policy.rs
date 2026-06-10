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
    resources::{ValidatingAdmissionPolicy, ValidatingAdmissionPolicyBinding},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

// ===== ValidatingAdmissionPolicy Handlers =====

pub async fn create_validating_admission_policy(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut policy): DumpingJson<ValidatingAdmissionPolicy>,
) -> Result<(StatusCode, Json<ValidatingAdmissionPolicy>)> {
    info!(
        "Creating ValidatingAdmissionPolicy: {}",
        policy.metadata.name
    );

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::require_object_name(&policy.metadata)?;

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "validatingadmissionpolicies")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Enrich metadata with system fields
    policy.metadata.ensure_uid();
    policy.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingAdmissionPolicy validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(policy)));
    }

    let key = build_key("validatingadmissionpolicies", None, &policy.metadata.name);
    let created = state.storage.create(&key, &policy).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_validating_admission_policy(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ValidatingAdmissionPolicy>> {
    debug!("Getting ValidatingAdmissionPolicy: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "validatingadmissionpolicies")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("validatingadmissionpolicies", None, &name);
    let policy = state.storage.get(&key).await?;

    Ok(Json(policy))
}

pub async fn update_validating_admission_policy(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut policy): DumpingJson<ValidatingAdmissionPolicy>,
) -> Result<Json<ValidatingAdmissionPolicy>> {
    info!("Updating ValidatingAdmissionPolicy: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "validatingadmissionpolicies")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    policy.metadata.name = name.clone();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingAdmissionPolicy validated successfully (not updated)");
        return Ok(Json(policy));
    }

    let key = build_key("validatingadmissionpolicies", None, &name);

    let result = match state.storage.update(&key, &policy).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &policy).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_validating_admission_policy(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ValidatingAdmissionPolicy>> {
    info!("Deleting ValidatingAdmissionPolicy: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "validatingadmissionpolicies")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("validatingadmissionpolicies", None, &name);

    // Get the resource for finalizer handling
    let resource: ValidatingAdmissionPolicy = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingAdmissionPolicy validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &resource,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(resource))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ValidatingAdmissionPolicy = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_validating_admission_policies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ValidatingAdmissionPolicy>(
            state,
            auth_ctx,
            "validatingadmissionpolicies",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ValidatingAdmissionPolicies");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingadmissionpolicies")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("validatingadmissionpolicies", None);
    let mut policies = state
        .storage
        .list::<ValidatingAdmissionPolicy>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut policies, &params)?;

    let mut list = List::new(
        "ValidatingAdmissionPolicyList",
        "admissionregistration.k8s.io/v1",
        policies,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_validating_admission_policy,
    ValidatingAdmissionPolicy,
    "validatingadmissionpolicies",
    "admissionregistration.k8s.io"
);

// ===== ValidatingAdmissionPolicyBinding Handlers =====

pub async fn create_validating_admission_policy_binding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut binding): DumpingJson<ValidatingAdmissionPolicyBinding>,
) -> Result<(StatusCode, Json<ValidatingAdmissionPolicyBinding>)> {
    info!(
        "Creating ValidatingAdmissionPolicyBinding: {}",
        binding.metadata.name
    );

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::require_object_name(&binding.metadata)?;

    // Check authorization
    let attrs =
        RequestAttributes::new(auth_ctx.user, "create", "validatingadmissionpolicybindings")
            .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Enrich metadata with system fields
    binding.metadata.ensure_uid();
    binding.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingAdmissionPolicyBinding validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(binding)));
    }

    let key = build_key(
        "validatingadmissionpolicybindings",
        None,
        &binding.metadata.name,
    );
    let created = state.storage.create(&key, &binding).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_validating_admission_policy_binding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ValidatingAdmissionPolicyBinding>> {
    debug!("Getting ValidatingAdmissionPolicyBinding: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "validatingadmissionpolicybindings")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("validatingadmissionpolicybindings", None, &name);
    let binding = state.storage.get(&key).await?;

    Ok(Json(binding))
}

pub async fn update_validating_admission_policy_binding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut binding): DumpingJson<ValidatingAdmissionPolicyBinding>,
) -> Result<Json<ValidatingAdmissionPolicyBinding>> {
    info!("Updating ValidatingAdmissionPolicyBinding: {}", name);

    // Check authorization
    let attrs =
        RequestAttributes::new(auth_ctx.user, "update", "validatingadmissionpolicybindings")
            .with_api_group("admissionregistration.k8s.io")
            .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    binding.metadata.name = name.clone();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingAdmissionPolicyBinding validated successfully (not updated)");
        return Ok(Json(binding));
    }

    let key = build_key("validatingadmissionpolicybindings", None, &name);

    let result = match state.storage.update(&key, &binding).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &binding).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_validating_admission_policy_binding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ValidatingAdmissionPolicyBinding>> {
    info!("Deleting ValidatingAdmissionPolicyBinding: {}", name);

    // Check authorization
    let attrs =
        RequestAttributes::new(auth_ctx.user, "delete", "validatingadmissionpolicybindings")
            .with_api_group("admissionregistration.k8s.io")
            .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("validatingadmissionpolicybindings", None, &name);

    // Get the resource for finalizer handling
    let resource: ValidatingAdmissionPolicyBinding = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingAdmissionPolicyBinding validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &resource,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(resource))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ValidatingAdmissionPolicyBinding = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_validating_admission_policy_bindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ValidatingAdmissionPolicyBinding>(
            state,
            auth_ctx,
            "validatingadmissionpolicybindings",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ValidatingAdmissionPolicyBindings");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingadmissionpolicybindings")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("validatingadmissionpolicybindings", None);
    let mut bindings = state
        .storage
        .list::<ValidatingAdmissionPolicyBinding>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut bindings, &params)?;

    let mut list = List::new(
        "ValidatingAdmissionPolicyBindingList",
        "admissionregistration.k8s.io/v1",
        bindings,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_validating_admission_policy_binding,
    ValidatingAdmissionPolicyBinding,
    "validatingadmissionpolicybindings",
    "admissionregistration.k8s.io"
);

pub async fn deletecollection_validatingadmissionpolicies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection validatingadmissionpolicies with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "validatingadmissionpolicies",
    )
    .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingAdmissionPolicy collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all validatingadmissionpolicies
    let prefix = build_prefix("validatingadmissionpolicies", None);
    let mut items = state
        .storage
        .list::<ValidatingAdmissionPolicy>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("validatingadmissionpolicies", None, &item.metadata.name);

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
        "DeleteCollection completed: {} validatingadmissionpolicies deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_validatingadmissionpolicybindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection validatingadmissionpolicybindings with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "validatingadmissionpolicybindings",
    )
    .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!(
            "Dry-run: ValidatingAdmissionPolicyBinding collection would be deleted (not deleted)"
        );
        return Ok(StatusCode::OK);
    }

    // Get all validatingadmissionpolicybindings
    let prefix = build_prefix("validatingadmissionpolicybindings", None);
    let mut items = state
        .storage
        .list::<ValidatingAdmissionPolicyBinding>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key(
            "validatingadmissionpolicybindings",
            None,
            &item.metadata.name,
        );

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
        "DeleteCollection completed: {} validatingadmissionpolicybindings deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
