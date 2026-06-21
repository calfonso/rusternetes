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
    resources::IngressClass,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_ingressclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut ingress_class): DumpingJson<IngressClass>,
) -> Result<(StatusCode, Json<IngressClass>)> {
    info!("Creating IngressClass: {}", ingress_class.metadata.name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "ingressclasses")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &ingress_class.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Validate spec (upstream networking ValidateIngressClass): controller
    // domain-prefixed path + parameters reference scope/namespace coupling.
    let errs = rusternetes_common::validation::ingressclass::validate_ingress_class(&ingress_class);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Enrich metadata with system fields
    ingress_class.metadata.ensure_uid();
    ingress_class.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: IngressClass validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(ingress_class)));
    }

    let key = build_key("ingressclasses", None, &ingress_class.metadata.name);
    let created = state.storage.create(&key, &ingress_class).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_ingressclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<IngressClass>> {
    debug!("Getting IngressClass: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "ingressclasses")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("ingressclasses", None, &name);
    let ingress_class = state.storage.get(&key).await?;

    Ok(Json(ingress_class))
}

pub async fn update_ingressclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut ingress_class): DumpingJson<IngressClass>,
) -> Result<Json<IngressClass>> {
    info!("Updating IngressClass: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "ingressclasses")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    ingress_class.metadata.name = name.clone();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: IngressClass validated successfully (not updated)");
        return Ok(Json(ingress_class));
    }

    let key = build_key("ingressclasses", None, &name);

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &ingress_class).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &ingress_class).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_ingressclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<IngressClass>> {
    info!("Deleting IngressClass: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "ingressclasses")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("ingressclasses", None, &name);

    // Get the resource for finalizer handling
    let ingress_class: IngressClass = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: IngressClass validated successfully (not deleted)");
        return Ok(Json(ingress_class));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &ingress_class,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(ingress_class))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: IngressClass = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_ingressclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<IngressClass>(
            state,
            auth_ctx,
            "ingressclasses",
            "networking.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing IngressClasses");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "ingressclasses")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("ingressclasses", None);
    let mut ingress_classes: Vec<IngressClass> = state.storage.list(&prefix).await?;
    crate::handlers::filtering::apply_selectors(&mut ingress_classes, &params)?;

    let mut list = List::new("IngressClassList", "networking.k8s.io/v1", ingress_classes);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler for cluster-scoped IngressClass
crate::patch_handler_cluster!(
    patch_ingressclass,
    IngressClass,
    "ingressclasses",
    "networking.k8s.io"
);

pub async fn deletecollection_ingressclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection ingressclasses with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "ingressclasses")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: IngressClass collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all ingressclasses
    let prefix = build_prefix("ingressclasses", None);
    let mut items = state.storage.list::<IngressClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("ingressclasses", None, &item.metadata.name);

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
        "DeleteCollection completed: {} ingressclasses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
