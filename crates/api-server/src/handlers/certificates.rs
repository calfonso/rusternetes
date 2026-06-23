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
    resources::CertificateSigningRequest,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_certificate_signing_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut csr): DumpingJson<CertificateSigningRequest>,
) -> Result<(StatusCode, Json<CertificateSigningRequest>)> {
    info!("Creating CertificateSigningRequest: {}", csr.metadata.name);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    // Full create-time ValidateObjectMeta (#1087). CertificateSigningRequest is
    // cluster-scoped and imposes no name-format constraint
    // (ValidateCertificateRequestName returns nil).
    crate::handlers::validation::validate_create_object_meta(
        &csr.metadata,
        None,
        crate::handlers::validation::NameKind::NoConstraint,
    )?;

    // Field validation (mirrors upstream ValidateCertificateSigningRequestCreate).
    {
        let errs =
            rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_create(&csr);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Enrich metadata with system fields
    csr.metadata.ensure_uid();
    csr.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CertificateSigningRequest validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(csr)));
    }

    let key = build_key("certificatesigningrequests", None, &csr.metadata.name);
    let created = state.storage.create(&key, &csr).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_certificate_signing_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<CertificateSigningRequest>> {
    debug!("Getting CertificateSigningRequest: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("certificatesigningrequests", None, &name);
    let csr = state.storage.get(&key).await?;

    Ok(Json(csr))
}

pub async fn update_certificate_signing_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut csr): DumpingJson<CertificateSigningRequest>,
) -> Result<Json<CertificateSigningRequest>> {
    info!("Updating CertificateSigningRequest: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    csr.metadata.name = name.clone();
    csr.kind = "CertificateSigningRequest".to_string();
    csr.api_version = "certificates.k8s.io/v1".to_string();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CertificateSigningRequest validated successfully (not updated)");
        return Ok(Json(csr));
    }

    let key = build_key("certificatesigningrequests", None, &name);

    // Check resourceVersion for optimistic concurrency
    if let Ok(existing) = state.storage.get::<CertificateSigningRequest>(&key).await {
        crate::handlers::lifecycle::check_resource_version(
            existing.metadata.resource_version.as_deref(),
            csr.metadata.resource_version.as_deref(),
            &name,
        )?;
        // Preserve status if not provided
        if csr.status.is_none() {
            csr.status = existing.status.clone();
        }
        // Update validation (upstream ValidateCertificateSigningRequestUpdate):
        // re-validates spec/conditions/cert against the new object and forbids
        // removing existing Approved/Denied/Failed conditions, adding/modifying
        // Approved/Denied conditions, or mutating status.certificate via the
        // main resource endpoint.
        let errs = rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_update_main(&csr, &existing);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    let result = match state.storage.update(&key, &csr).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &csr).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_certificate_signing_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<CertificateSigningRequest>> {
    info!("Deleting CertificateSigningRequest: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("certificatesigningrequests", None, &name);

    // Get the resource for finalizer handling
    let resource: CertificateSigningRequest = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CertificateSigningRequest validated successfully (not deleted)");
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
        let updated: CertificateSigningRequest = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_certificate_signing_requests(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<CertificateSigningRequest>(
            state,
            auth_ctx,
            "certificatesigningrequests",
            "certificates.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing CertificateSigningRequests");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "certificatesigningrequests")
        .with_api_group("certificates.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let prefix = build_prefix("certificatesigningrequests", None);
    let mut items = state
        .storage
        .list::<CertificateSigningRequest>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "CertificateSigningRequestList",
        "certificates.k8s.io/v1",
        items,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Status subresource handlers
pub async fn get_certificate_signing_request_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<CertificateSigningRequest>> {
    debug!("Getting CertificateSigningRequest status: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "certificatesigningrequests/status")
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("certificatesigningrequests", None, &name);
    let csr = state.storage.get(&key).await?;

    Ok(Json(csr))
}

pub async fn update_certificate_signing_request_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    DumpingJson(updated_csr): DumpingJson<CertificateSigningRequest>,
) -> Result<Json<CertificateSigningRequest>> {
    update_csr_status_inner(
        State(state),
        Extension(auth_ctx),
        Path(name),
        updated_csr,
        false,
    )
    .await
}

/// Shared body for the `/status` and `/approval` subresource updates. `approval`
/// selects which upstream update validator runs:
/// `ValidateCertificateSigningRequestApprovalUpdate` (allows setting
/// Approved/Denied conditions) vs `…StatusUpdate` (allows setting the cert).
async fn update_csr_status_inner(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    updated_csr: CertificateSigningRequest,
    approval: bool,
) -> Result<Json<CertificateSigningRequest>> {
    info!("Updating CertificateSigningRequest status: {}", name);

    let subresource = if approval {
        "certificatesigningrequests/approval"
    } else {
        "certificatesigningrequests/status"
    };
    let attrs = RequestAttributes::new(auth_ctx.user, "update", subresource)
        .with_api_group("certificates.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("certificatesigningrequests", None, &name);

    // Get existing CSR; keep the pre-update copy for the update-diff rules.
    let mut existing_csr: CertificateSigningRequest = state.storage.get(&key).await?;
    let old_csr = existing_csr.clone();

    // Update status and metadata (K8s allows metadata changes via status subresource)
    existing_csr.status = updated_csr.status;

    // Update validation against the stored object — upstream
    // ValidateCertificateSigningRequest{Status,Approval}Update. Runs the
    // create-style field checks (conditions valid type/status, Approved/Denied
    // mutual exclusion, certificate PEM parse) plus the diff rules: existing
    // Approved/Denied/Failed conditions may not be removed; the /status path
    // may set (but not modify an existing) certificate; only the /approval path
    // may add/modify Approved/Denied conditions.
    let errs = if approval {
        rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_approval_update(&existing_csr, &old_csr)
    } else {
        rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_status_update(&existing_csr, &old_csr)
    };
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Merge annotations from the patch into existing (don't replace entirely)
    if let Some(new_annotations) = updated_csr.metadata.annotations {
        let annotations = existing_csr
            .metadata
            .annotations
            .get_or_insert_with(Default::default);
        for (k, v) in new_annotations {
            annotations.insert(k, v);
        }
    }
    // Merge labels from the patch into existing
    if let Some(new_labels) = updated_csr.metadata.labels {
        let labels = existing_csr
            .metadata
            .labels
            .get_or_insert_with(Default::default);
        for (k, v) in new_labels {
            labels.insert(k, v);
        }
    }

    let result = state.storage.update(&key, &existing_csr).await?;

    Ok(Json(result))
}

// Approval subresource — same status-merge body, but runs the approval update
// validator (may set Approved/Denied conditions; may not set the certificate).
pub async fn approve_certificate_signing_request(
    state: State<Arc<ApiServerState>>,
    auth_ctx: Extension<AuthContext>,
    name: Path<String>,
    DumpingJson(csr): DumpingJson<CertificateSigningRequest>,
) -> Result<Json<CertificateSigningRequest>> {
    update_csr_status_inner(state, auth_ctx, name, csr, true).await
}

crate::patch_handler_cluster!(
    patch_certificate_signing_request,
    CertificateSigningRequest,
    "certificatesigningrequests",
    "certificates.k8s.io"
);

pub async fn deletecollection_certificatesigningrequests(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection certificatesigningrequests with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "certificatesigningrequests",
    )
    .with_api_group("certificates.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CertificateSigningRequest collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all certificatesigningrequests
    let prefix = build_prefix("certificatesigningrequests", None);
    let mut items = state
        .storage
        .list::<CertificateSigningRequest>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("certificatesigningrequests", None, &item.metadata.name);

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
        "DeleteCollection completed: {} certificatesigningrequests deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
