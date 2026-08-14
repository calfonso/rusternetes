use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    auth::ServiceAccountClaims,
    authz::{Decision, RequestAttributes},
    resources::{Secret, ServiceAccount},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut service_account): Json<ServiceAccount>,
) -> Result<(StatusCode, Json<ServiceAccount>)> {
    info!(
        "Creating service account: {}/{}",
        namespace, service_account.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "serviceaccounts")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    service_account.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    service_account.metadata.ensure_uid();
    service_account.metadata.ensure_creation_timestamp();

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ServiceAccount {}/{} validated successfully (not created)",
            namespace, service_account.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(service_account)));
    }

    let key = build_key(
        "serviceaccounts",
        Some(&namespace),
        &service_account.metadata.name,
    );
    let created = state.storage.create(&key, &service_account).await?;
    info!("ServiceAccount stored successfully, preparing to create token Secret");

    // Generate ServiceAccount token and store it in a Secret
    let sa_uid = created.metadata.uid.clone();
    let sa_name = created.metadata.name.clone();
    info!("ServiceAccount UID: {}, Name: {}", sa_uid, sa_name);

    // Generate JWT token (valid for 10 years - Kubernetes default for static tokens)
    let claims = ServiceAccountClaims::new(
        sa_name.clone(),
        namespace.clone(),
        sa_uid.clone(),
        87600, // 10 years in hours
    );

    let token = state.token_manager.generate_token(claims)?;

    // Create Secret to store the token
    let secret_name = format!("{}-token", sa_name);
    let mut string_data = HashMap::new();
    string_data.insert("token".to_string(), token.clone());
    string_data.insert("namespace".to_string(), namespace.clone());

    // Add CA certificate if available
    if let Some(ref ca_cert) = state.ca_cert_pem {
        string_data.insert("ca.crt".to_string(), ca_cert.clone());
    }

    let mut secret =
        Secret::new(&secret_name, &namespace).with_type("kubernetes.io/service-account-token");

    // Add labels and annotations
    secret.metadata.labels = Some({
        let mut labels = HashMap::new();
        labels.insert(
            "kubernetes.io/service-account.name".to_string(),
            sa_name.clone(),
        );
        labels
    });
    secret.metadata.annotations = Some({
        let mut annotations = HashMap::new();
        annotations.insert(
            "kubernetes.io/service-account.uid".to_string(),
            sa_uid.clone(),
        );
        annotations
    });
    secret.string_data = Some(string_data);

    // Normalize: convert stringData to base64-encoded data before storing
    secret.normalize();

    // Store the secret
    let secret_key = build_key("secrets", Some(&namespace), &secret_name);
    info!(
        "Attempting to create ServiceAccount token secret: {}",
        secret_name
    );
    match state.storage.create(&secret_key, &secret).await {
        Ok(_) => {
            info!(
                "Successfully created ServiceAccount token secret: {}",
                secret_name
            );
        }
        Err(e) => {
            info!(
                "Warning: Failed to create ServiceAccount token secret {}: {}",
                secret_name, e
            );
            // Don't fail the ServiceAccount creation if secret creation fails
        }
    }

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ServiceAccount>> {
    debug!("Getting service account: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "serviceaccounts")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("serviceaccounts", Some(&namespace), &name);
    let service_account = state.storage.get(&key).await?;

    Ok(Json(service_account))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut service_account): Json<ServiceAccount>,
) -> Result<Json<ServiceAccount>> {
    info!("Updating service account: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "serviceaccounts")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    service_account.metadata.name = name.clone();
    service_account.metadata.namespace = Some(namespace.clone());

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ServiceAccount {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(service_account));
    }

    let key = build_key("serviceaccounts", Some(&namespace), &name);
    let updated = state.storage.update(&key, &service_account).await?;

    Ok(Json(updated))
}

pub async fn delete_service_account(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ServiceAccount>> {
    info!("Deleting service account: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "serviceaccounts")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("serviceaccounts", Some(&namespace), &name);

    // Get the resource to check if it exists
    let sa: ServiceAccount = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: ServiceAccount {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(sa));
    }

    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers(&*state.storage, &key, &sa)
            .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ServiceAccount = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(sa))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| v.parse::<bool>().ok())
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
        return crate::handlers::watch::watch_namespaced::<ServiceAccount>(
            state,
            auth_ctx,
            namespace,
            "serviceaccounts",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing service accounts in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "serviceaccounts")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("serviceaccounts", Some(&namespace));
    let mut service_accounts: Vec<ServiceAccount> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut service_accounts, &params)?;

    let list = List::new("ServiceAccountList", "v1", service_accounts);
    Ok(Json(list).into_response())
}

/// List all serviceaccounts across all namespaces
pub async fn list_all_serviceaccounts(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| v.parse::<bool>().ok())
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
        return crate::handlers::watch::watch_cluster_scoped::<ServiceAccount>(
            state,
            auth_ctx,
            "serviceaccounts",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all serviceaccounts");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "serviceaccounts").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("serviceaccounts", None);
    let mut service_accounts = state.storage.list::<ServiceAccount>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut service_accounts, &params)?;

    let list = List::new("ServiceAccountList", "v1", service_accounts);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, ServiceAccount, "serviceaccounts", "");

pub async fn deletecollection_serviceaccounts(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection serviceaccounts in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "serviceaccounts")
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
        info!("Dry-run: ServiceAccount collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all serviceaccounts in the namespace
    let prefix = build_prefix("serviceaccounts", Some(&namespace));
    let mut items = state.storage.list::<ServiceAccount>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("serviceaccounts", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} serviceaccounts deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

/// Create a token for a service account (TokenRequest API)
#[allow(dead_code)]
pub async fn create_token(
    State(state): State<Arc<ApiServerState>>,
    Extension(_auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>> {
    info!("Creating token for service account: {}/{}", namespace, name);

    // Verify the service account exists
    let key = build_key("serviceaccounts", Some(&namespace), &name);
    let _sa: ServiceAccount = state.storage.get(&key).await?;

    // Generate a token using the token manager
    let now = chrono::Utc::now();
    let claims = rusternetes_common::auth::ServiceAccountClaims {
        sub: format!("system:serviceaccount:{}:{}", namespace, name),
        iss: "https://kubernetes.default.svc.cluster.local".to_string(),
        namespace: namespace.clone(),
        uid: _sa.metadata.uid.clone(),
        iat: now.timestamp(),
        exp: (now + chrono::Duration::hours(1)).timestamp(),
        aud: vec!["https://kubernetes.default.svc".to_string()],
        kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
            namespace: namespace.clone(),
            svcacct: rusternetes_common::auth::KubeRef {
                name: name.clone(),
                uid: _sa.metadata.uid.clone(),
            },
            pod: None,
            node: None,
        }),
        pod_name: None,
        pod_uid: None,
        node_name: None,
        node_uid: None,
    };
    let token = state
        .token_manager
        .generate_token(claims)
        .unwrap_or_else(|_| format!("sa-token-{}-{}", namespace, name));

    // Build TokenRequest response
    let expiration_seconds = body
        .get("spec")
        .and_then(|s| s.get("expirationSeconds"))
        .and_then(|e| e.as_i64())
        .unwrap_or(3600);

    let expiration_time = chrono::Utc::now() + chrono::Duration::seconds(expiration_seconds);

    let response = serde_json::json!({
        "kind": "TokenRequest",
        "apiVersion": "authentication.k8s.io/v1",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "creationTimestamp": rusternetes_common::time::k8s_time::format(&chrono::Utc::now()),
        },
        "spec": body.get("spec").cloned().unwrap_or(serde_json::json!({})),
        "status": {
            "token": token,
            "expirationTimestamp": rusternetes_common::time::k8s_time::format(&expiration_time),
        }
    });

    Ok(Json(response))
}
