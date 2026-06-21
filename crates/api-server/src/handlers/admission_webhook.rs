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
    resources::{MutatingWebhookConfiguration, ValidatingWebhookConfiguration},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

// ===== ValidatingWebhookConfiguration Handlers =====

pub async fn create_validating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut config): DumpingJson<ValidatingWebhookConfiguration>,
) -> Result<(StatusCode, Json<ValidatingWebhookConfiguration>)> {
    info!(
        "Creating ValidatingWebhookConfiguration: {}",
        config.metadata.name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &config.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Field validation (mirrors upstream ValidateValidatingWebhookConfiguration).
    {
        let errs = rusternetes_common::validation::webhookconfiguration::validate_validating_webhook_configuration(&config);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Validate matchConditions CEL expressions with type-checking
    if let Some(webhooks) = &config.webhooks {
        for webhook in webhooks {
            if let Some(conditions) = &webhook.match_conditions {
                for (i, condition) in conditions.iter().enumerate() {
                    if condition.expression.is_empty() {
                        return Err(rusternetes_common::Error::InvalidResource(
                            "matchConditions[].expression must be non-empty".to_string(),
                        ));
                    }
                    if condition.name.is_empty() {
                        return Err(rusternetes_common::Error::InvalidResource(format!(
                            "matchConditions[{}].name must be non-empty",
                            i
                        )));
                    }
                    // Compile the expression — catch panics from antlr4rust parser
                    // which panics on certain invalid expressions instead of returning Err
                    let expr_clone = condition.expression.clone();
                    let compile_result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            cel::Program::compile(&expr_clone)
                        }));
                    let program = match compile_result {
                        Ok(Ok(p)) => p,
                        Ok(Err(e)) => {
                            let err_str = format!("{}", e);
                            let err_lower = err_str.to_lowercase();
                            // Allow "no such key" errors — the CEL library does
                            // type-checking at compile time and rejects references
                            // to undeclared variables like `object.metadata`. These
                            // are valid K8s matchCondition expressions that will work
                            // at runtime when the actual object is provided.
                            if err_lower.contains("no such key")
                                || err_lower.contains("not found")
                                || err_lower.contains("undeclared")
                                || err_lower.contains("undefined")
                            {
                                continue;
                            }
                            return Err(rusternetes_common::Error::InvalidResource(format!(
                                "matchConditions[{}].expression: compilation failed: {}",
                                i, e
                            )));
                        }
                        Err(_panic) => {
                            return Err(rusternetes_common::Error::InvalidResource(format!(
                                "matchConditions[{}].expression: compilation failed: invalid CEL expression '{}'",
                                i, condition.expression
                            )));
                        }
                    };

                    // The cel crate's parser accepts some invalid expressions
                    // that K8s rejects. Try executing with an empty context as
                    // an additional validation step — genuinely invalid syntax
                    // will fail at execution even with no variables.
                    let test_ctx = cel::Context::default();
                    let exec_result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            program.execute(&test_ctx)
                        }));
                    match exec_result {
                        Ok(Ok(_)) => {} // Valid expression
                        Ok(Err(e)) => {
                            let err_str = format!("{}", e);
                            let err_lower = err_str.to_lowercase();
                            // Allow runtime errors from missing variables — those
                            // are expected since we don't provide the real context
                            if err_lower.contains("no such key")
                                || err_lower.contains("not found")
                                || err_lower.contains("undeclared")
                                || err_lower.contains("undefined")
                                || err_lower.contains("no matching overload")
                            {
                                // Valid expression, just missing runtime variables
                            } else {
                                return Err(rusternetes_common::Error::InvalidResource(format!(
                                    "matchConditions[{}].expression: compilation failed: {}",
                                    i, e
                                )));
                            }
                        }
                        Err(_panic) => {
                            return Err(rusternetes_common::Error::InvalidResource(format!(
                                "matchConditions[{}].expression: compilation failed: invalid CEL expression '{}'",
                                i, condition.expression
                            )));
                        }
                    }
                }
            }
        }
    }

    // Enrich metadata with system fields
    config.metadata.ensure_uid();
    config.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingWebhookConfiguration validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(config)));
    }

    let key = build_key(
        "validatingwebhookconfigurations",
        None,
        &config.metadata.name,
    );
    let created = state.storage.create(&key, &config).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_validating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ValidatingWebhookConfiguration>> {
    debug!("Getting ValidatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("validatingwebhookconfigurations", None, &name);
    let config = state.storage.get(&key).await?;

    Ok(Json(config))
}

pub async fn update_validating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut config): DumpingJson<ValidatingWebhookConfiguration>,
) -> Result<Json<ValidatingWebhookConfiguration>> {
    info!("Updating ValidatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    config.metadata.name = name.clone();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingWebhookConfiguration validated successfully (not updated)");
        return Ok(Json(config));
    }

    let key = build_key("validatingwebhookconfigurations", None, &name);

    let result = match state.storage.update(&key, &config).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &config).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_validating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ValidatingWebhookConfiguration>> {
    info!("Deleting ValidatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("validatingwebhookconfigurations", None, &name);

    // Get the resource for finalizer handling
    let resource: ValidatingWebhookConfiguration = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ValidatingWebhookConfiguration validated successfully (not deleted)");
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
        let updated: ValidatingWebhookConfiguration = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_validating_webhooks(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ValidatingWebhookConfiguration>(
            state,
            auth_ctx,
            "validatingwebhookconfigurations",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ValidatingWebhookConfigurations");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "validatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("validatingwebhookconfigurations", None);
    let mut configs = state
        .storage
        .list::<ValidatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configs, &params)?;

    let mut list = List::new(
        "ValidatingWebhookConfigurationList",
        "admissionregistration.k8s.io/v1",
        configs,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_validating_webhook,
    ValidatingWebhookConfiguration,
    "validatingwebhookconfigurations",
    "admissionregistration.k8s.io"
);

// ===== MutatingWebhookConfiguration Handlers =====

pub async fn create_mutating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut config): DumpingJson<MutatingWebhookConfiguration>,
) -> Result<(StatusCode, Json<MutatingWebhookConfiguration>)> {
    info!(
        "Creating MutatingWebhookConfiguration: {}",
        config.metadata.name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &config.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Field validation (mirrors upstream ValidateMutatingWebhookConfiguration).
    {
        let errs = rusternetes_common::validation::webhookconfiguration::validate_mutating_webhook_configuration(&config);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Validate matchConditions CEL expressions with type-checking
    if let Some(webhooks) = &config.webhooks {
        for webhook in webhooks {
            if let Some(conditions) = &webhook.match_conditions {
                for (i, condition) in conditions.iter().enumerate() {
                    if condition.expression.is_empty() {
                        return Err(rusternetes_common::Error::InvalidResource(
                            "matchConditions[].expression must be non-empty".to_string(),
                        ));
                    }
                    if condition.name.is_empty() {
                        return Err(rusternetes_common::Error::InvalidResource(format!(
                            "matchConditions[{}].name must be non-empty",
                            i
                        )));
                    }
                    let expr_clone = condition.expression.clone();
                    let compile_result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            cel::Program::compile(&expr_clone)
                        }));
                    let program = match compile_result {
                        Ok(Ok(p)) => p,
                        Ok(Err(e)) => {
                            let err_str = format!("{}", e);
                            let err_lower = err_str.to_lowercase();
                            if err_lower.contains("no such key")
                                || err_lower.contains("not found")
                                || err_lower.contains("undeclared")
                                || err_lower.contains("undefined")
                            {
                                continue;
                            }
                            return Err(rusternetes_common::Error::InvalidResource(format!(
                                "matchConditions[{}].expression: compilation failed: {}",
                                i, e
                            )));
                        }
                        Err(_panic) => {
                            return Err(rusternetes_common::Error::InvalidResource(format!(
                                "matchConditions[{}].expression: compilation failed: invalid CEL expression '{}'",
                                i, condition.expression
                            )));
                        }
                    };

                    // The cel crate's parser accepts some invalid expressions.
                    // Try executing with empty context as validation.
                    let test_ctx = cel::Context::default();
                    let exec_result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            program.execute(&test_ctx)
                        }));
                    match exec_result {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            let err_str = format!("{}", e);
                            let err_lower = err_str.to_lowercase();
                            if !err_lower.contains("no such key")
                                && !err_lower.contains("not found")
                                && !err_lower.contains("undeclared")
                                && !err_lower.contains("undefined")
                                && !err_lower.contains("no matching overload")
                            {
                                return Err(rusternetes_common::Error::InvalidResource(format!(
                                    "matchConditions[{}].expression: compilation failed: {}",
                                    i, e
                                )));
                            }
                        }
                        Err(_panic) => {
                            return Err(rusternetes_common::Error::InvalidResource(format!(
                                "matchConditions[{}].expression: compilation failed: invalid CEL expression '{}'",
                                i, condition.expression
                            )));
                        }
                    }
                }
            }
        }
    }

    // Enrich metadata with system fields
    config.metadata.ensure_uid();
    config.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: MutatingWebhookConfiguration validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(config)));
    }

    let key = build_key("mutatingwebhookconfigurations", None, &config.metadata.name);
    let created = state.storage.create(&key, &config).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_mutating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<MutatingWebhookConfiguration>> {
    debug!("Getting MutatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("mutatingwebhookconfigurations", None, &name);
    let config = state.storage.get(&key).await?;

    Ok(Json(config))
}

pub async fn update_mutating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut config): DumpingJson<MutatingWebhookConfiguration>,
) -> Result<Json<MutatingWebhookConfiguration>> {
    info!("Updating MutatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    config.metadata.name = name.clone();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: MutatingWebhookConfiguration validated successfully (not updated)");
        return Ok(Json(config));
    }

    let key = build_key("mutatingwebhookconfigurations", None, &name);

    let result = match state.storage.update(&key, &config).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &config).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_mutating_webhook(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<MutatingWebhookConfiguration>> {
    info!("Deleting MutatingWebhookConfiguration: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("mutatingwebhookconfigurations", None, &name);

    // Get the resource for finalizer handling
    let resource: MutatingWebhookConfiguration = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: MutatingWebhookConfiguration validated successfully (not deleted)");
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
        let updated: MutatingWebhookConfiguration = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_mutating_webhooks(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<MutatingWebhookConfiguration>(
            state,
            auth_ctx,
            "mutatingwebhookconfigurations",
            "admissionregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing MutatingWebhookConfigurations");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "mutatingwebhookconfigurations")
        .with_api_group("admissionregistration.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("mutatingwebhookconfigurations", None);
    let mut configs = state
        .storage
        .list::<MutatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configs, &params)?;

    let mut list = List::new(
        "MutatingWebhookConfigurationList",
        "admissionregistration.k8s.io/v1",
        configs,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_mutating_webhook,
    MutatingWebhookConfiguration,
    "mutatingwebhookconfigurations",
    "admissionregistration.k8s.io"
);

pub async fn deletecollection_validatingwebhookconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection validatingwebhookconfigurations with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "validatingwebhookconfigurations",
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
        info!("Dry-run: ValidatingWebhookConfiguration collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all validatingwebhookconfigurations
    let prefix = build_prefix("validatingwebhookconfigurations", None);
    let mut items = state
        .storage
        .list::<ValidatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("validatingwebhookconfigurations", None, &item.metadata.name);

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
        "DeleteCollection completed: {} validatingwebhookconfigurations deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_mutatingwebhookconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection mutatingwebhookconfigurations with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "mutatingwebhookconfigurations",
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
        info!("Dry-run: MutatingWebhookConfiguration collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all mutatingwebhookconfigurations
    let prefix = build_prefix("mutatingwebhookconfigurations", None);
    let mut items = state
        .storage
        .list::<MutatingWebhookConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("mutatingwebhookconfigurations", None, &item.metadata.name);

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
        "DeleteCollection completed: {} mutatingwebhookconfigurations deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
