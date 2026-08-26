use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::HorizontalPodAutoscaler,
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
    Json(mut hpa): Json<HorizontalPodAutoscaler>,
) -> Result<(StatusCode, Json<HorizontalPodAutoscaler>)> {
    info!(
        "Creating horizontalpodautoscaler: {}/{}",
        namespace, hpa.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "horizontalpodautoscalers")
        .with_namespace(&namespace)
        .with_api_group("autoscaling");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    hpa.metadata.namespace = Some(namespace.clone());
    hpa.metadata.ensure_uid();
    hpa.metadata.ensure_creation_timestamp();

    let key = build_key(
        "horizontalpodautoscalers",
        Some(&namespace),
        &hpa.metadata.name,
    );

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: HorizontalPodAutoscaler {}/{} validated successfully (not created)",
            namespace, hpa.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(hpa)));
    }

    let created = state.storage.create(&key, &hpa).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<HorizontalPodAutoscaler>> {
    debug!("Getting horizontalpodautoscaler: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "horizontalpodautoscalers")
        .with_namespace(&namespace)
        .with_api_group("autoscaling")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("horizontalpodautoscalers", Some(&namespace), &name);
    let hpa = state.storage.get(&key).await?;

    Ok(Json(hpa))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut hpa): Json<HorizontalPodAutoscaler>,
) -> Result<Json<HorizontalPodAutoscaler>> {
    info!("Updating horizontalpodautoscaler: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "horizontalpodautoscalers")
        .with_namespace(&namespace)
        .with_api_group("autoscaling")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    hpa.metadata.name = name.clone();
    hpa.metadata.namespace = Some(namespace.clone());

    let key = build_key("horizontalpodautoscalers", Some(&namespace), &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: HorizontalPodAutoscaler {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(hpa));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &hpa).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &hpa).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<HorizontalPodAutoscaler>> {
    info!("Deleting horizontalpodautoscaler: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "horizontalpodautoscalers")
        .with_namespace(&namespace)
        .with_api_group("autoscaling")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("horizontalpodautoscalers", Some(&namespace), &name);

    // Get the HPA for finalizer handling
    let hpa: HorizontalPodAutoscaler = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: HorizontalPodAutoscaler {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(hpa));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &hpa)
            .await?;

    if deleted_immediately {
        Ok(Json(hpa))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: HorizontalPodAutoscaler = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<HorizontalPodAutoscaler>(
            state,
            auth_ctx,
            namespace,
            "horizontalpodautoscalers",
            "autoscaling",
            watch_params,
        )
        .await;
    }

    info!(
        "Listing horizontalpodautoscalers in namespace: {}",
        namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "horizontalpodautoscalers")
        .with_namespace(&namespace)
        .with_api_group("autoscaling");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("horizontalpodautoscalers", Some(&namespace));
    let mut hpas = state
        .storage
        .list::<HorizontalPodAutoscaler>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut hpas, &params)?;

    let list = List::new("HorizontalPodAutoscalerList", "autoscaling/v2", hpas);
    Ok(Json(list).into_response())
}

pub async fn list_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<HorizontalPodAutoscaler>(
            state,
            auth_ctx,
            "horizontalpodautoscalers",
            "autoscaling",
            watch_params,
        )
        .await;
    }

    debug!("Listing all horizontalpodautoscalers");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "horizontalpodautoscalers")
        .with_api_group("autoscaling");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("horizontalpodautoscalers", None);
    let mut hpas = state
        .storage
        .list::<HorizontalPodAutoscaler>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut hpas, &params)?;

    let list = List::new("HorizontalPodAutoscalerList", "autoscaling/v2", hpas);
    Ok(Json(list).into_response())
}

// Status subresource handlers
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<HorizontalPodAutoscaler>> {
    info!(
        "Getting horizontalpodautoscaler status: {}/{}",
        namespace, name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "horizontalpodautoscalers/status")
        .with_namespace(&namespace)
        .with_api_group("autoscaling")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("horizontalpodautoscalers", Some(&namespace), &name);
    let hpa = state.storage.get(&key).await?;

    Ok(Json(hpa))
}

pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Json(hpa): Json<HorizontalPodAutoscaler>,
) -> Result<Json<HorizontalPodAutoscaler>> {
    info!(
        "Updating horizontalpodautoscaler status: {}/{}",
        namespace, name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "horizontalpodautoscalers/status")
        .with_namespace(&namespace)
        .with_api_group("autoscaling")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("horizontalpodautoscalers", Some(&namespace), &name);

    // Get existing HPA to preserve spec
    let mut existing: HorizontalPodAutoscaler = state.storage.get(&key).await?;

    // Update only the status field
    existing.status = hpa.status;

    let updated = state.storage.update(&key, &existing).await?;

    Ok(Json(updated))
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(
    patch,
    HorizontalPodAutoscaler,
    "horizontalpodautoscalers",
    "autoscaling"
);

pub async fn deletecollection_horizontalpodautoscalers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection horizontalpodautoscalers in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "horizontalpodautoscalers",
    )
    .with_namespace(&namespace)
    .with_api_group("autoscaling");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: HorizontalPodAutoscaler collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all horizontalpodautoscalers in the namespace
    let prefix = build_prefix("horizontalpodautoscalers", Some(&namespace));
    let mut items = state
        .storage
        .list::<HorizontalPodAutoscaler>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key(
            "horizontalpodautoscalers",
            Some(&namespace),
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
        "DeleteCollection completed: {} horizontalpodautoscalers deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        CrossVersionObjectReference, HorizontalPodAutoscalerSpec, MetricSpec, MetricTarget,
        ResourceMetricSource,
    };

    #[test]
    fn test_hpa_handler_structure() {
        // Basic test to ensure handler structure is correct
        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "test".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(1),
            max_replicas: 10,
            metrics: Some(vec![MetricSpec {
                metric_type: "Resource".to_string(),
                resource: Some(ResourceMetricSource {
                    name: "cpu".to_string(),
                    target: MetricTarget {
                        target_type: "Utilization".to_string(),
                        value: None,
                        average_value: None,
                        average_utilization: Some(80),
                    },
                }),
                pods: None,
                object: None,
                external: None,
                container_resource: None,
            }]),
            behavior: None,
        };

        let hpa = HorizontalPodAutoscaler::new("test-hpa", "default", spec);
        assert_eq!(hpa.metadata.name, "test-hpa");
        assert_eq!(hpa.metadata.namespace, Some("default".to_string()));
    }
}
