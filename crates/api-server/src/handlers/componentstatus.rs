use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, State},
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::ComponentStatus,
    List, Result,
};
use std::sync::Arc;
use tracing::debug;

/// Get a specific component status
pub async fn get(
    State(_state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ComponentStatus>> {
    debug!("Getting componentstatus: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "componentstatuses")
        .with_api_group("")
        .with_name(&name);

    if let Decision::Deny(reason) = _state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // ComponentStatus is a special resource that checks the health of system components
    // For now, we'll return healthy status for known components
    let component_status = match name.as_str() {
        "scheduler" => ComponentStatus::healthy("scheduler"),
        "controller-manager" => ComponentStatus::healthy("controller-manager"),
        "etcd-0" | "etcd-1" | "etcd-2" => ComponentStatus::healthy(&name),
        _ => {
            return Err(rusternetes_common::Error::NotFound(format!(
                "Component {} not found",
                name
            )));
        }
    };

    Ok(Json(component_status))
}

/// List all component statuses
pub async fn list(
    State(_state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
) -> Result<Json<List<ComponentStatus>>> {
    debug!("Listing componentstatuses");

    // Check authorization
    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "componentstatuses").with_api_group("");

    if let Decision::Deny(reason) = _state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Return status for all known components
    let components = vec![
        ComponentStatus::healthy("scheduler"),
        ComponentStatus::healthy("controller-manager"),
        ComponentStatus::healthy("etcd-0"),
    ];

    let mut list = List::new("ComponentStatusList", "v1", components);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&_state.storage, &list.items).await);
    Ok(Json(list))
}
