use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{Event, EventList},
    Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// List all events in a namespace
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
        return crate::handlers::watch::watch_namespaced::<Event>(
            state,
            auth_ctx,
            namespace,
            "events",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing events in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "events")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("events", Some(&namespace));
    let mut events: Vec<Event> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut events, &params)?;

    Ok(Json(EventList {
        api_version: "v1".to_string(),
        kind: "EventList".to_string(),
        metadata: rusternetes_common::types::ListMeta::default(),
        items: events,
    })
    .into_response())
}

/// List all events across all namespaces
pub async fn list_all(
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
        return crate::handlers::watch::watch_cluster_scoped::<Event>(
            state,
            auth_ctx,
            "events",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all events across all namespaces");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "events").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("events", None);
    let mut events: Vec<Event> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut events, &params)?;

    Ok(Json(EventList {
        api_version: "v1".to_string(),
        kind: "EventList".to_string(),
        metadata: rusternetes_common::types::ListMeta::default(),
        items: events,
    })
    .into_response())
}

/// Get a specific event
pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Event>> {
    debug!("Getting event: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "events")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("events", Some(&namespace), &name);
    let event: Event = state.storage.get(&key).await?;

    Ok(Json(event))
}

/// Create a new event
pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut event): Json<Event>,
) -> Result<(StatusCode, Json<Event>)> {
    info!("Creating event in namespace: {}", namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "events")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace matches
    event.metadata.namespace = Some(namespace.clone());

    // Generate UID if not present
    event.metadata.ensure_uid();

    // Generate name if not present
    if event.metadata.name.is_empty() {
        let name = Event::generate_name(&event.involved_object, &event.reason);
        event.metadata.name = name;
    }

    // Ensure creation timestamp is set
    event.metadata.ensure_creation_timestamp();

    let key = build_key("events", Some(&namespace), &event.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Event {}/{} validated successfully (not created)",
            namespace, event.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(event)));
    }

    let created = state.storage.create(&key, &event).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

/// Update an existing event
pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut event): Json<Event>,
) -> Result<Json<Event>> {
    info!("Updating event: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "events")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace and name match
    event.metadata.namespace = Some(namespace.clone());
    event.metadata.name = name.clone();

    let key = build_key("events", Some(&namespace), &name);

    // Preserve creation_timestamp from the existing resource — the client
    // may send a truncated version (Go's time.Time loses nanosecond precision
    // during JSON round-trip in some cases).
    if let Ok(existing) = state.storage.get::<Event>(&key).await {
        if existing.metadata.creation_timestamp.is_some() {
            event.metadata.creation_timestamp = existing.metadata.creation_timestamp;
        }
        // Also preserve UID — must not change on update
        if !existing.metadata.uid.is_empty() {
            event.metadata.uid = existing.metadata.uid;
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Event {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(event));
    }

    let updated = state.storage.update(&key, &event).await?;

    Ok(Json(updated))
}

/// Delete an event
pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Event>> {
    info!("Deleting event: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "events")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("events", Some(&namespace), &name);

    // Get the event for finalizer handling
    let event: Event = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Event {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(event));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &event)
            .await?;

    if deleted_immediately {
        Ok(Json(event))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Event = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::EventType;
    use rusternetes_common::resources::ObjectReference;

    #[test]
    fn test_event_creation() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref,
            "PodStarted".to_string(),
            "Pod started successfully".to_string(),
            EventType::Normal,
        );

        assert_eq!(event.metadata.name, "test-event".to_string());
        assert_eq!(event.metadata.namespace, Some("default".to_string()));
        assert_eq!(event.reason, "PodStarted");
        assert_eq!(event.count, 1);
    }

    #[test]
    fn test_event_field_selector_filtering() {
        // Simulate what list_all does: filter events using apply_selectors
        let mut events = vec![
            Event::new(
                "event-1".to_string(),
                "default".to_string(),
                ObjectReference {
                    kind: Some("Pod".to_string()),
                    namespace: Some("default".to_string()),
                    name: Some("pod-a".to_string()),
                    uid: Some("uid1".to_string()),
                    api_version: Some("v1".to_string()),
                    resource_version: None,
                    field_path: None,
                },
                "Started".to_string(),
                "Pod started".to_string(),
                EventType::Normal,
            ),
            Event::new(
                "event-2".to_string(),
                "kube-system".to_string(),
                ObjectReference {
                    kind: Some("Pod".to_string()),
                    namespace: Some("kube-system".to_string()),
                    name: Some("pod-b".to_string()),
                    uid: Some("uid2".to_string()),
                    api_version: Some("v1".to_string()),
                    resource_version: None,
                    field_path: None,
                },
                "Failed".to_string(),
                "Pod failed".to_string(),
                EventType::Warning,
            ),
        ];

        // Filter by involvedObject.name
        let mut params = HashMap::new();
        params.insert(
            "fieldSelector".to_string(),
            "involvedObject.name=pod-a".to_string(),
        );
        crate::handlers::filtering::apply_selectors(&mut events, &params).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].metadata.name, "event-1");
        assert_eq!(events[0].involved_object.name, Some("pod-a".to_string()));
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, Event, "events", "");

pub async fn deletecollection_events(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection events in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "events")
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
        info!("Dry-run: Event collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all events in the namespace
    let prefix = build_prefix("events", Some(&namespace));
    let mut items = state.storage.list::<Event>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("events", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} events deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

// ---- events.k8s.io/v1 API group handlers ----
// These return apiVersion "events.k8s.io/v1" instead of "v1"
// and use api_group "events.k8s.io" for authorization.

/// List events via events.k8s.io/v1 (namespaced)
pub async fn list_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
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
        return crate::handlers::watch::watch_namespaced::<Event>(
            state,
            auth_ctx,
            namespace,
            "events",
            "events.k8s.io",
            watch_params,
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("events", Some(&namespace));
    let mut events: Vec<Event> = state.storage.list(&prefix).await?;
    crate::handlers::filtering::apply_selectors(&mut events, &params)?;

    // Set apiVersion to events.k8s.io/v1 for each event
    for event in &mut events {
        event.api_version = "events.k8s.io/v1".to_string();
    }

    Ok(Json(EventList {
        api_version: "events.k8s.io/v1".to_string(),
        kind: "EventList".to_string(),
        metadata: rusternetes_common::types::ListMeta::default(),
        items: events,
    })
    .into_response())
}

/// List all events via events.k8s.io/v1 (cluster-scoped)
pub async fn list_all_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
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
        return crate::handlers::watch::watch_cluster_scoped::<Event>(
            state,
            auth_ctx,
            "events",
            "events.k8s.io",
            watch_params,
        )
        .await;
    }

    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "events").with_api_group("events.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("events", None);
    let mut events: Vec<Event> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut events, &params)?;

    for event in &mut events {
        event.api_version = "events.k8s.io/v1".to_string();
    }

    Ok(Json(EventList {
        api_version: "events.k8s.io/v1".to_string(),
        kind: "EventList".to_string(),
        metadata: rusternetes_common::types::ListMeta::default(),
        items: events,
    })
    .into_response())
}

/// Get a single event via events.k8s.io/v1
pub async fn get_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Event>> {
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("events", Some(&namespace), &name);
    let mut event: Event = state.storage.get(&key).await?;
    event.api_version = "events.k8s.io/v1".to_string();

    Ok(Json(event))
}

/// Create an event via events.k8s.io/v1
pub async fn create_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut event): Json<Event>,
) -> Result<(StatusCode, Json<Event>)> {
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    event.metadata.namespace = Some(namespace.clone());
    event.metadata.ensure_uid();
    event.api_version = "events.k8s.io/v1".to_string();

    // Map events.k8s.io/v1 fields to core/v1 equivalents for field selector compatibility.
    // reportingComponent → source.component, note → message, regarding → involvedObject
    if event.source.component.is_empty() {
        if let Some(ref rc) = event.reporting_component {
            event.source.component = rc.clone();
        }
    }
    if event.message.is_empty() {
        if let Some(ref note) = event.note {
            event.message = note.clone();
        }
    }
    if event
        .involved_object
        .name
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        if let Some(ref regarding) = event.regarding {
            event.involved_object = regarding.clone();
        }
    }

    if event.metadata.name.is_empty() {
        let name = Event::generate_name(&event.involved_object, &event.reason);
        event.metadata.name = name;
    }

    event.metadata.ensure_creation_timestamp();

    let key = build_key("events", Some(&namespace), &event.metadata.name);

    if is_dry_run {
        return Ok((StatusCode::CREATED, Json(event)));
    }

    let mut created: Event = state.storage.create(&key, &event).await?;
    created.api_version = "events.k8s.io/v1".to_string();

    Ok((StatusCode::CREATED, Json(created)))
}

/// Update an event via events.k8s.io/v1
pub async fn update_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut event): Json<Event>,
) -> Result<Json<Event>> {
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    event.metadata.namespace = Some(namespace.clone());
    event.metadata.name = name.clone();
    event.api_version = "events.k8s.io/v1".to_string();

    let key = build_key("events", Some(&namespace), &name);

    // Preserve creation_timestamp and UID from existing resource — the client
    // may send a truncated version (Go's time.Time loses nanosecond precision
    // during JSON round-trip in some cases).
    if let Ok(existing) = state.storage.get::<Event>(&key).await {
        if existing.metadata.creation_timestamp.is_some() {
            event.metadata.creation_timestamp = existing.metadata.creation_timestamp;
        }
        // Also preserve UID — must not change on update
        if !existing.metadata.uid.is_empty() {
            event.metadata.uid = existing.metadata.uid;
        }
    }

    // Map events.k8s.io/v1 fields to core/v1 equivalents
    if event.source.component.is_empty() {
        if let Some(ref rc) = event.reporting_component {
            event.source.component = rc.clone();
        }
    }
    if event.message.is_empty() {
        if let Some(ref note) = event.note {
            event.message = note.clone();
        }
    }
    if event
        .involved_object
        .name
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        if let Some(ref regarding) = event.regarding {
            event.involved_object = regarding.clone();
        }
    }

    if is_dry_run {
        return Ok(Json(event));
    }

    let mut updated: Event = state.storage.update(&key, &event).await?;
    updated.api_version = "events.k8s.io/v1".to_string();

    Ok(Json(updated))
}

/// Delete an event via events.k8s.io/v1
pub async fn delete_events_v1(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Event>> {
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "events")
        .with_namespace(&namespace)
        .with_api_group("events.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("events", Some(&namespace), &name);
    let mut event: Event = state.storage.get(&key).await?;
    event.api_version = "events.k8s.io/v1".to_string();

    if is_dry_run {
        return Ok(Json(event));
    }

    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &event)
            .await?;

    if deleted_immediately {
        Ok(Json(event))
    } else {
        let mut updated: Event = state.storage.get(&key).await?;
        updated.api_version = "events.k8s.io/v1".to_string();
        Ok(Json(updated))
    }
}

// PATCH handler for events.k8s.io/v1
crate::patch_handler_namespaced!(patch_events_v1, Event, "events", "events.k8s.io");
