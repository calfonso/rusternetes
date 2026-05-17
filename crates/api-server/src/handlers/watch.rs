#![allow(dead_code)]

use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use futures::StreamExt;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    types::ObjectMeta,
    Error, Result,
};
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, timeout};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info};

/// Kubernetes watch event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WatchEventType {
    Added,
    Modified,
    Deleted,
    Bookmark,
    Error,
}

/// Kubernetes watch event wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sWatchEvent<T> {
    #[serde(rename = "type")]
    pub event_type: WatchEventType,
    pub object: T,
}

/// Query parameters for watch requests
#[derive(Debug, Deserialize)]
pub struct WatchParams {
    /// Resource version to watch from
    #[serde(
        rename = "resourceVersion",
        deserialize_with = "deserialize_empty_string_as_none",
        default
    )]
    pub resource_version: Option<String>,

    /// Timeout in seconds
    #[serde(rename = "timeoutSeconds")]
    pub timeout_seconds: Option<u64>,

    /// Label selector
    #[serde(rename = "labelSelector")]
    pub label_selector: Option<String>,

    /// Field selector
    #[serde(rename = "fieldSelector")]
    pub field_selector: Option<String>,

    /// Watch for changes
    pub watch: Option<bool>,

    /// Allow watch bookmarks
    #[serde(rename = "allowWatchBookmarks")]
    pub allow_watch_bookmarks: Option<bool>,

    /// Send initial events (consistent reads from cache, K8s 1.30+)
    /// When true, send all existing resources as ADDED events followed by
    /// a BOOKMARK to signal initial list is complete.
    #[serde(rename = "sendInitialEvents")]
    pub send_initial_events: Option<bool>,
}

/// Deserialize empty strings as None for resourceVersion
fn deserialize_empty_string_as_none<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt.filter(|s| !s.is_empty()))
}

/// Normalize a resourceVersion value: treat empty string as None (= "start from current")
pub fn normalize_resource_version(rv: Option<String>) -> Option<String> {
    rv.filter(|s| !s.is_empty())
}

/// Check if a query param map indicates a watch request
pub fn is_watch_request(params: &std::collections::HashMap<String, String>) -> bool {
    params
        .get("watch")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
}

/// Convert query parameters to WatchParams
pub fn watch_params_from_query(params: &std::collections::HashMap<String, String>) -> WatchParams {
    WatchParams {
        resource_version: normalize_resource_version(params.get("resourceVersion").cloned()),
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
    }
}

/// Generic watch handler for namespaced resources
pub async fn watch_namespaced<T>(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    namespace: String,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
) -> Result<Response>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Clone + HasMetadata,
{
    info!(
        "Starting watch for {} in namespace {} (timeout: {:?}s, bookmarks: {})",
        resource_type,
        namespace,
        params.timeout_seconds,
        params.allow_watch_bookmarks.unwrap_or(false)
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "watch", resource_type)
        .with_namespace(&namespace)
        .with_api_group(api_group);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Create watch stream via the shared watch cache (one etcd watch per prefix)
    let prefix = build_prefix(resource_type, Some(&namespace));

    // Extract parameters
    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    let send_initial_events = params.send_initial_events.unwrap_or(false);
    // Watch timeout: honor client-requested timeout, capped at 10 minutes.
    // K8s default --min-request-timeout is 1800s (30 min). Conformance tests
    // request 350-572s timeouts. Capping below the client's request causes
    // "context canceled" errors when the server closes the watch early.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go
    let timeout_duration = Some(Duration::from_secs(
        params.timeout_seconds.unwrap_or(600).min(600),
    ));
    let label_selector = params.label_selector.clone();
    let field_selector = params.field_selector.clone();
    let requested_rv = params.resource_version.clone();
    let (bookmark_kind, bookmark_api_version) =
        resource_type_to_kind_and_version(resource_type, api_group);

    // Determine if we have a specific non-zero resourceVersion to replay from.
    // rv=0 and rv=1 are treated as "list current state" — don't replay from etcd
    // history because early revisions may have been compacted.
    // Also filter out timestamp-based RVs (> 1 billion) which would cause etcd errors.
    let current_rev = state.storage.current_revision().await.unwrap_or(1);
    let replay_revision = requested_rv
        .as_deref()
        .filter(|rv| !rv.is_empty() && *rv != "0" && *rv != "1")
        .and_then(|rv| rv.parse::<i64>().ok())
        .filter(|&rv| rv > 0 && rv <= current_rev + 1000);

    // Subscribe to watch events.
    // If a specific resourceVersion was given, use etcd's watch_from_revision
    // directly to replay ALL events since that revision from etcd's history.
    // This is more reliable than the in-memory history buffer which only
    // captures events while a watcher was active.
    let watch_stream = if let Some(since_rev) = replay_revision {
        // Use etcd watch_from_revision for reliable history replay.
        // Add 1 because etcd start_revision is inclusive and we want events AFTER since_rev.
        match state
            .storage
            .watch_from_revision(&prefix, since_rev + 1)
            .await
        {
            Ok(stream) => {
                debug!(
                    "Started etcd watch from revision {} for prefix {}",
                    since_rev + 1,
                    prefix
                );
                stream
            }
            Err(e) => {
                error!(
                    "Failed to create watch from revision {}: {}, falling back to cache",
                    since_rev, e
                );
                let (history, rx) = state.watch_cache.subscribe_from(&prefix, since_rev).await;
                crate::watch_cache::broadcast_to_stream_with_history(history, rx)
            }
        }
    } else {
        let rx = state.watch_cache.subscribe(&prefix).await;
        crate::watch_cache::broadcast_to_stream(rx)
    };

    // List existing resources to send as initial ADDED events
    let existing_resources = state.storage.list::<T>(&prefix).await?;

    // Get the current revision from storage for bookmark fallback.
    // This prevents sending bookmark RV "0" which confuses client-go.
    let current_rev = state.storage.current_revision().await.unwrap_or(1);
    let current_rev_str = current_rev.to_string();

    // Create channel for sending events to client.
    // Buffer must be large enough to hold initial events + bookmarks without
    // blocking the pre-buffer loop (which uses try_send). 256 is enough for
    // most namespaces while keeping memory usage reasonable. Real events in
    // the background task use send().await to guarantee delivery.
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<String, std::io::Error>>(256);

    // Determine whether to send initial ADDED events:
    // - If sendInitialEvents=true: always send
    // - If resourceVersion is "0", "1", or absent: send initial events
    // - If resourceVersion is a specific value (> 1): skip initial events (etcd watch replay handles it)
    let should_send_initial = send_initial_events
        || requested_rv.as_deref() == Some("0")
        || requested_rv.as_deref() == Some("1")
        || requested_rv.is_none();

    // PRE-BUFFER initial events BEFORE returning the Response.
    // K8s sends headers + first events synchronously (watch.go:237-282).
    // If we return Response with empty Body, client-go times out waiting
    // for first DATA frame → "context canceled" (1777 failures in round 137).
    // By pre-populating the channel, Hyper has data available immediately
    // when it first polls the Body stream.
    let mut initial_latest_rv: Option<String> = None;
    if should_send_initial {
        for object in &existing_resources {
            if let Some(rv) = object.metadata().resource_version.as_ref() {
                initial_latest_rv = Some(rv.clone());
            }
            if !matches_label_selector(object.metadata(), &label_selector)
                || !matches_field_selector(object.metadata(), &field_selector)
            {
                continue;
            }
            let k8s_event = K8sWatchEvent {
                event_type: WatchEventType::Added,
                object: object.clone(),
            };
            if let Ok(json) = serde_json::to_string(&k8s_event) {
                let _ = tx.try_send(Ok(format!("{}\n", json)));
            }
        }
    }

    // Send initial-events-end bookmark if sendInitialEvents was requested
    if send_initial_events {
        let rv = initial_latest_rv
            .clone()
            .unwrap_or_else(|| current_rev_str.clone());
        let mut annotations = std::collections::HashMap::new();
        annotations.insert("k8s.io/initial-events-end".to_string(), "true".to_string());
        let bookmark = BookmarkObject {
            kind: Some(bookmark_kind.clone()),
            api_version: Some(bookmark_api_version.clone()),
            metadata: ObjectMeta {
                resource_version: Some(rv.clone()),
                annotations: Some(annotations),
                ..Default::default()
            },
        };
        let k8s_event = K8sWatchEvent {
            event_type: WatchEventType::Bookmark,
            object: bookmark,
        };
        if let Ok(json) = serde_json::to_string(&k8s_event) {
            let _ = tx.try_send(Ok(format!("{}\n", json)));
        }
    }

    // If no initial events were sent, send an immediate bookmark so the
    // client sees data right away and doesn't timeout waiting for the
    // first HTTP/2 DATA frame.
    // ONLY send when allowWatchBookmarks is true — clients that don't
    // request bookmarks treat them as unexpected events and fail.
    if (!should_send_initial || existing_resources.is_empty()) && allow_bookmarks {
        let rv = initial_latest_rv
            .clone()
            .or_else(|| requested_rv.clone())
            .unwrap_or_else(|| current_rev_str.clone());
        let bookmark = BookmarkObject {
            kind: Some(bookmark_kind.clone()),
            api_version: Some(bookmark_api_version.clone()),
            metadata: ObjectMeta {
                resource_version: Some(rv),
                ..Default::default()
            },
        };
        let k8s_event = K8sWatchEvent {
            event_type: WatchEventType::Bookmark,
            object: bookmark,
        };
        if let Ok(json) = serde_json::to_string(&k8s_event) {
            let _ = tx.try_send(Ok(format!("{}\n", json)));
        }
    }

    // Spawn background task for ongoing watch events (etcd watch stream).
    // Initial events are already in the channel — this task handles
    // subsequent ADDED/MODIFIED/DELETED events and periodic bookmarks.
    tokio::spawn(async move {
        // Initial events already sent to channel before spawn.
        // Track the latest resourceVersion for bookmarks.
        let mut latest_resource_version: Option<String> = {
            let base_rv = initial_latest_rv.or_else(|| {
                requested_rv
                    .as_deref()
                    .and_then(|rv| rv.parse::<i64>().ok())
                    .map(|rv| rv.to_string())
            });
            match base_rv {
                Some(rv) => {
                    if let Ok(rv_i64) = rv.parse::<i64>() {
                        Some(rv_i64.max(current_rev).to_string())
                    } else {
                        Some(current_rev.to_string())
                    }
                }
                None => Some(current_rev.to_string()),
            }
        };

        // Initial events and bookmarks already pre-buffered.
        {}

        // Always send periodic bookmarks as keep-alive to prevent the K8s client
        // from closing the watch connection due to inactivity
        // K8s flushes after every event when the channel buffer is empty
        // (watch.go:275). More frequent bookmarks act as keepalives that
        // prevent client-go from timing out on idle connections.
        let mut bookmark_interval = Some(interval(Duration::from_secs(1)));

        // Box-pin the watch stream so it can be replaced on reconnect
        let mut watch_stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = rusternetes_common::Result<WatchEvent>> + Send>,
        > = Box::pin(watch_stream);

        // Track objects DELETED from watch due to label selector mismatch.
        let mut deleted_from_watch: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Watch loop with timeout support
        let watch_future = async {
            loop {
                tokio::select! {
                    // Process watch events
                    event_opt = watch_stream.next() => {
                        match event_opt {
                            Some(Ok(WatchEvent::Added(key, value))) => {
                                info!("Watch ADDED event for key={}, should_send_initial={}", key, should_send_initial);
                                if let Ok(object) = serde_json::from_str::<T>(&value) {
                                    // Update latest resourceVersion
                                    if let Some(rv) = object.metadata().resource_version.as_ref() {
                                        latest_resource_version = Some(rv.clone());
                                    }

                                    // Filter by label and field selectors
                                    if !matches_label_selector(object.metadata(), &label_selector)
                                        || !matches_field_selector(object.metadata(), &field_selector)
                                    {
                                        continue;
                                    }

                                    let k8s_event = K8sWatchEvent {
                                        event_type: WatchEventType::Added,
                                        object,
                                    };
                                    if let Ok(json) = serde_json::to_string(&k8s_event) {
                                        // Use send().await to guarantee delivery. With
                                        // rhino/SQLite the poll loop has up to 1s latency,
                                        // so events can arrive in bursts. try_send() would
                                        // drop events when the channel is temporarily full
                                        // (e.g. HTTP/2 back-pressure), permanently losing
                                        // watch notifications. send() waits for channel
                                        // space or returns Err if the receiver is dropped.
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            debug!("Watch: tx.send failed, client disconnected");
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(Ok(WatchEvent::Modified(key, value))) => {
                                info!("Watch MODIFIED event for key={}", key);
                                if let Ok(object) = serde_json::from_str::<T>(&value) {
                                    // Update latest resourceVersion
                                    if let Some(rv) = object.metadata().resource_version.as_ref() {
                                        latest_resource_version = Some(rv.clone());
                                    }

                                    if !matches_field_selector(object.metadata(), &field_selector)
                                    {
                                        continue;
                                    }

                                    // For label-filtered watches, MODIFIED events need special handling:
                                    // - If labels NOW match but didn't before → synthetic ADDED
                                    // - If labels DON'T match but did before → synthetic DELETED
                                    // K8s watch semantics for label selectors:
                                    // - If labels no longer match → DELETED
                                    // - If labels now match but didn't before → ADDED
                                    // - If labels match and matched before → MODIFIED
                                    let matches_labels = matches_label_selector(object.metadata(), &label_selector);
                                    let obj_key = object.metadata().name.clone();
                                    let event_type = if label_selector.is_some() && !matches_labels {
                                        deleted_from_watch.insert(obj_key);
                                        WatchEventType::Deleted
                                    } else if label_selector.is_some() && deleted_from_watch.remove(&obj_key) {
                                        WatchEventType::Added
                                    } else {
                                        WatchEventType::Modified
                                    };

                                    let k8s_event = K8sWatchEvent {
                                        event_type,
                                        object,
                                    };
                                    if let Ok(json) = serde_json::to_string(&k8s_event) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            debug!("Watch: tx.send failed, client disconnected");
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(Ok(WatchEvent::Deleted(key, prev_value))) => {
                                debug!("Watch event - Deleted: {}", key);
                                // For DELETE events, Kubernetes requires the full object with metadata.
                                // Try typed deserialization first; fall back to raw JSON if it fails.
                                // prev_kv can be empty after etcd compaction or when the storage
                                // backend doesn't capture the previous value. Silently dropping
                                // the DELETE event causes watchers to hang (conformance #4).
                                if let Ok(object) = serde_json::from_str::<T>(&prev_value) {
                                    // Update latest resourceVersion
                                    if let Some(rv) = object.metadata().resource_version.as_ref() {
                                        latest_resource_version = Some(rv.clone());
                                    }

                                    // Filter DELETED events by both label and field selector.
                                    // Only send DELETED to watchers whose selector matches the
                                    // deleted object — otherwise watchers receive spurious deletes
                                    // for objects they never saw as ADDED.
                                    if !matches_label_selector(object.metadata(), &label_selector)
                                        || !matches_field_selector(object.metadata(), &field_selector)
                                    {
                                        continue;
                                    }

                                    // Remove from deleted_from_watch tracking since the object
                                    // is truly gone now
                                    let obj_key = object.metadata().name.clone();
                                    deleted_from_watch.remove(&obj_key);

                                    let k8s_event = K8sWatchEvent {
                                        event_type: WatchEventType::Deleted,
                                        object,
                                    };
                                    if let Ok(json) = serde_json::to_string(&k8s_event) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            debug!("Watch: tx.send failed, client disconnected");
                                            break;
                                        }
                                    }
                                } else {
                                    // Typed deserialization failed — use raw JSON fallback.
                                    debug!("Watch: typed deser failed for DELETE key={}, using raw fallback", key);
                                    if let Some(rv) = extract_rv_from_json(&prev_value) {
                                        latest_resource_version = Some(rv);
                                    }
                                    if let Some(json) = build_delete_fallback_json(&key, &prev_value) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            debug!("Watch: tx.send failed, client disconnected");
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                // Empty watch responses and transient errors are normal —
                                // etcd sends keep-alive responses with no events. Don't break.
                                debug!("Watch stream transient error (continuing): {}", e);
                                continue;
                            }
                            None => {
                                // Watch stream ended — resubscribe from cache.
                                // Small delay to prevent tight loop if channel keeps closing.
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                let new_rx = state.watch_cache.subscribe(&prefix).await;
                                watch_stream = Box::pin(crate::watch_cache::broadcast_to_stream(new_rx));
                                continue;
                            }
                        }
                    }
                    // Send periodic bookmarks if enabled
                    _ = async {
                        if let Some(ref mut interval) = bookmark_interval {
                            interval.tick().await;
                        } else {
                            // If bookmarks are disabled, park this branch forever
                            futures::future::pending::<()>().await
                        }
                    } => {
                        if allow_bookmarks || send_initial_events {
                            if let Some(ref rv) = latest_resource_version {
                                debug!("Sending bookmark with resourceVersion: {}", rv);
                                let bookmark = BookmarkObject {
                                    kind: Some(bookmark_kind.clone()),
                                    api_version: Some(bookmark_api_version.clone()),
                                    metadata: ObjectMeta {
                                        resource_version: Some(rv.clone()),
                                        ..Default::default()
                                    },
                                };
                                let k8s_event = K8sWatchEvent {
                                    event_type: WatchEventType::Bookmark,
                                    object: bookmark,
                                };
                                if let Ok(json) = serde_json::to_string(&k8s_event) {
                                    let _ = tx.try_send(Ok(format!("{}\n", json)));
                                    // Don't break on bookmark send failure — the client
                                    // might have reset just the bookmark stream but the
                                    // watch connection is still alive.
                                }
                            }
                        }
                    }
                }
            }
        };

        // Apply timeout if specified
        if let Some(timeout_dur) = timeout_duration {
            match timeout(timeout_dur, watch_future).await {
                Ok(_) => {
                    debug!("Watch stream completed normally");
                }
                Err(_) => {
                    info!("Watch stream timeout after {:?}", timeout_dur);
                    // Send final bookmark before closing if bookmarks are enabled
                    if allow_bookmarks || send_initial_events {
                        if let Some(ref rv) = latest_resource_version {
                            let bookmark = BookmarkObject {
                                kind: Some(bookmark_kind.clone()),
                                api_version: Some(bookmark_api_version.clone()),
                                metadata: ObjectMeta {
                                    resource_version: Some(rv.clone()),
                                    ..Default::default()
                                },
                            };
                            let k8s_event = K8sWatchEvent {
                                event_type: WatchEventType::Bookmark,
                                object: bookmark,
                            };
                            if let Ok(json) = serde_json::to_string(&k8s_event) {
                                let _ = tx.try_send(Ok(format!("{}\n", json)));
                            }
                        }
                    }
                }
            }
        } else {
            // No timeout, run forever
            watch_future.await;
        }
    });

    // Convert receiver to stream
    let stream = ReceiverStream::new(rx);

    // Build response with proper headers for streaming.
    // Note: Do NOT set Connection header — it's prohibited in HTTP/2
    // and can cause client-go to drop watch connections.
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-cache, private")
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(Body::from_stream(stream))
        .map_err(|e| Error::Internal(format!("Failed to build response: {}", e)))?;

    Ok(response)
}

/// Generic watch handler for cluster-scoped resources
pub async fn watch_cluster_scoped<T>(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
) -> Result<Response>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Clone + HasMetadata,
{
    info!(
        "Starting watch for cluster-scoped {} (timeout: {:?}s, bookmarks: {})",
        resource_type,
        params.timeout_seconds,
        params.allow_watch_bookmarks.unwrap_or(false)
    );
    info!(
        "  Watch params: rv={:?}, sendInitialEvents={:?}, labelSelector={:?}",
        params.resource_version, params.send_initial_events, params.label_selector
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "watch", resource_type)
        .with_api_group(api_group);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Create watch stream via the shared watch cache
    let prefix = build_prefix(resource_type, None);

    // Extract parameters
    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    let send_initial_events = params.send_initial_events.unwrap_or(false);
    // Watch timeout: honor client-requested timeout, capped at 10 minutes.
    // K8s default --min-request-timeout is 1800s (30 min). Conformance tests
    // request 350-572s timeouts. Capping below the client's request causes
    // "context canceled" errors when the server closes the watch early.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go
    let timeout_duration = Some(Duration::from_secs(
        params.timeout_seconds.unwrap_or(600).min(600),
    ));
    let label_selector = params.label_selector.clone();
    let field_selector = params.field_selector.clone();
    let requested_rv = params.resource_version.clone();
    let (bookmark_kind, bookmark_api_version) =
        resource_type_to_kind_and_version(resource_type, api_group);

    // Determine if we have a specific non-zero resourceVersion to replay from.
    // rv=0 and rv=1 are treated as "list current state" — don't replay from etcd
    // history because early revisions may have been compacted.
    // Also filter out timestamp-based RVs (> 1 billion) which would cause etcd errors.
    let current_rev = state.storage.current_revision().await.unwrap_or(1);
    let replay_revision = requested_rv
        .as_deref()
        .filter(|rv| !rv.is_empty() && *rv != "0" && *rv != "1")
        .and_then(|rv| rv.parse::<i64>().ok())
        .filter(|&rv| rv > 0 && rv <= current_rev + 1000);

    // Subscribe to watch events.
    // If a specific resourceVersion was given, use etcd's watch_from_revision
    // directly to replay ALL events since that revision from etcd's history.
    let watch_stream = if let Some(since_rev) = replay_revision {
        match state
            .storage
            .watch_from_revision(&prefix, since_rev + 1)
            .await
        {
            Ok(stream) => {
                debug!(
                    "Started etcd watch from revision {} for prefix {}",
                    since_rev + 1,
                    prefix
                );
                stream
            }
            Err(e) => {
                error!(
                    "Failed to create watch from revision {}: {}, falling back to cache",
                    since_rev, e
                );
                let (history, rx) = state.watch_cache.subscribe_from(&prefix, since_rev).await;
                crate::watch_cache::broadcast_to_stream_with_history(history, rx)
            }
        }
    } else {
        let rx = state.watch_cache.subscribe(&prefix).await;
        crate::watch_cache::broadcast_to_stream(rx)
    };

    // List existing resources to send as initial ADDED events
    let existing_resources = state.storage.list::<T>(&prefix).await?;

    // Get the current revision from storage for bookmark fallback.
    let current_rev = state.storage.current_revision().await.unwrap_or(1);
    let current_rev_str = current_rev.to_string();

    // Create channel for sending events to client.
    // Buffer must be large enough to hold initial events without blocking
    // the spawned task. The task uses send().await for real events to
    // guarantee delivery under HTTP/2 back-pressure.
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<String, std::io::Error>>(256);

    // Determine whether to send initial ADDED events
    let should_send_initial =
        send_initial_events || requested_rv.as_deref() == Some("0") || requested_rv.is_none();

    // Spawn task to convert watch events to HTTP response
    tokio::spawn(async move {
        // Track the latest resourceVersion for bookmarks.
        // Initialize to MAX of current revision and requested RV so bookmarks
        // never report a lower RV than what the client already knows.
        let mut latest_resource_version: Option<String> = {
            let rv = requested_rv
                .as_deref()
                .and_then(|rv| rv.parse::<i64>().ok())
                .unwrap_or(0)
                .max(current_rev);
            Some(rv.to_string())
        };

        // Send initial state as ADDED events (only when appropriate)
        if should_send_initial {
            for object in &existing_resources {
                // Update latest resourceVersion
                if let Some(rv) = object.metadata().resource_version.as_ref() {
                    latest_resource_version = Some(rv.clone());
                }

                // Filter by label and field selectors
                if !matches_label_selector(object.metadata(), &label_selector)
                    || !matches_field_selector(object.metadata(), &field_selector)
                {
                    continue;
                }

                let k8s_event = K8sWatchEvent {
                    event_type: WatchEventType::Added,
                    object,
                };
                if let Ok(json) = serde_json::to_string(&k8s_event) {
                    // Use send().await to guarantee delivery. try_send() caused
                    // initial events to be silently dropped when the channel was
                    // full (before Hyper starts draining), which then caused the
                    // task to exit and all subsequent events to be lost.
                    if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                        return; // Client disconnected
                    }
                }
            }
        } // end should_send_initial

        // If no initial events were sent and client supports bookmarks,
        // send an immediate bookmark so the client sees data right away.
        // Only send when allow_bookmarks is true — clients that don't
        // request bookmarks treat them as unexpected events and fail.
        if (!should_send_initial || existing_resources.is_empty()) && allow_bookmarks {
            let rv = latest_resource_version
                .clone()
                .or_else(|| requested_rv.clone())
                .unwrap_or_else(|| current_rev_str.clone());
            let bookmark = BookmarkObject {
                kind: Some(bookmark_kind.clone()),
                api_version: Some(bookmark_api_version.clone()),
                metadata: ObjectMeta {
                    resource_version: Some(rv),
                    ..Default::default()
                },
            };
            let k8s_event = K8sWatchEvent {
                event_type: WatchEventType::Bookmark,
                object: bookmark,
            };
            if let Ok(json) = serde_json::to_string(&k8s_event) {
                let _ = tx.try_send(Ok(format!("{}\n", json)));
            }
        }

        // When sendInitialEvents=true, send an initial BOOKMARK after the ADDED
        // events to signal "initial list is complete". The bookmark must have the
        // annotation "k8s.io/initial-events-end": "true" — client-go checks for
        // this specific annotation to know initial sync is done.
        if send_initial_events {
            // MUST send initial-events-end bookmark — client hangs without it.
            // Use latest resourceVersion from initial resources, or "0" as fallback.
            let rv = latest_resource_version
                .clone()
                .unwrap_or_else(|| "1".to_string());
            let mut annotations = std::collections::HashMap::new();
            annotations.insert("k8s.io/initial-events-end".to_string(), "true".to_string());
            let bookmark = BookmarkObject {
                kind: Some(bookmark_kind.clone()),
                api_version: Some(bookmark_api_version.clone()),
                metadata: ObjectMeta {
                    resource_version: Some(rv.clone()),
                    annotations: Some(annotations),
                    ..Default::default()
                },
            };
            let k8s_event = K8sWatchEvent {
                event_type: WatchEventType::Bookmark,
                object: bookmark,
            };
            if let Ok(json) = serde_json::to_string(&k8s_event) {
                let _ = tx.try_send(Ok(format!("{}\n", json)));
            }
            debug!(
                "Sent initial-events-end bookmark with resourceVersion: {}",
                rv
            );
            // Ensure latest_resource_version is set so periodic bookmarks work
            if latest_resource_version.is_none() {
                latest_resource_version = Some(rv);
            }
        }

        // Always send periodic bookmarks as keep-alive to prevent the K8s client
        // from closing the watch connection due to inactivity
        // K8s flushes after every event when the channel buffer is empty
        // (watch.go:275). More frequent bookmarks act as keepalives that
        // prevent client-go from timing out on idle connections.
        let mut bookmark_interval = Some(interval(Duration::from_secs(1)));

        // Box-pin the watch stream so it can be replaced on reconnect
        let mut watch_stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = rusternetes_common::Result<WatchEvent>> + Send>,
        > = Box::pin(watch_stream);

        // Track objects DELETED from watch due to label selector mismatch.
        let mut deleted_from_watch: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Watch loop with timeout support
        let watch_future = async {
            loop {
                tokio::select! {
                    // Process watch events
                    event_opt = watch_stream.next() => {
                        match event_opt {
                            Some(Ok(WatchEvent::Added(key, value))) => {
                                info!("Watch ADDED event for key={}, should_send_initial={}", key, should_send_initial);
                                if let Ok(object) = serde_json::from_str::<T>(&value) {
                                    // Update latest resourceVersion
                                    if let Some(rv) = object.metadata().resource_version.as_ref() {
                                        latest_resource_version = Some(rv.clone());
                                    }

                                    // Filter by label and field selectors
                                    if !matches_label_selector(object.metadata(), &label_selector)
                                        || !matches_field_selector(object.metadata(), &field_selector)
                                    {
                                        continue;
                                    }

                                    let k8s_event = K8sWatchEvent {
                                        event_type: WatchEventType::Added,
                                        object,
                                    };
                                    if let Ok(json) = serde_json::to_string(&k8s_event) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            debug!("Watch: tx.send failed, client disconnected");
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(Ok(WatchEvent::Modified(key, value))) => {
                                info!("Watch MODIFIED event for key={}", key);
                                if let Ok(object) = serde_json::from_str::<T>(&value) {
                                    // Update latest resourceVersion
                                    if let Some(rv) = object.metadata().resource_version.as_ref() {
                                        latest_resource_version = Some(rv.clone());
                                    }

                                    if !matches_field_selector(object.metadata(), &field_selector)
                                    {
                                        continue;
                                    }

                                    // For label-filtered watches, MODIFIED events need special handling:
                                    // - If labels NOW match but didn't before → synthetic ADDED
                                    // - If labels DON'T match but did before → synthetic DELETED
                                    // K8s watch semantics for label selectors:
                                    // - If labels no longer match → DELETED
                                    // - If labels now match but didn't before → ADDED
                                    // - If labels match and matched before → MODIFIED
                                    let matches_labels = matches_label_selector(object.metadata(), &label_selector);
                                    let obj_key = object.metadata().name.clone();
                                    let event_type = if label_selector.is_some() && !matches_labels {
                                        deleted_from_watch.insert(obj_key);
                                        WatchEventType::Deleted
                                    } else if label_selector.is_some() && deleted_from_watch.remove(&obj_key) {
                                        WatchEventType::Added
                                    } else {
                                        WatchEventType::Modified
                                    };

                                    let k8s_event = K8sWatchEvent {
                                        event_type,
                                        object,
                                    };
                                    if let Ok(json) = serde_json::to_string(&k8s_event) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            debug!("Watch: tx.send failed, client disconnected");
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(Ok(WatchEvent::Deleted(key, prev_value))) => {
                                debug!("Watch event - Deleted: {}", key);
                                // For DELETE events, Kubernetes requires the full object with metadata.
                                // Try typed deserialization first; fall back to raw JSON if it fails.
                                // prev_kv can be empty after etcd compaction or when the storage
                                // backend doesn't capture the previous value. Silently dropping
                                // the DELETE event causes watchers to hang (conformance #4).
                                if let Ok(object) = serde_json::from_str::<T>(&prev_value) {
                                    // Update latest resourceVersion
                                    if let Some(rv) = object.metadata().resource_version.as_ref() {
                                        latest_resource_version = Some(rv.clone());
                                    }

                                    // Filter DELETED events by both label and field selector.
                                    // Only send DELETED to watchers whose selector matches the
                                    // deleted object — otherwise watchers receive spurious deletes
                                    // for objects they never saw as ADDED.
                                    if !matches_label_selector(object.metadata(), &label_selector)
                                        || !matches_field_selector(object.metadata(), &field_selector)
                                    {
                                        continue;
                                    }

                                    // Remove from deleted_from_watch tracking since the object
                                    // is truly gone now
                                    let obj_key = object.metadata().name.clone();
                                    deleted_from_watch.remove(&obj_key);

                                    let k8s_event = K8sWatchEvent {
                                        event_type: WatchEventType::Deleted,
                                        object,
                                    };
                                    if let Ok(json) = serde_json::to_string(&k8s_event) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            debug!("Watch: tx.send failed, client disconnected");
                                            break;
                                        }
                                    }
                                } else {
                                    // Typed deserialization failed — use raw JSON fallback.
                                    debug!("Watch: typed deser failed for DELETE key={}, using raw fallback", key);
                                    if let Some(rv) = extract_rv_from_json(&prev_value) {
                                        latest_resource_version = Some(rv);
                                    }
                                    if let Some(json) = build_delete_fallback_json(&key, &prev_value) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            debug!("Watch: tx.send failed, client disconnected");
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                // Empty watch responses and transient errors are normal —
                                // etcd sends keep-alive responses with no events. Don't break.
                                debug!("Watch stream transient error (continuing): {}", e);
                                continue;
                            }
                            None => {
                                // Watch stream ended — resubscribe from cache.
                                // Small delay to prevent tight loop if channel keeps closing.
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                let new_rx = state.watch_cache.subscribe(&prefix).await;
                                watch_stream = Box::pin(crate::watch_cache::broadcast_to_stream(new_rx));
                                continue;
                            }
                        }
                    }
                    // Send periodic bookmarks if enabled
                    _ = async {
                        if let Some(ref mut interval) = bookmark_interval {
                            interval.tick().await;
                        } else {
                            // If bookmarks are disabled, park this branch forever
                            futures::future::pending::<()>().await
                        }
                    } => {
                        if allow_bookmarks || send_initial_events {
                            if let Some(ref rv) = latest_resource_version {
                                debug!("Sending bookmark with resourceVersion: {}", rv);
                                let bookmark = BookmarkObject {
                                    kind: Some(bookmark_kind.clone()),
                                    api_version: Some(bookmark_api_version.clone()),
                                    metadata: ObjectMeta {
                                        resource_version: Some(rv.clone()),
                                        ..Default::default()
                                    },
                                };
                                let k8s_event = K8sWatchEvent {
                                    event_type: WatchEventType::Bookmark,
                                    object: bookmark,
                                };
                                if let Ok(json) = serde_json::to_string(&k8s_event) {
                                    let _ = tx.try_send(Ok(format!("{}\n", json)));
                                    // Don't break on bookmark send failure — the client
                                    // might have reset just the bookmark stream but the
                                    // watch connection is still alive.
                                }
                            }
                        }
                    }
                }
            }
        };

        // Apply timeout if specified
        if let Some(timeout_dur) = timeout_duration {
            match timeout(timeout_dur, watch_future).await {
                Ok(_) => {
                    debug!("Watch stream completed normally");
                }
                Err(_) => {
                    info!("Watch stream timeout after {:?}", timeout_dur);
                    // Send final bookmark before closing if bookmarks are enabled
                    if allow_bookmarks || send_initial_events {
                        if let Some(ref rv) = latest_resource_version {
                            let bookmark = BookmarkObject {
                                kind: Some(bookmark_kind.clone()),
                                api_version: Some(bookmark_api_version.clone()),
                                metadata: ObjectMeta {
                                    resource_version: Some(rv.clone()),
                                    ..Default::default()
                                },
                            };
                            let k8s_event = K8sWatchEvent {
                                event_type: WatchEventType::Bookmark,
                                object: bookmark,
                            };
                            if let Ok(json) = serde_json::to_string(&k8s_event) {
                                let _ = tx.try_send(Ok(format!("{}\n", json)));
                            }
                        }
                    }
                }
            }
        } else {
            // No timeout, run forever
            watch_future.await;
        }
    });

    // Convert receiver to stream
    let stream = ReceiverStream::new(rx);

    // Build response with proper headers for streaming.
    // Note: Do NOT set Connection header — it's prohibited in HTTP/2
    // and can cause client-go to drop watch connections.
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-cache, private")
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(Body::from_stream(stream))
        .map_err(|e| Error::Internal(format!("Failed to build response: {}", e)))?;

    Ok(response)
}

/// Check if an object matches a label selector
fn matches_label_selector(metadata: &ObjectMeta, selector: &Option<String>) -> bool {
    let selector = match selector {
        Some(s) if !s.is_empty() => s,
        _ => return true, // No selector = match all
    };

    let labels = match &metadata.labels {
        Some(l) => l,
        None => return false, // No labels but selector exists = no match
    };

    // Parse selector: supports key=value, key!=value, key in (v1,v2), key notin (v1,v2), key, !key
    for requirement in split_label_requirements(selector) {
        let requirement = requirement.trim();
        if requirement.is_empty() {
            continue;
        }

        // Handle "key in (v1,v2,...)" — set-based
        if let Some(captures) = parse_set_requirement(requirement) {
            match captures {
                SetRequirement::In(key, values) => {
                    let label_val = labels.get(key);
                    if !values.iter().any(|v| label_val.is_some_and(|lv| lv == v)) {
                        return false;
                    }
                }
                SetRequirement::NotIn(key, values) => {
                    let label_val = labels.get(key);
                    if values.iter().any(|v| label_val.is_some_and(|lv| lv == v)) {
                        return false;
                    }
                }
                SetRequirement::Exists(key) => {
                    if !labels.contains_key(key) {
                        return false;
                    }
                }
                SetRequirement::NotExists(key) => {
                    if labels.contains_key(key) {
                        return false;
                    }
                }
            }
            continue;
        }

        if let Some((key, value)) = requirement.split_once('=') {
            // Handle != (key!=value)
            if key.ends_with('!') {
                let key = key.trim_end_matches('!');
                if labels.get(key).is_some_and(|v| v == value) {
                    return false; // Must NOT equal
                }
            } else {
                // key=value or key==value: must match
                let value = value.trim_start_matches('='); // handle ==
                if labels.get(key).is_none_or(|v| v != value) {
                    return false;
                }
            }
        } else if let Some(key) = requirement.strip_prefix('!') {
            // !key — key must not exist
            if labels.contains_key(key) {
                return false;
            }
        } else {
            // Just a key with no value — check existence
            if !labels.contains_key(requirement) {
                return false;
            }
        }
    }
    true
}

enum SetRequirement<'a> {
    In(&'a str, Vec<&'a str>),
    NotIn(&'a str, Vec<&'a str>),
    Exists(&'a str),
    NotExists(&'a str),
}

fn parse_set_requirement(s: &str) -> Option<SetRequirement<'_>> {
    // "key in (v1,v2)" or "key notin (v1,v2)"
    if let Some(idx) = s.find(" in (") {
        let key = s[..idx].trim();
        let values_str = &s[idx + 5..];
        let values_str = values_str.trim_end_matches(')');
        let values: Vec<&str> = values_str.split(',').map(|v| v.trim()).collect();
        return Some(SetRequirement::In(key, values));
    }
    if let Some(idx) = s.find(" notin (") {
        let key = s[..idx].trim();
        let values_str = &s[idx + 8..];
        let values_str = values_str.trim_end_matches(')');
        let values: Vec<&str> = values_str.split(',').map(|v| v.trim()).collect();
        return Some(SetRequirement::NotIn(key, values));
    }
    None
}

/// Split label selector string into requirements, respecting parentheses in set-based expressions
fn split_label_requirements(selector: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in selector.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                results.push(&selector[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < selector.len() {
        results.push(&selector[start..]);
    }
    results
}

/// Check if an object matches a field selector (common: metadata.name, metadata.namespace)
fn matches_field_selector(metadata: &ObjectMeta, selector: &Option<String>) -> bool {
    let selector = match selector {
        Some(s) if !s.is_empty() => s,
        _ => return true,
    };

    for requirement in selector.split(',') {
        let requirement = requirement.trim();
        if let Some((field, value)) = requirement.split_once('=') {
            match field {
                "metadata.name" if metadata.name != value => {
                    return false;
                }
                "metadata.namespace" if metadata.namespace.as_deref() != Some(value) => {
                    return false;
                }
                _ => {} // Unknown fields pass through
            }
        }
    }
    true
}

/// Construct a fallback DELETE event JSON when typed deserialization fails.
///
/// When etcd's prev_kv is absent (compaction) or the stored JSON doesn't match
/// the typed struct, the DELETE event must still be delivered. K8s always
/// delivers DELETE events; the object payload is best-effort.
///
/// Returns `Some(json_string)` if a valid event was constructed, `None` otherwise.
pub fn build_delete_fallback_json(key: &str, prev_value: &str) -> Option<String> {
    // Try to parse prev_value as raw JSON
    if let Ok(raw_obj) = serde_json::from_str::<serde_json::Value>(prev_value) {
        let k8s_event = serde_json::json!({
            "type": "DELETED",
            "object": raw_obj
        });
        return serde_json::to_string(&k8s_event).ok();
    }

    // prev_value is not valid JSON — construct minimal DELETE event from the key path.
    // Key format: /registry/{type}/{ns}/{name} or /registry/{type}/{name}
    let parts: Vec<&str> = key.split('/').collect();
    let name = parts.last().unwrap_or(&"");
    let ns = if parts.len() >= 5 {
        parts[parts.len() - 2]
    } else {
        ""
    };
    let k8s_event = serde_json::json!({
        "type": "DELETED",
        "object": {
            "metadata": {
                "name": name,
                "namespace": ns,
            }
        }
    });
    serde_json::to_string(&k8s_event).ok()
}

/// Extract resourceVersion from a raw JSON string.
pub fn extract_rv_from_json(json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|v| {
            v.pointer("/metadata/resourceVersion")
                .and_then(|rv| rv.as_str())
                .map(|s| s.to_string())
        })
}

/// Derive the Kind and apiVersion from resource_type and api_group
fn resource_type_to_kind_and_version(resource_type: &str, api_group: &str) -> (String, String) {
    let kind = match resource_type {
        "pods" => "Pod",
        "services" => "Service",
        "deployments" => "Deployment",
        "replicasets" => "ReplicaSet",
        "statefulsets" => "StatefulSet",
        "daemonsets" => "DaemonSet",
        "jobs" => "Job",
        "cronjobs" => "CronJob",
        "configmaps" => "ConfigMap",
        "secrets" => "Secret",
        "serviceaccounts" => "ServiceAccount",
        "namespaces" => "Namespace",
        "nodes" => "Node",
        "persistentvolumes" => "PersistentVolume",
        "persistentvolumeclaims" => "PersistentVolumeClaim",
        "endpoints" => "Endpoints",
        "endpointslices" => "EndpointSlice",
        "events" => "Event",
        "ingresses" => "Ingress",
        "networkpolicies" => "NetworkPolicy",
        "leases" => "Lease",
        "clusterroles" => "ClusterRole",
        "clusterrolebindings" => "ClusterRoleBinding",
        "roles" => "Role",
        "rolebindings" => "RoleBinding",
        "storageclasses" => "StorageClass",
        "customresourcedefinitions" => "CustomResourceDefinition",
        "poddisruptionbudgets" => "PodDisruptionBudget",
        "ipaddresses" => "IPAddress",
        "limitranges" => "LimitRange",
        "resourcequotas" => "ResourceQuota",
        "runtimeclasses" => "RuntimeClass",
        "ingressclasses" => "IngressClass",
        "priorityclasses" => "PriorityClass",
        "validatingwebhookconfigurations" => "ValidatingWebhookConfiguration",
        "mutatingwebhookconfigurations" => "MutatingWebhookConfiguration",
        "validatingadmissionpolicies" => "ValidatingAdmissionPolicy",
        "validatingadmissionpolicybindings" => "ValidatingAdmissionPolicyBinding",
        "certificatesigningrequests" => "CertificateSigningRequest",
        "flowschemas" => "FlowSchema",
        "prioritylevelconfigurations" => "PriorityLevelConfiguration",
        "servicecidrs" => "ServiceCIDR",
        "replicationcontrollers" => "ReplicationController",
        "horizontalpodautoscalers" => "HorizontalPodAutoscaler",
        "controllerrevisions" => "ControllerRevision",
        "csistoragecapacities" => "CSIStorageCapacity",
        "csidrivers" => "CSIDriver",
        "csinodes" => "CSINode",
        other => {
            // CamelCase heuristic: capitalize first letter, remove trailing 's'
            let s = other.strip_suffix('s').unwrap_or(other);
            return (
                format!("{}{}", &s[..1].to_uppercase(), &s[1..]),
                if api_group.is_empty() {
                    "v1".to_string()
                } else {
                    format!("{}/v1", api_group)
                },
            );
        }
    };
    let api_version = if api_group.is_empty() {
        "v1".to_string()
    } else {
        format!("{}/v1", api_group)
    };
    (kind.to_string(), api_version)
}

/// Trait for types that have metadata (all Kubernetes resources)
pub trait HasMetadata {
    fn metadata(&self) -> &ObjectMeta;
    fn metadata_mut(&mut self) -> &mut ObjectMeta;
}

/// Bookmark object containing only metadata with resourceVersion
/// Note: Bookmarks in Kubernetes watch streams don't need apiVersion/kind
/// as they are just checkpoint markers
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookmarkObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    pub metadata: ObjectMeta,
}

// Implement for common resource types
// Macro to reduce boilerplate for HasMetadata implementations
macro_rules! impl_has_metadata {
    ($($type:ty),*) => {
        $(
            impl HasMetadata for $type {
                fn metadata(&self) -> &ObjectMeta {
                    &self.metadata
                }
                fn metadata_mut(&mut self) -> &mut ObjectMeta {
                    &mut self.metadata
                }
            }
        )*
    };
}

impl_has_metadata!(
    rusternetes_common::resources::Pod,
    rusternetes_common::resources::Service,
    rusternetes_common::resources::Deployment,
    rusternetes_common::resources::ConfigMap,
    rusternetes_common::resources::Secret,
    rusternetes_common::resources::Node,
    rusternetes_common::resources::Namespace,
    rusternetes_common::resources::Endpoints,
    rusternetes_common::resources::EndpointSlice,
    rusternetes_common::resources::StatefulSet,
    rusternetes_common::resources::ReplicaSet,
    rusternetes_common::resources::DaemonSet,
    rusternetes_common::resources::Job,
    rusternetes_common::resources::CronJob,
    rusternetes_common::resources::Event,
    rusternetes_common::resources::ServiceAccount,
    rusternetes_common::resources::PersistentVolume,
    rusternetes_common::resources::PersistentVolumeClaim,
    rusternetes_common::resources::Lease,
    rusternetes_common::resources::Ingress,
    rusternetes_common::resources::NetworkPolicy,
    rusternetes_common::resources::PodDisruptionBudget,
    rusternetes_common::resources::IPAddress,
    rusternetes_common::resources::PodTemplate,
    rusternetes_common::resources::ControllerRevision,
    rusternetes_common::resources::RuntimeClass,
    rusternetes_common::resources::ResourceQuota,
    rusternetes_common::resources::ServiceCIDR,
    rusternetes_common::resources::CustomResourceDefinition,
    rusternetes_common::resources::ValidatingWebhookConfiguration,
    rusternetes_common::resources::MutatingWebhookConfiguration,
    rusternetes_common::resources::ValidatingAdmissionPolicy,
    rusternetes_common::resources::ValidatingAdmissionPolicyBinding,
    rusternetes_common::resources::LimitRange,
    rusternetes_common::resources::ReplicationController,
    rusternetes_common::resources::PriorityClass,
    rusternetes_common::resources::StorageClass,
    rusternetes_common::resources::HorizontalPodAutoscaler,
    rusternetes_common::resources::ClusterRole,
    rusternetes_common::resources::ClusterRoleBinding,
    rusternetes_common::resources::Role,
    rusternetes_common::resources::RoleBinding,
    rusternetes_common::resources::CertificateSigningRequest,
    rusternetes_common::resources::FlowSchema,
    rusternetes_common::resources::PriorityLevelConfiguration,
    rusternetes_common::resources::IngressClass,
    rusternetes_common::resources::CSIStorageCapacity,
    rusternetes_common::resources::CustomResource
);

// Concrete handler functions for specific resources

/// Watch pods in a namespace
pub async fn watch_pods(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_namespaced::<rusternetes_common::resources::Pod>(
        state, auth_ctx, namespace, "pods", "", params,
    )
    .await
}

/// Watch services in a namespace
pub async fn watch_services(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Service>(
        state, auth_ctx, namespace, "services", "", params,
    )
    .await
}

/// Watch deployments in a namespace
pub async fn watch_deployments(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_namespaced::<rusternetes_common::resources::Deployment>(
        state,
        auth_ctx,
        namespace,
        "deployments",
        "apps",
        params,
    )
    .await
}

/// Watch configmaps in a namespace
pub async fn watch_configmaps(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_namespaced::<rusternetes_common::resources::ConfigMap>(
        state,
        auth_ctx,
        namespace,
        "configmaps",
        "",
        params,
    )
    .await
}

/// Watch secrets in a namespace
pub async fn watch_secrets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_namespaced::<rusternetes_common::resources::Secret>(
        state, auth_ctx, namespace, "secrets", "", params,
    )
    .await
}

/// Watch nodes (cluster-scoped)
pub async fn watch_nodes(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_cluster_scoped::<rusternetes_common::resources::Node>(
        state, auth_ctx, "nodes", "", params,
    )
    .await
}

/// Watch namespaces (cluster-scoped)
pub async fn watch_namespaces(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::Namespace>(
        state,
        auth_ctx,
        "namespaces",
        "",
        params,
    )
    .await
}

/// Watch endpoints in a namespace
pub async fn watch_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Endpoints>(
        state,
        auth_ctx,
        namespace,
        "endpoints",
        "",
        params,
    )
    .await
}

/// Watch endpointslices in a namespace
pub async fn watch_endpointslices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::EndpointSlice>(
        state,
        auth_ctx,
        namespace,
        "endpointslices",
        "discovery.k8s.io",
        params,
    )
    .await
}

/// Watch statefulsets in a namespace
pub async fn watch_statefulsets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::StatefulSet>(
        state,
        auth_ctx,
        namespace,
        "statefulsets",
        "apps",
        params,
    )
    .await
}

/// Watch replicasets in a namespace
pub async fn watch_replicasets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ReplicaSet>(
        state,
        auth_ctx,
        namespace,
        "replicasets",
        "apps",
        params,
    )
    .await
}

/// Watch daemonsets in a namespace
pub async fn watch_daemonsets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::DaemonSet>(
        state,
        auth_ctx,
        namespace,
        "daemonsets",
        "apps",
        params,
    )
    .await
}

/// Watch jobs in a namespace
pub async fn watch_jobs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Job>(
        state, auth_ctx, namespace, "jobs", "batch", params,
    )
    .await
}

/// Watch cronjobs in a namespace
pub async fn watch_cronjobs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::CronJob>(
        state, auth_ctx, namespace, "cronjobs", "batch", params,
    )
    .await
}

/// Watch events in a namespace
pub async fn watch_events(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Event>(
        state, auth_ctx, namespace, "events", "", params,
    )
    .await
}

/// Watch serviceaccounts in a namespace
pub async fn watch_serviceaccounts(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ServiceAccount>(
        state,
        auth_ctx,
        namespace,
        "serviceaccounts",
        "",
        params,
    )
    .await
}

/// Watch persistentvolumes (cluster-scoped)
pub async fn watch_persistentvolumes(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::PersistentVolume>(
        state,
        auth_ctx,
        "persistentvolumes",
        "",
        params,
    )
    .await
}

/// Watch persistentvolumeclaims in a namespace
pub async fn watch_persistentvolumeclaims(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::PersistentVolumeClaim>(
        state,
        auth_ctx,
        namespace,
        "persistentvolumeclaims",
        "",
        params,
    )
    .await
}

/// Watch runtimeclasses (cluster-scoped)
pub async fn watch_runtimeclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::RuntimeClass>(
        state,
        auth_ctx,
        "runtimeclasses",
        "node.k8s.io",
        params,
    )
    .await
}

/// Watch resourcequotas in a namespace
pub async fn watch_resourcequotas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ResourceQuota>(
        state,
        auth_ctx,
        namespace,
        "resourcequotas",
        "",
        params,
    )
    .await
}

/// Watch resourcequotas across all namespaces
pub async fn watch_resourcequotas_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ResourceQuota>(
        state,
        auth_ctx,
        "resourcequotas",
        "",
        params,
    )
    .await
}

/// Watch servicecidrs (cluster-scoped)
pub async fn watch_servicecidrs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ServiceCIDR>(
        state,
        auth_ctx,
        "servicecidrs",
        "networking.k8s.io",
        params,
    )
    .await
}

/// Watch ipaddresses (cluster-scoped)
pub async fn watch_ipaddresses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::IPAddress>(
        state,
        auth_ctx,
        "ipaddresses",
        "networking.k8s.io",
        params,
    )
    .await
}

/// Watch customresourcedefinitions (cluster-scoped)
pub async fn watch_crds(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::CustomResourceDefinition>(
        state,
        auth_ctx,
        "customresourcedefinitions",
        "apiextensions.k8s.io",
        params,
    )
    .await
}

/// Watch validatingwebhookconfigurations (cluster-scoped)
pub async fn watch_validatingwebhookconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ValidatingWebhookConfiguration>(
        state,
        auth_ctx,
        "validatingwebhookconfigurations",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch mutatingwebhookconfigurations (cluster-scoped)
pub async fn watch_mutatingwebhookconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::MutatingWebhookConfiguration>(
        state,
        auth_ctx,
        "mutatingwebhookconfigurations",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch validatingadmissionpolicies (cluster-scoped)
pub async fn watch_validatingadmissionpolicies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ValidatingAdmissionPolicy>(
        state,
        auth_ctx,
        "validatingadmissionpolicies",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch validatingadmissionpolicybindings (cluster-scoped)
pub async fn watch_validatingadmissionpolicybindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ValidatingAdmissionPolicyBinding>(
        state,
        auth_ctx,
        "validatingadmissionpolicybindings",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch poddisruptionbudgets in a namespace
pub async fn watch_poddisruptionbudgets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::PodDisruptionBudget>(
        state,
        auth_ctx,
        namespace,
        "poddisruptionbudgets",
        "policy",
        params,
    )
    .await
}

/// Watch poddisruptionbudgets across all namespaces
pub async fn watch_poddisruptionbudgets_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::PodDisruptionBudget>(
        state,
        auth_ctx,
        "poddisruptionbudgets",
        "policy",
        params,
    )
    .await
}

/// Watch limitranges in a namespace
pub async fn watch_limitranges(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::LimitRange>(
        state,
        auth_ctx,
        namespace,
        "limitranges",
        "",
        params,
    )
    .await
}

/// Watch replicationcontrollers in a namespace
pub async fn watch_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ReplicationController>(
        state,
        auth_ctx,
        namespace,
        "replicationcontrollers",
        "",
        params,
    )
    .await
}

/// Watch priorityclasses (cluster-scoped)
pub async fn watch_priorityclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::PriorityClass>(
        state,
        auth_ctx,
        "priorityclasses",
        "scheduling.k8s.io",
        params,
    )
    .await
}

/// Watch storageclasses (cluster-scoped)
pub async fn watch_storageclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::StorageClass>(
        state,
        auth_ctx,
        "storageclasses",
        "storage.k8s.io",
        params,
    )
    .await
}

/// Watch horizontalpodautoscalers in a namespace
pub async fn watch_horizontalpodautoscalers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::HorizontalPodAutoscaler>(
        state,
        auth_ctx,
        namespace,
        "horizontalpodautoscalers",
        "autoscaling",
        params,
    )
    .await
}

/// Watch clusterroles (cluster-scoped)
pub async fn watch_clusterroles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ClusterRole>(
        state,
        auth_ctx,
        "clusterroles",
        "rbac.authorization.k8s.io",
        params,
    )
    .await
}

/// Watch clusterrolebindings (cluster-scoped)
pub async fn watch_clusterrolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ClusterRoleBinding>(
        state,
        auth_ctx,
        "clusterrolebindings",
        "rbac.authorization.k8s.io",
        params,
    )
    .await
}

/// Watch roles in a namespace
pub async fn watch_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Role>(
        state,
        auth_ctx,
        namespace,
        "roles",
        "rbac.authorization.k8s.io",
        params,
    )
    .await
}

/// Watch rolebindings in a namespace
pub async fn watch_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::RoleBinding>(
        state,
        auth_ctx,
        namespace,
        "rolebindings",
        "rbac.authorization.k8s.io",
        params,
    )
    .await
}

/// Watch leases in a namespace
pub async fn watch_leases(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Lease>(
        state,
        auth_ctx,
        namespace,
        "leases",
        "coordination.k8s.io",
        params,
    )
    .await
}

/// Watch ingresses in a namespace
pub async fn watch_ingresses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Ingress>(
        state,
        auth_ctx,
        namespace,
        "ingresses",
        "networking.k8s.io",
        params,
    )
    .await
}

/// Watch networkpolicies in a namespace
pub async fn watch_networkpolicies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::NetworkPolicy>(
        state,
        auth_ctx,
        namespace,
        "networkpolicies",
        "networking.k8s.io",
        params,
    )
    .await
}

/// Watch certificatesigningrequests (cluster-scoped)
pub async fn watch_certificatesigningrequests(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::CertificateSigningRequest>(
        state,
        auth_ctx,
        "certificatesigningrequests",
        "certificates.k8s.io",
        params,
    )
    .await
}

/// Watch flowschemas (cluster-scoped)
pub async fn watch_flowschemas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::FlowSchema>(
        state,
        auth_ctx,
        "flowschemas",
        "flowcontrol.apiserver.k8s.io",
        params,
    )
    .await
}

/// Watch prioritylevelconfigurations (cluster-scoped)
pub async fn watch_prioritylevelconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::PriorityLevelConfiguration>(
        state,
        auth_ctx,
        "prioritylevelconfigurations",
        "flowcontrol.apiserver.k8s.io",
        params,
    )
    .await
}

/// Watch podtemplates in a namespace
pub async fn watch_podtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::PodTemplate>(
        state,
        auth_ctx,
        namespace,
        "podtemplates",
        "",
        params,
    )
    .await
}

/// Watch controllerrevisions in a namespace
pub async fn watch_controllerrevisions(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ControllerRevision>(
        state,
        auth_ctx,
        namespace,
        "controllerrevisions",
        "apps",
        params,
    )
    .await
}

/// Helper to extract metadata fields from a serde_json::Value
fn json_resource_version(val: &serde_json::Value) -> Option<String> {
    val.get("metadata")?
        .get("resourceVersion")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Watch cluster-scoped resources using serde_json::Value (for DRA types without HasMetadata)
pub async fn watch_cluster_scoped_json(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
) -> Result<Response> {
    info!("Starting JSON watch for cluster-scoped {}", resource_type);

    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "watch", resource_type)
        .with_api_group(api_group);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix(resource_type, None);

    // Use history-aware subscription when a specific resourceVersion is requested.
    // This replays MODIFIED events from the watch cache history, which is critical
    // for CRD Established condition delivery — the Go informer ignores duplicate
    // ADDED events but processes MODIFIED events from history replay.
    let requested_rv = params.resource_version.clone();
    let (watch_stream, existing_resources) = if let Some(ref rv_str) = requested_rv {
        if let Ok(rv) = rv_str.parse::<i64>() {
            if rv > 1 {
                // Specific RV: replay history from that revision
                let (history, rx) = state.watch_cache.subscribe_from(&prefix, rv).await;
                let stream = crate::watch_cache::broadcast_to_stream_with_history(history, rx);
                // Don't send initial ADDED events — history replay delivers MODIFIED events
                (stream, Vec::new())
            } else {
                let rx = state.watch_cache.subscribe(&prefix).await;
                let stream = crate::watch_cache::broadcast_to_stream(rx);
                let resources: Vec<serde_json::Value> = state.storage.list(&prefix).await?;
                (stream, resources)
            }
        } else {
            let rx = state.watch_cache.subscribe(&prefix).await;
            let stream = crate::watch_cache::broadcast_to_stream(rx);
            let resources: Vec<serde_json::Value> = state.storage.list(&prefix).await?;
            (stream, resources)
        }
    } else {
        let rx = state.watch_cache.subscribe(&prefix).await;
        let stream = crate::watch_cache::broadcast_to_stream(rx);
        let resources: Vec<serde_json::Value> = state.storage.list(&prefix).await?;
        (stream, resources)
    };

    let current_rev = state.storage.current_revision().await.unwrap_or(1);
    let current_rev_str = current_rev.to_string();

    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<String, std::io::Error>>(256);

    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    let send_initial_events = params.send_initial_events.unwrap_or(false);
    let timeout_duration = Some(Duration::from_secs(
        params.timeout_seconds.unwrap_or(300).min(300),
    ));
    let (bookmark_kind, bookmark_api_version) =
        resource_type_to_kind_and_version(resource_type, api_group);

    let should_send_initial =
        send_initial_events || requested_rv.as_deref() == Some("0") || requested_rv.is_none();

    let prefix_for_reconnect = prefix.clone();
    let state_for_reconnect = state.clone();

    tokio::spawn(async move {
        let mut latest_resource_version: Option<String> = Some(current_rev_str);

        if should_send_initial {
            for object in existing_resources {
                if let Some(rv) = json_resource_version(&object) {
                    latest_resource_version = Some(rv);
                }
                let k8s_event = serde_json::json!({
                    "type": "ADDED",
                    "object": object
                });
                if let Ok(json) = serde_json::to_string(&k8s_event) {
                    if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                        return;
                    }
                }
            }
        }

        if send_initial_events {
            if let Some(ref rv) = latest_resource_version {
                let bookmark = serde_json::json!({
                    "type": "BOOKMARK",
                    "object": {
                        "kind": bookmark_kind,
                        "apiVersion": bookmark_api_version,
                        "metadata": {
                            "resourceVersion": rv,
                            "annotations": {
                                "k8s.io/initial-events-end": "true"
                            }
                        }
                    }
                });
                if let Ok(json) = serde_json::to_string(&bookmark) {
                    let _ = tx.try_send(Ok(format!("{}\n", json)));
                }
            }
        }

        let mut bookmark_interval = if allow_bookmarks || send_initial_events {
            Some(interval(Duration::from_secs(5)))
        } else {
            None
        };

        let mut watch_stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = rusternetes_common::Result<WatchEvent>> + Send>,
        > = Box::pin(watch_stream);

        let watch_future = async {
            loop {
                tokio::select! {
                    event_opt = watch_stream.next() => {
                        match event_opt {
                            Some(Ok(event)) => {
                                let (event_type, value_str) = match event {
                                    WatchEvent::Added(_, v) => ("ADDED", v),
                                    WatchEvent::Modified(_, v) => ("MODIFIED", v),
                                    WatchEvent::Deleted(_, v) => ("DELETED", v),
                                };
                                if let Ok(object) = serde_json::from_str::<serde_json::Value>(&value_str) {
                                    if let Some(rv) = json_resource_version(&object) {
                                        latest_resource_version = Some(rv);
                                    }
                                    let k8s_event = serde_json::json!({
                                        "type": event_type,
                                        "object": object
                                    });
                                    if let Ok(json) = serde_json::to_string(&k8s_event) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                debug!("Watch stream transient error (continuing): {}", e);
                                continue;
                            }
                            None => {
                                // Watch stream ended — resubscribe from cache
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                let new_rx = state_for_reconnect.watch_cache.subscribe(&prefix_for_reconnect).await;
                                watch_stream = Box::pin(crate::watch_cache::broadcast_to_stream(new_rx));
                                continue;
                            }
                        }
                    }
                    _ = async {
                        if let Some(ref mut bi) = bookmark_interval {
                            bi.tick().await
                        } else {
                            std::future::pending::<tokio::time::Instant>().await
                        }
                    } => {
                        if let Some(ref rv) = latest_resource_version {
                            let bookmark = serde_json::json!({
                                "type": "BOOKMARK",
                                "object": {
                                    "kind": bookmark_kind,
                                    "apiVersion": bookmark_api_version,
                                    "metadata": {
                                        "resourceVersion": rv
                                    }
                                }
                            });
                            if let Ok(json) = serde_json::to_string(&bookmark) {
                                let _ = tx.try_send(Ok(format!("{}\n", json)));
                            }
                        }
                    }
                }
            }
        };

        if let Some(dur) = timeout_duration {
            let _ = timeout(dur, watch_future).await;
        } else {
            watch_future.await;
        }
    });

    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(body)
        .unwrap())
}

/// Watch namespaced resources using serde_json::Value (for DRA types without HasMetadata)
pub async fn watch_namespaced_json(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    namespace: String,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
) -> Result<Response> {
    info!(
        "Starting JSON watch for namespaced {}/{}",
        namespace, resource_type
    );

    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "watch", resource_type)
        .with_api_group(api_group)
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix(resource_type, Some(&namespace));
    let watch_rx = state.watch_cache.subscribe(&prefix).await;
    let watch_stream = crate::watch_cache::broadcast_to_stream(watch_rx);
    let existing_resources: Vec<serde_json::Value> = state.storage.list(&prefix).await?;
    let current_rev = state.storage.current_revision().await.unwrap_or(1);
    let current_rev_str = current_rev.to_string();

    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<String, std::io::Error>>(256);

    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    let send_initial_events = params.send_initial_events.unwrap_or(false);
    let timeout_duration = Some(Duration::from_secs(
        params.timeout_seconds.unwrap_or(300).min(300),
    ));
    let _requested_rv = params.resource_version.clone();
    let (bookmark_kind, bookmark_api_version) =
        resource_type_to_kind_and_version(resource_type, api_group);

    // Always send initial events for namespaced JSON watches.
    // When the client watches with a specific resourceVersion (from a CREATE),
    // our broadcast subscription only gets future events, missing the MODIFIED
    // event that already happened. Sending current state as ADDED ensures the
    // client sees the latest status (e.g. CRD Established=True condition).
    let should_send_initial = true;

    let prefix_for_reconnect = prefix.clone();
    let state_for_reconnect = state.clone();

    tokio::spawn(async move {
        let mut latest_resource_version: Option<String> = Some(current_rev_str);

        if should_send_initial {
            for object in existing_resources {
                if let Some(rv) = json_resource_version(&object) {
                    latest_resource_version = Some(rv);
                }
                let k8s_event = serde_json::json!({
                    "type": "ADDED",
                    "object": object
                });
                if let Ok(json) = serde_json::to_string(&k8s_event) {
                    if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                        return;
                    }
                }
            }
        }

        if send_initial_events {
            if let Some(ref rv) = latest_resource_version {
                let bookmark = serde_json::json!({
                    "type": "BOOKMARK",
                    "object": {
                        "kind": bookmark_kind,
                        "apiVersion": bookmark_api_version,
                        "metadata": {
                            "resourceVersion": rv,
                            "annotations": {
                                "k8s.io/initial-events-end": "true"
                            }
                        }
                    }
                });
                if let Ok(json) = serde_json::to_string(&bookmark) {
                    let _ = tx.try_send(Ok(format!("{}\n", json)));
                }
            }
        }

        let mut bookmark_interval = if allow_bookmarks || send_initial_events {
            Some(interval(Duration::from_secs(5)))
        } else {
            None
        };

        let mut watch_stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = rusternetes_common::Result<WatchEvent>> + Send>,
        > = Box::pin(watch_stream);

        let watch_future = async {
            loop {
                tokio::select! {
                    event_opt = watch_stream.next() => {
                        match event_opt {
                            Some(Ok(event)) => {
                                let (event_type, value_str) = match event {
                                    WatchEvent::Added(_, v) => ("ADDED", v),
                                    WatchEvent::Modified(_, v) => ("MODIFIED", v),
                                    WatchEvent::Deleted(_, v) => ("DELETED", v),
                                };
                                if let Ok(object) = serde_json::from_str::<serde_json::Value>(&value_str) {
                                    if let Some(rv) = json_resource_version(&object) {
                                        latest_resource_version = Some(rv);
                                    }
                                    let k8s_event = serde_json::json!({
                                        "type": event_type,
                                        "object": object
                                    });
                                    if let Ok(json) = serde_json::to_string(&k8s_event) {
                                        if tx.send(Ok(format!("{}\n", json))).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                debug!("Watch stream transient error (continuing): {}", e);
                                continue;
                            }
                            None => {
                                // Watch stream ended — resubscribe from cache
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                let new_rx = state_for_reconnect.watch_cache.subscribe(&prefix_for_reconnect).await;
                                watch_stream = Box::pin(crate::watch_cache::broadcast_to_stream(new_rx));
                                continue;
                            }
                        }
                    }
                    _ = async {
                        if let Some(ref mut bi) = bookmark_interval {
                            bi.tick().await
                        } else {
                            std::future::pending::<tokio::time::Instant>().await
                        }
                    } => {
                        if let Some(ref rv) = latest_resource_version {
                            let bookmark = serde_json::json!({
                                "type": "BOOKMARK",
                                "object": {
                                    "kind": bookmark_kind,
                                    "apiVersion": bookmark_api_version,
                                    "metadata": {
                                        "resourceVersion": rv
                                    }
                                }
                            });
                            if let Ok(json) = serde_json::to_string(&bookmark) {
                                let _ = tx.try_send(Ok(format!("{}\n", json)));
                            }
                        }
                    }
                }
            }
        };

        if let Some(dur) = timeout_duration {
            let _ = timeout(dur, watch_future).await;
        } else {
            watch_future.await;
        }
    });

    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(body)
        .unwrap())
}
