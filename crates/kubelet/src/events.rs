//! Kubelet event emission.
//!
//! Upstream, the kubelet is the source of truth for per-container lifecycle
//! events — `Pulling`, `Pulled`, `Failed` (image), `Created`, `Started`,
//! `Killing`, `BackOff`, `Unhealthy` (probe). Before this module the rusternetes
//! kubelet emitted none of these; the controller-manager's `EventsController`
//! *inferred* a subset of them by polling `pod.status`. With a unified
//! [`EventRecorder`] available, the kubelet now records the real events at the
//! actual transition points and the controller-manager stops inferring them.
//!
//! The reason strings here mirror `pkg/kubelet/events/event.go` exactly so the
//! events match what conformance and tooling (Lens, `kubectl describe`) expect.

use rusternetes_common::resources::{EventType, ObjectReference, Pod};
use rusternetes_storage::{EventRecorder, Storage};

// --- Reason strings, verbatim from pkg/kubelet/events/event.go ---

/// Image pull started.
pub const PULLING_IMAGE: &str = "Pulling";
/// Image pull succeeded.
pub const PULLED_IMAGE: &str = "Pulled";
/// Image pull failed (`FailedToPullImage`).
pub const FAILED_TO_PULL_IMAGE: &str = "Failed";
/// Container created.
pub const CREATED_CONTAINER: &str = "Created";
/// Container started.
pub const STARTED_CONTAINER: &str = "Started";
/// Container create/start failed (`FailedToCreateContainer` / `FailedToStartContainer`).
pub const FAILED_CONTAINER: &str = "Failed";
/// Container is being killed.
pub const KILLING_CONTAINER: &str = "Killing";
/// Container start is in back-off (`BackOffStartContainer`).
pub const BACK_OFF_START_CONTAINER: &str = "BackOff";
/// A liveness/readiness/startup probe failed (`ContainerUnhealthy`).
pub const CONTAINER_UNHEALTHY: &str = "Unhealthy";
/// A probe succeeded with warning (`ContainerProbeWarning`).
pub const CONTAINER_PROBE_WARNING: &str = "ProbeWarning";

/// The component name the kubelet stamps on every event it sources.
pub const KUBELET_COMPONENT: &str = "kubelet";

/// Build the `involvedObject` reference for a pod-scoped event.
pub fn pod_object_reference(pod: &Pod) -> ObjectReference {
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    ObjectReference {
        kind: Some("Pod".to_string()),
        namespace: Some(namespace),
        name: Some(pod.metadata.name.clone()),
        uid: Some(pod.metadata.uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: pod.metadata.resource_version.clone(),
        field_path: None,
    }
}

/// `spec.containers{<name>}` — the upstream `fieldPath` form that pins a
/// container-scoped event to a specific container in `kubectl describe`.
pub fn container_field_path(container_name: &str) -> String {
    format!("spec.containers{{{}}}", container_name)
}

/// Build the `involvedObject` reference for a container-scoped event: the pod
/// reference with `fieldPath` set to the container.
pub fn container_object_reference(pod: &Pod, container_name: &str) -> ObjectReference {
    let mut object_ref = pod_object_reference(pod);
    object_ref.field_path = Some(container_field_path(container_name));
    object_ref
}

/// Emit a kubelet lifecycle event through the shared recorder.
///
/// `container_name = Some(name)` scopes the event to a container (sets
/// `fieldPath`); `None` scopes it to the pod. The recorder applies the
/// correlator (spam-filter + aggregation + count) before persisting.
///
/// Taking the recorder explicitly (rather than reaching through a
/// `ContainerRuntime`, which needs a live Docker connection) keeps this
/// unit-testable against an in-memory backend.
pub async fn emit_lifecycle_event<S: Storage + ?Sized>(
    recorder: &EventRecorder<S>,
    pod: &Pod,
    container_name: Option<&str>,
    reason: &str,
    event_type: EventType,
    message: &str,
) -> rusternetes_common::Result<()> {
    let involved = match container_name {
        Some(name) => container_object_reference(pod, name),
        None => pod_object_reference(pod),
    };
    let source = rusternetes_common::resources::EventSource {
        component: KUBELET_COMPONENT.to_string(),
        host: pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .filter(|n| !n.is_empty()),
    };
    recorder
        .event(&involved, &source, event_type, reason, message)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Event, PodSpec};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::{Storage, StorageBackend};
    use std::sync::Arc;

    fn make_pod(name: &str, namespace: &str, node: Option<&str>) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name).with_namespace(namespace);
                m.uid = format!("{}-uid-abcdef01", name);
                m
            },
            spec: Some(PodSpec {
                node_name: node.map(|s| s.to_string()),
                ..Default::default()
            }),
            status: None,
        }
    }

    #[test]
    fn container_field_path_matches_upstream_form() {
        assert_eq!(container_field_path("web"), "spec.containers{web}");
    }

    #[test]
    fn container_reference_carries_field_path_and_uid() {
        let pod = make_pod("web", "default", Some("node-1"));
        let r = container_object_reference(&pod, "app");
        assert_eq!(r.field_path.as_deref(), Some("spec.containers{app}"));
        assert_eq!(r.kind.as_deref(), Some("Pod"));
        assert_eq!(r.name.as_deref(), Some("web"));
        assert_eq!(r.uid.as_deref(), Some("web-uid-abcdef01"));
    }

    #[tokio::test]
    async fn emit_writes_container_event_with_kubelet_source_and_field_path() {
        let storage = Arc::new(StorageBackend::new_memory());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let pod = make_pod("web", "default", Some("node-7"));

        emit_lifecycle_event(
            &recorder,
            &pod,
            Some("app"),
            STARTED_CONTAINER,
            EventType::Normal,
            "Started container app",
        )
        .await
        .unwrap();

        let obj = container_object_reference(&pod, "app");
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, STARTED_CONTAINER)
        );
        let ev: Event = storage.get(&key).await.expect("event should be recorded");
        assert_eq!(ev.reason, "Started");
        assert_eq!(ev.source.component, "kubelet");
        assert_eq!(ev.source.host.as_deref(), Some("node-7"));
        assert_eq!(
            ev.involved_object.field_path.as_deref(),
            Some("spec.containers{app}")
        );
    }

    #[tokio::test]
    async fn emit_failed_pull_is_a_warning() {
        let storage = Arc::new(StorageBackend::new_memory());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let pod = make_pod("web", "default", None);

        emit_lifecycle_event(
            &recorder,
            &pod,
            Some("app"),
            FAILED_TO_PULL_IMAGE,
            EventType::Warning,
            "Failed to pull image \"nope:latest\"",
        )
        .await
        .unwrap();

        let obj = container_object_reference(&pod, "app");
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, FAILED_TO_PULL_IMAGE)
        );
        let ev: Event = storage.get(&key).await.unwrap();
        assert_eq!(ev.event_type, EventType::Warning);
        assert!(ev.source.host.is_none(), "no node → no host");
    }
}
