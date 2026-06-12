//! Docker container labels that record the Kubernetes pod/container identity.
//!
//! Mirrors the upstream kubelet, which keys the container↔pod relationship on
//! the pod **UID** rather than the pod name. Without these labels, two pods
//! that share a name but differ in UID (e.g. a StatefulSet pod that is evicted
//! and recreated) are indistinguishable to the runtime: the kubelet would
//! happily adopt the previous incarnation's still-running container as if it
//! belonged to the new pod.
//!
//! K8s parity:
//! - `staging/src/k8s.io/kubelet/pkg/types/labels.go` — the label key constants.
//! - `pkg/kubelet/kuberuntime/labels.go` (`newPodLabels`) — population of the
//!   pod identity labels at container/sandbox create time.
//! - `pkg/kubelet/kuberuntime/kuberuntime_manager.go` (`computePodActions`) —
//!   containers belonging to a different pod UID are treated as not-this-pod's
//!   and killed.

use rusternetes_common::resources::Pod;
use std::collections::HashMap;

/// `io.kubernetes.pod.name` — the pod's `metadata.name`.
pub const POD_NAME_LABEL: &str = "io.kubernetes.pod.name";
/// `io.kubernetes.pod.namespace` — the pod's `metadata.namespace`.
pub const POD_NAMESPACE_LABEL: &str = "io.kubernetes.pod.namespace";
/// `io.kubernetes.pod.uid` — the pod's `metadata.uid`. The identity key.
pub const POD_UID_LABEL: &str = "io.kubernetes.pod.uid";
/// `io.kubernetes.container.name` — the container's name within the pod.
pub const CONTAINER_NAME_LABEL: &str = "io.kubernetes.container.name";

/// Build the four upstream pod/container identity labels for a Docker
/// container belonging to `pod`.
///
/// The namespace defaults to `"default"` when unset, matching the rest of the
/// kubelet's namespace handling. The UID is taken verbatim from
/// `pod.metadata.uid` (the API server always assigns one).
pub fn pod_container_labels(pod: &Pod, container_name: &str) -> HashMap<String, String> {
    let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
    let mut labels = HashMap::with_capacity(4);
    labels.insert(POD_NAME_LABEL.to_string(), pod.metadata.name.clone());
    labels.insert(POD_NAMESPACE_LABEL.to_string(), namespace.to_string());
    labels.insert(POD_UID_LABEL.to_string(), pod.metadata.uid.clone());
    labels.insert(CONTAINER_NAME_LABEL.to_string(), container_name.to_string());
    labels
}

/// Decide whether an existing Docker container is a **stale incarnation of
/// THIS pod** — i.e. its labels positively claim the same pod name AND the
/// same namespace, but carry a pod UID that disagrees with the UID we are
/// currently reconciling.
///
/// Returns `true` only when ALL of:
/// - `pod_uid` is non-empty, AND
/// - the labels' `io.kubernetes.pod.name` equals `pod_name`, AND
/// - the labels' `io.kubernetes.pod.namespace` equals `pod_namespace`, AND
/// - the labels' `io.kubernetes.pod.uid` exists and differs from `pod_uid`.
///
/// Any missing label (legacy containers created before labeling existed, or
/// containers not managed by this kubelet at all) means **NOT stale**: we only
/// ever destroy containers we can positively attribute to a previous
/// incarnation of the exact same namespaced pod. Container names carry no
/// namespace and Docker name filters are substring matches, so name-based
/// matching alone could hit unrelated or cross-namespace pods — the full
/// name+namespace+uid triple is the removal contract.
pub fn is_stale_same_pod_incarnation(
    container_labels: Option<&HashMap<String, String>>,
    pod_name: &str,
    pod_namespace: &str,
    pod_uid: &str,
) -> bool {
    if pod_uid.is_empty() {
        return false;
    }
    let Some(labels) = container_labels else {
        return false;
    };
    if labels.get(POD_NAME_LABEL).map(String::as_str) != Some(pod_name) {
        return false;
    }
    if labels.get(POD_NAMESPACE_LABEL).map(String::as_str) != Some(pod_namespace) {
        return false;
    }
    match labels.get(POD_UID_LABEL) {
        Some(uid) => uid != pod_uid,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::ObjectMeta;

    fn pod_with(name: &str, namespace: Option<&str>, uid: &str) -> Pod {
        Pod {
            type_meta: Default::default(),
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: namespace.map(|s| s.to_string()),
                uid: uid.to_string(),
                ..Default::default()
            },
            spec: None,
            status: None,
        }
    }

    #[test]
    fn pod_container_labels_has_four_labels() {
        let pod = pod_with("web-0", Some("apps"), "uid-abc");
        let labels = pod_container_labels(&pod, "nginx");
        assert_eq!(labels.len(), 4);
        assert_eq!(labels.get(POD_NAME_LABEL).unwrap(), "web-0");
        assert_eq!(labels.get(POD_NAMESPACE_LABEL).unwrap(), "apps");
        assert_eq!(labels.get(POD_UID_LABEL).unwrap(), "uid-abc");
        assert_eq!(labels.get(CONTAINER_NAME_LABEL).unwrap(), "nginx");
    }

    #[test]
    fn pod_container_labels_defaults_empty_namespace_to_default() {
        let pod = pod_with("web-0", None, "uid-abc");
        let labels = pod_container_labels(&pod, "nginx");
        assert_eq!(labels.get(POD_NAMESPACE_LABEL).unwrap(), "default");
    }

    /// Full identity-label set for a container of pod `name`/`ns` with `uid`.
    fn full_labels(name: &str, ns: &str, uid: &str) -> HashMap<String, String> {
        let mut l = HashMap::new();
        l.insert(POD_NAME_LABEL.to_string(), name.to_string());
        l.insert(POD_NAMESPACE_LABEL.to_string(), ns.to_string());
        l.insert(POD_UID_LABEL.to_string(), uid.to_string());
        l
    }

    #[test]
    fn stale_when_same_name_same_ns_different_uid() {
        let labels = full_labels("web-0", "apps", "old-uid");
        assert!(is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_different_pod_name() {
        // Docker's name filter is a substring match — a container of pod
        // "myweb-0" can show up when listing for pod "web-0". The predicate
        // must reject it.
        let labels = full_labels("myweb-0", "apps", "old-uid");
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_different_namespace() {
        // Same-named pods in different namespaces share the docker name
        // pattern; only the same-namespace pod may be swept.
        let labels = full_labels("web-0", "other-ns", "old-uid");
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_uid_matches() {
        let labels = full_labels("web-0", "apps", "same-uid");
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "same-uid"
        ));
    }

    #[test]
    fn not_stale_when_uid_label_missing() {
        let mut labels = full_labels("web-0", "apps", "x");
        labels.remove(POD_UID_LABEL);
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_name_label_missing() {
        let mut labels = full_labels("web-0", "apps", "old-uid");
        labels.remove(POD_NAME_LABEL);
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_namespace_label_missing() {
        let mut labels = full_labels("web-0", "apps", "old-uid");
        labels.remove(POD_NAMESPACE_LABEL);
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_pod_uid_empty() {
        let labels = full_labels("web-0", "apps", "old-uid");
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            ""
        ));
    }

    #[test]
    fn not_stale_when_labels_map_empty() {
        let labels = HashMap::new();
        assert!(!is_stale_same_pod_incarnation(
            Some(&labels),
            "web-0",
            "apps",
            "new-uid"
        ));
    }

    #[test]
    fn not_stale_when_labels_none() {
        assert!(!is_stale_same_pod_incarnation(
            None, "web-0", "apps", "new-uid"
        ));
    }
}
