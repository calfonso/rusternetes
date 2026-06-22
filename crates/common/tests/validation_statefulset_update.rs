use rusternetes_common::resources::workloads::StatefulSet;
use rusternetes_common::validation::apps::validate_statefulset_update;

fn base() -> StatefulSet {
    serde_json::from_value(serde_json::json!({
        "metadata": {"name": "web"},
        "spec": {
            "serviceName": "svc",
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": {"type": "RollingUpdate"},
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    }))
    .unwrap()
}

#[test]
fn service_name_immutable() {
    let old = base();
    let mut new = base();
    new.spec.service_name = "other".to_string();
    assert_eq!(validate_statefulset_update(&new, &old).len(), 1);
}

#[test]
fn pod_management_policy_immutable() {
    let mut old = base();
    old.spec.pod_management_policy = Some("OrderedReady".to_string());
    let mut new = base();
    new.spec.pod_management_policy = Some("Parallel".to_string());
    assert_eq!(validate_statefulset_update(&new, &old).len(), 1);
}

#[test]
fn volume_claim_templates_immutable() {
    let old = base();
    let mut new: StatefulSet = serde_json::from_value(serde_json::json!({
        "metadata": {"name": "web"},
        "spec": {
            "serviceName": "svc",
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": {"type": "RollingUpdate"},
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            },
            "volumeClaimTemplates": [{"metadata": {"name": "data"}, "spec": {}}]
        }
    }))
    .unwrap();
    assert_eq!(validate_statefulset_update(&new, &old).len(), 1);
    new.spec.volume_claim_templates = None;
    assert!(validate_statefulset_update(&new, &old).is_empty());
}

#[test]
fn full_spec_revalidated_on_update() {
    // Upstream ValidateStatefulSetUpdate re-runs ValidateStatefulSetSpec on the
    // new object, so an update that introduces an invalid spec (here a negative
    // replica count) must be rejected even though no immutable field changed.
    let old = base();
    let mut new = base();
    new.spec.replicas = Some(-1);
    assert!(!validate_statefulset_update(&new, &old).is_empty());
}

#[test]
fn mutable_fields_allowed() {
    let old = base();
    let mut new = base();
    new.spec.replicas = Some(5);
    new.spec.min_ready_seconds = Some(10);
    new.spec.revision_history_limit = Some(3);
    assert!(validate_statefulset_update(&new, &old).is_empty());
}
