//! Tests for RBAC validation (port of upstream `pkg/apis/rbac/validation`).

use rusternetes_common::resources::rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
};
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::rbac::{
    default_cluster_role_binding, default_role_binding, validate_cluster_role,
    validate_cluster_role_binding, validate_role, validate_role_binding,
};

const RBAC_GROUP: &str = "rbac.authorization.k8s.io";

fn resource_rule() -> PolicyRule {
    PolicyRule {
        verbs: vec!["get".to_string()],
        api_groups: Some(vec!["".to_string()]),
        resources: Some(vec!["pods".to_string()]),
        resource_names: None,
        non_resource_urls: None,
    }
}

fn role(rules: Vec<PolicyRule>) -> Role {
    let mut r = Role {
        type_meta: Default::default(),
        metadata: Default::default(),
        rules,
    };
    r.metadata.name = "r".to_string();
    r
}

fn subject(kind: &str, name: &str, api_group: Option<&str>) -> Subject {
    Subject {
        kind: kind.to_string(),
        name: name.to_string(),
        namespace: None,
        api_group: api_group.map(|s| s.to_string()),
    }
}

fn role_binding(role_ref: RoleRef, subjects: Vec<Subject>) -> RoleBinding {
    let mut rb = RoleBinding {
        type_meta: Default::default(),
        metadata: Default::default(),
        subjects,
        role_ref,
    };
    rb.metadata.name = "rb".to_string();
    rb
}

fn has(errs: &[Error], field_substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field_substr))
}

#[test]
fn valid_role_passes() {
    assert!(validate_role(&role(vec![resource_rule()])).is_empty());
}

#[test]
fn rule_without_verbs_rejected() {
    let mut rule = resource_rule();
    rule.verbs = vec![];
    assert!(has(&validate_role(&role(vec![rule])), "rules[0].verbs"));
}

#[test]
fn resource_rule_without_apigroups_rejected() {
    let mut rule = resource_rule();
    rule.api_groups = None;
    assert!(has(&validate_role(&role(vec![rule])), "rules[0].apiGroups"));
}

#[test]
fn nonresource_url_in_namespaced_role_rejected() {
    let rule = PolicyRule {
        verbs: vec!["get".to_string()],
        api_groups: None,
        resources: None,
        resource_names: None,
        non_resource_urls: Some(vec!["/healthz".to_string()]),
    };
    // Namespaced Role cannot use nonResourceURLs.
    assert!(has(
        &validate_role(&role(vec![rule])),
        "rules[0].nonResourceURLs"
    ));
}

#[test]
fn nonresource_url_with_resources_rejected_in_clusterrole() {
    let rule = PolicyRule {
        verbs: vec!["get".to_string()],
        api_groups: Some(vec!["".to_string()]),
        resources: Some(vec!["pods".to_string()]),
        resource_names: None,
        non_resource_urls: Some(vec!["/healthz".to_string()]),
    };
    let cr = ClusterRole {
        type_meta: Default::default(),
        metadata: Default::default(),
        rules: vec![rule],
        aggregation_rule: None,
    };
    assert!(has(&validate_cluster_role(&cr), "rules[0].nonResourceURLs"));
}

#[test]
fn nonresource_url_ok_in_clusterrole() {
    let rule = PolicyRule {
        verbs: vec!["get".to_string()],
        api_groups: None,
        resources: None,
        resource_names: None,
        non_resource_urls: Some(vec!["/healthz".to_string()]),
    };
    let cr = ClusterRole {
        type_meta: Default::default(),
        metadata: Default::default(),
        rules: vec![rule],
        aggregation_rule: None,
    };
    assert!(validate_cluster_role(&cr).is_empty());
}

#[test]
fn rolebinding_bad_roleref_apigroup_rejected() {
    let rr = RoleRef {
        api_group: "wrong.group".to_string(),
        kind: "Role".to_string(),
        name: "r".to_string(),
    };
    let errs = validate_role_binding(&role_binding(rr, vec![]));
    assert!(errs
        .iter()
        .any(|e| e.field == "roleRef.apiGroup" && e.error_type == ErrorType::NotSupported));
}

#[test]
fn rolebinding_bad_roleref_kind_rejected() {
    let rr = RoleRef {
        api_group: RBAC_GROUP.to_string(),
        kind: "Pod".to_string(),
        name: "r".to_string(),
    };
    assert!(has(
        &validate_role_binding(&role_binding(rr, vec![])),
        "roleRef.kind"
    ));
}

#[test]
fn rolebinding_missing_roleref_name_rejected() {
    let rr = RoleRef {
        api_group: RBAC_GROUP.to_string(),
        kind: "ClusterRole".to_string(),
        name: String::new(),
    };
    assert!(has(
        &validate_role_binding(&role_binding(rr, vec![])),
        "roleRef.name"
    ));
}

#[test]
fn valid_rolebinding_with_sa_subject_passes() {
    let rr = RoleRef {
        api_group: RBAC_GROUP.to_string(),
        kind: "ClusterRole".to_string(),
        name: "admin".to_string(),
    };
    let subj = subject("ServiceAccount", "default", None);
    assert!(validate_role_binding(&role_binding(rr, vec![subj])).is_empty());
}

#[test]
fn user_subject_requires_rbac_apigroup() {
    let rr = RoleRef {
        api_group: RBAC_GROUP.to_string(),
        kind: "ClusterRole".to_string(),
        name: "admin".to_string(),
    };
    // User subject with no apiGroup → NotSupported.
    let subj = subject("User", "alice", None);
    assert!(has(
        &validate_role_binding(&role_binding(rr, vec![subj])),
        "subjects[0].apiGroup"
    ));
}

#[test]
fn unknown_subject_kind_rejected() {
    let rr = RoleRef {
        api_group: RBAC_GROUP.to_string(),
        kind: "ClusterRole".to_string(),
        name: "admin".to_string(),
    };
    let subj = subject("Robot", "r2d2", None);
    assert!(has(
        &validate_role_binding(&role_binding(rr, vec![subj])),
        "subjects[0].kind"
    ));
}

#[test]
fn clusterrolebinding_rejects_role_kind() {
    let rr = RoleRef {
        api_group: RBAC_GROUP.to_string(),
        kind: "Role".to_string(), // only ClusterRole allowed
        name: "admin".to_string(),
    };
    let crb = ClusterRoleBinding {
        type_meta: Default::default(),
        metadata: Default::default(),
        subjects: vec![],
        role_ref: rr,
    };
    assert!(has(&validate_cluster_role_binding(&crb), "roleRef.kind"));
}

#[test]
fn clusterrolebinding_sa_subject_requires_namespace() {
    let rr = RoleRef {
        api_group: RBAC_GROUP.to_string(),
        kind: "ClusterRole".to_string(),
        name: "admin".to_string(),
    };
    // Cluster-scoped binding: a ServiceAccount subject must carry a namespace.
    let subj = subject("ServiceAccount", "default", None);
    let crb = ClusterRoleBinding {
        type_meta: Default::default(),
        metadata: Default::default(),
        subjects: vec![subj],
        role_ref: rr,
    };
    assert!(has(
        &validate_cluster_role_binding(&crb),
        "subjects[0].namespace"
    ));
}

// --- Defaulting (upstream SetDefaults_RoleBinding / _ClusterRoleBinding / _Subject) ---

#[test]
fn rolebinding_omitted_roleref_apigroup_defaults_then_validates() {
    // The sig-auth webhook BeforeEach posts a RoleBinding whose roleRef omits
    // apiGroup: upstream defaults it to the RBAC group, then validation passes.
    let rr = RoleRef {
        api_group: String::new(),
        kind: "Role".to_string(),
        name: "reader".to_string(),
    };
    let mut rb = role_binding(rr, vec![subject("ServiceAccount", "default", None)]);
    default_role_binding(&mut rb);
    assert_eq!(rb.role_ref.api_group, RBAC_GROUP);
    assert!(
        validate_role_binding(&rb).is_empty(),
        "defaulted RoleBinding must validate: {:?}",
        validate_role_binding(&rb)
    );
}

#[test]
fn binding_subject_apigroup_defaults_by_kind() {
    // User/Group subjects default apiGroup to the RBAC group; ServiceAccount
    // keeps the empty/core group (upstream SetDefaults_Subject).
    let rr = RoleRef {
        api_group: String::new(),
        kind: "ClusterRole".to_string(),
        name: "admin".to_string(),
    };
    let mut crb = ClusterRoleBinding {
        type_meta: Default::default(),
        metadata: Default::default(),
        subjects: vec![
            subject("User", "alice", None),
            subject("ServiceAccount", "default", Some("")),
        ],
        role_ref: rr,
    };
    crb.subjects[1].namespace = Some("kube-system".to_string());
    default_cluster_role_binding(&mut crb);
    assert_eq!(crb.role_ref.api_group, RBAC_GROUP);
    assert_eq!(crb.subjects[0].api_group.as_deref(), Some(RBAC_GROUP));
    // ServiceAccount apiGroup stays the empty/core group.
    assert_eq!(crb.subjects[1].api_group.as_deref().unwrap_or(""), "");
    assert!(validate_cluster_role_binding(&crb).is_empty());
}
