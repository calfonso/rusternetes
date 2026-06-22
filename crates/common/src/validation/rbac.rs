//! RBAC validation — port of upstream Kubernetes
//! `pkg/apis/rbac/validation/validation.go` (release-1.35).
//!
//! Covers `Role` / `ClusterRole` policy rules, `ClusterRole` aggregationRule
//! selectors, and `RoleBinding` / `ClusterRoleBinding` roleRef + subjects.
//! ObjectMeta (incl. the path-segment RBAC name) is validated separately by the
//! handler (#1087 / #1277, `NameKind::PathSegment`).

use crate::resources::rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_subdomain, validate_label_selector, LabelSelectorValidationOptions,
};

const RBAC_GROUP: &str = "rbac.authorization.k8s.io";
const SERVICE_ACCOUNT_KIND: &str = "ServiceAccount";
const USER_KIND: &str = "User";
const GROUP_KIND: &str = "Group";

/// Port of upstream `ValidateRBACName` → `path.IsValidPathSegmentName`: a name
/// may not be `.`/`..`, nor contain `/` or `%`.
fn rbac_name_errors(name: &str) -> Vec<String> {
    if name == "." || name == ".." {
        return vec![format!("may not be '{}'", name)];
    }
    let mut errs = Vec::new();
    if name.contains('/') {
        errs.push("may not contain '/'".to_string());
    }
    if name.contains('%') {
        errs.push("may not contain '%'".to_string());
    }
    errs
}

fn is_empty(v: &Option<Vec<String>>) -> bool {
    v.as_ref().is_none_or(|x| x.is_empty())
}

/// Port of upstream `ValidatePolicyRule`.
fn validate_policy_rule(rule: &PolicyRule, is_namespaced: bool, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if rule.verbs.is_empty() {
        errs.push(Error::required(
            &fld_path.child("verbs"),
            "verbs must contain at least one value",
        ));
    }

    let non_resource_urls = rule.non_resource_urls.as_deref().unwrap_or(&[]);
    if !non_resource_urls.is_empty() {
        let joined = non_resource_urls.join(",");
        if is_namespaced {
            errs.push(Error::invalid(
                &fld_path.child("nonResourceURLs"),
                joined.clone(),
                "namespaced rules cannot apply to non-resource URLs",
            ));
        }
        if !is_empty(&rule.api_groups)
            || !is_empty(&rule.resources)
            || !is_empty(&rule.resource_names)
        {
            errs.push(Error::invalid(
                &fld_path.child("nonResourceURLs"),
                joined,
                "rules cannot apply to both regular resources and non-resource URLs",
            ));
        }
        return errs;
    }

    if is_empty(&rule.api_groups) {
        errs.push(Error::required(
            &fld_path.child("apiGroups"),
            "resource rules must supply at least one api group",
        ));
    }
    if is_empty(&rule.resources) {
        errs.push(Error::required(
            &fld_path.child("resources"),
            "resource rules must supply at least one resource",
        ));
    }
    errs
}

/// Port of upstream `ValidateRoleBindingSubject`.
fn validate_subject(subject: &Subject, is_namespaced: bool, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if subject.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    }
    let api_group = subject.api_group.as_deref().unwrap_or("");

    match subject.kind.as_str() {
        SERVICE_ACCOUNT_KIND => {
            if !subject.name.is_empty() {
                for msg in is_dns1123_subdomain(&subject.name) {
                    errs.push(Error::invalid(
                        &fld_path.child("name"),
                        subject.name.clone(),
                        msg,
                    ));
                }
            }
            if !api_group.is_empty() {
                errs.push(Error::not_supported(
                    &fld_path.child("apiGroup"),
                    api_group.to_string(),
                    &[""],
                ));
            }
            if !is_namespaced && subject.namespace.as_deref().unwrap_or("").is_empty() {
                errs.push(Error::required(&fld_path.child("namespace"), ""));
            }
        }
        USER_KIND | GROUP_KIND => {
            if api_group != RBAC_GROUP {
                errs.push(Error::not_supported(
                    &fld_path.child("apiGroup"),
                    api_group.to_string(),
                    &[RBAC_GROUP],
                ));
            }
        }
        other => {
            errs.push(Error::not_supported(
                &fld_path.child("kind"),
                other.to_string(),
                &[SERVICE_ACCOUNT_KIND, USER_KIND, GROUP_KIND],
            ));
        }
    }
    errs
}

/// Shared roleRef + subjects validation for the two binding kinds. `role_kinds`
/// is the set of valid `roleRef.kind` values (Role+ClusterRole for a namespaced
/// RoleBinding, ClusterRole only for a ClusterRoleBinding).
fn validate_binding(
    role_ref: &RoleRef,
    subjects: &[Subject],
    role_kinds: &[&str],
    is_namespaced: bool,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let rr = Path::new("roleRef");

    if role_ref.api_group != RBAC_GROUP {
        errs.push(Error::not_supported(
            &rr.child("apiGroup"),
            role_ref.api_group.clone(),
            &[RBAC_GROUP],
        ));
    }
    if !role_kinds.contains(&role_ref.kind.as_str()) {
        errs.push(Error::not_supported(
            &rr.child("kind"),
            role_ref.kind.clone(),
            role_kinds,
        ));
    }
    if role_ref.name.is_empty() {
        errs.push(Error::required(&rr.child("name"), ""));
    } else {
        for msg in rbac_name_errors(&role_ref.name) {
            errs.push(Error::invalid(
                &rr.child("name"),
                role_ref.name.clone(),
                msg,
            ));
        }
    }

    let subjects_path = Path::new("subjects");
    for (i, subject) in subjects.iter().enumerate() {
        errs.extend(validate_subject(
            subject,
            is_namespaced,
            &subjects_path.index(i),
        ));
    }
    errs
}

/// Validate a `Role` on create (upstream `ValidateRole`, minus ObjectMeta).
pub fn validate_role(role: &Role) -> ErrorList {
    let rules_path = Path::new("rules");
    let mut errs: ErrorList = Vec::new();
    for (i, rule) in role.rules.iter().enumerate() {
        errs.extend(validate_policy_rule(rule, true, &rules_path.index(i)));
    }
    errs
}

/// Validate a `ClusterRole` on create (upstream `ValidateClusterRole`, minus
/// ObjectMeta).
pub fn validate_cluster_role(role: &ClusterRole) -> ErrorList {
    let rules_path = Path::new("rules");
    let mut errs: ErrorList = Vec::new();
    for (i, rule) in role.rules.iter().enumerate() {
        errs.extend(validate_policy_rule(rule, false, &rules_path.index(i)));
    }

    if let Some(agg) = &role.aggregation_rule {
        let base = Path::new("aggregationRule").child("clusterRoleSelectors");
        match &agg.cluster_role_selectors {
            None => errs.push(Error::required(
                &base,
                "at least one clusterRoleSelector required if aggregationRule is non-nil",
            )),
            Some(sels) if sels.is_empty() => errs.push(Error::required(
                &base,
                "at least one clusterRoleSelector required if aggregationRule is non-nil",
            )),
            Some(sels) => {
                for (i, sel) in sels.iter().enumerate() {
                    errs.extend(validate_label_selector(
                        sel,
                        LabelSelectorValidationOptions::default(),
                        &base.index(i),
                    ));
                }
            }
        }
    }
    errs
}

/// Validate a `RoleBinding` on create (upstream `ValidateRoleBinding`, minus
/// ObjectMeta). roleRef may point at a Role or ClusterRole; subjects are
/// namespaced.
pub fn validate_role_binding(rb: &RoleBinding) -> ErrorList {
    validate_binding(&rb.role_ref, &rb.subjects, &["Role", "ClusterRole"], true)
}

/// Validate a `ClusterRoleBinding` on create (upstream
/// `ValidateClusterRoleBinding`, minus ObjectMeta). roleRef may only point at a
/// ClusterRole; subjects are cluster-scoped.
pub fn validate_cluster_role_binding(crb: &ClusterRoleBinding) -> ErrorList {
    validate_binding(&crb.role_ref, &crb.subjects, &["ClusterRole"], false)
}

/// Validate a `RoleBinding` on update (upstream `ValidateRoleBindingUpdate`):
/// the create checks plus `roleRef` immutability.
pub fn validate_role_binding_update(new: &RoleBinding, old: &RoleBinding) -> ErrorList {
    let mut errs = validate_role_binding(new);
    if new.role_ref != old.role_ref {
        errs.push(Error::invalid(
            &Path::new("roleRef"),
            new.role_ref.name.clone(),
            "cannot change roleRef",
        ));
    }
    errs
}

/// Validate a `ClusterRoleBinding` on update (upstream
/// `ValidateClusterRoleBindingUpdate`): the create checks plus `roleRef`
/// immutability.
pub fn validate_cluster_role_binding_update(
    new: &ClusterRoleBinding,
    old: &ClusterRoleBinding,
) -> ErrorList {
    let mut errs = validate_cluster_role_binding(new);
    if new.role_ref != old.role_ref {
        errs.push(Error::invalid(
            &Path::new("roleRef"),
            new.role_ref.name.clone(),
            "cannot change roleRef",
        ));
    }
    errs
}

/// Port of upstream `SetDefaults_Subject` (`pkg/apis/rbac/v1/defaults.go`): an
/// omitted `apiGroup` defaults by kind — User/Group → the RBAC group,
/// ServiceAccount (and unknown kinds) keep the empty/core group.
fn default_subject(subject: &mut Subject) {
    if subject.api_group.as_deref().unwrap_or("").is_empty()
        && matches!(subject.kind.as_str(), USER_KIND | GROUP_KIND)
    {
        subject.api_group = Some(RBAC_GROUP.to_string());
    }
}

/// Port of upstream `SetDefaults_RoleBinding` (`pkg/apis/rbac/v1/defaults.go`):
/// an omitted `roleRef.apiGroup` defaults to the RBAC group, and each subject is
/// defaulted per [`default_subject`]. Run *before* validation so a client that
/// omits these fields (e.g. the sig-auth webhook BeforeEach, which posts a
/// RoleBinding with `roleRef.apiGroup` absent) is admitted, then validated.
pub fn default_role_binding(rb: &mut RoleBinding) {
    if rb.role_ref.api_group.is_empty() {
        rb.role_ref.api_group = RBAC_GROUP.to_string();
    }
    for subject in &mut rb.subjects {
        default_subject(subject);
    }
}

/// Port of upstream `SetDefaults_ClusterRoleBinding` + `SetDefaults_Subject`.
pub fn default_cluster_role_binding(crb: &mut ClusterRoleBinding) {
    if crb.role_ref.api_group.is_empty() {
        crb.role_ref.api_group = RBAC_GROUP.to_string();
    }
    for subject in &mut crb.subjects {
        default_subject(subject);
    }
}

#[cfg(test)]
mod update_tests {
    use super::*;

    fn rb(role_ref_name: &str) -> RoleBinding {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "rb", "namespace": "default"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role_ref_name},
            "subjects": [{"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}]
        }))
        .unwrap()
    }

    fn crb(role_ref_name: &str) -> ClusterRoleBinding {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "crb"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role_ref_name},
            "subjects": [{"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}]
        }))
        .unwrap()
    }

    #[test]
    fn unchanged_role_ref_passes() {
        assert!(validate_role_binding_update(&rb("admin"), &rb("admin")).is_empty());
        assert!(validate_cluster_role_binding_update(&crb("admin"), &crb("admin")).is_empty());
    }

    #[test]
    fn changing_role_ref_rejected() {
        let e = validate_role_binding_update(&rb("editor"), &rb("admin"));
        assert!(
            e.iter().any(|x| x.detail == "cannot change roleRef"),
            "{e:?}"
        );
        let ce = validate_cluster_role_binding_update(&crb("editor"), &crb("admin"));
        assert!(
            ce.iter().any(|x| x.detail == "cannot change roleRef"),
            "{ce:?}"
        );
    }
}
