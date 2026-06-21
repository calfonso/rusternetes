//! FlowSchema (APF) validation — port of upstream Kubernetes
//! `pkg/apis/flowcontrol/validation/validation.go` (release-1.35).
//!
//! Covers `matchingPrecedence` bounds + the `exempt`-name rule,
//! `priorityLevelConfiguration.name`, and each rule's subjects +
//! resourceRules/nonResourceRules. ObjectMeta is validated separately
//! (#1087 / #1277). `kind` / `distinguisherMethod.type` are typed enums, so
//! those upstream `NotSupported` checks are enforced by the type system.
//! `ValidateNonResourceURLPath` is approximated (must start with `/`).

use crate::resources::flowcontrol::{
    FlowSchema, FlowSchemaSubject, NonResourcePolicyRule, PolicyRulesWithSubjects,
    ResourcePolicyRule, SubjectKind,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};

const MAX_MATCHING_PRECEDENCE: i32 = 10000;
const SUPPORTED_VERBS: [&str; 9] = [
    "get",
    "list",
    "create",
    "update",
    "delete",
    "deletecollection",
    "patch",
    "watch",
    "proxy",
];

fn has_wildcard(v: &[String]) -> bool {
    v.iter().any(|s| s == "*")
}

/// Verb-list validation shared by resource + non-resource rules.
fn validate_verbs(verbs: &[String], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let vp = fld_path.child("verbs");
    if verbs.is_empty() {
        errs.push(Error::required(
            &vp,
            "verbs must contain at least one value",
        ));
    } else if has_wildcard(verbs) {
        if verbs.len() > 1 {
            errs.push(Error::invalid(
                &vp,
                verbs.to_vec(),
                "if '*' is present, must not specify other verbs",
            ));
        }
    } else if !verbs.iter().all(|v| SUPPORTED_VERBS.contains(&v.as_str())) {
        errs.push(Error::not_supported(&vp, verbs.to_vec(), &SUPPORTED_VERBS));
    }
    errs
}

fn validate_subject(subject: &FlowSchemaSubject, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let forbid = |errs: &mut ErrorList, present: bool, child: &str, msg: &str| {
        if present {
            errs.push(Error::forbidden(&fld_path.child(child), msg));
        }
    };
    match subject.kind {
        SubjectKind::ServiceAccount => {
            let sp = fld_path.child("serviceAccount");
            match &subject.service_account {
                None => errs.push(Error::required(
                    &sp,
                    "serviceAccount is required when subject kind is 'ServiceAccount'",
                )),
                Some(sa) => {
                    if sa.name.is_empty() {
                        errs.push(Error::required(&sp.child("name"), ""));
                    } else if sa.name != "*" {
                        for msg in is_dns1123_subdomain(&sa.name) {
                            errs.push(Error::invalid(&sp.child("name"), sa.name.clone(), msg));
                        }
                    }
                    if sa.namespace.is_empty() {
                        errs.push(Error::required(
                            &sp.child("namespace"),
                            "must specify namespace for service account",
                        ));
                    } else {
                        for msg in is_dns1123_label(&sa.namespace) {
                            errs.push(Error::invalid(
                                &sp.child("namespace"),
                                sa.namespace.clone(),
                                msg,
                            ));
                        }
                    }
                }
            }
            forbid(
                &mut errs,
                subject.user.is_some(),
                "user",
                "user is forbidden when subject kind is not 'User'",
            );
            forbid(
                &mut errs,
                subject.group.is_some(),
                "group",
                "group is forbidden when subject kind is not 'Group'",
            );
        }
        SubjectKind::User => {
            let up = fld_path.child("user");
            match &subject.user {
                None => errs.push(Error::required(
                    &up,
                    "user is required when subject kind is 'User'",
                )),
                Some(u) if u.name.is_empty() => errs.push(Error::required(&up.child("name"), "")),
                Some(_) => {}
            }
            forbid(
                &mut errs,
                subject.service_account.is_some(),
                "serviceAccount",
                "serviceAccount is forbidden when subject kind is not 'ServiceAccount'",
            );
            forbid(
                &mut errs,
                subject.group.is_some(),
                "group",
                "group is forbidden when subject kind is not 'Group'",
            );
        }
        SubjectKind::Group => {
            let gp = fld_path.child("group");
            match &subject.group {
                None => errs.push(Error::required(
                    &gp,
                    "group is required when subject kind is 'Group'",
                )),
                Some(g) if g.name.is_empty() => errs.push(Error::required(&gp.child("name"), "")),
                Some(_) => {}
            }
            forbid(
                &mut errs,
                subject.service_account.is_some(),
                "serviceAccount",
                "serviceAccount is forbidden when subject kind is not 'ServiceAccount'",
            );
            forbid(
                &mut errs,
                subject.user.is_some(),
                "user",
                "user is forbidden when subject kind is not 'User'",
            );
        }
    }
    errs
}

fn validate_resource_rule(rule: &ResourcePolicyRule, fld_path: &Path) -> ErrorList {
    let mut errs = validate_verbs(&rule.verbs, fld_path);
    let ap = fld_path.child("apiGroups");
    if rule.api_groups.is_empty() {
        errs.push(Error::required(
            &ap,
            "resource rules must supply at least one api group",
        ));
    } else if rule.api_groups.len() > 1 && has_wildcard(&rule.api_groups) {
        errs.push(Error::invalid(
            &ap,
            rule.api_groups.to_vec(),
            "if '*' is present, must not specify other api groups",
        ));
    }
    let rp = fld_path.child("resources");
    if rule.resources.is_empty() {
        errs.push(Error::required(
            &rp,
            "resource rules must supply at least one resource",
        ));
    } else if rule.resources.len() > 1 && has_wildcard(&rule.resources) {
        errs.push(Error::invalid(
            &rp,
            rule.resources.to_vec(),
            "if '*' is present, must not specify other resources",
        ));
    }
    let nsp = fld_path.child("namespaces");
    let namespaces = rule.namespaces.as_deref().unwrap_or(&[]);
    let cluster_scope = rule.cluster_scope.unwrap_or(false);
    if namespaces.is_empty() && !cluster_scope {
        errs.push(Error::required(
            &nsp,
            "resource rules that are not cluster scoped must supply at least one namespace",
        ));
    } else if has_wildcard(namespaces) {
        if namespaces.len() > 1 {
            errs.push(Error::invalid(
                &nsp,
                namespaces.to_vec(),
                "if '*' is present, must not specify other namespaces",
            ));
        }
    } else {
        for (i, ns) in namespaces.iter().enumerate() {
            for msg in is_dns1123_label(ns) {
                errs.push(Error::invalid(&nsp.index(i), ns.clone(), msg));
            }
        }
    }
    errs
}

fn validate_non_resource_rule(rule: &NonResourcePolicyRule, fld_path: &Path) -> ErrorList {
    let mut errs = validate_verbs(&rule.verbs, fld_path);
    let up = fld_path.child("nonResourceURLs");
    if rule.non_resource_urls.is_empty() {
        errs.push(Error::required(
            &up,
            "nonResourceURLs must contain at least one value",
        ));
    } else if has_wildcard(&rule.non_resource_urls) {
        if rule.non_resource_urls.len() > 1 {
            errs.push(Error::invalid(
                &up,
                rule.non_resource_urls.to_vec(),
                "if '*' is present, must not specify other non-resource URLs",
            ));
        }
    } else {
        for (i, url) in rule.non_resource_urls.iter().enumerate() {
            if !url.starts_with('/') {
                errs.push(Error::invalid(
                    &up.index(i),
                    url.clone(),
                    "must be an absolute path",
                ));
            }
        }
    }
    errs
}

fn validate_rule(rule: &PolicyRulesWithSubjects, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if rule.subjects.is_empty() {
        errs.push(Error::required(
            &fld_path.child("subjects"),
            "subjects must contain at least one value",
        ));
    } else {
        for (i, s) in rule.subjects.iter().enumerate() {
            errs.extend(validate_subject(s, &fld_path.child("subjects").index(i)));
        }
    }
    let res = rule.resource_rules.as_deref().unwrap_or(&[]);
    let nonres = rule.non_resource_rules.as_deref().unwrap_or(&[]);
    if res.is_empty() && nonres.is_empty() {
        errs.push(Error::required(
            fld_path,
            "at least one of resourceRules and nonResourceRules has to be non-empty",
        ));
    }
    for (i, r) in res.iter().enumerate() {
        errs.extend(validate_resource_rule(
            r,
            &fld_path.child("resourceRules").index(i),
        ));
    }
    for (i, r) in nonres.iter().enumerate() {
        errs.extend(validate_non_resource_rule(
            r,
            &fld_path.child("nonResourceRules").index(i),
        ));
    }
    errs
}

/// Validate a `FlowSchema` on create. Mirrors upstream `ValidateFlowSchemaSpec`
/// minus ObjectMeta.
pub fn validate_flow_schema(fs: &FlowSchema) -> ErrorList {
    let spec_path = Path::new("spec");
    let mut errs: ErrorList = Vec::new();
    let spec = &fs.spec;
    let mp_path = spec_path.child("matchingPrecedence");

    if spec.matching_precedence <= 0 {
        errs.push(Error::invalid(
            &mp_path,
            spec.matching_precedence,
            "must be a positive value",
        ));
    }
    if spec.matching_precedence > MAX_MATCHING_PRECEDENCE {
        errs.push(Error::invalid(
            &mp_path,
            spec.matching_precedence,
            format!("must not be greater than {}", MAX_MATCHING_PRECEDENCE),
        ));
    }
    if spec.matching_precedence == 1 && fs.metadata.name != "exempt" {
        errs.push(Error::invalid(
            &mp_path,
            spec.matching_precedence,
            "only the schema named 'exempt' may have matchingPrecedence 1",
        ));
    }

    let plc_name_path = spec_path.child("priorityLevelConfiguration").child("name");
    let plc_name = &spec.priority_level_configuration.name;
    if plc_name.is_empty() {
        errs.push(Error::required(
            &plc_name_path,
            "must reference a priority level",
        ));
    } else {
        for msg in is_dns1123_subdomain(plc_name) {
            errs.push(Error::invalid(&plc_name_path, plc_name.clone(), msg));
        }
    }

    if let Some(rules) = &spec.rules {
        for (i, rule) in rules.iter().enumerate() {
            errs.extend(validate_rule(rule, &spec_path.child("rules").index(i)));
        }
    }

    errs
}
