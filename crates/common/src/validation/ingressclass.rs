//! IngressClass validation — port of upstream Kubernetes
//! `pkg/apis/networking/validation/validation.go::ValidateIngressClass`
//! (release-1.35).
//!
//! Covers `spec.controller` (length + domain-prefixed path) and
//! `spec.parameters` (typed local object reference + scope/namespace coupling).
//! ObjectMeta is validated separately (#1087 / #1277).
//!
//! The path component of `controller` is checked for the domain-prefixed
//! *structure* (host is a DNS-1123 subdomain, both segments non-empty); the
//! exact upstream `httpPathRegexp` on the trailing path segment is not
//! replicated (rarely material).

use crate::resources::ingressclass::{IngressClass, IngressClassParametersReference};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};

const MAX_CONTROLLER_LEN: usize = 250;
const SCOPE_NAMESPACE: &str = "Namespace";
const SCOPE_CLUSTER: &str = "Cluster";

/// Port of upstream `path.IsValidPathSegmentName`: not `.`/`..`, no `/` or `%`.
fn path_segment_errors(name: &str) -> Vec<String> {
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

/// Port of upstream `IsDomainPrefixedPath` (structure + host subdomain).
fn validate_domain_prefixed_path(value: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if value.is_empty() {
        errs.push(Error::required(fld_path, ""));
        return errs;
    }
    let segments: Vec<&str> = value.splitn(2, '/').collect();
    if segments.len() != 2 || segments[0].is_empty() || segments[1].is_empty() {
        errs.push(Error::invalid(
            fld_path,
            value.to_string(),
            "must be a domain-prefixed path (such as \"acme.io/foo\")",
        ));
        return errs;
    }
    for msg in is_dns1123_subdomain(segments[0]) {
        errs.push(Error::invalid(fld_path, segments[0].to_string(), msg));
    }
    errs
}

/// Port of upstream `validateIngressClassParametersReference`.
fn validate_parameters(params: &IngressClassParametersReference, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // typed local object reference
    if let Some(api_group) = &params.api_group {
        for msg in is_dns1123_subdomain(api_group) {
            errs.push(Error::invalid(
                &fld_path.child("apiGroup"),
                api_group.clone(),
                msg,
            ));
        }
    }
    if params.kind.is_empty() {
        errs.push(Error::required(&fld_path.child("kind"), ""));
    } else {
        for msg in path_segment_errors(&params.kind) {
            errs.push(Error::invalid(
                &fld_path.child("kind"),
                params.kind.clone(),
                msg,
            ));
        }
    }
    if params.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    } else {
        for msg in path_segment_errors(&params.name) {
            errs.push(Error::invalid(
                &fld_path.child("name"),
                params.name.clone(),
                msg,
            ));
        }
    }

    // scope + namespace coupling
    let Some(scope) = params.scope.as_deref() else {
        errs.push(Error::required(&fld_path.child("scope"), ""));
        return errs;
    };
    if scope != SCOPE_NAMESPACE && scope != SCOPE_CLUSTER {
        errs.push(Error::not_supported(
            &fld_path.child("scope"),
            scope.to_string(),
            &[SCOPE_CLUSTER, SCOPE_NAMESPACE],
        ));
        return errs;
    }
    if scope == SCOPE_NAMESPACE {
        match params.namespace.as_deref() {
            None => errs.push(Error::required(
                &fld_path.child("namespace"),
                "`parameters.scope` is set to 'Namespace'",
            )),
            Some(ns) => {
                for msg in is_dns1123_label(ns) {
                    errs.push(Error::invalid(
                        &fld_path.child("namespace"),
                        ns.to_string(),
                        msg,
                    ));
                }
            }
        }
    } else if scope == SCOPE_CLUSTER && params.namespace.is_some() {
        errs.push(Error::forbidden(
            &fld_path.child("namespace"),
            "`parameters.scope` is set to 'Cluster'",
        ));
    }

    errs
}

/// Validate an `IngressClass` on create. Mirrors upstream `ValidateIngressClass`
/// minus ObjectMeta.
pub fn validate_ingress_class(ic: &IngressClass) -> ErrorList {
    let spec_path = Path::new("spec");
    let controller_path = spec_path.child("controller");
    let mut errs: ErrorList = Vec::new();

    // Go's IngressClassSpec is a non-pointer struct (always present); a missing
    // spec here means a missing controller → upstream's required error.
    let Some(spec) = &ic.spec else {
        errs.push(Error::required(&controller_path, ""));
        return errs;
    };

    if spec.controller.len() > MAX_CONTROLLER_LEN {
        errs.push(Error::too_long(&controller_path, MAX_CONTROLLER_LEN));
    }
    errs.extend(validate_domain_prefixed_path(
        &spec.controller,
        &controller_path,
    ));

    if let Some(params) = &spec.parameters {
        errs.extend(validate_parameters(params, &spec_path.child("parameters")));
    }

    errs
}

/// Validate an IngressClass update — upstream `ValidateIngressClassUpdate`
/// (pkg/apis/networking/validation): `spec.controller` is immutable, plus full
/// re-validation of the new object.
pub fn validate_ingress_class_update(new_ic: &IngressClass, old_ic: &IngressClass) -> ErrorList {
    let mut errs = validate_ingress_class(new_ic);
    let new_ctrl = new_ic
        .spec
        .as_ref()
        .map(|s| s.controller.as_str())
        .unwrap_or("");
    let old_ctrl = old_ic
        .spec
        .as_ref()
        .map(|s| s.controller.as_str())
        .unwrap_or("");
    if new_ctrl != old_ctrl {
        errs.push(Error::invalid(
            &Path::new("spec").child("controller"),
            new_ctrl.to_string(),
            "field is immutable",
        ));
    }
    errs
}
