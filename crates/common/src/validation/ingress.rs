//! Ingress validation — port of upstream Kubernetes
//! `pkg/apis/networking/validation/validation.go::ValidateIngressSpec`
//! (release-1.35).
//!
//! Scope: the load-bearing field checks — rules-or-defaultBackend, host
//! (DNS, not IP), HTTP paths (required, pathType, absolute path), and backends
//! (exactly one of service/resource; service name + port). TLS detail, the
//! invalid-path-sequence checks, and wildcard-host specifics are left as a
//! follow-up.

use std::net::IpAddr;
use std::str::FromStr;

use crate::resources::ingress::{HTTPIngressPath, Ingress, IngressBackend, IngressSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};

/// Upstream `IsValidPortName`: IANA_SVC_NAME (DNS-1123 label ≤15 chars with a
/// letter).
fn is_valid_port_name(s: &str) -> bool {
    s.len() <= 15 && is_dns1123_label(s).is_empty() && s.chars().any(|c| c.is_ascii_alphabetic())
}

/// Validate an `IngressBackend`. Mirrors upstream `validateIngressBackend`.
fn validate_backend(backend: &IngressBackend, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let has_service = backend.service.is_some();
    let has_resource = backend.resource.is_some();

    match (has_service, has_resource) {
        (true, true) => {
            errs.push(Error::invalid(
                fld_path,
                String::new(),
                "cannot set both resource and service backends",
            ));
        }
        (false, false) => {
            // A backend must reference something.
            errs.push(Error::required(
                fld_path,
                "must specify a service or resource",
            ));
        }
        (true, false) => {
            let svc = backend.service.as_ref().unwrap();
            if svc.name.is_empty() {
                errs.push(Error::required(
                    &fld_path.child("service").child("name"),
                    "",
                ));
            } else {
                for msg in is_dns1123_label(&svc.name) {
                    errs.push(Error::invalid(
                        &fld_path.child("service").child("name"),
                        svc.name.clone(),
                        msg,
                    ));
                }
            }
            match &svc.port {
                None => errs.push(Error::required(
                    &fld_path.child("service").child("port"),
                    "must specify a port name or number",
                )),
                Some(port) => {
                    let has_name = port.name.as_deref().is_some_and(|n| !n.is_empty());
                    let has_number = port.number.is_some_and(|n| n != 0);
                    if has_name && has_number {
                        errs.push(Error::invalid(
                            fld_path,
                            String::new(),
                            "cannot set both port name & port number",
                        ));
                    } else if has_name {
                        if !is_valid_port_name(port.name.as_deref().unwrap()) {
                            errs.push(Error::invalid(
                                &fld_path.child("service").child("port").child("name"),
                                port.name.clone().unwrap(),
                                "must be an IANA_SVC_NAME",
                            ));
                        }
                    } else if has_number {
                        let n = port.number.unwrap();
                        if !(1..=65535).contains(&n) {
                            errs.push(Error::invalid(
                                &fld_path.child("service").child("port").child("number"),
                                n,
                                "must be between 1 and 65535, inclusive",
                            ));
                        }
                    } else {
                        errs.push(Error::required(
                            &fld_path.child("service").child("port"),
                            "must specify a port name or number",
                        ));
                    }
                }
            }
        }
        (false, true) => {
            let res = backend.resource.as_ref().unwrap();
            if res.kind.is_empty() {
                errs.push(Error::required(
                    &fld_path.child("resource").child("kind"),
                    "",
                ));
            }
            if res.name.is_empty() {
                errs.push(Error::required(
                    &fld_path.child("resource").child("name"),
                    "",
                ));
            }
        }
    }
    errs
}

/// Validate a single HTTP path. Mirrors upstream `validateHTTPIngressPath`.
fn validate_http_path(path: &HTTPIngressPath, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match path.path_type.as_str() {
        "" => {
            errs.push(Error::required(
                &fld_path.child("pathType"),
                "pathType must be specified",
            ));
        }
        "Exact" | "Prefix" => {
            let p = path.path.as_deref().unwrap_or("");
            if !p.starts_with('/') {
                errs.push(Error::invalid(
                    &fld_path.child("path"),
                    p.to_string(),
                    "must be an absolute path",
                ));
            }
        }
        "ImplementationSpecific" => {
            if let Some(p) = path.path.as_deref() {
                if !p.is_empty() && !p.starts_with('/') {
                    errs.push(Error::invalid(
                        &fld_path.child("path"),
                        p.to_string(),
                        "must be an absolute path",
                    ));
                }
            }
        }
        other => errs.push(Error::not_supported(
            &fld_path.child("pathType"),
            other.to_string(),
            &["Exact", "Prefix", "ImplementationSpecific"],
        )),
    }
    errs.extend(validate_backend(&path.backend, &fld_path.child("backend")));
    errs
}

/// Validate an `IngressSpec`. Mirrors upstream `ValidateIngressSpec`.
pub fn validate_ingress_spec(spec: &IngressSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let rules = spec.rules.as_deref().unwrap_or(&[]);
    if rules.is_empty() && spec.default_backend.is_none() {
        errs.push(Error::invalid(
            fld_path,
            String::new(),
            "either `defaultBackend` or `rules` must be specified",
        ));
    }

    if let Some(db) = &spec.default_backend {
        errs.extend(validate_backend(db, &fld_path.child("defaultBackend")));
    }

    for (i, rule) in rules.iter().enumerate() {
        let rule_path = fld_path.child("rules").index(i);
        if let Some(host) = &rule.host {
            if !host.is_empty() {
                if IpAddr::from_str(host).is_ok() {
                    errs.push(Error::invalid(
                        &rule_path.child("host"),
                        host.clone(),
                        "must be a DNS name, not an IP address",
                    ));
                } else if !host.contains('*') {
                    for msg in is_dns1123_subdomain(host) {
                        errs.push(Error::invalid(&rule_path.child("host"), host.clone(), msg));
                    }
                }
            }
        }
        if let Some(http) = &rule.http {
            let http_path = rule_path.child("http");
            if http.paths.is_empty() {
                errs.push(Error::required(&http_path.child("paths"), ""));
            }
            for (j, p) in http.paths.iter().enumerate() {
                errs.extend(validate_http_path(p, &http_path.child("paths").index(j)));
            }
        }
    }

    errs
}

/// Validate a new `Ingress`. Mirrors upstream `ValidateIngress`.
pub fn validate_ingress(ing: &Ingress) -> ErrorList {
    match &ing.spec {
        Some(spec) => validate_ingress_spec(spec, &Path::new("spec")),
        None => Vec::new(),
    }
}
