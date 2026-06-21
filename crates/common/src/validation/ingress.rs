//! Ingress validation — port of upstream Kubernetes
//! `pkg/apis/networking/validation/validation.go::ValidateIngressSpec`
//! (release-1.35).
//!
//! Scope: the load-bearing field checks — rules-or-defaultBackend, host
//! (DNS, not IP), HTTP paths (required, pathType, absolute path), backends
//! (exactly one of service/resource; service name + port), and `spec.tls`
//! (host (wildcard) DNS names + secretName). The invalid-path-sequence regex
//! checks and rule-host wildcard specifics are left as a follow-up.

use std::net::IpAddr;
use std::str::FromStr;

use crate::resources::ingress::{HTTPIngressPath, Ingress, IngressBackend, IngressSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
use once_cell::sync::Lazy;
use regex::Regex;

const DNS1123_SUBDOMAIN_MAX_LENGTH: usize = 253;

/// Upstream `wildcardDNS1123SubdomainFmt` = `\*\.` + `dns1123SubdomainFmt`.
static WILDCARD_DNS1123_SUBDOMAIN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^\*\.[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")
        .expect("wildcard dns1123 subdomain regex")
});

/// Port of upstream `validation.IsWildcardDNS1123Subdomain`: `*.` followed by a
/// valid DNS-1123 subdomain.
fn is_wildcard_dns1123_subdomain(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > DNS1123_SUBDOMAIN_MAX_LENGTH {
        errs.push(format!(
            "must be no more than {DNS1123_SUBDOMAIN_MAX_LENGTH} characters"
        ));
    }
    if !WILDCARD_DNS1123_SUBDOMAIN_RE.is_match(value) {
        errs.push("a wildcard DNS-1123 subdomain must start with '*.', followed by a valid DNS subdomain, which must consist of lower case alphanumeric characters, '-' or '.' and end with an alphanumeric character (e.g. '*.example.com', regex used for validation is '\\*\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')".to_string());
    }
    errs
}

/// Port of upstream `validateIngressTLS`: each `tls[].hosts[]` entry is a
/// (wildcard) DNS-1123 subdomain, and `secretName` (when set) is a DNS-1123
/// subdomain.
fn validate_ingress_tls(spec: &IngressSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(tls) = spec.tls.as_ref() else {
        return errs;
    };
    for (ti, itls) in tls.iter().enumerate() {
        let tls_path = fld_path.index(ti);
        if let Some(hosts) = &itls.hosts {
            for (hi, host) in hosts.iter().enumerate() {
                let host_path = tls_path.child("hosts").index(hi);
                let msgs = if host.contains('*') {
                    is_wildcard_dns1123_subdomain(host)
                } else {
                    is_dns1123_subdomain(host)
                };
                for msg in msgs {
                    errs.push(Error::invalid(&host_path, host.clone(), msg));
                }
            }
        }
        if let Some(secret) = itls.secret_name.as_deref().filter(|s| !s.is_empty()) {
            for msg in is_dns1123_subdomain(secret) {
                errs.push(Error::invalid(
                    &tls_path.child("secretName"),
                    secret.to_string(),
                    msg,
                ));
            }
        }
    }
    errs
}

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

    errs.extend(validate_ingress_tls(spec, &fld_path.child("tls")));

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

#[cfg(test)]
mod tls_tests {
    use super::*;

    fn tls_errs(json: serde_json::Value) -> Vec<String> {
        let spec: IngressSpec = serde_json::from_value(json).unwrap();
        validate_ingress_tls(&spec, &Path::new("spec").child("tls"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    fn base(tls: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "defaultBackend": {"service": {"name": "s", "port": {"number": 80}}},
            "tls": tls
        })
    }

    #[test]
    fn valid_tls_passes() {
        assert!(tls_errs(base(serde_json::json!([
            {"hosts": ["example.com", "*.example.com"], "secretName": "my-tls"}
        ])))
        .is_empty());
    }

    #[test]
    fn invalid_host_rejected() {
        let e = tls_errs(base(serde_json::json!([{"hosts": ["NotADNSName"]}])));
        assert!(e.iter().any(|m| m.contains("hosts")), "{e:?}");
    }

    #[test]
    fn invalid_wildcard_host_rejected() {
        // bare "*" is not a valid wildcard subdomain (needs "*.")
        let e = tls_errs(base(serde_json::json!([{"hosts": ["*"]}])));
        assert!(e.iter().any(|m| m.contains("wildcard")), "{e:?}");
    }

    #[test]
    fn invalid_secret_name_rejected() {
        let e = tls_errs(base(
            serde_json::json!([{"hosts": ["example.com"], "secretName": "Bad_Name"}]),
        ));
        assert!(e.iter().any(|m| m.contains("secretName")), "{e:?}");
    }
}
