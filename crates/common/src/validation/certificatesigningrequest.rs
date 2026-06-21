//! Validation for `certificates.k8s.io` CertificateSigningRequest, ported from
//! upstream `pkg/apis/certificates/validation/validation.go`
//! (`ValidateCertificateSigningRequestCreate`) and the shared
//! `ValidateSignerName` from `pkg/apis/core/validation/names.go`.
//!
//! Scope: the crypto-free create-time field checks — `request` non-empty,
//! `usages` required + no duplicates, `signerName` format (incl. the v1
//! rejection of the legacy signer), and `expirationSeconds >= 600`.
//!
//! `usages` is a typed enum in the resource model, so an unrecognized usage
//! *string* is already rejected at decode time (upstream's `allValidUsages`
//! `NotSupported` check). The full upstream `validateCSR` — PKCS#10 PEM parse
//! plus `CheckSignature` — is **not** ported here (needs x509/PKCS#10 parsing
//! + signature verification); we only enforce that `request` is present.

use crate::resources::certificates::{CertificateSigningRequest, KeyUsage};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};

// Mirror upstream apimachinery length constants.
const DNS1123_SUBDOMAIN_MAX_LENGTH: usize = 253;
const DNS1123_LABEL_MAX_LENGTH: usize = 63;

/// `certificates.LegacyUnknownSignerName` — not allowed via the v1 create API.
const LEGACY_UNKNOWN_SIGNER_NAME: &str = "kubernetes.io/legacy-unknown";

fn usage_str(u: &KeyUsage) -> String {
    serde_json::to_value(u)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

/// Port of upstream `ValidateSignerName` (pkg/apis/core/validation/names.go):
/// `signerName` must be a fully qualified `domain.com/path` — domain a dotted
/// set of DNS1123 labels (≥2 segments), path a dotted set of DNS1123
/// subdomains, with the documented length caps.
pub fn validate_signer_name(path: &Path, signer_name: &str) -> ErrorList {
    let mut el = ErrorList::new();
    if signer_name.is_empty() {
        el.push(Error::required(path, ""));
        return el;
    }

    let segments: Vec<&str> = signer_name.split('/').collect();
    if segments.len() != 2 {
        el.push(Error::invalid(
            path,
            signer_name.to_string(),
            "must be a fully qualified domain and path of the form 'example.com/signer-name'",
        ));
        return el;
    }

    let domain = segments[0];
    let signer_path = segments[1];

    if domain.len() > DNS1123_SUBDOMAIN_MAX_LENGTH {
        el.push(Error::too_long(path, DNS1123_SUBDOMAIN_MAX_LENGTH));
    }
    for lbl in domain.split('.') {
        let errs = is_dns1123_label(lbl);
        if !errs.is_empty() {
            for err in errs {
                el.push(Error::invalid(
                    path,
                    domain.to_string(),
                    format!("validating label \"{lbl}\": {err}"),
                ));
            }
            break;
        }
    }
    if domain.split('.').count() < 2 {
        el.push(Error::invalid(
            path,
            domain.to_string(),
            "should be a domain with at least two segments separated by dots",
        ));
    }

    for lbl in signer_path.split('.') {
        let errs = is_dns1123_subdomain(lbl);
        if !errs.is_empty() {
            for err in errs {
                el.push(Error::invalid(
                    path,
                    signer_path.to_string(),
                    format!("validating label \"{lbl}\": {err}"),
                ));
            }
            break;
        }
    }

    let max_path_segment_length = DNS1123_SUBDOMAIN_MAX_LENGTH + DNS1123_LABEL_MAX_LENGTH + 1;
    let max_signer_name_length = DNS1123_SUBDOMAIN_MAX_LENGTH + max_path_segment_length + 1;
    if signer_name.len() > max_signer_name_length {
        el.push(Error::too_long(path, max_signer_name_length));
    }

    el
}

/// Validate a CertificateSigningRequest on create — upstream
/// `ValidateCertificateSigningRequestCreate`.
pub fn validate_certificate_signing_request_create(csr: &CertificateSigningRequest) -> ErrorList {
    let mut errs = ErrorList::new();
    let spec = Path::new("spec");

    // request: full PKCS#10 parse + signature check (upstream validateCSR) is
    // deferred; enforce presence so an empty request is rejected as upstream's
    // parse would.
    if csr.spec.request.is_empty() {
        errs.push(Error::invalid(
            &spec.child("request"),
            String::new(),
            "must contain a PEM-encoded PKCS#10 certificate signing request",
        ));
    }

    if csr.spec.usages.is_empty() {
        errs.push(Error::required(&spec.child("usages"), ""));
    }
    // Duplicate usages (the enum already constrains the value set at decode).
    let mut seen: Vec<&KeyUsage> = Vec::new();
    for (i, usage) in csr.spec.usages.iter().enumerate() {
        if seen.contains(&usage) {
            errs.push(Error::duplicate(
                &spec.child("usages").index(i),
                usage_str(usage),
            ));
        } else {
            seen.push(usage);
        }
    }

    // signerName: the legacy signer is not allowed via the v1 create API;
    // otherwise enforce the standard format.
    if csr.spec.signer_name == LEGACY_UNKNOWN_SIGNER_NAME {
        errs.push(Error::invalid(
            &spec.child("signerName"),
            csr.spec.signer_name.clone(),
            "the legacy signerName is not allowed via this API version",
        ));
    } else {
        errs.extend(validate_signer_name(
            &spec.child("signerName"),
            &csr.spec.signer_name,
        ));
    }

    if let Some(exp) = csr.spec.expiration_seconds {
        if exp < 600 {
            errs.push(Error::invalid(
                &spec.child("expirationSeconds"),
                exp,
                "may not specify a duration less than 600 seconds (10 minutes)",
            ));
        }
    }

    errs
}
