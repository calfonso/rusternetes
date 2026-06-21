//! Validation for `certificates.k8s.io` CertificateSigningRequest, ported from
//! upstream `pkg/apis/certificates/validation/validation.go`
//! (`ValidateCertificateSigningRequestCreate`) and the shared
//! `ValidateSignerName` from `pkg/apis/core/validation/names.go`.
//!
//! Scope: the create-time field checks — `request` is parsed as a PEM-encoded
//! PKCS#10 certificate request (upstream `validateCSR`'s `ParseCSR` step),
//! `usages` required + no duplicates, `signerName` format (incl. the v1
//! rejection of the legacy signer), and `expirationSeconds >= 600`.
//!
//! `usages` is a typed enum in the resource model, so an unrecognized usage
//! *string* is already rejected at decode time (upstream's `allValidUsages`
//! `NotSupported` check). The `request` self-signature verification
//! (`CheckSignature`) is the one remaining piece of upstream `validateCSR`
//! not yet ported — the structural PKCS#10 parse is done here.

use crate::resources::certificates::{CertificateSigningRequest, KeyUsage};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
use base64::Engine;
use x509_parser::prelude::FromDer;

/// Structurally validate `csr.spec.request` — upstream `validateCSR`'s
/// `certificates.ParseCSR` step. The field carries the base64 of a
/// PEM-encoded PKCS#10 certificate request. Returns `Some(detail)` describing
/// the first parse failure, or `None` when the request parses.
///
/// Self-signature verification (`CheckSignature`) is not performed here.
fn parse_csr_request_error(request: &str) -> Option<String> {
    if request.is_empty() {
        return Some("must contain a PEM-encoded PKCS#10 certificate signing request".to_string());
    }
    let pem_bytes = match base64::engine::general_purpose::STANDARD.decode(request) {
        Ok(b) => b,
        Err(e) => return Some(format!("error parsing request: invalid base64: {e}")),
    };
    let parsed = match pem::parse(&pem_bytes) {
        Ok(p) => p,
        Err(e) => return Some(format!("error parsing request: invalid PEM block: {e}")),
    };
    if parsed.tag() != "CERTIFICATE REQUEST" {
        return Some(format!(
            "error parsing request: PEM block type must be CERTIFICATE REQUEST, got {:?}",
            parsed.tag()
        ));
    }
    match x509_parser::certification_request::X509CertificationRequest::from_der(parsed.contents())
    {
        Ok(_) => None,
        Err(e) => Some(format!("error parsing request: {e}")),
    }
}

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

    // request: upstream validateCSR parses the PEM-encoded PKCS#10 request.
    // We perform the structural parse (base64 → PEM "CERTIFICATE REQUEST" block
    // → PKCS#10 DER). Self-signature verification (CheckSignature) is not yet
    // performed.
    if let Some(detail) = parse_csr_request_error(&csr.spec.request) {
        errs.push(Error::invalid(
            &spec.child("request"),
            "<csr request>".to_string(),
            detail,
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

/// Validate CSR `status.conditions` — the self-contained `validateConditions`
/// rules from upstream (pkg/apis/certificates/validation): a condition `type`
/// must be non-empty; `status` must be one of True/False/Unknown, and for the
/// Approved/Denied/Failed types only `True`; and a CSR may not carry both an
/// Approved and a Denied condition.
///
/// The diff-based rules (may-not-remove/modify existing Approved/Denied/Failed)
/// are not covered here — those need the stored object.
pub fn validate_csr_status_conditions(
    conditions: &[crate::resources::certificates::CertificateSigningRequestCondition],
) -> ErrorList {
    let mut errs = ErrorList::new();
    let path = Path::new("status").child("conditions");
    let mut has_approved = false;
    let mut has_denied = false;
    for (i, c) in conditions.iter().enumerate() {
        let cp = path.index(i);
        if c.type_.is_empty() {
            errs.push(Error::required(&cp.child("type"), ""));
        }
        let true_only = matches!(c.type_.as_str(), "Approved" | "Denied" | "Failed");
        let allowed: &[&str] = if true_only {
            &["True"]
        } else {
            &["True", "False", "Unknown"]
        };
        if c.status.is_empty() {
            errs.push(Error::required(&cp.child("status"), ""));
        } else if !allowed.contains(&c.status.as_str()) {
            errs.push(Error::not_supported(
                &cp.child("status"),
                c.status.clone(),
                allowed,
            ));
        }
        match c.type_.as_str() {
            "Approved" => {
                has_approved = true;
                if has_denied {
                    errs.push(Error::invalid(
                        &path,
                        "Approved".to_string(),
                        "Approved and Denied conditions are mutually exclusive",
                    ));
                }
            }
            "Denied" => {
                has_denied = true;
                if has_approved {
                    errs.push(Error::invalid(
                        &path,
                        "Denied".to_string(),
                        "Approved and Denied conditions are mutually exclusive",
                    ));
                }
            }
            _ => {}
        }
    }
    errs
}

/// PEM-validate `status.certificate` — upstream `validateCertificate`. The field
/// carries the base64 of the PEM data (Go `[]byte` JSON marshalling). Returns
/// `Some(detail)` on the first failure, `None` when valid or unset/empty.
fn status_certificate_error(certificate: Option<&str>) -> Option<String> {
    let cert = certificate?;
    if cert.is_empty() {
        return None;
    }
    let pem_bytes = match base64::engine::general_purpose::STANDARD.decode(cert) {
        Ok(b) => b,
        Err(e) => return Some(format!("invalid base64: {e}")),
    };
    let blocks = match pem::parse_many(&pem_bytes) {
        Ok(b) => b,
        Err(e) => return Some(format!("invalid PEM data: {e}")),
    };
    for block in &blocks {
        if block.tag() != "CERTIFICATE" {
            return Some(format!(
                "only CERTIFICATE PEM blocks are allowed, found {:?}",
                block.tag()
            ));
        }
        if block.headers().iter().next().is_some() {
            return Some("no PEM block headers are permitted".to_string());
        }
        if x509_parser::certificate::X509Certificate::from_der(block.contents()).is_err() {
            return Some(
                "found CERTIFICATE PEM block containing an invalid certificate".to_string(),
            );
        }
    }
    // Upstream requires at least one CERTIFICATE block once non-empty data is
    // present (a `pem.Decode` loop that found nothing).
    if blocks.is_empty() {
        return Some("must contain at least one CERTIFICATE PEM block".to_string());
    }
    None
}

/// Validate CSR `status.certificate` — upstream `validateCertificate`, used on
/// the `/status` and `/approval` update paths. When set, the certificate must
/// be one or more PEM `CERTIFICATE` blocks (no other block type, no PEM
/// headers), each a parseable X.509 certificate, with at least one block.
pub fn validate_csr_status_certificate(certificate: Option<&str>) -> ErrorList {
    let mut errs = ErrorList::new();
    if let Some(detail) = status_certificate_error(certificate) {
        errs.push(Error::invalid(
            &Path::new("status").child("certificate"),
            "<certificate data>".to_string(),
            detail,
        ));
    }
    errs
}

#[cfg(test)]
mod status_certificate_tests {
    use super::status_certificate_error;
    use base64::Engine;

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    /// A real self-signed cert PEM via rcgen.
    fn valid_cert_pem() -> String {
        rcgen::generate_simple_self_signed(vec!["test.example.com".to_string()])
            .expect("cert")
            .cert
            .pem()
    }

    #[test]
    fn unset_or_empty_passes() {
        assert_eq!(status_certificate_error(None), None);
        assert_eq!(status_certificate_error(Some("")), None);
    }

    #[test]
    fn valid_certificate_passes() {
        let data = b64(&valid_cert_pem());
        assert_eq!(status_certificate_error(Some(&data)), None);
    }

    #[test]
    fn two_concatenated_certs_pass() {
        let chain = format!("{}{}", valid_cert_pem(), valid_cert_pem());
        let data = b64(&chain);
        assert_eq!(status_certificate_error(Some(&data)), None);
    }

    #[test]
    fn garbage_base64_rejected() {
        assert!(status_certificate_error(Some("not base64!!!")).is_some());
    }

    #[test]
    fn non_pem_payload_rejected() {
        // valid base64, but no PEM blocks inside.
        let data = b64("definitely not a pem block");
        let err = status_certificate_error(Some(&data)).expect("err");
        assert!(err.contains("at least one CERTIFICATE PEM block"), "{err}");
    }

    #[test]
    fn wrong_block_type_rejected() {
        let data =
            b64("-----BEGIN CERTIFICATE REQUEST-----\nMIIB\n-----END CERTIFICATE REQUEST-----\n");
        let err = status_certificate_error(Some(&data)).expect("err");
        assert!(
            err.contains("only CERTIFICATE PEM blocks are allowed"),
            "{err}"
        );
    }
}
