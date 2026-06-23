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
//! `NotSupported` check). The `request` self-signature is verified
//! (`CheckSignature`) after the structural PKCS#10 parse, mirroring upstream
//! `validateCSR`.

use crate::resources::certificates::{
    CertificateSigningRequest, CertificateSigningRequestCondition, KeyUsage,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
use base64::Engine;
use x509_parser::prelude::FromDer;

/// The Approved/Denied/Failed condition types — upstream
/// `certificates.CertificateApproved` / `CertificateDenied` / `CertificateFailed`.
const APPROVED: &str = "Approved";
const DENIED: &str = "Denied";
const FAILED: &str = "Failed";

/// Structurally validate `csr.spec.request` — upstream `validateCSR`'s
/// `certificates.ParseCSR` step. The field carries the base64 of a
/// PEM-encoded PKCS#10 certificate request. Returns `Some(detail)` describing
/// the first parse failure, or `None` when the request parses and its
/// self-signature verifies.
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
        // Upstream `validateCSR` parses the PKCS#10 request and then calls
        // `csr.CheckSignature()` to confirm it is self-signed by the embedded
        // public key. Parsing alone is not enough — verify the signature too.
        Ok((_, csr)) => match csr.verify_signature() {
            Ok(()) => None,
            Err(e) => Some(format!(
                "error parsing request: signature verification failed: {e}"
            )),
        },
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
/// `ValidateCertificateSigningRequestCreate`. On create none of the
/// `certificateValidationOptions` are set: empty/duplicate condition types and
/// arbitrary certificates are all rejected, so this is the field validation
/// with the strict (all-`false`) options.
pub fn validate_certificate_signing_request_create(csr: &CertificateSigningRequest) -> ErrorList {
    validate_certificate_signing_request_with_opts(csr, &UpdateValidationOptions::strict())
}

/// Compatibility flags mirrored from upstream `certificateValidationOptions`
/// (only the ones `validateConditions` consults). On create all flags are
/// `false`; on update they are loosened to tolerate pre-existing data in the
/// old object (`getValidationOptions`).
#[derive(Default, Clone, Copy)]
struct ConditionValidationOptions {
    /// `allowEmptyConditionType` — old object already had an empty `type`.
    allow_empty_condition_type: bool,
    /// `allowBothApprovedAndDenied` — old object already had both.
    allow_both_approved_and_denied: bool,
    /// `allowDuplicateConditionTypes` — old object already had duplicates.
    allow_duplicate_condition_types: bool,
}

/// Borrow `csr.status.conditions` as a slice (empty when status/conditions unset).
fn conditions_of(csr: &CertificateSigningRequest) -> &[CertificateSigningRequestCondition] {
    csr.status
        .as_ref()
        .and_then(|s| s.conditions.as_deref())
        .unwrap_or(&[])
}

/// Borrow `csr.status.certificate` (None when status/certificate unset).
fn status_certificate(csr: &CertificateSigningRequest) -> Option<&str> {
    csr.status.as_ref().and_then(|s| s.certificate.as_deref())
}

/// Port of upstream `validateConditions` (validation.go:228). For each
/// condition: `type` must be non-empty (unless `allow_empty_condition_type`);
/// `status` must be one of True/False/Unknown, and for Approved/Denied/Failed
/// only `True`; Approved+Denied are mutually exclusive (unless
/// `allow_both_approved_and_denied`); and duplicate condition types are
/// rejected (unless `allow_duplicate_condition_types`).
fn validate_conditions(
    csr: &CertificateSigningRequest,
    opts: &ConditionValidationOptions,
) -> ErrorList {
    validate_conditions_slice(conditions_of(csr), opts)
}

/// Core of `validateConditions`, operating on a borrowed condition slice so it
/// can be shared between the create/update CSR validators and the
/// handler-facing [`validate_csr_status_conditions`].
fn validate_conditions_slice(
    conditions: &[CertificateSigningRequestCondition],
    opts: &ConditionValidationOptions,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let path = Path::new("status").child("conditions");
    let mut seen_types: Vec<&str> = Vec::new();
    let mut has_approved = false;
    let mut has_denied = false;

    for (i, c) in conditions.iter().enumerate() {
        let cp = path.index(i);
        if !opts.allow_empty_condition_type && c.type_.is_empty() {
            errs.push(Error::required(&cp.child("type"), ""));
        }

        let true_only = matches!(c.type_.as_str(), APPROVED | DENIED | FAILED);
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

        if !opts.allow_both_approved_and_denied {
            match c.type_.as_str() {
                APPROVED => {
                    has_approved = true;
                    if has_denied {
                        errs.push(Error::invalid(
                            &path,
                            APPROVED.to_string(),
                            "Approved and Denied conditions are mutually exclusive",
                        ));
                    }
                }
                DENIED => {
                    has_denied = true;
                    if has_approved {
                        errs.push(Error::invalid(
                            &path,
                            DENIED.to_string(),
                            "Approved and Denied conditions are mutually exclusive",
                        ));
                    }
                }
                _ => {}
            }
        }

        if !opts.allow_duplicate_condition_types {
            if seen_types.contains(&c.type_.as_str()) {
                errs.push(Error::duplicate(&cp.child("type"), c.type_.clone()));
            }
            seen_types.push(c.type_.as_str());
        }
    }

    errs
}

/// Validate CSR `status.conditions` — the self-contained `validateConditions`
/// rules from upstream (pkg/apis/certificates/validation): a condition `type`
/// must be non-empty; `status` must be one of True/False/Unknown, and for the
/// Approved/Denied/Failed types only `True`; a CSR may not carry both an
/// Approved and a Denied condition; and duplicate condition types are rejected.
///
/// This is the handler-facing entry that operates on a bare condition slice
/// with the strict (create-equivalent) options. The diff-based rules
/// (may-not-remove/modify existing Approved/Denied/Failed) are layered on top
/// by [`validate_certificate_signing_request_update`], which needs the stored
/// object.
pub fn validate_csr_status_conditions(
    conditions: &[CertificateSigningRequestCondition],
) -> ErrorList {
    validate_conditions_slice(conditions, &ConditionValidationOptions::default())
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

// ---------------------------------------------------------------------------
// Update paths — upstream `ValidateCertificateSigningRequest{,Status,Approval}Update`
// ---------------------------------------------------------------------------

/// Subset of upstream `certificateValidationOptions` relevant to the update
/// paths. Mirrors `getValidationOptions` (validation.go:353): all create-time
/// compatibility flags are derived from the *old* object, while the
/// subresource capability flags (`allowSettingCertificate` /
/// `allowSettingApprovalConditions`) are set by the entry point.
#[derive(Clone, Copy)]
struct UpdateValidationOptions {
    conditions: ConditionValidationOptions,
    /// `allowArbitraryCertificate` — skip the PEM `validateCertificate` check.
    allow_arbitrary_certificate: bool,
    /// `allowSettingCertificate` — the `/status` subresource may set the cert.
    allow_setting_certificate: bool,
    /// `allowResettingCertificate` — always false in upstream `getValidationOptions`.
    allow_resetting_certificate: bool,
    /// `allowSettingApprovalConditions` — the `/approval` subresource may
    /// add/modify Approved/Denied conditions.
    allow_setting_approval_conditions: bool,
}

impl UpdateValidationOptions {
    /// The create-time options: every compatibility/capability flag `false`
    /// (`allowArbitraryCertificate` included), mirroring an empty
    /// `certificateValidationOptions`.
    fn strict() -> Self {
        Self {
            conditions: ConditionValidationOptions::default(),
            allow_arbitrary_certificate: false,
            allow_setting_certificate: false,
            allow_resetting_certificate: false,
            allow_setting_approval_conditions: false,
        }
    }
}

/// All instances of conditions of the given type — upstream `findConditions`.
fn find_conditions<'a>(
    csr: &'a CertificateSigningRequest,
    condition_type: &str,
) -> Vec<&'a CertificateSigningRequestCondition> {
    conditions_of(csr)
        .iter()
        .filter(|c| c.type_ == condition_type)
        .collect()
}

/// `apiequality.Semantic.DeepEqual` over the validation-relevant condition
/// fields. `lastUpdateTime` / `lastTransitionTime` are metadata-ish but
/// upstream's DeepEqual compares the whole struct, so we compare every field.
fn conditions_deep_equal(
    a: &[&CertificateSigningRequestCondition],
    b: &[&CertificateSigningRequestCondition],
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| {
        x.type_ == y.type_
            && x.status == y.status
            && x.reason == y.reason
            && x.message == y.message
            && x.last_update_time == y.last_update_time
            && x.last_transition_time == y.last_transition_time
    })
}

/// `getValidationOptions(newCSR, oldCSR)` — derive the compatibility flags from
/// the existing (old) object so that updates tolerate pre-existing data, plus
/// the new-vs-old comparisons (`allowArbitraryCertificate`).
fn get_validation_options(
    new_csr: &CertificateSigningRequest,
    old_csr: &CertificateSigningRequest,
) -> UpdateValidationOptions {
    UpdateValidationOptions {
        conditions: ConditionValidationOptions {
            allow_empty_condition_type: has_empty_condition_type(old_csr),
            allow_both_approved_and_denied: allow_both_approved_and_denied(old_csr),
            allow_duplicate_condition_types: has_duplicate_condition_types(old_csr),
        },
        allow_arbitrary_certificate: allow_arbitrary_certificate(new_csr, old_csr),
        allow_setting_certificate: false,
        allow_resetting_certificate: false,
        allow_setting_approval_conditions: false,
    }
}

/// `allowBothApprovedAndDenied` — true when the old object already carries both.
fn allow_both_approved_and_denied(old_csr: &CertificateSigningRequest) -> bool {
    let mut approved = false;
    let mut denied = false;
    for c in conditions_of(old_csr) {
        match c.type_.as_str() {
            APPROVED => approved = true,
            DENIED => denied = true,
            _ => {}
        }
    }
    approved && denied
}

/// `hasDuplicateConditionTypes` over the old object.
fn has_duplicate_condition_types(old_csr: &CertificateSigningRequest) -> bool {
    let mut seen: Vec<&str> = Vec::new();
    for c in conditions_of(old_csr) {
        if seen.contains(&c.type_.as_str()) {
            return true;
        }
        seen.push(c.type_.as_str());
    }
    false
}

/// `hasEmptyConditionType` over the old object.
fn has_empty_condition_type(old_csr: &CertificateSigningRequest) -> bool {
    conditions_of(old_csr).iter().any(|c| c.type_.is_empty())
}

/// `allowArbitraryCertificate` — tolerate updates that don't touch the cert, or
/// where the old object already stored an invalid certificate.
fn allow_arbitrary_certificate(
    new_csr: &CertificateSigningRequest,
    old_csr: &CertificateSigningRequest,
) -> bool {
    if status_certificate(new_csr) == status_certificate(old_csr) {
        return true; // tolerate updates that don't touch status.certificate
    }
    status_certificate_error(status_certificate(old_csr)).is_some()
}

/// Create-style field validation reused by the update path — upstream
/// `validateCertificateSigningRequest(newCSR, opts)`. Identical to
/// [`validate_certificate_signing_request_create`] except the condition and
/// certificate checks consult the update compatibility flags.
fn validate_certificate_signing_request_with_opts(
    csr: &CertificateSigningRequest,
    opts: &UpdateValidationOptions,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let spec = Path::new("spec");

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

    errs.extend(validate_conditions(csr, &opts.conditions));

    if !opts.allow_arbitrary_certificate {
        if let Some(detail) = status_certificate_error(status_certificate(csr)) {
            errs.push(Error::invalid(
                &Path::new("status").child("certificate"),
                "<certificate data>".to_string(),
                detail,
            ));
        }
    }

    errs
}

/// Shared body of the update paths — upstream
/// `validateCertificateSigningRequestUpdate(newCSR, oldCSR, opts)`. Runs the
/// create-style validation against the new object, then layers the diff rules:
/// existing Approved/Denied/Failed conditions may not be removed; unless the
/// `/approval` subresource, Approved/Denied conditions may not be
/// added/removed/modified; and `status.certificate` is immutable unless the
/// `/status` subresource is setting it for the first time.
fn validate_certificate_signing_request_update(
    new_csr: &CertificateSigningRequest,
    old_csr: &CertificateSigningRequest,
    opts: &UpdateValidationOptions,
) -> ErrorList {
    let mut errs = validate_certificate_signing_request_with_opts(new_csr, opts);
    let conditions_path = Path::new("status").child("conditions");

    // Prevent removal of existing Approved/Denied/Failed conditions.
    for t in [APPROVED, DENIED, FAILED] {
        let old_conditions = find_conditions(old_csr, t);
        let new_conditions = find_conditions(new_csr, t);
        if new_conditions.len() < old_conditions.len() {
            errs.push(Error::forbidden(
                &conditions_path,
                format!("updates may not remove a condition of type {t:?}"),
            ));
        }
    }

    // Unless the /approval subresource, prevent addition/removal/modification
    // of Approved/Denied conditions.
    if !opts.allow_setting_approval_conditions {
        for t in [APPROVED, DENIED] {
            let old_conditions = find_conditions(old_csr, t);
            let new_conditions = find_conditions(new_csr, t);
            if new_conditions.len() < old_conditions.len() {
                // removals are prevented above
            } else if new_conditions.len() > old_conditions.len() {
                errs.push(Error::forbidden(
                    &conditions_path,
                    format!("updates may not add a condition of type {t:?}"),
                ));
            } else if !conditions_deep_equal(&old_conditions, &new_conditions) {
                errs.push(Error::forbidden(
                    &conditions_path,
                    format!("updates may not modify a condition of type {t:?}"),
                ));
            }
        }
    }

    // status.certificate immutability.
    if status_certificate(new_csr) != status_certificate(old_csr) {
        let cert_path = Path::new("status").child("certificate");
        if !opts.allow_setting_certificate {
            errs.push(Error::forbidden(
                &cert_path,
                "updates may not set certificate content",
            ));
        } else if !opts.allow_resetting_certificate
            && status_certificate(old_csr).is_some_and(|c| !c.is_empty())
        {
            errs.push(Error::forbidden(
                &cert_path,
                "updates may not modify existing certificate content",
            ));
        }
    }

    errs
}

/// Validate a CertificateSigningRequest spec/metadata update (the main resource,
/// not a subresource) — upstream `ValidateCertificateSigningRequestUpdate`.
pub fn validate_certificate_signing_request_update_main(
    new_csr: &CertificateSigningRequest,
    old_csr: &CertificateSigningRequest,
) -> ErrorList {
    let opts = get_validation_options(new_csr, old_csr);
    validate_certificate_signing_request_update(new_csr, old_csr, &opts)
}

/// Validate a `/status` subresource update — upstream
/// `ValidateCertificateSigningRequestStatusUpdate`. Sets `allowSettingCertificate`.
pub fn validate_certificate_signing_request_status_update(
    new_csr: &CertificateSigningRequest,
    old_csr: &CertificateSigningRequest,
) -> ErrorList {
    let mut opts = get_validation_options(new_csr, old_csr);
    opts.allow_setting_certificate = true;
    validate_certificate_signing_request_update(new_csr, old_csr, &opts)
}

/// Validate an `/approval` subresource update — upstream
/// `ValidateCertificateSigningRequestApprovalUpdate`. Sets
/// `allowSettingApprovalConditions`.
pub fn validate_certificate_signing_request_approval_update(
    new_csr: &CertificateSigningRequest,
    old_csr: &CertificateSigningRequest,
) -> ErrorList {
    let mut opts = get_validation_options(new_csr, old_csr);
    opts.allow_setting_approval_conditions = true;
    validate_certificate_signing_request_update(new_csr, old_csr, &opts)
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

#[cfg(test)]
mod request_signature_tests {
    use super::parse_csr_request_error;
    use base64::Engine;

    /// Generate a real PKCS#10 CSR PEM with a valid self-signature via rcgen.
    fn valid_csr_pem() -> String {
        let key_pair = rcgen::KeyPair::generate().expect("keypair");
        let params =
            rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).expect("params");
        params
            .serialize_request(&key_pair)
            .expect("csr")
            .pem()
            .expect("pem")
    }

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    #[test]
    fn valid_csr_passes() {
        let request = b64(&valid_csr_pem());
        assert_eq!(parse_csr_request_error(&request), None);
    }

    #[test]
    fn empty_request_rejected() {
        assert!(parse_csr_request_error("").is_some());
    }

    #[test]
    fn garbage_base64_rejected() {
        let err = parse_csr_request_error("not base64!!!").expect("err");
        assert!(err.contains("error parsing request"), "{err}");
    }

    #[test]
    fn non_csr_pem_rejected() {
        let request = b64("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n");
        let err = parse_csr_request_error(&request).expect("err");
        assert!(err.contains("CERTIFICATE REQUEST"), "{err}");
    }

    #[test]
    fn tampered_signature_rejected() {
        // Decode a valid CSR to DER, flip the final signature byte (keeps the
        // ASN.1 lengths intact so the structure still parses), re-PEM it.
        let pem_str = valid_csr_pem();
        let block = pem::parse(pem_str.as_bytes()).expect("pem");
        let mut der = block.contents().to_vec();
        let last = der.len() - 1;
        der[last] ^= 0xff;
        let tampered_pem = pem::encode(&pem::Pem::new("CERTIFICATE REQUEST", der));
        let request = b64(&tampered_pem);

        let err = parse_csr_request_error(&request).expect("tampered must fail");
        assert!(
            err.contains("signature verification failed") || err.contains("error parsing request"),
            "{err}"
        );
    }
}

#[cfg(test)]
mod update_and_wiring_tests {
    use super::*;
    use crate::resources::certificates::{
        CertificateSigningRequestCondition, CertificateSigningRequestSpec,
        CertificateSigningRequestStatus,
    };
    use crate::types::ObjectMeta;
    use base64::Engine;

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    fn valid_csr_request_b64() -> String {
        let key_pair = rcgen::KeyPair::generate().expect("keypair");
        let params =
            rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).expect("params");
        let pem = params
            .serialize_request(&key_pair)
            .expect("csr")
            .pem()
            .expect("pem");
        b64(&pem)
    }

    fn valid_cert_b64() -> String {
        let pem = rcgen::generate_simple_self_signed(vec!["test.example.com".to_string()])
            .expect("cert")
            .cert
            .pem();
        b64(&pem)
    }

    fn cond(type_: &str, status: &str) -> CertificateSigningRequestCondition {
        CertificateSigningRequestCondition {
            type_: type_.to_string(),
            status: status.to_string(),
            reason: None,
            message: None,
            last_update_time: None,
            last_transition_time: None,
        }
    }

    fn base_csr() -> CertificateSigningRequest {
        CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: ObjectMeta::default(),
            spec: CertificateSigningRequestSpec {
                request: valid_csr_request_b64(),
                signer_name: "example.com/my-signer".to_string(),
                usages: vec![KeyUsage::ClientAuth],
                ..Default::default()
            },
            status: None,
        }
    }

    fn with_conditions(
        mut csr: CertificateSigningRequest,
        conditions: Vec<CertificateSigningRequestCondition>,
    ) -> CertificateSigningRequest {
        csr.status = Some(CertificateSigningRequestStatus {
            conditions: Some(conditions),
            certificate: None,
        });
        csr
    }

    fn with_cert(
        mut csr: CertificateSigningRequest,
        cert: Option<String>,
    ) -> CertificateSigningRequest {
        let conditions = csr.status.as_ref().and_then(|s| s.conditions.clone());
        csr.status = Some(CertificateSigningRequestStatus {
            conditions,
            certificate: cert,
        });
        csr
    }

    // ---- create-path wiring ----

    #[test]
    fn create_rejects_invalid_condition_status() {
        // Approved must be True; "False" is rejected via the now-wired validateConditions.
        let csr = with_conditions(base_csr(), vec![cond("Approved", "False")]);
        let errs = validate_certificate_signing_request_create(&csr);
        assert!(!errs.is_empty(), "expected condition status rejection");
    }

    #[test]
    fn create_rejects_duplicate_condition_types() {
        let csr = with_conditions(
            base_csr(),
            vec![cond("Approved", "True"), cond("Approved", "True")],
        );
        let errs = validate_certificate_signing_request_create(&csr);
        assert!(
            errs.iter().any(|e| format!("{e}").contains("Duplicate")
                || format!("{e:?}").to_lowercase().contains("duplicate")),
            "expected duplicate condition type rejection, got {errs:?}"
        );
    }

    #[test]
    fn create_rejects_arbitrary_certificate() {
        let csr = with_cert(base_csr(), Some(b64("not a pem cert")));
        let errs = validate_certificate_signing_request_create(&csr);
        assert!(!errs.is_empty(), "expected certificate rejection on create");
    }

    #[test]
    fn create_accepts_valid_certificate_and_conditions() {
        let csr = with_cert(
            with_conditions(base_csr(), vec![cond("Approved", "True")]),
            Some(valid_cert_b64()),
        );
        let errs = validate_certificate_signing_request_create(&csr);
        assert!(errs.is_empty(), "expected clean create, got {errs:?}");
    }

    // ---- update: removal of existing Approved/Denied/Failed ----

    #[test]
    fn update_forbids_removing_approved_condition() {
        let old = with_conditions(base_csr(), vec![cond("Approved", "True")]);
        let new = base_csr(); // no conditions
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not remove a condition")),
            "expected forbid-remove, got {errs:?}"
        );
    }

    #[test]
    fn update_forbids_removing_failed_condition() {
        let old = with_conditions(base_csr(), vec![cond("Failed", "True")]);
        let new = base_csr();
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not remove a condition")),
            "expected forbid-remove Failed, got {errs:?}"
        );
    }

    // ---- update: add/modify Approved/Denied only via /approval ----

    #[test]
    fn main_update_forbids_adding_approved() {
        let old = base_csr();
        let new = with_conditions(base_csr(), vec![cond("Approved", "True")]);
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not add a condition")),
            "expected forbid-add via main update, got {errs:?}"
        );
    }

    #[test]
    fn approval_update_allows_adding_approved() {
        let old = base_csr();
        let new = with_conditions(base_csr(), vec![cond("Approved", "True")]);
        let errs = validate_certificate_signing_request_approval_update(&new, &old);
        assert!(
            errs.is_empty(),
            "approval update should add Approved, got {errs:?}"
        );
    }

    #[test]
    fn main_update_forbids_modifying_denied() {
        let old = with_conditions(base_csr(), vec![cond("Denied", "True")]);
        let mut modified = cond("Denied", "True");
        modified.reason = Some("changed".to_string());
        let new = with_conditions(base_csr(), vec![modified]);
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not modify a condition")),
            "expected forbid-modify, got {errs:?}"
        );
    }

    // ---- update: status.certificate immutability ----

    #[test]
    fn main_update_forbids_setting_certificate() {
        let old = base_csr();
        let new = with_cert(base_csr(), Some(valid_cert_b64()));
        let errs = validate_certificate_signing_request_update_main(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not set certificate content")),
            "expected forbid-set-cert on main update, got {errs:?}"
        );
    }

    #[test]
    fn status_update_allows_setting_certificate_once() {
        let old = base_csr();
        let new = with_cert(base_csr(), Some(valid_cert_b64()));
        let errs = validate_certificate_signing_request_status_update(&new, &old);
        assert!(
            errs.is_empty(),
            "status update should set cert, got {errs:?}"
        );
    }

    #[test]
    fn status_update_forbids_modifying_existing_certificate() {
        let old = with_cert(base_csr(), Some(valid_cert_b64()));
        let new = with_cert(base_csr(), Some(valid_cert_b64())); // different bytes (new keypair)
        let errs = validate_certificate_signing_request_status_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| format!("{e}").contains("may not modify existing certificate")),
            "expected forbid-modify-existing-cert, got {errs:?}"
        );
    }

    #[test]
    fn update_tolerates_preexisting_invalid_certificate_when_unchanged() {
        // allowArbitraryCertificate: old has an invalid cert; new leaves it
        // unchanged -> the PEM check is skipped and no immutability error fires.
        let bad = b64("garbage cert");
        let old = with_cert(base_csr(), Some(bad.clone()));
        let new = with_cert(base_csr(), Some(bad));
        let errs = validate_certificate_signing_request_status_update(&new, &old);
        assert!(
            errs.is_empty(),
            "unchanged invalid cert should be tolerated, got {errs:?}"
        );
    }
}
