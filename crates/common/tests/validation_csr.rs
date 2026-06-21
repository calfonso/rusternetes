use rusternetes_common::resources::certificates::CertificateSigningRequest;
use rusternetes_common::validation::certificatesigningrequest::validate_certificate_signing_request_create;
use serde_json::json;

fn csr(spec: serde_json::Value) -> CertificateSigningRequest {
    serde_json::from_value(json!({
        "apiVersion": "certificates.k8s.io/v1",
        "kind": "CertificateSigningRequest",
        "metadata": {"name": "csr-1"},
        "spec": spec
    }))
    .unwrap()
}

/// A real ECDSA P-256 PKCS#10 certificate request, base64 of the PEM
/// (CN=test.example.com). This is what `spec.request` carries on the wire.
const REAL_CSR_B64: &str = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0KTUlIc01JR1RBZ0VBTURFeEdUQVhCZ05WQkFNTUVIUmxjM1F1WlhoaGJYQnNaUzVqYjIweEZEQVNCZ05WQkFvTQpDM0oxYzNSbGNtNWxkR1Z6TUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFL2k2cjBkem16d3dRCnFWTXhSTDlkK2MwOE5VNzNCVTRjNzRFVS9GazgxVGI0UVFJMWhHNVE3U3hocklaUjIzQ3NMTFFEaFNJUitweHgKODhiSkpaNzRJYUFBTUFvR0NDcUdTTTQ5QkFNQ0EwZ0FNRVVDSUgvbE5mWkdDOUtsTlgzRmh5M0tzTFhzVituSApZMlRybGRabWo5Zm5rTVVjQWlFQW4xRTM4S0hLb050NUl6aFVSVWZPRDdlNTB1aDBVcjVBNTdzcDU5b2gyQTA9Ci0tLS0tRU5EIENFUlRJRklDQVRFIFJFUVVFU1QtLS0tLQo=";

fn valid_spec() -> serde_json::Value {
    json!({
        "request": REAL_CSR_B64,
        "signerName": "kubernetes.io/kube-apiserver-client",
        "usages": ["client auth", "digital signature"]
    })
}

#[test]
fn valid_csr_passes() {
    assert!(validate_certificate_signing_request_create(&csr(valid_spec())).is_empty());
}

#[test]
fn malformed_request_rejected() {
    // base64 of "-----BEGIN CERTIFICATE REQUEST-----\n" only — a PEM header
    // with no body; parses as neither a PEM block nor a PKCS#10 request.
    let mut s = valid_spec();
    s["request"] = json!("LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0K");
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter().any(|e| e.to_string().contains("request")),
        "malformed CSR request must be rejected: {errs:?}"
    );
}

#[test]
fn non_base64_request_rejected() {
    let mut s = valid_spec();
    s["request"] = json!("@@@not base64@@@");
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter().any(|e| e.to_string().contains("request")),
        "{errs:?}"
    );
}

#[test]
fn empty_request_rejected() {
    let mut s = valid_spec();
    s["request"] = json!("");
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter().any(|e| e.to_string().contains("request")),
        "{errs:?}"
    );
}

#[test]
fn usages_required() {
    let mut s = valid_spec();
    s["usages"] = json!([]);
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter().any(|e| e.to_string().contains("usages")),
        "{errs:?}"
    );
}

#[test]
fn duplicate_usages_rejected() {
    let mut s = valid_spec();
    s["usages"] = json!(["client auth", "client auth"]);
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter().any(|e| e.to_string().contains("usages")),
        "{errs:?}"
    );
}

#[test]
fn signer_name_must_be_domain_slash_path() {
    let mut s = valid_spec();
    s["signerName"] = json!("no-slash");
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter().any(|e| e.to_string().contains("signerName")),
        "{errs:?}"
    );
}

#[test]
fn signer_name_domain_needs_two_segments() {
    let mut s = valid_spec();
    s["signerName"] = json!("nodot/path");
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter().any(|e| e.to_string().contains("signerName")),
        "{errs:?}"
    );
}

#[test]
fn legacy_signer_name_rejected() {
    let mut s = valid_spec();
    s["signerName"] = json!("kubernetes.io/legacy-unknown");
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter().any(|e| e.to_string().contains("legacy")),
        "{errs:?}"
    );
}

#[test]
fn expiration_seconds_floor() {
    let mut s = valid_spec();
    s["expirationSeconds"] = json!(599);
    let errs = validate_certificate_signing_request_create(&csr(s));
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("expirationSeconds")),
        "{errs:?}"
    );

    let mut ok = valid_spec();
    ok["expirationSeconds"] = json!(600);
    assert!(validate_certificate_signing_request_create(&csr(ok)).is_empty());
}
