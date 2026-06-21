use rusternetes_common::resources::Secret;
use rusternetes_common::validation::secret::validate_secret_type;
use serde_json::json;

// data values are base64 in JSON (the Secret deserializer decodes to bytes).
fn b64(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s)
}

fn secret(secret_type: &str, data: serde_json::Value, annotations: serde_json::Value) -> Secret {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "s", "namespace": "default", "annotations": annotations},
        "type": secret_type,
        "data": data
    }))
    .unwrap()
}

#[test]
fn tls_requires_cert_and_key() {
    let ok = secret(
        "kubernetes.io/tls",
        json!({"tls.crt": b64("c"), "tls.key": b64("k")}),
        json!({}),
    );
    assert!(validate_secret_type(&ok).is_empty());

    let missing = secret("kubernetes.io/tls", json!({"tls.crt": b64("c")}), json!({}));
    let errs = validate_secret_type(&missing);
    assert!(
        errs.iter().any(|e| e.to_string().contains("tls.key")),
        "{errs:?}"
    );
}

#[test]
fn opaque_and_custom_have_no_constraints() {
    assert!(validate_secret_type(&secret("Opaque", json!({}), json!({}))).is_empty());
    assert!(validate_secret_type(&secret("example.com/custom", json!({}), json!({}))).is_empty());
}

#[test]
fn service_account_token_requires_name_annotation() {
    let missing = secret("kubernetes.io/service-account-token", json!({}), json!({}));
    assert!(!validate_secret_type(&missing).is_empty());

    let ok = secret(
        "kubernetes.io/service-account-token",
        json!({}),
        json!({"kubernetes.io/service-account.name": "default"}),
    );
    assert!(validate_secret_type(&ok).is_empty());
}

#[test]
fn dockercfg_requires_valid_json() {
    let missing = secret("kubernetes.io/dockercfg", json!({}), json!({}));
    assert!(!validate_secret_type(&missing).is_empty());

    let bad = secret(
        "kubernetes.io/dockercfg",
        json!({".dockercfg": b64("not json")}),
        json!({}),
    );
    assert!(!validate_secret_type(&bad).is_empty());

    let ok = secret(
        "kubernetes.io/dockercfg",
        json!({".dockercfg": b64("{}")}),
        json!({}),
    );
    assert!(validate_secret_type(&ok).is_empty());
}

#[test]
fn basic_auth_requires_username_or_password() {
    let missing = secret("kubernetes.io/basic-auth", json!({}), json!({}));
    assert_eq!(validate_secret_type(&missing).len(), 2);

    let ok = secret(
        "kubernetes.io/basic-auth",
        json!({"username": b64("u")}),
        json!({}),
    );
    assert!(validate_secret_type(&ok).is_empty());
}

#[test]
fn ssh_auth_requires_private_key() {
    let missing = secret("kubernetes.io/ssh-auth", json!({}), json!({}));
    assert!(!validate_secret_type(&missing).is_empty());

    let ok = secret(
        "kubernetes.io/ssh-auth",
        json!({"ssh-privatekey": b64("KEY")}),
        json!({}),
    );
    assert!(validate_secret_type(&ok).is_empty());
}
