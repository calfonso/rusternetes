use rusternetes_common::resources::Secret;
use rusternetes_common::validation::secret::{
    validate_secret, validate_secret_type, validate_secret_update,
};
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

// --- Full ValidateSecret -------------------------------------------------

#[test]
fn validate_secret_accepts_valid_opaque() {
    let ok = secret("Opaque", json!({"api-key": b64("v")}), json!({}));
    assert!(validate_secret(&ok).is_empty());
}

#[test]
fn validate_secret_rejects_bad_data_key() {
    // "@" is not allowed by IsConfigMapKey.
    let bad = secret("Opaque", json!({"bad@key": b64("v")}), json!({}));
    let errs = validate_secret(&bad);
    assert!(
        errs.iter().any(|e| e.to_string().contains("bad@key")),
        "{errs:?}"
    );
}

#[test]
fn validate_secret_rejects_dotdot_data_key() {
    let bad = secret("Opaque", json!({"..": b64("v")}), json!({}));
    assert!(!validate_secret(&bad).is_empty());
}

#[test]
fn validate_secret_enforces_max_size() {
    // One value over 1 MiB trips field.TooLong on `data`.
    let big = b64(&"x".repeat(1024 * 1024 + 1));
    let bad = secret("Opaque", json!({"k": big}), json!({}));
    let errs = validate_secret(&bad);
    assert!(
        errs.iter().any(|e| e.to_string().contains("data")),
        "{errs:?}"
    );
}

#[test]
fn validate_secret_rejects_invalid_metadata_name() {
    let bad: Secret = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "Bad_Name", "namespace": "default"},
        "type": "Opaque",
        "data": {}
    }))
    .unwrap();
    let errs = validate_secret(&bad);
    assert!(
        errs.iter().any(|e| e.to_string().contains("name")),
        "{errs:?}"
    );
}

#[test]
fn validate_secret_tls_required_keys_flow_through() {
    let missing = secret("kubernetes.io/tls", json!({"tls.crt": b64("c")}), json!({}));
    assert!(!validate_secret(&missing).is_empty());
}

// --- ValidateSecretUpdate ------------------------------------------------

fn secret_rv(secret_type: &str, data: serde_json::Value, rv: &str) -> Secret {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "s", "namespace": "default", "resourceVersion": rv},
        "type": secret_type,
        "data": data
    }))
    .unwrap()
}

#[test]
fn update_allows_data_change_when_mutable() {
    let old = secret_rv("Opaque", json!({"k": b64("v1")}), "1");
    let new = secret_rv("Opaque", json!({"k": b64("v2")}), "1");
    assert!(
        validate_secret_update(&old, &new).is_empty(),
        "mutable update"
    );
}

#[test]
fn update_rejects_type_change() {
    let old = secret_rv("Opaque", json!({}), "1");
    let new = secret_rv(
        "kubernetes.io/tls",
        json!({"tls.crt": b64("c"), "tls.key": b64("k")}),
        "1",
    );
    let errs = validate_secret_update(&old, &new);
    assert!(
        errs.iter().any(|e| {
            let s = e.to_string();
            s.contains("type") && s.contains("immutable")
        }),
        "{errs:?}"
    );
}

#[test]
fn update_type_defaults_to_opaque_both_sides() {
    // old has no type, new sends empty string — both default to Opaque, so the
    // immutability check must not fire.
    let old: Secret = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": "s", "namespace": "default", "resourceVersion": "1"},
        "data": {}
    }))
    .unwrap();
    let new = secret_rv("", json!({}), "1");
    let errs = validate_secret_update(&old, &new);
    assert!(
        !errs.iter().any(|e| e.to_string().contains("type")),
        "{errs:?}"
    );
}

#[test]
fn update_rejects_data_change_when_immutable() {
    let mut old = secret_rv("Opaque", json!({"k": b64("v1")}), "1");
    old.immutable = Some(true);
    let mut new = secret_rv("Opaque", json!({"k": b64("v2")}), "1");
    new.immutable = Some(true);
    let errs = validate_secret_update(&old, &new);
    assert!(
        errs.iter().any(|e| {
            let s = e.to_string();
            s.contains("data") && s.contains("immutable")
        }),
        "{errs:?}"
    );
}

#[test]
fn update_rejects_clearing_immutable_flag() {
    let mut old = secret_rv("Opaque", json!({"k": b64("v1")}), "1");
    old.immutable = Some(true);
    let mut new = secret_rv("Opaque", json!({"k": b64("v1")}), "1");
    new.immutable = Some(false);
    let errs = validate_secret_update(&old, &new);
    assert!(
        errs.iter().any(|e| e.to_string().contains("immutable")),
        "{errs:?}"
    );
}

#[test]
fn update_allows_unchanged_immutable_secret() {
    let mut old = secret_rv("Opaque", json!({"k": b64("v1")}), "1");
    old.immutable = Some(true);
    let mut new = secret_rv("Opaque", json!({"k": b64("v1")}), "1");
    new.immutable = Some(true);
    assert!(validate_secret_update(&old, &new).is_empty());
}

#[test]
fn update_requires_resource_version() {
    // ValidateSecretUpdate -> ValidateObjectMetaUpdate requires resourceVersion.
    let old = secret_rv("Opaque", json!({}), "1");
    let new = secret_rv("Opaque", json!({}), "");
    let errs = validate_secret_update(&old, &new);
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("resourceVersion")),
        "{errs:?}"
    );
}
