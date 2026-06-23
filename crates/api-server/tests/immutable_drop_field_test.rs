//! Upstream `ValidateConfigMapUpdate` / `ValidateSecretUpdate` parity: once an
//! object is created with `immutable: true`, a PUT may not flip it to `false`
//! NOR drop the field entirely. Upstream guards both via
//! `newObj.Immutable == nil || !*newObj.Immutable`. rusternetes previously
//! accepted the omitted (`nil`) case for Secrets — this regression test pins
//! the corrected behaviour for both ConfigMap and Secret.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn b64(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s)
}

fn configmaps_uri() -> String {
    format!("/api/v1/namespaces/{NS}/configmaps")
}

fn secrets_uri() -> String {
    format!("/api/v1/namespaces/{NS}/secrets")
}

#[tokio::test]
async fn configmap_dropping_immutable_field_is_rejected() {
    let state = TestApiServer::new();
    let name = "immutable-cm";
    let create = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name},
        "immutable": true,
        "data": {"k": "v"}
    });
    let (code, body) = state.post(&configmaps_uri(), &create).await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");

    let item = format!("{}/{name}", configmaps_uri());

    // PUT that omits `immutable` entirely (same data) must be rejected.
    let put_drop = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name},
        "data": {"k": "v"}
    });
    let (code, body) = state.put(&item, &put_drop).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "dropping `immutable` on an immutable ConfigMap must be rejected: {body}"
    );

    // PUT that flips immutable to false must also be rejected.
    let put_false = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name},
        "immutable": false,
        "data": {"k": "v"}
    });
    let (code, body) = state.put(&item, &put_false).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "flipping `immutable` to false must be rejected: {body}"
    );

    // PUT that keeps immutable true and data unchanged succeeds.
    let (code, body) = state.put(&item, &create).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "no-op update of an immutable ConfigMap must succeed: {body}"
    );
}

#[tokio::test]
async fn secret_dropping_immutable_field_is_rejected() {
    let state = TestApiServer::new();
    let name = "immutable-sec";
    let create: Value = json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": name},
        "type": "Opaque",
        "immutable": true,
        "data": {"k": b64("v")}
    });
    let (code, body) = state.post(&secrets_uri(), &create).await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");

    let item = format!("{}/{name}", secrets_uri());

    // PUT that omits `immutable` entirely (same data) must be rejected.
    // This is the regression case: previously accepted because the guard was
    // `immutable != Some(true) && immutable.is_some()`, which let `None` pass.
    let put_drop = json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": name},
        "type": "Opaque",
        "data": {"k": b64("v")}
    });
    let (code, body) = state.put(&item, &put_drop).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "dropping `immutable` on an immutable Secret must be rejected: {body}"
    );

    // PUT that flips immutable to false must also be rejected.
    let put_false = json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": name},
        "type": "Opaque",
        "immutable": false,
        "data": {"k": b64("v")}
    });
    let (code, body) = state.put(&item, &put_false).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "flipping `immutable` to false must be rejected: {body}"
    );

    // PUT keeping immutable true and data unchanged succeeds.
    let (code, body) = state.put(&item, &create).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "no-op update of an immutable Secret must succeed: {body}"
    );
}
