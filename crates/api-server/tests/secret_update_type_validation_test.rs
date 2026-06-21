//! On update, a typed Secret must still satisfy its type-specific required
//! keys (upstream ValidateSecretUpdate -> ValidateSecret). A non-immutable
//! kubernetes.io/tls secret may change data, but not drop tls.crt/tls.key.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn b64(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s)
}

fn secrets_uri() -> String {
    format!("/api/v1/namespaces/{NS}/secrets")
}

fn tls_secret(name: &str, data: Value) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": name},
        "type": "kubernetes.io/tls",
        "data": data
    })
}

#[tokio::test]
async fn tls_secret_update_must_keep_required_keys() {
    let state = TestApiServer::new();
    let name = "tls-sec";
    let good = json!({"tls.crt": b64("cert"), "tls.key": b64("key")});
    let (code, _) = state
        .post(&secrets_uri(), &tls_secret(name, good.clone()))
        .await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed");

    let item = format!("{}/{name}", secrets_uri());

    // Update dropping tls.key must be rejected.
    let (code, body) = state
        .put(&item, &tls_secret(name, json!({"tls.crt": b64("cert")})))
        .await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "TLS secret losing tls.key on update must be rejected: {body}"
    );

    // Update keeping both keys (changing cert value) succeeds.
    let (code, _) = state
        .put(
            &item,
            &tls_secret(
                name,
                json!({"tls.crt": b64("cert2"), "tls.key": b64("key2")}),
            ),
        )
        .await;
    assert_eq!(code, StatusCode::OK, "valid TLS secret update must succeed");
}
