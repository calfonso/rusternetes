//! Upstream-mirror RED-state TDD pins for Secret consumption + admission
//! surface, modelled after the Kubernetes v1.35 e2e suite at
//! `test/e2e/common/storage/secrets.go` and `test/e2e/common/node/secrets.go`.
//!
//! Sources of truth (permalinks):
//! - <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/common/storage/secrets.go>
//! - <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/common/node/secrets.go>
//!
//! The upstream e2e tests exercise three angles:
//!
//! 1. Typed Secret round-trips (`Opaque`, `kubernetes.io/dockerconfigjson`,
//!    `kubernetes.io/tls`, `kubernetes.io/basic-auth`) — these encode the
//!    documented "well-known" Secret types every Kubernetes-compatible
//!    apiserver must accept and store losslessly.
//! 2. Immutable enforcement — once `.immutable: true` is set, subsequent
//!    updates that try to change `.data`, `.stringData`, or flip the flag back
//!    to `false` must be rejected with `422 Invalid` (upstream
//!    `ValidateImmutableField`). Upstream PR #93660 added this guarantee.
//! 3. Pod-spec admission — pods that reference Secrets via `volumes[].secret`
//!    or `containers[].envFrom[].secretRef` must be admitted at the
//!    apiserver layer regardless of whether the referenced Secret exists yet;
//!    the kubelet is responsible for surfacing the binding failure at
//!    mount/env-resolution time. Mirrors upstream comment in
//!    `DoTestSecrets`: "this pod may fail to run, but we don't currently
//!    prevent this."
//!
//! All tests use the inline `spawn_router()` harness pattern documented in
//! `tests/integration_namespace_conditions.rs:74`, talking to an
//! `Arc<MemoryStorage>` via `tower::ServiceExt::oneshot` so each test owns
//! its own storage and runs concurrently.

use axum::http::{Method, StatusCode};
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `mem` is the
// backing store; each test owns its own storage and runs concurrently.
// ---------------------------------------------------------------------------

fn spawn_router() -> (TestApiServer, Arc<MemoryStorage>) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (api, mem)
}

/// Issue a single oneshot request and return `(status, parsed JSON body)`.
async fn send(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    router.send(method.as_str(), uri, content_type, body).await
}

/// Pre-create a namespace so the secret routes don't have to fall back to a
/// "default" namespace assumption. Mirrors upstream `framework.Namespace`
/// allocation done by the e2e harness before every test body runs.
async fn create_namespace(router: &TestApiServer, name: &str) {
    let (status, body) = send(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces",
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": name },
        })),
    )
    .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::CONFLICT,
        "namespace setup must return 201 or 409 (already-exists is fine), got {status}: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// Secret CRUD round-trip (type: Opaque)
//
// Mirrors `DoTestSecrets` in `test/integration/secrets/secrets_test.go` and
// the basic `should be consumable` cases from `test/e2e/common/storage/
// secrets.go`. Exercises: create -> get -> list -> update -> delete on an
// `Opaque` Secret. Asserts metadata + data round-trip cleanly through the
// router.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_secret_crud_round_trip_opaque() {
    let (router, _mem) = spawn_router();
    let ns = "secret-crud-ns";
    create_namespace(&router, ns).await;

    // CREATE — base64("value1\n") = "dmFsdWUxCg=="
    let secret_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "crud-secret", "namespace": ns },
        "type": "Opaque",
        "data": { "key": "dmFsdWUxCg==" }
    });
    let (status, body) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/secrets"),
        Some(&secret_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "Secret CREATE must return 201; got {status} body={body}"
    );
    assert_eq!(
        body.get("type").and_then(|v| v.as_str()),
        Some("Opaque"),
        "type round-trip must equal Opaque"
    );

    // GET
    let (status, body) = send(
        router.clone(),
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/secrets/crud-secret"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Secret GET must return 200");
    assert_eq!(
        body.pointer("/data/key").and_then(|v| v.as_str()),
        Some("dmFsdWUxCg=="),
        "Secret data must round-trip base64-encoded; body={body}"
    );

    // LIST (namespaced)
    let (status, body) = send(
        router.clone(),
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/secrets"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Secret LIST must return 200");
    let items = body
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        items
            .iter()
            .any(|s| s.pointer("/metadata/name").and_then(|n| n.as_str()) == Some("crud-secret")),
        "LIST must contain the just-created secret; items={items:#?}"
    );

    // UPDATE — replace the data map (PUT is upsert in our handler).
    let update_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "crud-secret", "namespace": ns },
        "type": "Opaque",
        "data": { "key": "dmFsdWUyCg==" } // base64("value2\n")
    });
    let (status, body) = send(
        router.clone(),
        Method::PUT,
        &format!("/api/v1/namespaces/{ns}/secrets/crud-secret"),
        Some(&update_body),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Secret UPDATE must return 200");
    assert_eq!(
        body.pointer("/data/key").and_then(|v| v.as_str()),
        Some("dmFsdWUyCg=="),
        "Secret data must reflect the update"
    );

    // DELETE
    let (status, _body) = send(
        router.clone(),
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns}/secrets/crud-secret"),
        None,
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "Secret DELETE must return 200/202; got {status}"
    );

    // GET-after-DELETE must 404.
    let (status, _body) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/secrets/crud-secret"),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "GET after DELETE must return 404"
    );
}

// ---------------------------------------------------------------------------
// Typed Secret round-trip — kubernetes.io/dockerconfigjson
//
// Mirrors `framework.SecretFromDockerConfigJSON` in upstream e2e helpers.
// The well-known data key is `.dockerconfigjson` and the body must be a
// base64-encoded `{"auths": {...}}` payload.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_secret_type_dockerconfigjson_round_trip() {
    let (router, _mem) = spawn_router();
    let ns = "secret-docker-ns";
    create_namespace(&router, ns).await;

    // base64('{"auths":{"https://index.docker.io/v1/":{"auth":"dXNlcjpwYXNz"}}}')
    let docker_b64 =
        "eyJhdXRocyI6eyJodHRwczovL2luZGV4LmRvY2tlci5pby92MS8iOnsiYXV0aCI6ImRYTmxjanB3WVhOeiJ9fX0=";
    let secret_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "regcred", "namespace": ns },
        "type": "kubernetes.io/dockerconfigjson",
        "data": { ".dockerconfigjson": docker_b64 }
    });
    let (status, body) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/secrets"),
        Some(&secret_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "dockerconfigjson Secret CREATE must return 201; got {status} body={body}"
    );
    assert_eq!(
        body.get("type").and_then(|v| v.as_str()),
        Some("kubernetes.io/dockerconfigjson"),
        "type must round-trip exactly; body={body}"
    );

    // GET — payload must still be base64-encoded on the wire.
    let (status, body) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/secrets/regcred"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.pointer("/data/.dockerconfigjson")
            .and_then(|v| v.as_str()),
        Some(docker_b64),
        "dockerconfigjson body must round-trip byte-identically; body={body}"
    );
}

// ---------------------------------------------------------------------------
// Typed Secret round-trip — kubernetes.io/tls
//
// Mirrors `framework.SecretFromTLS` in upstream e2e helpers. Well-known keys
// are `tls.crt` and `tls.key`. The apiserver must accept these payloads
// (upstream's strict validation against PEM bytes happens at admission, not
// strategy — we round-trip them losslessly).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_secret_type_tls_round_trip() {
    let (router, _mem) = spawn_router();
    let ns = "secret-tls-ns";
    create_namespace(&router, ns).await;

    // Tiny placeholder PEM-shaped values — base64-encoded so the wire format
    // matches what kubectl produces. The api-server does not enforce PEM
    // parsing; that's a kubelet/ingress concern.
    let crt_b64 = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJoVENDQVF1Z0F3SUJBZ0lVQ09mYWtlLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQ==";
    let key_b64 = "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUVwQUlCQUFLQ0FRRUFmYWtlCi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0=";
    let secret_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "tls-secret", "namespace": ns },
        "type": "kubernetes.io/tls",
        "data": { "tls.crt": crt_b64, "tls.key": key_b64 }
    });
    let (status, body) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/secrets"),
        Some(&secret_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "TLS Secret CREATE must return 201; got {status} body={body}"
    );
    assert_eq!(
        body.get("type").and_then(|v| v.as_str()),
        Some("kubernetes.io/tls"),
        "TLS Secret type must round-trip"
    );

    // GET — both well-known keys must round-trip.
    let (status, body) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/secrets/tls-secret"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.pointer("/data/tls.crt").and_then(|v| v.as_str()),
        Some(crt_b64),
        "tls.crt must round-trip byte-identically; body={body}"
    );
    assert_eq!(
        body.pointer("/data/tls.key").and_then(|v| v.as_str()),
        Some(key_b64),
        "tls.key must round-trip byte-identically; body={body}"
    );
}

// ---------------------------------------------------------------------------
// Typed Secret round-trip — kubernetes.io/basic-auth
//
// Well-known keys: `username`, `password`. Mirrors the
// `SecretTypeBasicAuth` cases in upstream `pkg/apis/core/validation`.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_secret_type_basic_auth_round_trip() {
    let (router, _mem) = spawn_router();
    let ns = "secret-basic-auth-ns";
    create_namespace(&router, ns).await;

    let secret_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "basic-secret", "namespace": ns },
        "type": "kubernetes.io/basic-auth",
        "data": {
            "username": "YWRtaW4=",     // base64("admin")
            "password": "cGFzc3dvcmQ=", // base64("password")
        }
    });
    let (status, body) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/secrets"),
        Some(&secret_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "basic-auth Secret CREATE must return 201; got {status} body={body}"
    );
    assert_eq!(
        body.get("type").and_then(|v| v.as_str()),
        Some("kubernetes.io/basic-auth"),
        "basic-auth Secret type must round-trip"
    );

    // GET — both keys must round-trip.
    let (status, body) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/secrets/basic-secret"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.pointer("/data/username").and_then(|v| v.as_str()),
        Some("YWRtaW4=")
    );
    assert_eq!(
        body.pointer("/data/password").and_then(|v| v.as_str()),
        Some("cGFzc3dvcmQ=")
    );
}

// ---------------------------------------------------------------------------
// Secret immutable field — update must be rejected with 422
//
// Mirrors `pkg/registry/core/secret/strategy.go::ValidateUpdate` which calls
// `ValidateImmutableField` (upstream PR #93660). Once a Secret is created
// with `immutable: true`, any subsequent UPDATE that changes `.data` or
// `.stringData` must return `422 Invalid` with the message containing
// `is immutable`.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_secret_immutable_update_rejected() {
    let (router, _mem) = spawn_router();
    let ns = "secret-immutable-ns";
    create_namespace(&router, ns).await;

    // CREATE an immutable secret.
    let create_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "locked", "namespace": ns },
        "type": "Opaque",
        "immutable": true,
        "data": { "key": "dmFsdWUx" } // base64("value1")
    });
    let (status, body) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/secrets"),
        Some(&create_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "immutable Secret CREATE must return 201; got {status} body={body}"
    );
    assert_eq!(
        body.get("immutable"),
        Some(&json!(true)),
        "immutable: true must round-trip on create; body={body}"
    );

    // UPDATE that changes `.data` must be rejected with 422 Invalid.
    let bad_update = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "locked", "namespace": ns },
        "type": "Opaque",
        "immutable": true,
        "data": { "key": "dmFsdWUy" } // base64("value2") — different!
    });
    let (status, body) = send(
        router,
        Method::PUT,
        &format!("/api/v1/namespaces/{ns}/secrets/locked"),
        Some(&bad_update),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "mutating an immutable Secret's data must return 422 Invalid; got {status} body={body}"
    );
    let msg = body
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        msg.contains("immutable"),
        "rejection message must mention immutability; got {msg:?}"
    );
}

// ---------------------------------------------------------------------------
// Pod admission — volumes[].secret reference validation
//
// Mirrors `DoTestSecrets` in upstream `test/integration/secrets/
// secrets_test.go` and the `should be consumable from pods in volume`
// case in `test/e2e/common/storage/secrets.go`. A pod that references a
// Secret via a `secret` volume must be admitted by the apiserver REGARDLESS
// of whether the Secret exists yet. The kubelet surfaces missing-secret
// failures at mount time.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pod_secret_volume_reference_admitted() {
    let (router, _mem) = spawn_router();
    let ns = "secret-vol-ns";
    create_namespace(&router, ns).await;

    // Create the referenced Secret first — common path.
    let (status, _body) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/secrets"),
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": "vol-secret", "namespace": ns },
            "type": "Opaque",
            "data": { "key": "dmFsdWU=" } // base64("value")
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "uses-secret", "namespace": ns },
        "spec": {
            "volumes": [{
                "name": "secvol",
                "secret": { "secretName": "vol-secret" }
            }],
            "containers": [{
                "name": "main",
                "image": "registry.k8s.io/pause:3.10",
                "volumeMounts": [{
                    "name": "secvol",
                    "mountPath": "/etc/secret",
                    "readOnly": true
                }]
            }]
        }
    });
    let (status, body) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&pod_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "Pod referencing an existing Secret volume must be admitted; got {status} body={body}"
    );
    assert_eq!(
        body.pointer("/spec/volumes/0/secret/secretName")
            .and_then(|v| v.as_str()),
        Some("vol-secret"),
        "secret volume reference must round-trip exactly; body={body}"
    );

    // Pod that references a NON-existent Secret must also be admitted — the
    // apiserver does not validate cross-resource references at create time.
    // (Upstream comment: "this pod may fail to run, but we don't currently
    // prevent this.")
    let pod_missing = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "uses-missing-secret", "namespace": ns },
        "spec": {
            "volumes": [{
                "name": "secvol",
                "secret": { "secretName": "does-not-exist" }
            }],
            "containers": [{
                "name": "main",
                "image": "registry.k8s.io/pause:3.10",
                "volumeMounts": [{
                    "name": "secvol",
                    "mountPath": "/etc/secret",
                    "readOnly": true
                }]
            }]
        }
    });
    let (status, body) = send(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&pod_missing),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "Pod referencing a NON-existent Secret volume must still be admitted \
         (kubelet handles missing secret at mount time); got {status} body={body}"
    );
}

// ---------------------------------------------------------------------------
// Pod admission — containers[].envFrom[].secretRef validation
//
// Mirrors the `should be consumable via the environment` case in upstream
// `test/e2e/common/node/secrets.go`. A pod that pulls env vars from a Secret
// via `envFrom[].secretRef` must be admitted by the apiserver. The
// `envFrom` block must round-trip with the secretRef name intact.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pod_secret_envfrom_reference_admitted() {
    let (router, _mem) = spawn_router();
    let ns = "secret-envfrom-ns";
    create_namespace(&router, ns).await;

    // Pre-create the referenced Secret so the request models the common
    // case where ordering is correct.
    let (status, _body) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/secrets"),
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": "env-secret", "namespace": ns },
            "type": "Opaque",
            "data": {
                "GREETING": "aGVsbG8=", // base64("hello")
                "TARGET":   "d29ybGQ=", // base64("world")
            }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "envfrom-secret-pod", "namespace": ns },
        "spec": {
            "containers": [{
                "name": "main",
                "image": "registry.k8s.io/pause:3.10",
                "envFrom": [{
                    "secretRef": { "name": "env-secret" }
                }]
            }]
        }
    });
    let (status, body) = send(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&pod_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "Pod referencing a Secret via envFrom must be admitted; got {status} body={body}"
    );
    assert_eq!(
        body.pointer("/spec/containers/0/envFrom/0/secretRef/name")
            .and_then(|v| v.as_str()),
        Some("env-secret"),
        "envFrom.secretRef.name must round-trip exactly; body={body}"
    );
}
