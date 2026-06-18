//! Integration-test mirror of upstream `TestSecrets`.
//!
//! Source of truth: Kubernetes v1.35
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/secrets/secrets_test.go>
//!
//! This file is a RED-state TDD pin for the api-server side of Secret object
//! lifecycle behaviour. It mirrors **one** upstream entry point, `TestSecrets`,
//! which in turn drives two sub-suites in the upstream file:
//!
//!   * `DoTestSecrets` — create a Secret, then create two pods that mount it
//!     via a `SecretVolumeSource` (one references an existing Secret, one
//!     references a non-existent Secret; both pod creates must succeed at the
//!     api-server layer — the kubelet is responsible for resolving the volume,
//!     not the api-server).
//!   * `DoTestSecretsImmutableWithEmptyValue` — create an immutable Secret
//!     whose `data` map contains an empty-string value, then strategic-merge
//!     PATCH the Secret with the *same* data + a new label. Upstream commit
//!     #119229 fixed a bug where the apiserver would reject this patch as
//!     "mutating an immutable Secret" because the empty byte slice round-trips
//!     through JSON differently than the stored value. The expected outcome is
//!     that the PATCH succeeds (HTTP 200), proving the apiserver compares
//!     normalised Secret data, not raw JSON.
//!
//! Test count: **1 monolithic** (`test_secrets`) — keeping the same name as
//! upstream so future regressions trace cleanly to the Go source.
//!
//! ## RED / GREEN breakdown
//!
//! The test is not `#[ignore]`d: every surface it touches (the secret routes,
//! the pod create route, the strategic-merge PATCH route) exists in
//! `crates/api-server/src/router.rs` today, and the underlying Secret +
//! immutable-data + normalisation logic is exercised by
//! `tests/secret_handler_test.rs`. Anything that breaks the upstream contract
//! — for example, regressing the empty-value PATCH fix (#119229), rejecting a
//! pod that mounts a non-existent Secret at admission time, or losing the
//! `immutable: true` flag on round-trip — will trip an assertion here.
//!
//! HTTP layer: `Arc<MemoryStorage>` → `StorageBackend::Memory` → `ApiServerState`
//! (`skip_auth=true`, `AlwaysAllowAuthorizer`) → `build_router(...)` → tower
//! `oneshot`, identical to the pattern in
//! `tests/conformance_apimachinery_admission_webhooks.rs` and
//! `tests/patch_cas_retry_test.rs`.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// HTTP harness — thin `(u16, Value)` shims over the shared `TestApiServer`,
// preserving this file's `send(&router, …)` / `send_json(&router, …)` call sites.
// ---------------------------------------------------------------------------

/// `TestApiServer` factory (kept tuple-shaped: tests destructure
/// `let (_mem, router) = spawn_router();`, where `_mem` is the backing store).
fn spawn_router() -> (
    std::sync::Arc<rusternetes_storage::memory::MemoryStorage>,
    TestApiServer,
) {
    let api = TestApiServer::new();
    (api.storage.clone(), api)
}

/// Issue a request and return `(status, parsed body)`. The `content_type`
/// argument lets the test pick `application/json` for POST/PUT vs
/// `application/strategic-merge-patch+json` for PATCH.
async fn send(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
    content_type: &str,
) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some(content_type), body).await;
    (status.as_u16(), value)
}

async fn send_json(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    send(api, method, uri, body, "application/json").await
}

// ---------------------------------------------------------------------------
// Upstream `TestSecrets`
//
// Source: test/integration/secrets/secrets_test.go (release-1.35)
//
//   func TestSecrets(t *testing.T) {
//       // ... start apiserver, create namespace "secret" ...
//       DoTestSecrets(t, client, ns)
//       DoTestSecretsImmutableWithEmptyValue(t, client, ns)
//   }
//
// We collapse both sub-suites into one Rust `#[tokio::test]` named
// `test_secrets`, matching the upstream entry-point.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_secrets() {
    let (_mem, router) = spawn_router();
    let ns = "secret";

    // -----------------------------------------------------------------
    // DoTestSecrets — Secret CREATE + Pod CREATE (mount existing + mount
    // non-existent Secret).
    // -----------------------------------------------------------------

    // Upstream:
    //   s := v1.Secret{
    //       ObjectMeta: metav1.ObjectMeta{Name: "secret", Namespace: ns.Name},
    //       Data:       map[string][]byte{"data": []byte("value1\n")},
    //   }
    //   client.CoreV1().Secrets(ns).Create(ctx, &s, metav1.CreateOptions{})
    //
    // `data["data"] = []byte("value1\n")`. Wire-encoded, that's base64
    // of "value1\n" → "dmFsdWUxCg==". Our Secret serializer base64-encodes
    // on the way out, so the JSON we send must already be base64-encoded.
    let secret_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "secret", "namespace": ns },
        "data": { "data": "dmFsdWUxCg==" }
    });
    let (status, body) = send_json(
        &router,
        "POST",
        &format!("/api/v1/namespaces/{}/secrets", ns),
        Some(&secret_body),
    )
    .await;
    assert_eq!(
        status, 201,
        "Secret CREATE must return 201 Created (mirrors `client.CoreV1().Secrets(ns).Create`); \
         got {} — body={}",
        status, body
    );
    assert_eq!(
        body.get("metadata").and_then(|m| m.get("name")),
        Some(&json!("secret")),
        "created Secret metadata.name must round-trip exactly; body={}",
        body
    );

    // Upstream uses the same Pod spec for both pods, only changing the name.
    // Volumes: one `Secret` volume named `secvol` referencing SecretName="secret".
    // Containers: one container `fake-name`, image `fakeimage`, with a
    // read-only volume mount of `secvol` at `/fake/path`.
    let pod_template = |pod_name: &str| -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": pod_name, "namespace": ns },
            "spec": {
                "volumes": [{
                    "name": "secvol",
                    "secret": { "secretName": "secret" }
                }],
                "containers": [{
                    "name": "fake-name",
                    "image": "fakeimage",
                    "volumeMounts": [{
                        "name": "secvol",
                        "mountPath": "/fake/path",
                        "readOnly": true
                    }]
                }]
            }
        })
    };

    // Pod 1: `uses-secret` — references an existing Secret.
    let (status, body) = send_json(
        &router,
        "POST",
        &format!("/api/v1/namespaces/{}/pods", ns),
        Some(&pod_template("uses-secret")),
    )
    .await;
    assert_eq!(
        status, 201,
        "Pod that mounts an existing Secret must be admitted at the api-server \
         layer (mirrors upstream `client.CoreV1().Pods(ns).Create(uses-secret)`); \
         got {} — body={}",
        status, body
    );

    // Pod 2: `uses-non-existent-secret` — references a missing Secret. The
    // upstream comment is explicit: "This pod may fail to run, but we don't
    // currently prevent this." Admission must NOT reject; the kubelet is
    // responsible for surfacing the missing Secret as a volume-mount failure.
    let (status, body) = send_json(
        &router,
        "POST",
        &format!("/api/v1/namespaces/{}/pods", ns),
        Some(&pod_template("uses-non-existent-secret")),
    )
    .await;
    assert_eq!(
        status, 201,
        "Pod that mounts a non-existent Secret must still be admitted by the \
         api-server (kubelet handles missing secrets at mount time); got {} — body={}",
        status, body
    );

    // Clean up the Secret to mirror upstream's `defer deleteSecretOrErrorf`.
    let (status, body) = send_json(
        &router,
        "DELETE",
        &format!("/api/v1/namespaces/{}/secrets/secret", ns),
        None,
    )
    .await;
    assert!(
        status == 200 || status == 202,
        "Secret DELETE must return 200/202 (mirrors `deleteSecretOrErrorf`); \
         got {} — body={}",
        status,
        body
    );

    // -----------------------------------------------------------------
    // DoTestSecretsImmutableWithEmptyValue — regression for upstream
    // PR #119229. Strategic-merge PATCH on an immutable Secret whose
    // data map already contains an empty-string value must succeed when
    // the patch repeats that same empty value (no real mutation).
    // -----------------------------------------------------------------

    // Re-create the same Secret name but mark it `Immutable: true` and seed
    // it with a single key whose value is the empty byte slice.
    // The upstream Go code uses `Data: map[string][]byte{"emptyData": {}}`.
    // base64("") == "" — represented in JSON as `"emptyData": ""`.
    let immutable_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "secret", "namespace": ns },
        "immutable": true,
        "data": { "emptyData": "" }
    });
    let (status, body) = send_json(
        &router,
        "POST",
        &format!("/api/v1/namespaces/{}/secrets", ns),
        Some(&immutable_body),
    )
    .await;
    assert_eq!(
        status, 201,
        "Immutable Secret with an empty-value data entry must be creatable \
         (mirrors `client.CoreV1().Secrets(s.Namespace).Create` in \
         DoTestSecretsImmutableWithEmptyValue); got {} — body={}",
        status, body
    );
    assert_eq!(
        body.get("immutable"),
        Some(&json!(true)),
        "Created Secret must round-trip `immutable: true`; body={}",
        body
    );

    // Strategic-merge PATCH that adds a label *and* re-states the same
    // immutable data. Upstream marshals the full Secret object as the patch
    // body and uses `types.StrategicMergePatchType` —
    // `application/strategic-merge-patch+json`.
    let patch_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "secret",
            "namespace": ns,
            "labels": { "foo": "bar" }
        },
        "immutable": true,
        "data": { "emptyData": "" }
    });
    let (status, body) = send(
        &router,
        "PATCH",
        &format!("/api/v1/namespaces/{}/secrets/secret", ns),
        Some(&patch_body),
        "application/strategic-merge-patch+json",
    )
    .await;
    assert_eq!(
        status, 200,
        "Strategic-merge PATCH that re-states the same immutable data + adds \
         a label must succeed (regression for upstream #119229); got {} — body={}",
        status, body
    );
    assert_eq!(
        body.pointer("/metadata/labels/foo"),
        Some(&json!("bar")),
        "PATCH must apply the new label; body={}",
        body
    );
    assert_eq!(
        body.get("immutable"),
        Some(&json!(true)),
        "PATCH must preserve `immutable: true`; body={}",
        body
    );

    // Mirror upstream's deferred cleanup.
    let (status, body) = send_json(
        &router,
        "DELETE",
        &format!("/api/v1/namespaces/{}/secrets/secret", ns),
        None,
    )
    .await;
    assert!(
        status == 200 || status == 202,
        "Final Secret DELETE must return 200/202; got {} — body={}",
        status,
        body
    );
}
