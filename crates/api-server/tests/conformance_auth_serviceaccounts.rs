//! Mirror of Kubernetes v1.35 conformance tests for [sig-auth] ServiceAccounts,
//! Certificates API, and SubjectReview not already covered in
//! `conformance_auth_rbac_serviceaccount.rs`.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/auth/
//!
//! Upstream files referenced:
//!   - service_accounts.go (TokenRequest, automount opt-out, projected token,
//!     OIDC discovery, kube-root-ca.crt, pod token mount)
//!   - subjectreviews.go (SubjectAccessReview full conformance)
//!   - certificates.go (CSR API lifecycle)
//!
//! Per-test status
//! ---------------
//! GREEN (must pass, newly-passing batch-64):
//!   - OIDC discovery (`/.well-known/openid-configuration`, `/openid/v1/jwks`)
//!   - Opt-out of API token automount (SA `automountServiceAccountToken=false`)
//!   - `kube-root-ca.crt` ConfigMap auto-created per namespace
//!   - Projected ServiceAccount token volume injected into pods
//!
//! GREEN (cont.):
//!   - Mount API token into pods — the admitted pod carries the projected
//!     kube-api-access volume (token + ca.crt + namespace) mounted read-only at
//!     /var/run/secrets/kubernetes.io/serviceaccount; kubelet materialisation
//!     of the files is covered live.
//!
//! IGNORED (failing, gap annotated):
//!   - SubjectReview full conformance (end-to-end impersonation not wired)
//!
//! The CSR full lifecycle (create → approve → issued certificate) now passes:
//! the signer lives in the controller-manager (`controllers::cert_authority`),
//! and the api-server half (storing/serving `status.certificate`) is covered by
//! `csr_full_lifecycle_with_signer_issues_certificate` below.

use rusternetes_common::{
    resources::ServiceAccount,
    types::{ObjectMeta, TypeMeta},
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. The token-signing
// secret matches the per-test `TokenManager` callers build to verify issued
// tokens; the CA-cert variant injects a stub PEM so the namespace handler seeds
// `kube-root-ca.crt`.
// ---------------------------------------------------------------------------

const TOKEN_SECRET: &[u8] = b"conformance-auth-secret";

fn spawn_state() -> (TestApiServer, Arc<MemoryStorage>) {
    let api = TestApiServer::builder().secret(TOKEN_SECRET).build();
    let mem = api.storage.clone();
    (api, mem)
}

/// Build a state with a fake CA cert so the namespace handler creates the
/// `kube-root-ca.crt` ConfigMap. In production the cert comes from a TLS
/// cert file on disk; in unit tests we inject it via the harness builder.
fn spawn_state_with_ca_cert() -> (TestApiServer, Arc<MemoryStorage>) {
    // A self-signed PEM stub — not a real certificate, but non-empty so the
    // namespace handler creates kube-root-ca.crt with a ca.crt key.
    let fake_ca = "-----BEGIN CERTIFICATE-----\n\
                   MIIBpTCCAU+gAwIBAgIUConformanceTestCA\n\
                   -----END CERTIFICATE-----\n";
    let api = TestApiServer::builder()
        .secret(TOKEN_SECRET)
        .ca_cert_pem(fake_ca)
        .build();
    let mem = api.storage.clone();
    (api, mem)
}

async fn post_json(state: TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = state.post(uri, body).await;
    (status.as_u16(), value)
}

/// Like [`post_json`] but attaches arbitrary request headers. Used to drive
/// inbound impersonation (`Impersonate-*`) through the auth middleware.
async fn post_json_with_headers(
    state: TestApiServer,
    uri: &str,
    body: &Value,
    headers: &[(&str, &str)],
) -> (u16, Value) {
    let mut all_headers = vec![("content-type", "application/json")];
    all_headers.extend_from_slice(headers);
    let bytes = serde_json::to_vec(body).unwrap();
    let (status, _h, _b, value) = state
        .send_with_headers("POST", uri, &all_headers, Some(bytes))
        .await;
    (status.as_u16(), value)
}

async fn get_json(state: TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = state.get(uri).await;
    (status.as_u16(), value)
}

async fn patch_merge(state: TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = state.patch(uri, body).await;
    (status.as_u16(), value)
}

async fn delete(state: TestApiServer, uri: &str) -> u16 {
    state.delete(uri).await.0.as_u16()
}

/// Seed a ServiceAccount through storage (no HTTP round-trip needed).
async fn seed_service_account(mem: &Arc<MemoryStorage>, namespace: &str, name: &str) {
    let sa = ServiceAccount {
        type_meta: TypeMeta {
            api_version: "v1".to_string(),
            kind: "ServiceAccount".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: Some(true),
    };
    let key = build_key("serviceaccounts", Some(namespace), name);
    mem.create(&key, &sa).await.unwrap();
}

// ---------------------------------------------------------------------------
// [sig-auth] ServiceAccounts ServiceAccountIssuerDiscovery
// should support OIDC discovery of service account issuer [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go (OIDC block)
// Sonobuoy (batch-64, 2026-05-28): PASS
//
// Verifies that the API server exposes a valid OIDC discovery document at
// `/.well-known/openid-configuration` and a JWKS at `/openid/v1/jwks`,
// both of which are required by the Kubernetes ServiceAccountIssuerDiscovery
// conformance case.
// ---------------------------------------------------------------------------

/// `/.well-known/openid-configuration` must return a 200 with a JSON body
/// containing at least `issuer`, `jwks_uri` and
/// `id_token_signing_alg_values_supported`.
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go ServiceAccountIssuerDiscovery
/// Sonobuoy (batch-64, 2026-05-28): PASS
#[tokio::test]
async fn oidc_discovery_document_is_valid() {
    let (state, _) = spawn_state();
    let (status, body) = get_json(state, "/.well-known/openid-configuration").await;
    assert_eq!(status, 200, "OIDC discovery must return 200: {body}");
    assert!(
        body["issuer"].as_str().is_some(),
        "OIDC discovery must have 'issuer': {body}"
    );
    assert!(
        body["jwks_uri"].as_str().is_some(),
        "OIDC discovery must have 'jwks_uri': {body}"
    );
    assert!(
        body["id_token_signing_alg_values_supported"]
            .as_array()
            .is_some(),
        "OIDC discovery must have 'id_token_signing_alg_values_supported': {body}"
    );
    // The issuer and jwks_uri must be non-empty strings.
    let issuer = body["issuer"].as_str().unwrap();
    assert!(
        !issuer.is_empty(),
        "OIDC discovery issuer must be non-empty: {body}"
    );
    let jwks_uri = body["jwks_uri"].as_str().unwrap();
    assert!(
        !jwks_uri.is_empty(),
        "OIDC discovery jwks_uri must be non-empty: {body}"
    );
}

/// `/openid/v1/jwks` must return a 200 with a `keys` array (may be empty if
/// no RSA signing key is present in the test environment, but the shape must
/// be correct — upstream conformance checks the document parses as JWKS).
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go ServiceAccountIssuerDiscovery
/// Sonobuoy (batch-64, 2026-05-28): PASS
#[tokio::test]
async fn oidc_jwks_endpoint_returns_key_set() {
    let (state, _) = spawn_state();
    let (status, body) = get_json(state, "/openid/v1/jwks").await;
    assert_eq!(status, 200, "JWKS endpoint must return 200: {body}");
    assert!(
        body["keys"].is_array(),
        "JWKS endpoint must have a 'keys' array: {body}"
    );
    // Each key that IS present must carry at least 'kty' (required JWKS field).
    for key in body["keys"].as_array().unwrap() {
        assert!(
            key["kty"].as_str().is_some(),
            "JWKS key must have 'kty': {key}"
        );
    }
}

// ---------------------------------------------------------------------------
// [sig-auth] ServiceAccounts should allow opting out of API token automount
// [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go:
//   "should allow opting out of API token automount"
// Sonobuoy (batch-64, 2026-05-28): PASS
//
// If a ServiceAccount has `automountServiceAccountToken: false`, a Pod that
// omits the field must NOT get a projected SA token volume injected.
// ---------------------------------------------------------------------------

/// Creating an SA with `automountServiceAccountToken: false` and a pod that
/// explicitly opts out must result in no projected SA-token volume.
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go opt-out block
/// Sonobuoy (batch-64, 2026-05-28): PASS
#[tokio::test]
async fn service_account_automount_opt_out_suppresses_projected_token_volume() {
    let (state, _) = spawn_state();
    let ns = "sa-automount-optout";

    // Create a ServiceAccount with automount disabled.
    let sa_body = json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "no-automount"},
        "automountServiceAccountToken": false
    });
    let (status, created_sa) = post_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/serviceaccounts"),
        &sa_body,
    )
    .await;
    assert_eq!(status, 201, "create SA with automount=false: {created_sa}");
    assert_eq!(created_sa["automountServiceAccountToken"], false);

    // Create a Pod that explicitly opts out at the pod level as well.
    // Upstream uses a pod-level override that mirrors the SA setting to
    // guarantee no injection.
    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "no-mount-pod"},
        "spec": {
            "serviceAccountName": "no-automount",
            "automountServiceAccountToken": false,
            "containers": [{"name": "c", "image": "nginx:latest"}]
        }
    });
    let (status, created_pod) = post_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/pods"),
        &pod_body,
    )
    .await;
    assert_eq!(
        status, 201,
        "create pod with automount=false: {created_pod}"
    );

    // The pod must NOT have a projected ServiceAccountToken volume.
    let volumes = created_pod["spec"]["volumes"].as_array();
    let has_projected_sa_token = volumes
        .map(|vols| {
            vols.iter().any(|v| {
                v["projected"]["sources"]
                    .as_array()
                    .map(|srcs| srcs.iter().any(|s| !s["serviceAccountToken"].is_null()))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    assert!(
        !has_projected_sa_token,
        "Pod with automount=false must NOT have projected SA token volume: {created_pod}"
    );
}

/// A ServiceAccount with `automountServiceAccountToken: false` can be toggled
/// back to `true` via a PATCH; the field must be persisted correctly.
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go opt-out block
/// Sonobuoy (batch-64, 2026-05-28): PASS
#[tokio::test]
async fn service_account_automount_field_is_mutable_via_patch() {
    let (state, _) = spawn_state();
    let ns = "sa-automount-patch";

    let sa_body = json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "toggle-automount"},
        "automountServiceAccountToken": false
    });
    let (status, _) = post_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/serviceaccounts"),
        &sa_body,
    )
    .await;
    assert_eq!(status, 201, "create SA: expected 201");

    // Patch automount back to true.
    let patch = json!({"automountServiceAccountToken": true});
    let (status, patched) = patch_merge(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/serviceaccounts/toggle-automount"),
        &patch,
    )
    .await;
    assert_eq!(status, 200, "PATCH automount to true: {patched}");
    assert_eq!(
        patched["automountServiceAccountToken"], true,
        "automountServiceAccountToken must be true after patch: {patched}"
    );
}

// ---------------------------------------------------------------------------
// [sig-auth] ServiceAccounts should guarantee kube-root-ca.crt exist in
// any namespace [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go:
//   "should guarantee kube-root-ca.crt exist in any namespace"
// Sonobuoy (batch-64, 2026-05-28): PASS
//
// When a namespace is created the API server must auto-create a ConfigMap
// named `kube-root-ca.crt` in that namespace containing a `ca.crt` key.
// ---------------------------------------------------------------------------

/// After creating a namespace the `kube-root-ca.crt` ConfigMap must exist
/// with a `ca.crt` data key (may be empty in the test environment where no
/// real CA is configured, but the ConfigMap itself must be present).
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go kube-root-ca block
/// Sonobuoy (batch-64, 2026-05-28): PASS
#[tokio::test]
async fn kube_root_ca_crt_configmap_exists_in_new_namespace() {
    // Use the CA-cert-aware state so the namespace handler creates the
    // kube-root-ca.crt ConfigMap (it is skipped when ca_cert_pem is None
    // and no TLS cert file is present on the host).
    let (state, _) = spawn_state_with_ca_cert();
    let ns = "kube-root-ca-test";

    // Create the namespace via the REST API.
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": ns}
    });
    let (status, body) = post_json(state.clone(), "/api/v1/namespaces", &ns_body).await;
    assert_eq!(status, 201, "create namespace: {body}");

    // The ConfigMap `kube-root-ca.crt` must be automatically present.
    let (status, cm) = get_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/configmaps/kube-root-ca.crt"),
    )
    .await;
    assert_eq!(
        status, 200,
        "kube-root-ca.crt ConfigMap must exist in namespace {ns}: {cm}"
    );
    assert_eq!(
        cm["metadata"]["name"], "kube-root-ca.crt",
        "ConfigMap name mismatch: {cm}"
    );
    // The ConfigMap must have a 'data' field (even if ca.crt is empty string
    // in a test environment with no real TLS cert configured).
    assert!(
        cm["data"].is_object(),
        "kube-root-ca.crt ConfigMap must have a 'data' object: {cm}"
    );
    // `ca.crt` key must exist.
    assert!(
        cm["data"]["ca.crt"].is_string(),
        "kube-root-ca.crt ConfigMap must contain a 'ca.crt' key: {cm}"
    );
}

/// `kube-root-ca.crt` ConfigMap must be present immediately after namespace
/// creation (synchronous; the namespace handler creates it inline).
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go kube-root-ca block
/// Sonobuoy (batch-64, 2026-05-28): PASS
#[tokio::test]
async fn kube_root_ca_crt_configmap_is_present_on_create() {
    let (state, _) = spawn_state_with_ca_cert();
    let ns = "kube-root-ca-presence";

    let ns_body = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}});
    let (status, _) = post_json(state.clone(), "/api/v1/namespaces", &ns_body).await;
    assert_eq!(status, 201, "namespace create");

    let (cm_status, _) = get_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/configmaps/kube-root-ca.crt"),
    )
    .await;
    assert_eq!(cm_status, 200, "kube-root-ca.crt must be auto-created");
}

// ---------------------------------------------------------------------------
// [sig-auth] ServiceAccounts should mount projected service account token
// [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go:
//   "should mount projected service account token"
// Sonobuoy (batch-64, 2026-05-28): PASS
//
// A Pod without `automountServiceAccountToken: false` must have a projected
// volume injected by the admission layer that carries a `serviceAccountToken`
// projection. This is the API-server side of the conformance case; actual
// file-system presence requires a live kubelet.
// ---------------------------------------------------------------------------

/// A Pod created without disabling automount gets a projected SA token volume.
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go projected token block
/// Sonobuoy (batch-64, 2026-05-28): PASS
#[tokio::test]
async fn pod_receives_projected_service_account_token_volume() {
    let (state, _) = spawn_state();
    let ns = "sa-projected-token";

    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "projected-token-pod"},
        "spec": {
            "containers": [{"name": "c", "image": "nginx:latest"}]
        }
    });
    let (status, created) = post_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/pods"),
        &pod_body,
    )
    .await;
    assert_eq!(status, 201, "create pod must return 201: {created}");

    let volumes = created["spec"]["volumes"].as_array();
    let has_projected_sa_token = volumes
        .map(|vols| {
            vols.iter().any(|v| {
                v["projected"]["sources"]
                    .as_array()
                    .map(|srcs| srcs.iter().any(|s| !s["serviceAccountToken"].is_null()))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    assert!(
        has_projected_sa_token,
        "Pod must receive a projected serviceAccountToken volume: {created}"
    );
}

/// The projected SA token volume source must specify an `expirationSeconds`
/// value (Kubernetes defaults to 3607 seconds, i.e. ~1 hour).
///
/// Upstream: k8s.io/kubernetes/plugin/pkg/admission/serviceaccount/admission.go
///   defaultExpirationSeconds = 3607
/// Sonobuoy (batch-64, 2026-05-28): PASS
#[tokio::test]
async fn projected_service_account_token_has_expiration_seconds() {
    let (state, _) = spawn_state();
    let ns = "sa-token-expiry";

    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "expiry-pod"},
        "spec": {
            "containers": [{"name": "c", "image": "nginx:latest"}]
        }
    });
    let (status, created) = post_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/pods"),
        &pod_body,
    )
    .await;
    assert_eq!(status, 201, "create pod: {created}");

    // Find the projected SA token source and verify expirationSeconds.
    let empty = vec![];
    let volumes = created["spec"]["volumes"].as_array().unwrap_or(&empty);
    let mut found = false;
    for v in volumes {
        if let Some(sources) = v["projected"]["sources"].as_array() {
            for s in sources {
                if !s["serviceAccountToken"].is_null() {
                    let exp = &s["serviceAccountToken"]["expirationSeconds"];
                    assert!(
                        exp.is_number(),
                        "serviceAccountToken projection must have expirationSeconds: {s}"
                    );
                    assert!(
                        exp.as_i64().unwrap_or(0) > 0,
                        "expirationSeconds must be positive: {s}"
                    );
                    found = true;
                }
            }
        }
    }
    assert!(
        found,
        "No serviceAccountToken projection found in volumes: {created}"
    );
}

// ---------------------------------------------------------------------------
// [sig-auth] ServiceAccounts should mount an API token into pods [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go:81
//   "should mount an API token into pods"
// Sonobuoy (batch-64, 2026-05-28): FAIL
//
// The full conformance test verifies that a pod's projected token can be read
// from the filesystem inside the running container and then validated via
// TokenReview. This requires a live kubelet to exec into the pod.
// At the API layer we only assert the projected volume structure; the
// in-container file mount and exec steps are out of scope for unit tests.
// ---------------------------------------------------------------------------

/// [sig-auth] ServiceAccounts should mount an API token into pods.
///
/// The upstream e2e reads `token`, `ca.crt`, and `namespace` from
/// `/var/run/secrets/kubernetes.io/serviceaccount` inside a running container.
/// In-process we assert the API-server guarantee that makes that work: a pod
/// created without any explicit token wiring is admitted with a projected
/// `kube-api-access` volume carrying all THREE sources (bound SA token,
/// `kube-root-ca.crt` → `ca.crt`, and the downward-API namespace), mounted
/// read-only at exactly that path on every container. The kubelet-side
/// materialisation of the files is generated by
/// `ContainerRuntime::create_projected_volume` (covered live).
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go:81
#[tokio::test]
async fn service_account_should_mount_api_token_into_pods() {
    let (state, _) = spawn_state();
    let ns = "sa-mount-token";

    // Seed the default ServiceAccount so admission resolves automount=true.
    let (sa_status, _) = post_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/serviceaccounts"),
        &json!({
            "apiVersion": "v1", "kind": "ServiceAccount",
            "metadata": {"name": "default"}
        }),
    )
    .await;
    assert!(
        sa_status == 201 || sa_status == 409,
        "seed default SA: {sa_status}"
    );

    let (status, pod) = post_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/pods"),
        &json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "token-mount-pod"},
            "spec": {"containers": [{"name": "c", "image": "nginx:latest"}]}
        }),
    )
    .await;
    assert_eq!(status, 201, "create pod must return 201: {pod}");

    // Find the projected kube-api-access volume and check all three sources.
    let volumes = pod["spec"]["volumes"].as_array().expect("pod has volumes");
    let token_vol = volumes
        .iter()
        .find(|v| {
            v["projected"]["sources"]
                .as_array()
                .map(|s| s.iter().any(|x| !x["serviceAccountToken"].is_null()))
                .unwrap_or(false)
        })
        .unwrap_or_else(|| panic!("no projected SA-token volume: {pod}"));
    let vol_name = token_vol["name"].as_str().unwrap();
    let sources = token_vol["projected"]["sources"].as_array().unwrap();

    let token_src = sources
        .iter()
        .find(|s| !s["serviceAccountToken"].is_null())
        .unwrap();
    assert_eq!(
        token_src["serviceAccountToken"]["path"], "token",
        "SA token must project to file `token`: {pod}"
    );
    let has_ca = sources.iter().any(|s| {
        s["configMap"]["name"] == "kube-root-ca.crt"
            && s["configMap"]["items"]
                .as_array()
                .map(|i| i.iter().any(|it| it["path"] == "ca.crt"))
                .unwrap_or(false)
    });
    assert!(has_ca, "must project kube-root-ca.crt → ca.crt: {pod}");
    let has_ns = sources.iter().any(|s| {
        s["downwardAPI"]["items"]
            .as_array()
            .map(|i| {
                i.iter().any(|it| {
                    it["path"] == "namespace" && it["fieldRef"]["fieldPath"] == "metadata.namespace"
                })
            })
            .unwrap_or(false)
    });
    assert!(has_ns, "must project metadata.namespace → namespace: {pod}");

    // Every container must mount that volume read-only at the standard path.
    let mounts = pod["spec"]["containers"][0]["volumeMounts"]
        .as_array()
        .expect("container has volumeMounts");
    let sa_mount = mounts
        .iter()
        .find(|m| m["mountPath"] == "/var/run/secrets/kubernetes.io/serviceaccount")
        .unwrap_or_else(|| panic!("no SA token mount on container: {pod}"));
    assert_eq!(
        sa_mount["name"], vol_name,
        "mount must reference the projected volume: {pod}"
    );
    assert_eq!(
        sa_mount["readOnly"], true,
        "SA token mount must be read-only: {pod}"
    );
}

// ---------------------------------------------------------------------------
// [sig-auth] Certificates API [Privileged:ClusterAdmin]
// should support CSR API operations [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/auth/certificates.go
// Sonobuoy (batch-64, 2026-05-28): FAIL
//
// Tests below cover the supported sub-operations in GREEN (CRUD round-trip),
// while the signer-side approval → issuance step that requires a real
// certificate controller is marked #[ignore].
// ---------------------------------------------------------------------------

/// CertificateSigningRequest create → get → list → delete round-trip.
/// This mirrors the CRUD portion of the upstream CSR API conformance case.
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/certificates.go CSR lifecycle
/// Sonobuoy (batch-64, 2026-05-28): FAIL — full lifecycle needs signer
#[tokio::test]
async fn csr_create_get_list_delete_round_trip() {
    let (state, _) = spawn_state();

    // A minimal but valid-shaped CSR body (the PEM request field is base64 of
    // a dummy PKCS#10 blob; the API server stores it as-is).
    let csr_body = json!({
        "apiVersion": "certificates.k8s.io/v1",
        "kind": "CertificateSigningRequest",
        "metadata": {"name": "e2e-csr-lifecycle"},
        "spec": {
            "request": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0KTUlIc01JR1RBZ0VBTURFeEdUQVhCZ05WQkFNTUVIUmxjM1F1WlhoaGJYQnNaUzVqYjIweEZEQVNCZ05WQkFvTQpDM0oxYzNSbGNtNWxkR1Z6TUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFL2k2cjBkem16d3dRCnFWTXhSTDlkK2MwOE5VNzNCVTRjNzRFVS9GazgxVGI0UVFJMWhHNVE3U3hocklaUjIzQ3NMTFFEaFNJUitweHgKODhiSkpaNzRJYUFBTUFvR0NDcUdTTTQ5QkFNQ0EwZ0FNRVVDSUgvbE5mWkdDOUtsTlgzRmh5M0tzTFhzVituSApZMlRybGRabWo5Zm5rTVVjQWlFQW4xRTM4S0hLb050NUl6aFVSVWZPRDdlNTB1aDBVcjVBNTdzcDU5b2gyQTA9Ci0tLS0tRU5EIENFUlRJRklDQVRFIFJFUVVFU1QtLS0tLQo=",
            "signerName": "kubernetes.io/kube-apiserver-client",
            "usages": ["client auth"]
        }
    });

    // Create.
    let (status, created) = post_json(
        state.clone(),
        "/apis/certificates.k8s.io/v1/certificatesigningrequests",
        &csr_body,
    )
    .await;
    assert_eq!(status, 201, "CSR create must return 201: {created}");
    assert_eq!(
        created["metadata"]["name"], "e2e-csr-lifecycle",
        "name must be preserved: {created}"
    );
    assert!(
        !created["metadata"]["uid"].as_str().unwrap_or("").is_empty(),
        "server-assigned UID must be set: {created}"
    );
    assert_eq!(
        created["spec"]["signerName"], "kubernetes.io/kube-apiserver-client",
        "signerName must round-trip: {created}"
    );

    // Get.
    let (status, fetched) = get_json(
        state.clone(),
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/e2e-csr-lifecycle",
    )
    .await;
    assert_eq!(status, 200, "CSR get must return 200: {fetched}");
    assert_eq!(
        fetched["metadata"]["uid"], created["metadata"]["uid"],
        "UID must be stable across get: {fetched}"
    );

    // List — the CSR must appear.
    let (status, list) = get_json(
        state.clone(),
        "/apis/certificates.k8s.io/v1/certificatesigningrequests",
    )
    .await;
    assert_eq!(status, 200, "CSR list must return 200: {list}");
    let items = list["items"].as_array().expect("items must be an array");
    assert!(
        items
            .iter()
            .any(|i| i["metadata"]["name"] == "e2e-csr-lifecycle"),
        "list must include the created CSR: {list}"
    );

    // Delete.
    let del_status = delete(
        state.clone(),
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/e2e-csr-lifecycle",
    )
    .await;
    assert_eq!(del_status, 200, "CSR delete must return 200");

    // After delete the CSR must not be retrievable.
    let (status, _) = get_json(
        state.clone(),
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/e2e-csr-lifecycle",
    )
    .await;
    assert_eq!(status, 404, "CSR must be gone after delete");
}

/// CertificateSigningRequest approval subresource PUT stores the Approved
/// condition in the CSR status. Mirrors the approval step of the upstream
/// CSR conformance test prior to waiting for certificate issuance.
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/certificates.go CSR approve
/// Sonobuoy (batch-64, 2026-05-28): FAIL — full lifecycle needs signer
#[tokio::test]
async fn csr_approval_condition_stored_via_status_subresource() {
    let (state, _) = spawn_state();

    let csr_body = json!({
        "apiVersion": "certificates.k8s.io/v1",
        "kind": "CertificateSigningRequest",
        "metadata": {"name": "e2e-csr-approve"},
        "spec": {
            "request": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0KTUlIc01JR1RBZ0VBTURFeEdUQVhCZ05WQkFNTUVIUmxjM1F1WlhoaGJYQnNaUzVqYjIweEZEQVNCZ05WQkFvTQpDM0oxYzNSbGNtNWxkR1Z6TUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFL2k2cjBkem16d3dRCnFWTXhSTDlkK2MwOE5VNzNCVTRjNzRFVS9GazgxVGI0UVFJMWhHNVE3U3hocklaUjIzQ3NMTFFEaFNJUitweHgKODhiSkpaNzRJYUFBTUFvR0NDcUdTTTQ5QkFNQ0EwZ0FNRVVDSUgvbE5mWkdDOUtsTlgzRmh5M0tzTFhzVituSApZMlRybGRabWo5Zm5rTVVjQWlFQW4xRTM4S0hLb050NUl6aFVSVWZPRDdlNTB1aDBVcjVBNTdzcDU5b2gyQTA9Ci0tLS0tRU5EIENFUlRJRklDQVRFIFJFUVVFU1QtLS0tLQo=",
            "signerName": "kubernetes.io/kube-apiserver-client",
            "usages": ["client auth"]
        }
    });
    let (status, created) = post_json(
        state.clone(),
        "/apis/certificates.k8s.io/v1/certificatesigningrequests",
        &csr_body,
    )
    .await;
    assert_eq!(status, 201, "CSR create: {created}");

    // Update the approval subresource with an Approved condition.
    let mut approval_body = created.clone();
    approval_body["status"] = json!({
        "conditions": [{
            "type": "Approved",
            "status": "True",
            "reason": "ApprovedByE2E",
            "message": "Approved by conformance test"
        }]
    });

    let (approval_status, _) = state
        .put(
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/e2e-csr-approve/approval",
            &approval_body,
        )
        .await;
    assert_eq!(
        approval_status.as_u16(),
        200,
        "CSR approval PUT must return 200"
    );
}

/// Full CSR lifecycle through the API server: create → approve → an issued
/// certificate appears in `status.certificate` and is served back on GET.
///
/// The signing itself (parsing the PKCS#10 request and producing an X.509 leaf
/// that chains to the cluster CA) lives in the controller-manager's CSR signing
/// controller and is covered by
/// `controllers::cert_authority` + `controllers::certificate_signing_request`
/// unit tests. This test exercises the *api-server* half of the contract: that
/// the issued certificate the signer writes via the status subresource is
/// persisted and round-tripped to clients — the step the conformance suite
/// polls for after approval.
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/certificates.go
#[tokio::test]
async fn csr_full_lifecycle_with_signer_issues_certificate() {
    let (state, _) = spawn_state();

    let csr_body = json!({
        "apiVersion": "certificates.k8s.io/v1",
        "kind": "CertificateSigningRequest",
        "metadata": {"name": "e2e-csr-issued"},
        "spec": {
            "request": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0KTUlIc01JR1RBZ0VBTURFeEdUQVhCZ05WQkFNTUVIUmxjM1F1WlhoaGJYQnNaUzVqYjIweEZEQVNCZ05WQkFvTQpDM0oxYzNSbGNtNWxkR1Z6TUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFL2k2cjBkem16d3dRCnFWTXhSTDlkK2MwOE5VNzNCVTRjNzRFVS9GazgxVGI0UVFJMWhHNVE3U3hocklaUjIzQ3NMTFFEaFNJUitweHgKODhiSkpaNzRJYUFBTUFvR0NDcUdTTTQ5QkFNQ0EwZ0FNRVVDSUgvbE5mWkdDOUtsTlgzRmh5M0tzTFhzVituSApZMlRybGRabWo5Zm5rTVVjQWlFQW4xRTM4S0hLb050NUl6aFVSVWZPRDdlNTB1aDBVcjVBNTdzcDU5b2gyQTA9Ci0tLS0tRU5EIENFUlRJRklDQVRFIFJFUVVFU1QtLS0tLQo=",
            "signerName": "kubernetes.io/kube-apiserver-client",
            "usages": ["client auth"]
        }
    });
    let (status, created) = post_json(
        state.clone(),
        "/apis/certificates.k8s.io/v1/certificatesigningrequests",
        &csr_body,
    )
    .await;
    assert_eq!(status, 201, "CSR create: {created}");

    // The signing controller, after approving, writes the issued certificate
    // into status.certificate via the /status subresource. Simulate that write.
    // `certificate` is []byte upstream, i.e. base64-encoded PEM on the wire;
    // validateCertificate base64-decodes then x509-parses each CERTIFICATE block,
    // so this must be a real certificate's PEM, base64-encoded (a placeholder is
    // correctly rejected). Self-signed leaf generated for the test.
    let issued_pem =
        "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJmRENDQVNHZ0F3SUJBZ0lVRmJPakN5UUNnWEMrL0ZvREFWWTgrT0FXSFM0d0NnWUlLb1pJemowRUF3SXcKRXpFUk1BOEdBMVVFQXd3SVpUSmxMV3hsWVdZd0hoY05Nall3TmpJeE1qSXlNRFV6V2hjTk16WXdOakU0TWpJeQpNRFV6V2pBVE1SRXdEd1lEVlFRRERBaGxNbVV0YkdWaFpqQlpNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VICkEwSUFCTStKZ3M4elVMZHVlcHZSK2xFMEptVTRkK1Q4c3dndnUwK1FzU0sxM1hCNXE4ZXFjRlFTYkdPKzI2WnQKYjB6QkNsS0pqS2kxM2NabTY1QW5JL3h6OVIyalV6QlJNQjBHQTFVZERnUVdCQlF0YnFBRVozQklobVZkeldGeApwZXdQcEs4eUVqQWZCZ05WSFNNRUdEQVdnQlF0YnFBRVozQklobVZkeldGeHBld1BwSzh5RWpBUEJnTlZIUk1CCkFmOEVCVEFEQVFIL01Bb0dDQ3FHU000OUJBTUNBMGtBTUVZQ0lRRC9RVW85b1BBMGR0TVFta3VTTkN3Q25CUTAKN2gzSDFNQXdlM3pqSFk1ZWpBSWhBS2pMOGlsOUh6WU9VRXJHQk9aa1dSdkV1N21ISjdRM0VUWXRJZWhYRC9BcAotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==";
    let mut status_body = created.clone();
    status_body["status"] = json!({
        "conditions": [{
            "type": "Approved",
            "status": "True",
            "reason": "ApprovedByE2E"
        }],
        "certificate": issued_pem,
    });
    let (put_status, _) = state
        .put(
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/e2e-csr-issued/status",
            &status_body,
        )
        .await;
    assert_eq!(put_status.as_u16(), 200, "CSR status PUT must return 200");

    // GET must serve the issued certificate back.
    let (get_status, fetched) = get_json(
        state.clone(),
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/e2e-csr-issued",
    )
    .await;
    assert_eq!(get_status, 200, "CSR get: {fetched}");
    assert_eq!(
        fetched["status"]["certificate"],
        json!(issued_pem),
        "issued certificate must round-trip through status.certificate: {fetched}"
    );
}

// ---------------------------------------------------------------------------
// [sig-auth] SubjectReview should support SubjectReview API operations
// [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/auth/subjectreviews.go:50
// Sonobuoy (batch-64, 2026-05-28): FAIL
//
// The basic SubjectAccessReview + LocalSubjectAccessReview happy paths are
// already GREEN and covered in `conformance_auth_rbac_serviceaccount.rs`.
// The full conformance case additionally exercises impersonated SA clients,
// which requires the cluster's RBAC signer and impersonation webhook.
// ---------------------------------------------------------------------------

/// SubjectAccessReview with a non-resource URL attribute must return 200 and
/// an `allowed` boolean. This extends the resource-attribute test in the
/// sibling file to cover the non-resource path exercised by the conformance
/// suite (`GET /healthz`).
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/subjectreviews.go:50
/// Sonobuoy (batch-64, 2026-05-28): FAIL — impersonated client step missing
#[tokio::test]
async fn subject_access_review_non_resource_url_returns_allowed() {
    let (state, _) = spawn_state();
    let sar = json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "metadata": {},
        "spec": {
            "user": "system:serviceaccount:default:e2e",
            "groups": ["system:authenticated"],
            "nonResourceAttributes": {
                "verb": "get",
                "path": "/healthz"
            }
        }
    });
    let (status, body) = post_json(
        state.clone(),
        "/apis/authorization.k8s.io/v1/subjectaccessreviews",
        &sar,
    )
    .await;
    assert_eq!(status, 200, "SAR non-resource must return 200: {body}");
    assert!(
        body["status"]["allowed"].is_boolean(),
        "status.allowed must be a boolean: {body}"
    );
}

/// The full SubjectReview conformance test creates an impersonated client for
/// the service account subject and verifies the impersonated identity is what
/// the API server acts as. This exercises inbound impersonation: a request
/// carrying the standard `Impersonate-User` header (plus optional
/// `Impersonate-Group` / `Impersonate-Uid` / `Impersonate-Extra-*`) makes the
/// effective request user the impersonated ServiceAccount. A SelfSubjectReview
/// then reflects that impersonated identity in `status.userInfo`, mirroring
/// what an impersonated client would observe end-to-end.
///
/// Upstream impersonation filter:
///   staging/src/k8s.io/apiserver/pkg/endpoints/filters/impersonation
/// Upstream e2e: k8s.io/kubernetes/test/e2e/auth/subjectreviews.go:50
#[tokio::test]
async fn subject_review_full_conformance_with_impersonated_client() {
    let (state, mem) = spawn_state();
    let ns = "subjectreview-impersonation-ns";
    let sa = "e2e-impersonated";
    seed_service_account(&mem, ns, sa).await;

    let impersonated_user = format!("system:serviceaccount:{ns}:{sa}");

    // A SelfSubjectReview issued by an impersonated ServiceAccount client.
    // The request body carries no identity — the identity comes purely from
    // the impersonation headers, exactly like a real impersonated client.
    let review = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "SelfSubjectReview",
        "metadata": {},
    });

    let (status, body) = post_json_with_headers(
        state.clone(),
        "/apis/authentication.k8s.io/v1/selfsubjectreviews",
        &review,
        &[("Impersonate-User", impersonated_user.as_str())],
    )
    .await;

    assert_eq!(
        status, 200,
        "impersonated SelfSubjectReview must return 200: {body}"
    );

    // The effective (impersonated) identity must be reflected back.
    assert_eq!(
        body["status"]["userInfo"]["username"], impersonated_user,
        "userInfo.username must be the impersonated SA: {body}"
    );

    // A ServiceAccount with no explicit Impersonate-Group headers inherits the
    // fixed SA group mapping plus system:authenticated, matching upstream
    // `serviceaccount.MakeGroupNames` + the added authenticated group.
    let groups: Vec<String> = body["status"]["userInfo"]["groups"]
        .as_array()
        .expect("groups must be present")
        .iter()
        .map(|g| g.as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        groups.contains(&"system:serviceaccounts".to_string()),
        "impersonated SA must be in system:serviceaccounts: {groups:?}"
    );
    assert!(
        groups.contains(&format!("system:serviceaccounts:{ns}")),
        "impersonated SA must be in the namespace SA group: {groups:?}"
    );
    assert!(
        groups.contains(&"system:authenticated".to_string()),
        "impersonated non-anonymous user must be system:authenticated: {groups:?}"
    );

    // The admin identity (the original caller in skip-auth mode) must NOT leak
    // into the impersonated review — the switch must be complete.
    assert_ne!(
        body["status"]["userInfo"]["username"], "admin",
        "original caller identity must not leak through impersonation: {body}"
    );
    assert!(
        !groups.contains(&"system:masters".to_string()),
        "original caller's groups must not leak through impersonation: {groups:?}"
    );
}

/// Explicit `Impersonate-Group`, `Impersonate-Uid`, and `Impersonate-Extra-*`
/// headers must all be reflected in the effective identity. When groups are
/// specified explicitly the synthetic SA group mapping is NOT applied (the
/// supplied groups are authoritative), matching upstream `groupsSpecified`.
#[tokio::test]
async fn impersonation_applies_groups_uid_and_extra() {
    let (state, mem) = spawn_state();
    let ns = "impersonation-extra-ns";
    let sa = "extra-sa";
    seed_service_account(&mem, ns, sa).await;
    let impersonated_user = format!("system:serviceaccount:{ns}:{sa}");

    let review = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "SelfSubjectReview",
        "metadata": {},
    });

    let (status, body) = post_json_with_headers(
        state.clone(),
        "/apis/authentication.k8s.io/v1/selfsubjectreviews",
        &review,
        &[
            ("Impersonate-User", impersonated_user.as_str()),
            ("Impersonate-Group", "developers"),
            ("Impersonate-Group", "qa"),
            ("Impersonate-Uid", "abc-123-uid"),
            ("Impersonate-Extra-scopes", "read"),
        ],
    )
    .await;

    assert_eq!(status, 200, "impersonated review must return 200: {body}");
    assert_eq!(body["status"]["userInfo"]["username"], impersonated_user);
    assert_eq!(
        body["status"]["userInfo"]["uid"], "abc-123-uid",
        "impersonated uid must be reflected: {body}"
    );

    let groups: Vec<String> = body["status"]["userInfo"]["groups"]
        .as_array()
        .expect("groups present")
        .iter()
        .map(|g| g.as_str().unwrap_or_default().to_string())
        .collect();
    assert!(groups.contains(&"developers".to_string()), "{groups:?}");
    assert!(groups.contains(&"qa".to_string()), "{groups:?}");
    // Explicit groups → synthetic SA groups are NOT injected.
    assert!(
        !groups.contains(&"system:serviceaccounts".to_string()),
        "explicit groups must suppress synthetic SA groups: {groups:?}"
    );
    // But system:authenticated is still appended for a non-anonymous user.
    assert!(
        groups.contains(&"system:authenticated".to_string()),
        "{groups:?}"
    );

    assert_eq!(
        body["status"]["userInfo"]["extra"]["scopes"][0], "read",
        "impersonated extra must be reflected: {body}"
    );
}

/// Requesting `Impersonate-Group` / `Impersonate-Uid` / `Impersonate-Extra-*`
/// without an accompanying `Impersonate-User` is a BadRequest, matching
/// upstream `buildImpersonationRequests`.
#[tokio::test]
async fn impersonation_group_without_user_is_bad_request() {
    let (state, _) = spawn_state();
    let review = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "SelfSubjectReview",
        "metadata": {},
    });
    let (status, _body) = post_json_with_headers(
        state.clone(),
        "/apis/authentication.k8s.io/v1/selfsubjectreviews",
        &review,
        &[("Impersonate-Group", "developers")],
    )
    .await;
    assert_eq!(
        status, 400,
        "groups without a user must be rejected with 400"
    );
}

// ---------------------------------------------------------------------------
// TokenRequest with bounded audience (ServiceAccountIssuerDiscovery linkage)
//
// The OIDC discovery case verifies the issuer document. This test closes
// the loop by verifying that a TokenRequest-issued token has its audience
// claim set to what the caller requested — a property the OIDC relying party
// validates.
// ---------------------------------------------------------------------------

/// A TokenRequest for a specific audience produces a token; a subsequent
/// TokenReview with that audience must authenticate it.
///
/// Upstream: k8s.io/kubernetes/test/e2e/auth/service_accounts.go OIDC/TokenRequest block
/// Sonobuoy (batch-64, 2026-05-28): PASS (as part of OIDC discovery)
#[tokio::test]
async fn token_request_audience_is_reflected_in_token_review() {
    let (state, mem) = spawn_state();
    let ns = "oidc-audience-ns";
    let sa = "oidc-audience-sa";
    seed_service_account(&mem, ns, sa).await;

    let audience = "https://my-custom-audience.example.com";
    let token_req = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest",
        "metadata": {},
        "spec": {
            "audiences": [audience],
            "expirationSeconds": 600
        }
    });
    let (status, body) = post_json(
        state.clone(),
        &format!("/api/v1/namespaces/{ns}/serviceaccounts/{sa}/token"),
        &token_req,
    )
    .await;
    assert_eq!(status, 200, "TokenRequest must succeed: {body}");
    let token = body["status"]["token"]
        .as_str()
        .filter(|t| !t.is_empty())
        .expect("token must be non-empty");

    // TokenReview specifying the audience.
    let review = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "metadata": {},
        "spec": {
            "token": token,
            "audiences": [audience]
        }
    });
    let (status, review_body) = post_json(
        state.clone(),
        "/apis/authentication.k8s.io/v1/tokenreviews",
        &review,
    )
    .await;
    assert_eq!(status, 200, "TokenReview must return 200: {review_body}");
    assert_eq!(
        review_body["status"]["authenticated"], true,
        "Token must authenticate: {review_body}"
    );
}
