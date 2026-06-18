//! Accept-header content-negotiation matrix for GET requests.
//!
//! Mirrors the upstream Kubernetes negotiation tests in
//! `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/negotiated_codec_factory_test.go`
//! and `negotiate_test.go`. Upstream apiserver uses the Accept header to pick
//! a Serializer / MediaType (JSON, YAML, Protobuf, Table, PartialObjectMetadata)
//! and falls back through the quality-value list before returning
//! 406 Not Acceptable.
//!
//! Rusternetes' current negotiation surface:
//! - `crates/api-server/src/response.rs::negotiate_content_type` recognizes
//!   `application/vnd.kubernetes.protobuf` and returns `ContentType::Protobuf`,
//!   but the routes never use this helper directly — handlers always emit
//!   JSON via `axum::Json<T>`.
//! - `crates/api-server/src/middleware.rs` (the response wrapping branch at
//!   line 353) has `wants_protobuf = false` hardcoded — the comment cites
//!   `scripts/run-conformance.sh:78-80`: K8s protobuf requires native protobuf
//!   bytes which Rusternetes cannot produce, so the server forces JSON even
//!   when the client requests protobuf. Real client-go always sends
//!   `Accept: application/vnd.kubernetes.protobuf, application/json` and
//!   falls back to JSON when protobuf is unavailable.
//! - Neither `Accept-Encoding` nor `as=Table` / `as=PartialObjectMetadata`
//!   is implemented — the router has no `tower_http::compression` layer
//!   and no Table conversion code path.
//!
//! These tests pin the ACTUAL behavior — they do NOT force upstream contract
//! when Rusternetes diverges intentionally. Divergences are documented in
//! per-test docstrings. Gaps that block conformance get `#[ignore]` with a
//! one-liner pointing at the missing feature.
//!
//! Harness: in-process axum router over `StorageBackend::Memory`, driven via
//! `tower::ServiceExt::oneshot`. Same shape as
//! `decoder_content_type_test.rs` and the `conformance_apimachinery_*` files.

use axum::http::StatusCode;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

const TEST_NS: &str = "default";

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `send_with_headers`
// drives content negotiation (Accept / Accept-Encoding) and returns the
// response `HeaderMap` so these tests can read the resulting Content-Type /
// Content-Encoding. `mem` is the backing store for seeding GET targets.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// GET helper with an arbitrary list of request headers. Returns
/// `(status, response Content-Type, body bytes)`.
async fn get_with_headers(
    router: TestApiServer,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, String, Vec<u8>) {
    let (status, header_map, bytes, _) = router.send_with_headers("GET", uri, headers, None).await;
    let content_type = header_map
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    (status, content_type, bytes)
}

/// Convenience wrapper for the most common case: a single `Accept` header.
async fn get_with_accept(
    router: TestApiServer,
    uri: &str,
    accept: &str,
) -> (StatusCode, String, Vec<u8>) {
    get_with_headers(router, uri, &[("accept", accept)]).await
}

/// Seed a Pod into memory storage so we have a concrete GET target with a
/// stable JSON shape.
async fn seed_pod(mem: &Arc<MemoryStorage>, name: &str) {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
        },
        "spec": {
            "containers": [{"name": "c", "image": "busybox"}]
        }
    });
    let key = build_key("pods", Some(TEST_NS), name);
    mem.create(&key, &pod).await.expect("seed pod");
}

/// Seed a ConfigMap — a resource with ObjectMeta but no custom Table printer.
async fn seed_configmap(mem: &Arc<MemoryStorage>, name: &str) {
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
            "creationTimestamp": "2026-06-02T00:00:00Z",
        },
        "data": {"k": "v"}
    });
    let key = build_key("configmaps", Some(TEST_NS), name);
    mem.create(&key, &cm).await.expect("seed configmap");
}

/// Parse the body as JSON and assert it looks like the seeded Pod.
fn assert_pod_body(name: &str, body: &[u8]) {
    let v: Value = serde_json::from_slice(body).unwrap_or_else(|e| {
        panic!(
            "body must parse as JSON for {}: {:?}; raw={:?}",
            name, e, body
        )
    });
    assert_eq!(v["kind"], "Pod", "kind must be Pod; got {}", v);
    assert_eq!(
        v["metadata"]["name"], name,
        "metadata.name mismatch; got {}",
        v
    );
}

// ---------------------------------------------------------------------------
// 1-3. Default JSON path: explicit application/json, */*, and no Accept header
// ---------------------------------------------------------------------------

/// `Accept: application/json` is the canonical client-go default. Server must
/// answer with `Content-Type: application/json` and a JSON-decodable Pod.
#[tokio::test]
async fn accept_application_json_returns_json() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-json").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-json",
        "application/json",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "status={} body={:?}", status, body);
    assert!(
        ct.starts_with("application/json"),
        "Content-Type must be application/json; got {}",
        ct
    );
    assert_pod_body("p-json", &body);
}

/// `Accept: */*` — RFC 7231 wildcard. Server picks its default representation.
/// In Rusternetes the only available representation is JSON.
#[tokio::test]
async fn accept_wildcard_falls_back_to_default_json() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-wild").await;

    let (status, ct, body) =
        get_with_accept(router, "/api/v1/namespaces/default/pods/p-wild", "*/*").await;

    assert_eq!(status, StatusCode::OK, "status={} body={:?}", status, body);
    assert!(
        ct.starts_with("application/json"),
        "Accept: */* must default to application/json; got {}",
        ct
    );
    assert_pod_body("p-wild", &body);
}

/// No Accept header at all. RFC 7231 §5.3.2 says missing Accept is
/// equivalent to `*/*`. Server must serve the default representation.
#[tokio::test]
async fn no_accept_header_defaults_to_json() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-none").await;

    // No headers — `get_with_headers` passes an empty slice.
    let (status, ct, body) =
        get_with_headers(router, "/api/v1/namespaces/default/pods/p-none", &[]).await;

    assert_eq!(status, StatusCode::OK, "status={} body={:?}", status, body);
    assert!(
        ct.starts_with("application/json"),
        "missing Accept must default to application/json; got {}",
        ct
    );
    assert_pod_body("p-none", &body);
}

// ---------------------------------------------------------------------------
// 4. application/yaml — unsupported in Rusternetes
// ---------------------------------------------------------------------------

/// `Accept: application/yaml`. Upstream apiserver registers a YAML serializer
/// and emits a YAML document. Rusternetes' router has no YAML response
/// serializer — handlers always produce JSON via `axum::Json<T>`. Per RFC
/// 7231 §5.3.2 a non-matching Accept should produce 406 Not Acceptable, but
/// Rusternetes ignores the header and returns JSON regardless.
///
/// Pin the actual behavior: 200 + Content-Type: application/json, no 406.
/// This is a divergence from upstream; flip the assertion if a YAML
/// serializer ever lands.
#[tokio::test]
async fn accept_application_yaml_falls_back_to_json() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-yaml").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-yaml",
        "application/yaml",
    )
    .await;

    // Rusternetes does NOT honor application/yaml — it serves JSON anyway.
    assert_eq!(
        status,
        StatusCode::OK,
        "rusternetes serves JSON for application/yaml (no 406); status={} body={:?}",
        status,
        body
    );
    assert!(
        ct.starts_with("application/json"),
        "Rusternetes does not implement YAML; falls back to JSON; got {}",
        ct
    );
    assert_pod_body("p-yaml", &body);
}

// ---------------------------------------------------------------------------
// 5. application/vnd.kubernetes.protobuf — forced JSON fallback
// ---------------------------------------------------------------------------

/// `Accept: application/vnd.kubernetes.protobuf`. Real K8s wraps the
/// resource in the `k8s\0`-prefixed Unknown envelope and returns
/// `Content-Type: application/vnd.kubernetes.protobuf`.
///
/// Native protobuf encoding ships for the Pod GET path — see
/// `crates/api-server/src/response.rs::{NativeProtoOptIn,
/// NativePodProtoEncoder}` and the opt-in in
/// `crates/api-server/src/handlers/pod.rs::get`. Rusternetes responds with
/// a `k8s\0`-framed `runtime.Unknown` envelope whose `raw` field carries
/// native proto bytes (`ProtoRegistry::encode_message("Pod", …)`).
///
/// Pin: 200 + protobuf Content-Type + `k8s\0`-prefixed body.
#[tokio::test]
async fn accept_protobuf_returns_native_envelope() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-pb").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-pb",
        "application/vnd.kubernetes.protobuf",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "status={} body={:?}", status, body);
    assert!(
        ct.starts_with("application/vnd.kubernetes.protobuf"),
        "Pod GET opts in to protobuf responses; got {}",
        ct
    );
    assert!(
        body.starts_with(b"k8s\0"),
        "body must start with the k8s\\0 magic prefix; first bytes={:?}",
        &body[..body.len().min(16)]
    );
}

// ---------------------------------------------------------------------------
// 6-7. Quality values
// ---------------------------------------------------------------------------

/// `Accept: application/vnd.kubernetes.protobuf;q=0.9, application/json;q=1.0`.
/// Rusternetes' protobuf-opt-in path matches on a substring `contains`
/// check and does not yet parse quality values — so any Accept that names
/// protobuf produces a protobuf response, even when JSON has a higher q.
/// This is a known divergence from upstream RFC 7231; pin the actual
/// behaviour so a future q-value parser flips the assertion deliberately.
#[tokio::test]
async fn accept_q_values_protobuf_wins_despite_lower_q() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-q1").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-q1",
        "application/vnd.kubernetes.protobuf;q=0.9, application/json;q=1.0",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.starts_with("application/vnd.kubernetes.protobuf"),
        "no q-value parser yet — protobuf wins on contains() match; got {}",
        ct
    );
    assert!(
        body.starts_with(b"k8s\0"),
        "body must be a protobuf envelope; first bytes={:?}",
        &body[..body.len().min(16)]
    );
}

/// `Accept: application/vnd.kubernetes.protobuf;q=1.0, application/json;q=0.5`.
/// Upstream RFC 7231 contract: protobuf has higher q AND is supported by
/// rusternetes for Pod GET, so protobuf wins. Same outcome the upstream
/// contract demands.
#[tokio::test]
async fn accept_q_values_protobuf_first_returns_protobuf() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-q2").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-q2",
        "application/vnd.kubernetes.protobuf;q=1.0, application/json;q=0.5",
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "must return protobuf, not 406; status={} body={:?}",
        status,
        body
    );
    assert!(
        ct.starts_with("application/vnd.kubernetes.protobuf"),
        "protobuf has higher q AND is supported; must be picked; got {}",
        ct
    );
    assert!(body.starts_with(b"k8s\0"));
}

// ---------------------------------------------------------------------------
// 8-9. Table & PartialObjectMetadata conversions (NOT implemented)
// ---------------------------------------------------------------------------

/// `Accept: application/json;as=Table;v=v1;g=meta.k8s.io`. Upstream
/// converts the resource into a `meta.k8s.io/v1.Table` for kubectl's
/// columnar output. Rusternetes' `normalize_content_type_middleware`
/// intercepts the response and rebuilds it as a Table when the Accept
/// header carries `as=Table`.
#[tokio::test]
async fn accept_as_table_returns_table() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-table").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-table",
        "application/json;as=Table;v=v1;g=meta.k8s.io",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.contains("as=Table") || ct.contains("Table"),
        "expected Table Content-Type; got {}",
        ct
    );
    let v: Value = serde_json::from_slice(&body).expect("Table body is JSON");
    assert_eq!(v["kind"], "Table", "kind must be Table; got {}", v);
    assert_eq!(
        v["apiVersion"], "meta.k8s.io/v1",
        "apiVersion must be meta.k8s.io/v1; got {}",
        v
    );
    let cols = v["columnDefinitions"]
        .as_array()
        .expect("columnDefinitions must be array");
    assert!(
        !cols.is_empty(),
        "columnDefinitions must not be empty; got {}",
        v
    );
    let rows = v["rows"].as_array().expect("rows must be array");
    assert_eq!(rows.len(), 1, "single-object Table has one row; got {}", v);
    assert_eq!(
        rows[0]["object"]["metadata"]["name"], "p-table",
        "row.object must carry the source Pod; got {}",
        rows[0]
    );
}

/// A pod LIST whose handler already produced a `kind: "Table"` body (its list
/// path branches on `wants_table`) must be passed through verbatim — one row
/// per pod. Regression guard against the response middleware re-wrapping the
/// whole Table as a single row of an outer generic Table.
#[tokio::test]
async fn accept_as_table_pod_list_is_not_double_wrapped() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "pl-a").await;
    seed_pod(&mem, "pl-b").await;

    let (status, _ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods",
        "application/json;as=Table;v=v1;g=meta.k8s.io",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).expect("Table body is JSON");
    assert_eq!(v["kind"], "Table", "kind must be Table; got {}", v);
    let rows = v["rows"].as_array().expect("rows must be array");
    assert_eq!(
        rows.len(),
        2,
        "pod-list Table must have one row per pod, not one row wrapping the Table; got {}",
        v
    );
    // The inner object of each row is a Pod, never another Table.
    assert_ne!(
        rows[0]["object"]["kind"], "Table",
        "row.object must be the source resource, not a nested Table; got {}",
        rows[0]
    );
}

/// `Accept: application/json;as=Table` for a resource WITHOUT a custom printer
/// (ConfigMap) must return a 200 default NAME/AGE Table, NOT 406.
///
/// Upstream contract verified against
/// `test/e2e/apimachinery/table_conversion.go` (release-1.35): every resource
/// carrying ObjectMeta gets a Table. Kinds with no custom printer fall back to
/// the `defaultTableConvertor` (NAME from `metadata.name`, AGE from
/// `metadata.creationTimestamp`) in
/// `staging/src/k8s.io/apiserver/pkg/registry/rest/table.go`. 406 is reserved
/// for metadata-less review backends (see the SelfSubjectAccessReview test in
/// `conformance_apimachinery_vap_apf_server.rs`).
///
/// Regression guard for the #918 allowlist that wrongly 406'd ~12 common kinds.
#[tokio::test]
async fn accept_as_table_returns_default_table_for_resource_without_printer() {
    let (mem, router) = spawn_router();
    seed_configmap(&mem, "cm-table").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/configmaps",
        "application/json;as=Table;v=v1;g=meta.k8s.io",
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "configmaps must return a 200 default Table, not 406; ct={ct} body={:?}",
        String::from_utf8_lossy(&body),
    );
    let v: Value = serde_json::from_slice(&body).expect("Table body is JSON");
    assert_eq!(v["kind"], "Table", "kind must be Table; got {}", v);
    let cols = v["columnDefinitions"]
        .as_array()
        .expect("columnDefinitions must be array");
    let col_names: Vec<&str> = cols.iter().filter_map(|c| c["name"].as_str()).collect();
    assert!(
        col_names.iter().any(|n| n.eq_ignore_ascii_case("Name")),
        "default Table must have a Name column; got {col_names:?}"
    );
    assert!(
        col_names.iter().any(|n| n.eq_ignore_ascii_case("Age")),
        "default Table must have an Age column; got {col_names:?}"
    );
    let rows = v["rows"].as_array().expect("rows must be array");
    assert_eq!(rows.len(), 1, "one row per configmap; got {}", v);
    assert_eq!(
        rows[0]["object"]["metadata"]["name"], "cm-table",
        "row.object must carry the source ConfigMap; got {}",
        rows[0]
    );
}

/// Same default-Table fallback must hold for `secrets` and `podtemplates` —
/// neither has a custom printer in Rusternetes, both carry ObjectMeta, so both
/// must convert to a 200 NAME/AGE Table rather than 406.
#[tokio::test]
async fn accept_as_table_default_table_for_secrets_and_podtemplates() {
    for (resource, kind) in [("secrets", "Secret"), ("podtemplates", "PodTemplate")] {
        let (_mem, router) = spawn_router();
        let (status, ct, body) = get_with_accept(
            router,
            &format!("/api/v1/namespaces/default/{resource}"),
            "application/json;as=Table;v=v1;g=meta.k8s.io",
        )
        .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "{kind} list must return a 200 default Table, not 406; ct={ct} body={:?}",
            String::from_utf8_lossy(&body),
        );
        let v: Value = serde_json::from_slice(&body).expect("Table body is JSON");
        assert_eq!(v["kind"], "Table", "{kind}: kind must be Table; got {}", v);
        let cols = v["columnDefinitions"]
            .as_array()
            .expect("columnDefinitions must be array");
        assert!(
            !cols.is_empty(),
            "{kind}: default Table must define columns; got {}",
            v
        );
    }
}

/// `Accept: application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io`.
/// Upstream strips `spec` and `status`, returning just TypeMeta +
/// ObjectMeta to keep watch traffic light. Rusternetes' middleware
/// performs the same projection.
#[tokio::test]
async fn accept_as_partial_object_metadata_strips_spec_status() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-pom").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-pom",
        "application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.contains("PartialObjectMetadata"),
        "expected PartialObjectMetadata Content-Type; got {}",
        ct
    );
    let v: Value = serde_json::from_slice(&body).expect("JSON");
    assert_eq!(v["kind"], "PartialObjectMetadata");
    assert_eq!(v["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(v["metadata"]["name"], "p-pom");
    assert!(
        v.get("spec").is_none(),
        "PartialObjectMetadata must strip spec; got {}",
        v
    );
    assert!(
        v.get("status").is_none(),
        "PartialObjectMetadata must strip status; got {}",
        v
    );
}

// ---------------------------------------------------------------------------
// 10-11. Accept-Encoding (compression)
// ---------------------------------------------------------------------------

/// `Accept-Encoding: gzip`. RFC 7231 §5.3.4 permits the server to send
/// the response uncompressed (no Content-Encoding) or compressed (with
/// `Content-Encoding: gzip`). Rusternetes wires no `tower_http::compression`
/// layer into the router, so the server returns identity-encoded JSON
/// (no Content-Encoding header). Pin that contract — gzip request must
/// still produce a valid uncompressed JSON Pod.
#[tokio::test]
async fn accept_encoding_gzip_returns_identity() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-gz").await;

    let (status, header_map, bytes, _) = router
        .send_with_headers(
            "GET",
            "/api/v1/namespaces/default/pods/p-gz",
            &[("accept", "application/json"), ("accept-encoding", "gzip")],
            None,
        )
        .await;
    let content_encoding = header_map
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    assert_eq!(status, StatusCode::OK);
    // Rusternetes does not compress; Content-Encoding header is either
    // absent or set to "identity". RFC 7231 permits either.
    assert!(
        content_encoding.is_empty() || content_encoding.eq_ignore_ascii_case("identity"),
        "rusternetes must not compress (no compression layer); got Content-Encoding={:?}",
        content_encoding
    );
    // Body must be plain (uncompressed) JSON. Gzip magic is 1f 8b.
    assert!(
        !(bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b),
        "body must not be gzip-encoded; first bytes={:?}",
        &bytes[..bytes.len().min(8)]
    );
    assert_pod_body("p-gz", &bytes);
}

/// `Accept-Encoding: br` (Brotli). Even less likely to be supported than
/// gzip — pin identity encoding.
#[tokio::test]
async fn accept_encoding_brotli_returns_identity() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-br").await;

    let (status, ct, bytes) = get_with_headers(
        router,
        "/api/v1/namespaces/default/pods/p-br",
        &[("accept", "application/json"), ("accept-encoding", "br")],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.starts_with("application/json"),
        "Content-Type still JSON; got {}",
        ct
    );
    // Body must NOT be Brotli — there's no Brotli magic number but the body
    // must parse as JSON.
    assert_pod_body("p-br", &bytes);
}

// ---------------------------------------------------------------------------
// 12. Garbage / unsupported types
// ---------------------------------------------------------------------------

/// `Accept: application/garbage`. Per RFC 7231 §5.3.2 + §6.5.6 the server
/// should return 406 Not Acceptable when none of the requested media types
/// can be served. Rusternetes ignores Accept entirely and always returns
/// JSON, so a garbage Accept still yields a JSON response.
///
/// Pin actual: 200 + Content-Type: application/json. Divergence from
/// upstream RFC contract; flip the assertion when negotiation is wired up.
#[tokio::test]
async fn accept_garbage_falls_back_to_json() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-garbage").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-garbage",
        "application/garbage",
    )
    .await;

    // Upstream contract would be 406 here; Rusternetes ignores Accept.
    assert_eq!(
        status,
        StatusCode::OK,
        "rusternetes ignores Accept and serves JSON for any value; status={} body={:?}",
        status,
        body
    );
    assert!(
        ct.starts_with("application/json"),
        "rusternetes always returns JSON regardless of Accept; got {}",
        ct
    );
    assert_pod_body("p-garbage", &body);
}
