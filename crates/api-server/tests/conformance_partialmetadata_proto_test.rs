//! Regression tests for the `PartialObjectMetadata` content-negotiation
//! surface. Mirrors upstream
//! `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go` /
//! `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/protobuf` and
//! the metadata-informer path in client-go's
//! `metadata/metadatainformer` that drives every metadata-only watch.
//!
//! Three negotiation surfaces are exercised:
//!
//! 1. `Accept: application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io`
//!    on a single object — `spec` / `status` must be stripped, `kind` /
//!    `apiVersion` rewritten to `meta.k8s.io/v1.PartialObjectMetadata`,
//!    and the response `Content-Type` echoes the `as=` parameter so
//!    clients can dispatch on the served shape.
//! 2. The same on a list — `kind` becomes `PartialObjectMetadataList`,
//!    each `items[i]` is downgraded to `PartialObjectMetadata`, `spec` /
//!    `status` stripped from every entry.
//! 3. `Accept: application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;
//!    g=meta.k8s.io;v=v1` — the projected JSON is wrapped in the K8s
//!    `k8s\0` Unknown envelope and the response `Content-Type` carries
//!    the protobuf base. `ProtoRegistry::decode_k8s_resource` round-trips
//!    the envelope back to the same projected JSON.
//!
//! These tests pin the gap noted in `tests/decoder_accept_header_test.rs`
//! (lines 415-451 — JSON projection already worked) plus the new proto
//! envelope path. Run with:
//!
//!   cargo test -p rusternetes-api-server --test conformance_partialmetadata_proto_test

use axum::http::StatusCode;
use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

const TEST_NS: &str = "default";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn get_with_accept(
    router: TestApiServer,
    uri: &str,
    accept: &str,
) -> (StatusCode, String, Vec<u8>) {
    let (status, headers, bytes, _) = router.send_full("GET", uri, None, Some(accept), None).await;
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    (status, content_type, bytes)
}

async fn seed_pod(mem: &Arc<MemoryStorage>, name: &str) {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
            "labels": {"app": name},
        },
        "spec": {
            "containers": [{"name": "c", "image": "busybox"}]
        },
        "status": {
            "phase": "Running",
        }
    });
    let key = build_key("pods", Some(TEST_NS), name);
    mem.create(&key, &pod).await.expect("seed pod");
}

// ---------------------------------------------------------------------------
// 1. JSON projection on a single object
// ---------------------------------------------------------------------------

/// `Accept: application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io`
/// on a single Pod must:
/// - rewrite kind to `PartialObjectMetadata`
/// - rewrite apiVersion to `meta.k8s.io/v1`
/// - preserve `metadata` verbatim (name, namespace, labels)
/// - strip `spec` and `status`
/// - echo the `as=` parameter in Content-Type
#[tokio::test]
async fn accept_as_partial_object_metadata_json_single_object() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-pom-json").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-pom-json",
        "application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "status={status} body={body:?}");
    assert!(
        ct.contains("as=PartialObjectMetadata"),
        "Content-Type must echo as=PartialObjectMetadata; got {ct}",
    );
    let v: Value = serde_json::from_slice(&body).expect("body must be JSON");
    assert_eq!(v["kind"], "PartialObjectMetadata", "kind mismatch; got {v}");
    assert_eq!(
        v["apiVersion"], "meta.k8s.io/v1",
        "apiVersion mismatch; got {v}",
    );
    assert_eq!(v["metadata"]["name"], "p-pom-json");
    assert_eq!(v["metadata"]["namespace"], "default");
    assert_eq!(v["metadata"]["labels"]["app"], "p-pom-json");
    assert!(v.get("spec").is_none(), "spec must be stripped; got {v}");
    assert!(
        v.get("status").is_none(),
        "status must be stripped; got {v}",
    );
}

// ---------------------------------------------------------------------------
// 2. JSON projection on a list collection
// ---------------------------------------------------------------------------

/// LIST with `Accept: application/json;as=PartialObjectMetadataList;v=v1;
/// g=meta.k8s.io` must convert the upstream PodList into a
/// PartialObjectMetadataList where each item is a PartialObjectMetadata
/// with no spec/status.
#[tokio::test]
async fn accept_as_partial_object_metadata_list_json() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-pom-list-a").await;
    seed_pod(&mem, "p-pom-list-b").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods",
        "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "status={status}");
    assert!(
        ct.contains("as=PartialObjectMetadataList"),
        "Content-Type must echo as=PartialObjectMetadataList; got {ct}",
    );
    let v: Value = serde_json::from_slice(&body).expect("body must be JSON");
    assert_eq!(
        v["kind"], "PartialObjectMetadataList",
        "kind mismatch; got {v}"
    );
    assert_eq!(v["apiVersion"], "meta.k8s.io/v1");
    let items = v["items"].as_array().expect("items must be array");
    assert_eq!(items.len(), 2, "want 2 items; got {v}");
    for item in items {
        assert_eq!(item["kind"], "PartialObjectMetadata");
        assert_eq!(item["apiVersion"], "meta.k8s.io/v1");
        assert!(
            item.get("spec").is_none(),
            "spec must be stripped from list item; got {item}",
        );
        assert!(
            item.get("status").is_none(),
            "status must be stripped from list item; got {item}",
        );
        let name = item["metadata"]["name"].as_str().expect("name present");
        assert!(
            name.starts_with("p-pom-list-"),
            "metadata.name preserved; got {item}",
        );
    }
}

// ---------------------------------------------------------------------------
// 3. Protobuf envelope on a single object
// ---------------------------------------------------------------------------

/// `Accept: application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;
/// g=meta.k8s.io;v=v1` must:
/// - return `Content-Type: application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;…`
/// - wrap the projected JSON in the K8s `k8s\0` Unknown envelope so
///   `client-go` (`runtime/serializer/protobuf`) can decode it
/// - have TypeMeta inside the envelope point at PartialObjectMetadata, not Pod
/// - round-trip via `ProtoRegistry::decode_k8s_resource` to the same
///   stripped JSON
#[tokio::test]
async fn accept_as_partial_object_metadata_proto_single_object() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "p-pom-pb").await;

    let (status, ct, body) = get_with_accept(
        router,
        "/api/v1/namespaces/default/pods/p-pom-pb",
        "application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;g=meta.k8s.io;v=v1",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "status={status} body={body:?}");
    assert!(
        ct.starts_with("application/vnd.kubernetes.protobuf"),
        "Content-Type must be protobuf base; got {ct}",
    );
    assert!(
        ct.contains("as=PartialObjectMetadata"),
        "Content-Type must echo as=PartialObjectMetadata; got {ct}",
    );

    // Body must be a K8s Unknown envelope, NOT raw JSON.
    assert!(
        body.starts_with(b"k8s\0"),
        "proto-negotiated body must start with k8s\\0 magic; got {:?}",
        &body[..body.len().min(16)],
    );

    // Round-trip through the schema registry. `decode_k8s_resource` picks
    // up the TypeMeta (apiVersion=meta.k8s.io/v1, kind=PartialObjectMetadata)
    // and uses the newly-registered schema to surface `metadata`.
    let registry = ProtoRegistry::new();
    let decoded_json = registry
        .decode_k8s_resource(&body)
        .expect("envelope must decode via registered PartialObjectMetadata schema");
    let v: Value = serde_json::from_slice(&decoded_json).unwrap_or_else(|e| {
        panic!(
            "decoded body must be JSON: {e}; raw={:?}",
            String::from_utf8_lossy(&decoded_json),
        )
    });
    assert_eq!(v["kind"], "PartialObjectMetadata", "got {v}");
    assert_eq!(v["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(v["metadata"]["name"], "p-pom-pb");
    assert_eq!(v["metadata"]["namespace"], "default");
    assert!(v.get("spec").is_none(), "spec must be absent; got {v}");
    assert!(v.get("status").is_none(), "status must be absent; got {v}");
}

// ---------------------------------------------------------------------------
// 4. Schema registry sanity — PartialObjectMetadata + List are present
// ---------------------------------------------------------------------------

/// The registry must carry both message schemas so the parity test and
/// the proto decoder agree. Without these entries, `decode_k8s_resource`
/// returns `None` for the envelope produced in test #3.
#[test]
fn registry_has_partial_object_metadata_schemas() {
    let registry = ProtoRegistry::new();
    let names: std::collections::BTreeSet<String> = registry
        .iter_schemas()
        .map(|(n, _)| n.to_string())
        .collect();
    assert!(
        names.contains("PartialObjectMetadata"),
        "registry missing PartialObjectMetadata; got {names:?}",
    );
    assert!(
        names.contains("PartialObjectMetadataList"),
        "registry missing PartialObjectMetadataList; got {names:?}",
    );
}
