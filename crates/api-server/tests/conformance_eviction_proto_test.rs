//! Wire-format parity tests for the `policy/v1.Eviction` subresource.
//!
//! `kubectl drain` and the upstream NodeLifecycle controller post Pod
//! evictions through `/api/v1/namespaces/{ns}/pods/{name}/eviction`. The
//! `Eviction` body is small (just ObjectMeta + DeleteOptions), but it
//! travels over the wire as `application/vnd.kubernetes.protobuf` whenever
//! client-go's content negotiator picks the protobuf serializer — which is
//! the default for write paths.
//!
//! The `ProtoRegistry` already registers `Eviction`,
//! `PodDisruptionBudget*`, `DeleteOptions`, and `Preconditions` (see
//! `crates/api-server/src/protobuf.rs::register_policy_v1`), but no test
//! exercised the wire path. This file pins three layers:
//!
//!   1. **Schema decode** — hand-crafted `Eviction` proto bytes round-trip
//!      to JSON with `metadata.name`, `metadata.namespace`, and the nested
//!      `deleteOptions.preconditions.uid` / `deleteOptions.dryRun` correctly
//!      surfaced. This is the regression baseline for the schema entries.
//!
//!   2. **`decode_k8s_resource` envelope** — wrap the inner bytes in the
//!      canonical `k8s\0` + `Unknown { typeMeta, raw }` envelope client-go
//!      emits, decode via `ProtoRegistry::decode_k8s_resource`, and check
//!      that `apiVersion=policy/v1`, `kind=Eviction`, and the metadata
//!      survives the round-trip. Mirrors
//!      `conformance_events_microtime_test.rs` for events.k8s.io/v1.
//!
//!   3. **End-to-end HTTP** — POST a protobuf-encoded Eviction body to
//!      `/api/v1/namespaces/{ns}/pods/{name}/eviction` and assert the
//!      handler accepts it (201/200) AND deletes the pod. This is the
//!      surface that gets hit by `kubectl drain` against a real cluster.
//!      Pre-fix the handler returned 4xx because the `normalize_content_type`
//!      middleware path either dropped the body or produced JSON whose
//!      shape didn't match the eviction parser.
//!
//! Upstream reference:
//!   * `k8s.io/api/policy/v1/generated.proto` — Eviction = ObjectMeta(1) +
//!     DeleteOptions(2).
//!   * `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto`
//!     — DeleteOptions { gracePeriodSeconds=1, preconditions=2, dryRun=5 }.
//!     Preconditions { uid=1, resourceVersion=2 }.
//!   * `pkg/registry/core/pod/storage/eviction.go` (release-1.35) — the
//!     handler upstream.
//!   * JSON-side mirror: `tests/integration_eviction_subresource.rs`.

use axum::http::StatusCode;
use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_common::resources::{Container, Pod, PodSpec, PodStatus};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Proto wire-format helpers — small, copy-paste-stable across this file.
// ---------------------------------------------------------------------------

/// Append a base-128 varint `v` to `out` (little-endian groups of 7 bits).
fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Emit `(tag, value)` for a length-delimited (wire type 2) string field.
fn write_string_field(out: &mut Vec<u8>, field_num: u32, value: &str) {
    let tag = (field_num << 3) | 2;
    write_varint(out, tag as u64);
    write_varint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

/// Emit `(tag, value)` for a length-delimited (wire type 2) embedded message.
fn write_message_field(out: &mut Vec<u8>, field_num: u32, payload: &[u8]) {
    let tag = (field_num << 3) | 2;
    write_varint(out, tag as u64);
    write_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

/// `ObjectMeta { name, namespace }` — proto fields 1 and 3 per
/// `k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto`.
fn object_meta_bytes(name: &str, namespace: &str) -> Vec<u8> {
    let mut out = Vec::new();
    write_string_field(&mut out, 1, name);
    write_string_field(&mut out, 3, namespace);
    out
}

/// `Preconditions { uid }` — proto field 1.
fn preconditions_bytes(uid: &str) -> Vec<u8> {
    let mut out = Vec::new();
    write_string_field(&mut out, 1, uid);
    out
}

/// `DeleteOptions { preconditions?, dryRun? }`. Per upstream:
///   field 2 = preconditions (message)
///   field 5 = dryRun       (repeated string)
fn delete_options_bytes(preconditions: Option<&[u8]>, dry_run_all: bool) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(p) = preconditions {
        write_message_field(&mut out, 2, p);
    }
    if dry_run_all {
        write_string_field(&mut out, 5, "All");
    }
    out
}

/// `Eviction { metadata, deleteOptions? }`. Per upstream
/// `k8s.io/api/policy/v1/generated.proto`:
///   field 1 = metadata       (ObjectMeta)
///   field 2 = deleteOptions  (DeleteOptions)
fn eviction_bytes(metadata: &[u8], delete_options: Option<&[u8]>) -> Vec<u8> {
    let mut out = Vec::new();
    write_message_field(&mut out, 1, metadata);
    if let Some(d) = delete_options {
        write_message_field(&mut out, 2, d);
    }
    out
}

/// Wrap inner protobuf bytes in the canonical `k8s\0` + `Unknown { typeMeta,
/// raw }` envelope client-go uses. Field numbers per
/// `staging/src/k8s.io/apimachinery/pkg/runtime/generated.proto`:
///   Unknown.typeMeta = 1, Unknown.raw = 2
///   TypeMeta.apiVersion = 1, TypeMeta.kind = 2
fn k8s_envelope(api_version: &str, kind: &str, inner: &[u8]) -> Vec<u8> {
    let mut typemeta = Vec::new();
    write_string_field(&mut typemeta, 1, api_version);
    write_string_field(&mut typemeta, 2, kind);

    let mut envelope = Vec::new();
    envelope.extend_from_slice(b"k8s\0");
    write_message_field(&mut envelope, 1, &typemeta);
    write_message_field(&mut envelope, 2, inner);
    envelope
}

// ---------------------------------------------------------------------------
// (1) Schema decode — bare `Eviction` proto bytes through the registry.
// ---------------------------------------------------------------------------

/// Minimal Eviction with just `metadata.name` + `metadata.namespace` set —
/// the canonical body client-go emits when no `deleteOptions` are supplied.
/// The decoder must surface both fields in the nested `metadata` object.
#[test]
fn test_eviction_minimal_metadata_decodes_via_registry() {
    let registry = ProtoRegistry::new();
    let meta = object_meta_bytes("my-pod", "my-ns");
    let bytes = eviction_bytes(&meta, None);

    let decoded = registry
        .decode_message("Eviction", &bytes)
        .expect("Eviction schema must be registered");

    let metadata = decoded
        .get("metadata")
        .unwrap_or_else(|| panic!("metadata missing in {decoded}"));
    assert_eq!(
        metadata.get("name").and_then(|v| v.as_str()),
        Some("my-pod"),
        "Eviction.metadata.name must round-trip; got {decoded}",
    );
    assert_eq!(
        metadata.get("namespace").and_then(|v| v.as_str()),
        Some("my-ns"),
        "Eviction.metadata.namespace must round-trip; got {decoded}",
    );
    // No deleteOptions on the wire → no key in the JSON projection.
    assert!(
        decoded.get("deleteOptions").is_none(),
        "absent deleteOptions must NOT appear in decoded JSON; got {decoded}",
    );
}

/// Eviction with `deleteOptions.preconditions.uid` set. The decoder must
/// surface the nested UID at `deleteOptions.preconditions.uid`, exactly
/// where the eviction handler's `body.get("deleteOptions")` chain reads it
/// from (`pod_subresources::create_eviction`).
#[test]
fn test_eviction_with_uid_precondition_decodes_via_registry() {
    let registry = ProtoRegistry::new();
    let meta = object_meta_bytes("pre-pod", "pre-ns");
    let precond = preconditions_bytes("deadbeef-0000-0000-0000-000000000000");
    let dopts = delete_options_bytes(Some(&precond), false);
    let bytes = eviction_bytes(&meta, Some(&dopts));

    let decoded = registry
        .decode_message("Eviction", &bytes)
        .expect("Eviction schema must be registered");

    let dopts_json = decoded
        .get("deleteOptions")
        .unwrap_or_else(|| panic!("deleteOptions missing in {decoded}"));
    let pre = dopts_json
        .get("preconditions")
        .unwrap_or_else(|| panic!("preconditions missing in {decoded}"));
    assert_eq!(
        pre.get("uid").and_then(|v| v.as_str()),
        Some("deadbeef-0000-0000-0000-000000000000"),
        "deleteOptions.preconditions.uid must round-trip; got {decoded}",
    );
}

/// Eviction with `deleteOptions.dryRun = ["All"]`. The dryRun array is the
/// trigger for the upstream short-circuit that skips the storage delete —
/// our handler reads it via `delete_opts.get("dryRun").and_then(as_array)`,
/// so a missing key here would silently demote a dry-run into a real delete.
#[test]
fn test_eviction_with_dry_run_decodes_via_registry() {
    let registry = ProtoRegistry::new();
    let meta = object_meta_bytes("dry-pod", "dry-ns");
    let dopts = delete_options_bytes(None, true);
    let bytes = eviction_bytes(&meta, Some(&dopts));

    let decoded = registry
        .decode_message("Eviction", &bytes)
        .expect("Eviction schema must be registered");

    let dopts_json = decoded
        .get("deleteOptions")
        .unwrap_or_else(|| panic!("deleteOptions missing in {decoded}"));
    let dry_run = dopts_json
        .get("dryRun")
        .unwrap_or_else(|| panic!("dryRun missing in {decoded}"));
    let arr = dry_run
        .as_array()
        .unwrap_or_else(|| panic!("dryRun must be a JSON array, got {dry_run}"));
    assert_eq!(arr.len(), 1, "dryRun must contain exactly one entry");
    assert_eq!(
        arr[0].as_str(),
        Some("All"),
        "dryRun[0] must be \"All\"; got {decoded}",
    );
}

// ---------------------------------------------------------------------------
// (2) `decode_k8s_resource` envelope path.
// ---------------------------------------------------------------------------

/// End-to-end through `decode_k8s_resource`: wrap a minimal Eviction in the
/// `k8s\0 / TypeMeta + raw` envelope (the exact shape client-go puts on the
/// wire), decode, and check the resulting JSON carries the right TypeMeta
/// plus the nested metadata. The schema is registered under the bare key
/// `Eviction`; `decode_k8s_resource` falls back to the bare kind when the
/// group-qualified `policy/v1.Eviction` lookup misses.
#[test]
fn test_eviction_envelope_round_trips_via_decode_k8s_resource() {
    let registry = ProtoRegistry::new();
    let meta = object_meta_bytes("env-pod", "env-ns");
    let inner = eviction_bytes(&meta, None);
    let envelope = k8s_envelope("policy/v1", "Eviction", &inner);

    let json_bytes = registry
        .decode_k8s_resource(&envelope)
        .expect("decode_k8s_resource must accept the policy/v1 Eviction envelope");
    let decoded: serde_json::Value =
        serde_json::from_slice(&json_bytes).expect("decode_k8s_resource must produce valid JSON");

    assert_eq!(decoded["apiVersion"], "policy/v1");
    assert_eq!(decoded["kind"], "Eviction");
    assert_eq!(
        decoded["metadata"]["name"], "env-pod",
        "metadata.name must round-trip through the envelope; got {decoded}",
    );
    assert_eq!(
        decoded["metadata"]["namespace"], "env-ns",
        "metadata.namespace must round-trip through the envelope; got {decoded}",
    );
}

// ---------------------------------------------------------------------------
// (3) End-to-end HTTP — POST proto body to the eviction subresource.
// ---------------------------------------------------------------------------
//
// Mirrors `crates/api-server/tests/integration_eviction_subresource.rs` but
// posts a protobuf-encoded body with `Content-Type:
// application/vnd.kubernetes.protobuf`. The router's
// `normalize_content_type_middleware` strips the envelope and routes the
// decoded JSON into the existing `create_eviction` handler.

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// POST a protobuf-encoded body (`application/vnd.kubernetes.protobuf`) and
/// return `(status, raw response bytes)`.
async fn post_proto(router: &TestApiServer, uri: &str, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
    let (status, _headers, bytes, _) = router
        .send_with_headers(
            "POST",
            uri,
            &[("content-type", "application/vnd.kubernetes.protobuf")],
            Some(body),
        )
        .await;
    (status, bytes)
}

fn running_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:latest".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        }),
    }
}

/// POST a protobuf-encoded Eviction to `/pods/{name}/eviction`.
///
/// The handler must:
///   1. Accept `Content-Type: application/vnd.kubernetes.protobuf` (the
///      middleware strips the envelope before calling the handler).
///   2. Return 200/201 — matches the JSON-side path in
///      `integration_eviction_subresource.rs::test_terminal_pod_eviction`.
///   3. Delete the pod (verified by re-reading from storage).
#[tokio::test]
async fn test_eviction_subresource_accepts_protobuf_body() {
    let (mem, router) = spawn_router();
    let ns = "eviction-proto-ns";
    let name = "evictable-pod";

    // Seed a Running pod with no matching PDB — the eviction must succeed.
    let pod = running_pod(name, ns);
    mem.create(&build_key("pods", Some(ns), name), &pod)
        .await
        .unwrap();

    // Build the proto wire bytes: Eviction { metadata { name, namespace } }
    // wrapped in the k8s\0 envelope.
    let meta = object_meta_bytes(name, ns);
    let inner = eviction_bytes(&meta, None);
    let envelope = k8s_envelope("policy/v1", "Eviction", &inner);

    let uri = format!("/api/v1/namespaces/{}/pods/{}/eviction", ns, name);
    let (status, bytes) = post_proto(&router, &uri, envelope).await;
    let body_str = String::from_utf8_lossy(&bytes).to_string();

    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "POST protobuf Eviction must return 200/201 — got {status}, body={body_str}",
    );

    // Pod must be gone — confirms the handler exercised the delete path
    // rather than failing silently on body parse.
    let key = build_key("pods", Some(ns), name);
    let after: Result<Pod, _> = mem.get(&key).await;
    assert!(
        after.is_err(),
        "pod must be deleted after a successful protobuf Eviction (got Ok)",
    );
}

/// Protobuf Eviction with a matching UID precondition. Mirrors the JSON-side
/// `test_eviction_with_precondition` (case 1). The handler reads the UID
/// from `deleteOptions.preconditions.uid`; if the middleware drops the
/// nested message during proto→JSON conversion, this test fails because
/// the matching UID would no longer be visible to the handler.
#[tokio::test]
async fn test_eviction_subresource_protobuf_honors_matching_uid_precondition() {
    let (mem, router) = spawn_router();
    let ns = "eviction-proto-pre-ns";
    let name = "uid-pod";

    let pod = running_pod(name, ns);
    let real_uid = pod.metadata.uid.clone();
    mem.create(&build_key("pods", Some(ns), name), &pod)
        .await
        .unwrap();

    let meta = object_meta_bytes(name, ns);
    let precond = preconditions_bytes(&real_uid);
    let dopts = delete_options_bytes(Some(&precond), false);
    let inner = eviction_bytes(&meta, Some(&dopts));
    let envelope = k8s_envelope("policy/v1", "Eviction", &inner);

    let uri = format!("/api/v1/namespaces/{}/pods/{}/eviction", ns, name);
    let (status, _bytes) = post_proto(&router, &uri, envelope).await;

    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "protobuf Eviction with matching UID precondition must succeed; got {status}",
    );
    let key = build_key("pods", Some(ns), name);
    assert!(
        mem.get::<Pod>(&key).await.is_err(),
        "pod must be deleted on a successful UID-matched protobuf eviction",
    );
}

/// Protobuf Eviction with a mismatched UID precondition must surface the
/// upstream 409 Conflict. The handler builds the conflict from the body's
/// `deleteOptions.preconditions.uid` — so this test trips the same
/// "did the middleware preserve the nested message?" guard as the matching
/// case, but inverted: the pod must survive.
#[tokio::test]
async fn test_eviction_subresource_protobuf_rejects_mismatched_uid_precondition() {
    let (mem, router) = spawn_router();
    let ns = "eviction-proto-pre-bad-ns";
    let name = "uid-bad-pod";

    let pod = running_pod(name, ns);
    mem.create(&build_key("pods", Some(ns), name), &pod)
        .await
        .unwrap();

    let meta = object_meta_bytes(name, ns);
    // Deliberately wrong UID — must not match the freshly-generated pod UID.
    let precond = preconditions_bytes("00000000-0000-0000-0000-000000000000");
    let dopts = delete_options_bytes(Some(&precond), false);
    let inner = eviction_bytes(&meta, Some(&dopts));
    let envelope = k8s_envelope("policy/v1", "Eviction", &inner);

    let uri = format!("/api/v1/namespaces/{}/pods/{}/eviction", ns, name);
    let (status, _bytes) = post_proto(&router, &uri, envelope).await;

    assert!(
        status.is_client_error(),
        "protobuf Eviction with mismatched UID precondition must return 4xx; got {status}",
    );
    let key = build_key("pods", Some(ns), name);
    assert!(
        mem.get::<Pod>(&key).await.is_ok(),
        "pod must survive a failed UID precondition check (proto body)",
    );
}
