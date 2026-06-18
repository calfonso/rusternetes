//! Router-driven PATCH compatibility tests for PodDisruptionBudget.
//!
//! Mirrors upstream `TestPatchCompatibility` from
//! `test/integration/disruption/disruption_test.go`
//! (<https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/disruption/disruption_test.go>),
//! which patches a PDB's `spec.selector` via three different patch types and
//! verifies the resulting selector matches the per-patch-type semantics:
//!
//!   * `application/strategic-merge-patch+json` — merges fields key-by-key;
//!     `matchExpressions` from the original is preserved when the patch only
//!     supplies `matchLabels`.
//!   * `application/merge-patch+json` (RFC 7386) — replaces nested objects
//!     wholesale; supplying just `matchLabels` wipes the original
//!     `matchExpressions`.
//!   * `application/apply-patch+yaml` (server-side apply) — atomic-replaces
//!     the selector (LabelSelector is `+listType=atomic` upstream), so any
//!     existing field not in the apply body is dropped.
//!
//! Companion to `crates/controller-manager/tests/integration_pdb_disruption.rs`
//! which already pins the selector-on-disk round-trip
//! (`test_patch_compatibility_selector_round_trip`). The three router-driven
//! tests below cover the missing surface, replacing the three `#[ignore]`d
//! stubs that previously lived in that file.
//!
//! Harness pattern mirrors `patch_strategic_merge_semantics_test.rs` /
//! `patch_json_patch_semantics_test.rs` / `integration_eviction_subresource.rs`:
//! `Arc<MemoryStorage>` wrapped in `StorageBackend::Memory`, router built via
//! `build_router`, requests dispatched with `tower::ServiceExt::oneshot`.

use rusternetes_common::{
    resources::{IntOrString, PodDisruptionBudget, PodDisruptionBudgetSpec},
    types::{LabelSelector, LabelSelectorRequirement, ObjectMeta, TypeMeta},
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "default";
const PDB_NAME: &str = "test-pdb";

const SMP_CT: &str = "application/strategic-merge-patch+json";
const MERGE_CT: &str = "application/merge-patch+json";
const APPLY_CT: &str = "application/apply-patch+yaml";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// Mirror of the upstream base PDB used by `TestPatchCompatibility`:
/// `matchLabels: {basematch: "true"}` PLUS a `matchExpressions` entry on
/// `baseexpression`. Each test below patches this exact starting point.
fn base_pdb() -> PodDisruptionBudget {
    PodDisruptionBudget {
        type_meta: TypeMeta {
            api_version: "policy/v1".to_string(),
            kind: "PodDisruptionBudget".to_string(),
        },
        metadata: ObjectMeta {
            name: PDB_NAME.to_string(),
            namespace: Some(TEST_NS.to_string()),
            ..Default::default()
        },
        spec: PodDisruptionBudgetSpec {
            min_available: None,
            max_unavailable: Some(IntOrString::Int(2)),
            selector: LabelSelector {
                match_labels: Some(HashMap::from([(
                    "basematch".to_string(),
                    "true".to_string(),
                )])),
                match_expressions: Some(vec![LabelSelectorRequirement {
                    key: "baseexpression".to_string(),
                    operator: "In".to_string(),
                    values: Some(vec!["true".to_string()]),
                }]),
            },
            unhealthy_pod_eviction_policy: None,
        },
        status: None,
    }
}

/// Seed the base PDB into storage. Returns the registry key for later reads.
async fn seed_base_pdb(mem: &Arc<MemoryStorage>) -> String {
    let key = build_key("poddisruptionbudgets", Some(TEST_NS), PDB_NAME);
    mem.create(&key, &base_pdb())
        .await
        .expect("seed base PDB into memory storage");
    key
}

/// Send a PATCH request to the PDB endpoint with the given content-type and
/// body. Returns `(http_status_code, response_body_json)`.
async fn patch_pdb(
    router: TestApiServer,
    content_type: &str,
    query: Option<&str>,
    body: &Value,
) -> (u16, Value) {
    let mut uri = format!(
        "/apis/policy/v1/namespaces/{ns}/poddisruptionbudgets/{name}",
        ns = TEST_NS,
        name = PDB_NAME,
    );
    if let Some(q) = query {
        uri.push('?');
        uri.push_str(q);
    }

    let (status, value) = router
        .send("PATCH", &uri, Some(content_type), Some(body))
        .await;
    (status.as_u16(), value)
}

/// Read the stored PDB as raw JSON so per-field assertions don't have to
/// round-trip through the `LabelSelector` Rust struct. (Both shapes are
/// equivalent in storage, but the JSON form keeps the test assertions
/// readable.)
async fn read_stored(mem: &Arc<MemoryStorage>, key: &str) -> Value {
    mem.get::<Value>(key)
        .await
        .unwrap_or_else(|e| panic!("expected key {} to exist: {:?}", key, e))
}

fn assert_2xx(status: u16, body: &Value, what: &str) {
    assert!(
        (200..300).contains(&status),
        "{} should return 2xx; got {} body={}",
        what,
        status,
        body
    );
}

// ---------------------------------------------------------------------------
// 1. application/strategic-merge-patch+json
//
// Upstream: a patch carrying only `matchLabels` MERGES key-by-key with the
// original selector, leaving `matchExpressions` untouched.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_patch_compatibility_v1_strategic_merge() {
    let (mem, router) = spawn_router();
    let key = seed_base_pdb(&mem).await;

    // Strategic-merge patch: supply only `matchLabels.patchmatch=true`. The
    // existing `basematch=true` should stay (key-by-key merge), AND the
    // existing matchExpressions entry MUST be preserved (SMP descends into
    // selector key-by-key rather than replacing it wholesale).
    let patch = json!({
        "spec": {
            "selector": {
                "matchLabels": {"patchmatch": "true"}
            }
        }
    });

    let (status, body) = patch_pdb(router, SMP_CT, None, &patch).await;
    assert_2xx(status, &body, "SMP patch on PDB selector");

    let stored = read_stored(&mem, &key).await;
    let selector = &stored["spec"]["selector"];

    // matchLabels was merged in: BOTH base and patch labels are present.
    let labels = selector["matchLabels"]
        .as_object()
        .unwrap_or_else(|| panic!("matchLabels missing/non-object in stored PDB: {}", stored));
    assert_eq!(
        labels.get("basematch").and_then(|v| v.as_str()),
        Some("true"),
        "SMP: original matchLabel `basematch` must survive the merge; got {:?}",
        labels
    );
    assert_eq!(
        labels.get("patchmatch").and_then(|v| v.as_str()),
        Some("true"),
        "SMP: patched matchLabel `patchmatch` must be added; got {:?}",
        labels
    );

    // matchExpressions was NOT in the patch — must be preserved untouched.
    let exprs = selector["matchExpressions"].as_array().unwrap_or_else(|| {
        panic!(
            "SMP: matchExpressions must be preserved when patch omits it; got {}",
            stored
        )
    });
    assert_eq!(
        exprs.len(),
        1,
        "SMP: exactly the original matchExpressions entry must remain; got {:?}",
        exprs
    );
    assert_eq!(exprs[0]["key"], "baseexpression");
    assert_eq!(exprs[0]["operator"], "In");
}

// ---------------------------------------------------------------------------
// 2. application/merge-patch+json (RFC 7386)
//
// Upstream: RFC 7386 recursively merges OBJECTS key-by-key, but ARRAYS are
// always replaced wholesale (the spec has no "merge a list element by key"
// concept). Patching `matchExpressions` with a new array therefore replaces
// the original array entirely, while `matchLabels` (a map) merges per-key.
// This pins the array-replace-vs-merge divergence from strategic-merge.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_patch_compatibility_v1_merge_patch() {
    let (mem, router) = spawn_router();
    let key = seed_base_pdb(&mem).await;

    // RFC 7386 merge:
    //   * matchLabels is an object → recursive merge → both basematch and
    //     patchmatch survive.
    //   * matchExpressions is an array → REPLACED wholesale → original
    //     `baseexpression` entry is gone; only `patchexpression` remains.
    let patch = json!({
        "spec": {
            "selector": {
                "matchLabels": {"patchmatch": "true"},
                "matchExpressions": [
                    {"key": "patchexpression", "operator": "In", "values": ["true"]}
                ]
            }
        }
    });

    let (status, body) = patch_pdb(router, MERGE_CT, None, &patch).await;
    assert_2xx(status, &body, "JSON merge patch on PDB selector");

    let stored = read_stored(&mem, &key).await;
    let selector = &stored["spec"]["selector"];

    // matchLabels — both keys present (object recursive-merge).
    let labels = selector["matchLabels"]
        .as_object()
        .unwrap_or_else(|| panic!("matchLabels missing/non-object in stored PDB: {}", stored));
    assert_eq!(
        labels.get("basematch").and_then(|v| v.as_str()),
        Some("true"),
        "merge-patch: original matchLabel `basematch` must survive map merge; got {:?}",
        labels
    );
    assert_eq!(
        labels.get("patchmatch").and_then(|v| v.as_str()),
        Some("true"),
        "merge-patch: patched matchLabel `patchmatch` must be added; got {:?}",
        labels
    );

    // matchExpressions — array REPLACED wholesale (RFC 7386 has no list-merge
    // semantics). The original `baseexpression` entry must be gone; only the
    // patched `patchexpression` remains.
    let exprs = selector["matchExpressions"].as_array().unwrap_or_else(|| {
        panic!(
            "merge-patch: matchExpressions must be present after array replace; got {}",
            stored
        )
    });
    assert_eq!(
        exprs.len(),
        1,
        "merge-patch: array replace must yield exactly the patched entry; got {:?}",
        exprs
    );
    assert_eq!(
        exprs[0]["key"], "patchexpression",
        "merge-patch: only patched matchExpression must remain; got {:?}",
        exprs
    );
    assert!(
        !exprs.iter().any(|e| e["key"] == "baseexpression"),
        "merge-patch: original `baseexpression` must be dropped (array replaced); got {:?}",
        exprs
    );
}

// ---------------------------------------------------------------------------
// 3. application/apply-patch+yaml (server-side apply)
//
// Upstream: LabelSelector is `+listType=atomic`, so server-side apply with a
// selector that only carries `matchLabels` atomically replaces the entire
// selector — `matchExpressions` from the original is dropped.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_patch_compatibility_v1_apply_patch() {
    let (mem, router) = spawn_router();
    let key = seed_base_pdb(&mem).await;

    // Server-side apply requires the full intended shape from one fieldManager.
    // We resend the full PDB metadata (apiVersion/kind/name) plus the
    // intended spec, with the selector containing ONLY matchLabels.
    let apply = json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": PDB_NAME,
            "namespace": TEST_NS,
        },
        "spec": {
            "maxUnavailable": 2,
            "selector": {
                "matchLabels": {"patchmatch": "true"}
            }
        }
    });

    let (status, body) = patch_pdb(
        router,
        APPLY_CT,
        Some("fieldManager=test-mgr&force=true"),
        &apply,
    )
    .await;
    assert_2xx(status, &body, "server-side apply on PDB");

    let stored = read_stored(&mem, &key).await;
    let selector = &stored["spec"]["selector"];

    // matchLabels in the applied selector survives.
    let labels = selector["matchLabels"]
        .as_object()
        .unwrap_or_else(|| panic!("matchLabels missing/non-object in stored PDB: {}", stored));
    assert_eq!(
        labels.get("patchmatch").and_then(|v| v.as_str()),
        Some("true"),
        "apply-patch: applied matchLabel must be present; got {:?}",
        labels
    );
    assert!(
        labels.get("basematch").is_none(),
        "apply-patch: original `basematch` must be replaced (atomic selector); got {:?}",
        labels
    );

    // The selector is atomic: matchExpressions from the original must NOT
    // survive after an apply that doesn't include it.
    let exprs = &selector["matchExpressions"];
    assert!(
        exprs.is_null() || exprs.as_array().is_some_and(|a| a.is_empty()),
        "apply-patch: matchExpressions must be cleared (atomic selector replace); got {:?}",
        exprs
    );
}
