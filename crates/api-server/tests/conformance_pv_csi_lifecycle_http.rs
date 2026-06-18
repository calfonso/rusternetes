//! Router-level replay of the upstream conformance test
//! `[sig-storage] PersistentVolumes CSI Conformance should run through the
//! lifecycle of a PV and a PVC` (persistent_volumes.go).
//!
//! Drives every step over the real Axum router (not the storage layer):
//!   create PV + PVC, list PVs by labelSelector, list PVCs in ns, patch PV
//!   label, patch PVC label, read both (UID present), delete both + confirm,
//!   create replacement PV + PVC, PUT-update labels, deleteCollection both +
//!   confirm. Mirrors the harness in `list_resource_version_router_test.rs`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// Thin shim over the shared `TestApiServer`, preserving this file's
// `send(&state, method, uri, content_type, body)` call sites.
async fn send(
    state: &TestApiServer,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    state.send(method, uri, content_type, body).await
}

fn label(body: &Value, key: &str) -> Option<String> {
    body.get("metadata")?
        .get("labels")?
        .get(key)?
        .as_str()
        .map(String::from)
}

const NS: &str = "pv-csi-lifecycle";

fn pv_body(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": { "name": name, "labels": { "volume-type": "csi" } },
        "spec": {
            "capacity": { "storage": "2Gi" },
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "standard",
            "hostPath": { "path": "/mnt/data" }
        }
    })
}

fn pvc_body(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": name, "labels": { "volume-type": "csi" } },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "standard",
            "resources": { "requests": { "storage": "2Gi" } }
        }
    })
}

#[tokio::test]
async fn pv_pvc_csi_lifecycle_over_http() {
    let state = TestApiServer::new();
    let pvc_uri = format!("/api/v1/namespaces/{NS}/persistentvolumeclaims");

    // 1. Create PV + PVC.
    let (s, pv) = send(
        &state,
        "POST",
        "/api/v1/persistentvolumes",
        Some("application/json"),
        Some(&pv_body("pv-csi-1")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create PV: {pv}");
    let (s, pvc) = send(
        &state,
        "POST",
        &pvc_uri,
        Some("application/json"),
        Some(&pvc_body("pvc-csi-1")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create PVC: {pvc}");

    // 2. List PVs with labelSelector MUST return exactly one.
    let (s, list) = send(
        &state,
        "GET",
        "/api/v1/persistentvolumes?labelSelector=volume-type%3Dcsi",
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let items = list
        .get("items")
        .and_then(|i| i.as_array())
        .expect("PV list items");
    assert_eq!(
        items.len(),
        1,
        "labelSelector PV list expected 1, got: {list}"
    );

    // 3. List PVCs in namespace MUST succeed.
    let (s, list) = send(&state, "GET", &pvc_uri, None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        list.get("items")
            .and_then(|i| i.as_array())
            .map(|a| a.len()),
        Some(1),
        "PVC list: {list}"
    );

    // 4. Patch PV MUST succeed with its new label found.
    let patch = json!({ "metadata": { "labels": { "patched": "pv" } } });
    let (s, pv) = send(
        &state,
        "PATCH",
        "/api/v1/persistentvolumes/pv-csi-1",
        Some("application/merge-patch+json"),
        Some(&patch),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "patch PV: {pv}");
    assert_eq!(
        label(&pv, "patched").as_deref(),
        Some("pv"),
        "patched PV label missing: {pv}"
    );

    // 5. Patch PVC MUST succeed with its new label found.
    let (s, pvc) = send(
        &state,
        "PATCH",
        &format!("{pvc_uri}/pvc-csi-1"),
        Some("application/merge-patch+json"),
        Some(&patch),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "patch PVC: {pvc}");
    assert_eq!(
        label(&pvc, "patched").as_deref(),
        Some("pv"),
        "patched PVC label missing: {pvc}"
    );

    // 6. Read PV and PVC MUST succeed with required UID retrieved.
    let (s, pv) = send(
        &state,
        "GET",
        "/api/v1/persistentvolumes/pv-csi-1",
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        pv.get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .map(|u| !u.is_empty())
            .unwrap_or(false),
        "PV uid empty: {pv}"
    );
    let (s, pvc) = send(&state, "GET", &format!("{pvc_uri}/pvc-csi-1"), None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        pvc.get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .map(|u| !u.is_empty())
            .unwrap_or(false),
        "PVC uid empty: {pvc}"
    );

    // 7. Delete PVC and PV MUST succeed and MUST be confirmed.
    let (s, _) = send(
        &state,
        "DELETE",
        &format!("{pvc_uri}/pvc-csi-1"),
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "delete PVC");
    let (s, _) = send(
        &state,
        "DELETE",
        "/api/v1/persistentvolumes/pv-csi-1",
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "delete PV");
    let (s, _) = send(
        &state,
        "GET",
        "/api/v1/persistentvolumes/pv-csi-1",
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "PV not confirmed deleted");
    let (s, _) = send(&state, "GET", &format!("{pvc_uri}/pvc-csi-1"), None, None).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "PVC not confirmed deleted");

    // 8. Replacement PV and PVC MUST be created.
    let (s, _) = send(
        &state,
        "POST",
        "/api/v1/persistentvolumes",
        Some("application/json"),
        Some(&pv_body("pv-csi-2")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, _) = send(
        &state,
        "POST",
        &pvc_uri,
        Some("application/json"),
        Some(&pvc_body("pvc-csi-2")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);

    // 9/10. Update (PUT) PV + PVC with a new label MUST succeed with label found.
    let (_, current_pv) = send(
        &state,
        "GET",
        "/api/v1/persistentvolumes/pv-csi-2",
        None,
        None,
    )
    .await;
    let mut updated_pv = current_pv.clone();
    updated_pv["metadata"]["labels"]["updated"] = json!("pv");
    let (s, pv) = send(
        &state,
        "PUT",
        "/api/v1/persistentvolumes/pv-csi-2",
        Some("application/json"),
        Some(&updated_pv),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "update PV: {pv}");
    assert_eq!(
        label(&pv, "updated").as_deref(),
        Some("pv"),
        "updated PV label missing: {pv}"
    );

    let (_, current_pvc) = send(&state, "GET", &format!("{pvc_uri}/pvc-csi-2"), None, None).await;
    let mut updated_pvc = current_pvc.clone();
    updated_pvc["metadata"]["labels"]["updated"] = json!("pvc");
    let (s, pvc) = send(
        &state,
        "PUT",
        &format!("{pvc_uri}/pvc-csi-2"),
        Some("application/json"),
        Some(&updated_pvc),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "update PVC: {pvc}");
    assert_eq!(
        label(&pvc, "updated").as_deref(),
        Some("pvc"),
        "updated PVC label missing: {pvc}"
    );

    // 11. deleteCollection PVC and PV MUST succeed and MUST be confirmed.
    let (s, _) = send(&state, "DELETE", &pvc_uri, None, None).await;
    assert!(s.is_success(), "deleteCollection PVC status: {s}");
    let (s, _) = send(&state, "DELETE", "/api/v1/persistentvolumes", None, None).await;
    assert!(s.is_success(), "deleteCollection PV status: {s}");
    let (s, list) = send(&state, "GET", &pvc_uri, None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        list.get("items")
            .and_then(|i| i.as_array())
            .map(|a| a.len()),
        Some(0),
        "PVC collection not empty: {list}"
    );
    let (s, list) = send(&state, "GET", "/api/v1/persistentvolumes", None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        list.get("items")
            .and_then(|i| i.as_array())
            .map(|a| a.len()),
        Some(0),
        "PV collection not empty: {list}"
    );
}
