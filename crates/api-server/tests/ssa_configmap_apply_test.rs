//! Server-Side Apply (SSA) scaffold tests for ConfigMap.
//!
//! Pins the four contract points the scaffold has to honour:
//!   1. Apply create — no existing object → HTTP 201 + manager owns every
//!      leaf in the body.
//!   2. Apply update no conflict — same `fieldManager` reapplies → HTTP 200,
//!      manager still owns the relevant leaves, others unchanged.
//!   3. Apply update conflict — different `fieldManager`, `force=false` →
//!      HTTP 409 Conflict, original value preserved.
//!   4. Apply update force conflict — different manager, `force=true` →
//!      HTTP 200, value overwritten, ownership transferred.
//!
//! Harness mirrors `integration_configmap_lifecycle.rs`: `MemoryStorage` +
//! `build_router` + `tower::ServiceExt::oneshot`. `skip_auth=true` so no
//! bearer token is required.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn make_state() -> TestApiServer {
    TestApiServer::new()
}

/// Issue an SSA PATCH with `Content-Type: application/apply-patch+yaml`.
async fn apply(
    state: &TestApiServer,
    namespace: &str,
    name: &str,
    field_manager: &str,
    force: bool,
    desired: &Value,
) -> (StatusCode, Value) {
    let mut uri = format!(
        "/api/v1/namespaces/{}/configmaps/{}?fieldManager={}",
        namespace, name, field_manager
    );
    if force {
        uri.push_str("&force=true");
    }
    // YAML body is the canonical SSA content-type. We send JSON-as-YAML
    // (well-formed JSON parses as YAML 1.2), so the standard `send` path —
    // which serialises the `Value` to JSON bytes — produces a valid body.
    state
        .send(
            "PATCH",
            &uri,
            Some("application/apply-patch+yaml"),
            Some(desired),
        )
        .await
}

fn desired_configmap(name: &str, data: serde_json::Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": name, "namespace": "default" },
        "data": data,
    })
}

// ---------------------------------------------------------------------------
// 1. Apply create (no existing object → 201)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_create_returns_201_and_records_manager_ownership() {
    let state = make_state();

    let desired = desired_configmap("cm-create", json!({"k1": "v1", "k2": "v2"}));
    let (status, body) = apply(&state, "default", "cm-create", "kubectl", false, &desired).await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");

    assert_eq!(body["data"]["k1"], "v1");
    assert_eq!(body["data"]["k2"], "v2");

    // managedFields must contain a single Apply entry owned by `kubectl`
    // covering both leaves.
    let mf = body["metadata"]["managedFields"].as_array().unwrap();
    assert_eq!(mf.len(), 1);
    assert_eq!(mf[0]["manager"], "kubectl");
    assert_eq!(mf[0]["operation"], "Apply");
    assert_eq!(mf[0]["apiVersion"], "v1");
    assert_eq!(mf[0]["fieldsType"], "FieldsV1");
    let fv1 = &mf[0]["fieldsV1"];
    assert!(fv1["f:data"]["f:k1"].is_object());
    assert!(fv1["f:data"]["f:k2"].is_object());
}

// ---------------------------------------------------------------------------
// 2. Apply update no conflict (same fieldManager → 200, ownership intact)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_reapply_same_manager_returns_200_and_keeps_ownership() {
    let state = make_state();

    // Initial create.
    let initial = desired_configmap("cm-reapply", json!({"k1": "v1"}));
    let (status, _) = apply(&state, "default", "cm-reapply", "kubectl", false, &initial).await;
    assert_eq!(status, StatusCode::CREATED);

    // Same manager applies a new leaf alongside the existing one.
    let next = desired_configmap("cm-reapply", json!({"k1": "v1", "k2": "v2"}));
    let (status, body) = apply(&state, "default", "cm-reapply", "kubectl", false, &next).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    assert_eq!(body["data"]["k1"], "v1");
    assert_eq!(body["data"]["k2"], "v2");

    let mf = body["metadata"]["managedFields"].as_array().unwrap();
    // Still exactly one entry — the applier reused its existing record.
    assert_eq!(mf.len(), 1);
    assert_eq!(mf[0]["manager"], "kubectl");
    assert_eq!(mf[0]["operation"], "Apply");
    let fv1 = &mf[0]["fieldsV1"];
    assert!(fv1["f:data"]["f:k1"].is_object());
    assert!(fv1["f:data"]["f:k2"].is_object());

    // Re-apply that drops `k2` should release ownership AND remove the leaf.
    let drop_k2 = desired_configmap("cm-reapply", json!({"k1": "v1"}));
    let (status, body) = apply(&state, "default", "cm-reapply", "kubectl", false, &drop_k2).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["data"]["k1"], "v1");
    assert!(
        body["data"].get("k2").is_none(),
        "k2 should have been released and removed: {body}"
    );
}

// ---------------------------------------------------------------------------
// 3. Apply update conflict (different manager, no force → 409)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_conflict_without_force_returns_409_and_preserves_value() {
    let state = make_state();

    // Manager A creates with k1=A.
    let a_body = desired_configmap("cm-conflict", json!({"k1": "A"}));
    let (status, _) = apply(
        &state,
        "default",
        "cm-conflict",
        "manager-a",
        false,
        &a_body,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Manager B tries to set k1=B without force — must 409.
    let b_body = desired_configmap("cm-conflict", json!({"k1": "B"}));
    let (status, body) = apply(
        &state,
        "default",
        "cm-conflict",
        "manager-b",
        false,
        &b_body,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    assert_eq!(body["kind"], "Status");
    assert_eq!(body["reason"], "Conflict");

    // Stored object must still have k1=A and manager-a as owner.
    let (gs, gbody) = state
        .get("/api/v1/namespaces/default/configmaps/cm-conflict")
        .await;
    assert_eq!(gs, StatusCode::OK);
    assert_eq!(gbody["data"]["k1"], "A");
    let mf = gbody["metadata"]["managedFields"].as_array().unwrap();
    assert_eq!(mf.len(), 1);
    assert_eq!(mf[0]["manager"], "manager-a");
}

// ---------------------------------------------------------------------------
// 4. Apply update force conflict (different manager, force=true → 200,
//    ownership transferred)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_force_transfers_ownership_and_returns_200() {
    let state = make_state();

    let a_body = desired_configmap("cm-force", json!({"k1": "A"}));
    let (status, _) = apply(&state, "default", "cm-force", "manager-a", false, &a_body).await;
    assert_eq!(status, StatusCode::CREATED);

    let b_body = desired_configmap("cm-force", json!({"k1": "B"}));
    let (status, body) = apply(&state, "default", "cm-force", "manager-b", true, &b_body).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["data"]["k1"], "B");

    let mf = body["metadata"]["managedFields"].as_array().unwrap();
    // manager-a should have been stripped of its ownership of /data/k1 —
    // since that's its only leaf the whole entry is dropped. manager-b is
    // the sole remaining manager.
    let names: Vec<&str> = mf
        .iter()
        .map(|e| e["manager"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(names, vec!["manager-b"], "managedFields: {mf:?}");
    assert!(mf[0]["fieldsV1"]["f:data"]["f:k1"].is_object());
}

// ---------------------------------------------------------------------------
// 5. Bonus: missing fieldManager is rejected.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_missing_field_manager_returns_400() {
    let state = make_state();
    let body = desired_configmap("cm-nofm", json!({"k1": "v1"}));
    let (status, _) = state
        .send(
            "PATCH",
            "/api/v1/namespaces/default/configmaps/cm-nofm",
            Some("application/apply-patch+yaml"),
            Some(&body),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// 6. Bonus: apply-patch+json content-type also works.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_apply_patch_json_content_type_is_accepted() {
    let state = make_state();
    let body = desired_configmap("cm-json", json!({"k1": "v1"}));
    let (status, body) = state
        .send(
            "PATCH",
            "/api/v1/namespaces/default/configmaps/cm-json?fieldManager=kubectl",
            Some("application/apply-patch+json"),
            Some(&body),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert_eq!(body["data"]["k1"], "v1");
}

// ---------------------------------------------------------------------------
// 7. Bonus: SSA must honour `immutable=true` like PUT does.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_immutable_configmap_rejects_data_change() {
    let state = make_state();

    // Create with immutable=true via plain POST.
    let create_body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-immut", "namespace": "default" },
        "data": { "k1": "v1" },
        "immutable": true,
    });
    let (cs, _) = state
        .post("/api/v1/namespaces/default/configmaps", &create_body)
        .await;
    assert_eq!(cs, StatusCode::CREATED);

    // Attempt SSA mutation of data on an immutable ConfigMap.
    let desired = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-immut", "namespace": "default" },
        "data": { "k1": "v2" },
        "immutable": true,
    });
    let (status, _) = apply(&state, "default", "cm-immut", "kubectl", false, &desired).await;
    // Upstream returns 422 (Invalid) for immutability violations; our
    // legacy update handler returns the same. SSA must match.
    assert!(
        status == StatusCode::UNPROCESSABLE_ENTITY || status == StatusCode::BAD_REQUEST,
        "expected 422 or 400, got {status}"
    );
}
