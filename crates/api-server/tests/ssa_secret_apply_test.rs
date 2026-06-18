//! Server-Side Apply (SSA) tests for Secret.
//!
//! Mirrors `ssa_configmap_apply_test.rs` — the same seven contract points
//! plus two Secret-specific cases:
//!
//!   1. Apply create — no existing object → HTTP 201 + manager owns every
//!      leaf in the body.
//!   2. Apply update no conflict — same `fieldManager` reapplies → HTTP 200,
//!      manager still owns the relevant leaves, others unchanged.
//!   3. Apply update conflict — different `fieldManager`, `force=false` →
//!      HTTP 409 Conflict, original value preserved.
//!   4. Apply update force conflict — different manager, `force=true` →
//!      HTTP 200, value overwritten, ownership transferred.
//!   5. Missing `fieldManager` → HTTP 400.
//!   6. `apply-patch+json` Content-Type accepted.
//!   7. `immutable: true` Secret rejects data mutation.
//!   8. `Secret.type` is immutable post-create (parity with upstream
//!      `pkg/registry/core/secret/strategy.go::ValidateUpdate`).
//!   9. `stringData` takes precedence over `data` when both target the
//!      same key — parity with upstream `Secret::PrepareForCreate` /
//!      `PrepareForUpdate` which fold `stringData` into base64-encoded
//!      `data` before persistence.
//!
//! Harness: `MemoryStorage` + `build_router` + `tower::ServiceExt::oneshot`,
//! same shape as the ConfigMap test so the regression guard is symmetric.

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
        "/api/v1/namespaces/{}/secrets/{}?fieldManager={}",
        namespace, name, field_manager
    );
    if force {
        uri.push_str("&force=true");
    }
    // JSON-as-YAML body routes through the standard `send` (Value → JSON bytes).
    state
        .send(
            "PATCH",
            &uri,
            Some("application/apply-patch+yaml"),
            Some(desired),
        )
        .await
}

/// Helper: base64-encode a string. Secret `data` values are base64 strings
/// on the wire; `stringData` is plain text.
fn b64(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

fn desired_secret(name: &str, data: serde_json::Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": "default" },
        "type": "Opaque",
        "data": data,
    })
}

// ---------------------------------------------------------------------------
// 1. Apply create (no existing object → 201)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_create_returns_201_and_records_manager_ownership() {
    let state = make_state();

    let desired = desired_secret("sec-create", json!({"k1": b64("v1"), "k2": b64("v2")}));
    let (status, body) = apply(&state, "default", "sec-create", "kubectl", false, &desired).await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");

    assert_eq!(body["data"]["k1"], b64("v1"));
    assert_eq!(body["data"]["k2"], b64("v2"));

    // managedFields must contain a single Apply entry owned by `kubectl`
    // covering both data leaves AND the `type` atomic leaf.
    let mf = body["metadata"]["managedFields"].as_array().unwrap();
    assert_eq!(mf.len(), 1);
    assert_eq!(mf[0]["manager"], "kubectl");
    assert_eq!(mf[0]["operation"], "Apply");
    assert_eq!(mf[0]["apiVersion"], "v1");
    assert_eq!(mf[0]["fieldsType"], "FieldsV1");
    let fv1 = &mf[0]["fieldsV1"];
    assert!(fv1["f:data"]["f:k1"].is_object());
    assert!(fv1["f:data"]["f:k2"].is_object());
    assert!(
        fv1["f:type"].is_object(),
        "type atomic leaf must be claimed: {fv1}"
    );
}

// ---------------------------------------------------------------------------
// 2. Apply update no conflict (same fieldManager → 200, ownership intact)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_reapply_same_manager_returns_200_and_keeps_ownership() {
    let state = make_state();

    // Initial create.
    let initial = desired_secret("sec-reapply", json!({"k1": b64("v1")}));
    let (status, _) = apply(&state, "default", "sec-reapply", "kubectl", false, &initial).await;
    assert_eq!(status, StatusCode::CREATED);

    // Same manager applies a new leaf alongside the existing one.
    let next = desired_secret("sec-reapply", json!({"k1": b64("v1"), "k2": b64("v2")}));
    let (status, body) = apply(&state, "default", "sec-reapply", "kubectl", false, &next).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    assert_eq!(body["data"]["k1"], b64("v1"));
    assert_eq!(body["data"]["k2"], b64("v2"));

    let mf = body["metadata"]["managedFields"].as_array().unwrap();
    // Still exactly one entry — the applier reused its existing record.
    assert_eq!(mf.len(), 1);
    assert_eq!(mf[0]["manager"], "kubectl");
    assert_eq!(mf[0]["operation"], "Apply");
    let fv1 = &mf[0]["fieldsV1"];
    assert!(fv1["f:data"]["f:k1"].is_object());
    assert!(fv1["f:data"]["f:k2"].is_object());

    // Re-apply that drops `k2` should release ownership AND remove the leaf.
    let drop_k2 = desired_secret("sec-reapply", json!({"k1": b64("v1")}));
    let (status, body) = apply(&state, "default", "sec-reapply", "kubectl", false, &drop_k2).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["data"]["k1"], b64("v1"));
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
    let a_body = desired_secret("sec-conflict", json!({"k1": b64("A")}));
    let (status, _) = apply(
        &state,
        "default",
        "sec-conflict",
        "manager-a",
        false,
        &a_body,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Manager B tries to set k1=B without force — must 409.
    let b_body = desired_secret("sec-conflict", json!({"k1": b64("B")}));
    let (status, body) = apply(
        &state,
        "default",
        "sec-conflict",
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
        .get("/api/v1/namespaces/default/secrets/sec-conflict")
        .await;
    assert_eq!(gs, StatusCode::OK);
    assert_eq!(gbody["data"]["k1"], b64("A"));
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

    let a_body = desired_secret("sec-force", json!({"k1": b64("A")}));
    let (status, _) = apply(&state, "default", "sec-force", "manager-a", false, &a_body).await;
    assert_eq!(status, StatusCode::CREATED);

    let b_body = desired_secret("sec-force", json!({"k1": b64("B")}));
    let (status, body) = apply(&state, "default", "sec-force", "manager-b", true, &b_body).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["data"]["k1"], b64("B"));

    let mf = body["metadata"]["managedFields"].as_array().unwrap();
    // manager-a should have been stripped of /data/k1 — its remaining
    // leaves are now under manager-b's control too (both managers had
    // claimed type=Opaque on create/force-update, but with shared
    // ownership manager-a keeps its `type` claim). The applier
    // (manager-b) is always present.
    let names: std::collections::BTreeSet<&str> = mf
        .iter()
        .map(|e| e["manager"].as_str().unwrap_or(""))
        .collect();
    assert!(
        names.contains("manager-b"),
        "manager-b must be present: {mf:?}"
    );
    // The applier owns /data/k1 with the new value.
    let mgr_b = mf
        .iter()
        .find(|e| e["manager"] == "manager-b")
        .expect("manager-b entry");
    assert!(mgr_b["fieldsV1"]["f:data"]["f:k1"].is_object());
}

// ---------------------------------------------------------------------------
// 5. Bonus: missing fieldManager is rejected.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_missing_field_manager_returns_400() {
    let state = make_state();
    let body = desired_secret("sec-nofm", json!({"k1": b64("v1")}));
    let (status, _) = state
        .send(
            "PATCH",
            "/api/v1/namespaces/default/secrets/sec-nofm",
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
    let body = desired_secret("sec-json", json!({"k1": b64("v1")}));
    let (status, body) = state
        .send(
            "PATCH",
            "/api/v1/namespaces/default/secrets/sec-json?fieldManager=kubectl",
            Some("application/apply-patch+json"),
            Some(&body),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert_eq!(body["data"]["k1"], b64("v1"));
}

// ---------------------------------------------------------------------------
// 7. Bonus: SSA must honour `immutable=true` like PUT does.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_immutable_secret_rejects_data_change() {
    let state = make_state();

    // Create with immutable=true via plain POST.
    let create_body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-immut", "namespace": "default" },
        "type": "Opaque",
        "data": { "k1": b64("v1") },
        "immutable": true,
    });
    let (cs, _) = state
        .post("/api/v1/namespaces/default/secrets", &create_body)
        .await;
    assert_eq!(cs, StatusCode::CREATED);

    // Attempt SSA mutation of data on an immutable Secret.
    let desired = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-immut", "namespace": "default" },
        "type": "Opaque",
        "data": { "k1": b64("v2") },
        "immutable": true,
    });
    let (status, _) = apply(&state, "default", "sec-immut", "kubectl", false, &desired).await;
    // Upstream returns 422 (Invalid) for immutability violations; our
    // legacy update handler returns the same. SSA must match.
    assert!(
        status == StatusCode::UNPROCESSABLE_ENTITY || status == StatusCode::BAD_REQUEST,
        "expected 422 or 400, got {status}"
    );
}

// ---------------------------------------------------------------------------
// 8. Secret-specific: `type` field is immutable post-create.
//
//    Upstream pin:
//    `pkg/registry/core/secret/strategy.go::ValidateUpdate` calls
//    `apivalidation.ValidateImmutableField(newSecret.Type, oldSecret.Type, …)`
//    unconditionally. SSA must enforce that fence too — otherwise an
//    applier could flip Opaque → kubernetes.io/basic-auth and bypass the
//    PUT-path guard.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_secret_type_field_is_immutable_post_create() {
    let state = make_state();

    // Create via SSA with type=Opaque.
    let initial = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-type", "namespace": "default" },
        "type": "Opaque",
        "data": { "k1": b64("v1") },
    });
    let (status, _) = apply(&state, "default", "sec-type", "kubectl", false, &initial).await;
    assert_eq!(status, StatusCode::CREATED);

    // Try to flip type to kubernetes.io/basic-auth — must be rejected.
    let flip = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-type", "namespace": "default" },
        "type": "kubernetes.io/basic-auth",
        "data": { "k1": b64("v1") },
    });
    let (status, body) = apply(&state, "default", "sec-type", "kubectl", false, &flip).await;
    assert!(
        (400..500).contains(&status.as_u16()),
        "type-change must be 4xx; got {status}, body={body}"
    );

    // The stored object must still have type=Opaque.
    let (gs, gbody) = state
        .get("/api/v1/namespaces/default/secrets/sec-type")
        .await;
    assert_eq!(gs, StatusCode::OK);
    assert_eq!(gbody["type"], "Opaque");
}

// ---------------------------------------------------------------------------
// 9. Secret-specific: stringData clobbers data for the same key.
//
//    Upstream pin:
//    `pkg/apis/core/strategy.go` Secret `PrepareForCreate` /
//    `PrepareForUpdate` (and the `normalize()` shim on
//    `rusternetes_common::resources::Secret`) fold `stringData` entries
//    into base64-encoded `data` entries — later entries (stringData) win.
//    SSA must produce the same effective storage state.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ssa_stringdata_takes_precedence_over_data() {
    let state = make_state();

    // Apply both `data.k1 = base64("data-value")` and
    // `stringData.k1 = "stringdata-value"`. After storage, k1 must be
    // base64("stringdata-value") — the stringData entry wins.
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-prec", "namespace": "default" },
        "type": "Opaque",
        "data":       { "k1": b64("data-value") },
        "stringData": { "k1": "stringdata-value" },
    });
    let (status, resp) = apply(&state, "default", "sec-prec", "kubectl", false, &body).await;
    assert_eq!(status, StatusCode::CREATED, "body: {resp}");

    // Stored object: data.k1 must be base64("stringdata-value"),
    // stringData absent (write-only).
    let (gs, gbody) = state
        .get("/api/v1/namespaces/default/secrets/sec-prec")
        .await;
    assert_eq!(gs, StatusCode::OK);
    assert_eq!(
        gbody["data"]["k1"],
        b64("stringdata-value"),
        "stringData must clobber data: {gbody}"
    );
    assert!(
        gbody.get("stringData").is_none() || gbody["stringData"].is_null(),
        "stringData is write-only and must not round-trip: {gbody}"
    );
}
