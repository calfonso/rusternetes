//! Update immutability for RuntimeClass (handler) and ServiceCIDR (spec.cidrs),
//! ported from upstream ValidateRuntimeClassUpdate / ValidateServiceCIDRUpdate.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---- RuntimeClass: handler is immutable ----------------------------------

fn rc_uri() -> &'static str {
    "/apis/node.k8s.io/v1/runtimeclasses"
}
fn rc(name: &str, handler: &str) -> Value {
    json!({
        "apiVersion": "node.k8s.io/v1", "kind": "RuntimeClass",
        "metadata": {"name": name}, "handler": handler
    })
}

#[tokio::test]
async fn runtimeclass_handler_immutable() {
    let state = TestApiServer::new();
    let name = "myrc";
    let (code, _) = state.post(rc_uri(), &rc(name, "runc")).await;
    assert_eq!(code, StatusCode::CREATED);
    let item = format!("{}/{name}", rc_uri());

    let (code, _) = state.put(&item, &rc(name, "gvisor")).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "handler change must be rejected"
    );

    let (code, _) = state.put(&item, &rc(name, "runc")).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "unchanged handler update must succeed"
    );
}

// ---- ServiceCIDR: spec.cidrs immutable (dual-stack expansion allowed) -----

fn sc_uri() -> &'static str {
    "/apis/networking.k8s.io/v1/servicecidrs"
}
fn sc(name: &str, cidrs: Value) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "ServiceCIDR",
        "metadata": {"name": name}, "spec": {"cidrs": cidrs}
    })
}

#[tokio::test]
async fn servicecidr_cidrs_immutable() {
    let state = TestApiServer::new();
    let name = "mycidr";
    let (code, _) = state
        .post(sc_uri(), &sc(name, json!(["10.0.0.0/24"])))
        .await;
    assert_eq!(code, StatusCode::CREATED);
    let item = format!("{}/{name}", sc_uri());

    // Changing the existing CIDR is rejected.
    let (code, _) = state.put(&item, &sc(name, json!(["10.1.0.0/24"]))).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "changing cidr must be rejected"
    );

    // Appending a second (dual-stack) CIDR is allowed.
    let (code, _) = state
        .put(&item, &sc(name, json!(["10.0.0.0/24", "2001:db8::/64"])))
        .await;
    assert_eq!(
        code,
        StatusCode::OK,
        "single->dual-stack expansion must be allowed"
    );
}
