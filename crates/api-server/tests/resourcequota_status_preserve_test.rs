//! Regression test for issue #1061: a main (non-`/status`) PUT to a
//! ResourceQuota must NOT wipe the controller-computed status.
//!
//! Upstream `resourcequotaStrategy.PrepareForUpdate` copies the stored
//! object's status onto the incoming object so a spec-only PUT (which carries
//! an empty status) cannot clobber `used`/`hard`. This mirrors the symmetric
//! `/status` strategy fixed in #268.

use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// Thin shim over the shared harness: returns just the JSON body (this file
// asserts on the body, not the status). `TestApiServer` boots build_router on
// MemoryStorage with --skip-auth; `api.storage` is the backing store the
// PrepareForUpdate assertions read directly.
async fn send_json(api: &TestApiServer, method: &str, uri: &str, body: Option<&Value>) -> Value {
    let content_type = body.map(|_| "application/json");
    let (_status, _raw, value) = api.send_raw(method, uri, content_type, body).await;
    value
}

#[tokio::test]
async fn spec_put_preserves_controller_status() {
    let api = TestApiServer::new();
    let mem = api.storage.clone();

    // Create the quota with hard limits (status will be auto-initialized).
    let create = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "q", "namespace": "default" },
        "spec": { "hard": { "pods": "10" } },
    });
    send_json(
        &api,
        "POST",
        "/api/v1/namespaces/default/resourcequotas",
        Some(&create),
    )
    .await;

    // Simulate the controller computing usage by writing status directly.
    let key = build_key("resourcequotas", Some("default"), "q");
    let mut stored: Value = mem.get(&key).await.unwrap();
    stored["status"] = json!({
        "hard": { "pods": "10" },
        "used": { "pods": "3" },
    });
    mem.update(&key, &stored).await.unwrap();

    // Client PUTs a spec-only update with an empty/absent status.
    let put = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "q", "namespace": "default" },
        "spec": { "hard": { "pods": "20" } },
    });
    let resp = send_json(
        &api,
        "PUT",
        "/api/v1/namespaces/default/resourcequotas/q",
        Some(&put),
    )
    .await;

    // Spec change applied...
    assert_eq!(resp["spec"]["hard"]["pods"], json!("20"));
    // ...but controller-computed used status survived the spec PUT.
    assert_eq!(
        resp["status"]["used"]["pods"],
        json!("3"),
        "spec PUT must not wipe controller status: {resp}"
    );

    // And a subsequent GET still sees the preserved status.
    let got = send_json(
        &api,
        "GET",
        "/api/v1/namespaces/default/resourcequotas/q",
        None,
    )
    .await;
    assert_eq!(got["status"]["used"]["pods"], json!("3"));
}
