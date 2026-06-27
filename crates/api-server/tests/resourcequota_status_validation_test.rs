//! ResourceQuota /status subresource runs validate_resource_quota_status_update
//! (#1484): the status `hard`/`used` maps must carry valid resource names and
//! parseable quantities, mirroring upstream ValidateResourceQuotaStatusUpdate.

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

async fn create_quota(api: &TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": name, "namespace": "default" },
        "spec": { "hard": { "pods": "10" } }
    });
    let (status, b): (StatusCode, Value) = api
        .send(
            Method::POST.as_str(),
            "/api/v1/namespaces/default/resourcequotas",
            Some("application/json"),
            Some(&body),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "create quota: {b}");
}

#[tokio::test]
async fn status_update_rejects_unparseable_used_quantity() {
    let api = TestApiServer::new();
    create_quota(&api, "rq-bad").await;

    let bad = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "rq-bad", "namespace": "default" },
        "status": { "hard": { "pods": "10" }, "used": { "pods": "notaquantity" } }
    });
    let (status, b): (StatusCode, Value) = api
        .send(
            Method::PUT.as_str(),
            "/api/v1/namespaces/default/resourcequotas/rq-bad/status",
            Some("application/json"),
            Some(&bad),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unparseable status.used quantity must be rejected, got {status}: {b}"
    );
}

#[tokio::test]
async fn status_update_accepts_valid_used_quantity() {
    let api = TestApiServer::new();
    create_quota(&api, "rq-ok").await;

    let good = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "rq-ok", "namespace": "default" },
        "status": { "hard": { "pods": "10" }, "used": { "pods": "3" } }
    });
    let (status, b): (StatusCode, Value) = api
        .send(
            Method::PUT.as_str(),
            "/api/v1/namespaces/default/resourcequotas/rq-ok/status",
            Some("application/json"),
            Some(&good),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "valid status update must succeed: {b}"
    );
    assert_eq!(b["status"]["used"]["pods"], json!("3"), "{b}");
}
