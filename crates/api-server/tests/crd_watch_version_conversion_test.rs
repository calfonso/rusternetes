//! Regression test for #54: a custom-resource **watch** on a non-storage
//! version must convert each streamed object to the requested version before
//! field-selector filtering and emission — exactly as `list_custom_resources`
//! does (`convert_custom_resources`).
//!
//! Mirrors the `[sig-api-machinery] CustomResourceFieldSelectors MUST list and
//! watch custom resources matching the field selector` flow, minus a live
//! conversion webhook: this uses the `None` conversion strategy (apiVersion
//! rewrite, identical field layout across versions) so the test stays
//! self-contained. The webhook-layout half is exercised end-to-end by the
//! conformance canary, which reuses the same `convert_custom_resource` call.
//!
//! Before the fix the watch streamed stored-version (`v1`) objects and
//! field-selected against the stored layout; this asserts the streamed object
//! now carries `apiVersion: .../v2` and that the `color=blue` selector matches.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GROUP: &str = "stable.example.com";

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

async fn post_json(router: &TestApiServer, uri: &str, body: &Value) -> StatusCode {
    router.post(uri, body).await.0
}

/// Drive a `?watch=true` GET and collect the full streamed body (the watch
/// closes itself after `timeoutSeconds`), returning the parsed line events.
async fn collect_watch(router: &TestApiServer, uri: &str) -> Vec<Value> {
    let (status, _headers, bytes, _) = router.send_full("GET", uri, None, None, None).await;
    assert_eq!(status, StatusCode::OK, "watch must return 200");
    let body = String::from_utf8(bytes).unwrap();
    body.lines()
        .filter(|l| !l.is_empty())
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .collect()
}

fn two_version_crd() -> Value {
    // None conversion strategy (omit spec.conversion). Both versions declare the
    // same top-level selectable field `.color`, so the only cross-version
    // difference is apiVersion — which the watch must rewrite to the requested
    // version. schema=None ⇒ no pruning, so the top-level `color` survives.
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("crontabs.{GROUP}") },
        "spec": {
            "group": GROUP,
            "scope": "Namespaced",
            "names": {
                "plural": "crontabs",
                "singular": "crontab",
                "kind": "CronTab",
                "listKind": "CronTabList"
            },
            "versions": [
                {
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "selectableFields": [ { "jsonPath": ".color" } ]
                },
                {
                    "name": "v2",
                    "served": true,
                    "storage": false,
                    "selectableFields": [ { "jsonPath": ".color" } ]
                }
            ]
        }
    })
}

fn crontab(name: &str, color: &str) -> Value {
    json!({
        "apiVersion": format!("{GROUP}/v1"),
        "kind": "CronTab",
        "metadata": { "name": name, "namespace": "default" },
        "color": color
    })
}

#[tokio::test]
async fn watch_v2_converts_stored_v1_objects_and_honors_field_selector() {
    let router = spawn_router();

    assert_eq!(
        post_json(
            &router,
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &two_version_crd(),
        )
        .await,
        StatusCode::CREATED
    );

    // Two CRs, stored at the storage version v1.
    let base = format!("/apis/{GROUP}/v1/namespaces/default/crontabs");
    assert_eq!(
        post_json(&router, &base, &crontab("blue-one", "blue")).await,
        StatusCode::CREATED
    );
    assert_eq!(
        post_json(&router, &base, &crontab("red-one", "red")).await,
        StatusCode::CREATED
    );

    // Watch v2 with a field selector on the selectable field. The handler must
    // convert each stored v1 object to v2 before filtering + emitting.
    let watch_uri = format!(
        "/apis/{GROUP}/v2/namespaces/default/crontabs?watch=true&timeoutSeconds=1&fieldSelector=color=blue"
    );
    let events = collect_watch(&router, &watch_uri).await;

    let added: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == "ADDED")
        .map(|e| &e["object"])
        .collect();

    // Exactly the blue CR matches the selector...
    assert_eq!(
        added.len(),
        1,
        "expected one ADDED (color=blue) under fieldSelector, got: {events:?}"
    );
    let obj = added[0];
    assert_eq!(obj["metadata"]["name"], "blue-one");
    assert_eq!(obj["color"], "blue");

    // ...and it is delivered in the requested version v2, not the stored v1.
    assert_eq!(
        obj["apiVersion"],
        format!("{GROUP}/v2"),
        "watch must convert the streamed object to the requested version"
    );
}

#[tokio::test]
async fn watch_v2_without_selector_converts_all_objects() {
    let router = spawn_router();
    post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &two_version_crd(),
    )
    .await;

    let base = format!("/apis/{GROUP}/v1/namespaces/default/crontabs");
    post_json(&router, &base, &crontab("a", "blue")).await;
    post_json(&router, &base, &crontab("b", "red")).await;

    let watch_uri =
        format!("/apis/{GROUP}/v2/namespaces/default/crontabs?watch=true&timeoutSeconds=1");
    let events = collect_watch(&router, &watch_uri).await;

    let added: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == "ADDED")
        .map(|e| &e["object"])
        .collect();
    assert_eq!(added.len(), 2, "both CRs delivered, got: {events:?}");
    for obj in added {
        assert_eq!(
            obj["apiVersion"],
            format!("{GROUP}/v2"),
            "every streamed object converted to v2"
        );
    }
}
