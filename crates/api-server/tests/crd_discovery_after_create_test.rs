//! Integration tests reproducing the upstream conformance flow where a freshly
//! created CustomResourceDefinition must become **served and discoverable
//! promptly**, so that a polling dynamic client converges instead of hitting
//! `context deadline exceeded`.
//!
//! Targets the `[sig-api-machinery]` specs that drive
//! `fixtures.CreateNewV1CustomResourceDefinition`
//! (`test/e2e/apimachinery/crd_publish_openapi.go`,
//! `crd_watch.go`, `aggregated_discovery.go`): create CRD → wait for
//! Established → poll until the new custom resource is served and visible in
//! discovery / the RESTMapper before listing it.
//!
//! Upstream apiextensions-apiserver registers a CRD's API group/version/resource
//! dynamically the moment the CRD is created. Rusternetes synthesizes the same
//! discovery documents on-read from stored CRDs. These tests assert every
//! discovery surface a client-go RESTMapper touches reflects the CRD right
//! after `POST`:
//!
//!   * `GET /apis` (legacy APIGroupList)
//!   * `GET /apis` aggregated (apidiscovery.k8s.io/v2 APIGroupDiscoveryList)
//!   * `GET /apis/{group}/` (single-group, trailing slash — the path client-go's
//!     RESTMapper hits)
//!   * `GET /apis/{group}/{version}` (APIResourceList with verbs)
//!   * `GET .../{plural}` collection (served, 200 empty list)
//!   * `POST .../{plural}` instance (CR can be created + listed)
//!
//! Harness mirrors `tests/conformance_apimachinery_crd_lifecycle.rs`.

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

/// Issue a request with an explicit `Accept` header (for aggregated-discovery
/// content negotiation) and return `(status, parsed body)`.
async fn send_accept(
    router: &TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
    accept: &str,
) -> (StatusCode, Value) {
    let bytes = body.map(|v| serde_json::to_vec(v).unwrap());
    let (status, _headers, _bytes, value) = router
        .send_full(
            method.as_str(),
            uri,
            Some("application/json"),
            Some(accept),
            bytes,
        )
        .await;
    (status, value)
}

async fn send(
    router: &TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    send_accept(router, method, uri, body, "application/json").await
}

const CRDS_URI: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

/// Build a v1 CRD body mirroring `fixtures.NewRandomNameV1CustomResourceDefinition`:
/// a group, namespaced scope, and one or more served versions.
fn crd_body(group: &str, plural: &str, kind: &str, versions: &[(&str, bool, bool)]) -> Value {
    let vers: Vec<Value> = versions
        .iter()
        .map(|(name, served, storage)| {
            json!({
                "name": name,
                "served": served,
                "storage": storage,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "x-kubernetes-preserve-unknown-fields": true
                    }
                }
            })
        })
        .collect();
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{}.{}", plural, group) },
        "spec": {
            "group": group,
            "names": {
                "plural": plural,
                "singular": kind.to_lowercase(),
                "kind": kind,
                "listKind": format!("{}List", kind)
            },
            "scope": "Namespaced",
            "versions": vers
        }
    })
}

/// The aggregated-discovery Accept header that client-go sends by default.
const AGG_ACCEPT: &str =
    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json";

// ---------------------------------------------------------------------------
// 1. Full upstream flow: create CRD, then every discovery surface + CR I/O.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crd_is_served_and_discoverable_immediately_after_create() {
    let router = spawn_router();
    let group = "e.example.com";

    let (s, body) = send(
        &router,
        Method::POST,
        CRDS_URI,
        Some(&crd_body(
            group,
            "examples",
            "Example",
            &[("v1", true, true)],
        )),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "CRD create failed: {body}");

    // Established immediately (the conformance helper waits on this first).
    let conditions = body
        .pointer("/status/conditions")
        .and_then(|c| c.as_array())
        .expect("status.conditions present right after create");
    assert!(
        conditions.iter().any(
            |c| c.get("type").and_then(|t| t.as_str()) == Some("Established")
                && c.get("status").and_then(|t| t.as_str()) == Some("True")
        ),
        "Established=True must be set immediately: {conditions:?}"
    );

    // (a) legacy APIGroupList includes the new group.
    let (s, groups) = send(&router, Method::GET, "/apis", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        groups
            .pointer("/groups")
            .and_then(|g| g.as_array())
            .unwrap()
            .iter()
            .any(|g| g.get("name").and_then(|n| n.as_str()) == Some(group)),
        "new CRD group missing from /apis: {groups}"
    );

    // (b) trailing-slash single-group discovery (client-go RESTMapper path).
    let (s, grp) = send(&router, Method::GET, &format!("/apis/{group}/"), None).await;
    assert_eq!(s, StatusCode::OK, "/apis/{group}/ must not 404: {grp}");
    assert_eq!(grp.get("name").and_then(|n| n.as_str()), Some(group));
    assert_eq!(
        grp.pointer("/preferredVersion/version")
            .and_then(|v| v.as_str()),
        Some("v1")
    );

    // (c) APIResourceList for group/version lists the resource with verbs.
    let (s, rl) = send(&router, Method::GET, &format!("/apis/{group}/v1"), None).await;
    assert_eq!(s, StatusCode::OK);
    let res = rl
        .pointer("/resources")
        .and_then(|r| r.as_array())
        .unwrap()
        .iter()
        .find(|r| r.get("name").and_then(|n| n.as_str()) == Some("examples"))
        .expect("examples resource present in APIResourceList");
    assert_eq!(res.get("kind").and_then(|k| k.as_str()), Some("Example"));
    assert!(res
        .get("verbs")
        .and_then(|v| v.as_array())
        .map(|v| v.iter().any(|x| x.as_str() == Some("list")))
        .unwrap_or(false));

    // (d) aggregated discovery surfaces the group and resource too.
    let (s, agg) = send_accept(&router, Method::GET, "/apis", None, AGG_ACCEPT).await;
    assert_eq!(s, StatusCode::OK);
    let agg_groups = agg.pointer("/items").and_then(|g| g.as_array()).unwrap();
    let agg_group = agg_groups
        .iter()
        .find(|g| g.pointer("/metadata/name").and_then(|n| n.as_str()) == Some(group))
        .expect("group present in aggregated discovery");
    let agg_res = agg_group
        .pointer("/versions/0/resources")
        .and_then(|r| r.as_array())
        .unwrap();
    assert!(
        agg_res
            .iter()
            .any(|r| r.get("resource").and_then(|n| n.as_str()) == Some("examples")),
        "examples resource missing from aggregated discovery: {agg_group}"
    );

    // (e) CR collection is served (200 empty list), then a CR can be created/listed.
    let coll = format!("/apis/{group}/v1/namespaces/default/examples");
    let (s, list) = send(&router, Method::GET, &coll, None).await;
    assert_eq!(s, StatusCode::OK, "CR collection not served: {list}");
    assert_eq!(
        list.pointer("/items")
            .and_then(|i| i.as_array())
            .map(|a| a.len()),
        Some(0)
    );

    let cr = json!({
        "apiVersion": format!("{group}/v1"),
        "kind": "Example",
        "metadata": { "name": "foo", "namespace": "default" },
        "spec": {}
    });
    let (s, created) = send(&router, Method::POST, &coll, Some(&cr)).await;
    assert_eq!(s, StatusCode::CREATED, "CR create failed: {created}");

    let (s, list) = send(&router, Method::GET, &coll, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        list.pointer("/items")
            .and_then(|i| i.as_array())
            .map(|a| a.len()),
        Some(1),
        "created CR not listed: {list}"
    );
}

// ---------------------------------------------------------------------------
// 2. Trailing-slash single-group discovery regression (was a hard 404).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_group_trailing_slash_is_404_until_crd_created() {
    let router = spawn_router();

    // Before any CRD declares it, the group does not resolve.
    let (s, _) = send(&router, Method::GET, "/apis/w.example.com/", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let (s, _) = send(
        &router,
        Method::POST,
        CRDS_URI,
        Some(&crd_body(
            "w.example.com",
            "widgets",
            "Widget",
            &[("v1", true, true)],
        )),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);

    // After create, the same trailing-slash lookup resolves the CRD's group.
    let (s, grp) = send(&router, Method::GET, "/apis/w.example.com/", None).await;
    assert_eq!(
        s,
        StatusCode::OK,
        "trailing-slash group lookup must resolve after create: {grp}"
    );
    assert_eq!(
        grp.get("name").and_then(|n| n.as_str()),
        Some("w.example.com")
    );
}

// ---------------------------------------------------------------------------
// 3. Multiple versions + a not-served version: discovery reflects served set.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn discovery_reflects_served_versions_only() {
    let router = spawn_router();
    let group = "m.example.com";

    // v1 served (storage), v2 served, v1beta1 NOT served.
    let (s, _) = send(
        &router,
        Method::POST,
        CRDS_URI,
        Some(&crd_body(
            group,
            "multis",
            "Multi",
            &[
                ("v1", true, true),
                ("v2", true, false),
                ("v1beta1", false, false),
            ],
        )),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);

    // Trailing-slash group lists exactly the served versions.
    let (s, grp) = send(&router, Method::GET, &format!("/apis/{group}/"), None).await;
    assert_eq!(s, StatusCode::OK);
    let versions: Vec<&str> = grp
        .pointer("/versions")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .filter_map(|v| v.get("version").and_then(|x| x.as_str()))
        .collect();
    assert!(versions.contains(&"v1"), "v1 served: {versions:?}");
    assert!(versions.contains(&"v2"), "v2 served: {versions:?}");
    assert!(
        !versions.contains(&"v1beta1"),
        "v1beta1 is not served and must be absent from discovery: {versions:?}"
    );

    // The served v2 group/version returns an APIResourceList...
    let (s, rl) = send(&router, Method::GET, &format!("/apis/{group}/v2"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(rl
        .pointer("/resources")
        .and_then(|r| r.as_array())
        .map(|r| r
            .iter()
            .any(|x| x.get("name").and_then(|n| n.as_str()) == Some("multis")))
        .unwrap_or(false));

    // ...while the not-served v1beta1 group/version exposes no resources.
    let (s, rl) = send(
        &router,
        Method::GET,
        &format!("/apis/{group}/v1beta1"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        rl.pointer("/resources")
            .and_then(|r| r.as_array())
            .map(|r| r.len()),
        Some(0),
        "not-served version must expose zero resources: {rl}"
    );
}
