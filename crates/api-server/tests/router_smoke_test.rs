/// Router smoke test: verifies that build_router does not panic due to
/// duplicate route registrations.
///
/// Background: two production fixes removed duplicate `(method, path)` pairs
/// that caused axum to panic inside `Router::merge` at server startup:
///   b6df424 — duplicate .delete() on /apis/flowcontrol.apiserver.k8s.io/v1/flowschemas/:name
///   86dc04a — duplicate POST on /api/v1/namespaces/:ns/serviceaccounts/:sa/token
///
/// If either duplicate is reintroduced, `build_router` will panic and this
/// test will fail with a panic message from axum.
use rusternetes_test_support::harness::TestApiServer;

#[tokio::test]
async fn router_builds_without_duplicate_route_panic() {
    // TestApiServer::new() calls build_router internally (axum Router::merge);
    // any duplicate (method, path) pair causes an immediate panic here.
    // Reaching the end means no panic — both fixes are intact.
    let _api = TestApiServer::new();
}
