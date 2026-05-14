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
use rusternetes_api_server::router::build_router;
use rusternetes_api_server::state::ApiServerState;
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;

async fn make_test_state() -> Arc<ApiServerState> {
    let storage = Arc::new(StorageBackend::new_memory());
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(
        MetricsRegistry::new()
            .with_api_server_metrics()
            .expect("metrics init"),
    );
    Arc::new(ApiServerState::new(
        storage,
        token_manager,
        authorizer,
        metrics,
        true, // skip_auth
    ))
}

#[tokio::test]
async fn router_builds_without_duplicate_route_panic() {
    let state = make_test_state().await;
    // build_router performs axum Router::merge internally; any duplicate
    // (method, path) pair causes an immediate panic here.
    let _router = build_router(state, None);
    // Reaching this line means no panic — both fixes are intact.
}
