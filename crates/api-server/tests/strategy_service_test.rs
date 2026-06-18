//! Router-driven mirror of upstream Kubernetes v1.35
//! `pkg/registry/core/service/strategy_test.go` semantics.
//!
//! Source (permalink):
//! https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/core/service/strategy_test.go
//!
//! Upstream's strategy tests exercise the registry-layer defaulting and
//! immutability rules that fire on CREATE / UPDATE. Rusternetes does not
//! separate "strategy" from "handler" — the same logic lives in
//! `crates/api-server/src/handlers/service.rs`. This file pins those rules at
//! the HTTP edge using the in-process Axum router pattern from
//! `tests/integration_dryrun_all_resources.rs`.
//!
//! Scenarios covered (one `#[tokio::test]` per scenario):
//!   - ClusterIP immutability after assignment (cannot change, cannot clear,
//!     cannot toggle to/from headless `"None"`).
//!   - `clusterIPs` / `ipFamilies` / `ipFamilyPolicy` defaulting on create
//!     (SingleStack default, family list populated).
//!   - `sessionAffinity` defaulting to `"None"` when omitted.
//!   - `sessionAffinityConfig` cleared when affinity is None on update.
//!   - `ServicePort.protocol` defaults to TCP; `targetPort` defaults to
//!     `port`.
//!   - `publishNotReadyAddresses` is preserved as-set (`false` default in
//!     the absence of explicit set).
//!   - Type transitions: ClusterIP -> NodePort allocates NodePort;
//!     ClusterIP -> LoadBalancer allocates NodePort; ClusterIP -> ExternalName
//!     clears the ClusterIP.
//!
//! Tests that surface a real registry-layer gap (e.g. ClusterIP immutability
//! is not yet enforced at the handler) are marked `#[ignore = "blocked on
//! issue #TBD: <reason>"]` per the batch convention so the regression is
//! recorded without breaking the green build.

use axum::http::Method;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

const NS: &str = "strategy-svc-ns";

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `mem` is the
// backing store for stored-object assertions.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send_json(router: TestApiServer, method: Method, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router
        .send(method.as_str(), uri, Some("application/json"), Some(body))
        .await;
    (status.as_u16(), value)
}

async fn stored(mem: &Arc<MemoryStorage>, name: &str) -> Option<Value> {
    let key = build_key("services", Some(NS), name);
    mem.get::<Value>(&key).await.ok()
}

fn services_uri() -> String {
    format!("/api/v1/namespaces/{}/services", NS)
}

fn service_item_uri(name: &str) -> String {
    format!("/api/v1/namespaces/{}/services/{}", NS, name)
}

/// Minimal ClusterIP service body. Caller may further mutate.
fn cluster_ip_body(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [{"port": 80}],
            "selector": {"app": name}
        }
    })
}

/// POST a service and return `(status, body)`.
async fn create_service(router: TestApiServer, body: &Value) -> (u16, Value) {
    send_json(router, Method::POST, &services_uri(), body).await
}

/// PUT a service and return `(status, body)`.
async fn update_service(router: TestApiServer, name: &str, body: &Value) -> (u16, Value) {
    send_json(router, Method::PUT, &service_item_uri(name), body).await
}

/// PATCH a service with a JSON merge patch and return `(status, body)`.
async fn patch_service_merge(router: TestApiServer, name: &str, patch: &Value) -> (u16, Value) {
    let (status, value) = router
        .send(
            "PATCH",
            &service_item_uri(name),
            Some("application/merge-patch+json"),
            Some(patch),
        )
        .await;
    (status.as_u16(), value)
}

// ---------------------------------------------------------------------------
// ClusterIP immutability — upstream
// `TestServiceStrategy_PrepareForUpdate` / `TestServiceClusterIPMutability`.
// ---------------------------------------------------------------------------

/// CREATE without specifying clusterIP → allocator picks one and the spec is
/// populated. clusterIPs is populated as `[clusterIP]`.
#[tokio::test]
async fn test_service_strategy_cluster_ip_allocated_on_create() {
    let (mem, router) = spawn_router();
    let body = cluster_ip_body("alloc1");

    let (status, response) = create_service(router, &body).await;
    assert!(
        (200..300).contains(&status),
        "create should succeed; got {status} body={response}"
    );

    let cip = response["spec"]["clusterIP"]
        .as_str()
        .expect("clusterIP populated");
    assert!(!cip.is_empty(), "clusterIP should be a real IP, got empty");
    assert_ne!(cip, "None");

    // clusterIPs must mirror clusterIP per K8s convention.
    let cips = response["spec"]["clusterIPs"]
        .as_array()
        .expect("clusterIPs populated");
    assert_eq!(cips, &vec![json!(cip)]);

    // Storage must contain what we returned.
    let persisted = stored(&mem, "alloc1").await.expect("persisted");
    assert_eq!(persisted["spec"]["clusterIP"], json!(cip));
}

/// Updating a service with a *different* clusterIP must be rejected
/// (upstream returns `clusterIP: field is immutable`).
#[tokio::test]
async fn test_service_strategy_cluster_ip_immutable_change() {
    let (_mem, router) = spawn_router();

    let body = cluster_ip_body("immut1");
    let (status, created) = create_service(router.clone(), &body).await;
    assert!((200..300).contains(&status));
    let original_ip = created["spec"]["clusterIP"].as_str().unwrap().to_string();

    // Attempt to change clusterIP to something else.
    let mut updated = created.clone();
    updated["spec"]["clusterIP"] = json!("10.99.99.99");
    updated["spec"]["clusterIPs"] = json!(["10.99.99.99"]);

    let (status, body) = update_service(router, "immut1", &updated).await;
    assert!(
        status >= 400,
        "expected error when changing clusterIP from {} to 10.99.99.99; got {} body={}",
        original_ip,
        status,
        body
    );
}

/// Updating a service to clear the assigned ClusterIP (set to "") must be
/// rejected — upstream `ValidateServiceUpdate` flags this as immutable.
#[tokio::test]
async fn test_service_strategy_cluster_ip_immutable_clear() {
    let (mem, router) = spawn_router();

    let body = cluster_ip_body("immut2");
    let (status, created) = create_service(router.clone(), &body).await;
    assert!((200..300).contains(&status));
    let original_ip = created["spec"]["clusterIP"].as_str().unwrap().to_string();

    let mut updated = created.clone();
    updated["spec"]["clusterIP"] = json!("");
    // clusterIPs left intact would also be a contradiction; drop it too.
    updated["spec"]["clusterIPs"] = json!([]);

    let (status, body) = update_service(router, "immut2", &updated).await;
    assert!(
        status >= 400,
        "expected error when clearing clusterIP; got {} body={}",
        status,
        body
    );
    // Even if the API returned an error, storage MUST still hold the original.
    let persisted = stored(&mem, "immut2").await.expect("persisted");
    assert_eq!(persisted["spec"]["clusterIP"], json!(original_ip));
}

/// Headless services (`clusterIP: "None"`) cannot transition to a real IP,
/// and a real-IP service cannot transition to headless.
#[tokio::test]
async fn test_service_strategy_cluster_ip_headless_immutable() {
    let (_mem, router) = spawn_router();

    // Headless service creation.
    let mut body = cluster_ip_body("headless1");
    body["spec"]["clusterIP"] = json!("None");
    let (status, created) = create_service(router.clone(), &body).await;
    assert!((200..300).contains(&status));
    assert_eq!(created["spec"]["clusterIP"], json!("None"));

    // Try to switch from headless to a concrete address.
    let mut updated = created.clone();
    updated["spec"]["clusterIP"] = json!("10.96.5.5");
    updated["spec"]["clusterIPs"] = json!(["10.96.5.5"]);
    let (status, body) = update_service(router, "headless1", &updated).await;
    assert!(
        status >= 400,
        "expected error when switching from headless to concrete IP; got {} body={}",
        status,
        body
    );
}

// ---------------------------------------------------------------------------
// Dual-stack defaulting — upstream `TestDropDisabledField` / dual-stack tests.
// ---------------------------------------------------------------------------

/// On CREATE without ipFamilyPolicy/ipFamilies the handler must default to
/// SingleStack with [IPv4].
#[tokio::test]
async fn test_service_strategy_ip_family_defaults_single_stack() {
    let (mem, router) = spawn_router();
    let body = cluster_ip_body("ipfam1");

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status), "got {status}");

    assert_eq!(
        response["spec"]["ipFamilyPolicy"], "SingleStack",
        "ipFamilyPolicy should default to SingleStack"
    );
    assert_eq!(
        response["spec"]["ipFamilies"],
        json!(["IPv4"]),
        "ipFamilies should default to [IPv4]"
    );

    let persisted = stored(&mem, "ipfam1").await.expect("persisted");
    assert_eq!(persisted["spec"]["ipFamilyPolicy"], "SingleStack");
    assert_eq!(persisted["spec"]["ipFamilies"], json!(["IPv4"]));
}

/// An explicit RequireDualStack request must round-trip through the handler
/// unchanged.
#[tokio::test]
async fn test_service_strategy_ip_family_explicit_require_dual_stack() {
    let (_mem, router) = spawn_router();
    let mut body = cluster_ip_body("ipfam2");
    body["spec"]["ipFamilyPolicy"] = json!("RequireDualStack");
    body["spec"]["ipFamilies"] = json!(["IPv4", "IPv6"]);

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status), "got {status}");
    assert_eq!(response["spec"]["ipFamilyPolicy"], "RequireDualStack");
    assert_eq!(response["spec"]["ipFamilies"], json!(["IPv4", "IPv6"]));
}

/// ExternalName services are not assigned ClusterIPs and must not get
/// ipFamilies/ipFamilyPolicy defaulted.
#[tokio::test]
async fn test_service_strategy_external_name_no_ip_family_default() {
    let (_mem, router) = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "extname1", "namespace": NS},
        "spec": {
            "type": "ExternalName",
            "externalName": "example.com"
        }
    });

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status), "got {status}");
    assert!(
        response["spec"]["ipFamilyPolicy"].is_null(),
        "ipFamilyPolicy must not be defaulted on ExternalName; got {}",
        response["spec"]["ipFamilyPolicy"]
    );
    assert!(
        response["spec"]["ipFamilies"].is_null(),
        "ipFamilies must not be defaulted on ExternalName; got {}",
        response["spec"]["ipFamilies"]
    );
}

// ---------------------------------------------------------------------------
// sessionAffinity defaulting — upstream
// `TestServiceStrategy_PrepareForCreate`.
// ---------------------------------------------------------------------------

/// Missing sessionAffinity must default to "None".
#[tokio::test]
async fn test_service_strategy_session_affinity_defaults_to_none() {
    let (mem, router) = spawn_router();
    let body = cluster_ip_body("sa1");

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status));
    assert_eq!(
        response["spec"]["sessionAffinity"], "None",
        "sessionAffinity should default to None"
    );
    let persisted = stored(&mem, "sa1").await.expect("persisted");
    assert_eq!(persisted["spec"]["sessionAffinity"], "None");
}

/// ClientIP affinity request gets timeoutSeconds defaulted to 10800 (3h)
/// when omitted — upstream `SetDefaults_Service`.
#[tokio::test]
async fn test_service_strategy_session_affinity_client_ip_defaults_timeout() {
    let (_mem, router) = spawn_router();
    let mut body = cluster_ip_body("sa2");
    body["spec"]["sessionAffinity"] = json!("ClientIP");

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status));
    assert_eq!(response["spec"]["sessionAffinity"], "ClientIP");
    assert_eq!(
        response["spec"]["sessionAffinityConfig"]["clientIP"]["timeoutSeconds"],
        10800
    );
}

/// Update flow: when sessionAffinity is "None" the stored object must NOT
/// retain a stale sessionAffinityConfig — the registry strategy clears it.
#[tokio::test]
async fn test_service_strategy_session_affinity_none_clears_config_on_update() {
    let (mem, router) = spawn_router();

    // CREATE with ClientIP affinity & timeout.
    let mut body = cluster_ip_body("sa3");
    body["spec"]["sessionAffinity"] = json!("ClientIP");
    body["spec"]["sessionAffinityConfig"] = json!({
        "clientIP": {"timeoutSeconds": 60}
    });
    let (status, created) = create_service(router.clone(), &body).await;
    assert!((200..300).contains(&status));
    assert_eq!(
        created["spec"]["sessionAffinityConfig"]["clientIP"]["timeoutSeconds"],
        60
    );

    // UPDATE: flip affinity to None but leave the (now-stale) config in place.
    let mut updated = created.clone();
    updated["spec"]["sessionAffinity"] = json!("None");
    let (status, response) = update_service(router, "sa3", &updated).await;
    assert!((200..300).contains(&status), "got {status} body={response}");
    assert_eq!(response["spec"]["sessionAffinity"], "None");
    assert!(
        response["spec"]["sessionAffinityConfig"].is_null(),
        "sessionAffinityConfig must be cleared when affinity is None; got {}",
        response["spec"]["sessionAffinityConfig"]
    );

    // Persistence must agree.
    let persisted = stored(&mem, "sa3").await.expect("persisted");
    assert!(
        persisted["spec"]["sessionAffinityConfig"].is_null(),
        "persisted sessionAffinityConfig must be cleared; got {}",
        persisted["spec"]["sessionAffinityConfig"]
    );
}

// ---------------------------------------------------------------------------
// Port defaulting — upstream
// `TestServiceStrategy_PrepareForCreate` (port branch).
// ---------------------------------------------------------------------------

/// Port without protocol → defaulted to TCP.
/// Port without targetPort → defaulted to the same integer as `port`.
#[tokio::test]
async fn test_service_strategy_ports_default_protocol_tcp_and_target_port() {
    let (mem, router) = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "ports1", "namespace": NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [{"port": 8080}],
            "selector": {"app": "ports1"}
        }
    });

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status), "got {status}");

    let port = &response["spec"]["ports"][0];
    assert_eq!(port["protocol"], "TCP", "protocol must default to TCP");
    assert_eq!(
        port["targetPort"], 8080,
        "targetPort must default to the port value"
    );

    let persisted = stored(&mem, "ports1").await.expect("persisted");
    assert_eq!(persisted["spec"]["ports"][0]["protocol"], "TCP");
    assert_eq!(persisted["spec"]["ports"][0]["targetPort"], 8080);
}

/// Explicit `targetPort: 0` must be treated as unset and defaulted to `port`.
///
/// Upstream `SetDefaults_Service` defaults targetPort when it equals
/// `intstr.FromInt32(0)` or `intstr.FromString("")`, not only when the field
/// is absent. Go's typed e2e clients (k8s.io/api ServicePort) serialize an
/// unset targetPort as the zero IntOrString `0`, so every e2e Service that
/// omits targetPort arrives here as an explicit `0`. Without this defaulting
/// the request is rejected with `targetPort: Invalid value: 0`.
/// K8s ref: pkg/apis/core/v1/defaults.go (SetDefaults_Service port loop).
#[tokio::test]
async fn test_service_strategy_explicit_zero_target_port_is_defaulted() {
    let (mem, router) = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "tpzero", "namespace": NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [
                {"name": "a", "port": 80, "targetPort": 0},
                {"name": "b", "port": 443, "targetPort": 0}
            ],
            "selector": {"app": "tpzero"}
        }
    });

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status), "got {status}: {response}");

    assert_eq!(
        response["spec"]["ports"][0]["targetPort"], 80,
        "explicit targetPort 0 on port[0] must default to its port value"
    );
    assert_eq!(
        response["spec"]["ports"][1]["targetPort"], 443,
        "explicit targetPort 0 on port[1] must default to its port value"
    );

    let persisted = stored(&mem, "tpzero").await.expect("persisted");
    assert_eq!(persisted["spec"]["ports"][0]["targetPort"], 80);
    assert_eq!(persisted["spec"]["ports"][1]["targetPort"], 443);
}

/// Explicit `targetPort: 0` on UPDATE must be defaulted to `port`, just like
/// CREATE. Upstream runs SetDefaults_Service on the update path too, so a PUT
/// carrying a zero IntOrString targetPort must not be rejected with
/// `targetPort: Invalid value: 0`.
#[tokio::test]
async fn test_service_strategy_explicit_zero_target_port_is_defaulted_on_update() {
    let (mem, router) = spawn_router();

    // Create with two ports (targetPort defaults applied on create).
    let create_body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "tpupd", "namespace": NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [
                {"name": "a", "port": 80, "targetPort": 80},
                {"name": "b", "port": 443, "targetPort": 443}
            ],
            "selector": {"app": "tpupd"}
        }
    });
    let (status, created) = create_service(router.clone(), &create_body).await;
    assert!(
        (200..300).contains(&status),
        "create got {status}: {created}"
    );

    // PUT the full object back with both targetPorts reset to an explicit 0
    // (what a Go typed client sends when targetPort is left unset on update).
    let mut updated = created.clone();
    updated["spec"]["ports"][0]["targetPort"] = json!(0);
    updated["spec"]["ports"][1]["targetPort"] = json!(0);

    let (status, response) = update_service(router, "tpupd", &updated).await;
    assert!(
        (200..300).contains(&status),
        "update got {status}: {response}"
    );

    assert_eq!(
        response["spec"]["ports"][0]["targetPort"], 80,
        "explicit targetPort 0 on update must default to its port value"
    );
    assert_eq!(
        response["spec"]["ports"][1]["targetPort"], 443,
        "explicit targetPort 0 on update must default to its port value"
    );

    let persisted = stored(&mem, "tpupd").await.expect("persisted");
    assert_eq!(persisted["spec"]["ports"][0]["targetPort"], 80);
    assert_eq!(persisted["spec"]["ports"][1]["targetPort"], 443);
}

/// A merge PATCH that rewrites `spec.ports` with an explicit `targetPort: 0`
/// must default it to `port`, not silently persist a zero targetPort. The
/// patch path delegates to generic_patch (no Service-specific defaulting), so
/// the Service handler must re-default after the merge.
#[tokio::test]
async fn test_service_strategy_explicit_zero_target_port_is_defaulted_on_patch() {
    let (mem, router) = spawn_router();

    let create_body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "tppatch", "namespace": NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [{"name": "a", "port": 80, "targetPort": 80}],
            "selector": {"app": "tppatch"}
        }
    });
    let (status, created) = create_service(router.clone(), &create_body).await;
    assert!(
        (200..300).contains(&status),
        "create got {status}: {created}"
    );

    // Merge patch replaces the ports array with a port carrying targetPort 0.
    let patch = json!({
        "spec": {"ports": [{"name": "a", "port": 80, "targetPort": 0}]}
    });
    let (status, response) = patch_service_merge(router, "tppatch", &patch).await;
    assert!(
        (200..300).contains(&status),
        "patch got {status}: {response}"
    );

    assert_eq!(
        response["spec"]["ports"][0]["targetPort"], 80,
        "explicit targetPort 0 via patch must default to its port value"
    );

    let persisted = stored(&mem, "tppatch").await.expect("persisted");
    assert_eq!(
        persisted["spec"]["ports"][0]["targetPort"], 80,
        "persisted targetPort must be defaulted, not stored as 0"
    );
}

/// Explicit UDP and named targetPort must NOT be overridden.
#[tokio::test]
async fn test_service_strategy_ports_preserve_explicit_protocol_and_target_port() {
    let (_mem, router) = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "ports2", "namespace": NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [{
                "port": 53,
                "protocol": "UDP",
                "targetPort": "dns"
            }],
            "selector": {"app": "ports2"}
        }
    });
    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status));

    let port = &response["spec"]["ports"][0];
    assert_eq!(port["protocol"], "UDP");
    assert_eq!(port["targetPort"], "dns");
}

// ---------------------------------------------------------------------------
// publishNotReadyAddresses default — upstream
// `TestServiceStrategy_PrepareForCreate`.
// ---------------------------------------------------------------------------

/// `publishNotReadyAddresses` is omitted by default. Upstream does not flip
/// it to an explicit `false` either, so the field round-trips as absent.
#[tokio::test]
async fn test_service_strategy_publish_not_ready_addresses_default_absent() {
    let (_mem, router) = spawn_router();
    let body = cluster_ip_body("pnra1");

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status));

    // Either absent or explicitly false is acceptable; explicit true would be a bug.
    let v = &response["spec"]["publishNotReadyAddresses"];
    assert!(
        v.is_null() || v == &json!(false),
        "publishNotReadyAddresses should default false/absent, got {}",
        v
    );
}

/// Explicit `publishNotReadyAddresses: true` is preserved.
#[tokio::test]
async fn test_service_strategy_publish_not_ready_addresses_preserve_true() {
    let (mem, router) = spawn_router();
    let mut body = cluster_ip_body("pnra2");
    body["spec"]["publishNotReadyAddresses"] = json!(true);

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status));
    assert_eq!(response["spec"]["publishNotReadyAddresses"], true);

    let persisted = stored(&mem, "pnra2").await.expect("persisted");
    assert_eq!(persisted["spec"]["publishNotReadyAddresses"], true);
}

// ---------------------------------------------------------------------------
// Type transitions — upstream `ValidateServiceUpdate` allows
// ClusterIP <-> NodePort <-> LoadBalancer freely and clears NodePorts/ClusterIP
// when type becomes ExternalName.
// ---------------------------------------------------------------------------

/// ClusterIP -> NodePort: NodePort must be allocated for each port on update.
#[tokio::test]
async fn test_service_strategy_type_transition_cluster_ip_to_node_port() {
    let (mem, router) = spawn_router();

    let (status, created) = create_service(router.clone(), &cluster_ip_body("tt1")).await;
    assert!((200..300).contains(&status));
    assert_eq!(created["spec"]["type"], "ClusterIP");

    let mut updated = created.clone();
    updated["spec"]["type"] = json!("NodePort");
    let (status, response) = update_service(router, "tt1", &updated).await;
    assert!((200..300).contains(&status), "got {status} body={response}");
    assert_eq!(response["spec"]["type"], "NodePort");

    let np = response["spec"]["ports"][0]["nodePort"]
        .as_u64()
        .expect("NodePort allocated");
    assert!(
        (30000..=32767).contains(&np),
        "NodePort {np} should be in 30000-32767 service-node-port range"
    );

    let persisted = stored(&mem, "tt1").await.expect("persisted");
    assert_eq!(persisted["spec"]["type"], "NodePort");
    assert_eq!(persisted["spec"]["ports"][0]["nodePort"], np);
}

/// ClusterIP -> LoadBalancer: NodePort still allocated (load balancer back-end).
#[tokio::test]
async fn test_service_strategy_type_transition_cluster_ip_to_load_balancer() {
    let (_mem, router) = spawn_router();

    let (status, created) = create_service(router.clone(), &cluster_ip_body("tt2")).await;
    assert!((200..300).contains(&status));

    let mut updated = created.clone();
    updated["spec"]["type"] = json!("LoadBalancer");
    let (status, response) = update_service(router, "tt2", &updated).await;
    assert!((200..300).contains(&status), "got {status} body={response}");
    assert_eq!(response["spec"]["type"], "LoadBalancer");

    let np = response["spec"]["ports"][0]["nodePort"]
        .as_u64()
        .expect("NodePort allocated for LoadBalancer");
    assert!((30000..=32767).contains(&np));
}

/// Transition to ExternalName clears the ClusterIP and NodePorts.
#[tokio::test]
async fn test_service_strategy_type_transition_to_external_name_clears_cluster_ip() {
    let (mem, router) = spawn_router();

    // Create as NodePort so we have a NodePort to clear.
    let mut body = cluster_ip_body("tt3");
    body["spec"]["type"] = json!("NodePort");
    let (status, created) = create_service(router.clone(), &body).await;
    assert!((200..300).contains(&status));
    assert!(!created["spec"]["clusterIP"].as_str().unwrap().is_empty());
    assert!(created["spec"]["ports"][0]["nodePort"].is_number());

    let mut updated = created.clone();
    updated["spec"]["type"] = json!("ExternalName");
    updated["spec"]["externalName"] = json!("svc.example.com");

    let (status, response) = update_service(router, "tt3", &updated).await;
    assert!((200..300).contains(&status), "got {status} body={response}");
    assert_eq!(response["spec"]["type"], "ExternalName");
    // ClusterIP must be cleared (handler stores ""). clusterIPs cleared.
    let cip = response["spec"]["clusterIP"].as_str().unwrap_or("");
    assert_eq!(
        cip, "",
        "clusterIP should be cleared on ExternalName transition"
    );
    assert!(
        response["spec"]["clusterIPs"].is_null(),
        "clusterIPs should be cleared on ExternalName transition; got {}",
        response["spec"]["clusterIPs"]
    );
    // NodePort cleared on all ports.
    assert!(
        response["spec"]["ports"][0]["nodePort"].is_null(),
        "nodePort should be cleared; got {}",
        response["spec"]["ports"][0]["nodePort"]
    );

    let persisted = stored(&mem, "tt3").await.expect("persisted");
    let cip = persisted["spec"]["clusterIP"].as_str().unwrap_or("");
    assert_eq!(cip, "");
}

/// Type defaults to ClusterIP when omitted on create.
#[tokio::test]
async fn test_service_strategy_type_defaults_to_cluster_ip() {
    let (_mem, router) = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "deftype1", "namespace": NS},
        "spec": {
            "ports": [{"port": 80}],
            "selector": {"app": "deftype1"}
        }
    });

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status), "got {status}");
    assert_eq!(response["spec"]["type"], "ClusterIP");
}

/// internalTrafficPolicy defaults to "Cluster" for ClusterIP services.
#[tokio::test]
async fn test_service_strategy_internal_traffic_policy_defaults_cluster() {
    let (_mem, router) = spawn_router();
    let body = cluster_ip_body("itp1");

    let (status, response) = create_service(router, &body).await;
    assert!((200..300).contains(&status));
    assert_eq!(response["spec"]["internalTrafficPolicy"], "Cluster");
}
