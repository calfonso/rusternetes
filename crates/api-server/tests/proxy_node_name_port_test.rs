//! Integration regression: node-proxy URLs of the form
//! `/api/v1/nodes/<name>:<port>/proxy/<path>` must route to the actual
//! node `<name>`, not to a literal node id of `<name>:<port>`.
//!
//! Symptom we are reproducing: PR #182 conformance dump contained
//!
//!     Unable to retrieve kubelet pods for node node-1:
//!         nodes "node-1:10250" not found
//!
//! The upstream e2e framework (and `kubectl proxy`) constructs node-proxy
//! URLs by joining the node name with the kubelet port — see
//! `staging/src/k8s.io/apimachinery/pkg/util/net.SplitSchemeNamePort` and
//! `pkg/registry/core/node/strategy.go::ResourceLocation` at
//! release-1.35. The api-server's node REST handler is expected to
//! recognise the `<name>:<port>` form, look up the storage entry by name
//! alone, and use the embedded port as the override for the kubelet
//! endpoint.
//!
//! Strategy: spawn a tokio TcpListener on a random local port that
//! returns a known body, seed a Node whose InternalIP is `127.0.0.1` and
//! whose `daemonEndpoints.kubeletEndpoint.port` is *not* the listener
//! port (we leave it as the upstream default `10250`), then drive
//!
//!     GET /api/v1/nodes/node-1:<listener_port>/proxy/pods
//!
//! through `build_router`. The handler must:
//!   1. Split the id into ("", "node-1", "<listener_port>").
//!   2. Look up the Node by `node-1`, not `node-1:<listener_port>`.
//!   3. Use `<listener_port>` from the URL — not the node-advertised
//!      `10250` — to dial the kubelet.
//!
//! All three behaviours are independently asserted via the response body
//! pass-through and the absence of a 404 on the node lookup.

use rusternetes_common::resources::{
    DaemonEndpoint, Node, NodeAddress, NodeDaemonEndpoints, NodeStatus,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Spawn an HTTP-1.1 backend on a random `127.0.0.1` port that returns
/// `200 OK` with a deterministic body for any request. The handler exits
/// after `max_requests` connections.
async fn spawn_kubelet_backend(
    body: &'static str,
    max_requests: usize,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        for _ in 0..max_requests {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(response.as_bytes()).await;
                let _ = sock.flush().await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (port, handle)
}

/// Build a Node seeded with an InternalIP of `127.0.0.1` and an
/// (intentionally wrong) advertised kubelet port. The intent of the
/// wrong port is to prove the URL-embedded port wins when supplied —
/// matching upstream `ResourceLocation` semantics where the requested
/// port overrides the node-advertised port unless equal.
fn node_with_addresses(name: &str, advertised_port: i32) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name),
        spec: None,
        status: Some(NodeStatus {
            addresses: Some(vec![NodeAddress {
                address_type: "InternalIP".to_string(),
                address: "127.0.0.1".to_string(),
            }]),
            daemon_endpoints: Some(NodeDaemonEndpoints {
                kubelet_endpoint: Some(DaemonEndpoint {
                    port: advertised_port,
                }),
            }),
            ..NodeStatus::default()
        }),
    }
}

/// GET helper — drive a request through the api-server router and
/// return `(status, body)`.
async fn proxy_get(router: &TestApiServer, uri: &str) -> (u16, String) {
    let (status, _headers, body_bytes, _) = router.send_full("GET", uri, None, None, None).await;
    let body = String::from_utf8_lossy(&body_bytes).to_string();
    (status.as_u16(), body)
}

/// Regression: `/api/v1/nodes/<name>:<port>/proxy/<path>` must use the
/// embedded `<port>` to dial the kubelet, look up the Node by `<name>`,
/// and forward the backend response verbatim.
///
/// Reproduces the conformance failure: when the URL was treated as
/// having a literal node name of `node-1:10250`, storage returned
/// `NotFound` and the proxy never ran.
#[tokio::test]
async fn proxy_node_with_port_in_name_routes_correctly() {
    // 1. Spawn a kubelet-shaped backend on a random port. The Node we
    //    register advertises a DIFFERENT port so a passing test proves
    //    the URL-embedded port took precedence.
    let (kubelet_port, _handle) = spawn_kubelet_backend("pods-list-from-kubelet", 4).await;
    assert_ne!(kubelet_port, 10250, "RAND must not collide with default");

    // 2. Seed storage with a Node whose InternalIP is the loopback and
    //    whose advertised kubelet port is intentionally wrong.
    let api = TestApiServer::new();
    let node = node_with_addresses("node-1", 10250);
    api.storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .expect("create node");

    // 3. Drive the request through the router using the `<name>:<port>`
    //    form the upstream e2e framework constructs.
    let uri = format!("/api/v1/nodes/node-1:{}/proxy/pods", kubelet_port);
    let (status, body) = proxy_get(&api, &uri).await;

    // 4. The handler must split the id, look up by name, and dial the
    //    URL-supplied port. Both conditions must hold:
    //     - status 200 (not 404 from storage; not 502 from a wrong port)
    //     - body matches what the backend returned
    assert_eq!(
        status, 200,
        "node-proxy with `<name>:<port>` id must reach the kubelet (got body: {})",
        body
    );
    assert!(
        body.contains("pods-list-from-kubelet"),
        "expected backend body verbatim, got: {}",
        body
    );
}

/// Regression: when only `<name>` (no port) is supplied, the handler
/// must fall back to `status.daemonEndpoints.kubeletEndpoint.port` —
/// the existing behaviour. This pins down that the new parsing code
/// path doesn't regress the default lookup.
#[tokio::test]
async fn proxy_node_without_port_uses_advertised_port() {
    let (kubelet_port, _handle) = spawn_kubelet_backend("default-port-ok", 2).await;

    let api = TestApiServer::new();
    // Node advertises the listener's port. URL omits the port, so the
    // handler must read it off `daemonEndpoints.kubeletEndpoint.port`.
    let node = node_with_addresses("node-2", kubelet_port as i32);
    api.storage
        .create(&build_key("nodes", None, "node-2"), &node)
        .await
        .expect("create node");

    let (status, body) = proxy_get(&api, "/api/v1/nodes/node-2/proxy/pods").await;
    assert_eq!(status, 200, "plain `<name>` form must still work");
    assert!(
        body.contains("default-port-ok"),
        "expected backend body, got: {}",
        body
    );
}
