//! Scoped mirror of the Kubernetes v1.35 conformance suite for the
//! [sig-network] `/proxy` subresource — api-server half.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/network/proxy.go
//!
//! Companion: the kube-proxy-side iptables-surface mirror lives at
//! `crates/kube-proxy/tests/conformance_network_services_proxy.rs`. That
//! file asserts the iptables rules built from the same storage shape.
//! This file drives the `/proxy` HTTP path through a real axum router
//! against an in-process backend so the upstream
//! `proxy.go:432–503` response-code matrix can be reproduced end-to-end.
//!
//! Strategy: spawn a tokio TcpListener that returns a known response per
//! request path (200 OK, 404, 503, 301-with-Location). Register a Pod
//! whose `status.podIP` points at `127.0.0.1`, a Service with one port,
//! and an EndpointSlice tying the Service to the same address. Build the
//! api-server router via `build_router`, send the request through
//! `tower::ServiceExt::oneshot`, and assert the proxy forwards the
//! backend response verbatim — through both the pod-proxy URL and the
//! service-proxy URL.

use rusternetes_common::resources::endpointslice::EndpointPort as ESEndpointPort;
use rusternetes_common::resources::{
    Container, ContainerPort, Endpoint, EndpointConditions, EndpointSlice, IntOrString, Pod, PodIP,
    PodSpec, PodStatus, Service, ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// --------------------------------------------------------------------------
// Test fixtures
// --------------------------------------------------------------------------

/// Build a Pod listening on `127.0.0.1:<port>`. The api-server proxy
/// handler reads `status.podIPs` first, then falls back to `status.podIP`,
/// then resolves the destination port from the URL or the first
/// `containerPort` if absent. We populate all three so the upstream
/// resolution algorithm matches verbatim.
fn pod_listening_on(namespace: &str, name: &str, port: u16) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                image: "nginx:alpine".to_string(),
                ports: Some(vec![ContainerPort {
                    container_port: port,
                    name: Some("http".to_string()),
                    protocol: Some("TCP".to_string()),
                    host_port: None,
                    host_ip: None,
                }]),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: Some("127.0.0.1".to_string()),
            pod_i_ps: Some(vec![PodIP {
                ip: "127.0.0.1".to_string(),
            }]),
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            conditions: None,
            ..Default::default()
        }),
    }
}

/// Build a minimal ClusterIP Service with one TCP port → 127.0.0.1:port.
fn service_to_pod_port(namespace: &str, name: &str, svc_port: u16, target_port: u16) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: ServiceSpec {
            selector: Some(HashMap::new()),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: svc_port,
                target_port: Some(IntOrString::Int(target_port as i32)),
                protocol: Some("TCP".to_string()),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: Some("10.96.0.230".to_string()),
            ..ServiceSpec::default()
        },
        status: None,
    }
}

/// Build an EndpointSlice that links the Service named `service` to the
/// given pod IP + port. The api-server's service-proxy handler keys off
/// the `kubernetes.io/service-name` label exactly as the upstream
/// EndpointSlice mirroring controller writes it.
fn endpoint_slice_for(namespace: &str, service: &str, addr: &str, port: u16) -> EndpointSlice {
    let mut labels = HashMap::new();
    labels.insert(
        "kubernetes.io/service-name".to_string(),
        service.to_string(),
    );
    let mut es = EndpointSlice::new(format!("{}-abc12", service), "IPv4");
    es.metadata.namespace = Some(namespace.to_string());
    es.metadata.labels = Some(labels);
    es.endpoints = vec![Endpoint {
        addresses: vec![addr.to_string()],
        conditions: Some(EndpointConditions {
            ready: Some(true),
            serving: Some(true),
            terminating: Some(false),
        }),
        hostname: None,
        target_ref: None,
        node_name: None,
        zone: None,
        hints: None,
        deprecated_topology: None,
    }];
    es.ports = vec![ESEndpointPort {
        name: Some("http".to_string()),
        port: Some(port as i32),
        protocol: Some("TCP".to_string()),
        app_protocol: None,
    }];
    es
}

/// Spawn an HTTP-1.1 backend on `127.0.0.1` that returns a deterministic
/// response per request-path. Returns the bound port + a join handle. The
/// backend accepts up to `max_requests` connections then exits.
///
/// Response matrix (mirrors proxy.go:432-503):
///   GET /              → 200 OK with body "pod-and-service-proxy-ok"
///   GET /notfound      → 404 Not Found with body "missing"
///   GET /unavailable   → 503 Service Unavailable with body "down"
///   GET /redirect      → 301 Moved Permanently with Location header
async fn spawn_response_matrix_backend(max_requests: usize) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        for _ in 0..max_requests {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = match sock.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };
                let request_line = String::from_utf8_lossy(&buf[..n])
                    .lines()
                    .next()
                    .unwrap_or("")
                    .to_string();
                // Parse "GET /<path> HTTP/1.1"
                let path = request_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("/")
                    .to_string();
                let response: &[u8] = match path.as_str() {
                    "/notfound" => b"HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 7\r\nConnection: close\r\n\r\nmissing",
                    "/unavailable" => b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndown",
                    "/redirect" => b"HTTP/1.1 301 Moved Permanently\r\nLocation: /elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    _ => b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 24\r\nConnection: close\r\n\r\npod-and-service-proxy-ok",
                };
                let _ = sock.write_all(response).await;
                let _ = sock.flush().await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (port, handle)
}

/// Drive a GET request through the api-server router and return
/// (status, body, location-header).
async fn proxy_get(router: &TestApiServer, uri: &str) -> (u16, String, Option<String>) {
    let (status, headers, body_bytes, _) = router.send_full("GET", uri, None, None, None).await;
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let body = String::from_utf8_lossy(&body_bytes).to_string();
    (status.as_u16(), body, location)
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

/// [sig-network] Proxy version v1 [Conformance] — A set of valid responses
/// are returned for both pod and service Proxy
///
/// Upstream: `test/e2e/network/proxy.go:432–503`
///
/// Walks the upstream response matrix (200, 404, 503, 301) through both
/// the pod-proxy URL (`/api/v1/namespaces/{ns}/pods/{pod}/proxy/{path}`)
/// and the service-proxy URL
/// (`/api/v1/namespaces/{ns}/services/{svc}/proxy/{path}`). Each request
/// must reach the same backend (the pod's IP:containerPort) and the
/// upstream response code + body must be forwarded verbatim.
///
/// This is the api-server half of the conformance mirror; the
/// kube-proxy-side companion (iptables-surface invariants) lives in
/// `crates/kube-proxy/tests/conformance_network_services_proxy.rs`.
#[tokio::test]
async fn proxy_valid_responses_for_pod_and_service() {
    // 1. Spawn the in-process backend.
    //    4 paths × 2 routes (pod + service) = 8 requests total.
    let (backend_port, _backend_handle) = spawn_response_matrix_backend(16).await;

    // 2. Seed MemoryStorage with the Pod / Service / EndpointSlice shape
    //    the api-server proxy handlers query.
    let api = TestApiServer::new();
    let pod = pod_listening_on("default", "proxy-pod", backend_port);
    api.storage
        .create(&build_key("pods", Some("default"), "proxy-pod"), &pod)
        .await
        .expect("create pod");

    let svc = service_to_pod_port("default", "proxy-svc", 80, backend_port);
    api.storage
        .create(&build_key("services", Some("default"), "proxy-svc"), &svc)
        .await
        .expect("create service");

    let slice = endpoint_slice_for("default", "proxy-svc", "127.0.0.1", backend_port);
    api.storage
        .create(
            &build_key("endpointslices", Some("default"), &slice.metadata.name),
            &slice,
        )
        .await
        .expect("create endpointslice");

    // 3. Walk the response matrix through the Pod-proxy URL.
    //    Format: /api/v1/namespaces/{ns}/pods/{name}/proxy/{path}
    //
    //    The handler resolves the pod IP from `status.podIPs[0]` and the
    //    port from the first containerPort (we registered both for
    //    backend_port above).
    let pod_base = "/api/v1/namespaces/default/pods/proxy-pod/proxy";

    let (status, body, _) = proxy_get(&api, &format!("{}/", pod_base)).await;
    assert_eq!(status, 200, "pod proxy GET / should return 200");
    assert!(
        body.contains("pod-and-service-proxy-ok"),
        "pod proxy body mismatch: {}",
        body
    );

    let (status, body, _) = proxy_get(&api, &format!("{}/notfound", pod_base)).await;
    assert_eq!(status, 404, "pod proxy /notfound should return 404");
    assert_eq!(body, "missing", "pod proxy 404 body mismatch");

    let (status, body, _) = proxy_get(&api, &format!("{}/unavailable", pod_base)).await;
    assert_eq!(status, 503, "pod proxy /unavailable should return 503");
    assert_eq!(body, "down", "pod proxy 503 body mismatch");

    let (status, _body, location) = proxy_get(&api, &format!("{}/redirect", pod_base)).await;
    assert_eq!(status, 301, "pod proxy /redirect should return 301");
    // The redirect-following policy is `none` (proxy.rs:577) so the 301
    // and Location header must be forwarded verbatim — the e2e client
    // (not the proxy) decides whether to follow.
    assert_eq!(
        location.as_deref(),
        Some("/elsewhere"),
        "pod proxy must forward Location header verbatim"
    );

    // 4. Walk the same matrix through the Service-proxy URL.
    //    Format: /api/v1/namespaces/{ns}/services/{name}/proxy/{path}
    //
    //    The handler resolves the backend IP via the EndpointSlice we
    //    seeded above (proxy.rs:295-341) and the port from the
    //    slice's `ports[0]` — both pointing at 127.0.0.1:backend_port.
    let svc_base = "/api/v1/namespaces/default/services/proxy-svc/proxy";

    let (status, body, _) = proxy_get(&api, &format!("{}/", svc_base)).await;
    assert_eq!(status, 200, "service proxy GET / should return 200");
    assert!(
        body.contains("pod-and-service-proxy-ok"),
        "service proxy body mismatch: {}",
        body
    );

    let (status, body, _) = proxy_get(&api, &format!("{}/notfound", svc_base)).await;
    assert_eq!(status, 404, "service proxy /notfound should return 404");
    assert_eq!(body, "missing", "service proxy 404 body mismatch");

    let (status, body, _) = proxy_get(&api, &format!("{}/unavailable", svc_base)).await;
    assert_eq!(status, 503, "service proxy /unavailable should return 503");
    assert_eq!(body, "down", "service proxy 503 body mismatch");

    let (status, _body, location) = proxy_get(&api, &format!("{}/redirect", svc_base)).await;
    assert_eq!(status, 301, "service proxy /redirect should return 301");
    assert_eq!(
        location.as_deref(),
        Some("/elsewhere"),
        "service proxy must forward Location header verbatim"
    );
}
