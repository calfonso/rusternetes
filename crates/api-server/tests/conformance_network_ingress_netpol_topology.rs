//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-network] Ingress + NetworkPolicy + Topology-aware hints.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/network/
//! (`ingress.go`, `ingressclass.go`, `netpol/network_policy_api.go`,
//! `netpol/network_policy.go`, `topology_hints.go`).
//!
//! See docs/conformance/network-ingress-netpol-topology.md for the
//! test-by-test status table.
//!
//! This crate owns the REST surface for `networking.k8s.io/v1` Ingress,
//! IngressClass and NetworkPolicy plus the `discovery.k8s.io/v1`
//! EndpointSlice resource that carries `hints.forZones` used by
//! topology-aware routing. The upstream Conformance tests in scope all
//! exercise the API contract — CRUD round-trip, list/watch/patch
//! semantics, schema preservation of nested rule structures, and
//! cross-namespace selectors — rather than dataplane behaviour, so the
//! axum router on top of `MemoryStorage` is the right test depth.
//!
//! Harness: spawn the real Axum router on top of `StorageBackend::Memory`
//! and drive it through `tower::ServiceExt::oneshot`. This is the same
//! handler stack the production api-server uses for kubectl traffic.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// HTTP harness — thin `(u16, Value)` shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_state() -> TestApiServer {
    TestApiServer::new()
}

async fn post_json(state: TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = state.post(uri, body).await;
    (status.as_u16(), value)
}

async fn get_json(state: TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = state.get(uri).await;
    (status.as_u16(), value)
}

async fn put_json(state: TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = state.put(uri, body).await;
    (status.as_u16(), value)
}

async fn patch_merge(state: TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = state.patch(uri, body).await;
    (status.as_u16(), value)
}

async fn delete(state: TestApiServer, uri: &str) -> u16 {
    state.delete(uri).await.0.as_u16()
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

/// Minimal Ingress body equivalent to the one created by
/// `test/e2e/network/ingress.go::ConformanceIt("should support creating
/// Ingress API operations")`. Single rule, single path, single Service
/// backend on port 80.
fn ingress_body(name: &str, namespace: &str, class: Option<&str>) -> Value {
    let mut spec = json!({
        "rules": [{
            "host": "ingress.example.com",
            "http": {
                "paths": [{
                    "path": "/",
                    "pathType": "Prefix",
                    "backend": {
                        "service": {
                            "name": "backend-svc",
                            "port": { "number": 80 }
                        }
                    }
                }]
            }
        }]
    });
    if let Some(c) = class {
        spec.as_object_mut()
            .unwrap()
            .insert("ingressClassName".to_string(), json!(c));
    }
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": { "name": name, "namespace": namespace },
        "spec": spec,
    })
}

fn ingressclass_body(name: &str, controller: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IngressClass",
        "metadata": { "name": name },
        "spec": { "controller": controller },
    })
}

fn netpol_body(name: &str, namespace: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": { "name": name, "namespace": namespace },
        "spec": spec,
    })
}

fn endpointslice_with_hints(name: &str, namespace: &str, zone: &str) -> Value {
    json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": { "kubernetes.io/service-name": "topo-svc" }
        },
        "addressType": "IPv4",
        "endpoints": [{
            "addresses": ["10.0.1.5"],
            "nodeName": "node-a",
            "zone": zone,
            "hints": { "forZones": [{ "name": zone }] }
        }],
        "ports": [{ "name": "http", "protocol": "TCP", "port": 80 }],
    })
}

// ---------------------------------------------------------------------------
// Ingress API operations
// ---------------------------------------------------------------------------

/// [sig-network] Ingress API should support creating Ingress API operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/ingress.go:54
/// Sonobuoy (Round 160, 2026-04-26): PASS — Ingress is not in any of the
/// nine failure buckets enumerated in docs/CONFORMANCE.md:40-53.
#[tokio::test]
async fn ingress_api_supports_create_get_list_round_trip() {
    let state = spawn_state();
    let ns = "ingress-conformance";
    let body = ingress_body("ing-1", ns, Some("nginx"));

    let (status, created) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
        &body,
    )
    .await;
    assert_eq!(status, 201, "POST must return 201 Created: {created}");
    assert_eq!(created["metadata"]["name"], "ing-1");
    assert!(
        !created["metadata"]["uid"].as_str().unwrap_or("").is_empty(),
        "uid must be assigned: {created}"
    );
    assert_eq!(created["spec"]["ingressClassName"], "nginx");

    let (gs, got) = get_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses/ing-1"),
    )
    .await;
    assert_eq!(gs, 200, "GET must return 200: {got}");
    assert_eq!(got["spec"]["rules"][0]["host"], "ingress.example.com");

    let (ls, list) = get_json(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
    )
    .await;
    assert_eq!(ls, 200, "LIST must return 200: {list}");
    assert_eq!(list["kind"], "IngressList");
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
}

/// [sig-network] Ingress API should support update (PUT) and partial update (PATCH) [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/ingress.go:54 (verbs:
///   update + patch in the same ConformanceIt block)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn ingress_api_supports_put_and_patch() {
    let state = spawn_state();
    let ns = "ingress-update";
    let body = ingress_body("ing-up", ns, Some("nginx"));
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
        &body,
    )
    .await;
    assert_eq!(cs, 201);

    // PUT — swap class name and add a second path.
    let mut updated = body.clone();
    updated["spec"]["ingressClassName"] = json!("haproxy");
    updated["spec"]["rules"][0]["http"]["paths"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "path": "/api",
            "pathType": "Prefix",
            "backend": { "service": { "name": "api-svc", "port": { "number": 8080 } } }
        }));
    let (us, after_put) = put_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses/ing-up"),
        &updated,
    )
    .await;
    assert_eq!(us, 200, "PUT must return 200: {after_put}");
    assert_eq!(after_put["spec"]["ingressClassName"], "haproxy");
    assert_eq!(
        after_put["spec"]["rules"][0]["http"]["paths"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // PATCH — strategic-style merge to add an annotation.
    let patch = json!({
        "metadata": { "annotations": { "ingress.kubernetes.io/rewrite-target": "/" } }
    });
    let (ps, after_patch) = patch_merge(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses/ing-up"),
        &patch,
    )
    .await;
    assert_eq!(ps, 200, "PATCH must return 200: {after_patch}");
    assert_eq!(
        after_patch["metadata"]["annotations"]["ingress.kubernetes.io/rewrite-target"],
        "/"
    );
}

/// [sig-network] Ingress API should support delete and deletecollection [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/ingress.go:54 (verbs:
///   delete + deletecollection)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn ingress_api_supports_delete_and_deletecollection() {
    let state = spawn_state();
    let ns = "ingress-del";
    let a = ingress_body("ing-a", ns, None);
    let b = ingress_body("ing-b", ns, None);
    post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
        &a,
    )
    .await;
    post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
        &b,
    )
    .await;

    let ds = delete(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses/ing-a"),
    )
    .await;
    assert!(matches!(ds, 200 | 202), "DELETE must be 200/202, got {ds}");
    let (gs, _) = get_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses/ing-a"),
    )
    .await;
    assert_eq!(gs, 404, "deleted ingress must 404");

    let dcs = delete(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
    )
    .await;
    assert_eq!(dcs, 200, "deletecollection must return 200");
    let (_, list) = get_json(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
    )
    .await;
    assert_eq!(list["items"].as_array().unwrap().len(), 0);
}

/// [sig-network] Ingress API should support the /status subresource [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/ingress.go:54 (status
///   subresource verbs: get + update + patch)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn ingress_status_subresource_round_trip() {
    let state = spawn_state();
    let ns = "ingress-status";
    let body = ingress_body("ing-status", ns, None);
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
        &body,
    )
    .await;
    assert_eq!(cs, 201);

    let status_body = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": { "name": "ing-status", "namespace": ns },
        "spec": body["spec"].clone(),
        "status": {
            "loadBalancer": {
                "ingress": [{
                    "ip": "203.0.113.7",
                    "ports": [{ "port": 443, "protocol": "TCP" }]
                }]
            }
        }
    });
    let (us, after) = put_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses/ing-status/status"),
        &status_body,
    )
    .await;
    assert_eq!(us, 200, "status PUT must return 200: {after}");
    assert_eq!(
        after["status"]["loadBalancer"]["ingress"][0]["ip"], "203.0.113.7",
        "status write must persist load-balancer IP"
    );

    let (gs, got) = get_json(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses/ing-status/status"),
    )
    .await;
    assert_eq!(gs, 200, "status GET must return 200");
    assert_eq!(
        got["status"]["loadBalancer"]["ingress"][0]["ip"],
        "203.0.113.7"
    );
}

/// [sig-network] Ingress backend resolution preserves Service + port references
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/ingress.go:54
///   (the ConformanceIt block round-trips both named-port and numeric-port
///    Service backends as part of create/get)
/// Sonobuoy (Round 160): PASS
///
/// Mirror rationale: the server-side serde round-trip must keep both
/// `port.number` and `port.name` variants of `ServiceBackendPort` —
/// dropping either was a real bug in early ingress handler patches.
#[tokio::test]
async fn ingress_backend_resolution_preserves_named_and_numeric_ports() {
    let state = spawn_state();
    let ns = "ingress-backend";
    let body = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": { "name": "ing-backends", "namespace": ns },
        "spec": {
            "defaultBackend": {
                "service": { "name": "default-svc", "port": { "number": 8080 } }
            },
            "rules": [{
                "host": "named.example.com",
                "http": {
                    "paths": [
                        {
                            "path": "/exact",
                            "pathType": "Exact",
                            "backend": { "service": { "name": "exact-svc", "port": { "name": "http" } } }
                        },
                        {
                            "path": "/prefix",
                            "pathType": "Prefix",
                            "backend": { "service": { "name": "prefix-svc", "port": { "number": 9090 } } }
                        }
                    ]
                }
            }]
        }
    });
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses"),
        &body,
    )
    .await;
    assert_eq!(cs, 201);

    let (_, got) = get_json(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses/ing-backends"),
    )
    .await;

    assert_eq!(
        got["spec"]["defaultBackend"]["service"]["port"]["number"],
        8080
    );
    let paths = got["spec"]["rules"][0]["http"]["paths"].as_array().unwrap();
    assert_eq!(paths[0]["backend"]["service"]["port"]["name"], "http");
    assert_eq!(paths[0]["pathType"], "Exact");
    assert_eq!(paths[1]["backend"]["service"]["port"]["number"], 9090);
    assert_eq!(paths[1]["pathType"], "Prefix");
}

// ---------------------------------------------------------------------------
// IngressClass API operations
// ---------------------------------------------------------------------------

/// [sig-network] IngressClass API should support creating IngressClass API operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/ingressclass.go:198
/// Sonobuoy (Round 160): PASS — IngressClass not in failure buckets.
///
/// IngressClass is cluster-scoped (no namespace segment in the URL).
#[tokio::test]
async fn ingressclass_api_supports_create_get_list_delete() {
    let state = spawn_state();
    let body = ingressclass_body("nginx-class", "k8s.io/ingress-nginx");

    let (cs, created) = post_json(
        state.clone(),
        "/apis/networking.k8s.io/v1/ingressclasses",
        &body,
    )
    .await;
    assert_eq!(cs, 201, "POST IngressClass must return 201: {created}");
    assert_eq!(created["spec"]["controller"], "k8s.io/ingress-nginx");

    let (gs, got) = get_json(
        state.clone(),
        "/apis/networking.k8s.io/v1/ingressclasses/nginx-class",
    )
    .await;
    assert_eq!(gs, 200, "GET IngressClass must return 200: {got}");

    let (ls, list) = get_json(state.clone(), "/apis/networking.k8s.io/v1/ingressclasses").await;
    assert_eq!(ls, 200);
    assert_eq!(list["kind"], "IngressClassList");
    assert!(
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["metadata"]["name"] == "nginx-class"),
        "IngressClass list must include the created class"
    );

    let ds = delete(
        state.clone(),
        "/apis/networking.k8s.io/v1/ingressclasses/nginx-class",
    )
    .await;
    assert!(
        matches!(ds, 200 | 202),
        "DELETE IngressClass must succeed, got {ds}"
    );
    let (after, _) = get_json(
        state,
        "/apis/networking.k8s.io/v1/ingressclasses/nginx-class",
    )
    .await;
    assert_eq!(after, 404, "deleted IngressClass must 404");
}

/// [sig-network] IngressClass with parameters reference is preserved on read
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/ingressclass.go:167
///   (`should allow IngressClass to have Namespace-scoped parameters`)
/// Sonobuoy (Round 160): PASS — the spec-level `parameters` field is part
///   of the ConformanceIt block at :198 even though the dedicated scenario
///   at :167 is not itself flagged [Conformance].
#[tokio::test]
async fn ingressclass_with_namespace_scoped_parameters_round_trip() {
    let state = spawn_state();
    let body = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IngressClass",
        "metadata": { "name": "external-lb" },
        "spec": {
            "controller": "example.com/ingress-controller",
            "parameters": {
                "apiGroup": "k8s.example.com",
                "kind": "IngressParameters",
                "name": "external-lb",
                "namespace": "ingress-system",
                "scope": "Namespace"
            }
        }
    });
    let (cs, _) = post_json(
        state.clone(),
        "/apis/networking.k8s.io/v1/ingressclasses",
        &body,
    )
    .await;
    assert_eq!(cs, 201);

    let (gs, got) = get_json(
        state,
        "/apis/networking.k8s.io/v1/ingressclasses/external-lb",
    )
    .await;
    assert_eq!(gs, 200);
    assert_eq!(got["spec"]["parameters"]["scope"], "Namespace");
    assert_eq!(got["spec"]["parameters"]["namespace"], "ingress-system");
    assert_eq!(got["spec"]["parameters"]["kind"], "IngressParameters");
}

// ---------------------------------------------------------------------------
// NetworkPolicy API operations
// ---------------------------------------------------------------------------

/// [sig-network] NetworkPolicy API should support creating NetworkPolicy API operations
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/netpol/network_policy_api.go:47
/// Sonobuoy (Round 160): PASS — NetworkPolicy not in failure buckets.
///
/// Note: the upstream test is not [Conformance]-tagged but the file
/// header documents `Testname: NetworkPolicies API` (Release: v1.20) and
/// it is part of the canonical sig-network suite Sonobuoy runs.
#[tokio::test]
async fn networkpolicy_api_supports_create_get_list_delete() {
    let state = spawn_state();
    let ns = "netpol-api";
    let spec = json!({
        "podSelector": { "matchLabels": { "app": "server" } },
        "policyTypes": ["Ingress"],
        "ingress": [{
            "from": [{ "podSelector": { "matchLabels": { "role": "client" } } }],
            "ports": [{ "protocol": "TCP", "port": 8080 }]
        }]
    });
    let body = netpol_body("np-1", ns, spec);

    let (cs, created) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies"),
        &body,
    )
    .await;
    assert_eq!(cs, 201, "POST NetworkPolicy must return 201: {created}");
    assert_eq!(
        created["spec"]["podSelector"]["matchLabels"]["app"],
        "server"
    );

    let (gs, got) = get_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies/np-1"),
    )
    .await;
    assert_eq!(gs, 200, "GET NetworkPolicy must return 200: {got}");

    let (ls, list) = get_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies"),
    )
    .await;
    assert_eq!(ls, 200);
    assert_eq!(list["kind"], "NetworkPolicyList");
    assert_eq!(list["items"].as_array().unwrap().len(), 1);

    let ds = delete(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies/np-1"),
    )
    .await;
    assert!(
        matches!(ds, 200 | 202),
        "DELETE NetworkPolicy must succeed, got {ds}"
    );
}

/// [sig-network] NetworkPolicy API with endport field
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/netpol/network_policy_api.go:180
///   (`should support creating NetworkPolicy API with endport field`)
/// Sonobuoy (Round 160): PASS
///
/// Mirror rationale: the `endPort` field gates a whole feature gate
/// (NetworkPolicyEndPort) and our serde struct uses `rename = "endPort"`
/// — a typo in that rename would silently drop the field on read.
#[tokio::test]
async fn networkpolicy_endport_field_is_preserved() {
    let state = spawn_state();
    let ns = "netpol-endport";
    let spec = json!({
        "podSelector": { "matchLabels": { "app": "db" } },
        "policyTypes": ["Ingress"],
        "ingress": [{
            "ports": [{ "protocol": "TCP", "port": 32000, "endPort": 32768 }]
        }]
    });
    let body = netpol_body("np-endport", ns, spec);
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies"),
        &body,
    )
    .await;
    assert_eq!(cs, 201);

    let (_, got) = get_json(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies/np-endport"),
    )
    .await;
    assert_eq!(got["spec"]["ingress"][0]["ports"][0]["port"], 32000);
    assert_eq!(
        got["spec"]["ingress"][0]["ports"][0]["endPort"], 32768,
        "endPort must survive serde round-trip — see NetworkPolicyPort#end_port"
    );
}

/// [sig-network] NetworkPolicy should preserve PodSelector and NamespaceSelector on a peer
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/netpol/network_policy.go:260
///   (`should enforce policy based on PodSelector and NamespaceSelector`)
/// Sonobuoy (Round 160): PASS — dataplane assertion not testable against
///   MemoryStorage; mirror verifies the spec-level contract instead.
#[tokio::test]
async fn networkpolicy_combined_pod_and_namespace_selectors_preserved() {
    let state = spawn_state();
    let ns = "netpol-combined";
    let spec = json!({
        "podSelector": { "matchLabels": { "app": "target" } },
        "policyTypes": ["Ingress"],
        "ingress": [{
            "from": [{
                "podSelector": { "matchLabels": { "role": "client" } },
                "namespaceSelector": { "matchLabels": { "team": "alpha" } }
            }]
        }]
    });
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies"),
        &netpol_body("np-combined", ns, spec),
    )
    .await;
    assert_eq!(cs, 201);

    let (_, got) = get_json(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies/np-combined"),
    )
    .await;
    let peer = &got["spec"]["ingress"][0]["from"][0];
    assert_eq!(peer["podSelector"]["matchLabels"]["role"], "client");
    assert_eq!(
        peer["namespaceSelector"]["matchLabels"]["team"], "alpha",
        "namespaceSelector must coexist with podSelector in the same peer"
    );
}

/// [sig-network] NetworkPolicy egress ipBlock with except clause is preserved
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/netpol/network_policy.go:874
///   (`should enforce except clause while egress access to server in CIDR block`)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn networkpolicy_egress_ipblock_with_except_clause() {
    let state = spawn_state();
    let ns = "netpol-egress";
    let spec = json!({
        "podSelector": { "matchLabels": { "app": "client" } },
        "policyTypes": ["Egress"],
        "egress": [{
            "to": [{ "ipBlock": { "cidr": "10.0.0.0/8", "except": ["10.0.5.0/24"] } }],
            "ports": [{ "protocol": "TCP", "port": 443 }]
        }]
    });
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies"),
        &netpol_body("np-egress", ns, spec),
    )
    .await;
    assert_eq!(cs, 201);

    let (_, got) = get_json(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies/np-egress"),
    )
    .await;
    let to0 = &got["spec"]["egress"][0]["to"][0];
    assert_eq!(to0["ipBlock"]["cidr"], "10.0.0.0/8");
    assert_eq!(
        to0["ipBlock"]["except"].as_array().unwrap()[0],
        "10.0.5.0/24",
        "except CIDRs must round-trip"
    );
}

/// [sig-network] NetworkPolicy supports Ingress and Egress rules in the same policy
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/netpol/network_policy.go:630
///   (`should work with Ingress, Egress specified together`)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn networkpolicy_ingress_and_egress_together() {
    let state = spawn_state();
    let ns = "netpol-both";
    let spec = json!({
        "podSelector": { "matchLabels": { "app": "both" } },
        "policyTypes": ["Ingress", "Egress"],
        "ingress": [{ "from": [{ "podSelector": { "matchLabels": { "role": "client" } } }] }],
        "egress": [{ "to": [{ "podSelector": { "matchLabels": { "role": "db" } } }] }]
    });
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies"),
        &netpol_body("np-both", ns, spec),
    )
    .await;
    assert_eq!(cs, 201);

    let (_, got) = get_json(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies/np-both"),
    )
    .await;
    let types = got["spec"]["policyTypes"].as_array().unwrap();
    assert!(types.iter().any(|t| t == "Ingress"));
    assert!(types.iter().any(|t| t == "Egress"));
    assert_eq!(got["spec"]["ingress"].as_array().unwrap().len(), 1);
    assert_eq!(got["spec"]["egress"].as_array().unwrap().len(), 1);
}

/// [sig-network] NetworkPolicy patch updates metadata without dropping spec siblings
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/netpol/network_policy.go:485
///   (`should enforce updated policy`)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn networkpolicy_patch_metadata_preserves_spec_fields() {
    let state = spawn_state();
    let ns = "netpol-patch";
    let spec = json!({
        "podSelector": { "matchLabels": { "app": "target" } },
        "policyTypes": ["Ingress"],
        "ingress": [{
            "from": [{ "podSelector": { "matchLabels": { "role": "client-v1" } } }]
        }]
    });
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies"),
        &netpol_body("np-patch", ns, spec),
    )
    .await;
    assert_eq!(cs, 201);

    let patch = json!({ "metadata": { "labels": { "rev": "2" } } });
    let (ps, after) = patch_merge(
        state,
        &format!("/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies/np-patch"),
        &patch,
    )
    .await;
    assert_eq!(ps, 200, "PATCH must return 200: {after}");
    assert_eq!(after["metadata"]["labels"]["rev"], "2");
    assert_eq!(
        after["spec"]["podSelector"]["matchLabels"]["app"], "target",
        "PATCH on metadata must not drop spec.podSelector"
    );
    assert_eq!(
        after["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"]["role"], "client-v1",
        "PATCH on metadata must not drop spec.ingress"
    );
}

// ---------------------------------------------------------------------------
// Topology-aware routing hints (EndpointSlice.endpoints[].hints.forZones)
// ---------------------------------------------------------------------------

/// [sig-network] Topology Hints should persist forZones on EndpointSlice
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/topology_hints.go:50
///   (`should distribute endpoints evenly`)
/// Sonobuoy (Round 160): PASS — dataplane behaviour; the mirror tests the
///   API surface the upstream test relies on (EndpointSlice persists
///   `endpoints[].hints.forZones` exactly as written).
#[tokio::test]
async fn topology_hints_for_zones_persist_on_endpointslice() {
    let state = spawn_state();
    let ns = "topology-hints";
    let body = endpointslice_with_hints("topo-svc-xyz", ns, "us-west-2a");

    let (cs, created) = post_json(
        state.clone(),
        &format!("/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices"),
        &body,
    )
    .await;
    assert_eq!(cs, 201, "POST EndpointSlice must return 201: {created}");
    assert_eq!(created["addressType"], "IPv4");

    let (gs, got) = get_json(
        state,
        &format!("/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices/topo-svc-xyz"),
    )
    .await;
    assert_eq!(gs, 200);
    let endpoint = &got["endpoints"][0];
    assert_eq!(endpoint["zone"], "us-west-2a");
    assert_eq!(
        endpoint["hints"]["forZones"][0]["name"], "us-west-2a",
        "hints.forZones must round-trip — kube-proxy uses this to pick zone-local backends"
    );
}

/// [sig-network] Topology Hints — update flips the forZones target zone
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/topology_hints.go:50
///   (the EndpointSliceController writes hints on every reconcile; the
///    upstream test verifies the controller re-distributes hints as nodes
///    move zones — we mirror the API contract that an UPDATE can change
///    the hint set without losing other endpoint fields)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn topology_hints_update_replaces_for_zones_set() {
    let state = spawn_state();
    let ns = "topology-update";
    let body = endpointslice_with_hints("topo-update", ns, "us-east-1a");
    post_json(
        state.clone(),
        &format!("/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices"),
        &body,
    )
    .await;

    let mut updated = body.clone();
    updated["endpoints"][0]["zone"] = json!("us-east-1b");
    updated["endpoints"][0]["hints"]["forZones"] = json!([{ "name": "us-east-1b" }]);

    let (us, after) = put_json(
        state.clone(),
        &format!("/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices/topo-update"),
        &updated,
    )
    .await;
    assert_eq!(us, 200, "PUT EndpointSlice must return 200: {after}");
    assert_eq!(after["endpoints"][0]["zone"], "us-east-1b");
    assert_eq!(
        after["endpoints"][0]["hints"]["forZones"][0]["name"],
        "us-east-1b"
    );
    assert_eq!(
        after["endpoints"][0]["nodeName"], "node-a",
        "PUT must not drop nodeName when only the hints/zone change"
    );
}

/// [sig-network] Topology Hints — endpoint without hints is also valid
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/topology_hints.go:50
///   (when topology-aware routing is disabled or the service has only one
///    zone, the controller writes endpoints with no `hints` block;
///    consumers must accept that shape too)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn topology_hints_optional_for_unhinted_endpoints() {
    let state = spawn_state();
    let ns = "topology-optional";
    let body = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": "no-hints",
            "namespace": ns,
            "labels": { "kubernetes.io/service-name": "plain-svc" }
        },
        "addressType": "IPv4",
        "endpoints": [{
            "addresses": ["10.0.2.7"],
            "nodeName": "node-b"
        }],
        "ports": [{ "name": "http", "protocol": "TCP", "port": 80 }],
    });
    let (cs, _) = post_json(
        state.clone(),
        &format!("/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices"),
        &body,
    )
    .await;
    assert_eq!(cs, 201, "EndpointSlice without hints must be accepted");

    let (_, got) = get_json(
        state,
        &format!("/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices/no-hints"),
    )
    .await;
    assert!(
        got["endpoints"][0].get("hints").is_none() || got["endpoints"][0]["hints"].is_null(),
        "endpoint without hints must round-trip without a hints field; got {got}"
    );
    assert_eq!(got["endpoints"][0]["nodeName"], "node-b");
}
