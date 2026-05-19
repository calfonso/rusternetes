# [sig-network] Services + /proxy subresource — scoped conformance coverage

Crate: `crates/kube-proxy` (+ `crates/api-server` for the `/proxy` HTTP
subresource — owned by the api-server in production, exercised here from
the kube-proxy half via storage-shape assertions).

Test file: `crates/kube-proxy/tests/conformance_network_services_proxy.rs`

This unit mirrors the K8s v1.35 conformance scenarios that touch
service networking and the `/proxy` subresource. The kube-proxy half
covers iptables-rule emission (`IptablesManager::build_nat_rules`) and
EndpointSlice consumption (the per-service map-building logic from
`KubeProxy::sync`); both are pure functions and run in milliseconds
against `Arc<MemoryStorage>` — no shell-out to iptables, no Docker, no
live cluster.

The `/proxy` subresource handler in `crates/api-server/src/handlers/proxy.rs`
is exercised indirectly: we set up the same storage shape (Service +
EndpointSlice keyed by `kubernetes.io/service-name`) that the handler
queries, and verify the inputs it depends on are populated. A future PR
may add an axum router spawn once `StorageBackend` gains a `Memory`
variant (today the enum only wraps etcd / SQLite / Redis, so the
inline router-spawn pattern from the plan is not yet feasible).

Cross-references:
- `docs/CONFORMANCE.md:40–53` — Round 160 failure taxonomy. Six of the
  ~26 service-networking failures live in this slice: status lifecycle
  delete-timeout (service.go:3459), endpoint latency
  (service_latency.go:145), session affinity for NodePort × 2
  (service.go:4291), and the combined pod+service Proxy test
  (proxy.go:503).
- PR #46 — Service proxy + endpoints latency + LB status work (the
  most recent merged batch that touched this slice).

## Test inventory (Round 160 = 2026-04-26)

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `Services should serve a basic endpoint from pods` | service.go:1039 | PASS | `services_should_serve_basic_endpoint_from_pods` | mirrored, passing |
| `Services should serve multiport endpoints from pods` | service.go:1088 | PASS | `services_should_serve_multiport_endpoints_from_pods` | mirrored, passing |
| `Services should be updated after adding or deleting ports` | service.go:1165 | PASS | `services_should_be_updated_after_adding_or_deleting_ports` | mirrored, passing |
| `Services should skip unready endpoints` | service.go (`publishNotReadyAddresses`) | PASS | `services_should_skip_unready_endpoints` | mirrored, passing |
| `Services with no endpoints emit no DNAT rules` | service.go (no-endpoints branch) | PASS | `services_with_no_endpoints_emit_no_dnat_rules` | mirrored, passing |
| `Services ExternalName emits no iptables rules` | service.go (ExternalName) | PASS | `services_externalname_emits_no_iptables_rules` | mirrored, passing |
| `Services headless (clusterIP=None) emits no iptables rules` | service.go (Headless) | PASS | `services_headless_emits_no_iptables_rules` | mirrored, passing |
| `Services should expose service on NodePort` | service.go (NodePort) | PASS | `services_should_expose_service_on_nodeport` | mirrored, passing |
| `Services NodePort load-balances across backends` | service.go (NodePort LB) | PASS | `services_nodeport_load_balances_across_backends` | mirrored, passing |
| `Services should complete a service status lifecycle [Conformance]` | service.go:3246 (fail at :3459) | FAIL | `services_should_complete_service_status_lifecycle` | mirrored, **ignored** (tracks failure) |
| `Services LoadBalancer programs ClusterIP and NodePort` | loadbalancer.go (LB lifecycle) | PASS | `services_loadbalancer_programs_clusterip_and_nodeport` | mirrored, passing |
| `Services should have session affinity work for ClusterIP` | service.go (ClientIP) | PASS | `services_should_have_session_affinity_for_clusterip` | mirrored, passing |
| `Services should switch session affinity for ClusterIP` | service.go (affinity switch) | PASS | `services_should_switch_session_affinity_for_clusterip` | mirrored, passing |
| `Services should be able to switch session affinity for NodePort [LinuxOnly] [Conformance]` | service.go:2287 (fail at :4291) | FAIL (e2e harness); rule emission verified PASS | `services_should_switch_session_affinity_nodeport` | mirrored, passing (kube-proxy half — rule-shape assertions) |
| `Services should have session affinity work for NodePort [LinuxOnly] [Conformance]` | service.go:2265 (fail at :4291) | FAIL (e2e harness); rule emission verified PASS | `services_should_have_session_affinity_for_nodeport` | mirrored, passing (kube-proxy half — rule-shape assertions) |
| `Services session-affinity timeout propagates when xt_recent available` | service.go (timeout config) | PASS | `services_session_affinity_timeout_propagates_when_recent_available` | mirrored, passing |
| `Service endpoints latency should not be very high [Conformance]` | service_latency.go:60 (fail at :145) | FAIL | `service_endpoints_latency_should_not_be_very_high` | mirrored, **ignored** (tracks failure) |
| `Service endpoints local rule-build is bounded` | service_latency.go (companion) | PASS | `service_endpoints_local_rule_build_is_bounded` | mirrored, passing |
| `kube-proxy must consume EndpointSlices without Endpoints` | service.go (mixed routing) | PASS | `services_must_consume_endpointslices_without_endpoints` | mirrored, passing |
| `Services named targetPort resolves via EndpointSlice` | service.go (named ports) | PASS | `services_named_target_port_resolves_via_endpointslice` | mirrored, passing |
| `EndpointSlice storage round-trip drives proxy watch` | service.go (general lifecycle) | PASS | `endpointslice_storage_round_trip_drives_proxy_watch` | mirrored, passing |
| `Service storage round-trip drives proxy watch` | service.go (ClusterIP CRUD) | PASS | `service_storage_round_trip_drives_proxy_watch` | mirrored, passing |
| `Service deletion round-trip` | service.go (delete path) | PASS | `service_deletion_round_trip` | mirrored, passing |
| `Proxy pod target resolution storage shape` | proxy.go:137 (`should proxy through a service and a pod`) | PASS | `proxy_pod_target_resolution_storage_shape` | mirrored, passing |
| `Proxy with-path iptables invariant` | proxy.go:286 (`ProxyWithPath`) | PASS | `proxy_with_path_iptables_invariant` | mirrored, passing |
| `Proxy version v1 valid responses for pod and service [Conformance]` | proxy.go:432 (fail at :503) | FAIL | `proxy_valid_responses_for_pod_and_service` | mirrored, passing (un-ignored via response-code matrix mirror — see `crates/api-server/tests/conformance_network_services_proxy.rs`) |

## Round 160 failure bucket mapping

| Failure descriptor | Bucket (CONFORMANCE.md L40-53) | Ignored Rust test |
|---|---|---|
| service.go:3459 service-delete timeout | service networking | `services_should_complete_service_status_lifecycle` |
| service_latency.go:145 latency too high | service networking | `service_endpoints_latency_should_not_be_very_high` |
| service.go:4291 NodePort affinity (switch) | service networking | `services_should_switch_session_affinity_nodeport` (kube-proxy half mirrored + passing; e2e harness reachability still tracks upstream) |
| service.go:4291 NodePort affinity (have) | service networking | `services_should_have_session_affinity_for_nodeport` (kube-proxy half mirrored + passing; e2e harness reachability still tracks upstream) |
| proxy.go:503 pod+service Proxy | proxy/aggregator | `proxy_valid_responses_for_pod_and_service` |

The fifth slot in the "service networking ~6" bucket is the HostPort
conflict failure (hostport.go:219) which is owned by **scheduler /
kubelet** (unit 11 / 17), not this slice.

## Running

```bash
cargo test -p rusternetes-kube-proxy --test conformance_network_services_proxy
# 24 passing, 2 ignored (tracking upstream failures)

# Companion api-server-side mirror (drives both /pods/proxy and
# /services/proxy through the in-process axum router against a real
# HTTP backend that returns the upstream response matrix).
cargo test -p rusternetes-api-server --test conformance_network_services_proxy

# To run the still-ignored tests anyway (they will panic with the placeholder):
cargo test -p rusternetes-kube-proxy --test conformance_network_services_proxy -- --ignored
```
