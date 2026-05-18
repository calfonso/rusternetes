# [sig-network] Ingress + NetworkPolicy + Topology hints — scoped conformance coverage

Crate: `crates/api-server` · Test file: `tests/conformance_network_ingress_netpol_topology.rs`

This unit mirrors the Kubernetes v1.35 conformance scenarios that exercise
three closely related sig-network slices owned by the api-server REST
surface:

1. **Ingress + IngressClass** — `networking.k8s.io/v1` CRUD plus the
   `/status` subresource, backend Service+port reference preservation,
   IngressClass cluster-scoped operations and namespace-scoped parameter
   references.
2. **NetworkPolicy** — `networking.k8s.io/v1` CRUD plus the spec-shape
   contracts the dataplane tests in `netpol/network_policy.go` rely on:
   `podSelector` + `namespaceSelector` peers, `ipBlock` + `except`,
   `ports[].endPort`, and the ability for a single policy to carry both
   `ingress` and `egress` rules.
3. **Topology-aware routing hints** — `discovery.k8s.io/v1`
   EndpointSlice persists `endpoints[].zone` and
   `endpoints[].hints.forZones`, the API contract the upstream
   `topology_hints.go` test and kube-proxy's zone-aware backend filter
   both depend on.

The dataplane assertions in upstream tests (TCP/UDP connectivity through
iptables rules) cannot be exercised against `MemoryStorage`, so the
mirror tests the API surface those scenarios depend on: a regression in
the serde round-trip of any nested rule structure would break the
Sonobuoy run with a confusing failure deep in the connectivity probe.

Cross-reference: `docs/CONFORMANCE.md` does not list a dedicated
"ingress" or "networkpolicy" failure bucket — all three slices show
green in Round 160 (415/441), which matches the `mostly PASS`
expectation noted in the worker brief.

Harness: every test drives the real Axum router via
`tower::ServiceExt::oneshot`, backed by `StorageBackend::Memory` and
`AlwaysAllowAuthorizer`. The router is built per request (cheap with
`MemoryStorage`) because `Router::oneshot` consumes `self`.

## Coverage matrix

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `should support creating Ingress API operations` | ingress.go:54 | PASS | `ingress_api_supports_create_get_list_round_trip` | mirrored, passing |
| `should support creating Ingress API operations` (PUT + PATCH verbs) | ingress.go:54 | PASS | `ingress_api_supports_put_and_patch` | mirrored, passing |
| `should support creating Ingress API operations` (DELETE + deletecollection verbs) | ingress.go:54 | PASS | `ingress_api_supports_delete_and_deletecollection` | mirrored, passing |
| `should support creating Ingress API operations` (/status subresource) | ingress.go:54 | PASS | `ingress_status_subresource_round_trip` | mirrored, passing |
| `should support creating Ingress API operations` (Service backend named + numeric port) | ingress.go:54 | PASS | `ingress_backend_resolution_preserves_named_and_numeric_ports` | mirrored, passing |
| `should support creating IngressClass API operations` | ingressclass.go:198 | PASS | `ingressclass_api_supports_create_get_list_delete` | mirrored, passing |
| `should allow IngressClass to have Namespace-scoped parameters` | ingressclass.go:167 | PASS | `ingressclass_with_namespace_scoped_parameters_round_trip` | mirrored, passing |
| `should support creating NetworkPolicy API operations` | netpol/network_policy_api.go:47 | PASS | `networkpolicy_api_supports_create_get_list_delete` | mirrored, passing |
| `should support creating NetworkPolicy API with endport field` | netpol/network_policy_api.go:180 | PASS | `networkpolicy_endport_field_is_preserved` | mirrored, passing |
| `should enforce policy based on PodSelector and NamespaceSelector` | netpol/network_policy.go:260 | PASS | `networkpolicy_combined_pod_and_namespace_selectors_preserved` | mirrored, passing (API contract only) |
| `should enforce except clause while egress access to server in CIDR block` | netpol/network_policy.go:874 | PASS | `networkpolicy_egress_ipblock_with_except_clause` | mirrored, passing (API contract only) |
| `should work with Ingress, Egress specified together` | netpol/network_policy.go:630 | PASS | `networkpolicy_ingress_and_egress_together` | mirrored, passing |
| `should enforce updated policy` | netpol/network_policy.go:485 | PASS | `networkpolicy_patch_metadata_preserves_spec_fields` | mirrored, passing |
| `should distribute endpoints evenly` (topology hints forZones round-trip) | topology_hints.go:50 | PASS | `topology_hints_for_zones_persist_on_endpointslice` | mirrored, passing (API contract only) |
| `should distribute endpoints evenly` (forZones update flip) | topology_hints.go:50 | PASS | `topology_hints_update_replaces_for_zones_set` | mirrored, passing (API contract only) |
| `should distribute endpoints evenly` (endpoint without hints) | topology_hints.go:50 | PASS | `topology_hints_optional_for_unhinted_endpoints` | mirrored, passing (API contract only) |

Notes:

- Several upstream tests in `netpol/network_policy.go` and the entire
  `topology_hints.go` file rely on real iptables and zone-aware
  scheduling — those assertions cannot run against `MemoryStorage`. The
  mirror covers the **API contract** those tests depend on: the
  EndpointSliceController must be able to write a `hints.forZones`
  block, kube-proxy must be able to read it back, and NetworkPolicy
  controllers must be able to walk `ingress[].from[].podSelector` and
  `egress[].to[].ipBlock.except` arrays without losing fields to a
  serde rename mistake. Dataplane parity is verified by the full
  Sonobuoy run, not this scoped suite.
- `topology_hints_optional_for_unhinted_endpoints` is explicitly there
  because the EndpointSliceController writes both shapes (with hints
  when topology-aware routing is enabled, without when it is not), and
  the api-server must accept and round-trip both without coercing the
  missing `hints` field to a non-null sentinel.
- All tests in this file mirror upstream Sonobuoy-passing scenarios.
  None are `#[ignore]`d. If a regression appears, the test must either
  be fixed (the mirror was wrong) or marked
  `#[ignore = "Conformance failure tracker — see docs/conformance/network-ingress-netpol-topology.md"]`
  and the status above flipped to FAIL so the regression is tracked,
  not masked.
