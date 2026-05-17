# [sig-network] EndpointSlices + headless services + DNS — scoped conformance coverage

Crate: `crates/kube-proxy` · Test file: `tests/conformance_network_endpointslices_dns_headless.rs`

Slice: EndpointSlice population from Service + pod readiness · Endpoints (legacy v1)
reconciliation · headless Service DNS (CoreDNS A/AAAA + SRV records) ·
EndpointsController updates on pod state change.

This is the **kube-proxy facing** slice: the tests verify the EndpointSlice /
Endpoints objects that the EndpointSlice controller emits — which is exactly
what kube-proxy consumes — and the DNS resource records that the CoreDNS
`kubernetes` plugin derives from those same in-tree objects (A / SRV /
ExternalName CNAME). CoreDNS itself runs as an external pod in the cluster;
no test in this file shells out to a DNS server. We assert the **data shape**
that our controllers produce, mirroring the assertions in upstream
`test/e2e/network/dns.go` and `test/e2e/network/endpointslice.go`.

Sonobuoy reference: `.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`.
Per `docs/CONFORMANCE.md:40–53` the sig-network EndpointSlice + DNS slice is
mostly PASS in Round 160 (no failures specifically attributed to this slice;
the 6 service-networking failures bucket belongs to `network_services_proxy`,
work unit 18).

## Group 1 — EndpointSlice population from Service + pod readiness

Upstream source: `k8s.io/kubernetes/test/e2e/network/endpointslice.go`.

| Upstream Ginkgo descriptor | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `should create and delete EndpointSlices for a Service with a selector that matches no pods` | endpointslice.go:73 | PASS | `endpointslice_should_create_empty_slice_when_selector_matches_no_pods` | mirrored, passing |
| `should create Endpoints and EndpointSlices for Pods matching a Service` | endpointslice.go:116 | PASS | `endpointslice_should_populate_slice_with_ready_pods` | mirrored, passing |
| `should create Endpoints and EndpointSlices for Pods matching a Service` (readiness sub-assertion) | endpointslice.go:116 | PASS | `endpointslice_should_mark_not_ready_pod_as_not_ready` | mirrored, passing |
| `should support a Service with multiple ports specified in multiple EndpointSlices` | endpointslice.go:395 | PASS | `endpointslice_should_carry_multiple_named_ports` | mirrored, passing |
| `should support a Service with multiple endpoint IPs specified in multiple EndpointSlices` | endpointslice.go:501 | PASS | `endpointslice_should_carry_multiple_endpoint_ips` | mirrored, passing |
| `should support creating EndpointSlice API operations` (service-name label) | endpointslice.go:231 | PASS | `endpointslice_should_label_with_service_name` | mirrored, passing |
| `should support creating EndpointSlice API operations` (managed-by label) | endpointslice.go:231 | PASS | `endpointslice_should_carry_managed_by_label` | mirrored, passing |
| `should create Endpoints and EndpointSlices for Pods matching a Service` (owner ref) | endpointslice.go:116 | PASS | `endpointslice_should_have_service_owner_reference` | mirrored, passing |
| `should create Endpoints and EndpointSlices for Pods matching a Service` (targetRef) | endpointslice.go:116 | PASS | `endpointslice_endpoint_should_carry_pod_target_ref` | mirrored, passing |
| `should create Endpoints and EndpointSlices for Pods matching a Service` (pod delete) | endpointslice.go:116 | PASS | `endpointslice_should_evict_endpoint_when_pod_deleted` | mirrored, passing |
| EndpointSlice pod phase filter (Succeeded/Failed excluded) | endpointslice.go | PASS | `endpointslice_should_skip_terminal_phase_pods` | mirrored, passing |
| EndpointSlice graceful shutdown semantics (terminating pod kept w/ ready=false) | endpointslice.go (utils.go `podEndpointConditions`) | PASS | `endpointslice_should_mark_terminating_pods_as_not_ready` | mirrored, passing |
| EndpointSlice reactive readiness flip | endpointslice.go:116 | PASS | `endpointslice_should_flip_endpoint_ready_on_pod_state_change` | mirrored, passing |

## Group 2 — Endpoints (legacy v1) reconciliation

Upstream source: `k8s.io/kubernetes/test/e2e/network/{service,endpointslicemirroring}.go`.

| Upstream Ginkgo descriptor | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| Legacy v1.Endpoints populated for matching pods (asserted across Service tests) | service.go | PASS | `endpoints_v1_should_populate_subsets_for_matching_pods` | mirrored, passing |
| Legacy v1.Endpoints separates ready / notReadyAddresses | service.go | PASS | `endpoints_v1_should_separate_ready_from_not_ready_addresses` | mirrored, passing |
| `should mirror a custom Endpoints resource through create update and delete` | endpointslicemirroring.go:50 | PASS | `endpoints_v1_without_selector_should_be_mirrored_to_endpointslice` | mirrored, passing |

## Group 3 — Headless Service DNS (records the CoreDNS kubernetes plugin emits)

Upstream source: `k8s.io/kubernetes/test/e2e/network/dns.go` (+ helpers in `dns_common.go`).
CoreDNS itself is an external pod; we test the in-tree data shape it consumes.

| Upstream Ginkgo descriptor | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `should provide DNS for the cluster` | dns.go:46 | PASS | `dns_should_provide_a_record_for_cluster_ip_service` | mirrored, passing |
| `should provide DNS for services` (headless A records) | dns.go:130 | PASS | `dns_should_provide_a_record_per_endpoint_for_headless_service` | mirrored, passing |
| `should provide DNS for services` (ready filter) | dns.go:130 | PASS | `dns_should_exclude_not_ready_endpoints_from_headless_a_records` | mirrored, passing |
| `should provide DNS for pods for Hostname` | dns.go:209 | PASS | `dns_should_provide_pod_a_record_for_hostname` | mirrored, passing |
| `should provide DNS for pods for Subdomain` (dashed-IP fallback) | dns.go:240 | PASS | `dns_should_provide_pod_a_record_with_dashed_ip_fallback` | mirrored, passing |
| `should provide DNS for services` (SRV records, regular service) | dns.go:130 | PASS | `dns_should_build_srv_record_for_named_service_port` | mirrored, passing |
| `should provide DNS for services` (SRV records, headless service) | dns.go:130 | PASS | `dns_should_build_srv_records_per_endpoint_for_headless_service` | mirrored, passing |
| DNS SRV records require a named port (no SRV for unnamed ports) | dns.go | PASS | `dns_should_skip_srv_records_for_unnamed_ports` | mirrored, passing |
| `should provide DNS for services` (headless w/ no ready backends) | dns.go:130 | PASS | `dns_headless_with_no_ready_endpoints_yields_no_a_records` | mirrored, passing |
| `should provide DNS for ExternalName services` | dns.go:271 | PASS | `dns_externalname_service_should_not_emit_a_records` | mirrored, passing |

## Group 4 — EndpointsController updates on pod state change

Upstream source: `k8s.io/kubernetes/test/e2e/network/{endpointslice,service}.go` (reactive update assertions).

| Upstream behavior asserted across upstream tests | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| New ready pod is added to Endpoints | endpointslice.go:116 | PASS | `endpoints_controller_should_add_address_when_new_pod_becomes_ready` | mirrored, passing |
| Pod Ready→NotReady moves address to notReadyAddresses | endpointslice.go:116 | PASS | `endpoints_controller_should_drop_address_when_pod_becomes_not_ready` | mirrored, passing |
| Pod delete removes address from Endpoints | service.go | PASS | `endpoints_controller_should_remove_address_when_pod_deleted` | mirrored, passing |

## Group 5 — kube-proxy consumption of EndpointSlices

These mirror the **consumer-side** contract that the upstream EndpointSlice
tests implicitly rely on: kube-proxy joins slices by the
`kubernetes.io/service-name` label, skips NotReady endpoints when programming
DNAT backends, and treats nil `EndpointConditions` as ready (legacy fallback).
The behavior is exercised end-to-end by every passing `[sig-network] Service`
test in Sonobuoy R160 — we lift it into focused unit assertions so a regression
fails locally instead of waiting for a 60-minute Sonobuoy run.

| Upstream contract (consumer side) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| Group slices by `kubernetes.io/service-name` label | pkg/proxy/endpointslicecache.go | PASS | `kube_proxy_should_group_endpointslices_by_service_name_label` | mirrored, passing |
| Skip NotReady endpoints when programming backends | pkg/proxy/topology.go | PASS | `kube_proxy_should_skip_not_ready_endpoints_in_backend_set` | mirrored, passing |
| Nil EndpointConditions => treat as ready | pkg/proxy/endpointslicecache.go | PASS | `kube_proxy_should_treat_nil_conditions_as_ready` | mirrored, passing |

## Notes on scope

- No `#[ignore]` tests: the sig-network EndpointSlice + DNS slice has no
  Sonobuoy failures attributed to it in Round 160 (`docs/CONFORMANCE.md:40–53`).
  When a regression lands, switch the affected row's Status to `mirrored,
  ignored (tracks failure)`, add the failure message verbatim, and gate the
  test with `#[ignore = "Conformance failure tracker — see
  docs/conformance/network-endpointslices-dns-headless.md"]`.
- The DNS record builders inline in the test file (`build_a_records`,
  `build_srv_records`) mirror the CoreDNS `kubernetes` plugin behavior.
  They live in the test crate to keep this batch self-contained; if we add a
  DNS-record builder to a production crate later, the tests can be retargeted
  to call it directly.
- `ExternalName` CNAME emission is left to CoreDNS — this test only asserts
  that the in-tree EndpointSlice set stays empty for ExternalName services so
  CoreDNS picks the CNAME path instead of the A-record path.
