# Ignored conformance-tracker tests — fix plan

Scope: every `#[ignore = "Conformance failure tracker — …"]` or `#[ignore = "Ratcheting tracker — …"]` in `crates/*/tests/conformance_*.rs`. 19 tests across 6 functional areas. Out of scope: `#[ignore] // requires etcd`, `#[ignore = "perf microbenchmark …"]`, `#[ignore = "moved to …"]`, and `protobuf_test::test_content_negotiation` (no tracker reason).

Each item is a separate work unit. Tackle items independently; tick the box when the `#[ignore]` attribute is gone and the test passes.

## Implementation priority

Order chosen by complexity ascending so easy wins land first and unblock CI signal early.

1. **Trivial / docs-only** — items 18, 19 (apps-deployment-replicaset, design decision)
2. **Small** — items 1, 10, 11, 12, 15, 16 (single-component fixes, ≤3 days)
3. **Medium** — items 13, 14, 17, 8, 9 (multi-component, 1–2 weeks)
4. **Large** — items 2–7 (ratcheting infrastructure, multi-week, gated on schema-diff engine)

---

## Area 1 — apimachinery-aggregation-discovery (1 test)

### [ ] 1. `aggregator_sample_apiserver_full_lifecycle`
- File: `crates/api-server/tests/conformance_apimachinery_aggregation_discovery.rs:493`
- Upstream: `[sig-api-machinery] Aggregator Should be able to support the 1.17 Sample API Server using the current Aggregator [LinuxOnly] [Conformance]` (`test/e2e/apimachinery/aggregator.go:102`)
- Root cause: not a code defect in the aggregator REST surface. `SetUpSampleAPIServer()` upstream waits for `registry.k8s.io/e2e-test-images/sample-apiserver` pod to reach Ready; rusternetes kubelet can't pull/start that image in the in-process harness. The 12 unignored sibling tests cover CRUD, discovery merge, and HTTP proxy already.
- Fix plan:
  1. Resolve kubelet image-pull + pod-readiness lifecycle for the harness (separate infra work).
  2. Port the 19 sub-assertions from upstream `aggregator.go:285–541` (APIService creation, status conditions, discovery merge, proxy forwarding, 503 on backend down).
  3. Drop `#[ignore]`; run `cargo test --test conformance_apimachinery_aggregation_discovery aggregator_sample_apiserver_full_lifecycle`.
- Complexity: **small** (1–3 days) once kubelet infra is sorted.
- Blocker: kubelet image-pull infra in test harness.

---

## Area 2 — apimachinery-crd-lifecycle (8 ratcheting tests)

All 8 currently have empty `{}` bodies. Shared blocker: a JSON-Schema diff engine in `crates/common/src/schema_validation.rs`. Implementation steps below apply to the whole area; individual tests just exercise distinct branches of the same machinery. Upstream source: `test/e2e/apimachinery/crd_validation_ratcheting.go`.

Shared fix-plan skeleton (do once, then fill each test body):
1. Add `SchemaDiff` in `crates/common/src/schema_validation.rs` that walks `(old_value, new_value, schema)` and returns a `Vec<JsonPath>` of changed nodes. Must handle nested objects, arrays (with `x-kubernetes-list-type`), maps-of-objects (with `x-kubernetes-map-keys`), and unions.
2. Add correlation detection: an entry is "correlatable" if it can be matched between old and new (e.g., array-of-objects with a `x-kubernetes-list-map-keys`). Conservative default: not correlatable.
3. Plumb a `ValidationScope { ValidateAll | ValidateChangedOnly(Vec<JsonPath>) }` through `crates/api-server/src/handlers/crd.rs::update_custom_resource` and `patch_custom_resource`. CREATE uses `ValidateAll`; UPDATE uses `ValidateChangedOnly`.
4. Update the schema validator to honour the scope.
5. In `crates/api-server/src/handlers/cel_validation.rs::evaluate_rules`, skip non-transition rules whose `fieldPath` lies outside the scope. Always evaluate transition rules (rules referencing `oldSelf`).
6. Parse `optionalOldSelf` on each `x-kubernetes-validations` entry; on UPDATE, when scope says the field is new, bind `oldSelf = nil`.

Complexity: **large (multi-week)** for items 2–7; **medium** for items 8–9 (mostly assemble on top of the engine).

### [ ] 2. `ratcheting_unchanged_correlatable_jsonschema_errors_allowed`
- File: `crates/api-server/tests/conformance_apimachinery_crd_lifecycle.rs:1268`
- Upstream: `crd_validation_ratcheting.go:201` — unchanged correlatable fields must NOT re-fail under a new constraint.
- Test body to write: create CRD with stricter schema on an existing CR's unchanged field, PATCH the CR's *other* field; assert PATCH succeeds.

### [ ] 3. `ratcheting_unchanged_uncorrelatable_jsonschema_errors_blocked`
- File: `…crd_lifecycle.rs:1276`
- Upstream: `crd_validation_ratcheting.go:244` — unchanged *uncorrelatable* fields must still re-fail.
- Test body: same setup as #2 but field has no correlation key; assert PATCH rejected.

### [ ] 4. `ratcheting_changed_jsonschema_errors_blocked`
- File: `…crd_lifecycle.rs:1284`
- Upstream: `crd_validation_ratcheting.go:280` — changed fields always re-validate.
- Test body: PATCH writes an invalid value into a *changed* field; assert rejection.

### [ ] 5. `ratcheting_unchanged_correlatable_cel_errors_allowed`
- File: `…crd_lifecycle.rs:1292`
- Upstream: `crd_validation_ratcheting.go:333` — CEL rules on unchanged correlatable fields skipped on UPDATE.
- Test body: install a CEL rule that would fail on the existing field; PATCH unrelated field; assert PATCH allowed.

### [ ] 6. `ratcheting_unchanged_uncorrelatable_cel_errors_blocked`
- File: `…crd_lifecycle.rs:1300`
- Upstream: `crd_validation_ratcheting.go:412` — CEL rules on unchanged uncorrelatable fields must still fire.
- Test body: same as #5 but rule has no `fieldPath`; assert rejection.

### [ ] 7. `ratcheting_changed_cel_errors_blocked`
- File: `…crd_lifecycle.rs:1308`
- Upstream: `crd_validation_ratcheting.go:448` — CEL rules on changed fields always fire.
- Test body: PATCH changes a value that violates the rule; assert rejection.

### [ ] 8. `ratcheting_transition_rule_errors_never_ratcheted`
- File: `…crd_lifecycle.rs:1316`
- Upstream: `crd_validation_ratcheting.go:511` — transition rules (rules referencing `oldSelf`) NEVER ratchet.
- Extra fix-plan step: in `cel_validation.rs`, flag rules whose source contains `oldSelf` (detect at parse time, not regex on the rule body — use the CEL AST). Always evaluate flagged rules irrespective of scope.
- Complexity: **medium** (CEL flag + always-evaluate path is small; mostly coordination with the diff engine).

### [ ] 9. `ratcheting_optional_old_self_nil_for_new_values`
- File: `…crd_lifecycle.rs:1324`
- Upstream: `crd_validation_ratcheting.go:569` — `optionalOldSelf: true` binds `oldSelf = nil` for fields that didn't exist in the old object.
- Extra fix-plan step: extend `ValidationRule` with `optional_old_self: bool`; in `evaluate_rules`, when this flag is set and the diff engine reports the field is new, pass `nil` for `oldSelf` into the CEL evaluator.
- Complexity: **medium** (small CEL change; "is new field" requires schema-diff).

---

## Area 3 — network-services-proxy (5 tests)

### [ ] 10. `services_should_complete_service_status_lifecycle`
- File: `crates/kube-proxy/tests/conformance_network_services_proxy.rs:523`
- Upstream: `[sig-network] Services should complete a service status lifecycle [Conformance]` (`service.go:3246`, fails at 3459).
- Root cause: kube-proxy emits LoadBalancer rules correctly, but the api-server / cloud-controller path never populates `status.loadBalancer.ingress[]`, so the status lifecycle check times out.
- Fix plan:
  1. Audit `crates/controller-manager/src/controllers/loadbalancer.rs::update_service_status` (~line 443) — add retry with exponential backoff when the cloud-provider call returns transient errors.
  2. In `reconcile()` (~line 310) propagate errors instead of swallowing them; emit a structured event.
  3. Add a mock cloud-provider in tests so the status lifecycle can be exercised without external I/O.
  4. Test body: create LoadBalancer Service, assert `status.loadBalancer.ingress` becomes non-empty within a deadline; delete; assert ingress is cleared.
- Complexity: **small–medium**.
- Blocker: none.

### [ ] 11. `services_should_switch_session_affinity_nodeport`
- File: `…proxy.rs:640`
- Upstream: `[sig-network] Services should be able to switch session affinity for NodePort [LinuxOnly] [Conformance]` (`service.go:2287`, fails at 4291).
- Root cause: rule-emission path exists in `crates/kube-proxy/src/iptables.rs` (`emit_nodeport_rules`, ~lines 1273–1340) with `KUBE-SEP-*` chains and `xt_recent --rcheck` matchers. Test body currently panics; needs the same `assert_ne!` diffing pattern that the passing ClusterIP test uses (line 598+).
- Fix plan:
  1. Build two NodePort Services — `svc_none` (no affinity) and `svc_clientip` (`sessionAffinity: ClientIP`).
  2. Call `build_nat_rules` for each; assert the rule sets differ and that the affinity variant contains `KUBE-SEP-*` chain references referenced by NODEPORTS before the direct DNAT.
  3. Assert switching the Service back to `None` returns to the original rule shape (no residual chains).
  4. Verify ordering: NODEPORTS chain must reference `KUBE-SEP-*` before direct DNAT or matching breaks.
- Complexity: **small**.

### [ ] 12. `services_should_have_session_affinity_for_nodeport`
- File: `…proxy.rs:652`
- Upstream: `[sig-network] Services should have session affinity work for NodePort [LinuxOnly] [Conformance]` (`service.go:2265`).
- Root cause: same as #11 — missing test body, machinery is already present.
- Fix plan: build a NodePort Service with `sessionAffinity: ClientIP` and 2+ EndpointSlice endpoints; assert `KUBE-SEP-*` references appear when `recent_available=true`, and assert direct-DNAT fallback when `recent_available=false`.
- Complexity: **small**.

### [ ] 13. `service_endpoints_latency_should_not_be_very_high`
- File: `…proxy.rs:711`
- Upstream: `[sig-network] Service endpoints latency should not be very high [Conformance]` (`service_latency.go:60`, fails at 145).
- Root cause: not in kube-proxy — kube-proxy's local rule-build latency is already tested O(1) by `service_endpoints_local_rule_build_is_bounded` (line 727, passing). The end-to-end delay is in the EndpointSlice mirroring loop in controller-manager.
- Fix plan:
  1. Locate the EndpointSlice mirroring controller in `crates/controller-manager/src/controllers/` (likely `endpoints.rs` or `endpointslices.rs`). Confirm it watches Pod/Endpoints changes rather than polling.
  2. Profile the path: time from "Pod becomes Ready" to "EndpointSlice updated in storage".
  3. Convert any polling to a workqueue with event-driven re-enqueue.
  4. Test body: spawn the controller against `MemoryStorage`, write N services and ramp pods, assert p99 propagation time is under the upstream threshold.
- Complexity: **medium–large** (requires controller refactor if it polls).

### [ ] 14. `proxy_valid_responses_for_pod_and_service`
- File: `…proxy.rs:986`
- Upstream: `[sig-network] Proxy version v1 [Conformance] A set of valid responses are returned for both pod and service Proxy` (`proxy.go:432`, fails at 503).
- Root cause: api-server's service-proxy resolves through EndpointSlice (existing in `crates/api-server/src/handlers/proxy.rs`, ~lines 197/295–328), but the combined pod+service proxy flow exercised by the conformance test trips on routing/aggregation.
- Fix plan:
  1. Re-read upstream `proxy.go:432–503` to understand the combined flow.
  2. Confirm `/api/v1/namespaces/{ns}/pods/{name}/proxy/{path}` and `/api/v1/namespaces/{ns}/services/{name}/proxy/{path}` are both registered in `crates/api-server/src/router.rs`.
  3. Verify `proxy_service` resolves to exactly one endpoint IP + port and that `proxy_pod` extracts pod IP + port correctly.
  4. Add an integration test: Service → EndpointSlice → Pod chain, proxy through the Service and assert it reaches the Pod (and that direct Pod proxy hits the same Pod).
- Complexity: **medium**.

---

## Area 4 — node-exec-logs-downward (2 tests)

### [ ] 15. `pod_exec_over_websocket_query_format_matches_upstream`
- File: `crates/kubelet/tests/conformance_node_exec_logs_downward.rs:590`
- Upstream: `[sig-node] Pods should support remote command execution over websockets` (`test/e2e/common/node/pods.go:517`).
- Root cause: this Rust test pins the query-string shape (`container=…&command=%2Fbin%2Fsh&command=-c&stdout=1&stderr=1`) that clients would send. The api-server already accepts the `v4.channel.k8s.io` / `v5.channel.k8s.io` subprotocol (`pod_subresources.rs:590`, `streaming.rs:11–24`). The Sonobuoy failure is end-to-end (kubelet not reachable through the harness), not a query-format defect.
- Fix plan:
  1. Confirm the test body actually verifies query construction (it should be a pure unit test). If it's a placeholder, fill it: build the expected URL with `url::form_urlencoded`, assert the encoded form matches upstream byte-for-byte.
  2. Drop `#[ignore]` if the Rust assertion passes — the marker exists to track the upstream Sonobuoy failure, not because the Rust test fails. Move that tracker to `docs/conformance/node-exec-logs-downward.md` instead.
- Complexity: **small**.

### [ ] 16. `pod_log_over_websocket_query_is_container_only`
- File: `…exec_logs_downward.rs:609`
- Upstream: `[sig-node] Pods should support retrieving logs from the container over websockets` (`pods.go:583`).
- Root cause: the api-server log-over-websocket path currently sends plain text frames (`pod_subresources.rs:204–214`) instead of the channel-prefixed binary subprotocol used by exec.
- Fix plan:
  1. Generalise the binary framer in `crates/api-server/src/streaming.rs` (currently exec-specific) so it can wrap log output with channel 1 = stdout.
  2. Switch the `get_logs()` websocket branch in `pod_subresources.rs` to use the new handler.
  3. The test itself asserts a query invariant (`container=` exactly once); fill the body, drop `#[ignore]`, run.
- Complexity: **small–medium**.

---

## Area 5 — scheduling-priority-preemption-hostport (1 test)

### [ ] 17. `preemption_execution_path_replicaset_available_replicas`
- File: `crates/scheduler/tests/conformance_scheduling_priority_preemption_hostport.rs:347`
- Upstream: `[sig-scheduling] SchedulerPreemption [Serial] PreemptionExecutionPath runs ReplicaSets to verify preemption running path` (`preemption.go:756`, fails at 1025).
- Root cause: four-way interplay (scheduler → controller-manager → kubelet → api-server). After preemption evicts a victim, the recreated pod must reach Ready and the ReplicaSet's `status.availableReplicas` must reflect the new count within the test's window.
- Fix plan:
  1. In `crates/scheduler/src/advanced.rs` (around `check_preemption`, ~line 826), confirm victims are actually deleted (not just marked).
  2. In `crates/controller-manager/src/controllers/replicaset.rs`, verify the controller watches pod deletions and re-enqueues quickly; confirm `update_status()` writes `availableReplicas` once new pods reach Ready (~lines 560–630).
  3. Add a `tokio::test` that wires the scheduler + ReplicaSet controller against `MemoryStorage`, creates a low-priority RS that fills capacity, schedules a high-priority pod that forces preemption, and asserts the RS regains `availableReplicas == desired` within a deadline.
  4. If the cycle is too slow, move from interval-polling to event-driven reconcile.
- Complexity: **medium–large**.
- Blocker: controller must respond to pod deletion events, not just periodic reconcile.

---

## Area 6 — apps-deployment-replicaset (2 tests)

These two are intentionally out-of-scope for controller-level unit tests. The upstream test curls every pod IP and verifies HTTP 200 from a real `nginx` image — this requires a kubelet, image pull, container runtime, and pod networking, all of which live outside the controller-manager crate. Existing sibling unit tests already pin the controller contract (N pods created with correct image + labels + spec propagation).

### [ ] 18. `replicaset_should_serve_basic_image_on_each_replica`
- File: `crates/controller-manager/tests/conformance_apps_deployment_replicaset.rs:831`
- Upstream: `[sig-apps] ReplicaSet should serve a basic image on each replica with a public image` (`test/e2e/apps/replica_set.go:95`).
- Recommended resolution: leave ignored but improve the marker. Two-line change:
  1. Update the `#[ignore = …]` text from `"basic image serving E2E"` to `"E2E conformance only: requires kubelet + container runtime + pod networking; controller contract is covered by sibling tests"`.
  2. Append a note to `docs/conformance/apps-deployment-replicaset.md` lines 41–46 explaining the intentional scope boundary.
- Complexity: **trivial** (docs / marker only). No production code change.
- Alternative if we ever want to remove `#[ignore]`: add a bollard-driven integration test that pulls `nginx:stable-alpine`, runs it on the host, and curls it — but that crosses the "no Docker in unit tests" line and is not recommended.

### [ ] 19. `rc_should_serve_basic_image_on_each_replica`
- File: `…deployment_replicaset.rs:1100`
- Upstream: `[sig-apps] ReplicationController should serve a basic image on each replica with a public image` (`test/e2e/apps/rc.go:65`).
- Same resolution as #18 — update marker + docs note. The `ReplicationControllerController` reconciler is unit-covered already (see `rc_publishes_status_after_reconcile`); only the e2e curl step is missing, and it belongs to Sonobuoy.
- Complexity: **trivial**.

---

## Verification per item

When un-ignoring any of items 1–17:

1. `cargo test -p <crate> --test <test_file> <test_name> -- --exact`
2. `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`
3. `make pre-commit` (fmt + clippy + test)
4. After each batch of fixes lands, run `bash scripts/run-conformance.sh` and update `docs/CONFORMANCE.md` round counter + failure bucket counts.

For items 18–19 (docs-only): no test run; just confirm the new marker compiles (`cargo build --tests -p controller-manager`) and the doc renders.

## Tracking

This file is the source of truth for ignored-tracker progress. Tick the box, remove the `#[ignore]`, and update the corresponding `docs/conformance/<area>.md` coverage matrix so the area-level matrix stays consistent.
