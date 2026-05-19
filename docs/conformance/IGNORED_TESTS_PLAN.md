# Ignored conformance-tracker tests — fix plan

## Two layers (do not confuse)

**Layer A — Sonobuoy/Hydrophone E2E.** Upstream Ginkgo tests run against the live cluster. This is the conformance gate. The score that goes into `docs/CONFORMANCE.md` is Round-N Sonobuoy pass count.

**Layer B — Rust mirror tests.** `crates/*/tests/conformance_*.rs` are in-process unit/integration tests that mimic the upstream assertion shape. They are a fast-CI proxy for catching regressions; they are NOT the conformance gate.

A failed Sonobuoy test costs conformance score. An `#[ignore]`d Rust mirror does not — but it hides regressions and lies about coverage. Reaching 100% conformance means **every Layer A test passes**. Un-ignoring Layer B is a separate, parallel obligation.

## Scope

Every `#[ignore = "Conformance failure tracker — …"]` or `#[ignore = "Ratcheting tracker — …"]` in `crates/*/tests/conformance_*.rs`. 19 tests, 6 areas.

Out of scope: `#[ignore] // requires etcd`, `#[ignore = "perf microbenchmark …"]`, `#[ignore = "moved to …"]`, `protobuf_test::test_content_negotiation` (no tracker reason).

## Snapshot

- Sonobuoy Round 160: **415/441 passing (94.1%)** — 26 upstream tests still FAIL.
- Of the 19 ignored Rust mirrors here, **11 mirror upstream tests that currently FAIL in Sonobuoy** (items 1, 10–19). Fixing these moves the conformance score.
- **8 mirror ratcheting scenarios** (items 2–9) that the upstream `certified-conformance` mode does not exercise today. These do NOT currently block 100% conformance, but they will become relevant when KEP-4008 ratcheting promotes; treat them as future-coverage work.

## Categories

**A. Sonobuoy-blocking** — items 1, 10–19. Goal: get the cluster to pass the upstream Ginkgo test. The Rust mirror is secondary.

**B. Future-coverage** — items 2–9. Goal: build the Rust mirror so we are ready when upstream gates on ratcheting. Sonobuoy is not affected today.

## Per-test entries

Each entry shows both layers explicitly. Tick the boxes independently: a test can have Layer A green and Layer B still ignored, or vice versa.

---

## Area 1 — apimachinery-aggregation-discovery (1 test)

### 1. `aggregator_sample_apiserver_full_lifecycle`
- File: `crates/api-server/tests/conformance_apimachinery_aggregation_discovery.rs:493`
- Upstream: `[sig-api-machinery] Aggregator Should be able to support the 1.17 Sample API Server using the current Aggregator [LinuxOnly] [Conformance]` (`test/e2e/apimachinery/aggregator.go:102`)
- Sonobuoy Round 160: **FAIL** at `aggregator.go:359` waiting for the sample-apiserver pod to reach Ready.
- Root cause: the cluster's kubelet cannot get `registry.k8s.io/e2e-test-images/sample-apiserver` running. This is a real kubelet/runtime defect — Sonobuoy is exercising the production code path, not the test harness.

**[ ] Layer A (Sonobuoy fix — required for 100%)**
1. Reproduce: `kubectl --kubeconfig ~/.kube/rusternetes-config run sample-apiserver --image=registry.k8s.io/e2e-test-images/sample-apiserver:1.17.7 --restart=Never`. Watch the pod through to Ready.
2. Trace each stage that fails:
   - Image pull (`crates/kubelet/src/runtime.rs` / bollard `create_image`). Registry auth, mirror config, network from the kubelet container.
   - Container start (bollard `start_container`). Resource limits, readiness probe HTTP path.
   - Status reporting (kubelet → api-server). Pod phase + container statuses written back through `/status` subresource.
3. Fix whichever stage fails. Add a regression test under `crates/kubelet/tests/` that pulls + starts a small public image (e.g. `registry.k8s.io/pause:3.10`) and asserts Ready transition.
4. Re-run conformance — assert this test moves to PASS.

**[ ] Layer B (Rust mirror un-ignore)**
- Once Layer A holds, port the 19 sub-assertions from `aggregator.go:285–541` into the empty test body (APIService CRUD, status conditions, discovery merge, proxy forwarding, 503 on backend down). Drive against `MemoryStorage` + a stubbed aggregated API backend (no real container needed — the mirror exercises the REST + proxy layer, not the kubelet).
- Drop `#[ignore]`.

Complexity: **medium** (Layer A is the unknown; Layer B is straightforward once the cluster works).

---

## Area 2 — apimachinery-crd-lifecycle: ratcheting (8 tests, items 2–9)

All 8 currently have empty `{}` bodies. Shared dependency: a JSON-Schema diff engine.

Sonobuoy Round 160: **PASS** for all 8 — the upstream `certified-conformance` test set does not currently include the CRD ratcheting cases (they live in the broader `validation` test bucket gated on KEP-4008 GA promotion). These items do NOT block 100% conformance today.

When KEP-4008 promotes into certified-conformance, these become Layer-A blockers. Build the machinery now so the mirror catches regressions, and the future Layer A bump is free.

Shared fix-plan skeleton (do once, then fill each test body):
1. Add `SchemaDiff` in `crates/common/src/schema_validation.rs` that walks `(old_value, new_value, schema)` and returns a `Vec<JsonPath>` of changed nodes. Handle nested objects, arrays with `x-kubernetes-list-type`, maps-of-objects with `x-kubernetes-map-keys`, and unions.
2. Correlation detection: an entry is "correlatable" iff it can be matched between old and new (array-of-objects with `x-kubernetes-list-map-keys`). Conservative default: not correlatable.
3. Plumb a `ValidationScope { ValidateAll | ValidateChangedOnly(Vec<JsonPath>) }` through `crates/api-server/src/handlers/crd.rs::update_custom_resource` and `patch_custom_resource`. CREATE → `ValidateAll`; UPDATE → `ValidateChangedOnly`.
4. Update the schema validator to honour the scope.
5. In `crates/api-server/src/handlers/cel_validation.rs::evaluate_rules`, skip non-transition rules whose `fieldPath` lies outside the scope. Always evaluate transition rules (rules whose CEL AST references `oldSelf`).
6. Parse `optionalOldSelf`; on UPDATE, when scope reports the field as new, bind `oldSelf = nil`.

Layer A status for items 2–9: **already PASS** in current certified-conformance scope.

Layer B work below:

### 2. `ratcheting_unchanged_correlatable_jsonschema_errors_allowed`
- File: `…crd_lifecycle.rs:1268` · Upstream: `crd_validation_ratcheting.go:201`
- **[ ] Layer B**: implement shared steps 1–4. Test body: create CRD with stricter schema on an existing CR's unchanged field, PATCH the CR's other field, assert success.

### 3. `ratcheting_unchanged_uncorrelatable_jsonschema_errors_blocked`
- File: `…crd_lifecycle.rs:1276` · Upstream: `crd_validation_ratcheting.go:244`
- **[ ] Layer B**: same setup as #2 but field has no correlation key; assert PATCH rejected.

### 4. `ratcheting_changed_jsonschema_errors_blocked`
- File: `…crd_lifecycle.rs:1284` · Upstream: `crd_validation_ratcheting.go:280`
- **[ ] Layer B**: PATCH writes invalid value into a changed field; assert rejection.

### 5. `ratcheting_unchanged_correlatable_cel_errors_allowed`
- File: `…crd_lifecycle.rs:1292` · Upstream: `crd_validation_ratcheting.go:333`
- **[ ] Layer B**: install CEL rule that would fail on existing field; PATCH unrelated field; assert PATCH allowed.

### 6. `ratcheting_unchanged_uncorrelatable_cel_errors_blocked`
- File: `…crd_lifecycle.rs:1300` · Upstream: `crd_validation_ratcheting.go:412`
- **[ ] Layer B**: same as #5 but rule has no `fieldPath`; assert rejection.

### 7. `ratcheting_changed_cel_errors_blocked`
- File: `…crd_lifecycle.rs:1308` · Upstream: `crd_validation_ratcheting.go:448`
- **[ ] Layer B**: PATCH changes a value violating the rule; assert rejection.

### 8. `ratcheting_transition_rule_errors_never_ratcheted`
- File: `…crd_lifecycle.rs:1316` · Upstream: `crd_validation_ratcheting.go:511`
- **[ ] Layer B**: flag rules whose CEL AST references `oldSelf` at parse time. Always evaluate flagged rules regardless of scope.

### 9. `ratcheting_optional_old_self_nil_for_new_values`
- File: `…crd_lifecycle.rs:1324` · Upstream: `crd_validation_ratcheting.go:569`
- **[ ] Layer B**: extend `ValidationRule` with `optional_old_self: bool`; on UPDATE bind `oldSelf = nil` when the diff engine reports the field as new.

Complexity for items 2–9: **large** (shared multi-week schema-diff engine), individual test bodies trivial once the engine lands.

---

## Area 3 — network-services-proxy (5 tests)

### 10. `services_should_complete_service_status_lifecycle`
- File: `crates/kube-proxy/tests/conformance_network_services_proxy.rs:523`
- Upstream: `[sig-network] Services should complete a service status lifecycle [Conformance]` (`service.go:3246`, fails at 3459)
- Sonobuoy Round 160: **FAIL** — api-server / cloud-controller never populates `status.loadBalancer.ingress[]`.

**[x] Layer A — controller-manager retry + warning event (PR fix/loadbalancer-status-lifecycle)**
1. `crates/controller-manager/src/controllers/loadbalancer.rs::update_service_status` now retries the storage PATCH with exponential backoff (5 attempts, 100ms → 1.6s) and preserves `status.conditions` set by other controllers instead of clobbering them with `None`.
2. `ensure_load_balancer_with_retry` retries the cloud-provider call under the same budget.
3. On terminal failure both paths emit a Warning `Event` (`EnsuringLoadBalancerFailed` / `UpdateLoadBalancerStatusFailed`) against the Service so `kubectl describe svc` shows the failure state instead of just an empty status. The error then propagates back through the work-queue, which already rate-limits requeue.

**[x] Layer B — authentic mirror moved to controller-manager**
- The kube-proxy mirror at line 523 was structurally wrong (a `panic!()` placeholder testing controller-manager behavior from the wrong crate). It is now un-ignored as an authentic kube-proxy assertion (LB-typed Service programs ClusterIP + NodePort rules; after delete both rule classes disappear).
- The status-lifecycle behavior itself is covered by `crates/controller-manager/tests/loadbalancer_status_lifecycle_test.rs` with four test cases: happy-path populate-on-first-reconcile, retry-on-transient-failure (2 transient → success), Warning-event emission on terminal cloud-provider failure, and delete-mid-reconcile race tolerance. These use a stub `CloudProvider` so no network I/O is needed.

Complexity: **small–medium**.

### 11. `services_should_switch_session_affinity_nodeport`
- File: `…proxy.rs:640`
- Upstream: `[sig-network] Services should be able to switch session affinity for NodePort [LinuxOnly] [Conformance]` (`service.go:2287`, fails at 4291)
- Sonobuoy Round 160: **FAIL** — NodePort affinity reachability check times out.

**[ ] Layer A**
1. Audit `crates/kube-proxy/src/iptables.rs::emit_nodeport_rules` (~lines 1273–1340). When `session_affinity=ClientIP` and `recent_available=true`, confirm `KUBE-SEP-*` chains are inserted before direct DNAT in the NODEPORTS chain (iptables matches first hit).
2. Confirm `xt_recent --rcheck` is configured with the right table size; tune `RECENT_NAME` so multiple Services don't collide.
3. Run conformance again; if still failing, capture the iptables-save output and diff against upstream kube-proxy.

**[ ] Layer B**
- Build `svc_none` (no affinity) and `svc_clientip` (ClientIP) NodePort Services; call `build_nat_rules` for both; assert `assert_ne!(rules_none, rules_aff)` and that the affinity variant contains `KUBE-SEP-*` references referenced before direct DNAT. Mirror the diffing pattern used by the passing ClusterIP test (line 598).

Complexity: **small** (machinery exists; mostly diagnosis + test scaffolding).

### 12. `services_should_have_session_affinity_for_nodeport`
- File: `…proxy.rs:652`
- Upstream: `[sig-network] Services should have session affinity work for NodePort [LinuxOnly] [Conformance]` (`service.go:2265`)
- Sonobuoy Round 160: **FAIL** — same root path as #11.

**[ ] Layer A**: covered by the #11 fix (same iptables path).

**[ ] Layer B**: build a NodePort Service with `sessionAffinity: ClientIP` and 2+ EndpointSlice endpoints; assert `KUBE-SEP-*` references appear when `recent_available=true` and direct-DNAT fallback when `recent_available=false`.

Complexity: **small**.

### 13. `service_endpoints_latency_should_not_be_very_high`
- File: `…proxy.rs:711`
- Upstream: `[sig-network] Service endpoints latency should not be very high [Conformance]` (`service_latency.go:60`, fails at 145)
- Sonobuoy Round 160: **FAIL** — end-to-end propagation delay from Pod-Ready to EndpointSlice update is over threshold.

**[ ] Layer A**
1. Locate the EndpointSlice mirroring controller in `crates/controller-manager/src/controllers/`. Confirm it's event-driven (watch on Pod / Endpoints) and not polling.
2. Profile: time from Pod-Ready to EndpointSlice update visible in storage. Target: well under upstream p99 threshold (~500 ms in upstream test).
3. If polling, convert to a workqueue with watch-driven re-enqueue. If already event-driven, find the slow step (storage write, watch fan-out).
4. Re-run conformance.

**[ ] Layer B**
- Spawn the controller against `MemoryStorage`; write N services and ramp pods; assert p99 propagation time under threshold.

Complexity: **medium–large** (controller refactor possible).

### 14. `proxy_valid_responses_for_pod_and_service`
- File: `…proxy.rs:986`
- Upstream: `[sig-network] Proxy version v1 [Conformance] A set of valid responses are returned for both pod and service Proxy` (`proxy.go:432`, fails at 503)
- Sonobuoy Round 160: **FAIL** — combined pod-proxy + service-proxy flow.

**[ ] Layer A**
1. Re-read upstream `proxy.go:432–503` end-to-end to map every assertion.
2. Confirm both proxy routes are registered in `crates/api-server/src/router.rs`: `/api/v1/namespaces/{ns}/pods/{name}/proxy/{path}` and `/api/v1/namespaces/{ns}/services/{name}/proxy/{path}`.
3. In `crates/api-server/src/handlers/proxy.rs`, verify `proxy_service` resolves through EndpointSlice (~lines 295–328) to a single endpoint and that `proxy_pod` extracts pod IP + port.
4. Walk every response code the upstream asserts (200, 301/302 follow, body content); fix whichever path mis-codes.

**[ ] Layer B**
- Integration test: Service → EndpointSlice → Pod chain; proxy through Service and assert it reaches Pod; direct Pod proxy hits the same Pod.

Complexity: **medium**.

---

## Area 4 — node-exec-logs-downward (2 tests)

### 15. `pod_exec_over_websocket_query_format_matches_upstream`
- File: `crates/kubelet/tests/conformance_node_exec_logs_downward.rs:590`
- Upstream: `[sig-node] Pods should support remote command execution over websockets` (`pods.go:517`)
- Sonobuoy Round 160: **FAIL** — end-to-end exec-over-websocket round-trip.

**[ ] Layer A**
1. Audit the full path: client → api-server `/exec` (`pod_subresources.rs:129`) → `handle_exec_websocket_with_protocol` (line 592) → `streaming.rs::handle_ws_exec` → kubelet exec endpoint (bollard exec → container).
2. Confirm `v4.channel.k8s.io` and `v5.channel.k8s.io` subprotocols are negotiated and that channel bytes (0=stdin, 1=stdout, 2=stderr, 3=err, 4=resize for v5) are wired through bollard.
3. Walk through with a manual `kubectl exec -it` against a real pod; capture failure point.

**[ ] Layer B**
- Pure query-format unit test (the file name suggests this). Build the expected URL via `url::form_urlencoded`, assert byte-for-byte match with the upstream constructor. Drop `#[ignore]` once it passes.

Complexity: **small–medium**.

### 16. `pod_log_over_websocket_query_is_container_only`
- File: `…exec_logs_downward.rs:609`
- Upstream: `[sig-node] Pods should support retrieving logs from the container over websockets` (`pods.go:583`)
- Sonobuoy Round 160: **FAIL** — log endpoint sends plain text instead of channel-prefixed binary frames.

**[ ] Layer A**
1. Generalise the binary framer in `crates/api-server/src/streaming.rs` (currently exec-specific) so it can wrap log output with channel 1 = stdout.
2. Switch the `get_logs()` websocket branch in `pod_subresources.rs:204–214` to the new framer.
3. Verify the `binary.k8s.io` (or equivalent) subprotocol is negotiated.

**[ ] Layer B**
- Assert the query string contains `container=<name>` exactly once and that the websocket emits framed binary output on channel 1.

Complexity: **small–medium**.

---

## Area 5 — scheduling-priority-preemption-hostport (1 test)

### 17. `preemption_execution_path_replicaset_available_replicas`
- File: `crates/scheduler/tests/conformance_scheduling_priority_preemption_hostport.rs:347`
- Upstream: `[sig-scheduling] SchedulerPreemption [Serial] PreemptionExecutionPath runs ReplicaSets to verify preemption running path` (`preemption.go:756`, fails at 1025)
- Sonobuoy Round 160: **FAIL** — after preemption, recreated RS pod never becomes Ready in time; `status.availableReplicas` stays below desired.

**[ ] Layer A**
1. In `crates/scheduler/src/advanced.rs` (~`check_preemption` line 826), confirm victims are actually deleted (not just marked) and that the deletion flows through to kubelet pod-stop quickly.
2. In `crates/controller-manager/src/controllers/replicaset.rs`, verify the controller watches Pod deletions (not periodic poll) and re-enqueues the parent RS. Confirm `update_status` (~lines 560–630) writes `availableReplicas` once new pods reach Ready.
3. Time-budget audit: scheduler→kubelet→api-server→RS-controller→status-update must complete inside the upstream test's `framework.PodStartTimeout` (~5 min for the full cycle, but each step should be sub-second).
4. Tighten any slow step (workqueue re-enqueue, watch latency).

**[ ] Layer B**
- `tokio::test` wiring scheduler + RS controller against `MemoryStorage`; low-priority RS fills capacity; high-priority pod preempts; assert RS regains `availableReplicas == desired` within deadline.

Complexity: **medium–large** (cross-component).

---

## Area 6 — apps-deployment-replicaset (2 tests)

Both upstream tests curl every replica pod IP and verify HTTP 200 from a real public image (`nginx:stable-alpine` / similar). Passing them means the whole stack works: kubelet pulls real images, container starts, pod networking gives the pod a reachable IP, Service / EndpointSlice routes traffic, kube-proxy programs iptables that actually forward.

Earlier framing of "leave ignored, this is e2e-only" was wrong: it would let the Rust mirror hide the gap, but it does nothing for Sonobuoy. Sonobuoy must pass these. The Rust mirror has two honest options — drive bollard or remove it — picked below.

### 18. `replicaset_should_serve_basic_image_on_each_replica`
- File: `crates/controller-manager/tests/conformance_apps_deployment_replicaset.rs:831`
- Upstream: `[sig-apps] ReplicaSet should serve a basic image on each replica with a public image` (`replica_set.go:95`)
- Sonobuoy Round 160: **FAIL** — full E2E (pull image + serve HTTP + pod-IP reachable + curl works).

**[ ] Layer A** (this is the conformance work — share with #19)
1. Verify kubelet pulls and starts `nginx:stable-alpine` against the live cluster (same image-pull path audited in #1).
2. Verify pod is assigned a reachable IP via the kubelet's CNI/network setup (`crates/kubelet/src/network*.rs` or equivalent).
3. From inside another pod or from the host on the cluster network, `curl http://<pod-ip>:80/` must return HTTP 200.
4. RS controller: ReplicaSet of 3 must report `status.availableReplicas == 3` once all three are Ready (same status-reconciliation work as #17).
5. Re-run conformance; assert PASS.

**[ ] Layer B** — pick one:
- (a) Convert the mirror to a bollard-driven smoke test: spawn nginx via the local Docker daemon, bind to an ephemeral host port, curl `http://127.0.0.1:<port>/`. Covers image-pull + container-start + HTTP-serving, NOT pod networking. Honest about scope.
- (b) Delete the mirror entirely. Sonobuoy is authoritative; the mirror added no signal in its `#[ignore]`d state and a stand-in (a) only covers part of the stack.
- Recommendation: **(b) for now**. The pod-networking and Service-routing pieces are covered by the kube-proxy mirrors (Area 3). If a regression in image-pull / runtime is a real risk, add a dedicated `crates/kubelet/tests/integration_bollard_pull_and_serve.rs` rather than smuggling it into the RS controller's mirror.

Complexity: Layer A **medium–large** (overlaps Area 5 + #1). Layer B **trivial** (delete + matrix update).

### 19. `rc_should_serve_basic_image_on_each_replica`
- File: `…deployment_replicaset.rs:1100`
- Upstream: `[sig-apps] ReplicationController should serve a basic image on each replica with a public image` (`rc.go:65`)
- Sonobuoy Round 160: **FAIL** — same root cause as #18.

**[ ] Layer A**: covered by the #18 work. Same fix unlocks both.

**[ ] Layer B**: same options as #18; recommendation: delete this mirror too, for the same reason. The `ReplicationControllerController` reconciler is unit-covered by `rc_publishes_status_after_reconcile` already.

Complexity: same as #18.

---

## Implementation order

Drive Sonobuoy score up first; pick up the Layer-B leftovers in the wake.

1. **Round 161 target — Sonobuoy-blocking, isolated fixes** (items 10, 11, 12, 14, 15, 16) — single-component work, predictable. Expected delta: +5 to +6 tests passing.
2. **Round 162 target — Cross-component fixes** (items 1, 13, 17, 18, 19) — kubelet image-pull, controller-manager latency, preemption→RS-status cycle. These share infrastructure; doing the kubelet image-pull work for #1 unlocks #18/#19. Expected delta: another +5.
3. **Round 163+ — ratcheting machinery** (items 2–9) — multi-week, parallel to conformance work. Lands when KEP-4008 promotes or sooner if we want CI safety.

After each round: run `bash scripts/run-conformance.sh`, update `docs/CONFORMANCE.md` round counter + failure bucket counts, update area-specific matrices in `docs/conformance/<area>.md`, tick the Layer-A and/or Layer-B boxes here.

## Verification per item

Layer A (Sonobuoy): full conformance suite — `bash scripts/run-conformance.sh` (or the per-test single-test runner from PR #119 once merged). Compare junit before/after.

Layer B (Rust mirror): `cargo test -p <crate> --test <test_file> <test_name> -- --exact`, then `make pre-commit`.

## Tracking

This file is the source of truth for ignored-tracker progress. For each item, tick Layer A when Sonobuoy passes upstream, tick Layer B when the Rust mirror passes without `#[ignore]`. The 100% conformance goal is "every Layer A box ticked"; Layer B is a parallel hygiene obligation.
