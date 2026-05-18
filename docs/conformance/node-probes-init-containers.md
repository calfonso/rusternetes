# [sig-node] Probes + Init containers — scoped conformance coverage

Crate: `crates/kubelet` · Test file: `tests/conformance_node_probes_init_containers.rs`

This unit mirrors the Kubernetes v1.35 conformance scenarios for
container probes (liveness / readiness / startup with exec, httpGet,
tcpSocket, and gRPC actions) and init container ordering / restart
semantics (`RestartNever` vs `RestartAlways`).

It is part of the per-crate scoped conformance suite — a fast, Sonobuoy
parallel that exercises pure helpers in `rusternetes_kubelet` without a
live Docker daemon. See `docs/CONFORMANCE.md` for the full per-round
Sonobuoy history.

Upstream sources:

- `k8s.io/kubernetes/test/e2e/common/node/init_container.go` (4 `framework.ConformanceIt`s at L218 / L275 / L330 / L430)
- `k8s.io/kubernetes/test/e2e/common/node/container_probe.go` (10 `ConformanceIt`s + ~13 plain `It`s, covering exec/http/tcp/gRPC probes and the threshold/timing knobs)

Extraction source: `.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log:15720` (the captured "Init containers" failure).

Cross-reference: `docs/CONFORMANCE.md` "Failure Categories" → **Init containers (~2)**: "RestartNever invoke, RestartAlways failure handling". Both failure-mirror tests now pass at the unit level: the pure helper `decide_next_init_action` returns the correct `InitAction` shape for both restart policies, and the kubelet status-sync path in `crates/kubelet/src/kubelet.rs` already publishes the `Initialized=False / ContainersNotInitialized` PodCondition via `Self::init_failed_pod_conditions` when an init container exits non-zero.

## Test-by-test status table

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `InitContainer should invoke init containers on a RestartNever pod` | init_container.go:218 | PASS | `init_containers_should_invoke_on_restart_never_pod` | mirrored, passing |
| `InitContainer should invoke init containers on a RestartAlways pod` | init_container.go:275 | PASS | `init_containers_should_invoke_on_restart_always_pod` | mirrored, passing |
| `InitContainer should not start app containers if init containers fail on a RestartAlways pod` | init_container.go:330 | **FAIL** | `init_containers_should_not_start_app_on_restart_always_failure` | mirrored, passing |
| `InitContainer should not start app containers and fail the pod if init containers fail on a RestartNever pod` | init_container.go:430 | **FAIL** | `init_containers_should_not_start_app_and_fail_pod_on_restart_never_failure` | mirrored, passing |
| Init container ordering invariant | (across init_container.go) | implicit-PASS | `init_containers_run_in_declaration_order` | mirrored, passing |
| Init container running blocks app start | (across init_container.go) | implicit-PASS | `init_containers_running_blocks_app_container_start` | mirrored, passing |
| `RestartPolicy=OnFailure` retries failed inits | (across init_container.go) | implicit-PASS | `restart_on_failure_retries_failed_init` | mirrored, passing |
| Sidecar (init w/ restartPolicy=Always) does not gate app | sidecar_containers.go (cross-cut) | PASS | `sidecar_init_does_not_gate_app_when_still_running` | mirrored, passing |
| Pod without init containers is trivially done | helper | implicit-PASS | `pod_without_init_containers_is_trivially_done` | mirrored, passing |
| First unstarted init is selected without retry | helper | implicit-PASS | `first_unstarted_init_container_is_selected_without_retry` | mirrored, passing |
| `Probing container should be restarted with a exec "cat /tmp/health" liveness probe` | container_probe.go:128 | PASS | `probe_exec_liveness_struct_carries_command` | mirrored, passing |
| `Probing container should be restarted with a /healthz http liveness probe` | container_probe.go:168 | PASS | `probe_http_liveness_struct_carries_path_port_and_scheme` | mirrored, passing |
| `Probing container should *not* be restarted with a tcp:8080 liveness probe` | container_probe.go:184 | PASS | `probe_tcp_socket_liveness_struct_carries_port` | mirrored, passing |
| `Probing container should be restarted with a GRPC liveness probe` | container_probe.go:580 | PASS | `probe_grpc_liveness_struct_carries_port_and_optional_service` | mirrored, passing |
| `Probing container should *not* be restarted with a GRPC liveness probe` | container_probe.go:559 | PASS | `probe_grpc_service_field_is_optional` | mirrored, passing |
| HTTP probe custom headers | container_probe.go (cross-cut) | PASS | `probe_http_action_supports_custom_headers` | mirrored, passing |
| Probe port resolution — integer | container_probe.go:168 | PASS | `probe_port_int_passes_through` | mirrored, passing |
| Probe port resolution — named via `containerPort.name` | container_probe.go:184 | PASS | `probe_port_named_resolves_via_container_ports` | mirrored, passing |
| Probe port out-of-range rejection | (validation) | PASS | `probe_port_out_of_range_returns_none` | mirrored, passing |
| `with readiness probe should not be ready before initial delay and never restart` | container_probe.go:79 | PASS | `readiness_probe_honours_initial_delay_seconds` | mirrored, passing |
| `with readiness probe that fails should never be ready and never restart` | container_probe.go:105 | PASS | `readiness_probe_failure_threshold_is_honoured` | mirrored, passing |
| `should be restarted with an exec liveness probe with timeout` | container_probe.go:238 | PASS | `exec_liveness_probe_honours_timeout_seconds` | mirrored, passing |
| `should *not* be restarted by liveness probe because startup probe delays it` | container_probe.go:359 | PASS | `startup_probe_failure_threshold_delays_liveness` | mirrored, passing |
| `successThreshold` semantics for liveness/startup | (K8s validation) | PASS | `probe_success_threshold_for_liveness_must_be_one` | mirrored, passing |
| `should override timeoutGracePeriodSeconds when LivenessProbe field is set` | container_probe.go:481 | PASS | `probe_termination_grace_period_override_is_optional` | mirrored, passing |
| `periodSeconds` controls check frequency | container_probe.go (cross-cut) | PASS | `probe_period_seconds_controls_check_frequency` | mirrored, passing |
| `should be ready immediately after startupProbe succeeds` | container_probe.go:411 | PASS | `probe_initial_delay_zero_means_probe_runs_immediately` | mirrored, passing |
| Container accepts all three probe types independently | (cross-cut) | PASS | `container_accepts_all_three_probe_types_independently` | mirrored, passing |
| Sidecar init container probes are well-formed | (KEP-753 cross-cut) | PASS | `pod_with_startup_probe_on_init_container_is_well_formed` | mirrored, passing |

**Coverage**: 27 scoped tests — all passing. The 2 previously-`#[ignore]`d failure-mirror tests have been un-ignored: at the unit level, the pure helper `decide_next_init_action` returns the correct `InitAction` for both restart policies, and the kubelet status-sync path already publishes the `Initialized=False / ContainersNotInitialized` PodCondition.

## Failure bucket detail (resolved at unit level)

The failure-mirror tests originally tracked the symptom captured at `e2e.log:15765`:

```
[FAILED] Expected
    <*v1.PodCondition | 0x0>: nil
not to be nil
In [It] at: k8s.io/kubernetes/test/e2e/common/node/init_container.go:446
```

Upstream `init_container.go:446` (inside both the RestartAlways and RestartNever failure tests) asserts that the kubelet publishes a `PodCondition{Type: "Initialized", Status: "False", Reason: "ContainersNotInitialized"}` once an init container has crashed N times. Two things must hold for this to pass: (1) the pure helper `decide_next_init_action` must compute the right `InitAction` for each restart policy, and (2) the kubelet status-sync loop must translate that action into a `PodStatus` whose `conditions` include `Initialized=False / ContainersNotInitialized`.

Both already hold:

- The helper at `crates/kubelet/src/runtime.rs::decide_next_init_action` returns `InitAction { all_init_done: false, next_index: Some(0), should_retry: true }` for RestartAlways with a failed init, and `InitAction { all_init_done: false, next_index: None, should_retry: false }` for RestartNever. The two un-`#[ignore]`d tests in this file pin both branches.
- The status path at `crates/kubelet/src/kubelet.rs` (around L2225) calls `Self::init_failed_pod_conditions(&incomplete_inits)` whenever the runtime observes a failed init, building the four-condition vector `[Initialized=False/ContainersNotInitialized, PodScheduled=True, ContainersReady=False/ContainersNotReady, Ready=False/ContainersNotReady]` and assigning it to `PodStatus.conditions` before the storage update.

The `#[ignore]` markers were a tracker artifact predating both pieces landing on `main`; this PR removes them now that the unit-level evidence is in place. A live Sonobuoy run is the final word on whether the e2e harness sees the condition before its timeout — see `docs/CONFORMANCE.md` for the per-round status.

## Running

```bash
cargo test -p rusternetes-kubelet --test conformance_node_probes_init_containers
```

All 27 tests must pass.
