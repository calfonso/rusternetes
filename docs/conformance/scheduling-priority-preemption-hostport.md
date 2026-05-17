# [sig-scheduling] Priority + Preemption + HostPort — scoped conformance coverage

Crate: `crates/scheduler` · Test file: `tests/conformance_scheduling_priority_preemption_hostport.rs`

Mirrors the Kubernetes v1.35 Sonobuoy conformance scenarios for PriorityClass
resolution, scheduler preemption, and HostPort conflict detection. Drives the
scheduler crate directly (no HTTP harness) because every behavior in scope is
implemented as a pure helper in `rusternetes_scheduler::advanced` plus the
`PriorityClass` resource model in `rusternetes-common`.

Upstream source tree:
<https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/scheduling/>
and
<https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/network/>

Extraction source: `.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`.

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `PriorityClass explicit value wins over class name` | priorities.go (admission resolution) | PASS (implicit) | `priority_class_explicit_value_wins_over_class_name` | mirrored, passing |
| `PriorityClass name resolves to class value` | priorities.go | PASS | `priority_class_name_resolves_to_class_value` | mirrored, passing |
| `PriorityClass globalDefault applies` | priorities.go | PASS | `priority_class_global_default_applies_to_pods_without_class` | mirrored, passing |
| `PriorityClass value ordering low < medium < high < system-critical` | preemption.go:697-699 | PASS | `priority_class_values_order_low_medium_high` | mirrored, passing |
| `SchedulerPreemption validates basic preemption works` | preemption.go:218 | PASS | `preemption_evicts_lower_priority_pod_to_fit_high_priority` | mirrored, passing |
| `SchedulerPreemption skips equal-priority pods` | preemption.go (basic suite) | PASS | `preemption_skips_when_only_equal_priority_pods_present` | mirrored, passing |
| `SchedulerPreemption respects preemptionPolicy=Never` | preemption.go:~479 | PASS | `preemption_skipped_when_pod_has_preemption_policy_never` | mirrored, passing |
| `SchedulerPreemption protects system-critical pods` | preemption.go:697-699 | PASS | `preemption_protects_system_critical_pods_from_lower_priority_preemptor` | mirrored, passing |
| `SchedulerPreemption [Serial] PreemptionExecutionPath runs ReplicaSets to verify preemption running path` | preemption.go:756 | **FAIL** — `replicaset "rs-pod1" never had desired number of .status.availableReplicas` (preemption.go:1025) | `preemption_execution_path_replicaset_available_replicas` | mirrored, ignored (tracks failure) |
| `HostPort same port + same hostIP conflicts` | hostport.go:63 | PASS | `hostport_same_port_same_host_ip_conflicts` | mirrored, passing |
| `HostPort no conflict between pods with same hostPort but different hostIP and protocol [LinuxOnly] [Conformance]` | hostport.go:219 | **FAIL** — `wait for pod2 timeout 300s` (kubelet scheduling timing, not filter logic) | `hostport_same_port_different_host_ip_does_not_conflict` | mirrored, passing (scheduler-side invariant only) |
| `HostPort same port different protocol does not conflict` | hostport.go:219 (matrix half) | PASS | `hostport_same_port_different_protocol_does_not_conflict` | mirrored, passing |
| `HostPort wildcard hostIP conflicts with specific hostIP` | hostport.go (wildcard matrix) | PASS | `hostport_wildcard_host_ip_conflicts_with_specific_host_ip` | mirrored, passing |
| `HostPort terminated pods do not block allocation` | hostport.go (implicit) | PASS | `hostport_terminated_pods_do_not_conflict` | mirrored, passing |

## Failure analysis (R160)

Two scenarios in this slice are currently red on the full Sonobuoy run. Per
the `/batch` convention, the scheduler-only invariants they exercise are
mirrored where possible; the cross-component pieces are tracked as `#[ignore]`d
marker tests.

### `preemption.go:1025` — `replicaset "rs-pod1" never had desired number of .status.availableReplicas`

Upstream test: `[sig-scheduling] SchedulerPreemption [Serial]
PreemptionExecutionPath runs ReplicaSets to verify preemption running path`
(entry at `preemption.go:756`).

Symptom: after the preemptor evicts a victim, the recreated ReplicaSet pod
never becomes Ready, so `availableReplicas` stays below desired and the test
times out.

Why scoped tests can't reach the same failure path: the scenario is a four-way
interplay between the scheduler, the controller-manager (ReplicaSet
controller), the kubelet (pod startup), and the api-server (status updates).
Even with `MemoryStorage`, the scheduler-only unit cannot recreate the
`availableReplicas` reconciliation loop. The doc fragment records the failure
and the test compiles as an `#[ignore]`d marker.

Falls under bucket: `apps controllers ~3` / `node lifecycle ~3` from
`docs/CONFORMANCE.md:40–53`.

### `hostport.go:219` — `wait for pod2 timeout 300s`

Upstream test: `[sig-network] HostPort validates that there is no conflict
between pods with same hostPort but different hostIP and protocol`.

Symptom: pod1 schedules and runs; pod2 — which asks for the same hostPort on
a *different* hostIP — never reaches Running within 5 min. The Ginkgo failure
is on the pod-wait, not on the scheduler's filter decision.

Scheduler-side invariant verified locally:
`hostport_same_port_different_host_ip_does_not_conflict` passes against
`check_host_port_conflicts`, confirming the filter plugin would admit pod2 on
the node. The remaining failure is in kubelet or the cluster bootstrap that
prevents the second pod from actually starting on the chosen interface.

Falls under bucket: `service networking ~6` (cross-listed in the kubelet/node
buckets in `docs/CONFORMANCE.md:40–53`).
