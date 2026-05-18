# [sig-apps] StatefulSet + DaemonSet — scoped conformance coverage

Crate: `crates/controller-manager` · Test file: `tests/conformance_apps_statefulset_daemonset.rs`

This unit mirrors the Kubernetes v1.35 conformance scenarios for the
StatefulSet and DaemonSet controllers — `OrderedReady` vs `Parallel` pod
management, partitioned canary rollouts, eviction recovery, scale
subresource, RollingUpdate with `maxUnavailable`, node-selector targeting,
status-field reconciliation, and namespace isolation. The goal is a sub-
second `cargo test` signal that complements the hour-long Sonobuoy run
captured in `.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`
and `.rusternetes/volumes/sonobuoy-e2e-job-53eadf2451e4467c/results/e2e.log`
(the second log captures the StatefulSet rolling-update test verbatim at
line 21370).

Cross-reference: `docs/CONFORMANCE.md` failure bucket **"Other"** (R160).
The DaemonSet and StatefulSet conformance scenarios live in that bucket
because Sonobuoy currently cannot drive them end-to-end against the live
cluster (kubelet eviction + ControllerRevision races); the scoped mirrors
here run the controller reconcile logic in milliseconds and expose the same
invariants without needing a real kubelet.

Unlike the api-server units, this file does **not** spin up the axum router.
Both controllers consume the `Storage` trait directly, so tests drive
`StatefulSetController::reconcile_all()` and
`DaemonSetController::reconcile_all()` against an `Arc<MemoryStorage>`.
Where a real kubelet would normally reap pods that the controller marked
for deletion, `simulate_kubelet_cleanup` replays that step synchronously so
the controller's view of storage stays consistent across reconcile cycles.
That matches the prior art in
`crates/controller-manager/tests/{statefulset_controller_test.rs,daemonset_controller_test.rs,daemonset_controller_revision_test.rs}`.

## Coverage matrix

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `StatefulSet should perform rolling updates and roll backs of template modifications` | apps/statefulset.go:360 | PASS (job 53eadf2451e4467c line 21370) | `statefulset_should_perform_rolling_updates_and_rollbacks_of_template_modifications` | mirrored, passing |
| `StatefulSet should perform canary updates and phased rolling updates of template modifications` | apps/statefulset.go:376 | PASS | `statefulset_should_perform_canary_updates_with_partition` | mirrored, passing |
| `StatefulSet Scaling should happen in predictable order and halt if any stateful pod is unhealthy` | apps/statefulset.go:593 | PASS | `statefulset_scaling_should_happen_in_predictable_order_with_ordered_ready` | mirrored, passing |
| `StatefulSet Burst scaling should run to completion even with unhealthy pods` | apps/statefulset.go:616 | PASS | `statefulset_burst_scaling_should_run_to_completion_with_parallel_policy` | mirrored, passing |
| `StatefulSet Should recreate evicted statefulset` | apps/statefulset.go:641 | PASS | `statefulset_should_recreate_evicted_pod_with_same_ordinal` | mirrored, passing |
| `StatefulSet should have a working scale subresource` | apps/statefulset.go:714 | PASS | `statefulset_should_have_a_working_scale_subresource` | mirrored, passing |
| `StatefulSet should list, patch and delete a collection of StatefulSets` | apps/statefulset.go:760 | PASS | `statefulset_should_list_patch_and_delete_collection` | mirrored, passing |
| `StatefulSet should validate Statefulset Status endpoints` | apps/statefulset.go:811 | PASS | `statefulset_should_validate_status_endpoint_fields` | mirrored, passing |
| `StatefulSet AvailableReplicas should get updated accordingly when MinReadySeconds is enabled` | apps/statefulset.go (MinReadySeconds suite) | FAIL ("Other" bucket) | `statefulset_available_replicas_should_track_min_ready_seconds` | mirrored, ignored — `availableReplicas` / `minReadySeconds` not implemented in the controller |
| `StatefulSet PVC retention — whenScaled=Delete reclaims PVCs` | apps/statefulset.go (PVC retention suite) | PASS | `statefulset_pvc_retention_policy_should_delete_pvcs_on_scale_down` | mirrored, passing — `gc_scaled_down_pvcs` deletes PVCs for ordinals beyond `spec.replicas` when `whenScaled=Delete` |
| `StatefulSet PVC retention — whenScaled=Retain keeps PVCs` | apps/statefulset.go (PVC retention suite) | PASS | `statefulset_pvc_retention_policy_retain_keeps_pvcs_on_scale_down` | mirrored, passing (default Retain behaviour) |
| `StatefulSet pods bind their headless Service via subdomain` | apps/statefulset.go (identity tests) | FAIL ("Other" bucket) | `statefulset_pods_should_bind_headless_service_via_subdomain` | mirrored, ignored — controller does not stamp `pod.spec.subdomain` from `serviceName` |
| `Daemon set [Serial] should run and stop simple daemon` | apps/daemon_set.go:240 | FAIL ("Other" bucket — DaemonSet) | `daemonset_should_run_and_stop_simple_daemon` | mirrored, passing |
| `Daemon set [Serial] should run and stop complex daemon` | apps/daemon_set.go:258 | FAIL ("Other" bucket — DaemonSet) | `daemonset_should_run_and_stop_complex_daemon_with_node_selector` | mirrored, passing |
| `Daemon set [Serial] should retry creating failed daemon pods` | apps/daemon_set.go:368 | FAIL ("Other" bucket — DaemonSet) | `daemonset_should_retry_creating_failed_daemon_pods` | mirrored, passing |
| `Daemon set [Serial] should update pod when spec was updated and update strategy is RollingUpdate` | apps/daemon_set.go:427 | FAIL ("Other" bucket — DaemonSet) | `daemonset_should_rolling_update_pods_when_spec_changes` | mirrored, passing |
| `Daemon set [Serial] should rollback without unnecessary restarts` | apps/daemon_set.go:493 | FAIL ("Other" bucket — DaemonSet) | `daemonset_should_rollback_without_unnecessary_restarts` | mirrored, passing |
| `Daemon set [Serial] should list and delete a collection of DaemonSets` | apps/daemon_set.go:603 | FAIL ("Other" bucket — DaemonSet) | `daemonset_should_list_and_delete_collection` | mirrored, passing |
| `Daemon set [Serial] should verify changes to a daemon set status` | apps/daemon_set.go:646 | FAIL ("Other" bucket — DaemonSet) | `daemonset_should_verify_status_field_changes` | mirrored, passing |
| `Daemon set lifecycle — pod added when node joins` | apps/daemon_set.go (lifecycle subtests) | PASS | `daemonset_should_add_pod_when_node_joins` | mirrored, passing |
| `Daemon set lifecycle — pod removed when node leaves` | apps/daemon_set.go (lifecycle subtests) | PASS | `daemonset_should_remove_pod_when_node_leaves` | mirrored, passing |
| `Daemon set multi-namespace isolation` | apps/daemon_set.go (multi-namespace) | PASS | `daemonset_namespaces_are_isolated` | mirrored, passing |

## Notes on intentional scope reductions

- **`availableReplicas` / `minReadySeconds`** — the StatefulSet controller
  populates `replicas`, `currentReplicas`, and `updatedReplicas` but does
  not yet compute `availableReplicas` from `minReadySeconds`. The mirror
  test is `#[ignore]`d so a future enablement is one re-run away.
- **PVC retention (`whenScaled=Delete`)** — `ensure_pvcs_for_ordinal`
  creates PVCs from `volumeClaimTemplates`, and `gc_scaled_down_pvcs`
  reclaims PVCs whose ordinal is beyond `spec.replicas` when the policy's
  `whenScaled` is `Delete`. The `whenDeleted` half (StatefulSet-level
  teardown) still relies on default GC semantics. The `Retain` variant
  is also exercised because that is the default behaviour.
- **Headless Service binding** — upstream stamps
  `pod.spec.subdomain = statefulset.spec.serviceName` so DNS A records
  resolve under the headless Service. Our `create_pod` path leaves
  `subdomain` empty; the mirror is `#[ignore]`d until that field is set.
- **Eviction simulation** — Sonobuoy relies on the kubelet to actually
  eject a pod. We emulate that by deleting the pod from storage outright
  before reconciling, which is the minimum signal needed to verify the
  controller's recovery loop.
- **Rolling updates** — the loop counts (`for _ in 0..N`) are sized so
  each test completes within a handful of reconcile cycles; if a future
  refactor changes the per-cycle deletion budget, the assertion message
  identifies which invariant broke first.
