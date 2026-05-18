# [sig-apps] Deployment + ReplicaSet + ReplicationController — scoped conformance coverage

Crate: `crates/controller-manager` · Test file: `tests/conformance_apps_deployment_replicaset.rs`

This unit mirrors the Kubernetes v1.35 conformance scenarios that exercise the
three workload controllers in `sig-apps`:

- **Deployment** — rolling and recreate strategies, paused rollouts, scaling,
  rollback, `maxSurge` / `maxUnavailable` knobs, `/scale` subresource,
  lifecycle (create / patch / scale / delete), status endpoint
- **ReplicaSet** — basic image serving, adopt / release, `/scale`,
  list / deleteCollection, status endpoint, self-healing
- **ReplicationController** — basic image serving, adopt / release, lifecycle,
  `/scale`, self-healing, ReplicaFailure status surface

The tests drive `DeploymentController`, `ReplicaSetController`, and
`ReplicationControllerController` directly against `Arc<MemoryStorage>`.
Unlike the api-server-owned units in this batch there is no axum harness —
the contract under test belongs to the reconcile loops, not the REST surface,
and the existing pattern in `crates/controller-manager/tests/` already calls
`reconcile_all().await` against `MemoryStorage`. We reuse that style verbatim
so the scoped suite stays in the sub-second cargo-test budget.

Cross-reference: `docs/CONFORMANCE.md` failure bucket **"Apps controllers"**
(~3 failures in Round 160 — `docs/CONFORMANCE.md:48`). The remaining known
failures in this slice are:

1. ~~`Deployment should support rollover` — `deployment.go:129`~~ — the
   controller-level mirror (`deployment_should_support_rollover`) is now
   active and passing locally as of 2026-05-18: the v3 RS converges to the
   desired replica count after a mid-rollout template flip. The end-to-end
   Sonobuoy verdict is still tracked in `docs/CONFORMANCE.md` pending a
   fresh run.
2. ~~`Deployment should support proportional scaling` — `deployment.go:154`~~
   — the controller-level mirror
   (`deployment_should_support_proportional_scaling`) is now active and
   passing locally: a scale event mid-rollout raises the active RS to the
   new desired count without overshooting `desired + maxSurge`. The
   end-to-end Sonobuoy verdict is still tracked in `docs/CONFORMANCE.md`
   pending a fresh run.
3. `ReplicaSet / ReplicationController should serve a basic image on each
   replica with a public image` — `replica_set.go:95`, `rc.go:65` — the
   end-to-end image-serving contract fails because the conformance check
   curls each pod IP. Tracked via
   `replicaset_should_serve_basic_image_on_each_replica` and
   `rc_should_serve_basic_image_on_each_replica`.

`Deployment paused` is now honored — `reconcile_deployment` short-circuits to
a status-only path when `spec.paused` is true, so a template hash change does
not trigger a new ReplicaSet. The mirror test
`deployment_paused_should_not_create_new_replicaset_on_template_change`
exercises that guarantee.

## Coverage matrix

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `RollingUpdateDeployment should delete old pods and create new ones` | deployment.go:106 | PASS | `deployment_rolling_update_should_delete_old_pods_and_create_new_ones` | mirrored, passing |
| `RecreateDeployment should delete old pods and create new ones` | deployment.go:113 | PASS | `deployment_recreate_should_delete_old_pods_before_creating_new_ones` | mirrored, passing |
| `deployment should delete old replica sets` | deployment.go:121 | PASS | `deployment_should_track_old_replicasets_for_history` | mirrored, passing |
| `deployment should support rollover` | deployment.go:129 | FAIL | `deployment_should_support_rollover` | mirrored, passing locally (2026-05-18); upstream verdict pending re-run |
| `Deployment should have a working scale subresource` | deployment.go:144 | PASS | `deployment_scale_subresource_changes_replicaset_size` | mirrored, passing |
| `deployment should support proportional scaling` | deployment.go:154 | FAIL | `deployment_should_support_proportional_scaling` | mirrored, passing locally (2026-05-18); upstream verdict pending re-run |
| `should run the lifecycle of a Deployment` | deployment.go:207 | PASS | `deployment_lifecycle_create_scale_patch_delete` | mirrored, passing |
| `should validate Deployment Status endpoints` | deployment.go:216 | PASS | `deployment_status_replicas_match_replicaset_pods` | mirrored, passing |
| Strategy: paused deployment should not progress | deployment.go (paused-rollout helper) | PASS | `deployment_paused_should_not_create_new_replicaset_on_template_change` | mirrored, passing |
| Strategy: RollingUpdate maxSurge=0 maxUnavailable=1 | deployment.go:106 (knob variant) | PASS | `deployment_rolling_update_zero_surge_one_unavailable_caps_replicas` | mirrored, passing |
| Strategy: RollingUpdate maxSurge=2 maxUnavailable=0 | deployment.go:106 (knob variant) | PASS | `deployment_rolling_update_with_surge_permits_extra_replicas` | mirrored, passing |
| Strategy: rollback to previous template | deployment.go:207 (lifecycle rollback) | PASS | `deployment_rollback_reuses_existing_old_replicaset` | mirrored, passing |
| Strategy: scale to zero drains pods | deployment.go:207 (lifecycle scale-to-zero) | PASS | `deployment_scale_to_zero_drains_replicaset_to_zero` | mirrored, passing |
| Strategy: cross-namespace deployments are isolated | deployment.go lifecycle (per-namespace) | PASS | `deployment_namespaces_are_isolated` | mirrored, passing |
| `ReplicaSet should serve a basic image on each replica with a public image` | replica_set.go:95 | FAIL | `replicaset_should_serve_basic_image_on_each_replica` | mirrored, ignored (tracks failure) |
| `ReplicaSet should adopt matching pods on creation and release no longer matching pods` | replica_set.go:115 | PASS | `replicaset_should_adopt_matching_pods_and_release_mismatched` | mirrored, passing |
| `Replicaset should have a working scale subresource` | replica_set.go:128 | PASS | `replicaset_scale_subresource_resizes_pod_population` | mirrored, passing |
| `ReplicaSet Replace and Patch tests` | replica_set.go:142 | PASS | `replicaset_replace_and_patch_propagates_to_pods` | mirrored, passing |
| `ReplicaSet should list and delete a collection of ReplicaSets` | replica_set.go:156 | PASS | `replicaset_list_and_delete_collection` | mirrored, passing |
| `ReplicaSet should validate Replicaset Status endpoints` | replica_set.go:169 | PASS | `replicaset_status_replicas_match_pod_count` | mirrored, passing |
| ReplicaSet self-healing on pod deletion | replica_set.go:95 (implied invariant) | PASS | `replicaset_recreates_deleted_pod` | mirrored, passing |
| `ReplicationController should serve a basic image on each replica with a public image` | rc.go:65 | FAIL | `rc_should_serve_basic_image_on_each_replica` | mirrored, ignored (tracks failure) |
| `ReplicationController should adopt matching pods on creation` | rc.go:89 | PASS | `rc_should_adopt_matching_pods_on_creation` | mirrored, passing |
| `ReplicationController should release no longer matching pods` | rc.go:99 | PASS | `rc_should_release_pods_whose_labels_no_longer_match` | mirrored, passing |
| `ReplicationController should test the lifecycle of a ReplicationController` | rc.go:109 | PASS | `rc_lifecycle_create_scale_patch_delete` | mirrored, passing |
| `ReplicationController should get and update a ReplicationController scale` | rc.go:406 | PASS | `rc_scale_subresource_resizes_pod_population` | mirrored, passing |
| `ReplicationController should surface a failure condition on a common issue like exceeded quota` | rc.go:76 | PASS | `rc_publishes_status_after_reconcile` | mirrored, passing (narrowed to status emission) |
| ReplicationController self-healing on pod deletion | rc.go:65 (implied invariant) | PASS | `rc_recreates_deleted_pod` | mirrored, passing |

## Notes

- The ignored tests stay compiled and named so a future fix can flip them
  active simply by removing the `#[ignore]` attribute. They are the
  per-controller manifestation of the three failures in
  `docs/CONFORMANCE.md:48` ("Apps controllers (~3)").
- The `rc_publishes_status_after_reconcile` test is a deliberately narrower
  mirror of the upstream "surface a failure condition on exceeded quota"
  scenario. The upstream test couples the ReplicaFailure condition to an
  active ResourceQuota admission rejection — that coupling lives in the
  api-server admission plugin, not in `ReplicationControllerController`. The
  test pins the contract the controller owns (status replicas count is
  emitted after reconcile) so that any regression in status publication is
  caught locally even though the full upstream scenario is out of scope.
- Multi-iteration rollouts use the inline `run_rollout` helper which
  alternates `DeploymentController::reconcile_all` and
  `ReplicaSetController::reconcile_all` then marks every pod Ready. This
  mirrors the existing convention in `tests/deployment_controller_test.rs`
  and `tests/deployment_scaling_test.rs`.
- All tests are sub-second (`MemoryStorage`, no Docker, no HTTP). The full
  file completes inside the workspace `cargo test` budget; a future PR will
  consolidate the per-unit helpers into a shared `test_support` module.
