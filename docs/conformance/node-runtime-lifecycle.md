# [sig-node] Container runtime + lifecycle + exit + preStop — scoped conformance coverage

Crate: `crates/kubelet` · Test file: `tests/conformance_node_runtime_lifecycle.rs`

This unit mirrors the upstream Kubernetes v1.35 conformance scenarios for
container start/stop/exit semantics, restart policy, preStop hook timing,
`terminationGracePeriodSeconds` and image pull policy. It maps to the
**Node lifecycle (~3)** bucket in
[`docs/CONFORMANCE.md`](../CONFORMANCE.md#failure-categories) (Round 160:
container exit status, preStop hook).

Upstream sources mirrored:
- `test/e2e/common/node/runtime.go` — exit codes, restart policy table,
  termination message, image pull policy
- `test/e2e/common/node/lifecycle_hook.go` — preStop/postStart exec/HTTP
  hooks, sleep action, GracePeriodSeconds reduction
- `pkg/kubelet/kuberuntime/kuberuntime_container.go::killContainer` —
  grace-period contract for preStop + SIGTERM ordering

Style: kubelet-internal unit (no axum router). Pure helpers in
`rusternetes_kubelet::lifecycle` are exercised directly, matching the
prior-art `tests/runtime_prestop_exit_test.rs`.

## Sonobuoy Round 160 (2026-04-26) status table

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `Container Runtime blackbox test when starting a container that exits should run with the expected status` (Always) | runtime.go:53 | FAIL | `container_should_run_with_expected_status_restart_always` | mirrored, ignored (tracks failure) |
| `Container Runtime blackbox test … should run with the expected status` (OnFailure) | runtime.go:53 | FAIL | `container_should_run_with_expected_status_restart_on_failure` | mirrored, ignored (tracks failure) |
| `Container Runtime blackbox test … should run with the expected status` (Never) | runtime.go:53 | FAIL | `container_should_run_with_expected_status_restart_never` | mirrored, ignored (tracks failure) |
| `Container Runtime — exit code 0 → reason=Completed` | runtime.go:115 | PASS | `exit_code_zero_propagates_as_completed` | mirrored, passing |
| `Container Runtime — non-zero exit → reason=Error` | runtime.go:115 | PASS | `nonzero_exit_code_propagates_with_error_reason` | mirrored, passing |
| `Container Runtime — exit 137 → reason=OOMKilled` | runtime.go:115 | PASS | `exit_code_137_propagates_as_oom_killed` | mirrored, passing |
| `Container Runtime — docker `error` field overrides reason` | runtime.go:115 | PASS | `docker_error_field_overrides_reason` | mirrored, passing |
| `Container Runtime — Terminated state round-trips through ContainerStatus` | runtime.go:115 | PASS | `exit_code_propagates_through_container_status_struct` | mirrored, passing |
| `Container Runtime — terminated container reports termination message` | runtime.go:198 | PASS | `termination_message_round_trips_through_terminated_state` | mirrored, passing |
| `Container Runtime — non-default termination message path` | runtime.go:219 | PASS | `termination_message_path_is_preserved_on_container_spec` | mirrored, passing |
| `Container Runtime — FallbackToLogsOnError carries empty message on success` | runtime.go:261 | PASS | `termination_message_empty_when_pod_succeeds_under_fallback_policy` | mirrored, passing |
| `Container Runtime — imagePullPolicy=Always always pulls` | runtime.go:307 | PASS | `image_pull_policy_always_pulls_regardless_of_presence` | mirrored, passing |
| `Container Runtime — imagePullPolicy=Never never pulls` | runtime.go:307 | PASS | `image_pull_policy_never_skips_pull_even_when_missing` | mirrored, passing |
| `Container Runtime — imagePullPolicy=IfNotPresent only pulls when missing` | runtime.go:307 | PASS | `image_pull_policy_if_not_present_only_pulls_when_missing` | mirrored, passing |
| `Container Runtime — `:latest` tag defaults to Always pull policy` | pkg/api/v1/pod/util.go | PASS | `default_image_pull_policy_follows_latest_tag_rule` | mirrored, passing |
| `Container Lifecycle Hook — preStop budget bounded by gracePeriod` | lifecycle_hook.go:194 | PASS | `prestop_budget_bounded_by_grace_period` | mirrored, passing |
| `Container Lifecycle Hook — preStop budget zero when grace=0` | lifecycle_hook.go:194 | PASS | `prestop_budget_zero_when_grace_period_zero` | mirrored, passing |
| `Container Lifecycle Hook — preStop runs for default 30s grace` | lifecycle_hook.go:194 | PASS | `prestop_runs_for_default_30s_grace` | mirrored, passing |
| `Container Lifecycle Hook — remaining grace floors at 2s after preStop overrun` | kuberuntime_container.go:860 | PASS | `remaining_grace_after_prestop_floors_at_two_seconds` | mirrored, passing |
| `Container Lifecycle Hook — preStop exec discovered from pod spec` | lifecycle_hook.go:194 | PASS | `prestop_exec_hook_discovered_from_pod_spec` | mirrored, passing |
| `Lifecycle Sleep Hook — valid prestop hook using sleep action` | lifecycle_hook.go:595 | PASS | `prestop_sleep_action_discovered_from_pod_spec` | mirrored, passing |
| `Lifecycle sleep action zero value` | lifecycle_hook.go:714 | PASS | `prestop_zero_duration_sleep_is_accepted` | mirrored, passing |
| `Container Lifecycle Hook — pods without preStop produce empty map` | kuberuntime_container.go | PASS | `no_prestop_hook_yields_empty_lifecycle_map` | mirrored, passing |
| `Container Lifecycle Hook — preStop runs BEFORE SIGTERM (ordering)` | lifecycle_hook.go:194 | FAIL | `prestop_runs_before_sigterm_for_pod_with_hook` | mirrored, ignored (tracks failure) |
| `Container Lifecycle Hook — force-deleted pod skips preStop` | lifecycle_hook.go:194 | PASS | `force_deleted_pod_with_prestop_skips_hook` | mirrored, passing |
| `Container Lifecycle Hook — reduce GracePeriodSeconds during runtime` | lifecycle_hook.go:630 | PASS | `reduced_grace_period_shrinks_prestop_budget` | mirrored, passing |
| `Container Lifecycle Hook — ignore terminated container` | lifecycle_hook.go:667 | PASS | `already_terminated_container_does_not_need_prestop` | mirrored, passing |
| `terminationGracePeriodSeconds — defaults to 30 when unset` | defaults.go | PASS | `termination_grace_period_defaults_to_thirty` | mirrored, passing |
| `terminationGracePeriodSeconds — explicit value passes through` | defaults.go | PASS | `termination_grace_period_passes_through_explicit_value` | mirrored, passing |
| `terminationGracePeriodSeconds — zero means force-delete` | lifecycle_hook.go:194 | PASS | `termination_grace_period_zero_is_force_delete` | mirrored, passing |
| `terminationGracePeriodSeconds — negatives clamp to zero` | validation.go | PASS | `termination_grace_period_negative_clamps_to_zero` | mirrored, passing |
| `terminationGracePeriodSeconds — flows into preStop budget` | lifecycle_hook.go:630 | PASS | `termination_grace_period_flows_into_prestop_budget` | mirrored, passing |
| `PodSpec restartPolicy defaults to Always when unset` | defaults.go::SetDefaults_PodSpec | PASS | `pod_spec_restart_policy_unset_treated_as_always` | mirrored, passing |
| `Sidecar init container (per-container restartPolicy=Always) always restarts` | sidecar_containers.go (KEP-753) | PASS | `sidecar_init_container_always_restarts` | mirrored, passing |
| `restartPolicy=OnFailure does NOT restart after clean exit` | runtime.go:53 | FAIL | `on_failure_does_not_restart_after_clean_exit` | mirrored, ignored (tracks failure) |
| `Never+failure produces phase=Failed` | runtime.go:53 | FAIL | `never_policy_with_failure_yields_failed_phase` | mirrored, ignored (tracks failure) |
| `Composite: default-grace pod with preStop runs full two-phase termination` | lifecycle_hook.go:194 | PASS | `default_grace_pod_with_prestop_runs_full_two_phase_termination` | mirrored, passing |
| `Composite: force-delete short-circuits preStop+SIGTERM` | kuberuntime_container.go::killContainer | PASS | `force_delete_short_circuits_to_immediate_sigkill` | mirrored, passing |

## Failure tracker

Round 160 leaves the following upstream conformance scenarios FAILING in
this slice; the mirrored Rust tests are `#[ignore]`d with a reason that
links back to this fragment.

1. **`Container Runtime blackbox test when starting a container that
   exits should run with the expected status`** (runtime.go:53).
   Three restart-policy cases (Always / OnFailure / Never) — all three
   currently mis-report exit code or restart count. Bucketed under
   "Node lifecycle" / "container exit status".
2. **`Container Lifecycle Hook should execute prestop exec hook
   properly`** (lifecycle_hook.go:194). The pure preStop budget helper
   (`lifecycle::compute_prestop_budget`) returns the correct value when
   exercised in isolation; the live failure is in the runtime ordering
   between hook execution and SIGTERM delivery, which is not yet
   covered by a pure helper.

Fixing the underlying runtime behaviour is **out of scope** for this
batch — the mirrored tests stay `#[ignore]`d so a green PR does not
mask the regression. Once `runtime.rs` is updated to honour the upstream
contract, drop the `#[ignore]` attribute and re-run Sonobuoy to confirm
the round delta.

## Verification

```bash
cd /home/jones/PhpstormProjects/rusternetes
cargo fmt --all
cargo clippy -p rusternetes-kubelet --tests -- -D warnings
cargo test -p rusternetes-kubelet --test conformance_node_runtime_lifecycle
```

Expected: every non-`#[ignore]` test passes in <1 s.

## Related units

- `tests/runtime_prestop_exit_test.rs` — original preStop + exit code
  pin (prior art for this convention).
- `tests/init_container_restart_test.rs` — init-container restart
  semantics (covered by `node_probes_init_containers` worker).
- `tests/sidecar_containers_test.rs` — sidecar lifecycle (KEP-753).
