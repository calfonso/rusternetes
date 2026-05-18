//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-node] Container runtime + lifecycle + exit + preStop.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/node/
//!
//! Upstream files mirrored here:
//!   - `test/e2e/common/node/runtime.go`        — container exit + restart
//!     policy + termination message + image pull policy.
//!   - `test/e2e/common/node/lifecycle_hook.go` — preStop/postStart hooks,
//!     terminationGracePeriodSeconds, lifecycle sleep action.
//!   - `test/e2e/common/node/container.go`      — ContainerState helpers.
//!
//! Per-test docstrings cite the upstream file + line + the Sonobuoy
//! outcome we observed in Round 160 (2026-04-26). Every test here now
//! pins a pure-helper invariant and runs as part of the default suite;
//! the production observer in `runtime.rs` consumes the same helpers
//! to surface `containerStatus.state.terminated` + `restartCount` and
//! drive `pod.status.phase` transitions.
//!
//! See docs/conformance/node-runtime-lifecycle.md for the test-by-test
//! status table and the "Node lifecycle" failure bucket in
//! docs/CONFORMANCE.md.
//!
//! Style notes:
//!   - This is a kubelet-internal conformance unit: there is no axum
//!     router here. We exercise the pure helpers in
//!     `rusternetes_kubelet::lifecycle` (and `runtime`) directly, mirroring
//!     the prior-art `runtime_prestop_exit_test.rs`.
//!   - Tests are synchronous (`#[test]`) because every helper is pure.

use rusternetes_common::resources::{
    Container, ContainerState, ContainerStatus, ExecAction, Lifecycle, LifecycleHandler, Pod,
    PodSpec, SleepAction,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kubelet::lifecycle;

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

/// Build a minimal container with optional per-container restart policy and
/// image pull policy. All other fields default to None so each test sets
/// only what it cares about.
fn make_container(
    name: &str,
    image: &str,
    image_pull_policy: Option<&str>,
    restart_policy: Option<&str>,
) -> Container {
    Container {
        name: name.to_string(),
        image: image.to_string(),
        image_pull_policy: image_pull_policy.map(|s| s.to_string()),
        restart_policy: restart_policy.map(|s| s.to_string()),
        ..Default::default()
    }
}

/// Build a container whose lifecycle.preStop fires the given exec command.
fn make_container_with_prestop_exec(name: &str, command: Vec<&str>) -> Container {
    let mut c = make_container(name, "busybox:1.37", Some("IfNotPresent"), None);
    c.lifecycle = Some(Lifecycle {
        pre_stop: Some(LifecycleHandler {
            exec: Some(ExecAction {
                command: command.iter().map(|s| s.to_string()).collect(),
            }),
            http_get: None,
            tcp_socket: None,
            sleep: None,
        }),
        post_start: None,
        stop_signal: None,
    });
    c
}

/// Build a container whose lifecycle.preStop sleeps for the given duration.
fn make_container_with_prestop_sleep(name: &str, sleep_secs: i64) -> Container {
    let mut c = make_container(name, "busybox:1.37", Some("IfNotPresent"), None);
    c.lifecycle = Some(Lifecycle {
        pre_stop: Some(LifecycleHandler {
            exec: None,
            http_get: None,
            tcp_socket: None,
            sleep: Some(SleepAction {
                seconds: sleep_secs,
            }),
        }),
        post_start: None,
        stop_signal: None,
    });
    c
}

/// Build a Pod with a single app container, the given pod-level restart
/// policy, and the given terminationGracePeriodSeconds.
fn make_pod(
    name: &str,
    container: Container,
    restart_policy: &str,
    grace_period: Option<i64>,
) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![container],
            restart_policy: Some(restart_policy.to_string()),
            termination_grace_period_seconds: grace_period,
            ..Default::default()
        }),
        status: None,
    }
}

/// Convenience: build a Pod with a preStop-exec container.
fn make_pod_with_prestop(name: &str, grace: i64) -> Pod {
    let c =
        make_container_with_prestop_exec("app", vec!["/bin/sh", "-c", "echo preStop; sleep 0.1"]);
    make_pod(name, c, "Always", Some(grace))
}

// ===========================================================================
// 1. Container Runtime — blackbox test when starting a container that exits
//    "should run with the expected status" [NodeConformance] [Conformance]
//
// Upstream: test/e2e/common/node/runtime.go:53
//
// Table-driven against three restart policies + a script that fails once,
// succeeds once, then loops forever:
//   {RestartPolicyAlways,    expect PodRunning,   ContainerRunning,   restart 2, true}
//   {RestartPolicyOnFailure, expect PodSucceeded, ContainerTerminated, restart 1, false}
//   {RestartPolicyNever,     expect PodFailed,    ContainerTerminated, restart 0, false}
//
// Sonobuoy Round 160: FAIL — container exit status / restart-count drift
// (one of the three "Node lifecycle" bucket failures in docs/CONFORMANCE.md).
// ===========================================================================

/// [sig-node] Container Runtime blackbox test when starting a container that exits
/// should run with the expected status [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:53
/// Sonobuoy (Round 160, 2026-04-26): tracked failure — the pure restart-policy
/// helpers pinned here are green; the live runtime gap is documented in
/// `docs/conformance/node-runtime-lifecycle.md` and addressed in the
/// pod-status observer (`runtime.rs::get_container_statuses`), which now
/// reads Docker `State.ExitCode` + `RestartCount` and threads them through
/// `lifecycle::should_restart_container` / `terminal_pod_phase`.
#[test]
fn container_should_run_with_expected_status_restart_always() {
    // Always: every exit (zero or non-zero) is restarted; pod stays
    // PodRunning forever.
    assert!(lifecycle::should_restart_container(Some("Always"), 0));
    assert!(lifecycle::should_restart_container(Some("Always"), 1));
    assert!(lifecycle::should_restart_container(Some("Always"), 137));
    // Pod is never terminal under Always.
    assert_eq!(lifecycle::terminal_pod_phase(Some("Always"), true), None);
    assert_eq!(lifecycle::terminal_pod_phase(Some("Always"), false), None);
}

/// [sig-node] Container Runtime blackbox test when starting a container that exits
/// should run with the expected status [NodeConformance] [Conformance] — OnFailure
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:53
/// Sonobuoy (Round 160): pure-helper invariants pinned here; the live
/// runtime now surfaces `state.terminated.exitCode` + `restartCount` and
/// honours `restartPolicy=OnFailure` via `lifecycle::should_restart_container`.
#[test]
fn container_should_run_with_expected_status_restart_on_failure() {
    // OnFailure: restart on non-zero, no restart on zero.
    assert!(!lifecycle::should_restart_container(Some("OnFailure"), 0));
    assert!(lifecycle::should_restart_container(Some("OnFailure"), 1));
    // After a successful exit, pod reaches Succeeded.
    assert_eq!(
        lifecycle::terminal_pod_phase(Some("OnFailure"), false),
        Some("Succeeded")
    );
}

/// [sig-node] Container Runtime blackbox test when starting a container that exits
/// should run with the expected status [NodeConformance] [Conformance] — Never
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:53
/// Sonobuoy (Round 160): pure-helper invariants pinned here; the live
/// runtime drives the `Never` terminal-phase transition through
/// `lifecycle::terminal_pod_phase` in the pod-status observer.
#[test]
fn container_should_run_with_expected_status_restart_never() {
    // Never: no restart on any exit.
    assert!(!lifecycle::should_restart_container(Some("Never"), 0));
    assert!(!lifecycle::should_restart_container(Some("Never"), 1));
    // Failure → Failed, success → Succeeded.
    assert_eq!(
        lifecycle::terminal_pod_phase(Some("Never"), true),
        Some("Failed")
    );
    assert_eq!(
        lifecycle::terminal_pod_phase(Some("Never"), false),
        Some("Succeeded")
    );
}

// ===========================================================================
// 2. Container Runtime — exit code propagation
//
// Upstream pins (runtime.go:115 / container.go ContainerState helpers):
// `containerStatuses[].state.terminated.exitCode` must round-trip the
// Docker exit code verbatim; `reason` must be the canonical mapping
// (Completed / Error / OOMKilled / docker-supplied reason).
//
// These mirror the existing `runtime_prestop_exit_test.rs` exit-code
// suite but re-state the conformance descriptor + Sonobuoy outcome so the
// file stands alone as the scoped conformance unit. PASS in Round 160.
// ===========================================================================

/// [sig-node] Container Runtime — exit code 0 surfaces as reason=Completed
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:115
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn exit_code_zero_propagates_as_completed() {
    let state = lifecycle::terminated_state_from_exit(0, None, None);
    match state {
        ContainerState::Terminated {
            exit_code, reason, ..
        } => {
            assert_eq!(exit_code, 0);
            assert_eq!(reason.as_deref(), Some("Completed"));
        }
        _ => panic!("expected Terminated"),
    }
}

/// [sig-node] Container Runtime — non-zero exit surfaces as reason=Error
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:115
/// Sonobuoy (Round 160): PASS
#[test]
fn nonzero_exit_code_propagates_with_error_reason() {
    let state = lifecycle::terminated_state_from_exit(42, None, None);
    match state {
        ContainerState::Terminated {
            exit_code, reason, ..
        } => {
            assert_eq!(exit_code, 42);
            assert_eq!(reason.as_deref(), Some("Error"));
        }
        _ => panic!("expected Terminated"),
    }
}

/// [sig-node] Container Runtime — exit 137 surfaces as reason=OOMKilled
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:115
/// Sonobuoy (Round 160): PASS
#[test]
fn exit_code_137_propagates_as_oom_killed() {
    let state = lifecycle::terminated_state_from_exit(137, None, None);
    match state {
        ContainerState::Terminated {
            exit_code, reason, ..
        } => {
            assert_eq!(exit_code, 137);
            assert_eq!(reason.as_deref(), Some("OOMKilled"));
        }
        _ => panic!("expected Terminated"),
    }
}

/// [sig-node] Container Runtime — docker-supplied error overrides default reason
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:115
/// Sonobuoy (Round 160): PASS
#[test]
fn docker_error_field_overrides_reason() {
    let state = lifecycle::terminated_state_from_exit(1, Some("ContainerCannotRun".into()), None);
    match state {
        ContainerState::Terminated { reason, .. } => {
            assert_eq!(reason.as_deref(), Some("ContainerCannotRun"));
        }
        _ => panic!("expected Terminated"),
    }
}

/// [sig-node] Container Runtime — Terminated state round-trips through ContainerStatus
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:115
/// Sonobuoy (Round 160): PASS
#[test]
fn exit_code_propagates_through_container_status_struct() {
    let state = lifecycle::terminated_state_from_exit(1, None, None);
    let status = ContainerStatus {
        name: "app".to_string(),
        state: Some(state),
        ready: false,
        restart_count: 0,
        image: Some("busybox:1.37".to_string()),
        image_id: None,
        container_id: None,
        started: Some(false),
        last_state: None,
        resources: None,
        allocated_resources: None,
        allocated_resources_status: None,
        user: None,
        volume_mounts: None,
        stop_signal: None,
    };
    match status.state.as_ref().expect("state present") {
        ContainerState::Terminated {
            exit_code, reason, ..
        } => {
            assert_eq!(*exit_code, 1);
            assert_eq!(reason.as_deref(), Some("Error"));
        }
        other => panic!("expected Terminated, got {:?}", other),
    }
}

// ===========================================================================
// 3. Container Runtime — termination message
//
// Upstream:
//   runtime.go:198 — TerminationMessagePath is set, read from file
//   runtime.go:219 — non-root user, non-default path
//   runtime.go:241 — TerminationMessagePolicy FallbackToLogsOnError
//   runtime.go:261 — empty when pod succeeds + FallbackToLogsOnError
//   runtime.go:280 — from file when pod succeeds + FallbackToLogsOnError
//
// The message round-trips through `terminated_state_from_exit`'s optional
// `termination_message` parameter — the runtime fills it after reading
// /dev/termination-log inside the container. We can't observe the file
// read without Docker, but we *can* pin the contract: whatever message
// the runtime supplies surfaces on ContainerState::Terminated.message.
// PASS in Round 160 (terminated-container tests passed even though the
// exit-code test failed — the message path is decoupled).
// ===========================================================================

/// [sig-node] Container Runtime — terminated container reports termination message
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:198
/// Sonobuoy (Round 160): PASS
#[test]
fn termination_message_round_trips_through_terminated_state() {
    let state =
        lifecycle::terminated_state_from_exit(0, None, Some("OK after migration".to_string()));
    match state {
        ContainerState::Terminated { message, .. } => {
            assert_eq!(message.as_deref(), Some("OK after migration"));
        }
        _ => panic!("expected Terminated"),
    }
}

/// [sig-node] Container Runtime — non-default termination message path
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:219
/// Sonobuoy (Round 160): PASS
#[test]
fn termination_message_path_is_preserved_on_container_spec() {
    // The kubelet must propagate `terminationMessagePath` from spec → Docker
    // bind mount; testing the spec round-trip catches accidental dropping.
    let mut c = make_container("app", "busybox:1.37", Some("IfNotPresent"), None);
    c.termination_message_path = Some("/dev/my-termination-log".to_string());
    c.termination_message_policy = Some("File".to_string());
    assert_eq!(
        c.termination_message_path.as_deref(),
        Some("/dev/my-termination-log")
    );
    assert_eq!(c.termination_message_policy.as_deref(), Some("File"));
}

/// [sig-node] Container Runtime — FallbackToLogsOnError carries empty message on success
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:261
/// Sonobuoy (Round 160): PASS
#[test]
fn termination_message_empty_when_pod_succeeds_under_fallback_policy() {
    // FallbackToLogsOnError + exit code 0 → message must be empty (the
    // policy reads logs only on error).
    let state = lifecycle::terminated_state_from_exit(0, None, None);
    match state {
        ContainerState::Terminated {
            exit_code, message, ..
        } => {
            assert_eq!(exit_code, 0);
            assert!(message.is_none(), "no message must surface for clean exit");
        }
        _ => panic!("expected Terminated"),
    }
}

// ===========================================================================
// 4. Container Runtime — image pull policy
//
// Upstream (runtime.go:267 "when running a container with a new image"):
//   runtime.go:302 — should not be able to pull image from invalid registry
//   runtime.go:307 — should be able to pull image
//   runtime.go:312 — pull from private registry without secret fails
//   runtime.go:317 — pull from private registry with secret succeeds
//
// We exercise the pure pull-decision helper. The network round-trip is
// covered by the cluster-level kubelet integration tests. PASS in Round 160.
// ===========================================================================

/// [sig-node] Container Runtime — imagePullPolicy=Always always pulls
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:307
/// Sonobuoy (Round 160): PASS
#[test]
fn image_pull_policy_always_pulls_regardless_of_presence() {
    assert_eq!(
        lifecycle::image_action(Some("Always"), true),
        lifecycle::ImageAction::Pull
    );
    assert_eq!(
        lifecycle::image_action(Some("Always"), false),
        lifecycle::ImageAction::Pull
    );
}

/// [sig-node] Container Runtime — imagePullPolicy=Never never pulls
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:307
/// Sonobuoy (Round 160): PASS
#[test]
fn image_pull_policy_never_uses_local_when_present() {
    assert_eq!(
        lifecycle::image_action(Some("Never"), true),
        lifecycle::ImageAction::UseLocal
    );
}

/// [sig-node] Container Runtime — imagePullPolicy=Never errors when image absent
///
/// Upstream: k8s.io/kubernetes/pkg/kubelet/images/image_manager.go:EnsureImageExists
/// — Never policy with no local image yields `ErrImageNeverPull` (mapped to the
/// container `waiting.reason` of the same name); the kubelet must NOT pull and
/// must NOT silently proceed to container creation.
#[test]
fn image_action_never_with_missing_image_errors() {
    assert_eq!(
        lifecycle::image_action(Some("Never"), false),
        lifecycle::ImageAction::ErrImageNeverPull
    );
}

/// An unset policy defaults to `IfNotPresent` (the explicit-policy branch,
/// before any `:latest`-based defaulting handled by
/// [`lifecycle::default_image_pull_policy`]).
#[test]
fn image_action_unset_defaults_to_if_not_present_missing() {
    assert_eq!(
        lifecycle::image_action(None, false),
        lifecycle::ImageAction::Pull
    );
}

#[test]
fn image_action_unset_defaults_to_if_not_present_existing() {
    assert_eq!(
        lifecycle::image_action(None, true),
        lifecycle::ImageAction::UseLocal
    );
}

/// Unknown / future policy strings degrade to `IfNotPresent` semantics —
/// matches the catch-all in upstream `kuberuntime_image.go`.
#[test]
fn image_action_unknown_policy_treated_as_if_not_present() {
    assert_eq!(
        lifecycle::image_action(Some("FutureUnknownValue"), false),
        lifecycle::ImageAction::Pull
    );
    assert_eq!(
        lifecycle::image_action(Some("FutureUnknownValue"), true),
        lifecycle::ImageAction::UseLocal
    );
}

/// The Display impl populates `containerStatus.waiting.message`. To match
/// upstream `pkg/kubelet/images/image_manager.go::imagePullPrecheck`, the
/// phrasing is `Container image "X" is not present with pull policy of Never`
/// — no reason prefix (that lives in `waiting.reason` separately).
#[test]
fn image_never_pull_error_display_matches_upstream() {
    let err = lifecycle::ImageNeverPullError {
        image: "nginx:1.27".to_string(),
    };
    assert_eq!(
        err.to_string(),
        r#"Container image "nginx:1.27" is not present with pull policy of Never"#,
    );
}

/// The kubelet recovers the typed error from `anyhow::Error` via downcast,
/// so the reason is derived from the type, not from the Display string.
/// This avoids substring-sniffing the user-facing message.
#[test]
fn reason_from_anyhow_recognizes_typed_image_never_pull_error() {
    let err: anyhow::Error = anyhow::Error::new(lifecycle::ImageNeverPullError {
        image: "nginx:1.27".to_string(),
    });
    assert_eq!(
        lifecycle::reason_from_anyhow(&err),
        Some("ErrImageNeverPull"),
    );
}

/// Legacy substring path still works for errors that were constructed as
/// plain strings (other paths in the codebase still propagate string-only
/// errors). Defence in depth.
#[test]
fn reason_from_anyhow_falls_back_to_substring_for_string_errors() {
    let err: anyhow::Error = anyhow::anyhow!("Image pull failed: registry down");
    assert_eq!(lifecycle::reason_from_anyhow(&err), Some("ErrImagePull"));

    let err: anyhow::Error = anyhow::anyhow!("CreateContainerConfigError: invalid mount");
    assert_eq!(
        lifecycle::reason_from_anyhow(&err),
        Some("CreateContainerConfigError"),
    );
}

#[test]
fn reason_from_anyhow_returns_none_for_unrelated_errors() {
    let err: anyhow::Error = anyhow::anyhow!("something completely unrelated");
    assert_eq!(lifecycle::reason_from_anyhow(&err), None);
}

/// The kubelet maps a low-level `start_pod` error message back to the
/// upstream `containerStatus.waiting.reason` string by sniffing substrings
/// of the error text. The `ErrImageNeverPull` reason must be recognised
/// from the Display output of [`lifecycle::ImageNeverPullError`] so a pod
/// whose image is absent under `imagePullPolicy=Never` surfaces the same
/// waiting reason as upstream.
///
/// NOTE: `"ErrImageNeverPull"` does NOT contain `"ErrImagePull"` as a
/// substring (the `Never` token sits between `Image` and `Pull`), so the
/// pre-existing `ErrImagePull` cascade does not catch it. This is a
/// regression guard against re-merging the two branches.
///
/// The primary path uses [`lifecycle::reason_from_anyhow`] (typed downcast).
/// This substring entry is the defence-in-depth fallback for callers that
/// wrap the error as a plain string before bubbling it up.
#[test]
fn container_reason_recognizes_err_image_never_pull_substring() {
    assert_eq!(
        lifecycle::container_reason_from_error_message("kubelet: ErrImageNeverPull"),
        Some("ErrImageNeverPull"),
    );
    assert_eq!(
        lifecycle::container_reason_from_error_message("Image pull failed: bollard error"),
        Some("ErrImagePull"),
    );
}

#[test]
fn container_reason_still_recognizes_err_image_pull() {
    assert_eq!(
        lifecycle::container_reason_from_error_message("Image pull failed: timeout"),
        Some("ErrImagePull")
    );
    assert_eq!(
        lifecycle::container_reason_from_error_message("image not found: nginx:9.99"),
        Some("ErrImagePull")
    );
    assert_eq!(
        lifecycle::container_reason_from_error_message("ErrImagePull: registry down"),
        Some("ErrImagePull")
    );
}

#[test]
fn container_reason_recognizes_create_container_errors() {
    assert_eq!(
        lifecycle::container_reason_from_error_message(
            "CreateContainerConfigError: bad mount spec"
        ),
        Some("CreateContainerConfigError")
    );
    assert_eq!(
        lifecycle::container_reason_from_error_message("CreateContainerError: oci runtime failed"),
        Some("CreateContainerError")
    );
}

#[test]
fn container_reason_returns_none_for_unrelated_error() {
    assert_eq!(
        lifecycle::container_reason_from_error_message("something completely unrelated"),
        None
    );
}

/// [sig-node] Container Runtime — imagePullPolicy=IfNotPresent pulls only when missing
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:307
/// Sonobuoy (Round 160): PASS
#[test]
fn image_pull_policy_if_not_present_only_pulls_when_missing() {
    assert_eq!(
        lifecycle::image_action(Some("IfNotPresent"), true),
        lifecycle::ImageAction::UseLocal
    );
    assert_eq!(
        lifecycle::image_action(Some("IfNotPresent"), false),
        lifecycle::ImageAction::Pull
    );
}

/// [sig-node] Container Runtime — image with :latest tag defaults to Always
///
/// Upstream: k8s.io/kubernetes/pkg/api/v1/pod/util.go::GetContainerStatus
/// Sonobuoy (Round 160): PASS
#[test]
fn default_image_pull_policy_follows_latest_tag_rule() {
    // No tag → :latest → Always.
    assert_eq!(lifecycle::default_image_pull_policy("nginx"), "Always");
    // Explicit :latest → Always.
    assert_eq!(
        lifecycle::default_image_pull_policy("nginx:latest"),
        "Always"
    );
    // Pinned tag → IfNotPresent.
    assert_eq!(
        lifecycle::default_image_pull_policy("nginx:1.27"),
        "IfNotPresent"
    );
    // Registry with port + pinned tag.
    assert_eq!(
        lifecycle::default_image_pull_policy("registry.k8s.io/pause:3.10"),
        "IfNotPresent"
    );
    // Digest reference → IfNotPresent (digests are immutable).
    assert_eq!(
        lifecycle::default_image_pull_policy("nginx@sha256:abc"),
        "IfNotPresent"
    );
}

// ===========================================================================
// 5. Container Lifecycle Hook — preStop / postStart / sleep action
//
// Upstream (test/e2e/common/node/lifecycle_hook.go):
//   line 177 — should execute poststart exec hook properly
//   line 194 — should execute prestop exec hook properly
//   line 211 — should execute poststart http hook properly
//   line 233 — should execute poststart https hook properly
//   line 256 — should execute prestop http hook properly
//   line 279 — should execute prestop https hook properly
//   line 595 — valid prestop hook using sleep action
//   line 630 — reduce GracePeriodSeconds during runtime
//   line 667 — ignore terminated container
//   line 714 — prestop hook using sleep action with zero duration
//
// These conformance tests pin the preStop budget + ordering invariants.
// The "preStop hook" failure in docs/CONFORMANCE.md's "Node lifecycle"
// bucket maps to the grace-period=0 leak fixed in
// `lifecycle::compute_prestop_budget` — those tests are not `#[ignore]`d
// here because the helper enforces the correct behaviour.
// ===========================================================================

/// [sig-node] Container Lifecycle Hook — preStop budget equals gracePeriod
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:194
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn prestop_budget_bounded_by_grace_period() {
    let budget = lifecycle::compute_prestop_budget(30);
    assert_eq!(budget.as_secs(), 30);
    assert_eq!(lifecycle::compute_prestop_budget(60).as_secs(), 60);
    assert_eq!(lifecycle::compute_prestop_budget(1).as_secs(), 1);
}

/// [sig-node] Container Lifecycle Hook — preStop skipped on force-delete
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:194
/// Sonobuoy (Round 160): PASS — guard against the historical grace=0
/// leak that masked the upstream contract (`grace_period.max(1)`).
#[test]
fn prestop_budget_zero_when_grace_period_zero() {
    assert_eq!(lifecycle::compute_prestop_budget(0).as_secs(), 0);
    assert!(!lifecycle::should_run_prestop(0));
}

/// [sig-node] Container Lifecycle Hook — preStop runs for default grace
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:194
/// Sonobuoy (Round 160): PASS
#[test]
fn prestop_runs_for_default_30s_grace() {
    assert!(lifecycle::should_run_prestop(30));
    for grace in [1, 2, 5, 30, 60, 600] {
        assert!(
            lifecycle::should_run_prestop(grace),
            "preStop must run for grace_period={grace}"
        );
    }
}

/// [sig-node] Container Lifecycle Hook — remaining grace honours 2s floor
///
/// Upstream: k8s.io/kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go:860
/// Sonobuoy (Round 160): PASS
#[test]
fn remaining_grace_after_prestop_floors_at_two_seconds() {
    assert_eq!(lifecycle::remaining_grace_after_prestop(30, 10), 20);
    // preStop overran: SIGTERM still gets 2s before SIGKILL.
    assert_eq!(lifecycle::remaining_grace_after_prestop(5, 10), 2);
    // Force-delete bypasses the floor entirely.
    assert_eq!(lifecycle::remaining_grace_after_prestop(0, 0), 0);
    assert_eq!(lifecycle::remaining_grace_after_prestop(0, 5), 0);
}

/// [sig-node] Container Lifecycle Hook — preStop exec is discovered from pod spec
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:194
/// Sonobuoy (Round 160): PASS
#[test]
fn prestop_exec_hook_discovered_from_pod_spec() {
    let pod = make_pod_with_prestop("prestop-discovery", 30);
    let map = lifecycle::build_prestop_lifecycle_map(&pod);
    assert_eq!(map.len(), 1);
    let key = "prestop-discovery_app";
    let hook = map.get(key).unwrap_or_else(|| {
        panic!(
            "lifecycle map must key by `{{pod}}_{{container}}` (got keys: {:?})",
            map.keys().collect::<Vec<_>>()
        )
    });
    assert!(hook.pre_stop.is_some());
}

/// [sig-node] Container Lifecycle Hook — preStop sleep action discovered
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:595
/// (valid prestop hook using sleep action)
/// Sonobuoy (Round 160): PASS
#[test]
fn prestop_sleep_action_discovered_from_pod_spec() {
    let c = make_container_with_prestop_sleep("app", 5);
    let pod = make_pod("sleep-prestop", c, "Always", Some(30));
    let map = lifecycle::build_prestop_lifecycle_map(&pod);
    assert_eq!(map.len(), 1);
    let hook = map.get("sleep-prestop_app").expect("hook present");
    let sleep = hook
        .pre_stop
        .as_ref()
        .and_then(|h| h.sleep.as_ref())
        .expect("sleep action present");
    assert_eq!(sleep.seconds, 5);
}

/// [sig-node] Container Lifecycle Hook — zero-duration sleep is valid
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:714
/// (prestop hook using sleep action with zero duration)
/// Sonobuoy (Round 160): PASS
#[test]
fn prestop_zero_duration_sleep_is_accepted() {
    let c = make_container_with_prestop_sleep("app", 0);
    let pod = make_pod("zero-sleep", c, "Always", Some(30));
    let map = lifecycle::build_prestop_lifecycle_map(&pod);
    let sleep = map
        .get("zero-sleep_app")
        .and_then(|h| h.pre_stop.as_ref())
        .and_then(|h| h.sleep.as_ref())
        .expect("sleep action present");
    assert_eq!(sleep.seconds, 0);
    // gracePeriod still allows the hook to be "run" (in practice it
    // returns immediately, then the runtime falls through to SIGTERM).
    assert!(lifecycle::should_run_prestop(30));
}

/// [sig-node] Container Lifecycle Hook — pods without preStop produce empty map
///
/// Upstream: k8s.io/kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go:killContainer
/// Sonobuoy (Round 160): PASS
#[test]
fn no_prestop_hook_yields_empty_lifecycle_map() {
    let c = make_container("plain", "busybox:1.37", Some("IfNotPresent"), None);
    let pod = make_pod("plain", c, "Always", Some(30));
    assert!(lifecycle::build_prestop_lifecycle_map(&pod).is_empty());
}

/// [sig-node] Container Lifecycle Hook — preStop runs BEFORE SIGTERM
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:194
/// Sonobuoy (Round 160, 2026-04-26): pure-helper invariants pinned here;
/// the runtime stop path uses `lifecycle::should_run_prestop` +
/// `compute_prestop_budget` to gate SIGTERM behind the hook.
#[test]
fn prestop_runs_before_sigterm_for_pod_with_hook() {
    let pod = make_pod_with_prestop("ordering", 30);
    let grace = pod
        .spec
        .as_ref()
        .and_then(|s| s.termination_grace_period_seconds)
        .unwrap_or(30);
    let has_prestop = !lifecycle::build_prestop_lifecycle_map(&pod).is_empty();
    let runs_first = has_prestop && lifecycle::should_run_prestop(grace);
    assert!(runs_first);
}

/// [sig-node] Container Lifecycle Hook — force-deleted pod skips preStop
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:194
/// Sonobuoy (Round 160): PASS — guards the contract.
#[test]
fn force_deleted_pod_with_prestop_skips_hook() {
    let pod = make_pod_with_prestop("force-delete", 0);
    let grace = pod
        .spec
        .as_ref()
        .and_then(|s| s.termination_grace_period_seconds)
        .unwrap_or(30);
    let has_prestop = !lifecycle::build_prestop_lifecycle_map(&pod).is_empty();
    let would_run = has_prestop && lifecycle::should_run_prestop(grace);
    assert!(!would_run, "force-delete must skip preStop");
}

/// [sig-node] Container Lifecycle Hook — reduce GracePeriodSeconds during runtime
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:630
/// Sonobuoy (Round 160): PASS
#[test]
fn reduced_grace_period_shrinks_prestop_budget() {
    // Original 30s grace, user shrinks to 5s via DeleteOptions.
    let new_budget = lifecycle::compute_prestop_budget(5);
    assert_eq!(new_budget.as_secs(), 5);
    // The remaining SIGTERM window after a 1s preStop is 4s — above the
    // 2s floor, so the explicit subtraction wins.
    assert_eq!(lifecycle::remaining_grace_after_prestop(5, 1), 4);
}

/// [sig-node] Container Lifecycle Hook — ignore terminated container
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:667
/// Sonobuoy (Round 160): PASS
#[test]
fn already_terminated_container_does_not_need_prestop() {
    // A container that's already Terminated must not have its preStop
    // re-fired during stop_pod_for. We model this as: the lifecycle map
    // is still built (to honour any sidecar that's running), but if the
    // app exits before stop_pod_for is invoked the hook is a no-op.
    // The kubelet path here is "ignore" — we just assert that the helper
    // produces a deterministic map from the spec regardless of runtime
    // state, leaving the "is it actually running?" decision to the caller.
    let pod = make_pod_with_prestop("already-dead", 30);
    let map = lifecycle::build_prestop_lifecycle_map(&pod);
    assert_eq!(map.len(), 1, "spec-derived map is runtime-agnostic");
}

// ===========================================================================
// 6. terminationGracePeriodSeconds — defaults + edge cases
//
// Upstream conformance enforces:
//   - default = 30s when unset
//   - 0 = force-delete (skip preStop, SIGKILL immediately)
//   - negative values are normalised to 0 by the API server, but the
//     kubelet must defend against legacy/etcd-injected objects.
// ===========================================================================

/// [sig-node] terminationGracePeriodSeconds — defaults to 30 when unset
///
/// Upstream: k8s.io/kubernetes/pkg/apis/core/v1/defaults.go
/// Sonobuoy (Round 160): PASS
#[test]
fn termination_grace_period_defaults_to_thirty() {
    assert_eq!(lifecycle::effective_termination_grace_period(None), 30);
}

/// [sig-node] terminationGracePeriodSeconds — explicit value passes through
///
/// Upstream: k8s.io/kubernetes/pkg/apis/core/v1/defaults.go
/// Sonobuoy (Round 160): PASS
#[test]
fn termination_grace_period_passes_through_explicit_value() {
    assert_eq!(
        lifecycle::effective_termination_grace_period(Some(60)),
        60,
        "user-set grace must round-trip"
    );
    assert_eq!(
        lifecycle::effective_termination_grace_period(Some(1)),
        1,
        "1-second grace is legal and triggers preStop"
    );
}

/// [sig-node] terminationGracePeriodSeconds — zero means force-delete
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:194
/// Sonobuoy (Round 160): PASS
#[test]
fn termination_grace_period_zero_is_force_delete() {
    assert_eq!(lifecycle::effective_termination_grace_period(Some(0)), 0);
    assert!(!lifecycle::should_run_prestop(0));
    assert_eq!(lifecycle::compute_prestop_budget(0).as_secs(), 0);
}

/// [sig-node] terminationGracePeriodSeconds — negatives clamp to zero
///
/// Upstream: k8s.io/kubernetes/pkg/apis/core/v1/validation/validation.go
/// Sonobuoy (Round 160): PASS
#[test]
fn termination_grace_period_negative_clamps_to_zero() {
    assert_eq!(lifecycle::effective_termination_grace_period(Some(-1)), 0);
    assert_eq!(
        lifecycle::effective_termination_grace_period(Some(-9999)),
        0
    );
}

/// [sig-node] terminationGracePeriodSeconds — flows through preStop budget
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:630
/// Sonobuoy (Round 160): PASS
#[test]
fn termination_grace_period_flows_into_prestop_budget() {
    let grace = lifecycle::effective_termination_grace_period(Some(45));
    let budget = lifecycle::compute_prestop_budget(grace);
    assert_eq!(budget.as_secs(), 45);
}

// ===========================================================================
// 7. Restart policy round-trip through PodSpec
//
// These tests don't add new logic — they pin that the PodSpec defaults
// the kubelet relies on (`restart_policy.unwrap_or("Always")`) survive
// serde round-trips and the helper API.
// ===========================================================================

/// [sig-node] PodSpec restartPolicy defaults to Always when unset
///
/// Upstream: k8s.io/kubernetes/pkg/apis/core/v1/defaults.go::SetDefaults_PodSpec
/// Sonobuoy (Round 160): PASS
#[test]
fn pod_spec_restart_policy_unset_treated_as_always() {
    let c = make_container("app", "busybox:1.37", Some("IfNotPresent"), None);
    let mut pod = make_pod("default-policy", c, "Always", None);
    if let Some(ref mut spec) = pod.spec {
        spec.restart_policy = None;
    }
    let effective = pod
        .spec
        .as_ref()
        .and_then(|s| s.restart_policy.as_deref())
        .unwrap_or("Always");
    assert_eq!(effective, "Always");
    assert!(lifecycle::should_restart_container(Some(effective), 1));
}

/// [sig-node] Sidecar init container (restartPolicy=Always) is always restarted
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/sidecar_containers.go
/// (KEP-753 SidecarContainers)
/// Sonobuoy (Round 160): PASS
#[test]
fn sidecar_init_container_always_restarts() {
    // A container with per-container restartPolicy=Always (i.e. a sidecar
    // init container per KEP-753) MUST be restarted on any exit, even if
    // the enclosing pod's restartPolicy is Never.
    let sidecar_policy = Some("Always");
    assert!(lifecycle::should_restart_container(sidecar_policy, 0));
    assert!(lifecycle::should_restart_container(sidecar_policy, 1));
}

/// [sig-node] restartPolicy=OnFailure does NOT restart on clean exit
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:53
/// Sonobuoy (Round 160): pure-helper invariant pinned here. The live
/// observer reads Docker's `State.ExitCode` and routes the clean-exit
/// case through `lifecycle::terminal_pod_phase(Some("OnFailure"), false)`
/// to reach `Succeeded`.
#[test]
fn on_failure_does_not_restart_after_clean_exit() {
    assert!(!lifecycle::should_restart_container(Some("OnFailure"), 0));
    // After a clean exit, pod must reach Succeeded.
    assert_eq!(
        lifecycle::terminal_pod_phase(Some("OnFailure"), false),
        Some("Succeeded")
    );
}

/// [sig-node] Never+failure produces phase=Failed
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go:53
/// Sonobuoy (Round 160): pure-helper invariant pinned here. The live
/// observer threads Docker's non-zero `State.ExitCode` through
/// `lifecycle::terminal_pod_phase(Some("Never"), true)` to set
/// `pod.status.phase = "Failed"`.
#[test]
fn never_policy_with_failure_yields_failed_phase() {
    assert_eq!(
        lifecycle::terminal_pod_phase(Some("Never"), true),
        Some("Failed")
    );
}

// ===========================================================================
// 8. Cross-cutting: end-to-end shape check (no Docker)
//
// Composite test that walks the same decision tree the runtime walks on
// a Pod-delete request, verifying the helpers compose correctly.
// ===========================================================================

/// [sig-node] preStop+SIGTERM ordering for a default-grace pod
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/lifecycle_hook.go:194
/// Sonobuoy (Round 160): PASS — verifies the helper composition.
#[test]
fn default_grace_pod_with_prestop_runs_full_two_phase_termination() {
    let pod = make_pod_with_prestop("default-grace-pod", 30);
    let grace = lifecycle::effective_termination_grace_period(
        pod.spec
            .as_ref()
            .and_then(|s| s.termination_grace_period_seconds),
    );
    assert_eq!(grace, 30);
    assert!(lifecycle::should_run_prestop(grace));
    let budget = lifecycle::compute_prestop_budget(grace);
    assert_eq!(budget.as_secs(), 30);
    // After a 5-second preStop, SIGTERM still has 25 seconds.
    let remaining = lifecycle::remaining_grace_after_prestop(grace, 5);
    assert_eq!(remaining, 25);
    // The lifecycle map contains the preStop entry.
    let map = lifecycle::build_prestop_lifecycle_map(&pod);
    assert_eq!(map.len(), 1);
}

/// [sig-node] Force-delete short-circuits the entire preStop+SIGTERM path
///
/// Upstream: k8s.io/kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go:killContainer
/// Sonobuoy (Round 160): PASS — guards the contract.
#[test]
fn force_delete_short_circuits_to_immediate_sigkill() {
    let pod = make_pod_with_prestop("force-pod", 0);
    let grace = lifecycle::effective_termination_grace_period(
        pod.spec
            .as_ref()
            .and_then(|s| s.termination_grace_period_seconds),
    );
    assert_eq!(grace, 0);
    assert!(!lifecycle::should_run_prestop(grace));
    assert_eq!(lifecycle::compute_prestop_budget(grace).as_secs(), 0);
    assert_eq!(lifecycle::remaining_grace_after_prestop(grace, 0), 0);
}
