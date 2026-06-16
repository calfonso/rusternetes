//! Conformance Unit 10 — Container exit code + preStop hook timing
//!
//! Upstream e2e sites covered:
//!   - test/e2e/common/node/runtime.go:115  — container exit code is propagated
//!     through `containerStatuses[].state.terminated.exitCode`.
//!   - test/e2e/common/node/runtime.go:158  — `lifecycle.preStop` MUST be
//!     executed before SIGTERM and MUST complete within
//!     `terminationGracePeriodSeconds`.
//!
//! These tests pin three invariants the kubelet runtime must honour:
//!
//!   1. Exit code propagation: a non-zero Docker exit code is reflected
//!      verbatim in `containerStatuses[].state.terminated.exitCode`, with
//!      `reason="Error"` for non-zero exits and `reason="Completed"` for 0.
//!
//!   2. preStop budget bounding: the total preStop window is bounded by
//!      `terminationGracePeriodSeconds`. Hooks that overrun MUST be aborted
//!      and SIGTERM MUST be delivered with the remaining grace.
//!
//!   3. preStop is SKIPPED entirely when the effective grace period is 0
//!      (`kubectl delete --grace-period=0`, force delete). K8s
//!      `kuberuntime_container.go:killContainer` short-circuits to immediate
//!      SIGKILL without running preStop in that case. The current main
//!      branch's `stop_pod_for` uses `grace_period_seconds.max(1)` for the
//!      budget, which means a 1-second preStop window leaks into force
//!      deletes — this is what the failing test exposes.
//!
//! The tests are written against the pure helpers in
//! `rusternetes_kubelet::lifecycle`, so they exercise the lifecycle/preStop
//! block of `runtime.rs` without requiring a live Docker daemon.

use rusternetes_common::resources::{
    Container, ContainerState, ContainerStatus, ExecAction, Lifecycle, LifecycleHandler, Pod,
    PodSpec,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kubelet::lifecycle;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn make_container_with_prestop_exec(name: &str, command: Vec<&str>) -> Container {
    Container {
        name: name.to_string(),
        image: "busybox:latest".to_string(),
        image_pull_policy: Some("IfNotPresent".to_string()),
        command: None,
        args: None,
        ports: None,
        env: None,
        volume_mounts: None,
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
        resources: None,
        working_dir: None,
        security_context: None,
        restart_policy: None,
        resize_policy: None,
        lifecycle: Some(Lifecycle {
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
        }),
        termination_message_path: None,
        termination_message_policy: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        env_from: None,
        volume_devices: None,
        ..Default::default()
    }
}

fn make_pod_with_prestop(name: &str, grace_period: i64) -> Pod {
    let container =
        make_container_with_prestop_exec("app", vec!["/bin/sh", "-c", "echo preStop ran"]);
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![container],
            termination_grace_period_seconds: Some(grace_period),
            ..Default::default()
        }),
        status: None,
    }
}

// ===========================================================================
// 1. Container exit code propagation
//
// runtime.go:115 (TestContainer.GetPhase / TestContainer.Failed):
//   Conformance e2e creates a Pod whose container `cmd` exits with a
//   user-supplied exit code, then asserts:
//     pod.status.containerStatuses[0].state.terminated.exitCode == expected
//     pod.status.containerStatuses[0].state.terminated.reason   ==
//       (exit_code == 0 ? "Completed" : "Error" /* or OOMKilled etc */)
//
// `lifecycle::terminated_state_from_exit` mirrors the exact mapping used
// by runtime.rs:5800 when building ContainerState::Terminated from a
// Docker InspectContainer response. Pinning the mapping here guards
// against accidental reason/exit_code drift.
// ===========================================================================

#[test]
fn exit_code_zero_propagates_as_completed() {
    let state = lifecycle::terminated_state_from_exit(0, None, None);
    match state {
        ContainerState::Terminated {
            exit_code, reason, ..
        } => {
            assert_eq!(exit_code, 0, "exit code 0 must round-trip verbatim");
            assert_eq!(
                reason.as_deref(),
                Some("Completed"),
                "exit code 0 must surface reason=Completed (runtime.go:115)"
            );
        }
        _ => panic!("expected Terminated state"),
    }
}

#[test]
fn nonzero_exit_code_propagates_with_error_reason() {
    // The canonical e2e case: a container running `exit 42` must surface
    // exitCode=42 and reason="Error" in containerStatuses[].state.terminated.
    let state = lifecycle::terminated_state_from_exit(42, None, None);
    match state {
        ContainerState::Terminated {
            exit_code, reason, ..
        } => {
            assert_eq!(
                exit_code, 42,
                "non-zero exit code must propagate verbatim (runtime.go:115)"
            );
            assert_eq!(
                reason.as_deref(),
                Some("Error"),
                "non-zero exit code must surface reason=Error"
            );
        }
        _ => panic!("expected Terminated state"),
    }
}

#[test]
fn exit_code_137_propagates_as_oom_killed_by_default() {
    // Docker reports OOM kills as exit code 137 (SIGKILL+128). The kubelet
    // must surface this as reason="OOMKilled" unless the Docker `error`
    // field overrides it (e.g. user-supplied StopSignal).
    let state = lifecycle::terminated_state_from_exit(137, None, None);
    match state {
        ContainerState::Terminated {
            exit_code, reason, ..
        } => {
            assert_eq!(exit_code, 137);
            assert_eq!(
                reason.as_deref(),
                Some("OOMKilled"),
                "exit code 137 must surface reason=OOMKilled"
            );
        }
        _ => panic!("expected Terminated state"),
    }
}

#[test]
fn exit_code_propagates_through_container_status_struct() {
    // End-to-end: build a ContainerStatus with the canonical Terminated state
    // (the same shape the API server serialises to clients). The conformance
    // test reads it via `pod.status.containerStatuses[0].state.terminated`.
    let state = lifecycle::terminated_state_from_exit(1, None, None);
    let status = ContainerStatus {
        name: "app".to_string(),
        state: Some(state),
        ready: false,
        restart_count: 0,
        image: Some("busybox:latest".to_string()),
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
// 2. preStop hook timing
//
// runtime.go:158 (PreStop hook lifecycle):
//   The e2e test creates a Pod whose container has a `lifecycle.preStop`
//   exec/HTTP hook. It then deletes the Pod and asserts:
//     - preStop ran (observable side-effect: marker file / HTTP call /
//       container log line) BEFORE SIGTERM was delivered.
//     - preStop completed within `terminationGracePeriodSeconds`.
//     - On force-delete (gracePeriod == 0), preStop is skipped entirely.
//
// The kubelet enforces this by:
//   (a) executing every preStop hook for running containers BEFORE issuing
//       any `docker stop` (which delivers SIGTERM),
//   (b) bounding the preStop budget by gracePeriod,
//   (c) skipping preStop when gracePeriod == 0 (the K8s force-delete path).
// ===========================================================================

#[test]
fn prestop_budget_bounded_by_grace_period() {
    // A 30s grace period gives preStop 30 seconds — no more.
    let budget = lifecycle::compute_prestop_budget(30);
    assert_eq!(
        budget.as_secs(),
        30,
        "preStop budget must equal gracePeriod"
    );
}

#[test]
fn prestop_budget_zero_when_grace_period_zero() {
    // FAILS on main: `runtime.rs::stop_pod_for` uses
    //   prestop_budget = grace_period_seconds.max(1)
    // which yields a 1-second budget when grace_period is 0. K8s spec is
    // unambiguous: gracePeriod=0 means SIGKILL immediately, skipping
    // preStop entirely. The budget must be 0.
    let budget = lifecycle::compute_prestop_budget(0);
    assert_eq!(
        budget.as_secs(),
        0,
        "preStop must be skipped when gracePeriod==0 (force-delete) — runtime.go:158"
    );
}

#[test]
fn prestop_skipped_for_force_delete() {
    // Mirrors `kubectl delete pod --grace-period=0 --force`. K8s never
    // runs preStop for force-deleted pods — the container is SIGKILL'd
    // immediately.
    assert!(
        !lifecycle::should_run_prestop(0),
        "preStop must be skipped when effective grace period is 0"
    );
}

#[test]
fn prestop_runs_for_default_grace_period() {
    // K8s default terminationGracePeriodSeconds is 30; preStop runs.
    assert!(
        lifecycle::should_run_prestop(30),
        "preStop must run for the default 30s grace period"
    );
}

#[test]
fn prestop_runs_for_any_positive_grace_period() {
    for grace in [1, 2, 5, 30, 60, 600] {
        assert!(
            lifecycle::should_run_prestop(grace),
            "preStop must run for grace_period={}",
            grace
        );
    }
}

#[test]
fn remaining_grace_after_prestop_respects_minimum() {
    // K8s kuberuntime_container.go:860-862:
    //   gracePeriod -= preStopElapsed
    //   gracePeriod = max(gracePeriod, minimumGracePeriodInSeconds /* 2s */)
    // Even when preStop overruns the original window, SIGTERM gets at
    // least 2 seconds before SIGKILL — UNLESS gracePeriod was 0 to begin
    // with, in which case the kill is immediate.
    assert_eq!(lifecycle::remaining_grace_after_prestop(30, 10), 20);
    assert_eq!(
        lifecycle::remaining_grace_after_prestop(5, 10),
        2,
        "minimum 2s must be enforced when preStop overruns"
    );
    assert_eq!(
        lifecycle::remaining_grace_after_prestop(0, 0),
        0,
        "force-delete (grace=0) must remain 0 — runtime.go:158"
    );
}

#[test]
fn prestop_hook_discovered_from_pod_spec() {
    // The runtime builds a lifecycle map keyed by Docker container name
    // (`{pod_name}_{container_name}`). The map must contain exactly the
    // containers whose `lifecycle.preStop` is set.
    let pod = make_pod_with_prestop("test-prestop", 30);
    let map = lifecycle::build_prestop_lifecycle_map(&pod);

    assert_eq!(map.len(), 1, "exactly one container has a preStop hook");
    let key = "test-prestop_app";
    assert!(
        map.contains_key(key),
        "lifecycle map must key by `{{pod_name}}_{{container_name}}` (got keys: {:?})",
        map.keys().collect::<Vec<_>>()
    );
    let hook = map.get(key).expect("hook present");
    assert!(
        hook.pre_stop.is_some(),
        "lifecycle entry must carry the preStop handler"
    );
}

#[test]
fn no_prestop_lifecycle_map_when_no_hooks() {
    // A pod with no preStop hook must produce an empty lifecycle map
    // (skips the entire "first pass" preStop loop in stop_pod_for).
    let mut pod = make_pod_with_prestop("no-prestop", 30);
    if let Some(ref mut spec) = pod.spec {
        spec.containers[0].lifecycle = None;
    }
    let map = lifecycle::build_prestop_lifecycle_map(&pod);
    assert!(
        map.is_empty(),
        "lifecycle map must be empty when no container has preStop"
    );
}

// ===========================================================================
// 3. preStop ordering: ALL preStop hooks BEFORE any SIGTERM
//
// runtime.go:158 also pins that preStop runs *before* the container is
// stopped. We can't directly observe SIGTERM ordering without Docker, but
// the invariant is: `should_run_prestop` returns true ⇒ kubelet runs the
// hooks in pass 1, then issues `docker stop` in pass 2. The two passes
// must remain ordered and non-interleaved.
// ===========================================================================

#[test]
fn prestop_runs_before_sigterm_for_pod_with_hook() {
    // Composite check: a pod with a preStop hook + non-zero grace period
    // must trigger the "run preStop first" path.
    let pod = make_pod_with_prestop("ordering", 30);
    let grace = pod
        .spec
        .as_ref()
        .and_then(|s| s.termination_grace_period_seconds)
        .unwrap_or(30);

    let has_prestop = !lifecycle::build_prestop_lifecycle_map(&pod).is_empty();
    let runs_prestop_first = has_prestop && lifecycle::should_run_prestop(grace);

    assert!(
        runs_prestop_first,
        "Pod with preStop hook + grace>0 MUST run preStop before SIGTERM (runtime.go:158)"
    );
}

#[test]
fn force_deleted_pod_with_prestop_skips_hook() {
    // Pod has a preStop hook, but it's being force-deleted (grace=0):
    // hook MUST be skipped — immediate SIGKILL.
    let pod = make_pod_with_prestop("force-delete", 0);
    let grace = pod
        .spec
        .as_ref()
        .and_then(|s| s.termination_grace_period_seconds)
        .unwrap_or(30);

    let has_prestop = !lifecycle::build_prestop_lifecycle_map(&pod).is_empty();
    let runs_prestop = has_prestop && lifecycle::should_run_prestop(grace);

    assert!(
        !runs_prestop,
        "force-delete (grace=0) must skip preStop even when the hook is defined"
    );
}
