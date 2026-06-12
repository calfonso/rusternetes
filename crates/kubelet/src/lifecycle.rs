//! Pod lifecycle helpers — preStop hook timing and container exit code mapping.
//!
//! This module owns the pure logic that drives the lifecycle / preStop block
//! of [`crate::runtime`]. It is extracted into its own module so the
//! invariants pinned by upstream conformance tests
//! `test/e2e/common/node/runtime.go:115,158` (container exit code propagation
//! and preStop hook timing) can be verified without a live Docker daemon.
//!
//! K8s references:
//!   - `pkg/kubelet/kuberuntime/kuberuntime_container.go:killContainer` —
//!     preStop is skipped when grace period is 0; otherwise it runs first,
//!     bounded by the grace period, before SIGTERM is delivered.
//!   - `kubeletconfig.MinimumGracePeriodInSeconds = 2` — the floor for
//!     `SIGTERM → SIGKILL` even after preStop overruns, but only when the
//!     effective grace period started > 0.
//!   - `pkg/kubelet/kuberuntime/kuberuntime_container.go:760` — Terminated
//!     state mapping for `exit_code → (reason, message)`. Non-zero exit
//!     codes surface as `reason="Error"`, 137 as `reason="OOMKilled"`
//!     (unless the runtime supplied a specific `error` string).

use rusternetes_common::resources::{ContainerState, Lifecycle, Pod};
use rusternetes_common::types::Phase;
use std::collections::HashMap;
use std::time::Duration;

/// True when `phase` is one of the two terminal pod phases (`Succeeded`,
/// `Failed`).
///
/// Mirrors upstream `pkg/api/v1/pod/util.go:307 IsPodPhaseTerminal`, which
/// returns true for exactly `PodSucceeded` and `PodFailed`. Terminal phases
/// are an absorbing state: once a pod reaches one, the kubelet must never
/// report it back to a non-terminal phase.
#[inline]
pub fn phase_is_terminal(phase: Option<&Phase>) -> bool {
    matches!(phase, Some(Phase::Succeeded) | Some(Phase::Failed))
}

/// Decide whether a kubelet status write that would set the pod phase to
/// `incoming` must be SKIPPED because the pod already reached a terminal
/// phase in storage (`current`).
///
/// This is the rusternetes analogue of the terminal-phase stickiness guard
/// in upstream `pkg/kubelet/kubelet_pods.go:1934-1942 generateAPIPodStatus`
/// ("pods are not allowed to transition out of terminal phases"): when the
/// API server already shows `Failed`/`Succeeded` and the freshly computed
/// phase differs, the kubelet logs "Pod attempted illegal phase transition"
/// and forces the phase back to the API server's terminal value, so a
/// `Succeeded`/`Failed` pod is never regressed to `Running`/`Pending`.
///
/// Rules:
///   - current terminal, incoming non-terminal → SKIP (regression — the bug).
///   - current terminal, incoming the SAME terminal phase → ALLOW (reason /
///     message updates for the same terminal phase, e.g. `Failed → Failed`
///     for eviction or preemption, must still land).
///   - current terminal, incoming a DIFFERENT terminal phase
///     (`Succeeded → Failed` or vice-versa) → SKIP. The first terminal phase
///     a pod reaches wins; upstream never rewrites one terminal phase into
///     the other.
///   - current non-terminal → ALLOW everything (the normal
///     `Pending → Running → Succeeded` progression, including restartPolicy=
///     Always pods that the kubelet keeps in `Running`/`CrashLoopBackOff`
///     and therefore never have a terminal `current`).
#[inline]
pub fn should_skip_phase_write(current: Option<&Phase>, incoming: &Phase) -> bool {
    if !phase_is_terminal(current) {
        return false;
    }
    // current is terminal here; only an exact-same-phase write is allowed.
    current != Some(incoming)
}

/// Minimum grace period after preStop has overrun, in seconds.
///
/// Mirrors K8s `minimumGracePeriodInSeconds` so SIGTERM always has at
/// least 2 seconds before SIGKILL — but only when the caller asked for
/// graceful shutdown (gracePeriod > 0). Force-deletes (gracePeriod == 0)
/// stay at 0 and SIGKILL immediately.
pub const MINIMUM_GRACE_PERIOD_SECS: i64 = 2;

/// Whether the kubelet should run preStop hooks for a pod being terminated.
///
/// K8s spec: if `gracePeriod == 0` (force-delete via
/// `kubectl delete --grace-period=0`), preStop is skipped and the container
/// is SIGKILL'd immediately. Any positive grace period — including 1s —
/// triggers preStop execution.
#[inline]
pub fn should_run_prestop(grace_period_seconds: i64) -> bool {
    grace_period_seconds > 0
}

/// Total time budget for executing all preStop hooks of a pod.
///
/// Equals the effective grace period: K8s bounds the whole preStop window
/// by `terminationGracePeriodSeconds`, then deducts the elapsed time from
/// the grace period before issuing SIGTERM. A grace period of 0 means
/// force-delete: budget is 0 and preStop is skipped (see
/// [`should_run_prestop`]).
#[inline]
pub fn compute_prestop_budget(grace_period_seconds: i64) -> Duration {
    if grace_period_seconds <= 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(grace_period_seconds as u64)
    }
}

/// Remaining grace period for SIGTERM after preStop has finished.
///
/// K8s `kuberuntime_container.go:860-862`:
/// ```text
///   gracePeriod -= preStopElapsed
///   gracePeriod  = max(gracePeriod, minimumGracePeriodInSeconds /* 2 */)
/// ```
/// Special case: when the *initial* grace period was 0 (force-delete),
/// the remaining grace stays 0 — SIGKILL is delivered immediately and the
/// 2s floor does not apply. This matches K8s where `killContainer` short-
/// circuits the whole preStop+SIGTERM path for grace=0.
pub fn remaining_grace_after_prestop(grace_period_seconds: i64, prestop_elapsed_secs: i64) -> i64 {
    if grace_period_seconds <= 0 {
        return 0;
    }
    (grace_period_seconds - prestop_elapsed_secs).max(MINIMUM_GRACE_PERIOD_SECS)
}

/// Build a map of Docker container name → lifecycle for every container
/// in the pod spec that declares a preStop hook.
///
/// Keys use the kubelet's Docker naming convention
/// (`{pod_name}_{container_name}`) so the runtime can look up hooks via
/// the names returned by `docker list_containers`. Init containers are
/// included to match the K8s sidecar / restartable-init semantics.
pub fn build_prestop_lifecycle_map(pod: &Pod) -> HashMap<String, Lifecycle> {
    let mut map: HashMap<String, Lifecycle> = HashMap::new();
    let pod_name = &pod.metadata.name;
    let Some(spec) = pod.spec.as_ref() else {
        return map;
    };
    for container in &spec.containers {
        if let Some(ref lc) = container.lifecycle {
            if lc.pre_stop.is_some() {
                map.insert(format!("{}_{}", pod_name, container.name), lc.clone());
            }
        }
    }
    if let Some(init_containers) = spec.init_containers.as_ref() {
        for container in init_containers {
            if let Some(ref lc) = container.lifecycle {
                if lc.pre_stop.is_some() {
                    map.insert(format!("{}_{}", pod_name, container.name), lc.clone());
                }
            }
        }
    }
    map
}

/// Decide whether the kubelet should restart a container that has just
/// exited, given the pod's `restartPolicy` and the container's exit code.
///
/// Mirrors the upstream table-driven semantics pinned by
/// `test/e2e/common/node/runtime.go:53` ("should run with the expected
/// status"):
///   - `restartPolicy=Always`    → restart on every exit (zero or non-zero)
///   - `restartPolicy=OnFailure` → restart only on non-zero exit
///   - `restartPolicy=Never`     → never restart
///   - Unset → defaults to `Always` (K8s PodSpec default).
#[inline]
#[allow(dead_code)]
pub fn should_restart_container(restart_policy: Option<&str>, exit_code: i32) -> bool {
    match restart_policy.unwrap_or("Always") {
        "Always" => true,
        "OnFailure" => exit_code != 0,
        "Never" => false,
        // Unknown values are treated as the default ("Always") — this
        // matches K8s' tolerant handling of forward-compatible enum
        // values (see PodSpec.RestartPolicy comment in core/v1/types.go).
        _ => true,
    }
}

/// Decide the terminal pod phase for a pod whose every container has
/// exited, given the pod's `restartPolicy` and whether *any* container
/// failed (non-zero exit).
///
/// Pinned by `runtime.go:53` table:
///   - `Never` + any failure   → `Failed`
///   - `Never` + all succeeded → `Succeeded`
///   - `OnFailure` + all succeeded → `Succeeded`
///   - `Always` is non-terminal — callers should not invoke this for
///     `Always` pods (the kubelet restarts containers instead). We return
///     `None` so the caller can treat it as "no terminal phase".
///
/// Returns the K8s phase string used in `Pod.status.phase`.
#[inline]
#[allow(dead_code)]
pub fn terminal_pod_phase(restart_policy: Option<&str>, any_failed: bool) -> Option<&'static str> {
    match restart_policy.unwrap_or("Always") {
        "Always" => None,
        "OnFailure" if any_failed => None, // OnFailure with failure → restart, not terminal
        "OnFailure" => Some("Succeeded"),
        "Never" if any_failed => Some("Failed"),
        "Never" => Some("Succeeded"),
        _ => None,
    }
}

/// Decision returned by [`image_action`] when reconciling `imagePullPolicy`
/// against local image presence. Mirrors the three branches of upstream
/// `pkg/kubelet/images/image_manager.go::EnsureImageExists`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageAction {
    /// Image is present locally and policy allows reuse — no pull needed.
    UseLocal,
    /// Image must be pulled from the registry.
    Pull,
    /// `imagePullPolicy=Never` and the image is absent locally. The kubelet
    /// must surface this as a `waiting.reason="ErrImageNeverPull"` container
    /// status without attempting to pull.
    ErrImageNeverPull,
}

/// Decide what to do for a container image given the `imagePullPolicy` and
/// whether the image is already present locally. Pure replacement for
/// [`should_pull_image`] that also distinguishes the `Never`+missing case.
///
/// Mirrors `pkg/kubelet/images/image_manager.go::EnsureImageExists`:
///   - `Always`       → always `Pull`
///   - `Never`        → `UseLocal` if present, else `ErrImageNeverPull`
///   - `IfNotPresent` → `UseLocal` if present, else `Pull`
///   - Unset / unknown → behave like `IfNotPresent`
pub fn image_action(image_pull_policy: Option<&str>, image_exists_locally: bool) -> ImageAction {
    match image_pull_policy.unwrap_or("IfNotPresent") {
        "Always" => ImageAction::Pull,
        "Never" if image_exists_locally => ImageAction::UseLocal,
        "Never" => ImageAction::ErrImageNeverPull,
        _ if image_exists_locally => ImageAction::UseLocal,
        _ => ImageAction::Pull,
    }
}

/// Typed error surfaced when `imagePullPolicy=Never` collides with an absent
/// local image. The kubelet maps this to a container `waiting.reason` of
/// `ErrImageNeverPull` per upstream `kuberuntime_image.go`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageNeverPullError {
    pub image: String,
}

impl std::fmt::Display for ImageNeverPullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Phrasing mirrors upstream `pkg/kubelet/images/image_manager.go`
        // `imagePullPrecheck`: `Container image %q is not present with pull
        // policy of Never`. The reason token ("ErrImageNeverPull") lives in
        // `containerStatus.waiting.reason`, not here.
        write!(
            f,
            r#"Container image "{}" is not present with pull policy of Never"#,
            self.image
        )
    }
}

impl std::error::Error for ImageNeverPullError {}

/// Recover the kubelet `containerStatus.waiting.reason` string from an
/// [`anyhow::Error`] returned by [`crate::runtime::ContainerRuntime::start_pod`].
///
/// Prefers a typed downcast to [`ImageNeverPullError`] so the reason does
/// not depend on the user-facing Display string. Falls back to the legacy
/// substring matcher in [`container_reason_from_error_message`] for error
/// paths that propagate plain string errors (eg the pull retry loop in
/// `runtime::pull_image_with_retry` wraps bollard errors as
/// `anyhow::anyhow!("Image pull failed: ...")`).
///
/// Mirrors upstream's preference for typed errors over substring sniffing —
/// `pkg/kubelet/images/types.go` declares `ErrImageNeverPull` as a sentinel
/// and the kubelet matches on the typed value, not the message text.
pub fn reason_from_anyhow(err: &anyhow::Error) -> Option<&'static str> {
    if err.downcast_ref::<ImageNeverPullError>().is_some() {
        return Some("ErrImageNeverPull");
    }
    container_reason_from_error_message(&err.to_string())
}

/// Map a low-level `start_pod` error message back to the upstream
/// `containerStatus.waiting.reason` string. The kubelet's sync loop uses
/// this to translate a `Result<_, anyhow::Error>` from
/// [`crate::runtime::ContainerRuntime::start_pod`] into the reason field
/// of `ContainerState::Waiting`.
///
/// Order matters: `ErrImageNeverPull` is checked before `ErrImagePull`
/// because the former does not contain the latter as a substring (the
/// `Never` token splits `Image` and `Pull`), and we want the more specific
/// reason to win.
///
/// K8s ref: `pkg/kubelet/kuberuntime/kuberuntime_container.go` —
/// `containerStartingError` to `Waiting.reason`.
pub fn container_reason_from_error_message(err_msg: &str) -> Option<&'static str> {
    if err_msg.starts_with("CreateContainerConfigError:") {
        Some("CreateContainerConfigError")
    } else if err_msg.starts_with("CreateContainerError:") {
        Some("CreateContainerError")
    } else if err_msg.contains("ErrImageNeverPull") {
        Some("ErrImageNeverPull")
    } else if err_msg.contains("Image pull failed")
        || err_msg.contains("image not found")
        || err_msg.contains("ErrImagePull")
    {
        Some("ErrImagePull")
    } else {
        None
    }
}

/// Default `imagePullPolicy` for a container image when the user did not
/// set one. K8s rule (`pkg/api/v1/pod/util.go::GetContainerStatus`):
///   - tag is `:latest` or absent → `Always`
///   - any explicit non-`latest` tag → `IfNotPresent`
///   - digest reference → `IfNotPresent`
#[allow(dead_code)]
pub fn default_image_pull_policy(image: &str) -> &'static str {
    // Digest reference (`image@sha256:...`) → IfNotPresent.
    if image.contains('@') {
        return "IfNotPresent";
    }
    // Split off any tag after the last colon, but be careful with
    // registries that include a port (`registry:5000/img`). The K8s parser
    // uses go-containerregistry which treats the last `:` after the final
    // `/` as the tag separator.
    let after_slash = match image.rfind('/') {
        Some(i) => &image[i + 1..],
        None => image,
    };
    match after_slash.rfind(':') {
        Some(i) => {
            let tag = &after_slash[i + 1..];
            if tag == "latest" {
                "Always"
            } else {
                "IfNotPresent"
            }
        }
        // No tag at all → defaults to `:latest` → Always.
        None => "Always",
    }
}

/// Effective `terminationGracePeriodSeconds` for a pod, given the value
/// set on the PodSpec (if any) and the K8s default.
///
/// K8s defaults to 30 seconds when unset (`pkg/apis/core/v1/defaults.go`).
/// Negative values are clamped to 0 — the K8s API server normalises this
/// at admission time, but the kubelet must defend against legacy objects.
#[inline]
#[allow(dead_code)]
pub fn effective_termination_grace_period(spec_grace: Option<i64>) -> i64 {
    spec_grace.unwrap_or(30).max(0)
}

/// Map a Docker exit code → [`ContainerState::Terminated`] in the canonical
/// way the kubelet reports it on `containerStatuses[].state.terminated`.
///
/// Pinned by upstream conformance `runtime.go:115`:
///   - `exit_code == 0`  → `reason = "Completed"`
///   - `exit_code == 137` → `reason = "OOMKilled"` (unless Docker `error`
///     field overrides it, e.g. user-supplied custom signal)
///   - otherwise         → `reason = "Error"` (or Docker `error` if present)
///
/// The exit code itself is propagated verbatim — this is what e2e tests
/// assert on `containerStatuses[].state.terminated.exitCode`.
///
/// Used by `tests/runtime_prestop_exit_test.rs` as the single source of
/// truth for the mapping. The matching inline mapping in
/// `runtime.rs::get_container_statuses` lives outside the lifecycle/preStop
/// block — see CLAUDE.md scope rules.
#[allow(dead_code)]
pub fn terminated_state_from_exit(
    exit_code: i64,
    docker_error: Option<String>,
    termination_message: Option<String>,
) -> ContainerState {
    let reason = if exit_code == 0 {
        "Completed".to_string()
    } else if exit_code == 137 {
        docker_error
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| "OOMKilled".to_string())
    } else {
        docker_error
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| "Error".to_string())
    };
    ContainerState::Terminated {
        exit_code: exit_code as i32,
        signal: None,
        reason: Some(reason),
        message: termination_message,
        started_at: None,
        finished_at: None,
        container_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_run_prestop_threshold() {
        assert!(!should_run_prestop(0));
        assert!(!should_run_prestop(-1));
        assert!(should_run_prestop(1));
        assert!(should_run_prestop(30));
    }

    #[test]
    fn budget_zero_for_force_delete() {
        assert_eq!(compute_prestop_budget(0), Duration::ZERO);
        assert_eq!(compute_prestop_budget(-1), Duration::ZERO);
        assert_eq!(compute_prestop_budget(30), Duration::from_secs(30));
    }

    #[test]
    fn remaining_grace_force_delete_stays_zero() {
        assert_eq!(remaining_grace_after_prestop(0, 0), 0);
        assert_eq!(remaining_grace_after_prestop(0, 5), 0);
    }

    #[test]
    fn remaining_grace_floors_at_minimum_for_graceful_termination() {
        assert_eq!(
            remaining_grace_after_prestop(5, 10),
            MINIMUM_GRACE_PERIOD_SECS
        );
        assert_eq!(remaining_grace_after_prestop(30, 10), 20);
    }

    #[test]
    fn restart_decision_matches_policy_matrix() {
        assert!(should_restart_container(Some("Always"), 0));
        assert!(should_restart_container(Some("Always"), 1));
        assert!(!should_restart_container(Some("OnFailure"), 0));
        assert!(should_restart_container(Some("OnFailure"), 1));
        assert!(!should_restart_container(Some("Never"), 0));
        assert!(!should_restart_container(Some("Never"), 1));
        assert!(should_restart_container(None, 0));
    }

    #[test]
    fn terminal_phase_matches_policy_matrix() {
        assert_eq!(terminal_pod_phase(Some("Never"), true), Some("Failed"));
        assert_eq!(terminal_pod_phase(Some("Never"), false), Some("Succeeded"));
        assert_eq!(
            terminal_pod_phase(Some("OnFailure"), false),
            Some("Succeeded")
        );
        assert_eq!(terminal_pod_phase(Some("OnFailure"), true), None);
        assert_eq!(terminal_pod_phase(Some("Always"), false), None);
        assert_eq!(terminal_pod_phase(Some("Always"), true), None);
    }

    #[test]
    fn phase_is_terminal_only_for_succeeded_and_failed() {
        assert!(phase_is_terminal(Some(&Phase::Succeeded)));
        assert!(phase_is_terminal(Some(&Phase::Failed)));
        assert!(!phase_is_terminal(Some(&Phase::Running)));
        assert!(!phase_is_terminal(Some(&Phase::Pending)));
        assert!(!phase_is_terminal(Some(&Phase::Unknown)));
        assert!(!phase_is_terminal(None));
    }

    #[test]
    fn skip_terminal_succeeded_regression_to_running() {
        // The exact Indexed-Job flake: a Succeeded pod must not flap back to
        // Running (which would let the job controller delete it and drop its
        // completion index).
        assert!(should_skip_phase_write(
            Some(&Phase::Succeeded),
            &Phase::Running
        ));
        assert!(should_skip_phase_write(
            Some(&Phase::Succeeded),
            &Phase::Pending
        ));
    }

    #[test]
    fn allow_same_terminal_phase_reason_update() {
        // Failed -> Failed (new reason/message: eviction, preemption) must
        // still be written.
        assert!(!should_skip_phase_write(
            Some(&Phase::Failed),
            &Phase::Failed
        ));
        assert!(!should_skip_phase_write(
            Some(&Phase::Succeeded),
            &Phase::Succeeded
        ));
    }

    #[test]
    fn allow_nonterminal_progression_to_terminal() {
        // Normal Running -> Succeeded / Pending -> Failed progression.
        assert!(!should_skip_phase_write(
            Some(&Phase::Running),
            &Phase::Succeeded
        ));
        assert!(!should_skip_phase_write(
            Some(&Phase::Running),
            &Phase::Failed
        ));
        assert!(!should_skip_phase_write(
            Some(&Phase::Pending),
            &Phase::Running
        ));
        // No prior phase at all — never skip.
        assert!(!should_skip_phase_write(None, &Phase::Running));
    }

    #[test]
    fn skip_cross_terminal_rewrite() {
        // The first terminal phase wins; never rewrite one terminal into the
        // other.
        assert!(should_skip_phase_write(
            Some(&Phase::Succeeded),
            &Phase::Failed
        ));
        assert!(should_skip_phase_write(
            Some(&Phase::Failed),
            &Phase::Succeeded
        ));
    }

    #[test]
    fn image_action_matches_policy_matrix() {
        assert_eq!(image_action(Some("Always"), true), ImageAction::Pull);
        assert_eq!(image_action(Some("Always"), false), ImageAction::Pull);
        assert_eq!(image_action(Some("Never"), true), ImageAction::UseLocal);
        assert_eq!(
            image_action(Some("Never"), false),
            ImageAction::ErrImageNeverPull
        );
        assert_eq!(
            image_action(Some("IfNotPresent"), true),
            ImageAction::UseLocal
        );
        assert_eq!(image_action(Some("IfNotPresent"), false), ImageAction::Pull);
    }

    #[test]
    fn default_pull_policy_follows_tag_rule() {
        assert_eq!(default_image_pull_policy("nginx"), "Always");
        assert_eq!(default_image_pull_policy("nginx:latest"), "Always");
        assert_eq!(default_image_pull_policy("nginx:1.27"), "IfNotPresent");
        assert_eq!(
            default_image_pull_policy("registry.k8s.io/nginx:1.27"),
            "IfNotPresent"
        );
        assert_eq!(
            default_image_pull_policy("registry:5000/nginx:1.27"),
            "IfNotPresent"
        );
        assert_eq!(default_image_pull_policy("registry:5000/nginx"), "Always");
        assert_eq!(
            default_image_pull_policy("nginx@sha256:deadbeef"),
            "IfNotPresent"
        );
    }

    #[test]
    fn effective_grace_period_defaults_and_clamps() {
        assert_eq!(effective_termination_grace_period(None), 30);
        assert_eq!(effective_termination_grace_period(Some(60)), 60);
        assert_eq!(effective_termination_grace_period(Some(0)), 0);
        assert_eq!(effective_termination_grace_period(Some(-5)), 0);
    }

    #[test]
    fn docker_error_overrides_reason_for_137() {
        let state = terminated_state_from_exit(137, Some("ContainerCannotRun".into()), None);
        match state {
            ContainerState::Terminated { reason, .. } => {
                assert_eq!(reason.as_deref(), Some("ContainerCannotRun"));
            }
            _ => panic!("expected Terminated"),
        }
    }
}
