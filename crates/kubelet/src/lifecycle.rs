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
use std::collections::HashMap;
use std::time::Duration;

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
