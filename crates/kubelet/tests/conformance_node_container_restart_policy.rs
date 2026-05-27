//! Conformance: pod-level `restartPolicy` enforcement.
//!
//! Pins the decision table documented in
//! `pkg/kubelet/kuberuntime/kuberuntime_container.go::computePodActions`
//! (k8s v1.35). The pod-level table is `Always` / `OnFailure` / `Never`
//! — distinct from the alpha-gated per-container `ContainerRestartRules`
//! feature covered by `test/e2e/common/node/container_restart_policy.go`,
//! which this file does not shadow.
//!
//! Exercises the pure decision function
//! [`rusternetes_kubelet::lifecycle::should_restart_container`]: given a
//! `restartPolicy` and an exit code, should the kubelet restart the container?

use rusternetes_kubelet::lifecycle::should_restart_container;

#[test]
fn always_restarts_on_clean_exit() {
    assert!(should_restart_container(Some("Always"), 0));
}

#[test]
fn always_restarts_on_failure_exit() {
    assert!(should_restart_container(Some("Always"), 137));
}

#[test]
fn on_failure_does_not_restart_on_clean_exit() {
    assert!(!should_restart_container(Some("OnFailure"), 0));
}

#[test]
fn on_failure_restarts_on_nonzero_exit() {
    assert!(should_restart_container(Some("OnFailure"), 1));
    assert!(should_restart_container(Some("OnFailure"), 137));
}

#[test]
fn never_does_not_restart_on_clean_exit() {
    assert!(!should_restart_container(Some("Never"), 0));
}

#[test]
fn never_does_not_restart_on_failure_exit() {
    assert!(!should_restart_container(Some("Never"), 1));
}

#[test]
fn unset_policy_defaults_to_always() {
    assert!(should_restart_container(None, 0));
    assert!(should_restart_container(None, 1));
}
