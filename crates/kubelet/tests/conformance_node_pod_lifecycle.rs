//! Conformance: pod-lifecycle decision functions.
//!
//! Pins three pure kubelet decision functions, not the API-mediated
//! lifecycle scenarios in `test/e2e/common/node/pods.go`. Upstream
//! references for the contracts pinned here (k8s v1.35):
//!   - `pkg/kubelet/kuberuntime/runtime.go::terminalPodPhase` — phase a pod
//!     should land in once every container has exited.
//!   - `pkg/kubelet/images/image_manager.go::EnsureImageExists` — what to do
//!     about an image given `imagePullPolicy` and local presence.
//!   - `pkg/api/v1/pod/util.go::GetContainerImagePullPolicy` — default
//!     `imagePullPolicy` derivation from an image reference.

use rusternetes_kubelet::lifecycle::{
    default_image_pull_policy, image_action, terminal_pod_phase, ImageAction,
};

// ---------------------------------------------------------------------------
// terminal_pod_phase
// ---------------------------------------------------------------------------

#[test]
fn always_policy_has_no_terminal_phase() {
    assert_eq!(terminal_pod_phase(Some("Always"), false), None);
    assert_eq!(terminal_pod_phase(Some("Always"), true), None);
}

#[test]
fn never_policy_failed_pod_is_failed() {
    assert_eq!(terminal_pod_phase(Some("Never"), true), Some("Failed"));
}

#[test]
fn never_policy_clean_pod_is_succeeded() {
    assert_eq!(terminal_pod_phase(Some("Never"), false), Some("Succeeded"));
}

#[test]
fn on_failure_policy_clean_pod_is_succeeded() {
    assert_eq!(
        terminal_pod_phase(Some("OnFailure"), false),
        Some("Succeeded")
    );
}

#[test]
fn on_failure_policy_with_failure_keeps_running() {
    // OnFailure with a failure restarts — the pod is not yet terminal.
    assert_eq!(terminal_pod_phase(Some("OnFailure"), true), None);
}

// ---------------------------------------------------------------------------
// image_action
// ---------------------------------------------------------------------------

#[test]
fn pull_always_pulls_even_when_present() {
    assert_eq!(image_action(Some("Always"), true), ImageAction::Pull);
    assert_eq!(image_action(Some("Always"), false), ImageAction::Pull);
}

#[test]
fn pull_never_uses_local_when_present() {
    assert_eq!(image_action(Some("Never"), true), ImageAction::UseLocal);
}

#[test]
fn pull_never_errors_when_absent() {
    assert_eq!(
        image_action(Some("Never"), false),
        ImageAction::ErrImageNeverPull
    );
}

#[test]
fn if_not_present_uses_local_when_present() {
    assert_eq!(
        image_action(Some("IfNotPresent"), true),
        ImageAction::UseLocal
    );
}

#[test]
fn if_not_present_pulls_when_absent() {
    assert_eq!(image_action(Some("IfNotPresent"), false), ImageAction::Pull);
}

#[test]
fn unset_policy_behaves_like_if_not_present() {
    assert_eq!(image_action(None, true), ImageAction::UseLocal);
    assert_eq!(image_action(None, false), ImageAction::Pull);
}

// ---------------------------------------------------------------------------
// default_image_pull_policy
// ---------------------------------------------------------------------------

#[test]
fn default_policy_latest_tag_is_always() {
    assert_eq!(default_image_pull_policy("nginx:latest"), "Always");
}

#[test]
fn default_policy_untagged_is_always() {
    // "Tag absent" means an implicit `:latest`, so Always.
    assert_eq!(default_image_pull_policy("nginx"), "Always");
}

#[test]
fn default_policy_explicit_tag_is_if_not_present() {
    assert_eq!(default_image_pull_policy("nginx:1.25"), "IfNotPresent");
}

#[test]
fn default_policy_digest_is_if_not_present() {
    assert_eq!(
        default_image_pull_policy("nginx@sha256:abcdef"),
        "IfNotPresent"
    );
}

#[test]
fn default_policy_registry_port_does_not_confuse_tag_parser() {
    // The K8s parser uses the last `:` after the final `/`, so a port in
    // the registry host must not be mistaken for a tag.
    assert_eq!(
        default_image_pull_policy("registry:5000/app"),
        "Always",
        "no tag → treated as implicit :latest → Always"
    );
    assert_eq!(
        default_image_pull_policy("registry:5000/app:1.0"),
        "IfNotPresent",
    );
}
