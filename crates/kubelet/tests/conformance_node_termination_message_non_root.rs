//! Scoped mirror of the Kubernetes v1.35 conformance test
//!
//!   `[sig-node] Container Runtime blackbox test on terminated container
//!    should report termination message if TerminationMessagePath is set as
//!    non-root user and at a non-default path [NodeConformance] [Conformance]`
//!
//! Upstream source: test/e2e/common/node/runtime.go (release-1.35), the
//! `ginkgo.Context("on terminated container", ...)` block. The test creates
//! a pod whose container:
//!
//! - Runs as a non-root UID (10000).
//! - Sets `TerminationMessagePath` to a non-default path
//!   (`/dev/termination-custom-log`).
//! - Writes `"DONE"` to that path via shell redirection (`echo -n DONE > ...`),
//!   then exits 0.
//! - Expects the pod to reach `Succeeded` with the termination message round-
//!   tripping through `Status.ContainerStatuses[0].LastTerminationState.Terminated.Message`.
//!
//! Symptom on rusternetes (hydrophone canary on PR #182, 2026-05-20):
//! the pod transitions to `Failed`. The shell redirect inside the container
//! fails because the kubelet pre-creates the host-side bind-mounted file with
//! the umask-trimmed default mode (typically `0o664`, root-owned). A
//! container UID 10000 cannot write a root-owned `0o644`/`0o664` file, so the
//! redirect returns non-zero and the pod is reported `Failed`.
//!
//! Upstream fix (mirrored here):
//! `pkg/kubelet/kuberuntime/kuberuntime_container.go::makeMounts` in
//! release-1.35 (lines ~502-531) calls `os.Create()` then
//! `os.Chmod(containerLogPath, 0666)` to defeat the umask. We do the same in
//! `setup_termination_message_file`.

use rusternetes_kubelet::runtime::setup_termination_message_file;

/// [sig-node] Container Runtime blackbox test on terminated container should
/// report termination message if TerminationMessagePath is set as non-root
/// user and at a non-default path [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtime.go (release-1.35),
/// the "non-root user, non-default path" case. Upstream chmods the host-side
/// file to `0o666` after creation; the rusternetes equivalent must do the
/// same so a non-root container UID can write to the bind-mounted path.
#[test]
#[cfg(unix)]
fn setup_termination_message_file_is_world_writable_for_non_root_container() {
    use std::os::unix::fs::PermissionsExt;

    // Use an isolated tempdir per test so concurrent test runs don't race.
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("termination-log");
    let path_str = path.to_str().expect("path is utf-8");

    setup_termination_message_file(path_str).expect("setup termination file");

    let meta = std::fs::metadata(&path).expect("stat termination file");
    let mode = meta.permissions().mode() & 0o7777;

    // Upstream chmods to 0o666 explicitly. Anything more restrictive (0o644,
    // 0o664) blocks a non-root container from writing via `>` redirect, which
    // is exactly the conformance failure on PR #182's canary run.
    assert_eq!(
        mode, 0o666,
        "termination message file must be mode 0o666 (got {:o}) — upstream \
         pkg/kubelet/kuberuntime/kuberuntime_container.go::makeMounts \
         chmod's to 0666 after Create so a non-root container UID can write \
         to the bind-mounted path",
        mode
    );
}

/// Companion test: the helper must be idempotent. Repeated calls (e.g. a pod
/// restart) must not regress the mode back to the umask-trimmed default.
#[test]
#[cfg(unix)]
fn setup_termination_message_file_is_idempotent_and_preserves_mode() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("termination-log");
    let path_str = path.to_str().expect("path is utf-8");

    setup_termination_message_file(path_str).expect("first setup");
    // Simulate prior-pod artifacts: someone wrote content + dropped the mode.
    std::fs::write(&path, "stale content").expect("write stale content");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("clobber mode");

    setup_termination_message_file(path_str).expect("second setup");

    let meta = std::fs::metadata(&path).expect("stat after re-setup");
    assert_eq!(
        meta.permissions().mode() & 0o7777,
        0o666,
        "re-setup must restore 0o666 even if the file was left in a more \
         restrictive mode by a previous pod incarnation"
    );

    // Upstream's `os.Create` truncates an existing file to zero bytes. We
    // match that so the new pod incarnation reads back its own message, not
    // a leaked one from a prior container.
    let len = std::fs::metadata(&path).expect("stat for len").len();
    assert_eq!(
        len, 0,
        "re-setup must truncate any stale termination message from a prior \
         container incarnation"
    );
}
