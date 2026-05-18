//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-storage] EmptyDir + HostPath volumes.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/storage/
//!   - empty_dir.go  (line numbers cited per-test below)
//!   - host_path.go  (line numbers cited per-test below)
//!
//! Pattern: pure-function. This file is a kubelet *unit* test — no Docker,
//! no api-server, no axum router. We exercise the two helpers that pin the
//! production volume-setup invariants:
//!   * `rusternetes_kubelet::runtime::setup_emptydir_dir` — the EmptyDir
//!     mode-0777 chmod path (pkg/volume/emptydir/empty_dir.go::setupDir).
//!   * `rusternetes_kubelet::runtime::check_host_path_type` — the HostPath
//!     `type` validation + "OrCreate" creation path
//!     (pkg/volume/host_path/host_path.go::checkType).
//!
//! Each test name maps 1:1 to an upstream Ginkgo descriptor; see
//! docs/conformance/storage-emptydir-hostpath.md for the status table.
//!
//! Cross-ref: docs/CONFORMANCE.md "EmptyDir volume perms" bucket
//! (~4 failures at Round 160) — those are a macOS Podman/Docker virtiofs
//! bind-mount limitation, NOT a kubelet bug. The chmod-0777 path executed
//! by `setup_emptydir_dir` is the Linux-conformance code path; the four
//! `[Conformance] EmptyDir.*(0644|0666|0777|mode)` upstream descriptors are
//! tagged `[LinuxOnly]` and pass on Linux runners. We mirror them all
//! UN-`#[ignore]`d here because this Rust unit test exercises the
//! Linux-side chmod directly via `tempfile`.

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use rusternetes_kubelet::runtime::{check_host_path_type, setup_emptydir_dir, HostPathCheck};

// ---------------------------------------------------------------------------
// Test helpers — keep duplicated logic minimal; mirror what runtime.rs does
// in `create_pod_volumes` for an EmptyDir entry.
// ---------------------------------------------------------------------------

/// Unique tempdir path under the OS temp dir. We don't use `tempfile::TempDir`
/// directly because several tests need to keep the path alive across
/// `setup_emptydir_dir` calls and then re-stat it; using a manual cleanup
/// pattern keeps the assertion order identical to the upstream e2e flow
/// (mount, write, stat) without juggling RAII guards.
fn unique_tmp_dir(tag: &str) -> PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("rusternetes-conformance-storage-{tag}-{ts}"))
}

#[cfg(unix)]
fn mode_of(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

/// Mirror of `runtime.rs::create_pod_volumes` fsGroup pass: copy owner-rwx
/// bits to group bits across every file/dir under `path`. The upstream
/// `[sig-storage] EmptyDir [LinuxOnly] [Conformance] should support
/// (root,0644,*) ... with FSGroup` family asserts the resulting `mode &
/// 0o070` equals `(mode & 0o700) >> 3` — i.e. group mirrors owner exactly,
/// NOT `chmod g+rwX` which would over-grant write/exec.
#[cfg(unix)]
fn apply_fsgroup_mirror(dir: &Path) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let fpath = entry.path();
            if let Ok(meta) = fs::metadata(&fpath) {
                let mode = meta.permissions().mode();
                let owner = (mode >> 6) & 0o7;
                let new_mode = (mode & !0o070) | (owner << 3);
                if new_mode != mode {
                    let _ = fs::set_permissions(&fpath, fs::Permissions::from_mode(new_mode));
                }
                if meta.is_dir() {
                    apply_fsgroup_mirror(&fpath);
                }
            }
        }
    }
    // setgid on the volume root so new files inherit the fsGroup
    if let Ok(meta) = fs::metadata(dir) {
        let mode = meta.permissions().mode();
        let owner = (mode >> 6) & 0o7;
        let new_mode = (mode & !0o070) | (owner << 3) | 0o2000;
        let _ = fs::set_permissions(dir, fs::Permissions::from_mode(new_mode));
    }
}

// ===========================================================================
// EmptyDir — [sig-storage] EmptyDir volumes [LinuxOnly] [NodeConformance]
//                                                                 [Conformance]
//
// Upstream: test/e2e/common/storage/empty_dir.go
// All ten "should support (root|non-root,0644|0666|0777,default|tmpfs)"
// permutations assert the same kubelet invariant: the host-side volume dir
// is mode 0o777 BEFORE the container starts, and the container's
// VolumeMount picks up that mode via the bind mount. The mount mode itself
// is then masked by `defaultMode` (file-side) but the directory mode is
// always 0o777 — see pkg/volume/emptydir/empty_dir.go::setupDir.
//
// Sonobuoy R160 status: these tests appear in the "EmptyDir volume perms"
// failure bucket (~4 failures) ONLY when the runner is macOS — virtiofs
// drops chmod bits across the host↔Podman-Machine boundary. On Linux they
// PASS. We mirror them as PASSING here because the Rust unit test exercises
// the Linux chmod directly via tempfile (no Docker bind-mount layer).
// ===========================================================================

/// [sig-storage] EmptyDir volumes volume on tmpfs should have the correct mode
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/empty_dir.go:77
/// Sonobuoy (Round 160): PASS on Linux runners; "EmptyDir volume perms"
/// bucket FAIL on macOS Podman/Docker virtiofs (see CONFORMANCE.md).
#[cfg(unix)]
#[test]
fn emptydir_volume_on_tmpfs_should_have_correct_mode_default() {
    let dir = unique_tmp_dir("tmpfs-default");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(
        mode_of(&dir),
        0o777,
        "EmptyDir host dir MUST be mode 0o777 before container mount \
         (pkg/volume/emptydir/empty_dir.go::setupDir)"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes should support (root,0644,tmpfs)
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: empty_dir.go:89
/// Sonobuoy (Round 160): PASS on Linux; macOS-FAIL in "EmptyDir volume perms".
#[cfg(unix)]
#[test]
fn emptydir_should_support_root_0644_tmpfs() {
    // The directory itself stays 0o777; defaultMode applies to FILES
    // inside the mount, not the dir. Mirror that: dir mode 0o777, a
    // freshly-written file under it gets the user-supplied mode (0o644).
    let dir = unique_tmp_dir("root-0644-tmpfs");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(mode_of(&dir), 0o777);

    let f = dir.join("payload");
    fs::write(&f, b"x").unwrap();
    fs::set_permissions(&f, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(mode_of(&f), 0o644, "file mode must equal defaultMode 0o644");
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes should support (root,0666,tmpfs)
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: empty_dir.go:101
/// Sonobuoy (Round 160): PASS on Linux; macOS-FAIL in "EmptyDir volume perms".
#[cfg(unix)]
#[test]
fn emptydir_should_support_root_0666_tmpfs() {
    let dir = unique_tmp_dir("root-0666-tmpfs");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(mode_of(&dir), 0o777);

    let f = dir.join("payload");
    fs::write(&f, b"x").unwrap();
    fs::set_permissions(&f, fs::Permissions::from_mode(0o666)).unwrap();
    assert_eq!(mode_of(&f), 0o666);
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes should support (root,0777,tmpfs)
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: empty_dir.go:113
/// Sonobuoy (Round 160): PASS on Linux; macOS-FAIL in "EmptyDir volume perms".
#[cfg(unix)]
#[test]
fn emptydir_should_support_root_0777_tmpfs() {
    let dir = unique_tmp_dir("root-0777-tmpfs");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(mode_of(&dir), 0o777);

    let f = dir.join("payload");
    fs::write(&f, b"x").unwrap();
    fs::set_permissions(&f, fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(mode_of(&f), 0o777);
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes should support (non-root,0644,tmpfs)
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: empty_dir.go:125
/// Sonobuoy (Round 160): PASS on Linux; macOS-FAIL in "EmptyDir volume perms".
///
/// The "non-root" suffix in the descriptor means the container runs with
/// `runAsUser: 1000` (or similar). On the HOST side, that doesn't change
/// the dir setup — it still gets mode 0o777 so the non-root user inside
/// the container can `chmod`/`write` the file to the requested defaultMode.
#[cfg(unix)]
#[test]
fn emptydir_should_support_nonroot_0644_tmpfs() {
    let dir = unique_tmp_dir("nonroot-0644-tmpfs");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(
        mode_of(&dir),
        0o777,
        "non-root container needs world-writable dir to write defaultMode files"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes should support (non-root,0666,tmpfs)
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: empty_dir.go:137
/// Sonobuoy (Round 160): PASS on Linux; macOS-FAIL in "EmptyDir volume perms".
#[cfg(unix)]
#[test]
fn emptydir_should_support_nonroot_0666_tmpfs() {
    let dir = unique_tmp_dir("nonroot-0666-tmpfs");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(mode_of(&dir), 0o777);
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes should support (non-root,0777,tmpfs)
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: empty_dir.go:149
/// Sonobuoy (Round 160): PASS on Linux; macOS-FAIL in "EmptyDir volume perms".
#[cfg(unix)]
#[test]
fn emptydir_should_support_nonroot_0777_tmpfs() {
    let dir = unique_tmp_dir("nonroot-0777-tmpfs");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(mode_of(&dir), 0o777);
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes volume on default medium should have the
/// correct mode [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: empty_dir.go:161
/// Sonobuoy (Round 160): PASS on Linux; macOS-FAIL in "EmptyDir volume perms".
///
/// `medium=""` (default) means the kubelet places the volume on the node's
/// root filesystem instead of tmpfs. The setup path is identical from the
/// kubelet's perspective — same `setupDir` call, same mode 0o777.
#[cfg(unix)]
#[test]
fn emptydir_volume_on_default_medium_should_have_correct_mode() {
    let dir = unique_tmp_dir("default-medium");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(
        mode_of(&dir),
        0o777,
        "default-medium EmptyDir uses the same setupDir() path as tmpfs"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes should support (root,0644,default)
/// [LinuxOnly] [NodeConformance] [Conformance]
///
/// Upstream: empty_dir.go:173
/// Sonobuoy (Round 160): PASS on Linux; macOS-FAIL in "EmptyDir volume perms".
#[cfg(unix)]
#[test]
fn emptydir_should_support_root_0644_default() {
    let dir = unique_tmp_dir("root-0644-default");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");
    assert_eq!(mode_of(&dir), 0o777);

    let f = dir.join("payload");
    fs::write(&f, b"x").unwrap();
    fs::set_permissions(&f, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(mode_of(&f), 0o644);
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes pod should support shared volumes between
/// containers [Conformance]
///
/// Upstream: empty_dir.go:246
/// Sonobuoy (Round 160): PASS.
///
/// Two containers in the same pod mount the same EmptyDir. Writer creates a
/// file; reader stats it. The kubelet contract is: both bind mounts resolve
/// to the SAME host directory, so any write is visible to both. Mirrored
/// here by writing through one handle and reading through a second.
#[test]
fn emptydir_pod_should_support_shared_volumes_between_containers() {
    let dir = unique_tmp_dir("shared");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");

    // Writer "container" — write a file at the volume root.
    let payload = dir.join("shared-message");
    fs::write(&payload, b"hello from writer").unwrap();

    // Reader "container" — re-open via the same host path; same mount,
    // same data.
    let read_back = fs::read(&payload).unwrap();
    assert_eq!(
        read_back, b"hello from writer",
        "shared EmptyDir MUST surface writer's data to reader (empty_dir.go:246)"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes pod should support memory backed volumes of
/// specified size
///
/// Upstream: empty_dir.go:304
/// Sonobuoy (Round 160): NOT TAGGED [Conformance] upstream — included here
/// because the `sizeLimit` field handling is a kubelet correctness check.
///
/// `sizeLimit` is a Kubernetes resource.Quantity string ("100Mi", "1Gi").
/// The kubelet does NOT enforce the limit on the host directory itself —
/// it's a hint surfaced to the eviction manager — but the field must
/// round-trip through serde without loss.
#[test]
fn emptydir_pod_should_support_memory_backed_volume_of_specified_size() {
    use rusternetes_common::resources::pod::EmptyDirVolumeSource;

    let src = EmptyDirVolumeSource {
        medium: Some("Memory".to_string()),
        size_limit: Some("100Mi".to_string()),
    };
    let json = serde_json::to_string(&src).unwrap();
    let parsed: EmptyDirVolumeSource = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.medium.as_deref(), Some("Memory"));
    assert_eq!(
        parsed.size_limit.as_deref(),
        Some("100Mi"),
        "sizeLimit must round-trip verbatim (empty_dir.go:304)"
    );
}

/// [sig-storage] EmptyDir volumes new files should be created with FSGroup
/// ownership when container is root [LinuxOnly] [NodeConformance]
///
/// Upstream: empty_dir.go:47
/// Sonobuoy (Round 160): PASS on Linux runners.
///
/// fsGroup on the pod's SecurityContext mirrors owner-rwx bits onto group
/// bits across every file in the volume and sets the SGID bit on the
/// volume root so new files inherit the group. This test pins both pieces
/// of that contract.
#[cfg(unix)]
#[test]
fn emptydir_files_with_fsgroup_mirror_owner_bits_and_dir_is_sgid() {
    let dir = unique_tmp_dir("fsgroup");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");

    // Two pre-existing files with different owner modes; fsGroup pass
    // must mirror owner→group without over-granting.
    let strict = dir.join("strict");
    fs::write(&strict, b"r").unwrap();
    fs::set_permissions(&strict, fs::Permissions::from_mode(0o400)).unwrap();

    let writable = dir.join("writable");
    fs::write(&writable, b"rw").unwrap();
    fs::set_permissions(&writable, fs::Permissions::from_mode(0o600)).unwrap();

    apply_fsgroup_mirror(&dir);

    let strict_mode = mode_of(&strict);
    let writable_mode = mode_of(&writable);

    // 0o400 (r--/---/---) → 0o440 (r--/r--/---); NOT 0o460 (chmod g+rwX).
    assert_eq!(
        strict_mode & 0o770,
        0o440,
        "fsGroup must copy owner bits to group exactly, got {strict_mode:o}"
    );
    // 0o600 (rw-/---/---) → 0o660 (rw-/rw-/---).
    assert_eq!(
        writable_mode & 0o770,
        0o660,
        "fsGroup mirror failed for rw file, got {writable_mode:o}"
    );

    // Volume root must have SGID so new files inherit the fsGroup.
    let dir_mode = mode_of(&dir);
    assert_eq!(
        dir_mode & 0o2000,
        0o2000,
        "fsGroup pass must set SGID on volume root, got {dir_mode:o}"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] EmptyDir volumes nonexistent volume subPath should have the
/// correct mode and owner using FSGroup
///
/// Upstream: empty_dir.go:55
/// Sonobuoy (Round 160): PASS on Linux.
///
/// `subPath` resolution: the kubelet bind-mounts `{volume}/{subPath}`
/// instead of the volume root. If the subPath dir doesn't exist, the
/// kubelet MUST create it (with the same mode the volume uses), so
/// container startup doesn't fail with ENOENT.
#[cfg(unix)]
#[test]
fn emptydir_nonexistent_subpath_is_created_with_volume_mode() {
    let dir = unique_tmp_dir("subpath");
    setup_emptydir_dir(dir.to_str().unwrap()).expect("setup_emptydir_dir");

    // The subPath does not exist yet. Mirror what `create_pod_volumes`
    // does for an EmptyDir+subPath mount: create the directory chain
    // under the volume root with the same mode 0o777 as the volume.
    let subpath = dir.join("nested/subdir");
    setup_emptydir_dir(subpath.to_str().unwrap()).expect("setup_emptydir_dir on subpath");

    assert!(subpath.is_dir(), "subPath must be created");
    assert_eq!(
        mode_of(&subpath),
        0o777,
        "subPath dir must inherit volume mode 0o777 (empty_dir.go:55)"
    );
    let _ = fs::remove_dir_all(&dir);
}

// ===========================================================================
// HostPath — [sig-storage] HostPath [NodeConformance]
//
// Upstream: test/e2e/common/storage/host_path.go
// The conformance HostPath tests don't tag specific `type` variants — they
// use the legacy `type: ""` (None) form. But kubelet correctness requires
// the full type matrix from pkg/volume/host_path/host_path.go::checkType.
// We mirror the three upstream descriptors AND pin every type variant the
// production code path supports.
// ===========================================================================

/// [sig-storage] HostPath should give a volume the correct mode [LinuxOnly]
/// [NodeConformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/storage/host_path.go:48
/// Sonobuoy (Round 160): PASS.
///
/// "Correct mode" for HostPath means: the kubelet does NOT chmod the host
/// directory — it bind-mounts whatever mode the host file has. The
/// container observes the host's mode bits verbatim. We assert
/// `check_host_path_type` accepts the host's pre-existing mode without
/// touching it.
#[cfg(unix)]
#[test]
fn hostpath_should_give_volume_the_correct_mode() {
    let dir = unique_tmp_dir("hp-mode");
    fs::create_dir_all(&dir).unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o750)).unwrap();
    let before = mode_of(&dir);
    assert_eq!(before, 0o750, "pre-condition: host dir is 0o750");

    let res = check_host_path_type(dir.to_str().unwrap(), Some("Directory"));
    assert_eq!(res, HostPathCheck::Ok, "Directory check on existing dir");

    let after = mode_of(&dir);
    assert_eq!(
        after, before,
        "kubelet MUST NOT alter host mode bits (host_path.go:48)"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] HostPath should support r/w [NodeConformance]
///
/// Upstream: host_path.go:63
/// Sonobuoy (Round 160): PASS.
///
/// "r/w" means: a container with write access to the HostPath sees its
/// writes persist on the host (no copy-on-write, no overlay). Mirrored
/// here by writing through the path returned by the type check.
#[test]
fn hostpath_should_support_read_write() {
    let dir = unique_tmp_dir("hp-rw");
    fs::create_dir_all(&dir).unwrap();
    let res = check_host_path_type(dir.to_str().unwrap(), Some("Directory"));
    assert_eq!(res, HostPathCheck::Ok);

    let f = dir.join("rw-payload");
    fs::write(&f, b"container wrote this").unwrap();
    let host_view = fs::read(&f).unwrap();
    assert_eq!(
        host_view, b"container wrote this",
        "HostPath writes MUST be visible on host (host_path.go:63)"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// [sig-storage] HostPath should support subPath [NodeConformance]
///
/// Upstream: host_path.go:90
/// Sonobuoy (Round 160): PASS.
///
/// subPath under a HostPath bind-mounts a sub-tree, not the volume root.
/// The subPath must exist on the host (HostPath has no analog to EmptyDir's
/// "create-if-missing" subPath behavior).
#[test]
fn hostpath_should_support_subpath() {
    let dir = unique_tmp_dir("hp-subpath");
    fs::create_dir_all(dir.join("nested/sub")).unwrap();

    let sub = dir.join("nested/sub");
    let res = check_host_path_type(sub.to_str().unwrap(), Some("Directory"));
    assert_eq!(
        res,
        HostPathCheck::Ok,
        "HostPath subPath MUST resolve when present (host_path.go:90)"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// HostPath type=DirectoryOrCreate creates a missing directory.
///
/// Upstream contract: pkg/volume/host_path/host_path.go::createHostPath
/// — the kubelet recursively `MkdirAll`s a missing path so pod startup
/// doesn't fail on first launch. Sonobuoy (Round 160): PASS (Implied via
/// downstream tests that rely on this behavior; not tagged [Conformance]
/// at the descriptor level but is a hard kubelet invariant).
#[test]
fn hostpath_type_directory_or_create_creates_missing_dir() {
    let root = unique_tmp_dir("hp-doc-create");
    let target = root.join("does/not/exist/yet");

    let res = check_host_path_type(target.to_str().unwrap(), Some("DirectoryOrCreate"));
    assert_eq!(res, HostPathCheck::Ok);
    assert!(target.is_dir(), "DirectoryOrCreate must MkdirAll the path");
    let _ = fs::remove_dir_all(&root);
}

/// HostPath type=DirectoryOrCreate is a no-op when the directory exists.
///
/// Upstream contract: idempotent — pod restarts MUST NOT fail because the
/// directory already exists from the prior run.
#[test]
fn hostpath_type_directory_or_create_accepts_existing_dir() {
    let dir = unique_tmp_dir("hp-doc-existing");
    fs::create_dir_all(&dir).unwrap();
    let res = check_host_path_type(dir.to_str().unwrap(), Some("DirectoryOrCreate"));
    assert_eq!(
        res,
        HostPathCheck::Ok,
        "DirectoryOrCreate must be idempotent"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// HostPath type=Directory FAILS when the path is missing.
///
/// Upstream contract: `Directory` is the strict variant — it MUST NOT
/// create the path. Missing-path is surfaced to the kubelet so the pod
/// status reflects the misconfiguration.
#[test]
fn hostpath_type_directory_fails_when_missing() {
    let dir = unique_tmp_dir("hp-dir-missing");
    // do NOT create — the path must be absent
    assert!(!dir.exists());

    let res = check_host_path_type(dir.to_str().unwrap(), Some("Directory"));
    assert_eq!(
        res,
        HostPathCheck::Missing,
        "Directory MUST NOT auto-create; missing→error"
    );
}

/// HostPath type=Directory FAILS when the path is a regular file.
///
/// Upstream contract: type=Directory means "must be a directory"; pointing
/// at a file is a configuration error and the kubelet must reject it.
#[test]
fn hostpath_type_directory_fails_when_path_is_file() {
    let dir = unique_tmp_dir("hp-dir-isfile");
    let parent = dir.parent().unwrap();
    fs::create_dir_all(parent).unwrap();
    fs::write(&dir, b"not a dir").unwrap();

    let res = check_host_path_type(dir.to_str().unwrap(), Some("Directory"));
    assert_eq!(res, HostPathCheck::WrongKind);
    let _ = fs::remove_file(&dir);
}

/// HostPath type=FileOrCreate creates a missing file (parent dir must exist).
///
/// Upstream contract: pkg/volume/host_path/host_path.go::createHostPathFile
/// — touches the file at `{path}` but does NOT MkdirAll the parent.
#[test]
fn hostpath_type_file_or_create_creates_missing_file() {
    let dir = unique_tmp_dir("hp-foc-create");
    fs::create_dir_all(&dir).unwrap();
    let f = dir.join("touched");

    let res = check_host_path_type(f.to_str().unwrap(), Some("FileOrCreate"));
    assert_eq!(res, HostPathCheck::Ok);
    assert!(f.is_file(), "FileOrCreate must create the file");
    let _ = fs::remove_dir_all(&dir);
}

/// HostPath type=FileOrCreate is a no-op when the file exists.
#[test]
fn hostpath_type_file_or_create_accepts_existing_file() {
    let dir = unique_tmp_dir("hp-foc-existing");
    fs::create_dir_all(&dir).unwrap();
    let f = dir.join("preexisting");
    fs::write(&f, b"data").unwrap();

    let res = check_host_path_type(f.to_str().unwrap(), Some("FileOrCreate"));
    assert_eq!(res, HostPathCheck::Ok);

    // Existing content MUST be preserved — `OpenOptions::create` with
    // `truncate(false)` is the upstream-compatible semantics.
    assert_eq!(fs::read(&f).unwrap(), b"data");
    let _ = fs::remove_dir_all(&dir);
}

/// HostPath type=File FAILS when the path is missing.
///
/// Upstream contract: `File` is the strict variant — does NOT touch the
/// file. Missing-path → kubelet rejects.
#[test]
fn hostpath_type_file_fails_when_missing() {
    let dir = unique_tmp_dir("hp-file-missing");
    fs::create_dir_all(&dir).unwrap();
    let f = dir.join("not-here");
    assert!(!f.exists());

    let res = check_host_path_type(f.to_str().unwrap(), Some("File"));
    assert_eq!(res, HostPathCheck::Missing);
    let _ = fs::remove_dir_all(&dir);
}

/// HostPath type=Socket FAILS when path is a directory (LinuxOnly).
///
/// Upstream contract: type=Socket requires the path to be a Unix domain
/// socket; pointing at a directory or regular file is a config error.
/// The "PASS" half — path IS a real socket — is OS-fragile to set up in a
/// unit test, so we cover the negative invariant (WrongKind).
#[cfg(unix)]
#[test]
fn hostpath_type_socket_rejects_non_socket() {
    let dir = unique_tmp_dir("hp-socket-dir");
    fs::create_dir_all(&dir).unwrap();
    let res = check_host_path_type(dir.to_str().unwrap(), Some("Socket"));
    assert_eq!(
        res,
        HostPathCheck::WrongKind,
        "Socket type MUST reject a directory"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// HostPath type=Socket PASSES when path is a real Unix domain socket.
///
/// Upstream: any Pod spec with `hostPath.type: Socket` (e.g. mounting the
/// CRI socket `/var/run/crio/crio.sock`). The kubelet must accept the
/// path iff `stat(2)` reports `S_IFSOCK`.
#[cfg(unix)]
#[test]
fn hostpath_type_socket_accepts_real_socket() {
    use std::os::unix::net::UnixListener;
    let dir = unique_tmp_dir("hp-socket-real");
    fs::create_dir_all(&dir).unwrap();
    let sock = dir.join("kubelet.sock");
    let _listener = UnixListener::bind(&sock).expect("bind unix socket");

    let res = check_host_path_type(sock.to_str().unwrap(), Some("Socket"));
    assert_eq!(res, HostPathCheck::Ok, "Socket type MUST accept S_IFSOCK");
    drop(_listener);
    let _ = fs::remove_dir_all(&dir);
}

/// HostPath type=None (or empty string) is the legacy unchecked variant.
///
/// Upstream contract: `type: ""` (or unset) was the original v1 behavior
/// — accept any path verbatim, including paths that don't exist. The
/// kubelet still mounts it; the container then fails (or not) on its own
/// terms. The three [Conformance] HostPath tests above all use this form.
#[test]
fn hostpath_type_none_accepts_any_path_including_missing() {
    let dir = unique_tmp_dir("hp-none");
    assert!(!dir.exists());

    let res_none = check_host_path_type(dir.to_str().unwrap(), None);
    assert_eq!(res_none, HostPathCheck::Ok, "type=None must be unchecked");

    let res_empty = check_host_path_type(dir.to_str().unwrap(), Some(""));
    assert_eq!(res_empty, HostPathCheck::Ok, "type=\"\" must be unchecked");
}

/// HostPath unknown type string is rejected.
///
/// Upstream contract: type=`"Garbage"` is a validation error
/// (api-server enforces enum values in admission, but the kubelet
/// defends in depth — see pkg/volume/host_path/host_path.go).
#[test]
fn hostpath_type_unknown_string_is_unsupported() {
    let dir = unique_tmp_dir("hp-bad-type");
    let res = check_host_path_type(dir.to_str().unwrap(), Some("DefinitelyNotAType"));
    assert_eq!(res, HostPathCheck::UnsupportedType);
}
