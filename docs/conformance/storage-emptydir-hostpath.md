# [sig-storage] EmptyDir + HostPath volumes — scoped conformance coverage

Crate: `crates/kubelet` · Test file: `tests/conformance_storage_emptydir_hostpath.rs`

Pure-function kubelet unit. No Docker, no api-server, no live cluster. The
tests exercise two public helpers in `crates/kubelet/src/runtime.rs`:

- `setup_emptydir_dir(path)` — mirrors upstream
  `pkg/volume/emptydir/empty_dir.go::setupDir` (chmod 0o777, idempotent).
- `check_host_path_type(path, type_)` — mirrors upstream
  `pkg/volume/host_path/host_path.go::checkType` + `createHostPath{,File}`.

Sonobuoy extraction source: verbatim Ginkgo descriptors at
<https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/storage/>,
cross-checked against the captured e2e log at
`.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`.

Cross-ref: [`docs/CONFORMANCE.md`](../CONFORMANCE.md) "EmptyDir volume perms"
failure bucket (~4 failures at Round 160) — those are a macOS
Podman-Machine / Docker-Desktop virtiofs limitation on the dev runner
(host chmod bits aren't propagated through the shared-filesystem layer to
the container bind mount). On Linux runners the same tests PASS because
`setup_emptydir_dir`'s `chmod 0o777` is honored by the kernel. The Rust
unit test exercises the chmod via `tempfile` directly, so these mirrors
PASS on both Linux and macOS.

## EmptyDir — `test/e2e/common/storage/empty_dir.go`

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `volume on tmpfs should have the correct mode [LinuxOnly] [Conformance]` | empty_dir.go:77 | PASS (Linux) / macOS-FAIL bucket | `emptydir_volume_on_tmpfs_should_have_correct_mode_default` | mirrored, passing |
| `should support (root,0644,tmpfs) [LinuxOnly] [Conformance]` | empty_dir.go:89 | PASS (Linux) / macOS-FAIL bucket | `emptydir_should_support_root_0644_tmpfs` | mirrored, passing |
| `should support (root,0666,tmpfs) [LinuxOnly] [Conformance]` | empty_dir.go:101 | PASS (Linux) / macOS-FAIL bucket | `emptydir_should_support_root_0666_tmpfs` | mirrored, passing |
| `should support (root,0777,tmpfs) [LinuxOnly] [Conformance]` | empty_dir.go:113 | PASS (Linux) / macOS-FAIL bucket | `emptydir_should_support_root_0777_tmpfs` | mirrored, passing |
| `should support (non-root,0644,tmpfs) [LinuxOnly] [Conformance]` | empty_dir.go:125 | PASS (Linux) / macOS-FAIL bucket | `emptydir_should_support_nonroot_0644_tmpfs` | mirrored, passing |
| `should support (non-root,0666,tmpfs) [LinuxOnly] [Conformance]` | empty_dir.go:137 | PASS (Linux) / macOS-FAIL bucket | `emptydir_should_support_nonroot_0666_tmpfs` | mirrored, passing |
| `should support (non-root,0777,tmpfs) [LinuxOnly] [Conformance]` | empty_dir.go:149 | PASS (Linux) / macOS-FAIL bucket | `emptydir_should_support_nonroot_0777_tmpfs` | mirrored, passing |
| `volume on default medium should have the correct mode [LinuxOnly] [Conformance]` | empty_dir.go:161 | PASS (Linux) / macOS-FAIL bucket | `emptydir_volume_on_default_medium_should_have_correct_mode` | mirrored, passing |
| `should support (root,0644,default) [LinuxOnly] [Conformance]` | empty_dir.go:173 | PASS (Linux) / macOS-FAIL bucket | `emptydir_should_support_root_0644_default` | mirrored, passing |
| `pod should support shared volumes between containers [Conformance]` | empty_dir.go:246 | PASS | `emptydir_pod_should_support_shared_volumes_between_containers` | mirrored, passing |
| `pod should support memory backed volumes of specified size` (not [Conformance]) | empty_dir.go:304 | PASS | `emptydir_pod_should_support_memory_backed_volume_of_specified_size` | mirrored, passing |
| `new files should be created with FSGroup ownership when container is root [LinuxOnly]` | empty_dir.go:47 | PASS | `emptydir_files_with_fsgroup_mirror_owner_bits_and_dir_is_sgid` | mirrored, passing |
| `nonexistent volume subPath should have the correct mode and owner using FSGroup` | empty_dir.go:55 | PASS | `emptydir_nonexistent_subpath_is_created_with_volume_mode` | mirrored, passing |

### Known-failure bucket mapping (Round 160)

The four entries in `docs/CONFORMANCE.md` "EmptyDir volume perms" bucket
all map to the `emptydir_should_support_*` mode-bit family above. They are
NOT `#[ignore]`d in this scoped suite because the underlying kubelet code
path (`setup_emptydir_dir`'s `chmod 0o777`) is correct and is exercised
directly by `tempfile`; the macOS-only failure is in the Docker /
Podman-Machine virtiofs shim, not in our kubelet. A future PR migrating
the conformance runner to a Linux self-hosted runner is expected to clear
the bucket without code changes.

## HostPath — `test/e2e/common/storage/host_path.go`

The three upstream [NodeConformance] HostPath descriptors all use `type:
""` (legacy unchecked variant). We mirror those three AND add coverage for
every other `type` variant the production code path supports, since
correct type-validation is a hard kubelet invariant.

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `should give a volume the correct mode [LinuxOnly] [NodeConformance]` | host_path.go:48 | PASS | `hostpath_should_give_volume_the_correct_mode` | mirrored, passing |
| `should support r/w [NodeConformance]` | host_path.go:63 | PASS | `hostpath_should_support_read_write` | mirrored, passing |
| `should support subPath [NodeConformance]` | host_path.go:90 | PASS | `hostpath_should_support_subpath` | mirrored, passing |
| `type=DirectoryOrCreate creates missing dir` (kubelet invariant) | host_path.go::createHostPath | PASS | `hostpath_type_directory_or_create_creates_missing_dir` | mirrored, passing |
| `type=DirectoryOrCreate idempotent on existing dir` (kubelet invariant) | host_path.go::createHostPath | PASS | `hostpath_type_directory_or_create_accepts_existing_dir` | mirrored, passing |
| `type=Directory rejects missing path` (kubelet invariant) | host_path.go::checkType | PASS | `hostpath_type_directory_fails_when_missing` | mirrored, passing |
| `type=Directory rejects file path` (kubelet invariant) | host_path.go::checkType | PASS | `hostpath_type_directory_fails_when_path_is_file` | mirrored, passing |
| `type=FileOrCreate creates missing file` (kubelet invariant) | host_path.go::createHostPathFile | PASS | `hostpath_type_file_or_create_creates_missing_file` | mirrored, passing |
| `type=FileOrCreate idempotent on existing file` (kubelet invariant) | host_path.go::createHostPathFile | PASS | `hostpath_type_file_or_create_accepts_existing_file` | mirrored, passing |
| `type=File rejects missing path` (kubelet invariant) | host_path.go::checkType | PASS | `hostpath_type_file_fails_when_missing` | mirrored, passing |
| `type=Socket rejects non-socket path` (kubelet invariant) | host_path.go::checkType | PASS | `hostpath_type_socket_rejects_non_socket` | mirrored, passing |
| `type=Socket accepts real Unix socket` (kubelet invariant) | host_path.go::checkType | PASS | `hostpath_type_socket_accepts_real_socket` | mirrored, passing |
| `type=None / type="" accepts any path` (legacy v1 behavior) | host_path.go::checkType | PASS | `hostpath_type_none_accepts_any_path_including_missing` | mirrored, passing |
| `unknown type string rejected` (kubelet invariant) | host_path.go::checkType | PASS | `hostpath_type_unknown_string_is_unsupported` | mirrored, passing |

## Running

```bash
cargo test -p rusternetes-kubelet --test conformance_storage_emptydir_hostpath
```

Expected: every test PASSes (no `#[ignore]` markers). Tests gated by
`#[cfg(unix)]` are skipped on non-Unix targets; CI runs on Linux so all
mode-bit assertions execute.
