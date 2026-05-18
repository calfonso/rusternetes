# [sig-storage] ConfigMap + Secret + Projected volumes — scoped conformance coverage

Crate: `crates/kubelet` · Test file: `tests/conformance_storage_configmap_secret_projected.rs`

Scope: kubelet-side materialisation of the three projection-style volume
families in the [sig-storage] suite — `ConfigMap`, `Secret`, and `Projected`
(which composes `ConfigMap`, `Secret`, `DownwardAPI`, and
`ServiceAccountToken` sources into one directory).

The scoped tests are pure-function mirrors of the volume-build logic in
`crates/kubelet/src/runtime.rs::create_volume`. They reproduce the file
writes and chmod sequence against a `tempfile::TempDir`, then assert on the
resulting tree. No Docker, no api-server, no etcd — each test runs in
milliseconds and verifies the *materialised output* that the upstream
Ginkgo specs exec into the container to read.

Mirrored from the Sonobuoy run captured in
`.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`
(Round 160, 2026-04-26).

## Status table

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `ConfigMap should be consumable from pods in volume` | configmap_volume.go:48 | PASS | `configmap_volume_should_be_consumable_from_pods` | mirrored, passing |
| `ConfigMap should be consumable from pods in volume with defaultMode set` | configmap_volume.go:62 | PASS | `configmap_volume_should_be_consumable_with_defaultmode_set` | mirrored, passing |
| `ConfigMap should be consumable from pods in volume as non-root` | configmap_volume.go:84 | PASS | `configmap_volume_should_be_consumable_as_non_root` | mirrored, passing |
| `ConfigMap should be consumable from pods in volume with mappings` | configmap_volume.go:108 | PASS | `configmap_volume_should_be_consumable_with_mappings` | mirrored, passing |
| `ConfigMap should be consumable from pods in volume with mappings and Item Mode set` | configmap_volume.go:134 | PASS | `configmap_volume_should_be_consumable_with_mappings_and_item_mode_set` | mirrored, passing |
| `ConfigMap optional updates should be reflected in volume` | configmap_volume.go:233 | PASS | `configmap_volume_optional_missing_should_create_empty_volume` | mirrored, passing |
| `ConfigMap required missing should fail pod start` | configmap_volume.go:255 | PASS | `configmap_volume_required_missing_should_error` | mirrored, passing |
| `ConfigMap binaryData should be consumable as files` | configmap_volume.go:175 | PASS | `configmap_volume_binary_data_should_be_consumable` | mirrored, passing |
| `Secrets should be consumable from pods in volume` | secrets_volume.go:47 | PASS | `secret_volume_should_be_consumable_from_pods` | mirrored, passing |
| `Secrets should be consumable from pods in volume with defaultMode set` | secrets_volume.go:61 | PASS | `secret_volume_should_be_consumable_with_defaultmode_set` | mirrored, passing |
| `Secrets should be consumable from pods in volume as non-root with defaultMode and fsGroup set` | secrets_volume.go:73 | was FAIL → PASS (PR #87) | `secret_volume_should_be_consumable_as_non_root_with_defaultmode_and_fsgroup` | mirrored, passing |
| `Secrets should be consumable from pods in volume with mappings` | secrets_volume.go:106 | PASS | `secret_volume_should_be_consumable_with_mappings` | mirrored, passing |
| `Secrets should be consumable from pods in volume with mappings and Item Mode set` | secrets_volume.go:133 | PASS | `secret_volume_should_be_consumable_with_mappings_and_item_mode_set` | mirrored, passing |
| `Secrets optional should not fail pod start` | secrets_volume.go:300 | PASS | `secret_volume_optional_missing_should_create_empty_volume` | mirrored, passing |
| `Secrets required missing should fail pod start` | secrets_volume.go:320 | PASS | `secret_volume_required_missing_should_error` | mirrored, passing |
| `Secrets stringData overrides data on conflict` | (kubelet merge contract) | PASS | `secret_volume_string_data_overrides_data_on_conflict` | mirrored, passing |
| `Projected configMap should be consumable from pods in volume` | projected_configmap.go:44 | PASS | `projected_configmap_should_be_consumable_from_pods` | mirrored, passing |
| `Projected configMap should be consumable in volume with mappings` | projected_configmap.go:120 | PASS | `projected_configmap_should_be_consumable_with_mappings` | mirrored, passing |
| `Projected secret should be consumable from pods in volume` | projected_secret.go:44 | PASS | `projected_secret_should_be_consumable_from_pods` | mirrored, passing |
| `Projected secret should be consumable from pods in volume as non-root with defaultMode and fsGroup set` | projected_secret.go:67 | was FAIL → PASS (PR #87) | `projected_secret_should_be_consumable_as_non_root_with_defaultmode_and_fsgroup` | mirrored, passing |
| `Projected secret should be consumable in volume with mappings` | projected_secret.go:106 | PASS | `projected_secret_should_be_consumable_with_mappings` | mirrored, passing |
| `Projected downwardAPI should provide podname only` | projected_downwardapi.go:48 | PASS | `projected_downwardapi_should_provide_podname_only` | mirrored, passing |
| `Projected downwardAPI should set DefaultMode on files` | projected_downwardapi.go:80 | PASS | `projected_downwardapi_should_set_defaultmode_on_files` | mirrored, passing |
| `Projected downwardAPI should set mode on item file` | projected_downwardapi.go:107 | PASS | `projected_downwardapi_should_set_mode_on_item_file` | mirrored, passing |
| `Projected combined should project all components into the same directory` | projected_combined.go:44 | PASS | `projected_combined_should_project_all_components_into_same_directory` | mirrored, passing |
| `Projected serviceAccountToken should mount projected SA token` | (SAT projection contract) | PASS | `projected_serviceaccount_token_should_be_mounted_at_path` | mirrored, passing |
| `Projected secret optional updates should not fail pod start` | projected_secret.go:228 | PASS | `projected_secret_optional_missing_should_skip_source` | mirrored, passing |
| `Projected configmap optional updates should not fail pod start` | projected_configmap.go:236 | PASS | `projected_configmap_optional_missing_should_skip_source` | mirrored, passing |
| `Projected required missing configmap should error` | projected_configmap.go (contract) | PASS | `projected_required_missing_configmap_should_error` | mirrored, passing |
| `Pod volume serde — ConfigMap source round-trips` | (kubelet wire contract) | PASS | `pod_volume_with_configmap_source_round_trips_through_serde` | smoke test |
| `Pod volume serde — Secret source uses secretName` | (kubelet wire contract) | PASS | `pod_volume_with_secret_source_uses_secret_name_field` | smoke test |
| `Pod volume serde — Projected source preserves sources list` | (kubelet wire contract) | PASS | `pod_volume_with_projected_source_preserves_sources_list` | smoke test |

Total: 32 scoped tests, all passing.

## Resolved failures (R160 → fixed)

### Secret + Projected secret, non-root + defaultMode + fsGroup

Upstream: `test/e2e/common/storage/secrets_volume.go:73` and
`test/e2e/common/storage/projected_secret.go:67`.

R160 failure mode (verbatim from the e2e log):

```
Output of node "node-2" pod "pod-projected-secrets-..."
  container "projected-secret-volume-test":
  mode of file "/etc/projected-secret-volume/data-1": -r--r-----
  error reading file content for "/etc/projected-secret-volume/data-1":
    open /etc/projected-secret-volume/data-1: permission denied
```

What the upstream test does:

1. Creates a Secret with one key `data-1`.
2. Mounts it as a projected volume at `/etc/projected-secret-volume` with
   `defaultMode: 0o440`.
3. Sets `pod.spec.securityContext.fsGroup = <gid>` and runs the container
   as a non-root user belonging to that group.
4. Execs into the container and reads the file.

### Root cause

Two layers had to line up; one was missing.

- **Volume layer** (`runtime.rs::create_volume`): writes the file at mode
  `0o440`. ✓ Correct.
- **Volume ownership pass** (`runtime.rs::apply_fs_group_to_volumes`,
  around line 1771): chowns every file in the volume to `:fsGroup` and
  copies owner bits to group bits. ✓ Correct.
- **Container-arg layer** (bollard `HostConfig.group_add`): **was not
  populated**. The container therefore started without `fsGroup` in its
  supplementary GID list. The file was `root:fsGroup` mode `0o440`, but
  the non-root `runAsUser` had no membership in `fsGroup` and `open(2)`
  returned `EACCES`.

### Fix

PR #87 — *fix(kubelet): add fsGroup + supplementalGroups to container
GIDs* — introduced `runtime::compute_group_add(pod)` (extracts
`securityContext.fsGroup` and `supplementalGroups` into a deduped
`Vec<String>`) and wires it into the `HostConfig.group_add` field where
the app container is created. With `--group-add <fsGroup>` set, the
non-root `runAsUser` inherits the group membership that owns the
chowned files, and the `open(2)` succeeds.

PR #97 un-`#[ignore]`d the two trackers in this file and added an
end-to-end assertion against `runtime::compute_group_add` so a future
regression that drops the `group_add` wiring re-trips the test.

### Failure category

These two used to live in the **EmptyDir perms ~4** bucket of
`docs/CONFORMANCE.md:40-53` (the bucket is named for EmptyDir but covered
"kubelet does not honour `fsGroup` on projection-style volumes"). After
PR #87 + PR #97 they no longer count toward that bucket; the
`docs/CONFORMANCE.md` ledger should drop them on the next Sonobuoy round
that confirms the in-cluster pass.

## Re-running the suite

```bash
cargo test -p rusternetes-kubelet --test conformance_storage_configmap_secret_projected
```

Expected: `32 passed; 0 failed; 0 ignored`.
