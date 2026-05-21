# Upstream Kubernetes `.proto` snapshots

These files are verbatim copies of the Kubernetes `generated.proto` schemas at a
pinned release tag, used by the protobuf schema parity test
(`crates/api-server/tests/protobuf_schema_parity_upstream.rs`).

## Pinned version

* Tag: `release-1.35` (matches `registry.k8s.io/conformance:v1.35.0`)
* Destination directory: `v1.35/`

## File list

The upstream paths (relative to
`https://github.com/kubernetes/kubernetes/tree/release-1.35/staging/src/`) are:

* `k8s.io/api/core/v1/generated.proto`
* `k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto`
* `k8s.io/api/apps/v1/generated.proto`
* `k8s.io/api/batch/v1/generated.proto`
* `k8s.io/api/networking/v1/generated.proto`
* `k8s.io/api/policy/v1/generated.proto`
* `k8s.io/api/rbac/v1/generated.proto`
* `k8s.io/api/storage/v1/generated.proto`
* `k8s.io/api/autoscaling/v1/generated.proto`
* `k8s.io/api/autoscaling/v2/generated.proto`
* `k8s.io/api/discovery/v1/generated.proto`
* `k8s.io/apimachinery/pkg/runtime/generated.proto`
* `k8s.io/apimachinery/pkg/api/resource/generated.proto`
* `k8s.io/apimachinery/pkg/runtime/schema/generated.proto`
* `k8s.io/apimachinery/pkg/util/intstr/generated.proto`

The directory layout under `v1.35/` mirrors the upstream layout so re-syncs are
diff-clean.

## Bumping the pinned version

1. Edit `scripts/sync-upstream-protos.sh`:
   - Change the default `TAG` (`release-1.35` → e.g. `release-1.36`) and update
     the inline doc.
   - Change `DEST_VERSION` (or override per-run with the env var) so the new
     snapshot lives next to the previous one.
2. Run `bash scripts/sync-upstream-protos.sh`.
3. Update this README's "Pinned version" and "Destination directory" sections.
4. Update the test harness path constant if it references the version dir
   (`tests/protobuf_schema_parity_upstream.rs`).
5. Run the parity test and resolve any new mismatches.

## Do not edit these files

If a file refuses to parse, fix the parser glue in the parity test — not the
upstream snapshot.
