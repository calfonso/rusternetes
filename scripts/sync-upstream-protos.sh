#!/usr/bin/env bash
# Fetches upstream Kubernetes .proto files for the schema parity test.
#
# Usage:
#   bash scripts/sync-upstream-protos.sh                   # default tag release-1.35
#   bash scripts/sync-upstream-protos.sh release-1.36      # override
#
# Files are written verbatim under
#   crates/api-server/proto/upstream/v1.35/<upstream-path>
# mirroring the upstream layout so that subsequent re-syncs are diff-clean.
#
# When bumping the pinned tag, also bump the destination v1.35 directory name
# (and update crates/api-server/proto/upstream/README.md).

set -euo pipefail

TAG="${1:-release-1.35}"
DEST_VERSION="${DEST_VERSION:-v1.35}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST_ROOT="$ROOT/crates/api-server/proto/upstream/$DEST_VERSION"

# Each entry is the upstream path *relative to* https://raw.githubusercontent.com/kubernetes/kubernetes/<TAG>/staging/src/
FILES=(
    "k8s.io/api/core/v1/generated.proto"
    "k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto"
    "k8s.io/api/apps/v1/generated.proto"
    "k8s.io/api/batch/v1/generated.proto"
    "k8s.io/api/networking/v1/generated.proto"
    "k8s.io/api/policy/v1/generated.proto"
    "k8s.io/api/rbac/v1/generated.proto"
    "k8s.io/api/storage/v1/generated.proto"
    "k8s.io/api/autoscaling/v1/generated.proto"
    "k8s.io/api/autoscaling/v2/generated.proto"
    "k8s.io/api/discovery/v1/generated.proto"
    "k8s.io/api/admissionregistration/v1/generated.proto"
    "k8s.io/api/coordination/v1/generated.proto"
    "k8s.io/api/scheduling/v1/generated.proto"
    "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/generated.proto"
    "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/generated.proto"
    "k8s.io/apimachinery/pkg/runtime/generated.proto"
    "k8s.io/apimachinery/pkg/api/resource/generated.proto"
    "k8s.io/apimachinery/pkg/runtime/schema/generated.proto"
    "k8s.io/apimachinery/pkg/util/intstr/generated.proto"
)

BASE="https://raw.githubusercontent.com/kubernetes/kubernetes/${TAG}/staging/src"

echo "Syncing upstream Kubernetes .proto files from tag '${TAG}' into ${DEST_ROOT}"

for rel in "${FILES[@]}"; do
    url="${BASE}/${rel}"
    out="${DEST_ROOT}/${rel}"
    mkdir -p "$(dirname "$out")"
    echo "  fetch ${rel}"
    curl --fail --silent --show-error -L "${url}" -o "${out}"
done

echo "Done. ${#FILES[@]} file(s) written under ${DEST_ROOT}."
