#!/usr/bin/env bash
# Build the rusternetes service images once on the host, then `docker
# save` them to a tarball that every parallel-conformance agent dind
# loads at start-up. Eliminates the ~5–30 min per-dind cold build.
#
# Output: ${HOME}/.cache/rusternetes-conformance-agents/images.tar
# (override via AGENT_CACHE_DIR=…)
#
# Re-run after touching any crate or Dockerfile.services. Agents only
# pick up changes after the next `conformance-agent-up.sh` (they load
# the tarball at start-up, not on the fly).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

AGENT_CACHE_DIR="${AGENT_CACHE_DIR:-${HOME}/.cache/rusternetes-conformance-agents}"
AGENT_IMAGES_TAR="${AGENT_CACHE_DIR}/images.tar"
mkdir -p "$AGENT_CACHE_DIR"

if ! command -v docker >/dev/null; then
  echo "docker not on PATH. Parallel agents need docker (dind requires a docker host)." >&2
  exit 127
fi

# Compose default project name is the dir basename, so `compose build`
# tags images as `<dir>-<service>`. Force a stable project name so the
# tags don't shift if someone renames the worktree.
export COMPOSE_PROJECT_NAME=rusternetes

echo "==> Building rusternetes service images (compose project: rusternetes)"
docker compose -f compose.yml build --parallel

# Image set matches compose.yml's services with a `build:` block. etcd
# is upstream and pulled fresh inside each dind (cheap), no need to
# tarball it. Same for any future externally-pulled images.
IMAGES=(
  rusternetes-api-server
  rusternetes-scheduler
  rusternetes-controller-manager
  rusternetes-kubelet
  rusternetes-kubelet2
  rusternetes-kube-proxy
)

echo "==> Verifying images exist before save"
for img in "${IMAGES[@]}"; do
  if ! docker image inspect "$img" >/dev/null 2>&1; then
    echo "image $img missing after build — compose tag scheme drifted?" >&2
    docker image ls | grep -E "rusternetes" || true
    exit 1
  fi
done

echo "==> Saving ${#IMAGES[@]} images to $AGENT_IMAGES_TAR"
docker save -o "$AGENT_IMAGES_TAR" "${IMAGES[@]}"

ls -lh "$AGENT_IMAGES_TAR"
echo "==> Done. Each agent's dind will docker load this on start-up."
