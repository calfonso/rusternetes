#!/usr/bin/env bash
# Sourced helper for parallel conformance agents.
#
# Each agent owns:
#   * a privileged docker:dind sidecar (`rusternetes-agent-N-dind`),
#   * its own dockerd state under /tmp/rusternetes-agent-N/{sock,data},
#   * a host port 1644N → dind 6443 publish so kubectl on the host
#     reaches that agent's api-server,
#   * a workdir (usually a git worktree) whose .rusternetes/certs and
#     .rusternetes/volumes are bind-mounted into the dind.
#
# Two agents pointed at the SAME workdir will trample each other's
# certs and kubelet-volumes. Use one worktree per agent (the /batch
# skill already does this).

set -euo pipefail

if [[ -z "${AGENT_ID:-}" ]]; then
  echo "AGENT_ID env var or arg required (1..9). Example: AGENT_ID=1 $0" >&2
  exit 64
fi
case "$AGENT_ID" in
  [1-9]) ;;
  *) echo "AGENT_ID must be 1..9 (got: $AGENT_ID)" >&2; exit 64 ;;
esac

AGENT_WORKDIR="${AGENT_WORKDIR:-$PWD}"
if [[ ! -f "${AGENT_WORKDIR}/compose.yml" ]]; then
  echo "AGENT_WORKDIR=${AGENT_WORKDIR} has no compose.yml; run from a repo root or worktree." >&2
  exit 65
fi

# Shared image tarball lives outside any worktree so all agents reuse it.
AGENT_CACHE_DIR="${AGENT_CACHE_DIR:-${HOME}/.cache/rusternetes-conformance-agents}"
AGENT_IMAGES_TAR="${AGENT_CACHE_DIR}/images.tar"

# dind keeps its dockerd state (/var/lib/docker) and runtime sockets
# (/var/run/{docker,containerd}.sock) INSIDE the container. We don't
# bind-mount either: the image tarball is reloaded every up so caching
# /var/lib/docker buys nothing, and root-owned sockets in a /tmp bind
# mount can only be cleaned with sudo. All host-side interaction goes
# through `docker exec ${DIND_NAME} ...` instead of a bind-mounted sock.

# Per-agent artefacts inside the workdir (so /batch worktrees pick them up).
AGENT_ARTIFACT_DIR="${AGENT_WORKDIR}/.rusternetes/agents/${AGENT_ID}"
AGENT_KUBECONFIG="${AGENT_ARTIFACT_DIR}/kubeconfig"
AGENT_RESULTS_DIR="${AGENT_ARTIFACT_DIR}/results"
AGENT_LOG="${AGENT_ARTIFACT_DIR}/up.log"

DIND_NAME="rusternetes-agent-${AGENT_ID}-dind"
DIND_IMAGE="docker:28-dind"
API_HOST_PORT=$((16443 + AGENT_ID))

# Run an arbitrary shell command inside the dind container (not inside
# a container the dind has launched — inside the dind itself).
agent_dind_exec() {
  docker exec "${DIND_NAME}" "$@"
}

# Print a banner with the agent's identifiers — useful for /batch
# worker logs.
agent_banner() {
  printf '[agent-%s] %s\n' "${AGENT_ID}" "$*"
}

export AGENT_ID AGENT_WORKDIR AGENT_CACHE_DIR AGENT_IMAGES_TAR
export AGENT_ARTIFACT_DIR AGENT_KUBECONFIG AGENT_RESULTS_DIR AGENT_LOG
export DIND_NAME DIND_IMAGE API_HOST_PORT
