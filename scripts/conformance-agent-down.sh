#!/usr/bin/env bash
# Tear down one parallel-conformance agent. Idempotent — running against
# a missing agent prints a note and exits 0 so /batch cleanup hooks can
# call this unconditionally.
#
# Removes:
#   * compose stack inside the dind (compose down -v)
#   * the dind container itself
#   * .rusternetes/agents/N/                (kept unless --purge)

set -euo pipefail

PURGE=0
ARGS=()
for a in "$@"; do
  case "$a" in
    --purge) PURGE=1 ;;
    *) ARGS+=("$a") ;;
  esac
done
set -- "${ARGS[@]}"

if [[ -n "${1:-}" ]]; then
  export AGENT_ID="$1"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=conformance-agent-common.sh
source "${SCRIPT_DIR}/conformance-agent-common.sh"

agent_banner "tearing down (purge=${PURGE})"

if docker inspect "$DIND_NAME" >/dev/null 2>&1; then
  # Best-effort compose down — the dind may be unhealthy already.
  docker exec -w /workspace \
    -e COMPOSE_PROJECT_NAME=rusternetes \
    -e KUBELET_VOLUMES_PATH=/workspace/.rusternetes/agents/${AGENT_ID}/kubelet-volumes \
    "$DIND_NAME" \
    docker compose -f compose.yml -f compose.dind.yml down -v 2>/dev/null || true
  docker rm -f "$DIND_NAME" >/dev/null
  agent_banner "removed dind container"
else
  agent_banner "no dind container to remove"
fi

if [[ "$PURGE" -eq 1 ]]; then
  agent_banner "purging ${AGENT_ARTIFACT_DIR}"
  # kubelet (inside the dind, running as root) wrote SA tokens and
  # configmap projections into kubelet-volumes/, owned by root on the
  # host. The current shell can't unlink them without sudo — pay for a
  # throwaway alpine container that does have root to wipe the tree.
  if [[ -d "$AGENT_ARTIFACT_DIR" ]]; then
    docker run --rm -v "${AGENT_WORKDIR}/.rusternetes/agents:/data" alpine \
      rm -rf "/data/${AGENT_ID}" >/dev/null
  fi
fi

agent_banner "down"
