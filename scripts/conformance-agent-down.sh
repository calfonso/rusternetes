#!/usr/bin/env bash
# Tear down one parallel-conformance agent. Idempotent — running against
# a missing agent prints a note and exits 0 so /batch cleanup hooks can
# call this unconditionally.
#
# Removes:
#   * compose stack inside the dind (compose down -v)
#   * the dind container itself
#   * /tmp/rusternetes-agent-N/{sock,data}/
#   * .rusternetes/agents/N/results/        (kept unless --purge)
#   * .rusternetes/volumes/                 (per workdir; only if --purge)

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
    -e KUBELET_VOLUMES_PATH=/workspace/.rusternetes/volumes \
    "$DIND_NAME" \
    docker compose -f compose.yml -f compose.dind.yml down -v 2>/dev/null || true
  docker rm -f "$DIND_NAME" >/dev/null
  agent_banner "removed dind container"
else
  agent_banner "no dind container to remove"
fi

# /tmp dirs are always safe to wipe — they only hold dockerd state for
# this agent and a unix socket.
rm -rf "$AGENT_RUNTIME_DIR"

if [[ "$PURGE" -eq 1 ]]; then
  agent_banner "purging .rusternetes/agents/${AGENT_ID} and kubelet volumes"
  rm -rf "$AGENT_ARTIFACT_DIR" "${AGENT_WORKDIR}/.rusternetes/volumes"
fi

agent_banner "down"
