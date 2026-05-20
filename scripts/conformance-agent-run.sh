#!/usr/bin/env bash
# Run hydrophone with a focus regex against this agent's cluster. The
# hydrophone binary, kubeconfig, and resulting conformance pod all
# live inside the agent's dind — nothing leaks to the host.
#
# Usage:
#   AGENT_ID=1 bash scripts/conformance-agent-run.sh '\[sig-node\] Pods.*should be submitted and removed'
#
# Or pass --focus / --known-green:
#   AGENT_ID=1 bash scripts/conformance-agent-run.sh --known-green
#
# Output: ${AGENT_WORKDIR}/.rusternetes/agents/${AGENT_ID}/results/{e2e.log,junit_01.xml,…}
# Exit code mirrors hydrophone's: 0 on pass, non-zero on any failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=conformance-agent-common.sh
source "${SCRIPT_DIR}/conformance-agent-common.sh"

CONFORMANCE_IMAGE="${CONFORMANCE_IMAGE:-registry.k8s.io/conformance:v1.35.0}"
HYDROPHONE_VERSION="${HYDROPHONE_VERSION:-v0.7.0}"
FOCUS=""
USE_KNOWN_GREEN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --focus) FOCUS="$2"; shift 2 ;;
    --known-green) USE_KNOWN_GREEN=1; shift ;;
    --conformance-image) CONFORMANCE_IMAGE="$2"; shift 2 ;;
    -*) echo "unknown flag: $1" >&2; exit 64 ;;
    *)
      if [[ -z "$FOCUS" ]]; then FOCUS="$1"; shift
      else echo "unexpected arg: $1" >&2; exit 64
      fi ;;
  esac
done

if [[ "$USE_KNOWN_GREEN" -eq 1 && -n "$FOCUS" ]]; then
  echo "--known-green and --focus are mutually exclusive" >&2
  exit 64
fi
if [[ "$USE_KNOWN_GREEN" -eq 0 && -z "$FOCUS" ]]; then
  echo "either --focus REGEX or --known-green required" >&2
  exit 64
fi

if ! docker inspect "$DIND_NAME" >/dev/null 2>&1; then
  echo "dind $DIND_NAME not running — start the agent first:" >&2
  echo "  AGENT_ID=${AGENT_ID} bash scripts/conformance-agent-up.sh" >&2
  exit 1
fi

mkdir -p "$AGENT_RESULTS_DIR"
RUN_LOG="${AGENT_RESULTS_DIR}/run.log"
agent_banner "logging to $RUN_LOG"
exec > >(tee -a "$RUN_LOG") 2>&1

# --- install hydrophone inside the dind --------------------------------------
agent_banner "ensuring hydrophone present"
docker exec "$DIND_NAME" sh -c '
  set -e
  if command -v hydrophone >/dev/null; then exit 0; fi
  apk add --no-cache curl tar >/dev/null
  curl -fsSL -o /tmp/hydrophone.tgz \
    "https://github.com/kubernetes-sigs/hydrophone/releases/download/'"$HYDROPHONE_VERSION"'/hydrophone_Linux_x86_64.tar.gz"
  tar -xzf /tmp/hydrophone.tgz -C /usr/local/bin hydrophone
  chmod +x /usr/local/bin/hydrophone
'

# --- assemble the focus regex ------------------------------------------------
if [[ "$USE_KNOWN_GREEN" -eq 1 ]]; then
  # Reuse the canary harness — it understands known-green.txt + ratchet.
  agent_banner "delegating to conformance-canary-run.sh (known-green ratchet)"
  docker exec -w /workspace \
    -e KUBECONFIG="/workspace/.rusternetes/agents/${AGENT_ID}/kubeconfig-internal" \
    "$DIND_NAME" \
    bash scripts/conformance-canary-run.sh \
      --output-dir "/workspace/.rusternetes/agents/${AGENT_ID}/results" \
      --conformance-image "$CONFORMANCE_IMAGE"
  exit $?
fi

# --- direct hydrophone invocation --------------------------------------------
agent_banner "hydrophone --focus '$FOCUS'"
docker exec -w /workspace \
  -e KUBECONFIG="/workspace/.rusternetes/agents/${AGENT_ID}/kubeconfig-internal" \
  "$DIND_NAME" \
  hydrophone \
    --conformance-image "$CONFORMANCE_IMAGE" \
    --focus "$FOCUS" \
    --output-dir "/workspace/.rusternetes/agents/${AGENT_ID}/results"
