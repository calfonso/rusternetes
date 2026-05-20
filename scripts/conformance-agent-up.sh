#!/usr/bin/env bash
# Spawn one parallel-conformance agent: a privileged docker:dind sidecar
# that runs a full rusternetes cluster + the conformance harness in
# total isolation from every other agent on the host.
#
# Required: AGENT_ID env var or arg in 1..9.
# Optional: AGENT_WORKDIR (defaults to $PWD — must be a repo root or
#           git worktree of rusternetes).
#
# Layout:
#   /tmp/rusternetes-agent-N/sock/docker.sock   ← dind's dockerd sock
#   /tmp/rusternetes-agent-N/data/              ← dind's /var/lib/docker
#   ${AGENT_WORKDIR}/.rusternetes/agents/N/     ← kubeconfig, logs, results
#   127.0.0.1:1644N                             ← agent's api-server
#
# Re-running for an already-up agent is a no-op (idempotent).

set -euo pipefail

if [[ -n "${1:-}" ]]; then
  export AGENT_ID="$1"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=conformance-agent-common.sh
source "${SCRIPT_DIR}/conformance-agent-common.sh"

mkdir -p "$AGENT_ARTIFACT_DIR" "$AGENT_RESULTS_DIR" "$DIND_SOCK_DIR" "$DIND_DATA_DIR"
exec > >(tee -a "$AGENT_LOG") 2>&1

agent_banner "workdir=${AGENT_WORKDIR} host-port=${API_HOST_PORT} dind=${DIND_NAME}"

if [[ ! -f "$AGENT_IMAGES_TAR" ]]; then
  echo "Image tarball missing at $AGENT_IMAGES_TAR." >&2
  echo "Run scripts/conformance-agent-build-cache.sh on the host first." >&2
  exit 66
fi

# --- spawn dind (idempotent) -------------------------------------------------
if docker inspect "$DIND_NAME" >/dev/null 2>&1; then
  agent_banner "reusing existing dind container"
else
  agent_banner "creating dind container"
  docker run -d --rm \
    --name "$DIND_NAME" \
    --privileged \
    -p "127.0.0.1:${API_HOST_PORT}:6443" \
    -v "${DIND_SOCK_DIR}:/var/run" \
    -v "${DIND_DATA_DIR}:/var/lib/docker" \
    -v "${AGENT_WORKDIR}:/workspace" \
    -v "${AGENT_IMAGES_TAR}:/images.tar:ro" \
    "$DIND_IMAGE" \
    dockerd --host=unix:///var/run/docker.sock --group=0 >/dev/null
fi

# --- wait for dockerd inside the dind ----------------------------------------
agent_banner "waiting for dind dockerd"
for i in $(seq 1 60); do
  if docker exec "$DIND_NAME" docker info >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "dind dockerd did not become ready within 60s" >&2
    docker logs --tail=50 "$DIND_NAME" >&2 || true
    exit 1
  fi
done

# --- load image tarball ------------------------------------------------------
# `docker load` is idempotent — repeated loads are no-ops once the layers
# exist. Cheap to re-run if the dind survived a previous agent-up.
agent_banner "loading rusternetes images into dind"
docker exec "$DIND_NAME" docker load -i /images.tar | tail -10

# --- install bash + openssl + kubectl in the dind ----------------------------
# docker:dind is alpine. We need bash for the rusternetes shell scripts,
# openssl for generate-certs.sh, jq for that script's network inspection,
# kubectl for bootstrap-cluster.sh + the health loop.
agent_banner "installing bash/openssl/jq/kubectl in dind"
docker exec "$DIND_NAME" sh -c '
  set -e
  if ! command -v bash >/dev/null; then apk add --no-cache bash openssl jq curl docker-cli-compose; fi
  if ! command -v kubectl >/dev/null; then
    curl -fsSL -o /usr/local/bin/kubectl \
      https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kubectl
    chmod +x /usr/local/bin/kubectl
  fi
'

# --- generate TLS certs inside the workspace ---------------------------------
# generate-certs.sh writes to /workspace/.rusternetes/certs. Because the
# /workspace bind-mount is per-agent (one worktree per agent), certs are
# fully isolated.
agent_banner "generating TLS certs"
docker exec -w /workspace "$DIND_NAME" bash scripts/generate-certs.sh

# --- bring up the cluster ----------------------------------------------------
agent_banner "compose up"
docker exec -w /workspace \
  -e KUBELET_VOLUMES_PATH=/workspace/.rusternetes/volumes \
  "$DIND_NAME" \
  docker compose -f compose.yml -f compose.dind.yml up -d --no-build

# --- wait for api-server healthz ---------------------------------------------
# Inside dind, api-server is reachable on its compose service alias.
agent_banner "waiting for api-server healthz"
for i in $(seq 1 90); do
  out=$(docker exec "$DIND_NAME" \
    curl -ks --max-time 3 https://localhost:6443/healthz 2>&1 || true)
  if [[ "$out" == "ok" ]]; then
    agent_banner "api-server healthy after ${i} attempts"
    break
  fi
  if [[ $i -eq 90 ]]; then
    echo "api-server never returned 'ok' on /healthz" >&2
    docker exec -w /workspace "$DIND_NAME" \
      docker compose -f compose.yml -f compose.dind.yml ps >&2 || true
    docker exec -w /workspace "$DIND_NAME" \
      docker compose -f compose.yml -f compose.dind.yml logs --tail=80 api-server >&2 || true
    exit 1
  fi
  sleep 2
done

# --- write the per-agent kubeconfig ------------------------------------------
agent_banner "writing kubeconfig → ${AGENT_KUBECONFIG}"
cat > "$AGENT_KUBECONFIG" <<EOF
apiVersion: v1
kind: Config
clusters:
  - name: rusternetes-agent-${AGENT_ID}
    cluster:
      insecure-skip-tls-verify: true
      server: https://127.0.0.1:${API_HOST_PORT}
contexts:
  - name: rusternetes-agent-${AGENT_ID}
    context:
      cluster: rusternetes-agent-${AGENT_ID}
      user: admin
current-context: rusternetes-agent-${AGENT_ID}
users:
  - name: admin
    user:
      token: anonymous
EOF

# --- bootstrap CoreDNS, services, SA tokens ----------------------------------
agent_banner "bootstrap-cluster.sh"
docker exec -w /workspace \
  -e KUBECONFIG=/workspace/.rusternetes/agents/${AGENT_ID}/kubeconfig-internal \
  "$DIND_NAME" bash -c "
    cat > /workspace/.rusternetes/agents/${AGENT_ID}/kubeconfig-internal <<INNER
apiVersion: v1
kind: Config
clusters:
  - name: rusternetes
    cluster:
      insecure-skip-tls-verify: true
      server: https://localhost:6443
contexts:
  - name: rusternetes
    context:
      cluster: rusternetes
      user: admin
current-context: rusternetes
users:
  - name: admin
    user:
      token: anonymous
INNER
    bash scripts/bootstrap-cluster.sh docker
  "

agent_banner "up. kubectl --kubeconfig=${AGENT_KUBECONFIG} get nodes"
kubectl --kubeconfig="$AGENT_KUBECONFIG" get nodes || true
