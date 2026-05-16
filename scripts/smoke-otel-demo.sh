#!/usr/bin/env bash
#
# Rusternetes smoke test: bring up an all-in-one SQLite cluster, install
# ingress-nginx via Helm, deploy the opentelemetry-demo chart with all
# observability backends, curl the demo frontend through the Ingress, and
# tear everything down.
#
# Exit 0  — frontend reachable through the Ingress and HTML contains the
#           expected marker.
# Exit !=0 — any phase failed (preflight, cluster bring-up, Helm install,
#            curl, marker grep). Teardown still runs.
#
# Caveats:
#  * Rusternetes' kube-proxy runs in the host network namespace and
#    rewrites the host's iptables. While the cluster is up, the host
#    loses local DNS (127.0.0.53:53). Browser / docker pull / apt all
#    break until teardown completes. See memory note
#    `feedback_kube_proxy_dns_research.md`.
#  * The OpenTelemetry demo brings up ~22 pods including OpenSearch,
#    Jaeger, Prometheus and Grafana. Expect ~6 GB RAM and ~20 minutes
#    for everything to reach Ready.
#  * Rusternetes' Ingress resource is API-only — actual HTTP traffic is
#    served by the nginx-ingress controller we install here. If kube-proxy
#    fails to programme the right iptables for the controller's Service
#    Endpoints, the final curl will time out.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ---------------------------------------------------------------- output --
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
NC='\033[0m'

step()    { echo -e "\n${BLUE}==>${NC} $1"; }
ok()      { echo -e "${GREEN}✓${NC} $1"; }
warn()    { echo -e "${YELLOW}!${NC} $1"; }
fail()    { echo -e "${RED}✗${NC} $1" >&2; }
die()     { fail "$1"; exit 1; }

# ---------------------------------------------------------------- config --
INGRESS_NS="ingress-nginx"
INGRESS_RELEASE="ingress-nginx"
DEMO_NS="otel-demo"
DEMO_RELEASE="otel-demo"
DEMO_HOST="otel.local"
NODE_PORT_HTTP="30080"
API_SERVER="https://localhost:6443"
KUBECONFIG_FILE="${HOME}/.kube/rusternetes-config"
KUBELET_VOLUMES_PATH="${KUBELET_VOLUMES_PATH:-${PROJECT_ROOT}/.rusternetes/volumes}"
COMPOSE_FILE="compose.all-in-one.yml"

# --------------------------------------------------------------- preflight
phase_preflight() {
  step "Phase 0 — preflight"

  warn "This script temporarily breaks host DNS (kube-proxy clobbers"
  warn "iptables). Teardown restores it. See plan + memory note."

  for cmd in docker helm kubectl curl jq; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      die "required tool not found: $cmd"
    fi
  done
  ok "tools present: docker helm kubectl curl jq"

  if [ ! -d "$PROJECT_ROOT/../rhino" ]; then
    die "expected rhino checkout at $PROJECT_ROOT/../rhino — clone calfonso/rhino there before running"
  fi
  ok "rhino sibling crate present"

  if ss -ltnH 2>/dev/null | grep -q -E ':6443\s' ; then
    die "port 6443 is already in use — stop the existing process / cluster first"
  fi
  if ss -ltnH 2>/dev/null | grep -q -E ":${NODE_PORT_HTTP}\s" ; then
    die "port ${NODE_PORT_HTTP} is already in use — stop the existing process / cluster first"
  fi
  ok "ports 6443 + ${NODE_PORT_HTTP} are free"

  local free_mb
  free_mb=$(free -m | awk '/^Mem:/ {print $7}')
  if [ "${free_mb:-0}" -lt 8000 ]; then
    warn "only ${free_mb} MB available — the demo wants ~6 GB; expect slow scheduling or OOM"
  else
    ok "memory headroom ok (${free_mb} MB available)"
  fi
}

# ----------------------------------------------------------- cluster up --
phase_cluster_up() {
  step "Phase 1 — bring up rusternetes (all-in-one, sqlite)"

  export KUBELET_VOLUMES_PATH
  mkdir -p "$KUBELET_VOLUMES_PATH"

  docker compose -f "$COMPOSE_FILE" up -d --build

  echo -n "  waiting for api-server /healthz "
  local deadline=$(( $(date +%s) + 120 ))
  while true; do
    local code
    code=$(curl -k -s -o /dev/null -w '%{http_code}' "${API_SERVER}/healthz" || true)
    if [ "$code" = "200" ]; then
      echo
      ok "api-server /healthz reachable"
      break
    fi
    if [ "$(date +%s)" -gt "$deadline" ]; then
      echo
      die "api-server did not become healthy within 120s"
    fi
    echo -n "."
    sleep 2
  done
}

# ------------------------------------------------------------- bootstrap --
phase_bootstrap() {
  step "Phase 2 — bootstrap namespaces + CoreDNS"

  bash "$SCRIPT_DIR/bootstrap-cluster.sh"

  export KUBECONFIG="$KUBECONFIG_FILE"
  [ -f "$KUBECONFIG" ] || die "kubeconfig missing at $KUBECONFIG after bootstrap"

  echo -n "  waiting for CoreDNS Ready "
  local deadline=$(( $(date +%s) + 120 ))
  while true; do
    local phase
    phase=$(kubectl --insecure-skip-tls-verify --server "$API_SERVER" \
      -n kube-system get pod coredns -o jsonpath='{.status.phase}' 2>/dev/null || true)
    if [ "$phase" = "Running" ]; then
      echo
      ok "CoreDNS Running"
      break
    fi
    if [ "$(date +%s)" -gt "$deadline" ]; then
      echo
      die "CoreDNS did not reach Running within 120s"
    fi
    echo -n "."
    sleep 2
  done
}

# ------------------------------------------------------ ingress-nginx ----
phase_ingress_nginx() {
  step "Phase 3 — install ingress-nginx via Helm"

  helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx >/dev/null
  helm repo update >/dev/null

  helm upgrade --install "$INGRESS_RELEASE" ingress-nginx/ingress-nginx \
    --namespace "$INGRESS_NS" --create-namespace \
    --set controller.service.type=NodePort \
    --set controller.service.nodePorts.http="${NODE_PORT_HTTP}" \
    --set controller.kind=Deployment \
    --set controller.replicaCount=1 \
    --set controller.watchIngressWithoutClass=true \
    --set controller.ingressClassResource.default=true \
    --wait --timeout 5m

  ok "ingress-nginx Deployment Ready"
}

# --------------------------------------------------- opentelemetry-demo --
phase_otel_demo() {
  step "Phase 4 — install opentelemetry-demo via Helm (full stack)"

  helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null
  helm repo update >/dev/null

  helm upgrade --install "$DEMO_RELEASE" open-telemetry/opentelemetry-demo \
    --namespace "$DEMO_NS" --create-namespace \
    --set "default.ingress.enabled=true" \
    --set "default.ingress.className=nginx" \
    --set "default.ingress.hosts[0].host=${DEMO_HOST}" \
    --set "default.ingress.hosts[0].paths[0].path=/" \
    --set "default.ingress.hosts[0].paths[0].pathType=Prefix" \
    --wait --timeout 15m

  ok "opentelemetry-demo: all resources Ready"
}

# ---------------------------------------------------------- assert --------
phase_assert_frontend() {
  step "Phase 5 — curl frontend through ingress"

  local body=/tmp/smoke-otel-frontend.html
  local code
  code=$(curl -sS -H "Host: ${DEMO_HOST}" -o "$body" -w '%{http_code}' \
    "http://localhost:${NODE_PORT_HTTP}/" || true)

  if [ "$code" != "200" ]; then
    fail "expected HTTP 200 from http://localhost:${NODE_PORT_HTTP}/ (Host: ${DEMO_HOST}); got: $code"
    echo "  response body (first 40 lines):" >&2
    head -40 "$body" >&2 || true
    return 1
  fi
  ok "HTTP 200 from ingress"

  if ! grep -q "OpenTelemetry" "$body"; then
    fail "frontend HTML missing the 'OpenTelemetry' marker"
    head -40 "$body" >&2 || true
    return 1
  fi
  ok "frontend body contains 'OpenTelemetry' marker"
}

# ---------------------------------------------------------- teardown ------
teardown() {
  local rc=$?
  step "Phase 6 — teardown (rc=$rc)"

  if command -v helm >/dev/null 2>&1; then
    helm uninstall "$DEMO_RELEASE" -n "$DEMO_NS" --wait 2>/dev/null || true
    helm uninstall "$INGRESS_RELEASE" -n "$INGRESS_NS" --wait 2>/dev/null || true
  fi
  docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
  rm -rf "$KUBELET_VOLUMES_PATH" 2>/dev/null || true

  if [ "$rc" -eq 0 ]; then
    ok "smoke test PASSED"
  else
    fail "smoke test FAILED (rc=$rc)"
  fi
  exit "$rc"
}

# ============================================================== main =====
trap teardown EXIT

phase_preflight
phase_cluster_up
phase_bootstrap
phase_ingress_nginx
phase_otel_demo
phase_assert_frontend
# trap fires here with rc=0 → teardown reports PASS
