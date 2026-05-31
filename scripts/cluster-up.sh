#!/usr/bin/env bash
#
# cluster-up.sh — one omnipotent command to (re)create a Rusternetes cluster
# end-to-end and, optionally, drive a conformance suite against it.
#
# It is intentionally idempotent and safe to run whether or not a cluster is
# already up: it tears the previous one down first (including kubelet-created
# pod containers that block network removal), rebuilds images, starts a fresh
# stack, waits for the API server, generates TLS + ServiceAccount material,
# runs the bootstrap (namespaces, kube-dns wiring, SA tokens), and then either
# runs conformance or prints the exact command to start it yourself.
#
# The same script therefore serves three audiences:
#   * developers who want a clean local cluster in one shot,
#   * humans doing end-to-end manual testing,
#   * CI / automated end-to-end testing (Hydrophone or Sonobuoy).
#
# Usage:
#   scripts/cluster-up.sh [options]
#
# Options:
#   --backend etcd|sqlite|redis   Storage backend / compose stack (default: sqlite)
#   --runtime docker|podman|auto  Container runtime (default: auto-detect)
#   --build | --no-build          (Re)build images before starting (default: build)
#   --conformance MODE            none | hydrophone | sonobuoy (default: none)
#   --conformance-image IMG       (default: registry.k8s.io/conformance:v1.35.0)
#   --focus REGEX                 Ginkgo focus override for hydrophone (default: the
#                                 image's built-in [Conformance] suite)
#   --kubeconfig PATH             (default: $KUBECONFIG or ~/.kube/rusternetes-config)
#   --kubectl PATH                kubectl used for bootstrap (default: system kubectl
#                                 if present, else the in-tree target/release/kubectl)
#   --keep-running                Skip teardown; reuse the stack that is already up
#   --no-bootstrap                Bring the stack up but skip bootstrap-cluster.sh
#   --down-only                   Tear the cluster down and exit (no rebuild/up)
#   -h, --help                    Show this help and exit
#
# Examples:
#   # Clean SQLite cluster, build, bootstrap, then tell me how to run conformance
#   scripts/cluster-up.sh
#
#   # Full automated end-to-end: etcd stack + Hydrophone conformance
#   scripts/cluster-up.sh --backend etcd --conformance hydrophone
#
#   # Fast iteration: reuse running images, just recreate + bootstrap
#   scripts/cluster-up.sh --no-build
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

# ---- pretty output --------------------------------------------------------
BOLD='\033[1m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
step()  { printf "${GREEN}==>${NC} %s\n" "$*"; }
warn()  { printf "${YELLOW}WARNING:${NC} %s\n" "$*"; }
die()   { printf "${RED}ERROR:${NC} %s\n" "$*" >&2; exit 1; }

# ---- defaults -------------------------------------------------------------
BACKEND="sqlite"
RUNTIME="auto"
DO_BUILD=1
CONFORMANCE="none"
CONFORMANCE_IMAGE="registry.k8s.io/conformance:v1.35.0"
FOCUS=""
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
KUBECTL_BIN=""
KEEP_RUNNING=0
DO_BOOTSTRAP=1
DOWN_ONLY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --backend)           BACKEND="${2:?}"; shift 2 ;;
        --runtime)           RUNTIME="${2:?}"; shift 2 ;;
        --build)             DO_BUILD=1; shift ;;
        --no-build)          DO_BUILD=0; shift ;;
        --conformance)       CONFORMANCE="${2:?}"; shift 2 ;;
        --conformance-image) CONFORMANCE_IMAGE="${2:?}"; shift 2 ;;
        --focus)             FOCUS="${2:?}"; shift 2 ;;
        --kubeconfig)        KUBECONFIG_PATH="${2:?}"; shift 2 ;;
        --kubectl)           KUBECTL_BIN="${2:?}"; shift 2 ;;
        --keep-running)      KEEP_RUNNING=1; shift ;;
        --no-bootstrap)      DO_BOOTSTRAP=0; shift ;;
        --down-only)         DOWN_ONLY=1; shift ;;
        -h|--help)           sed -n '2,53p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                   die "unknown option: $1 (try --help)" ;;
    esac
done

case "$BACKEND" in etcd|sqlite|redis) ;; *) die "--backend must be etcd|sqlite|redis" ;; esac
case "$CONFORMANCE" in none|hydrophone|sonobuoy) ;; *) die "--conformance must be none|hydrophone|sonobuoy" ;; esac

# ---- resolve container runtime + compose ----------------------------------
if [[ "$RUNTIME" == "auto" ]]; then
    if command -v podman >/dev/null 2>&1 && [[ -S /run/podman/podman.sock ]]; then
        RUNTIME="podman"
    elif command -v docker >/dev/null 2>&1; then
        RUNTIME="docker"
    else
        die "no container runtime found (need docker or podman)"
    fi
fi

if [[ "$RUNTIME" == "podman" ]]; then
    command -v podman >/dev/null 2>&1 || die "podman requested but not installed"
    COMPOSE=(podman compose)
    RT=(podman)
else
    command -v docker >/dev/null 2>&1 || die "docker requested but not installed"
    COMPOSE=(docker compose)
    RT=(docker)
fi

# ---- compose file selection -----------------------------------------------
# Map backend -> base compose file. On Docker, layer the DinD override that
# swaps every podman-socket mount for the Docker socket (compose.yml hardcodes
# the podman socket path).
case "$BACKEND" in
    etcd)   BASE_COMPOSE="compose.yml" ;;
    sqlite) BASE_COMPOSE="compose.sqlite.yml" ;;
    redis)  BASE_COMPOSE="compose.redis.yml" ;;
esac
[[ -f "$BASE_COMPOSE" ]] || die "compose file not found: $BASE_COMPOSE"

COMPOSE_ARGS=(-f "$BASE_COMPOSE")
if [[ "$RUNTIME" == "docker" && -f "compose.dind.yml" ]]; then
    COMPOSE_ARGS+=(-f "compose.dind.yml")
fi

# kubelet needs an absolute host path for its volume mounts.
export KUBELET_VOLUMES_PATH="${KUBELET_VOLUMES_PATH:-$PROJECT_ROOT/.rusternetes/volumes}"
mkdir -p "$KUBELET_VOLUMES_PATH"

# ---- pick a bootstrap kubectl ---------------------------------------------
# The in-tree kubectl currently drops --insecure-skip-tls-verify / the
# kubeconfig CA on the apply discovery path, so bootstrap's `apply` fails TLS
# against the self-signed API server cert. Prefer the system kubectl for
# bootstrap reliability; fall back to the in-tree binary with a warning.
if [[ -z "$KUBECTL_BIN" ]]; then
    if command -v kubectl >/dev/null 2>&1; then
        KUBECTL_BIN="$(command -v kubectl)"
    elif [[ -x "$PROJECT_ROOT/target/release/kubectl" ]]; then
        KUBECTL_BIN="$PROJECT_ROOT/target/release/kubectl"
        warn "system kubectl not found; using in-tree kubectl for bootstrap (apply TLS bug may bite)"
    else
        die "no kubectl found (install kubectl or 'cargo build --release --bin kubectl')"
    fi
fi

echo ""
printf "${BOLD}Rusternetes cluster-up${NC}\n"
echo   "  backend          : $BACKEND  ($BASE_COMPOSE)"
echo   "  runtime          : $RUNTIME"
echo   "  compose          : ${COMPOSE[*]} ${COMPOSE_ARGS[*]}"
echo   "  volumes          : $KUBELET_VOLUMES_PATH"
echo   "  kubeconfig       : $KUBECONFIG_PATH"
echo   "  bootstrap kubectl: $KUBECTL_BIN"
echo   "  build images     : $([[ $DO_BUILD == 1 ]] && echo yes || echo no)"
echo   "  conformance      : $CONFORMANCE"
echo ""

compose() { "${COMPOSE[@]}" "${COMPOSE_ARGS[@]}" "$@"; }

# ---- teardown (idempotent) ------------------------------------------------
# Remove the compose stack AND any kubelet-created pod containers. Those pod
# containers are not compose-managed, so --remove-orphans won't catch them, and
# while they linger the cluster bridge network can't be removed.
teardown() {
    step "Tearing down any existing stack (down -v --remove-orphans)..."
    compose down -v --remove-orphans 2>/dev/null || true

    # Force-remove leftover pod containers still attached to a project network,
    # then drop the network so 'up' recreates it clean.
    local nets
    nets="$("${RT[@]}" network ls --format '{{.Name}}' 2>/dev/null | grep -iE 'rusternetes' || true)"
    for net in $nets; do
        local cids
        cids="$("${RT[@]}" network inspect "$net" -f '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null || true)"
        if [[ -n "${cids// }" ]]; then
            warn "removing leftover pod containers on $net: $cids"
            # shellcheck disable=SC2086
            "${RT[@]}" rm -f $cids 2>/dev/null || true
        fi
        "${RT[@]}" network rm "$net" 2>/dev/null || true
    done

    # Clear stale per-pod kubelet volume dirs from the previous cluster.
    rm -rf "${KUBELET_VOLUMES_PATH:?}/pods/"* 2>/dev/null || true
}

if [[ "$DOWN_ONLY" == 1 ]]; then
    teardown
    printf "\n${GREEN}✓${NC} ${BOLD}Cluster torn down.${NC}\n"
    exit 0
fi

if [[ "$KEEP_RUNNING" == 0 ]]; then
    teardown
else
    step "Reusing the running stack (--keep-running); skipping teardown"
fi

# ---- TLS + SA material ----------------------------------------------------
step "Ensuring TLS certificates and ServiceAccount signing key..."
bash "$SCRIPT_DIR/generate-certs.sh"

# ---- build ----------------------------------------------------------------
if [[ "$DO_BUILD" == 1 ]]; then
    step "Building container images..."
    compose build
fi

# ---- up -------------------------------------------------------------------
step "Starting the cluster..."
compose up -d

# ---- wait for API server --------------------------------------------------
step "Waiting for the API server to become healthy..."
api_ok=0
for i in $(seq 1 60); do
    if "$KUBECTL_BIN" --kubeconfig "$KUBECONFIG_PATH" --insecure-skip-tls-verify \
            get --raw='/healthz' >/dev/null 2>&1; then
        api_ok=1; break
    fi
    sleep 2
done
[[ "$api_ok" == 1 ]] || {
    warn "API server did not pass /healthz in time; recent api-server logs:"
    compose logs --tail=40 api-server 2>/dev/null || true
    die "aborting — cluster did not come up"
}
step "API server is healthy."

# ---- bootstrap ------------------------------------------------------------
if [[ "$DO_BOOTSTRAP" == 1 ]]; then
    step "Bootstrapping cluster (namespaces, kube-dns, ServiceAccount tokens)..."
    KUBECTL="$KUBECTL_BIN" KUBECONFIG="$KUBECONFIG_PATH" \
        bash "$SCRIPT_DIR/bootstrap-cluster.sh"
else
    step "Skipping bootstrap (--no-bootstrap)"
fi

printf "\n${GREEN}✓${NC} ${BOLD}Cluster is up.${NC}\n"
echo "  export KUBECONFIG=$KUBECONFIG_PATH"
echo "  $KUBECTL_BIN get nodes"
echo ""

# ---- conformance ----------------------------------------------------------
case "$CONFORMANCE" in
    none)
        cat <<EOF
${BOLD}Next:${NC} run a conformance suite against the cluster.

  # Hydrophone (fast, single conformance pod):
  KUBECONFIG=$KUBECONFIG_PATH hydrophone \\
    --conformance-image $CONFORMANCE_IMAGE \\
    --output-dir .rusternetes/volumes/conformance-\$(date +%Y%m%d-%H%M%S)

  # Sonobuoy (full lifecycle):
  bash scripts/run-conformance.sh

Or re-run this script with: --conformance hydrophone | --conformance sonobuoy
EOF
        ;;
    hydrophone)
        command -v hydrophone >/dev/null 2>&1 || die "hydrophone not found on PATH"
        OUT=".rusternetes/volumes/conformance-${BACKEND}-$(date +%Y%m%d-%H%M%S)"
        mkdir -p "$OUT"
        step "Running Hydrophone conformance → $OUT"
        hy_args=(--kubeconfig "$KUBECONFIG_PATH" --output-dir "$OUT"
                 --conformance-image "$CONFORMANCE_IMAGE")
        [[ -n "$FOCUS" ]] && hy_args+=(--focus "$FOCUS")
        set +e
        hydrophone "${hy_args[@]}" 2>&1 | tee "$OUT/run.log"
        rc=${PIPESTATUS[0]}
        set -e
        if [[ -f "$OUT/junit_01.xml" ]]; then
            step "Conformance finished (hydrophone rc=$rc). JUnit: $OUT/junit_01.xml"
        else
            warn "hydrophone rc=$rc and no junit produced; see $OUT/run.log"
        fi
        ;;
    sonobuoy)
        step "Delegating to scripts/run-conformance.sh (Sonobuoy)..."
        KUBECONFIG="$KUBECONFIG_PATH" bash "$SCRIPT_DIR/run-conformance.sh"
        ;;
esac
