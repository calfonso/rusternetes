#!/bin/bash

# Bootstrap Cluster Script
# This script handles the complete cluster bootstrap process:
# 1. Generate ServiceAccount tokens
# 2. Apply ServiceAccounts and Secrets
# 3. Apply bootstrap resources (namespaces, CoreDNS, etc.)

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${GREEN}==>${NC} $1"
}

# Detect container runtime (docker or podman)
# Usage: bootstrap-cluster.sh [docker|podman]
# Or set CONTAINER_RUNTIME=docker|podman
if [ -n "$1" ] && [[ "$1" == "docker" || "$1" == "podman" ]]; then
    CONTAINER_RT="$1"
elif [ -n "$CONTAINER_RUNTIME" ]; then
    CONTAINER_RT="$CONTAINER_RUNTIME"
else
    HAS_PODMAN=false
    HAS_DOCKER=false
    # Use background + wait to timeout commands that may hang (e.g. docker ps when Docker Desktop is stopped)
    if command -v podman &>/dev/null; then
        podman ps &>/dev/null 2>&1 & PID=$!; ( sleep 3; kill $PID 2>/dev/null ) &>/dev/null & wait $PID 2>/dev/null && HAS_PODMAN=true
    fi
    if command -v docker &>/dev/null; then
        docker ps &>/dev/null 2>&1 & PID=$!; ( sleep 3; kill $PID 2>/dev/null ) &>/dev/null & wait $PID 2>/dev/null && HAS_DOCKER=true
    fi

    if $HAS_PODMAN && $HAS_DOCKER; then
        echo "ERROR: Both docker and podman are available. Please specify which to use:"
        echo "  bash $0 docker"
        echo "  bash $0 podman"
        echo "  CONTAINER_RUNTIME=docker bash $0"
        exit 1
    elif $HAS_PODMAN; then
        CONTAINER_RT=podman
    elif $HAS_DOCKER; then
        CONTAINER_RT=docker
    else
        echo "ERROR: No container runtime (docker or podman) found"
        exit 1
    fi
fi

# SCRIPT_DIR / PROJECT_ROOT are needed for the bridge-gateway discovery
# below (and the later kubectl / yaml-application steps). Define them
# before any block that depends on them.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Discover the Docker bridge gateway (always [subnet].1) so we can
# bootstrap CoreDNS and other resources without hardcoding an IP.
# Uses the discover-bridge-gateway helper; callers can override via
# RUSTERNETES_BRIDGE_GATEWAY env var if discovery fails.
#
# Invoke as a subprocess (not `source`): the helper prints the gateway
# on stdout and ends with `exit 0`, which — if sourced — would terminate
# this bootstrap script before the actual bootstrap work runs. Capturing
# stdout gives us the value without that side-effect.
if [ -z "${RUSTERNETES_BRIDGE_GATEWAY:-}" ]; then
    if [ -f "$SCRIPT_DIR/discover-bridge-gateway.sh" ]; then
        RUSTERNETES_BRIDGE_GATEWAY="$(bash "$SCRIPT_DIR/discover-bridge-gateway.sh" 2>/dev/null || true)"
        export RUSTERNETES_BRIDGE_GATEWAY
    fi
fi

if [ -n "${RUSTERNETES_BRIDGE_GATEWAY:-}" ]; then
    echo "Docker bridge gateway: $RUSTERNETES_BRIDGE_GATEWAY"
fi

echo "Using container runtime: $CONTAINER_RT"

# Podman needs base images pre-pulled (Docker Desktop caches them)
if [ "$CONTAINER_RT" = "podman" ]; then
    for img in busybox:latest; do
        if ! podman image exists "$img" 2>/dev/null; then
            echo "  Pulling required image: $img"
            podman pull "$img" >/dev/null 2>&1 || true
        fi
    done
fi

print_warning() {
    echo -e "${YELLOW}WARNING:${NC} $1"
}

print_error() {
    echo -e "${RED}ERROR:${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Check if kubectl is available. A pre-set $KUBECTL env var wins so callers can
# pin a specific binary (e.g. the system kubectl when the in-tree one lacks a
# feature). Otherwise prefer the freshly-built in-tree kubectl, then $PATH.
if [ -n "${KUBECTL:-}" ]; then
    :
elif [ -f "$PROJECT_ROOT/target/release/kubectl" ]; then
    KUBECTL="$PROJECT_ROOT/target/release/kubectl"
elif command -v kubectl &> /dev/null; then
    KUBECTL="kubectl"
else
    print_error "kubectl not found. Please build it first with: cargo build --release --bin kubectl"
    exit 1
fi

# Determine kubectl flags
KUBECTL_FLAGS="--insecure-skip-tls-verify"
if [ -z "$KUBECONFIG" ] || [ "$KUBECONFIG" = "/dev/null" ]; then
    KUBECTL_FLAGS="$KUBECTL_FLAGS --server https://localhost:6443"
fi

print_step "Bootstrapping Rusternetes cluster..."
echo "Using kubectl: $KUBECTL"
echo "Kubectl flags: $KUBECTL_FLAGS"
echo ""

# Step 0: Template control-plane static pod manifests.
#
# The kube-scheduler static pod (manifests/control-plane/kube-scheduler.yaml)
# hostPath-mounts the certs dir. Because the kubelet runs inside a compose
# container, that hostPath must be the HOST-absolute path of .rusternetes/certs
# (Docker resolves pod hostPaths on the host AND the kubelet stat()s it inside
# its own container — so it has to exist at the same path on both sides; compose
# mounts CERTS_PATH:CERTS_PATH on the node-1 kubelet). We rewrite the committed
# @CERTS_PATH@ placeholder into the templated copy under .rusternetes/manifests,
# which is what the node-1 kubelet's --pod-manifest-path actually mounts.
#
# No RBAC is created for the scheduler: the api-server runs with --skip-auth
# (admin for every request), so the authorizer is bypassed and a
# system:kube-scheduler ClusterRoleBinding would be inert. See
# scripts/generate-certs.sh for the full authn investigation.
CERTS_PATH="$PROJECT_ROOT/.rusternetes/certs"
export CERTS_PATH
if [ -d "$PROJECT_ROOT/manifests/control-plane" ]; then
    print_step "Templating control-plane static pod manifests (CERTS_PATH=$CERTS_PATH)..."
    mkdir -p "$PROJECT_ROOT/.rusternetes/manifests"
    for src in "$PROJECT_ROOT/manifests/control-plane/"*.yaml; do
        [ -e "$src" ] || continue
        dst="$PROJECT_ROOT/.rusternetes/manifests/$(basename "$src")"
        sed "s|@CERTS_PATH@|${CERTS_PATH}|g" "$src" > "$dst"
        echo "  $(basename "$src") -> $dst"
    done
    # Persist CERTS_PATH so a compose restart picks up the same mount path.
    if [ -f "$PROJECT_ROOT/.env" ] && grep -q '^CERTS_PATH=' "$PROJECT_ROOT/.env" 2>/dev/null; then
        sed -i "s|^CERTS_PATH=.*|CERTS_PATH=${CERTS_PATH}|" "$PROJECT_ROOT/.env"
    else
        echo "CERTS_PATH=${CERTS_PATH}" >> "$PROJECT_ROOT/.env"
    fi
    print_success "Static pod manifests templated"
fi

# Step 1: Generate ServiceAccount tokens
print_step "Generating ServiceAccount tokens..."
if [ -f "$SCRIPT_DIR/generate-default-serviceaccounts.sh" ]; then
    bash "$SCRIPT_DIR/generate-default-serviceaccounts.sh"
    print_success "ServiceAccount tokens generated"
else
    print_error "generate-default-serviceaccounts.sh not found"
    exit 1
fi

# Wait a moment for file system sync
sleep 1

# Step 2: Apply ServiceAccounts and Secrets
if [ -f "$PROJECT_ROOT/.rusternetes/default-serviceaccounts.yaml" ]; then
    print_step "Applying ServiceAccounts and Secrets..."
    $KUBECTL $KUBECTL_FLAGS apply -f "$PROJECT_ROOT/.rusternetes/default-serviceaccounts.yaml"
    print_success "ServiceAccounts and Secrets created"
else
    print_warning "ServiceAccount YAML not found at .rusternetes/default-serviceaccounts.yaml"
    print_warning "Continuing with bootstrap, but pods may not have valid tokens"
fi

# Step 3: Delete existing CoreDNS resources to ensure fresh creation with proper service account token
print_step "Cleaning up existing CoreDNS resources (if any)..."
# Remove CoreDNS container
$CONTAINER_RT rm -f $($CONTAINER_RT ps -a --filter "name=coredns" --format "{{.ID}}") 2>/dev/null && echo "  Deleted CoreDNS container" || echo "  No CoreDNS container to delete"
# Remove CoreDNS pod from the api-server. kubectl works across every
# storage backend (etcd / sqlite / redis) — the previous variant did
# `docker exec rusternetes-etcd etcdctl del ...`, which silently no-ops
# on the all-in-one stack (no etcd container) and lets a stale pod with
# a bound nodeName survive into the next apply, where it then 422s with
# "spec.nodeName: Forbidden: field is immutable".
# --ignore-not-found is the upstream-kubectl flag, but the rusternetes
# kubectl built in this workspace rejects it (`unexpected argument`).
# Rely on the `|| echo "No CoreDNS pod..."` fallback to swallow the
# not-found case instead.
$KUBECTL $KUBECTL_FLAGS delete pod coredns -n kube-system --grace-period=0 --force 2>/dev/null && echo "  Deleted CoreDNS pod" || echo "  No CoreDNS pod to delete"

# Step 4: Apply bootstrap cluster resources
# bootstrap-cluster.yaml carries namespaces, the kubernetes + kube-dns
# Services, and PriorityClasses — but NOT the CoreDNS Pod/ConfigMap. Those
# live in bootstrap-coredns.yaml and are applied only on the
# USE_RUSTERNETES_DNS=0 path (Step 5). The default rusternetes-dns path
# never creates a CoreDNS Pod.
#
# The gateway is still injected so the USE_RUSTERNETES_DNS=0 path can reuse
# the discovered value, and to fail fast early if discovery broke (#787).
print_step "Applying bootstrap resources (namespaces, services, priority classes)..."
if [ -f "$PROJECT_ROOT/bootstrap-cluster.yaml" ]; then
    # Fail fast if discovery didn't give us a gateway — CoreDNS won't
    # be able to reach the API server without it after #787.
    if [ -z "${RUSTERNETES_BRIDGE_GATEWAY:-}" ]; then
        print_error "Bridge gateway discovery failed. Set RUSTERNETES_BRIDGE_GATEWAY or fix discover-bridge-gateway.sh"
        exit 1
    fi
    $KUBECTL $KUBECTL_FLAGS apply -f "$PROJECT_ROOT/bootstrap-cluster.yaml"
    print_success "Bootstrap resources created (gateway: $RUSTERNETES_BRIDGE_GATEWAY)"
else
    print_error "bootstrap-cluster.yaml not found"
    exit 1
fi

# If the compose files use ${DOCKER_GATEWAY} env var (post-#787), ensure
# the running cluster container sees the discovered value. Write a .env
# file and restart so the compose interpolation takes effect.
#
# Only applies to the all-in-one stack — the multi-container stack
# (compose.yml) doesn't substitute DOCKER_GATEWAY anywhere and the
# `rusternetes` container doesn't exist there, so attempting the restart
# would either no-op or, worse, race with a fresh `up -d` and bind-clash
# on the 6443 host port. Gate the restart on the all-in-one `rusternetes`
# container actually being up.
if grep -q '\${DOCKER_GATEWAY}' "$PROJECT_ROOT/compose.all-in-one.yml" 2>/dev/null \
    && "$CONTAINER_RT" ps --filter "name=^rusternetes$" --format '{{.Names}}' 2>/dev/null \
        | grep -qx 'rusternetes'; then
    print_step "Restarting cluster with discovered gateway..."
    echo "DOCKER_GATEWAY=${RUSTERNETES_BRIDGE_GATEWAY}" > "$PROJECT_ROOT/.env"
    echo "KUBELET_VOLUMES_PATH=${KUBELET_VOLUMES_PATH}" >> "$PROJECT_ROOT/.env"
    "$CONTAINER_RT" compose -f "$PROJECT_ROOT/compose.all-in-one.yml" -f "$PROJECT_ROOT/compose.dind.all-in-one.yml" up -d
    print_success "Cluster restarted with gateway $RUSTERNETES_BRIDGE_GATEWAY"
    echo "  .env file written: $PROJECT_ROOT/.env"
fi

# Step 5: Wire the kube-dns Service to a DNS backend.
#
# When USE_RUSTERNETES_DNS=1 (default), the backend depends on the stack:
#   - All-in-one stack (`rusternetes` container on the bridge): the DNS
#     server is an in-process task inside that container, so the script
#     creates a hand-written EndpointSlice pointing `kube-dns` at the
#     container's bridge IP (kube-proxy then DNATs 10.96.0.10:53 to it).
#   - Multi-container stacks: the script applies bootstrap-dns.yaml, which
#     runs rusternetes-dns as a kube-system Deployment (k8s-app=kube-dns).
#     The endpoints controller populates the kube-dns Service from the pod
#     via the Service selector — no manual EndpointSlice.
# Either way bootstrap-cluster.yaml only creates the kube-dns Service,
# whose ClusterIP 10.96.0.10 every Pod's /etc/resolv.conf references via
# kubelet's --cluster-dns flag, and which we keep stable.
#
# To run with a CoreDNS Pod instead (e.g. for A/B comparison), set
# USE_RUSTERNETES_DNS=0 in the environment; the script then applies
# bootstrap-coredns.yaml and waits for the CoreDNS Pod to come up.
USE_RUSTERNETES_DNS="${USE_RUSTERNETES_DNS:-1}"

# Some single-node stacks intentionally ship no in-cluster DNS backend (e.g.
# compose.node-conformance.yml — the [NodeConformance] suite has no
# cluster-DNS-resolution specs; those are full-cluster [Conformance]). For
# those, SKIP_DNS_WIRING=1 avoids a pointless 30s wait + an alarming
# "DNS will NOT be functional" warning. The kube-dns Service still exists
# (created above) with no endpoints, which node-scoped tests don't need.
if [ "${SKIP_DNS_WIRING:-0}" = "1" ]; then
    print_step "Skipping DNS backend wiring (SKIP_DNS_WIRING=1)."
    echo "  This stack has no in-cluster DNS backend; node-scoped tests don't need cluster DNS."
elif [ "$USE_RUSTERNETES_DNS" = "1" ]; then
    # No CoreDNS Pod/ConfigMap to tear down on this path — bootstrap-cluster.yaml
    # no longer creates them. The Step 3 cleanup above already removed any stale
    # CoreDNS Pod left over from a previous USE_RUSTERNETES_DNS=0 run. The
    # kube-dns Service stays (created in Step 4) so the ClusterIP is stable.

    # All-in-one stack detection: the `rusternetes` container runs the DNS
    # server as an in-process task binding 0.0.0.0:53 — there is no dns pod
    # image on that stack, so kube-dns is wired manually to the container IP.
    # The compose files pin the network name to `rusternetes-network`.
    DNS_NETWORK="rusternetes-network"
    AIO_DNS_IP=$($CONTAINER_RT inspect rusternetes \
        --format "{{(index .NetworkSettings.Networks \"$DNS_NETWORK\").IPAddress}}" \
        2>/dev/null || true)

    if [ -n "$AIO_DNS_IP" ] && [ "$AIO_DNS_IP" != "<no value>" ]; then
        print_step "Wiring kube-dns Service to the all-in-one rusternetes container..."
        echo "  Found rusternetes at $AIO_DNS_IP"

        # Wire up the EndpointSlice that backs the kube-dns Service.
        # Without this kube-proxy has nothing to DNAT 10.96.0.10:53 to.
        # The slice carries the standard `kubernetes.io/service-name`
        # label so kube-proxy + the EndpointSlice controller treat it
        # as belonging to kube-dns; the non-controller `managed-by` value
        # keeps the endpointslice controller from pruning it. `addressType:
        # IPv4` matches the bridge IPs; dual-stack support is a follow-up.
        cat <<EOF | $KUBECTL $KUBECTL_FLAGS apply -f -
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: kube-dns-rusternetes
  namespace: kube-system
  labels:
    kubernetes.io/service-name: kube-dns
    endpointslice.kubernetes.io/managed-by: bootstrap-cluster.sh
addressType: IPv4
ports:
  - name: dns
    port: 53
    protocol: UDP
  - name: dns-tcp
    port: 53
    protocol: TCP
endpoints:
  - addresses:
      - "$AIO_DNS_IP"
    conditions:
      ready: true
      serving: true
      terminating: false
EOF
        print_success "rusternetes wired up at $AIO_DNS_IP for kube-dns Service"
    else
        # Multi-container stack: rusternetes-dns runs as a kube-system
        # Deployment reading cluster state from the api-server. The
        # endpoints controller wires the kube-dns Service to the pod via
        # the k8s-app=kube-dns selector.
        print_step "Applying rusternetes-dns Deployment (bootstrap-dns.yaml)..."
        if [ ! -f "$PROJECT_ROOT/bootstrap-dns.yaml" ]; then
            print_error "bootstrap-dns.yaml not found"
            exit 1
        fi

        # A previous bootstrap may have wired kube-dns manually to a (now
        # stale) compose-container IP — drop that slice; the endpoints
        # controller owns the Service's endpoints from here on.
        $KUBECTL $KUBECTL_FLAGS delete endpointslice kube-dns-rusternetes -n kube-system 2>/dev/null \
            && echo "  Deleted stale kube-dns-rusternetes EndpointSlice" || true

        $KUBECTL $KUBECTL_FLAGS apply -f "$PROJECT_ROOT/bootstrap-dns.yaml"

        print_step "Waiting for the rusternetes-dns Deployment to be ready..."
        MAX_WAIT=60
        for i in $(seq 1 $MAX_WAIT); do
            DNS_READY=$($KUBECTL $KUBECTL_FLAGS get deployment rusternetes-dns -n kube-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")

            if [ "$DNS_READY" == "1" ]; then
                print_success "rusternetes-dns Deployment is ready!"
                break
            fi

            if [ $i -eq $MAX_WAIT ]; then
                print_warning "rusternetes-dns not ready after ${MAX_WAIT} attempts (readyReplicas: ${DNS_READY:-none})"
                print_warning "Check: $KUBECTL $KUBECTL_FLAGS get pods -n kube-system"
                print_warning "Is the rusternetes-dns:latest image built? Try: docker compose --profile build build dns"
            else
                echo "  Waiting for rusternetes-dns... ($i/$MAX_WAIT)"
                sleep 2
            fi
        done
    fi
else
    # Fallback path (USE_RUSTERNETES_DNS=0): apply the CoreDNS Pod + ConfigMap
    # from bootstrap-coredns.yaml (with the discovered gateway injected so
    # CoreDNS can reach the api-server, #787), then wait for the Pod.
    print_step "Applying CoreDNS Pod + ConfigMap (USE_RUSTERNETES_DNS=0)..."
    if [ -f "$PROJECT_ROOT/bootstrap-coredns.yaml" ]; then
        TEMP_COREDNS="$(mktemp)"
        trap "rm -f '$TEMP_COREDNS'" EXIT
        sed "s|\\\${DOCKER_GATEWAY}|${RUSTERNETES_BRIDGE_GATEWAY}|g" \
            "$PROJECT_ROOT/bootstrap-coredns.yaml" > "$TEMP_COREDNS"
        $KUBECTL $KUBECTL_FLAGS apply -f "$TEMP_COREDNS"
    else
        print_error "bootstrap-coredns.yaml not found (required for USE_RUSTERNETES_DNS=0)"
        exit 1
    fi

    print_step "Waiting for CoreDNS to be ready (USE_RUSTERNETES_DNS=0)..."
    MAX_WAIT=30
    for i in $(seq 1 $MAX_WAIT); do
        COREDNS_STATUS=$($KUBECTL $KUBECTL_FLAGS get pod coredns -n kube-system -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")

        if [ "$COREDNS_STATUS" == "Running" ]; then
            print_success "CoreDNS is running!"
            break
        fi

        if [ $i -eq $MAX_WAIT ]; then
            print_warning "CoreDNS not running after ${MAX_WAIT} seconds (status: $COREDNS_STATUS)"
            print_warning "You may need to check the logs: $KUBECTL $KUBECTL_FLAGS logs -n kube-system coredns"
        else
            echo "  Waiting for CoreDNS... ($i/$MAX_WAIT) Status: $COREDNS_STATUS"
            sleep 2
        fi
    done
fi

# Step 6: Label + taint node-1 as the control-plane node.
#
# node-1 runs the kube-scheduler static pod. Tainting it NoSchedule keeps
# workload pods off node-1 (they land on node-2) so the 2-node capacity isn't
# squeezed by the control-plane pod; the scheduler static pod itself tolerates
# the taint (see manifests/control-plane/kube-scheduler.yaml). Best-effort: a
# fresh node object may not exist yet on the very first bootstrap, so failures
# are non-fatal and the next bootstrap re-applies (--overwrite is idempotent).
print_step "Labeling + tainting node-1 as control-plane..."
$KUBECTL $KUBECTL_FLAGS label node node-1 node-role.kubernetes.io/control-plane= --overwrite 2>/dev/null \
    && echo "  Labeled node-1 control-plane" || print_warning "Could not label node-1 (not registered yet?)"
$KUBECTL $KUBECTL_FLAGS taint node node-1 node-role.kubernetes.io/control-plane=:NoSchedule --overwrite 2>/dev/null \
    && echo "  Tainted node-1 NoSchedule" || print_warning "Could not taint node-1 (not registered yet?)"

echo ""
print_success "Cluster bootstrap complete!"
echo ""
echo "Cluster resources:"
$KUBECTL $KUBECTL_FLAGS get namespaces
echo ""
$KUBECTL $KUBECTL_FLAGS get pods -A
echo ""
$KUBECTL $KUBECTL_FLAGS get services -A
echo ""

print_success "Bootstrap finished successfully!"
