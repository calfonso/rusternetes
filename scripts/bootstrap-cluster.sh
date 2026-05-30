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

# Check if kubectl is available
KUBECTL=""
if [ -f "$PROJECT_ROOT/target/release/kubectl" ]; then
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
# When USE_RUSTERNETES_DNS=1 (default), the script:
#   1. Resolves the rusternetes-dns container's IP on the
#      rusternetes-network bridge. (No CoreDNS Pod is ever created on this
#      path — bootstrap-cluster.yaml only creates the kube-dns Service,
#      whose ClusterIP 10.96.0.10 every Pod's /etc/resolv.conf references
#      via kubelet's --cluster-dns flag, and which we keep stable.)
#   2. Creates a hand-written EndpointSlice in kube-system that points
#      `kube-dns` at that container IP, so kube-proxy DNATs cluster DNS
#      queries (10.96.0.10:53) onto the standalone DNS server.
#
# To run with a CoreDNS Pod instead (e.g. for A/B comparison), set
# USE_RUSTERNETES_DNS=0 in the environment; the script then applies
# bootstrap-coredns.yaml and waits for the CoreDNS Pod to come up.
USE_RUSTERNETES_DNS="${USE_RUSTERNETES_DNS:-1}"

if [ "$USE_RUSTERNETES_DNS" = "1" ]; then
    print_step "Wiring kube-dns Service to rusternetes-dns container..."

    # No CoreDNS Pod/ConfigMap to tear down on this path — bootstrap-cluster.yaml
    # no longer creates them. The Step 3 cleanup above already removed any stale
    # CoreDNS Pod left over from a previous USE_RUSTERNETES_DNS=0 run. The
    # kube-dns Service stays (created in Step 4) so the ClusterIP is stable.

    # Discover a container on the rusternetes bridge that serves DNS on
    # :53. Two candidates depending on which stack is up:
    #   - `rusternetes-dns`  — multi-container stack (compose.yml).
    #   - `rusternetes`      — all-in-one stack (compose.all-in-one.yml),
    #                          where the in-process DNS task binds 0.0.0.0:53.
    # The compose files pin the network name to `rusternetes-network`
    # (see the `networks:` block in both files).
    DNS_CANDIDATES="rusternetes-dns rusternetes"
    DNS_NETWORK="rusternetes-network"
    DNS_CONTAINER_NAME=""
    DNS_IP=""
    for i in $(seq 1 30); do
        for candidate in $DNS_CANDIDATES; do
            DNS_IP=$($CONTAINER_RT inspect "$candidate" \
                --format "{{(index .NetworkSettings.Networks \"$DNS_NETWORK\").IPAddress}}" \
                2>/dev/null || true)
            if [ -n "$DNS_IP" ] && [ "$DNS_IP" != "<no value>" ]; then
                DNS_CONTAINER_NAME="$candidate"
                break 2
            fi
        done
        echo "  Waiting for a DNS container ($DNS_CANDIDATES) on $DNS_NETWORK... ($i/30)"
        sleep 1
    done

    if [ -z "$DNS_IP" ] || [ "$DNS_IP" = "<no value>" ]; then
        print_warning "No DNS container found on $DNS_NETWORK (tried: $DNS_CANDIDATES)."
        print_warning "Is the dns service running? Try: $CONTAINER_RT ps --filter name=rusternetes"
        print_warning "Falling back: cluster DNS will NOT be functional until kube-dns has endpoints."
    else
        echo "  Found $DNS_CONTAINER_NAME at $DNS_IP"

        # Wire up the EndpointSlice that backs the kube-dns Service.
        # Without this kube-proxy has nothing to DNAT 10.96.0.10:53 to.
        # The slice carries the standard `kubernetes.io/service-name`
        # label so kube-proxy + the EndpointSlice controller treat it
        # as belonging to kube-dns. `addressType: IPv4` matches the
        # bridge IPs; dual-stack support is a follow-up.
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
      - "$DNS_IP"
    conditions:
      ready: true
      serving: true
      terminating: false
EOF
        print_success "$DNS_CONTAINER_NAME wired up at $DNS_IP for kube-dns Service"
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
