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

# Discover the Docker bridge gateway (always [subnet].1) so we can
# bootstrap CoreDNS and other resources without hardcoding an IP.
# Uses the discover-bridge-gateway helper; callers can override via
# RUSTERNETES_BRIDGE_GATEWAY env var if discovery fails.
if [ -z "${RUSTERNETES_BRIDGE_GATEWAY:-}" ]; then
    if [ -f "$SCRIPT_DIR/discover-bridge-gateway.sh" ]; then
        # shellcheck disable=SC1091
        source "$SCRIPT_DIR/discover-bridge-gateway.sh"
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

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

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
# Remove CoreDNS pod from etcd
$CONTAINER_RT exec rusternetes-etcd etcdctl del /registry/pods/kube-system/coredns 2>/dev/null && echo "  Deleted CoreDNS pod from etcd" || echo "  No CoreDNS pod in etcd"

# Step 4: Apply bootstrap cluster resources
# Inject the discovered bridge gateway into the bootstrap yaml so CoreDNS
# can reach the api-server. Uses a temp file so the tracked yaml stays
# as a template.
print_step "Applying bootstrap resources (namespaces, services, CoreDNS)..."
if [ -f "$PROJECT_ROOT/bootstrap-cluster.yaml" ]; then
    # Fail fast if discovery didn't give us a gateway — CoreDNS won't
    # be able to reach the API server without it after #787.
    if [ -z "${RUSTERNETES_BRIDGE_GATEWAY:-}" ]; then
        print_error "Bridge gateway discovery failed. Set RUSTERNETES_BRIDGE_GATEWAY or fix discover-bridge-gateway.sh"
        exit 1
    fi
    TEMP_BOOTSTRAP="$(mktemp)"
    trap "rm -f '$TEMP_BOOTSTRAP'" EXIT
    sed "s|\\\${DOCKER_GATEWAY}|${RUSTERNETES_BRIDGE_GATEWAY}|g" \
        "$PROJECT_ROOT/bootstrap-cluster.yaml" > "$TEMP_BOOTSTRAP"
    $KUBECTL $KUBECTL_FLAGS apply -f "$TEMP_BOOTSTRAP"
    print_success "Bootstrap resources created (gateway: $RUSTERNETES_BRIDGE_GATEWAY)"
else
    print_error "bootstrap-cluster.yaml not found"
    exit 1
fi

# If the compose files use ${DOCKER_GATEWAY} env var (post-#787), ensure
# the running cluster container sees the discovered value. Write a .env
# file and restart so the compose interpolation takes effect.
if grep -q '\${DOCKER_GATEWAY}' "$PROJECT_ROOT/compose.all-in-one.yml" 2>/dev/null; then
    print_step "Restarting cluster with discovered gateway..."
    echo "DOCKER_GATEWAY=${RUSTERNETES_BRIDGE_GATEWAY}" > "$PROJECT_ROOT/.env"
    echo "KUBELET_VOLUMES_PATH=${KUBELET_VOLUMES_PATH}" >> "$PROJECT_ROOT/.env"
    "$CONTAINER_RT" compose -f "$PROJECT_ROOT/compose.all-in-one.yml" -f "$PROJECT_ROOT/compose.dind.all-in-one.yml" up -d
    print_success "Cluster restarted with gateway $RUSTERNETES_BRIDGE_GATEWAY"
    echo "  .env file written: $PROJECT_ROOT/.env"
fi

# Step 5: Wait for CoreDNS to be ready
print_step "Waiting for CoreDNS to be ready..."
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
