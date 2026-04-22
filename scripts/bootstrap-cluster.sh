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

# Detect container runtime (podman or docker)
if command -v podman &> /dev/null; then
    CONTAINER_CMD="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_CMD="docker"
else
    CONTAINER_CMD="docker" # Default to docker for compatibility
fi

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
$CONTAINER_CMD rm -f $($CONTAINER_CMD ps -a --filter "name=coredns" --format "{{.ID}}") 2>/dev/null && echo "  Deleted CoreDNS container" || echo "  No CoreDNS container to delete"
# Remove CoreDNS pod from etcd
$CONTAINER_CMD exec rusternetes-etcd etcdctl del /registry/pods/kube-system/coredns 2>/dev/null && echo "  Deleted CoreDNS pod from etcd" || echo "  No CoreDNS pod in etcd"

# Step 4: Apply bootstrap cluster resources
print_step "Applying bootstrap resources (namespaces, services, CoreDNS)..."
if [ -f "$PROJECT_ROOT/bootstrap-cluster.yaml" ]; then
    $KUBECTL $KUBECTL_FLAGS apply -f "$PROJECT_ROOT/bootstrap-cluster.yaml"
    print_success "Bootstrap resources created"
else
    print_error "bootstrap-cluster.yaml not found"
    exit 1
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
