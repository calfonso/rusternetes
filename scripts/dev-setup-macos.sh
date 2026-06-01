#!/bin/bash

# Rusternetes Development Environment Setup Script
# This script helps set up a local development environment using Docker on macOS

set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BOLD}${GREEN}======================================${NC}"
    echo -e "${BOLD}${GREEN}  Rusternetes Development Setup${NC}"
    echo -e "${BOLD}${GREEN}======================================${NC}"
}

print_step() {
    echo -e "\n${BOLD}${GREEN}==>${NC} $1"
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

check_command() {
    if command -v $1 &> /dev/null; then
        print_success "$1 is installed"
        return 0
    else
        print_error "$1 is not installed"
        return 1
    fi
}

print_header

# Check prerequisites
print_step "Checking prerequisites..."

MISSING_DEPS=0

if ! check_command "cargo"; then
    echo "  Install Rust from https://rustup.rs/"
    MISSING_DEPS=1
fi

# protoc is required for host-side `cargo build`/`cargo test` (the
# etcd-client build script compiles .proto files). The containerized
# cluster builds bring their own protoc, so this is a warning, not a
# hard failure — but local builds fail without it.
if ! check_command "protoc"; then
    print_warning "protoc not found — needed for local 'cargo build'/'cargo test'"
    echo "  Install with: brew install protobuf"
fi

# Check for Docker or Podman (prefer Docker on macOS)
CONTAINER_RUNTIME=""
if check_command "docker"; then
    CONTAINER_RUNTIME="docker"
elif check_command "podman"; then
    CONTAINER_RUNTIME="podman"
    print_warning "Podman detected. This script is optimized for Docker on macOS."
else
    print_error "Neither Docker nor Podman is installed"
    echo "  Install Docker Desktop from https://www.docker.com/products/docker-desktop"
    MISSING_DEPS=1
fi

# Check for docker-compose or podman-compose (prefer docker-compose on macOS)
COMPOSE_CMD=""
if [ "$CONTAINER_RUNTIME" = "docker" ]; then
    if check_command "docker-compose"; then
        COMPOSE_CMD="docker-compose"
    else
        print_error "docker-compose is not installed"
        echo "  docker-compose should be included with Docker Desktop"
        MISSING_DEPS=1
    fi
else
    if check_command "podman-compose"; then
        COMPOSE_CMD="podman-compose"
    elif check_command "docker-compose"; then
        COMPOSE_CMD="docker-compose"
        print_warning "Using docker-compose with Podman (experimental)"
    else
        print_error "podman-compose is not installed"
        echo "  Install with: pip3 install podman-compose"
        MISSING_DEPS=1
    fi
fi

if [ $MISSING_DEPS -eq 1 ]; then
    print_error "Missing required dependencies. Please install them and try again."
    exit 1
fi

print_success "All prerequisites are installed!"
echo "  Container runtime: $CONTAINER_RUNTIME"
echo "  Compose command: $COMPOSE_CMD"

# Check for KUBELET_VOLUMES_PATH environment variable
print_step "Checking KUBELET_VOLUMES_PATH environment variable..."
if [ -z "$KUBELET_VOLUMES_PATH" ]; then
    print_warning "KUBELET_VOLUMES_PATH is not set!"
    echo ""
    echo "This environment variable is required for the cluster to function properly."
    echo "It must be set to an absolute path where kubelet will store volume data."
    echo ""
    echo "Recommended path: $(pwd)/.rusternetes/volumes"
    echo ""
    read -p "Would you like to set it now? [Y/n]: " set_env
    set_env=${set_env:-Y}

    if [ "$set_env" = "Y" ] || [ "$set_env" = "y" ]; then
        KUBELET_VOLUMES_PATH="$(pwd)/.rusternetes/volumes"
        export KUBELET_VOLUMES_PATH
        print_success "KUBELET_VOLUMES_PATH set to: $KUBELET_VOLUMES_PATH"
        echo ""
        print_warning "This is only set for this session!"
        echo ""
        echo "To make it permanent, add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
        echo "  export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes"
        echo ""
    else
        print_error "KUBELET_VOLUMES_PATH must be set before starting the cluster!"
        echo ""
        echo "Set it manually with:"
        echo "  export KUBELET_VOLUMES_PATH=\$(pwd)/.rusternetes/volumes"
        echo ""
        echo "Then run this script again or start the cluster with:"
        echo "  make dev-up"
        exit 1
    fi
else
    print_success "KUBELET_VOLUMES_PATH is set to: $KUBELET_VOLUMES_PATH"
fi

# Check for kubectl
KUBECTL_CMD=""
if check_command "kubectl"; then
    KUBECTL_CMD="kubectl"
elif [ -f "./target/release/kubectl" ]; then
    KUBECTL_CMD="./target/release/kubectl"
    print_warning "Using local kubectl binary"
fi

# Ask user what they want to do
echo ""
echo "What would you like to do?"
echo "  1) Build all container images"
echo "  2) Start the development cluster"
echo "  3) Stop the development cluster"
echo "  4) Clean up (remove all containers and volumes)"
echo "  5) View logs"
echo "  6) Build Rust binaries locally"
echo "  7) Run tests"
echo "  8) Full setup (build + start)"
echo "  9) Install MetalLB (local LoadBalancer support)"
echo " 10) Setup DNS proxy (macOS/Podman Machine local development)"
echo " 11) Exit"
echo ""
read -p "Enter your choice [1-11]: " choice

case $choice in
    1)
        print_step "Building all container images..."
        $COMPOSE_CMD build
        print_success "All images built successfully!"
        ;;
    2)
        print_step "Starting development cluster..."

        # Verify KUBELET_VOLUMES_PATH is set
        if [ -z "$KUBELET_VOLUMES_PATH" ]; then
            print_error "KUBELET_VOLUMES_PATH is not set!"
            echo "Please restart this script to set it, or set it manually:"
            echo "  export KUBELET_VOLUMES_PATH=\$(pwd)/.rusternetes/volumes"
            exit 1
        fi

        # Generate TLS certificates if they don't exist
        if [ ! -f ".rusternetes/certs/api-server.crt" ] || [ ! -f ".rusternetes/certs/api-server.key" ]; then
            print_step "Generating TLS certificates for API server..."
            ./scripts/generate-certs.sh
            print_success "TLS certificates generated!"
        fi

        # Create volumes directory if it doesn't exist
        mkdir -p "$KUBELET_VOLUMES_PATH"
        print_success "Volumes directory ready at: $KUBELET_VOLUMES_PATH"

        $COMPOSE_CMD up -d
        print_success "Cluster started!"
        echo ""
        echo "API Server is available at: http://localhost:6443"
        echo "etcd is available at: http://localhost:2379"
        echo ""
        echo "View logs with: $COMPOSE_CMD logs -f"
        echo "Stop cluster with: $COMPOSE_CMD down"
        ;;
    3)
        print_step "Stopping development cluster..."
        $COMPOSE_CMD down
        print_success "Cluster stopped!"
        ;;
    4)
        print_step "Cleaning up all resources..."
        read -p "This will remove all containers, volumes, and images. Continue? [y/N]: " confirm
        if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
            $COMPOSE_CMD down -v
            $CONTAINER_RUNTIME rmi $(podman images "rusternetes*" -q) 2>/dev/null || true
            print_success "Cleanup complete!"
        else
            echo "Cleanup cancelled."
        fi
        ;;
    5)
        print_step "Viewing logs..."
        $COMPOSE_CMD logs -f
        ;;
    6)
        print_step "Building Rust binaries..."
        cargo build --release
        print_success "Binaries built successfully!"
        echo "Binaries are available in: target/release/"
        ;;
    7)
        print_step "Running tests..."
        cargo test
        print_success "Tests completed!"
        ;;
    8)
        print_step "Running full setup..."

        # Verify KUBELET_VOLUMES_PATH is set
        if [ -z "$KUBELET_VOLUMES_PATH" ]; then
            print_error "KUBELET_VOLUMES_PATH is not set!"
            echo "Please restart this script to set it, or set it manually:"
            echo "  export KUBELET_VOLUMES_PATH=\$(pwd)/.rusternetes/volumes"
            exit 1
        fi

        # Generate TLS certificates if they don't exist
        if [ ! -f ".rusternetes/certs/api-server.crt" ] || [ ! -f ".rusternetes/certs/api-server.key" ]; then
            print_step "Generating TLS certificates for API server..."
            ./scripts/generate-certs.sh
            print_success "TLS certificates generated!"
        fi

        # Create volumes directory if it doesn't exist
        mkdir -p "$KUBELET_VOLUMES_PATH"
        print_success "Volumes directory ready at: $KUBELET_VOLUMES_PATH"

        print_step "Building container images..."
        $COMPOSE_CMD build
        print_success "Images built!"

        print_step "Starting development cluster..."
        $COMPOSE_CMD up -d
        print_success "Cluster started!"

        echo ""
        print_step "Waiting for cluster to be ready..."
        sleep 5

        print_step "Bootstrapping cluster (ServiceAccounts, tokens, CoreDNS)..."
        if [ -f "./scripts/bootstrap-cluster.sh" ]; then
            ./scripts/bootstrap-cluster.sh
            print_success "Cluster bootstrapped!"
        else
            print_warning "Bootstrap script not found, skipping bootstrap"
            echo "You can manually bootstrap later with: ./scripts/bootstrap-cluster.sh"
        fi

        echo ""
        print_success "Development environment is ready!"
        echo ""
        echo "API Server: https://localhost:6443"
        echo "etcd: http://localhost:2379"
        echo ""
        echo "Cluster is bootstrapped with:"
        echo "  ✓ Default ServiceAccounts and tokens"
        echo "  ✓ CoreDNS for cluster DNS"
        echo "  ✓ kube-system and default namespaces"
        echo ""
        echo "Next steps:"
        echo "  - Install MetalLB: ./scripts/dev-setup-macos.sh (choose option 9)"
        echo "  - View logs: $COMPOSE_CMD logs -f"
        echo "  - Run kubectl: ./target/release/kubectl get pods -A"
        echo "  - Stop cluster: $COMPOSE_CMD down"
        ;;
    9)
        print_step "Installing MetalLB for LoadBalancer support..."

        if [ -z "$KUBECTL_CMD" ]; then
            print_error "kubectl is not available. Please install kubectl or build it with: cargo build --release --bin kubectl"
            exit 1
        fi

        # Check if cluster is running
        if ! $KUBECTL_CMD cluster-info &> /dev/null; then
            print_error "Kubernetes cluster is not running or not accessible"
            echo "  Start the cluster first with option 2"
            exit 1
        fi

        print_step "Installing MetalLB v0.14.3..."
        $KUBECTL_CMD apply -f https://raw.githubusercontent.com/metallb/metallb/v0.14.3/config/manifests/metallb-native.yaml

        print_step "Waiting for MetalLB to be ready..."
        $KUBECTL_CMD wait --namespace metallb-system \
            --for=condition=ready pod \
            --selector=app=metallb \
            --timeout=90s || {
            print_warning "MetalLB pods may still be initializing. Check with: kubectl get pods -n metallb-system"
        }

        print_step "Configuring MetalLB for Podman network..."

        # Detect Podman network range
        if [ "$CONTAINER_RUNTIME" = "podman" ] && command -v podman &> /dev/null; then
            PODMAN_SUBNET=$(podman network inspect podman 2>/dev/null | grep -oP '\d+\.\d+\.\d+\.\d+/\d+' | head -1 || echo "10.88.0.0/16")
            print_success "Detected Podman network: $PODMAN_SUBNET"
            IP_RANGE="10.88.100.1-10.88.100.50"
        else
            print_warning "Could not detect Podman network, using default range"
            IP_RANGE="192.168.1.240-192.168.1.250"
        fi

        echo "  Using IP range: $IP_RANGE"

        cat <<EOF | $KUBECTL_CMD apply -f -
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: default-pool
  namespace: metallb-system
spec:
  addresses:
  - $IP_RANGE
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: default-l2
  namespace: metallb-system
spec:
  ipAddressPools:
  - default-pool
EOF

        print_success "MetalLB installed and configured!"
        echo ""
        echo "MetalLB is ready! Create a LoadBalancer service to test:"
        echo "  $KUBECTL_CMD apply -f examples/test-loadbalancer-service.yaml"
        echo ""
        echo "Or run the automated test:"
        echo "  ./examples/metallb/test-metallb.sh"
        echo ""
        echo "For more information, see:"
        echo "  - docs/METALLB_INTEGRATION.md"
        echo "  - examples/metallb/QUICKSTART.md"
        ;;
    10)
        print_step "Setting up DNS proxy for local development..."

        if [ ! -f "./scripts/dns-proxy.sh" ]; then
            print_error "DNS proxy script not found at ./scripts/dns-proxy.sh"
            exit 1
        fi

        echo ""
        echo "What would you like to do with the DNS proxy?"
        echo "  1) Start DNS proxy"
        echo "  2) Stop DNS proxy"
        echo "  3) Check DNS proxy status"
        echo "  4) Restart DNS proxy"
        echo ""
        read -p "Enter your choice [1-4]: " dns_choice

        case $dns_choice in
            1)
                ./scripts/dns-proxy.sh start
                ;;
            2)
                ./scripts/dns-proxy.sh stop
                ;;
            3)
                ./scripts/dns-proxy.sh status
                ;;
            4)
                ./scripts/dns-proxy.sh restart
                ;;
            *)
                print_error "Invalid choice"
                exit 1
                ;;
        esac
        ;;
    11)
        echo "Exiting..."
        exit 0
        ;;
    *)
        print_error "Invalid choice"
        exit 1
        ;;
esac

print_success "Done!"
