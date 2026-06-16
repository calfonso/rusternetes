#!/bin/bash
#
# Rusternetes Fedora Production Installer
#
# This script automates the complete installation of Rusternetes on Fedora Linux
# in production-ready configuration with rootful Podman.
#
# Usage:
#   sudo ./fedora-install.sh [OPTIONS]
#
# Options:
#   --docker          Use Docker instead of Podman
#   --ha              Install High Availability configuration
#   --skip-firewall   Skip firewall configuration
#   --skip-selinux    Skip SELinux configuration
#   --non-interactive Skip confirmation prompts
#   --help            Show this help message
#
# Requirements:
#   - Fedora 38+ (also works on RHEL/CentOS Stream 9+)
#   - Run as root or with sudo
#   - Internet connection
#

set -e  # Exit on error
set -u  # Exit on undefined variable
set -o pipefail  # Exit on pipe failure

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
INSTALL_DIR="/opt/rusternetes"
USER="rusternetes"
USE_DOCKER=false
INSTALL_HA=false
SKIP_FIREWALL=false
SKIP_SELINUX=false
NON_INTERACTIVE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --docker)
            USE_DOCKER=true
            shift
            ;;
        --ha)
            INSTALL_HA=true
            shift
            ;;
        --skip-firewall)
            SKIP_FIREWALL=true
            shift
            ;;
        --skip-selinux)
            SKIP_SELINUX=true
            shift
            ;;
        --non-interactive)
            NON_INTERACTIVE=true
            shift
            ;;
        --help)
            grep '^#' "$0" | grep -v '#!/bin/bash' | sed 's/^# //'
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Run with --help for usage information"
            exit 1
            ;;
    esac
done

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root or with sudo"
        exit 1
    fi
}

confirm() {
    if [[ "$NON_INTERACTIVE" == "true" ]]; then
        return 0
    fi

    local message="$1"
    read -p "$message (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Installation cancelled by user"
        exit 0
    fi
}

# Check prerequisites
check_prerequisites() {
    log_step "Checking prerequisites..."

    # Check OS
    if [[ ! -f /etc/fedora-release ]] && [[ ! -f /etc/redhat-release ]]; then
        log_error "This script is designed for Fedora/RHEL/CentOS"
        exit 1
    fi

    # Check internet connectivity
    if ! ping -c 1 -W 2 8.8.8.8 &> /dev/null; then
        log_error "No internet connectivity detected"
        exit 1
    fi

    log_info "Prerequisites check passed"
}

# Display configuration
display_config() {
    echo
    echo "=========================================="
    echo "  Rusternetes Installation Configuration"
    echo "=========================================="
    echo "Installation Directory: $INSTALL_DIR"
    echo "Container Runtime: $([ "$USE_DOCKER" == "true" ] && echo "Docker" || echo "Podman (rootful)")"
    echo "Configuration: $([ "$INSTALL_HA" == "true" ] && echo "High Availability" || echo "Single-Node")"
    echo "Firewall Setup: $([ "$SKIP_FIREWALL" == "true" ] && echo "Skip" || echo "Yes")"
    echo "SELinux Setup: $([ "$SKIP_SELINUX" == "true" ] && echo "Skip" || echo "Permissive")"
    echo "=========================================="
    echo

    confirm "Continue with installation?"
}

# Update system
update_system() {
    log_step "Updating system packages..."
    dnf update -y
    log_info "System updated successfully"
}

# Install dependencies
install_dependencies() {
    log_step "Installing dependencies..."

    # Development tools
    dnf groupinstall -y "Development Tools"

    # Required packages
    dnf install -y \
        git \
        curl \
        wget \
        gcc \
        gcc-c++ \
        make \
        openssl-devel \
        pkg-config \
        lsof \
        envsubst

    log_info "Dependencies installed successfully"
}

# Install Rust
install_rust() {
    log_step "Installing Rust..."

    # Install for root
    if ! command -v cargo &> /dev/null; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source /root/.cargo/env
        log_info "Rust installed for root"
    else
        log_info "Rust already installed"
    fi

    # Also install for rusternetes user (if exists)
    if id "$USER" &>/dev/null; then
        if ! sudo -u "$USER" bash -c 'command -v cargo' &> /dev/null; then
            sudo -u "$USER" bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'
            log_info "Rust installed for $USER user"
        fi
    fi
}

# Install container runtime
install_container_runtime() {
    if [[ "$USE_DOCKER" == "true" ]]; then
        install_docker
    else
        install_podman
    fi
}

install_docker() {
    log_step "Installing Docker..."

    if command -v docker &> /dev/null; then
        log_info "Docker already installed"
        return 0
    fi

    # Add Docker repository
    dnf install -y dnf-plugins-core
    dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo

    # Install Docker
    dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # Start and enable
    systemctl start docker
    systemctl enable docker

    # Verify
    docker --version

    log_info "Docker installed successfully"
}

install_podman() {
    log_step "Installing Podman (rootful mode)..."

    if command -v podman &> /dev/null; then
        log_info "Podman already installed"
    else
        dnf install -y podman podman-compose podman-plugins
        log_info "Podman installed successfully"
    fi

    # Verify rootful access
    podman info --format '{{.Host.Security.Rootless}}' | grep -q false || log_warn "Podman may not be in rootful mode"
}

# Configure firewall
configure_firewall() {
    if [[ "$SKIP_FIREWALL" == "true" ]]; then
        log_info "Skipping firewall configuration"
        return 0
    fi

    log_step "Configuring firewall..."

    if ! systemctl is-active --quiet firewalld; then
        log_warn "firewalld is not running, skipping firewall configuration"
        return 0
    fi

    # API server
    firewall-cmd --permanent --add-port=6443/tcp

    # etcd
    firewall-cmd --permanent --add-port=2379-2380/tcp

    # NodePort range (optional)
    firewall-cmd --permanent --add-port=30000-32767/tcp

    # Reload
    firewall-cmd --reload

    log_info "Firewall configured successfully"
}

# Configure SELinux
configure_selinux() {
    if [[ "$SKIP_SELINUX" == "true" ]]; then
        log_info "Skipping SELinux configuration"
        return 0
    fi

    log_step "Configuring SELinux..."

    # Set to permissive for development
    sed -i 's/^SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config
    setenforce 0 || true

    log_info "SELinux set to permissive mode"
}

# Create user
create_user() {
    log_step "Creating rusternetes user..."

    if id "$USER" &>/dev/null; then
        log_info "User $USER already exists"
    else
        useradd -m -s /bin/bash "$USER"
        log_info "User $USER created"
    fi

    # Add to docker group if using Docker
    if [[ "$USE_DOCKER" == "true" ]]; then
        usermod -aG docker "$USER"
        log_info "User added to docker group"
    fi
}

# Clone repository
clone_repository() {
    log_step "Cloning Rusternetes repository..."

    if [[ -d "$INSTALL_DIR" ]]; then
        log_warn "Installation directory already exists, updating..."
        cd "$INSTALL_DIR"
        sudo -u "$USER" git pull || true
    else
        mkdir -p "$(dirname "$INSTALL_DIR")"
        git clone https://github.com/yourusername/rusternetes.git "$INSTALL_DIR"
        chown -R "$USER:$USER" "$INSTALL_DIR"
        log_info "Repository cloned successfully"
    fi
}

# Build binaries
build_binaries() {
    log_step "Building Rusternetes binaries..."

    cd "$INSTALL_DIR"

    # Build as rusternetes user
    sudo -u "$USER" bash -c 'source $HOME/.cargo/env && cargo build --release'

    log_info "Binaries built successfully"
}

# Build container images
build_images() {
    log_step "Building container images..."

    cd "$INSTALL_DIR"

    # Set environment variable
    export KUBELET_VOLUMES_PATH="$INSTALL_DIR/.rusternetes/volumes"
    mkdir -p "$KUBELET_VOLUMES_PATH"
    chown -R "$USER:$USER" "$INSTALL_DIR/.rusternetes"

    # Choose compose file
    local COMPOSE_FILE="docker-compose.yml"
    if [[ "$INSTALL_HA" == "true" ]]; then
        COMPOSE_FILE="docker-compose.ha.yml"
    fi

    # Build
    if [[ "$USE_DOCKER" == "true" ]]; then
        KUBELET_VOLUMES_PATH="$KUBELET_VOLUMES_PATH" docker-compose -f "$COMPOSE_FILE" build
    else
        KUBELET_VOLUMES_PATH="$KUBELET_VOLUMES_PATH" podman-compose -f "$COMPOSE_FILE" build
    fi

    log_info "Container images built successfully"
}

# Create systemd service
create_service() {
    log_step "Creating systemd service..."

    local COMPOSE_FILE="docker-compose.yml"
    if [[ "$INSTALL_HA" == "true" ]]; then
        COMPOSE_FILE="docker-compose.ha.yml"
    fi

    local COMPOSE_CMD="podman-compose"
    if [[ "$USE_DOCKER" == "true" ]]; then
        COMPOSE_CMD="docker-compose"
    fi

    cat > /etc/systemd/system/rusternetes.service <<EOF
[Unit]
Description=Rusternetes Kubernetes Cluster
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$INSTALL_DIR
Environment="KUBELET_VOLUMES_PATH=$INSTALL_DIR/.rusternetes/volumes"
ExecStart=/usr/bin/$COMPOSE_CMD -f $COMPOSE_FILE up -d
ExecStop=/usr/bin/$COMPOSE_CMD -f $COMPOSE_FILE down
User=root

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload

    log_info "Systemd service created"
}

# Start cluster
start_cluster() {
    log_step "Starting Rusternetes cluster..."

    systemctl start rusternetes
    systemctl enable rusternetes

    # Wait for services to be ready
    log_info "Waiting for services to start..."
    sleep 15

    log_info "Cluster started successfully"
}

# Bootstrap cluster
bootstrap_cluster() {
    log_step "Bootstrapping cluster (rusternetes-dns)..."

    cd "$INSTALL_DIR"

    export KUBELET_VOLUMES_PATH="$INSTALL_DIR/.rusternetes/volumes"

    # Apply the core resources (namespaces, kube-dns Service, priority classes).
    envsubst < bootstrap-cluster.yaml > /tmp/bootstrap-expanded.yaml
    KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify apply -f /tmp/bootstrap-expanded.yaml

    # CoreDNS has been removed. The all-in-one rusternetes container serves DNS
    # in-process, so wire the kube-dns Service to it via a hand-written
    # EndpointSlice (kube-proxy then DNATs 10.96.0.10:53 to the container).
    # Mirrors the all-in-one path in scripts/bootstrap-cluster.sh Step 5.
    local aio_dns_ip
    aio_dns_ip=$(docker inspect rusternetes \
        --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null || true)
    if [[ -n "$aio_dns_ip" ]]; then
        cat <<EOF | KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify apply -f -
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: kube-dns-rusternetes
  namespace: kube-system
  labels:
    kubernetes.io/service-name: kube-dns
    endpointslice.kubernetes.io/managed-by: fedora-install.sh
addressType: IPv4
ports:
  - {name: dns, port: 53, protocol: UDP}
  - {name: dns-tcp, port: 53, protocol: TCP}
endpoints:
  - addresses: ["$aio_dns_ip"]
    conditions: {ready: true, serving: true, terminating: false}
EOF
    else
        log_warn "Could not find the rusternetes container to wire kube-dns; DNS may be unavailable"
    fi

    log_info "Bootstrap completed successfully"
}

# Verify installation
verify_installation() {
    log_step "Verifying installation..."

    cd "$INSTALL_DIR"

    # Check if services are running
    if [[ "$USE_DOCKER" == "true" ]]; then
        docker-compose ps
    else
        podman-compose ps
    fi

    # Wait for cluster DNS
    log_info "Waiting for cluster DNS (rusternetes-dns) to be ready..."
    sleep 10

    # Check pods
    log_info "Checking pods..."
    KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get pods -A

    # Check services
    log_info "Checking services..."
    KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get svc -A

    log_info "Verification completed"
}

# Display completion message
display_completion() {
    echo
    echo "=========================================="
    echo "  Rusternetes Installation Complete!"
    echo "=========================================="
    echo
    echo -e "${GREEN}Installation successful!${NC}"
    echo
    echo "Installation details:"
    echo "  Location: $INSTALL_DIR"
    echo "  Service: systemctl status rusternetes"
    echo "  Logs: journalctl -u rusternetes -f"
    echo
    echo "Quick commands:"
    echo "  # Check cluster status"
    echo "  cd $INSTALL_DIR"
    echo "  KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get pods -A"
    echo
    echo "  # Stop cluster"
    echo "  sudo systemctl stop rusternetes"
    echo
    echo "  # Start cluster"
    echo "  sudo systemctl start rusternetes"
    echo
    echo "  # View logs"
    if [[ "$USE_DOCKER" == "true" ]]; then
        echo "  cd $INSTALL_DIR && docker-compose logs -f"
    else
        echo "  cd $INSTALL_DIR && sudo podman-compose logs -f"
    fi
    echo
    echo "Documentation: $INSTALL_DIR/docs/"
    echo "=========================================="
    echo
}

# Main installation flow
main() {
    log_info "Starting Rusternetes installation on Fedora..."

    check_root
    check_prerequisites
    display_config

    update_system
    install_dependencies
    install_rust
    install_container_runtime
    configure_firewall
    configure_selinux
    create_user
    clone_repository
    build_binaries
    build_images
    create_service
    start_cluster
    bootstrap_cluster
    verify_installation

    display_completion
}

# Run main function
main "$@"
