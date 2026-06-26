#!/bin/bash
#
# Rusternetes Development Setup for Fedora Linux
#
# This script sets up a local development environment for Rusternetes on Fedora/RHEL/CentOS.
# Unlike the production installer, this script:
# - Uses --skip-auth for easier development
# - Sets up convenient aliases and environment variables
# - Configures Podman in rootful mode for development
# - Creates development-friendly systemd services (optional)
# - Enables hot-reload workflows
#
# Usage:
#   sudo ./dev-setup-fedora.sh [OPTIONS]
#
# Options:
#   --podman          Use Podman (default)
#   --docker          Use Docker instead of Podman
#   --no-systemd      Don't create systemd service
#   --skip-build      Skip building binaries and images
#   --ha              Use HA compose file (docker-compose.ha.yml)
#   --help            Show this help message
#
# Prerequisites:
#   - Fedora 38+ (or RHEL/CentOS Stream 9+)
#   - Internet connection
#   - sudo access
#

set -e  # Exit on error
set -u  # Exit on undefined variable
set -o pipefail  # Exit on pipe failure

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
USE_DOCKER=false
CREATE_SYSTEMD=true
SKIP_BUILD=false
USE_HA=false
CURRENT_USER=${SUDO_USER:-$USER}
PROJECT_DIR="/home/$CURRENT_USER/rusternetes"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --docker)
            USE_DOCKER=true
            shift
            ;;
        --podman)
            USE_DOCKER=false
            shift
            ;;
        --no-systemd)
            CREATE_SYSTEMD=false
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --ha)
            USE_HA=true
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

log_dev() {
    echo -e "${CYAN}[DEV]${NC} $1"
}

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run with sudo"
        echo "Usage: sudo $0"
        exit 1
    fi
}

# Display banner
display_banner() {
    echo
    echo "=============================================="
    echo "  Rusternetes Development Setup - Fedora"
    echo "=============================================="
    echo "User: $CURRENT_USER"
    echo "Project: $PROJECT_DIR"
    echo "Container Runtime: $([ "$USE_DOCKER" == "true" ] && echo "Docker" || echo "Podman (rootful)")"
    echo "Configuration: $([ "$USE_HA" == "true" ] && echo "HA (docker-compose.ha.yml)" || echo "Single-Node (docker-compose.yml)")"
    echo "Systemd Service: $([ "$CREATE_SYSTEMD" == "true" ] && echo "Yes" || echo "No")"
    echo "=============================================="
    echo
}

# Check prerequisites
check_prerequisites() {
    log_step "Checking prerequisites..."

    # Check Fedora/RHEL
    if [[ ! -f /etc/fedora-release ]] && [[ ! -f /etc/redhat-release ]]; then
        log_error "This script is designed for Fedora/RHEL/CentOS"
        exit 1
    fi

    # Check internet
    if ! ping -c 1 -W 2 8.8.8.8 &> /dev/null; then
        log_warn "No internet connectivity detected (required for package installation)"
    fi

    log_info "Prerequisites check passed"
}

# Install system packages
install_system_packages() {
    log_step "Installing system packages..."

    # Update system
    dnf update -y

    # Development tools
    dnf groupinstall -y "Development Tools" 2>/dev/null || dnf group install -y "Development Tools"

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
        protobuf-compiler \
        protobuf-devel \
        lsof \
        gettext

    log_info "System packages installed"
}

# Install Rust
install_rust() {
    log_step "Installing Rust..."

    # Install for current user (not root)
    if ! su - "$CURRENT_USER" -c 'command -v cargo' &> /dev/null; then
        log_info "Installing Rust for $CURRENT_USER..."
        su - "$CURRENT_USER" -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'

        # Add to profile if not already there
        if ! grep -q 'source $HOME/.cargo/env' "/home/$CURRENT_USER/.bashrc"; then
            echo 'source $HOME/.cargo/env' >> "/home/$CURRENT_USER/.bashrc"
        fi

        log_info "Rust installed for $CURRENT_USER"
    else
        log_info "Rust already installed for $CURRENT_USER"
    fi

    # Also install for root (for sudo operations)
    if ! command -v cargo &> /dev/null; then
        log_info "Installing Rust for root..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source /root/.cargo/env
        log_info "Rust installed for root"
    else
        log_info "Rust already installed for root"
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
        systemctl is-active --quiet docker || systemctl start docker
        systemctl is-enabled --quiet docker || systemctl enable docker
        return 0
    fi

    # Add Docker repository
    dnf install -y dnf-plugins-core
    dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo || \
        dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo

    # Install Docker
    dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # Start and enable
    systemctl start docker
    systemctl enable docker

    # Add user to docker group
    usermod -aG docker "$CURRENT_USER"

    log_info "Docker installed successfully"
    log_dev "User $CURRENT_USER added to docker group (may need to log out/in)"
}

install_podman() {
    log_step "Installing Podman (rootful mode)..."

    if command -v podman &> /dev/null; then
        log_info "Podman already installed"
    else
        dnf install -y podman podman-compose podman-plugins
        log_info "Podman installed successfully"
    fi

    # Verify rootful
    if podman info --format '{{.Host.Security.Rootless}}' 2>/dev/null | grep -q true; then
        log_warn "Podman is in rootless mode - kube-proxy needs rootful mode"
        log_dev "Will run with sudo for rootful access"
    fi

    log_dev "Podman configured for development use"
}

# Configure SELinux for development
configure_selinux() {
    log_step "Configuring SELinux for development..."

    # Set to permissive for easier development
    if command -v getenforce &> /dev/null; then
        local CURRENT_MODE=$(getenforce)
        if [[ "$CURRENT_MODE" == "Enforcing" ]]; then
            log_dev "Setting SELinux to permissive mode for development"
            sed -i 's/^SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config
            setenforce 0 || true
            log_info "SELinux set to permissive mode"
        else
            log_info "SELinux already in permissive/disabled mode"
        fi
    fi
}

# Configure firewall
configure_firewall() {
    log_step "Configuring firewall..."

    if systemctl is-active --quiet firewalld; then
        log_dev "Opening development ports in firewall"

        # API server
        firewall-cmd --permanent --add-port=6443/tcp
        # etcd
        firewall-cmd --permanent --add-port=2379-2380/tcp
        # HA API servers (if HA)
        if [[ "$USE_HA" == "true" ]]; then
            firewall-cmd --permanent --add-port=6444-6446/tcp
        fi
        # HAProxy stats (if HA)
        if [[ "$USE_HA" == "true" ]]; then
            firewall-cmd --permanent --add-port=8404/tcp
        fi
        # NodePort range
        firewall-cmd --permanent --add-port=30000-32767/tcp

        firewall-cmd --reload

        log_info "Firewall configured for development"
    else
        log_info "Firewall not active, skipping configuration"
    fi
}

# Clone or update repository
setup_repository() {
    log_step "Setting up Rusternetes repository..."

    if [[ -d "$PROJECT_DIR" ]]; then
        log_info "Repository already exists at $PROJECT_DIR"
        cd "$PROJECT_DIR"
        su - "$CURRENT_USER" -c "cd '$PROJECT_DIR' && git pull" || true
    else
        log_warn "Project directory not found at $PROJECT_DIR"
        echo "Please clone the repository manually:"
        echo "  cd /home/$CURRENT_USER"
        echo "  git clone https://github.com/yourusername/rusternetes.git"
        echo "  cd rusternetes"
        return 1
    fi

    # Ensure ownership
    chown -R "$CURRENT_USER:$CURRENT_USER" "$PROJECT_DIR"

    log_info "Repository ready"
}

# Build binaries
build_binaries() {
    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "Skipping binary build (--skip-build specified)"
        return 0
    fi

    log_step "Building Rust binaries..."

    cd "$PROJECT_DIR"

    # Build as user
    su - "$CURRENT_USER" -c "cd '$PROJECT_DIR' && source \$HOME/.cargo/env && cargo build --release"

    log_info "Binaries built successfully"
    log_dev "Binaries available at: $PROJECT_DIR/target/release/"
}

# Build container images
build_images() {
    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "Skipping image build (--skip-build specified)"
        return 0
    fi

    log_step "Building container images..."

    cd "$PROJECT_DIR"

    # Set volumes path
    export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
    mkdir -p "$KUBELET_VOLUMES_PATH"
    chown -R "$CURRENT_USER:$CURRENT_USER" "$PROJECT_DIR/.rusternetes"

    # Choose compose file
    local COMPOSE_FILE="docker-compose.yml"
    if [[ "$USE_HA" == "true" ]]; then
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

# Create development helpers
create_dev_helpers() {
    log_step "Creating development helper scripts..."

    local HELPER_DIR="$PROJECT_DIR/.dev"
    mkdir -p "$HELPER_DIR"

    # Determine compose command
    local COMPOSE_CMD="podman-compose"
    local COMPOSE_SUDO="sudo "
    if [[ "$USE_DOCKER" == "true" ]]; then
        COMPOSE_CMD="docker-compose"
        COMPOSE_SUDO=""
    fi

    local COMPOSE_FILE="docker-compose.yml"
    if [[ "$USE_HA" == "true" ]]; then
        COMPOSE_FILE="docker-compose.ha.yml"
    fi

    # Start script
    cat > "$HELPER_DIR/start.sh" <<EOF
#!/bin/bash
# Start Rusternetes development cluster
cd "$PROJECT_DIR"
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
${COMPOSE_SUDO}KUBELET_VOLUMES_PATH=\$KUBELET_VOLUMES_PATH $COMPOSE_CMD -f $COMPOSE_FILE up -d
echo "Cluster started. Use './dev/logs.sh' to view logs"
EOF

    # Stop script
    cat > "$HELPER_DIR/stop.sh" <<EOF
#!/bin/bash
# Stop Rusternetes development cluster
cd "$PROJECT_DIR"
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
${COMPOSE_SUDO}KUBELET_VOLUMES_PATH=\$KUBELET_VOLUMES_PATH $COMPOSE_CMD -f $COMPOSE_FILE down
echo "Cluster stopped"
EOF

    # Restart script
    cat > "$HELPER_DIR/restart.sh" <<EOF
#!/bin/bash
# Restart Rusternetes development cluster
cd "$PROJECT_DIR"
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
${COMPOSE_SUDO}KUBELET_VOLUMES_PATH=\$KUBELET_VOLUMES_PATH $COMPOSE_CMD -f $COMPOSE_FILE restart
echo "Cluster restarted"
EOF

    # Logs script
    cat > "$HELPER_DIR/logs.sh" <<EOF
#!/bin/bash
# View cluster logs
cd "$PROJECT_DIR"
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
${COMPOSE_SUDO}KUBELET_VOLUMES_PATH=\$KUBELET_VOLUMES_PATH $COMPOSE_CMD -f $COMPOSE_FILE logs -f "\$@"
EOF

    # Status script
    cat > "$HELPER_DIR/status.sh" <<EOF
#!/bin/bash
# Check cluster status
cd "$PROJECT_DIR"
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
echo "=== Container Status ==="
${COMPOSE_SUDO}KUBELET_VOLUMES_PATH=\$KUBELET_VOLUMES_PATH $COMPOSE_CMD -f $COMPOSE_FILE ps
echo
echo "=== Cluster Resources ==="
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get nodes,pods,svc -A
EOF

    # Clean script
    cat > "$HELPER_DIR/clean.sh" <<EOF
#!/bin/bash
# Clean and restart cluster (fresh start)
cd "$PROJECT_DIR"
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
echo "Stopping cluster..."
${COMPOSE_SUDO}KUBELET_VOLUMES_PATH=\$KUBELET_VOLUMES_PATH $COMPOSE_CMD -f $COMPOSE_FILE down -v
echo "Cleaning volumes..."
${COMPOSE_SUDO}rm -rf "$PROJECT_DIR/.rusternetes/volumes"/*
echo "Starting fresh cluster..."
${COMPOSE_SUDO}KUBELET_VOLUMES_PATH=\$KUBELET_VOLUMES_PATH $COMPOSE_CMD -f $COMPOSE_FILE up -d
echo "Cluster cleaned and restarted"
EOF

    # Bootstrap script
    cat > "$HELPER_DIR/bootstrap.sh" <<EOF
#!/bin/bash
# Bootstrap cluster with ServiceAccounts, tokens, and CoreDNS
cd "$PROJECT_DIR"
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
./scripts/bootstrap-cluster.sh
EOF

    # Rebuild script
    cat > "$HELPER_DIR/rebuild.sh" <<EOF
#!/bin/bash
# Rebuild binaries and images
cd "$PROJECT_DIR"
echo "Building Rust binaries..."
cargo build --release
echo "Building container images..."
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"
${COMPOSE_SUDO}KUBELET_VOLUMES_PATH=\$KUBELET_VOLUMES_PATH $COMPOSE_CMD -f $COMPOSE_FILE build
echo "Rebuild complete. Use './dev/restart.sh' to apply changes"
EOF

    # Make executable
    chmod +x "$HELPER_DIR"/*.sh
    chown -R "$CURRENT_USER:$CURRENT_USER" "$HELPER_DIR"

    log_info "Development helper scripts created at $PROJECT_DIR/.dev/"
    log_dev "Quick commands:"
    log_dev "  ./.dev/start.sh      - Start cluster"
    log_dev "  ./.dev/stop.sh       - Stop cluster"
    log_dev "  ./.dev/restart.sh    - Restart cluster"
    log_dev "  ./.dev/logs.sh       - View logs (optionally specify service)"
    log_dev "  ./.dev/status.sh     - Check cluster status"
    log_dev "  ./.dev/clean.sh      - Clean restart"
    log_dev "  ./.dev/bootstrap.sh  - Bootstrap cluster"
    log_dev "  ./.dev/rebuild.sh    - Rebuild binaries and images"
}

# Create environment file
create_env_file() {
    log_step "Creating environment configuration..."

    cat > "$PROJECT_DIR/.env.dev" <<EOF
# Rusternetes Development Environment
# Source this file: source .env.dev

# Container runtime
export CONTAINER_RUNTIME=$([ "$USE_DOCKER" == "true" ] && echo "docker" || echo "podman")

# Volumes path
export KUBELET_VOLUMES_PATH="$PROJECT_DIR/.rusternetes/volumes"

# Compose file
export COMPOSE_FILE="$([ "$USE_HA" == "true" ] && echo "docker-compose.ha.yml" || echo "docker-compose.yml")"

# Kubectl alias (skip TLS and kubeconfig)
alias k='KUBECONFIG=/dev/null $PROJECT_DIR/target/release/kubectl --insecure-skip-tls-verify'

# Convenience functions
cluster-start() {
    cd "$PROJECT_DIR" && ./.dev/start.sh
}

cluster-stop() {
    cd "$PROJECT_DIR" && ./.dev/stop.sh
}

cluster-logs() {
    cd "$PROJECT_DIR" && ./.dev/logs.sh "\$@"
}

cluster-status() {
    cd "$PROJECT_DIR" && ./.dev/status.sh
}

cluster-clean() {
    cd "$PROJECT_DIR" && ./.dev/clean.sh
}

cluster-bootstrap() {
    cd "$PROJECT_DIR" && ./.dev/bootstrap.sh
}

cluster-rebuild() {
    cd "$PROJECT_DIR" && ./.dev/rebuild.sh
}

echo "Rusternetes development environment loaded"
echo "Commands: cluster-start, cluster-stop, cluster-logs, cluster-status, cluster-clean, cluster-bootstrap, cluster-rebuild"
echo "Kubectl alias: k (e.g., 'k get pods -A')"
EOF

    chown "$CURRENT_USER:$CURRENT_USER" "$PROJECT_DIR/.env.dev"

    # Add to .bashrc if not already there
    if ! grep -q 'source.*\.env\.dev' "/home/$CURRENT_USER/.bashrc"; then
        echo "" >> "/home/$CURRENT_USER/.bashrc"
        echo "# Rusternetes development environment" >> "/home/$CURRENT_USER/.bashrc"
        echo "if [ -f $PROJECT_DIR/.env.dev ]; then" >> "/home/$CURRENT_USER/.bashrc"
        echo "    source $PROJECT_DIR/.env.dev" >> "/home/$CURRENT_USER/.bashrc"
        echo "fi" >> "/home/$CURRENT_USER/.bashrc"
        log_dev "Added .env.dev to .bashrc"
    fi

    log_info "Development environment file created: $PROJECT_DIR/.env.dev"
}

# Create systemd service
create_systemd_service() {
    if [[ "$CREATE_SYSTEMD" != "true" ]]; then
        log_info "Skipping systemd service creation (--no-systemd specified)"
        return 0
    fi

    log_step "Creating systemd service for development..."

    local COMPOSE_FILE="docker-compose.yml"
    if [[ "$USE_HA" == "true" ]]; then
        COMPOSE_FILE="docker-compose.ha.yml"
    fi

    local COMPOSE_CMD="podman-compose"
    if [[ "$USE_DOCKER" == "true" ]]; then
        COMPOSE_CMD="docker-compose"
    fi

    cat > /etc/systemd/system/rusternetes-dev.service <<EOF
[Unit]
Description=Rusternetes Development Cluster
After=network-online.target
Wants=network-online.target
$([ "$USE_DOCKER" == "true" ] && echo "After=docker.service" || echo "")
$([ "$USE_DOCKER" == "true" ] && echo "Requires=docker.service" || echo "")

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$PROJECT_DIR
Environment="KUBELET_VOLUMES_PATH=$PROJECT_DIR/.rusternetes/volumes"
ExecStart=/usr/bin/$COMPOSE_CMD -f $COMPOSE_FILE up -d
ExecStop=/usr/bin/$COMPOSE_CMD -f $COMPOSE_FILE down
User=root

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload

    log_info "Systemd service created: rusternetes-dev.service"
    log_dev "Enable auto-start: sudo systemctl enable rusternetes-dev"
    log_dev "Start now: sudo systemctl start rusternetes-dev"
    log_dev "Check status: sudo systemctl status rusternetes-dev"
}

# Display completion message
display_completion() {
    echo
    echo "=============================================="
    echo "  Development Setup Complete!"
    echo "=============================================="
    echo
    echo -e "${GREEN}Rusternetes development environment is ready!${NC}"
    echo
    echo "Project location: $PROJECT_DIR"
    echo
    echo -e "${CYAN}Quick Start:${NC}"
    echo "  1. Log out and back in (or run: su - $CURRENT_USER)"
    echo "  2. cd $PROJECT_DIR"
    echo "  3. source .env.dev  # (or automatic from .bashrc)"
    echo "  4. cluster-start    # Start the cluster"
    echo "  5. cluster-bootstrap # Bootstrap with CoreDNS"
    echo "  6. k get pods -A    # Check cluster status"
    echo
    echo -e "${CYAN}Development Commands:${NC}"
    echo "  cluster-start      - Start the cluster"
    echo "  cluster-stop       - Stop the cluster"
    echo "  cluster-restart    - Restart the cluster"
    echo "  cluster-logs       - View logs (all services)"
    echo "  cluster-logs api-server - View specific service logs"
    echo "  cluster-status     - Check cluster status"
    echo "  cluster-clean      - Clean restart (wipe data)"
    echo "  cluster-bootstrap  - Apply bootstrap resources"
    echo "  cluster-rebuild    - Rebuild binaries and images"
    echo "  k <command>        - kubectl alias (e.g., 'k get nodes')"
    echo
    echo -e "${CYAN}Helper Scripts:${NC}"
    echo "  $PROJECT_DIR/.dev/start.sh"
    echo "  $PROJECT_DIR/.dev/stop.sh"
    echo "  $PROJECT_DIR/.dev/logs.sh"
    echo "  $PROJECT_DIR/.dev/status.sh"
    echo "  $PROJECT_DIR/.dev/clean.sh"
    echo "  $PROJECT_DIR/.dev/bootstrap.sh"
    echo "  $PROJECT_DIR/.dev/rebuild.sh"
    echo
    if [[ "$CREATE_SYSTEMD" == "true" ]]; then
        echo -e "${CYAN}Systemd Service (Optional):${NC}"
        echo "  sudo systemctl enable rusternetes-dev  # Auto-start on boot"
        echo "  sudo systemctl start rusternetes-dev   # Start now"
        echo "  sudo systemctl status rusternetes-dev  # Check status"
        echo
    fi
    echo -e "${CYAN}Development Features:${NC}"
    echo "  • Authentication disabled (--skip-auth)"
    echo "  • TLS self-signed certificates"
    echo "  • SELinux in permissive mode"
    echo "  • Firewall configured for development ports"
    echo "  • $([ "$USE_DOCKER" == "true" ] && echo "Docker" || echo "Podman (rootful)") container runtime"
    echo
    echo -e "${CYAN}Next Steps:${NC}"
    echo "  • Modify code in $PROJECT_DIR/crates/"
    echo "  • Run cluster-rebuild to rebuild and test changes"
    echo "  • Use cluster-logs to debug issues"
    echo "  • See docs/DEVELOPMENT.md for detailed workflows"
    echo
    echo "=============================================="
    echo
}

# Main setup flow
main() {
    log_info "Starting Rusternetes development setup for Fedora..."

    check_root
    display_banner
    check_prerequisites
    install_system_packages
    install_rust
    install_container_runtime
    configure_selinux
    configure_firewall

    setup_repository || {
        log_error "Repository setup failed. Please clone the repository first."
        exit 1
    }

    build_binaries
    build_images
    create_dev_helpers
    create_env_file
    create_systemd_service

    display_completion
}

# Run main function
main "$@"
