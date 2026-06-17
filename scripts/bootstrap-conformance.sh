#!/bin/bash

# Bootstrap Conformance Testing
# This script prepares volumes with fresh certificates for conformance testing
# Run this after dev-setup-macos.sh when new certificates are generated

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="${SCRIPT_DIR}/.rusternetes/certs"
VOLUMES_DIR="${SCRIPT_DIR}/.rusternetes/volumes"

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  Conformance Testing Bootstrap${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_step() {
    echo -e "\n${GREEN}==>${NC} $1"
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

print_header

# Check if certificates exist
if [ ! -f "${CERT_DIR}/api-server.crt" ]; then
    print_error "API server certificate not found at ${CERT_DIR}/api-server.crt"
    echo "Run './scripts/generate-certs.sh' or './scripts/dev-setup-macos.sh' first"
    exit 1
fi

print_success "Found API server certificate"

# Create volumes directory if it doesn't exist
mkdir -p "${VOLUMES_DIR}"

# Function to prepare service account volume for a pod
prepare_sa_volume() {
    local pod_name=$1
    local namespace=$2
    local volume_path="${VOLUMES_DIR}/${pod_name}/kube-api-access"

    print_step "Preparing service account volume for ${pod_name} (namespace: ${namespace})"

    # Create volume directory
    mkdir -p "${volume_path}"

    # Copy CA certificate
    cp "${CERT_DIR}/api-server.crt" "${volume_path}/ca.crt"
    print_success "Copied CA certificate to ${volume_path}/ca.crt"

    # Create namespace file
    echo -n "${namespace}" > "${volume_path}/namespace"
    print_success "Created namespace file: ${namespace}"

    # Create placeholder token file (will be populated by kubelet if needed)
    # For now, use a placeholder that indicates it needs to be populated
    if [ ! -f "${volume_path}/token" ]; then
        echo "# Token will be populated by kubelet" > "${volume_path}/token"
        print_success "Created placeholder token file"
    else
        print_warning "Token file already exists, preserving it"
    fi

    # Set appropriate permissions
    chmod 644 "${volume_path}/ca.crt"
    chmod 644 "${volume_path}/namespace"
    chmod 600 "${volume_path}/token"
}

# Main logic
echo ""
echo "This script will prepare service account volumes for conformance testing."
echo "It will copy the latest certificates into the volumes directory."
echo ""
echo "What would you like to do?"
echo "  1) Prepare volumes for Sonobuoy"
echo "  2) Clean up all volumes (fresh start)"
echo "  3) Clean and prepare Sonobuoy"
echo ""
read -p "Enter your choice [1-3]: " choice

# NOTE: cluster DNS (rusternetes-dns) is a Deployment with its own
# ServiceAccount, so the kubelet auto-projects its SA token — it needs no
# pre-staged volume here. Only Sonobuoy still requires manual pre-staging
# (#1172).
case $choice in
    1)
        prepare_sa_volume "sonobuoy" "sonobuoy"
        ;;
    2)
        print_step "Cleaning up all volumes..."
        if [ -d "${VOLUMES_DIR}" ]; then
            # Clean up but preserve the directory structure
            read -p "This will remove all files in ${VOLUMES_DIR}. Continue? [y/N]: " confirm
            if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
                rm -rf "${VOLUMES_DIR}"/*
                print_success "All volumes cleaned"
            else
                print_warning "Cleanup cancelled"
                exit 0
            fi
        else
            print_warning "Volumes directory doesn't exist yet"
        fi
        ;;
    3)
        print_step "Cleaning up all volumes..."
        if [ -d "${VOLUMES_DIR}" ]; then
            rm -rf "${VOLUMES_DIR}"/*
            print_success "All volumes cleaned"
        fi
        print_step "Preparing fresh volumes..."
        prepare_sa_volume "sonobuoy" "sonobuoy"
        ;;
    *)
        print_error "Invalid choice"
        exit 1
        ;;
esac

echo ""
print_success "Bootstrap complete!"
echo ""
echo "Next steps:"
echo "  1. Start the cluster: docker-compose up -d"
echo "  2. Apply bootstrap resources: ./target/release/kubectl apply -f bootstrap-cluster.yaml"
echo "  3. Run conformance tests: ./tests/scripts/test-conformance.sh"
echo ""
echo "Note: The kubelet will automatically populate service account tokens when pods are created."
