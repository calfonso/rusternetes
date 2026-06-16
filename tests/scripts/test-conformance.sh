#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Rusternetes Conformance Test Runner ===${NC}\n"

# Check if KUBELET_VOLUMES_PATH is set
if [ -z "$KUBELET_VOLUMES_PATH" ]; then
    echo -e "${RED}Error: KUBELET_VOLUMES_PATH environment variable is not set${NC}"
    echo ""
    echo "You must set this to an absolute path before starting the cluster."
    echo ""
    echo "Example:"
    echo "  export KUBELET_VOLUMES_PATH=\$(pwd)/.rusternetes/volumes"
    echo "  $0"
    echo ""
    exit 1
fi

echo -e "${GREEN}Using volume path: ${KUBELET_VOLUMES_PATH}${NC}\n"

# Check if sonobuoy is installed
if ! command -v sonobuoy &> /dev/null; then
    echo -e "${RED}Error: sonobuoy is not installed${NC}"
    echo "Install it with:"
    echo "  macOS: brew install sonobuoy"
    echo "  Linux: Download from https://github.com/vmware-tanzu/sonobuoy/releases"
    exit 1
fi

# Detect container runtime + compose command (mirrors Makefile)
if command -v podman-compose &> /dev/null; then
    COMPOSE_CMD="podman-compose"
    COMPOSE_FILE="compose.yml"
    RUNTIME="podman"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
    COMPOSE_FILE="docker-compose.yml"
    RUNTIME="docker"
elif docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
    COMPOSE_FILE="docker-compose.yml"
    RUNTIME="docker"
else
    echo -e "${RED}Error: no compose command found (podman-compose, docker-compose, or docker compose plugin)${NC}"
    exit 1
fi
COMPOSE="$COMPOSE_CMD -f $COMPOSE_FILE"

# Check if cluster is already running
if $RUNTIME ps --filter "name=rusternetes" --format "{{.Names}}" | grep -q rusternetes; then
    echo -e "${YELLOW}Cluster is already running${NC}"
    read -p "Do you want to restart it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping existing cluster..."
        $COMPOSE down
        echo "Starting fresh cluster..."
        $COMPOSE up -d
    fi
else
    echo "Starting Rusternetes cluster..."
    $COMPOSE up -d
fi

# Set up kubectl command with TLS skip
KCTL="./target/release/kubectl --insecure-skip-tls-verify"

# Create a kubeconfig for Sonobuoy (it needs one)
TEMP_KUBECONFIG=$(mktemp)
trap "rm -f $TEMP_KUBECONFIG" EXIT

cat > $TEMP_KUBECONFIG <<EOF
apiVersion: v1
kind: Config
clusters:
- cluster:
    insecure-skip-tls-verify: true
    server: https://localhost:6443
  name: rusternetes
contexts:
- context:
    cluster: rusternetes
    user: admin
  name: rusternetes
current-context: rusternetes
users:
- name: admin
  user: {}
EOF

export KUBECONFIG=$TEMP_KUBECONFIG

# Check if kubectl binary exists
if [ ! -f "./target/release/kubectl" ]; then
    echo -e "${RED}Error: kubectl binary not found at ./target/release/kubectl${NC}"
    echo "Build it with: cargo build --release --bin kubectl"
    exit 1
fi

echo -e "\n${GREEN}Waiting for cluster to be ready...${NC}"
echo "Checking for nodes..."

# Wait for nodes to be ready
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if $KCTL get nodes &>/dev/null; then
        # Check if node status shows True (Ready)
        if $KCTL get nodes 2>/dev/null | grep -q "True"; then
            echo -e "${GREEN}✓ Cluster is ready${NC}\n"
            break
        fi
    fi
    attempt=$((attempt + 1))
    echo "Attempt $attempt/$max_attempts..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo -e "${RED}Error: Cluster did not become ready in time${NC}"
    exit 1
fi

# Show cluster status
echo -e "${GREEN}Cluster status:${NC}"
$KCTL get nodes
echo

# Apply bootstrap resources (CoreDNS, etc.)
echo -e "${GREEN}Setting up cluster bootstrap resources...${NC}"
if [ -f "bootstrap-cluster.yaml" ]; then
    # Expand environment variables (like ${KUBELET_VOLUMES_PATH}) before applying
    envsubst < bootstrap-cluster.yaml | $KCTL apply -f -
    echo "Waiting for CoreDNS pod to be ready..."

    max_attempts=30
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if $KCTL get pods -n kube-system -l k8s-app=kube-dns 2>/dev/null | grep -q "Running"; then
            echo -e "${GREEN}✓ cluster DNS (rusternetes-dns) is ready${NC}\n"
            break
        fi
        attempt=$((attempt + 1))
        echo "Attempt $attempt/$max_attempts..."
        sleep 2
    done

    if [ $attempt -eq $max_attempts ]; then
        echo -e "${YELLOW}Warning: CoreDNS pod did not become ready in time${NC}"
        echo -e "${YELLOW}Proceeding anyway, but some tests may fail${NC}\n"
    fi
else
    echo -e "${YELLOW}Warning: bootstrap-cluster.yaml not found${NC}"
    echo -e "${YELLOW}Skipping bootstrap setup${NC}\n"
fi

# Clean up any previous sonobuoy runs
echo -e "\n${YELLOW}Cleaning up any previous test runs...${NC}"
# Try using sonobuoy delete first
sonobuoy delete --wait 2>/dev/null || true

# Clean up cluster-scoped RBAC resources that sonobuoy doesn't delete
echo "Cleaning cluster-scoped Sonobuoy resources..."
if docker exec rusternetes-etcd etcdctl --endpoints=http://localhost:2379 get --prefix "/registry/clusterrolebindings/sonobuoy" --keys-only 2>/dev/null | grep -q sonobuoy; then
    echo "  - Deleting Sonobuoy ClusterRoleBindings..."
    docker exec rusternetes-etcd etcdctl --endpoints=http://localhost:2379 del --prefix "/registry/clusterrolebindings/sonobuoy" >/dev/null 2>&1
fi

if docker exec rusternetes-etcd etcdctl --endpoints=http://localhost:2379 get --prefix "/registry/clusterroles/sonobuoy" --keys-only 2>/dev/null | grep -q sonobuoy; then
    echo "  - Deleting Sonobuoy ClusterRoles..."
    docker exec rusternetes-etcd etcdctl --endpoints=http://localhost:2379 del --prefix "/registry/clusterroles/sonobuoy" >/dev/null 2>&1
fi

# Delete namespace if it still exists (will cascade delete all namespaced resources)
if $KCTL get namespace sonobuoy &>/dev/null; then
    echo "  - Deleting Sonobuoy namespace..."
    $KCTL delete namespace sonobuoy 2>/dev/null || true
    sleep 2
fi

echo -e "${GREEN}✓ Environment cleaned${NC}\n"

# Choose test mode
echo "Select test mode:"
echo "  1) Quick mode (~10-15 minutes, basic conformance)"
echo "  2) Custom focus (specify test pattern)"
echo "  3) Full conformance (may take 1-2 hours)"
read -p "Enter choice (1-3) [default: 1]: " test_mode
test_mode=${test_mode:-1}

# Run tests based on selection
echo -e "\n${GREEN}Starting conformance tests...${NC}"
case $test_mode in
    1)
        echo "Running in quick mode..."
        sonobuoy run --mode=quick --wait
        ;;
    2)
        read -p "Enter test focus pattern (e.g., 'Pods.*should'): " focus_pattern
        sonobuoy run --plugin e2e \
            --e2e-focus="$focus_pattern" \
            --e2e-skip="Serial|Disruptive" \
            --wait
        ;;
    3)
        echo "Running full conformance suite..."
        echo -e "${YELLOW}Warning: This may take 1-2 hours${NC}"
        sonobuoy run --mode=certified-conformance --wait
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

# Retrieve and display results
echo -e "\n${GREEN}Collecting results...${NC}"
results=$(sonobuoy retrieve)
echo "Results saved to: $results"

echo -e "\n${GREEN}=== Test Results ===${NC}"
sonobuoy results $results --mode=detailed

# Show summary
echo -e "\n${GREEN}=== Summary ===${NC}"
sonobuoy results $results

# Ask if user wants to see failed tests
read -p "Show failed test details? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    sonobuoy results $results --mode=detailed --plugin e2e | grep -A 10 "failed"
fi

# Cleanup
echo -e "\n${YELLOW}Cleaning up test pods...${NC}"
sonobuoy delete --wait

echo -e "\n${GREEN}=== Conformance testing complete ===${NC}"
echo "Full results available at: $results"
echo "To extract: tar xzf $results"
