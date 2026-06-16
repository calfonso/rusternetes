#!/bin/bash
set -e
set -o pipefail

# Conformance test runner for Rusternetes
# This script handles the full lifecycle of running Kubernetes conformance tests

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Rusternetes Conformance Test Runner ==="
echo ""

# Setup kubeconfig
export KUBECONFIG=~/.kube/rusternetes-config

# Preflight: API server must be reachable, otherwise the cleanup step
# silently aborts under `set -e` when kubectl can't connect.
if ! curl -sk --max-time 5 https://localhost:6443/healthz >/dev/null 2>&1; then
    echo "ERROR: API server at https://localhost:6443 is not reachable." >&2
    echo "Start the cluster first, e.g.:" >&2
    echo "  export KUBELET_VOLUMES_PATH=\$(pwd)/.rusternetes/volumes" >&2
    echo "  docker compose -f compose.all-in-one.yml -f compose.dind.all-in-one.yml up -d" >&2
    echo "  bash scripts/bootstrap-cluster.sh" >&2
    exit 1
fi

# Step 1: Kill any running sonobuoy processes
echo "[1/5] Cleaning up old sonobuoy processes..."
pkill -f "sonobuoy run" || true
sleep 2

# Step 2: Delete sonobuoy resources from cluster (ignore errors)
echo "[2/5] Cleaning up sonobuoy resources..."
timeout 30 sonobuoy delete --wait 2>/dev/null || {
    echo "Sonobuoy delete timed out or failed, force cleaning..."
    set +e
    kubectl delete pods        --all -n sonobuoy --force --grace-period=0 2>/dev/null
    kubectl delete jobs        --all -n sonobuoy --force --grace-period=0 2>/dev/null
    kubectl delete daemonsets  --all -n sonobuoy --force --grace-period=0 2>/dev/null
    kubectl delete services    --all -n sonobuoy --force --grace-period=0 2>/dev/null
    timeout 10 kubectl delete namespace sonobuoy --force --grace-period=0 2>/dev/null
    set -e
}
sleep 2

# Step 3: Add required labels to nodes (required for sonobuoy e2e tests)
echo "[3/5] Adding required labels to nodes..."
curl -sk -X PATCH https://localhost:6443/api/v1/nodes/node-1 \
    -H "Content-Type: application/merge-patch+json" \
    -d '{"metadata":{"labels":{"kubernetes.io/os":"linux","kubernetes.io/arch":"amd64","kubernetes.io/hostname":"node-1"}}}' >/dev/null 2>&1 || echo "Warning: Could not label node-1"
curl -sk -X PATCH https://localhost:6443/api/v1/nodes/node-2 \
    -H "Content-Type: application/merge-patch+json" \
    -d '{"metadata":{"labels":{"kubernetes.io/os":"linux","kubernetes.io/arch":"amd64","kubernetes.io/hostname":"node-2"}}}' >/dev/null 2>&1 || echo "Warning: Could not label node-2"

# Step 4: Ensure cluster DNS has a ready backend.
# rusternetes-dns backs the kube-dns Service via EndpointSlices (CoreDNS has
# been removed), so check for a ready kube-dns endpoint rather than a specific
# DNS Pod.
echo "[4/5] Checking cluster DNS (kube-dns endpoints) status..."
DNS_READY=$(curl -sk "https://localhost:6443/apis/discovery.k8s.io/v1/namespaces/kube-system/endpointslices?labelSelector=kubernetes.io/service-name%3Dkube-dns" 2>/dev/null | grep -o '"ready":true' | head -n1 || echo "")

if [ -z "$DNS_READY" ]; then
    echo "kube-dns has no ready endpoints, (re)bootstrapping cluster DNS..."
    # Recreate via bootstrap script (includes ServiceAccount/token generation
    # and DNS backend wiring).
    ./scripts/bootstrap-cluster.sh
else
    echo "kube-dns has ready endpoints"
fi

# Step 5: Pre-pull required images
# sonobuoy CLI v0.57.4 doesn't have a published container image — use v0.57.3
echo "[5/7] Pre-pulling required container images..."
SONOBUOY_IMAGE_TAG="v0.57.3"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-$(command -v podman >/dev/null 2>&1 && echo podman || echo docker)}"
$CONTAINER_RUNTIME pull "docker.io/sonobuoy/sonobuoy:${SONOBUOY_IMAGE_TAG}" 2>/dev/null || true
$CONTAINER_RUNTIME tag "docker.io/sonobuoy/sonobuoy:${SONOBUOY_IMAGE_TAG}" "docker.io/sonobuoy/sonobuoy:v0.57.4" 2>/dev/null || true
$CONTAINER_RUNTIME pull docker.io/sonobuoy/systemd-logs:v0.4 2>/dev/null || true
$CONTAINER_RUNTIME pull registry.k8s.io/conformance:v1.35.0 2>/dev/null || true
echo "✓ Images pre-pulled"

# Step 6: Run conformance tests
# Accept an optional mode argument (default: certified-conformance)
SONOBUOY_MODE="${1:-certified-conformance}"
echo "[6/7] Starting conformance tests (this will take several minutes)..."
echo "Running: sonobuoy run --mode=${SONOBUOY_MODE} --wait"
echo ""

# Run sonobuoy and capture output
# Force JSON encoding (rusternetes doesn't support protobuf, which is client-go's default)
# The --kube-api-content-type flag tells the e2e test binary to use JSON for all API requests
if sonobuoy run --mode="${SONOBUOY_MODE}" \
    --timeout 86400 \
    --plugin-env "e2e.E2E_EXTRA_ARGS=--progress-report-url=http://localhost:8099/progress --kube-api-content-type=application/json" \
    --wait 2>&1 | tee /tmp/sonobuoy-latest.log; then
    TEST_RESULT="PASSED"
else
    TEST_RESULT="FAILED"
fi

# Step 7: Retrieve and display results
echo ""
echo "[7/7] Retrieving test results..."
echo ""

# Get the results
RESULTS_FILE=$(sonobuoy retrieve 2>/dev/null || echo "")
if [ -n "$RESULTS_FILE" ]; then
    echo "Results saved to: $RESULTS_FILE"
    echo ""
    echo "=== Test Summary ==="
    sonobuoy results "$RESULTS_FILE" 2>/dev/null || echo "Could not parse results"
    echo ""
    echo "=== Detailed Results ==="
    sonobuoy results "$RESULTS_FILE" --mode=detailed 2>/dev/null || echo "Could not get detailed results"
else
    echo "WARNING: Could not retrieve results file"
fi

echo ""
echo "=== Conformance Test Complete ==="
echo "Overall Status: $TEST_RESULT"
echo "Full log saved to: /tmp/sonobuoy-latest.log"
echo ""

if [ "$TEST_RESULT" == "FAILED" ]; then
    exit 1
fi
