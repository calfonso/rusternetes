#!/usr/bin/env bash
# Kubelet-scoped conformance runner.
# Boots compose.node-conformance.yml, bootstraps the cluster (kubernetes service,
# default ServiceAccounts), fetches the upstream e2e.test binary, runs ginkgo
# focused on [NodeConformance], dumps results.
#
# Why e2e.test and not e2e_node.test? The upstream e2e_node.test binary chroots
# into /rootfs during system validation in BeforeSuite — it is designed to run
# inside a privileged container with the host root bind-mounted, which is what
# the legacy registry.k8s.io/node-test:0.2 image provided. That image is
# end-of-lifed. e2e.test (the regular conformance binary) has the same
# [NodeConformance]-labeled specs (191 in v1.35), runs them via the api-server,
# and does not require rootfs chroot — making it the practical choice for a
# scaffold that runs against our containerised kubelet.
#
# See docs/superpowers/specs/2026-05-17-node-conformance-design.md.
set -euo pipefail

K8S_VERSION="${K8S_VERSION:-v1.35.0}"
ARCH="${ARCH:-linux-amd64}"
TEST_TARBALL_URL="https://dl.k8s.io/${K8S_VERSION}/kubernetes-test-${ARCH}.tar.gz"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${PROJECT_ROOT}/.bin"
RESULTS_DIR="/tmp/node-conformance"
FOCUS="${FOCUS:-\\[NodeConformance\\]}"
SKIP="${SKIP:-\\[Flaky\\]|\\[Serial\\]|\\[Slow\\]}"

export KUBELET_VOLUMES_PATH="${KUBELET_VOLUMES_PATH:-${PROJECT_ROOT}/.rusternetes/volumes}"
mkdir -p "${KUBELET_VOLUMES_PATH}" "${BIN_DIR}" "${RESULTS_DIR}"

KUBECONFIG_FILE="${HOME}/.kube/rusternetes-config"
# The kubeconfig is created on demand below (after the stack is up). Don't
# hard-fail here — earlier the script bailed before even attempting to bring
# up the cluster when run on a fresh CI runner (see PR fixing node-conformance
# kubeconfig-and-docker-sock).
if [ ! -f "${KUBECONFIG_FILE}" ]; then
    echo "kubeconfig not present at ${KUBECONFIG_FILE} — will write a minimal one after the stack is healthy."
fi

CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-$(command -v podman >/dev/null 2>&1 && echo podman || echo docker)}"

# Layer the dind override automatically when the runtime is docker so the
# api-server + kubelet bind /var/run/docker.sock instead of the podman socket
# that doesn't exist on Docker-only hosts (the CI ARC runner is one).
# Callers can override with EXTRA_COMPOSE_FILES (space-separated).
if [ -z "${EXTRA_COMPOSE_FILES:-}" ] && [ "${CONTAINER_RUNTIME}" = "docker" ] \
    && [ -f "${PROJECT_ROOT}/compose.dind.node-conformance.yml" ]; then
    EXTRA_COMPOSE_FILES="${PROJECT_ROOT}/compose.dind.node-conformance.yml"
fi

COMPOSE_FILES="-f ${PROJECT_ROOT}/compose.node-conformance.yml"
for extra in ${EXTRA_COMPOSE_FILES:-}; do
    COMPOSE_FILES="${COMPOSE_FILES} -f ${extra}"
done
# shellcheck disable=SC2086
# SC2086: intentional word-splitting — COMPOSE holds runtime + subcommand + flag triple
COMPOSE="${CONTAINER_RUNTIME} compose ${COMPOSE_FILES}"

echo "=== Rusternetes Node Conformance ==="
echo "K8S_VERSION=${K8S_VERSION} FOCUS=${FOCUS}"

echo "[1/7] Tearing down any previous node-conformance stack..."
# shellcheck disable=SC2086
${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true

echo "[2/7] Bringing up single-node stack..."
# shellcheck disable=SC2086
${COMPOSE} up -d --build

echo "[3/7] Waiting for kubelet to come up (max 60s)..."
for i in $(seq 1 60); do
    # Single HTTP server on :10250 serves both /healthz and /metrics —
    # see compose.node-conformance.yml kubelet block for the rationale.
    if curl -sfk "http://localhost:10250/healthz" >/dev/null 2>&1 \
        || curl -sfk "http://localhost:10250/metrics" >/dev/null 2>&1; then
        echo "kubelet is up"
        break
    fi
    sleep 1
    if [ "$i" -eq 60 ]; then
        echo "ERROR: kubelet did not come up within 60s"
        # shellcheck disable=SC2086
        ${COMPOSE} logs kubelet || true
        exit 1
    fi
done

if [ ! -f "${KUBECONFIG_FILE}" ]; then
    echo "Writing kubeconfig at ${KUBECONFIG_FILE} (api-server has --skip-auth, any bearer token works)..."
    mkdir -p "$(dirname "${KUBECONFIG_FILE}")"
    cat > "${KUBECONFIG_FILE}" <<'EOF'
apiVersion: v1
kind: Config
clusters:
  - name: rusternetes
    cluster:
      insecure-skip-tls-verify: true
      server: https://localhost:6443
contexts:
  - name: rusternetes
    context:
      cluster: rusternetes
      user: admin
current-context: rusternetes
users:
  - name: admin
    user:
      token: anonymous
EOF
fi
export KUBECONFIG="${KUBECONFIG_FILE}"

echo "[4/7] Bootstrapping cluster (kubernetes service, default ServiceAccounts, CoreDNS)..."
CONTAINER_RUNTIME="${CONTAINER_RUNTIME}" bash "${PROJECT_ROOT}/scripts/bootstrap-cluster.sh" || {
    echo "WARNING: bootstrap-cluster.sh exited non-zero — continuing anyway, some BeforeSuite checks may fail."
}

echo "[5/7] Fetching e2e.test (${K8S_VERSION} ${ARCH})..."
if [ ! -f "${BIN_DIR}/e2e.test" ] || [ ! -f "${BIN_DIR}/ginkgo" ]; then
    TMP_TARBALL="$(mktemp)"
    curl -fL "${TEST_TARBALL_URL}" -o "${TMP_TARBALL}"
    tar -xzf "${TMP_TARBALL}" -C "${BIN_DIR}" \
        --strip-components=3 \
        kubernetes/test/bin/e2e.test \
        kubernetes/test/bin/ginkgo
    rm -f "${TMP_TARBALL}"
    chmod +x "${BIN_DIR}/e2e.test" "${BIN_DIR}/ginkgo"
fi

echo "[6/7] Running ginkgo focus=${FOCUS}..."
# Disable errexit + pipefail across the pipe so we can capture ginkgo's
# real exit status from PIPESTATUS even when tee succeeds (or vice
# versa) without killing the script. Re-enable immediately after.
set +e
set +o pipefail
KUBECONFIG="${KUBECONFIG_FILE}" \
"${BIN_DIR}/ginkgo" \
    --focus="${FOCUS}" \
    --skip="${SKIP}" \
    --no-color \
    "${BIN_DIR}/e2e.test" \
    -- \
    --provider=local \
    --num-nodes=1 \
    --report-dir="${RESULTS_DIR}" \
    2>&1 | tee "${RESULTS_DIR}/ginkgo.log"
GINKGO_RC=${PIPESTATUS[0]}
set -eo pipefail

echo "[7/7] Parsing results..."
PASS=$(grep -cE '^\s*\[PASSED\]|• \[' "${RESULTS_DIR}/ginkgo.log" || true)
FAIL=$(grep -cE '^\s*\[FAILED\]|✗ ' "${RESULTS_DIR}/ginkgo.log" || true)
# Final-line ginkgo summary is the most reliable source for skip count.
SUMMARY=$(grep -E "Ran [0-9]+ of [0-9]+ Specs" "${RESULTS_DIR}/ginkgo.log" | tail -1 || true)

echo "PASS=${PASS} FAIL=${FAIL}"
echo "Summary: ${SUMMARY}"
echo "Full log: ${RESULTS_DIR}/ginkgo.log"
echo "Ginkgo exit code: ${GINKGO_RC}"

# Propagate failure so CI surfaces it. Either a non-zero ginkgo exit
# (covers BeforeSuite / infra failures) or any spec FAIL count is a
# real failure — even a single SynchronizedBeforeSuite failure ran 0
# specs and produces PASS=0 FAIL=2 in the parsed output (see run
# 26392683106 — the script previously masked this as success).
if [ "${GINKGO_RC}" -ne 0 ] || [ "${FAIL}" -gt 0 ]; then
    echo "ERROR: conformance run did not pass (ginkgo_rc=${GINKGO_RC}, FAIL=${FAIL})"
    exit 1
fi
