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

# The stack runs pods via the bundled `containerd` CRI service over the shared
# `containerd-run` socket volume (CONTAINER_RUNTIME_ENDPOINT in
# compose.node-conformance.yml), so it needs no host runtime socket and works
# as-is on both Docker and Podman. The old compose.dind.node-conformance.yml
# override (which bind-mounted /var/run/docker.sock for the dead bollard path)
# is gone — extra overrides can still be layered via EXTRA_COMPOSE_FILES.

COMPOSE_FILES="-f ${PROJECT_ROOT}/compose.node-conformance.yml"
for extra in ${EXTRA_COMPOSE_FILES:-}; do
    COMPOSE_FILES="${COMPOSE_FILES} -f ${extra}"
done

# Prebuilt-image mode (#1056): RUSTERNETES_IMAGE_TAG selects a GHCR tag
# published by .github/workflows/publish-images.yml (e.g. main, sha-<sha>).
# Pull-or-fallback: a successful pull layers the ghcr overlay (image: names)
# and skips the local build; a failed pull (tag missing, registry down)
# logs loudly and keeps today's --build path.
#
# REQUIRE_PREBUILT=true (set by the nightly schedule, where the `main` tag must
# exist once the GHCR packages are public) turns a failed pull into a HARD
# ERROR instead of a silent local-build fallback — otherwise a forgotten GHCR
# visibility flip leaves the nightly green-but-always-compiling forever (#1108).
UP_BUILD_FLAG="--build"
if [ -n "${RUSTERNETES_IMAGE_TAG:-}" ]; then
    GHCR_OVERLAY="${PROJECT_ROOT}/compose.ghcr.node-conformance.yml"
    # shellcheck disable=SC2086
    if ${CONTAINER_RUNTIME} compose ${COMPOSE_FILES} -f "${GHCR_OVERLAY}" pull; then
        COMPOSE_FILES="${COMPOSE_FILES} -f ${GHCR_OVERLAY}"
        UP_BUILD_FLAG="--no-build"
        echo "Using prebuilt images: tag=${RUSTERNETES_IMAGE_TAG}"
    elif [ "${REQUIRE_PREBUILT:-false}" = "true" ]; then
        echo "ERROR: prebuilt images tag=${RUSTERNETES_IMAGE_TAG} not pullable on a require-prebuilt run — the ghcr.io/indyjonesnl/rusternetes/* packages are likely still private (flip them to public, see PR #1106 checklist). Refusing to fall back to a local build." >&2
        if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
            {
                echo "### ❌ Prebuilt-image pull failed (hard error)"
                echo
                echo "Tag \`${RUSTERNETES_IMAGE_TAG}\` was not pullable and \`REQUIRE_PREBUILT\` is set."
                echo "Most likely the \`ghcr.io/indyjonesnl/rusternetes/*\` packages are still **private** (PR #1106 post-merge checklist)."
            } >> "${GITHUB_STEP_SUMMARY}"
        fi
        exit 1
    else
        echo "WARNING: prebuilt images tag=${RUSTERNETES_IMAGE_TAG} not pullable — falling back to local build" >&2
        if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
            {
                echo "### ⚠️ Prebuilt-image fallback: built locally"
                echo
                echo "Tag \`${RUSTERNETES_IMAGE_TAG}\` was not pullable, so images were **compiled locally**."
                echo "If this persists, the \`ghcr.io/indyjonesnl/rusternetes/*\` packages may still be private (PR #1106 post-merge checklist)."
            } >> "${GITHUB_STEP_SUMMARY}"
        fi
    fi
fi

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
${COMPOSE} up -d ${UP_BUILD_FLAG}

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
# The node-conformance stack uses its own bridge (`rusternetes-nc-net`,
# pinned in compose.node-conformance.yml) instead of the default
# `rusternetes-network` the discover-bridge-gateway helper assumes.
# Point the helper at the right network so CoreDNS gets a valid gateway IP.
#
# SKIP_DNS_WIRING=1: this single-node stack ships no in-cluster DNS backend
# (no rusternetes-dns container), and the [NodeConformance] suite has no
# cluster-DNS-resolution specs. Skipping the wiring avoids a 30s wait and a
# misleading "DNS will NOT be functional" warning every run.
CONTAINER_RUNTIME="${CONTAINER_RUNTIME}" \
RUSTERNETES_NETWORK_NAME="${RUSTERNETES_NETWORK_NAME:-rusternetes-nc-net}" \
SKIP_DNS_WIRING=1 \
bash "${PROJECT_ROOT}/scripts/bootstrap-cluster.sh" || {
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

# Ginkgo parallelism + suite timeout. Both are required for the suite to
# complete: ~150 [NodeConformance] specs run serially at ~30-60s each would
# blow past ginkgo's default 1h suite timeout (historically only ~8 specs ran
# before the cap — each blocked on an un-schedulable pod). Run several specs
# concurrently (each in its own namespace; the single kubelet handles parallel
# pods fine — measured 4-way locally) and raise the suite timeout to sit under
# the workflow's 90-minute job cap. Both overridable via env.
GINKGO_NODES="${GINKGO_NODES:-4}"
GINKGO_TIMEOUT="${GINKGO_TIMEOUT:-85m}"

echo "[6/7] Running ginkgo focus=${FOCUS} (nodes=${GINKGO_NODES}, timeout=${GINKGO_TIMEOUT})..."
# Disable errexit + pipefail across the pipe so we can capture ginkgo's
# real exit status from PIPESTATUS even when tee succeeds (or vice
# versa) without killing the script. Re-enable immediately after.
set +e
set +o pipefail
FAIL_FAST_FLAG=""
if [ "${FAIL_FAST:-0}" = "1" ]; then
    FAIL_FAST_FLAG="--fail-fast"
fi
KUBECONFIG="${KUBECONFIG_FILE}" \
"${BIN_DIR}/ginkgo" \
    --focus="${FOCUS}" \
    --skip="${SKIP}" \
    --nodes="${GINKGO_NODES}" \
    --timeout="${GINKGO_TIMEOUT}" \
    --no-color \
    ${FAIL_FAST_FLAG} \
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
