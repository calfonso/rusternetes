#!/usr/bin/env bash
# Run a single Kubernetes conformance test against the rusternetes cluster via
# hydrophone (the SIG-Testing replacement for sonobuoy single-test runs).
#
# Writes per-test artifacts (e2e.log, junit_01.xml, run.log) into a dedicated
# output directory so multiple invocations can run in parallel without
# stomping each other. After hydrophone exits, an optional splitter (Unit 1)
# or an inline xmlstarlet pass produces per-testcase .txt files for use as
# input to follow-up Claude Code worker prompts.
#
# Usage:
#   scripts/conformance-single-test.sh <test-name-or-regex> [flags]
#
# Flags:
#   --output-dir <dir>          Override output directory.
#   --kubeconfig <path>         Override kubeconfig (default: $KUBECONFIG or
#                               ~/.kube/rusternetes-config).
#   --conformance-image <img>   Conformance image (default:
#                               registry.k8s.io/conformance:v1.35.0).
#   --no-anchor                 Treat <test-name-or-regex> as a raw regex
#                               (skip regex escaping + ^…$ anchoring).
#   -h, --help                  Show this help.

set -euo pipefail
IFS=$'\n\t'

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DEFAULT_IMAGE="registry.k8s.io/conformance:v1.35.0"
HYDROPHONE_FALLBACK_IMAGE="registry.k8s.io/hydrophone:latest"

usage() {
    cat <<'EOF'
Run a single Kubernetes conformance test via hydrophone.

Usage:
  scripts/conformance-single-test.sh <test-name-or-regex> [flags]

Flags:
  --output-dir <dir>          Override output directory.
  --kubeconfig <path>         Override kubeconfig (default: $KUBECONFIG or
                              ~/.kube/rusternetes-config).
  --conformance-image <img>   Conformance image (default:
                              registry.k8s.io/conformance:v1.35.0).
  --no-anchor                 Treat <test-name-or-regex> as a raw regex
                              (skip regex escaping). Default already
                              uses substring match (no ^…$ anchors).
  -h, --help                  Show this help.
EOF
}

die() {
    echo "[${SCRIPT_NAME}] ERROR: $*" >&2
    exit 1
}

info() {
    echo "[${SCRIPT_NAME}] $*"
}

# ---------- argument parsing ----------

TEST_NAME=""
OUTPUT_DIR=""
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
IMAGE="$DEFAULT_IMAGE"
NO_ANCHOR=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --output-dir)
            [[ $# -ge 2 ]] || die "--output-dir requires a value"
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --kubeconfig)
            [[ $# -ge 2 ]] || die "--kubeconfig requires a value"
            KUBECONFIG_PATH="$2"
            shift 2
            ;;
        --conformance-image)
            [[ $# -ge 2 ]] || die "--conformance-image requires a value"
            IMAGE="$2"
            shift 2
            ;;
        --no-anchor)
            NO_ANCHOR=1
            shift
            ;;
        --)
            shift
            break
            ;;
        -*)
            die "unknown flag: $1 (use --help)"
            ;;
        *)
            if [[ -z "$TEST_NAME" ]]; then
                TEST_NAME="$1"
            else
                die "unexpected positional arg: $1"
            fi
            shift
            ;;
    esac
done

[[ -n "$TEST_NAME" ]] || { usage; die "missing required <test-name-or-regex>"; }

# ---------- focus regex ----------

if [[ "$NO_ANCHOR" -eq 1 ]]; then
    FOCUS_REGEX="$TEST_NAME"
else
    # Escape regex metachars common in K8s test names. Sed handles each char
    # in one pass; the leading backslash escapes the backslash itself in the
    # output, so '[Conformance]' becomes '\[Conformance\]'.
    # Escape regex metacharacters so the user can pass a literal test
    # description. Substring match (no ^…$
    # anchors) — Ginkgo's `--focus` is regex-anchorless, and real
    # testcase names carry `[NodeConformance] [Conformance]` suffixes
    # that anchoring would reject.
    escaped="$(printf '%s' "$TEST_NAME" | sed -e 's/[][\\.^$(){}*+?|]/\\&/g')"
    FOCUS_REGEX="$escaped"
fi

# ---------- output dir ----------

slug() {
    # First 80 chars of the test name, lowercased, non-alnum -> dash.
    printf '%s' "$1" \
        | tr '[:upper:]' '[:lower:]' \
        | tr -c 'a-z0-9' '-' \
        | sed -e 's/-\{2,\}/-/g' -e 's/^-//' -e 's/-$//' \
        | cut -c1-80
}

if [[ -z "$OUTPUT_DIR" ]]; then
    TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
    SLUG="$(slug "$TEST_NAME")"
    OUTPUT_DIR="$REPO_ROOT/.rusternetes/volumes/conformance-per-test-runs/${SLUG}-${TIMESTAMP}"
fi

mkdir -p "$OUTPUT_DIR"
RUN_LOG="$OUTPUT_DIR/run.log"
: > "$RUN_LOG"

info "test name        : $TEST_NAME"
info "focus regex      : $FOCUS_REGEX"
info "output dir       : $OUTPUT_DIR"
info "kubeconfig       : $KUBECONFIG_PATH"
info "conformance image: $IMAGE"

# ---------- precheck: cluster reachable ----------

if ! command -v kubectl >/dev/null 2>&1; then
    die "kubectl not on \$PATH — install kubectl to continue"
fi

if [[ ! -f "$KUBECONFIG_PATH" ]] && [[ "$KUBECONFIG_PATH" != "/dev/null" ]]; then
    die "kubeconfig not found at $KUBECONFIG_PATH (start cluster: podman compose up -d)"
fi

if ! kubectl --kubeconfig "$KUBECONFIG_PATH" get nodes >/dev/null 2>&1; then
    die "kubectl --kubeconfig $KUBECONFIG_PATH get nodes failed — is the cluster up? (podman compose up -d && bash scripts/bootstrap-cluster.sh)"
fi

# ---------- tool resolution ----------

HYDROPHONE_BIN=""
DOCKER_FALLBACK=0

resolve_hydrophone() {
    if command -v hydrophone >/dev/null 2>&1; then
        HYDROPHONE_BIN="$(command -v hydrophone)"
        info "using hydrophone from \$PATH: $HYDROPHONE_BIN"
        return
    fi

    if command -v go >/dev/null 2>&1; then
        local gobin
        gobin="$(mktemp -d -t hydrophone-gobin-XXXXXX)"
        info "hydrophone not on \$PATH — installing via 'go install' into $gobin"
        if GOBIN="$gobin" go install sigs.k8s.io/hydrophone@latest 2>&1 | tee -a "$RUN_LOG"; then
            if [[ -x "$gobin/hydrophone" ]]; then
                HYDROPHONE_BIN="$gobin/hydrophone"
                info "installed hydrophone: $HYDROPHONE_BIN"
                return
            fi
        fi
        info "go install failed — will try docker fallback"
    fi

    if command -v docker >/dev/null 2>&1; then
        DOCKER_FALLBACK=1
        info "falling back to docker image $HYDROPHONE_FALLBACK_IMAGE"
        return
    fi

    die "no hydrophone, no go, no docker — install hydrophone manually (https://github.com/kubernetes-sigs/hydrophone#installation)"
}

resolve_hydrophone

# ---------- run hydrophone ----------

HYDROPHONE_ARGS=(
    --focus "$FOCUS_REGEX"
    --output-dir "$OUTPUT_DIR"
    --kubeconfig "$KUBECONFIG_PATH"
    --conformance-image "$IMAGE"
    --skip-preflight conformance
)

info "invoking hydrophone (output streamed to $RUN_LOG)"
set +e
if [[ "$DOCKER_FALLBACK" -eq 1 ]]; then
    docker run --rm --network host \
        -v "$KUBECONFIG_PATH:/root/.kube/config:ro" \
        -v "$OUTPUT_DIR:/tmp/results" \
        "$HYDROPHONE_FALLBACK_IMAGE" \
        --focus "$FOCUS_REGEX" \
        --output-dir /tmp/results \
        --kubeconfig /root/.kube/config \
        --conformance-image "$IMAGE" \
        --skip-preflight conformance \
        2>&1 | tee -a "$RUN_LOG"
    HYDROPHONE_EXIT=${PIPESTATUS[0]}
else
    "$HYDROPHONE_BIN" "${HYDROPHONE_ARGS[@]}" 2>&1 | tee -a "$RUN_LOG"
    HYDROPHONE_EXIT=${PIPESTATUS[0]}
fi
set -e

if [[ "$HYDROPHONE_EXIT" -ne 0 ]]; then
    info "hydrophone exited non-zero ($HYDROPHONE_EXIT) — continuing to artifact extraction"
fi

JUNIT_FILE="$OUTPUT_DIR/junit_01.xml"

# ---------- per-test artifact extraction ----------

PER_TEST_DIR="$OUTPUT_DIR/per-test"
SPLITTER="$REPO_ROOT/scripts/conformance-split-junit.sh"

if [[ -f "$SPLITTER" ]]; then
    info "splitting junit via $SPLITTER"
    if ! bash "$SPLITTER" --input "$JUNIT_FILE" --output-dir "$PER_TEST_DIR" --all; then
        info "splitter returned non-zero — output may still be partial"
    fi
elif [[ ! -f "$JUNIT_FILE" ]]; then
    info "no $JUNIT_FILE produced — skipping per-test extraction"
elif ! command -v xmlstarlet >/dev/null 2>&1; then
    info "xmlstarlet not installed — leaving raw junit_01.xml + e2e.log only"
    info "(install xmlstarlet for inline per-test extraction)"
else
    info "splitter script missing — running inline xmlstarlet extraction"
    mkdir -p "$PER_TEST_DIR"
    # Count testcases.
    case_count="$(xmlstarlet sel -t -v 'count(//testcase)' "$JUNIT_FILE" 2>/dev/null || echo 0)"
    info "junit testcase count: $case_count"
    i=1
    while [[ "$i" -le "$case_count" ]]; do
        name="$(xmlstarlet sel -t -v "(//testcase)[$i]/@name" "$JUNIT_FILE" 2>/dev/null || true)"
        failure_msg="$(xmlstarlet sel -t -v "(//testcase)[$i]/failure/@message" "$JUNIT_FILE" 2>/dev/null || true)"
        failure_body="$(xmlstarlet sel -t -v "(//testcase)[$i]/failure" "$JUNIT_FILE" 2>/dev/null || true)"
        if [[ -n "$failure_msg" || -n "$failure_body" ]]; then
            fname="$(slug "$name").txt"
            out="$PER_TEST_DIR/$fname"
            {
                echo "Test: $name"
                echo "Status: FAILED"
                echo
                echo "--- failure message ---"
                echo "$failure_msg"
                echo
                echo "--- failure detail ---"
                echo "$failure_body"
            } > "$out"
            info "wrote $out"
        fi
        i=$((i + 1))
    done
fi

# ---------- summary ----------

TOTAL=0
FAILED=0
PASSED=0
if [[ -f "$JUNIT_FILE" ]] && command -v xmlstarlet >/dev/null 2>&1; then
    TOTAL="$(xmlstarlet sel -t -v 'count(//testcase)' "$JUNIT_FILE" 2>/dev/null || echo 0)"
    FAILED="$(xmlstarlet sel -t -v 'count(//testcase[failure])' "$JUNIT_FILE" 2>/dev/null || echo 0)"
    PASSED=$((TOTAL - FAILED))
fi

echo
info "=== summary ==="
info "total : $TOTAL"
info "passed: $PASSED"
info "failed: $FAILED"
info "artifacts: $OUTPUT_DIR"
if [[ -d "$PER_TEST_DIR" ]]; then
    info "per-test : $PER_TEST_DIR"
fi

exit "$HYDROPHONE_EXIT"
