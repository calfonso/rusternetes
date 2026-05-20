#!/usr/bin/env bash
# Run every test listed in `ci/conformance/known-green.txt` against an
# already-running rusternetes cluster via Hydrophone, then assert that
# each listed test PASSED. Exits non-zero on any regression.
#
# This is the ratchet enforcer for the conformance canary. It does NOT
# bring up a cluster — callers (the conformance-canary workflow,
# `docker compose up -d` + `bootstrap-cluster.sh` locally) are
# responsible for that. The script just calls Hydrophone with a
# focus regex built from the file and parses the resulting junit.
#
# Usage:
#   bash scripts/conformance-canary-run.sh [flags]
#
# Flags:
#   --known-green PATH    Override known-green.txt path
#                         (default: ci/conformance/known-green.txt)
#   --kubeconfig PATH     Override kubeconfig
#                         (default: $KUBECONFIG or ~/.kube/rusternetes-config)
#   --output-dir DIR      Output dir for hydrophone artifacts
#                         (default: .rusternetes/volumes/conformance-canary-<ts>)
#   --conformance-image IMG
#                         Override conformance image. Defaults to
#                         registry.k8s.io/conformance:v1.35.0 — the pin
#                         documented in known-green.txt.
#   --hydrophone PATH     Override hydrophone binary path (default: discover via $PATH).
#   -h, --help            Show this help.
#
# Exit codes:
#   0  every known-green test passed
#   1  at least one known-green test did not pass (regression)
#   2  usage / preflight error (no kubeconfig, missing hydrophone, etc.)

set -euo pipefail
IFS=$'\n\t'

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"

KNOWN_GREEN_FILE="$REPO_ROOT/ci/conformance/known-green.txt"
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
DEFAULT_IMAGE="registry.k8s.io/conformance:v1.35.0"
CONFORMANCE_IMAGE="$DEFAULT_IMAGE"
OUTPUT_DIR=""
HYDROPHONE_BIN=""

die() {
    echo "[${SCRIPT_NAME}] ERROR: $*" >&2
    exit 2
}

info() {
    echo "[${SCRIPT_NAME}] $*"
}

usage() {
    sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -40
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage; exit 0 ;;
        --known-green)
            [[ $# -ge 2 ]] || die "--known-green requires a value"
            KNOWN_GREEN_FILE="$2"; shift 2 ;;
        --kubeconfig)
            [[ $# -ge 2 ]] || die "--kubeconfig requires a value"
            KUBECONFIG_PATH="$2"; shift 2 ;;
        --output-dir)
            [[ $# -ge 2 ]] || die "--output-dir requires a value"
            OUTPUT_DIR="$2"; shift 2 ;;
        --conformance-image)
            [[ $# -ge 2 ]] || die "--conformance-image requires a value"
            CONFORMANCE_IMAGE="$2"; shift 2 ;;
        --hydrophone)
            [[ $# -ge 2 ]] || die "--hydrophone requires a value"
            HYDROPHONE_BIN="$2"; shift 2 ;;
        --)
            shift; break ;;
        -*)
            die "unknown flag: $1 (use --help)" ;;
        *)
            die "unexpected positional arg: $1" ;;
    esac
done

# ---------- preflight ----------

[[ -f "$KNOWN_GREEN_FILE" ]] || die "known-green file not found: $KNOWN_GREEN_FILE"
[[ -f "$KUBECONFIG_PATH" ]] || die "kubeconfig not found: $KUBECONFIG_PATH"

if [[ -z "$HYDROPHONE_BIN" ]]; then
    if command -v hydrophone >/dev/null 2>&1; then
        HYDROPHONE_BIN="$(command -v hydrophone)"
    else
        die "hydrophone not on \$PATH; install it (https://github.com/kubernetes-sigs/hydrophone) or pass --hydrophone"
    fi
fi
[[ -x "$HYDROPHONE_BIN" ]] || die "hydrophone binary not executable: $HYDROPHONE_BIN"

if [[ -z "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="$REPO_ROOT/.rusternetes/volumes/conformance-canary-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$OUTPUT_DIR"

# ---------- parse known-green.txt ----------

# Read each non-comment, non-blank line as a literal test-name substring.
mapfile -t ENTRIES < <(grep -vE '^\s*(#|$)' "$KNOWN_GREEN_FILE" || true)
[[ ${#ENTRIES[@]} -gt 0 ]] || die "no entries in $KNOWN_GREEN_FILE"

# Build a single regex by escaping each entry's metacharacters and
# joining with `|`. Hydrophone passes --focus straight to Ginkgo, which
# uses Go's regexp/syntax (RE2-ish). Escape characters that have
# meaning there: ] [ \ . ^ $ ( ) { } * + ? |
escape_regex() {
    printf '%s' "$1" | sed -e 's/[][\\.^$(){}*+?|]/\\&/g'
}

FOCUS=""
for entry in "${ENTRIES[@]}"; do
    esc=$(escape_regex "$entry")
    if [[ -n "$FOCUS" ]]; then
        FOCUS="${FOCUS}|${esc}"
    else
        FOCUS="$esc"
    fi
done

info "known-green entries : ${#ENTRIES[@]}"
info "conformance image   : $CONFORMANCE_IMAGE"
info "kubeconfig          : $KUBECONFIG_PATH"
info "output dir          : $OUTPUT_DIR"
info "hydrophone          : $HYDROPHONE_BIN"

# ---------- run hydrophone ----------

set +e
"$HYDROPHONE_BIN" \
    --focus "$FOCUS" \
    --output-dir "$OUTPUT_DIR" \
    --kubeconfig "$KUBECONFIG_PATH" \
    --conformance-image "$CONFORMANCE_IMAGE" 2>&1 | tee "$OUTPUT_DIR/run.log"
HYDROPHONE_EXIT=${PIPESTATUS[0]}
set -e

JUNIT="$OUTPUT_DIR/junit_01.xml"
if [[ ! -f "$JUNIT" ]]; then
    info "hydrophone exit=$HYDROPHONE_EXIT but no junit produced — preserving $OUTPUT_DIR for triage"
    exit 1
fi

# ---------- verify each entry passed ----------

REGRESS=()
PASS=()
for entry in "${ENTRIES[@]}"; do
    # Build a literal-match line that uniquely identifies the testcase.
    # Junit testcase names are `<sig> ... [It] <It-string> [Conformance]` —
    # the entry from the file matches as a substring of the @name attribute.
    line=$(grep -F "name=\"[It] ${entry}" "$JUNIT" | head -1 || true)
    if [[ -z "$line" ]]; then
        REGRESS+=("$entry  (no matching testcase in junit)")
        continue
    fi
    if [[ "$line" == *'status="passed"'* ]]; then
        PASS+=("$entry")
    elif [[ "$line" == *'status="failed"'* ]]; then
        REGRESS+=("$entry  (status=failed)")
    elif [[ "$line" == *'status="skipped"'* ]]; then
        REGRESS+=("$entry  (status=skipped — focus regex didn't include it)")
    else
        REGRESS+=("$entry  (unknown status in: ${line:0:120})")
    fi
done

echo
info "=== summary ==="
info "expected green : ${#ENTRIES[@]}"
info "actually green : ${#PASS[@]}"
info "regressions    : ${#REGRESS[@]}"

if [[ ${#REGRESS[@]} -gt 0 ]]; then
    echo
    info "REGRESSIONS:"
    printf '  - %s\n' "${REGRESS[@]}"
    info "artifacts: $OUTPUT_DIR"
    exit 1
fi

info "all known-green tests passed (hydrophone exit=$HYDROPHONE_EXIT)"
exit 0
