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
#
# Each known-green line is a substring of the Ginkgo It-description.
# Junit @name attributes look like `[It] <description> [tags...]`. For
# the canary to actually catch regressions, every entry must satisfy:
#
#   1. resolve to exactly one matching testcase (no entry too loose)
#   2. that testcase passed
#
# Additionally — and this is what the old version missed — any failed
# testcase in the junit must be accounted for by some entry. A loose
# entry that incidentally pulls in a sibling test which then fails is
# a regression even if every listed entry's primary match passed.

# Collect every testcase name + status from the junit. Use a tab to
# delimit so the name (which contains spaces / brackets) survives.
declare -a JUNIT_NAMES JUNIT_STATUSES
while IFS=$'\t' read -r name status; do
    JUNIT_NAMES+=("$name")
    JUNIT_STATUSES+=("$status")
done < <(grep -oE '<testcase name="[^"]+"[^>]*status="[^"]+"' "$JUNIT" \
    | sed -E 's|<testcase name="([^"]+)"[^>]*status="([^"]+)"|\1\t\2|' \
    | sed -e 's/&#39;/'"'"'/g' -e 's/&amp;/\&/g' -e 's/&lt;/</g' -e 's/&gt;/>/g' -e 's/&quot;/"/g')

REGRESS=()
PASS=()
# Index of which junit testcases were claimed by some entry. Used to
# detect orphan failures below.
declare -a CLAIMED
for ((i = 0; i < ${#JUNIT_NAMES[@]}; i++)); do CLAIMED[i]=0; done

for entry in "${ENTRIES[@]}"; do
    needle="[It] ${entry}"
    matches=()
    match_idxs=()
    for ((i = 0; i < ${#JUNIT_NAMES[@]}; i++)); do
        if [[ "${JUNIT_NAMES[i]}" == *"$needle"* ]]; then
            matches+=("${JUNIT_STATUSES[i]}")
            match_idxs+=("$i")
        fi
    done
    case "${#matches[@]}" in
        0)
            REGRESS+=("$entry  (no matching testcase in junit)")
            ;;
        1)
            for idx in "${match_idxs[@]}"; do CLAIMED[idx]=1; done
            case "${matches[0]}" in
                passed)  PASS+=("$entry") ;;
                failed)  REGRESS+=("$entry  (status=failed)") ;;
                skipped) REGRESS+=("$entry  (status=skipped — focus regex didn't include it)") ;;
                *)       REGRESS+=("$entry  (unknown status: ${matches[0]})") ;;
            esac
            ;;
        *)
            # Ambiguous match — entry is too loose. Even if all matches
            # passed, demand a tighter entry so the canary cannot silently
            # absorb a future sibling-test regression.
            for idx in "${match_idxs[@]}"; do CLAIMED[idx]=1; done
            REGRESS+=("$entry  (matched ${#matches[@]} testcases; tighten the entry)")
            ;;
    esac
done

# Orphan failures — any failed testcase not claimed by an entry above.
ORPHANS=()
for ((i = 0; i < ${#JUNIT_NAMES[@]}; i++)); do
    [[ "${JUNIT_STATUSES[i]}" == "failed" ]] || continue
    [[ "${CLAIMED[i]}" -eq 1 ]] && continue
    ORPHANS+=("${JUNIT_NAMES[i]}")
done

echo
info "=== summary ==="
info "expected green     : ${#ENTRIES[@]}"
info "actually green     : ${#PASS[@]}"
info "regressions        : ${#REGRESS[@]}"
info "orphan failures    : ${#ORPHANS[@]}"

if [[ ${#REGRESS[@]} -gt 0 || ${#ORPHANS[@]} -gt 0 ]]; then
    if [[ ${#REGRESS[@]} -gt 0 ]]; then
        echo
        info "REGRESSIONS:"
        printf '  - %s\n' "${REGRESS[@]}"
    fi
    if [[ ${#ORPHANS[@]} -gt 0 ]]; then
        echo
        info "ORPHAN FAILURES (focus regex pulled these in; tighten the entry that matched them):"
        printf '  - %s\n' "${ORPHANS[@]}"
    fi
    info "artifacts: $OUTPUT_DIR"
    exit 1
fi

info "all known-green tests passed (hydrophone exit=$HYDROPHONE_EXIT)"
exit 0
