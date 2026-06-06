#!/usr/bin/env bash
# Run the upstream Kubernetes [Conformance] suite against an already-running
# rusternetes cluster via Hydrophone, in the canonical two-phase split, and
# report pass/fail/skip counts per phase.
#
# This REPLACES the known-green.txt ratchet for the conformance canary. It is
# DISCOVERY / REPORTING, not a gate: a failing conformance test does NOT fail
# the job (rusternetes is not yet 100% conformant, so a raw tag run is red by
# construction). The job only fails on an infrastructure error — hydrophone
# producing no junit at all. To see the regression delta, compare the printed
# counts (and the uploaded junit) across runs.
#
# Two phases, matching the standard Sonobuoy / kube conformance recipe:
#
#   phase 1 (parallel) : focus [Conformance], skip [Slow] [Serial] [Flaky].
#                        Safe to run with multiple ginkgo threads.
#   phase 2 (serial)   : focus [Conformance] tests that are also [Serial] or
#                        [Slow], skip [Flaky]. Single ginkgo thread — serial
#                        tests mutate cluster-wide state and corrupt each
#                        other (and the parallel specs) if run concurrently.
#
# It does NOT bring up a cluster — callers (the conformance-canary workflow,
# or `docker compose up -d` + `bootstrap-cluster.sh` locally) own that.
#
# Usage:
#   bash scripts/conformance-tags-run.sh [flags]
#
# Flags:
#   --kubeconfig PATH     Override kubeconfig
#                         (default: $KUBECONFIG or ~/.kube/rusternetes-config)
#   --output-dir DIR      Output dir for hydrophone artifacts. Each phase gets
#                         a subdir (phase1-parallel/, phase2-serial/).
#                         (default: .rusternetes/volumes/conformance-tags-<ts>)
#   --conformance-image IMG
#                         Override conformance image. Defaults to
#                         registry.k8s.io/conformance:v1.35.0 (k8s target pin).
#   --parallel N          Ginkgo threads for phase 1 (default: 2). Phase 2 is
#                         always single-threaded.
#   --phase 1|2|both      Which phase(s) to run (default: both).
#   --hydrophone PATH     Override hydrophone binary path (default: discover via $PATH).
#   -h, --help            Show this help.
#
# Exit codes:
#   0  both requested phases ran and produced junit (regardless of pass/fail)
#   1  a phase produced no junit (infrastructure failure)
#   2  usage / preflight error (no kubeconfig, missing hydrophone, etc.)

set -euo pipefail
IFS=$'\n\t'

# Enable payload dumps in api-server/kubelet so any panic / 5xx / decode
# failure during conformance logs the offending request body.
export RUSTERNETES_DUMP_PAYLOADS=1

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"

KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
DEFAULT_IMAGE="registry.k8s.io/conformance:v1.35.0"
CONFORMANCE_IMAGE="$DEFAULT_IMAGE"
OUTPUT_DIR=""
HYDROPHONE_BIN=""
PARALLEL=2
PHASE="both"

# Tag regexes. Hydrophone passes --focus/--skip straight to Ginkgo (Go
# regexp/syntax), so the literal brackets are escaped.
PHASE1_FOCUS='\[Conformance\]'
PHASE1_SKIP='\[Slow\]|\[Serial\]|\[Flaky\]'
# "Conformance AND (Serial OR Slow)" — focus is a single regex, so both tag
# orders are enumerated for each combination.
PHASE2_FOCUS='\[Conformance\].*\[Serial\]|\[Serial\].*\[Conformance\]|\[Conformance\].*\[Slow\]|\[Slow\].*\[Conformance\]'
PHASE2_SKIP='\[Flaky\]'

die() {
    echo "[${SCRIPT_NAME}] ERROR: $*" >&2
    exit 2
}

info() {
    echo "[${SCRIPT_NAME}] $*"
}

usage() {
    sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -60
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage; exit 0 ;;
        --kubeconfig)
            [[ $# -ge 2 ]] || die "--kubeconfig requires a value"
            KUBECONFIG_PATH="$2"; shift 2 ;;
        --output-dir)
            [[ $# -ge 2 ]] || die "--output-dir requires a value"
            OUTPUT_DIR="$2"; shift 2 ;;
        --conformance-image)
            [[ $# -ge 2 ]] || die "--conformance-image requires a value"
            CONFORMANCE_IMAGE="$2"; shift 2 ;;
        --parallel)
            [[ $# -ge 2 ]] || die "--parallel requires a value"
            PARALLEL="$2"; shift 2 ;;
        --phase)
            [[ $# -ge 2 ]] || die "--phase requires a value"
            PHASE="$2"; shift 2 ;;
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

case "$PHASE" in
    1|2|both) ;;
    *) die "--phase must be 1, 2, or both (got: $PHASE)" ;;
esac
[[ "$PARALLEL" =~ ^[1-9][0-9]*$ ]] || die "--parallel must be a positive integer (got: $PARALLEL)"

# ---------- preflight ----------

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
    OUTPUT_DIR="$REPO_ROOT/.rusternetes/volumes/conformance-tags-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$OUTPUT_DIR"

info "conformance image : $CONFORMANCE_IMAGE"
info "kubeconfig        : $KUBECONFIG_PATH"
info "output dir        : $OUTPUT_DIR"
info "hydrophone        : $HYDROPHONE_BIN"
info "phase(s)          : $PHASE"

# ---------- per-phase runner ----------
#
# Runs one hydrophone phase into its own subdir, prints pass/fail/skip
# counts parsed from the junit, and records whether a junit was produced.
# Sets PHASE_HAD_JUNIT=0/1 in the caller's scope (via a global) so the
# top-level exit code can distinguish "tests failed" (fine) from "no junit"
# (infra failure).

NO_JUNIT_PHASES=()

run_phase() {
    local label="$1" subdir="$2" focus="$3" skip="$4" threads="$5"
    local dir="$OUTPUT_DIR/$subdir"
    mkdir -p "$dir"

    echo
    info "================================================================"
    info "phase: $label"
    info "  focus    : $focus"
    info "  skip     : $skip"
    info "  parallel : $threads"
    info "================================================================"

    set +e
    "$HYDROPHONE_BIN" \
        --focus "$focus" \
        --skip "$skip" \
        --parallel "$threads" \
        --output-dir "$dir" \
        --kubeconfig "$KUBECONFIG_PATH" \
        --conformance-image "$CONFORMANCE_IMAGE" 2>&1 | tee "$dir/run.log"
    local hydro_exit=${PIPESTATUS[0]}
    set -e

    local junit="$dir/junit_01.xml"
    if [[ ! -f "$junit" ]]; then
        info "[$label] hydrophone exit=$hydro_exit but NO junit produced — infra failure"
        NO_JUNIT_PHASES+=("$label")
        return
    fi

    # Count testcase statuses. Junit @status is passed|failed|skipped.
    local passed failed skipped
    passed=$(grep -oE 'status="passed"' "$junit" | wc -l | tr -d ' ')
    failed=$(grep -oE 'status="failed"' "$junit" | wc -l | tr -d ' ')
    skipped=$(grep -oE 'status="skipped"' "$junit" | wc -l | tr -d ' ')

    info "[$label] hydrophone exit=$hydro_exit  passed=$passed failed=$failed skipped=$skipped"

    if [[ "$failed" -gt 0 ]]; then
        info "[$label] FAILED tests:"
        grep -oE '<testcase name="[^"]+"[^>]*status="failed"' "$junit" \
            | sed -E 's|<testcase name="([^"]+)".*|  - \1|' \
            | sed -e 's/&#39;/'"'"'/g' -e 's/&amp;/\&/g' -e 's/&lt;/</g' \
                  -e 's/&gt;/>/g' -e 's/&quot;/"/g' -e 's/&#34;/"/g' || true
    fi
}

# ---------- run requested phases ----------

if [[ "$PHASE" == "1" || "$PHASE" == "both" ]]; then
    run_phase "1 (parallel)" "phase1-parallel" "$PHASE1_FOCUS" "$PHASE1_SKIP" "$PARALLEL"
fi
if [[ "$PHASE" == "2" || "$PHASE" == "both" ]]; then
    run_phase "2 (serial)" "phase2-serial" "$PHASE2_FOCUS" "$PHASE2_SKIP" "1"
fi

echo
info "=== done ==="
info "artifacts: $OUTPUT_DIR"

if [[ ${#NO_JUNIT_PHASES[@]} -gt 0 ]]; then
    info "infra failure — these phases produced no junit: ${NO_JUNIT_PHASES[*]}"
    exit 1
fi
exit 0
