#!/usr/bin/env bash
# Run the upstream Kubernetes conformance suite for ONE feature against an
# already-running rusternetes cluster via Hydrophone, parse the junit, and
# set the exit code per the feature's gating mode.
#
# Caller owns cluster bring-up (the feature-conformance workflow, or a local
# `docker compose up -d` + bootstrap-cluster.sh). This script does NOT bring
# up a cluster — same contract as scripts/conformance-tags-run.sh.
#
# Exit code:
#   reporting (gating=false): 0 unless NO junit was produced (infra fail => 1)
#   gating   (gating=true) : 1 if any spec FAILED; else 0. All-skip => 0
#                            (a feature-gate-off image must not false-red a
#                            graduated feature). No junit => 1 (infra fail).
#
# Usage:
#   bash scripts/conformance-feature-run.sh --name sysctls \
#       --focus '\[(Feature|NodeFeature):Sysctls\]' --skip '\[Flaky\]' \
#       --gating false [--kubeconfig P] [--conformance-image IMG]
#       [--output-dir DIR] [--hydrophone PATH]
#
# Flags: --name --focus --skip --gating(true|false) --kubeconfig
#        --conformance-image --output-dir --hydrophone -h|--help
set -euo pipefail
IFS=$'\n\t'

# Enable payload dumps in api-server/kubelet so any panic / 5xx / decode
# failure during the run logs the offending request body.
export RUSTERNETES_DUMP_PAYLOADS=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"

# Count testcase statuses in <dir>/junit_01.xml and apply the gating rule.
# Echoes "rc passed failed skipped". rc is the exit code the script should use.
feature_decide() {
    local dir="$1" gating="$2"
    local junit="$dir/junit_01.xml"
    if [ ! -f "$junit" ]; then
        echo "1 0 0 0"; return
    fi
    local passed failed skipped
    passed=$(grep -oE 'status="passed"' "$junit" | wc -l | tr -d ' ')
    failed=$(grep -oE 'status="failed"' "$junit" | wc -l | tr -d ' ')
    skipped=$(grep -oE 'status="skipped"' "$junit" | wc -l | tr -d ' ')
    local rc=0
    if [ "$gating" -eq 1 ] && [ "$failed" -gt 0 ]; then
        rc=1
    fi
    echo "$rc $passed $failed $skipped"
}

# When sourced by the unit test, stop here — don't parse args or run.
if [ -n "${FEATURE_RUN_LIB_ONLY:-}" ]; then
    return 0 2>/dev/null || true
fi

# ---------- arg parsing ----------
NAME=""; FOCUS=""; SKIP='\[Flaky\]'; GATING="false"
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
CONFORMANCE_IMAGE="registry.k8s.io/conformance:v1.35.0"
OUTPUT_DIR=""; HYDROPHONE_BIN=""

die() { echo "[conformance-feature-run] ERROR: $*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -40; exit 0 ;;
        --name) [[ $# -ge 2 ]] || die "--name requires a value"; NAME="$2"; shift 2 ;;
        --focus) [[ $# -ge 2 ]] || die "--focus requires a value"; FOCUS="$2"; shift 2 ;;
        --skip) [[ $# -ge 2 ]] || die "--skip requires a value"; SKIP="$2"; shift 2 ;;
        --gating) [[ $# -ge 2 ]] || die "--gating requires a value"; GATING="$2"; shift 2 ;;
        --kubeconfig) [[ $# -ge 2 ]] || die "--kubeconfig requires a value"; KUBECONFIG_PATH="$2"; shift 2 ;;
        --conformance-image) [[ $# -ge 2 ]] || die "--conformance-image requires a value"; CONFORMANCE_IMAGE="$2"; shift 2 ;;
        --output-dir) [[ $# -ge 2 ]] || die "--output-dir requires a value"; OUTPUT_DIR="$2"; shift 2 ;;
        --hydrophone) [[ $# -ge 2 ]] || die "--hydrophone requires a value"; HYDROPHONE_BIN="$2"; shift 2 ;;
        *) die "unknown flag: $1 (use --help)" ;;
    esac
done

[ -n "$NAME" ]  || die "--name required"
[ -n "$FOCUS" ] || die "--focus required"
case "$GATING" in true) GATING_INT=1 ;; false) GATING_INT=0 ;; *) die "--gating must be true|false" ;; esac
[ -f "$KUBECONFIG_PATH" ] || die "kubeconfig not found: $KUBECONFIG_PATH"

if [ -z "$HYDROPHONE_BIN" ]; then
    command -v hydrophone >/dev/null 2>&1 || die "hydrophone not on PATH; pass --hydrophone"
    HYDROPHONE_BIN="$(command -v hydrophone)"
fi
[ -x "$HYDROPHONE_BIN" ] || die "hydrophone not executable: $HYDROPHONE_BIN"

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="$REPO_ROOT/.rusternetes/volumes/feature-$NAME"
fi
mkdir -p "$OUTPUT_DIR"

echo "[conformance-feature-run] feature=$NAME gating=$GATING"
echo "  focus : $FOCUS"
echo "  skip  : $SKIP"
echo "  image : $CONFORMANCE_IMAGE"

# Per-feature isolation already gives this feature its own cluster, so a
# single ginkgo thread is safe and handles any [Serial] specs in the focus.
set +e
"$HYDROPHONE_BIN" \
    --focus "$FOCUS" \
    --skip "$SKIP" \
    --parallel 1 \
    --output-dir "$OUTPUT_DIR" \
    --kubeconfig "$KUBECONFIG_PATH" \
    --conformance-image "$CONFORMANCE_IMAGE" 2>&1 | tee "$OUTPUT_DIR/run.log"
hydro_exit=${PIPESTATUS[0]}
set -e

read -r RC PASSED FAILED SKIPPED <<<"$(feature_decide "$OUTPUT_DIR" "$GATING_INT")"
echo "[conformance-feature-run] feature=$NAME hydrophone_exit=$hydro_exit passed=$PASSED failed=$FAILED skipped=$SKIPPED gating=$GATING rc=$RC"
if [ ! -f "$OUTPUT_DIR/junit_01.xml" ]; then
    echo "[conformance-feature-run] NO junit produced — infra failure"
fi
exit "$RC"
