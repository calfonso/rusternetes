#!/bin/bash
# Conformance test progress monitor for Rusternetes
# Parses e2e container logs to show real-time pass/fail counts
# since sonobuoy's built-in progress reporting doesn't work with K8s v1.35
#
# Usage: bash scripts/conformance-progress.sh [interval_seconds]

INTERVAL="${1:-10}"

# Detect container runtime
# Override: CONTAINER_RUNTIME=docker or CONTAINER_RUNTIME=podman
if [ -n "$CONTAINER_RUNTIME" ]; then
    CRT="$CONTAINER_RUNTIME"
else
    HAS_PODMAN=false
    HAS_DOCKER=false
    # Use background + wait to timeout commands that may hang (e.g. docker ps when Docker Desktop is stopped)
    if command -v podman &>/dev/null; then
        podman ps &>/dev/null 2>&1 & PID=$!; ( sleep 3; kill $PID 2>/dev/null ) &>/dev/null & wait $PID 2>/dev/null && HAS_PODMAN=true
    fi
    if command -v docker &>/dev/null; then
        docker ps &>/dev/null 2>&1 & PID=$!; ( sleep 3; kill $PID 2>/dev/null ) &>/dev/null & wait $PID 2>/dev/null && HAS_DOCKER=true
    fi

    if $HAS_PODMAN && $HAS_DOCKER; then
        echo "ERROR: Both docker and podman are available. Set CONTAINER_RUNTIME=docker or CONTAINER_RUNTIME=podman"
        exit 1
    elif $HAS_PODMAN; then
        CRT=podman
    elif $HAS_DOCKER; then
        CRT=docker
    else
        echo "ERROR: No container runtime found"
        exit 1
    fi
fi

API="https://localhost:6443"

# Detect which conformance harness is running and where its ginkgo log lives.
# Prints "harness namespace pod container" or nothing if no run is found.
#   * Hydrophone (cluster-up.sh --conformance hydrophone): namespace
#     `conformance`, pod `e2e-conformance-test`, ginkgo streams to the
#     `conformance-container` stdout.
#   * Sonobuoy (run-conformance.sh): namespace `sonobuoy`, pod `e2e-job-*`,
#     ginkgo writes to a FILE inside the `e2e` container.
detect_harness() {
    # Hydrophone first — it's the default for local/e2e runs.
    if curl -sk "$API/api/v1/namespaces/conformance/pods/e2e-conformance-test" 2>/dev/null \
            | grep -q '"phase"'; then
        echo "hydrophone conformance e2e-conformance-test conformance-container"
        return
    fi
    # Sonobuoy fallback.
    local pod
    pod=$(curl -sk "$API/api/v1/namespaces/sonobuoy/pods" 2>/dev/null | python3 -c "
import sys,json
try:
    for p in json.load(sys.stdin).get('items',[]):
        if 'e2e-job' in p['metadata']['name']:
            print(p['metadata']['name']); break
except: pass
" 2>/dev/null)
    [ -n "$pod" ] && echo "sonobuoy sonobuoy $pod e2e"
}

# Return the raw ginkgo progress text for the detected harness.
get_progress_text() {
    local harness=$1 ns=$2 pod=$3 container=$4
    if [ "$harness" = "sonobuoy" ]; then
        # Sonobuoy's ginkgo writes to a file, not stdout — read it from the
        # container directly, falling back to the API log endpoint.
        local c
        c=$($CRT ps --format "{{.Names}}" | grep "e2e-job.*_e2e$" | head -1)
        if [ -n "$c" ]; then
            $CRT exec "$c" cat /tmp/sonobuoy/results/e2e.log 2>/dev/null
            return
        fi
    fi
    # Hydrophone (and the sonobuoy API fallback) stream ginkgo to stdout.
    curl -sk "$API/api/v1/namespaces/$ns/pods/$pod/log?container=$container" 2>/dev/null
}

parse_progress() {
    python3 -c "
import sys
text = sys.stdin.read()
lines = text.split('\n')
passed = 0
failed = 0
completed = False
last_test = ''
for line in lines:
    stripped = line.strip()
    # Count • on progress lines (SSSS•SSS) as passes
    if '\u2022' in stripped:
        if '[FAILED]' in stripped:
            failed += stripped.count('\u2022')
        else:
            passed += stripped.count('\u2022')
    if stripped.startswith('[sig-') or stripped.startswith('[k8s.io'):
        last_test = stripped[:120]
    if stripped.startswith('Ran '):
        completed = True

total = 441
done = passed + failed
remaining = max(0, total - done)
pct = f'{passed * 100 / done:.1f}' if done > 0 else '0.0'
print(f'{passed}|{failed}|{remaining}|{total}|{pct}|{1 if completed else 0}|{last_test}')
"
}

echo "=== Rusternetes Conformance Progress Monitor ==="
echo "Polling every ${INTERVAL}s (pass interval as arg to change)"
echo ""

while true; do
    read -r HARNESS NS E2E_POD CONTAINER <<< "$(detect_harness)"
    if [ -z "$E2E_POD" ]; then
        echo "$(date +%H:%M:%S) | No e2e pod found (sonobuoy or hydrophone). Waiting..."
        sleep "$INTERVAL"
        continue
    fi

    RESULT=$(get_progress_text "$HARNESS" "$NS" "$E2E_POD" "$CONTAINER" | parse_progress)

    if [ -z "$RESULT" ]; then
        echo "$(date +%H:%M:%S) | No logs yet. Waiting..."
        sleep "$INTERVAL"
        continue
    fi

    IFS='|' read -r PASSED FAILED REMAINING TOTAL PASS_RATE IS_COMPLETE LAST_TEST <<< "$RESULT"
    DONE=$((PASSED + FAILED))

    echo "$(date +%H:%M:%S) | Passed: ${PASSED} | Failed: ${FAILED} | Done: ${DONE}/${TOTAL} | Remaining: ${REMAINING} | Pass rate: ${PASS_RATE}%"

    if [ "$IS_COMPLETE" = "1" ]; then
        echo ""
        echo "=== Suite Complete ==="
        echo "Final: Passed=${PASSED} Failed=${FAILED} Total=${DONE}"
        break
    fi

    sleep "$INTERVAL"
done
