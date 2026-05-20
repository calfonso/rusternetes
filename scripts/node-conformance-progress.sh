#!/usr/bin/env bash
# Tail the in-flight node-conformance ginkgo log and print running counters.
#
# Usage:
#   scripts/run-node-conformance.sh        # in terminal A
#   scripts/node-conformance-progress.sh   # in terminal B (this script)
#
# Override the log path with $1 if needed. Sibling of scripts/conformance-progress.sh.
set -euo pipefail

LOG="${1:-/tmp/node-conformance/ginkgo.log}"
if [ ! -f "${LOG}" ]; then
    echo "No log at ${LOG}. Start scripts/run-node-conformance.sh in another shell."
    exit 1
fi

echo "Tailing ${LOG}. Ctrl-C to stop."
PASS=0
FAIL=0
SKIP=0
tail -n +1 -f "${LOG}" | while IFS= read -r line; do
    case "${line}" in
        *"[PASSED]"*)  PASS=$((PASS + 1)) ;;
        *"[FAILED]"*)  FAIL=$((FAIL + 1)) ;;
        *"[SKIPPED]"*) SKIP=$((SKIP + 1)) ;;
    esac
    printf "\rPASS=%d FAIL=%d SKIP=%d" "${PASS}" "${FAIL}" "${SKIP}"
done
