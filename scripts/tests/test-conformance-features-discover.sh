#!/usr/bin/env bash
# Unit test for scripts/conformance-features-discover.sh — feeds a captured
# ginkgo dry-run via --from-file and asserts the emitted manifest skeleton.
# No cluster / no image pull.
#
# Run with: bash scripts/tests/test-conformance-features-discover.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
DISCOVER="$REPO_ROOT/scripts/conformance-features-discover.sh"
FIXTURE="$REPO_ROOT/scripts/tests/fixtures/dry-run-sample.txt"

fail() { echo "FAIL: $*" >&2; exit 1; }

out=$(bash "$DISCOVER" --from-file "$FIXTURE")
echo "$out" | jq -e . >/dev/null 2>&1 || fail "output is not valid JSON: $out"

# Five distinct features discovered (Sysctls, ProbeTerminationGracePeriod,
# PerformanceDNS, DownwardAPI, SCTPConnectivity); the [Conformance]-only line
# is skipped.
n=$(echo "$out" | jq length)
[ "$n" -eq 5 ] || fail "expected 5 features, got $n: $out"

# Sysctls mapped to sig-node, gating defaults false, focus is the exact regex.
echo "$out" | jq -e '.[] | select(.name=="sysctls")
    | .sig=="sig-node" and .gating==false' >/dev/null \
    || fail "sysctls entry malformed: $out"
echo "$out" | jq -e '.[] | select(.name=="sysctls") | .focus == "\\[(Feature|NodeFeature):Sysctls\\]"' >/dev/null \
    || fail "sysctls focus malformed: $out"

# PerformanceDNS mapped to sig-network.
echo "$out" | jq -e '.[] | select(.name=="performance-dns") | .sig=="sig-network"' >/dev/null \
    || fail "performance-dns not mapped to sig-network: $out"

# Acronym-run slug: SCTPConnectivity -> sctp-connectivity, mapped to sig-network.
echo "$out" | jq -e '.[] | select(.name=="sctp-connectivity") | .sig=="sig-network"' >/dev/null \
    || fail "sctp-connectivity not mapped to sig-network: $out"

# --diff keeps stdout = JSON only; the diff goes to stderr.
diff_stdout=$(bash "$DISCOVER" --from-file "$FIXTURE" --diff 2>/dev/null)
echo "$diff_stdout" | jq -e . >/dev/null 2>&1 \
    || fail "--diff polluted stdout (not valid JSON): $diff_stdout"

echo "PASS: discovery emits expected skeleton"
