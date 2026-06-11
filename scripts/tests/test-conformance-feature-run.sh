#!/usr/bin/env bash
# Unit tests for scripts/conformance-feature-run.sh — exercises the
# junit-count + gating exit-code logic via the sourceable
# `feature_decide` function. No cluster / no Hydrophone.
#
# Run with: bash scripts/tests/test-conformance-feature-run.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
RUNNER="$REPO_ROOT/scripts/conformance-feature-run.sh"

PASS_COUNT=0; FAIL_COUNT=0; FAILED_TESTS=()

assert_eq() {
    local expected="$1" actual="$2" label="${3:-assertion}"
    if [ "$expected" != "$actual" ]; then
        echo "  FAIL: $label (expected=$expected actual=$actual)"; exit 1
    fi
}

# Source the runner in test mode so it defines functions without running main.
FEATURE_RUN_LIB_ONLY=1 source "$RUNNER"

write_junit() { # $1 dir, $2 passed, $3 failed, $4 skipped
    local f="$1/junit_01.xml"; : > "$f"
    { echo '<testsuite>'
      for _ in $(seq 1 "$2"); do echo '<testcase name="t" status="passed"/>'; done
      for _ in $(seq 1 "$3"); do echo '<testcase name="t" status="failed"/>'; done
      for _ in $(seq 1 "$4"); do echo '<testcase name="t" status="skipped"/>'; done
      echo '</testsuite>'; } >> "$f"
}

# feature_decide <junit-dir> <gating 0|1> -> echoes "rc passed failed skipped"
test_reporting_with_failures_is_green() {
    local d; d=$(mktemp -d); write_junit "$d" 3 2 1
    assert_eq "0 3 2 1" "$(feature_decide "$d" 0)" "reporting + failures => rc 0"
    rm -rf "$d"
}
test_gating_with_failures_is_red() {
    local d; d=$(mktemp -d); write_junit "$d" 3 2 1
    assert_eq "1 3 2 1" "$(feature_decide "$d" 1)" "gating + failures => rc 1"
    rm -rf "$d"
}
test_gating_all_pass_is_green() {
    local d; d=$(mktemp -d); write_junit "$d" 5 0 2
    assert_eq "0 5 0 2" "$(feature_decide "$d" 1)" "gating + all pass => rc 0"
    rm -rf "$d"
}
test_gating_all_skip_is_green() {
    local d; d=$(mktemp -d); write_junit "$d" 0 0 7
    assert_eq "0 0 0 7" "$(feature_decide "$d" 1)" "gating + all skip => rc 0"
    rm -rf "$d"
}
test_no_junit_is_infra_red_both_modes() {
    local d; d=$(mktemp -d)
    assert_eq "1 0 0 0" "$(feature_decide "$d" 0)" "no junit reporting => rc 1"
    assert_eq "1 0 0 0" "$(feature_decide "$d" 1)" "no junit gating => rc 1"
    rm -rf "$d"
}

for t in $(declare -F | awk '{print $3}' | grep '^test_'); do
    if ( "$t" ); then PASS_COUNT=$((PASS_COUNT+1)); echo "ok - $t"
    else FAIL_COUNT=$((FAIL_COUNT+1)); FAILED_TESTS+=("$t"); echo "not ok - $t"; fi
done
echo "----- $PASS_COUNT passed, $FAIL_COUNT failed -----"
[ "$FAIL_COUNT" -eq 0 ] || { printf 'FAILED: %s\n' "${FAILED_TESTS[@]}"; exit 1; }
