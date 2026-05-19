#!/usr/bin/env bash
# Test harness for scripts/conformance-coordinator.sh
#
# Each test is a function whose name starts with `test_`. The runner
# discovers them, creates an isolated TMPDIR (state file + fixture
# per-test directory live there), runs the test, and reports.
#
# Run with: bash scripts/tests/test-conformance-coordinator.sh

set -euo pipefail
IFS=$'\n\t'

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
COORDINATOR="$REPO_ROOT/scripts/conformance-coordinator.sh"

PASS_COUNT=0
FAIL_COUNT=0
FAILED_TESTS=()

# ----- Assertion helpers -----

assert_eq() {
    local expected="$1"
    local actual="$2"
    local label="${3:-assertion}"
    if [ "$expected" != "$actual" ]; then
        echo "  FAIL: $label"
        echo "    expected: $(printf '%q' "$expected")"
        echo "    actual:   $(printf '%q' "$actual")"
        # `exit` (not `return`) so a failed assertion inside a test
        # subshell kills that subshell and surfaces to the runner.
        # `return 1` would only be swallowed by the next statement.
        exit 1
    fi
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local label="${3:-contains}"
    case "$haystack" in
        *"$needle"*) ;;
        *)
            echo "  FAIL: $label"
            echo "    expected substring: $(printf '%q' "$needle")"
            echo "    actual:             $(printf '%q' "$haystack")"
            exit 1
            ;;
    esac
}

# ----- Fixture helpers -----

make_per_test_dir() {
    local dir="$1"
    mkdir -p "$dir"
    cat > "$dir/sig-network_Services_should_serve_basic_endpoint.txt" <<EOF
# [sig-network] Services should serve a basic endpoint from pods [Conformance]

Status:    failed
Classname: Kubernetes e2e suite
Sig area:  sig-network

## Failure message
endpoint unreachable
EOF
    cat > "$dir/sig-node_Pods_should_support_exec_websocket.txt" <<EOF
# [sig-node] Pods should support remote command execution over websockets [Conformance]

Status:    failed
Classname: Kubernetes e2e suite
Sig area:  sig-node

## Failure message
binary subprotocol not negotiated
EOF
    cat > "$dir/sig-api-machinery_CRD_lifecycle.txt" <<EOF
# [sig-api-machinery] CRD lifecycle [Conformance]

Status:    failed
Classname: Kubernetes e2e suite
Sig area:  sig-api-machinery

## Failure message
schema mismatch
EOF
}

run_coordinator() {
    # Always pass --state and --per-test-dir explicitly so tests don't
    # depend on auto-discovery against the real repo state.
    bash "$COORDINATOR" --state "$STATE" --per-test-dir "$PERTEST" "$@"
}

# ----- Tests -----

test_init_empty_dir_produces_empty_state() {
    mkdir -p "$PERTEST"
    run_coordinator init >/dev/null
    [ -f "$STATE" ] || { echo "  FAIL: state file not created"; return 1; }
    local count
    count=$(jq -r '.tests | length' "$STATE")
    assert_eq "0" "$count" "tests count"
}

test_init_populates_state_from_per_test_files() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local count
    count=$(jq -r '.tests | length' "$STATE")
    assert_eq "3" "$count" "tests count"
    local status
    status=$(jq -r '.tests["sig-node_Pods_should_support_exec_websocket"].status' "$STATE")
    assert_eq "fail" "$status" "default status"
}

test_init_preserves_existing_state() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    # Mutate one entry to claimed; re-run init; mutation must persist.
    jq '.tests["sig-network_Services_should_serve_basic_endpoint"].status = "claimed"' \
        "$STATE" > "$STATE.tmp" && mv "$STATE.tmp" "$STATE"
    run_coordinator init >/dev/null
    local status
    status=$(jq -r '.tests["sig-network_Services_should_serve_basic_endpoint"].status' "$STATE")
    assert_eq "claimed" "$status" "existing entry preserved"
}

test_next_returns_unclaimed_fail() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local out
    out=$(run_coordinator next)
    # next prints the safe name (first line) and the per-test file (second line)
    local name
    name=$(printf '%s\n' "$out" | head -n1)
    [ -n "$name" ] || { echo "  FAIL: next returned empty"; return 1; }
    # Picked entry must now be claimed.
    local status
    status=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "claimed" "$status" "next marks claimed"
}

test_next_skips_already_claimed() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local first second
    first=$(run_coordinator next | head -n1)
    second=$(run_coordinator next | head -n1)
    [ "$first" != "$second" ] || {
        echo "  FAIL: next returned the same test twice: $first"
        return 1
    }
}

test_next_exits_nonzero_when_no_unclaimed_left() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    run_coordinator next >/dev/null
    run_coordinator next >/dev/null
    run_coordinator next >/dev/null
    if run_coordinator next >/dev/null 2>&1; then
        echo "  FAIL: next succeeded when no unclaimed tests remained"
        return 1
    fi
}

test_status_summary_counts() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    run_coordinator next >/dev/null
    local out
    out=$(run_coordinator status)
    assert_contains "$out" "fail: 2" "fail count"
    assert_contains "$out" "claimed: 1" "claimed count"
}

test_release_unclaims_test() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    run_coordinator release "$name" >/dev/null
    local status
    status=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "fail" "$status" "release flips to fail"
}

test_claim_records_pr_url() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    run_coordinator claim "$name" --pr-url "https://github.com/x/y/pull/42" >/dev/null
    local pr_url status
    pr_url=$(jq -r --arg n "$name" '.tests[$n].pr_url' "$STATE")
    status=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "https://github.com/x/y/pull/42" "$pr_url" "pr_url recorded"
    assert_eq "pr_open" "$status" "status -> pr_open"
}

test_mark_done_flips_to_verified() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    run_coordinator mark-done "$name" >/dev/null
    local status
    status=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "verified" "$status" "mark-done -> verified"
}

# Mock `gh` by prepending a stub-script directory to PATH for the
# duration of one test. The stub reads its expected response from a file
# we control, so each test can dictate gh's behavior.
with_gh_stub() {
    local response="$1"
    local exit_code="${2:-0}"
    local stubdir
    stubdir=$(mktemp -d)
    cat > "$stubdir/gh" <<EOF
#!/usr/bin/env bash
echo '$response'
exit $exit_code
EOF
    chmod +x "$stubdir/gh"
    GH_STUB_DIR="$stubdir"
    PATH="$stubdir:$PATH"
    export PATH
}

cleanup_gh_stub() {
    [ -n "${GH_STUB_DIR:-}" ] || return 0
    rm -rf "$GH_STUB_DIR"
    GH_STUB_DIR=""
}

test_update_promotes_pr_open_to_pr_merged_when_gh_reports_merged() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    run_coordinator claim "$name" --pr-url "https://github.com/x/y/pull/42" >/dev/null
    with_gh_stub '{"state":"MERGED","mergedAt":"2026-05-19T10:00:00Z"}'
    run_coordinator update >/dev/null
    cleanup_gh_stub
    local status
    status=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "pr_merged" "$status" "update advances merged PR"
}

test_update_leaves_pr_open_when_gh_reports_open() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    run_coordinator claim "$name" --pr-url "https://github.com/x/y/pull/42" >/dev/null
    with_gh_stub '{"state":"OPEN","mergedAt":null}'
    run_coordinator update >/dev/null
    cleanup_gh_stub
    local status
    status=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "pr_open" "$status" "open PR unchanged"
}

test_update_releases_test_when_gh_reports_closed_unmerged() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    run_coordinator claim "$name" --pr-url "https://github.com/x/y/pull/42" >/dev/null
    with_gh_stub '{"state":"CLOSED","mergedAt":null}'
    run_coordinator update >/dev/null
    cleanup_gh_stub
    local status pr_url
    status=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    pr_url=$(jq -r --arg n "$name" '.tests[$n].pr_url' "$STATE")
    assert_eq "fail" "$status" "closed-unmerged returns to fail"
    assert_eq "null" "$pr_url" "pr_url cleared on close"
}

# Create a stub single-test-runner script that records its CLI args to
# $STUB_LOG and exits with the configured code. The caller can read
# $STUB_LOG afterward to assert what arguments the coordinator passed.
with_single_test_stub() {
    local exit_code="$1"
    local stubdir
    stubdir=$(mktemp -d)
    STUB_LOG="$stubdir/args.log"
    cat > "$stubdir/single-test-stub.sh" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$@" > "$STUB_LOG"
exit $exit_code
EOF
    chmod +x "$stubdir/single-test-stub.sh"
    SINGLE_TEST_STUB="$stubdir/single-test-stub.sh"
    SINGLE_TEST_STUB_DIR="$stubdir"
}

cleanup_single_test_stub() {
    [ -n "${SINGLE_TEST_STUB_DIR:-}" ] || return 0
    rm -rf "$SINGLE_TEST_STUB_DIR"
    SINGLE_TEST_STUB_DIR=""
}

test_verify_marks_verified_when_runner_exits_zero() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    run_coordinator claim "$name" --pr-url "https://github.com/x/y/pull/42" >/dev/null
    with_single_test_stub 0
    run_coordinator verify "$name" --single-test-script "$SINGLE_TEST_STUB" >/dev/null
    local status
    status=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "verified" "$status" "verify -> verified on runner exit 0"
    cleanup_single_test_stub
}

test_verify_passes_upstream_name_to_runner() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    local upstream
    upstream=$(jq -r --arg n "$name" '.tests[$n].upstream_name' "$STATE")
    with_single_test_stub 0
    run_coordinator verify "$name" --single-test-script "$SINGLE_TEST_STUB" >/dev/null
    local first_arg
    first_arg=$(head -n1 "$STUB_LOG")
    assert_eq "$upstream" "$first_arg" "runner invoked with upstream name"
    cleanup_single_test_stub
}

test_verify_leaves_state_alone_when_runner_fails() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    run_coordinator claim "$name" --pr-url "https://github.com/x/y/pull/42" >/dev/null
    local before
    before=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "pr_open" "$before" "precondition pr_open"

    with_single_test_stub 1
    if run_coordinator verify "$name" --single-test-script "$SINGLE_TEST_STUB" >/dev/null 2>&1; then
        echo "  FAIL: verify exited 0 when runner failed"
        cleanup_single_test_stub
        return 1
    fi
    cleanup_single_test_stub

    local after
    after=$(jq -r --arg n "$name" '.tests[$n].status' "$STATE")
    assert_eq "pr_open" "$after" "status unchanged on runner failure"
}

test_verify_errors_when_test_unknown() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    with_single_test_stub 0
    if run_coordinator verify "nonexistent-test-name" \
        --single-test-script "$SINGLE_TEST_STUB" >/dev/null 2>&1; then
        echo "  FAIL: verify accepted unknown test name"
        cleanup_single_test_stub
        return 1
    fi
    # Runner must NOT have been invoked.
    [ ! -f "${STUB_LOG:-/dev/null}" ] || {
        echo "  FAIL: runner was invoked for unknown test"
        cleanup_single_test_stub
        return 1
    }
    cleanup_single_test_stub
}

test_verify_errors_when_no_name_given() {
    with_single_test_stub 0
    if run_coordinator verify --single-test-script "$SINGLE_TEST_STUB" >/dev/null 2>&1; then
        echo "  FAIL: verify with no NAME succeeded"
        cleanup_single_test_stub
        return 1
    fi
    cleanup_single_test_stub
}

test_verify_records_verified_at_timestamp() {
    make_per_test_dir "$PERTEST"
    run_coordinator init >/dev/null
    local name
    name=$(run_coordinator next | head -n1)
    with_single_test_stub 0
    run_coordinator verify "$name" --single-test-script "$SINGLE_TEST_STUB" >/dev/null
    cleanup_single_test_stub
    local verified_at
    verified_at=$(jq -r --arg n "$name" '.tests[$n].verified_at // empty' "$STATE")
    [ -n "$verified_at" ] || {
        echo "  FAIL: verified_at not recorded"
        return 1
    }
}

# ----- Runner -----

run_all_tests() {
    local tests
    mapfile -t tests < <(declare -F | awk '$3 ~ /^test_/ {print $3}' | sort)
    for t in "${tests[@]}"; do
        # Fresh tmpdir per test.
        local tmpdir
        tmpdir=$(mktemp -d)
        STATE="$tmpdir/state.json"
        PERTEST="$tmpdir/per-test"
        echo "RUN  $t"
        # Run each test in its own subshell so that `exit 1` from a
        # failed assertion kills only that test, not the runner. The
        # subshell's exit code is captured into $rc separately from any
        # surrounding `if`/`||` context, which keeps bash's errexit
        # semantics intact inside the subshell.
        local rc=0
        ( "$t" ) || rc=$?
        if [ "$rc" -eq 0 ]; then
            echo "PASS $t"
            PASS_COUNT=$((PASS_COUNT + 1))
        else
            echo "FAIL $t"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            FAILED_TESTS+=("$t")
        fi
        rm -rf "$tmpdir"
    done
    echo
    echo "Passed: $PASS_COUNT"
    echo "Failed: $FAIL_COUNT"
    if [ "$FAIL_COUNT" -gt 0 ]; then
        echo "Failures:"
        for t in "${FAILED_TESTS[@]}"; do
            echo "  - $t"
        done
        exit 1
    fi
}

run_all_tests
