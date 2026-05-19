#!/usr/bin/env bash
# All single-quoted strings passed to jq are *jq* filter source, not shell.
# shellcheck disable=SC2016
#
# conformance-coordinator.sh
#
# Persistent-state coordinator over per-test conformance failures. Reads
# the .txt files produced by `conformance-split-junit.sh` and maintains a
# JSON state file that tracks which test is being worked on by which agent
# and what PR (if any) has landed for it.
#
# The driver session (a Claude Code worker spawner, or a human at the
# keyboard) calls subcommands here to:
#   1. Discover and ingest per-test failures (`init`)
#   2. Claim the next available failure to attack (`next`)
#   3. Record a PR URL once the worker opens one (`claim ... --pr-url X`)
#   4. Refresh PR statuses from GitHub (`update`)
#   5. Mark a test fully resolved after the shadow check passes (`mark-done`)
#   6. Unclaim if a worker abandons (`release`)
#   7. Print summary (`status`)
#
# This script does NOT call the `Agent` tool itself — the driver loop
# does that. The coordinator is pure state + lookup, so it is fully
# testable with bash + jq.

set -euo pipefail
IFS=$'\n\t'

SCRIPT_NAME="$(basename "$0")"
STATE=""
PERTEST=""

usage() {
    cat <<EOF
Usage: $SCRIPT_NAME [--state PATH] [--per-test-dir DIR] <subcommand> [args]

Subcommands:
  init                      Scan per-test-dir for *.txt failures, populate state.
                            Preserves existing entries; only adds new ones.
  next                      Print "<safe_name>\\n<per_test_file>" for the next
                            unclaimed fail and flip its status to "claimed".
                            Exits 1 if no unclaimed fail remains.
  claim NAME --pr-url URL   Record a PR URL for NAME and flip status to "pr_open".
  release NAME              Flip NAME's status back to "fail".
  mark-done NAME            Flip NAME's status to "verified" (shadow check passed).
  verify NAME [flags]       Run scripts/conformance-single-test.sh against the
                            upstream test name from NAME's entry. Flips status
                            to "verified" iff the runner exits 0; otherwise
                            leaves state unchanged and exits non-zero.
                            Flags:
                              --single-test-script PATH  Override runner path
                                                         (default: adjacent
                                                         conformance-single-test.sh)
                              --output-dir DIR           Forwarded to the runner
  update                    For each pr_open entry, query GitHub via 'gh pr view'
                            and advance status: MERGED -> pr_merged, CLOSED -> fail
                            (with pr_url cleared), OPEN -> unchanged.
  status                    Print summary counts grouped by status.
  list [--status STATUS]    Print one safe_name per line, filtered by status.

Options:
  --state PATH              JSON state file path.
                            Default: .rusternetes/volumes/conformance-coordinator-state.json
  --per-test-dir DIR        Directory of per-test .txt files (output of
                            conformance-split-junit.sh).
                            Default: .rusternetes/volumes/conformance-per-test
  -h, --help                Show this help.

State schema:
  {
    "version": 1,
    "updated_at": "<ISO-8601>",
    "tests": {
      "<safe_name>": {
        "upstream_name": "<first markdown header from per-test file>",
        "status": "fail|claimed|pr_open|pr_merged|verified",
        "per_test_file": "<absolute path>",
        "pr_url": "<url or null>",
        "claimed_at": "<ISO-8601 or null>",
        "updated_at": "<ISO-8601>"
      }
    }
  }
EOF
}

# ----- Argument parsing -----

while [ $# -gt 0 ]; do
    case "$1" in
        --state)
            [ $# -ge 2 ] || { echo "error: --state requires a value" >&2; exit 2; }
            STATE="$2"; shift 2 ;;
        --per-test-dir)
            [ $# -ge 2 ] || { echo "error: --per-test-dir requires a value" >&2; exit 2; }
            PERTEST="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        --) shift; break ;;
        -*) echo "error: unknown flag: $1" >&2; usage >&2; exit 2 ;;
        *) break ;;
    esac
done

if [ $# -lt 1 ]; then
    usage >&2
    exit 2
fi

SUBCOMMAND="$1"; shift

# Defaults derived after flag parsing so explicit flags win.
: "${STATE:=.rusternetes/volumes/conformance-coordinator-state.json}"
: "${PERTEST:=.rusternetes/volumes/conformance-per-test}"

command -v jq >/dev/null 2>&1 || {
    echo "error: jq is required (sudo apt-get install jq)" >&2
    exit 1
}

now_iso() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }

# Atomically rewrite the state file via a jq filter applied to the
# current contents. Reads STATE, pipes through jq with the given args,
# writes to a tmp file, then renames.
update_state() {
    local filter="$1"; shift
    local tmp
    tmp=$(mktemp "${STATE}.XXXXXX")
    jq "$@" "$filter" "$STATE" > "$tmp"
    mv "$tmp" "$STATE"
}

ensure_state_exists() {
    if [ ! -f "$STATE" ]; then
        mkdir -p "$(dirname "$STATE")"
        cat > "$STATE" <<EOF
{
  "version": 1,
  "updated_at": "$(now_iso)",
  "tests": {}
}
EOF
    fi
}

# ----- Subcommands -----

cmd_init() {
    ensure_state_exists
    if [ ! -d "$PERTEST" ]; then
        # No per-test dir is not fatal — coordinator still has empty state.
        update_state '.updated_at = $now' --arg now "$(now_iso)"
        return 0
    fi
    local now
    now=$(now_iso)
    local entries='{}'
    local fname safe upstream abs
    shopt -s nullglob
    for fname in "$PERTEST"/*.txt; do
        safe="$(basename "$fname" .txt)"
        # Skip the INDEX.md / non-test entries (defensive — splitter only
        # emits .txt for testcases, but be safe).
        case "$safe" in
            INDEX|index|README) continue ;;
        esac
        upstream="$(head -n1 "$fname" | sed 's/^# *//')"
        abs="$(cd "$(dirname "$fname")" && pwd -P)/$(basename "$fname")"
        entries=$(jq --arg name "$safe" \
                     --arg upstream "$upstream" \
                     --arg file "$abs" \
                     --arg now "$now" \
                     '. + {($name): {
                              upstream_name: $upstream,
                              status: "fail",
                              per_test_file: $file,
                              pr_url: null,
                              claimed_at: null,
                              updated_at: $now
                          }}' <<<"$entries")
    done
    shopt -u nullglob
    # Merge: keep existing entries; only add brand-new ones.
    update_state \
        '.tests = ($incoming + .tests) | .updated_at = $now' \
        --argjson incoming "$entries" \
        --arg now "$now"
}

cmd_next() {
    ensure_state_exists
    local name
    name=$(jq -r '.tests
                  | to_entries
                  | map(select(.value.status == "fail"))
                  | sort_by(.key)
                  | first
                  | .key // empty' "$STATE")
    if [ -z "$name" ]; then
        echo "no unclaimed failures" >&2
        exit 1
    fi
    local file
    file=$(jq -r --arg n "$name" '.tests[$n].per_test_file' "$STATE")
    update_state \
        '.tests[$n].status = "claimed"
         | .tests[$n].claimed_at = $now
         | .tests[$n].updated_at = $now
         | .updated_at = $now' \
        --arg n "$name" \
        --arg now "$(now_iso)"
    printf '%s\n%s\n' "$name" "$file"
}

cmd_claim() {
    [ $# -ge 1 ] || { echo "error: claim requires NAME" >&2; exit 2; }
    local name="$1"; shift
    local pr_url=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --pr-url)
                [ $# -ge 2 ] || { echo "error: --pr-url requires a value" >&2; exit 2; }
                pr_url="$2"; shift 2 ;;
            *) echo "error: unknown arg to claim: $1" >&2; exit 2 ;;
        esac
    done
    ensure_state_exists
    jq -e --arg n "$name" '.tests | has($n)' "$STATE" >/dev/null || {
        echo "error: no such test: $name" >&2
        exit 1
    }
    if [ -n "$pr_url" ]; then
        update_state \
            '.tests[$n].status = "pr_open"
             | .tests[$n].pr_url = $u
             | .tests[$n].updated_at = $now
             | .updated_at = $now' \
            --arg n "$name" \
            --arg u "$pr_url" \
            --arg now "$(now_iso)"
    else
        update_state \
            '.tests[$n].status = "claimed"
             | .tests[$n].claimed_at = (if .tests[$n].claimed_at == null then $now else .tests[$n].claimed_at end)
             | .tests[$n].updated_at = $now
             | .updated_at = $now' \
            --arg n "$name" \
            --arg now "$(now_iso)"
    fi
}

cmd_release() {
    [ $# -ge 1 ] || { echo "error: release requires NAME" >&2; exit 2; }
    local name="$1"
    ensure_state_exists
    jq -e --arg n "$name" '.tests | has($n)' "$STATE" >/dev/null || {
        echo "error: no such test: $name" >&2
        exit 1
    }
    update_state \
        '.tests[$n].status = "fail"
         | .tests[$n].claimed_at = null
         | .tests[$n].pr_url = null
         | .tests[$n].updated_at = $now
         | .updated_at = $now' \
        --arg n "$name" \
        --arg now "$(now_iso)"
}

cmd_mark_done() {
    [ $# -ge 1 ] || { echo "error: mark-done requires NAME" >&2; exit 2; }
    local name="$1"
    ensure_state_exists
    jq -e --arg n "$name" '.tests | has($n)' "$STATE" >/dev/null || {
        echo "error: no such test: $name" >&2
        exit 1
    }
    update_state \
        '.tests[$n].status = "verified"
         | .tests[$n].updated_at = $now
         | .updated_at = $now' \
        --arg n "$name" \
        --arg now "$(now_iso)"
}

cmd_verify() {
    [ $# -ge 1 ] || { echo "error: verify requires NAME" >&2; exit 2; }
    case "$1" in
        -*) echo "error: verify requires NAME (got flag $1)" >&2; exit 2 ;;
    esac
    local name="$1"; shift
    local script_path=""
    local output_dir=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --single-test-script)
                [ $# -ge 2 ] || { echo "error: --single-test-script requires a value" >&2; exit 2; }
                script_path="$2"; shift 2 ;;
            --output-dir)
                [ $# -ge 2 ] || { echo "error: --output-dir requires a value" >&2; exit 2; }
                output_dir="$2"; shift 2 ;;
            *) echo "error: unknown arg to verify: $1" >&2; exit 2 ;;
        esac
    done
    ensure_state_exists
    jq -e --arg n "$name" '.tests | has($n)' "$STATE" >/dev/null || {
        echo "error: no such test: $name" >&2
        exit 1
    }
    local upstream
    upstream=$(jq -r --arg n "$name" '.tests[$n].upstream_name' "$STATE")
    [ -n "$upstream" ] && [ "$upstream" != "null" ] || {
        echo "error: no upstream_name recorded for $name" >&2
        exit 1
    }
    # Resolve runner script path: explicit flag wins, else look adjacent
    # to this coordinator script.
    if [ -z "$script_path" ]; then
        local self_dir
        self_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
        script_path="$self_dir/conformance-single-test.sh"
    fi
    [ -x "$script_path" ] || [ -f "$script_path" ] || {
        echo "error: single-test runner not found at $script_path" >&2
        exit 1
    }
    # Run the shadow check. Disable errexit around the runner so we can
    # capture the exit code and decide whether to advance state.
    set +e
    if [ -n "$output_dir" ]; then
        bash "$script_path" "$upstream" --output-dir "$output_dir"
    else
        bash "$script_path" "$upstream"
    fi
    local runner_exit=$?
    set -e
    if [ "$runner_exit" -ne 0 ]; then
        echo "verify: runner exited $runner_exit for $name — leaving state unchanged" >&2
        exit 1
    fi
    update_state \
        '.tests[$n].status = "verified"
         | .tests[$n].verified_at = $now
         | .tests[$n].updated_at = $now
         | .updated_at = $now' \
        --arg n "$name" --arg now "$(now_iso)"
}

cmd_update() {
    ensure_state_exists
    command -v gh >/dev/null 2>&1 || {
        echo "error: gh CLI is required for update" >&2
        exit 1
    }
    local names
    mapfile -t names < <(jq -r '.tests
                                | to_entries
                                | map(select(.value.status == "pr_open"))
                                | sort_by(.key)
                                | .[].key' "$STATE")
    local name pr_url gh_out pr_state now
    for name in "${names[@]}"; do
        [ -n "$name" ] || continue
        pr_url=$(jq -r --arg n "$name" '.tests[$n].pr_url' "$STATE")
        [ -n "$pr_url" ] && [ "$pr_url" != "null" ] || continue
        gh_out=$(gh pr view "$pr_url" --json state,mergedAt 2>/dev/null || echo '{}')
        pr_state=$(jq -r '.state // empty' <<<"$gh_out")
        now=$(now_iso)
        case "$pr_state" in
            MERGED)
                update_state \
                    '.tests[$n].status = "pr_merged"
                     | .tests[$n].updated_at = $now
                     | .updated_at = $now' \
                    --arg n "$name" --arg now "$now"
                ;;
            CLOSED)
                update_state \
                    '.tests[$n].status = "fail"
                     | .tests[$n].pr_url = null
                     | .tests[$n].updated_at = $now
                     | .updated_at = $now' \
                    --arg n "$name" --arg now "$now"
                ;;
            OPEN|*)
                : # No change for open or unknown.
                ;;
        esac
    done
}

cmd_status() {
    ensure_state_exists
    local total fail claimed pr_open pr_merged verified
    total=$(jq -r '.tests | length' "$STATE")
    fail=$(jq -r '[.tests[] | select(.status == "fail")] | length' "$STATE")
    claimed=$(jq -r '[.tests[] | select(.status == "claimed")] | length' "$STATE")
    pr_open=$(jq -r '[.tests[] | select(.status == "pr_open")] | length' "$STATE")
    pr_merged=$(jq -r '[.tests[] | select(.status == "pr_merged")] | length' "$STATE")
    verified=$(jq -r '[.tests[] | select(.status == "verified")] | length' "$STATE")
    cat <<EOF
total: $total
fail: $fail
claimed: $claimed
pr_open: $pr_open
pr_merged: $pr_merged
verified: $verified
EOF
}

cmd_list() {
    local filter_status=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --status)
                [ $# -ge 2 ] || { echo "error: --status requires a value" >&2; exit 2; }
                filter_status="$2"; shift 2 ;;
            *) echo "error: unknown arg to list: $1" >&2; exit 2 ;;
        esac
    done
    ensure_state_exists
    if [ -n "$filter_status" ]; then
        jq -r --arg s "$filter_status" \
            '.tests | to_entries | map(select(.value.status == $s)) | sort_by(.key) | .[].key' \
            "$STATE"
    else
        jq -r '.tests | keys | .[]' "$STATE"
    fi
}

case "$SUBCOMMAND" in
    init) cmd_init "$@" ;;
    next) cmd_next "$@" ;;
    claim) cmd_claim "$@" ;;
    release) cmd_release "$@" ;;
    mark-done) cmd_mark_done "$@" ;;
    verify) cmd_verify "$@" ;;
    update) cmd_update "$@" ;;
    status) cmd_status "$@" ;;
    list) cmd_list "$@" ;;
    *) echo "error: unknown subcommand: $SUBCOMMAND" >&2; usage >&2; exit 2 ;;
esac
