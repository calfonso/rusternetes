#!/usr/bin/env bash
# conformance-split-junit.sh
#
# Convert a sonobuoy conformance `junit_01.xml` into one focused `.txt` file
# per testcase. Designed to produce per-test inputs that can be dispatched to
# Claude Code workers to investigate individual conformance failures.
#
# Usage:
#   bash scripts/conformance-split-junit.sh [--all] \
#       [--input <junit_01.xml>] [--output-dir <dir>]
#
# Defaults:
#   --input:      newest `junit_01.xml` under
#                 `.rusternetes/volumes/sonobuoy-e2e-job-*/results/`
#   --output-dir: `.rusternetes/volumes/conformance-per-test/`
#
# Without `--all`, only failing/erroring testcases are emitted.

set -euo pipefail
IFS=$'\n\t'

SCRIPT_NAME="$(basename "$0")"
EMIT_ALL=0
INPUT=""
OUTPUT_DIR=""

usage() {
    cat <<EOF
Usage: $SCRIPT_NAME [--all] [--input <junit_01.xml>] [--output-dir <dir>]

Options:
  --all              Emit one file per testcase, not only failures/errors.
  --input PATH       Path to junit_01.xml. Default: auto-discover newest under
                     .rusternetes/volumes/sonobuoy-e2e-job-*/results/.
  --output-dir DIR   Where to write per-test files. Default:
                     .rusternetes/volumes/conformance-per-test/
  -h, --help         Show this help.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --all)
            EMIT_ALL=1
            shift
            ;;
        --input)
            [ $# -ge 2 ] || { echo "error: --input requires a value" >&2; exit 2; }
            INPUT="$2"
            shift 2
            ;;
        --output-dir)
            [ $# -ge 2 ] || { echo "error: --output-dir requires a value" >&2; exit 2; }
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if ! command -v xmlstarlet >/dev/null 2>&1; then
    echo "error: xmlstarlet not installed; install with: sudo apt-get install xmlstarlet (or brew install xmlstarlet)" >&2
    exit 1
fi

# Auto-discover the newest junit_01.xml if --input wasn't given.
if [ -z "$INPUT" ]; then
    SEARCH_ROOT=".rusternetes/volumes"
    if [ ! -d "$SEARCH_ROOT" ]; then
        echo "error: no junit_01.xml provided and $SEARCH_ROOT does not exist." >&2
        echo "Run a conformance suite first: bash scripts/run-conformance.sh" >&2
        exit 1
    fi
    # Find newest junit_01.xml under sonobuoy-e2e-job-*/results/. Use printf'd
    # mtime to avoid relying on GNU-only `find -printf` formats. The
    # mtime+path tuple is sorted descending, and the top path is selected.
    CANDIDATE=""
    while IFS= read -r path; do
        [ -n "$path" ] || continue
        CANDIDATE="$path"
        break
    done < <(
        find "$SEARCH_ROOT" -type f -path '*sonobuoy-e2e-job-*/results/junit_01.xml' -print 2>/dev/null \
            | while IFS= read -r p; do
                # `stat -c` (GNU) and `stat -f` (BSD) differ — fall back to ls -t below if neither works.
                mtime=$(stat -c %Y "$p" 2>/dev/null || stat -f %m "$p" 2>/dev/null || echo 0)
                printf '%s\t%s\n' "$mtime" "$p"
            done \
            | sort -rn \
            | cut -f2-
    )
    if [ -z "$CANDIDATE" ]; then
        echo "error: no junit_01.xml found under $SEARCH_ROOT/sonobuoy-e2e-job-*/results/." >&2
        echo "Run a conformance suite first: bash scripts/run-conformance.sh" >&2
        exit 1
    fi
    INPUT="$CANDIDATE"
fi

if [ ! -f "$INPUT" ]; then
    echo "error: input file not found: $INPUT" >&2
    exit 1
fi

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR=".rusternetes/volumes/conformance-per-test"
fi

mkdir -p "$OUTPUT_DIR"

echo "Input:  $INPUT"
echo "Output: $OUTPUT_DIR"
echo "Mode:   $([ $EMIT_ALL -eq 1 ] && echo 'all testcases' || echo 'failures/errors only')"

# Count total testcases up front (used in INDEX summary).
TOTAL_TESTCASES=$(xmlstarlet sel -t -v 'count(//testcase)' "$INPUT" 2>/dev/null || echo 0)
TOTAL_TESTCASES="${TOTAL_TESTCASES:-0}"

if [ "$TOTAL_TESTCASES" = "0" ]; then
    echo "warning: no <testcase> elements found in $INPUT" >&2
fi

# Sanitize a string into a safe filename: keep alnum, dot, dash; everything
# else becomes '_'. Then collapse runs of '_' and truncate to 200 chars.
sanitize_filename() {
    local raw="$1"
    local safe
    safe=$(printf '%s' "$raw" | tr -c '[:alnum:].-' '_' | tr -s '_')
    # Trim leading/trailing underscores for tidiness.
    safe="${safe#_}"
    safe="${safe%_}"
    # Truncate to 200 chars (filename limit on most filesystems is 255 — leave room for `.txt`).
    if [ "${#safe}" -gt 200 ]; then
        safe="${safe:0:200}"
    fi
    [ -n "$safe" ] || safe="unnamed"
    printf '%s' "$safe"
}

# Extract the leading `[sig-xxx]` token from a test name, lowercased. Returns
# "unknown" if none is present.
sig_area_of() {
    local name="$1"
    # Match leading [sig-foo] or [k8s.io], strip brackets.
    case "$name" in
        '[sig-'*']'*)
            local token="${name%%]*}"
            token="${token#[}"
            printf '%s' "$token"
            ;;
        *)
            printf 'unknown'
            ;;
    esac
}

# Best-effort glob for related docs. Lists matching files (one per line) or
# nothing if no matches.
related_docs_for() {
    local area="$1"
    [ "$area" = "unknown" ] && return 0
    local short="${area#sig-}"
    [ -d "docs/conformance" ] || return 0
    # shellcheck disable=SC2010
    ls -1 docs/conformance/ 2>/dev/null \
        | while IFS= read -r f; do
            case "$f" in
                "${area}-"*.md|"${short}-"*.md)
                    printf 'docs/conformance/%s\n' "$f"
                    ;;
            esac
        done
}

# Per-testcase counters.
COUNT_PASSED=0
COUNT_FAILED=0
COUNT_SKIPPED=0
COUNT_ERROR=0
EMITTED=0

# Build a temp working directory we control.
TMPDIR_WORK=$(mktemp -d)
trap 'rm -rf "$TMPDIR_WORK"' EXIT

INDEX_FAILED="$TMPDIR_WORK/idx_failed"
INDEX_ERROR="$TMPDIR_WORK/idx_error"
INDEX_PASSED="$TMPDIR_WORK/idx_passed"
INDEX_SKIPPED="$TMPDIR_WORK/idx_skipped"
: > "$INDEX_FAILED"
: > "$INDEX_ERROR"
: > "$INDEX_PASSED"
: > "$INDEX_SKIPPED"

# Iterate testcases by index. xmlstarlet handles the XML — we never parse it
# with regex.
i=1
while [ "$i" -le "$TOTAL_TESTCASES" ]; do
    NAME=$(xmlstarlet sel -t -v "string((//testcase)[$i]/@name)" "$INPUT" 2>/dev/null || true)
    CLASSNAME=$(xmlstarlet sel -t -v "string((//testcase)[$i]/@classname)" "$INPUT" 2>/dev/null || true)
    TIME=$(xmlstarlet sel -t -v "string((//testcase)[$i]/@time)" "$INPUT" 2>/dev/null || true)

    # One query gets the local-name of the first status child (failure/error/skipped) — if any.
    STATUS_CHILD=$(xmlstarlet sel -t -v "name((//testcase)[$i]/*[self::failure or self::error or self::skipped][1])" "$INPUT" 2>/dev/null || true)

    case "$STATUS_CHILD" in
        failure) STATUS="failed";  COUNT_FAILED=$((COUNT_FAILED + 1)) ;;
        error)   STATUS="error";   COUNT_ERROR=$((COUNT_ERROR + 1)) ;;
        skipped) STATUS="skipped"; COUNT_SKIPPED=$((COUNT_SKIPPED + 1)) ;;
        *)       STATUS="passed";  COUNT_PASSED=$((COUNT_PASSED + 1)) ;;
    esac

    # Decide whether to emit a file for this testcase.
    EMIT=0
    if [ "$EMIT_ALL" -eq 1 ]; then
        EMIT=1
    elif [ "$STATUS" = "failed" ] || [ "$STATUS" = "error" ]; then
        EMIT=1
    fi

    if [ "$EMIT" -eq 1 ]; then
        SAFE_NAME=$(sanitize_filename "$NAME")
        OUT_FILE="$OUTPUT_DIR/${SAFE_NAME}.txt"

        AREA=$(sig_area_of "$NAME")
        RELATED=$(related_docs_for "$AREA")

        FAIL_MSG=""
        FAIL_TEXT=""
        if [ "$STATUS_CHILD" = "failure" ] || [ "$STATUS_CHILD" = "error" ]; then
            FAIL_MSG=$(xmlstarlet sel -t -v "string((//testcase)[$i]/${STATUS_CHILD}/@message)" "$INPUT" 2>/dev/null || true)
            FAIL_TEXT=$(xmlstarlet sel -t -v "(//testcase)[$i]/${STATUS_CHILD}" "$INPUT" 2>/dev/null || true)
        fi

        SYSTEM_OUT=$(xmlstarlet sel -t -v "(//testcase)[$i]/system-out" "$INPUT" 2>/dev/null || true)
        SYSTEM_OUT_TAIL=$(printf '%s\n' "$SYSTEM_OUT" | tail -n 200)

        {
            printf '# %s\n\n' "$NAME"
            printf 'Status:    %s\n' "$STATUS"
            printf 'Classname: %s\n' "$CLASSNAME"
            printf 'Time:      %ss\n' "$TIME"
            printf 'Sig area:  %s\n' "$AREA"
            if [ -n "$RELATED" ]; then
                printf 'Related docs:\n'
                printf '%s\n' "$RELATED" | sed 's/^/  - /'
            fi
            printf '\n## Failure message\n\n'
            if [ -n "$FAIL_MSG" ]; then
                printf '%s\n\n' "$FAIL_MSG"
            fi
            if [ -n "$FAIL_TEXT" ]; then
                printf '%s\n' "$FAIL_TEXT"
            fi
            if [ -z "$FAIL_MSG" ] && [ -z "$FAIL_TEXT" ]; then
                printf '(none)\n'
            fi
            printf '\n## System output (last 200 lines)\n\n'
            if [ -n "$SYSTEM_OUT_TAIL" ]; then
                printf '%s\n' "$SYSTEM_OUT_TAIL"
            else
                printf '(none)\n'
            fi
        } > "$OUT_FILE"

        EMITTED=$((EMITTED + 1))

        # Append a relative entry to the appropriate index list.
        REL_PATH="${SAFE_NAME}.txt"
        case "$STATUS" in
            failed)  printf -- '- [%s](%s)\n' "$NAME" "$REL_PATH" >> "$INDEX_FAILED" ;;
            error)   printf -- '- [%s](%s)\n' "$NAME" "$REL_PATH" >> "$INDEX_ERROR" ;;
            skipped) printf -- '- [%s](%s)\n' "$NAME" "$REL_PATH" >> "$INDEX_SKIPPED" ;;
            passed)  printf -- '- [%s](%s)\n' "$NAME" "$REL_PATH" >> "$INDEX_PASSED" ;;
        esac
    fi

    i=$((i + 1))
done

# Write INDEX.md summary.
INDEX_FILE="$OUTPUT_DIR/INDEX.md"
{
    printf '# Conformance per-test failure files\n\n'
    printf 'Source: `%s`\n\n' "$INPUT"
    printf 'Generated: %s\n\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '## Counts\n\n'
    printf -- '- Passed:  %s\n' "$COUNT_PASSED"
    printf -- '- Failed:  %s\n' "$COUNT_FAILED"
    printf -- '- Error:   %s\n' "$COUNT_ERROR"
    printf -- '- Skipped: %s\n' "$COUNT_SKIPPED"
    printf -- '- Total:   %s\n' "$TOTAL_TESTCASES"
    printf -- '- Emitted files: %s\n\n' "$EMITTED"

    if [ -s "$INDEX_FAILED" ]; then
        printf '## Failed (%s)\n\n' "$COUNT_FAILED"
        sort -u "$INDEX_FAILED"
        printf '\n'
    fi
    if [ -s "$INDEX_ERROR" ]; then
        printf '## Errored (%s)\n\n' "$COUNT_ERROR"
        sort -u "$INDEX_ERROR"
        printf '\n'
    fi
    if [ -s "$INDEX_SKIPPED" ]; then
        printf '## Skipped (%s)\n\n' "$COUNT_SKIPPED"
        sort -u "$INDEX_SKIPPED"
        printf '\n'
    fi
    if [ -s "$INDEX_PASSED" ]; then
        printf '## Passed (%s)\n\n' "$COUNT_PASSED"
        sort -u "$INDEX_PASSED"
        printf '\n'
    fi
} > "$INDEX_FILE"

echo ""
echo "Wrote $EMITTED per-test file(s) to $OUTPUT_DIR/"
echo "Summary: passed=$COUNT_PASSED failed=$COUNT_FAILED error=$COUNT_ERROR skipped=$COUNT_SKIPPED total=$TOTAL_TESTCASES"
echo "Index:   $INDEX_FILE"
