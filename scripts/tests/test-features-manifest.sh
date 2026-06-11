#!/usr/bin/env bash
# Validates ci/conformance/features.json: valid JSON, unique names,
# required fields present, focus/skip are valid EREs (Go RE2 ~= ERE for
# the bracket-escaped tag patterns we use).
#
# Run with: bash scripts/tests/test-features-manifest.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MANIFEST="$REPO_ROOT/ci/conformance/features.json"

fail() { echo "FAIL: $*" >&2; exit 1; }

command -v jq >/dev/null 2>&1 || fail "jq required"
[ -f "$MANIFEST" ] || fail "manifest missing: $MANIFEST"

jq -e . "$MANIFEST" >/dev/null 2>&1 || fail "manifest is not valid JSON"
jq -e 'type == "array" and length > 0' "$MANIFEST" >/dev/null \
    || fail "manifest must be a non-empty array"

# Every entry has the required string fields and a boolean gating.
jq -e 'all(.[];
        (.name|type=="string" and length>0)
    and (.sig|type=="string" and length>0)
    and (.focus|type=="string" and length>0)
    and (.skip|type=="string")
    and (.gating|type=="boolean"))' "$MANIFEST" >/dev/null \
    || fail "every entry needs string name/sig/focus, string skip, boolean gating"

# Names unique.
dupes=$(jq -r '[.[].name] | group_by(.) | map(select(length>1)) | .[][0]' "$MANIFEST")
[ -z "$dupes" ] || fail "duplicate feature names: $dupes"

# Names must be path-safe (used as a directory + artifact name in CI).
jq -e 'all(.[].name; test("^[a-z0-9][a-z0-9-]*$"))' "$MANIFEST" >/dev/null \
    || fail "feature names must match ^[a-z0-9][a-z0-9-]*\$ (path-safe)"

# focus/skip compile as EREs.
while IFS=$'\t' read -r name focus skip; do
    printf '' | grep -E "$focus" >/dev/null 2>&1 || [ $? -le 1 ] \
        || fail "feature '$name': focus is not a valid ERE: $focus"
    if [ -n "$skip" ]; then
        printf '' | grep -E "$skip" >/dev/null 2>&1 || [ $? -le 1 ] \
            || fail "feature '$name': skip is not a valid ERE: $skip"
    fi
done < <(jq -r '.[] | [.name, .focus, .skip] | @tsv' "$MANIFEST")

echo "PASS: features manifest valid ($(jq length "$MANIFEST") features)"
