#!/usr/bin/env bash
# Seed / refresh ci/conformance/features.json from the conformance image's
# ginkgo spec list. MAINTENANCE TOOL — not in the nightly path. Run it to
# bootstrap the manifest and on every k8s-version bump to catch new feature
# labels, then hand-review the printed diff before committing.
#
# It lists every spec, extracts each distinct [Feature:X] / [NodeFeature:X]
# label and the [sig-xxx] it co-occurs with, and emits a JSON manifest
# skeleton (gating:false, default skip) to stdout. It NEVER writes the
# manifest itself — pipe to the file deliberately after reviewing the diff.
#
# Usage:
#   bash scripts/conformance-features-discover.sh \
#       [--conformance-image IMG] [--from-file SPEC_DUMP] [--diff]
#
#   --from-file F   Read the ginkgo dry-run dump from F instead of running
#                   the image (used by the unit test; also handy offline).
#   --diff          Print a name-level diff vs the committed manifest to stderr.
set -euo pipefail
IFS=$'\n\t'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
MANIFEST="$REPO_ROOT/ci/conformance/features.json"

IMAGE="registry.k8s.io/conformance:v1.35.0"
FROM_FILE=""; DO_DIFF=0
die() { echo "[discover] ERROR: $*" >&2; exit 2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --conformance-image) [[ $# -ge 2 ]] || die "--conformance-image requires a value"; IMAGE="$2"; shift 2 ;;
        --from-file) [[ $# -ge 2 ]] || die "--from-file requires a value"; FROM_FILE="$2"; shift 2 ;;
        --diff) DO_DIFF=1; shift ;;
        -h|--help) awk 'NR==1{next} /^[^#]/{exit} {sub(/^# ?/,""); print}' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) die "unknown flag: $1" ;;
    esac
done

get_specs() {
    if [ -n "$FROM_FILE" ]; then
        [ -f "$FROM_FILE" ] || die "--from-file: file not found: $FROM_FILE"
        cat "$FROM_FILE"
    else
        docker run --rm "$IMAGE" \
            /usr/local/bin/e2e.test --ginkgo.dry-run --ginkgo.no-color 2>/dev/null
    fi
}

# Build "rawfeature<TAB>sig" pairs: for each spec line with a feature label,
# pair every feature label with the first sig label on that line. The grep is
# wrapped in `|| true` so a dump with zero feature labels emits `[]` under
# `set -o pipefail` instead of aborting the whole script.
emitted=$(
get_specs \
| { grep -E '\[(Feature|NodeFeature):' || true; } \
| while IFS= read -r line; do
    sig=$(echo "$line" | grep -oE '\[sig-[a-z-]+\]' | head -1 | tr -d '[]')
    [ -n "$sig" ] || sig="unknown"
    echo "$line" | grep -oE '\[(Feature|NodeFeature):[^]]+\]' | while IFS= read -r feat; do
        fname=$(echo "$feat" | sed -E 's/\[(Feature|NodeFeature):([^]]+)\]/\2/')
        printf '%s\t%s\n' "$fname" "$sig"
    done
done \
| sort -u \
| jq -R -s '
    split("\n") | map(select(length>0)) | map(split("\t")) | map({
        rawname: .[0], sig: .[1]
    })
    | group_by(.rawname) | map(.[0])
    | map({
        name: (.rawname
               | gsub("(?<a>[A-Z]+)(?<b>[A-Z][a-z])"; "\(.a)-\(.b)")
               | gsub("(?<a>[a-z0-9])(?<b>[A-Z])"; "\(.a)-\(.b)")
               | ascii_downcase | gsub("[^a-z0-9]+"; "-") | gsub("^-+|-+$"; "")),
        sig: .sig,
        focus: ("\\[(Feature|NodeFeature):" + .rawname + "\\]"),
        skip: "\\[Flaky\\]",
        gating: false
      })
  '
)

# stdout = JSON only.
echo "$emitted"

# --diff: name-level diff vs the committed manifest, to STDERR only.
if [ "$DO_DIFF" -eq 1 ] && [ -f "$MANIFEST" ]; then
    new=$(comm -23 <(echo "$emitted" | jq -r '.[].name' | sort) <(jq -r '.[].name' "$MANIFEST" | sort))
    gone=$(comm -13 <(echo "$emitted" | jq -r '.[].name' | sort) <(jq -r '.[].name' "$MANIFEST" | sort))
    {
        echo "[discover] diff vs $MANIFEST:"
        echo "  added:   ${new:-<none>}"
        echo "  removed: ${gone:-<none>}"
    } >&2
fi
