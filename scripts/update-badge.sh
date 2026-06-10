#!/usr/bin/env bash
# Publish a shields.io "endpoint" badge JSON to the orphan `badges` branch so the
# README success-rate badges reflect the latest CI run without any state file
# committed on main (no merge conflicts on parallel PRs — see issue #1051).
#
# Best-effort by design: it NEVER fails the caller. If the token, the badges
# branch, or the push is unavailable it logs and exits 0, so wiring it into a
# conformance workflow can't turn a test run red.
#
# Usage: update-badge.sh <slug> <label> <passed> <total>
#   slug   filename on the badges branch, e.g. "conformance" -> conformance.json
#   label  left-hand badge text, e.g. "conformance"
#   passed / total  spec counts; message becomes "<pct>% (<passed>/<total>)"
#
# Requires GITHUB_TOKEN + GITHUB_REPOSITORY in the environment (GitHub Actions).
set -uo pipefail

slug="${1:?usage: update-badge.sh <slug> <label> <passed> <total>}"
label="${2:?label required}"
passed="${3:-0}"
total="${4:-0}"

if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_REPOSITORY:-}" ]; then
    echo "update-badge: no GITHUB_TOKEN/GITHUB_REPOSITORY (not in CI) — skipping"
    exit 0
fi
case "$total" in '' | *[!0-9]*) total=0 ;; esac
case "$passed" in '' | *[!0-9]*) passed=0 ;; esac
if [ "$total" -le 0 ]; then
    echo "update-badge: total=0 (no specs counted) — skipping"
    exit 0
fi

pct=$(( passed * 100 / total ))
if   [ "$pct" -ge 98 ]; then color=brightgreen
elif [ "$pct" -ge 90 ]; then color=green
elif [ "$pct" -ge 75 ]; then color=yellowgreen
elif [ "$pct" -ge 50 ]; then color=yellow
else color=orange; fi

json="{\"schemaVersion\":1,\"label\":\"${label}\",\"message\":\"${pct}% (${passed}/${total})\",\"color\":\"${color}\"}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
url="https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"

if ! git clone --quiet --branch badges --depth 1 "$url" "$work" 2>/dev/null; then
    echo "update-badge: 'badges' branch not found — skipping (create it once to enable badges)"
    exit 0
fi

printf '%s\n' "$json" > "$work/${slug}.json"
(
    cd "$work" || exit 0
    git config user.name "github-actions[bot]"
    git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
    git add "${slug}.json"
    if git commit --quiet -m "ci: ${slug} badge ${pct}% (${passed}/${total})" 2>/dev/null; then
        git push --quiet origin badges 2>/dev/null \
            && echo "update-badge: ${slug} -> ${pct}% (${passed}/${total})" \
            || echo "update-badge: push failed (token lacks contents:write?) — skipping"
    else
        echo "update-badge: ${slug} unchanged (${pct}%) — nothing to push"
    fi
)
exit 0
