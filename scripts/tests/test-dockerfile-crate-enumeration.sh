#!/usr/bin/env bash
# Guard against Dockerfile crate-enumeration drift.
#
# The image Dockerfiles do NOT `COPY crates/` wholesale — for BuildKit layer
# caching they enumerate each workspace member's Cargo.toml by hand:
#   COPY crates/<c>/Cargo.toml crates/<c>/Cargo.toml
# cargo refuses to resolve the workspace if ANY `[workspace] members` manifest
# is absent, so adding a new member to the root Cargo.toml silently breaks every
# image build until it is added to these lists (see issues #1259, #1260).
#
# This test asserts that every `[workspace] members` crate has a matching
# Cargo.toml COPY in each enumerating Dockerfile. It does NOT check the
# dummy-source / real-src blocks — those only need the crates a given binary
# actually compiles, which varies per image.
#
# Requires: bash, grep, sed (no docker/cargo).
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

# Dockerfiles that enumerate crate manifests by hand.
DOCKERFILES=(
    Dockerfile.services
    Dockerfile.dns
    Dockerfile.kubectl
    Dockerfile.all-in-one
)

# Workspace members → bare crate dir names (e.g. "api-server").
members="$(sed -n '/^\[workspace\]/,/^\]/p' Cargo.toml \
    | grep -oE '"crates/[a-z0-9_-]+"' \
    | sed -E 's#"crates/([a-z0-9_-]+)"#\1#' \
    | sort -u)"

if [ -z "$members" ]; then
    echo "FAIL: could not parse [workspace] members from Cargo.toml" >&2
    exit 1
fi

fail=0
for df in "${DOCKERFILES[@]}"; do
    if [ ! -f "$df" ]; then
        echo "FAIL: $df not found" >&2
        fail=1
        continue
    fi

    # Crate names from `... crates/<c>/Cargo.toml` COPY lines. Only manifest
    # COPYs end in /Cargo.toml; build.rs/proto/src COPYs don't, so they're
    # naturally excluded.
    copied="$(grep -oE 'crates/[a-z0-9_-]+/Cargo\.toml' "$df" \
        | sed -E 's#crates/([a-z0-9_-]+)/Cargo\.toml#\1#' \
        | sort -u)"

    missing="$(comm -23 <(printf '%s\n' "$members") <(printf '%s\n' "$copied"))"
    if [ -n "$missing" ]; then
        echo "FAIL: $df is missing Cargo.toml COPYs for workspace members:" >&2
        echo "$missing" | sed 's/^/    crates\//' >&2
        fail=1
    else
        echo "OK: $df enumerates all $(printf '%s\n' "$members" | wc -l | tr -d ' ') workspace members"
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "test-dockerfile-crate-enumeration: FAILED" >&2
    exit 1
fi
echo "test-dockerfile-crate-enumeration: OK"
