#!/usr/bin/env bash
# Validate the GHCR image-name overlays:
#  1. Every service in each overlay exists in its base compose file
#     (catches service renames drifting out of sync).
#  2. Every buildable service in the base file has an image: entry in the
#     overlay (catches new services that would silently skip publishing).
#  3. The merged config resolves image names with both a default tag and an
#     explicit RUSTERNETES_IMAGE_TAG.
# Requires: docker compose v2, jq.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

command -v jq >/dev/null 2>&1 || { echo "FAIL: jq required" >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "FAIL: docker required" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "FAIL: docker compose v2 required" >&2; exit 1; }

# The dns service is build-only (profiles: ["build"]); activate the profile
# so `compose config` still enumerates it for the sync checks below.
export COMPOSE_PROFILES=build

fail=0

check_pair() {
    local base="$1" overlay="$2"
    echo "--- $base + $overlay"

    # Render merged config once per tag scenario. KUBELET_VOLUMES_PATH and
    # DOCKER_GATEWAY are interpolated by the base files; give them dummies.
    local cfg
    cfg=$(KUBELET_VOLUMES_PATH=/tmp/kv DOCKER_GATEWAY=10.0.0.1 RUSTERNETES_IMAGE_TAG=sha-deadbeef \
        docker compose -f "$base" -f "$overlay" config --format json)

    # 1+2. Service sets: overlay services ⊆ base services, and every service
    # with a build: section must carry a ghcr image name.
    local base_services overlay_services
    base_services=$(KUBELET_VOLUMES_PATH=/tmp/kv DOCKER_GATEWAY=10.0.0.1 \
        docker compose -f "$base" config --services | sort)
    # The overlays are valid standalone compose files (services + image only),
    # so let compose itself enumerate their services rather than text-parsing.
    overlay_services=$(docker compose -f "$overlay" config --services | sort)

    for svc in $overlay_services; do
        if ! echo "$base_services" | grep -qx "$svc"; then
            echo "FAIL: overlay service '$svc' not present in $base" >&2
            fail=1
        fi
    done

    for svc in $base_services; do
        has_build=$(echo "$cfg" | jq -r --arg s "$svc" '.services[$s] | has("build")')
        image=$(echo "$cfg" | jq -r --arg s "$svc" '.services[$s].image // ""')
        if [ "$has_build" = "true" ]; then
            case "$image" in
                ghcr.io/indyjonesnl/rusternetes/*:sha-deadbeef) ;;
                *)
                    echo "FAIL: buildable service '$svc' in $base lacks a ghcr image with the requested tag (got '$image')" >&2
                    fail=1
                    ;;
            esac
        fi
    done

    # 3. Default tag must resolve to :main when RUSTERNETES_IMAGE_TAG unset.
    # `env -u` clears any RUSTERNETES_IMAGE_TAG exported by the caller, which
    # would otherwise leak in and make this check fail spuriously.
    local default_img
    default_img=$(env -u RUSTERNETES_IMAGE_TAG \
        KUBELET_VOLUMES_PATH=/tmp/kv DOCKER_GATEWAY=10.0.0.1 \
        docker compose -f "$base" -f "$overlay" config --format json \
        | jq -r '.services["api-server"].image')
    if [ "$default_img" != "ghcr.io/indyjonesnl/rusternetes/api-server:main" ]; then
        echo "FAIL: default tag for api-server should be :main, got '$default_img'" >&2
        fail=1
    fi
}

check_pair compose.sqlite.yml compose.ghcr.yml
check_pair compose.node-conformance.yml compose.ghcr.node-conformance.yml

if [ "$fail" -ne 0 ]; then
    echo "test-ghcr-overlays: FAILED"
    exit 1
fi
echo "test-ghcr-overlays: OK"
