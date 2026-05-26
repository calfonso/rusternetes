#!/bin/bash
# Discover the Docker/Podman bridge network gateway IP for the rusternetes network.
#
# This is the scalable replacement for the hardcoded bridge IPs that PR #787
# introduced (172.20.0.2). The gateway is always [subnet].1 and works on every
# host regardless of which subnet Docker chooses.
#
# Usage:
#   source scripts/discover-bridge-gateway.sh
#   echo "$RUSTERNETES_BRIDGE_GATEWAY"
#
# The gateway is the .1 address in the docker-bridge subnet. From inside a pod,
# .1 is the host — port mapping 6443:6443 on the host then reaches the
# rusternetes container's api-server without needing to know the container's
# IP or the bridge subnet.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Prefer the compose network name; fall back to the name used by compose.all-in-one.yml
NETWORK_NAME="${RUSTERNETES_NETWORK_NAME:-rusternetes-network}"

# Helper: try to inspect the gateway from docker compose
discover_via_inspect() {
    # docker compose inspect is more portable and works for both docker and podman
    # (podman compose returns a jq-compatible JSON output for inspect)
    local gateway
    gateway=$(docker inspect "$NETWORK_NAME" 2>/dev/null | jq -r '.[0].IPAM.Config[0].Gateway // empty' 2>/dev/null) || true

    if [ -z "$gateway" ] || [ "$gateway" = "null" ]; then
        # podman-compose doesn't have 'inspect' but does 'network inspect'
        if command -v podman &>/dev/null; then
            gateway=$(podman network inspect "$NETWORK_NAME" --format '{{.Subnets[0].Gateway}}' 2>/dev/null || true)
        else
            gateway=$(docker network inspect "$NETWORK_NAME" -f '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || true)
        fi
    fi

    if [ -n "$gateway" ] && [ "$gateway" != "null" ]; then
        echo "$gateway"
        return 0
    fi

    return 1
}

# Try the inspect-based approach first
if gateway=$(discover_via_inspect); then
    echo "$gateway"
    exit 0
fi

# Fallback: discover via host routes (works when rusternetes network
# subnet is configured via Docker's default 172.18-172.30 range)
# This reads the default route's subnet and replaces .0 with .1
if ip route | grep -q "docker0\|rusternetes-network"; then
    SUBNET=$(ip route show | grep "docker0\|rusternetes-network" | grep -oP 'inet\K[0-9./]+' | head -1)
    if [ -n "$SUBNET" ]; then
        NETWORK=$(echo "$SUBNET" | cut -d/ -f1 | rev | cut -d. -f2,3,4 | rev)
        echo "${NETWORK}.1"
        exit 0
    fi
fi

# Last resort: use the default gateway
GATEWAY=$(ip route show default | grep -oP 'via \K[0-9./]+' | head -1 | cut -d/ -f1)
if [ -n "$GATEWAY" ]; then
    echo "$GATEWAY"
    exit 0
fi

# All discovery methods failed — print instructions
echo "" >&2
echo "ERROR: Could not discover Docker bridge gateway IP." >&2
echo "" >&2
echo "This is needed for CoreDNS to reach the API server after PR #787." >&2
echo "Ensure the 'rusternetes-network' network exists and try again, or set:" >&2
echo "" >&2
echo "  export DOCKER_GATEWAY=172.18.0.1  # replace with your host's gateway" >&2
echo "" >&2
exit 1
