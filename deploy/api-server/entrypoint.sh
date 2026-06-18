#!/bin/sh
# api-server entrypoint.
#
# The api-server serves the pod/service proxy subresources by dialing pod IPs
# directly (e.g. `GET pods/<p>/proxy/...`, which the DNS conformance tests use
# to read results off the test pod). In the multi-container compose stack every
# pod runs inside the shared `containerd` CRI service's CNI bridge (pod CIDR
# 10.244.0.0/16 on cni0) — a different network namespace from this api-server
# container. With no route to that CIDR the dial fails ("error trying to reach
# backend: error sending request") and the proxy request times out.
#
# Install a route to the pod CIDR via the containerd container, mirroring the
# node route a real cluster's CNI / route-controller would program for the pod
# network. Best-effort: never block startup on it — the api-server still serves
# everything that does not need direct pod reachability, and stacks without a
# separate containerd service (all-in-one, the stale etcd/redis composes) simply
# skip it.
POD_CIDR="${POD_CIDR:-10.244.0.0/16}"
POD_NET_GW="${POD_NET_GW:-}"
if [ -z "$POD_NET_GW" ]; then
    # The containerd CRI service owns the pod network; resolve it via compose DNS.
    POD_NET_GW="$(getent hosts containerd 2>/dev/null | awk '{ print $1; exit }')"
fi
if [ -n "$POD_NET_GW" ]; then
    if ip route replace "$POD_CIDR" via "$POD_NET_GW" 2>/dev/null; then
        echo "api-server entrypoint: routed pod CIDR $POD_CIDR via $POD_NET_GW (containerd) for pod/service proxy"
    else
        echo "api-server entrypoint: WARNING: could not add route $POD_CIDR via $POD_NET_GW (need cap_add NET_ADMIN?); pod/service proxy to pod IPs may fail" >&2
    fi
else
    echo "api-server entrypoint: note: no 'containerd' host to route pod CIDR via; skipping pod-network route (expected on all-in-one / non-CRI stacks)"
fi

exec /app/api-server "$@"
