#!/bin/sh
# kubelet entrypoint.
#
# The kubelet dials pod IPs directly for liveness/readiness/startup probes
# (httpGet/tcpSocket) and for HTTP lifecycle hooks (postStart/preStop). In the
# multi-container compose stack every pod runs inside the shared `containerd`
# CRI service's CNI bridge (pod CIDR 10.244.0.0/16 on cni0) — a different
# network namespace from this kubelet container. With no route to that CIDR the
# probe/hook dial fails, the kubelet marks the container unhealthy and restarts
# it in a loop, and probe/lifecycle-hook NodeConformance specs fail.
#
# Install a route to the pod CIDR via the containerd container, mirroring the
# api-server entrypoint (and the node route a real cluster's CNI / route
# controller programs for the pod network). Best-effort: never block startup —
# on stacks without a separate containerd service (all-in-one) it is simply
# skipped.
POD_CIDR="${POD_CIDR:-10.244.0.0/16}"
POD_NET_GW="${POD_NET_GW:-}"
if [ -z "$POD_NET_GW" ]; then
    # The containerd CRI service owns the pod network; resolve it via compose DNS.
    POD_NET_GW="$(getent hosts containerd 2>/dev/null | awk '{ print $1; exit }')"
fi
if [ -n "$POD_NET_GW" ]; then
    if ip route replace "$POD_CIDR" via "$POD_NET_GW" 2>/dev/null; then
        echo "kubelet entrypoint: routed pod CIDR $POD_CIDR via $POD_NET_GW (containerd) for probes/lifecycle hooks"
    else
        echo "kubelet entrypoint: WARNING: could not add route $POD_CIDR via $POD_NET_GW (need cap_add NET_ADMIN / privileged?); probes and HTTP lifecycle hooks to pod IPs may fail" >&2
    fi
else
    echo "kubelet entrypoint: note: no 'containerd' host to route pod CIDR via; skipping pod-network route (expected on all-in-one / non-CRI stacks)"
fi

exec /app/kubelet "$@"
