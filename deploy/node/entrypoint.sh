#!/bin/sh
# Entrypoint for a rusternetes node: one container running containerd (CRI +
# Youki) AND the kubelet, kind-style. Bundling them in a single container means
# the kubelet's hostPath validation and containerd's hostPath mounts share one
# filesystem — required for flannel's CNI install (/opt/cni/bin, /etc/cni/net.d,
# /run/flannel) and for pod hostPath volumes to resolve consistently.
set -e

# --- containerd prerequisites (see deploy/containerd/entrypoint.sh) -----------
sysctl -w fs.inotify.max_user_instances=1024 >/dev/null 2>&1 || true
sysctl -w fs.inotify.max_user_watches=1048576 >/dev/null 2>&1 || true

# cgroup v2 nesting fix (kind/k3d): move our processes into a leaf so controllers
# can be delegated, else Youki fails with "+io ... Not supported".
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    mkdir -p /sys/fs/cgroup/init
    while read -r pid; do
        echo "$pid" > /sys/fs/cgroup/init/cgroup.procs 2>/dev/null || true
    done < /sys/fs/cgroup/cgroup.procs
    for c in $(cat /sys/fs/cgroup/cgroup.controllers); do
        echo "+$c" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
    done
fi

# --- start containerd in the background ---------------------------------------
/usr/local/bin/containerd --config /etc/containerd/config.toml &
CONTAINERD_PID=$!

# Wait for the CRI socket before launching the kubelet.
for _ in $(seq 1 50); do
    [ -S /run/containerd/containerd.sock ] && break
    sleep 0.2
done

# If containerd died, surface it.
if ! kill -0 "$CONTAINERD_PID" 2>/dev/null; then
    echo "containerd failed to start" >&2
    exit 1
fi

# --- run the kubelet in the foreground ----------------------------------------
# CONTAINER_RUNTIME_ENDPOINT defaults to the local socket; the kubelet talks to
# this node's own containerd.
exec /usr/local/bin/kubelet "$@"
