#!/bin/sh
# Entrypoint for a rusternetes node-cdrs node: one container running
# containerd-rs (the Rust CRI runtime) + crun (OCI runtime) AND the kubelet,
# kind-style. Mirrors deploy/node/entrypoint.sh but launches containerd-rs
# instead of upstream containerd, and waits on /run/containerd-rs.sock instead
# of /run/containerd/containerd.sock. Bundling runtime + kubelet in one
# container keeps a shared filesystem for flannel's CNI install and pod
# hostPath volumes.
set -e

# --- runtime prerequisites (see deploy/node/entrypoint.sh) --------------------
sysctl -w fs.inotify.max_user_instances=1024 >/dev/null 2>&1 || true
sysctl -w fs.inotify.max_user_watches=1048576 >/dev/null 2>&1 || true

# cgroup v2 nesting fix (kind/k3d): move our processes into a leaf so controllers
# can be delegated, else the OCI runtime fails with "+io ... Not supported".
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    mkdir -p /sys/fs/cgroup/init
    while read -r pid; do
        echo "$pid" > /sys/fs/cgroup/init/cgroup.procs 2>/dev/null || true
    done < /sys/fs/cgroup/cgroup.procs
    for c in $(cat /sys/fs/cgroup/cgroup.controllers); do
        echo "+$c" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
    done
fi

# --- start containerd-rs in the background ------------------------------------
/usr/local/bin/containerd-rs --config /etc/containerd-rs/config.toml &
RUNTIME_PID=$!

# Wait for the CRI socket before launching the kubelet.
for _ in $(seq 1 50); do
    [ -S /run/containerd-rs.sock ] && break
    sleep 0.2
done

# If containerd-rs died, surface it.
if ! kill -0 "$RUNTIME_PID" 2>/dev/null; then
    echo "containerd-rs failed to start" >&2
    exit 1
fi

# --- run the kubelet in the foreground ----------------------------------------
# CONTAINER_RUNTIME_ENDPOINT (set in the image) points the kubelet at this
# node's own containerd-rs CRI socket.
exec /usr/local/bin/kubelet "$@"
