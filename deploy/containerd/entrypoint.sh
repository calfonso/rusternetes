#!/bin/sh
# Entrypoint for the rusternetes containerd (CRI runtime) service.
set -e

# containerd's CRI plugin starts a CNI-conf-dir fsnotify watcher; the default
# fs.inotify.max_user_instances (often 128) is too low and the plugin fails to
# load with "too many open files". Raise it (the container is privileged).
sysctl -w fs.inotify.max_user_instances=1024 >/dev/null 2>&1 || true
sysctl -w fs.inotify.max_user_watches=1048576 >/dev/null 2>&1 || true

exec /usr/local/bin/containerd --config /etc/containerd/config.toml
