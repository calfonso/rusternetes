# A rusternetes node-cdrs node (M1 spike): one container running containerd-rs
# (the Rust CRI runtime) + crun (the OCI runtime) AND the kubelet, kind-style.
#
# This mirrors Dockerfile.node, but swaps the runtime layer: instead of basing
# on the containerd (Dockerfile.containerd) image and its upstream
# containerd+youki, it builds a fresh debian base carrying containerd-rs + crun.
# The kubelet binary is reused unchanged from the same kubelet image
# Dockerfile.node consumes (/app/kubelet, built by Dockerfile.services'
# `kubelet` target).
#
# The containerd-rs binary is the static musl v0.1.3 build from the
# fix/k0s-worker-cri-gaps branch, staged into the build context at
# deploy/node-cdrs/bin/containerd-rs.
#
# Build (from the repo root):
#   docker build -f deploy/node-cdrs/Dockerfile -t rusternetes-node-cdrs:m1 .
ARG KUBELET_IMAGE=crs-cdrs-kubelet:latest

FROM ${KUBELET_IMAGE} AS kubeletbin

# Runtime base: deps containerd-rs / crun / CNI shell out to. crun is the OCI
# runtime; iproute2 + iptables back CNI / pod networking; ca-certificates backs
# registry pulls; procps for the entrypoint's process checks.
#
# debian:sid-slim (NOT bookworm) to match Dockerfile.containerd / the kubelet
# image's build base: the reused /app/kubelet binary is dynamically linked
# against sid's glibc (>= 2.38) and fails on bookworm's older glibc.
FROM debian:sid-slim

ARG CNI_VERSION=1.6.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates crun iproute2 iptables procps curl \
    && rm -rf /var/lib/apt/lists/*

# CNI plugins (loopback, etc.); flannel-rs installs its own conflist at runtime.
RUN mkdir -p /opt/cni/bin \
    && curl -fsSL "https://github.com/containernetworking/plugins/releases/download/v${CNI_VERSION}/cni-plugins-linux-amd64-v${CNI_VERSION}.tgz" \
      | tar -xz -C /opt/cni/bin \
    && rm -rf /var/lib/apt/lists/*

# containerd-rs invokes the OCI runtime as `runc` on PATH. /usr/local/bin
# precedes /usr/bin in the default PATH, so this symlink makes containerd-rs
# drive crun.
RUN ln -sf "$(command -v crun)" /usr/local/bin/runc

# The Rust CRI runtime (static musl v0.1.3), staged into the build context.
COPY --chmod=0755 deploy/node-cdrs/bin/containerd-rs /usr/local/bin/containerd-rs

# Reused, unmodified rusternetes kubelet binary.
COPY --from=kubeletbin /app/kubelet /usr/local/bin/kubelet

COPY deploy/node-cdrs/config.toml /etc/containerd-rs/config.toml
COPY deploy/node-cdrs/entrypoint.sh /usr/local/bin/node-cdrs-entrypoint.sh
RUN chmod +x /usr/local/bin/node-cdrs-entrypoint.sh

# Kubelet talks CRI to this node's own containerd-rs socket.
ENV CONTAINER_RUNTIME_ENDPOINT=unix:///run/containerd-rs.sock

# CRI socket + state dir (shared via a named volume if needed).
VOLUME ["/run/containerd-rs"]

ENTRYPOINT ["/usr/local/bin/node-cdrs-entrypoint.sh"]
