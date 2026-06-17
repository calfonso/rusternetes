# Shared CRI runtime for the rusternetes cluster: containerd (with the CRI
# plugin) driving Youki as the OCI runtime. The kubelets talk to this over CRI
# v1 instead of the Docker/Podman socket the old bollard kubelet used.
FROM debian:sid-slim

ARG CONTAINERD_VERSION=2.2.4
ARG RUNC_VERSION=1.2.6
ARG CNI_VERSION=1.6.2
ARG YOUKI_VERSION=0.6.0

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl iptables procps \
    && rm -rf /var/lib/apt/lists/*

# containerd (includes containerd, ctr, containerd-shim-runc-v2 under bin/).
RUN curl -fsSL "https://github.com/containerd/containerd/releases/download/v${CONTAINERD_VERSION}/containerd-${CONTAINERD_VERSION}-linux-amd64.tar.gz" \
      | tar -xz -C /usr/local

# runc (the containerd-runc-v2 shim's default; Youki is invoked via BinaryName).
RUN curl -fsSL -o /usr/local/sbin/runc \
      "https://github.com/opencontainers/runc/releases/download/v${RUNC_VERSION}/runc.amd64" \
    && chmod +x /usr/local/sbin/runc

# CNI plugins (bridge, host-local, portmap, loopback, …) for pod networking.
RUN mkdir -p /opt/cni/bin \
    && curl -fsSL "https://github.com/containernetworking/plugins/releases/download/v${CNI_VERSION}/cni-plugins-linux-amd64-v${CNI_VERSION}.tgz" \
      | tar -xz -C /opt/cni/bin

# Youki — the Rust OCI runtime the CRI runtime handler points at.
RUN curl -fsSL "https://github.com/youki-dev/youki/releases/download/v${YOUKI_VERSION}/youki-${YOUKI_VERSION}-x86_64-musl.tar.gz" \
      | tar -xz -C /tmp \
    && install -m 0755 "$(find /tmp -name youki -type f | head -1)" /usr/local/bin/youki \
    && rm -rf /tmp/youki*

COPY deploy/containerd/config.toml /etc/containerd/config.toml
COPY deploy/containerd/cni/10-rusternetes.conflist /etc/cni/net.d/10-rusternetes.conflist
COPY deploy/containerd/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# CRI gRPC socket the kubelets connect to (shared via a named volume).
VOLUME ["/run/containerd"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
