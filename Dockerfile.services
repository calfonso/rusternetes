# syntax=docker/dockerfile:1.6
#
# Combined Dockerfile for all five long-running rusternetes service
# components: api-server, kubelet, scheduler, controller-manager,
# kube-proxy.
#
# Why a single file instead of one per service:
#
# The five legacy Dockerfile.{api-server,kubelet,scheduler,
# controller-manager,kube-proxy} files were 90% identical, and each
# kicked off its own `cargo build --release --bin <name>` against the
# whole workspace. Because BuildKit cache mounts are scoped per
# Dockerfile *target*, the cargo registry / git / target caches were
# not shared across the five images either — a cold `docker compose
# build` therefore compiled the same dependency graph 5x, blowing the
# 30-min conformance-canary timeout (run 26126002894).
#
# Collapsing into one `cargo-builder` stage that produces all five
# binaries in a single `cargo build` invocation lets BuildKit dedupe
# the work: one compile, five thin runtime images that each `COPY
# --from=cargo-builder` the binary they need. The runtime stages
# differ only in their apt-package set and ENTRYPOINT/CMD.
#
# Compose selects the target stage via `build.target: <name>`.
# Cargo features can be plumbed through with `--build-arg
# CARGO_FEATURES=...` (e.g. `--features rusternetes-storage/sqlite`).
#
# ---- TWO-STAGE CARGO BUILD ----
#
# The cargo-builder stage uses a two-pass cargo build to maximise
# layer cache reuse:
#
#   Pass 1: COPY only Cargo.toml + Cargo.lock + build.rs + proto/ +
#           dummy src/lib.rs / src/main.rs files for every crate.
#           Run `cargo build --profile release-fast ... <bins>`. This
#           compiles the entire dependency graph (the dominant cost —
#           hundreds of crates from crates.io) into the cache-mounted
#           target/.
#           The Docker layer that caches this RUN is invalidated only
#           when a Cargo.toml, Cargo.lock, build.rs, or proto/ file
#           changes — which is rare.
#
#   Pass 2: COPY the real src/ + tests/ contents. `touch` every
#           Cargo.toml so cargo's mtime-based change detection picks
#           up the new source. Run cargo build again — this only
#           recompiles workspace crates whose source actually changed
#           plus the final link step. The dep graph is reused.
#
# Net effect: a leaf-crate edit goes from ~18 min (rebuild whole dep
# graph) to <5 min (recompile just that crate + link).
#
# ---- ADDING A NEW WORKSPACE CRATE ----
#
# When you add a new crate under `crates/<n>/`, update THREE blocks
# below labelled `# CRATE-ENUMERATION:` — they MUST stay in sync with
# the `[workspace] members` list in Cargo.toml. Each block has a
# one-line comment listing every crate so the matching set is easy
# to audit. Forgetting to update them will not always fail loudly:
# the build may succeed by accident if your new crate has no deps
# unique to itself, then break the moment someone changes its source.
#
# Current workspace members (Cargo.toml [workspace] members):
#   api-server, cloud-providers, common, controller-manager, dns,
#   kubectl, kubelet, kube-proxy, netstack, rusternetes, scheduler,
#   storage

# ---------------------------------------------------------------------
# Stage 1: console SPA build (only api-server uses the output, but
# defining it here keeps the file self-contained).
# ---------------------------------------------------------------------
FROM node:26-slim AS console-builder
WORKDIR /console
COPY console/package.json console/package-lock.json* ./
RUN npm ci --ignore-scripts
COPY console/ ./
RUN npm run build

# ---------------------------------------------------------------------
# Stage 2: shared cargo builder — compiles ALL service binaries in a
# single cargo invocation so dependency compile time is paid once.
# ---------------------------------------------------------------------
# Pin rust toolchain explicitly so the BuildKit cache mounts at
# `/usr/local/cargo/registry` / `/usr/local/cargo/git` / `/app/target`
# survive across rebuilds. With `rust:latest` the `target/` cache is
# silently invalidated every time upstream rolls a new minor version
# (each rustc has its own ABI-stable artifact directory layout), so
# a Wednesday `docker compose build` after a Tuesday Rust release
# pays the full ~25 min uncached cost. Bump this pin in a deliberate
# PR when the toolchain genuinely needs to move; do not chase nightly.
FROM rust:1.95 AS cargo-builder

# sccache wraps rustc so identical (crate, source, flags) compilations
# hit the BuildKit cache mount below instead of re-running rustc.
# Across ten workspace crates + ~300 deps this drops a cold rebuild from
# ~25 min to ~6 min on a warm cache. The cache id is shared with the
# kubectl and all-in-one Dockerfiles so the api-server build here warms
# the cache for those too. The sccache binary itself is downloaded once
# from upstream releases — installing via `cargo install` would re-compile
# a 100+ crate dep tree.
ARG SCCACHE_VERSION=v0.8.2
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://github.com/mozilla/sccache/releases/download/${SCCACHE_VERSION}/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
        | tar -xz -C /tmp \
    && install -m 0755 "/tmp/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl/sccache" /usr/local/bin/sccache \
    && rm -rf "/tmp/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl"

# sccache vs cargo-incremental — chosen per-build via USE_SCCACHE (set in
# the cargo RUN blocks below), NOT baked into ENV. Measured on this repo's
# cargo-builder stage (all 6 service bins):
#
#   build                       cold        incremental (1 leaf-crate edit)
#   ----------------------      --------    -------------------------------
#   sccache + INCREMENTAL=0     459s        39s   (cargo 36.2s, 0% sccache hits)
#   no-sccache + INCREMENTAL=1  486s         4s   (cargo  1.9s)
#
# sccache wraps rustc so identical (crate, source, flags) compilations hit
# the /sccache BuildKit mount instead of re-running rustc. But that mount is
# single-machine: on a persistent dev box cargo's own fingerprinting already
# skips unchanged crates for free, and forcing CARGO_INCREMENTAL=0 (sccache
# requires it) makes every dirty-crate rebuild pay full codegen — ~10x slower
# iteration. On the ephemeral ARC DinD runner the mount is cold every job
# (0% hits), so sccache buys nothing there either; the canary build is always
# cold (~tie). Net: sccache only helps a SHARED/distributed cache, which this
# docker path does not have (the sccache->MinIO backend is host-cargo only —
# clippy/nextest via RUSTC_WRAPPER, never this build).
#
# So: default ON for safety/parity (USE_SCCACHE=1) — CI keeps the exact prior
# behavior — but compose.sqlite.yml flips it OFF locally (USE_SCCACHE="" when
# GITHUB_ACTIONS is unset), giving the dev box incremental's ~10x iteration win.
# Override anywhere with `USE_SCCACHE=1 docker compose ... build`.
ARG USE_SCCACHE=1

# Static sccache config (only consulted when USE_SCCACHE=1 turns the wrapper
# on inside the cargo RUN blocks). RUSTC_WRAPPER / CARGO_INCREMENTAL are set
# there, not here, so the two modes don't leak into each other.
ENV SCCACHE_DIR=/sccache \
    SCCACHE_CACHE_SIZE=20G \
    SCCACHE_IDLE_TIMEOUT=0

WORKDIR /app

# Pull in the rhino submodule (named build context in compose.yml, pointed at
# the in-tree `./rhino` submodule). crates/storage/Cargo.toml declares
# `rhino = { path = "../../rhino", ... }` which from /app/crates/storage
# resolves to /app/rhino, so land it there.
COPY --from=rhino . /app/rhino

# ----- PASS 1: dependency-only compile (cache-friendly) -----

# Workspace manifest + lockfile (rare changes, dep-graph layer cache
# survives across ordinary source edits).
COPY Cargo.toml Cargo.lock* ./

# CRATE-ENUMERATION (1/3): one COPY per crate's Cargo.toml.
# Must mirror `[workspace] members` in Cargo.toml.
# Crates: api-server, cloud-providers, common, controller-manager, dns,
#         kubectl, kubelet, kube-proxy, netstack, rusternetes, scheduler,
#         storage
COPY crates/admission-webhook/Cargo.toml  crates/admission-webhook/Cargo.toml
COPY crates/api-server/Cargo.toml         crates/api-server/Cargo.toml
COPY crates/cloud-providers/Cargo.toml    crates/cloud-providers/Cargo.toml
COPY crates/client/Cargo.toml             crates/client/Cargo.toml
COPY crates/common/Cargo.toml             crates/common/Cargo.toml
COPY crates/controller-manager/Cargo.toml crates/controller-manager/Cargo.toml
COPY crates/cri/Cargo.toml                crates/cri/Cargo.toml
COPY crates/discovery/Cargo.toml          crates/discovery/Cargo.toml
COPY crates/dns/Cargo.toml                crates/dns/Cargo.toml
COPY crates/kubectl/Cargo.toml            crates/kubectl/Cargo.toml
COPY crates/kubelet/Cargo.toml            crates/kubelet/Cargo.toml
COPY crates/kube-proxy/Cargo.toml         crates/kube-proxy/Cargo.toml
COPY crates/middleware/Cargo.toml         crates/middleware/Cargo.toml
COPY crates/netstack/Cargo.toml           crates/netstack/Cargo.toml
COPY crates/protobuf/Cargo.toml           crates/protobuf/Cargo.toml
COPY crates/rusternetes/Cargo.toml        crates/rusternetes/Cargo.toml
COPY crates/scheduler/Cargo.toml          crates/scheduler/Cargo.toml
COPY crates/storage/Cargo.toml            crates/storage/Cargo.toml
COPY crates/streamproxy/Cargo.toml        crates/streamproxy/Cargo.toml
COPY crates/test_support/Cargo.toml       crates/test_support/Cargo.toml

# build.rs scripts + proto/ files belong with the manifests because
# they participate in dependency resolution / codegen during the
# Pass-1 build. common/build.rs stamps the version/SHA; api-server/build.rs
# compiles protos.
COPY crates/common/build.rs     crates/common/build.rs
COPY crates/api-server/build.rs crates/api-server/build.rs
COPY crates/api-server/proto    crates/api-server/proto
# crates/cri/build.rs runs tonic-build over the vendored CRI proto at compile
# time, so the proto must be present from Pass-1.
COPY crates/cri/build.rs        crates/cri/build.rs
COPY crates/cri/proto           crates/cri/proto

# CRATE-ENUMERATION (2/3): dummy lib.rs / main.rs per crate. Lib vs
# bin choice MUST match each Cargo.toml's [lib] + [[bin]] entries.
#   - lib only:   common, storage, cloud-providers, netstack, cri, test_support
#   - bin only:   kubectl, rusternetes
#   - lib + bin:  api-server, controller-manager, dns, kubelet, kube-proxy,
#                 scheduler
# `echo "fn main(){}"` in every main.rs; empty lib.rs is fine.
RUN set -eux; \
    for c in client common storage cloud-providers netstack protobuf middleware admission-webhook discovery cri streamproxy test_support; do \
        mkdir -p crates/$c/src && : > crates/$c/src/lib.rs; \
    done; \
    for c in kubectl rusternetes; do \
        mkdir -p crates/$c/src && echo "fn main(){}" > crates/$c/src/main.rs; \
    done; \
    for c in api-server controller-manager dns kubelet kube-proxy scheduler; do \
        mkdir -p crates/$c/src && \
        : > crates/$c/src/lib.rs && \
        echo "fn main(){}" > crates/$c/src/main.rs; \
    done

# Dummy bench files for crates that declare [[bench]] in Cargo.toml.
# Cargo fails to parse the manifest if the referenced bench file is
# missing — the Pass-1 dummy-source trick above doesn't create benches,
# so any `[[bench]] name = "..."` entry would break the dep-only build
# without an explicit dummy here. Keep this list in sync with every
# crate's [[bench]] declarations.
RUN mkdir -p crates/common/benches \
 && echo "fn main(){}" > crates/common/benches/regex_cache.rs \
 && mkdir -p crates/storage/benches \
 && echo "fn main(){}" > crates/storage/benches/watch_latency.rs

ARG CARGO_FEATURES=""

# Pass 1: compile the dep graph using dummy sources. The point of
# this RUN is to populate the cache-mounted target/ with every
# external dependency's .rlib so Pass 2 only has to recompile the
# workspace crates whose source actually changed. BuildKit invalidates
# this layer only when one of the COPYs above (manifests / build.rs /
# proto/) changes, which is rare.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/sccache,id=sccache-rusternetes,sharing=locked \
    if [ "$USE_SCCACHE" = "1" ]; then \
        export RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0; \
    else \
        export CARGO_INCREMENTAL=1; \
    fi; \
    cargo build --profile release-fast $CARGO_FEATURES \
        --bin api-server \
        --bin kubelet \
        --bin scheduler \
        --bin controller-manager \
        --bin kube-proxy \
        --bin rusternetes-dns \
 && cargo clean --profile release-fast \
        -p rusternetes-admission-webhook \
        -p rusternetes-api-server \
        -p rusternetes-client \
        -p rusternetes-cloud-providers \
        -p rusternetes-common \
        -p rusternetes-cri \
        -p rusternetes-middleware \
        -p rusternetes-protobuf \
        -p rusternetes-controller-manager \
        -p rusternetes-discovery \
        -p rusternetes-dns \
        -p rusternetes-kubectl \
        -p rusternetes-kubelet \
        -p rusternetes-kube-proxy \
        -p rusternetes \
        -p rusternetes-scheduler \
        -p rusternetes-storage \
        -p rusternetes-streamproxy \
 && { [ "$USE_SCCACHE" = "1" ] && sccache --show-stats || true; }

# ----- PASS 2: real source compile (only changed workspace crates rebuild) -----

# CRATE-ENUMERATION (3/3): copy each crate's real src/ (and tests/
# when present). Splitting per crate keeps BuildKit content-hash
# invalidation scoped: editing crates/kubelet/src/foo.rs only
# invalidates the kubelet COPY layers, leaving the other nine
# untouched. Tests/ aren't strictly required for the release build
# but are kept so future `cargo test` inside this image works
# without an extra COPY pass.
COPY crates/admission-webhook/src    crates/admission-webhook/src
COPY crates/api-server/src           crates/api-server/src
COPY crates/api-server/tests         crates/api-server/tests
COPY crates/cloud-providers/src      crates/cloud-providers/src
COPY crates/middleware/src           crates/middleware/src
COPY crates/protobuf/src             crates/protobuf/src
COPY crates/client/src               crates/client/src
COPY crates/common/src               crates/common/src
COPY crates/common/tests             crates/common/tests
COPY crates/controller-manager/src   crates/controller-manager/src
COPY crates/controller-manager/tests crates/controller-manager/tests
COPY crates/cri/src                  crates/cri/src
COPY crates/discovery/src            crates/discovery/src
COPY crates/dns/src                  crates/dns/src
COPY crates/kubectl/src              crates/kubectl/src
COPY crates/kubectl/tests            crates/kubectl/tests
COPY crates/kubelet/src              crates/kubelet/src
COPY crates/kubelet/tests            crates/kubelet/tests
COPY crates/kube-proxy/src           crates/kube-proxy/src
COPY crates/kube-proxy/tests         crates/kube-proxy/tests
COPY crates/netstack/src             crates/netstack/src
COPY crates/rusternetes/src          crates/rusternetes/src
COPY crates/scheduler/src            crates/scheduler/src
COPY crates/scheduler/tests          crates/scheduler/tests
COPY crates/storage/src              crates/storage/src
COPY crates/streamproxy/src          crates/streamproxy/src
# test_support: the in-process api-server test harness. A workspace member, so
# its manifest is required for `cargo` to resolve the workspace even though the
# release bins don't depend on it (dev-dependency); src is copied so in-image
# `cargo test` works (#1202 added the crate; the build broke until it was
# enumerated here — keep in sync with the [workspace] members list).
COPY crates/test_support/src         crates/test_support/src

# Rebuild with real sources. Pass 1 already wiped the dummy workspace
# artefacts via `cargo clean -p`, so cargo compiles every workspace
# crate from the real src copied in above without trusting any cached
# dummy rlib. (Previous iterations of this Dockerfile used a
# `find … -exec touch` hack to force rebuild — that worked but
# invalidated ALL workspace crates on every edit, even unchanged ones.
# The cleanup-after-Pass-1 approach scopes the rebuild to genuinely
# changed crates.)
#
# The binaries are copied out of the cache-mounted target/ into /out
# so the runtime stages can pick them up (cache-mount contents are
# discarded after the RUN finishes — they only persist across builds,
# not into the image layer).
#
# Inject the git SHA here (Pass 2 only) so common's build.rs bakes the real
# commit into every binary — `.git` is dockerignored, so without this the
# version banner would read "unknown". Scoped to Pass 2 to keep the Pass-1
# dependency cache from busting on every commit.
ARG RUSTERNETES_GIT_SHA=""
ENV RUSTERNETES_GIT_SHA=${RUSTERNETES_GIT_SHA}
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/sccache,id=sccache-rusternetes,sharing=locked \
    if [ "$USE_SCCACHE" = "1" ]; then \
        export RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0; \
    else \
        export CARGO_INCREMENTAL=1; \
    fi; \
    cargo build --profile release-fast $CARGO_FEATURES \
        --bin api-server \
        --bin kubelet \
        --bin scheduler \
        --bin controller-manager \
        --bin kube-proxy \
        --bin rusternetes-dns \
 && { [ "$USE_SCCACHE" = "1" ] && sccache --show-stats || true; } \
 && mkdir -p /out \
 && cp target/release-fast/api-server         /out/api-server \
 && cp target/release-fast/kubelet            /out/kubelet \
 && cp target/release-fast/scheduler          /out/scheduler \
 && cp target/release-fast/controller-manager /out/controller-manager \
 && cp target/release-fast/kube-proxy         /out/kube-proxy \
 && cp target/release-fast/rusternetes-dns    /out/rusternetes-dns

# ---------------------------------------------------------------------
# Stage 3: api-server runtime.
# ---------------------------------------------------------------------
FROM debian:sid-slim AS api-server

# iproute2: the entrypoint installs a route to the pod CIDR via the containerd
# container so the pod/service proxy subresources can reach pod IPs (see
# deploy/api-server/entrypoint.sh). Needs cap_add: NET_ADMIN at runtime.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=cargo-builder /out/api-server /app/api-server
COPY --from=console-builder /console/dist /app/console
COPY deploy/api-server/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 6443

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--bind-address", "0.0.0.0:6443", "--etcd-servers", "http://etcd:2379"]

# ---------------------------------------------------------------------
# Stage 4: kubelet runtime. Needs CNI plugins on /opt/cni/bin and the
# rusternetes CNI conflist on /etc/cni/net.d/.
# ---------------------------------------------------------------------
FROM debian:sid-slim AS kubelet

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    iproute2 \
    iptables \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install CNI plugins. Use `--retry` to survive transient GitHub
# download resets — an earlier canary build hit `curl: (35) Recv
# failure: Connection reset by peer` 150s into this URL because plain
# `curl -L` does not retry on connection-level errors. Download to a
# temp file rather than piping to tar so curl's non-zero exit is not
# masked by tar's success on a truncated stream.
RUN mkdir -p /opt/cni/bin && \
    ARCH=$(dpkg --print-architecture) && \
    curl -fSL --retry 5 --retry-all-errors --retry-delay 5 \
        https://github.com/containernetworking/plugins/releases/download/v1.4.0/cni-plugins-linux-${ARCH}-v1.4.0.tgz \
        -o /tmp/cni-plugins.tgz && \
    tar -C /opt/cni/bin -xzf /tmp/cni-plugins.tgz && \
    rm /tmp/cni-plugins.tgz

RUN mkdir -p /etc/cni/net.d
COPY cni-config/10-rusternetes.conflist /etc/cni/net.d/

COPY --from=cargo-builder /out/kubelet /app/kubelet

# Entrypoint installs a route to the pod CIDR via the containerd container so
# the kubelet can dial pod IPs for probes + HTTP lifecycle hooks (mirrors the
# api-server entrypoint). iproute2 is installed above; needs NET_ADMIN /
# privileged (the compose kubelet is privileged).
COPY deploy/kubelet/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# NOTE: do NOT declare `VOLUME ["/var/run"]` here. The container runtime
# socket is provided by the compose *bind* mount (/run/.../podman.sock or
# /var/run/docker.sock), not by a Docker volume. Declaring /var/run a VOLUME
# makes Docker create a fresh anonymous volume at /var/run on every container
# start; the bind-mounted sockets then live inside that volume, and with the
# rshared KUBELET_VOLUMES_PATH bind joining the host's shared mount peer group,
# each kubelet restart recursively copies the socket mounts into the
# accumulating anonymous volumes' _data dirs — a 2^n host mount-table explosion
# that exhausts fs.mount-max during crash-loops. See #66.

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--node-name", "node-1", "--etcd-servers", "http://etcd:2379"]

# ---------------------------------------------------------------------
# Stage 5: scheduler runtime.
# ---------------------------------------------------------------------
FROM debian:sid-slim AS scheduler

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=cargo-builder /out/scheduler /app/scheduler

ENTRYPOINT ["/app/scheduler"]
CMD ["--etcd-servers", "http://etcd:2379"]

# ---------------------------------------------------------------------
# Stage 6: controller-manager runtime.
# ---------------------------------------------------------------------
FROM debian:sid-slim AS controller-manager

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=cargo-builder /out/controller-manager /app/controller-manager

ENTRYPOINT ["/app/controller-manager"]
CMD ["--etcd-servers", "http://etcd:2379"]

# ---------------------------------------------------------------------
# Stage 7: kube-proxy runtime. Needs iptables for service routing, and kmod
# (modprobe) to load br_netfilter on startup so bridge-nf-call-iptables can be
# enabled — required for NodePort traffic to a node's bridge IP to hit the
# host-netns DNAT rules. The host module tree is bind-mounted at /lib/modules.
# ---------------------------------------------------------------------
FROM debian:sid-slim AS kube-proxy

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    iptables \
    iproute2 \
    kmod \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=cargo-builder /out/kube-proxy /app/kube-proxy

ENTRYPOINT ["/app/kube-proxy"]
CMD ["--node-name", "node-1"]

# ---------------------------------------------------------------------
# Stage 8: dns runtime. Serves cluster.local on UDP+TCP/53, replaces
# the CoreDNS pod from bootstrap-cluster.yaml. Stays a separate stage
# rather than bundling into the api-server / kube-proxy images so the
# blast radius of a DNS-only restart is one container, matching the
# CoreDNS pattern it replaces.
# ---------------------------------------------------------------------
FROM debian:sid-slim AS dns

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=cargo-builder /out/rusternetes-dns /app/rusternetes-dns

# 53 is privileged on Linux. The compose entry adds NET_BIND_SERVICE
# (or runs as root) so this works without sysctl knobs.
EXPOSE 53/udp
EXPOSE 53/tcp

ENTRYPOINT ["/app/rusternetes-dns"]
# No CMD: the in-cluster Deployment runs arg-free so the binary
# autodetects ClientConfig::in_cluster(); a baked-in --etcd-servers
# default would defeat that detection (clap sees an explicit flag).
