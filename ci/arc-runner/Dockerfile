# Custom ARC runner image for the rusternetes self-hosted CI scale set.
#
# Pre-bakes:
# - the Rust stable toolchain (rustup + clippy + rustfmt)
# - cargo-nextest binary
# - system build deps the workspace needs (protoc + well-known protos,
#   mold + clang, pkg-config + libssl-dev, jq used by fmt.yml)
#
# Without this image every job spends ~1-2 min on apt-get + rustup
# install. Bake once, reuse per job.
#
# Build + push happens via .github/workflows/build-runner-image.yml on
# push to main or when this Dockerfile changes. The arc-runner-set
# scale set's helm release pins this image via ci/arc-runner/values.yaml.
FROM ghcr.io/actions/actions-runner:latest

USER root

RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends \
      protobuf-compiler libprotobuf-dev \
      mold clang \
      pkg-config libssl-dev \
      build-essential \
      jq curl ca-certificates git \
 && rm -rf /var/lib/apt/lists/*

USER runner
ENV RUSTUP_HOME=/home/runner/.rustup \
    CARGO_HOME=/home/runner/.cargo \
    PATH=/home/runner/.cargo/bin:/usr/local/bin:/usr/bin:/bin

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal \
        --component clippy --component rustfmt \
 && rustup --version \
 && cargo --version \
 && rustc --version \
 && rustfmt --version \
 && cargo clippy --version

# cargo-nextest binary, prebuilt from the official release feed
RUN curl -LsSf https://get.nexte.st/latest/linux \
      | tar zxf - -C /home/runner/.cargo/bin \
 && cargo nextest --version

# docker-compose v2 plugin. gha-runner-scale-set's stock DinD bundle
# ships the docker CLI but not the compose plugin, so the conformance
# tier (which brings up the rusternetes cluster via `docker compose
# up`) needs the binary on PATH. Drop it into the runner-user's local
# plugin directory so no root and no global $PATH mutation is needed.
# Pinned to a known release tag; bumps land here, not in workflow files.
ENV DOCKER_COMPOSE_VERSION=v2.30.3
RUN mkdir -p /home/runner/.docker/cli-plugins \
 && curl -fsSL \
      "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64" \
      -o /home/runner/.docker/cli-plugins/docker-compose \
 && chmod +x /home/runner/.docker/cli-plugins/docker-compose \
 && /home/runner/.docker/cli-plugins/docker-compose version

# sccache — shared rustc compilation cache. The CI jobs set
# RUSTC_WRAPPER=sccache + SCCACHE_* (→ the in-cluster MinIO) via the ARC
# runner pod env (indy/arc-runner/values-*.yaml), so the binary MUST exist on
# PATH or every `cargo build`/`clippy`/`nextest` fails with "RUSTC_WRAPPER
# not found". The musl build is statically linked and portable across the
# runner base image. Pinned; bumps land here, not in workflow files.
ENV SCCACHE_VERSION=v0.8.2
RUN curl -fsSL \
      "https://github.com/mozilla/sccache/releases/download/${SCCACHE_VERSION}/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
      | tar xz -C /tmp \
 && mv "/tmp/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl/sccache" /home/runner/.cargo/bin/sccache \
 && chmod +x /home/runner/.cargo/bin/sccache \
 && rm -rf "/tmp/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl" \
 && sccache --version
