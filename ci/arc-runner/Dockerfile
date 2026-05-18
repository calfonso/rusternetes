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
