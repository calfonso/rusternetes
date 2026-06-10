# Multi-stage build for Rusternetes components
# This Dockerfile can build any component by specifying --build-arg COMPONENT=<name>

FROM rust:latest AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace manifest
COPY Cargo.toml Cargo.lock* ./

# Copy all crate manifests + source in one shot. No per-crate manifest
# pre-copy here: there is no intermediate `cargo build` before the source
# copy below, so a layered manifest cache buys nothing. (The cache-layered
# two-pass build lives in Dockerfile.services / Dockerfile.all-in-one, whose
# per-crate enumeration blocks must stay in sync with the workspace members.)
COPY crates ./crates

# Build for release
RUN cargo build --release

# Runtime stage
FROM debian:sid-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# This will be populated by the specific component Dockerfile
ARG COMPONENT
ENV COMPONENT=${COMPONENT}

# Copy the binary from builder
COPY --from=builder /app/target/release/${COMPONENT} /app/${COMPONENT}

# Expose default ports (these vary by component)
# API Server: 6443
# Others use etcd communication

# Run the component
ENTRYPOINT ["/bin/sh", "-c", "/app/${COMPONENT}"]
