# Performance & Footprint

The "k3s without melting your laptop" thesis rests on a number nobody had
measured. This page is the home for those numbers and how to reproduce them.

> Status: harness landed (`scripts/footprint-benchmark.sh`). The binary-size row
> below is a **real measurement** (release host target, glibc). The idle RSS /
> CPU / time-to-cluster cells are placeholders until run on reference hardware
> head-to-head against k3s — run the harness and replace the `TBD` cells.

## Why this matters

Footprint — idle RAM in particular — is the north-star for the lightweight-distro
positioning (see `indy/ROADMAP.md`). Two structural bets depend on having a
measurable baseline first:

- **In-process watch event bus (#1039)** — replace the apiserver-watch-cache
  HTTP hop for in-process consumers; expected to cut idle apiserver memory
  (watch-cache ring buffers). The before/after is meaningless without this
  baseline.
- **Adaptive / event-driven reconcile (#1040)** — cut idle CPU from controller
  polling.

Risk noted in #1038: `etcd-client`, `bollard`, `smoltcp`, and full `tokio` are
compiled into the all-in-one unconditionally, so the current footprint may not
yet beat k3s. **Measure before claiming.**

## Competitive bar (idle RAM)

| Distro | Idle RAM |
|---|---|
| k3s | ~535–750 MB (≈1–1.2 GB without `GOMEMLIMIT`) |
| k0s | ~658 MB |
| microk8s (HA off) | ~526 MB |
| OS baseline | ~167 MB |

Target to claim the niche: **sub-400 MB idle** (stretch 250–300 MB).

## Dimensions

1. **Binary size** — release `rusternetes` all-in-one, raw + stripped (and a
   `musl` static variant).
2. **Time-to-cluster** — `compose up` → first node `Ready`.
3. **Idle control-plane RSS** — single node, default CNI + DNS, no workload.
4. **Idle CPU %** — same window.

## Reproduce

```bash
# Everything (builds release, boots compose.all-in-one.yml, samples idle):
bash scripts/footprint-benchmark.sh

# Binary size only (no Docker needed):
bash scripts/footprint-benchmark.sh --size-only

# Longer idle sample window:
bash scripts/footprint-benchmark.sh --seconds 60
```

Process-level idle RSS of the all-in-one binary (used by the #1039 before/after)
is also available standalone:

```bash
cargo build --release -p rusternetes
scripts/bench-idle-memory.sh --seconds 60
```

For the musl static binary:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release -p rusternetes --target x86_64-unknown-linux-musl
strip target/x86_64-unknown-linux-musl/release/rusternetes
```

## Results

Reference hardware: _TBD (record CPU/RAM/OS when filling this in)_.

| Metric | rusternetes | k3s | Notes |
|---|---|---|---|
| Binary size (release, glibc) | **75.1 MiB** | — | dynamically linked; the release profile already strips symbols, so raw == stripped |
| Binary size (musl+strip) | TBD | ~70 MB | static |
| Time-to-cluster | TBD | TBD | `up` → first node Ready |
| Idle RSS (avg) | TBD | ~535–750 MB | all-in-one container |
| Idle RSS (max) | TBD | — | |
| Idle CPU (avg) | TBD | TBD | |

The 75 MiB all-in-one binary is in the same ballpark as k3s's static binary
(~70 MB) — but binary size is the *easy* dimension. The thesis lives or dies on
**idle RSS** (the `TBD` rows), which is why #1039/#1040 target it; measure those
before claiming the niche.

_Fill the remaining `TBD` cells from a `scripts/footprint-benchmark.sh` run plus
a k3s run on the same box._
