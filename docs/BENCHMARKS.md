# Benchmarks

Performance baselines for rusternetes. These exist so optimization work can show
a measured delta, not a claim.

## Watch event-bus baseline (#1039)

#1039 proposes replacing the intra-process watch path (`Storage::watch` backend
round-trip) with an in-process Tokio event bus. These benches capture the
**pre-bus** baseline so that PR can prove its latency and memory wins.

### Event-propagation latency

Measures `update` -> `WatchEvent::Modified` arrival on a `Storage::watch`
stream. `single_watcher` is the core latency; `memory_fanout` (N=1/10/50)
captures per-watch delivery cost.

```bash
# memory only (no submodule needed)
cargo bench -p rusternetes-storage --bench watch_latency

# include embedded-sqlite (the CI/conformance backend)
git submodule update --init rhino
cargo bench -p rusternetes-storage --bench watch_latency --features sqlite

# save a named baseline, then compare after a change
cargo bench -p rusternetes-storage --bench watch_latency --features sqlite -- --save-baseline pre-bus
cargo bench -p rusternetes-storage --bench watch_latency --features sqlite -- --baseline pre-bus
```

Pre-bus baseline (captured 2026-06-11, local dev workstation; memory + embedded-sqlite backends):

| case                              | median |
|-----------------------------------|--------|
| memory / single_watcher           | 1.7212 µs |
| memory_fanout / 1                 | 1.6594 µs |
| memory_fanout / 10                | 2.8014 µs |
| memory_fanout / 50                | 7.5003 µs |
| sqlite / single_watcher           | 962.27 µs |

The embedded-sqlite round-trip is ~two orders of magnitude slower than the
in-memory broadcast path — that gap is what the in-process event bus targets for
internal consumers.

### Idle resident memory

The watch-cache ring buffers the bus would shrink live in the apiserver process;
its idle VmRSS is the before/after number.

```bash
cargo build --release -p rusternetes
scripts/bench-idle-memory.sh                 # boot + sample 30s
scripts/bench-idle-memory.sh --pid <N>       # sample a running process (e.g. the api-server container)
```

Pre-bus baseline (captured 2026-06-11, local dev workstation): the idle
`rusternetes-api-server` container (multi-container sqlite stack) held
**avg 194 MiB** VmRSS (min 194 / max 194 over the window); the rhino storage
backend container was ~76 MiB at the same time. #1039 estimates 30-60% of idle
apiserver memory is watch-cache ring buffers — i.e. roughly 58-116 MiB is the
reclaimable target.

### Notes
- etcd backend latency is not benched (needs a container); add later if needed.
- The sqlite/rhino watch stream does not replay the seeded key as a priming
  event, so no warm-up drain is needed before timing.
- The idle-RSS number was sampled from the running api-server container via
  `--pid` rather than booting the all-in-one binary; either gives the same
  apiserver-process VmRSS that the bus would shrink.
