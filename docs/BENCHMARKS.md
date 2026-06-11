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

### Post-bus result (#1039 event bus): delivery latency

The bus's win is **event-propagation latency** — the time from a write
committing to an internal consumer observing the event — not the write itself
(the bus does not change backend write cost). `watch_delivery/sqlite` measures
this in isolation (the write is performed untimed; only delivery is timed):

| feed                         | delivery latency (median) |
|------------------------------|---------------------------|
| native rhino sqlite watch    | 124.07 µs |
| in-process bus (#1039)       | 603.66 ns |

The native backend watch delivers via a notify + SQLite re-query poll after each
write; the bus publishes synchronously inside the write, so the event is already
buffered when the write returns. That is a ~206x reduction in delivery
latency, matching the in-memory broadcast reference (`watch_latency/memory/single_watcher`
≈ 1.7 µs).

**Honest caveat — end-to-end write-to-observe is write-bound.** A single
write-then-observe round trip on embedded SQLite is dominated by the backend
write (~0.8–1 ms); the bus removes the ~124.07 µs delivery overhead on top of
that, so the single-writer end-to-end improvement is modest. The bus's larger
wins are (1) removing this per-consumer poll+re-query delivery overhead for
*every* internal watcher, and (2) eliminating ~35 redundant native watch
poll-loops in the all-in-one binary (CPU/memory), replaced by one O(1) broadcast
fan-out. External HTTP watch clients are unaffected — the watch_cache keeps the
native, RV-ordered feed via `StorageBackend::watch_backend`.

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
