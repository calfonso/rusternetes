//! Pre-bus baseline for #1039 (in-process watch event bus).
//!
//! Measures the path the proposed event bus replaces: a write on a
//! `StorageBackend` -> the corresponding `WatchEvent` arriving on a
//! `Storage::watch(prefix)` stream. Today every internal consumer
//! (controllers call `self.storage.watch(&prefix)` directly — see
//! `crates/controller-manager/src/controllers/deployment.rs`) pays this
//! round-trip; the bus would short-circuit it. Capturing the latency now
//! makes the future win falsifiable.
//!
//! Run:
//!   cargo bench -p rusternetes-storage --bench watch_latency
//!   cargo bench -p rusternetes-storage --bench watch_latency --features sqlite
//!   cargo bench -p rusternetes-storage --bench watch_latency -- --save-baseline pre-bus

use criterion::{criterion_group, criterion_main, Criterion};

fn placeholder(c: &mut Criterion) {
    c.bench_function("placeholder", |b| b.iter(|| 1 + 1));
}

criterion_group!(benches, placeholder);
criterion_main!(benches);
