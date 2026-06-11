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
use futures::StreamExt;
use rusternetes_storage::{MemoryStorage, Storage};
use std::sync::Arc;
use tokio::runtime::Runtime;

const PREFIX: &str = "/registry/pods/bench/";

/// A small resource-shaped JSON payload. `MemoryStorage::create` looks for a
/// `metadata` object to stamp uid/creationTimestamp, so mirror that shape.
fn payload(generation: u64) -> serde_json::Value {
    serde_json::json!({
        "metadata": { "name": "obj", "namespace": "bench", "generation": generation },
        "spec": { "value": generation }
    })
}

/// Build a memory backend with one key pre-seeded so the measured op is an
/// `update` (the controller hot path -> `Modified`), never a one-shot create.
async fn seed_memory() -> (Arc<MemoryStorage>, String) {
    let storage = Arc::new(MemoryStorage::new());
    let key = format!("{PREFIX}obj");
    storage
        .create::<serde_json::Value>(&key, &payload(0))
        .await
        .expect("seed create");
    (storage, key)
}

fn bench_memory_single(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (storage, key) = rt.block_on(seed_memory());

    let mut group = c.benchmark_group("watch_latency/memory");
    group.bench_function("single_watcher", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                // One live watcher subscribed before any measured write.
                let mut stream = storage.watch(PREFIX).await.expect("watch");
                let start = std::time::Instant::now();
                for i in 0..iters {
                    storage
                        .update::<serde_json::Value>(&key, &payload(i + 1))
                        .await
                        .expect("update");
                    // Await exactly the Modified event for this write.
                    let _ = stream.next().await.expect("event").expect("ok event");
                }
                start.elapsed()
            })
        });
    });
    group.finish();
}

criterion_group!(benches, bench_memory_single);
criterion_main!(benches);
