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

fn bench_memory_fanout(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (storage, key) = rt.block_on(seed_memory());

    let mut group = c.benchmark_group("watch_latency/memory_fanout");
    for n in [1usize, 10, 50] {
        group.bench_with_input(criterion::BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    // N live watchers, all subscribed before the measured write.
                    let mut streams = Vec::with_capacity(n);
                    for _ in 0..n {
                        streams.push(storage.watch(PREFIX).await.expect("watch"));
                    }
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        storage
                            .update::<serde_json::Value>(&key, &payload(i + 1))
                            .await
                            .expect("update");
                        // Every watcher must observe this write's event.
                        for s in streams.iter_mut() {
                            let _ = s.next().await.expect("event").expect("ok event");
                        }
                    }
                    start.elapsed()
                })
            });
        });
    }
    group.finish();
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_single(c: &mut Criterion) {
    use rusternetes_storage::{StorageBackend, StorageConfig};

    let rt = Runtime::new().expect("tokio runtime");
    // tempfile keeps the db out of the repo and auto-cleans on drop.
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("bench.sqlite");
    let key = format!("{PREFIX}obj");

    let storage = rt.block_on(async {
        let backend = StorageBackend::new(StorageConfig::Sqlite {
            path: db_path.to_string_lossy().into_owned(),
        })
        .await
        .expect("sqlite backend");
        backend
            .create::<serde_json::Value>(&key, &payload(0))
            .await
            .expect("seed create");
        Arc::new(backend)
    });

    let mut group = c.benchmark_group("watch_latency/sqlite");
    group.bench_function("single_watcher", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let mut stream = storage.watch(PREFIX).await.expect("watch");
                let start = std::time::Instant::now();
                for i in 0..iters {
                    storage
                        .update::<serde_json::Value>(&key, &payload(i + 1))
                        .await
                        .expect("update");
                    let _ = stream.next().await.expect("event").expect("ok event");
                }
                start.elapsed()
            })
        });
    });
    group.finish();
}

#[cfg(feature = "sqlite")]
criterion_group!(
    benches,
    bench_memory_single,
    bench_memory_fanout,
    bench_sqlite_single
);

#[cfg(not(feature = "sqlite"))]
criterion_group!(benches, bench_memory_single, bench_memory_fanout);

criterion_main!(benches);
