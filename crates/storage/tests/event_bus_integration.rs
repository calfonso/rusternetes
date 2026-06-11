//! All-in-one shape: one shared bus-enabled StorageBackend. Internal consumers
//! via `watch()` get the bus; the watch_cache-style `watch_backend()` consumer
//! gets the native, ordered feed. Both observe the same write. (#1039)
#![cfg(feature = "sqlite")]

use futures::StreamExt;
use rusternetes_storage::{Storage, StorageBackend, StorageConfig};
use std::sync::Arc;

#[tokio::test]
async fn bus_and_native_feeds_both_see_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("aio.sqlite").to_string_lossy().into_owned();
    std::mem::forget(dir);
    let mut backend = StorageBackend::new(StorageConfig::Sqlite { path })
        .await
        .unwrap();
    backend.enable_event_bus();
    let storage = Arc::new(backend);

    let mut internal = storage.watch("/registry/configmaps/").await.unwrap();
    let mut native = storage
        .watch_backend("/registry/configmaps/")
        .await
        .unwrap();

    let key = "/registry/configmaps/default/cm1";
    let obj = serde_json::json!({"metadata": {"name": "cm1", "namespace": "default"}});
    let _: serde_json::Value = storage.create(key, &obj).await.unwrap();

    let via_bus = internal.next().await.unwrap().unwrap();
    assert!(matches!(via_bus, rusternetes_storage::WatchEvent::Added(ref k, _) if k == key));

    let via_native = tokio::time::timeout(std::time::Duration::from_secs(5), native.next())
        .await
        .expect("native feed delivered within 5s")
        .unwrap()
        .unwrap();
    assert!(matches!(via_native, rusternetes_storage::WatchEvent::Added(ref k, _) if k == key));
}
