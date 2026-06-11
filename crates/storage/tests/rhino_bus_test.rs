//! Bus-enabled rhino backend: writes are observed on `watch()` in-process,
//! with correct resourceVersion and a previous value on delete (#1039).
#![cfg(feature = "sqlite")]

use futures::StreamExt;
use rusternetes_storage::{Storage, StorageBackend, StorageConfig};
use std::sync::Arc;

async fn bus_backend() -> Arc<StorageBackend> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bus.sqlite").to_string_lossy().into_owned();
    std::mem::forget(dir);
    let mut backend = StorageBackend::new(StorageConfig::Sqlite { path })
        .await
        .expect("backend");
    backend.enable_event_bus();
    Arc::new(backend)
}

#[tokio::test]
async fn watch_sees_create_update_delete_via_bus() {
    let storage = bus_backend().await;
    let key = "/registry/pods/default/p1";
    let mut stream = storage.watch("/registry/pods/").await.expect("watch");

    let obj = serde_json::json!({"metadata": {"name": "p1", "namespace": "default"}, "spec": {}});
    let created: serde_json::Value = storage.create(key, &obj).await.expect("create");

    let added = stream.next().await.expect("event").expect("ok");
    assert!(matches!(added, rusternetes_storage::WatchEvent::Added(ref k, _) if k == key));

    let updated_input = {
        let mut v = created.clone();
        v["spec"]["x"] = serde_json::json!(1);
        v
    };
    let _: serde_json::Value = storage.update(key, &updated_input).await.expect("update");
    let modified = stream.next().await.expect("event").expect("ok");
    assert!(matches!(modified, rusternetes_storage::WatchEvent::Modified(ref k, _) if k == key));

    storage.delete(key).await.expect("delete");
    let deleted = stream.next().await.expect("event").expect("ok");
    match deleted {
        rusternetes_storage::WatchEvent::Deleted(ref k, ref prev) => {
            assert_eq!(k, key);
            assert!(
                prev.contains("\"name\":\"p1\""),
                "prev value carried: {prev}"
            );
        }
        other => panic!("expected Deleted, got {other:?}"),
    }
}

/// A write that fails the CAS (stale resourceVersion -> Conflict) must publish
/// nothing to the bus: the publish calls sit after each `if !succeeded` guard.
#[tokio::test]
async fn failed_write_publishes_nothing() {
    let storage = bus_backend().await;
    let key = "/registry/pods/default/p2";
    let mut stream = storage.watch("/registry/pods/").await.expect("watch");

    let obj = serde_json::json!({"metadata": {"name": "p2", "namespace": "default"}, "spec": {}});
    let _: serde_json::Value = storage.create(key, &obj).await.expect("create");

    // Consume the create's Added so the stream is drained to "now".
    let added = stream.next().await.expect("event").expect("ok");
    assert!(matches!(added, rusternetes_storage::WatchEvent::Added(ref k, _) if k == key));

    // Update carrying a stale resourceVersion -> CAS fails -> Conflict.
    let stale = serde_json::json!({
        "metadata": {"name": "p2", "namespace": "default", "resourceVersion": "1"},
        "spec": {"x": 1}
    });
    let err = storage
        .update::<serde_json::Value>(key, &stale)
        .await
        .expect_err("stale update must Conflict");
    assert!(
        matches!(err, rusternetes_common::Error::Conflict(_)),
        "expected Conflict, got {err:?}"
    );

    // No event should arrive on the bus for the failed write.
    let next = tokio::time::timeout(std::time::Duration::from_millis(300), stream.next()).await;
    assert!(next.is_err(), "failed write must not publish; got {next:?}");
}
