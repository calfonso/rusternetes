//! Live end-to-end test for the production [`ApiListWatch`] + [`Reflector`]
//! against a real api-server over a TCP socket (#1126).
//!
//! Unlike `reflector_test.rs` (scripted `ListWatch` mock) and `watch_test.rs`
//! (literal JSON lines), this boots the actual api-server router on a loopback
//! port and drives the reflector through `reqwest`, exercising the pieces that
//! were "untested live": `ApiListWatch::list` reading
//! `KubernetesList.metadata.resourceVersion`, `?watch=true&resourceVersion=N`
//! resume, and `watch_stream`'s chunk-boundary buffering over a real chunked
//! response. Object mutations are made over HTTP so the whole path is e2e.

use std::sync::Arc;
use std::time::Duration;

use rusternetes_client::http::ApiClient;
use rusternetes_client::reflector::{ApiListWatch, ListWatch, Reflector};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use tokio::net::TcpListener;

/// Boot the real router on 127.0.0.1:0 and return (client, storage-backed
/// harness). The serve task runs for the lifetime of the test process.
async fn serve() -> (Arc<ApiClient>, TestApiServer) {
    let ts = TestApiServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = ts.router.clone();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = Arc::new(
        ApiClient::new(&format!("http://{addr}"), false, None).expect("build test ApiClient"),
    );
    (client, ts)
}

/// Poll `cond` every 100ms up to `secs` seconds. Returns true if it ever held.
async fn wait_until<F: Fn() -> bool>(secs: u64, cond: F) -> bool {
    for _ in 0..(secs * 10) {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cond()
}

fn configmap(name: &str, k: &str, v: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": name, "namespace": "default" },
        "data": { (k): v },
    })
}

const CMS: &str = "/api/v1/namespaces/default/configmaps";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reflector_lists_then_streams_live_mutations() {
    let (client, _ts) = serve().await;

    // Namespace + one pre-existing ConfigMap, created over the wire.
    let _: Value = client
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}),
        )
        .await
        .expect("create default namespace");
    let _: Value = client
        .post(CMS, &configmap("cm-a", "k", "v1"))
        .await
        .expect("create cm-a");

    // Start a reflector over the configmaps collection.
    let lw: Arc<dyn ListWatch<Value>> =
        Arc::new(ApiListWatch::new(client.clone(), CMS.to_string()));
    let reflector = Arc::new(Reflector::new(lw, |v: &Value| {
        v["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }));
    let r = reflector.clone();
    tokio::spawn(async move { r.run().await });

    let store = reflector.store();

    // 1) Initial LIST populates the store with the pre-existing object.
    assert!(
        wait_until(10, || store.get("cm-a").is_some()).await,
        "reflector must populate cm-a from the initial list"
    );

    // 2) A ConfigMap created AFTER the list arrives over the live WATCH (ADDED).
    let _: Value = client
        .post(CMS, &configmap("cm-b", "k", "vb"))
        .await
        .expect("create cm-b");
    assert!(
        wait_until(10, || store.get("cm-b").is_some()).await,
        "reflector must observe cm-b via watch ADDED"
    );

    // 3) Updating cm-a streams as MODIFIED and updates the stored object.
    let mut current: Value = client.get(&format!("{CMS}/cm-a")).await.unwrap();
    current["data"]["k"] = json!("v2");
    let _: Value = client
        .put(&format!("{CMS}/cm-a"), &current)
        .await
        .expect("update cm-a");
    assert!(
        wait_until(10, || store
            .get("cm-a")
            .and_then(|v| v["data"]["k"].as_str().map(str::to_string))
            == Some("v2".to_string()))
        .await,
        "reflector must observe the cm-a update via watch MODIFIED"
    );

    // 4) Deleting cm-b streams as DELETED and removes it from the store.
    client
        .delete_with_options(&format!("{CMS}/cm-b"), &[], None)
        .await
        .expect("delete cm-b");
    assert!(
        wait_until(10, || store.get("cm-b").is_none()).await,
        "reflector must remove cm-b via watch DELETED"
    );
}
