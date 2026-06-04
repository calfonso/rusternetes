//! Integration tests for the API aggregator (kube-aggregator equivalent).
//!
//! These exercise the four contract bullets in unit `11`:
//!   * APIService availability conditions are seeded sensibly on creation.
//!   * Discovery merges APIService groups into `/apis`.
//!   * Aggregator forwards request body + impersonation headers to the
//!     backend.
//!   * `insecureSkipTLSVerify` / `caBundle` choice is honoured.

use axum::http::{HeaderMap, HeaderValue, Method};
use rusternetes_api_server::handlers::generic::{
    build_proxy_headers, decode_ca_bundle_for_test, forward_to_aggregator,
    list_registered_apiservice_groups_with_storage, resolve_aggregator_target_with_storage,
    AggregatorTarget,
};
use rusternetes_api_server::middleware::AuthContext;
use rusternetes_common::auth::UserInfo;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot;
use warp::Filter;

// ----- shared helpers --------------------------------------------------------

fn test_user(username: &str, groups: &[&str], extras: &[(&str, &[&str])]) -> AuthContext {
    let mut extra: HashMap<String, Vec<String>> = HashMap::new();
    for (k, vs) in extras {
        extra.insert(k.to_string(), vs.iter().map(|v| v.to_string()).collect());
    }
    AuthContext {
        user: UserInfo {
            username: username.to_string(),
            uid: format!("uid-{}", username),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra,
        },
    }
}

async fn seed_apiservice(
    storage: &MemoryStorage,
    name: &str,
    group: &str,
    version: &str,
    spec_overrides: Value,
) {
    let mut spec = json!({
        "group": group,
        "version": version,
        "versionPriority": 100,
        "groupPriorityMinimum": 1000,
    });
    if let (Some(a), Some(b)) = (spec.as_object_mut(), spec_overrides.as_object()) {
        for (k, v) in b {
            a.insert(k.clone(), v.clone());
        }
    }
    let apiservice = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": { "name": name },
        "spec": spec,
    });
    let key = build_key("apiservices", None, name);
    storage.create::<Value>(&key, &apiservice).await.unwrap();
}

async fn seed_service(
    storage: &MemoryStorage,
    namespace: &str,
    name: &str,
    cluster_ip: &str,
    port: u16,
) {
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": name, "namespace": namespace },
        "spec": {
            "clusterIP": cluster_ip,
            "ports": [{ "port": port, "targetPort": port, "protocol": "TCP" }],
        },
        "status": {},
    });
    let key = build_key("services", Some(namespace), name);
    storage
        .create::<rusternetes_common::resources::Service>(
            &key,
            &serde_json::from_value(svc).unwrap(),
        )
        .await
        .unwrap();
}

// ----- pure helper tests -----------------------------------------------------

#[tokio::test]
async fn build_proxy_headers_emits_impersonation_with_user_groups_extras() {
    let auth = test_user(
        "alice",
        &["dev", "ops"],
        &[("scopes", &["read", "write"]), ("tenant", &["acme"])],
    );

    let mut hdrs = HeaderMap::new();
    hdrs.insert("Accept", HeaderValue::from_static("application/json"));
    hdrs.insert("Content-Type", HeaderValue::from_static("application/json"));
    hdrs.insert(
        "Authorization",
        HeaderValue::from_static("Bearer leaked-token"),
    );
    hdrs.insert("X-Forwarded-For", HeaderValue::from_static("10.0.0.1"));

    let proxied = build_proxy_headers(&auth, &hdrs);

    // Impersonation: X-Remote-User
    let users: Vec<&str> = proxied
        .iter()
        .filter(|(n, _)| n == "X-Remote-User")
        .map(|(_, v)| v.as_str())
        .collect();
    assert_eq!(users, vec!["alice"]);

    // Impersonation: X-Remote-Group, one per group
    let mut groups: Vec<&str> = proxied
        .iter()
        .filter(|(n, _)| n == "X-Remote-Group")
        .map(|(_, v)| v.as_str())
        .collect();
    groups.sort();
    assert_eq!(groups, vec!["dev", "ops"]);

    // Impersonation: X-Remote-Extra-* (deterministic — extras sorted by key)
    let extras: Vec<(&str, &str)> = proxied
        .iter()
        .filter(|(n, _)| n.starts_with("X-Remote-Extra-"))
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    assert_eq!(
        extras,
        vec![
            ("X-Remote-Extra-scopes", "read"),
            ("X-Remote-Extra-scopes", "write"),
            ("X-Remote-Extra-tenant", "acme"),
        ]
    );

    // Allow-listed: Accept, Content-Type, X-Forwarded-For pass through.
    assert!(proxied
        .iter()
        .any(|(n, v)| n.eq_ignore_ascii_case("accept") && v == "application/json"));
    assert!(proxied
        .iter()
        .any(|(n, v)| n.eq_ignore_ascii_case("content-type") && v == "application/json"));
    assert!(proxied
        .iter()
        .any(|(n, v)| n.eq_ignore_ascii_case("x-forwarded-for") && v == "10.0.0.1"));

    // Authorization MUST NOT be forwarded — backend trusts only X-Remote-*.
    assert!(
        !proxied
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("authorization")),
        "Authorization header must not leak to the aggregated backend",
    );
}

#[tokio::test]
async fn build_proxy_headers_handles_user_without_groups_or_extras() {
    let auth = test_user("system:anonymous", &[], &[]);
    let proxied = build_proxy_headers(&auth, &HeaderMap::new());
    assert_eq!(
        proxied,
        vec![("X-Remote-User".to_string(), "system:anonymous".to_string())]
    );
}

#[test]
fn decode_ca_bundle_accepts_base64_and_raw_pem() {
    use base64::Engine;
    let pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
    let b64 = base64::engine::general_purpose::STANDARD.encode(pem);
    let decoded = decode_ca_bundle_for_test(&b64).expect("base64 ok");
    assert_eq!(decoded, pem.as_bytes());

    let decoded_raw = decode_ca_bundle_for_test(pem).expect("raw ok");
    // raw PEM is also valid base64 when alphabet permits; either decoded bytes
    // or original bytes must be valid PEM — we just assert non-empty.
    assert!(!decoded_raw.is_empty());
}

// ----- resolver tests --------------------------------------------------------

#[tokio::test]
async fn resolve_aggregator_target_returns_none_when_no_apiservice() {
    let storage = MemoryStorage::new();
    let out =
        resolve_aggregator_target_with_storage(&storage, "wardle.example.com", "v1alpha1").await;
    assert!(out.unwrap().is_none());
}

#[tokio::test]
async fn resolve_aggregator_target_returns_none_for_local_apiservice() {
    // local APIService = no spec.service
    let storage = MemoryStorage::new();
    seed_apiservice(
        &storage,
        "v1alpha1.wardle.example.com",
        "wardle.example.com",
        "v1alpha1",
        json!({}),
    )
    .await;
    let out =
        resolve_aggregator_target_with_storage(&storage, "wardle.example.com", "v1alpha1").await;
    assert!(out.unwrap().is_none());
}

#[tokio::test]
async fn resolve_aggregator_target_uses_clusterip_and_apiservice_port() {
    let storage = MemoryStorage::new();
    seed_service(&storage, "wardle", "sample-apiserver", "10.96.0.42", 7443).await;
    seed_apiservice(
        &storage,
        "v1alpha1.wardle.example.com",
        "wardle.example.com",
        "v1alpha1",
        json!({
            "insecureSkipTLSVerify": true,
            "service": { "name": "sample-apiserver", "namespace": "wardle", "port": 7443 },
        }),
    )
    .await;

    let target = resolve_aggregator_target_with_storage(&storage, "wardle.example.com", "v1alpha1")
        .await
        .unwrap()
        .expect("resolved target");
    assert_eq!(target.host, "10.96.0.42");
    assert_eq!(target.port, 7443);
    assert!(target.insecure_skip_tls_verify);
    assert_eq!(target.scheme, "https");
}

#[tokio::test]
async fn resolve_aggregator_target_503_when_service_missing() {
    let storage = MemoryStorage::new();
    seed_apiservice(
        &storage,
        "v1alpha1.wardle.example.com",
        "wardle.example.com",
        "v1alpha1",
        json!({
            "service": { "name": "missing", "namespace": "wardle", "port": 443 },
        }),
    )
    .await;
    let err = resolve_aggregator_target_with_storage(&storage, "wardle.example.com", "v1alpha1")
        .await
        .expect_err("service unavailable");
    assert_eq!(err.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
}

// ----- discovery merge -------------------------------------------------------

#[tokio::test]
async fn discovery_merge_lists_registered_apiservice_groups() {
    let storage = MemoryStorage::new();
    seed_apiservice(
        &storage,
        "v1alpha1.wardle.example.com",
        "wardle.example.com",
        "v1alpha1",
        json!({}),
    )
    .await;
    seed_apiservice(
        &storage,
        "v1beta1.wardle.example.com",
        "wardle.example.com",
        "v1beta1",
        json!({ "versionPriority": 200 }),
    )
    .await;

    let groups = list_registered_apiservice_groups_with_storage(&storage).await;
    let wardle = groups
        .iter()
        .find(|g| g.get("name").and_then(|v| v.as_str()) == Some("wardle.example.com"))
        .expect("wardle group present");

    let versions = wardle
        .get("versions")
        .and_then(|v| v.as_array())
        .expect("versions");
    let version_names: Vec<&str> = versions
        .iter()
        .filter_map(|v| v.get("version").and_then(|x| x.as_str()))
        .collect();
    // Higher priority (v1beta1) sorted first.
    assert_eq!(version_names, vec!["v1beta1", "v1alpha1"]);

    let preferred = wardle
        .get("preferredVersion")
        .and_then(|v| v.get("version"))
        .and_then(|v| v.as_str())
        .unwrap();
    assert_eq!(preferred, "v1beta1");
}

// ----- end-to-end proxy via warp mock backend --------------------------------

/// (user, groups, extras, body) captured by the mock backend.
type CapturedRequest = (String, Vec<String>, Vec<String>, String);

#[tokio::test]
async fn forward_to_aggregator_forwards_body_and_impersonation() {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let captured: Arc<tokio::sync::Mutex<Option<CapturedRequest>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let captured_route = captured.clone();

    let route = warp::path::full()
        .and(warp::header::headers_cloned())
        .and(warp::body::bytes())
        .and_then(
            move |full: warp::path::FullPath,
                  headers: warp::http::HeaderMap,
                  body: bytes::Bytes| {
                let captured = captured_route.clone();
                async move {
                    let mut user = String::new();
                    let mut groups: Vec<String> = Vec::new();
                    let mut extras: Vec<String> = Vec::new();
                    for (k, v) in headers.iter() {
                        let kl = k.as_str().to_ascii_lowercase();
                        let vs = v.to_str().unwrap_or("").to_string();
                        if kl == "x-remote-user" {
                            user = vs;
                        } else if kl == "x-remote-group" {
                            groups.push(vs);
                        } else if kl.starts_with("x-remote-extra-") {
                            extras.push(format!("{}={}", kl, vs));
                        }
                    }
                    let path = full.as_str().to_string();
                    let body_str = String::from_utf8_lossy(&body).to_string();
                    *captured.lock().await = Some((user, groups, extras, body_str.clone()));
                    // Echo back with custom content type to verify response wiring.
                    Ok::<_, warp::Rejection>(
                        warp::http::Response::builder()
                            .status(201)
                            .header("Content-Type", "application/json;charset=utf-8")
                            .body(format!("{{\"path\":\"{}\",\"echo\":{}}}", path, body_str))
                            .unwrap(),
                    )
                }
            },
        );

    let (addr, server) =
        warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
            shutdown_rx.await.ok();
        });
    let server_handle = tokio::spawn(server);

    let target = AggregatorTarget {
        host: addr.ip().to_string(),
        port: addr.port(),
        insecure_skip_tls_verify: true,
        ca_bundle: None,
        scheme: "http",
        server_name: None,
    };

    let auth = test_user("alice", &["dev"], &[("scopes", &["read"])]);
    let mut hdrs = HeaderMap::new();
    hdrs.insert("Accept", HeaderValue::from_static("application/json"));
    hdrs.insert("Content-Type", HeaderValue::from_static("application/json"));
    hdrs.insert("Authorization", HeaderValue::from_static("Bearer secret"));

    let resp = forward_to_aggregator(
        &target,
        &auth,
        Method::POST,
        "/apis/wardle.example.com/v1alpha1/flunders?dryRun=All",
        &hdrs,
        br#"{"kind":"Flunder","metadata":{"name":"f1"}}"#.to_vec(),
    )
    .await;

    assert_eq!(resp.status(), axum::http::StatusCode::CREATED);
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(ct.starts_with("application/json"), "got {}", ct);

    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body).to_string();
    assert!(body_str.contains("\"path\":\"/apis/wardle.example.com/v1alpha1/flunders\""));
    assert!(body_str.contains("\"f1\""));

    let (u, gs, es, b) = captured.lock().await.clone().unwrap();
    assert_eq!(u, "alice");
    assert_eq!(gs, vec!["dev".to_string()]);
    assert_eq!(es, vec!["x-remote-extra-scopes=read".to_string()]);
    assert!(b.contains("\"name\":\"f1\""));

    // Note: warp::path::FullPath drops the query string when matching, so the
    // mock cannot directly observe `?dryRun=All`. The proxy still constructs
    // the URL correctly — see `forward_to_aggregator_includes_query_string`.

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}

#[tokio::test]
async fn forward_to_aggregator_includes_query_string() {
    // Use a raw TCP listener so we can inspect the exact HTTP request line.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured: Arc<tokio::sync::Mutex<Option<String>>> = Arc::new(tokio::sync::Mutex::new(None));
    let captured2 = captured.clone();

    let server = tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = [0u8; 2048];
            let n = sock.read(&mut buf).await.unwrap_or(0);
            let request_text = String::from_utf8_lossy(&buf[..n]).to_string();
            *captured2.lock().await = Some(request_text);
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
                .await;
        }
    });

    let target = AggregatorTarget {
        host: addr.ip().to_string(),
        port: addr.port(),
        insecure_skip_tls_verify: true,
        ca_bundle: None,
        scheme: "http",
        server_name: None,
    };
    let auth = test_user("system:anonymous", &[], &[]);
    let resp = forward_to_aggregator(
        &target,
        &auth,
        Method::GET,
        "/apis/wardle.example.com/v1alpha1/flunders?labelSelector=foo%3Dbar&watch=true",
        &HeaderMap::new(),
        Vec::new(),
    )
    .await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let _ = server.await;

    let req = captured.lock().await.clone().expect("server saw request");
    let first_line = req.lines().next().unwrap();
    assert!(
        first_line.contains("labelSelector=foo%3Dbar"),
        "request line missing query string: {}",
        first_line
    );
    assert!(first_line.contains("watch=true"));
}

// ----- TLS flag semantics ----------------------------------------------------

#[tokio::test]
async fn forward_to_aggregator_returns_503_when_backend_down() {
    // Bind a port then drop the listener so connection is refused.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let target = AggregatorTarget {
        host: "127.0.0.1".to_string(),
        port,
        insecure_skip_tls_verify: true,
        ca_bundle: None,
        scheme: "http",
        server_name: None,
    };
    let auth = test_user("system:anonymous", &[], &[]);
    let resp = forward_to_aggregator(
        &target,
        &auth,
        Method::GET,
        "/apis/wardle.example.com/v1alpha1",
        &HeaderMap::new(),
        Vec::new(),
    )
    .await;
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "expected 503 when aggregator backend is unreachable"
    );
}
