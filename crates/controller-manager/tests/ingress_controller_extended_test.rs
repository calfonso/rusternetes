//! Extended integration tests for the Ingress controller.
//!
//! Mirrors upstream e2e coverage in `kubernetes/test/e2e/network/ingress.go`
//! for path-type matching, TLS termination, default-backend catch-all routing,
//! host-based virtual hosting, IngressClass selection, and load-balancer
//! status population. These tests drive [`IngressController`] against a
//! [`MemoryStorage`] backend — no networking, no real LB provisioning.

use rusternetes_common::resources::ingress::{
    HTTPIngressPath, HTTPIngressRuleValue, IngressBackend, IngressRule, IngressServiceBackend,
    IngressSpec, IngressTLS, ServiceBackendPort,
};
use rusternetes_common::resources::{Ingress, IngressClass, Secret, Service, ServiceSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::ingress::IngressController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn make_backend(service_name: &str, port: i32) -> IngressBackend {
    IngressBackend {
        service: Some(IngressServiceBackend {
            name: service_name.to_string(),
            port: Some(ServiceBackendPort {
                name: None,
                number: Some(port),
            }),
        }),
        resource: None,
    }
}

fn make_path(path: &str, path_type: &str, backend: IngressBackend) -> HTTPIngressPath {
    HTTPIngressPath {
        path: Some(path.to_string()),
        path_type: path_type.to_string(),
        backend,
    }
}

fn make_ingress(name: &str, namespace: &str, spec: IngressSpec) -> Ingress {
    Ingress {
        type_meta: TypeMeta {
            kind: "Ingress".to_string(),
            api_version: "networking.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name).with_namespace(namespace);
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: Some(spec),
        status: None,
    }
}

async fn store_ingress(storage: &Arc<MemoryStorage>, ingress: &Ingress) {
    let key = build_key(
        "ingresses",
        ingress.metadata.namespace.as_deref(),
        &ingress.metadata.name,
    );
    storage.create(&key, ingress).await.unwrap();
}

async fn store_secret(storage: &Arc<MemoryStorage>, name: &str, namespace: &str) {
    let secret = Secret::new(name, namespace);
    let key = build_key("secrets", Some(namespace), name);
    storage.create(&key, &secret).await.unwrap();
}

async fn store_service(storage: &Arc<MemoryStorage>, name: &str, namespace: &str) {
    let mut svc = Service::new(name, ServiceSpec::default());
    svc.metadata.namespace = Some(namespace.to_string());
    let key = build_key("services", Some(namespace), name);
    storage.create(&key, &svc).await.unwrap();
}

// -- Path type matching ----------------------------------------------------

#[tokio::test]
async fn path_type_exact_prefix_and_implementation_specific_all_reconcile() {
    let storage = setup_test().await;
    store_service(&storage, "backend-svc", "default").await;

    let backend = make_backend("backend-svc", 80);
    let paths = vec![
        make_path("/exact", "Exact", backend.clone()),
        make_path("/prefix", "Prefix", backend.clone()),
        make_path("/impl", "ImplementationSpecific", backend),
    ];

    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: None,
        tls: None,
        rules: Some(vec![IngressRule {
            host: Some("example.com".to_string()),
            http: Some(HTTPIngressRuleValue { paths }),
        }]),
    };

    let ingress = make_ingress("multipath", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Validation should have accepted all three path types — reconcile leaves
    // the spec intact and populates status.
    let key = build_key("ingresses", Some("default"), "multipath");
    let stored: Ingress = storage.get(&key).await.unwrap();
    let rules = stored.spec.unwrap().rules.unwrap();
    let stored_paths = &rules[0].http.as_ref().unwrap().paths;
    assert_eq!(stored_paths.len(), 3);
    assert_eq!(stored_paths[0].path_type, "Exact");
    assert_eq!(stored_paths[1].path_type, "Prefix");
    assert_eq!(stored_paths[2].path_type, "ImplementationSpecific");
    assert!(stored.status.is_some(), "status must be populated");
}

#[tokio::test]
async fn invalid_path_type_skips_status_population() {
    let storage = setup_test().await;
    store_service(&storage, "backend-svc", "default").await;

    let backend = make_backend("backend-svc", 80);
    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: None,
        tls: None,
        rules: Some(vec![IngressRule {
            host: Some("example.com".to_string()),
            http: Some(HTTPIngressRuleValue {
                paths: vec![make_path("/", "Regex", backend)],
            }),
        }]),
    };

    let ingress = make_ingress("bad-path-type", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Validation rejects unknown path type; reconcile skips status update.
    let key = build_key("ingresses", Some("default"), "bad-path-type");
    let stored: Ingress = storage.get(&key).await.unwrap();
    assert!(
        stored.status.is_none(),
        "invalid path type must not produce LB status"
    );
}

// -- TLS termination -------------------------------------------------------

#[tokio::test]
async fn tls_block_accepted_and_persisted() {
    let storage = setup_test().await;
    store_service(&storage, "tls-svc", "default").await;
    store_secret(&storage, "tls-secret", "default").await;

    let backend = make_backend("tls-svc", 443);
    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: None,
        tls: Some(vec![IngressTLS {
            hosts: Some(vec!["secure.example.com".to_string()]),
            secret_name: Some("tls-secret".to_string()),
        }]),
        rules: Some(vec![IngressRule {
            host: Some("secure.example.com".to_string()),
            http: Some(HTTPIngressRuleValue {
                paths: vec![make_path("/", "Prefix", backend)],
            }),
        }]),
    };

    let ingress = make_ingress("tls-ingress", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "tls-ingress");
    let stored: Ingress = storage.get(&key).await.unwrap();
    let tls = stored.spec.unwrap().tls.expect("TLS block preserved");
    assert_eq!(tls.len(), 1);
    assert_eq!(tls[0].secret_name.as_deref(), Some("tls-secret"));
    assert_eq!(
        tls[0].hosts.as_ref().unwrap()[0],
        "secure.example.com",
        "TLS hosts list should round-trip"
    );
    assert!(
        stored.status.is_some(),
        "TLS ingress must still get an LB status"
    );
}

#[tokio::test]
async fn tls_referencing_missing_secret_fails_validation() {
    let storage = setup_test().await;
    store_service(&storage, "tls-svc", "default").await;

    let backend = make_backend("tls-svc", 443);
    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: None,
        tls: Some(vec![IngressTLS {
            hosts: Some(vec!["secure.example.com".to_string()]),
            secret_name: Some("missing-secret".to_string()),
        }]),
        rules: Some(vec![IngressRule {
            host: Some("secure.example.com".to_string()),
            http: Some(HTTPIngressRuleValue {
                paths: vec![make_path("/", "Prefix", backend)],
            }),
        }]),
    };

    let ingress = make_ingress("tls-missing-secret", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "tls-missing-secret");
    let stored: Ingress = storage.get(&key).await.unwrap();
    assert!(
        stored.status.is_none(),
        "missing TLS Secret should block status population"
    );
}

// -- Default backend (catch-all) ------------------------------------------

#[tokio::test]
async fn default_backend_only_is_valid_and_gets_status() {
    let storage = setup_test().await;
    store_service(&storage, "catch-all", "default").await;

    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: Some(make_backend("catch-all", 8080)),
        tls: None,
        rules: None,
    };

    let ingress = make_ingress("catchall-ing", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "catchall-ing");
    let stored: Ingress = storage.get(&key).await.unwrap();
    let s = stored.spec.unwrap();
    assert!(s.default_backend.is_some(), "default backend preserved");
    assert!(
        s.rules.is_none(),
        "no host rules — purely default-backend ingress"
    );
    let status = stored.status.expect("status must be populated");
    let lb = status.load_balancer.expect("load_balancer must be set");
    let entries = lb.ingress.expect("ingress list must exist");
    assert_eq!(entries.len(), 1);
    assert!(
        entries[0].ip.is_some(),
        "catch-all ingress must receive a simulated LB IP"
    );
}

// -- Host-based virtual hosting -------------------------------------------

#[tokio::test]
async fn host_based_routing_supports_multiple_virtual_hosts() {
    let storage = setup_test().await;
    store_service(&storage, "foo-svc", "default").await;
    store_service(&storage, "bar-svc", "default").await;

    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: None,
        tls: None,
        rules: Some(vec![
            IngressRule {
                host: Some("foo.example.com".to_string()),
                http: Some(HTTPIngressRuleValue {
                    paths: vec![make_path("/", "Prefix", make_backend("foo-svc", 80))],
                }),
            },
            IngressRule {
                host: Some("bar.example.com".to_string()),
                http: Some(HTTPIngressRuleValue {
                    paths: vec![make_path("/", "Prefix", make_backend("bar-svc", 80))],
                }),
            },
        ]),
    };

    let ingress = make_ingress("multi-host", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "multi-host");
    let stored: Ingress = storage.get(&key).await.unwrap();
    let rules = stored.spec.unwrap().rules.unwrap();
    assert_eq!(rules.len(), 2);
    let hosts: Vec<&str> = rules.iter().filter_map(|r| r.host.as_deref()).collect();
    assert!(hosts.contains(&"foo.example.com"));
    assert!(hosts.contains(&"bar.example.com"));
    assert!(stored.status.is_some());
}

// -- IngressClass selection ------------------------------------------------

#[tokio::test]
async fn ingress_class_name_is_preserved_after_reconcile() {
    let storage = setup_test().await;
    store_service(&storage, "classed-svc", "default").await;

    // Pre-create the referenced IngressClass so the controller's existence
    // check passes and the ingress is actually reconciled (status populated).
    // Without this the test would pass trivially: validation would fail, the
    // ingress would be skipped, and the spec field would round-trip untouched.
    storage
        .create(
            &build_key("ingressclasses", None, "nginx"),
            &IngressClass::new("nginx"),
        )
        .await
        .unwrap();

    let spec = IngressSpec {
        ingress_class_name: Some("nginx".to_string()),
        default_backend: None,
        tls: None,
        rules: Some(vec![IngressRule {
            host: Some("classed.example.com".to_string()),
            http: Some(HTTPIngressRuleValue {
                paths: vec![make_path("/", "Prefix", make_backend("classed-svc", 80))],
            }),
        }]),
    };

    let ingress = make_ingress("classed", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "classed");
    let stored: Ingress = storage.get(&key).await.unwrap();
    assert_eq!(
        stored.spec.unwrap().ingress_class_name.as_deref(),
        Some("nginx"),
        "ingressClassName must round-trip through reconcile"
    );
    assert!(
        stored.status.is_some(),
        "an ingress whose IngressClass exists must be reconciled and get LB status"
    );
}

#[tokio::test]
async fn ingress_referencing_unknown_class_is_rejected() {
    let storage = setup_test().await;
    store_service(&storage, "orphan-svc", "default").await;

    let spec = IngressSpec {
        ingress_class_name: Some("no-such-class".to_string()),
        default_backend: None,
        tls: None,
        rules: Some(vec![IngressRule {
            host: Some("orphan.example.com".to_string()),
            http: Some(HTTPIngressRuleValue {
                paths: vec![make_path("/", "Prefix", make_backend("orphan-svc", 80))],
            }),
        }]),
    };

    let ingress = make_ingress("orphan", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "orphan");
    let stored: Ingress = storage.get(&key).await.unwrap();
    assert!(
        stored.status.is_none(),
        "ingress with unknown ingressClassName must not get LB status"
    );
}

// -- Load-balancer status --------------------------------------------------

#[tokio::test]
async fn status_load_balancer_populated_on_first_reconcile() {
    let storage = setup_test().await;
    store_service(&storage, "lb-svc", "default").await;

    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: Some(make_backend("lb-svc", 80)),
        tls: None,
        rules: None,
    };

    let ingress = make_ingress("lb-status", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "lb-status");
    let stored: Ingress = storage.get(&key).await.unwrap();
    let lb = stored
        .status
        .and_then(|s| s.load_balancer)
        .and_then(|l| l.ingress)
        .expect("load_balancer.ingress must be present");
    assert_eq!(lb.len(), 1);
    let entry = &lb[0];
    assert!(entry.ip.is_some(), "LB ingress must have an IP set");
    let ip = entry.ip.as_deref().unwrap();
    assert!(
        ip.starts_with("10."),
        "simulated LB IP should be in the 10.0.0.0/8 range, got: {ip}"
    );
}

#[tokio::test]
async fn status_load_balancer_honors_user_supplied_annotation_ip() {
    let storage = setup_test().await;
    store_service(&storage, "anno-svc", "default").await;

    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: Some(make_backend("anno-svc", 80)),
        tls: None,
        rules: None,
    };

    let mut ingress = make_ingress("annotated", "default", spec);
    let mut annotations = std::collections::HashMap::new();
    annotations.insert(
        "ingress.rusternetes.io/load-balancer-ip".to_string(),
        "192.0.2.42".to_string(),
    );
    ingress.metadata.annotations = Some(annotations);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "annotated");
    let stored: Ingress = storage.get(&key).await.unwrap();
    let entries = stored
        .status
        .and_then(|s| s.load_balancer)
        .and_then(|l| l.ingress)
        .expect("load_balancer.ingress must be present");
    assert_eq!(entries[0].ip.as_deref(), Some("192.0.2.42"));
}

#[tokio::test]
async fn status_load_balancer_is_idempotent_across_reconciles() {
    let storage = setup_test().await;
    store_service(&storage, "stable-svc", "default").await;

    let spec = IngressSpec {
        ingress_class_name: None,
        default_backend: Some(make_backend("stable-svc", 80)),
        tls: None,
        rules: None,
    };

    let ingress = make_ingress("stable", "default", spec);
    store_ingress(&storage, &ingress).await;

    let controller = IngressController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key = build_key("ingresses", Some("default"), "stable");
    let first: Ingress = storage.get(&key).await.unwrap();
    let first_ip = first
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
        .and_then(|l| l.ingress.as_ref())
        .and_then(|v| v.first())
        .and_then(|e| e.ip.clone())
        .expect("first reconcile must set an IP");

    // Re-run reconcile; existing status should not be replaced.
    controller.reconcile_all().await.unwrap();

    let second: Ingress = storage.get(&key).await.unwrap();
    let second_ip = second
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
        .and_then(|l| l.ingress.as_ref())
        .and_then(|v| v.first())
        .and_then(|e| e.ip.clone())
        .expect("status must remain populated after second reconcile");

    assert_eq!(
        first_ip, second_ip,
        "LB IP must remain stable across reconciles"
    );
}
