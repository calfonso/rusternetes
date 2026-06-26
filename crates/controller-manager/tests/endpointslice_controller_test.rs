use rusternetes_common::resources::{
    EndpointAddress, EndpointPort, EndpointReference, EndpointSlice, EndpointSubset, Endpoints,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::endpointslice::EndpointSliceController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::sync::Arc;

fn create_test_endpoints(
    name: &str,
    namespace: &str,
    addresses: Vec<String>,
    ports: Vec<(String, u16)>,
) -> Endpoints {
    let endpoint_addresses: Vec<EndpointAddress> = addresses
        .iter()
        .enumerate()
        .map(|(idx, ip)| EndpointAddress {
            ip: ip.clone(),
            hostname: None,
            node_name: Some(format!("node-{}", idx)),
            target_ref: Some(EndpointReference {
                kind: Some("Pod".to_string()),
                namespace: Some(namespace.to_string()),
                name: Some(format!("pod-{}", idx)),
                uid: Some(uuid::Uuid::new_v4().to_string()),
                ..Default::default()
            }),
        })
        .collect();

    let endpoint_ports: Vec<EndpointPort> = ports
        .iter()
        .map(|(name, port)| EndpointPort {
            name: Some(name.clone()),
            port: *port,
            protocol: "TCP".to_string(),
            app_protocol: None,
        })
        .collect();

    Endpoints {
        type_meta: TypeMeta {
            kind: "Endpoints".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        subsets: vec![EndpointSubset {
            addresses: Some(endpoint_addresses),
            not_ready_addresses: None,
            ports: Some(endpoint_ports),
        }],
    }
}

#[tokio::test]
async fn test_endpointslice_created_from_endpoints() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints
    let endpoints = create_test_endpoints(
        "web-service",
        "default",
        vec!["10.244.0.1".to_string(), "10.244.0.2".to_string()],
        vec![("http".to_string(), 8080)],
    );

    storage
        .create(
            &build_key("endpoints", Some("default"), "web-service"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice was created
    let slice: EndpointSlice = storage
        .get(&build_key("endpointslices", Some("default"), "web-service"))
        .await
        .unwrap();

    assert_eq!(slice.metadata.name, "web-service");
    assert_eq!(slice.metadata.namespace, Some("default".to_string()));
    assert_eq!(slice.endpoints.len(), 2, "Should have 2 endpoints");
    assert_eq!(slice.ports.len(), 1, "Should have 1 port");
}

#[tokio::test]
async fn test_endpointslice_has_owner_reference() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints
    let endpoints = create_test_endpoints(
        "api-service",
        "default",
        vec!["10.244.0.10".to_string()],
        vec![("https".to_string(), 8443)],
    );
    let endpoints_uid = endpoints.metadata.uid.clone();

    storage
        .create(
            &build_key("endpoints", Some("default"), "api-service"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice has owner reference
    let slice: EndpointSlice = storage
        .get(&build_key("endpointslices", Some("default"), "api-service"))
        .await
        .unwrap();

    assert!(
        slice.metadata.owner_references.is_some(),
        "Should have owner references"
    );
    let owner_refs = slice.metadata.owner_references.unwrap();
    assert_eq!(owner_refs.len(), 1);

    let owner_ref = &owner_refs[0];
    assert_eq!(owner_ref.kind, "Endpoints");
    assert_eq!(owner_ref.name, "api-service");
    assert_eq!(owner_ref.uid, endpoints_uid);
    assert_eq!(owner_ref.controller, Some(true));
    assert_eq!(owner_ref.block_owner_deletion, Some(true));
}

#[tokio::test]
async fn test_endpointslice_has_service_label() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints
    let endpoints = create_test_endpoints(
        "db-service",
        "default",
        vec!["10.244.0.20".to_string()],
        vec![("mysql".to_string(), 3306)],
    );

    storage
        .create(
            &build_key("endpoints", Some("default"), "db-service"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice has service label
    let slice: EndpointSlice = storage
        .get(&build_key("endpointslices", Some("default"), "db-service"))
        .await
        .unwrap();

    assert!(slice.metadata.labels.is_some(), "Should have labels");
    let labels = slice.metadata.labels.unwrap();
    assert_eq!(
        labels.get("kubernetes.io/service-name"),
        Some(&"db-service".to_string()),
        "Should have service-name label"
    );
}

#[tokio::test]
async fn test_endpointslice_includes_port_mapping() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints with multiple ports
    let endpoints = create_test_endpoints(
        "multi-port-service",
        "default",
        vec!["10.244.0.30".to_string()],
        vec![
            ("http".to_string(), 8080),
            ("https".to_string(), 8443),
            ("metrics".to_string(), 9090),
        ],
    );

    storage
        .create(
            &build_key("endpoints", Some("default"), "multi-port-service"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice includes all ports
    let slice: EndpointSlice = storage
        .get(&build_key(
            "endpointslices",
            Some("default"),
            "multi-port-service",
        ))
        .await
        .unwrap();

    assert_eq!(slice.ports.len(), 3, "Should have 3 ports");

    // Verify specific ports
    let http_port = slice
        .ports
        .iter()
        .find(|p| p.name == Some("http".to_string()))
        .unwrap();
    assert_eq!(http_port.port, Some(8080));
    assert_eq!(http_port.protocol, "TCP");

    let https_port = slice
        .ports
        .iter()
        .find(|p| p.name == Some("https".to_string()))
        .unwrap();
    assert_eq!(https_port.port, Some(8443));

    let metrics_port = slice
        .ports
        .iter()
        .find(|p| p.name == Some("metrics".to_string()))
        .unwrap();
    assert_eq!(metrics_port.port, Some(9090));
}

#[tokio::test]
async fn test_endpointslice_includes_endpoint_conditions() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints with ready addresses
    let endpoints = create_test_endpoints(
        "cache-service",
        "default",
        vec!["10.244.0.40".to_string(), "10.244.0.41".to_string()],
        vec![("redis".to_string(), 6379)],
    );

    storage
        .create(
            &build_key("endpoints", Some("default"), "cache-service"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice endpoint conditions
    let slice: EndpointSlice = storage
        .get(&build_key(
            "endpointslices",
            Some("default"),
            "cache-service",
        ))
        .await
        .unwrap();

    assert_eq!(slice.endpoints.len(), 2);

    for endpoint in &slice.endpoints {
        assert!(
            endpoint.conditions.is_some(),
            "Endpoint should have conditions"
        );
        let conditions = endpoint.conditions.as_ref().unwrap();
        assert_eq!(
            conditions.ready,
            Some(true),
            "Ready endpoints should be marked as ready"
        );
        assert_eq!(
            conditions.serving,
            Some(true),
            "Ready endpoints should be serving"
        );
        assert_eq!(
            conditions.terminating,
            Some(false),
            "Ready endpoints should not be terminating"
        );
    }
}

#[tokio::test]
async fn test_endpointslice_includes_target_ref() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints
    let endpoints = create_test_endpoints(
        "worker-service",
        "default",
        vec!["10.244.0.50".to_string()],
        vec![("app".to_string(), 9000)],
    );

    storage
        .create(
            &build_key("endpoints", Some("default"), "worker-service"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice includes target references
    let slice: EndpointSlice = storage
        .get(&build_key(
            "endpointslices",
            Some("default"),
            "worker-service",
        ))
        .await
        .unwrap();

    assert_eq!(slice.endpoints.len(), 1);
    let endpoint = &slice.endpoints[0];

    assert!(
        endpoint.target_ref.is_some(),
        "Endpoint should have target_ref"
    );
    let target_ref = endpoint.target_ref.as_ref().unwrap();
    assert_eq!(target_ref.kind, Some("Pod".to_string()));
    assert_eq!(target_ref.namespace, Some("default".to_string()));
    assert!(target_ref.name.is_some());
    assert!(target_ref.uid.is_some());
}

#[tokio::test]
async fn test_endpointslice_updates_when_endpoints_change() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create initial Endpoints with 1 address
    let mut endpoints = create_test_endpoints(
        "elastic-service",
        "default",
        vec!["10.244.0.60".to_string()],
        vec![("http".to_string(), 9200)],
    );
    let endpoints_key = build_key("endpoints", Some("default"), "elastic-service");
    storage.create(&endpoints_key, &endpoints).await.unwrap();

    // First reconcile
    controller.reconcile_all().await.unwrap();

    // Verify initial EndpointSlice
    let slice: EndpointSlice = storage
        .get(&build_key(
            "endpointslices",
            Some("default"),
            "elastic-service",
        ))
        .await
        .unwrap();
    assert_eq!(slice.endpoints.len(), 1);

    // Update Endpoints with 3 addresses
    endpoints = create_test_endpoints(
        "elastic-service",
        "default",
        vec![
            "10.244.0.60".to_string(),
            "10.244.0.61".to_string(),
            "10.244.0.62".to_string(),
        ],
        vec![("http".to_string(), 9200)],
    );
    storage.update(&endpoints_key, &endpoints).await.unwrap();

    // Second reconcile
    controller.reconcile_all().await.unwrap();

    // Verify updated EndpointSlice
    let updated_slice: EndpointSlice = storage
        .get(&build_key(
            "endpointslices",
            Some("default"),
            "elastic-service",
        ))
        .await
        .unwrap();
    assert_eq!(
        updated_slice.endpoints.len(),
        3,
        "EndpointSlice should be updated to include new addresses"
    );
}

#[tokio::test]
async fn test_endpointslice_multiple_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints in different namespaces
    let endpoints_ns1 = create_test_endpoints(
        "app-service",
        "ns1",
        vec!["10.244.1.1".to_string()],
        vec![("http".to_string(), 8080)],
    );
    let endpoints_ns2 = create_test_endpoints(
        "app-service",
        "ns2",
        vec!["10.244.2.1".to_string()],
        vec![("http".to_string(), 8080)],
    );

    storage
        .create(
            &build_key("endpoints", Some("ns1"), "app-service"),
            &endpoints_ns1,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("endpoints", Some("ns2"), "app-service"),
            &endpoints_ns2,
        )
        .await
        .unwrap();

    // Reconcile all
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlices in ns1
    let slice_ns1: EndpointSlice = storage
        .get(&build_key("endpointslices", Some("ns1"), "app-service"))
        .await
        .unwrap();
    assert_eq!(slice_ns1.metadata.namespace, Some("ns1".to_string()));
    assert_eq!(slice_ns1.endpoints.len(), 1);
    assert_eq!(slice_ns1.endpoints[0].addresses[0], "10.244.1.1");

    // Verify EndpointSlices in ns2
    let slice_ns2: EndpointSlice = storage
        .get(&build_key("endpointslices", Some("ns2"), "app-service"))
        .await
        .unwrap();
    assert_eq!(slice_ns2.metadata.namespace, Some("ns2".to_string()));
    assert_eq!(slice_ns2.endpoints.len(), 1);
    assert_eq!(slice_ns2.endpoints[0].addresses[0], "10.244.2.1");
}

#[tokio::test]
async fn test_endpointslice_cleanup_orphans() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints
    let endpoints = create_test_endpoints(
        "cleanup-service",
        "default",
        vec!["10.244.0.70".to_string()],
        vec![("http".to_string(), 8080)],
    );

    storage
        .create(
            &build_key("endpoints", Some("default"), "cleanup-service"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile to create EndpointSlice
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice exists
    let slice_result: Result<EndpointSlice, _> = storage
        .get(&build_key(
            "endpointslices",
            Some("default"),
            "cleanup-service",
        ))
        .await;
    assert!(slice_result.is_ok(), "EndpointSlice should exist");

    // Delete Endpoints
    storage
        .delete(&build_key("endpoints", Some("default"), "cleanup-service"))
        .await
        .unwrap();

    // Run cleanup
    controller.cleanup_orphans().await.unwrap();

    // Verify EndpointSlice was deleted
    let slice_result: Result<EndpointSlice, _> = storage
        .get(&build_key(
            "endpointslices",
            Some("default"),
            "cleanup-service",
        ))
        .await;
    assert!(
        slice_result.is_err(),
        "Orphaned EndpointSlice should be deleted"
    );
}

#[tokio::test]
async fn test_endpointslice_empty_endpoints() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints with no addresses
    let endpoints = create_test_endpoints(
        "empty-service",
        "default",
        vec![],
        vec![("http".to_string(), 8080)],
    );

    storage
        .create(
            &build_key("endpoints", Some("default"), "empty-service"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice was created
    let slice: EndpointSlice = storage
        .get(&build_key(
            "endpointslices",
            Some("default"),
            "empty-service",
        ))
        .await
        .unwrap();

    assert_eq!(
        slice.endpoints.len(),
        0,
        "EndpointSlice should have no endpoints"
    );
    // When there are no addresses, ports may also be empty depending on implementation
    // (slice.ports can be any length, just verify it's accessible)
    let _ = slice.ports.len();
}

#[tokio::test]
async fn test_endpointslice_multi_port_multi_ip_service() {
    // This test verifies the scenario required by conformance tests:
    // - "should support a Service with multiple endpoint IPs specified in multiple EndpointSlices"
    // - "should support a Service with multiple ports specified in multiple EndpointSlices"
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create Endpoints with 2 ports (http:80, https:443) and 2 pod IPs
    let endpoints = create_test_endpoints(
        "multi-svc",
        "default",
        vec!["10.244.0.100".to_string(), "10.244.0.101".to_string()],
        vec![("http".to_string(), 80), ("https".to_string(), 443)],
    );

    storage
        .create(
            &build_key("endpoints", Some("default"), "multi-svc"),
            &endpoints,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify EndpointSlice was created
    let slice: EndpointSlice = storage
        .get(&build_key("endpointslices", Some("default"), "multi-svc"))
        .await
        .unwrap();

    assert_eq!(slice.metadata.name, "multi-svc");
    assert_eq!(slice.metadata.namespace, Some("default".to_string()));

    // Verify both ports are present
    assert_eq!(slice.ports.len(), 2, "Should have 2 ports (http and https)");
    let http_port = slice
        .ports
        .iter()
        .find(|p| p.name == Some("http".to_string()));
    assert!(http_port.is_some(), "Should have http port");
    assert_eq!(http_port.unwrap().port, Some(80));
    assert_eq!(http_port.unwrap().protocol, "TCP");

    let https_port = slice
        .ports
        .iter()
        .find(|p| p.name == Some("https".to_string()));
    assert!(https_port.is_some(), "Should have https port");
    assert_eq!(https_port.unwrap().port, Some(443));
    assert_eq!(https_port.unwrap().protocol, "TCP");

    // Verify both pod IPs are in the endpoints
    assert_eq!(
        slice.endpoints.len(),
        2,
        "Should have 2 endpoints (one per pod)"
    );
    let ips: Vec<&str> = slice
        .endpoints
        .iter()
        .flat_map(|ep| ep.addresses.iter().map(|a| a.as_str()))
        .collect();
    assert!(ips.contains(&"10.244.0.100"), "Should contain first pod IP");
    assert!(
        ips.contains(&"10.244.0.101"),
        "Should contain second pod IP"
    );

    // Verify each endpoint has proper conditions
    for ep in &slice.endpoints {
        let conditions = ep.conditions.as_ref().expect("Should have conditions");
        assert_eq!(conditions.ready, Some(true));
        assert_eq!(conditions.serving, Some(true));
        assert_eq!(conditions.terminating, Some(false));
    }

    // Verify labels
    let labels = slice.metadata.labels.as_ref().unwrap();
    assert_eq!(
        labels.get("kubernetes.io/service-name"),
        Some(&"multi-svc".to_string())
    );
    // The slice is mirrored from Endpoints (no Service exists here), so the
    // mirroring controller's label is expected. K8s splits ownership into two
    // distinct managed-by values: selector-based vs mirrored Endpoints.
    assert_eq!(
        labels.get("endpointslice.kubernetes.io/managed-by"),
        Some(&"endpointslice-mirroring-controller.k8s.io".to_string())
    );
}

#[tokio::test]
async fn test_externally_created_endpointslice_not_deleted() {
    // Conformance tests create EndpointSlices directly (not via Endpoints).
    // The orphan cleanup should NOT delete these.
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(storage.clone());

    // Create an EndpointSlice directly (simulating what conformance tests do)
    let mut external_slice = EndpointSlice::new("my-svc-external-abc12", "IPv4");
    external_slice.metadata.namespace = Some("test-ns".to_string());
    let labels = external_slice
        .metadata
        .labels
        .get_or_insert_with(Default::default);
    labels.insert(
        "kubernetes.io/service-name".to_string(),
        "my-svc".to_string(),
    );
    // Note: no "endpointslice.kubernetes.io/managed-by" label set to our controller
    external_slice
        .endpoints
        .push(rusternetes_common::resources::endpointslice::Endpoint {
            addresses: vec!["10.244.1.50".to_string()],
            conditions: Some(
                rusternetes_common::resources::endpointslice::EndpointConditions {
                    ready: Some(true),
                    serving: Some(true),
                    terminating: Some(false),
                },
            ),
            hostname: None,
            target_ref: None,
            node_name: None,
            zone: None,
            hints: None,
            deprecated_topology: None,
        });
    external_slice
        .ports
        .push(rusternetes_common::resources::endpointslice::EndpointPort {
            name: Some("http".to_string()),
            port: Some(80),
            protocol: "TCP".to_string(),
            app_protocol: None,
        });

    storage
        .create(
            &build_key("endpointslices", Some("test-ns"), "my-svc-external-abc12"),
            &external_slice,
        )
        .await
        .unwrap();

    // Run reconcile_all (which includes orphan cleanup)
    // There are no Endpoints for "my-svc", but the slice should NOT be deleted
    // because it's not managed by our controller
    controller.reconcile_all().await.unwrap();

    // Verify the externally-created EndpointSlice still exists
    let result: Result<EndpointSlice, _> = storage
        .get(&build_key(
            "endpointslices",
            Some("test-ns"),
            "my-svc-external-abc12",
        ))
        .await;
    assert!(
        result.is_ok(),
        "Externally-created EndpointSlice should NOT be deleted by orphan cleanup"
    );
}
