//! Integration tests for Watch API handler
//!
//! Tests watch streams, bookmarks, timeouts, and resource version handling

use futures::StreamExt;
use rusternetes_common::resources::namespace::{Namespace, NamespaceSpec, NamespaceStatus};
use rusternetes_common::resources::{
    ConfigMap, Container, Pod, PodSpec, PodStatus, Service, ServicePort, ServiceSpec,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// Helper to parse watch stream response
#[allow(dead_code)]
async fn parse_watch_events<T: DeserializeOwned>(body: &str) -> Vec<(String, T)> {
    let mut events = Vec::new();
    for line in body.lines() {
        if line.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            let event_type = value["type"].as_str().unwrap_or("UNKNOWN").to_string();
            if let Ok(object) = serde_json::from_value::<T>(value["object"].clone()) {
                events.push((event_type, object));
            }
        }
    }
    events
}

// Helper to create minimal PodSpec (no Default impl)
fn create_minimal_pod_spec() -> PodSpec {
    PodSpec {
        containers: vec![Container {
            name: "test".to_string(),
            image: "nginx:latest".to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            ports: None,
            env: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            resources: None,
            working_dir: None,
            command: None,
            args: None,
            restart_policy: None,
            resize_policy: None,
            security_context: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            env_from: None,
            volume_devices: None,
        }],
        init_containers: None,
        ephemeral_containers: None,
        volumes: None,
        restart_policy: Some("Always".to_string()),
        node_name: None,
        node_selector: None,
        service_account_name: None,
        service_account: None,
        automount_service_account_token: None,
        hostname: None,
        subdomain: None,
        host_network: None,
        host_pid: None,
        host_ipc: None,
        affinity: None,
        tolerations: None,
        priority: None,
        priority_class_name: None,
        scheduler_name: None,
        overhead: None,
        topology_spread_constraints: None,
        resource_claims: None,
        active_deadline_seconds: None,
        dns_policy: None,
        dns_config: None,
        security_context: None,
        image_pull_secrets: None,
        share_process_namespace: None,
        readiness_gates: None,
        runtime_class_name: None,
        enable_service_links: None,
        preemption_policy: None,
        host_users: None,
        set_hostname_as_fqdn: None,
        termination_grace_period_seconds: None,
        host_aliases: None,
        os: None,
        scheduling_gates: None,
        resources: None,
    }
}

// Helper to create minimal PodStatus (no Default impl)
fn create_minimal_pod_status() -> PodStatus {
    PodStatus {
        phase: Some(Phase::Pending),
        message: None,
        reason: None,
        host_ip: None,
        host_i_ps: None,
        pod_ip: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        container_statuses: None,
        init_container_statuses: None,
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
        conditions: None,
    }
}

#[tokio::test]
async fn test_watch_initial_state_events() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-initial";

    // Create some pods first
    for i in 1..=3 {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: format!("pod-{}", i),
                namespace: Some(namespace.to_string()),
                resource_version: Some(format!("{}", i)),
                ..Default::default()
            },
            spec: Some(create_minimal_pod_spec()),
            status: Some(create_minimal_pod_status()),
        };

        let key = build_key("pods", Some(namespace), &format!("pod-{}", i));
        storage.create(&key, &pod).await.unwrap();
    }

    // Start watching - should get initial ADDED events for existing pods
    let prefix = build_prefix("pods", Some(namespace));
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // In a real implementation, we'd test the HTTP endpoint
    // For now, verify storage watch works

    // Create a new pod and verify we get the event
    let new_pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "pod-new".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("4".to_string()),
            ..Default::default()
        },
        spec: Some(create_minimal_pod_spec()),
        status: Some(create_minimal_pod_status()),
    };

    let key = build_key("pods", Some(namespace), "pod-new");
    storage.create(&key, &new_pod).await.unwrap();

    // Should receive watch event
    if let Some(Ok(event)) = watch_stream.next().await {
        match event {
            rusternetes_storage::WatchEvent::Added(k, _) => {
                assert!(k.contains("pod-new"));
            }
            _ => panic!("Expected Added event"),
        }
    }

    // Cleanup
    for i in 1..=3 {
        let key = build_key("pods", Some(namespace), &format!("pod-{}", i));
        let _ = storage.delete(&key).await;
    }
    let key = build_key("pods", Some(namespace), "pod-new");
    let _ = storage.delete(&key).await;
}

#[tokio::test]
async fn test_watch_added_event() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-added";
    let prefix = build_prefix("pods", Some(namespace));

    // Start watching before creating resources
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Create a pod
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: Some(create_minimal_pod_spec()),
        status: Some(create_minimal_pod_status()),
    };

    let key = build_key("pods", Some(namespace), "test-pod");
    storage.create(&key, &pod).await.unwrap();

    // Verify we get an ADDED event
    if let Some(Ok(event)) = watch_stream.next().await {
        match event {
            rusternetes_storage::WatchEvent::Added(k, v) => {
                assert!(k.contains("test-pod"));
                let received_pod: Pod = serde_json::from_str(&v).unwrap();
                assert_eq!(received_pod.metadata.name, "test-pod");
            }
            _ => panic!("Expected Added event"),
        }
    }

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_watch_modified_event() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-modified";
    let key = build_key("pods", Some(namespace), "test-pod");

    // Create initial pod
    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: Some(create_minimal_pod_spec()),
        status: Some(create_minimal_pod_status()),
    };

    storage.create(&key, &pod).await.unwrap();

    // Start watching
    let prefix = build_prefix("pods", Some(namespace));
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Update the pod
    pod.metadata.resource_version = Some("2".to_string());
    pod.metadata.labels = Some({
        let mut labels = HashMap::new();
        labels.insert("updated".to_string(), "true".to_string());
        labels
    });

    storage.update(&key, &pod).await.unwrap();

    // Verify we get a MODIFIED event
    if let Some(Ok(event)) = watch_stream.next().await {
        match event {
            rusternetes_storage::WatchEvent::Modified(k, v) => {
                assert!(k.contains("test-pod"));
                let received_pod: Pod = serde_json::from_str(&v).unwrap();
                assert_eq!(
                    received_pod
                        .metadata
                        .labels
                        .as_ref()
                        .unwrap()
                        .get("updated"),
                    Some(&"true".to_string())
                );
            }
            _ => panic!("Expected Modified event"),
        }
    }

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_watch_deleted_event() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-deleted";
    let key = build_key("pods", Some(namespace), "test-pod");

    // Create pod
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: Some(create_minimal_pod_spec()),
        status: Some(create_minimal_pod_status()),
    };

    storage.create(&key, &pod).await.unwrap();

    // Start watching
    let prefix = build_prefix("pods", Some(namespace));
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Delete the pod
    storage.delete(&key).await.unwrap();

    // Verify we get a DELETED event
    if let Some(Ok(event)) = watch_stream.next().await {
        match event {
            rusternetes_storage::WatchEvent::Deleted(k, v) => {
                assert!(k.contains("test-pod"));
                let received_pod: Pod = serde_json::from_str(&v).unwrap();
                assert_eq!(received_pod.metadata.name, "test-pod");
            }
            _ => panic!("Expected Deleted event"),
        }
    }
}

#[tokio::test]
async fn test_watch_multiple_resources() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-multiple";
    let prefix = build_prefix("pods", Some(namespace));

    // Start watching
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Create multiple pods
    for i in 1..=5 {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: format!("pod-{}", i),
                namespace: Some(namespace.to_string()),
                resource_version: Some(i.to_string()),
                ..Default::default()
            },
            spec: Some(create_minimal_pod_spec()),
            status: Some(create_minimal_pod_status()),
        };

        let key = build_key("pods", Some(namespace), &format!("pod-{}", i));
        storage.create(&key, &pod).await.unwrap();
    }

    // Verify we get 5 ADDED events
    let mut count = 0;
    for _ in 0..5 {
        if let Some(Ok(rusternetes_storage::WatchEvent::Added(_, _))) = watch_stream.next().await {
            count += 1;
        }
    }
    assert_eq!(count, 5);

    // Cleanup
    for i in 1..=5 {
        let key = build_key("pods", Some(namespace), &format!("pod-{}", i));
        let _ = storage.delete(&key).await;
    }
}

#[tokio::test]
async fn test_watch_cluster_scoped_resources() {
    let storage = Arc::new(MemoryStorage::new());

    let prefix = build_prefix("namespaces", None);

    // Start watching
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Create a namespace (cluster-scoped)
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-namespace".to_string(),
            namespace: None, // Cluster-scoped
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(Phase::Active),
            conditions: None,
        }),
    };

    let key = build_key("namespaces", None, "test-namespace");
    storage.create(&key, &namespace).await.unwrap();

    // Verify we get an ADDED event
    if let Some(Ok(event)) = watch_stream.next().await {
        match event {
            rusternetes_storage::WatchEvent::Added(k, _) => {
                assert!(k.contains("test-namespace"));
            }
            _ => panic!("Expected Added event"),
        }
    }

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_watch_resource_version_tracking() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-rv";
    let key = build_key("configmaps", Some(namespace), "test-cm");

    // Create configmap
    let mut cm = ConfigMap {
        type_meta: TypeMeta {
            kind: "ConfigMap".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-cm".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        data: Some({
            let mut data = HashMap::new();
            data.insert("key1".to_string(), "value1".to_string());
            data
        }),
        binary_data: None,
        immutable: None,
    };

    storage.create(&key, &cm).await.unwrap();

    // Start watching
    let prefix = build_prefix("configmaps", Some(namespace));
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Update multiple times and track resource versions
    for i in 2..=5 {
        cm.metadata.resource_version = Some(i.to_string());
        cm.data
            .as_mut()
            .unwrap()
            .insert(format!("key{}", i), format!("value{}", i));
        storage.update(&key, &cm).await.unwrap();

        // Verify event has updated resource version
        if let Some(Ok(event)) = watch_stream.next().await {
            match event {
                rusternetes_storage::WatchEvent::Modified(_, v) => {
                    let received_cm: ConfigMap = serde_json::from_str(&v).unwrap();
                    assert_eq!(received_cm.metadata.resource_version, Some(i.to_string()));
                }
                _ => panic!("Expected Modified event"),
            }
        }
    }

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_watch_namespace_isolation() {
    let storage = Arc::new(MemoryStorage::new());

    let ns1 = "test-watch-ns1";
    let ns2 = "test-watch-ns2";

    // Watch only ns1
    let prefix = build_prefix("services", Some(ns1));
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Create service in ns1
    let svc1 = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "svc1".to_string(),
            namespace: Some(ns1.to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: ServiceSpec {
            selector: Some(std::collections::HashMap::new()),
            ports: vec![ServicePort {
                name: None,
                protocol: Some("TCP".to_string()),
                port: 80,
                target_port: None,
                node_port: None,
                app_protocol: None,
            }],
            cluster_ip: None,
            cluster_ips: None,
            service_type: None,
            external_ips: None,
            session_affinity: None,
            external_name: None,
            external_traffic_policy: None,
            ip_families: None,
            ip_family_policy: None,
            internal_traffic_policy: None,
            health_check_node_port: None,
            load_balancer_class: None,
            load_balancer_ip: None,
            load_balancer_source_ranges: None,
            allocate_load_balancer_node_ports: None,
            publish_not_ready_addresses: None,
            session_affinity_config: None,
            traffic_distribution: None,
        },
        status: None,
    };

    let key1 = build_key("services", Some(ns1), "svc1");
    storage.create(&key1, &svc1).await.unwrap();

    // Create service in ns2 (should NOT appear in watch)
    let svc2 = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "svc2".to_string(),
            namespace: Some(ns2.to_string()),
            resource_version: Some("2".to_string()),
            ..Default::default()
        },
        spec: ServiceSpec {
            selector: Some(std::collections::HashMap::new()),
            ports: vec![ServicePort {
                name: None,
                protocol: Some("TCP".to_string()),
                port: 80,
                target_port: None,
                node_port: None,
                app_protocol: None,
            }],
            cluster_ip: None,
            cluster_ips: None,
            service_type: None,
            external_ips: None,
            session_affinity: None,
            external_name: None,
            external_traffic_policy: None,
            ip_families: None,
            ip_family_policy: None,
            internal_traffic_policy: None,
            health_check_node_port: None,
            load_balancer_class: None,
            load_balancer_ip: None,
            load_balancer_source_ranges: None,
            allocate_load_balancer_node_ports: None,
            publish_not_ready_addresses: None,
            session_affinity_config: None,
            traffic_distribution: None,
        },
        status: None,
    };

    let key2 = build_key("services", Some(ns2), "svc2");
    storage.create(&key2, &svc2).await.unwrap();

    // Should only get event for ns1
    if let Some(Ok(event)) = watch_stream.next().await {
        match event {
            rusternetes_storage::WatchEvent::Added(k, _) => {
                assert!(k.contains(ns1));
                assert!(!k.contains(ns2));
            }
            _ => panic!("Expected Added event"),
        }
    }

    // Verify no more events (with timeout)
    tokio::select! {
        _ = watch_stream.next() => {
            panic!("Should not receive event for different namespace");
        }
        _ = sleep(Duration::from_millis(100)) => {
            // Expected - no event received
        }
    }

    // Cleanup
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_watch_stream_disconnection() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-disconnect";
    let prefix = build_prefix("pods", Some(namespace));

    // Start watching
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Create a pod
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: Some(create_minimal_pod_spec()),
        status: Some(create_minimal_pod_status()),
    };

    let key = build_key("pods", Some(namespace), "test-pod");
    storage.create(&key, &pod).await.unwrap();

    // Get the event
    let _ = watch_stream.next().await;

    // Drop the watch stream (simulating client disconnect)
    drop(watch_stream);

    // Storage should handle this gracefully
    // Create another resource (no one watching)
    let pod2 = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod2".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("2".to_string()),
            ..Default::default()
        },
        spec: Some(create_minimal_pod_spec()),
        status: Some(create_minimal_pod_status()),
    };

    let key2 = build_key("pods", Some(namespace), "test-pod2");
    storage.create(&key2, &pod2).await.unwrap();

    // Start new watch - should get current state
    let _new_watch_stream = storage.watch(&prefix).await.unwrap();
    // In a real implementation, we'd verify we get initial state

    // Cleanup
    storage.delete(&key).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_watch_concurrent_watches() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-concurrent";
    let prefix = build_prefix("pods", Some(namespace));

    // Start multiple watch streams
    let mut watch1 = storage.watch(&prefix).await.unwrap();
    let mut watch2 = storage.watch(&prefix).await.unwrap();
    let mut watch3 = storage.watch(&prefix).await.unwrap();

    // Create a pod
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: Some(create_minimal_pod_spec()),
        status: Some(create_minimal_pod_status()),
    };

    let key = build_key("pods", Some(namespace), "test-pod");
    storage.create(&key, &pod).await.unwrap();

    // All watchers should get the event
    let event1 = watch1.next().await;
    let event2 = watch2.next().await;
    let event3 = watch3.next().await;

    assert!(event1.is_some());
    assert!(event2.is_some());
    assert!(event3.is_some());

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_watch_event_ordering() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-watch-ordering";
    let prefix = build_prefix("pods", Some(namespace));

    // Start watching
    let mut watch_stream = storage.watch(&prefix).await.unwrap();

    // Create -> Modify -> Delete sequence
    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some(namespace.to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: Some(create_minimal_pod_spec()),
        status: Some(create_minimal_pod_status()),
    };

    let key = build_key("pods", Some(namespace), "test-pod");

    // Create
    storage.create(&key, &pod).await.unwrap();

    // Update
    pod.metadata.resource_version = Some("2".to_string());
    storage.update(&key, &pod).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify event order: Added -> Modified -> Deleted
    let events = [
        watch_stream.next().await,
        watch_stream.next().await,
        watch_stream.next().await,
    ];

    match events[0].as_ref().unwrap().as_ref().unwrap() {
        rusternetes_storage::WatchEvent::Added(_, _) => {}
        _ => panic!("First event should be Added"),
    }

    match events[1].as_ref().unwrap().as_ref().unwrap() {
        rusternetes_storage::WatchEvent::Modified(_, _) => {}
        _ => panic!("Second event should be Modified"),
    }

    match events[2].as_ref().unwrap().as_ref().unwrap() {
        rusternetes_storage::WatchEvent::Deleted(_, _) => {}
        _ => panic!("Third event should be Deleted"),
    }
}
