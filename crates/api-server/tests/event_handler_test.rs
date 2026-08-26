//! Integration tests for Event handler
//!
//! Tests all CRUD operations, edge cases, and error handling for Events

use rusternetes_common::resources::{Event, EventSource, EventType, ObjectReference};
use rusternetes_common::types::ObjectMeta;
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// Helper function to create test Event
fn create_test_event(name: &str, namespace: &str) -> Event {
    Event {
        api_version: "v1".to_string(),
        kind: "Event".to_string(),
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        involved_object: ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some(namespace.to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("pod-uid-123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: Some("1".to_string()),
            field_path: None,
        },
        reason: "PodStarted".to_string(),
        message: "Pod started successfully".to_string(),
        source: EventSource {
            component: "kubelet".to_string(),
            host: Some("node-1".to_string()),
        },
        first_timestamp: Some(chrono::Utc::now()),
        last_timestamp: Some(chrono::Utc::now()),
        count: 1,
        event_type: EventType::Normal,
        action: Some("Started".to_string()),
        related: None,
        series: None,
        event_time: None,
        reporting_component: None,
        reporting_instance: None,
        note: None,
        regarding: None,
        extra: None,
    }
}

#[tokio::test]
async fn test_event_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-create";
    let event = create_test_event("test-event", namespace);

    let key = build_key("events", Some(namespace), "test-event");
    let created: Event = storage.create(&key, &event).await.unwrap();

    assert_eq!(created.metadata.name, "test-event");
    assert_eq!(created.metadata.namespace, Some(namespace.to_string()));
    assert_eq!(created.reason, "PodStarted");
    assert_eq!(created.count, 1);
    assert!(!created.metadata.uid.is_empty());
    assert!(created.metadata.creation_timestamp.is_some());

    // Get the event
    let retrieved: Event = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-event");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_update() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-update";
    let mut event = create_test_event("test-event", namespace);

    let key = build_key("events", Some(namespace), "test-event");
    storage.create(&key, &event).await.unwrap();

    // Update event count (simulating repeated event)
    event.count = 5;
    event.message = "Pod started successfully (repeated 5 times)".to_string();
    event.last_timestamp = Some(chrono::Utc::now());

    let updated: Event = storage.update(&key, &event).await.unwrap();
    assert_eq!(updated.count, 5);
    assert!(updated.message.contains("repeated 5 times"));

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-delete";
    let event = create_test_event("test-event", namespace);

    let key = build_key("events", Some(namespace), "test-event");
    storage.create(&key, &event).await.unwrap();

    // Delete the event
    storage.delete(&key).await.unwrap();

    // Verify it's deleted
    let result: rusternetes_common::Result<Event> = storage.get(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_event_list_in_namespace() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-list";

    // Create multiple events
    for i in 1..=5 {
        let event = create_test_event(&format!("event-{}", i), namespace);
        let key = build_key("events", Some(namespace), &format!("event-{}", i));
        storage.create(&key, &event).await.unwrap();
    }

    // List events in namespace
    let prefix = build_prefix("events", Some(namespace));
    let events: Vec<Event> = storage.list(&prefix).await.unwrap();

    assert_eq!(events.len(), 5);

    // Cleanup
    for i in 1..=5 {
        let key = build_key("events", Some(namespace), &format!("event-{}", i));
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_event_namespace_isolation() {
    let storage = Arc::new(MemoryStorage::new());

    let ns1 = "test-event-ns1";
    let ns2 = "test-event-ns2";

    // Create events in different namespaces
    let event1 = create_test_event("event-1", ns1);
    let event2 = create_test_event("event-2", ns2);

    let key1 = build_key("events", Some(ns1), "event-1");
    let key2 = build_key("events", Some(ns2), "event-2");

    storage.create(&key1, &event1).await.unwrap();
    storage.create(&key2, &event2).await.unwrap();

    // List events in ns1 - should only see event1
    let prefix1 = build_prefix("events", Some(ns1));
    let events1: Vec<Event> = storage.list(&prefix1).await.unwrap();
    assert_eq!(events1.len(), 1);
    assert_eq!(events1[0].metadata.name, "event-1");

    // List events in ns2 - should only see event2
    let prefix2 = build_prefix("events", Some(ns2));
    let events2: Vec<Event> = storage.list(&prefix2).await.unwrap();
    assert_eq!(events2.len(), 1);
    assert_eq!(events2[0].metadata.name, "event-2");

    // Cleanup
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_event_types() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-types";

    // Normal event
    let mut normal_event = create_test_event("normal-event", namespace);
    normal_event.event_type = EventType::Normal;
    normal_event.reason = "Started".to_string();

    let key1 = build_key("events", Some(namespace), "normal-event");
    let created1: Event = storage.create(&key1, &normal_event).await.unwrap();
    assert_eq!(created1.event_type, EventType::Normal);

    // Warning event
    let mut warning_event = create_test_event("warning-event", namespace);
    warning_event.event_type = EventType::Warning;
    warning_event.reason = "FailedScheduling".to_string();
    warning_event.message = "Pod could not be scheduled".to_string();

    let key2 = build_key("events", Some(namespace), "warning-event");
    let created2: Event = storage.create(&key2, &warning_event).await.unwrap();
    assert_eq!(created2.event_type, EventType::Warning);
    assert_eq!(created2.reason, "FailedScheduling");

    // Cleanup
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_event_different_involved_objects() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-objects";

    // Pod event
    let pod_event = create_test_event("pod-event", namespace);
    let key1 = build_key("events", Some(namespace), "pod-event");
    let created1: Event = storage.create(&key1, &pod_event).await.unwrap();
    assert_eq!(created1.involved_object.kind, Some("Pod".to_string()));

    // Service event
    let mut service_event = create_test_event("service-event", namespace);
    service_event.involved_object = ObjectReference {
        kind: Some("Service".to_string()),
        namespace: Some(namespace.to_string()),
        name: Some("test-service".to_string()),
        uid: Some("service-uid-123".to_string()),
        api_version: Some("v1".to_string()),
        resource_version: Some("1".to_string()),
        field_path: None,
    };
    service_event.reason = "ServiceCreated".to_string();

    let key2 = build_key("events", Some(namespace), "service-event");
    let created2: Event = storage.create(&key2, &service_event).await.unwrap();
    assert_eq!(created2.involved_object.kind, Some("Service".to_string()));

    // Deployment event
    let mut deployment_event = create_test_event("deployment-event", namespace);
    deployment_event.involved_object = ObjectReference {
        kind: Some("Deployment".to_string()),
        namespace: Some(namespace.to_string()),
        name: Some("test-deployment".to_string()),
        uid: Some("deployment-uid-123".to_string()),
        api_version: Some("apps/v1".to_string()),
        resource_version: Some("1".to_string()),
        field_path: None,
    };
    deployment_event.reason = "ScalingReplicaSet".to_string();

    let key3 = build_key("events", Some(namespace), "deployment-event");
    let created3: Event = storage.create(&key3, &deployment_event).await.unwrap();
    assert_eq!(
        created3.involved_object.kind,
        Some("Deployment".to_string())
    );

    // Cleanup
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_event_source_components() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-sources";

    // Kubelet event
    let mut kubelet_event = create_test_event("kubelet-event", namespace);
    kubelet_event.source = EventSource {
        component: "kubelet".to_string(),
        host: Some("node-1".to_string()),
    };

    let key1 = build_key("events", Some(namespace), "kubelet-event");
    let created1: Event = storage.create(&key1, &kubelet_event).await.unwrap();
    assert_eq!(created1.source.component, "kubelet".to_string());

    // Scheduler event
    let mut scheduler_event = create_test_event("scheduler-event", namespace);
    scheduler_event.source = EventSource {
        component: "default-scheduler".to_string(),
        host: None,
    };
    scheduler_event.reason = "Scheduled".to_string();

    let key2 = build_key("events", Some(namespace), "scheduler-event");
    let created2: Event = storage.create(&key2, &scheduler_event).await.unwrap();
    assert_eq!(created2.source.component, "default-scheduler".to_string());

    // Controller manager event
    let mut controller_event = create_test_event("controller-event", namespace);
    controller_event.source = EventSource {
        component: "replicaset-controller".to_string(),
        host: None,
    };
    controller_event.reason = "SuccessfulCreate".to_string();

    let key3 = build_key("events", Some(namespace), "controller-event");
    let created3: Event = storage.create(&key3, &controller_event).await.unwrap();
    assert_eq!(
        created3.source.component,
        "replicaset-controller".to_string()
    );

    // Cleanup
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_event_count_aggregation() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-aggregation";
    let mut event = create_test_event("repeated-event", namespace);

    let key = build_key("events", Some(namespace), "repeated-event");

    // Create initial event
    event.count = 1;
    event.first_timestamp = Some(chrono::Utc::now());
    event.last_timestamp = Some(chrono::Utc::now());
    storage.create(&key, &event).await.unwrap();

    // Simulate event aggregation (count increases over time)
    for count in 2..=10 {
        event.count = count;
        event.last_timestamp = Some(chrono::Utc::now());
        storage.update(&key, &event).await.unwrap();
    }

    // Verify final count
    let final_event: Event = storage.get(&key).await.unwrap();
    assert_eq!(final_event.count, 10);

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_with_action() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-action";

    // Event with action field
    let mut event = create_test_event("action-event", namespace);
    event.action = Some("Pulling".to_string());
    event.reason = "Pulling".to_string();
    event.message = "Pulling image \"nginx:latest\"".to_string();

    let key = build_key("events", Some(namespace), "action-event");
    let created: Event = storage.create(&key, &event).await.unwrap();

    assert_eq!(created.action, Some("Pulling".to_string()));
    assert_eq!(created.reason, "Pulling");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_with_related_object() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-related";

    // Event with related object (e.g., ReplicaSet creates Pods)
    let mut event = create_test_event("related-event", namespace);
    event.involved_object = ObjectReference {
        kind: Some("Pod".to_string()),
        namespace: Some(namespace.to_string()),
        name: Some("test-pod-123".to_string()),
        uid: Some("pod-uid-123".to_string()),
        api_version: Some("v1".to_string()),
        resource_version: Some("1".to_string()),
        field_path: None,
    };
    event.related = Some(ObjectReference {
        kind: Some("ReplicaSet".to_string()),
        namespace: Some(namespace.to_string()),
        name: Some("test-rs".to_string()),
        uid: Some("rs-uid-123".to_string()),
        api_version: Some("apps/v1".to_string()),
        resource_version: Some("1".to_string()),
        field_path: None,
    });
    event.reason = "Created".to_string();

    let key = build_key("events", Some(namespace), "related-event");
    let created: Event = storage.create(&key, &event).await.unwrap();

    assert!(created.related.is_some());
    assert_eq!(
        created.related.as_ref().unwrap().kind,
        Some("ReplicaSet".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_with_field_path() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-field-path";

    // Event with field path (e.g., spec.containers{nginx})
    let mut event = create_test_event("field-path-event", namespace);
    event.involved_object.field_path = Some("spec.containers{nginx}".to_string());
    event.reason = "BackOff".to_string();
    event.message = "Back-off restarting failed container".to_string();

    let key = build_key("events", Some(namespace), "field-path-event");
    let created: Event = storage.create(&key, &event).await.unwrap();

    assert_eq!(
        created.involved_object.field_path,
        Some("spec.containers{nginx}".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-labels";
    let mut event = create_test_event("labeled-event", namespace);

    event.metadata.labels = Some({
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test-app".to_string());
        labels.insert("severity".to_string(), "warning".to_string());
        labels
    });

    let key = build_key("events", Some(namespace), "labeled-event");
    let created: Event = storage.create(&key, &event).await.unwrap();

    assert!(created.metadata.labels.is_some());
    assert_eq!(
        created.metadata.labels.as_ref().unwrap().get("app"),
        Some(&"test-app".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-annotations";
    let mut event = create_test_event("annotated-event", namespace);

    event.metadata.annotations = Some({
        let mut annotations = HashMap::new();
        annotations.insert(
            "description".to_string(),
            "Test event for monitoring".to_string(),
        );
        annotations.insert("alert-level".to_string(), "info".to_string());
        annotations
    });

    let key = build_key("events", Some(namespace), "annotated-event");
    let created: Event = storage.create(&key, &event).await.unwrap();

    assert!(created.metadata.annotations.is_some());
    assert_eq!(
        created
            .metadata
            .annotations
            .as_ref()
            .unwrap()
            .get("description"),
        Some(&"Test event for monitoring".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-immutability";
    let event = create_test_event("test-event", namespace);

    let key = build_key("events", Some(namespace), "test-event");
    let created: Event = storage.create(&key, &event).await.unwrap();

    let original_uid = created.metadata.uid.clone();
    let original_creation_timestamp = created.metadata.creation_timestamp;

    // Update should preserve UID and creation timestamp
    let updated: Event = storage.update(&key, &created).await.unwrap();

    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(
        updated.metadata.creation_timestamp,
        original_creation_timestamp
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_event_list_all_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create events in multiple namespaces
    for i in 1..=3 {
        let namespace = format!("ns-{}", i);
        let event = create_test_event(&format!("event-{}", i), &namespace);
        let key = build_key("events", Some(&namespace), &format!("event-{}", i));
        storage.create(&key, &event).await.unwrap();
    }

    // List all events across all namespaces
    let prefix = build_prefix("events", None);
    let all_events: Vec<Event> = storage.list(&prefix).await.unwrap();

    assert!(all_events.len() >= 3);

    // Cleanup
    for i in 1..=3 {
        let namespace = format!("ns-{}", i);
        let key = build_key("events", Some(&namespace), &format!("event-{}", i));
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_event_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-not-found";
    let key = build_key("events", Some(namespace), "nonexistent");

    let result: rusternetes_common::Result<Event> = storage.get(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_event_common_reasons() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-event-reasons";

    // Common Kubernetes event reasons
    let reasons = vec![
        ("Scheduled", "Successfully assigned pod to node"),
        ("Pulling", "Pulling image \"nginx:latest\""),
        ("Pulled", "Successfully pulled image \"nginx:latest\""),
        ("Created", "Created container nginx"),
        ("Started", "Started container nginx"),
        ("Killing", "Stopping container nginx"),
        ("FailedScheduling", "0/3 nodes are available"),
        ("BackOff", "Back-off restarting failed container"),
        ("Unhealthy", "Liveness probe failed"),
        ("FailedMount", "Unable to mount volumes"),
    ];

    for (idx, (reason, message)) in reasons.iter().enumerate() {
        let mut event = create_test_event(&format!("event-{}", idx), namespace);
        event.reason = reason.to_string();
        event.message = message.to_string();

        let key = build_key("events", Some(namespace), &format!("event-{}", idx));
        let created: Event = storage.create(&key, &event).await.unwrap();

        assert_eq!(created.reason, *reason);
        assert_eq!(created.message, *message);

        // Cleanup
        storage.delete(&key).await.unwrap();
    }
}
