//! Integration tests for Out of Resource (OOR) handling and pod eviction
//!
//! These tests verify that the kubelet correctly:
//! - Detects resource pressure (memory, disk)
//! - Updates node conditions (MemoryPressure, DiskPressure)
//! - Evicts pods based on QoS class priority
//! - Evicts pods based on resource usage within QoS class

use rusternetes_common::resources::{Container, Node, NodeCondition, NodeStatus, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
use rusternetes_kubelet::eviction::{
    get_qos_class, EvictionManager, EvictionSignal, EvictionThreshold, EvictionValue, NodeStats,
    PodStats, QoSClass,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Drive the eviction manager past its first-observation gate. Hard
/// thresholds are not reported on the very first call (mirrors upstream
/// `thresholdsFirstObservedAt`). This helper ticks once at `t=0`, then
/// returns the result of a second tick `11s` later — past the
/// `HARD_MIN_OBSERVATION` window.
fn check_after_observation_window(
    manager: &mut EvictionManager,
    stats: &NodeStats,
) -> Vec<EvictionSignal> {
    let t0 = Instant::now();
    manager.check_eviction_needed_at(stats, t0);
    manager.check_eviction_needed_at(stats, t0 + Duration::from_secs(11))
}

/// Test helper to create a pod with specific QoS class
fn create_pod_with_qos(name: &str, namespace: &str, qos: QoSClass) -> Pod {
    let resources = match qos {
        QoSClass::Guaranteed => {
            // Guaranteed: limits == requests for all resources
            Some(ResourceRequirements {
                requests: Some(HashMap::from([
                    ("cpu".to_string(), "100m".to_string()),
                    ("memory".to_string(), "128Mi".to_string()),
                ])),
                limits: Some(HashMap::from([
                    ("cpu".to_string(), "100m".to_string()),
                    ("memory".to_string(), "128Mi".to_string()),
                ])),
                claims: None,
            })
        }
        QoSClass::Burstable => {
            // Burstable: some containers have limits/requests but not matching
            Some(ResourceRequirements {
                requests: Some(HashMap::from([
                    ("cpu".to_string(), "100m".to_string()),
                    ("memory".to_string(), "128Mi".to_string()),
                ])),
                limits: Some(HashMap::from([
                    ("cpu".to_string(), "200m".to_string()),
                    ("memory".to_string(), "256Mi".to_string()),
                ])),
                claims: None,
            })
        }
        QoSClass::BestEffort => {
            // BestEffort: no limits or requests
            None
        }
    };

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
                image: "nginx:latest".to_string(),
                resources,
                image_pull_policy: None,
                command: None,
                args: None,
                ports: None,
                env: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                working_dir: None,
                security_context: None,
                restart_policy: None,
                resize_policy: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            init_containers: None,
            ephemeral_containers: None,
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            node_name: Some("test-node".to_string()),
            volumes: None,
            affinity: None,
            tolerations: None,
            service_account_name: None,
            service_account: None,
            priority: None,
            priority_class_name: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
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
            ..Default::default()
        }),
        status: None,
    }
}

#[test]
fn test_qos_class_best_effort() {
    let pod = create_pod_with_qos("test-pod", "default", QoSClass::BestEffort);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

#[test]
fn test_qos_class_burstable() {
    let pod = create_pod_with_qos("test-pod", "default", QoSClass::Burstable);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

#[test]
fn test_qos_class_guaranteed() {
    let pod = create_pod_with_qos("test-pod", "default", QoSClass::Guaranteed);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

#[test]
fn test_qos_class_ordering() {
    // QoS classes should be ordered for eviction priority
    // BestEffort (1) < Burstable (2) < Guaranteed (3)
    assert!(QoSClass::BestEffort < QoSClass::Burstable);
    assert!(QoSClass::Burstable < QoSClass::Guaranteed);
    assert!(QoSClass::BestEffort < QoSClass::Guaranteed);
}

#[test]
fn test_memory_pressure_detection_hard_threshold() {
    let mut manager = EvictionManager::new();

    // Simulate low memory (50 MiB available, threshold is 100Mi)
    let low_memory_stats = NodeStats {
        memory_available_bytes: 50 * 1024 * 1024,   // 50 MiB
        memory_total_bytes: 8 * 1024 * 1024 * 1024, // 8 GiB
        nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
        nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
        nodefs_inodes_free: 1_000_000,
        nodefs_inodes_total: 10_000_000,
        pid_available: 30000,
        pid_total: 32768,
    };

    let signals = check_after_observation_window(&mut manager, &low_memory_stats);
    assert!(
        signals.contains(&EvictionSignal::MemoryAvailable),
        "Memory pressure should be detected when available memory is below hard threshold"
    );
}

#[test]
fn test_disk_pressure_detection_hard_threshold() {
    let mut manager = EvictionManager::new();

    // Simulate low disk space (5% available, threshold is 10%)
    let low_disk_stats = NodeStats {
        memory_available_bytes: 2 * 1024 * 1024 * 1024,
        memory_total_bytes: 8 * 1024 * 1024 * 1024,
        nodefs_available_bytes: 5 * 1024 * 1024 * 1024, // 5 GiB
        nodefs_total_bytes: 100 * 1024 * 1024 * 1024,   // 100 GiB (5%)
        nodefs_inodes_free: 1_000_000,
        nodefs_inodes_total: 10_000_000,
        pid_available: 30000,
        pid_total: 32768,
    };

    let signals = check_after_observation_window(&mut manager, &low_disk_stats);
    assert!(
        signals.contains(&EvictionSignal::NodeFsAvailable),
        "Disk pressure should be detected when available disk is below hard threshold"
    );
}

#[test]
fn test_inode_pressure_detection() {
    let mut manager = EvictionManager::new();

    // Simulate low inodes (3% free, threshold is 5%)
    let low_inode_stats = NodeStats {
        memory_available_bytes: 2 * 1024 * 1024 * 1024,
        memory_total_bytes: 8 * 1024 * 1024 * 1024,
        nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
        nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
        nodefs_inodes_free: 300_000,     // 300k inodes free
        nodefs_inodes_total: 10_000_000, // 10M total (3%)
        pid_available: 30000,
        pid_total: 32768,
    };

    let signals = check_after_observation_window(&mut manager, &low_inode_stats);
    assert!(
        signals.contains(&EvictionSignal::NodeFsInodesFree),
        "Inode pressure should be detected when free inodes are below hard threshold"
    );
}

#[test]
fn test_no_pressure_with_sufficient_resources() {
    let mut manager = EvictionManager::new();

    // Simulate healthy node (plenty of resources)
    let healthy_stats = NodeStats {
        memory_available_bytes: 4 * 1024 * 1024 * 1024, // 4 GiB available
        memory_total_bytes: 8 * 1024 * 1024 * 1024,     // 8 GiB total (50%)
        nodefs_available_bytes: 60 * 1024 * 1024 * 1024, // 60 GiB available
        nodefs_total_bytes: 100 * 1024 * 1024 * 1024,   // 100 GiB total (60%)
        nodefs_inodes_free: 8_000_000,                  // 8M inodes free
        nodefs_inodes_total: 10_000_000,                // 10M total (80%)
        pid_available: 30000,
        pid_total: 32768,
    };

    let signals = manager.check_eviction_needed(&healthy_stats);
    assert!(
        signals.is_empty(),
        "No eviction signals should be detected with sufficient resources"
    );
}

#[test]
fn test_pod_eviction_priority_by_qos() {
    let manager = EvictionManager::new();

    // Create pods with different QoS classes
    let best_effort_pod = create_pod_with_qos("best-effort", "default", QoSClass::BestEffort);
    let burstable_pod = create_pod_with_qos("burstable", "default", QoSClass::Burstable);
    let guaranteed_pod = create_pod_with_qos("guaranteed", "default", QoSClass::Guaranteed);

    let pods = vec![guaranteed_pod, best_effort_pod, burstable_pod];

    // Create pod stats
    let mut pod_stats = HashMap::new();
    pod_stats.insert(
        "default/best-effort".to_string(),
        PodStats {
            name: "best-effort".to_string(),
            namespace: "default".to_string(),
            memory_usage_bytes: 100 * 1024 * 1024,
            disk_usage_bytes: 500 * 1024 * 1024,
            qos_class: QoSClass::BestEffort,
        },
    );
    pod_stats.insert(
        "default/burstable".to_string(),
        PodStats {
            name: "burstable".to_string(),
            namespace: "default".to_string(),
            memory_usage_bytes: 150 * 1024 * 1024,
            disk_usage_bytes: 600 * 1024 * 1024,
            qos_class: QoSClass::Burstable,
        },
    );
    pod_stats.insert(
        "default/guaranteed".to_string(),
        PodStats {
            name: "guaranteed".to_string(),
            namespace: "default".to_string(),
            memory_usage_bytes: 200 * 1024 * 1024,
            disk_usage_bytes: 700 * 1024 * 1024,
            qos_class: QoSClass::Guaranteed,
        },
    );

    let pods_to_evict =
        manager.select_pods_for_eviction(&pods, &pod_stats, &EvictionSignal::MemoryAvailable);

    // BestEffort should be evicted first
    assert!(
        !pods_to_evict.is_empty(),
        "At least one pod should be selected for eviction"
    );
    assert_eq!(
        pods_to_evict[0], "default/best-effort",
        "BestEffort pod should be evicted first"
    );
}

#[test]
fn test_pod_eviction_by_resource_usage() {
    let manager = EvictionManager::new();

    // Create multiple BestEffort pods with different memory usage
    let pod1 = create_pod_with_qos("pod-1", "default", QoSClass::BestEffort);
    let pod2 = create_pod_with_qos("pod-2", "default", QoSClass::BestEffort);
    let pod3 = create_pod_with_qos("pod-3", "default", QoSClass::BestEffort);

    let pods = vec![pod1, pod2, pod3];

    // Create pod stats with different memory usage
    let mut pod_stats = HashMap::new();
    pod_stats.insert(
        "default/pod-1".to_string(),
        PodStats {
            name: "pod-1".to_string(),
            namespace: "default".to_string(),
            memory_usage_bytes: 100 * 1024 * 1024, // 100 MiB
            disk_usage_bytes: 500 * 1024 * 1024,
            qos_class: QoSClass::BestEffort,
        },
    );
    pod_stats.insert(
        "default/pod-2".to_string(),
        PodStats {
            name: "pod-2".to_string(),
            namespace: "default".to_string(),
            memory_usage_bytes: 300 * 1024 * 1024, // 300 MiB (highest)
            disk_usage_bytes: 500 * 1024 * 1024,
            qos_class: QoSClass::BestEffort,
        },
    );
    pod_stats.insert(
        "default/pod-3".to_string(),
        PodStats {
            name: "pod-3".to_string(),
            namespace: "default".to_string(),
            memory_usage_bytes: 200 * 1024 * 1024, // 200 MiB
            disk_usage_bytes: 500 * 1024 * 1024,
            qos_class: QoSClass::BestEffort,
        },
    );

    let pods_to_evict =
        manager.select_pods_for_eviction(&pods, &pod_stats, &EvictionSignal::MemoryAvailable);

    // Pod with highest memory usage should be evicted first
    assert!(
        !pods_to_evict.is_empty(),
        "At least one pod should be selected for eviction"
    );
    assert_eq!(
        pods_to_evict[0], "default/pod-2",
        "Pod with highest memory usage should be evicted first"
    );
}

#[test]
fn test_node_condition_updates_memory_pressure() {
    let manager = EvictionManager::new();
    let mut node = Node::new("test-node");
    node.status = Some(NodeStatus {
        capacity: None,
        allocatable: None,
        conditions: Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_heartbeat_time: Some(chrono::Utc::now()),
            last_transition_time: Some(chrono::Utc::now()),
            reason: Some("KubeletReady".to_string()),
            message: Some("kubelet is ready".to_string()),
        }]),
        addresses: None,
        node_info: None,
        images: None,
        volumes_in_use: None,
        volumes_attached: None,
        daemon_endpoints: None,
        config: None,
        features: None,
        runtime_handlers: None,
        declared_features: None,
    });

    // Update conditions with memory pressure
    let signals = vec![EvictionSignal::MemoryAvailable];
    manager
        .update_node_conditions(&mut node, &signals)
        .expect("Failed to update node conditions");

    // Verify MemoryPressure condition is set
    let conditions = node.status.as_ref().unwrap().conditions.as_ref().unwrap();
    let memory_pressure = conditions
        .iter()
        .find(|c| c.condition_type == "MemoryPressure")
        .expect("MemoryPressure condition should be present");

    assert_eq!(
        memory_pressure.status, "True",
        "MemoryPressure should be True"
    );
    assert_eq!(
        memory_pressure.reason,
        Some("NodeHasMemoryPressure".to_string()),
        "Reason should indicate memory pressure"
    );
}

#[test]
fn test_node_condition_updates_disk_pressure() {
    let manager = EvictionManager::new();
    let mut node = Node::new("test-node");
    node.status = Some(NodeStatus {
        capacity: None,
        allocatable: None,
        conditions: Some(vec![]),
        addresses: None,
        node_info: None,
        images: None,
        volumes_in_use: None,
        volumes_attached: None,
        daemon_endpoints: None,
        config: None,
        features: None,
        runtime_handlers: None,
        declared_features: None,
    });

    // Update conditions with disk pressure
    let signals = vec![EvictionSignal::NodeFsAvailable];
    manager
        .update_node_conditions(&mut node, &signals)
        .expect("Failed to update node conditions");

    // Verify DiskPressure condition is set
    let conditions = node.status.as_ref().unwrap().conditions.as_ref().unwrap();
    let disk_pressure = conditions
        .iter()
        .find(|c| c.condition_type == "DiskPressure")
        .expect("DiskPressure condition should be present");

    assert_eq!(disk_pressure.status, "True", "DiskPressure should be True");
    assert_eq!(
        disk_pressure.reason,
        Some("NodeHasDiskPressure".to_string()),
        "Reason should indicate disk pressure"
    );
}

#[test]
fn test_node_condition_clears_when_pressure_resolved() {
    let manager = EvictionManager::new();
    let mut node = Node::new("test-node");
    node.status = Some(NodeStatus {
        capacity: None,
        allocatable: None,
        conditions: Some(vec![]),
        addresses: None,
        node_info: None,
        images: None,
        volumes_in_use: None,
        volumes_attached: None,
        daemon_endpoints: None,
        config: None,
        features: None,
        runtime_handlers: None,
        declared_features: None,
    });

    // Set memory pressure
    let signals = vec![EvictionSignal::MemoryAvailable];
    manager
        .update_node_conditions(&mut node, &signals)
        .expect("Failed to update node conditions");

    let conditions = node.status.as_ref().unwrap().conditions.as_ref().unwrap();
    let memory_pressure = conditions
        .iter()
        .find(|c| c.condition_type == "MemoryPressure")
        .unwrap();
    assert_eq!(memory_pressure.status, "True");

    // Clear pressure (empty signals)
    manager
        .update_node_conditions(&mut node, &[])
        .expect("Failed to clear node conditions");

    let conditions = node.status.as_ref().unwrap().conditions.as_ref().unwrap();
    let memory_pressure = conditions
        .iter()
        .find(|c| c.condition_type == "MemoryPressure")
        .unwrap();
    assert_eq!(
        memory_pressure.status, "False",
        "MemoryPressure should be False when pressure is resolved"
    );
    assert_eq!(
        memory_pressure.reason,
        Some("NodeHasSufficientMemory".to_string())
    );
}

#[test]
fn test_eviction_threshold_percentage() {
    let threshold = EvictionThreshold {
        signal: EvictionSignal::NodeFsAvailable,
        hard: Some(EvictionValue::Percentage(10.0)),
        soft: None,
        grace_period: None,
    };

    let mut manager = EvictionManager::new();
    manager.thresholds = vec![threshold];

    // 5% disk available (below 10% threshold)
    let stats = NodeStats {
        memory_available_bytes: 2 * 1024 * 1024 * 1024,
        memory_total_bytes: 8 * 1024 * 1024 * 1024,
        nodefs_available_bytes: 5 * 1024 * 1024 * 1024, // 5 GiB
        nodefs_total_bytes: 100 * 1024 * 1024 * 1024,   // 100 GiB (5%)
        nodefs_inodes_free: 1_000_000,
        nodefs_inodes_total: 10_000_000,
        pid_available: 30000,
        pid_total: 32768,
    };

    let signals = check_after_observation_window(&mut manager, &stats);
    assert!(signals.contains(&EvictionSignal::NodeFsAvailable));
}

#[test]
fn test_eviction_max_pods_evicted_per_iteration() {
    let manager = EvictionManager::new();

    // Create 10 BestEffort pods
    let pods: Vec<Pod> = (0..10)
        .map(|i| create_pod_with_qos(&format!("pod-{}", i), "default", QoSClass::BestEffort))
        .collect();

    // Create pod stats for all pods
    let mut pod_stats = HashMap::new();
    for i in 0..10 {
        pod_stats.insert(
            format!("default/pod-{}", i),
            PodStats {
                name: format!("pod-{}", i),
                namespace: "default".to_string(),
                memory_usage_bytes: (100 + i * 10) * 1024 * 1024,
                disk_usage_bytes: 500 * 1024 * 1024,
                qos_class: QoSClass::BestEffort,
            },
        );
    }

    let pods_to_evict =
        manager.select_pods_for_eviction(&pods, &pod_stats, &EvictionSignal::MemoryAvailable);

    // Should evict at most 5 pods per iteration
    assert!(
        pods_to_evict.len() <= 5,
        "Should evict at most 5 pods per iteration, got {}",
        pods_to_evict.len()
    );
}
