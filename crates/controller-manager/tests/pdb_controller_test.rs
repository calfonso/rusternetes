//! Integration tests for PodDisruptionBudgetController
//!
//! Tests validate that the controller correctly:
//! - Calculates currentHealthy by counting healthy pods matching selector
//! - Calculates desiredHealthy from minAvailable or maxUnavailable
//! - Calculates disruptionsAllowed = currentHealthy - desiredHealthy
//! - Updates PDB status fields
//!
//! Note: Eviction admission decisions happen in the API server's eviction subresource,
//! not in the PDB controller. The controller only maintains status.

use rusternetes_common::resources::{
    pod::{Container, Pod, PodSpec, PodStatus},
    IntOrString, PodDisruptionBudget, PodDisruptionBudgetSpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::pod_disruption_budget::PodDisruptionBudgetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

fn create_test_pod(
    name: &str,
    namespace: &str,
    labels: HashMap<String, String>,
    is_healthy: bool,
) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            labels: Some(labels),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx".to_string(),
                image_pull_policy: Some("IfNotPresent".to_string()),
                command: None,
                args: None,
                ports: None,
                env: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                resources: None,
                working_dir: None,
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
                ..Default::default()
            }],
            init_containers: None,
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
            ephemeral_containers: None,
            overhead: None,
            scheduler_name: None,
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
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(if is_healthy {
                Phase::Running
            } else {
                Phase::Pending
            }),
            message: None,
            reason: None,
            host_ip: Some("10.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.1".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        }),
    }
}

#[tokio::test]
async fn test_pdb_calculates_status_with_min_available() {
    let storage = setup_test().await;
    let controller = PodDisruptionBudgetController::new(storage.clone());

    // Create PDB with minAvailable=2
    let spec = PodDisruptionBudgetSpec {
        min_available: Some(IntOrString::Int(2)),
        max_unavailable: None,
        selector: LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
            match_expressions: None,
        },
        unhealthy_pod_eviction_policy: None,
    };

    let pdb = PodDisruptionBudget::new("web-pdb", "default", spec);
    let pdb_key = build_key("poddisruptionbudgets", Some("default"), "web-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create 3 healthy pods with matching labels
    for i in 0..3 {
        let pod = create_test_pod(
            &format!("web-{}", i),
            "default",
            HashMap::from([("app".to_string(), "web".to_string())]),
            true,
        );
        let pod_key = build_key("pods", Some("default"), &format!("web-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify status was updated correctly
    let updated_pdb: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated_pdb.status.expect("Status should be set");

    assert_eq!(status.current_healthy, 3, "Should count 3 healthy pods");
    assert_eq!(status.desired_healthy, 2, "minAvailable=2");
    assert_eq!(
        status.disruptions_allowed, 1,
        "3 - 2 = 1 disruption allowed"
    );
    assert_eq!(status.expected_pods, 3, "Total pods matching selector");
}

#[tokio::test]
async fn test_pdb_calculates_status_with_max_unavailable() {
    let storage = setup_test().await;
    let controller = PodDisruptionBudgetController::new(storage.clone());

    // Create PDB with maxUnavailable=1
    let spec = PodDisruptionBudgetSpec {
        min_available: None,
        max_unavailable: Some(IntOrString::Int(1)),
        selector: LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "api".to_string())])),
            match_expressions: None,
        },
        unhealthy_pod_eviction_policy: None,
    };

    let pdb = PodDisruptionBudget::new("api-pdb", "default", spec);
    let pdb_key = build_key("poddisruptionbudgets", Some("default"), "api-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create 5 healthy pods
    for i in 0..5 {
        let pod = create_test_pod(
            &format!("api-{}", i),
            "default",
            HashMap::from([("app".to_string(), "api".to_string())]),
            true,
        );
        let pod_key = build_key("pods", Some("default"), &format!("api-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify status
    let updated_pdb: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated_pdb.status.expect("Status should be set");

    assert_eq!(status.current_healthy, 5);
    assert_eq!(status.desired_healthy, 4, "5 - maxUnavailable(1) = 4");
    assert_eq!(status.disruptions_allowed, 1, "5 - 4 = 1");
    assert_eq!(status.expected_pods, 5);
}

#[tokio::test]
async fn test_pdb_blocks_disruptions_when_at_minimum() {
    let storage = setup_test().await;
    let controller = PodDisruptionBudgetController::new(storage.clone());

    // Create PDB requiring all 3 pods (minAvailable=3)
    let spec = PodDisruptionBudgetSpec {
        min_available: Some(IntOrString::Int(3)),
        max_unavailable: None,
        selector: LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "critical".to_string())])),
            match_expressions: None,
        },
        unhealthy_pod_eviction_policy: None,
    };

    let pdb = PodDisruptionBudget::new("critical-pdb", "default", spec);
    let pdb_key = build_key("poddisruptionbudgets", Some("default"), "critical-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create exactly 3 healthy pods
    for i in 0..3 {
        let pod = create_test_pod(
            &format!("critical-{}", i),
            "default",
            HashMap::from([("app".to_string(), "critical".to_string())]),
            true,
        );
        let pod_key = build_key("pods", Some("default"), &format!("critical-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify no disruptions allowed
    let updated_pdb: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated_pdb.status.expect("Status should be set");

    assert_eq!(status.current_healthy, 3);
    assert_eq!(status.desired_healthy, 3);
    assert_eq!(
        status.disruptions_allowed, 0,
        "No disruptions allowed - at minimum"
    );
}

#[tokio::test]
async fn test_pdb_respects_label_selector() {
    let storage = setup_test().await;
    let controller = PodDisruptionBudgetController::new(storage.clone());

    // Create PDB matching specific labels
    let spec = PodDisruptionBudgetSpec {
        min_available: Some(IntOrString::Int(2)),
        max_unavailable: None,
        selector: LabelSelector {
            match_labels: Some(HashMap::from([
                ("app".to_string(), "web".to_string()),
                ("tier".to_string(), "frontend".to_string()),
            ])),
            match_expressions: None,
        },
        unhealthy_pod_eviction_policy: None,
    };

    let pdb = PodDisruptionBudget::new("frontend-pdb", "default", spec);
    let pdb_key = build_key("poddisruptionbudgets", Some("default"), "frontend-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create 2 matching pods
    for i in 0..2 {
        let pod = create_test_pod(
            &format!("frontend-{}", i),
            "default",
            HashMap::from([
                ("app".to_string(), "web".to_string()),
                ("tier".to_string(), "frontend".to_string()),
            ]),
            true,
        );
        let pod_key = build_key("pods", Some("default"), &format!("frontend-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Create 2 non-matching pods (different tier)
    for i in 0..2 {
        let pod = create_test_pod(
            &format!("backend-{}", i),
            "default",
            HashMap::from([
                ("app".to_string(), "web".to_string()),
                ("tier".to_string(), "backend".to_string()),
            ]),
            true,
        );
        let pod_key = build_key("pods", Some("default"), &format!("backend-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify only matching pods counted
    let updated_pdb: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated_pdb.status.expect("Status should be set");

    assert_eq!(status.expected_pods, 2, "Should only count matching pods");
    assert_eq!(status.current_healthy, 2);
}

#[tokio::test]
async fn test_pdb_namespace_isolation() {
    let storage = setup_test().await;
    let controller = PodDisruptionBudgetController::new(storage.clone());

    // Create PDB in "production" namespace
    let spec = PodDisruptionBudgetSpec {
        min_available: Some(IntOrString::Int(3)),
        max_unavailable: None,
        selector: LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
            match_expressions: None,
        },
        unhealthy_pod_eviction_policy: None,
    };

    let pdb = PodDisruptionBudget::new("prod-pdb", "production", spec);
    let pdb_key = build_key("poddisruptionbudgets", Some("production"), "prod-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create 3 pods in production
    for i in 0..3 {
        let pod = create_test_pod(
            &format!("web-{}", i),
            "production",
            HashMap::from([("app".to_string(), "web".to_string())]),
            true,
        );
        let pod_key = build_key("pods", Some("production"), &format!("web-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Create 5 pods in staging (should NOT be counted)
    for i in 0..5 {
        let pod = create_test_pod(
            &format!("web-{}", i),
            "staging",
            HashMap::from([("app".to_string(), "web".to_string())]),
            true,
        );
        let pod_key = build_key("pods", Some("staging"), &format!("web-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify only production pods counted
    let updated_pdb: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated_pdb.status.expect("Status should be set");

    assert_eq!(status.expected_pods, 3, "Should only count production pods");
    assert_eq!(status.current_healthy, 3);
    assert_eq!(status.disruptions_allowed, 0);
}

#[tokio::test]
async fn test_pdb_percentage_min_available() {
    let storage = setup_test().await;
    let controller = PodDisruptionBudgetController::new(storage.clone());

    // Create PDB with 80% minAvailable
    let spec = PodDisruptionBudgetSpec {
        min_available: Some(IntOrString::String("80%".to_string())),
        max_unavailable: None,
        selector: LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "cache".to_string())])),
            match_expressions: None,
        },
        unhealthy_pod_eviction_policy: None,
    };

    let pdb = PodDisruptionBudget::new("cache-pdb", "default", spec);
    let pdb_key = build_key("poddisruptionbudgets", Some("default"), "cache-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create 10 healthy pods
    for i in 0..10 {
        let pod = create_test_pod(
            &format!("cache-{}", i),
            "default",
            HashMap::from([("app".to_string(), "cache".to_string())]),
            true,
        );
        let pod_key = build_key("pods", Some("default"), &format!("cache-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify percentage calculation
    let updated_pdb: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated_pdb.status.expect("Status should be set");

    assert_eq!(status.expected_pods, 10);
    assert_eq!(status.current_healthy, 10);
    assert_eq!(status.desired_healthy, 8, "80% of 10 = 8 (ceil)");
    assert_eq!(status.disruptions_allowed, 2, "10 - 8 = 2");
}

#[tokio::test]
async fn test_pdb_percentage_max_unavailable() {
    let storage = setup_test().await;
    let controller = PodDisruptionBudgetController::new(storage.clone());

    // Create PDB with 30% maxUnavailable
    let spec = PodDisruptionBudgetSpec {
        min_available: None,
        max_unavailable: Some(IntOrString::String("30%".to_string())),
        selector: LabelSelector {
            match_labels: Some(HashMap::from([(
                "component".to_string(),
                "worker".to_string(),
            )])),
            match_expressions: None,
        },
        unhealthy_pod_eviction_policy: None,
    };

    let pdb = PodDisruptionBudget::new("worker-pdb", "default", spec);
    let pdb_key = build_key("poddisruptionbudgets", Some("default"), "worker-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create 10 healthy pods
    for i in 0..10 {
        let pod = create_test_pod(
            &format!("worker-{}", i),
            "default",
            HashMap::from([("component".to_string(), "worker".to_string())]),
            true,
        );
        let pod_key = build_key("pods", Some("default"), &format!("worker-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify percentage calculation
    let updated_pdb: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated_pdb.status.expect("Status should be set");

    assert_eq!(status.expected_pods, 10);
    assert_eq!(status.current_healthy, 10);
    assert_eq!(
        status.desired_healthy, 7,
        "10 - floor(30% of 10) = 10 - 3 = 7"
    );
    assert_eq!(status.disruptions_allowed, 3, "10 - 7 = 3");
}

#[tokio::test]
async fn test_pdb_only_counts_healthy_pods() {
    let storage = setup_test().await;
    let controller = PodDisruptionBudgetController::new(storage.clone());

    let spec = PodDisruptionBudgetSpec {
        min_available: Some(IntOrString::Int(3)),
        max_unavailable: None,
        selector: LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "db".to_string())])),
            match_expressions: None,
        },
        unhealthy_pod_eviction_policy: None,
    };

    let pdb = PodDisruptionBudget::new("db-pdb", "default", spec);
    let pdb_key = build_key("poddisruptionbudgets", Some("default"), "db-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create 3 healthy pods
    for i in 0..3 {
        let pod = create_test_pod(
            &format!("db-healthy-{}", i),
            "default",
            HashMap::from([("app".to_string(), "db".to_string())]),
            true,
        );
        let pod_key = build_key("pods", Some("default"), &format!("db-healthy-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Create 2 unhealthy pods (Pending phase)
    for i in 0..2 {
        let pod = create_test_pod(
            &format!("db-pending-{}", i),
            "default",
            HashMap::from([("app".to_string(), "db".to_string())]),
            false, // Not healthy
        );
        let pod_key = build_key("pods", Some("default"), &format!("db-pending-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify only healthy pods counted
    let updated_pdb: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated_pdb.status.expect("Status should be set");

    assert_eq!(status.expected_pods, 5, "Total pods matching selector");
    assert_eq!(status.current_healthy, 3, "Only Running pods are healthy");
    assert_eq!(status.desired_healthy, 3);
    assert_eq!(
        status.disruptions_allowed, 0,
        "At minimum healthy threshold"
    );
}
