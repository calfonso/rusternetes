use rusternetes_common::resources::{Container, Pod, PodSpec, ResourceQuota, ResourceQuotaSpec};
use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// Note: Admission tests have been moved to api-server tests
// These tests focus on the background reconciliation of ResourceQuota status

fn setup_test_storage() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

fn create_test_pod(name: &str, cpu_request: &str, memory_request: &str) -> Pod {
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), cpu_request.to_string());
    requests.insert("memory".to_string(), memory_request.to_string());

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("test-namespace"),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "test-container".to_string(),
                image: "nginx:latest".to_string(),
                command: None,
                args: None,
                working_dir: None,
                ports: None,
                env: None,
                resources: Some(ResourceRequirements {
                    requests: Some(requests),
                    limits: None,
                    claims: None,
                }),
                volume_mounts: None,
                image_pull_policy: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
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
            volumes: None,
            restart_policy: None,
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority_class_name: None,
            priority: None,
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
        status: None,
    }
}

fn create_test_quota(cpu_limit: &str, memory_limit: &str, pod_limit: &str) -> ResourceQuota {
    let mut hard = HashMap::new();
    hard.insert("requests.cpu".to_string(), cpu_limit.to_string());
    hard.insert("requests.memory".to_string(), memory_limit.to_string());
    hard.insert("pods".to_string(), pod_limit.to_string());

    ResourceQuota::new(
        "test-quota",
        "test-namespace",
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    )
}

#[tokio::test]
async fn test_resource_quota_tracks_usage() -> Result<(), Box<dyn std::error::Error>> {
    let storage = setup_test_storage();
    let controller = ResourceQuotaController::new(storage.clone());

    // Create a quota
    let quota = create_test_quota("2", "4Gi", "5");
    let quota_key = build_key("resourcequotas", Some("test-namespace"), "test-quota");
    storage.create(&quota_key, &quota).await?;

    // Create a few pods
    let pod1 = create_test_pod("pod-1", "500m", "1Gi");
    let pod1_key = build_key("pods", Some("test-namespace"), "pod-1");
    storage.create(&pod1_key, &pod1).await?;

    let pod2 = create_test_pod("pod-2", "300m", "512Mi");
    let pod2_key = build_key("pods", Some("test-namespace"), "pod-2");
    storage.create(&pod2_key, &pod2).await?;

    // Run reconciliation
    controller.reconcile_all().await?;

    // Check that quota status was updated
    let updated_quota: ResourceQuota = storage.get(&quota_key).await?;
    assert!(updated_quota.status.is_some());

    let status = updated_quota.status.unwrap();
    assert!(status.used.is_some());

    let used = status.used.unwrap();
    assert_eq!(used.get("pods").unwrap(), "2");

    // CPU: 500m + 300m = 800m
    assert_eq!(used.get("requests.cpu").unwrap(), "800m");

    // Memory: 1Gi + 512Mi = 1536Mi
    assert_eq!(used.get("requests.memory").unwrap(), "1536Mi");

    // Cleanup
    storage.delete(&quota_key).await?;
    storage.delete(&pod1_key).await?;
    storage.delete(&pod2_key).await?;

    Ok(())
}

// Admission tests have been moved to api-server/tests/admission_test.rs
// The ResourceQuotaController in controller-manager only handles background reconciliation
