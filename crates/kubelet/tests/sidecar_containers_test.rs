use rusternetes_common::resources::{
    Container, ContainerState, ContainerStatus, Pod, PodSpec, PodStatus,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};

/// Test helper to create a pod with sidecar containers
fn create_pod_with_sidecar(
    name: &str,
    init_count: usize,
    sidecar_count: usize,
    app_count: usize,
) -> Pod {
    let mut init_containers = vec![];

    // Regular init containers (run to completion)
    for i in 0..init_count {
        init_containers.push(Container {
            name: format!("init-{}", i),
            image: format!("busybox:{}", i),
            image_pull_policy: Some("IfNotPresent".to_string()),
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("echo init-{}", i),
            ]),
            args: None,
            ports: None,
            env: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            resources: None,
            working_dir: None,
            security_context: None,
            restart_policy: None, // Regular init container - runs to completion
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
        });
    }

    // Sidecar containers (run alongside main containers)
    for i in 0..sidecar_count {
        init_containers.push(Container {
            name: format!("sidecar-{}", i),
            image: format!("nginx:{}", i),
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
            security_context: None,
            restart_policy: Some("Always".to_string()), // Sidecar - runs alongside main containers
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
        });
    }

    let mut containers = vec![];
    for i in 0..app_count {
        containers.push(Container {
            name: format!("app-{}", i),
            image: "nginx:latest".to_string(),
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
        });
    }

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            init_containers: Some(init_containers),
            containers,
            ephemeral_containers: None,
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            node_name: None,
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
fn test_pod_with_sidecar_structure() {
    let pod = create_pod_with_sidecar("test-pod", 1, 2, 1);

    assert_eq!(pod.metadata.name, "test-pod");

    let spec = pod.spec.as_ref().unwrap();
    assert!(spec.init_containers.is_some());

    let init_containers = spec.init_containers.as_ref().unwrap();
    assert_eq!(init_containers.len(), 3); // 1 regular init + 2 sidecars

    // First init container is regular (no restartPolicy)
    assert_eq!(init_containers[0].name, "init-0");
    assert_eq!(init_containers[0].restart_policy, None);

    // Next two are sidecars (restartPolicy: Always)
    assert_eq!(init_containers[1].name, "sidecar-0");
    assert_eq!(
        init_containers[1].restart_policy,
        Some("Always".to_string())
    );
    assert_eq!(init_containers[2].name, "sidecar-1");
    assert_eq!(
        init_containers[2].restart_policy,
        Some("Always".to_string())
    );

    assert_eq!(spec.containers.len(), 1);
    assert_eq!(spec.containers[0].name, "app-0");
}

#[test]
fn test_sidecar_runs_alongside_main_containers() {
    let mut pod = create_pod_with_sidecar("sidecar-test", 1, 1, 1);

    // Simulate the state where:
    // 1. Regular init container has completed
    // 2. Sidecar is running alongside main container
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        message: Some("All containers running".to_string()),
        reason: None,
        pod_ip: Some("10.244.0.5".to_string()),
        host_ip: Some("192.168.1.10".to_string()),
        host_i_ps: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        init_container_statuses: Some(vec![
            // Regular init container - completed
            ContainerStatus {
                name: "init-0".to_string(),
                state: Some(ContainerState::Terminated {
                    exit_code: 0,
                    reason: Some("Completed".to_string()),
                    signal: None,
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
                ready: false,
                restart_count: 0,
                last_state: None,
                image: Some("busybox:0".to_string()),
                image_id: None,
                container_id: Some("init-0-container".to_string()),
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            },
            // Sidecar - running
            ContainerStatus {
                name: "sidecar-0".to_string(),
                state: Some(ContainerState::Running {
                    started_at: Some("2024-01-01T00:00:05Z".to_string()),
                }),
                ready: true,
                restart_count: 0,
                last_state: None,
                image: Some("nginx:0".to_string()),
                image_id: None,
                container_id: Some("sidecar-0-container".to_string()),
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            },
        ]),
        container_statuses: Some(vec![ContainerStatus {
            name: "app-0".to_string(),
            state: Some(ContainerState::Running {
                started_at: Some("2024-01-01T00:00:06Z".to_string()),
            }),
            ready: true,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: Some("app-0-container".to_string()),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]),
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
        conditions: None,
        ..Default::default()
    });

    let status = pod.status.as_ref().unwrap();
    assert_eq!(status.phase, Some(Phase::Running));

    // Verify init container statuses
    let init_statuses = status.init_container_statuses.as_ref().unwrap();
    assert_eq!(init_statuses.len(), 2);

    // Regular init container should be terminated
    match &init_statuses[0].state {
        Some(ContainerState::Terminated {
            exit_code, reason, ..
        }) => {
            assert_eq!(*exit_code, 0);
            assert_eq!(*reason, Some("Completed".to_string()));
        }
        _ => panic!("Expected regular init container to be terminated"),
    }

    // Sidecar should be running
    assert_eq!(init_statuses[1].name, "sidecar-0");
    assert!(matches!(
        init_statuses[1].state,
        Some(ContainerState::Running { .. })
    ));
    assert!(init_statuses[1].ready);

    // Main container should also be running
    let app_statuses = status.container_statuses.as_ref().unwrap();
    assert_eq!(app_statuses.len(), 1);
    assert!(matches!(
        app_statuses[0].state,
        Some(ContainerState::Running { .. })
    ));
    assert!(app_statuses[0].ready);
}

#[test]
fn test_multiple_sidecars_with_init_containers() {
    let pod = create_pod_with_sidecar("multi-sidecar", 2, 3, 2);

    let spec = pod.spec.as_ref().unwrap();
    let init_containers = spec.init_containers.as_ref().unwrap();

    // Should have 2 regular init containers + 3 sidecars = 5 total
    assert_eq!(init_containers.len(), 5);

    // First 2 are regular init containers
    for (i, container) in init_containers.iter().enumerate().take(2) {
        assert_eq!(container.name, format!("init-{}", i));
        assert_eq!(container.restart_policy, None);
    }

    // Next 3 are sidecars
    for (i, container) in init_containers.iter().skip(2).enumerate().take(3) {
        assert_eq!(container.name, format!("sidecar-{}", i));
        assert_eq!(container.restart_policy, Some("Always".to_string()));
    }

    // Should have 2 main containers
    assert_eq!(spec.containers.len(), 2);
}

#[test]
fn test_sidecar_restart_policy_validation() {
    let pod = create_pod_with_sidecar("restart-test", 0, 1, 1);

    let spec = pod.spec.as_ref().unwrap();
    let init_containers = spec.init_containers.as_ref().unwrap();

    // Verify sidecar has correct restart policy
    assert_eq!(init_containers[0].name, "sidecar-0");
    assert_eq!(
        init_containers[0].restart_policy,
        Some("Always".to_string())
    );
}

#[test]
fn test_pod_serialization_with_sidecars() {
    let pod = create_pod_with_sidecar("serialize-test", 1, 1, 1);

    // Serialize to JSON
    let json = serde_json::to_string(&pod).expect("Failed to serialize pod");

    // Deserialize back
    let deserialized: Pod = serde_json::from_str(&json).expect("Failed to deserialize pod");

    assert_eq!(deserialized.metadata.name, "serialize-test");

    let spec = deserialized.spec.as_ref().unwrap();
    assert!(spec.init_containers.is_some());

    let init_containers = spec.init_containers.as_ref().unwrap();
    assert_eq!(init_containers.len(), 2);

    // Verify restart policies preserved
    assert_eq!(init_containers[0].restart_policy, None); // Regular init
    assert_eq!(
        init_containers[1].restart_policy,
        Some("Always".to_string())
    ); // Sidecar
}

#[test]
fn test_only_sidecars_no_regular_init() {
    let pod = create_pod_with_sidecar("sidecar-only", 0, 2, 1);

    let spec = pod.spec.as_ref().unwrap();
    let init_containers = spec.init_containers.as_ref().unwrap();

    // Should have only sidecars
    assert_eq!(init_containers.len(), 2);

    for (i, container) in init_containers.iter().enumerate().take(2) {
        assert_eq!(container.name, format!("sidecar-{}", i));
        assert_eq!(container.restart_policy, Some("Always".to_string()));
    }
}

#[test]
fn test_sidecar_failure_should_not_block_pod() {
    let mut pod = create_pod_with_sidecar("sidecar-failure", 0, 1, 1);

    // Simulate sidecar failing but pod still running
    // (In Kubernetes, sidecar failures don't necessarily fail the whole pod)
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        message: Some("Main container running despite sidecar issues".to_string()),
        reason: None,
        pod_ip: Some("10.244.0.5".to_string()),
        host_ip: Some("192.168.1.10".to_string()),
        host_i_ps: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        init_container_statuses: Some(vec![ContainerStatus {
            name: "sidecar-0".to_string(),
            state: Some(ContainerState::Terminated {
                exit_code: 1,
                reason: Some("Error".to_string()),
                signal: None,
                message: None,
                started_at: None,
                finished_at: None,
                container_id: None,
            }),
            ready: false,
            restart_count: 3,
            last_state: None,
            image: Some("nginx:0".to_string()),
            image_id: None,
            container_id: Some("sidecar-0-container".to_string()),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]),
        container_statuses: Some(vec![ContainerStatus {
            name: "app-0".to_string(),
            state: Some(ContainerState::Running {
                started_at: Some("2024-01-01T00:00:00Z".to_string()),
            }),
            ready: true,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: Some("app-0-container".to_string()),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]),
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
        conditions: None,
        ..Default::default()
    });

    let status = pod.status.as_ref().unwrap();

    // Pod should still be running
    assert_eq!(status.phase, Some(Phase::Running));

    // Verify sidecar has restarted multiple times
    let init_statuses = status.init_container_statuses.as_ref().unwrap();
    assert_eq!(init_statuses[0].restart_count, 3);
}

#[test]
fn test_execution_order_concept() {
    // This test documents the expected execution order
    let pod = create_pod_with_sidecar("order-test", 2, 2, 2);

    let spec = pod.spec.as_ref().unwrap();
    let init_containers = spec.init_containers.as_ref().unwrap();

    // Expected execution order:
    // 1. init-0 (runs to completion)
    // 2. init-1 (runs to completion)
    // 3. sidecar-0 (starts running, doesn't block)
    // 4. sidecar-1 (starts running, doesn't block)
    // 5. app-0 (starts running)
    // 6. app-1 (starts running)

    // All containers after regular init containers run concurrently
    let regular_init_count = init_containers
        .iter()
        .filter(|c| c.restart_policy.is_none())
        .count();
    assert_eq!(
        regular_init_count, 2,
        "Should have 2 regular init containers"
    );

    let sidecar_count = init_containers
        .iter()
        .filter(|c| c.restart_policy == Some("Always".to_string()))
        .count();
    assert_eq!(sidecar_count, 2, "Should have 2 sidecar containers");

    assert_eq!(spec.containers.len(), 2, "Should have 2 main containers");
}
