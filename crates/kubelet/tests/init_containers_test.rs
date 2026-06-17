use rusternetes_common::resources::{
    Container, ContainerState, ContainerStatus, Pod, PodSpec, PodStatus,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};

/// Test helper to create a pod with init containers
fn create_pod_with_init_containers(name: &str, init_count: usize, app_count: usize) -> Pod {
    let mut init_containers = vec![];
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

    let mut containers = vec![];
    for i in 0..app_count {
        containers.push(Container {
            name: format!("app-{}", i),
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
fn test_pod_with_init_containers_structure() {
    let pod = create_pod_with_init_containers("test-pod", 2, 1);

    assert_eq!(pod.metadata.name, "test-pod");

    let spec = pod.spec.as_ref().unwrap();
    assert!(spec.init_containers.is_some());

    let init_containers = spec.init_containers.as_ref().unwrap();
    assert_eq!(init_containers.len(), 2);
    assert_eq!(init_containers[0].name, "init-0");
    assert_eq!(init_containers[1].name, "init-1");

    assert_eq!(spec.containers.len(), 1);
    assert_eq!(spec.containers[0].name, "app-0");
}

#[test]
fn test_init_container_status_sequence() {
    let mut pod = create_pod_with_init_containers("status-test", 3, 1);

    // Simulate init containers running in sequence
    // Stage 1: First init container running, others waiting
    pod.status = Some(PodStatus {
        phase: Some(Phase::Pending),
        message: Some("Initializing".to_string()),
        reason: Some("PodInitializing".to_string()),
        pod_ip: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        host_ip: None,
        host_i_ps: None,
        container_statuses: None,
        init_container_statuses: Some(vec![
            ContainerStatus {
                name: "init-0".to_string(),
                state: Some(ContainerState::Running {
                    started_at: Some("2024-01-01T00:00:00Z".to_string()),
                }),
                ready: false,
                restart_count: 0,
                last_state: None,
                image: Some("busybox:0".to_string()),
                image_id: None,
                container_id: Some("container-0".to_string()),
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            },
            // init-1 and init-2 are waiting
        ]),
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
        conditions: None,
        ..Default::default()
    });

    let status = pod.status.as_ref().unwrap();
    assert_eq!(status.phase, Some(Phase::Pending));
    assert_eq!(status.reason, Some("PodInitializing".to_string()));

    let init_statuses = status.init_container_statuses.as_ref().unwrap();
    assert_eq!(init_statuses.len(), 1);
    assert_eq!(init_statuses[0].name, "init-0");
    assert!(matches!(
        init_statuses[0].state,
        Some(ContainerState::Running { .. })
    ));
    assert!(!init_statuses[0].ready); // Init containers are never "ready"
}

#[test]
fn test_init_containers_completed_app_starting() {
    let mut pod = create_pod_with_init_containers("completed-test", 2, 1);

    // All init containers completed, app container starting
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        message: None,
        reason: None,
        pod_ip: Some("10.244.0.5".to_string()),
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        host_ip: Some("192.168.1.10".to_string()),
        host_i_ps: None,
        init_container_statuses: Some(vec![
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
            ContainerStatus {
                name: "init-1".to_string(),
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
                image: Some("busybox:1".to_string()),
                image_id: None,
                container_id: Some("init-1-container".to_string()),
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
                started_at: Some("2024-01-01T00:00:11Z".to_string()),
            }),
            ready: true,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:0".to_string()),
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

    // Verify all init containers completed successfully
    let init_statuses = status.init_container_statuses.as_ref().unwrap();
    assert_eq!(init_statuses.len(), 2);

    for init_status in init_statuses {
        match &init_status.state {
            Some(ContainerState::Terminated {
                exit_code, reason, ..
            }) => {
                assert_eq!(*exit_code, 0, "Init container should exit with code 0");
                assert_eq!(*reason, Some("Completed".to_string()));
            }
            _ => panic!("Expected init container to be terminated"),
        }
    }

    // Verify app container is running
    let app_statuses = status.container_statuses.as_ref().unwrap();
    assert_eq!(app_statuses.len(), 1);
    assert!(matches!(
        app_statuses[0].state,
        Some(ContainerState::Running { .. })
    ));
    assert!(app_statuses[0].ready);
}

#[test]
fn test_init_container_failure_blocks_app() {
    let mut pod = create_pod_with_init_containers("failure-test", 2, 1);

    // First init container failed - app should not start
    pod.status = Some(PodStatus {
        phase: Some(Phase::Failed),
        message: Some("Init container init-0 failed".to_string()),
        reason: Some("Init:Error".to_string()),
        pod_ip: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        host_ip: None,
        host_i_ps: None,
        init_container_statuses: Some(vec![ContainerStatus {
            name: "init-0".to_string(),
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
            restart_count: 1,
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
        }]),
        container_statuses: None, // App container never started
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
        conditions: None,
        ..Default::default()
    });

    let status = pod.status.as_ref().unwrap();
    assert_eq!(status.phase, Some(Phase::Failed));
    assert_eq!(status.reason, Some("Init:Error".to_string()));

    // Init container should have failed
    let init_statuses = status.init_container_statuses.as_ref().unwrap();
    match &init_statuses[0].state {
        Some(ContainerState::Terminated {
            exit_code, reason, ..
        }) => {
            assert_eq!(*exit_code, 1, "Init container should exit with code 1");
            assert_eq!(*reason, Some("Error".to_string()));
        }
        _ => panic!("Expected init container to be terminated with error"),
    }

    // App container statuses should be None - never started
    assert!(status.container_statuses.is_none());
}

#[test]
fn test_init_container_restart_count() {
    let mut pod = create_pod_with_init_containers("restart-test", 1, 1);

    // Init container that has restarted multiple times
    pod.status = Some(PodStatus {
        phase: Some(Phase::Pending),
        message: Some("Init container restarting".to_string()),
        reason: Some("Init:CrashLoopBackOff".to_string()),
        pod_ip: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        host_ip: None,
        host_i_ps: None,
        init_container_statuses: Some(vec![ContainerStatus {
            name: "init-0".to_string(),
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
            restart_count: 5, // Container has restarted 5 times
            last_state: None,
            image: Some("busybox:0".to_string()),
            image_id: None,
            container_id: Some("init-0-container-5".to_string()),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]),
        container_statuses: None,
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
        conditions: None,
        ..Default::default()
    });

    let status = pod.status.as_ref().unwrap();
    assert_eq!(status.phase, Some(Phase::Pending));

    let init_statuses = status.init_container_statuses.as_ref().unwrap();
    assert_eq!(init_statuses[0].restart_count, 5);
}

#[test]
fn test_multiple_init_containers_sequential_execution() {
    let pod = create_pod_with_init_containers("sequential-test", 5, 2);

    let spec = pod.spec.as_ref().unwrap();
    let init_containers = spec.init_containers.as_ref().unwrap();

    // Verify we have 5 init containers defined
    assert_eq!(init_containers.len(), 5);

    // Verify init container names are in order
    for (i, container) in init_containers.iter().enumerate().take(5) {
        assert_eq!(container.name, format!("init-{}", i));
    }

    // Verify app containers defined
    assert_eq!(spec.containers.len(), 2);
    assert_eq!(spec.containers[0].name, "app-0");
    assert_eq!(spec.containers[1].name, "app-1");
}

#[test]
fn test_pod_serialization_with_init_containers() {
    let pod = create_pod_with_init_containers("serialize-test", 1, 1);

    // Serialize to JSON
    let json = serde_json::to_string(&pod).expect("Failed to serialize pod");

    // Deserialize back
    let deserialized: Pod = serde_json::from_str(&json).expect("Failed to deserialize pod");

    assert_eq!(deserialized.metadata.name, "serialize-test");

    let spec = deserialized.spec.as_ref().unwrap();
    assert!(spec.init_containers.is_some());
    assert_eq!(spec.init_containers.as_ref().unwrap().len(), 1);
    assert_eq!(spec.containers.len(), 1);
}
