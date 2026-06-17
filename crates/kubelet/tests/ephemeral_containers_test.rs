use rusternetes_common::resources::{
    Container, ContainerState, ContainerStatus, EphemeralContainer, Pod, PodSpec, PodStatus,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};

/// Test helper to create a running pod
fn create_running_pod(name: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
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
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            message: Some("Pod is running".to_string()),
            reason: None,
            host_ip: Some("192.168.1.10".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: Some(vec![ContainerStatus {
                name: "main".to_string(),
                state: Some(ContainerState::Running {
                    started_at: Some("2024-01-01T00:00:00Z".to_string()),
                }),
                ready: true,
                restart_count: 0,
                last_state: None,
                image: Some("nginx:latest".to_string()),
                image_id: None,
                container_id: Some("main-container-id".to_string()),
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            }]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            conditions: None,
            ..Default::default()
        }),
    }
}

/// Create an ephemeral container for debugging
fn create_ephemeral_container(name: &str, target_container: Option<&str>) -> EphemeralContainer {
    EphemeralContainer {
        name: name.to_string(),
        image: "busybox:latest".to_string(),
        command: Some(vec!["sh".to_string()]),
        args: None,
        working_dir: None,
        env: None,
        volume_mounts: None,
        image_pull_policy: Some("IfNotPresent".to_string()),
        security_context: None,
        target_container_name: target_container.map(|s| s.to_string()),
        stdin: Some(true),
        stdin_once: Some(false),
        tty: Some(true),
        resize_policy: None,
        restart_policy: None,
        resources: None,
        termination_message_path: None,
        termination_message_policy: None,
        ..Default::default()
    }
}

#[test]
fn test_ephemeral_container_basic_structure() {
    let ephemeral = create_ephemeral_container("debugger", Some("main"));

    assert_eq!(ephemeral.name, "debugger");
    assert_eq!(ephemeral.image, "busybox:latest");
    assert_eq!(ephemeral.target_container_name, Some("main".to_string()));
    assert_eq!(ephemeral.stdin, Some(true));
    assert_eq!(ephemeral.tty, Some(true));
}

#[test]
fn test_add_ephemeral_container_to_running_pod() {
    let mut pod = create_running_pod("test-pod");

    // Add an ephemeral container
    let ephemeral = create_ephemeral_container("debugger", Some("main"));

    if let Some(ref mut spec) = pod.spec {
        spec.ephemeral_containers = Some(vec![ephemeral]);
    }

    // Verify ephemeral container was added
    assert!(pod.spec.is_some());
    let spec = pod.spec.as_ref().unwrap();
    assert!(spec.ephemeral_containers.is_some());

    let ephemeral_containers = spec.ephemeral_containers.as_ref().unwrap();
    assert_eq!(ephemeral_containers.len(), 1);
    assert_eq!(ephemeral_containers[0].name, "debugger");
    assert_eq!(ephemeral_containers[0].image, "busybox:latest");
}

#[test]
fn test_multiple_ephemeral_containers() {
    let mut pod = create_running_pod("multi-ephemeral");

    // Add multiple ephemeral containers
    let ephemeral1 = create_ephemeral_container("debugger-1", Some("main"));
    let ephemeral2 = create_ephemeral_container("debugger-2", Some("main"));
    let ephemeral3 = create_ephemeral_container("network-debug", None);

    if let Some(ref mut spec) = pod.spec {
        spec.ephemeral_containers = Some(vec![ephemeral1, ephemeral2, ephemeral3]);
    }

    // Verify all ephemeral containers were added
    let spec = pod.spec.as_ref().unwrap();
    let ephemeral_containers = spec.ephemeral_containers.as_ref().unwrap();

    assert_eq!(ephemeral_containers.len(), 3);
    assert_eq!(ephemeral_containers[0].name, "debugger-1");
    assert_eq!(ephemeral_containers[1].name, "debugger-2");
    assert_eq!(ephemeral_containers[2].name, "network-debug");

    // Verify target container names
    assert_eq!(
        ephemeral_containers[0].target_container_name,
        Some("main".to_string())
    );
    assert_eq!(
        ephemeral_containers[1].target_container_name,
        Some("main".to_string())
    );
    assert_eq!(ephemeral_containers[2].target_container_name, None);
}

#[test]
fn test_ephemeral_container_name_uniqueness() {
    let pod = create_running_pod("unique-names");

    let spec = pod.spec.as_ref().unwrap();

    // Collect all container names
    let mut all_names = std::collections::HashSet::new();

    // Add main container names
    for container in &spec.containers {
        assert!(
            all_names.insert(&container.name),
            "Duplicate container name: {}",
            container.name
        );
    }

    // Add init container names (if any)
    if let Some(ref init_containers) = spec.init_containers {
        for container in init_containers {
            assert!(
                all_names.insert(&container.name),
                "Duplicate init container name: {}",
                container.name
            );
        }
    }

    // Ephemeral containers should have unique names
    let ephemeral1 = create_ephemeral_container("debugger-1", Some("main"));
    let ephemeral2 = create_ephemeral_container("debugger-2", Some("main"));

    assert!(all_names.insert(&ephemeral1.name));
    assert!(all_names.insert(&ephemeral2.name));

    // This would fail (name conflict)
    // let ephemeral_conflict = create_ephemeral_container("main", Some("main"));
    // assert!(!all_names.insert(&ephemeral_conflict.name), "Should detect name conflict");
}

#[test]
fn test_ephemeral_container_serialization() {
    let mut pod = create_running_pod("serialize-test");

    let ephemeral = create_ephemeral_container("debugger", Some("main"));

    if let Some(ref mut spec) = pod.spec {
        spec.ephemeral_containers = Some(vec![ephemeral]);
    }

    // Serialize to JSON
    let json = serde_json::to_string(&pod).expect("Failed to serialize pod");

    // Deserialize back
    let deserialized: Pod = serde_json::from_str(&json).expect("Failed to deserialize pod");

    // Verify ephemeral containers preserved
    assert!(deserialized.spec.is_some());
    let spec = deserialized.spec.as_ref().unwrap();
    assert!(spec.ephemeral_containers.is_some());

    let ephemeral_containers = spec.ephemeral_containers.as_ref().unwrap();
    assert_eq!(ephemeral_containers.len(), 1);
    assert_eq!(ephemeral_containers[0].name, "debugger");
    assert_eq!(
        ephemeral_containers[0].target_container_name,
        Some("main".to_string())
    );
}

#[test]
fn test_ephemeral_container_status() {
    let mut pod = create_running_pod("status-test");

    // Add ephemeral container to spec
    let ephemeral = create_ephemeral_container("debugger", Some("main"));
    if let Some(ref mut spec) = pod.spec {
        spec.ephemeral_containers = Some(vec![ephemeral]);
    }

    // Simulate ephemeral container running
    if let Some(ref mut status) = pod.status {
        status.ephemeral_container_statuses = Some(vec![ContainerStatus {
            name: "debugger".to_string(),
            state: Some(ContainerState::Running {
                started_at: Some("2024-01-01T00:05:00Z".to_string()),
            }),
            ready: true,
            restart_count: 0,
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: Some("debugger-container-id".to_string()),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]);
    }

    // Verify ephemeral container status
    let status = pod.status.as_ref().unwrap();
    assert!(status.ephemeral_container_statuses.is_some());

    let ephemeral_statuses = status.ephemeral_container_statuses.as_ref().unwrap();
    assert_eq!(ephemeral_statuses.len(), 1);
    assert_eq!(ephemeral_statuses[0].name, "debugger");
    assert!(matches!(
        ephemeral_statuses[0].state,
        Some(ContainerState::Running { .. })
    ));
}

#[test]
fn test_ephemeral_container_with_volume_mounts() {
    use rusternetes_common::resources::VolumeMount;

    let mut ephemeral = create_ephemeral_container("debugger", Some("main"));

    // Add volume mounts to ephemeral container
    ephemeral.volume_mounts = Some(vec![
        VolumeMount {
            name: "shared-data".to_string(),
            mount_path: "/data".to_string(),
            read_only: Some(true),
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: None,
            recursive_read_only: None,
        },
        VolumeMount {
            name: "logs".to_string(),
            mount_path: "/logs".to_string(),
            read_only: Some(true),
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: None,
            recursive_read_only: None,
        },
    ]);

    // Verify volume mounts
    assert!(ephemeral.volume_mounts.is_some());
    let mounts = ephemeral.volume_mounts.as_ref().unwrap();
    assert_eq!(mounts.len(), 2);
    assert_eq!(mounts[0].name, "shared-data");
    assert_eq!(mounts[0].mount_path, "/data");
    assert_eq!(mounts[1].name, "logs");
}

#[test]
fn test_ephemeral_container_with_env_vars() {
    use rusternetes_common::resources::EnvVar;

    let mut ephemeral = create_ephemeral_container("debugger", Some("main"));

    // Add environment variables
    ephemeral.env = Some(vec![
        EnvVar {
            name: "DEBUG_LEVEL".to_string(),
            value: Some("verbose".to_string()),
            value_from: None,
        },
        EnvVar {
            name: "TARGET_POD".to_string(),
            value: Some("main".to_string()),
            value_from: None,
        },
    ]);

    // Verify environment variables
    assert!(ephemeral.env.is_some());
    let env = ephemeral.env.as_ref().unwrap();
    assert_eq!(env.len(), 2);
    assert_eq!(env[0].name, "DEBUG_LEVEL");
    assert_eq!(env[0].value, Some("verbose".to_string()));
}

#[test]
fn test_ephemeral_container_tty_and_stdin() {
    // Ephemeral container with interactive shell
    let ephemeral = EphemeralContainer {
        name: "shell".to_string(),
        image: "alpine:latest".to_string(),
        command: Some(vec!["sh".to_string()]),
        args: None,
        working_dir: None,
        env: None,
        volume_mounts: None,
        image_pull_policy: Some("IfNotPresent".to_string()),
        security_context: None,
        target_container_name: Some("main".to_string()),
        stdin: Some(true),
        stdin_once: Some(false),
        tty: Some(true),
        resize_policy: None,
        restart_policy: None,
        resources: None,
        termination_message_path: None,
        termination_message_policy: None,
        ..Default::default()
    };

    assert_eq!(ephemeral.stdin, Some(true));
    assert_eq!(ephemeral.stdin_once, Some(false));
    assert_eq!(ephemeral.tty, Some(true));
}

#[test]
fn test_ephemeral_container_lifecycle() {
    let mut pod = create_running_pod("lifecycle-test");

    // Pod starts without ephemeral containers
    assert!(pod.spec.as_ref().unwrap().ephemeral_containers.is_none());

    // Add ephemeral container (simulating PATCH request)
    let ephemeral = create_ephemeral_container("debugger", Some("main"));
    if let Some(ref mut spec) = pod.spec {
        spec.ephemeral_containers = Some(vec![ephemeral]);
    }

    // Verify ephemeral container added
    assert!(pod.spec.as_ref().unwrap().ephemeral_containers.is_some());

    // Simulate ephemeral container starting
    if let Some(ref mut status) = pod.status {
        status.ephemeral_container_statuses = Some(vec![ContainerStatus {
            name: "debugger".to_string(),
            state: Some(ContainerState::Waiting {
                reason: Some("ContainerCreating".to_string()),
                message: None,
            }),
            ready: false,
            restart_count: 0,
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]);
    }

    // Verify ephemeral container waiting
    let status = pod.status.as_ref().unwrap();
    let ephemeral_statuses = status.ephemeral_container_statuses.as_ref().unwrap();
    assert!(matches!(
        ephemeral_statuses[0].state,
        Some(ContainerState::Waiting { .. })
    ));

    // Simulate ephemeral container running
    if let Some(ref mut status) = pod.status {
        status.ephemeral_container_statuses = Some(vec![ContainerStatus {
            name: "debugger".to_string(),
            state: Some(ContainerState::Running {
                started_at: Some("2024-01-01T00:10:00Z".to_string()),
            }),
            ready: true,
            restart_count: 0,
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: Some("debugger-container-id".to_string()),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]);
    }

    // Verify ephemeral container running
    let status = pod.status.as_ref().unwrap();
    let ephemeral_statuses = status.ephemeral_container_statuses.as_ref().unwrap();
    assert!(matches!(
        ephemeral_statuses[0].state,
        Some(ContainerState::Running { .. })
    ));
    assert!(ephemeral_statuses[0].ready);
}
