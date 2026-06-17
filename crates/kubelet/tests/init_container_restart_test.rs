//! Conformance test for init container restart semantics.
//!
//! Covers the upstream k8s e2e site `common/node/init_container.go:446`
//! ("should not start app containers and fail the pod if init containers
//! fail on a RestartNever pod") and its sibling test that asserts sidecar
//! init containers (`restartPolicy=Always` on an init container) are
//! started alongside the main containers instead of being treated as a
//! one-shot init container that must exit before app containers run.
//!
//! These tests exercise the pure decision helper `decide_next_init_action`
//! in `rusternetes_kubelet::runtime`, which captures the init-container
//! restart state machine independently of the Docker runtime. The helper
//! is called from `ContainerRuntime::compute_init_container_actions` after
//! Docker observations have been gathered, so testing it directly gives
//! us deterministic coverage of the restart semantics without needing a
//! real container runtime.

use rusternetes_common::resources::{Container, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kubelet::runtime::{decide_next_init_action, InitAction, InitContainerObserved};

/// Build a minimal init container with the given name and optional
/// per-container restart policy (which marks it as a sidecar when set
/// to "Always", per the KEP-753 / SidecarContainers feature).
fn make_init(name: &str, restart_policy: Option<&str>) -> Container {
    Container {
        name: name.to_string(),
        image: "busybox:latest".to_string(),
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
        restart_policy: restart_policy.map(|s| s.to_string()),
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
    }
}

fn make_app(name: &str) -> Container {
    make_init(name, None)
}

/// Build a pod with the given init containers, a single "app" container,
/// and the supplied pod-level restart policy.
fn make_pod(name: &str, restart_policy: &str, inits: Vec<Container>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![make_app("app")],
            init_containers: Some(inits),
            ephemeral_containers: None,
            restart_policy: Some(restart_policy.to_string()),
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
            priority: None,
            priority_class_name: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
            resource_claims: None,
            volumes: None,
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

// ---------------------------------------------------------------------------
// RestartPolicy=Never: failed init must NOT be retried, and the pod becomes
// terminal (no next_index). This is the heart of `init_container.go:446`.
// ---------------------------------------------------------------------------

#[test]
fn restart_never_failed_init_is_terminal_not_retried() {
    let pod = make_pod(
        "fail-never",
        "Never",
        vec![make_init("init-0", None), make_init("init-1", None)],
    );
    // init-0 exited with code 1, init-1 was never started.
    let observed = vec![
        InitContainerObserved::Exited(1),
        InitContainerObserved::NotStarted,
    ];

    let action = decide_next_init_action(&pod, &observed);

    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: None,
            should_retry: false,
        },
        "RestartNever pod with a failed init container must be terminal, \
         not request a retry (upstream: init_container.go:446)"
    );
}

// ---------------------------------------------------------------------------
// RestartPolicy=Always: failed init MUST be retried (CrashLoopBackOff).
// ---------------------------------------------------------------------------

#[test]
fn restart_always_failed_init_is_retried() {
    let pod = make_pod("fail-always", "Always", vec![make_init("init-0", None)]);
    let observed = vec![InitContainerObserved::Exited(1)];

    let action = decide_next_init_action(&pod, &observed);

    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: Some(0),
            should_retry: true,
        },
        "RestartAlways pod with a failed init container must be retried"
    );
}

// ---------------------------------------------------------------------------
// RestartPolicy=OnFailure: failed init MUST also be retried (per upstream
// kubelet, OnFailure behaves like Always for non-sidecar init containers).
// ---------------------------------------------------------------------------

#[test]
fn restart_on_failure_failed_init_is_retried() {
    let pod = make_pod(
        "fail-on-failure",
        "OnFailure",
        vec![make_init("init-0", None)],
    );
    let observed = vec![InitContainerObserved::Exited(1)];

    let action = decide_next_init_action(&pod, &observed);

    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: Some(0),
            should_retry: true,
        },
        "RestartOnFailure pod with a failed init container must be retried"
    );
}

// ---------------------------------------------------------------------------
// Sidecar (init container with restartPolicy=Always) does NOT gate the
// "all init done" decision and does NOT block app containers from starting.
// This is the second half of the unit's scope.
// ---------------------------------------------------------------------------

#[test]
fn sidecar_init_does_not_block_app_when_running() {
    // A regular init container that completed + a sidecar init that is
    // still running. The sidecar must NOT prevent us from declaring all
    // (non-sidecar) inits done — app containers can start while it runs.
    let pod = make_pod(
        "with-sidecar",
        "Always",
        vec![
            make_init("init-0", None),
            make_init("sidecar-0", Some("Always")),
        ],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::Running,
    ];

    let action = decide_next_init_action(&pod, &observed);

    assert_eq!(
        action,
        InitAction {
            all_init_done: true,
            next_index: None,
            should_retry: false,
        },
        "Sidecar init containers (restartPolicy=Always) must not block app \
         containers from starting once regular inits have completed"
    );
}

#[test]
fn sidecar_init_does_not_block_app_when_exited() {
    // Even if the sidecar has exited (it may legitimately exit before
    // pod termination), regular-init completion still gates the decision,
    // not the sidecar.
    let pod = make_pod(
        "with-sidecar-exited",
        "Always",
        vec![
            make_init("init-0", None),
            make_init("sidecar-0", Some("Always")),
        ],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::Exited(0),
    ];

    let action = decide_next_init_action(&pod, &observed);

    assert!(
        action.all_init_done,
        "Sidecar init container state must not influence all_init_done"
    );
}

// ---------------------------------------------------------------------------
// Sequencing: only one regular init runs at a time. If init-0 is still
// running, we do not advance to init-1.
// ---------------------------------------------------------------------------

#[test]
fn running_init_blocks_advancement() {
    let pod = make_pod(
        "running",
        "Always",
        vec![make_init("init-0", None), make_init("init-1", None)],
    );
    let observed = vec![
        InitContainerObserved::Running,
        InitContainerObserved::NotStarted,
    ];

    let action = decide_next_init_action(&pod, &observed);

    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: None,
            should_retry: false,
        },
        "While the current init container is still running, no next index \
         should be returned (we wait for it)"
    );
}

// ---------------------------------------------------------------------------
// Init container has never been created — return its index so the kubelet
// can start it. This is NOT a retry.
// ---------------------------------------------------------------------------

#[test]
fn not_started_init_returns_index_without_retry() {
    let pod = make_pod("fresh", "Always", vec![make_init("init-0", None)]);
    let observed = vec![InitContainerObserved::NotStarted];

    let action = decide_next_init_action(&pod, &observed);

    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: Some(0),
            should_retry: false,
        }
    );
}

// ---------------------------------------------------------------------------
// All regular init containers exited 0 → all done.
// ---------------------------------------------------------------------------

#[test]
fn all_inits_completed_returns_all_done() {
    let pod = make_pod(
        "done",
        "Always",
        vec![make_init("init-0", None), make_init("init-1", None)],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::Exited(0),
    ];

    let action = decide_next_init_action(&pod, &observed);

    assert_eq!(
        action,
        InitAction {
            all_init_done: true,
            next_index: None,
            should_retry: false,
        }
    );
}

// ---------------------------------------------------------------------------
// No init containers declared → trivially all done.
// ---------------------------------------------------------------------------

#[test]
fn no_init_containers_is_all_done() {
    let pod = make_pod("no-inits", "Always", vec![]);
    let action = decide_next_init_action(&pod, &[]);

    assert_eq!(
        action,
        InitAction {
            all_init_done: true,
            next_index: None,
            should_retry: false,
        }
    );
}
