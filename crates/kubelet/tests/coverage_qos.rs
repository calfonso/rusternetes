//! Unit tests for Pod QoS class determination.
//!
//! These tests pin the behaviour of [`rusternetes_kubelet::eviction::get_qos_class`].
//! The high-level algorithm is modelled on upstream Kubernetes' `GetPodQOS` in
//! <https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/qos/qos.go>
//! (test suite at
//! <https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/qos/qos_test.go>),
//! but the Rusternetes implementation is **not** a faithful port — see the
//! divergences below. Only cases that genuinely match upstream carry a
//! `Mirrors:` reference.
//!
//! `get_qos_class` classifies a pod into one of three QoS classes, inspecting
//! **only the regular (app) containers in `spec.containers`**:
//!
//! - **Guaranteed** – every app container has explicit CPU **and** memory
//!   limits, AND explicit requests, AND requests equal limits for every
//!   resource.
//! - **Burstable** – at least one app container has some resource request or
//!   limit, but the pod does not qualify as Guaranteed. In particular, a
//!   container with limits but **no** requests is **Burstable** here: the
//!   function requires both to be set explicitly and does not apply the
//!   "missing request defaults to the limit" rule before classifying.
//! - **BestEffort** – no app container has any resource requests or limits.
//!
//! ## Known divergences from upstream Go
//!
//! 1. **Init / sidecar containers are ignored.** Upstream `GetPodQOS` folds
//!    init containers into the QoS calculation; `get_qos_class` only inspects
//!    `spec.containers`. The `#[ignore]`d tests below assert the
//!    upstream-correct expectation and document the gap; companion live tests
//!    assert the actual current Rusternetes behaviour so the divergence is
//!    visible rather than hidden.
//! 2. **Requests do not default to limits.** Upstream defaults a missing
//!    request to the matching limit before classifying, so a limits-only
//!    container is Guaranteed upstream. `get_qos_class` treats a missing
//!    request as non-Guaranteed, yielding Burstable.

use rusternetes_common::resources::{Container, EphemeralContainer, PodSpec};
use rusternetes_common::resources::{Pod, PodStatus};
use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
use rusternetes_kubelet::eviction::{get_qos_class, QoSClass};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Builder helpers
// ---------------------------------------------------------------------------

/// Minimal `Container` with only the name and image set; all other fields are
/// `None`.  Use [`with_resources`] to attach resource requirements.
fn make_container(name: &str) -> Container {
    Container {
        name: name.to_string(),
        image: "test-image:latest".to_string(),
        resources: None,
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
    }
}

/// Attach `ResourceRequirements` to a `Container`.
fn with_resources(mut c: Container, r: ResourceRequirements) -> Container {
    c.resources = Some(r);
    c
}

/// Build a `ResourceRequirements` where both `requests` and `limits` contain
/// the same cpu/memory pair (the Guaranteed pattern).
fn guaranteed_resources(cpu: &str, memory: &str) -> ResourceRequirements {
    let map = HashMap::from([
        ("cpu".to_string(), cpu.to_string()),
        ("memory".to_string(), memory.to_string()),
    ]);
    ResourceRequirements {
        requests: Some(map.clone()),
        limits: Some(map),
        claims: None,
    }
}

/// Build a `ResourceRequirements` with only limits set (no explicit requests).
///
/// Upstream Kubernetes would default the missing request to the limit and
/// classify this as Guaranteed. `get_qos_class` in `eviction.rs` does NOT
/// apply that defaulting: it requires both requests and limits to be set
/// explicitly, so a limits-only container is classified Burstable.
fn limits_only_resources(cpu: &str, memory: &str) -> ResourceRequirements {
    ResourceRequirements {
        requests: None,
        limits: Some(HashMap::from([
            ("cpu".to_string(), cpu.to_string()),
            ("memory".to_string(), memory.to_string()),
        ])),
        claims: None,
    }
}

/// Build a `ResourceRequirements` with mismatched requests vs limits
/// (classic Burstable pattern).
fn burstable_resources(
    req_cpu: &str,
    req_mem: &str,
    lim_cpu: &str,
    lim_mem: &str,
) -> ResourceRequirements {
    ResourceRequirements {
        requests: Some(HashMap::from([
            ("cpu".to_string(), req_cpu.to_string()),
            ("memory".to_string(), req_mem.to_string()),
        ])),
        limits: Some(HashMap::from([
            ("cpu".to_string(), lim_cpu.to_string()),
            ("memory".to_string(), lim_mem.to_string()),
        ])),
        claims: None,
    }
}

/// Build a `ResourceRequirements` with only requests set (no limits).
fn requests_only_resources(cpu: &str, memory: &str) -> ResourceRequirements {
    ResourceRequirements {
        requests: Some(HashMap::from([
            ("cpu".to_string(), cpu.to_string()),
            ("memory".to_string(), memory.to_string()),
        ])),
        limits: None,
        claims: None,
    }
}

/// Assemble a minimal `Pod` from a list of app containers.  `init_containers`
/// and `ephemeral_containers` are left `None` unless specified.
fn make_pod(
    name: &str,
    containers: Vec<Container>,
    init_containers: Option<Vec<Container>>,
    ephemeral_containers: Option<Vec<EphemeralContainer>>,
) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers,
            init_containers,
            ephemeral_containers,
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
        }),
        status: None,
    }
}

// ---------------------------------------------------------------------------
// BestEffort cases
// ---------------------------------------------------------------------------

/// A pod with no spec at all is BestEffort.
///
/// Analogous to the empty-resources rows of upstream `TestGetPodQOS` in
/// `pkg/apis/core/v1/helper/qos/qos_test.go`.
#[test]
fn best_effort_no_spec() {
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("no-spec").with_namespace("default"),
        spec: None,
        status: None,
    };
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

/// Single container with no resource requirements → BestEffort.
///
/// Matches the "best-effort" rows of upstream `TestGetPodQOS`.
#[test]
fn best_effort_single_container_no_resources() {
    let c = make_container("c1");
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

/// Multiple containers, none with resource requirements → BestEffort.
///
/// Matches the "best-effort" rows of upstream `TestGetPodQOS`.
#[test]
fn best_effort_multiple_containers_no_resources() {
    let containers = vec![
        make_container("c1"),
        make_container("c2"),
        make_container("c3"),
    ];
    let pod = make_pod("pod", containers, None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

/// Resource-less init containers leave a BestEffort pod BestEffort.
///
/// This is the one init-container scenario where Rusternetes and upstream
/// agree: when the init containers carry no resources there is nothing to fold
/// into the QoS calculation, so both classify the pod BestEffort.
#[test]
fn best_effort_with_init_containers_no_resources() {
    let app_containers = vec![make_container("app")];
    let init_containers = vec![make_container("init1"), make_container("init2")];
    let pod = make_pod("pod", app_containers, Some(init_containers), None);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

// ---------------------------------------------------------------------------
// Guaranteed cases
// ---------------------------------------------------------------------------

/// Single container with matching CPU + memory limits == requests → Guaranteed.
///
/// Matches the "guaranteed" rows of upstream `TestGetPodQOS`.
#[test]
fn guaranteed_single_container_limits_eq_requests() {
    let c = with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi"));
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Multiple containers, all with matching limits == requests → Guaranteed.
///
/// Matches the "guaranteed" rows of upstream `TestGetPodQOS`.
#[test]
fn guaranteed_multiple_containers_all_matching() {
    let containers = vec![
        with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
        with_resources(make_container("c2"), guaranteed_resources("200m", "256Mi")),
        with_resources(make_container("c3"), guaranteed_resources("50m", "64Mi")),
    ];
    let pod = make_pod("pod", containers, None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Guaranteed app containers plus Guaranteed init containers → Guaranteed.
///
/// Rusternetes and upstream agree on the *result* here, but for different
/// reasons: upstream folds the (also-Guaranteed) init container into the
/// calculation; `get_qos_class` ignores init containers and classifies on the
/// Guaranteed app container alone. See the `#[ignore]`d divergence tests below
/// for cases where the two implementations disagree.
#[test]
fn guaranteed_app_containers_with_guaranteed_init_containers() {
    let app = vec![with_resources(
        make_container("app"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let init = vec![with_resources(
        make_container("init"),
        guaranteed_resources("50m", "64Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Ephemeral containers do NOT affect QoS class. A pod with Guaranteed app
/// containers stays Guaranteed even when an ephemeral (debug) container carries
/// mismatched resources.
///
/// This matches upstream: `GetPodQOS` in
/// `pkg/apis/core/v1/helper/qos/qos.go` iterates only regular and init
/// containers; ephemeral containers are excluded by design (they are also
/// forbidden from declaring resources in the API). Rusternetes likewise
/// excludes them.
#[test]
fn guaranteed_unaffected_by_ephemeral_containers() {
    let app = vec![with_resources(
        make_container("app"),
        guaranteed_resources("100m", "128Mi"),
    )];
    // Ephemeral container with resources set to something that would be Burstable
    let eph = EphemeralContainer {
        name: "debugger".to_string(),
        image: "debug-image:latest".to_string(),
        command: None,
        args: None,
        working_dir: None,
        env: None,
        volume_mounts: None,
        image_pull_policy: None,
        security_context: None,
        target_container_name: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        resize_policy: None,
        restart_policy: None,
        resources: Some(ResourceRequirements {
            requests: Some(HashMap::from([
                ("cpu".to_string(), "50m".to_string()),
                ("memory".to_string(), "64Mi".to_string()),
            ])),
            limits: Some(HashMap::from([
                ("cpu".to_string(), "200m".to_string()),
                ("memory".to_string(), "256Mi".to_string()),
            ])),
            claims: None,
        }),
        termination_message_path: None,
        termination_message_policy: None,
    };
    let pod = make_pod("pod", app, None, Some(vec![eph]));
    assert_eq!(
        get_qos_class(&pod),
        QoSClass::Guaranteed,
        "ephemeral containers must not affect QoS class"
    );
}

// ---------------------------------------------------------------------------
// Burstable cases
// ---------------------------------------------------------------------------

/// requests < limits → Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_requests_less_than_limits() {
    let c = with_resources(
        make_container("c1"),
        burstable_resources("100m", "128Mi", "200m", "256Mi"),
    );
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// requests only (no limits) → Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_requests_only_no_limits() {
    let c = with_resources(
        make_container("c1"),
        requests_only_resources("100m", "128Mi"),
    );
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// DIVERGENCE (live test, current Rusternetes behaviour): a container with
/// limits but no requests is classified **Burstable** by `get_qos_class`,
/// because the function requires both requests and limits to be set explicitly
/// and does not default a missing request to the limit.
///
/// Upstream Kubernetes classifies this as **Guaranteed** (it defaults the
/// missing request to the limit first). See
/// `upstream_limits_only_should_be_guaranteed` below for the upstream-correct
/// expectation.
#[test]
fn burstable_limits_only_no_requests_current_behaviour() {
    let c = with_resources(make_container("c1"), limits_only_resources("100m", "128Mi"));
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// GAP: upstream Kubernetes defaults a missing request to the matching limit
/// before classifying, so a limits-only container is **Guaranteed**.
/// `get_qos_class` does not apply that defaulting and returns Burstable, so
/// this assertion of the upstream-correct expectation currently fails.
///
/// Upstream reference: `GetPodQOS` in
/// `pkg/apis/core/v1/helper/qos/qos.go`.
#[test]
#[ignore = "GAP: Rusternetes get_qos_class does not default missing requests to limits; \
            upstream pkg/apis/core/v1/helper/qos classifies limits-only as Guaranteed"]
fn upstream_limits_only_should_be_guaranteed() {
    let c = with_resources(make_container("c1"), limits_only_resources("100m", "128Mi"));
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// Mixed containers — one Guaranteed, one with no resources → Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_mixed_guaranteed_and_best_effort_containers() {
    let containers = vec![
        with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
        make_container("c2"), // no resources — BestEffort
    ];
    let pod = make_pod("pod", containers, None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// Partial CPU-only limit (no memory) → Burstable: Guaranteed requires both
/// cpu AND memory limits.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_cpu_only_limit_no_memory() {
    let r = ResourceRequirements {
        requests: Some(HashMap::from([("cpu".to_string(), "100m".to_string())])),
        limits: Some(HashMap::from([("cpu".to_string(), "100m".to_string())])),
        claims: None,
    };
    let c = with_resources(make_container("c1"), r);
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// Memory-only limit (no cpu) → Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_memory_only_limit_no_cpu() {
    let r = ResourceRequirements {
        requests: Some(HashMap::from([("memory".to_string(), "128Mi".to_string())])),
        limits: Some(HashMap::from([("memory".to_string(), "128Mi".to_string())])),
        claims: None,
    };
    let c = with_resources(make_container("c1"), r);
    let pod = make_pod("pod", vec![c], None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// One container Guaranteed, another with mismatched requests/limits → the
/// whole pod is Burstable.
///
/// Matches the "burstable" rows of upstream `TestGetPodQOS`.
#[test]
fn burstable_one_of_two_containers_not_guaranteed() {
    let containers = vec![
        with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
        with_resources(
            make_container("c2"),
            burstable_resources("50m", "64Mi", "100m", "128Mi"),
        ),
    ];
    let pod = make_pod("pod", containers, None, None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

// ---------------------------------------------------------------------------
// Init-container contribution — DIVERGENCE from upstream
//
// Upstream `GetPodQOS` (pkg/apis/core/v1/helper/qos/qos.go) folds init
// containers into the QoS calculation. Rusternetes' `get_qos_class` looks only
// at `spec.containers` and ignores init containers entirely. The pairs below
// pin both sides: a live test asserting the actual current behaviour, and an
// `#[ignore]`d test asserting the upstream-correct expectation, so the gap is
// documented rather than silently baked in.
// ---------------------------------------------------------------------------

/// LIVE (current Rusternetes behaviour): a BestEffort app container with a
/// Guaranteed init container stays BestEffort, because init containers are
/// ignored by `get_qos_class`.
#[test]
fn init_containers_guaranteed_ignored_app_stays_best_effort() {
    let app = vec![make_container("app")]; // no resources
    let init = vec![with_resources(
        make_container("init"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

/// GAP (upstream-correct expectation): upstream folds the Guaranteed init
/// container into the calculation, so a BestEffort app container plus a
/// Guaranteed init container is **Burstable** (the pod has some requests/limits
/// overall but not on every container). `get_qos_class` ignores init containers
/// and returns BestEffort, so this assertion currently fails.
#[test]
#[ignore = "GAP: Rusternetes QoS ignores init containers; \
            upstream pkg/apis/core/v1/helper/qos includes them"]
fn upstream_guaranteed_init_makes_best_effort_app_burstable() {
    let app = vec![make_container("app")]; // no resources
    let init = vec![with_resources(
        make_container("init"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

/// LIVE (current Rusternetes behaviour): a Guaranteed app container with a
/// Burstable init container stays Guaranteed, because init containers are
/// ignored by `get_qos_class`.
#[test]
fn init_containers_burstable_ignored_app_stays_guaranteed() {
    let app = vec![with_resources(
        make_container("app"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let init = vec![with_resources(
        make_container("init"),
        burstable_resources("50m", "64Mi", "100m", "128Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

/// GAP (upstream-correct expectation): upstream folds the Burstable init
/// container into the calculation, so a Guaranteed app container plus a
/// Burstable init container is **Burstable**. `get_qos_class` ignores init
/// containers and returns Guaranteed, so this assertion currently fails.
#[test]
#[ignore = "GAP: Rusternetes QoS ignores init containers; \
            upstream pkg/apis/core/v1/helper/qos includes them"]
fn upstream_burstable_init_downgrades_guaranteed_app_to_burstable() {
    let app = vec![with_resources(
        make_container("app"),
        guaranteed_resources("100m", "128Mi"),
    )];
    let init = vec![with_resources(
        make_container("init"),
        burstable_resources("50m", "64Mi", "100m", "128Mi"),
    )];
    let pod = make_pod("pod", app, Some(init), None);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

// QoSClass ordering (BestEffort < Burstable < Guaranteed) is already covered
// by `test_qos_class_ordering` in `eviction_test.rs`; not duplicated here.

// ---------------------------------------------------------------------------
// Table-driven sweep
// ---------------------------------------------------------------------------

/// Table-driven sweep covering all three QoS classes with multiple container
/// configurations, asserting the **current** `get_qos_class` behaviour in a
/// single pass. Structured like upstream's table-driven `TestGetPodQOS`
/// (<https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/qos/qos_test.go>),
/// but the "limits only" row encodes the Rusternetes divergence (Burstable),
/// not the upstream result (Guaranteed).
#[test]
fn qos_classify_table() {
    struct Case {
        label: &'static str,
        containers: Vec<Container>,
        expected: QoSClass,
    }

    let cases = vec![
        Case {
            label: "no containers → BestEffort",
            containers: vec![],
            expected: QoSClass::BestEffort,
        },
        Case {
            label: "single container no resources → BestEffort",
            containers: vec![make_container("c")],
            expected: QoSClass::BestEffort,
        },
        Case {
            label: "two containers no resources → BestEffort",
            containers: vec![make_container("c1"), make_container("c2")],
            expected: QoSClass::BestEffort,
        },
        Case {
            label: "single container guaranteed → Guaranteed",
            containers: vec![with_resources(
                make_container("c"),
                guaranteed_resources("100m", "128Mi"),
            )],
            expected: QoSClass::Guaranteed,
        },
        Case {
            label: "two containers both guaranteed → Guaranteed",
            containers: vec![
                with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
                with_resources(make_container("c2"), guaranteed_resources("200m", "256Mi")),
            ],
            expected: QoSClass::Guaranteed,
        },
        Case {
            label: "requests only → Burstable",
            containers: vec![with_resources(
                make_container("c"),
                requests_only_resources("100m", "128Mi"),
            )],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "limits only → Burstable (Rusternetes divergence; upstream Guaranteed)",
            containers: vec![with_resources(
                make_container("c"),
                limits_only_resources("100m", "128Mi"),
            )],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "requests < limits → Burstable",
            containers: vec![with_resources(
                make_container("c"),
                burstable_resources("100m", "128Mi", "200m", "256Mi"),
            )],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "one guaranteed one no-resources → Burstable",
            containers: vec![
                with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi")),
                make_container("c2"),
            ],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "cpu-only limit → Burstable",
            containers: vec![with_resources(
                make_container("c"),
                ResourceRequirements {
                    requests: Some(HashMap::from([("cpu".to_string(), "100m".to_string())])),
                    limits: Some(HashMap::from([("cpu".to_string(), "100m".to_string())])),
                    claims: None,
                },
            )],
            expected: QoSClass::Burstable,
        },
        Case {
            label: "memory-only limit → Burstable",
            containers: vec![with_resources(
                make_container("c"),
                ResourceRequirements {
                    requests: Some(HashMap::from([("memory".to_string(), "128Mi".to_string())])),
                    limits: Some(HashMap::from([("memory".to_string(), "128Mi".to_string())])),
                    claims: None,
                },
            )],
            expected: QoSClass::Burstable,
        },
    ];

    for case in cases {
        let pod = make_pod("pod", case.containers, None, None);
        assert_eq!(
            get_qos_class(&pod),
            case.expected,
            "case failed: {}",
            case.label
        );
    }
}

// ---------------------------------------------------------------------------
// Status field reflection
// ---------------------------------------------------------------------------

/// When a pod already has a `.status.qos_class` string set (written by the
/// kubelet after scheduling), the classification function still derives the
/// class from the spec, not from the cached status field.
#[test]
fn get_qos_class_ignores_status_field() {
    let c = with_resources(make_container("c1"), guaranteed_resources("100m", "128Mi"));
    let mut pod = make_pod("pod", vec![c], None, None);
    // Deliberately set an incorrect status field.
    pod.status = Some(PodStatus {
        qos_class: Some("BestEffort".to_string()),
        ..Default::default()
    });
    // Classification must derive from spec, not status.
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}
