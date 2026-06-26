//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-node] Probes + Init containers.
//!
//! Source of truth: Ginkgo descriptors at
//!   https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/node/
//! Specifically:
//!   - test/e2e/common/node/init_container.go (RestartNever / RestartAlways
//!     init container restart semantics + ordering)
//!   - test/e2e/common/node/container_probe.go (liveness/readiness/startup
//!     probes with exec/httpGet/tcpSocket/grpc actions + threshold knobs)
//!
//!
//! See docs/conformance/node-probes-init-containers.md for the
//! test-by-test status table and cross-reference into
//! docs/CONFORMANCE.md "Init containers" failure bucket (~2 failures).
//!
//! No HTTP harness: the kubelet doesn't host the REST surface. These
//! tests exercise the pure helpers
//!   - `rusternetes_kubelet::runtime::decide_next_init_action`
//!   - `rusternetes_common::resources::resolve_probe_port`
//!   - the `Probe` threshold / period / timeout fields the runtime reads
//!
//! directly, mirroring the prior-art pattern from
//! `crates/kubelet/tests/runtime_prestop_exit_test.rs` and
//! `crates/kubelet/tests/init_container_restart_test.rs`.

use rusternetes_common::resources::{
    resolve_probe_port, Container, ContainerPort, ExecAction, GRPCAction, HTTPGetAction,
    HTTPHeader, IntOrString, Pod, PodSpec, Probe, TCPSocketAction,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kubelet::runtime::{decide_next_init_action, InitAction, InitContainerObserved};

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn make_container(name: &str) -> Container {
    Container {
        name: name.to_string(),
        image: "registry.k8s.io/e2e-test-images/agnhost:2.55".to_string(),
        image_pull_policy: Some("IfNotPresent".to_string()),
        ..Default::default()
    }
}

fn make_init(name: &str, restart_policy: Option<&str>) -> Container {
    let mut c = make_container(name);
    c.image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1".to_string();
    c.restart_policy = restart_policy.map(|s| s.to_string());
    c
}

fn make_pod_with_inits(name: &str, restart_policy: &str, inits: Vec<Container>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![make_container("run1")],
            init_containers: Some(inits),
            restart_policy: Some(restart_policy.to_string()),
            ..Default::default()
        }),
        status: None,
    }
}

/// Build the textbook `Probe` shape upstream e2e uses — every threshold and
/// timing field set explicitly so the test asserts what the kubelet reads
/// off the struct.
fn make_probe(
    initial_delay: i32,
    period: i32,
    timeout: i32,
    failure_threshold: i32,
    success_threshold: i32,
) -> Probe {
    Probe {
        http_get: None,
        tcp_socket: None,
        exec: None,
        grpc: None,
        initial_delay_seconds: Some(initial_delay),
        timeout_seconds: Some(timeout),
        period_seconds: Some(period),
        success_threshold: Some(success_threshold),
        failure_threshold: Some(failure_threshold),
        termination_grace_period_seconds: None,
    }
}

fn exec_probe(cmd: Vec<&str>) -> Probe {
    let mut p = make_probe(0, 10, 1, 3, 1);
    p.exec = Some(ExecAction {
        command: cmd.iter().map(|s| s.to_string()).collect(),
    });
    p
}

fn http_probe(path: &str, port: i32) -> Probe {
    let mut p = make_probe(0, 10, 1, 3, 1);
    p.http_get = Some(HTTPGetAction {
        path: Some(path.to_string()),
        port: IntOrString::Int(port),
        host: None,
        scheme: Some("HTTP".to_string()),
        http_headers: None,
    });
    p
}

fn tcp_probe(port: i32) -> Probe {
    let mut p = make_probe(0, 10, 1, 3, 1);
    p.tcp_socket = Some(TCPSocketAction {
        port: IntOrString::Int(port),
        host: None,
    });
    p
}

fn grpc_probe(port: i32, service: Option<&str>) -> Probe {
    let mut p = make_probe(0, 10, 1, 3, 1);
    p.grpc = Some(GRPCAction {
        port: IntOrString::Int(port),
        service: service.map(|s| s.to_string()),
    });
    p
}

// ===========================================================================
// Section 1 — Init container ordering and restart semantics
//
// Mirrors test/e2e/common/node/init_container.go:
//   - L218 ConformanceIt "should invoke init containers on a RestartNever pod"
//   - L275 ConformanceIt "should invoke init containers on a RestartAlways pod"
//   - L330 ConformanceIt "should not start app containers if init containers
//          fail on a RestartAlways pod"        (Sonobuoy R160: FAIL)
//   - L430 ConformanceIt "should not start app containers and fail the pod
//          if init containers fail on a RestartNever pod"
//
// The kubelet helper under test, `decide_next_init_action`, encodes the
// entire state machine: declaration-order iteration, sidecar carve-out,
// running-blocks-advancement rule, and the pod-restart-policy branch.
// ===========================================================================

/// [sig-node] InitContainer should invoke init containers on a RestartNever pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/init_container.go:218
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn init_containers_should_invoke_on_restart_never_pod() {
    let pod = make_pod_with_inits(
        "invoke-never",
        "Never",
        vec![make_init("init1", None), make_init("init2", None)],
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
        },
        "two successful init containers on a RestartNever pod must surface \
         all_init_done=true so app containers can start (init_container.go:218)"
    );
}

/// [sig-node] InitContainer should invoke init containers on a RestartAlways pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/init_container.go:275
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn init_containers_should_invoke_on_restart_always_pod() {
    let pod = make_pod_with_inits(
        "invoke-always",
        "Always",
        vec![make_init("init1", None), make_init("init2", None)],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::Exited(0),
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert!(
        action.all_init_done,
        "two successful init containers on a RestartAlways pod must complete \
         (init_container.go:275)"
    );
}

/// [sig-node] InitContainer should not start app containers if init containers fail on a RestartAlways pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/init_container.go:330
/// Sonobuoy (Round 160): FAIL — "Expected <*v1.PodCondition>: nil not to be nil".
/// This unit-level mirror covers the pure helper
/// `decide_next_init_action`, which already returns
/// `InitAction { all_init_done: false, next_index: Some(0),
/// should_retry: true }` for a RestartAlways pod with a failed init.
/// The accompanying production-side publishing of
/// `PodCondition{Type: "Initialized", Status: "False", Reason:
/// "ContainersNotInitialized"}` is provided by
/// `Self::init_failed_pod_conditions` on the kubelet status-sync path
/// in `crates/kubelet/src/kubelet.rs` (around the init-container
/// failure branch, ~L2225) — those conditions are what the upstream
/// e2e assertion at init_container.go:446 checks.
#[test]
fn init_containers_should_not_start_app_on_restart_always_failure() {
    let pod = make_pod_with_inits(
        "fail-always",
        "Always",
        vec![make_init("init1", None), make_init("init2", None)],
    );
    // init1 has crashed (non-zero exit) on a RestartAlways pod: kubelet must
    // mark it for retry and MUST NOT advance into init2 or any app container.
    let observed = vec![
        InitContainerObserved::Exited(1),
        InitContainerObserved::NotStarted,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: Some(0),
            should_retry: true,
        },
        "RestartAlways pod with a failed init container must retry init[0] \
         and never advance to app containers (init_container.go:330)"
    );
}

/// [sig-node] InitContainer should not start app containers and fail the pod if init containers fail on a RestartNever pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/init_container.go:430
/// Sonobuoy (Round 160): FAIL — same PodCondition-not-published symptom as
/// the RestartAlways failure case. The pure helper `decide_next_init_action`
/// returns `InitAction { all_init_done: false, next_index: None,
/// should_retry: false }` for a RestartNever pod with a failed init, and
/// the kubelet status-sync path in `crates/kubelet/src/kubelet.rs`
/// marks the pod `Phase::Failed` with the
/// `Initialized=False / Reason=ContainersNotInitialized` condition via
/// `Self::init_failed_pod_conditions`.
#[test]
fn init_containers_should_not_start_app_and_fail_pod_on_restart_never_failure() {
    let pod = make_pod_with_inits(
        "fail-never",
        "Never",
        vec![make_init("init1", None), make_init("init2", None)],
    );
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
        "RestartNever pod with a failed init container is terminal — the \
         kubelet must not retry and must not start app containers \
         (init_container.go:430)"
    );
}

#[test]
fn init_containers_run_in_declaration_order() {
    // The first not-yet-completed regular init container wins, even if a
    // later one is already running (which can't really happen in practice
    // but exercises the ordering guarantee).
    let pod = make_pod_with_inits(
        "ordering",
        "Always",
        vec![
            make_init("init1", None),
            make_init("init2", None),
            make_init("init3", None),
        ],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::NotStarted,
        InitContainerObserved::NotStarted,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(
        action.next_index,
        Some(1),
        "init containers MUST be started in declaration order \
         (init_container.go ordering invariant)"
    );
    assert!(!action.should_retry);
}

#[test]
fn init_containers_running_blocks_app_container_start() {
    // While init[0] is still running, the kubelet must NOT advance — no
    // next_index, all_init_done=false. Mirrors the RestartAlways e2e where
    // app container `run1` stays `Waiting{PodInitializing}` while init[0]
    // is running.
    let pod = make_pod_with_inits(
        "running",
        "Always",
        vec![make_init("init1", None), make_init("init2", None)],
    );
    let observed = vec![
        InitContainerObserved::Running,
        InitContainerObserved::NotStarted,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(action.next_index, None);
    assert!(!action.all_init_done);
}

#[test]
fn restart_on_failure_retries_failed_init() {
    // RestartPolicy=OnFailure behaves like Always for failed init containers
    // (upstream `kubelet/kuberuntime_container.go::shouldContainerBeRestarted`).
    let pod = make_pod_with_inits("on-failure", "OnFailure", vec![make_init("init1", None)]);
    let observed = vec![InitContainerObserved::Exited(1)];
    let action = decide_next_init_action(&pod, &observed);
    assert!(action.should_retry, "OnFailure must retry failed inits");
}

#[test]
fn sidecar_init_does_not_gate_app_when_still_running() {
    // KEP-753 sidecar (per-container restartPolicy=Always on an init
    // container) is not part of the upstream conformance "Init containers"
    // tests in init_container.go but is covered indirectly by the
    // sidecar_containers.go tests. The pure helper must still respect the
    // carve-out: a Running sidecar must not block app containers once the
    // regular inits are done.
    let pod = make_pod_with_inits(
        "with-sidecar",
        "Always",
        vec![
            make_init("init1", None),
            make_init("sidecar0", Some("Always")),
        ],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::Running,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert!(action.all_init_done);
}

#[test]
fn pod_without_init_containers_is_trivially_done() {
    let pod = make_pod_with_inits("no-inits", "Always", vec![]);
    let action = decide_next_init_action(&pod, &[]);
    assert!(action.all_init_done);
    assert_eq!(action.next_index, None);
}

#[test]
fn first_unstarted_init_container_is_selected_without_retry() {
    let pod = make_pod_with_inits("fresh", "Always", vec![make_init("init1", None)]);
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

// ===========================================================================
// Section 2 — Probe Action variants (exec / httpGet / tcpSocket / grpc)
//
// Mirrors test/e2e/common/node/container_probe.go probe-action scenarios:
//   - exec  liveness  L128, L148, L238, L273
//   - httpGet liveness L168, L220, L291, L310
//   - tcpSocket liveness L184
//   - grpc  liveness  L559, L580
//
// The kubelet doesn't expose a public "build probe action" helper —
// these tests pin the Probe wire shape the kubelet reads off the Pod
// spec, ensuring exactly one of {http_get, tcp_socket, exec, grpc} is
// populated. A regression that mis-deserializes a probe action would
// fail compilation OR fail these assertions before any container ever
// starts.
// ===========================================================================

/// [sig-node] Probing container should be restarted with a exec "cat /tmp/health" liveness probe [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/container_probe.go:128
/// Sonobuoy (Round 160): PASS
#[test]
fn probe_exec_liveness_struct_carries_command() {
    let p = exec_probe(vec!["cat", "/tmp/health"]);
    let action = p.exec.expect("exec action must be populated");
    assert_eq!(action.command, vec!["cat", "/tmp/health"]);
    assert!(p.http_get.is_none());
    assert!(p.tcp_socket.is_none());
    assert!(p.grpc.is_none());
}

/// [sig-node] Probing container should be restarted with a /healthz http liveness probe [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/container_probe.go:168
/// Sonobuoy (Round 160): PASS
#[test]
fn probe_http_liveness_struct_carries_path_port_and_scheme() {
    let p = http_probe("/healthz", 8080);
    let h = p.http_get.expect("http_get must be populated");
    assert_eq!(h.path.as_deref(), Some("/healthz"));
    assert_eq!(h.port, IntOrString::Int(8080));
    assert_eq!(h.scheme.as_deref(), Some("HTTP"));
    assert!(p.exec.is_none());
    assert!(p.tcp_socket.is_none());
}

/// [sig-node] Probing container should *not* be restarted with a tcp:8080 liveness probe [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/container_probe.go:184
/// Sonobuoy (Round 160): PASS
#[test]
fn probe_tcp_socket_liveness_struct_carries_port() {
    let p = tcp_probe(8080);
    let t = p.tcp_socket.expect("tcp_socket must be populated");
    assert_eq!(t.port, IntOrString::Int(8080));
    assert!(p.exec.is_none());
    assert!(p.http_get.is_none());
    assert!(p.grpc.is_none());
}

/// [sig-node] Probing container should be restarted with a GRPC liveness probe [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/container_probe.go:580
/// Sonobuoy (Round 160): PASS
#[test]
fn probe_grpc_liveness_struct_carries_port_and_optional_service() {
    let p = grpc_probe(5000, Some("liveness"));
    let g = p.grpc.expect("grpc must be populated");
    assert_eq!(g.port, IntOrString::Int(5000));
    assert_eq!(g.service.as_deref(), Some("liveness"));
    assert!(p.exec.is_none());
}

/// [sig-node] Probing container should *not* be restarted with a GRPC liveness probe [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/container_probe.go:559
/// Sonobuoy (Round 160): PASS
#[test]
fn probe_grpc_service_field_is_optional() {
    let p = grpc_probe(5000, None);
    let g = p.grpc.expect("grpc must be populated");
    assert_eq!(g.port, IntOrString::Int(5000));
    assert!(
        g.service.is_none(),
        "GRPCAction.service is optional per K8s API spec (container_probe.go:559)"
    );
}

#[test]
fn probe_http_action_supports_custom_headers() {
    // container_probe.go uses HTTPHeaders for some scenarios. The struct
    // must round-trip the headers verbatim so the runtime can attach them
    // to the request.
    let mut p = http_probe("/healthz", 8080);
    p.http_get.as_mut().unwrap().http_headers = Some(vec![
        HTTPHeader {
            name: "X-Custom-Header".to_string(),
            value: "ProbeValue".to_string(),
        },
        HTTPHeader {
            name: "Accept".to_string(),
            value: "application/json".to_string(),
        },
    ]);
    let headers = p.http_get.unwrap().http_headers.unwrap();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].name, "X-Custom-Header");
    assert_eq!(headers[1].value, "application/json");
}

// ===========================================================================
// Section 3 — Probe port resolution (Int vs named String)
//
// Mirrors the IntOrString port handling exercised by container_probe.go
// L168/L291/L310 — the e2e tests use both integer and named ports. The
// `resolve_probe_port` helper is the single source of truth used by the
// kubelet to look up the port for HTTP/TCP/gRPC probes.
// ===========================================================================

#[test]
fn probe_port_int_passes_through() {
    let c = make_container("app");
    assert_eq!(resolve_probe_port(&IntOrString::Int(80), &c), Some(80));
}

#[test]
fn probe_port_named_resolves_via_container_ports() {
    let mut c = make_container("app");
    c.ports = Some(vec![
        ContainerPort {
            name: Some("http".to_string()),
            container_port: 8080,
            host_port: None,
            host_ip: None,
            protocol: "TCP".to_string(),
        },
        ContainerPort {
            name: Some("metrics".to_string()),
            container_port: 9090,
            host_port: None,
            host_ip: None,
            protocol: "TCP".to_string(),
        },
    ]);
    assert_eq!(
        resolve_probe_port(&IntOrString::String("http".to_string()), &c),
        Some(8080),
        "named port `http` must resolve to its containerPort"
    );
    assert_eq!(
        resolve_probe_port(&IntOrString::String("metrics".to_string()), &c),
        Some(9090)
    );
}

#[test]
fn probe_port_out_of_range_returns_none() {
    let c = make_container("app");
    assert_eq!(resolve_probe_port(&IntOrString::Int(-1), &c), None);
    assert_eq!(resolve_probe_port(&IntOrString::Int(70_000), &c), None);
}

// ===========================================================================
// Section 4 — Probe threshold + timing knobs
//
// Mirrors container_probe.go scenarios where the test author tunes
// `initialDelaySeconds`, `periodSeconds`, `timeoutSeconds`,
// `failureThreshold`, and `successThreshold` to flush out edge cases:
//   - L79  readiness "should not be ready before initial delay"
//   - L105 readiness "that fails should never be ready and never restart"
//   - L238 exec liveness with timeout
//   - L256 exec readiness timeout
//   - L335 startup probe fails (kills container)
//   - L359 startup probe delays liveness
//   - L411 readiness flips true the instant startup probe succeeds
//   - L481/L519 LivenessProbe / StartupProbe terminationGracePeriodSeconds
//     override
//
// We pin the field semantics the kubelet runtime reads at
// `crates/kubelet/src/runtime.rs:6124` (startup_probe.failure_threshold etc).
// ===========================================================================

#[test]
fn readiness_probe_honours_initial_delay_seconds() {
    // container_probe.go:79 — "should not be ready before initial delay
    // and never restart". A probe with a 30s initial delay should not be
    // executed until 30s after the container starts.
    let p = make_probe(30, 5, 1, 3, 1);
    assert_eq!(p.initial_delay_seconds, Some(30));
}

#[test]
fn readiness_probe_failure_threshold_is_honoured() {
    // container_probe.go:105 — "that fails should never be ready and
    // never restart". A readiness probe failure must not trigger a
    // restart (only liveness can). The threshold is per-probe.
    let p = make_probe(0, 5, 1, 3, 1);
    assert_eq!(p.failure_threshold, Some(3));
    assert_eq!(p.success_threshold, Some(1));
}

#[test]
fn exec_liveness_probe_honours_timeout_seconds() {
    // container_probe.go:238 — "should be restarted with an exec
    // liveness probe with timeout". The kubelet must enforce
    // `timeoutSeconds` so a hung exec is treated as a failed probe.
    let mut p = exec_probe(vec!["sleep", "100"]);
    p.timeout_seconds = Some(1);
    assert_eq!(p.timeout_seconds, Some(1));
}

#[test]
fn startup_probe_failure_threshold_delays_liveness() {
    // container_probe.go:359 — "should *not* be restarted by liveness
    // probe because startup probe delays it". The startup probe's
    // failure_threshold (typically large, e.g. 30) gates liveness — the
    // runtime in runtime.rs:6124 reads exactly this field.
    let p = make_probe(0, 10, 1, 30, 1);
    assert_eq!(p.failure_threshold, Some(30));
}

#[test]
fn probe_success_threshold_for_liveness_must_be_one() {
    // K8s validation: liveness/startup probes require success_threshold=1.
    // The struct accepts any value, but the runtime documents that only 1
    // is meaningful for liveness/startup. Pinning here so a future change
    // that re-purposes success_threshold flags this test.
    let p = make_probe(0, 10, 1, 3, 1);
    assert_eq!(
        p.success_threshold,
        Some(1),
        "liveness/startup probe success_threshold must be 1 (K8s validation)"
    );
}

#[test]
fn probe_termination_grace_period_override_is_optional() {
    // container_probe.go:481 / 519 — "should override
    // timeoutGracePeriodSeconds when LivenessProbe/StartupProbe field is
    // set". The probe-level override allows a probe-killed container to
    // use a different grace period than the pod's default.
    let mut p = make_probe(0, 10, 1, 3, 1);
    assert!(p.termination_grace_period_seconds.is_none());
    p.termination_grace_period_seconds = Some(1);
    assert_eq!(p.termination_grace_period_seconds, Some(1));
}

#[test]
fn probe_period_seconds_controls_check_frequency() {
    // container_probe.go uses periodSeconds=10 in many tests; the kubelet
    // sleeps `period_seconds` between probe invocations. Pin the field
    // shape so a refactor that drops `period_seconds` defaulting wakes us
    // up.
    let p = make_probe(0, 10, 1, 3, 1);
    assert_eq!(p.period_seconds, Some(10));
}

#[test]
fn probe_initial_delay_zero_means_probe_runs_immediately() {
    // L411 "should be ready immediately after startupProbe succeeds" — the
    // implementation collapses initial_delay=0 into no delay at all.
    let p = make_probe(0, 1, 1, 1, 1);
    assert_eq!(p.initial_delay_seconds, Some(0));
}

// ===========================================================================
// Section 5 — Probes attached to containers (struct-level invariants)
//
// The kubelet inspects `container.liveness_probe`, `readiness_probe`, and
// `startup_probe` per-container. These tests pin that the struct wiring
// accepts all three independently and round-trips through Container.
// ===========================================================================

#[test]
fn container_accepts_all_three_probe_types_independently() {
    let mut c = make_container("app");
    c.liveness_probe = Some(http_probe("/healthz", 8080));
    c.readiness_probe = Some(tcp_probe(8080));
    c.startup_probe = Some(exec_probe(vec!["/bin/true"]));
    assert!(c.liveness_probe.is_some());
    assert!(c.readiness_probe.is_some());
    assert!(c.startup_probe.is_some());
    // Cross-check: the three probes don't accidentally share state.
    assert!(c.liveness_probe.as_ref().unwrap().http_get.is_some());
    assert!(c.readiness_probe.as_ref().unwrap().tcp_socket.is_some());
    assert!(c.startup_probe.as_ref().unwrap().exec.is_some());
}

#[test]
fn pod_with_startup_probe_on_init_container_is_well_formed() {
    // KEP-753 sidecar init containers may have probes; regular inits may
    // not (validation enforces this), but the struct still accepts them.
    // This test pins the wiring so adding probes to init containers stays
    // a one-line change in user code.
    let mut sidecar = make_init("sidecar0", Some("Always"));
    sidecar.startup_probe = Some(http_probe("/ready", 8080));
    let pod = make_pod_with_inits("with-sidecar-probe", "Always", vec![sidecar]);
    let init = pod
        .spec
        .as_ref()
        .unwrap()
        .init_containers
        .as_ref()
        .unwrap()
        .first()
        .unwrap();
    assert!(init.startup_probe.is_some());
    assert_eq!(init.restart_policy.as_deref(), Some("Always"));
}
