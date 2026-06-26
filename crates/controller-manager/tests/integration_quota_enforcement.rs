//! Scoped mirror of the upstream Kubernetes v1.35 integration test file
//! `test/integration/quota/quota_test.go` as a RED-state TDD pin.
//!
//! Upstream permalink (release-1.35):
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/quota/quota_test.go>
//!
//! The upstream tests exercise three lanes of ResourceQuota behavior:
//!   1. `TestQuota` — pod-count enforcement: a `ResourceQuota` with
//!      `hard.pods = 1000` must let a replication controller scale up
//!      to 100 replicas, while the controller publishes a `status.used`
//!      that matches reality (the upstream test asserts scale-up
//!      eventually converges; we mirror the convergence signal as
//!      `status.used.pods` matching the live pod count).
//!   2. `TestQuotaLimitedResourceDenial` — admission-side denial of pod
//!      creates in a namespace whose quota's hard limit is reached.
//!      The upstream test wires the `ResourceQuota` admission plugin
//!      with `limitedResources: [resource: pods, matchContains: pods]`.
//!      In our split, admission lives in the api-server and the
//!      controller publishes `status.used` such that an admission
//!      check (or, in this controller-level mirror, a direct
//!      comparison `status.used.pods >= status.hard.pods`) would
//!      forbid further pod creations.
//!   3. `TestQuotaLimitService` — service-tier counts: `services`,
//!      `services.nodeports`, and `services.loadbalancers` are tracked
//!      independently. Creating a NodePort consumes one slot in
//!      `services` and `services.nodeports`; a LoadBalancer with
//!      node ports consumes all three counters; a LoadBalancer
//!      without node ports consumes `services` and
//!      `services.loadbalancers` only. The upstream test asserts
//!      `status.used` reflects each create.
//!
//! Both lanes are driven through `ResourceQuotaController::reconcile_all`
//! against a `MemoryStorage`, matching the canonical controller-driver
//! pattern used by the rest of `crates/controller-manager/tests/`
//! (see e.g. `resource_quota_test.rs`, `resource_quota_idempotency_test.rs`).
//!
//! ## RED-state acceptance
//!
//! These tests are TDD pins for upstream parity. Each `#[tokio::test]`
//! keeps its upstream name (`TestX` → `test_x` snake_case) so cross-
//! referencing the Go source is mechanical. The assertions encode what
//! the upstream tests prove; any future regression that drifts our
//! quota controller away from K8s semantics will trip exactly one of
//! these checks.
//!
//!   - `test_quota` — GREEN today. Pins the pod-count convergence
//!     guarantee.
//!   - `test_quota_limited_resource_denial` — GREEN today. Pins the
//!     `pods` / `count/pods` matchContains lane that admission relies
//!     on for the `limitedResources` configuration.
//!   - `test_quota_limit_service` — RED today. The current
//!     `ResourceQuotaController::calculate_usage` treats every
//!     `LoadBalancer` service as consuming a `services.nodeports`
//!     slot regardless of `spec.allocateLoadBalancerNodePorts`.
//!     Upstream K8s only counts the NodePort slot when an LB
//!     actually allocates node ports — see `pkg/quota/v1/evaluator/
//!     core/services.go`. This pin will go green once the controller
//!     filters NodePort counting by `allocate_load_balancer_node_ports`
//!     for LB services.
//!
//! Part of the /batch landing upstream integration-test mirrors as
//! RED-state TDD pins.

use rusternetes_common::resources::{
    Container, Pod, PodSpec, ResourceQuota, ResourceQuotaSpec, Service, ServicePort, ServiceSpec,
    ServiceType,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Build a minimal BestEffort pod (no resources) in `namespace`. Mirrors
/// upstream `scale()`'s pod template (busybox container, no resources) —
/// the busybox image is replaced with `pause:latest` since image identity
/// is irrelevant to quota counting.
fn make_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "container".to_string(),
                image: "pause:latest".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    }
}

/// Build a Service mirroring upstream `newService()`. Per upstream,
/// `allocateLoadBalancerNodePorts` is only set when type is LoadBalancer.
/// The quota controller classifies services by `spec.type` (and, for the
/// upstream-correct lane this file pins, by `allocateLoadBalancerNodePorts`
/// when type is LoadBalancer) — `spec.ports[].nodePort` is api-server
/// allocation state and not consulted for quota counting.
fn make_service(
    name: &str,
    namespace: &str,
    svc_type: ServiceType,
    allocate_node_port: bool,
) -> Service {
    let allocate_lb_nps = if svc_type == ServiceType::LoadBalancer {
        Some(allocate_node_port)
    } else {
        None
    };
    let spec = ServiceSpec {
        service_type: Some(svc_type),
        allocate_load_balancer_node_ports: allocate_lb_nps,
        ports: vec![ServicePort {
            name: None,
            port: 80,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        }],
        ..Default::default()
    };
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec,
        status: None,
    }
}

/// Build a `ResourceQuota` with `hard = h` in `namespace`.
fn make_quota(name: &str, namespace: &str, hard: HashMap<String, String>) -> ResourceQuota {
    ResourceQuota::new(
        name,
        namespace,
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    )
}

/// Mirrors upstream `TestQuota`: a `ResourceQuota` with `hard.pods = 1000`
/// must let a controller "scale up" to 100 pods, and after reconciliation
/// the quota's `status.used.pods` must equal 100.
///
/// The upstream test uses the replication controller and watches the RC's
/// status.replicas converge; here we materialize the pods directly through
/// storage (the quota controller only reads pods to count usage, the RC
/// is incidental to the quota assertion).
#[tokio::test]
async fn test_quota() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ResourceQuotaController::new(storage.clone());

    // Two namespaces: one with quota ("quotaed"), one without ("non-quotaed").
    // Both get 100 pods seeded. Only the quotaed namespace gets a quota.
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "1000".to_string());
    let quota = make_quota("quota", "quotaed", hard);
    let quota_key = build_key("resourcequotas", Some("quotaed"), "quota");
    storage.create(&quota_key, &quota).await.unwrap();

    for i in 0..100 {
        let pod_name = format!("foo-{}", i);

        let pod_quotaed = make_pod(&pod_name, "quotaed");
        let key_q = build_key("pods", Some("quotaed"), &pod_name);
        storage.create(&key_q, &pod_quotaed).await.unwrap();

        let pod_nonquotaed = make_pod(&pod_name, "non-quotaed");
        let key_nq = build_key("pods", Some("non-quotaed"), &pod_name);
        storage.create(&key_nq, &pod_nonquotaed).await.unwrap();
    }

    // Reconcile populates status.used.
    controller.reconcile_all().await.unwrap();

    let updated: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let status = updated.status.expect("quota status must be populated");
    let used = status.used.expect("status.used must be populated");

    // Upstream: "Took 12.021640372s to scale up with quota" — convergence
    // signal is that the controller publishes the live pod count.
    assert_eq!(
        used.get("pods").map(String::as_str),
        Some("100"),
        "ResourceQuotaController must publish status.used.pods matching \
         live pod count (got {:?})",
        used.get("pods")
    );

    // status.hard must be preserved verbatim from spec.hard.
    let hard_status = status.hard.expect("status.hard must mirror spec.hard");
    assert_eq!(
        hard_status.get("pods").map(String::as_str),
        Some("1000"),
        "status.hard.pods must mirror spec.hard.pods"
    );
}

/// Mirrors upstream `TestQuotaLimitedResourceDenial`: the `ResourceQuota`
/// admission plugin's `limitedResources: [resource: pods, matchContains: pods]`
/// causes pod creates to be DENIED when no quota covering "pods" exists in
/// the namespace, and ALLOWED once a covering quota is in place AND the
/// quota's status reflects available headroom.
///
/// Admission proper lives in the api-server (see
/// `crates/api-server/tests/admission_test.rs`); at the controller-manager
/// layer the lane we can pin is the second half of the upstream assertion:
/// after a covering quota is created and reconciled, the quota's
/// `status.used` must show 0 pods (i.e. there is headroom) AND
/// `status.hard` must include `pods` so admission can compare against it.
#[tokio::test]
async fn test_quota_limited_resource_denial() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ResourceQuotaController::new(storage.clone());

    // Create the covering quota:
    //   pods: 1000
    //   count/pods: 1000   (matchContains "pods" matches both keys upstream)
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "1000".to_string());
    hard.insert("count/pods".to_string(), "1000".to_string());
    let quota = make_quota("quota", "quota-ns", hard);
    let quota_key = build_key("resourcequotas", Some("quota-ns"), "quota");
    storage.create(&quota_key, &quota).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let updated: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let status = updated.status.expect("status must be populated");
    let used = status.used.expect("status.used must be populated");

    // Headroom check: a fresh quota with no pods in the namespace must
    // report status.used.pods = "0" so admission allows the next create.
    assert_eq!(
        used.get("pods").map(String::as_str),
        Some("0"),
        "fresh covering quota in empty namespace must report \
         status.used.pods = 0 to provide admission headroom"
    );
    assert_eq!(
        used.get("count/pods").map(String::as_str),
        Some("0"),
        "fresh covering quota in empty namespace must report \
         status.used.count/pods = 0 (matchContains lane)"
    );

    // status.hard must echo BOTH keys so admission's matchContains can
    // resolve "pods" against either.
    let hard_status = status.hard.expect("status.hard must be populated");
    assert!(
        hard_status.contains_key("pods"),
        "status.hard must include 'pods' for admission lookup"
    );
    assert!(
        hard_status.contains_key("count/pods"),
        "status.hard must include 'count/pods' for matchContains lookup"
    );

    // Now seed a pod and reconcile: status.used must increment to 1 so
    // future admission compares 1 < 1000 (allow) and would forbid once
    // it reaches 1000.
    let pod = make_pod("foo", "quota-ns");
    let pod_key = build_key("pods", Some("quota-ns"), "foo");
    storage.create(&pod_key, &pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let post: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let post_used = post
        .status
        .and_then(|s| s.used)
        .expect("status.used must remain populated after pod create");
    assert_eq!(
        post_used.get("pods").map(String::as_str),
        Some("1"),
        "creating a pod in quota-ns must increment status.used.pods to 1"
    );
    assert_eq!(
        post_used.get("count/pods").map(String::as_str),
        Some("1"),
        "count/pods must track pods 1:1 (matchContains lane)"
    );
}

/// Mirrors upstream `TestQuotaLimitService`: services, NodePort services,
/// and LoadBalancer services have independent counters. The upstream test
/// walks a sequence:
///
///   1. Quota: services=4, services.nodeports=2, services.loadbalancers=2.
///   2. Create NodePort `np-svc` → used: services=1, nps=1, lbs=0.
///   3. Create LB+nodeport `lb-svc-withnp1` → used: services=2, nps=2, lbs=1.
///   4. Attempt LB+nodeport `lb-svc-withnp2` → FORBIDDEN (nps exhausted).
///   5. Create LB no-nodeport `lb-svc-wonp1` → used: services=3, nps=2, lbs=2.
///   6. Attempt LB no-nodeport `lb-svc-wonp2` → FORBIDDEN (lbs exhausted).
///   7. Create ClusterIP `clusterip-svc1` → used: services=4, nps=2, lbs=2.
///   8. Attempt ClusterIP `clusterip-svc2` → FORBIDDEN (services exhausted).
///
/// The FORBIDDEN steps are admission-plugin behavior (api-server). At the
/// controller layer we mirror the counting lane: after each "successful"
/// create the quota controller must publish `status.used` that matches
/// the upstream `expectedQuotaUsed` for that step. Admission can then
/// compute `used >= hard` and reject step (4), (6), (8) — which the
/// controller correctly enables by keeping the counters in sync.
#[tokio::test]
async fn test_quota_limit_service() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ResourceQuotaController::new(storage.clone());

    let mut hard = HashMap::new();
    hard.insert("services".to_string(), "4".to_string());
    hard.insert("services.nodeports".to_string(), "2".to_string());
    hard.insert("services.loadbalancers".to_string(), "2".to_string());
    let quota = make_quota("quota", "quota-ns", hard);
    let quota_key = build_key("resourcequotas", Some("quota-ns"), "quota");
    storage.create(&quota_key, &quota).await.unwrap();

    // Step 2: create NodePort service.
    let np = make_service("np-svc", "quota-ns", ServiceType::NodePort, true);
    storage
        .create(&build_key("services", Some("quota-ns"), "np-svc"), &np)
        .await
        .unwrap();

    // Step 3: create LoadBalancer service with node ports.
    let lb_with_np = make_service(
        "lb-svc-withnp1",
        "quota-ns",
        ServiceType::LoadBalancer,
        true,
    );
    storage
        .create(
            &build_key("services", Some("quota-ns"), "lb-svc-withnp1"),
            &lb_with_np,
        )
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let after_two: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let used_two = after_two
        .status
        .and_then(|s| s.used)
        .expect("status.used must be populated after step 3");
    assert_eq!(
        used_two.get("services").map(String::as_str),
        Some("2"),
        "after NodePort + LB-with-NP creates, services count must be 2"
    );
    assert_eq!(
        used_two.get("services.nodeports").map(String::as_str),
        Some("2"),
        "after NodePort + LB-with-NP creates, services.nodeports must be 2 \
         (both consume a node port slot)"
    );
    assert_eq!(
        used_two.get("services.loadbalancers").map(String::as_str),
        Some("1"),
        "after NodePort + LB-with-NP creates, services.loadbalancers must be 1"
    );

    // Step 5: create LoadBalancer service WITHOUT node ports.
    let lb_no_np = make_service("lb-svc-wonp1", "quota-ns", ServiceType::LoadBalancer, false);
    storage
        .create(
            &build_key("services", Some("quota-ns"), "lb-svc-wonp1"),
            &lb_no_np,
        )
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let after_three: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let used_three = after_three
        .status
        .and_then(|s| s.used)
        .expect("status.used must be populated after step 5");
    assert_eq!(
        used_three.get("services").map(String::as_str),
        Some("3"),
        "after step 5, services count must be 3"
    );
    assert_eq!(
        used_three.get("services.loadbalancers").map(String::as_str),
        Some("2"),
        "after step 5, services.loadbalancers must be 2 \
         (both LBs counted regardless of node-port allocation)"
    );

    // Step 7: create ClusterIP service. Saturates `services` at 4 but
    // does NOT consume NodePort or LoadBalancer slots.
    let cluster_ip = make_service("clusterip-svc1", "quota-ns", ServiceType::ClusterIP, false);
    storage
        .create(
            &build_key("services", Some("quota-ns"), "clusterip-svc1"),
            &cluster_ip,
        )
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let after_four: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let used_four = after_four
        .status
        .and_then(|s| s.used)
        .expect("status.used must be populated after step 7");
    assert_eq!(
        used_four.get("services").map(String::as_str),
        Some("4"),
        "after ClusterIP create, services count must be 4 (quota saturated)"
    );
    assert_eq!(
        used_four.get("services.nodeports").map(String::as_str),
        Some("2"),
        "ClusterIP create must NOT increment services.nodeports"
    );
    assert_eq!(
        used_four.get("services.loadbalancers").map(String::as_str),
        Some("2"),
        "ClusterIP create must NOT increment services.loadbalancers"
    );

    // Headroom check encoding the upstream FORBIDDEN assertion: at this
    // point any of the three counters has reached its hard limit, so
    // admission would reject further creates. We assert the controller
    // has surfaced the saturation correctly.
    assert!(
        used_four.get("services").map(String::as_str) == Some("4")
            && used_four.get("services.nodeports").map(String::as_str) == Some("2")
            && used_four.get("services.loadbalancers").map(String::as_str) == Some("2"),
        "all three counters must be saturated; admission will then \
         deny further service creates per upstream FORBIDDEN steps"
    );
}
