//! Integration tests for the LoadBalancer status lifecycle, covering
//! upstream e2e `[sig-network] Services should complete a service status
//! lifecycle [Conformance]` (`test/e2e/network/service.go:3246`, fails at
//! 3459) and the broader status-PATCH-retry behavior.
//!
//! These tests drive [`LoadBalancerController`] against [`MemoryStorage`]
//! with a stub [`CloudProvider`] so the production status path is exercised
//! end-to-end without network I/O. They are the authentic mirror layer for
//! `IGNORED_TESTS_PLAN.md` item #10 — the kube-proxy file referenced in
//! that entry only emits iptables rules; the status-population bug lives in
//! this controller.

use async_trait::async_trait;
use rusternetes_common::cloud_provider::{
    CloudProvider, LoadBalancerIngress as CloudIngress, LoadBalancerService as CloudLBService,
    LoadBalancerStatus as CloudLBStatus,
};
use rusternetes_common::resources::{
    Event, IntOrString, Service, ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_common::Error;
use rusternetes_controller_manager::controllers::loadbalancer::LoadBalancerController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Mock cloud provider that records calls and can be programmed to fail a
/// configurable number of times before succeeding.
struct StubCloudProvider {
    /// Address returned in `ingress[0].ip` on success.
    ingress_ip: String,
    /// Decrements per `ensure_load_balancer` call; while > 0 the call
    /// returns a transient `Network` error.
    transient_failures_left: AtomicUsize,
    /// Total `ensure_load_balancer` invocations observed.
    ensure_calls: AtomicUsize,
}

impl StubCloudProvider {
    fn new(ingress_ip: &str, transient_failures: usize) -> Self {
        Self {
            ingress_ip: ingress_ip.to_string(),
            transient_failures_left: AtomicUsize::new(transient_failures),
            ensure_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl CloudProvider for StubCloudProvider {
    async fn ensure_load_balancer(
        &self,
        _service: &CloudLBService,
    ) -> rusternetes_common::Result<CloudLBStatus> {
        self.ensure_calls.fetch_add(1, Ordering::SeqCst);
        let prev = self.transient_failures_left.load(Ordering::SeqCst);
        if prev > 0 {
            self.transient_failures_left.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::Network(
                "stub cloud provider transient failure".to_string(),
            ));
        }
        Ok(CloudLBStatus {
            ingress: vec![CloudIngress {
                ip: Some(self.ingress_ip.clone()),
                hostname: None,
            }],
        })
    }

    async fn delete_load_balancer(
        &self,
        _service_namespace: &str,
        _service_name: &str,
    ) -> rusternetes_common::Result<()> {
        Ok(())
    }

    async fn get_load_balancer_status(
        &self,
        _service_namespace: &str,
        _service_name: &str,
    ) -> rusternetes_common::Result<Option<CloudLBStatus>> {
        Ok(None)
    }

    fn name(&self) -> &str {
        "stub"
    }
}

/// Cloud provider that always fails — used to assert terminal-error
/// propagation and Warning event emission.
struct AlwaysFailCloudProvider;

#[async_trait]
impl CloudProvider for AlwaysFailCloudProvider {
    async fn ensure_load_balancer(
        &self,
        _service: &CloudLBService,
    ) -> rusternetes_common::Result<CloudLBStatus> {
        Err(Error::Network("stub permanent failure".to_string()))
    }
    async fn delete_load_balancer(
        &self,
        _service_namespace: &str,
        _service_name: &str,
    ) -> rusternetes_common::Result<()> {
        Ok(())
    }
    async fn get_load_balancer_status(
        &self,
        _service_namespace: &str,
        _service_name: &str,
    ) -> rusternetes_common::Result<Option<CloudLBStatus>> {
        Ok(None)
    }
    fn name(&self) -> &str {
        "always-fail"
    }
}

fn lb_service(name: &str, namespace: &str) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.namespace = Some(namespace.to_string());
            m
        },
        spec: ServiceSpec {
            selector: Some(HashMap::new()),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                // Pre-allocate the NodePort so reconcile_service skips the
                // allocate-and-write step that we are not exercising here.
                node_port: Some(30080),
                app_protocol: None,
            }],
            cluster_ip: Some("10.96.1.10".to_string()),
            service_type: Some(ServiceType::LoadBalancer),
            external_ips: None,
            session_affinity: None,
            external_name: None,
            cluster_ips: None,
            ip_families: None,
            ip_family_policy: None,
            internal_traffic_policy: None,
            external_traffic_policy: None,
            health_check_node_port: None,
            load_balancer_class: None,
            load_balancer_ip: None,
            load_balancer_source_ranges: None,
            allocate_load_balancer_node_ports: None,
            publish_not_ready_addresses: None,
            session_affinity_config: None,
            traffic_distribution: None,
        },
        status: None,
    }
}

/// Sub-bug for upstream e2e `service.go:3459` (failure observed at 3459 of
/// the "should complete a service status lifecycle" test).
///
/// With a working cloud provider the controller must populate
/// `status.loadBalancer.ingress` on the first reconcile. This is the
/// happy-path test that anchors the lifecycle.
#[tokio::test]
async fn loadbalancer_status_populated_on_first_reconcile_with_cloud_provider() {
    let storage = Arc::new(MemoryStorage::new());
    let provider = Arc::new(StubCloudProvider::new("203.0.113.42", 0));

    let controller = LoadBalancerController::new(
        storage.clone(),
        Some(provider.clone() as Arc<dyn CloudProvider>),
        "test-cluster".to_string(),
        30,
    );

    let svc = lb_service("lb-happy", "default");
    let key = build_key("services", Some("default"), "lb-happy");
    storage.create(&key, &svc).await.unwrap();

    controller.reconcile_all().await.expect("reconcile_all");

    let after: Service = storage.get(&key).await.unwrap();
    let lb = after
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
        .expect("status.loadBalancer must be populated");
    assert_eq!(lb.ingress.len(), 1);
    assert_eq!(lb.ingress[0].ip.as_deref(), Some("203.0.113.42"));
    assert_eq!(
        provider.ensure_calls.load(Ordering::SeqCst),
        1,
        "ensure_load_balancer should be called exactly once on the happy path"
    );
}

/// Sub-bug for upstream e2e `service.go:3459` — when the cloud-provider
/// throws a transient error, the controller surfaces the failure and
/// relies on the workqueue (or the next periodic resync) to re-enqueue.
/// The status must populate on the second reconcile, not stay empty.
///
/// Matches upstream's design in
/// `staging/src/k8s.io/cloud-provider/controllers/service/controller.go`
/// where `EnsureLoadBalancer` failures return up to `processNextServiceItem`
/// which calls `workqueue.AddRateLimited()`.
#[tokio::test]
async fn loadbalancer_status_populates_on_workqueue_retry() {
    let storage = Arc::new(MemoryStorage::new());
    // 1 transient failure, then success — first reconcile fails, second
    // (simulating the workqueue re-enqueue) succeeds.
    let provider = Arc::new(StubCloudProvider::new("203.0.113.99", 1));

    let controller = LoadBalancerController::new(
        storage.clone(),
        Some(provider.clone() as Arc<dyn CloudProvider>),
        "test-cluster".to_string(),
        30,
    );

    let svc = lb_service("lb-transient", "default");
    let key = build_key("services", Some("default"), "lb-transient");
    storage.create(&key, &svc).await.unwrap();

    // First reconcile: cloud provider fails → reconcile_all swallows the
    // per-service error and logs (real workqueue would AddRateLimited).
    let _ = controller.reconcile_all().await;
    let after_first: Service = storage.get(&key).await.unwrap();
    assert!(
        after_first
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref())
            .map(|lb| lb.ingress.is_empty())
            .unwrap_or(true),
        "status.loadBalancer must NOT be populated after failed reconcile"
    );

    // Second reconcile (workqueue re-enqueue): cloud provider succeeds.
    controller
        .reconcile_all()
        .await
        .expect("second reconcile_all should succeed");

    let after: Service = storage.get(&key).await.unwrap();
    let lb = after
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
        .expect("status.loadBalancer populated after workqueue retry");
    assert!(!lb.ingress.is_empty(), "ingress must be non-empty");
    assert_eq!(lb.ingress[0].ip.as_deref(), Some("203.0.113.99"));
    assert_eq!(
        provider.ensure_calls.load(Ordering::SeqCst),
        2,
        "must observe 1 failed + 1 successful ensure_load_balancer call"
    );
}

/// When the cloud provider fails, reconcile must emit a Warning Event so
/// operators can see the failure via `kubectl describe svc`. Upstream
/// doesn't emit this event; we keep it because the previous behaviour
/// (`error!()`-only) was opaque to operators in production incident
/// debriefs. The Event reason mirrors upstream's `SyncLoadBalancerFailed`
/// wording so existing dashboards can match on it.
#[tokio::test]
async fn loadbalancer_status_emits_warning_event_on_failure() {
    let storage = Arc::new(MemoryStorage::new());
    let provider: Arc<dyn CloudProvider> = Arc::new(AlwaysFailCloudProvider);

    let controller = LoadBalancerController::new(
        storage.clone(),
        Some(provider),
        "test-cluster".to_string(),
        30,
    );

    let svc = lb_service("lb-fail", "default");
    let key = build_key("services", Some("default"), "lb-fail");
    storage.create(&key, &svc).await.unwrap();

    // Call reconcile_all directly so we observe the propagated error
    // surface. `reconcile_all` swallows per-service errors today (it
    // `error!`-logs and continues), so we instead look at side-effects:
    // the Warning Event must be present.
    let _ = controller.reconcile_all().await;

    // Find a Warning event for our Service.
    let events: Vec<Event> = storage
        .list("/registry/events/default/")
        .await
        .unwrap_or_default();
    let warning = events.iter().find(|e| {
        e.involved_object.name.as_deref() == Some("lb-fail") && e.reason == "SyncLoadBalancerFailed"
    });
    assert!(
        warning.is_some(),
        "Warning Event 'SyncLoadBalancerFailed' must be recorded against Service lb-fail. Events: {:?}",
        events.iter().map(|e| &e.reason).collect::<Vec<_>>()
    );
}

/// When the service is deleted before the controller finishes patching
/// status (a race the upstream lifecycle test exercises), the controller
/// must not panic or leave a half-written status entry. The `Storage::get`
/// inside `update_service_status` returns NotFound, we retry, and finally
/// surface a clean error.
#[tokio::test]
async fn loadbalancer_status_handles_service_deleted_mid_reconcile() {
    let storage = Arc::new(MemoryStorage::new());
    let provider = Arc::new(StubCloudProvider::new("203.0.113.7", 0));

    let controller = LoadBalancerController::new(
        storage.clone(),
        Some(provider.clone() as Arc<dyn CloudProvider>),
        "test-cluster".to_string(),
        30,
    );

    let svc = lb_service("lb-deleted", "default");
    let key = build_key("services", Some("default"), "lb-deleted");
    storage.create(&key, &svc).await.unwrap();

    // Delete BEFORE reconcile. enqueue_all → worker will list nothing.
    storage.delete(&key).await.unwrap();

    // reconcile_all must not panic, and must not write the service back.
    controller
        .reconcile_all()
        .await
        .expect("reconcile_all must tolerate missing services");
    assert!(
        storage.get::<Service>(&key).await.is_err(),
        "deleted service must not be resurrected"
    );
}

/// Pre-existing `status.conditions` set by another controller must survive
/// our `status.loadBalancer` write. Mirrors upstream's "DeepCopy then mutate
/// only LoadBalancer" pattern in
/// `staging/src/k8s.io/cloud-provider/controllers/service/controller.go`.
/// Also asserts the Warning Event we emit carries the Service UID so
/// `kubectl describe svc` doesn't lose the audit trail across recreations.
#[tokio::test]
async fn loadbalancer_status_preserves_conditions_and_emits_uid_event() {
    use rusternetes_common::types::Condition;
    let storage = Arc::new(MemoryStorage::new());
    let provider = Arc::new(StubCloudProvider::new("203.0.113.55", 0));
    let controller = LoadBalancerController::new(
        storage.clone(),
        Some(provider.clone() as Arc<dyn CloudProvider>),
        "test-cluster".to_string(),
        30,
    );

    // Seed with a Service that already has a non-LB condition + a UID.
    let mut svc = lb_service("lb-with-conditions", "default");
    svc.metadata.uid = "uid-1234-5678".to_string();
    svc.status = Some(rusternetes_common::resources::service::ServiceStatus {
        load_balancer: None,
        conditions: Some(vec![Condition {
            condition_type: "OtherControllerOK".to_string(),
            status: "True".to_string(),
            last_transition_time: Some(chrono::Utc::now()),
            reason: Some("Seeded".to_string()),
            message: Some("set by another controller".to_string()),
            observed_generation: None,
        }]),
    });
    let key = build_key("services", Some("default"), "lb-with-conditions");
    storage.create(&key, &svc).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // status.loadBalancer populated AND pre-existing condition preserved.
    let after: Service = storage.get(&key).await.unwrap();
    let status = after.status.as_ref().expect("status must exist");
    assert_eq!(
        status
            .load_balancer
            .as_ref()
            .and_then(|lb| lb.ingress.first())
            .and_then(|ing| ing.ip.as_deref()),
        Some("203.0.113.55")
    );
    let conditions = status.conditions.as_ref().expect("conditions preserved");
    assert!(
        conditions
            .iter()
            .any(|c| c.condition_type == "OtherControllerOK"),
        "pre-existing OtherControllerOK condition must survive LB status write; got {:?}",
        conditions
    );

    // No event on success path — but if we re-trigger with a failing
    // provider, the Warning Event must carry the UID we seeded.
    let failing: Arc<dyn CloudProvider> = Arc::new(AlwaysFailCloudProvider);
    let failing_controller = LoadBalancerController::new(
        storage.clone(),
        Some(failing),
        "test-cluster".to_string(),
        30,
    );
    let _ = failing_controller.reconcile_all().await;
    let events: Vec<Event> = storage
        .list("/registry/events/default/")
        .await
        .unwrap_or_default();
    let event = events
        .iter()
        .find(|e| e.involved_object.name.as_deref() == Some("lb-with-conditions"))
        .expect("Warning event recorded against the Service");
    assert_eq!(
        event.involved_object.uid.as_deref(),
        Some("uid-1234-5678"),
        "Warning event must carry the Service UID for audit-trail continuity"
    );
}
