//! RED-state TDD mirror of Kubernetes v1.35 integration test
//! `test/integration/dryrun/dryrun_test.go::TestDryRun`.
//!
//! Source (permalink):
//! https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/dryrun/dryrun_test.go
//!
//! Upstream `TestDryRun` discovers every server-registered resource via
//! `client.Discovery().ServerGroupsAndResources()` and, for each resource,
//! exercises five dry-run paths (`CREATE`, `UPDATE`, `PATCH`, `DELETE`,
//! `DELETE collection`, plus `scale` PATCH/UPDATE when applicable), asserting
//! that **nothing is persisted to storage**. The five `DryRunXTest` helpers in
//! the upstream file (lines 47-220) embody the persistence assertions; this
//! file mirrors the same assertions through the in-process Axum router via
//! tower's `oneshot` against an `Arc<MemoryStorage>` so we can read the
//! storage state directly after each request.
//!
//! ## Resource coverage
//!
//! Upstream sweeps ~120 GVRs via `etcd.GetEtcdStorageData()`. Mirroring the
//! full sweep would require porting every stub, every CRD fixture, and every
//! sub-resource adapter — well beyond the scope of a single RED-state pin.
//! This file instead covers a **representative subset** that maps 1:1 onto
//! resources that already have a Rust handler in
//! `crates/api-server/src/handlers/`:
//!
//! Mirrored (CREATE / UPDATE / DELETE with `?dryRun=All`):
//!   - core/v1: configmaps, secrets, services, pods, serviceaccounts,
//!     persistentvolumeclaims, resourcequotas, limitranges
//!   - apps/v1: deployments, replicasets, daemonsets, statefulsets
//!   - batch/v1: jobs, cronjobs
//!   - networking.k8s.io/v1: ingresses, networkpolicies
//!   - coordination.k8s.io/v1: leases
//!   - discovery.k8s.io/v1: endpointslices
//!
//! Skipped (documented out-of-scope for this RED pin):
//!   - cluster-scoped resources (PVs, Nodes, ClusterRoles, …) — same dry-run
//!     code path, covered in cluster-scope tests.
//!   - apiextensions.k8s.io/v1 CRDs and custom resources — would require a
//!     parallel CRD-fixture sweep; covered by `crd_*` tests.
//!   - flowcontrol.apiserver.k8s.io, policy/v1/poddisruptionbudgets — add
//!     once a dry-run regression actually appears on those handlers.
//!   - alpha/beta lifecycle resources — stability gate; mirror after GA.
//!   - sub-resources (`scale`, `status`) — covered by dedicated tests; this
//!     pin focuses on the root verbs that upstream's helpers target.
//!
//! ## Red / green breakdown
//!
//! Three `#[tokio::test]`s iterate the resource matrix:
//!   - `test_dry_run_create_does_not_persist` — POST `?dryRun=All`, then
//!     assert the registry key is ABSENT.
//!   - `test_dry_run_update_does_not_persist` — pre-seed via storage, PUT
//!     `?dryRun=All` with a mutated body, then assert the stored object did
//!     NOT pick up the mutation.
//!   - `test_dry_run_delete_does_not_persist` — pre-seed, DELETE
//!     `?dryRun=All`, then assert the object is still present with no
//!     `deletionTimestamp`.
//!
//! Tests are NOT `#[ignore]`d. They run on every CI build to pin the dry-run
//! contract for the resources above. Handlers that forget to honor
//! `is_dry_run` will fail one of the three assertions with a clear message
//! identifying which GVR / verb regressed.

use axum::http::{Method, StatusCode};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `mem` is the
// backing store so tests can assert that dry-run requests never persist.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "dryrunnamespace";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send_json(router: TestApiServer, method: Method, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router
        .send(method.as_str(), uri, Some("application/json"), Some(body))
        .await;
    (status.as_u16(), value)
}

async fn send_delete(router: TestApiServer, uri: &str) -> StatusCode {
    let (status, _) = router.delete(uri).await;
    status
}

/// Seed `mem` with `body` at the conventional registry key.
async fn seed(
    mem: &Arc<MemoryStorage>,
    resource: &str,
    namespace: Option<&str>,
    name: &str,
    body: &Value,
) -> String {
    let key = build_key(resource, namespace, name);
    mem.create(&key, body).await.expect("seed create");
    key
}

/// Snapshot the JSON stored at `key`. Returns `None` if the key is absent.
async fn snapshot(mem: &Arc<MemoryStorage>, key: &str) -> Option<Value> {
    mem.get::<Value>(key).await.ok()
}

// ---------------------------------------------------------------------------
// Resource matrix — one entry per GVR we mirror from upstream
// `etcd.GetEtcdStorageData()`.
// ---------------------------------------------------------------------------

struct ResourceCase {
    /// Display label used in assertion messages.
    label: &'static str,
    /// Storage prefix segment (e.g. `"configmaps"`).
    storage_resource: &'static str,
    /// Optional namespace — `None` for cluster-scoped resources.
    namespace: Option<&'static str>,
    /// Name of the seeded / created object.
    name: &'static str,
    /// Collection URI for POST.
    create_uri: &'static str,
    /// Item URI for PUT / DELETE.
    item_uri: &'static str,
    /// Stub body constructor — mirrors `etcd.GetEtcdStorageData()[gvr].Stub`
    /// with `apiVersion`/`kind` filled in (upstream lets the dynamic client
    /// infer those; we send them on the wire).
    stub: fn() -> Value,
}

fn cm_stub() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm1", "namespace": TEST_NS},
        "data": {"foo": "bar"}
    })
}

fn secret_stub() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "secret1", "namespace": TEST_NS},
        "data": {"key": "ZGF0YSBmaWxl"}
    })
}

fn service_stub() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "service1", "namespace": TEST_NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [{"port": 10000, "targetPort": 11000}],
            "selector": {"test": "data"}
        }
    })
}

fn pod_stub() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod1", "namespace": TEST_NS},
        "spec": {"containers": [{"name": "c1", "image": "busybox"}]}
    })
}

fn sa_stub() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "sa1name", "namespace": TEST_NS}
    })
}

fn pvc_stub() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "pvc1", "namespace": TEST_NS},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Mi"}}
        }
    })
}

fn rq_stub() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "rq1name", "namespace": TEST_NS},
        "spec": {"hard": {"cpu": "5"}}
    })
}

fn lr_stub() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "lr1name", "namespace": TEST_NS},
        "spec": {"limits": [{"type": "Pod"}]}
    })
}

fn deployment_stub() -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "deployment4", "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"f": "z"}},
            "template": {
                "metadata": {"labels": {"f": "z"}},
                "spec": {"containers": [{"image": "busybox", "name": "c"}]}
            }
        }
    })
}

fn replicaset_stub() -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "rs3", "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"g": "h"}},
            "template": {
                "metadata": {"labels": {"g": "h"}},
                "spec": {"containers": [{"image": "busybox", "name": "c"}]}
            }
        }
    })
}

fn daemonset_stub() -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "ds6", "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"a": "b"}},
            "template": {
                "metadata": {"labels": {"a": "b"}},
                "spec": {"containers": [{"image": "busybox", "name": "c"}]}
            }
        }
    })
}

fn statefulset_stub() -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "ss3", "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"a": "b"}},
            "template": {
                "metadata": {"labels": {"a": "b"}},
                "spec": {"containers": [{"image": "busybox", "name": "c"}]}
            }
        }
    })
}

fn job_stub() -> Value {
    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "job1", "namespace": TEST_NS},
        "spec": {
            "template": {
                "metadata": {"labels": {"controller-uid": "uid1"}},
                "spec": {
                    "containers": [{"image": "busybox", "name": "c"}],
                    "restartPolicy": "Never"
                }
            }
        }
    })
}

fn cronjob_stub() -> Value {
    json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "cjv1", "namespace": TEST_NS},
        "spec": {
            "schedule": "* * * * *",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "metadata": {"labels": {"controller-uid": "uid0"}},
                        "spec": {
                            "containers": [{"image": "busybox", "name": "c"}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        }
    })
}

fn ingress_stub() -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {"name": "ingress3", "namespace": TEST_NS},
        "spec": {
            "defaultBackend": {
                "service": {"name": "service", "port": {"number": 5000}}
            }
        }
    })
}

fn networkpolicy_stub() -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": "np2", "namespace": TEST_NS},
        "spec": {"podSelector": {"matchLabels": {"e": "f"}}}
    })
}

fn lease_stub() -> Value {
    json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {"name": "leasev1", "namespace": TEST_NS},
        "spec": {"holderIdentity": "holder", "leaseDurationSeconds": 5}
    })
}

fn endpointslice_stub() -> Value {
    json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {"name": "slicev1", "namespace": TEST_NS},
        "addressType": "IPv4",
        "endpoints": [],
        "ports": []
    })
}

/// The mirrored subset of `etcd.GetEtcdStorageData()` — see module docstring
/// for the in/out rationale.
fn cases() -> Vec<ResourceCase> {
    vec![
        ResourceCase {
            label: "core/v1/configmaps",
            storage_resource: "configmaps",
            namespace: Some(TEST_NS),
            name: "cm1",
            create_uri: "/api/v1/namespaces/dryrunnamespace/configmaps",
            item_uri: "/api/v1/namespaces/dryrunnamespace/configmaps/cm1",
            stub: cm_stub,
        },
        ResourceCase {
            label: "core/v1/secrets",
            storage_resource: "secrets",
            namespace: Some(TEST_NS),
            name: "secret1",
            create_uri: "/api/v1/namespaces/dryrunnamespace/secrets",
            item_uri: "/api/v1/namespaces/dryrunnamespace/secrets/secret1",
            stub: secret_stub,
        },
        ResourceCase {
            label: "core/v1/services",
            storage_resource: "services",
            namespace: Some(TEST_NS),
            name: "service1",
            create_uri: "/api/v1/namespaces/dryrunnamespace/services",
            item_uri: "/api/v1/namespaces/dryrunnamespace/services/service1",
            stub: service_stub,
        },
        ResourceCase {
            label: "core/v1/pods",
            storage_resource: "pods",
            namespace: Some(TEST_NS),
            name: "pod1",
            create_uri: "/api/v1/namespaces/dryrunnamespace/pods",
            item_uri: "/api/v1/namespaces/dryrunnamespace/pods/pod1",
            stub: pod_stub,
        },
        ResourceCase {
            label: "core/v1/serviceaccounts",
            storage_resource: "serviceaccounts",
            namespace: Some(TEST_NS),
            name: "sa1name",
            create_uri: "/api/v1/namespaces/dryrunnamespace/serviceaccounts",
            item_uri: "/api/v1/namespaces/dryrunnamespace/serviceaccounts/sa1name",
            stub: sa_stub,
        },
        ResourceCase {
            label: "core/v1/persistentvolumeclaims",
            storage_resource: "persistentvolumeclaims",
            namespace: Some(TEST_NS),
            name: "pvc1",
            create_uri: "/api/v1/namespaces/dryrunnamespace/persistentvolumeclaims",
            item_uri: "/api/v1/namespaces/dryrunnamespace/persistentvolumeclaims/pvc1",
            stub: pvc_stub,
        },
        ResourceCase {
            label: "core/v1/resourcequotas",
            storage_resource: "resourcequotas",
            namespace: Some(TEST_NS),
            name: "rq1name",
            create_uri: "/api/v1/namespaces/dryrunnamespace/resourcequotas",
            item_uri: "/api/v1/namespaces/dryrunnamespace/resourcequotas/rq1name",
            stub: rq_stub,
        },
        ResourceCase {
            label: "core/v1/limitranges",
            storage_resource: "limitranges",
            namespace: Some(TEST_NS),
            name: "lr1name",
            create_uri: "/api/v1/namespaces/dryrunnamespace/limitranges",
            item_uri: "/api/v1/namespaces/dryrunnamespace/limitranges/lr1name",
            stub: lr_stub,
        },
        ResourceCase {
            label: "apps/v1/deployments",
            storage_resource: "deployments",
            namespace: Some(TEST_NS),
            name: "deployment4",
            create_uri: "/apis/apps/v1/namespaces/dryrunnamespace/deployments",
            item_uri: "/apis/apps/v1/namespaces/dryrunnamespace/deployments/deployment4",
            stub: deployment_stub,
        },
        ResourceCase {
            label: "apps/v1/replicasets",
            storage_resource: "replicasets",
            namespace: Some(TEST_NS),
            name: "rs3",
            create_uri: "/apis/apps/v1/namespaces/dryrunnamespace/replicasets",
            item_uri: "/apis/apps/v1/namespaces/dryrunnamespace/replicasets/rs3",
            stub: replicaset_stub,
        },
        ResourceCase {
            label: "apps/v1/daemonsets",
            storage_resource: "daemonsets",
            namespace: Some(TEST_NS),
            name: "ds6",
            create_uri: "/apis/apps/v1/namespaces/dryrunnamespace/daemonsets",
            item_uri: "/apis/apps/v1/namespaces/dryrunnamespace/daemonsets/ds6",
            stub: daemonset_stub,
        },
        ResourceCase {
            label: "apps/v1/statefulsets",
            storage_resource: "statefulsets",
            namespace: Some(TEST_NS),
            name: "ss3",
            create_uri: "/apis/apps/v1/namespaces/dryrunnamespace/statefulsets",
            item_uri: "/apis/apps/v1/namespaces/dryrunnamespace/statefulsets/ss3",
            stub: statefulset_stub,
        },
        ResourceCase {
            label: "batch/v1/jobs",
            storage_resource: "jobs",
            namespace: Some(TEST_NS),
            name: "job1",
            create_uri: "/apis/batch/v1/namespaces/dryrunnamespace/jobs",
            item_uri: "/apis/batch/v1/namespaces/dryrunnamespace/jobs/job1",
            stub: job_stub,
        },
        ResourceCase {
            label: "batch/v1/cronjobs",
            storage_resource: "cronjobs",
            namespace: Some(TEST_NS),
            name: "cjv1",
            create_uri: "/apis/batch/v1/namespaces/dryrunnamespace/cronjobs",
            item_uri: "/apis/batch/v1/namespaces/dryrunnamespace/cronjobs/cjv1",
            stub: cronjob_stub,
        },
        ResourceCase {
            label: "networking.k8s.io/v1/ingresses",
            storage_resource: "ingresses",
            namespace: Some(TEST_NS),
            name: "ingress3",
            create_uri: "/apis/networking.k8s.io/v1/namespaces/dryrunnamespace/ingresses",
            item_uri: "/apis/networking.k8s.io/v1/namespaces/dryrunnamespace/ingresses/ingress3",
            stub: ingress_stub,
        },
        ResourceCase {
            label: "networking.k8s.io/v1/networkpolicies",
            storage_resource: "networkpolicies",
            namespace: Some(TEST_NS),
            name: "np2",
            create_uri: "/apis/networking.k8s.io/v1/namespaces/dryrunnamespace/networkpolicies",
            item_uri: "/apis/networking.k8s.io/v1/namespaces/dryrunnamespace/networkpolicies/np2",
            stub: networkpolicy_stub,
        },
        ResourceCase {
            label: "coordination.k8s.io/v1/leases",
            storage_resource: "leases",
            namespace: Some(TEST_NS),
            name: "leasev1",
            create_uri: "/apis/coordination.k8s.io/v1/namespaces/dryrunnamespace/leases",
            item_uri: "/apis/coordination.k8s.io/v1/namespaces/dryrunnamespace/leases/leasev1",
            stub: lease_stub,
        },
        ResourceCase {
            label: "discovery.k8s.io/v1/endpointslices",
            storage_resource: "endpointslices",
            namespace: Some(TEST_NS),
            name: "slicev1",
            create_uri: "/apis/discovery.k8s.io/v1/namespaces/dryrunnamespace/endpointslices",
            item_uri: "/apis/discovery.k8s.io/v1/namespaces/dryrunnamespace/endpointslices/slicev1",
            stub: endpointslice_stub,
        },
    ]
}

// ---------------------------------------------------------------------------
// Per-verb sweeps. Each #[tokio::test] iterates the full case list. The
// upstream test itself uses `t.Run` for naming only; we use a single Rust
// test per verb and rely on the `[label]` prefix in panic messages to
// pinpoint the regressing GVR.
// ---------------------------------------------------------------------------

/// Mirror of upstream `DryRunCreateTest`: CREATE with `?dryRun=All` must
/// return a valid object but MUST NOT persist anything to storage.
#[tokio::test]
async fn test_dry_run_create_does_not_persist() {
    for case in cases() {
        let (mem, router) = spawn_router();
        let body = (case.stub)();

        let uri = format!("{}?dryRun=All", case.create_uri);
        let (status, response_body) = send_json(router, Method::POST, &uri, &body).await;

        assert!(
            (200..300).contains(&status),
            "[{}] dry-run CREATE should succeed (2xx), got {} body={}",
            case.label,
            status,
            response_body
        );

        let key = build_key(case.storage_resource, case.namespace, case.name);
        let persisted = snapshot(&mem, &key).await;
        assert!(
            persisted.is_none(),
            "[{}] dry-run CREATE must not persist; storage at {} contains {:?}",
            case.label,
            key,
            persisted
        );
    }
}

/// Mirror of upstream `DryRunUpdateTest`: UPDATE with `?dryRun=All` must
/// return the mutated object but MUST NOT mutate storage.
#[tokio::test]
async fn test_dry_run_update_does_not_persist() {
    for case in cases() {
        let (mem, router) = spawn_router();
        let seeded = (case.stub)();
        let key = seed(
            &mem,
            case.storage_resource,
            case.namespace,
            case.name,
            &seeded,
        )
        .await;
        let before = snapshot(&mem, &key).await.expect("seeded object missing");

        // Mirror upstream `obj.SetAnnotations({"update": "true"})`.
        let mut updated = seeded.clone();
        updated["metadata"]["annotations"] = json!({"update": "true"});

        let uri = format!("{}?dryRun=All", case.item_uri);
        let (status, response_body) = send_json(router, Method::PUT, &uri, &updated).await;

        assert!(
            (200..300).contains(&status),
            "[{}] dry-run UPDATE should succeed (2xx), got {} body={}",
            case.label,
            status,
            response_body
        );

        let after = snapshot(&mem, &key).await.expect("object disappeared");
        let persisted_anno = after
            .get("metadata")
            .and_then(|m| m.get("annotations"))
            .and_then(|a| a.get("update"))
            .and_then(|v| v.as_str());
        assert_ne!(
            persisted_anno,
            Some("true"),
            "[{}] dry-run UPDATE persisted annotation; before={} after={}",
            case.label,
            before,
            after
        );
    }
}

/// Mirror of upstream `DryRunDeleteTest`: DELETE with `?dryRun=All` must
/// return success but MUST leave the object in storage with no
/// `deletionTimestamp`.
#[tokio::test]
async fn test_dry_run_delete_does_not_persist() {
    for case in cases() {
        let (mem, router) = spawn_router();
        let seeded = (case.stub)();
        let key = seed(
            &mem,
            case.storage_resource,
            case.namespace,
            case.name,
            &seeded,
        )
        .await;

        let uri = format!("{}?dryRun=All", case.item_uri);
        let status = send_delete(router, &uri).await;

        assert!(
            status.is_success() || status == StatusCode::ACCEPTED,
            "[{}] dry-run DELETE should succeed, got {}",
            case.label,
            status
        );

        let after = snapshot(&mem, &key).await;
        assert!(
            after.is_some(),
            "[{}] dry-run DELETE removed the object from storage at {}",
            case.label,
            key
        );
        if let Some(obj) = after {
            let ts = obj
                .get("metadata")
                .and_then(|m| m.get("deletionTimestamp"))
                .and_then(|t| t.as_str());
            assert!(
                ts.is_none(),
                "[{}] dry-run DELETE set deletionTimestamp={:?}",
                case.label,
                ts
            );
        }
    }
}
