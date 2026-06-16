//! Integration-test mirror of upstream PodDisruptionBudget disruption controller suite.
//!
//! Upstream source (Kubernetes release-1.35):
//!   test/integration/disruption/disruption_test.go
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/disruption/disruption_test.go>
//!
//! These tests are translated 1:1 from the upstream Go integration tests. They
//! drive the in-repo `PodDisruptionBudgetController` (and the
//! `StalePodDisruptionController` sub-controller) directly against an
//! `Arc<MemoryStorage>` rather than spinning up an actual api-server — the
//! goal is to mirror the *observable* behaviour upstream exercises (status
//! counters, selector semantics, stale DisruptionTarget handling) and pin it
//! against future regressions.
//!
//! Upstream test count: 5
//!   - TestPDBWithScaleSubresource
//!   - TestEmptySelector
//!   - TestSelectorsForPodsWithoutLabels
//!   - TestPatchCompatibility
//!   - TestStalePodDisruption
//!
//! Result: 12 green pins. The three router-driven patch-verb tests on the
//! PDB selector — strategic-merge / JSON-merge / server-side apply — moved
//! to `crates/api-server/tests/pdb_patch_compatibility_test.rs`, which uses
//! a `build_router` + `tower::ServiceExt::oneshot` harness to exercise the
//! real PATCH dispatch in `crates/api-server/src/patch.rs`. Remaining
//! `#[ignore]`d surfaces in this file cite the missing controller-side
//! features (e.g. CRD scale subresource for `expectedPods`) and stay
//! discoverable via `cargo test -- --ignored`.

use rusternetes_common::resources::pod::{Container, Pod, PodCondition, PodSpec, PodStatus};
use rusternetes_common::resources::{
    CustomResource, CustomResourceDefinition, CustomResourceDefinitionVersion,
    CustomResourceSubresourceScale, CustomResourceSubresources, IntOrString, PodDisruptionBudget,
    PodDisruptionBudgetSpec, PodDisruptionBudgetStatus,
};
use rusternetes_common::types::{
    LabelSelector, LabelSelectorRequirement, ObjectMeta, OwnerReference, Phase, TypeMeta,
};
use rusternetes_controller_manager::controllers::pod_disruption_budget::{
    PodDisruptionBudgetController, StalePodDisruptionController,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn new_storage() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

fn new_controller(storage: Arc<MemoryStorage>) -> PodDisruptionBudgetController<MemoryStorage> {
    PodDisruptionBudgetController::new(storage)
}

/// Mirror of upstream `createPod` + `addPodConditionReady` — emits a Running
/// pod with a `Ready=True` condition so the controller counts it as healthy.
fn create_test_pod(name: &str, namespace: &str, labels: HashMap<String, String>) -> Pod {
    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            labels: if labels.is_empty() {
                None
            } else {
                Some(labels)
            },
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "fake-name".to_string(),
                image: "fakeimage".to_string(),
                image_pull_policy: None,
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
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            node_name: Some("foo".to_string()),
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
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        }),
    };
    // Mirror the upstream guarantee that the pod is observed Running+Ready.
    pod.metadata.generation = Some(1);
    pod
}

async fn put_pod(storage: &Arc<MemoryStorage>, pod: &Pod) {
    let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
    let key = build_key("pods", Some(ns), &pod.metadata.name);
    storage.create(&key, pod).await.expect("create pod");
}

async fn put_pdb(storage: &Arc<MemoryStorage>, pdb: &PodDisruptionBudget) {
    let ns = pdb.metadata.namespace.as_deref().unwrap_or("default");
    let key = build_key("poddisruptionbudgets", Some(ns), &pdb.metadata.name);
    storage.create(&key, pdb).await.expect("create pdb");
}

async fn get_pdb(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) -> PodDisruptionBudget {
    let key = build_key("poddisruptionbudgets", Some(namespace), name);
    storage
        .get::<PodDisruptionBudget>(&key)
        .await
        .expect("get pdb")
}

fn assert_pdb_status(
    status: &PodDisruptionBudgetStatus,
    expected_pods: i32,
    current_healthy: i32,
    desired_healthy: i32,
    disruptions_allowed: i32,
) {
    assert_eq!(
        status.expected_pods, expected_pods,
        "expectedPods mismatch (want {} got {})",
        expected_pods, status.expected_pods
    );
    assert_eq!(
        status.current_healthy, current_healthy,
        "currentHealthy mismatch (want {} got {})",
        current_healthy, status.current_healthy
    );
    assert_eq!(
        status.desired_healthy, desired_healthy,
        "desiredHealthy mismatch (want {} got {})",
        desired_healthy, status.desired_healthy
    );
    assert_eq!(
        status.disruptions_allowed, disruptions_allowed,
        "disruptionsAllowed mismatch (want {} got {})",
        disruptions_allowed, status.disruptions_allowed
    );
}

// ---------------------------------------------------------------------------
// TestPDBWithScaleSubresource
// ---------------------------------------------------------------------------
//
// Upstream verifies that a PDB selecting custom resources (via a CRD that
// exposes the scale subresource) correctly resolves the workload size and
// reports `ExpectedPods=replicas` and `DesiredHealthy=replicas-maxUnavailable`.
//
// We don't have a scale subresource / CRD pipeline yet — the PDB controller
// only knows how to count *pods*. We pin the upstream expectation against pods
// directly: 4 healthy pods + `maxUnavailable=2` should yield
// expectedPods=4, currentHealthy=4, desiredHealthy=2, disruptionsAllowed=2.
//
// The variant that uses an arbitrary CRD with a scale subresource is marked
// `#[ignore]` until rusternetes grows that surface.
#[tokio::test]
async fn test_pdb_with_scale_subresource() {
    let storage = new_storage();
    let controller = new_controller(storage.clone());

    let ns = "pdb-scale-subresource";
    let labels = HashMap::from([("app".to_string(), "test-crd".to_string())]);
    let replicas: i32 = 4;
    let max_unavailable: i32 = 2;

    let pdb = PodDisruptionBudget::new(
        "test-pdb",
        ns,
        PodDisruptionBudgetSpec {
            min_available: None,
            max_unavailable: Some(IntOrString::Int(max_unavailable)),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
    );
    put_pdb(&storage, &pdb).await;

    for i in 0..replicas {
        let pod = create_test_pod(&format!("pod-{}", i), ns, labels.clone());
        put_pod(&storage, &pod).await;
    }

    controller.reconcile_all().await.expect("reconcile_all");

    let updated = get_pdb(&storage, ns, "test-pdb").await;
    let status = updated.status.expect("status");
    assert_pdb_status(
        &status,
        replicas,
        replicas,
        replicas - max_unavailable,
        max_unavailable,
    );
}

/// Mirror of upstream `TestPDBWithScaleSubresource` CRD variant
/// (`test/integration/disruption/disruption_test.go`).
///
/// The PDB controller must walk every selected pod's controller
/// ownerReference up to the CRD root, find the CRD's `scale` subresource
/// definition, and read `expectedPods` from the JSON path declared by
/// `subresources.scale.specReplicasPath`. Pods alone undercount the
/// workload when the CRD's spec.replicas is greater than the number of
/// currently-running pods.
#[tokio::test]
async fn test_pdb_with_scale_subresource_crd_variant() {
    let storage = new_storage();
    let controller = new_controller(storage.clone());

    let ns = "pdb-scale-subresource-crd";
    let labels = HashMap::from([("app".to_string(), "test-crd".to_string())]);
    // CRD declares spec.replicas = 4 — this is the canonical "expected" count.
    // Only 3 pods are currently scheduled (e.g. mid-rollout); the controller
    // must still report expectedPods=4 from the scale subresource.
    let crd_replicas: i32 = 4;
    let live_pods: i32 = 3;
    let max_unavailable: i32 = 1;

    // 1. Define the CRD with a /scale subresource at .spec.replicas.
    let crd = CustomResourceDefinition {
        api_version: "apiextensions.k8s.io/v1".to_string(),
        kind: "CustomResourceDefinition".to_string(),
        metadata: ObjectMeta::new("scalecrds.example.com"),
        spec: rusternetes_common::resources::CustomResourceDefinitionSpec {
            group: "example.com".to_string(),
            names: rusternetes_common::resources::CustomResourceDefinitionNames {
                plural: "scalecrds".to_string(),
                singular: Some("scalecrd".to_string()),
                kind: "ScaleCRD".to_string(),
                short_names: None,
                categories: None,
                list_kind: Some("ScaleCRDList".to_string()),
            },
            scope: rusternetes_common::resources::ResourceScope::Namespaced,
            versions: vec![CustomResourceDefinitionVersion {
                name: "v1".to_string(),
                served: true,
                storage: true,
                deprecated: None,
                deprecation_warning: None,
                schema: None,
                subresources: Some(CustomResourceSubresources {
                    status: None,
                    scale: Some(CustomResourceSubresourceScale {
                        spec_replicas_path: ".spec.replicas".to_string(),
                        status_replicas_path: ".status.replicas".to_string(),
                        label_selector_path: None,
                    }),
                }),
                additional_printer_columns: None,
                selectable_fields: None,
            }],
            conversion: None,
            preserve_unknown_fields: None,
        },
        status: None,
    };
    let crd_key = build_key("customresourcedefinitions", None, &crd.metadata.name);
    storage.create(&crd_key, &crd).await.expect("create crd");

    // 2. Define a CR with spec.replicas == crd_replicas.
    let cr_uid = uuid::Uuid::new_v4().to_string();
    let cr_name = "my-scalecrd";
    let cr = CustomResource {
        api_version: "example.com/v1".to_string(),
        kind: "ScaleCRD".to_string(),
        metadata: ObjectMeta {
            name: cr_name.to_string(),
            namespace: Some(ns.to_string()),
            uid: cr_uid.clone(),
            generation: Some(1),
            creation_timestamp: Some(chrono::Utc::now()),
            ..Default::default()
        },
        spec: Some(serde_json::json!({ "replicas": crd_replicas })),
        status: None,
        extra: HashMap::new(),
    };
    // Storage key matches the api-server convention: `<group>_<plural>`
    let cr_key = build_key("example_com_scalecrds", Some(ns), cr_name);
    storage.create(&cr_key, &cr).await.expect("create cr");

    // 3. Create `live_pods` pods, each pointing back at the CR via a controller
    //    ownerReference (the canonical CRD adoption shape).
    for i in 0..live_pods {
        let mut pod = create_test_pod(&format!("pod-{}", i), ns, labels.clone());
        pod.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "example.com/v1".to_string(),
            kind: "ScaleCRD".to_string(),
            name: cr_name.to_string(),
            uid: cr_uid.clone(),
            controller: Some(true),
            block_owner_deletion: None,
        }]);
        put_pod(&storage, &pod).await;
    }

    // 4. Define a PDB selecting the same labels.
    let pdb = PodDisruptionBudget::new(
        "test-pdb",
        ns,
        PodDisruptionBudgetSpec {
            min_available: None,
            max_unavailable: Some(IntOrString::Int(max_unavailable)),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
    );
    put_pdb(&storage, &pdb).await;

    // 5. Reconcile and assert: expectedPods MUST equal crd.spec.replicas (4),
    //    NOT the count of live pods (3). currentHealthy is still 3 (the
    //    pods that actually exist + are Ready). desiredHealthy is
    //    expectedPods - maxUnavailable = 3. disruptionsAllowed is
    //    currentHealthy - desiredHealthy = 0.
    controller.reconcile_all().await.expect("reconcile_all");

    let updated = get_pdb(&storage, ns, "test-pdb").await;
    let status = updated.status.expect("status");
    assert_pdb_status(
        &status,
        crd_replicas,
        live_pods,
        crd_replicas - max_unavailable,
        live_pods - (crd_replicas - max_unavailable),
    );
}

// ---------------------------------------------------------------------------
// TestEmptySelector
// ---------------------------------------------------------------------------
//
// Upstream runs two sub-cases:
//   - policy/v1beta1 with an empty selector: should NOT target any pods
//     (currentHealthy = 0).
//   - policy/v1   with an empty selector: SHOULD target every pod in the
//     namespace (currentHealthy = 4).
//
// rusternetes only ships policy/v1 PDB types; the v1beta1 case is pinned as
// `#[ignore]` and the v1 case is exercised directly.
#[tokio::test]
async fn test_empty_selector_v1_targets_all_pods() {
    let storage = new_storage();
    let controller = new_controller(storage.clone());

    let ns = "pdb-empty-selector-v1";
    let replicas: i32 = 4;
    let min_available: i32 = 2;
    let labels = HashMap::from([("app".to_string(), "test-crd".to_string())]);

    for j in 0..replicas {
        let pod = create_test_pod(&format!("pod-{}", j), ns, labels.clone());
        put_pod(&storage, &pod).await;
    }

    let pdb = PodDisruptionBudget::new(
        "test-pdb",
        ns,
        PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            max_unavailable: None,
            selector: LabelSelector {
                // empty selector -> matches all pods in the namespace per upstream v1 semantics
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
    );
    put_pdb(&storage, &pdb).await;

    controller.reconcile_all().await.expect("reconcile_all");

    let updated = get_pdb(&storage, ns, "test-pdb").await;
    let status = updated.status.expect("status");
    // policy/v1: empty selector matches all 4 pods.
    assert_eq!(
        status.current_healthy, replicas,
        "policy/v1 empty selector must target every pod in the namespace"
    );
    assert_eq!(status.expected_pods, replicas);
}

#[tokio::test]
async fn test_empty_selector_v1beta1_targets_no_pods() {
    // Mirror upstream `TestEmptySelector` v1beta1 case: a `policy/v1beta1`
    // PDB with an empty selector must match NO pods (currentHealthy == 0).
    // This is the opposite of the v1 contract — upstream preserved it as a
    // compat shim because v1beta1 was created directly in etcd by tooling
    // that relied on the empty-selector-means-nothing semantics.
    let storage = new_storage();
    let controller = new_controller(storage.clone());

    let ns = "pdb-empty-selector-v1beta1";
    let replicas: i32 = 4;
    let labels = HashMap::from([("app".to_string(), "test-crd".to_string())]);

    for j in 0..replicas {
        let pod = create_test_pod(&format!("pod-{}", j), ns, labels.clone());
        put_pod(&storage, &pod).await;
    }

    let mut pdb = PodDisruptionBudget::new(
        "test-pdb",
        ns,
        PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(2)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
    );
    pdb.type_meta.api_version = "policy/v1beta1".to_string();
    put_pdb(&storage, &pdb).await;

    controller.reconcile_all().await.expect("reconcile_all");

    let updated = get_pdb(&storage, ns, "test-pdb").await;
    let status = updated.status.expect("status");
    assert_eq!(
        status.current_healthy, 0,
        "policy/v1beta1 empty selector must target no pods (compat shim)"
    );
    assert_eq!(
        status.expected_pods, 0,
        "policy/v1beta1 empty selector must have expectedPods=0"
    );
}

// ---------------------------------------------------------------------------
// TestSelectorsForPodsWithoutLabels
// ---------------------------------------------------------------------------
//
// Upstream exercises three sub-cases — all of them verify that a pod *without
// any labels* is still selectable by a PDB:
//   1. v1 PDB with an empty selector       → currentHealthy = 1
//   2. v1 PDB with DoesNotExist expression → currentHealthy = 1
//   3. v1beta1 PDB with DoesNotExist expr  → currentHealthy = 1
//
// rusternetes' current selector matcher rejects pods that have no labels
// (`pod.metadata.labels == None`). The first sub-case is the canonical pin —
// it MUST be observable. The other two require selector parity and v1beta1
// compat, both of which are pinned as `#[ignore]` for now.
#[tokio::test]
async fn test_selectors_for_pods_without_labels_empty_selector_v1() {
    let storage = new_storage();
    let controller = new_controller(storage.clone());

    let ns = "pdb-selectors-empty-v1";
    let min_available: i32 = 1;

    // Create the PDB first (mirrors the upstream order).
    let pdb = PodDisruptionBudget::new(
        "test-pdb",
        ns,
        PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
    );
    put_pdb(&storage, &pdb).await;

    // Create a single pod with NO labels.
    let pod = create_test_pod("pod", ns, HashMap::new());
    put_pod(&storage, &pod).await;

    controller.reconcile_all().await.expect("reconcile_all");

    let updated = get_pdb(&storage, ns, "test-pdb").await;
    let status = updated.status.expect("status");
    assert_eq!(
        status.current_healthy, 1,
        "v1 PDB with empty selector must still pick up label-less pods"
    );
    assert_eq!(status.expected_pods, 1);
}

#[tokio::test]
async fn test_selectors_for_pods_without_labels_does_not_exist_v1() {
    // Mirrors upstream `TestSelectorsForPodsWithoutLabels` DoesNotExist case:
    // a `policy/v1` PDB whose selector is `{Key: "DoesNotExist", Operator:
    // DoesNotExist}` must pick up label-less pods (the key is absent → matches).
    let storage = new_storage();
    let controller = new_controller(storage.clone());

    let ns = "pdb-selectors-doesnotexist-v1";
    let min_available: i32 = 1;

    let pdb = PodDisruptionBudget::new(
        "test-pdb",
        ns,
        PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: None,
                match_expressions: Some(vec![LabelSelectorRequirement {
                    key: "DoesNotExist".to_string(),
                    operator: "DoesNotExist".to_string(),
                    values: None,
                }]),
            },
            unhealthy_pod_eviction_policy: None,
        },
    );
    put_pdb(&storage, &pdb).await;

    let pod = create_test_pod("pod", ns, HashMap::new());
    put_pod(&storage, &pod).await;

    controller.reconcile_all().await.expect("reconcile_all");

    let updated = get_pdb(&storage, ns, "test-pdb").await;
    let status = updated.status.expect("status");
    assert_eq!(
        status.current_healthy, 1,
        "v1 PDB with `DoesNotExist` matchExpression must select label-less pods"
    );
    assert_eq!(status.expected_pods, 1);
}

#[tokio::test]
async fn test_selectors_for_pods_without_labels_does_not_exist_v1beta1() {
    // Mirror upstream `TestSelectorsForPodsWithoutLabels` v1beta1 case: a
    // `policy/v1beta1` PDB with `{Key: "DoesNotExist", Operator: DoesNotExist}`
    // must pick up label-less pods. The non-empty selector means the v1/v1beta1
    // empty-selector divergence doesn't apply here — DoesNotExist matches the
    // same way in both versions.
    let storage = new_storage();
    let controller = new_controller(storage.clone());

    let ns = "pdb-selectors-doesnotexist-v1beta1";

    let mut pdb = PodDisruptionBudget::new(
        "test-pdb",
        ns,
        PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: None,
                match_expressions: Some(vec![LabelSelectorRequirement {
                    key: "DoesNotExist".to_string(),
                    operator: "DoesNotExist".to_string(),
                    values: None,
                }]),
            },
            unhealthy_pod_eviction_policy: None,
        },
    );
    pdb.type_meta.api_version = "policy/v1beta1".to_string();
    put_pdb(&storage, &pdb).await;

    let pod = create_test_pod("pod", ns, HashMap::new());
    put_pod(&storage, &pod).await;

    controller.reconcile_all().await.expect("reconcile_all");

    let updated = get_pdb(&storage, ns, "test-pdb").await;
    let status = updated.status.expect("status");
    assert_eq!(
        status.current_healthy, 1,
        "v1beta1 PDB with `DoesNotExist` matchExpression must select label-less pods"
    );
    assert_eq!(status.expected_pods, 1);
}

// ---------------------------------------------------------------------------
// TestPatchCompatibility
// ---------------------------------------------------------------------------
//
// Upstream patches a PDB's selector via three different patch types
// (strategic-merge, JSON-merge, server-side apply) and asserts that the
// resulting selector matches the expected (atomic-replace vs merge) outcome.
//
// The three patch verbs themselves are already implemented in
// `crates/api-server/src/patch.rs` and the PDB patch handler is wired at
// `handlers/poddisruptionbudget.rs`, but exercising them needs a
// router-driven test harness (`tower::ServiceExt::oneshot`) which doesn't
// belong in this controller-direct file. The three router-driven test
// bodies are tracked as a follow-up to land in
// `crates/api-server/tests/`, while the round-trip pin below stays here
// to lock down the `LabelSelector` wire shape on storage.
#[tokio::test]
async fn test_patch_compatibility_selector_round_trip() {
    let storage = new_storage();

    let ns = "default";
    let pdb = PodDisruptionBudget::new(
        "test-pdb",
        ns,
        PodDisruptionBudgetSpec {
            min_available: None,
            max_unavailable: Some(IntOrString::Int(2)),
            selector: LabelSelector {
                match_labels: Some(HashMap::from([(
                    "basematch".to_string(),
                    "true".to_string(),
                )])),
                match_expressions: Some(vec![
                    rusternetes_common::types::LabelSelectorRequirement {
                        key: "baseexpression".to_string(),
                        operator: "In".to_string(),
                        values: Some(vec!["true".to_string()]),
                    },
                ]),
            },
            unhealthy_pod_eviction_policy: None,
        },
    );
    put_pdb(&storage, &pdb).await;

    let fetched = get_pdb(&storage, ns, "test-pdb").await;
    let selector = fetched.spec.selector;
    let labels = selector
        .match_labels
        .as_ref()
        .expect("match_labels round-trip");
    assert_eq!(
        labels.get("basematch").map(String::as_str),
        Some("true"),
        "matchLabels must round-trip through storage"
    );
    let exprs = selector
        .match_expressions
        .as_ref()
        .expect("match_expressions round-trip");
    assert_eq!(exprs.len(), 1);
    assert_eq!(exprs[0].key, "baseexpression");
    assert_eq!(exprs[0].operator, "In");
}

// The three router-driven patch-verb tests
// (test_patch_compatibility_v1_strategic_merge / _merge_patch / _apply_patch)
// live in `crates/api-server/tests/pdb_patch_compatibility_test.rs` — they
// need the in-process Axum router + `tower::ServiceExt::oneshot` harness to
// exercise real Content-Type dispatch in the PATCH handler.

// ---------------------------------------------------------------------------
// TestStalePodDisruption
// ---------------------------------------------------------------------------
//
// Upstream verifies that the disruption controller flips a pod's
// `DisruptionTarget` condition from `True` → `False` after
// `stalePodDisruptionTimeout` elapses, unless the pod is already terminating
// (`DeletionTimestamp != nil`) or failed. Implemented in this PR via
// `StalePodDisruptionController`; the four upstream sub-cases are pinned
// below alongside the LabelSelector round-trip test that locks down the
// `DisruptionTarget` wire format.
#[tokio::test]
async fn test_stale_pod_disruption_condition_round_trip() {
    let storage = new_storage();

    let ns = "pdb-stale-pod-disruption";
    let mut pod = create_test_pod("disruption-target", ns, HashMap::new());
    if let Some(ref mut status) = pod.status {
        // Mirror upstream: set DisruptionTarget=True on the pod.
        let mut conds = status.conditions.clone().unwrap_or_default();
        conds.push(PodCondition {
            condition_type: "DisruptionTarget".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(chrono::Utc::now()),
            observed_generation: Some(1),
        });
        status.conditions = Some(conds);
    }
    put_pod(&storage, &pod).await;

    let key = build_key("pods", Some(ns), &pod.metadata.name);
    let fetched: Pod = storage.get(&key).await.expect("get pod");
    let conditions = fetched
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("conditions");
    assert!(
        conditions
            .iter()
            .any(|c| c.condition_type == "DisruptionTarget" && c.status == "True"),
        "DisruptionTarget condition must round-trip through storage"
    );
}

/// Build a pod carrying `DisruptionTarget=True` with the condition's
/// `lastTransitionTime` set to `transition_at`. `phase` lets the caller
/// pick Running / Failed; `reason` is the per-case DisruptionTarget reason.
fn make_pod_with_disruption_target(
    name: &str,
    namespace: &str,
    phase: Phase,
    reason: Option<&str>,
    transition_at: chrono::DateTime<chrono::Utc>,
) -> Pod {
    let mut pod = create_test_pod(name, namespace, HashMap::new());
    let status = pod.status.as_mut().expect("pod has status");
    status.phase = Some(phase);
    let mut conditions = status.conditions.clone().unwrap_or_default();
    conditions.push(PodCondition {
        condition_type: "DisruptionTarget".to_string(),
        status: "True".to_string(),
        reason: reason.map(|s| s.to_string()),
        message: None,
        last_probe_time: None,
        last_transition_time: Some(transition_at),
        observed_generation: Some(1),
    });
    status.conditions = Some(conditions);
    pod
}

/// Build a [`StalePodDisruptionController`] with a short timeout so tests
/// can exercise stale-condition behaviour without waiting 120s.
fn new_stale_controller(
    storage: Arc<MemoryStorage>,
) -> StalePodDisruptionController<MemoryStorage> {
    StalePodDisruptionController::with_timeout(storage, std::time::Duration::from_millis(50))
}

#[tokio::test]
async fn test_stale_pod_disruption_stale_condition_on_running_pod() {
    // Upstream `TestStalePodDisruption` case "stale-condition": a Running pod
    // whose DisruptionTarget=True condition has aged past the timeout must be
    // flipped to DisruptionTarget=False by the stale-pod-disruption
    // sub-controller.
    let storage = new_storage();
    let stale_ctrl = new_stale_controller(storage.clone());

    let ns = "pdb-stale-running";
    let stale_when = chrono::Utc::now() - chrono::Duration::seconds(30);
    let pod = make_pod_with_disruption_target("running-pod", ns, Phase::Running, None, stale_when);
    put_pod(&storage, &pod).await;

    // Ensure the configured 50ms timeout has actually elapsed in wall-clock.
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    stale_ctrl.reconcile_all().await.expect("reconcile_all");

    let key = build_key("pods", Some(ns), "running-pod");
    let fetched: Pod = storage.get(&key).await.expect("get pod");
    let dt = fetched
        .status
        .and_then(|s| s.conditions)
        .and_then(|cs| {
            cs.into_iter()
                .find(|c| c.condition_type == "DisruptionTarget")
        })
        .expect("DisruptionTarget condition must still exist");
    assert_eq!(
        dt.status, "False",
        "stale DisruptionTarget on a Running pod must be flipped to False"
    );
}

#[tokio::test]
async fn test_stale_pod_disruption_deleted_pod_keeps_condition() {
    // Upstream `TestStalePodDisruption` case "deleted-pod": a pod with
    // `deletionTimestamp` set is terminating; the sub-controller must NOT
    // touch its DisruptionTarget condition even when stale.
    let storage = new_storage();
    let stale_ctrl = new_stale_controller(storage.clone());

    let ns = "pdb-stale-deleted";
    let stale_when = chrono::Utc::now() - chrono::Duration::seconds(30);
    let mut pod =
        make_pod_with_disruption_target("deleted-pod", ns, Phase::Running, None, stale_when);
    pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
    put_pod(&storage, &pod).await;

    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    stale_ctrl.reconcile_all().await.expect("reconcile_all");

    let key = build_key("pods", Some(ns), "deleted-pod");
    let fetched: Pod = storage.get(&key).await.expect("get pod");
    let dt = fetched
        .status
        .and_then(|s| s.conditions)
        .and_then(|cs| {
            cs.into_iter()
                .find(|c| c.condition_type == "DisruptionTarget")
        })
        .expect("DisruptionTarget condition must still exist");
    assert_eq!(
        dt.status, "True",
        "deleted (terminating) pod must keep DisruptionTarget=True"
    );
}

#[tokio::test]
async fn test_stale_pod_disruption_failed_pod_termination_by_kubelet() {
    // Upstream `TestStalePodDisruption` case "disruption-condition-by-kubelet":
    // a Failed pod whose DisruptionTarget=True carries `reason=TerminationByKubelet`
    // must keep the condition AND the reason intact — kubelet truly disrupted
    // it and the audit reason must survive.
    let storage = new_storage();
    let stale_ctrl = new_stale_controller(storage.clone());

    let ns = "pdb-stale-failed-kubelet";
    let stale_when = chrono::Utc::now() - chrono::Duration::seconds(30);
    let pod = make_pod_with_disruption_target(
        "failed-kubelet-pod",
        ns,
        Phase::Failed,
        Some("TerminationByKubelet"),
        stale_when,
    );
    put_pod(&storage, &pod).await;

    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    stale_ctrl.reconcile_all().await.expect("reconcile_all");

    let key = build_key("pods", Some(ns), "failed-kubelet-pod");
    let fetched: Pod = storage.get(&key).await.expect("get pod");
    let dt = fetched
        .status
        .and_then(|s| s.conditions)
        .and_then(|cs| {
            cs.into_iter()
                .find(|c| c.condition_type == "DisruptionTarget")
        })
        .expect("DisruptionTarget condition must still exist");
    assert_eq!(
        dt.status, "True",
        "Failed pod (TerminationByKubelet) must keep DisruptionTarget=True"
    );
    assert_eq!(
        dt.reason.as_deref(),
        Some("TerminationByKubelet"),
        "reason must be preserved verbatim"
    );
}

#[tokio::test]
async fn test_stale_pod_disruption_failed_pod_generic() {
    // Upstream `TestStalePodDisruption` case "disruption-condition-on-failed-pod":
    // a Failed pod must keep DisruptionTarget=True regardless of reason.
    let storage = new_storage();
    let stale_ctrl = new_stale_controller(storage.clone());

    let ns = "pdb-stale-failed-generic";
    let stale_when = chrono::Utc::now() - chrono::Duration::seconds(30);
    let pod =
        make_pod_with_disruption_target("failed-generic-pod", ns, Phase::Failed, None, stale_when);
    put_pod(&storage, &pod).await;

    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    stale_ctrl.reconcile_all().await.expect("reconcile_all");

    let key = build_key("pods", Some(ns), "failed-generic-pod");
    let fetched: Pod = storage.get(&key).await.expect("get pod");
    let dt = fetched
        .status
        .and_then(|s| s.conditions)
        .and_then(|cs| {
            cs.into_iter()
                .find(|c| c.condition_type == "DisruptionTarget")
        })
        .expect("DisruptionTarget condition must still exist");
    assert_eq!(
        dt.status, "True",
        "Failed pod must keep DisruptionTarget=True regardless of reason"
    );
}
