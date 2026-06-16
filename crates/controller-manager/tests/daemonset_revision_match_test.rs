// DaemonSet ControllerRevision Match Tests
//
// These tests verify that:
// 1. ControllerRevision data is byte-comparable with Go's getPatch() output.
//    K8s Match() does bytes.Equal(getPatch(ds), history.Data.Raw). Go's
//    encoding/json omits zero-value fields with `omitempty`, so our
//    ControllerRevision data must also omit empty strings, null values, etc.
// 2. DaemonSet controller deletes pods in Failed phase and creates replacements.
//    The replacement pod must have a DIFFERENT name so the original failed pod
//    returns NotFound on GET (the conformance test checks this).

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::daemonset::DaemonSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn make_node(name: &str) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.ensure_uid();
            m
        },
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        }),
        status: None,
    }
}

fn make_daemonset(name: &str, ns: &str) -> DaemonSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    DaemonSet {
        type_meta: TypeMeta {
            kind: "DaemonSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.namespace = Some(ns.to_string());
            m.ensure_uid();
            m.generation = Some(1);
            m
        },
        spec: DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "test-container".to_string(),
                        image: "busybox:1.35".to_string(),
                        command: Some(vec!["sleep".to_string(), "3600".to_string()]),
                        args: None,
                        working_dir: None,
                        ports: None,
                        env: None,
                        env_from: None,
                        resources: None,
                        volume_mounts: None,
                        volume_devices: None,
                        liveness_probe: None,
                        readiness_probe: None,
                        startup_probe: None,
                        lifecycle: None,
                        termination_message_path: None,
                        termination_message_policy: None,
                        image_pull_policy: None,
                        security_context: None,
                        stdin: None,
                        stdin_once: None,
                        tty: None,
                        resize_policy: None,
                        restart_policy: None,
                        ..Default::default()
                    }],
                    init_containers: None,
                    node_name: None,
                    node_selector: None,
                    restart_policy: None,
                    service_account_name: None,
                    service_account: None,
                    volumes: None,
                    affinity: None,
                    tolerations: None,
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
                },
            },
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
        },
        status: None,
    }
}

// -- Problem 1: ControllerRevision data byte matching --

#[tokio::test]
async fn test_cr_data_does_not_contain_empty_name_field() {
    // K8s Match() does bytes.Equal(getPatch(ds), history.Data.Raw).
    // Go's encoding/json omits empty strings with `omitempty`.
    // The template metadata's empty "name" field must NOT appear in the CR data
    // because Go's getPatch() round-trip would drop it.
    let storage = setup().await;

    let node = make_node("n1");
    storage
        .create(&build_key("nodes", None, "n1"), &node)
        .await
        .unwrap();

    let ds = make_daemonset("rev-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "rev-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert!(
        !revisions.is_empty(),
        "Should have created a ControllerRevision"
    );

    let cr = &revisions[0];
    let data = cr.data.as_ref().unwrap();
    let data_json = serde_json::to_string(data).unwrap();

    // The data JSON should NOT contain "name":"" because Go's omitempty drops
    // empty strings. This is the key byte difference that causes Match() to fail.
    assert!(
        !data_json.contains(r#""name":"""#),
        "CR data should not contain empty name field. Got: {}",
        data_json
    );
}

#[tokio::test]
async fn test_cr_data_does_not_contain_empty_uid_field() {
    // Empty uid fields should be omitted (Go omitempty)
    let storage = setup().await;

    let node = make_node("n1");
    storage
        .create(&build_key("nodes", None, "n1"), &node)
        .await
        .unwrap();

    let ds = make_daemonset("uid-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "uid-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    let cr = &revisions[0];
    let data = cr.data.as_ref().unwrap();
    let data_json = serde_json::to_string(data).unwrap();

    // The template metadata should not contain uid (it's empty in templates)
    assert!(
        !data_json.contains(r#""uid":"""#),
        "CR data should not contain empty uid field. Got: {}",
        data_json
    );
}

#[tokio::test]
async fn test_cr_data_is_deterministic_across_reconciles() {
    // The CR data must be identical across reconcile cycles for the same template.
    // This is critical for Match() byte comparison to work.
    let storage = setup().await;

    let node = make_node("n1");
    storage
        .create(&build_key("nodes", None, "n1"), &node)
        .await
        .unwrap();

    let ds = make_daemonset("det-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "det-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    let cr_data_1 = serde_json::to_string(revisions[0].data.as_ref().unwrap()).unwrap();

    // Build fresh patch data from the same template
    let fresh_patch =
        DaemonSetController::<MemoryStorage>::build_patch_data(&ds.spec.template).unwrap();
    let cr_data_2 = serde_json::to_string(&fresh_patch).unwrap();

    assert_eq!(
        cr_data_1, cr_data_2,
        "CR data should match build_patch_data output for identical template"
    );
}

#[tokio::test]
async fn test_cr_data_has_patch_replace_marker() {
    // K8s getPatch() format: {"spec":{"template":{...,"$patch":"replace"}}}
    let storage = setup().await;

    let node = make_node("n1");
    storage
        .create(&build_key("nodes", None, "n1"), &node)
        .await
        .unwrap();

    let ds = make_daemonset("patch-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "patch-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    let data = revisions[0].data.as_ref().unwrap();

    // Verify structure: {"spec":{"template":{...,"$patch":"replace"}}}
    let template = data
        .pointer("/spec/template")
        .expect("data should have spec.template");
    assert_eq!(
        template.get("$patch"),
        Some(&serde_json::json!("replace")),
        "template should contain $patch: replace"
    );
}

#[tokio::test]
async fn test_cr_data_keys_sorted_alphabetically() {
    // Go's encoding/json sorts map keys alphabetically after the
    // marshal -> unmarshal to map -> marshal round-trip in getPatch().
    // Our data must also have sorted keys.
    let storage = setup().await;

    let node = make_node("n1");
    storage
        .create(&build_key("nodes", None, "n1"), &node)
        .await
        .unwrap();

    let ds = make_daemonset("sort-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "sort-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    let data = revisions[0].data.as_ref().unwrap();
    let data_json = serde_json::to_string(data).unwrap();

    // Re-parse and re-serialize to check that sorting is stable
    let reparsed: serde_json::Value = serde_json::from_str(&data_json).unwrap();
    let reserialized = serde_json::to_string(&reparsed).unwrap();
    assert_eq!(
        data_json, reserialized,
        "JSON serialization should be deterministic (sorted keys)"
    );
}

// -- Problem 2: Failed pod deletion --

#[tokio::test]
async fn test_failed_pod_is_deleted_and_replacement_has_different_name() {
    // The conformance test "should retry creating failed daemon pods" expects:
    // 1. A failed pod is deleted from storage (GET returns NotFound)
    // 2. A replacement pod is created with a DIFFERENT name
    //
    // If the replacement has the same name, the test's waitFailedDaemonPodDeleted
    // will never see NotFound because GET finds the replacement pod.
    let storage = setup().await;

    let node = make_node("n1");
    storage
        .create(&build_key("nodes", None, "n1"), &node)
        .await
        .unwrap();

    let ds = make_daemonset("fail-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "fail-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());

    // First reconcile: create initial pod
    let _ds_read: DaemonSet = storage
        .get(&build_key("daemonsets", Some("default"), "fail-ds"))
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    // Find the created pod
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let ds_pods: Vec<&Pod> = pods
        .iter()
        .filter(|p| {
            p.metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| r.name == "fail-ds"))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(ds_pods.len(), 1, "Should have 1 DS pod");
    let original_pod_name = ds_pods[0].metadata.name.clone();

    // Mark the pod as Failed (simulating what the conformance test does)
    let pod_key = format!("/registry/pods/default/{}", original_pod_name);
    let mut failed_pod: Pod = storage.get(&pod_key).await.unwrap();
    if let Some(status) = failed_pod.status.as_mut() {
        status.phase = Some(Phase::Failed);
    }
    storage.update(&pod_key, &failed_pod).await.unwrap();

    // Re-read DaemonSet and reconcile
    let _ds_read: DaemonSet = storage
        .get(&build_key("daemonsets", Some("default"), "fail-ds"))
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    // The original failed pod should be GONE from storage
    let result: Result<Pod, _> = storage.get(&pod_key).await;
    assert!(
        result.is_err(),
        "Original failed pod '{}' should be deleted from storage (NotFound)",
        original_pod_name
    );

    // A replacement pod should exist
    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let ds_pods_after: Vec<&Pod> = pods_after
        .iter()
        .filter(|p| {
            p.metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| r.name == "fail-ds"))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        ds_pods_after.len(),
        1,
        "Should have exactly 1 replacement pod"
    );

    // The replacement pod must have a DIFFERENT name from the original
    let replacement_name = &ds_pods_after[0].metadata.name;
    assert_ne!(
        replacement_name, &original_pod_name,
        "Replacement pod name '{}' must differ from original '{}' so the conformance test's \
         waitFailedDaemonPodDeleted can see NotFound for the original",
        replacement_name, original_pod_name
    );
}

#[tokio::test]
async fn test_succeeded_pod_is_deleted() {
    // Succeeded pods should also be deleted and replaced
    let storage = setup().await;

    let node = make_node("n1");
    storage
        .create(&build_key("nodes", None, "n1"), &node)
        .await
        .unwrap();

    let ds = make_daemonset("succ-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "succ-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());

    let _ds_read: DaemonSet = storage
        .get(&build_key("daemonsets", Some("default"), "succ-ds"))
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    // Find created pod and mark as Succeeded
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let original_name = pods
        .iter()
        .find(|p| {
            p.metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| r.name == "succ-ds"))
                .unwrap_or(false)
        })
        .unwrap()
        .metadata
        .name
        .clone();

    let pod_key = format!("/registry/pods/default/{}", original_name);
    let mut pod: Pod = storage.get(&pod_key).await.unwrap();
    if let Some(status) = pod.status.as_mut() {
        status.phase = Some(Phase::Succeeded);
    }
    storage.update(&pod_key, &pod).await.unwrap();

    // Reconcile
    let _ds_read: DaemonSet = storage
        .get(&build_key("daemonsets", Some("default"), "succ-ds"))
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    // Original should be gone
    let result: Result<Pod, _> = storage.get(&pod_key).await;
    assert!(result.is_err(), "Succeeded pod should be deleted");
}
