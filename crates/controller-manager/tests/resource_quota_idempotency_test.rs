//! Regression guards against ResourceQuotaController hot-loops.
//!
//! During a v1.35 conformance run a pod was observed cycling through
//! resourceVersion 897500+. The primary cause was traced to kubelet
//! `sync_pod` writing terminal-pod status repeatedly (fixed in f0eea82).
//! These tests verify the ResourceQuota controller does not contribute
//! to similar storms when state is already consistent:
//!
//! 1. `reconcile_all` is byte-equal idempotent when nothing changes.
//! 2. `reconcile_all` never writes to the Pod object it inspects.
//! 3. The pods/services/etc. watch path (`enqueue_quotas_for_event`)
//!    does not feed back into itself: a single foreign-resource MODIFY
//!    must produce at most one quota re-enqueue per affected quota in
//!    a debounce window.

use rusternetes_common::resources::{
    Container, Pod, PodSpec, PodStatus, ResourceQuota, ResourceQuotaSpec,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage, WatchEvent};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn setup() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

/// Build a minimal pod in `ns-a` with the given phase. The pod carries no
/// resources so it is BestEffort QoS — quota calculations only count it
/// toward `pods` / `count/pods`.
fn make_pod(name: &str, namespace: &str, phase: Phase) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(phase),
            ..Default::default()
        }),
    }
}

/// ResourceQuota in `ns-a` with `hard: { pods: 10 }`. We pre-seed
/// `status.used = { pods: "0" }` so the controller's idempotency
/// guard (`if quota.status != new_status`) has a chance to fire.
fn make_quota(namespace: &str) -> ResourceQuota {
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "10".to_string());
    let mut used = HashMap::new();
    used.insert("pods".to_string(), "0".to_string());
    used.insert("count/pods".to_string(), "0".to_string());

    let mut q = ResourceQuota::new(
        "quota-a",
        namespace,
        ResourceQuotaSpec {
            hard: Some(hard.clone()),
            scopes: None,
            scope_selector: None,
        },
    );
    q.status = Some(rusternetes_common::resources::ResourceQuotaStatus {
        hard: Some(hard),
        used: Some(used),
    });
    q
}

/// Test 1: reconcile_all is byte-equal idempotent on unchanged state.
///
/// Seeds one ResourceQuota with status that already matches the desired
/// status (1 Succeeded pod is excluded from the count so used.pods stays
/// at "0"). Captures the stored JSON before, between, and after two
/// `reconcile_all` calls. All three captures MUST be byte-equal — any
/// difference indicates a spurious write and a hot-loop risk.
#[tokio::test]
async fn test_quota_reconcile_is_idempotent_on_unchanged_state() {
    let storage = setup();
    let controller = ResourceQuotaController::new(storage.clone());

    // Seed the namespace with one Succeeded pod. Succeeded pods are
    // skipped by `calculate_usage`, so the steady-state usage of
    // `pods` is "0" — matching the pre-seeded status.
    let pod = make_pod("pod-done", "ns-a", Phase::Succeeded);
    let pod_key = build_key("pods", Some("ns-a"), "pod-done");
    storage.create(&pod_key, &pod).await.unwrap();

    let quota = make_quota("ns-a");
    let quota_key = build_key("resourcequotas", Some("ns-a"), "quota-a");
    storage.create(&quota_key, &quota).await.unwrap();

    // First reconcile establishes status.used. Capture the post-state as
    // serde_json::Value so PartialEq is order-independent (HashMap iteration
    // order is non-deterministic across re-reads even when storage content is
    // unchanged).
    controller.reconcile_all().await.unwrap();
    let after_first: serde_json::Value = serde_json::to_value(
        storage
            .get::<ResourceQuota>(&quota_key)
            .await
            .expect("quota present after first reconcile"),
    )
    .unwrap();

    controller.reconcile_all().await.unwrap();
    let after_second: serde_json::Value = serde_json::to_value(
        storage
            .get::<ResourceQuota>(&quota_key)
            .await
            .expect("quota present after second reconcile"),
    )
    .unwrap();

    controller.reconcile_all().await.unwrap();
    let after_third: serde_json::Value = serde_json::to_value(
        storage
            .get::<ResourceQuota>(&quota_key)
            .await
            .expect("quota present after third reconcile"),
    )
    .unwrap();

    assert_eq!(
        after_first, after_second,
        "second reconcile_all mutated the ResourceQuota — hot-loop risk"
    );
    assert_eq!(
        after_second, after_third,
        "third reconcile_all mutated the ResourceQuota — hot-loop risk"
    );
}

/// Test 2: reconcile_all must never write to the Pod objects it inspects.
///
/// The quota controller only reads pods to compute usage. A regression
/// that touches pod state (even just round-tripping) would inflate
/// pod resourceVersion every reconcile, exactly the symptom seen at
/// resourceVersion 897500+.
#[tokio::test]
async fn test_quota_does_not_write_pod() {
    let storage = setup();
    let controller = ResourceQuotaController::new(storage.clone());

    let pod = make_pod("pod-done", "ns-a", Phase::Succeeded);
    let pod_key = build_key("pods", Some("ns-a"), "pod-done");
    storage.create(&pod_key, &pod).await.unwrap();
    let seeded_pod_json: String = serde_json::to_string(
        &storage
            .get::<Pod>(&pod_key)
            .await
            .expect("pod present after seed"),
    )
    .unwrap();

    let quota = make_quota("ns-a");
    let quota_key = build_key("resourcequotas", Some("ns-a"), "quota-a");
    storage.create(&quota_key, &quota).await.unwrap();

    // Reconcile a few times — none of these may touch the Pod.
    for _ in 0..3 {
        controller.reconcile_all().await.unwrap();
    }

    let final_pod_json: String = serde_json::to_string(
        &storage
            .get::<Pod>(&pod_key)
            .await
            .expect("pod present after reconcile"),
    )
    .unwrap();

    assert_eq!(
        seeded_pod_json, final_pod_json,
        "ResourceQuotaController wrote to a Pod it only reads — hot-loop risk"
    );
}

/// Test 3: a single MODIFY of a watched non-quota resource (pod) must
/// not trigger a runaway cascade of ResourceQuota updates.
///
/// We spawn the controller's full `run()` loop (which at f0eea82+ watches
/// pods/services/configmaps/secrets/PVCs and re-enqueues quotas on each
/// event) and subscribe to `/registry/resourcequotas/`. We then trigger a
/// single pod MODIFY and count quota-side WatchEvents emitted during a
/// short debounce window.
///
/// The idempotency guard inside `reconcile_quota` (`if quota.status !=
/// new_status`) means: even if `enqueue_quotas_for_event` enqueues the
/// quota, the worker reads it, sees status already matches, and writes
/// nothing — so the quota's storage state never changes, which means no
/// quota WatchEvent is emitted from the controller's own activity. If
/// the quota emits more than one event per pod modify, the controller
/// is self-feeding and a hot-loop will form once the foreign resource is
/// updated for any reason.
#[tokio::test]
async fn test_enqueue_quotas_for_event_no_self_loop() {
    use futures::StreamExt;

    let storage = setup();

    // Seed a steady state: one Succeeded pod (excluded from quota
    // counts) and one quota whose status already matches reality.
    let pod = make_pod("pod-done", "ns-a", Phase::Succeeded);
    let pod_key = build_key("pods", Some("ns-a"), "pod-done");
    storage.create(&pod_key, &pod).await.unwrap();

    let quota = make_quota("ns-a");
    let quota_key = build_key("resourcequotas", Some("ns-a"), "quota-a");
    storage.create(&quota_key, &quota).await.unwrap();

    // First reconcile to converge the quota's status with what the
    // controller computes — after this, any subsequent reconcile of
    // the same state must be a no-op write.
    let bootstrap = ResourceQuotaController::new(storage.clone());
    bootstrap.reconcile_all().await.unwrap();

    // Subscribe to quota events BEFORE starting the controller so we
    // capture everything it emits.
    let mut quota_watch = storage
        .watch(&build_prefix("resourcequotas", None))
        .await
        .expect("watch resourcequotas");

    // Spawn the controller's full event loop. Abort on test exit.
    let ctrl = Arc::new(ResourceQuotaController::new(storage.clone()));
    let run_handle = {
        let ctrl = Arc::clone(&ctrl);
        tokio::spawn(async move {
            let _ = ctrl.run().await;
        })
    };

    // Give run() a moment to install its watches and finish the
    // initial enqueue_all → reconcile loop. The initial reconcile may
    // emit at most one quota write (only if the bootstrap status
    // computed slightly differently — defensively allow it).
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Drain any startup events the controller may have produced.
    let mut startup_events = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_millis(50), quota_watch.next()).await {
            Ok(Some(Ok(_ev))) => startup_events += 1,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break, // timeout — drained
        }
        if startup_events > 5 {
            // sanity: more than 5 startup events is already suspicious
            run_handle.abort();
            panic!(
                "controller emitted {} quota writes during idle startup — hot-loop on bootstrap",
                startup_events
            );
        }
    }

    // Trigger a single pod MODIFY. At f0eea82+ this fires
    // enqueue_quotas_for_event, which adds the quota to the work queue.
    // The worker then re-reconciles, sees the quota status already
    // matches reality, and (per the guard) writes nothing.
    let mut updated_pod: Pod = storage.get(&pod_key).await.unwrap();
    if let Some(meta) = updated_pod.metadata.labels.as_mut() {
        meta.insert("touched".to_string(), "1".to_string());
    } else {
        let mut m = HashMap::new();
        m.insert("touched".to_string(), "1".to_string());
        updated_pod.metadata.labels = Some(m);
    }
    storage.update(&pod_key, &updated_pod).await.unwrap();

    // Count quota events emitted during the debounce window.
    let debounce = Duration::from_millis(500);
    let deadline = tokio::time::Instant::now() + debounce;
    let mut quota_modifies = 0u32;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, quota_watch.next()).await {
            Ok(Some(Ok(ev))) => match ev {
                WatchEvent::Modified(_, _) | WatchEvent::Added(_, _) => {
                    quota_modifies += 1;
                    if quota_modifies > 1 {
                        // Bail early — already over budget.
                        break;
                    }
                }
                WatchEvent::Deleted(_, _) => {}
            },
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }

    run_handle.abort();

    assert!(
        quota_modifies <= 1,
        "single pod MODIFY produced {} quota writes — enqueue_quotas_for_event \
         is self-feeding (hot-loop). Each foreign-resource event must result in \
         at most one quota write because reconcile_quota's idempotency guard \
         should short-circuit subsequent reconciles of unchanged state.",
        quota_modifies
    );
}
