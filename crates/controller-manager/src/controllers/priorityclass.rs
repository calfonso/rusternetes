use anyhow::Result;
use rusternetes_common::resources::{Pod, PriorityClass};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::info;

/// Annotation key used on a Namespace to opt into a per-namespace default
/// PriorityClass. Pods in the namespace that omit `priorityClassName` inherit
/// the named class's `value` instead of the cluster-wide `globalDefault`.
///
/// This annotation key is consistent with how some Kubernetes distributions
/// and cluster operators express per-namespace priority defaults.
pub const NS_DEFAULT_PRIORITY_CLASS_ANNOTATION: &str = "scheduling.k8s.io/default-priority-class";

/// PriorityClassController watches `PriorityClass` resources cluster-wide and is
/// responsible for the lifecycle behaviors that surround scheduling priority:
///
/// * Resolving each pod's `priorityClassName` -> numeric `spec.priority` /
///   `spec.preemptionPolicy` (admission-time work).
/// * Tracking the cluster-wide `globalDefault: true` PriorityClass and applying
///   its value to pods that omit a `priorityClassName`.
/// * Surfacing namespace-scoped default priority (e.g. via a default
///   PriorityClass per-namespace annotation) so newly created pods inherit it.
///
/// Preemption and eviction are NOT this controller's job — that is the
/// scheduler's (`crates/scheduler/src/scheduler.rs::try_preempt`). This
/// controller only resolves `priorityClassName` -> `spec.priority` +
/// `spec.preemptionPolicy` as a defaulting backstop mirroring the upstream
/// `Priority` admission plugin.
///
/// # Upstream note
///
/// In upstream Kubernetes, pod priority injection (resolving `priorityClassName`
/// to a numeric `spec.priority`) is performed at **admission time** by the
/// `Priority` admission plugin
/// (`plugin/pkg/admission/priority/admission.go`). This project drives the same
/// behaviour through the controller layer because the test suite
/// (`crates/controller-manager/tests/priorityclass_controller_test.rs`) frames
/// it as a `PriorityClassController` responsibility. The semantics are identical;
/// only the trigger point differs.
///
/// Upstream reference: `kubernetes/test/e2e/scheduling/priorityclass.go`,
/// `kubernetes/pkg/controller/priorityclass`.
pub struct PriorityClassController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
}

impl<S: Storage + 'static> PriorityClassController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(30),
        }
    }

    /// Long-running reconciliation loop. Mirrors the shape of `pv_binder.rs` /
    /// `network_policy.rs` so future watch-based work can slot in without
    /// changing the call site.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;
        info!("Starting PriorityClass controller");

        loop {
            // Reconcile once, then block on PriorityClass writes instead of
            // busy-polling — idle CPU drops to ~0 between real changes. The
            // periodic resync is the wedge-safe fallback, matching the
            // informer+resync model the other controllers already use (#1040).
            if let Err(e) = self.reconcile_all().await {
                tracing::error!("PriorityClass reconcile failed: {}", e);
            }

            let mut watch = match self
                .storage
                .watch(&build_prefix("priorityclasses", None))
                .await
            {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!(
                        "PriorityClass watch failed: {e}; retrying in {:?}",
                        self.interval
                    );
                    time::sleep(self.interval).await;
                    continue;
                }
            };
            let mut resync = time::interval(self.interval);
            resync.tick().await; // drop the immediate first tick

            loop {
                tokio::select! {
                    ev = watch.next() => match ev {
                        Some(Ok(_)) => {
                            if let Err(e) = self.reconcile_all().await {
                                tracing::error!("PriorityClass reconcile failed: {}", e);
                            }
                        }
                        // Stream ended/errored → reconnect via the outer loop
                        // (which re-reconciles first).
                        _ => break,
                    },
                    _ = resync.tick() => {
                        if let Err(e) = self.reconcile_all().await {
                            tracing::error!("PriorityClass reconcile failed: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Reconcile every PriorityClass and Pod in the cluster.
    ///
    /// # Behaviours
    ///
    /// 1. **Multiple global defaults** — if more than one `PriorityClass` has
    ///    `globalDefault: true`, return `Err` immediately (upstream invariant:
    ///    at most one global default may exist).
    ///
    /// 2. **Explicit `priorityClassName`** — pods that already name a class
    ///    have `spec.priority` resolved to that class's `value`.
    ///
    /// 3. **Namespace default** — pods with no `priorityClassName` in a
    ///    namespace annotated with [`NS_DEFAULT_PRIORITY_CLASS_ANNOTATION`]
    ///    inherit the named class's `value`.
    ///
    /// 4. **Global default** — pods with no `priorityClassName` in namespaces
    ///    that carry no namespace-default annotation inherit the cluster-wide
    ///    `globalDefault` class's `value` (if one exists).
    ///
    /// 5. **preemptionPolicy propagation** — when a class resolves (named,
    ///    namespace default, or globalDefault), its `preemptionPolicy` is also
    ///    injected onto the pod, but only when the pod does not already carry an
    ///    explicit `spec.preemptionPolicy` (which is never overwritten).
    pub async fn reconcile_all(&self) -> Result<()> {
        // -----------------------------------------------------------------
        // 1. Load all PriorityClasses into a name → value map.
        // -----------------------------------------------------------------
        // Propagate storage errors (the run loop logs + retries) rather than
        // proceeding with a partial/empty view — acting on an empty map here
        // would wrongly treat every class as "absent" and could clear pod
        // priorities cluster-wide.
        let all_pcs: Vec<PriorityClass> = self
            .storage
            .list(&build_prefix("priorityclasses", None))
            .await?;

        // name → (value, preemptionPolicy) (order-independent).
        let pc_map: HashMap<String, (i32, Option<String>)> = all_pcs
            .iter()
            .map(|pc| {
                (
                    pc.metadata.name.clone(),
                    (pc.value, pc.preemption_policy.clone()),
                )
            })
            .collect();

        // -----------------------------------------------------------------
        // 2. Validate: at most one globalDefault.
        // -----------------------------------------------------------------
        let global_default_names: Vec<&str> = all_pcs
            .iter()
            .filter(|pc| pc.global_default == Some(true))
            .map(|pc| pc.metadata.name.as_str())
            .collect();

        if global_default_names.len() > 1 {
            return Err(anyhow::anyhow!(
                "invalid PriorityClass configuration: {} PriorityClasses have \
                 globalDefault=true ({}); at most one is allowed",
                global_default_names.len(),
                global_default_names.join(", ")
            ));
        }

        let global_default: Option<(i32, Option<String>)> = all_pcs
            .iter()
            .find(|pc| pc.global_default == Some(true))
            .map(|pc| (pc.value, pc.preemption_policy.clone()));

        // -----------------------------------------------------------------
        // 3. Load namespace annotations for per-namespace defaults.
        //
        //    Namespaces are stored as raw JSON values; we avoid a dependency
        //    on the concrete Namespace struct so this remains forward-compatible.
        //    Key: namespace name → resolved (priority value, preemptionPolicy)
        //    pair of the namespace's default PriorityClass.
        // -----------------------------------------------------------------
        let ns_default_map: HashMap<String, (i32, Option<String>)> = {
            let namespaces: Vec<serde_json::Value> =
                self.storage.list(&build_prefix("namespaces", None)).await?;

            namespaces
                .into_iter()
                .filter_map(|ns| {
                    let ns_name = ns
                        .pointer("/metadata/name")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)?;

                    // JSON Pointer escaping (RFC 6901): "~" → "~0" first, then
                    // "/" → "~1".
                    let annotation_ptr = format!(
                        "/metadata/annotations/{}",
                        NS_DEFAULT_PRIORITY_CLASS_ANNOTATION
                            .replace('~', "~0")
                            .replace('/', "~1")
                    );
                    let class_name = ns.pointer(&annotation_ptr).and_then(|v| v.as_str())?;

                    let resolved = pc_map.get(class_name)?.clone();
                    Some((ns_name, resolved))
                })
                .collect()
        };

        // -----------------------------------------------------------------
        // 4. Process all pods cluster-wide.
        //
        //    List with the top-level pods prefix so pods are found even when
        //    no Namespace object exists in storage (e.g. in tests where only
        //    the pod is created directly).
        // -----------------------------------------------------------------
        let all_pods: Vec<Pod> = self.storage.list(&build_prefix("pods", None)).await?;

        for mut pod in all_pods {
            let ns = pod
                .metadata
                .namespace
                .as_deref()
                .unwrap_or("default")
                .to_string();

            // A pod with no spec can't carry a priority — nothing to inject.
            let Some(spec_ref) = pod.spec.as_ref() else {
                continue;
            };

            // Resolve the (priority, preemptionPolicy) pair to inject. This
            // controller only ever *sets* values; it never clears one, so any
            // case where no class applies leaves the pod untouched (`continue`).
            let (desired_value, desired_policy): (i32, Option<String>) =
                match spec_ref.priority_class_name.as_deref() {
                    // Pod names a specific class — resolve to its value. If the class
                    // is absent (deleted, or a transient read gap), leave the pod's
                    // existing priority alone rather than zeroing it.
                    Some(class_name) => match pc_map.get(class_name) {
                        Some(resolved) => resolved.clone(),
                        None => continue,
                    },
                    // Pod omits priorityClassName — apply the effective default:
                    // namespace annotation wins over cluster globalDefault. No
                    // default configured ⇒ leave the pod untouched.
                    None => match ns_default_map
                        .get(&ns)
                        .cloned()
                        .or_else(|| global_default.clone())
                    {
                        Some(resolved) => resolved,
                        None => continue,
                    },
                };

            // Only inject the policy when the pod doesn't already carry one —
            // an explicit spec.preemptionPolicy (user-set or api-server
            // admission) must never be overwritten.
            let policy_to_set = if spec_ref.preemption_policy.is_none() {
                desired_policy
            } else {
                None
            };

            // Idempotent: only write when something actually changes.
            if spec_ref.priority == Some(desired_value) && policy_to_set.is_none() {
                continue;
            }
            if let Some(spec) = pod.spec.as_mut() {
                spec.priority = Some(desired_value);
                if let Some(policy) = policy_to_set {
                    spec.preemption_policy = Some(policy);
                }
            }
            let pod_key = build_key("pods", Some(&ns), &pod.metadata.name);
            if let Err(e) = self.storage.update(&pod_key, &pod).await {
                tracing::warn!(
                    "PriorityClass: failed to set priority for pod {}/{}: {}",
                    ns,
                    pod.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_reconcile_all_is_noop_when_empty() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PriorityClassController::new(storage);
        // Must succeed without doing anything when there is no data.
        controller.reconcile_all().await.unwrap();
    }
}
