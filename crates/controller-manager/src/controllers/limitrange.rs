use anyhow::Result;
use rusternetes_common::resources::LimitRange;
use rusternetes_storage::{build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

/// LimitRangeController watches LimitRange resources.
///
/// NOTE: In upstream Kubernetes, LimitRange enforcement (defaulting,
/// min/max checks, ratio constraints) lives in the `LimitRanger` admission
/// plugin — *not* in a background controller. Rusternetes already has a
/// `LimitRangerController` admission stub in
/// `crates/common/src/admission.rs` for that path.
///
/// This struct exists so that the controller-manager has a place to watch
/// LimitRange resources for future side-effects (metrics, status updates,
/// validation backfill on existing pods). Today it is a no-op: `run` watches
/// the LimitRange prefix and `reconcile_all` returns `Ok(())` without
/// touching anything. The RED-state tests in
/// `tests/limitrange_controller_test.rs` document the enforcement
/// behaviour that this controller does NOT yet provide.
pub struct LimitRangeController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> LimitRangeController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    // Allowed-dead-code: not yet wired into `controller_manager::run()`
    // because enforcement lives at admission today. The watch loop is
    // kept so wiring it later is a single-line change in `lib.rs`.
    #[allow(dead_code)]
    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting LimitRange controller (stub — enforcement is at admission)");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("limitranges", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch on limitranges: {}, retrying", e);
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("LimitRange watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("LimitRange watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            // Stub: nothing to do. Enforcement happens in the admission
            // plugin (see `LimitRangerController` in
            // `crates/common/src/admission.rs`). We just acknowledge the
            // item so the work queue drains cleanly.
            debug!("LimitRange controller observed {} (no-op)", key);
            queue.forget(&key).await;
            queue.done(&key).await;
        }
    }

    #[allow(dead_code)]
    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<LimitRange>("/registry/limitranges/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("limitranges/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list limitranges for enqueue: {}", e);
            }
        }
    }

    /// Reconcile all LimitRange resources.
    ///
    /// Stub: returns `Ok(())` without performing any enforcement. The real
    /// enforcement (defaulting, min/max, ratio, PVC) lives in the admission
    /// plugin path. This method exists to mirror the API of the other
    /// controllers (e.g. `PVBinderController::reconcile_all`) so future work
    /// can wire in side-effects without changing the public surface.
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_reconcile_all_is_noop_on_empty_storage() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = LimitRangeController::new(storage);
        // Stub MUST return Ok even with nothing in storage.
        controller.reconcile_all().await.unwrap();
    }
}
