//! PersistentVolumeClaim Controller (STUB).
//!
//! This module is part of Phase 2.2 of the conformance-test scaffolding effort.
//! It exists as a minimal stub so that RED-state integration tests under
//! `tests/pvc_controller_test.rs` have a real type to point at. The intent is
//! that the tests express the behaviours we *want* the PVC controller to
//! implement, while the controller itself is intentionally a no-op.
//!
//! Behaviour pinned by the companion test file (all currently `#[ignore]`):
//!
//! * Binding modes: `Immediate` vs `WaitForFirstConsumer`
//! * Storage class selection: explicit class vs default class
//! * Online volume expansion (PVC resize)
//! * PVC cloning (snapshot- and PVC-to-PVC)
//! * DataSource population (e.g. restoring from snapshots)
//!
//! Once a real implementation lands, the `#[ignore]` attributes on those tests
//! should be removed one-by-one to drive the controller GREEN.
//!
//! Shape mirrors `pv_binder.rs` so the eventual real implementation can slot
//! straight in next to it. Upstream reference:
//! <https://github.com/kubernetes/kubernetes/blob/master/test/e2e/storage/persistent_volumes_claim.go>.

use anyhow::Result;
use rusternetes_common::resources::PersistentVolumeClaim;
use rusternetes_storage::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, info};

/// Default reconcile interval. Tuned to match other storage controllers
/// (e.g. `volume_expansion`, `pv_binder` resync window).
const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// PersistentVolumeClaim controller stub.
///
/// In its real form this will own the PVC lifecycle: binding modes,
/// storage-class defaulting, online resize, cloning, and data-source
/// population. For now every method is a no-op; the type exists purely so the
/// RED-state test file in `tests/pvc_controller_test.rs` can compile.
#[allow(dead_code)]
pub struct PvcController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
}

impl<S: Storage + 'static> PvcController<S> {
    /// Construct a new PVC controller with the default reconcile interval.
    #[allow(dead_code)]
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            interval: DEFAULT_RECONCILE_INTERVAL,
        }
    }

    /// Construct a new PVC controller with an explicit reconcile interval.
    #[allow(dead_code)]
    pub fn with_interval(storage: Arc<S>, interval: Duration) -> Self {
        Self { storage, interval }
    }

    /// Drive the reconcile loop until cancelled.
    ///
    /// Currently this just sleeps for the configured interval and re-runs the
    /// no-op `reconcile_all`. The intent is that adding behaviour to
    /// `reconcile_all` is sufficient to make the controller useful; nothing
    /// else in this method needs to change.
    #[allow(dead_code)]
    pub async fn run(&self) -> Result<()> {
        use futures::StreamExt;
        info!("Starting PVC Controller (stub)");

        // reconcile_all is a no-op stub, so the old fixed-interval loop just
        // burned a wake-up every interval forever. Block on PVC writes instead
        // (idle CPU → ~0); the periodic resync is the wedge-safe fallback and
        // keeps the loop event-driven for when the real PVC lifecycle reconcile
        // lands (#1040). (Actual binding is the watch-driven pv_binder.)
        loop {
            if let Err(e) = self.reconcile_all().await {
                debug!("PVC reconcile_all returned error: {}", e);
            }

            let mut watch = match self
                .storage
                .watch("/registry/persistentvolumeclaims/")
                .await
            {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!("PVC watch failed: {e}; retrying in {:?}", self.interval);
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
                                debug!("PVC reconcile_all returned error: {}", e);
                            }
                        }
                        _ => break,
                    },
                    _ = resync.tick() => {
                        if let Err(e) = self.reconcile_all().await {
                            debug!("PVC reconcile_all returned error: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Reconcile every PVC in the cluster. STUB: always Ok.
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        Ok(())
    }

    /// Reconcile a single PVC. STUB: always Ok.
    #[allow(dead_code)]
    pub async fn reconcile_one(&self, _pvc: &PersistentVolumeClaim) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn stub_reconcile_all_is_ok() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PvcController::new(storage);
        controller.reconcile_all().await.unwrap();
    }

    #[tokio::test]
    async fn stub_reconcile_one_is_ok() {
        use rusternetes_common::resources::volume::{
            PersistentVolumeAccessMode, PersistentVolumeClaimSpec, ResourceRequirements,
        };
        use rusternetes_common::types::{ObjectMeta, TypeMeta};

        let storage = Arc::new(MemoryStorage::new());
        let controller = PvcController::new(storage);

        let pvc = PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut meta = ObjectMeta::new("dummy");
                meta.namespace = Some("default".to_string());
                meta
            },
            spec: PersistentVolumeClaimSpec {
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                resources: ResourceRequirements::default(),
                volume_name: None,
                storage_class_name: None,
                volume_mode: None,
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: None,
        };

        controller.reconcile_one(&pvc).await.unwrap();
    }
}
