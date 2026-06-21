//! VolumeAttachment controller (stub).
//!
//! Upstream reference: `kubernetes/pkg/controller/volume/attachdetach` and the
//! CSI-attacher external sidecar. The real controller observes
//! `VolumeAttachment` objects (cluster-scoped, in `storage.k8s.io/v1`) and
//! drives the CSI ControllerPublishVolume / ControllerUnpublishVolume calls
//! for each attach/detach intent, reflecting progress in
//! `VolumeAttachment.status.attached` and `attachError` / `detachError`.
//!
//! This crate currently only ships a stub so downstream code (the all-in-one
//! binary, integration tests) can wire the controller in. The reconciliation
//! body is intentionally a no-op; RED-state tests in
//! `tests/volume_attachment_test.rs` track the missing behaviour.
use anyhow::Result;
use rusternetes_storage::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::info;

/// Periodic reconcile interval used by the eventual implementation. The stub
/// uses the same value so wiring code (`controller-manager` main) sees the
/// final API surface.
const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(15);

/// Reconciles `VolumeAttachment` resources by driving CSI attach / detach
/// calls. Cluster-scoped, like its upstream counterpart.
pub struct VolumeAttachmentController<S: Storage> {
    #[allow(dead_code)]
    storage: Arc<S>,
    #[allow(dead_code)]
    interval: Duration,
}

impl<S: Storage + 'static> VolumeAttachmentController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            interval: DEFAULT_RECONCILE_INTERVAL,
        }
    }

    /// Long-running loop — currently a stub that only logs a warning and
    /// sleeps on the reconcile interval. Real implementation should watch
    /// `/registry/volumeattachments/` and invoke the CSI driver.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;
        info!("Starting VolumeAttachment Controller (stub: CSI attach/detach not yet implemented)");

        // The reconcile is currently a no-op stub, so the old fixed-interval
        // loop just burned a wake-up every interval forever. Block on
        // VolumeAttachment writes instead (idle CPU → ~0); the periodic resync
        // is the wedge-safe fallback and keeps the loop event-driven for when
        // the CSI attach/detach reconcile is implemented (#1040).
        loop {
            if let Err(e) = self.reconcile_all().await {
                tracing::error!("VolumeAttachment reconcile_all failed: {}", e);
            }

            let mut watch = match self.storage.watch("/registry/volumeattachments/").await {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!(
                        "VolumeAttachment watch failed: {e}; retrying in {:?}",
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
                                tracing::error!("VolumeAttachment reconcile_all failed: {}", e);
                            }
                        }
                        _ => break,
                    },
                    _ = resync.tick() => {
                        if let Err(e) = self.reconcile_all().await {
                            tracing::error!("VolumeAttachment reconcile_all failed: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Reconcile all `VolumeAttachment` objects. Currently a no-op stub.
    pub async fn reconcile_all(&self) -> Result<()> {
        Ok(())
    }
}
