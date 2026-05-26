//! StorageClass controller (stub).
//!
//! This controller is responsible for managing `StorageClass` objects:
//! enforcing the single-default-class invariant
//! (`storageclass.kubernetes.io/is-default-class` annotation), validating
//! provisioner parameters, propagating mount options to dynamically
//! provisioned `PersistentVolume` objects, and defaulting the reclaim
//! policy when one is not specified.
//!
//! The implementation is intentionally a stub right now — its sole purpose
//! is to give the rest of the controller-manager wiring a stable type to
//! reference while the behaviour is built out incrementally. The companion
//! integration tests under
//! `crates/controller-manager/tests/storageclass_controller_test.rs`
//! encode the expected behaviour as `#[ignore]`'d RED-state assertions and
//! will be flipped on as each piece of functionality lands.
//!
//! Upstream reference: `kubernetes/test/e2e/storage/storage_class.go`.

use anyhow::Result;
use rusternetes_storage::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::info;

/// Annotation that marks a `StorageClass` as the cluster-wide default.
///
/// At most one `StorageClass` should carry this annotation set to `"true"`
/// at any given time. Enforcement of that invariant is the responsibility
/// of this controller.
///
/// Note: the api-server admission layer (`crates/api-server/src/admission.rs`)
/// also honours the legacy beta variant
/// `storageclass.beta.kubernetes.io/is-default-class`. When this controller
/// grows real reconcile logic it must inspect *both* annotations to remain
/// consistent with admission; tracking that as a follow-up.
pub const IS_DEFAULT_STORAGE_CLASS_ANNOTATION: &str = "storageclass.kubernetes.io/is-default-class";

/// Legacy beta variant of [`IS_DEFAULT_STORAGE_CLASS_ANNOTATION`]. Still
/// accepted by the api-server admission layer; kept here so the
/// controller's RED-state coverage can be extended once it learns to read
/// it. Not yet referenced by the stub.
#[allow(dead_code)]
pub const IS_DEFAULT_STORAGE_CLASS_BETA_ANNOTATION: &str =
    "storageclass.beta.kubernetes.io/is-default-class";

/// `StorageClassController` reconciles `StorageClass` objects.
///
/// Currently a stub: `reconcile_all` is a no-op and `run` just sleeps on
/// the configured interval. See module docs for the intended behaviour.
pub struct StorageClassController<S: Storage> {
    #[allow(dead_code)]
    storage: Arc<S>,
    interval: Duration,
}

impl<S: Storage + 'static> StorageClassController<S> {
    /// Build a new `StorageClassController` with a 30-second reconcile
    /// interval (the default cadence used by the other volume-related
    /// controllers in this crate).
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(30),
        }
    }

    /// Run the controller loop.
    ///
    /// Calls [`reconcile_all`](Self::reconcile_all) once per
    /// `self.interval` until cancelled. Errors from a single reconcile
    /// pass are intentionally swallowed — the controller will retry on
    /// the next tick.
    pub async fn run(&self) -> Result<()> {
        info!("Starting StorageClass Controller (stub)");
        loop {
            if let Err(e) = self.reconcile_all().await {
                tracing::error!("StorageClass reconcile failed: {}", e);
            }
            time::sleep(self.interval).await;
        }
    }

    /// Reconcile every `StorageClass` in the cluster.
    ///
    /// Currently a no-op; the RED-state tests in
    /// `tests/storageclass_controller_test.rs` describe the behaviour
    /// this method must eventually implement.
    pub async fn reconcile_all(&self) -> Result<()> {
        Ok(())
    }
}
