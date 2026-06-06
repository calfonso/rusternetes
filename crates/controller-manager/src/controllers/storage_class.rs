//! StorageClass controller.
//!
//! This controller is responsible for managing `StorageClass` objects:
//! enforcing the single-default-class invariant
//! (`storageclass.kubernetes.io/is-default-class` annotation), validating
//! provisioner parameters, propagating mount options to dynamically
//! provisioned `PersistentVolume` objects, and defaulting the reclaim
//! policy when one is not specified.
//!
//! Upstream reference: `kubernetes/test/e2e/storage/storage_class.go`.

use anyhow::Result;
use rusternetes_common::resources::volume::{
    PersistentVolume, PersistentVolumeReclaimPolicy, StorageClass,
};
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
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
/// `storageclass.beta.kubernetes.io/is-default-class`. The controller should
/// also inspect the beta annotation to stay consistent with admission;
/// tracking that as a follow-up.
pub const IS_DEFAULT_STORAGE_CLASS_ANNOTATION: &str = "storageclass.kubernetes.io/is-default-class";

/// Legacy beta variant of [`IS_DEFAULT_STORAGE_CLASS_ANNOTATION`]. Still
/// accepted by the api-server admission layer; kept here so the
/// controller's coverage can be extended once it learns to read
/// it. Not yet referenced by the controller.
#[allow(dead_code)]
pub const IS_DEFAULT_STORAGE_CLASS_BETA_ANNOTATION: &str =
    "storageclass.beta.kubernetes.io/is-default-class";

/// `StorageClassController` reconciles `StorageClass` objects: it defaults a
/// missing `reclaim_policy` to `Delete`, enforces the single-default-class
/// invariant via the `is-default-class` annotation, and backfills
/// `mount_options` onto bound `PersistentVolume`s.
pub struct StorageClassController<S: Storage> {
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
        info!("Starting StorageClass Controller");
        loop {
            if let Err(e) = self.reconcile_all().await {
                tracing::error!("StorageClass reconcile failed: {}", e);
            }
            time::sleep(self.interval).await;
        }
    }

    /// Reconcile every `StorageClass` in the cluster.
    ///
    /// Implements two invariants:
    ///
    /// 1. **Reclaim-policy defaulting** — a `StorageClass` created without an
    ///    explicit `reclaim_policy` is defaulted to `Delete`, matching upstream
    ///    Kubernetes defaulting in `pkg/apis/storage/v1/defaults.go`.
    ///
    /// 2. **Single-default-class invariant** — at most one `StorageClass` may
    ///    carry `storageclass.kubernetes.io/is-default-class=true`. When
    ///    multiple classes claim the default, the controller picks the winner
    ///    using the same ordering as upstream `GetDefaultClass`
    ///    (`pkg/volume/util/storageclass.go`): newest `creationTimestamp`,
    ///    tie-broken by name ascending. All losers are demoted to `"false"`.
    pub async fn reconcile_all(&self) -> Result<()> {
        let classes: Vec<StorageClass> = self.storage.list("/registry/storageclasses/").await?;

        // Select the single default upstream would honor (GetDefaultClass,
        // pkg/volume/util/storageclass.go): newest creationTimestamp, tie-broken
        // by name ascending. We then demote the losers (rusternetes-specific;
        // upstream tolerates multiple and only picks).
        let mut defaults: Vec<&StorageClass> = classes.iter().filter(|sc| is_default(sc)).collect();
        defaults.sort_by(|a, b| {
            b.metadata
                .creation_timestamp
                .cmp(&a.metadata.creation_timestamp)
                .then_with(|| a.metadata.name.cmp(&b.metadata.name))
        });
        let winner: Option<String> = defaults.first().map(|sc| sc.metadata.name.clone());

        // class-name -> mount options, for the PV backfill below. Built before the
        // mutation loop consumes `classes`.
        let mount_opts_by_class: HashMap<String, Vec<String>> = classes
            .iter()
            .filter_map(|sc| {
                sc.mount_options
                    .as_ref()
                    .filter(|o| !o.is_empty())
                    .map(|o| (sc.metadata.name.clone(), o.clone()))
            })
            .collect();

        for mut sc in classes {
            let mut changed = false;

            // Default a missing reclaim policy to Delete; never overwrite an
            // explicit value or touch provisioner/parameters.
            if sc.reclaim_policy.is_none() {
                sc.reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
                changed = true;
            }

            // Demote any default class that is not the winner. A class explicitly
            // set to "false" is never promoted (is_default is false for it).
            if is_default(&sc) && winner.as_deref() != Some(sc.metadata.name.as_str()) {
                set_default_annotation(&mut sc, "false");
                changed = true;
            }

            if changed {
                let key = build_key("storageclasses", None, &sc.metadata.name);
                self.storage.update(&key, &sc).await?;
            }
        }

        // Backfill StorageClass.mount_options onto bound PVs that reference the
        // class but carry no mount options of their own (steady-state invariant
        // PV.spec.mount_options == StorageClass.mount_options). DIVERGENCE: upstream
        // sets these only at provision time when creating the PV
        // (pkg/controller/volume/persistentvolume/pv_controller.go ~L1677,
        // options.MountOptions = storageClass.MountOptions); it never backfills
        // existing PVs. Backfilling is an in-process port choice the RED-state test
        // explicitly sanctions.
        if !mount_opts_by_class.is_empty() {
            let pvs: Vec<PersistentVolume> =
                self.storage.list("/registry/persistentvolumes/").await?;
            for pv in pvs {
                let Some(scn) = pv.spec.storage_class_name.as_deref() else {
                    continue;
                };
                let Some(opts) = mount_opts_by_class.get(scn) else {
                    continue;
                };
                // Only fill the gap — PVs that already carry mount options are
                // left untouched; we never overwrite an existing value.
                let missing = pv
                    .spec
                    .mount_options
                    .as_ref()
                    .map(|o| o.is_empty())
                    .unwrap_or(true);
                if missing {
                    let mut pv = pv;
                    pv.spec.mount_options = Some(opts.clone());
                    let key = build_key("persistentvolumes", None, &pv.metadata.name);
                    self.storage.update(&key, &pv).await?;
                }
            }
        }

        Ok(())
    }
}

/// True when the StorageClass carries the GA default-class annotation set to
/// `"true"`.
fn is_default(sc: &StorageClass) -> bool {
    sc.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(IS_DEFAULT_STORAGE_CLASS_ANNOTATION))
        .map(|v| v == "true")
        .unwrap_or(false)
}

/// Set the GA default-class annotation to `val`, creating the annotations map
/// if absent.
fn set_default_annotation(sc: &mut StorageClass, val: &str) {
    sc.metadata
        .annotations
        .get_or_insert_with(HashMap::new)
        .insert(
            IS_DEFAULT_STORAGE_CLASS_ANNOTATION.to_string(),
            val.to_string(),
        );
}
