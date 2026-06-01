// Garbage Collector - Manages cascade deletion and orphaning of dependent resources
//
// Implements:
// - Owner reference tracking
// - Cascade deletion (foreground and background)
// - Orphan deletion
// - Finalizer handling for deletion protection

use rusternetes_common::types::{DeletionPropagation, ObjectMeta};
use rusternetes_storage::Storage;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

/// Garbage collector controller
#[allow(dead_code)]
pub struct GarbageCollector<S: Storage> {
    storage: Arc<S>,
    /// How often to run GC scan
    scan_interval: Duration,
    /// Maximum number of concurrent delete operations
    max_concurrent_deletes: usize,
    /// Batch size for deletion operations
    delete_batch_size: usize,
    /// Maximum retry attempts for failed deletions
    max_retries: u32,
    /// Orphans detected in the previous scan. Only delete orphans that appear
    /// in TWO consecutive scans. This prevents race conditions where a resource
    /// is created between the GC listing owners and listing dependents.
    /// K8s avoids this via informer caches; we use a grace period.
    pending_orphans: std::sync::Mutex<HashSet<String>>,
}

impl<S: Storage + 'static> GarbageCollector<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            scan_interval: Duration::from_secs(5),
            max_concurrent_deletes: 50,
            delete_batch_size: 100,
            max_retries: 3,
            pending_orphans: std::sync::Mutex::new(HashSet::new()),
        }
    }

    /// Create a new garbage collector with custom settings
    #[allow(dead_code)]
    pub fn with_config(
        storage: Arc<S>,
        scan_interval_secs: u64,
        max_concurrent_deletes: usize,
        delete_batch_size: usize,
    ) -> Self {
        Self {
            storage,
            scan_interval: Duration::from_secs(scan_interval_secs),
            max_concurrent_deletes,
            delete_batch_size,
            max_retries: 3,
            pending_orphans: std::sync::Mutex::new(HashSet::new()),
        }
    }

    /// Start the garbage collector
    pub async fn run(&self) {
        info!("Starting Garbage Collector");
        loop {
            if let Err(e) = self.scan_and_collect().await {
                error!("Garbage collection scan failed: {}", e);
            }
            sleep(self.scan_interval).await;
        }
    }

    /// Scan all resources and collect orphans
    pub async fn scan_and_collect(&self) -> rusternetes_common::Result<()> {
        debug!("Running garbage collection scan");

        // Get all resources from storage
        let all_resources = self.get_all_resources().await?;

        // Build owner-dependent relationship map
        let (owner_map, dependent_map) = self.build_relationship_maps(&all_resources);

        // Detect cycles in dependency graph (non-blocking, just warn)
        if let Err(cycle_info) = self.detect_cycles(&dependent_map, &all_resources) {
            warn!(
                "Detected dependency cycle in resource graph: {}",
                cycle_info
            );
        }

        // Find orphaned resources (resources whose owners no longer exist)
        let orphans = self.find_orphans(&all_resources, &owner_map);

        // K8s GC does not require multiple scans to confirm an orphan — it
        // re-reads each owner from the apiserver before deleting the dependent
        // (attemptToDeleteItem → getObject in garbagecollector.go:521). We
        // do the same in `delete_orphan`, which re-reads both the dependent
        // and every owner from storage and only deletes when all owners are
        // confirmed gone. The previous 2-scan grace introduced a full-cycle
        // delay between owner deletion and dependent removal, which
        // conformance tests for GC orphan-pod cleanup observe as a failure.
        //
        // We still record `pending_orphans` as a no-op so existing fields
        // compile, but it is no longer consulted to gate deletion.
        {
            let mut pending = self.pending_orphans.lock().unwrap();
            *pending = orphans.iter().map(|o| o.key.clone()).collect();
        }

        if !orphans.is_empty() {
            info!(
                "Found {} orphaned resources to verify and delete",
                orphans.len()
            );

            let mut deleted_count = 0;
            let mut failed_count = 0;

            for orphan in &orphans {
                match self.delete_orphan(orphan).await {
                    Ok(_) => deleted_count += 1,
                    Err(e) => {
                        failed_count += 1;
                        error!("Failed to delete orphan {}: {}", orphan.key, e);
                    }
                }
            }

            info!(
                "GC orphan deletion complete: {} deleted, {} failed",
                deleted_count, failed_count
            );
        }

        // NOTE: Namespace deletion is handled by the NamespaceController, NOT the GC.
        // K8s GC handles ownerReference cascading (e.g. Deployment → ReplicaSet → Pod).
        // Namespace cleanup (deleting all resources in a namespace) is done by the
        // NamespacedResourcesDeleter (our NamespaceController).
        // Previously, the GC also did cascade_delete_namespace which force-deleted
        // all resources ignoring finalizers, racing with the namespace controller
        // and breaking conformance tests that rely on finalizer-blocked deletion ordering.
        // K8s ref: pkg/controller/namespace/deletion/namespaced_resources_deleter.go
        //
        // Skipping namespace cascade in GC. The deleted_namespaces detection below
        // is kept but the cascade is removed.
        let deleted_namespaces: Vec<_> = all_resources
            .iter()
            .filter(|r| r.resource_type == "namespaces" && r.metadata.is_being_deleted())
            .collect();

        for _namespace in deleted_namespaces {
            // Namespace cleanup handled by NamespaceController — do nothing here.
            // The namespace controller respects finalizers and deletion ordering.
        }

        // Process resources with deletion timestamp
        let being_deleted: Vec<_> = all_resources
            .iter()
            .filter(|r| r.metadata.is_being_deleted())
            .collect();

        for resource in being_deleted {
            if let Err(e) = self.process_deletion(resource, &dependent_map).await {
                error!("Failed to process deletion for {:?}: {}", resource.key, e);
            }
        }

        debug!("Garbage collection scan complete");
        Ok(())
    }

    /// Get all resources from storage
    async fn get_all_resources(&self) -> rusternetes_common::Result<Vec<ResourceInfo>> {
        let mut resources = Vec::new();

        // List all resources across all namespaces and resource types
        // In a real implementation, this would be more sophisticated
        // For now, we'll scan known resource types
        let resource_types = vec![
            ("namespaces", false), // cluster-scoped
            ("pods", true),
            ("services", true),
            ("endpoints", true),
            ("endpointslices", true),
            ("ingresses", true),
            ("networkpolicies", true),
            ("verticalpodautoscalers", true),
            ("volumesnapshots", true),
            ("volumesnapshotcontents", false), // cluster-scoped
            ("resourceclaims", true),
            ("certificatesigningrequests", false), // cluster-scoped
            ("customresourcedefinitions", false),  // cluster-scoped
            ("deployments", true),
            ("replicationcontrollers", true),
            ("replicasets", true),
            ("statefulsets", true),
            ("daemonsets", true),
            ("controllerrevisions", true),
            ("jobs", true),
            ("cronjobs", true),
            ("configmaps", true),
            ("secrets", true),
            ("serviceaccounts", true),
            ("persistentvolumeclaims", true),
            ("persistentvolumes", false),   // cluster-scoped
            ("clusterroles", false),        // cluster-scoped
            ("clusterrolebindings", false), // cluster-scoped
        ];

        for (resource_type, namespaced) in resource_types {
            if namespaced {
                // For namespaced resources, we need to list across all namespaces
                // This is simplified - in reality we'd list all namespaces first
                let prefix = format!("/registry/{}/", resource_type);
                if let Ok(items) = self
                    .list_resources_with_metadata(&prefix, resource_type)
                    .await
                {
                    resources.extend(items);
                }
            } else {
                // For cluster-scoped resources
                let prefix = format!("/registry/{}/", resource_type);
                if let Ok(items) = self
                    .list_resources_with_metadata(&prefix, resource_type)
                    .await
                {
                    resources.extend(items);
                }
            }
        }

        Ok(resources)
    }

    /// List resources with metadata
    async fn list_resources_with_metadata(
        &self,
        prefix: &str,
        resource_type: &str,
    ) -> rusternetes_common::Result<Vec<ResourceInfo>> {
        let values: Vec<Value> = self.storage.list(prefix).await?;
        let mut resources = Vec::new();

        for value in values {
            if let Err(e) = self.extract_metadata(&value) {
                debug!(
                    "GC: Failed to extract metadata from {} resource: {} (key hint: {:?})",
                    resource_type,
                    e,
                    value.pointer("/metadata/name").and_then(|v| v.as_str())
                );
            }
            if let Ok(metadata) = self.extract_metadata(&value) {
                // Reconstruct the storage key using the same format as build_key
                let key = match &metadata.namespace {
                    Some(ns) => format!("/registry/{}/{}/{}", resource_type, ns, metadata.name),
                    None => format!("/registry/{}/{}", resource_type, metadata.name),
                };

                resources.push(ResourceInfo {
                    key,
                    metadata,
                    resource_type: resource_type.to_string(),
                    value,
                });
            }
        }

        Ok(resources)
    }

    /// Extract metadata from a resource
    fn extract_metadata(&self, value: &Value) -> rusternetes_common::Result<ObjectMeta> {
        let metadata = value.get("metadata").ok_or_else(|| {
            rusternetes_common::Error::InvalidResource("Missing metadata".to_string())
        })?;

        serde_json::from_value(metadata.clone())
            .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))
    }

    /// Build owner-dependent relationship maps
    fn build_relationship_maps(
        &self,
        resources: &[ResourceInfo],
    ) -> (HashMap<String, Vec<String>>, HashMap<String, Vec<String>>) {
        let mut owner_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut dependent_map: HashMap<String, Vec<String>> = HashMap::new();

        for resource in resources {
            let resource_uid = &resource.metadata.uid;

            // Track what this resource owns
            if let Some(owner_refs) = &resource.metadata.owner_references {
                for owner_ref in owner_refs {
                    // Map: owner UID -> list of dependent UIDs
                    owner_map
                        .entry(owner_ref.uid.clone())
                        .or_default()
                        .push(resource_uid.clone());

                    // Map: dependent UID -> list of owner UIDs
                    dependent_map
                        .entry(resource_uid.clone())
                        .or_default()
                        .push(owner_ref.uid.clone());
                }
            }
        }

        (owner_map, dependent_map)
    }

    /// Find orphaned resources — resources where ALL owner references point to
    /// non-existent owners. A resource with at least one valid owner is NOT an orphan.
    ///
    /// Owner resolution is **namespace-scoped**, matching upstream
    /// `pkg/controller/garbagecollector/garbagecollector.go` (`classifyReferences`):
    ///
    ///   * A namespaced dependent's owner ref resolves only against resources
    ///     in the same namespace *or* cluster-scoped resources. A UID that
    ///     happens to exist in a different namespace is treated as
    ///     unresolvable — cross-namespace owner refs are invalid by K8s API
    ///     contract, so they MUST orphan the dependent regardless of UID
    ///     collisions elsewhere in the cluster.
    ///   * A cluster-scoped dependent's owner ref must resolve to another
    ///     cluster-scoped resource (cluster → namespaced refs are illegal).
    ///
    /// Resources with `deletionTimestamp` are still treated as "existing"
    /// here — they're only truly gone once removed from storage, and their
    /// dependents are handled by the foreground/orphan finalizer paths.
    fn find_orphans(
        &self,
        resources: &[ResourceInfo],
        _owner_map: &HashMap<String, Vec<String>>,
    ) -> Vec<ResourceInfo> {
        // Bucket existing UIDs by namespace (`None` = cluster-scoped).
        let mut uids_by_namespace: HashMap<Option<&str>, HashSet<&str>> = HashMap::new();
        for r in resources {
            let ns = r.metadata.namespace.as_deref();
            uids_by_namespace
                .entry(ns)
                .or_default()
                .insert(r.metadata.uid.as_str());
        }
        let cluster_scoped_uids: &HashSet<&str> = uids_by_namespace
            .get(&None)
            .map(|s| s as &HashSet<&str>)
            .unwrap_or({
                static EMPTY: std::sync::OnceLock<HashSet<&str>> = std::sync::OnceLock::new();
                EMPTY.get_or_init(HashSet::new)
            });

        let mut orphans = Vec::new();
        for resource in resources {
            let owner_refs = match &resource.metadata.owner_references {
                Some(refs) if !refs.is_empty() => refs,
                _ => continue,
            };
            let dep_ns = resource.metadata.namespace.as_deref();
            let same_ns_uids = uids_by_namespace.get(&dep_ns);

            let all_owners_missing = owner_refs.iter().all(|owner_ref| {
                let uid = owner_ref.uid.as_str();
                let in_same_ns = same_ns_uids.map(|s| s.contains(uid)).unwrap_or(false);
                let in_cluster_scope = cluster_scoped_uids.contains(uid);
                // A namespaced dependent accepts same-namespace OR cluster-scoped
                // owners; a cluster-scoped dependent accepts only cluster-scoped
                // owners. Either way, an owner sitting in some *other* namespace
                // is unresolvable.
                match dep_ns {
                    Some(_) => !(in_same_ns || in_cluster_scope),
                    None => !in_cluster_scope,
                }
            });

            if all_owners_missing {
                debug!(
                    "GC: orphan {} — ownerRef UIDs {:?} unresolvable in namespace {:?}",
                    resource.key,
                    owner_refs
                        .iter()
                        .map(|r| r.uid.as_str())
                        .collect::<Vec<_>>(),
                    dep_ns,
                );
                orphans.push(resource.clone());
            }
        }

        orphans
    }

    /// Delete an orphaned resource, but only after re-verifying the owner is gone.
    ///
    /// The initial orphan detection uses a snapshot which can be stale — resources
    /// created between the scan start and the orphan check won't be in the snapshot.
    /// K8s GC re-reads the owner from the API server before deleting dependents:
    /// see attemptToDeleteItem → getObject in garbagecollector.go:521.
    ///
    /// We re-read both the dependent (to get fresh ownerRefs) and then look up
    /// each owner by constructing the storage key from the ownerReference fields.
    async fn delete_orphan(&self, orphan: &ResourceInfo) -> rusternetes_common::Result<()> {
        // Re-read the resource from storage to get fresh ownerReferences.
        // It may have been updated since the scan snapshot.
        let fresh: Value = match self.storage.get(&orphan.key).await {
            Ok(v) => v,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()), // already gone
            Err(e) => return Err(e),
        };
        let fresh_meta = match self.extract_metadata(&fresh) {
            Ok(m) => m,
            Err(_) => return Ok(()), // can't parse metadata, skip
        };

        // If ownerReferences were removed (orphan policy processed), skip deletion
        let owner_refs = match &fresh_meta.owner_references {
            Some(refs) if !refs.is_empty() => refs,
            _ => return Ok(()), // no owners = not an orphan (or already orphaned)
        };

        // For each ownerReference, construct the storage key and check if the owner exists.
        // K8s uses the owner's GVR + namespace + name to look it up.
        // We use kind → plural resource name mapping + namespace from the dependent.
        let namespace = fresh_meta.namespace.as_deref();

        for owner_ref in owner_refs {
            let plural = kind_to_plural(&owner_ref.kind);
            if plural.is_empty() {
                // Unknown kind — be conservative, don't delete
                debug!(
                    "GC: {} has owner of unknown kind '{}', skipping",
                    orphan.key, owner_ref.kind
                );
                return Ok(());
            }
            let owner_key = if let Some(ns) = namespace {
                format!("/registry/{}/{}/{}", plural, ns, owner_ref.name)
            } else {
                format!("/registry/{}/{}", plural, owner_ref.name)
            };

            // Try to read the owner from storage
            match self.storage.get::<Value>(&owner_key).await {
                Ok(owner_value) => {
                    // Owner exists — verify UID matches
                    if let Some(uid) = owner_value
                        .pointer("/metadata/uid")
                        .and_then(|u| u.as_str())
                    {
                        if uid == owner_ref.uid {
                            // Owner with matching UID exists — NOT an orphan
                            debug!(
                                "GC: {} is NOT orphan — owner {}/{} (uid={}) still exists",
                                orphan.key, owner_ref.kind, owner_ref.name, uid
                            );
                            return Ok(());
                        }
                        // UID mismatch — the resource was recreated with a different UID.
                        // The old owner is gone, this ownerRef is dangling.
                    }
                }
                Err(rusternetes_common::Error::NotFound(_)) => {
                    // Owner not found — this ownerRef is dangling
                }
                Err(_) => {
                    // Storage error — be conservative, don't delete
                    return Ok(());
                }
            }
        }

        // All owners verified as gone — this is truly an orphan
        info!(
            "Deleting orphaned resource: {} ({}) — all owners verified gone",
            orphan.key, orphan.resource_type
        );
        self.storage.delete(&orphan.key).await
    }

    /// Process deletion for a resource with deletion timestamp
    async fn process_deletion(
        &self,
        resource: &ResourceInfo,
        dependent_map: &HashMap<String, Vec<String>>,
    ) -> rusternetes_common::Result<()> {
        // Check deletion propagation policy from finalizers
        let propagation_policy = self.determine_propagation_policy(&resource.metadata);

        match propagation_policy {
            DeletionPropagation::Foreground => {
                // In foreground deletion, we must delete all dependents first,
                // then remove the foregroundDeletion finalizer.
                self.delete_dependents_foreground(resource, dependent_map)
                    .await?;

                // GATE: only remove the foregroundDeletion finalizer once every
                // blocking dependent is actually gone from storage.
                // `delete_dependents_foreground` operates on a snapshot, so pods
                // that were still draining (or created after the snapshot) can
                // outlive a single pass. Removing the finalizer now would delete
                // the owner while dependents remain — the exact failure the
                // "keep the rc around until all its pods are deleted" conformance
                // test catches. Upstream keeps the finalizer until all blocking
                // dependents are removed (garbagecollector.go attemptToDeleteItem);
                // we re-check and wait for the next scan instead.
                if self.still_has_dependents(&resource.metadata.uid).await? {
                    debug!(
                        "Foreground deletion of {} waiting: dependents still present in storage",
                        resource.key
                    );
                    return Ok(());
                }

                // Remove the foregroundDeletion finalizer from the resource
                self.remove_finalizer(resource, "foregroundDeletion")
                    .await?;
            }
            DeletionPropagation::Orphan => {
                // In orphan mode, we remove owner references from dependents,
                // then remove the orphan finalizer
                self.orphan_dependents(resource, dependent_map).await?;

                // Remove the orphan finalizer from the resource
                self.remove_finalizer(resource, "orphan").await?;
            }
            DeletionPropagation::Background => {
                // In background deletion, we delete the owner and let GC clean up dependents
                // via the orphan detection in the next scan
            }
        }

        // Re-read the resource to see if it still has finalizers
        let current: rusternetes_common::Result<Value> = self.storage.get(&resource.key).await;
        match current {
            Ok(value) => {
                if let Ok(meta) = self.extract_metadata(&value) {
                    if !meta.has_finalizers() {
                        info!(
                            "Deleting resource (no finalizers remaining): {}",
                            resource.key
                        );
                        self.storage.delete(&resource.key).await?;
                    } else {
                        debug!(
                            "Resource {} still has finalizers {:?}, waiting",
                            resource.key, meta.finalizers
                        );
                    }
                }
            }
            Err(_) => {
                // Resource already deleted
                debug!("Resource {} already deleted", resource.key);
            }
        }

        Ok(())
    }

    /// Remove a specific finalizer from a resource in storage
    async fn remove_finalizer(
        &self,
        resource: &ResourceInfo,
        finalizer: &str,
    ) -> rusternetes_common::Result<()> {
        // Re-read the resource to get the latest version
        let current: Value = self.storage.get(&resource.key).await?;
        let mut updated = current;

        if let Some(metadata) = updated.get_mut("metadata") {
            if let Some(finalizers) = metadata.get_mut("finalizers") {
                if let Some(arr) = finalizers.as_array_mut() {
                    arr.retain(|f| f.as_str() != Some(finalizer));
                    if arr.is_empty() {
                        if let Some(obj) = metadata.as_object_mut() {
                            obj.remove("finalizers");
                        }
                    }
                }
            }
        }

        info!("Removing {} finalizer from {}", finalizer, resource.key);
        self.storage.update_raw(&resource.key, &updated).await?;
        Ok(())
    }

    /// Determine deletion propagation policy from metadata
    fn determine_propagation_policy(&self, metadata: &ObjectMeta) -> DeletionPropagation {
        if let Some(finalizers) = &metadata.finalizers {
            if finalizers.contains(&"foregroundDeletion".to_string()) {
                return DeletionPropagation::Foreground;
            }
            if finalizers.contains(&"orphan".to_string()) {
                return DeletionPropagation::Orphan;
            }
        }
        // Default to background deletion
        DeletionPropagation::Background
    }

    /// Delete dependents in foreground mode.
    /// Deletes dependents whose ONLY owner is the resource being deleted.
    /// Dependents with other valid owners are not deleted — instead, the
    /// owner reference to the deleted resource is removed from them.
    async fn delete_dependents_foreground(
        &self,
        resource: &ResourceInfo,
        _dependent_map: &HashMap<String, Vec<String>>,
    ) -> rusternetes_common::Result<()> {
        let resource_uid = &resource.metadata.uid;

        // Find all resources that have this resource as an owner
        let all_resources = self.get_all_resources().await?;
        let dependents: Vec<_> = all_resources
            .iter()
            .filter(|r| {
                r.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|oref| oref.uid == *resource_uid))
            })
            .collect();

        if dependents.is_empty() {
            debug!("No dependents to delete for resource {}", resource.key);
            return Ok(());
        }

        info!(
            "Foreground deletion: processing {} dependents of {}",
            dependents.len(),
            resource.key
        );

        let existing_uids: HashSet<_> = all_resources
            .iter()
            .map(|r| r.metadata.uid.as_str())
            .collect();

        for dependent in dependents {
            if let Some(owner_refs) = &dependent.metadata.owner_references {
                // Check if this dependent has other VALID owners (besides the one being deleted)
                let has_other_valid_owner = owner_refs.iter().any(|oref| {
                    oref.uid != *resource_uid && existing_uids.contains(oref.uid.as_str())
                });

                if has_other_valid_owner {
                    // Dependent has another valid owner — just remove the reference
                    // to the owner being deleted
                    info!(
                        "Dependent {} has other valid owners, removing reference to {}",
                        dependent.key, resource.key
                    );
                    let mut dependent_value = dependent.value.clone();
                    if let Some(metadata) = dependent_value.get_mut("metadata") {
                        if let Some(owner_refs_val) = metadata.get_mut("ownerReferences") {
                            if let Some(arr) = owner_refs_val.as_array_mut() {
                                arr.retain(|oref| {
                                    oref.get("uid")
                                        .and_then(|u| u.as_str())
                                        .map(|u| u != resource_uid)
                                        .unwrap_or(true)
                                });
                            }
                        }
                    }
                    if let Err(e) = self
                        .storage
                        .update_raw(&dependent.key, &dependent_value)
                        .await
                    {
                        error!("Failed to update dependent {}: {}", dependent.key, e);
                    }
                } else {
                    // Dependent's only owner is the one being deleted — delete it
                    info!(
                        "Deleting dependent {} (sole owner being deleted)",
                        dependent.key
                    );

                    // Recursively handle foreground deletion for this dependent's dependents
                    let (_, sub_dependent_map) = self.build_relationship_maps(&all_resources);
                    Box::pin(self.delete_dependents_foreground(dependent, &sub_dependent_map))
                        .await?;

                    if let Err(e) = self.storage.delete(&dependent.key).await {
                        error!("Failed to delete dependent {}: {}", dependent.key, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns true if any resource in storage still lists `owner_uid` in its
    /// ownerReferences — i.e. the owner still has blocking dependents.
    ///
    /// Used to gate foreground-deletion finalizer removal: the owner's
    /// `foregroundDeletion` finalizer must stay (and the owner must not be
    /// deleted) until every dependent has actually been removed from storage,
    /// not merely had a delete issued against it. Mirrors upstream GC, which
    /// keeps the owner blocked until its dependent set is empty.
    async fn still_has_dependents(&self, owner_uid: &str) -> rusternetes_common::Result<bool> {
        let all_resources = self.get_all_resources().await?;
        Ok(all_resources.iter().any(|r| {
            r.metadata
                .owner_references
                .as_ref()
                .is_some_and(|refs| refs.iter().any(|oref| oref.uid == owner_uid))
        }))
    }

    /// Orphan dependents by removing owner references.
    /// Finds all resources that have the deleted resource as an owner,
    /// then removes the owner reference to the deleted resource from each.
    async fn orphan_dependents(
        &self,
        resource: &ResourceInfo,
        _dependent_map: &HashMap<String, Vec<String>>,
    ) -> rusternetes_common::Result<()> {
        let resource_uid = &resource.metadata.uid;

        // Find all resources that reference this resource as an owner
        let all_resources = self.get_all_resources().await?;
        let dependents: Vec<_> = all_resources
            .iter()
            .filter(|r| {
                r.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|oref| oref.uid == *resource_uid))
            })
            .collect();

        if dependents.is_empty() {
            debug!("No dependents to orphan for resource {}", resource.key);
            return Ok(());
        }

        info!(
            "Orphan deletion: removing owner references from {} dependents of {}",
            dependents.len(),
            resource.key
        );

        // Remove owner references from each dependent
        for dependent in dependents {
            info!("Orphaning dependent {}", dependent.key);

            // Parse the dependent's full object
            let mut dependent_value = dependent.value.clone();

            // Remove the owner reference to this resource
            if let Some(metadata) = dependent_value.get_mut("metadata") {
                if let Some(owner_refs) = metadata.get_mut("ownerReferences") {
                    if let Some(owner_refs_array) = owner_refs.as_array_mut() {
                        // Filter out the owner reference matching this resource
                        owner_refs_array.retain(|owner_ref| {
                            owner_ref
                                .get("uid")
                                .and_then(|uid| uid.as_str())
                                .map(|uid| uid != resource_uid)
                                .unwrap_or(true)
                        });

                        // If no more owner references, remove the field entirely
                        if owner_refs_array.is_empty() {
                            if let Some(metadata_obj) = metadata.as_object_mut() {
                                metadata_obj.remove("ownerReferences");
                            }
                        }
                    }
                }
            }

            // Update the dependent in storage
            if let Err(e) = self
                .storage
                .update_raw(&dependent.key, &dependent_value)
                .await
            {
                error!("Failed to orphan dependent {}: {}", dependent.key, e);
                // Return error so the orphan finalizer is NOT removed.
                // This prevents the race where the owner is deleted while
                // dependents still have ownerReferences pointing to it.
                return Err(rusternetes_common::Error::Internal(format!(
                    "Failed to orphan dependent {}: {}",
                    dependent.key, e
                )));
            }
        }

        Ok(())
    }

    /// Cascade delete all resources in a namespace
    #[allow(dead_code)]
    async fn cascade_delete_namespace(
        &self,
        namespace: &ResourceInfo,
        all_resources: &[ResourceInfo],
    ) -> rusternetes_common::Result<()> {
        let namespace_name = &namespace.metadata.name;
        info!("Cascading delete for namespace: {}", namespace_name);

        // Find all resources in this namespace
        let resources_in_namespace: Vec<_> = all_resources
            .iter()
            .filter(|r| {
                r.metadata.namespace.as_deref() == Some(namespace_name)
                    && r.resource_type != "namespaces"
            })
            .collect();

        // Delete all resources in the namespace
        for resource in resources_in_namespace {
            info!(
                "Deleting {} {} in namespace {}",
                resource.resource_type, resource.metadata.name, namespace_name
            );
            if let Err(e) = self.storage.delete(&resource.key).await {
                error!(
                    "Failed to delete {} {}: {}",
                    resource.resource_type, resource.metadata.name, e
                );
            }
        }

        // If no resources left in namespace and no finalizers, delete the namespace
        if !namespace.metadata.has_finalizers() {
            info!("Deleting namespace: {}", namespace_name);
            self.storage.delete(&namespace.key).await?;
        }

        Ok(())
    }

    /// Detect cycles in the dependency graph
    /// Returns Ok(()) if no cycles, Err with cycle info if found
    fn detect_cycles(
        &self,
        dependent_map: &HashMap<String, Vec<String>>,
        resources: &[ResourceInfo],
    ) -> Result<(), String> {
        // Build UID to resource name map for better error messages
        let uid_to_name: HashMap<_, _> = resources
            .iter()
            .map(|r| (r.metadata.uid.clone(), r.metadata.name.clone()))
            .collect();

        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        // DFS cycle detection
        for resource in resources {
            if !visited.contains(&resource.metadata.uid) {
                if let Some(cycle_path) = self.detect_cycle_dfs(
                    &resource.metadata.uid,
                    dependent_map,
                    &mut visited,
                    &mut rec_stack,
                    &mut Vec::new(),
                    &uid_to_name,
                ) {
                    return Err(format!("Cycle detected in ownership chain: {}", cycle_path));
                }
            }
        }

        Ok(())
    }

    /// DFS helper for cycle detection
    #[allow(clippy::only_used_in_recursion)]
    fn detect_cycle_dfs(
        &self,
        uid: &str,
        dependent_map: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
        uid_to_name: &HashMap<String, String>,
    ) -> Option<String> {
        visited.insert(uid.to_string());
        rec_stack.insert(uid.to_string());
        path.push(uid.to_string());

        // Check all owners of this resource (dependents -> owners)
        if let Some(owners) = dependent_map.get(uid) {
            for owner_uid in owners {
                // Skip self-references — a resource owning itself is not a cycle
                if owner_uid == uid {
                    continue;
                }
                if !visited.contains(owner_uid) {
                    if let Some(cycle) = self.detect_cycle_dfs(
                        owner_uid,
                        dependent_map,
                        visited,
                        rec_stack,
                        path,
                        uid_to_name,
                    ) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(owner_uid) {
                    // Cycle detected! Build human-readable path
                    let cycle_start_idx = path.iter().position(|u| u == owner_uid).unwrap();
                    let cycle_path: Vec<_> = path[cycle_start_idx..]
                        .iter()
                        .map(|uid| {
                            uid_to_name
                                .get(uid)
                                .map(|name| name.as_str())
                                .unwrap_or("unknown")
                        })
                        .collect();
                    return Some(format!("{} -> {}", cycle_path.join(" -> "), cycle_path[0]));
                }
            }
        }

        path.pop();
        rec_stack.remove(uid);
        None
    }

    /// Delete a batch of orphans with retry logic (no re-verification).
    /// NOTE: For orphan deletion, use `delete_orphan()` which re-verifies
    /// owner existence before deleting. This method is kept for non-orphan
    /// batch deletions where re-verification is not needed.
    #[allow(dead_code)]
    async fn delete_batch_with_retry(&self, orphans: &[ResourceInfo]) -> Vec<Result<(), String>> {
        use futures::future::join_all;

        // Limit concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent_deletes));
        let mut tasks = Vec::new();

        for orphan in orphans {
            let sem = Arc::clone(&semaphore);
            let storage = Arc::clone(&self.storage);
            let orphan_clone = orphan.clone();
            let max_retries = self.max_retries;

            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                // Retry with exponential backoff
                let mut attempt = 0;
                let mut last_error = None;

                while attempt < max_retries {
                    match storage.delete(&orphan_clone.key).await {
                        Ok(_) => {
                            info!(
                                "Successfully deleted orphan {} (attempt {})",
                                orphan_clone.key,
                                attempt + 1
                            );
                            return Ok(());
                        }
                        Err(e) => {
                            attempt += 1;
                            last_error = Some(e.to_string());

                            if attempt < max_retries {
                                // Exponential backoff: 100ms, 200ms, 400ms, ...
                                let backoff_ms = 100 * (1 << attempt);
                                debug!(
                                    "Failed to delete {} (attempt {}), retrying in {}ms: {}",
                                    orphan_clone.key, attempt, backoff_ms, e
                                );
                                sleep(Duration::from_millis(backoff_ms)).await;
                            }
                        }
                    }
                }

                Err(format!(
                    "Failed to delete {} after {} attempts: {}",
                    orphan_clone.key,
                    max_retries,
                    last_error.unwrap_or_else(|| "unknown error".to_string())
                ))
            });

            tasks.push(task);
        }

        // Wait for all tasks and collect results
        let results = join_all(tasks).await;
        results
            .into_iter()
            .map(|r| r.unwrap_or_else(|e| Err(format!("Task panicked: {}", e))))
            .collect()
    }
}

/// Information about a resource for GC purposes
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ResourceInfo {
    /// Storage key for the resource
    key: String,
    /// Resource metadata
    metadata: ObjectMeta,
    /// Resource type (e.g., "pods", "deployments")
    resource_type: String,
    /// Full resource value
    value: Value,
}

/// Cascade deletion options
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteOptions {
    /// Deletion propagation policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub propagation_policy: Option<DeletionPropagation>,

    /// Grace period seconds before deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grace_period_seconds: Option<i64>,

    /// Preconditions for deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preconditions: Option<Preconditions>,

    /// Whether to orphan dependents (deprecated, use propagation_policy)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orphan_dependents: Option<bool>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Preconditions {
    /// UID must match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    /// ResourceVersion must match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            propagation_policy: Some(DeletionPropagation::Background),
            grace_period_seconds: Some(30),
            preconditions: None,
            orphan_dependents: None,
        }
    }
}

/// Map K8s Kind names to their plural storage resource names.
/// K8s uses discovery API for this; we use a static mapping.
fn kind_to_plural(kind: &str) -> &str {
    match kind {
        "Pod" => "pods",
        "Service" => "services",
        "Endpoints" => "endpoints",
        "EndpointSlice" => "endpointslices",
        "Namespace" => "namespaces",
        "Node" => "nodes",
        "ConfigMap" => "configmaps",
        "Secret" => "secrets",
        "ServiceAccount" => "serviceaccounts",
        "Deployment" => "deployments",
        "ReplicaSet" => "replicasets",
        "StatefulSet" => "statefulsets",
        "DaemonSet" => "daemonsets",
        "ReplicationController" => "replicationcontrollers",
        "Job" => "jobs",
        "CronJob" => "cronjobs",
        "Ingress" => "ingresses",
        "NetworkPolicy" => "networkpolicies",
        "PersistentVolumeClaim" => "persistentvolumeclaims",
        "PersistentVolume" => "persistentvolumes",
        "StorageClass" => "storageclasses",
        "ClusterRole" => "clusterroles",
        "ClusterRoleBinding" => "clusterrolebindings",
        "Role" => "roles",
        "RoleBinding" => "rolebindings",
        "CustomResourceDefinition" => "customresourcedefinitions",
        "ControllerRevision" => "controllerrevisions",
        "HorizontalPodAutoscaler" => "horizontalpodautoscalers",
        "PodDisruptionBudget" => "poddisruptionbudgets",
        "ResourceQuota" => "resourcequotas",
        "LimitRange" => "limitranges",
        _ => {
            // Fallback: lowercase + "s" (works for many K8s kinds)
            // This is imperfect but better than failing
            tracing::warn!("GC: unknown kind '{}', using lowercase+s fallback", kind);
            // Return a static str — caller should handle the fallback case
            // We can't return a dynamically constructed string as &str,
            // so return an empty string to signal "unknown"
            ""
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::OwnerReference;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_deletion_propagation_policy() {
        let gc = GarbageCollector::new(Arc::new(MemoryStorage::new()));

        // Test foreground deletion
        let mut metadata = ObjectMeta::new("test");
        metadata.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Foreground
        );

        // Test orphan deletion
        metadata.finalizers = Some(vec!["orphan".to_string()]);
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Orphan
        );

        // Test default (background)
        metadata.finalizers = None;
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Background
        );
    }

    #[test]
    fn test_owner_reference_creation() {
        let owner_ref = OwnerReference::new("v1", "Pod", "my-pod", "abc-123")
            .with_controller(true)
            .with_block_owner_deletion(true);

        assert_eq!(owner_ref.kind, "Pod");
        assert_eq!(owner_ref.controller, Some(true));
        assert_eq!(owner_ref.block_owner_deletion, Some(true));
    }

    #[test]
    fn test_metadata_finalizer_helpers() {
        let mut metadata = ObjectMeta::new("test");

        // Test add_finalizer
        metadata.add_finalizer("my-finalizer".to_string());
        assert!(metadata.has_finalizers());
        assert_eq!(metadata.finalizers.as_ref().unwrap().len(), 1);

        // Test idempotent add
        metadata.add_finalizer("my-finalizer".to_string());
        assert_eq!(metadata.finalizers.as_ref().unwrap().len(), 1);

        // Test remove_finalizer
        metadata.remove_finalizer("my-finalizer");
        assert!(!metadata.has_finalizers());
    }

    /// Foreground deletion must NOT remove the `foregroundDeletion` finalizer
    /// (and thus must not delete the owner) while any blocking dependent still
    /// exists in storage.
    ///
    /// Reproduces `[sig-api-machinery] Garbage collector should keep the rc
    /// around until all its pods are deleted if the deleteOptions says so`
    /// (garbage_collector.go:711), which deletes the rc with
    /// PropagationPolicy=Foreground and then asserts there are zero pods once
    /// the rc is gone. The previous code removed the finalizer in the same pass
    /// it issued the dependent deletes — gated only on the delete calls
    /// returning, not on the dependents actually being gone — so a stale
    /// snapshot (pods still draining or created after the snapshot) let the rc
    /// be deleted while pods remained. `still_has_dependents` is the gate.
    #[tokio::test]
    async fn still_has_dependents_blocks_until_dependent_gone() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let owner_uid = "rc-foreground-uid";

        let mut pod_meta = ObjectMeta::new("dependent-pod");
        pod_meta.namespace = Some("default".to_string());
        pod_meta.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: "simpletest.rc".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: None,
            status: None,
        };
        let pod_key = "/registry/pods/default/dependent-pod";
        storage.create(pod_key, &pod).await.unwrap();

        assert!(
            gc.still_has_dependents(owner_uid).await.unwrap(),
            "owner with an owned pod still in storage must report remaining dependents"
        );

        // Pod drained — gate must clear so the owner can be finalized/deleted.
        storage.delete(pod_key).await.unwrap();
        assert!(
            !gc.still_has_dependents(owner_uid).await.unwrap(),
            "once the owned pod is gone, the owner must report no remaining dependents"
        );
    }

    /// Once all dependents drain, the foreground gate must clear: the owner's
    /// `foregroundDeletion` finalizer is removed and the owner deleted. Guards
    /// the gate against deadlocking the happy path.
    #[tokio::test]
    async fn foreground_owner_deleted_after_dependents_drain() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let owner_uid = "rc-fg-complete-uid";

        // Owner rc: being foreground-deleted (deletionTimestamp + finalizer).
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        let rc = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 1},
        });
        let rc_key = "/registry/replicationcontrollers/default/simpletest.rc";
        storage.create(rc_key, &rc).await.unwrap();

        // One pod owned solely by the rc.
        let mut pod_meta = ObjectMeta::new("rc-pod");
        pod_meta.namespace = Some("default".to_string());
        pod_meta.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: "simpletest.rc".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: None,
            status: None,
        };
        storage
            .create("/registry/pods/default/rc-pod", &pod)
            .await
            .unwrap();

        // First scan: deletes the pod, gate clears (0 dependents), finalizer
        // removed, rc deleted — all in one scan since the snapshot is complete.
        gc.scan_and_collect().await.unwrap();

        let pods: Vec<Pod> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "dependent pod must be deleted, {} left",
            pods.len()
        );

        let rc_after: rusternetes_common::Result<Value> = storage.get(rc_key).await;
        assert!(
            rc_after.is_err(),
            "rc must be deleted once its dependents are gone (foreground complete)"
        );
    }

    /// The exact Bug-B race: an in-flight pod the RC controller created just
    /// before observing the owner's deletion lands AFTER the foreground delete
    /// pass took its snapshot. The owner's `foregroundDeletion` finalizer must
    /// NOT be removed (owner must NOT be deleted) while that pod still exists —
    /// only once it drains. This is what
    /// `[sig-api-machinery] Garbage collector should keep the rc around until
    /// all its pods are deleted` (garbage_collector.go:711) asserts; the old
    /// code removed the finalizer in the same pass and deleted the rc with pods
    /// still present.
    #[tokio::test]
    async fn foreground_gate_holds_for_inflight_pod_after_delete_pass() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());
        let owner_uid = "rc-fg-inflight-uid";

        // rc being foreground-deleted.
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        let rc_key = "/registry/replicationcontrollers/default/simpletest.rc";
        let rc_val = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 1},
        });
        storage.create(rc_key, &rc_val).await.unwrap();

        // Helper: a pod owned solely by the rc.
        let mk_pod = |name: &str| Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some("default".to_string());
                m.owner_references = Some(vec![OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "ReplicationController".to_string(),
                    name: "simpletest.rc".to_string(),
                    uid: owner_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);
                m
            },
            spec: None,
            status: None,
        };

        // pod_a is present when the foreground delete pass takes its snapshot.
        let pod_a_key = "/registry/pods/default/rc-pod-a";
        storage
            .create(pod_a_key, &mk_pod("rc-pod-a"))
            .await
            .unwrap();

        let rc_info = ResourceInfo {
            key: rc_key.to_string(),
            metadata: rc_meta.clone(),
            resource_type: "replicationcontrollers".to_string(),
            value: rc_val.clone(),
        };
        let empty_dep_map: HashMap<String, Vec<String>> = HashMap::new();

        // 1) Foreground delete pass: deletes the dependents it saw (pod_a).
        gc.delete_dependents_foreground(&rc_info, &empty_dep_map)
            .await
            .unwrap();
        assert!(
            storage.get::<Value>(pod_a_key).await.is_err(),
            "pod_a should have been deleted by the foreground pass"
        );

        // 2) An in-flight pod the RC controller created before noticing the
        //    deletion lands AFTER the delete pass's snapshot.
        let pod_b_key = "/registry/pods/default/rc-pod-b";
        storage
            .create(pod_b_key, &mk_pod("rc-pod-b"))
            .await
            .unwrap();

        // 3) GATE (the protection this pass provides): a dependent still exists,
        //    so the foreground finalizer must NOT be removed and the rc must be
        //    retained. The old code removed it here and deleted the rc.
        assert!(
            gc.still_has_dependents(owner_uid).await.unwrap(),
            "gate must report remaining dependents while the in-flight pod exists"
        );
        assert!(
            storage.get::<Value>(rc_key).await.is_ok(),
            "rc must be retained while the in-flight pod_b still exists"
        );

        // 4) pod_b drains; a subsequent scan deletes any remaining dependent and,
        //    finding the gate clear, finally collects the rc.
        storage.delete(pod_b_key).await.unwrap();
        gc.scan_and_collect().await.unwrap();
        assert!(
            storage.get::<Value>(rc_key).await.is_err(),
            "rc must be collected once every dependent has drained"
        );
    }

    /// A single GC scan must remove an orphan whose owner is already gone.
    /// Conformance tests for orphan pod cleanup observe per-cycle latency,
    /// so the previous "2-scan grace" gating added a full reconcile cycle
    /// of wait before any orphan was reaped. `delete_orphan` already re-reads
    /// the owner from storage as a race guard, so the second scan was
    /// unnecessary and observably slow.
    #[tokio::test]
    async fn test_orphan_deleted_on_first_scan() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let mut m = ObjectMeta::new("orphan-pod");
        m.namespace = Some("default".to_string());
        m.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: "missing-rs".to_string(),
            uid: "missing-rs-uid-never-existed".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: m,
            spec: None,
            status: None,
        };
        storage
            .create("/registry/pods/default/orphan-pod", &pod)
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        let pods: Vec<Pod> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "orphan must be deleted on the first GC scan, got {} remaining",
            pods.len()
        );
    }
}
