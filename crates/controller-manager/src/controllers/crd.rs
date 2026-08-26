use anyhow::{anyhow, Result};
use rusternetes_common::resources::{
    CustomResourceDefinition, CustomResourceDefinitionCondition, CustomResourceDefinitionStatus,
};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// CRDController manages CustomResourceDefinition resources.
///
/// In a production Kubernetes cluster, the CRD controller:
/// 1. Validates CRD specifications
/// 2. Ensures API server can serve the custom resources
/// 3. Updates CRD status with conditions and accepted names
/// 4. Manages versioning and schema validation
///
/// For conformance, this controller provides:
/// - CRD validation (names, versions, schema)
/// - Status updates with conditions
/// - Stored versions tracking
pub struct CRDController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> CRDController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting CRD controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("customresourcedefinitions", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
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
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
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
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let name = key
                .strip_prefix("customresourcedefinitions/")
                .unwrap_or(&key);
            let storage_key = build_key("customresourcedefinitions", None, name);
            match self
                .storage
                .get::<CustomResourceDefinition>(&storage_key)
                .await
            {
                Ok(resource) => match self.reconcile_crd(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<CustomResourceDefinition>("/registry/customresourcedefinitions/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = format!("customresourcedefinitions/{}", item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!(
                    "Failed to list customresourcedefinitions for enqueue: {}",
                    e
                );
            }
        }
    }

    /// Main reconciliation loop - processes all CRD resources
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting CRD reconciliation");

        // List all CRDs (CRDs are cluster-scoped, not namespaced)
        let crds: Vec<CustomResourceDefinition> = self
            .storage
            .list("/registry/customresourcedefinitions/")
            .await?;

        debug!(
            "Found {} custom resource definitions to reconcile",
            crds.len()
        );

        for crd in crds {
            if let Err(e) = self.reconcile_crd(&crd).await {
                error!("Failed to reconcile CRD {}: {}", &crd.metadata.name, e);
            }
        }

        Ok(())
    }

    /// Reconcile a single CustomResourceDefinition
    async fn reconcile_crd(&self, crd: &CustomResourceDefinition) -> Result<()> {
        let crd_name = &crd.metadata.name;
        debug!("Reconciling CRD {}", crd_name);

        // Handle CRD deletion with finalizer cleanup.
        // K8s has a CRD finalizer controller that processes the
        // "customresourcecleanup.apiextensions.k8s.io" finalizer by deleting
        // all custom resource instances, then removing the finalizer so the
        // CRD can be garbage collected.
        if crd.metadata.deletion_timestamp.is_some() {
            return self.handle_crd_deletion(crd).await;
        }

        // Validate the CRD spec
        if let Err(e) = self.validate_crd_spec(crd) {
            warn!("CRD {} validation failed: {}", crd_name, e);
            // In production, would update CRD status with error condition
            return Ok(());
        }

        // Check if CRD already has established status
        if let Some(status) = &crd.status {
            if let Some(conditions) = &status.conditions {
                for condition in conditions {
                    if condition.type_ == "Established" && condition.status == "True" {
                        debug!("CRD {} is already established", crd_name);
                        return Ok(());
                    }
                }
            }
        }

        debug!("CRD {} reconciled successfully", crd_name);

        Ok(())
    }

    /// Handle CRD deletion: process finalizers by cleaning up custom resources,
    /// then remove the finalizer and delete the CRD.
    ///
    /// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/finalizer/crd_finalizer.go
    async fn handle_crd_deletion(&self, crd: &CustomResourceDefinition) -> Result<()> {
        let crd_name = &crd.metadata.name;
        let finalizers = crd.metadata.finalizers.as_deref().unwrap_or(&[]);

        // Check for the cleanup finalizer
        let cleanup_finalizer = "customresourcecleanup.apiextensions.k8s.io";
        if !finalizers.contains(&cleanup_finalizer.to_string()) {
            debug!("CRD {} has no cleanup finalizer, nothing to do", crd_name);
            return Ok(());
        }

        info!("Processing cleanup finalizer for CRD {}", crd_name);

        // Delete all custom resource instances for this CRD.
        // CRs are stored at /apis/{group}/{version}/{plural}/{ns}/{name}
        for version in &crd.spec.versions {
            let cr_prefix = format!(
                "/apis/{}/{}/{}/",
                crd.spec.group, version.name, crd.spec.names.plural
            );

            let crs: Vec<serde_json::Value> =
                self.storage.list(&cr_prefix).await.unwrap_or_default();

            for cr in &crs {
                let cr_name = cr
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let cr_ns = cr
                    .pointer("/metadata/namespace")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let cr_key = if cr_ns.is_empty() {
                    format!(
                        "/apis/{}/{}/{}/{}",
                        crd.spec.group, version.name, crd.spec.names.plural, cr_name
                    )
                } else {
                    format!(
                        "/apis/{}/{}/{}/{}/{}",
                        crd.spec.group, version.name, crd.spec.names.plural, cr_ns, cr_name
                    )
                };

                if let Err(e) = self.storage.delete(&cr_key).await {
                    warn!("Failed to delete CR {} for CRD {}: {}", cr_key, crd_name, e);
                } else {
                    debug!("Deleted CR {} for CRD {}", cr_key, crd_name);
                }
            }
        }

        // Remove the cleanup finalizer from the CRD
        let crd_key = build_key("customresourcedefinitions", None, crd_name);
        match self.storage.get::<serde_json::Value>(&crd_key).await {
            Ok(mut crd_value) => {
                // Remove the finalizer
                if let Some(meta) = crd_value
                    .get_mut("metadata")
                    .and_then(|m| m.as_object_mut())
                {
                    if let Some(fins) = meta.get_mut("finalizers").and_then(|f| f.as_array_mut()) {
                        fins.retain(|f| f.as_str() != Some(cleanup_finalizer));
                        if fins.is_empty() {
                            meta.remove("finalizers");
                        }
                    }
                }

                // Check if any finalizers remain
                let has_finalizers = crd_value
                    .pointer("/metadata/finalizers")
                    .and_then(|f| f.as_array())
                    .is_some_and(|f| !f.is_empty());

                if has_finalizers {
                    // Other finalizers remain — just update to remove ours
                    if let Err(e) = self.storage.update(&crd_key, &crd_value).await {
                        error!(
                            "Failed to update CRD {} after removing finalizer: {}",
                            crd_name, e
                        );
                        return Err(anyhow!("Failed to update CRD: {}", e));
                    }
                    info!(
                        "Removed cleanup finalizer from CRD {}, other finalizers remain",
                        crd_name
                    );
                } else {
                    // No finalizers left — delete the CRD
                    if let Err(e) = self.storage.delete(&crd_key).await {
                        error!("Failed to delete CRD {}: {}", crd_name, e);
                        return Err(anyhow!("Failed to delete CRD: {}", e));
                    }
                    info!("CRD {} fully deleted after finalizer cleanup", crd_name);
                }
            }
            Err(e) => {
                // CRD already deleted
                debug!("CRD {} already deleted: {}", crd_name, e);
            }
        }

        Ok(())
    }

    /// Validate CRD specification
    fn validate_crd_spec(&self, crd: &CustomResourceDefinition) -> Result<()> {
        let spec = &crd.spec;

        // Validate group name
        if spec.group.is_empty() {
            return Err(anyhow!("CRD group cannot be empty"));
        }

        // Validate names
        if spec.names.plural.is_empty() {
            return Err(anyhow!("CRD plural name cannot be empty"));
        }

        if spec.names.kind.is_empty() {
            return Err(anyhow!("CRD kind cannot be empty"));
        }

        // Validate CRD has at least one version
        if spec.versions.is_empty() {
            return Err(anyhow!("CRD must have at least one version"));
        }

        // Validate exactly one version is marked as storage version
        let storage_versions: Vec<_> = spec.versions.iter().filter(|v| v.storage).collect();

        if storage_versions.is_empty() {
            return Err(anyhow!("CRD must have exactly one storage version"));
        }

        if storage_versions.len() > 1 {
            return Err(anyhow!("CRD can only have one storage version"));
        }

        // Validate version names are unique
        let mut version_names = HashSet::new();
        for version in &spec.versions {
            if version.name.is_empty() {
                return Err(anyhow!("CRD version name cannot be empty"));
            }

            if !version_names.insert(&version.name) {
                return Err(anyhow!(
                    "CRD version names must be unique: {}",
                    version.name
                ));
            }

            // At least one version must be served
            // (This is checked collectively below)
        }

        // Validate at least one version is served
        let has_served_version = spec.versions.iter().any(|v| v.served);
        if !has_served_version {
            return Err(anyhow!("CRD must have at least one served version"));
        }

        // Validate metadata name format: <plural>.<group>
        let expected_name = format!("{}.{}", spec.names.plural, spec.group);
        if crd.metadata.name != expected_name {
            return Err(anyhow!(
                "CRD metadata.name must be '<plural>.<group>', expected '{}' but got '{}'",
                expected_name,
                crd.metadata.name
            ));
        }

        Ok(())
    }

    /// Create or update CRD status with conditions
    #[allow(dead_code)]
    pub async fn update_crd_status(
        &self,
        crd_name: &str,
        established: bool,
        names_accepted: bool,
    ) -> Result<()> {
        let crd_key = format!("/registry/customresourcedefinitions/{}", crd_name);
        let mut crd: CustomResourceDefinition = self.storage.get(&crd_key).await?;
        let prev_status = crd.status.clone();

        // Capture prior Established / NamesAccepted timestamps so we can carry
        // them forward when the condition's .status hasn't actually changed.
        // K8s convention: last_transition_time updates ONLY on a real status
        // transition. Re-stamping it on every reconcile would emit a MODIFIED
        // watch event every resync interval (30s here) — a controller hot-loop.
        let lookup_prev_time = |target_type: &str, target_status: &str| -> Option<String> {
            prev_status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .and_then(|cs| {
                    cs.iter()
                        .find(|c| c.type_ == target_type && c.status == target_status)
                        .and_then(|c| c.last_transition_time.clone())
                })
        };

        // Preserve existing conditions and only update/add Established and NamesAccepted.
        // Other conditions (e.g., set by tests or external controllers) must be kept.
        let mut conditions: Vec<CustomResourceDefinitionCondition> = crd
            .status
            .as_ref()
            .and_then(|s| s.conditions.clone())
            .unwrap_or_default();

        // Remove existing Established and NamesAccepted conditions (we'll re-add them)
        conditions.retain(|c| c.type_ != "Established" && c.type_ != "NamesAccepted");

        let now_rfc3339 = chrono::Utc::now().to_rfc3339();

        if names_accepted {
            let last_transition_time =
                lookup_prev_time("NamesAccepted", "True").unwrap_or_else(|| now_rfc3339.clone());
            conditions.push(CustomResourceDefinitionCondition {
                type_: "NamesAccepted".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(last_transition_time),
                reason: Some("NoConflicts".to_string()),
                message: Some("no conflicts found".to_string()),
            });
        }

        if established {
            let last_transition_time =
                lookup_prev_time("Established", "True").unwrap_or_else(|| now_rfc3339.clone());
            conditions.push(CustomResourceDefinitionCondition {
                type_: "Established".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(last_transition_time),
                reason: Some("InitialNamesAccepted".to_string()),
                message: Some("the initial names have been accepted".to_string()),
            });
        }

        // Collect stored versions
        let stored_versions: Vec<String> = crd
            .spec
            .versions
            .iter()
            .filter(|v| v.storage)
            .map(|v| v.name.clone())
            .collect();

        let new_status = CustomResourceDefinitionStatus {
            conditions: Some(conditions),
            accepted_names: Some(crd.spec.names.clone()),
            stored_versions: Some(stored_versions),
        };

        // Skip the write when nothing actually changed. Compare via canonical
        // JSON so optional field ordering doesn't trigger spurious updates.
        let old_v = serde_json::to_value(&prev_status).ok();
        let new_v = serde_json::to_value(Some(&new_status)).ok();
        if old_v == new_v {
            return Ok(());
        }

        crd.status = Some(new_status);
        self.storage.update(&crd_key, &crd).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, ResourceScope,
    };
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::memory::MemoryStorage;

    fn create_test_crd(name: &str, group: &str, plural: &str) -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: ObjectMeta::new(format!("{}.{}", plural, group)),
            spec: CustomResourceDefinitionSpec {
                group: group.to_string(),
                names: CustomResourceDefinitionNames {
                    plural: plural.to_string(),
                    singular: Some(name.to_string()),
                    kind: format!("{}Kind", name),
                    short_names: None,
                    categories: None,
                    list_kind: Some(format!("{}List", name)),
                },
                scope: ResourceScope::Namespaced,
                versions: vec![CustomResourceDefinitionVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: true,
                    deprecated: None,
                    deprecation_warning: None,
                    schema: None,
                    subresources: None,
                    additional_printer_columns: None,
                }],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn test_validate_valid_crd() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        assert!(controller.validate_crd_spec(&crd).is_ok());
    }

    #[tokio::test]
    async fn test_validate_empty_group() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "", "crontabs");
        crd.metadata.name = "crontabs.".to_string();
        crd.spec.group = "".to_string();

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("group"));
    }

    #[tokio::test]
    async fn test_validate_empty_plural() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "");
        crd.metadata.name = ".stable.example.com".to_string();

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("plural"));
    }

    #[tokio::test]
    async fn test_validate_empty_kind() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        crd.spec.names.kind = "".to_string();

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("kind"));
    }

    #[tokio::test]
    async fn test_validate_no_versions() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        crd.spec.versions = vec![];

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one version"));
    }

    #[tokio::test]
    async fn test_validate_no_storage_version() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        crd.spec.versions[0].storage = false;

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("one storage version"));
    }

    #[tokio::test]
    async fn test_validate_multiple_storage_versions() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        crd.spec.versions.push(CustomResourceDefinitionVersion {
            name: "v2".to_string(),
            served: true,
            storage: true, // Second storage version - invalid!
            deprecated: None,
            deprecation_warning: None,
            schema: None,
            subresources: None,
            additional_printer_columns: None,
        });

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only have one storage version"));
    }

    #[tokio::test]
    async fn test_validate_duplicate_version_names() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        crd.spec.versions.push(CustomResourceDefinitionVersion {
            name: "v1".to_string(), // Duplicate!
            served: true,
            storage: false,
            deprecated: None,
            deprecation_warning: None,
            schema: None,
            subresources: None,
            additional_printer_columns: None,
        });

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unique"));
    }

    #[tokio::test]
    async fn test_validate_no_served_version() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        crd.spec.versions[0].served = false;

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("served version"));
    }

    #[tokio::test]
    async fn test_validate_incorrect_metadata_name() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        crd.metadata.name = "wrong-name".to_string();

        let result = controller.validate_crd_spec(&crd);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("metadata.name"));
    }

    #[tokio::test]
    async fn test_validate_multiple_versions_valid() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        let mut crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        crd.spec.versions.push(CustomResourceDefinitionVersion {
            name: "v1beta1".to_string(),
            served: true,
            storage: false, // Not storage version
            deprecated: Some(true),
            deprecation_warning: Some("v1beta1 is deprecated, use v1".to_string()),
            schema: None,
            subresources: None,
            additional_printer_columns: None,
        });

        assert!(controller.validate_crd_spec(&crd).is_ok());
    }

    #[tokio::test]
    async fn test_reconcile_crd() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage.clone());

        let crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        let crd_key = format!("/registry/customresourcedefinitions/{}", crd.metadata.name);

        storage.create(&crd_key, &crd).await.unwrap();

        // Reconcile the CRD
        assert!(controller.reconcile_crd(&crd).await.is_ok());
    }

    #[tokio::test]
    async fn test_update_crd_status() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage.clone());

        let crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        let crd_key = format!("/registry/customresourcedefinitions/{}", crd.metadata.name);

        storage.create(&crd_key, &crd).await.unwrap();

        // Update status
        controller
            .update_crd_status(&crd.metadata.name, true, true)
            .await
            .unwrap();

        // Verify status was updated
        let updated_crd: CustomResourceDefinition = storage.get(&crd_key).await.unwrap();
        assert!(updated_crd.status.is_some());

        let status = updated_crd.status.unwrap();
        assert!(status.conditions.is_some());
        assert!(status.accepted_names.is_some());
        assert!(status.stored_versions.is_some());

        let conditions = status.conditions.unwrap();
        assert_eq!(conditions.len(), 2);

        // Check for Established and NamesAccepted conditions
        let has_established = conditions.iter().any(|c| c.type_ == "Established");
        let has_names_accepted = conditions.iter().any(|c| c.type_ == "NamesAccepted");
        assert!(has_established);
        assert!(has_names_accepted);
    }

    /// Regression test: update_crd_status must not churn last_transition_time
    /// on successive reconciles when the established/names_accepted state is
    /// unchanged. Without preservation, every 30s resync re-stamped Utc::now()
    /// and rewrote the CRD — emitting a MODIFIED watch event each cycle.
    #[tokio::test]
    async fn test_update_crd_status_idempotent_when_unchanged() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage.clone());

        let crd = create_test_crd("crontab", "stable.example.com", "crontabs");
        let crd_key = format!("/registry/customresourcedefinitions/{}", crd.metadata.name);
        storage.create(&crd_key, &crd).await.unwrap();

        // First reconcile establishes status + condition timestamps.
        controller
            .update_crd_status(&crd.metadata.name, true, true)
            .await
            .unwrap();
        let after_first: CustomResourceDefinition = storage.get(&crd_key).await.unwrap();
        let first_v: serde_json::Value = serde_json::to_value(after_first.status.as_ref()).unwrap();

        // Sleep enough that any Utc::now() on a re-write would advance past
        // the first call's timestamps.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Second update with identical inputs must NOT bump timestamps and
        // must NOT re-write the CRD.
        controller
            .update_crd_status(&crd.metadata.name, true, true)
            .await
            .unwrap();
        let after_second: CustomResourceDefinition = storage.get(&crd_key).await.unwrap();
        let second_v: serde_json::Value =
            serde_json::to_value(after_second.status.as_ref()).unwrap();

        assert_eq!(
            first_v, second_v,
            "CRD status (incl. condition.last_transition_time) must remain stable \
             on a no-op update — otherwise every resync writes the CRD and churns watchers."
        );
    }

    #[tokio::test]
    async fn test_reconcile_all_no_crds() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage);

        // Should not fail with no CRDs
        assert!(controller.reconcile_all().await.is_ok());
    }

    #[tokio::test]
    async fn test_reconcile_all_multiple_crds() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CRDController::new(storage.clone());

        // Create multiple CRDs
        let crds = vec![
            create_test_crd("crontab", "stable.example.com", "crontabs"),
            create_test_crd("backup", "storage.example.com", "backups"),
            create_test_crd("database", "apps.example.com", "databases"),
        ];

        for crd in &crds {
            let crd_key = format!("/registry/customresourcedefinitions/{}", crd.metadata.name);
            storage.create(&crd_key, crd).await.unwrap();
        }

        // Reconcile all CRDs
        assert!(controller.reconcile_all().await.is_ok());
    }
}
