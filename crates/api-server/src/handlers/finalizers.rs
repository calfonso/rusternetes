use chrono::Utc;
use rusternetes_common::Result;
use rusternetes_storage::Storage;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, info};

/// Handle deletion of a resource that may have finalizers.
///
/// This function implements the Kubernetes finalizer protocol:
/// 1. If the resource has finalizers AND does NOT have a deletionTimestamp:
///    - Set deletionTimestamp to current time
///    - Update the resource in storage
///    - Return Ok(true) to indicate the resource was marked for deletion
/// 2. If the resource has finalizers AND has a deletionTimestamp:
///    - Do nothing (wait for controllers to remove finalizers)
///    - Return Ok(true) to indicate the resource is being finalized
/// 3. If the resource has NO finalizers (or empty finalizers list):
///    - Delete the resource from storage immediately
///    - Return Ok(false) to indicate the resource was deleted
///
/// # Arguments
///
/// * `storage` - The storage backend
/// * `key` - The storage key for the resource
/// * `resource` - The resource to potentially delete
///
/// # Returns
///
/// * `Ok(true)` - Resource has finalizers and was marked for deletion or is being finalized
/// * `Ok(false)` - Resource had no finalizers and was deleted from storage
/// * `Err(_)` - An error occurred
///
/// # Example
///
/// ```no_run
/// use rusternetes_api_server::handlers::finalizers::handle_delete_with_finalizers;
/// use rusternetes_common::resources::Pod;
/// use rusternetes_common::Result;
/// use rusternetes_storage::Storage;
/// use tracing::info;
///
/// async fn delete_pod<S: Storage>(storage: &S, key: &str) -> Result<()> {
///     // Get the resource
///     let pod: Pod = storage.get(key).await?;
///
///     // Handle deletion with finalizers
///     let marked_for_deletion = handle_delete_with_finalizers(
///         storage,
///         key,
///         &pod,
///     ).await?;
///
///     if marked_for_deletion {
///         info!("Pod marked for deletion, waiting for finalizers to be removed");
///     } else {
///         info!("Pod deleted immediately (no finalizers)");
///     }
///
///     Ok(())
/// }
/// ```
pub async fn handle_delete_with_finalizers<S, T>(
    storage: &S,
    key: &str,
    resource: &T,
) -> Result<bool>
where
    S: Storage,
    T: HasMetadata + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    handle_delete_with_finalizers_and_propagation(storage, key, resource, None).await
}

/// Handle deletion with propagation policy support.
/// When propagation_policy is "Foreground", adds the "foregroundDeletion" finalizer
/// so the garbage collector knows to delete dependents before the owner.
/// When propagation_policy is "Orphan", adds the "orphan" finalizer so dependents
/// are not deleted.
pub async fn handle_delete_with_finalizers_and_propagation<S, T>(
    storage: &S,
    key: &str,
    resource: &T,
    propagation_policy: Option<&str>,
) -> Result<bool>
where
    S: Storage,
    T: HasMetadata + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    let metadata = resource.metadata();

    // If already marked for deletion, handle as before
    if metadata.deletion_timestamp.is_some() {
        let has_finalizers = metadata.finalizers.as_ref().is_some_and(|f| !f.is_empty());

        if has_finalizers {
            debug!(
                "Resource {} already marked for deletion at {:?}, waiting for finalizers to be removed",
                key, metadata.deletion_timestamp
            );
            info!(
                "Resource {} has {} finalizers remaining: {:?}",
                key,
                metadata.finalizers.as_ref().unwrap().len(),
                metadata.finalizers.as_ref().unwrap()
            );
            return Ok(true);
        } else {
            // No finalizers left, delete now
            debug!("Resource {} has no finalizers remaining, deleting", key);
            storage.delete(key).await?;
            return Ok(false);
        }
    }

    // Not yet marked for deletion — apply propagation policy finalizers first
    let mut updated_resource = resource.clone();
    let meta = updated_resource.metadata_mut();

    // Add propagation policy finalizer if needed
    match propagation_policy {
        Some("Foreground") => {
            let finalizers = meta.finalizers.get_or_insert_with(Vec::new);
            if !finalizers.contains(&"foregroundDeletion".to_string()) {
                finalizers.push("foregroundDeletion".to_string());
                info!("Added foregroundDeletion finalizer to {}", key);
            }
        }
        Some("Orphan") => {
            let finalizers = meta.finalizers.get_or_insert_with(Vec::new);
            if !finalizers.contains(&"orphan".to_string()) {
                finalizers.push("orphan".to_string());
                info!("Added orphan finalizer to {}", key);
            }
        }
        _ => {
            // Background or unspecified — no extra finalizer
        }
    }

    // Check if the resource has finalizers (including any we just added)
    let has_finalizers = meta.finalizers.as_ref().is_some_and(|f| !f.is_empty());

    if !has_finalizers {
        // No finalizers - delete immediately
        debug!("Resource {} has no finalizers, deleting immediately", key);
        storage.delete(key).await?;
        return Ok(false);
    }

    // Resource has finalizers — set deletionTimestamp and update in storage
    meta.deletion_timestamp = Some(Utc::now());

    info!(
        "Resource {} marked for deletion with finalizers: {:?}",
        key, meta.finalizers
    );

    storage.update(key, &updated_resource).await?;

    Ok(true)
}

/// Trait for resources that have metadata with finalizers.
/// This allows the handle_delete_with_finalizers function to work with any
/// Kubernetes resource type.
pub trait HasMetadata {
    /// Get an immutable reference to the resource's metadata
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta;

    /// Get a mutable reference to the resource's metadata
    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta;
}

// Implement HasMetadata for common resource types
// Note: This can be extended with a macro if needed for many types

impl HasMetadata for rusternetes_common::resources::Namespace {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Pod {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Deployment {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Service {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ConfigMap {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Secret {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ServiceAccount {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicaSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::DaemonSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::StatefulSet {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Job {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CronJob {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PersistentVolume {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PersistentVolumeClaim {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::StorageClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Ingress {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::IngressClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::NetworkPolicy {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ResourceQuota {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::LimitRange {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PodDisruptionBudget {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::HorizontalPodAutoscaler {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Node {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshot {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshotClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeSnapshotContent {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSIDriver {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSINode {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CSIStorageCapacity {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeAttachment {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::VolumeAttributesClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingWebhookConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::MutatingWebhookConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingAdmissionPolicy {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ValidatingAdmissionPolicyBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CertificateSigningRequest {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::FlowSchema {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PriorityLevelConfiguration {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

// NOTE: DRA resources (ResourceClaim, ResourceClaimTemplate, DeviceClass, ResourceSlice)
// use a different ObjectMeta type (rusternetes_common::resources::dra::ObjectMeta)
// which is incompatible with rusternetes_common::types::ObjectMeta.
// Therefore, we cannot implement HasMetadata for DRA resources, and they do not support finalizers.

impl HasMetadata for rusternetes_common::resources::Role {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::RoleBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ClusterRole {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ClusterRoleBinding {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PodTemplate {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ControllerRevision {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ServiceCIDR {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::IPAddress {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CustomResourceDefinition {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Event {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::PriorityClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Lease {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::RuntimeClass {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::EndpointSlice {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Endpoints {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicationController {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::CustomResource {
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        &mut self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Pod, PodSpec};
    use rusternetes_storage::memory::MemoryStorage;

    fn make_test_pod(name: &str) -> Pod {
        let spec = PodSpec {
            containers: vec![],
            init_containers: None,
            ephemeral_containers: None,
            volumes: None,
            restart_policy: None,
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority: None,
            priority_class_name: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
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
        };
        let mut pod = Pod::new(name, spec);
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.ensure_uid();
        pod.metadata.ensure_creation_timestamp();
        pod
    }

    #[tokio::test]
    async fn test_delete_without_finalizers() {
        let storage = MemoryStorage::new();
        let pod = make_test_pod("test-pod");
        let key = "test/pods/default/test-pod";

        storage.create(key, &pod).await.unwrap();

        let deleted = handle_delete_with_finalizers(&storage, key, &pod)
            .await
            .unwrap();

        assert!(
            !deleted,
            "Resource without finalizers should be deleted immediately"
        );

        let result = storage.get::<Pod>(key).await;
        assert!(result.is_err(), "Resource should be deleted from storage");
    }

    #[tokio::test]
    async fn test_delete_with_finalizers() {
        let storage = MemoryStorage::new();
        let mut pod = make_test_pod("test-pod-finalizers");
        pod.metadata.finalizers = Some(vec!["test.finalizer.io".to_string()]);
        let key = "test/pods/default/test-pod-finalizers";

        storage.create(key, &pod).await.unwrap();

        let marked = handle_delete_with_finalizers(&storage, key, &pod)
            .await
            .unwrap();
        assert!(
            marked,
            "Resource with finalizers should be marked for deletion"
        );

        let updated_pod: Pod = storage.get(key).await.unwrap();
        assert!(
            updated_pod.metadata.deletion_timestamp.is_some(),
            "Resource should have deletionTimestamp"
        );
        assert_eq!(
            updated_pod.metadata.finalizers,
            Some(vec!["test.finalizer.io".to_string()]),
            "Finalizers should still be present"
        );

        // Second delete should also return marked (no-op)
        let marked_again = handle_delete_with_finalizers(&storage, key, &updated_pod)
            .await
            .unwrap();
        assert!(marked_again, "Resource should still be marked for deletion");

        storage.delete(key).await.unwrap();
    }

    #[tokio::test]
    async fn test_finalizer_removed_then_deleted() {
        let storage = MemoryStorage::new();
        let mut pod = make_test_pod("test-pod-remove-finalizer");
        pod.metadata.finalizers = Some(vec!["test.finalizer.io".to_string()]);
        let key = "test/pods/default/test-pod-remove-finalizer";

        storage.create(key, &pod).await.unwrap();

        let marked = handle_delete_with_finalizers(&storage, key, &pod)
            .await
            .unwrap();
        assert!(marked);

        // Simulate controller removing finalizer
        let mut updated_pod: Pod = storage.get(key).await.unwrap();
        updated_pod.metadata.finalizers = None;
        storage.update(key, &updated_pod).await.unwrap();

        let deleted = handle_delete_with_finalizers(&storage, key, &updated_pod)
            .await
            .unwrap();
        assert!(!deleted, "Resource without finalizers should be deleted");

        let result = storage.get::<Pod>(key).await;
        assert!(result.is_err(), "Resource should be deleted from storage");
    }
}
