use crate::resources::serde_helpers::empty_string_as_none;
use crate::resources::service_account::ObjectReference;
use crate::types::{ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// PersistentVolume represents a storage resource in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolume {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: PersistentVolumeSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PersistentVolumeStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeSpec {
    /// Storage capacity. Optional on the wire: upstream
    /// `PersistentVolumeSpec.Capacity` is `+optional` / `json:",omitempty"`,
    /// so a PV may be posted without it (e.g. the CSI status conformance
    /// test). Default to an empty map rather than failing to decode.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub capacity: HashMap<String, String>,

    /// Access modes. Optional on the wire (`+optional` upstream); default to
    /// an empty list so decode never fails on its absence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<PersistentVolumeAccessMode>,

    /// Reclaim policy
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub persistent_volume_reclaim_policy: Option<PersistentVolumeReclaimPolicy>,

    /// Storage class name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,

    /// Mount options
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mount_options: Option<Vec<String>>,

    /// Volume mode
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub volume_mode: Option<PersistentVolumeMode>,

    /// Node affinity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<VolumeNodeAffinity>,

    /// Claim reference (binding to a PVC)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_ref: Option<ObjectReference>,

    // Volume source fields (only one should be set, matching Kubernetes flat struct layout)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_path: Option<HostPathVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub nfs: Option<NFSVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub iscsi: Option<ISCSIVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub local: Option<LocalVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub csi: Option<CSIVolumeSource>,

    /// volumeAttributesClassName may be used to set the VolumeAttributesClass used by this claim
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_attributes_class_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostPathVolumeSource {
    pub path: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub r#type: Option<HostPathType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HostPathType {
    DirectoryOrCreate,
    Directory,
    FileOrCreate,
    File,
    Socket,
    CharDevice,
    BlockDevice,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NFSVolumeSource {
    pub server: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ISCSIVolumeSource {
    pub target_portal: String,
    pub iqn: String,
    pub lun: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iscsi_interface: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// iSCSI Target Portal List (other than the primary target_portal)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub portals: Option<Vec<String>>,
    /// whether to support iSCSI Discovery CHAP authentication
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chap_auth_discovery: Option<bool>,
    /// whether to support iSCSI Session CHAP authentication
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chap_auth_session: Option<bool>,
    /// CHAP Secret for iSCSI target and initiator authentication
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
    /// custom iSCSI Initiator Name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initiator_name: Option<String>,
}

/// SecretReference represents a Secret reference by name and namespace.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SecretReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalVolumeSource {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIVolumeSource {
    pub driver: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_handle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_attributes: Option<HashMap<String, String>>,

    // CSIPersistentVolumeSource secret references (PersistentVolume csi source).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub controller_publish_secret_ref: Option<SecretReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_stage_secret_ref: Option<SecretReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_publish_secret_ref: Option<SecretReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub controller_expand_secret_ref: Option<SecretReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_expand_secret_ref: Option<SecretReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PersistentVolumeAccessMode {
    ReadWriteOnce,
    ReadOnlyMany,
    ReadWriteMany,
    ReadWriteOncePod,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PersistentVolumeReclaimPolicy {
    Retain,
    Recycle,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PersistentVolumeMode {
    Filesystem,
    Block,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeNodeAffinity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<NodeSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelector {
    pub node_selector_terms: Vec<NodeSelectorTerm>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelectorTerm {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<NodeSelectorRequirement>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_fields: Option<Vec<NodeSelectorRequirement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelectorRequirement {
    pub key: String,
    pub operator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeStatus {
    #[serde(default)]
    pub phase: PersistentVolumePhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// lastPhaseTransitionTime is the time the phase transitioned from one to another
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_phase_transition_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum PersistentVolumePhase {
    #[default]
    Pending,
    Available,
    Bound,
    Released,
    Failed,
}

/// PersistentVolumeClaim is a request for storage by a user
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaim {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: PersistentVolumeClaimSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PersistentVolumeClaimStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaimSpec {
    /// Access modes
    #[serde(default)]
    pub access_modes: Vec<PersistentVolumeAccessMode>,

    /// Resource requirements
    #[serde(default)]
    pub resources: ResourceRequirements,

    /// Volume name (if binding to specific PV)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_name: Option<String>,

    /// Storage class name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,

    /// Volume mode
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub volume_mode: Option<PersistentVolumeMode>,

    /// Selector for PV
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,

    /// Data source
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_source: Option<TypedLocalObjectReference>,

    /// DataSourceRef specifies the object from which to populate the volume with data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_source_ref: Option<TypedObjectReference>,

    /// VolumeAttributesClassName may be used to set the VolumeAttributesClass used by this claim
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_attributes_class_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub limits: Option<HashMap<String, String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub requests: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<LabelSelectorRequirement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorRequirement {
    pub key: String,
    pub operator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypedLocalObjectReference {
    pub api_group: Option<String>,
    pub kind: String,
    pub name: String,
}

/// TypedObjectReference contains enough information to let you locate the typed referenced object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypedObjectReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,
    pub kind: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaimStatus {
    #[serde(default)]
    pub phase: PersistentVolumeClaimPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_modes: Option<Vec<PersistentVolumeAccessMode>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub capacity: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<PersistentVolumeClaimCondition>>,
    /// Allocated resources represents the resources allocated to the PVC
    /// Used during volume expansion to track the new size
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub allocated_resources: Option<HashMap<String, String>>,
    /// AllocatedResourceStatuses stores status of resource being resized for the given PVC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated_resource_statuses: Option<HashMap<String, String>>,
    /// Resize status indicates the state of volume resize operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resize_status: Option<PersistentVolumeClaimResizeStatus>,

    /// currentVolumeAttributesClassName is the current name of the VolumeAttributesClass
    /// the PVC is using
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_volume_attributes_class_name: Option<String>,

    /// modifyVolumeStatus represents the status object of ControllerModifyVolume operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modify_volume_status: Option<ModifyVolumeStatus>,
}

/// ModifyVolumeStatus represents the status object of ControllerModifyVolume operation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModifyVolumeStatus {
    /// targetVolumeAttributesClassName is the name of the VolumeAttributesClass the PVC currently being reconciled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_volume_attributes_class_name: Option<String>,

    /// status is the status of the ControllerModifyVolume operation
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum PersistentVolumeClaimPhase {
    #[default]
    Pending,
    Bound,
    Lost,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaimCondition {
    pub r#type: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_probe_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// PersistentVolumeClaimResizeStatus indicates the state of a resize operation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PersistentVolumeClaimResizeStatus {
    /// No resize operation in progress
    #[serde(rename = "")]
    None,
    /// Controller resize is in progress
    ControllerResizeInProgress,
    /// Controller resize has failed
    ControllerResizeFailed,
    /// Node resize is required
    NodeResizeRequired,
    /// Node resize is in progress
    NodeResizeInProgress,
    /// Node resize has failed
    NodeResizeFailed,
}

/// StorageClass describes the parameters for a class of storage
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageClass {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,

    /// Provisioner name
    pub provisioner: String,

    /// Parameters for the provisioner
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, String>>,

    /// Reclaim policy
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub reclaim_policy: Option<PersistentVolumeReclaimPolicy>,

    /// Volume binding mode
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub volume_binding_mode: Option<VolumeBindingMode>,

    /// Allowed topologies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_topologies: Option<Vec<TopologySelectorTerm>>,

    /// Allow volume expansion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_volume_expansion: Option<bool>,

    /// Dynamically provisioned PersistentVolumes of this storage class are
    /// created with these mountOptions (e.g., ["ro", "soft"])
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mount_options: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VolumeBindingMode {
    Immediate,
    WaitForFirstConsumer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologySelectorTerm {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_label_expressions: Option<Vec<TopologySelectorLabelRequirement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologySelectorLabelRequirement {
    pub key: String,
    pub values: Vec<String>,
}

/// VolumeSnapshot represents a snapshot of a volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshot {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: VolumeSnapshotSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<VolumeSnapshotStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotSpec {
    /// Source of the snapshot
    pub source: VolumeSnapshotSource,

    /// VolumeSnapshotClass name
    pub volume_snapshot_class_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotSource {
    /// Reference to PVC to snapshot
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistent_volume_claim_name: Option<String>,

    /// Reference to existing VolumeSnapshotContent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_snapshot_content_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotStatus {
    /// Bound VolumeSnapshotContent name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_volume_snapshot_content_name: Option<String>,

    /// Creation time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_time: Option<String>,

    /// Ready to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_to_use: Option<bool>,

    /// Restore size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_size: Option<String>,

    /// Error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<VolumeSnapshotError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotError {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// VolumeSnapshotClass describes the parameters for a class of volume snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotClass {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,

    /// Driver name (snapshotter)
    pub driver: String,

    /// Parameters for the driver
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, String>>,

    /// Deletion policy
    pub deletion_policy: DeletionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeletionPolicy {
    Delete,
    Retain,
}

/// VolumeSnapshotContent represents the actual snapshot data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotContent {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: VolumeSnapshotContentSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<VolumeSnapshotContentStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotContentSpec {
    /// Source of the snapshot
    pub source: VolumeSnapshotContentSource,

    /// Reference to VolumeSnapshot
    pub volume_snapshot_ref: ObjectReference,

    /// VolumeSnapshotClass name
    pub volume_snapshot_class_name: String,

    /// Deletion policy
    pub deletion_policy: DeletionPolicy,

    /// Driver name
    pub driver: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotContentSource {
    /// Snapshot handle (CSI snapshot ID)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_handle: Option<String>,

    /// Volume handle to snapshot from
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_handle: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotContentStatus {
    /// Snapshot handle
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_handle: Option<String>,

    /// Creation time (Unix timestamp in nanoseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_time: Option<i64>,

    /// Ready to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_to_use: Option<bool>,

    /// Restore size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_size: Option<i64>,

    /// Error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<VolumeSnapshotError>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pv_serialization() {
        let mut capacity = HashMap::new();
        capacity.insert("storage".to_string(), "10Gi".to_string());

        let pv = PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pv"),
            spec: PersistentVolumeSpec {
                capacity,
                host_path: Some(HostPathVolumeSource {
                    path: "/mnt/data".to_string(),
                    r#type: Some(HostPathType::DirectoryOrCreate),
                }),
                nfs: None,
                iscsi: None,
                local: None,
                csi: None,
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Retain),
                storage_class_name: Some("manual".to_string()),
                mount_options: None,
                volume_mode: Some(PersistentVolumeMode::Filesystem),
                node_affinity: None,
                claim_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Available,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            }),
        };

        let json = serde_json::to_string(&pv).unwrap();
        let _deserialized: PersistentVolume = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_pvc_serialization() {
        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "5Gi".to_string());

        let pvc = PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pvc").with_namespace("default"),
            spec: PersistentVolumeClaimSpec {
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                resources: ResourceRequirements {
                    limits: None,
                    requests: Some(requests),
                },
                volume_name: None,
                storage_class_name: Some("manual".to_string()),
                volume_mode: Some(PersistentVolumeMode::Filesystem),
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Pending,
                access_modes: None,
                capacity: None,
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            }),
        };

        let json = serde_json::to_string(&pvc).unwrap();
        let _deserialized: PersistentVolumeClaim = serde_json::from_str(&json).unwrap();
    }
}
