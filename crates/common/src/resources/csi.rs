use crate::resources::service_account::ObjectReference;
use crate::resources::volume::LabelSelector;
use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// CSIDriver represents information about a CSI volume driver installed on a cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIDriver {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: CSIDriverSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIDriverSpec {
    /// attachRequired indicates this CSI volume driver requires an attach operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_required: Option<bool>,

    /// podInfoOnMount indicates this CSI volume driver requires additional pod information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_info_on_mount: Option<bool>,

    /// fsGroupPolicy defines if the volume supports changing ownership and permission of the volume
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_group_policy: Option<FSGroupPolicy>,

    /// storageCapacity indicates that the CSI volume driver wants pod scheduling to consider storage capacity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_capacity: Option<bool>,

    /// volumeLifecycleModes defines what kind of volumes this CSI volume driver supports
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_lifecycle_modes: Option<Vec<VolumeLifecycleMode>>,

    /// tokenRequests indicates the CSI driver needs service account tokens
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_requests: Option<Vec<TokenRequest>>,

    /// requiresRepublish indicates the CSI driver wants NodePublishVolume to be periodically called
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_republish: Option<bool>,

    /// seLinuxMount specifies if the CSI driver supports "-o context" mount option
    #[serde(skip_serializing_if = "Option::is_none", rename = "seLinuxMount")]
    pub se_linux_mount: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_allocatable_update_period_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FSGroupPolicy {
    ReadWriteOnceWithFSType,
    File,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VolumeLifecycleMode {
    Persistent,
    Ephemeral,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRequest {
    /// audience is the intended audience of the token
    pub audience: String,

    /// expirationSeconds is the requested duration of validity of the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_seconds: Option<i64>,
}

/// CSINode holds information about all CSI drivers installed on a node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSINode {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: CSINodeSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSINodeSpec {
    /// drivers is a list of information of all CSI Drivers existing on a node
    #[serde(default, deserialize_with = "crate::deserialize_null_default")]
    pub drivers: Vec<CSINodeDriver>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSINodeDriver {
    /// name represents the name of the CSI driver
    #[serde(default)]
    pub name: String,

    /// nodeID of the node from the driver point of view
    #[serde(rename = "nodeID", default)]
    pub node_id: String,

    /// topologyKeys is the list of keys supported by the driver
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_keys: Option<Vec<String>>,

    /// allocatable represents the volume resources of a node that are available for scheduling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocatable: Option<VolumeNodeResources>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeNodeResources {
    /// count indicates the maximum number of unique volumes managed by the CSI driver
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<i32>,
}

/// VolumeAttachment captures the intent to attach or detach the specified volume to/from the specified node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachment {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: VolumeAttachmentSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<VolumeAttachmentStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachmentSpec {
    /// attacher indicates the name of the volume driver that MUST handle this request
    pub attacher: String,

    /// nodeName represents the node that the volume should be attached to
    pub node_name: String,

    /// source represents the volume that should be attached
    pub source: VolumeAttachmentSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachmentSource {
    /// persistentVolumeName represents the name of the persistent volume to attach
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistent_volume_name: Option<String>,

    /// inlineVolumeSpec contains all the information necessary to attach a persistent volume defined by a pod's inline VolumeSource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_volume_spec: Option<InlineVolumeSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InlineVolumeSpec {
    /// Standard Kubernetes inline volume specification
    /// This is a simplified version - in production would include full PersistentVolumeSpec
    #[serde(skip_serializing_if = "Option::is_none")]
    pub csi: Option<CSIVolumeSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIVolumeSource {
    /// driver is the name of the driver to use for this volume
    pub driver: String,

    /// volumeHandle is the unique volume name returned by the CSI volume plugin's CreateVolume
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_handle: Option<String>,

    /// readOnly value to pass to ControllerPublishVolumeRequest
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,

    /// fsType to mount
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,

    /// volumeAttributes of the volume to publish
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_attributes: Option<HashMap<String, String>>,

    /// nodePublishSecretRef is a reference to the secret object containing sensitive information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_publish_secret_ref: Option<ObjectReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachmentStatus {
    /// attached indicates the volume is successfully attached
    pub attached: bool,

    /// attachmentMetadata is populated with any information returned by the attach operation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment_metadata: Option<HashMap<String, String>>,

    /// attachError represents the last error encountered during attach operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_error: Option<VolumeError>,

    /// detachError represents the last error encountered during detach operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_error: Option<VolumeError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeError {
    /// time the error was encountered
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,

    /// message is a string detailing the encountered error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// CSIStorageCapacity stores the result of one CSI GetCapacity call
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CSIStorageCapacity {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,

    /// storageClassName represents the name of the StorageClass
    pub storage_class_name: String,

    /// capacity is the value reported by the CSI driver in its GetCapacityResponse
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,

    /// maximumVolumeSize is the value reported by the CSI driver in its GetCapacityResponse
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum_volume_size: Option<String>,

    /// nodeTopology defines which nodes have access to the storage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_topology: Option<LabelSelector>,
}

/// VolumeAttributesClass represents a specification of mutable volume attributes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttributesClass {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,

    /// driverName is the name of the CSI driver that this class applies to
    pub driver_name: String,

    /// parameters hold volume attributes defined by the CSI driver
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csi_driver_serialization() {
        let csi_driver = CSIDriver {
            type_meta: TypeMeta {
                kind: "CSIDriver".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-driver"),
            spec: CSIDriverSpec {
                attach_required: Some(true),
                pod_info_on_mount: Some(false),
                fs_group_policy: Some(FSGroupPolicy::ReadWriteOnceWithFSType),
                storage_capacity: Some(true),
                volume_lifecycle_modes: Some(vec![VolumeLifecycleMode::Persistent]),
                token_requests: None,
                requires_republish: Some(false),
                se_linux_mount: Some(false),
                node_allocatable_update_period_seconds: None,
            },
        };

        let json = serde_json::to_string(&csi_driver).unwrap();
        let deserialized: CSIDriver = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "test-driver");
    }

    #[test]
    fn test_csi_node_serialization() {
        let csi_node = CSINode {
            type_meta: TypeMeta {
                kind: "CSINode".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("node1"),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: "test-driver".to_string(),
                    node_id: "node1-id".to_string(),
                    topology_keys: Some(vec!["topology.kubernetes.io/zone".to_string()]),
                    allocatable: Some(VolumeNodeResources { count: Some(100) }),
                }],
            },
        };

        let json = serde_json::to_string(&csi_node).unwrap();
        let deserialized: CSINode = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.spec.drivers.len(), 1);
    }

    #[test]
    fn test_volume_attachment_serialization() {
        let va = VolumeAttachment {
            type_meta: TypeMeta {
                kind: "VolumeAttachment".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-va"),
            spec: VolumeAttachmentSpec {
                attacher: "test-driver".to_string(),
                node_name: "node1".to_string(),
                source: VolumeAttachmentSource {
                    persistent_volume_name: Some("pv-123".to_string()),
                    inline_volume_spec: None,
                },
            },
            status: Some(VolumeAttachmentStatus {
                attached: true,
                attachment_metadata: None,
                attach_error: None,
                detach_error: None,
            }),
        };

        let json = serde_json::to_string(&va).unwrap();
        let deserialized: VolumeAttachment = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.spec.attacher, "test-driver");
    }

    #[test]
    fn test_csi_storage_capacity_serialization() {
        let csc = CSIStorageCapacity {
            type_meta: TypeMeta {
                kind: "CSIStorageCapacity".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-csc").with_namespace("default"),
            storage_class_name: "fast-ssd".to_string(),
            capacity: Some("100Gi".to_string()),
            maximum_volume_size: Some("10Gi".to_string()),
            node_topology: None,
        };

        let json = serde_json::to_string(&csc).unwrap();
        let deserialized: CSIStorageCapacity = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.storage_class_name, "fast-ssd");
    }

    #[test]
    fn test_volume_attributes_class_serialization() {
        let mut params = HashMap::new();
        params.insert("type".to_string(), "ssd".to_string());
        params.insert("iops".to_string(), "3000".to_string());

        let vac = VolumeAttributesClass {
            type_meta: TypeMeta {
                kind: "VolumeAttributesClass".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("fast-storage"),
            driver_name: "test-driver".to_string(),
            parameters: Some(params),
        };

        let json = serde_json::to_string(&vac).unwrap();
        let deserialized: VolumeAttributesClass = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.driver_name, "test-driver");
        assert_eq!(
            deserialized
                .parameters
                .as_ref()
                .unwrap()
                .get("type")
                .unwrap(),
            "ssd"
        );
    }
}
