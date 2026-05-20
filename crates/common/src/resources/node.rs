use crate::types::{ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Node is a worker machine in Kubernetes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: Option<NodeSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<NodeStatus>,
}

impl Node {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: None,
            status: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSpec {
    #[serde(rename = "podCIDR", skip_serializing_if = "Option::is_none")]
    pub pod_cidr: Option<String>,

    /// PodCIDRs represents the IP ranges assigned to the node for usage by pods (supports dual-stack)
    #[serde(rename = "podCIDRs", skip_serializing_if = "Option::is_none")]
    pub pod_cidrs: Option<Vec<String>>,

    #[serde(rename = "providerID", skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,

    #[serde(skip_serializing_if = "is_unschedulable_none")]
    #[serde(default)]
    pub unschedulable: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub taints: Option<Vec<Taint>>,
}

fn is_unschedulable_none(value: &Option<bool>) -> bool {
    // Always serialize unschedulable field, even if it's Some(false)
    // Only skip if it's None
    value.is_none()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Taint {
    #[serde(default)]
    pub key: String,
    pub value: Option<String>,
    #[serde(default)]
    pub effect: String, // NoSchedule, PreferNoSchedule, NoExecute
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_added: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub capacity: Option<HashMap<String, String>>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub allocatable: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<NodeCondition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub addresses: Option<Vec<NodeAddress>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_info: Option<NodeSystemInfo>,

    /// Images is the list of container images on this node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub images: Option<Vec<ContainerImage>>,

    /// VolumesInUse is the list of unique volumes in use (mounted) by the node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volumes_in_use: Option<Vec<String>>,

    /// VolumesAttached is the list of volumes attached to the node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volumes_attached: Option<Vec<AttachedVolume>>,

    /// DaemonEndpoints contains endpoints of daemons running on the node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daemon_endpoints: Option<NodeDaemonEndpoints>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<NodeConfigStatus>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub features: Option<NodeFeatures>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_handlers: Option<Vec<NodeRuntimeHandler>>,
}

/// ContainerImage describes a container image present on the node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerImage {
    /// Names is the list of names by which this image is known
    #[serde(skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,

    /// SizeBytes is the size of the image in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<i64>,
}

/// AttachedVolume describes a volume attached to a node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachedVolume {
    /// Name of the attached volume
    #[serde(default)]
    pub name: String,

    /// DevicePath is the path where the volume is attached on the host
    #[serde(default)]
    pub device_path: String,
}

/// NodeDaemonEndpoints lists ports opened by daemons running on the node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDaemonEndpoints {
    /// KubeletEndpoint is the endpoint on which Kubelet is listening
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kubelet_endpoint: Option<DaemonEndpoint>,
}

/// DaemonEndpoint contains information about a single Daemon endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonEndpoint {
    /// Port number of the given endpoint
    #[serde(rename = "Port", default)]
    pub port: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCondition {
    #[serde(rename = "type", default)]
    pub condition_type: String, // Ready, MemoryPressure, DiskPressure, PIDPressure, NetworkUnavailable

    #[serde(default)]
    pub status: String, // True, False, Unknown

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_time: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeAddress {
    #[serde(rename = "type", default)]
    pub address_type: String, // Hostname, ExternalIP, InternalIP

    #[serde(default)]
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSystemInfo {
    #[serde(rename = "machineID", default)]
    pub machine_id: String,
    #[serde(rename = "systemUUID", default)]
    pub system_uuid: String,
    #[serde(rename = "bootID", default)]
    pub boot_id: String,
    #[serde(default)]
    pub kernel_version: String,
    #[serde(default)]
    pub os_image: String,
    #[serde(default)]
    pub container_runtime_version: String,
    #[serde(default)]
    pub kubelet_version: String,
    #[serde(default)]
    pub kube_proxy_version: String,
    #[serde(default)]
    pub operating_system: String,
    #[serde(default)]
    pub architecture: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap: Option<NodeSwapStatus>,
}

/// NodeSwapStatus represents the swap status of a node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeSwapStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<i64>,
}

/// NodeConfigStatus describes the status of the config assigned by Node.Spec.ConfigSource
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeConfigStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned: Option<NodeConfigSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<NodeConfigSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_known_good: Option<NodeConfigSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// NodeConfigSource specifies a source of node configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeConfigSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapNodeConfigSource>,
}

/// ConfigMapNodeConfigSource contains the information to reference a ConfigMap as a config source
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapNodeConfigSource {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kubelet_config_key: Option<String>,
}

/// NodeFeatures describes the set of features implemented by the CRI implementation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeFeatures {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplemental_groups_policy: Option<bool>,
}

/// NodeRuntimeHandler is a set of runtime handler information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeRuntimeHandler {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub features: Option<NodeRuntimeHandlerFeatures>,
}

/// NodeRuntimeHandlerFeatures is a set of features implemented by the runtime handler
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeRuntimeHandlerFeatures {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive_read_only_mounts: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_namespaces: Option<bool>,
}
