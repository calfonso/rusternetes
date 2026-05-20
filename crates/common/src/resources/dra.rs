// Dynamic Resource Allocation (DRA) types
// API Group: resource.k8s.io/v1

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::serde_helpers::empty_string_as_none;
use super::NodeSelector;

// =============================================================================
// ResourceClaim
// =============================================================================

/// ResourceClaim describes a request for access to resources in the cluster,
/// for use by workloads. For example, if a workload needs an accelerator device
/// with specific properties, this is how that request is expressed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaim {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,

    #[serde(rename = "kind", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    pub spec: ResourceClaimSpec,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ResourceClaimStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaimSpec {
    #[serde(default)]
    pub devices: DeviceClaim,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeviceClaim {
    /// Requests represent individual requests for distinct devices which
    /// must all be satisfied.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requests: Vec<DeviceRequest>,

    /// These constraints must be satisfied by the set of devices that get
    /// allocated for the claim.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<DeviceConstraint>,

    /// Configuration for multiple potential drivers which could satisfy requests
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<DeviceClaimConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceRequest {
    /// Name can be used to reference this request in a pod.spec.containers[].resources.claims entry
    pub name: String,

    /// Exactly specifies the details for a single request
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exactly: Option<ExactDeviceRequest>,

    /// FirstAvailable contains subrequests, of which exactly one will be selected
    #[serde(
        rename = "firstAvailable",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub first_available: Vec<DeviceSubRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExactDeviceRequest {
    /// DeviceClassName references a specific DeviceClass
    #[serde(rename = "deviceClassName")]
    pub device_class_name: String,

    /// Selectors define criteria which must be satisfied by a specific device
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selectors: Vec<DeviceSelector>,

    /// AllocationMode defines how devices are allocated
    #[serde(
        rename = "allocationMode",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub allocation_mode: Option<DeviceAllocationMode>,

    /// Count is used only when the mode is "ExactCount"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<i64>,

    /// AdminAccess indicates administrative access to the device(s)
    #[serde(
        rename = "adminAccess",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub admin_access: Option<bool>,

    /// Tolerations for device taints
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<DeviceToleration>,

    /// Capacity defines resource requirements against each capacity
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<BTreeMap<String, DeviceCapacityRequirement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceSubRequest {
    pub name: String,

    #[serde(rename = "deviceClassName")]
    pub device_class_name: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selectors: Vec<DeviceSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeviceAllocationMode {
    ExactCount,
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceSelector {
    /// CEL expression which evaluates to true when a device is suitable
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cel: Option<CELDeviceSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CELDeviceSelector {
    /// Expression is a CEL expression which evaluates to a boolean
    pub expression: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceToleration {
    /// Key is the taint key that the toleration applies to
    pub key: String,

    /// Value is the taint value the toleration matches
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// Effect indicates the taint effect to match
    pub effect: DeviceTaintEffect,

    /// Operator represents a key's relationship to the value
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub operator: Option<TolerationOperator>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TolerationOperator {
    Equal,
    Exists,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceCapacityRequirement {
    /// Value defines the requested amount of capacity
    pub value: String, // resource.Quantity as string
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceConstraint {
    /// Requests is a list of one or more requests
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requests: Vec<String>,

    /// MatchAttribute specifies device attribute constraints
    #[serde(
        rename = "matchAttribute",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub match_attribute: Option<FullyQualifiedName>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceClaimConfiguration {
    /// Requests lists the names of requests where the configuration applies
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requests: Vec<String>,

    /// Opaque contains driver-specific configuration parameters
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opaque: Option<OpaqueDeviceConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OpaqueDeviceConfiguration {
    pub driver: String,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaimStatus {
    /// Allocation is set once the claim has been allocated successfully
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocation: Option<AllocationResult>,

    /// Devices contains details about allocated devices
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<AllocatedDeviceStatus>,

    /// ReservedFor indicates which entities are currently allowed to use the claim
    #[serde(rename = "reservedFor", default, skip_serializing_if = "Vec::is_empty")]
    pub reserved_for: Vec<ResourceClaimConsumerReference>,

    /// DeallocationRequested indicates that a ResourceClaim is to be deallocated
    #[serde(
        rename = "deallocationRequested",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub deallocation_requested: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AllocationResult {
    /// Devices contains the result of allocating devices
    #[serde(default)]
    pub devices: DeviceAllocationResult,

    /// NodeSelector defines where the allocated resources are available
    #[serde(
        rename = "nodeSelector",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub node_selector: Option<NodeSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeviceAllocationResult {
    /// Results lists all allocated devices
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<DeviceRequestAllocationResult>,

    /// Config contains configuration parameters for allocated devices
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<DeviceAllocationConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceRequestAllocationResult {
    /// Request is the name of the request in the claim
    pub request: String,

    /// Driver specifies the name of the DRA driver
    pub driver: String,

    /// Pool specifies the name of the device pool
    pub pool: String,

    /// Device specifies the name of the allocated device
    pub device: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceAllocationConfiguration {
    /// Source describes where the configuration comes from
    pub source: AllocationConfigSource,

    /// Requests lists the names of requests associated with the config
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requests: Vec<String>,

    /// Opaque provides driver-specific configuration parameters
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opaque: Option<OpaqueDeviceConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AllocationConfigSource {
    /// FromClass indicates configuration comes from DeviceClass
    FromClass,
    /// FromClaim indicates configuration comes from ResourceClaim
    FromClaim,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AllocatedDeviceStatus {
    /// Device references one device instance
    pub device: String,

    /// Driver is the name of the DRA driver
    pub driver: String,

    /// Pool is the name of the device pool
    pub pool: String,

    /// Conditions represents the latest observation of the device
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<DeviceCondition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceCondition {
    #[serde(rename = "type")]
    pub condition_type: String,

    pub status: String,

    #[serde(
        rename = "lastTransitionTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_transition_time: Option<DateTime<Utc>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaimConsumerReference {
    #[serde(rename = "apiGroup", default, skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,

    pub resource: String,

    pub name: String,

    pub uid: String,
}

// =============================================================================
// ResourceClaimTemplate
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaimTemplate {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,

    #[serde(rename = "kind", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    pub spec: ResourceClaimTemplateSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaimTemplateSpec {
    /// Metadata to be applied to ResourceClaims created from this template
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    /// Spec for the ResourceClaim created from this template
    pub spec: ResourceClaimSpec,
}

// =============================================================================
// DeviceClass
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceClass {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,

    #[serde(rename = "kind", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    pub spec: DeviceClassSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeviceClassSpec {
    /// Selectors define criteria for selecting devices
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selectors: Vec<DeviceSelector>,

    /// Config contains configuration parameters for devices in this class
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<DeviceClassConfiguration>,

    /// SuitableNodes defines which nodes have devices that might get allocated for claims
    #[serde(
        rename = "suitableNodes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub suitable_nodes: Option<NodeSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceClassConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opaque: Option<OpaqueDeviceConfiguration>,
}

// =============================================================================
// ResourceSlice
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSlice {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,

    #[serde(rename = "kind", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    pub spec: ResourceSliceSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSliceSpec {
    /// Driver identifies the DRA driver providing the capacity information
    pub driver: String,

    /// Pool describes the pool that this ResourceSlice belongs to
    pub pool: ResourcePool,

    /// NodeName identifies the node which provides the resources
    #[serde(rename = "nodeName", default, skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,

    /// NodeSelector defines which nodes have access to the resources
    #[serde(
        rename = "nodeSelector",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub node_selector: Option<NodeSelector>,

    /// AllNodes indicates that all nodes have access to resources in the pool
    #[serde(rename = "allNodes", default, skip_serializing_if = "Option::is_none")]
    pub all_nodes: Option<bool>,

    /// Devices lists devices in this pool
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<Device>,

    /// PerDeviceNodeSelection defines whether node access is per-device
    #[serde(
        rename = "perDeviceNodeSelection",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub per_device_node_selection: Option<bool>,

    /// SharedCounters defines counter sets available to devices
    #[serde(
        rename = "sharedCounters",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub shared_counters: Vec<CounterSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourcePool {
    /// Name is used to identify the pool
    pub name: String,

    /// Generation tracks changes in a pool over time
    pub generation: i64,

    /// ResourceSliceCount is the total number of ResourceSlices in the pool
    #[serde(rename = "resourceSliceCount")]
    pub resource_slice_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    /// Name is unique identifier among all devices managed by the driver
    pub name: String,

    /// Attributes defines the set of attributes for this device
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<BTreeMap<String, DeviceAttribute>>,

    /// Capacity defines the set of capacities for this device
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<BTreeMap<String, DeviceCapacity>>,

    /// ConsumesCounters defines references to sharedCounters
    #[serde(
        rename = "consumesCounters",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub consumes_counters: Vec<DeviceCounterConsumption>,

    /// NodeName identifies the node where the device is available
    #[serde(rename = "nodeName", default, skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,

    /// NodeSelector defines the nodes where the device is available
    #[serde(
        rename = "nodeSelector",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub node_selector: Option<NodeSelector>,

    /// AllNodes indicates that all nodes have access to the device
    #[serde(rename = "allNodes", default, skip_serializing_if = "Option::is_none")]
    pub all_nodes: Option<bool>,

    /// Taints are driver-defined taints
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub taints: Vec<DeviceTaint>,

    /// BindsToNode indicates if allocation must be limited to the chosen node
    #[serde(
        rename = "bindsToNode",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub binds_to_node: Option<bool>,

    /// BindingConditions defines conditions for proceeding with binding
    #[serde(
        rename = "bindingConditions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub binding_conditions: Vec<String>,

    /// BindingFailureConditions defines conditions for binding failure
    #[serde(
        rename = "bindingFailureConditions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub binding_failure_conditions: Vec<String>,

    /// AllowMultipleAllocations marks whether device can be allocated multiple times
    #[serde(
        rename = "allowMultipleAllocations",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub allow_multiple_allocations: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceAttribute {
    #[serde(rename = "int", default, skip_serializing_if = "Option::is_none")]
    pub int_value: Option<i64>,

    #[serde(rename = "bool", default, skip_serializing_if = "Option::is_none")]
    pub bool_value: Option<bool>,

    #[serde(rename = "string", default, skip_serializing_if = "Option::is_none")]
    pub string_value: Option<String>,

    #[serde(rename = "version", default, skip_serializing_if = "Option::is_none")]
    pub version_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceCapacity {
    /// Value defines how much capacity the device has
    pub value: String, // resource.Quantity as string

    /// RequestPolicy defines how capacity must be consumed
    #[serde(
        rename = "requestPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub request_policy: Option<CapacityRequestPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CapacityRequestPolicy {
    /// Default specifies default consumed amount
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,

    /// ValidValues defines acceptable quantity values
    #[serde(rename = "validValues", default, skip_serializing_if = "Vec::is_empty")]
    pub valid_values: Vec<String>,

    /// ValidRange defines an acceptable quantity value range
    #[serde(
        rename = "validRange",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub valid_range: Option<CapacityRequestPolicyRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CapacityRequestPolicyRange {
    /// Min specifies minimum capacity allowed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,

    /// Max defines upper limit for capacity
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,

    /// Step defines step size between valid capacity amounts
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceTaint {
    /// Key is the taint key to be applied to a device
    pub key: String,

    /// Value is the taint value corresponding to the key
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// Effect is the effect of the taint
    pub effect: DeviceTaintEffect,

    /// TimeAdded represents when the taint was added
    #[serde(rename = "timeAdded", default, skip_serializing_if = "Option::is_none")]
    pub time_added: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeviceTaintEffect {
    None,
    NoSchedule,
    NoExecute,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeviceCounterConsumption {
    /// CounterSet is the name of the set from which counters are consumed
    #[serde(rename = "counterSet")]
    pub counter_set: String,

    /// Counters defines the counters consumed by the device
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counters: Option<BTreeMap<String, Counter>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CounterSet {
    /// Name defines the name of the counter set
    pub name: String,

    /// Counters defines the set of counters for this CounterSet
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counters: Option<BTreeMap<String, Counter>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Counter {
    /// Value defines how much of a counter is available
    pub value: String, // resource.Quantity as string
}

// =============================================================================
// Common Types
// =============================================================================

pub type FullyQualifiedName = String;
pub type QualifiedName = String;

// Re-export ObjectMeta from parent
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    #[serde(
        rename = "resourceVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub resource_version: Option<String>,

    #[serde(
        rename = "creationTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub creation_timestamp: Option<DateTime<Utc>>,

    #[serde(
        rename = "deletionTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub deletion_timestamp: Option<DateTime<Utc>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<BTreeMap<String, String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<BTreeMap<String, String>>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalizers: Vec<String>,

    #[serde(
        rename = "ownerReferences",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub owner_references: Vec<OwnerReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReference {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub uid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller: Option<bool>,
    #[serde(
        rename = "blockOwnerDeletion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub block_owner_deletion: Option<bool>,
}

// List types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaimList {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,

    #[serde(rename = "kind", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ListMeta>,

    pub items: Vec<ResourceClaim>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaimTemplateList {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,

    #[serde(rename = "kind", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ListMeta>,

    pub items: Vec<ResourceClaimTemplate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceClassList {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,

    #[serde(rename = "kind", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ListMeta>,

    pub items: Vec<DeviceClass>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSliceList {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,

    #[serde(rename = "kind", default, skip_serializing_if = "String::is_empty")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ListMeta>,

    pub items: Vec<ResourceSlice>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListMeta {
    #[serde(
        rename = "resourceVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub resource_version: Option<String>,

    #[serde(rename = "continue", default, skip_serializing_if = "Option::is_none")]
    pub continue_token: Option<String>,

    #[serde(
        rename = "remainingItemCount",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub remaining_item_count: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_claim_serialization() {
        let claim = ResourceClaim {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "ResourceClaim".to_string(),
            metadata: Some(ObjectMeta {
                name: Some("test-claim".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            }),
            spec: ResourceClaimSpec {
                devices: DeviceClaim {
                    requests: vec![DeviceRequest {
                        name: "req1".to_string(),
                        exactly: Some(ExactDeviceRequest {
                            device_class_name: "gpu".to_string(),
                            selectors: vec![],
                            allocation_mode: Some(DeviceAllocationMode::ExactCount),
                            count: Some(1),
                            admin_access: None,
                            tolerations: vec![],
                            capacity: None,
                        }),
                        first_available: vec![],
                    }],
                    constraints: vec![],
                    config: vec![],
                },
            },
            status: None,
        };

        let json = serde_json::to_string(&claim).unwrap();
        let deserialized: ResourceClaim = serde_json::from_str(&json).unwrap();
        assert_eq!(claim, deserialized);
    }

    #[test]
    fn test_device_class_serialization() {
        let device_class = DeviceClass {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "DeviceClass".to_string(),
            metadata: Some(ObjectMeta {
                name: Some("gpu-class".to_string()),
                ..Default::default()
            }),
            spec: DeviceClassSpec {
                selectors: vec![],
                config: vec![],
                suitable_nodes: None,
            },
        };

        let json = serde_json::to_string(&device_class).unwrap();
        let deserialized: DeviceClass = serde_json::from_str(&json).unwrap();
        assert_eq!(device_class, deserialized);
    }

    #[test]
    fn test_resource_slice_serialization() {
        let slice = ResourceSlice {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "ResourceSlice".to_string(),
            metadata: Some(ObjectMeta {
                name: Some("test-slice".to_string()),
                ..Default::default()
            }),
            spec: ResourceSliceSpec {
                driver: "test.driver".to_string(),
                pool: ResourcePool {
                    name: "pool1".to_string(),
                    generation: 1,
                    resource_slice_count: 1,
                },
                node_name: Some("node1".to_string()),
                node_selector: None,
                all_nodes: None,
                devices: vec![],
                per_device_node_selection: None,
                shared_counters: vec![],
            },
        };

        let json = serde_json::to_string(&slice).unwrap();
        let deserialized: ResourceSlice = serde_json::from_str(&json).unwrap();
        assert_eq!(slice, deserialized);
    }
}
