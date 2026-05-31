use crate::resources::pod::PodSpec;
use crate::types::{LabelSelector, ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ReplicationController ensures that a specified number of pod replicas are running at any given time
/// This is a legacy resource - ReplicaSets/Deployments are preferred for new workloads
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationController {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: ReplicationControllerSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ReplicationControllerStatus>,
}

impl ReplicationController {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: ReplicationControllerSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ReplicationController".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// ReplicationControllerSpec defines the desired state of a ReplicationController
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationControllerSpec {
    /// Number of desired pods (defaults to 1)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,

    /// Selector for pods (label query)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<HashMap<String, String>>,

    /// Template for pod creation — default allows RC creation without template
    /// (K8s allows this for headless/selector-only RCs)
    #[serde(default)]
    pub template: PodTemplateSpec,

    /// Minimum number of seconds for which a newly created pod should be ready
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,
}

/// ReplicationControllerStatus represents the current state of a ReplicationController
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationControllerStatus {
    /// Number of replicas (Go: int32, json:"replicas" — no omitempty, so Go leaves absent
    /// fields at zero value; we must do the same via #[serde(default)])
    #[serde(default)]
    pub replicas: i32,

    /// Number of fully labeled replicas
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fully_labeled_replicas: Option<i32>,

    /// Number of ready replicas
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,

    /// Number of available replicas
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,

    /// ObservedGeneration reflects the generation of the most recently observed ReplicationController
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Conditions represent the latest available observations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<ReplicationControllerCondition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationControllerCondition {
    /// Type of replication controller condition
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition: True, False, Unknown
    pub status: String,

    /// Last time the condition transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<chrono::DateTime<chrono::Utc>>,

    /// The reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// A human readable message indicating details about the transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// ReplicaSet ensures that a specified number of pod replicas are running at any given time
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSet {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: ReplicaSetSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ReplicaSetStatus>,
}

impl ReplicaSet {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: ReplicaSetSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// ReplicaSetSpec defines the desired state of a ReplicaSet
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSetSpec {
    /// Number of desired pods
    #[serde(default = "default_one_replica")]
    pub replicas: i32,

    /// Selector for pods
    #[serde(default)]
    pub selector: LabelSelector,

    /// Template for pod creation
    pub template: PodTemplateSpec,

    /// Minimum number of seconds for which a newly created pod should be ready
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,
}

fn default_one_replica() -> i32 {
    1
}

/// ReplicaSetStatus represents the current state of a ReplicaSet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSetStatus {
    /// Number of replicas (Go: int32, json:"replicas" — no omitempty, Go leaves absent
    /// fields at zero value; we match with #[serde(default)])
    #[serde(default)]
    pub replicas: i32,

    /// Number of fully labeled replicas
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fully_labeled_replicas: Option<i32>,

    /// Number of ready replicas
    #[serde(default)]
    pub ready_replicas: i32,

    /// Number of available replicas
    #[serde(default)]
    pub available_replicas: i32,

    /// ObservedGeneration reflects the generation of the most recently observed ReplicaSet
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Conditions represent the latest available observations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<ReplicaSetCondition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminating_replicas: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSetCondition {
    /// Type of replica set condition
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition: True, False, Unknown
    pub status: String,

    /// Last time the condition transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<chrono::DateTime<chrono::Utc>>,

    /// The reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// A human readable message indicating details about the transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// StatefulSet represents a set of pods with consistent identities
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSet {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: StatefulSetSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StatefulSetStatus>,
}

impl StatefulSet {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: StatefulSetSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "StatefulSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// StatefulSetSpec defines the desired state of a StatefulSet
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetSpec {
    /// Number of desired pods (defaults to 1)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,

    /// Selector for pods
    #[serde(default)]
    pub selector: LabelSelector,

    /// Service name for network identity
    #[serde(default, alias = "serviceName")]
    pub service_name: String,

    /// Template for pod creation
    pub template: PodTemplateSpec,

    /// Update strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_strategy: Option<StatefulSetUpdateStrategy>,

    /// Pod management policy: OrderedReady or Parallel
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_management_policy: Option<String>,

    /// Minimum seconds for a pod to be ready before it's considered available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,

    /// Number of revisions to retain in history
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision_history_limit: Option<i32>,

    /// Volume claim templates for persistent storage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_claim_templates: Option<Vec<crate::resources::volume::PersistentVolumeClaim>>,

    /// Policy for PVC retention when StatefulSet is deleted or scaled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistent_volume_claim_retention_policy:
        Option<StatefulSetPersistentVolumeClaimRetentionPolicy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordinals: Option<StatefulSetOrdinals>,
}

/// StatefulSetUpdateStrategy indicates the strategy for updating StatefulSet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetUpdateStrategy {
    /// Type of update strategy: RollingUpdate or OnDelete
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub strategy_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<RollingUpdateStatefulSetStrategy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RollingUpdateStatefulSetStrategy {
    /// Partition indicates the ordinal at which the StatefulSet should be partitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,

    /// The maximum number of pods that can be unavailable during the update.
    /// Value can be an absolute number (ex: 5) or a percentage (ex: 10%).
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::resources::deployment::deserialize_int_or_string_opt",
        default
    )]
    pub max_unavailable: Option<String>,
}

/// StatefulSetPersistentVolumeClaimRetentionPolicy describes the policy for PVC lifecycle
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetPersistentVolumeClaimRetentionPolicy {
    /// WhenDeleted specifies what happens to PVCs when the StatefulSet is deleted
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when_deleted: Option<String>,

    /// WhenScaled specifies what happens to PVCs when the StatefulSet is scaled down
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when_scaled: Option<String>,
}

/// StatefulSetStatus represents the current state of a StatefulSet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetStatus {
    /// Number of replicas (Go: int32, json:"replicas" — no omitempty, Go leaves absent
    /// fields at zero value; we match with #[serde(default)])
    #[serde(default)]
    pub replicas: i32,

    /// Number of ready replicas
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,

    /// Number of current replicas
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_replicas: Option<i32>,

    /// Number of updated replicas
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_replicas: Option<i32>,

    /// Number of available replicas (ready for at least minReadySeconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,

    /// Hash collision count for pod template
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collision_count: Option<i32>,

    /// Generation observed by the controller
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Current update revision
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_revision: Option<String>,

    /// Update revision
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_revision: Option<String>,

    /// Conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<StatefulSetCondition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetCondition {
    #[serde(rename = "type")]
    pub condition_type: String,

    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,
}

/// DaemonSet ensures that all (or some) nodes run a copy of a pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonSet {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: DaemonSetSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<DaemonSetStatus>,
}

impl DaemonSet {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>, spec: DaemonSetSpec) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "DaemonSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// DaemonSetSpec defines the desired state of a DaemonSet
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonSetSpec {
    /// Selector for pods
    #[serde(default)]
    pub selector: LabelSelector,

    /// Template for pod creation
    pub template: PodTemplateSpec,

    /// Update strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_strategy: Option<DaemonSetUpdateStrategy>,

    /// Minimum seconds for a pod to be ready before it's considered available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,

    /// Number of revisions to retain in history
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision_history_limit: Option<i32>,
}

/// DaemonSetUpdateStrategy indicates the strategy for updating DaemonSet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonSetUpdateStrategy {
    /// Type of update strategy: RollingUpdate or OnDelete
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub strategy_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<RollingUpdateDaemonSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RollingUpdateDaemonSet {
    /// The maximum number of pods that can be unavailable during the update
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::resources::deployment::deserialize_int_or_string_opt",
        default
    )]
    pub max_unavailable: Option<String>, // IntOrString

    /// The maximum number of nodes with an existing available daemonset pod that can have an updated one
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::resources::deployment::deserialize_int_or_string_opt",
        default
    )]
    pub max_surge: Option<String>, // IntOrString
}

/// DaemonSetStatus represents the current state of a DaemonSet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonSetStatus {
    /// Number of nodes that should be running the daemon pod
    #[serde(default)]
    pub desired_number_scheduled: i32,

    /// Number of nodes running at least one daemon pod
    #[serde(default)]
    pub current_number_scheduled: i32,

    /// Number of nodes with ready daemon pods
    #[serde(default)]
    pub number_ready: i32,

    /// Number of nodes that should be running but aren't
    #[serde(default)]
    pub number_misscheduled: i32,

    /// Number of nodes running an available daemon pod (ready for at least minReadySeconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number_available: Option<i32>,

    /// Number of nodes that should be running the daemon pod but don't have one running
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number_unavailable: Option<i32>,

    /// Number of nodes that are running an updated daemon pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_number_scheduled: Option<i32>,

    /// Generation observed by the controller
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Hash collision count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collision_count: Option<i32>,

    /// Conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<DaemonSetCondition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonSetCondition {
    #[serde(rename = "type")]
    pub condition_type: String,

    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,
}

/// Job represents a single batch process
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: JobSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<JobStatus>,
}

impl Job {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>, spec: JobSpec) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// JobSpec defines the desired state of a Job
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSpec {
    /// Template for pod creation
    pub template: PodTemplateSpec,

    /// Number of successful completions required
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completions: Option<i32>,

    /// Maximum number of pods that can run in parallel
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<i32>,

    /// Number of retries before marking the job as failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff_limit: Option<i32>,

    /// Duration in seconds relative to startTime that the job may be active
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deadline_seconds: Option<i64>,

    /// Label selector for pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,

    /// Allow manual selector (bypass automatic selector generation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manual_selector: Option<bool>,

    /// Suspend specifies whether the job controller should create pods or not
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspend: Option<bool>,

    /// TTL seconds after job finishes before auto-cleanup
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds_after_finished: Option<i32>,

    /// Completion mode: NonIndexed or Indexed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_mode: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff_limit_per_index: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_failed_indexes: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_failure_policy: Option<PodFailurePolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_replacement_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success_policy: Option<SuccessPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_by: Option<String>,
}

/// PodFailurePolicy describes how failed pods influence the backoffLimit.
///
/// Mirrors `k8s.io/api/batch/v1.PodFailurePolicy` (proto field 11 of `JobSpec`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodFailurePolicy {
    /// A list of pod failure policy rules. The rules are evaluated in order.
    /// Once a rule matches a Pod failure, the remaining of the rules are ignored.
    /// When no rule matches the Pod failure, the default handling applies — the
    /// counter of pod failures is incremented and it is checked against the
    /// backoffLimit. At most 20 elements are allowed.
    #[serde(default)]
    pub rules: Vec<PodFailurePolicyRule>,
}

/// PodFailurePolicyRule describes how a pod failure is handled when the
/// requirements are met. Only one of `on_exit_codes` and `on_pod_conditions`
/// can be set per rule.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodFailurePolicyRule {
    /// Action taken on a pod failure when the requirements are satisfied.
    /// Possible values: `FailJob`, `FailIndex`, `Ignore`, `Count`.
    pub action: String,

    /// Requirement on the container exit codes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_exit_codes: Option<PodFailurePolicyOnExitCodesRequirement>,

    /// Requirement on the pod conditions. The requirement is satisfied if at
    /// least one pattern matches an actual pod condition. At most 20 elements
    /// are allowed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_pod_conditions: Vec<PodFailurePolicyOnPodConditionsPattern>,
}

/// PodFailurePolicyOnExitCodesRequirement describes the requirement for
/// handling a failed pod based on its container exit codes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodFailurePolicyOnExitCodesRequirement {
    /// Restricts the check for exit codes to the container with the specified
    /// name. When `None`, the rule applies to all containers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,

    /// Relationship between the container exit code(s) and the specified
    /// values. Possible values: `In`, `NotIn`.
    pub operator: String,

    /// Set of values. At least one element is required; at most 255 allowed.
    /// `0` cannot be used with the `In` operator.
    pub values: Vec<i32>,
}

/// PodFailurePolicyOnPodConditionsPattern describes a pattern for matching
/// an actual pod condition type/status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodFailurePolicyOnPodConditionsPattern {
    /// Required Pod condition type.
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Required Pod condition status. Defaults to `True` when unspecified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// SuccessPolicy describes when a Job can be declared as succeeded based on
/// the success of some indexes.
///
/// Mirrors `k8s.io/api/batch/v1.SuccessPolicy` (proto field 16 of `JobSpec`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SuccessPolicy {
    /// Rules represents the list of alternative rules for declaring the Jobs
    /// as successful before `.status.succeeded >= .spec.completions`. Once any
    /// of the rules are met, the `SuccessCriteriaMet` condition is added and
    /// the lingering pods are removed. At most 20 elements are allowed.
    #[serde(default)]
    pub rules: Vec<SuccessPolicyRule>,
}

/// SuccessPolicyRule describes a rule for declaring a Job as succeeded.
/// Each rule must have at least one of `succeeded_indexes` or `succeeded_count`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SuccessPolicyRule {
    /// Set of indexes which must be contained in the actual set of succeeded
    /// indexes for the Job. Expressed as comma-separated intervals
    /// (e.g. `"1,3-5,7"`). When `None`, the field is not evaluated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub succeeded_indexes: Option<String>,

    /// Minimal required size of the actual set of succeeded indexes for the
    /// Job. When `None`, the field is not evaluated. Must be positive when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub succeeded_count: Option<i32>,
}

/// JobStatus represents the current state of a Job
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    /// Number of actively running pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<i32>,

    /// Number of successfully completed pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub succeeded: Option<i32>,

    /// Number of failed pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed: Option<i32>,

    /// Conditions of the job
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<JobCondition>>,

    /// When the job started
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<DateTime<Utc>>,

    /// When the job completed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_time: Option<DateTime<Utc>>,

    /// Number of pods which have a ready condition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<i32>,

    /// Number of pods which are terminating
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminating: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_indexes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_indexes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncounted_terminated_pods: Option<UncountedTerminatedPods>,

    /// Generation observed by the controller
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JobCondition {
    /// Type of condition: Complete or Failed
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition: True, False, Unknown
    pub status: String,

    /// Last time the condition was probed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_probe_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Last time the condition transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<chrono::DateTime<chrono::Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// CronJob manages time-based jobs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronJob {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: CronJobSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<CronJobStatus>,
}

impl CronJob {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>, spec: CronJobSpec) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "CronJob".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// CronJobSpec defines the desired state of a CronJob
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronJobSpec {
    /// Cron schedule (e.g., "0 * * * *")
    pub schedule: String,

    /// Job template
    #[serde(alias = "jobTemplate")]
    pub job_template: JobTemplateSpec,

    /// Concurrency policy: Allow, Forbid, or Replace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency_policy: Option<String>,

    /// Whether the cron job is suspended
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspend: Option<bool>,

    /// Number of successful job history to retain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub successful_jobs_history_limit: Option<i32>,

    /// Number of failed job history to retain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_jobs_history_limit: Option<i32>,

    /// Deadline in seconds for starting a job if it misses its scheduled time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub starting_deadline_seconds: Option<i64>,

    /// IANA timezone for the schedule (e.g., "America/New_York")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTemplateSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    pub spec: JobSpec,
}

/// CronJobStatus represents the current state of a CronJob
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CronJobStatus {
    /// List of currently running jobs
    #[serde(default)]
    pub active: Vec<crate::resources::service_account::ObjectReference>,

    /// Last time the job was scheduled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_schedule_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Last successful job time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_time: Option<chrono::DateTime<chrono::Utc>>,
}

/// PodTemplate describes a template for creating copies of a predefined pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodTemplate {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    /// Template defines the pods that will be created from this pod template
    pub template: PodTemplateSpec,
}

impl PodTemplate {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        template: PodTemplateSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "PodTemplate".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            template,
        }
    }
}

/// PodTemplateSpec describes the pod that will be created
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodTemplateSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    pub spec: PodSpec,
}

/// StatefulSetOrdinals describes the policy used for replica index assignment
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetOrdinals {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<i32>,
}

/// UncountedTerminatedPods holds UIDs of Pods that have terminated but haven't been accounted for yet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UncountedTerminatedPods {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub succeeded: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_controller_creation() {
        let pod_template_spec = PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers: vec![],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: Some("Always".to_string()),
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
            },
        };

        let rc_spec = ReplicationControllerSpec {
            replicas: Some(3),
            selector: Some(
                [("app".to_string(), "nginx".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
            ),
            template: pod_template_spec,
            min_ready_seconds: None,
        };

        let rc = ReplicationController::new("test-rc", "default", rc_spec);

        assert_eq!(rc.metadata.name, "test-rc");
        assert_eq!(rc.metadata.namespace, Some("default".to_string()));
        assert_eq!(rc.type_meta.kind, "ReplicationController");
        assert_eq!(rc.type_meta.api_version, "v1");
        assert_eq!(rc.spec.replicas, Some(3));
    }

    #[test]
    fn test_pod_template_creation() {
        let pod_template_spec = PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers: vec![],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: Some("Always".to_string()),
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
            },
        };

        let pod_template = PodTemplate::new("test-template", "default", pod_template_spec);

        assert_eq!(pod_template.metadata.name, "test-template");
        assert_eq!(pod_template.metadata.namespace, Some("default".to_string()));
        assert_eq!(pod_template.type_meta.kind, "PodTemplate");
        assert_eq!(pod_template.type_meta.api_version, "v1");
    }

    #[test]
    fn test_job_creation() {
        let template = PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers: vec![],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: Some("Never".to_string()),
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
            },
        };

        let job_spec = JobSpec {
            template,
            completions: Some(1),
            parallelism: Some(1),
            backoff_limit: Some(3),
            active_deadline_seconds: None,
            selector: None,
            manual_selector: None,
            suspend: None,
            ttl_seconds_after_finished: None,
            completion_mode: None,
            backoff_limit_per_index: None,
            max_failed_indexes: None,
            pod_failure_policy: None,
            pod_replacement_policy: None,
            success_policy: None,
            managed_by: None,
        };

        let job = Job::new("test-job", "default", job_spec);

        assert_eq!(job.metadata.name, "test-job");
        assert_eq!(job.type_meta.api_version, "batch/v1");
    }

    /// Reproducing the "missing field `replicas`" conformance failure.
    /// A conformance test creates an RC without specifying replicas.
    /// Go's json.Unmarshal leaves *int32 at nil; we must also accept absence.
    #[test]
    fn test_rc_decode_without_replicas() {
        // Typical RC body sent by k8s conformance tests (no `replicas` field)
        let json = r#"{
            "kind": "ReplicationController",
            "apiVersion": "v1",
            "metadata": {
                "name": "e2e-test-rc",
                "namespace": "replication-controller-9005",
                "labels": {"app": "e2e-test-rc"},
                "creationTimestamp": null
            },
            "spec": {
                "selector": {"app": "e2e-test-rc"},
                "template": {
                    "metadata": {
                        "labels": {"app": "e2e-test-rc"},
                        "creationTimestamp": null
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": "busybox",
                                "image": "registry.k8s.io/e2e-test-images/busybox:1.36.1-2",
                                "resources": {}
                            }
                        ],
                        "restartPolicy": "Always",
                        "terminationGracePeriodSeconds": 30,
                        "dnsPolicy": "ClusterFirst"
                    }
                }
            }
        }"#;

        let rc: ReplicationController =
            serde_json::from_str(json).expect("RC without replicas must decode");
        // replicas absent → None (Go: *int32 nil → defaults to 1 in registry)
        assert!(
            rc.spec.replicas.is_none(),
            "replicas absent in wire body should decode as None"
        );
    }

    /// ReplicationControllerStatus.replicas is `int32` in Go (no omitempty),
    /// so Go leaves it at 0 when absent — we must decode it to 0 too.
    /// This reproduces the "missing field `replicas`" 400 BadRequest that
    /// blocked ~30 conformance specs (garbage_collector, rc lifecycle, etc.)
    #[test]
    fn test_rc_status_replicas_defaults_to_zero() {
        // status present but replicas absent — the exact shape sent by
        // many conformance tests.
        let json = r#"{
            "kind": "ReplicationController",
            "apiVersion": "v1",
            "metadata": {
                "name": "test-rc",
                "namespace": "default",
                "creationTimestamp": null
            },
            "spec": {
                "selector": {"app": "test"},
                "template": {
                    "metadata": {"labels": {"app": "test"}, "creationTimestamp": null},
                    "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                }
            },
            "status": {
                "readyReplicas": 0,
                "availableReplicas": 0
            }
        }"#;

        let rc: ReplicationController = serde_json::from_str(json)
            .expect("RC with status missing replicas must decode (Go zero-value parity)");
        let status = rc.status.expect("status must be present");
        assert_eq!(
            status.replicas, 0,
            "status.replicas absent on wire must decode as 0"
        );
    }

    /// ReplicaSetStatus.replicas is `int32` in Go (no omitempty).
    #[test]
    fn test_replicaset_status_replicas_defaults_to_zero() {
        let json = r#"{
            "kind": "ReplicaSet",
            "apiVersion": "apps/v1",
            "metadata": {"name": "test-rs"},
            "spec": {
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                }
            },
            "status": {
                "readyReplicas": 0,
                "availableReplicas": 0
            }
        }"#;

        let rs: ReplicaSet = serde_json::from_str(json)
            .expect("ReplicaSet with status missing replicas must decode");
        let status = rs.status.expect("status must be present");
        assert_eq!(
            status.replicas, 0,
            "status.replicas absent on wire must decode as 0"
        );
    }

    /// StatefulSetStatus.replicas is `int32` in Go (no omitempty).
    #[test]
    fn test_statefulset_status_replicas_defaults_to_zero() {
        let json = r#"{
            "kind": "StatefulSet",
            "apiVersion": "apps/v1",
            "metadata": {"name": "test-ss"},
            "spec": {
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                },
                "serviceName": "test"
            },
            "status": {
                "readyReplicas": 0
            }
        }"#;

        let ss: StatefulSet = serde_json::from_str(json)
            .expect("StatefulSet with status missing replicas must decode");
        let status = ss.status.expect("status must be present");
        assert_eq!(
            status.replicas, 0,
            "status.replicas absent on wire must decode as 0"
        );
    }
}
