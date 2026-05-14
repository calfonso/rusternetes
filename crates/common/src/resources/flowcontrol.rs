use crate::types::ObjectMeta;
use serde::{Deserialize, Serialize};

/// PriorityLevelConfiguration defines the priority level and fairness for API requests
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfiguration {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_plc_kind")]
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: PriorityLevelConfigurationSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PriorityLevelConfigurationStatus>,
}

fn default_api_version() -> String {
    "flowcontrol.apiserver.k8s.io/v1".to_string()
}

fn default_plc_kind() -> String {
    "PriorityLevelConfiguration".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfigurationSpec {
    #[serde(rename = "type")]
    pub type_: PriorityLevelType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limited: Option<LimitedPriorityLevelConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exempt: Option<ExemptPriorityLevelConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PriorityLevelType {
    Limited,
    Exempt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitedPriorityLevelConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominal_concurrency_shares: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lending_concurrency_limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub borrowing_limit_percent: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_response: Option<LimitResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitResponse {
    #[serde(rename = "type")]
    pub type_: LimitResponseType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queuing: Option<QueuingConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LimitResponseType {
    Queue,
    Reject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuingConfiguration {
    pub queues: i32,
    pub hand_size: i32,
    pub queue_length_limit: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExemptPriorityLevelConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominal_concurrency_shares: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lending_concurrency_limit: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfigurationStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<PriorityLevelConfigurationCondition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfigurationCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// FlowSchema defines routing rules for requests to priority levels
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowSchema {
    #[serde(default = "default_fs_api_version")]
    pub api_version: String,
    #[serde(default = "default_fs_kind")]
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: FlowSchemaSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<FlowSchemaStatus>,
}

fn default_fs_api_version() -> String {
    "flowcontrol.apiserver.k8s.io/v1".to_string()
}

fn default_fs_kind() -> String {
    "FlowSchema".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowSchemaSpec {
    pub priority_level_configuration: PriorityLevelConfigurationReference,
    pub matching_precedence: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distinguisher_method: Option<FlowDistinguisherMethod>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<Vec<PolicyRulesWithSubjects>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfigurationReference {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowDistinguisherMethod {
    #[serde(rename = "type")]
    pub type_: FlowDistinguisherMethodType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowDistinguisherMethodType {
    ByUser,
    ByNamespace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRulesWithSubjects {
    pub subjects: Vec<FlowSchemaSubject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_rules: Option<Vec<ResourcePolicyRule>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_resource_rules: Option<Vec<NonResourcePolicyRule>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowSchemaSubject {
    pub kind: SubjectKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<UserSubject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<GroupSubject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account: Option<ServiceAccountSubject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubjectKind {
    User,
    Group,
    ServiceAccount,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserSubject {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupSubject {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountSubject {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcePolicyRule {
    pub verbs: Vec<String>,
    pub api_groups: Vec<String>,
    pub resources: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_scope: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonResourcePolicyRule {
    pub verbs: Vec<String>,
    #[serde(rename = "nonResourceURLs")]
    pub non_resource_urls: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowSchemaStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<FlowSchemaCondition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowSchemaCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
