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
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfigurationSpec {
    #[serde(rename = "type", default)]
    pub type_: PriorityLevelType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limited: Option<LimitedPriorityLevelConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exempt: Option<ExemptPriorityLevelConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum PriorityLevelType {
    #[default]
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
    /// Percent of this level's nominal concurrency limit that may be borrowed
    /// by other levels. Upstream `flowcontrol/v1` field; validated 0..=100.
    #[serde(rename = "lendablePercent", skip_serializing_if = "Option::is_none")]
    pub lendable_percent: Option<i32>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueuingConfiguration {
    #[serde(default)]
    pub queues: i32,
    #[serde(default)]
    pub hand_size: i32,
    #[serde(default)]
    pub queue_length_limit: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExemptPriorityLevelConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominal_concurrency_shares: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lending_concurrency_limit: Option<i32>,
    /// Percent of the nominal concurrency limit lendable to other levels.
    /// Upstream `flowcontrol/v1` field; validated 0..=100.
    #[serde(rename = "lendablePercent", skip_serializing_if = "Option::is_none")]
    pub lendable_percent: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfigurationStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<PriorityLevelConfigurationCondition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfigurationCondition {
    #[serde(rename = "type", default)]
    pub type_: String,
    #[serde(default)]
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
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FlowSchemaSpec {
    #[serde(default)]
    pub priority_level_configuration: PriorityLevelConfigurationReference,
    /// Go: int32 — absent leaves 0; #[serde(default)] matches.
    #[serde(default)]
    pub matching_precedence: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distinguisher_method: Option<FlowDistinguisherMethod>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<Vec<PolicyRulesWithSubjects>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PriorityLevelConfigurationReference {
    #[serde(default)]
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
    /// Go: []FlowSchemaSubject — absent leaves empty slice; #[serde(default)] matches.
    #[serde(default)]
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
    /// Go: []string — absent leaves empty slice; #[serde(default)] matches.
    #[serde(default)]
    pub verbs: Vec<String>,
    /// Go: []string — absent leaves empty slice; #[serde(default)] matches.
    #[serde(default)]
    pub api_groups: Vec<String>,
    /// Go: []string — absent leaves empty slice; #[serde(default)] matches.
    #[serde(default)]
    pub resources: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_scope: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonResourcePolicyRule {
    /// Verbs is a list of matching HTTP verbs.
    /// Go: []string json:"verbs" — absent leaves empty slice; #[serde(default)] matches.
    #[serde(default)]
    pub verbs: Vec<String>,
    /// NonResourceURLs is a set of URL paths that have been matched.
    /// Go: []string json:"nonResourceURLs" — absent leaves empty slice; #[serde(default)]
    /// matches Go behavior so a rule omitting this field decodes instead of erroring.
    #[serde(rename = "nonResourceURLs", default)]
    pub non_resource_urls: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowSchemaStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<FlowSchemaCondition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FlowSchemaCondition {
    #[serde(rename = "type", default)]
    pub type_: String,
    #[serde(default)]
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FlowSchema nonResourceRules without nonResourceURLs must decode.
    /// Reproduces sig-api-machinery FlowControl 422 "missing field `nonResourceURLs`"
    /// from the 2026-05-31 conformance run (flowcontrol.go:379).
    /// Go: []string json:"nonResourceURLs" — absent → empty slice via #[serde(default)].
    #[test]
    fn test_non_resource_policy_rule_without_urls_decodes() {
        let json = r#"{"verbs": ["get"]}"#;
        let rule: NonResourcePolicyRule =
            serde_json::from_str(json).expect("NonResourcePolicyRule without urls must decode");
        assert_eq!(rule.verbs, vec!["get"]);
        assert!(rule.non_resource_urls.is_empty());
    }

    /// FlowSchema with missing required scalar fields must decode to zero values.
    #[test]
    fn test_flow_schema_spec_without_matching_precedence_decodes() {
        let json = r#"{
            "priorityLevelConfiguration": {"name": "exempt"},
            "rules": []
        }"#;
        let spec: FlowSchemaSpec = serde_json::from_str(json)
            .expect("FlowSchemaSpec without matchingPrecedence must decode");
        assert_eq!(spec.matching_precedence, 0);
    }

    /// ResourcePolicyRule without verbs/apiGroups/resources must decode to empty slices.
    #[test]
    fn test_resource_policy_rule_without_required_fields_decodes() {
        let json = r#"{}"#;
        let rule: ResourcePolicyRule = serde_json::from_str(json)
            .expect("ResourcePolicyRule without required fields must decode");
        assert!(rule.verbs.is_empty());
        assert!(rule.api_groups.is_empty());
        assert!(rule.resources.is_empty());
    }
}
