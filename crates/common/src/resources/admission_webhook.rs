// Admission Webhook Configuration resources
//
// This module defines ValidatingWebhookConfiguration and MutatingWebhookConfiguration
// resources that configure external admission webhooks.

use crate::resources::WebhookClientConfig;
use crate::types::ObjectMeta;
use serde::{Deserialize, Serialize};

/// ValidatingWebhookConfiguration describes admission webhooks that validate resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingWebhookConfiguration {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhooks: Option<Vec<ValidatingWebhook>>,
}

impl ValidatingWebhookConfiguration {
    pub fn new(name: &str) -> Self {
        Self {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
            metadata: ObjectMeta::new(name),
            webhooks: None,
        }
    }
}

/// ValidatingWebhook describes a single validating webhook
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingWebhook {
    /// Name is the full-qualified name of the webhook
    pub name: String,

    /// ClientConfig defines how to communicate with the webhook
    pub client_config: WebhookClientConfig,

    /// Rules describes what operations on what resources the webhook cares about
    pub rules: Vec<RuleWithOperations>,

    /// FailurePolicy defines how unrecognized errors are handled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,

    /// MatchPolicy defines how the rules are applied
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_policy: Option<MatchPolicy>,

    /// NamespaceSelector decides whether to run the webhook on an object based on namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,

    /// ObjectSelector decides whether to run the webhook on an object based on labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_selector: Option<LabelSelector>,

    /// SideEffects states whether this webhook has side effects
    pub side_effects: SideEffectClass,

    /// TimeoutSeconds specifies the timeout for this webhook (1-30 seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    /// AdmissionReviewVersions is an ordered list of AdmissionReview versions the webhook accepts
    pub admission_review_versions: Vec<String>,

    /// MatchConditions are CEL expressions that must be true for the webhook to be called
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_conditions: Option<Vec<MatchCondition>>,
}

/// MutatingWebhookConfiguration describes admission webhooks that mutate resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MutatingWebhookConfiguration {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhooks: Option<Vec<MutatingWebhook>>,
}

impl MutatingWebhookConfiguration {
    pub fn new(name: &str) -> Self {
        Self {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "MutatingWebhookConfiguration".to_string(),
            metadata: ObjectMeta::new(name),
            webhooks: None,
        }
    }
}

/// MutatingWebhook describes a single mutating webhook
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MutatingWebhook {
    /// Name is the full-qualified name of the webhook
    pub name: String,

    /// ClientConfig defines how to communicate with the webhook
    pub client_config: WebhookClientConfig,

    /// Rules describes what operations on what resources the webhook cares about
    pub rules: Vec<RuleWithOperations>,

    /// FailurePolicy defines how unrecognized errors are handled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,

    /// MatchPolicy defines how the rules are applied
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_policy: Option<MatchPolicy>,

    /// NamespaceSelector decides whether to run the webhook on an object based on namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,

    /// ObjectSelector decides whether to run the webhook on an object based on labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_selector: Option<LabelSelector>,

    /// SideEffects states whether this webhook has side effects
    pub side_effects: SideEffectClass,

    /// TimeoutSeconds specifies the timeout for this webhook (1-30 seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    /// AdmissionReviewVersions is an ordered list of AdmissionReview versions the webhook accepts
    pub admission_review_versions: Vec<String>,

    /// MatchConditions are CEL expressions that must be true for the webhook to be called
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_conditions: Option<Vec<MatchCondition>>,

    /// ReinvocationPolicy indicates whether this webhook should be called multiple times
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reinvocation_policy: Option<ReinvocationPolicy>,
}

/// RuleWithOperations describes what operations on what resources the webhook cares about
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuleWithOperations {
    /// Operations is the list of operations the webhook cares about
    pub operations: Vec<OperationType>,

    /// Rule is embedded, it describes other criteria
    #[serde(flatten)]
    pub rule: Rule,
}

/// Rule describes what resources and scopes to match
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    /// APIGroups is the API groups the resources belong to ('*' means all)
    #[serde(rename = "apiGroups")]
    pub api_groups: Vec<String>,

    /// APIVersions is the API versions the resources belong to ('*' means all)
    #[serde(rename = "apiVersions")]
    pub api_versions: Vec<String>,

    /// Resources is a list of resources this rule applies to ('*' means all)
    pub resources: Vec<String>,

    /// Scope specifies the scope of this rule (Cluster, Namespaced, or *)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// OperationType specifies an operation for a request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OperationType {
    Create,
    Update,
    Delete,
    Connect,
    #[serde(rename = "*")]
    All,
}

/// FailurePolicy defines how unrecognized errors from the webhook are handled
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FailurePolicy {
    /// Ignore means the error is ignored and the API request is allowed to continue
    Ignore,
    /// Fail means the API request is rejected
    Fail,
}

/// MatchPolicy defines how the rules are applied when the request matches multiple rules
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MatchPolicy {
    /// Exact means the request matches only exact rules
    Exact,
    /// Equivalent means the request matches equivalent rules
    Equivalent,
}

/// SideEffectClass denotes the level of side effects a webhook may have
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SideEffectClass {
    /// Unknown means the webhook may have unknown side effects
    Unknown,
    /// None means the webhook has no side effects on dryRun
    None,
    /// Some means the webhook may have side effects on dryRun
    Some,
    /// NoneOnDryRun means the webhook has no side effects when run in dry-run mode
    NoneOnDryRun,
}

/// ReinvocationPolicy indicates whether a webhook should be called multiple times
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReinvocationPolicy {
    /// Never means the webhook will not be called more than once in a single admission evaluation
    Never,
    /// IfNeeded means the webhook may be called again as part of the admission evaluation
    IfNeeded,
}

/// LabelSelector is used to select resources by labels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    /// MatchLabels is a map of {key,value} pairs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<std::collections::HashMap<String, String>>,

    /// MatchExpressions is a list of label selector requirements
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<LabelSelectorRequirement>>,
}

/// LabelSelectorRequirement is a selector that contains values, a key, and an operator
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorRequirement {
    /// Key is the label key that the selector applies to
    pub key: String,

    /// Operator represents a key's relationship to a set of values
    pub operator: LabelSelectorOperator,

    /// Values is an array of string values
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// LabelSelectorOperator is the set of operators for label selector requirements
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LabelSelectorOperator {
    In,
    NotIn,
    Exists,
    DoesNotExist,
}

/// MatchCondition represents a condition that must be fulfilled for a request to be sent to a webhook
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MatchCondition {
    /// Name is an identifier for this match condition
    pub name: String,

    /// Expression is a CEL expression that must evaluate to true
    pub expression: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validating_webhook_config_creation() {
        let config = ValidatingWebhookConfiguration::new("test-webhook");
        assert_eq!(config.metadata.name, "test-webhook");
        assert_eq!(config.api_version, "admissionregistration.k8s.io/v1");
        assert_eq!(config.kind, "ValidatingWebhookConfiguration");
    }

    #[test]
    fn test_mutating_webhook_config_creation() {
        let config = MutatingWebhookConfiguration::new("test-webhook");
        assert_eq!(config.metadata.name, "test-webhook");
        assert_eq!(config.api_version, "admissionregistration.k8s.io/v1");
        assert_eq!(config.kind, "MutatingWebhookConfiguration");
    }

    #[test]
    fn test_operation_type_serialization() {
        let create = serde_json::to_string(&OperationType::Create).unwrap();
        assert_eq!(create, r#""CREATE""#);

        let all = serde_json::to_string(&OperationType::All).unwrap();
        assert_eq!(all, r#""*""#);
    }

    #[test]
    fn test_failure_policy() {
        assert_eq!(
            serde_json::to_string(&FailurePolicy::Ignore).unwrap(),
            r#""Ignore""#
        );
        assert_eq!(
            serde_json::to_string(&FailurePolicy::Fail).unwrap(),
            r#""Fail""#
        );
    }

    #[test]
    fn test_side_effect_class() {
        assert_eq!(
            serde_json::to_string(&SideEffectClass::None).unwrap(),
            r#""None""#
        );
        assert_eq!(
            serde_json::to_string(&SideEffectClass::NoneOnDryRun).unwrap(),
            r#""NoneOnDryRun""#
        );
    }
}
