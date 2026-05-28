// Validating Admission Policy resources
//
// This module defines ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding
// resources that enable CEL-based admission control.

use crate::resources::serde_helpers::empty_string_as_none;
use crate::resources::{LabelSelector, MatchCondition};
use crate::types::ObjectMeta;
use serde::{Deserialize, Serialize};

/// ValidatingAdmissionPolicy describes the definition of a CEL-based admission validation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingAdmissionPolicy {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<ValidatingAdmissionPolicySpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ValidatingAdmissionPolicyStatus>,
}

impl ValidatingAdmissionPolicy {
    pub fn new(name: &str) -> Self {
        Self {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingAdmissionPolicy".to_string(),
            metadata: ObjectMeta::new(name),
            spec: None,
            status: None,
        }
    }
}

/// ValidatingAdmissionPolicySpec describes the policy spec
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingAdmissionPolicySpec {
    /// ParamKind specifies the kind of resources used to parameterize this policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param_kind: Option<ParamKind>,

    /// MatchConstraints specifies what resources this policy should validate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_constraints: Option<MatchResources>,

    /// Validations contain CEL expressions which is used to apply the validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validations: Option<Vec<Validation>>,

    /// FailurePolicy defines how to handle failures for the admission policy
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub failure_policy: Option<FailurePolicy>,

    /// AuditAnnotations contains CEL expressions which are used to produce audit annotations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit_annotations: Option<Vec<AuditAnnotation>>,

    /// MatchConditions is a list of conditions that must be met for a request to be validated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_conditions: Option<Vec<MatchCondition>>,

    /// Variables contain definitions of variables that can be used in composition of other expressions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<Vec<Variable>>,
}

/// ParamKind describes the kind of resources used to parameterize this policy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ParamKind {
    /// APIVersion is the API group version the resources belong to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,

    /// Kind is the API kind the resources belong to
    pub kind: String,
}

/// MatchResources decides what resources match the policy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MatchResources {
    /// NamespaceSelector decides whether to run the admission control on an object based on namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,

    /// ObjectSelector decides whether to run the validation based on object labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_selector: Option<LabelSelector>,

    /// ResourceRules describes what operations on what resources the policy matches
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_rules: Option<Vec<NamedRuleWithOperations>>,

    /// ExcludeResourceRules describes what operations on what resources should be excluded
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_resource_rules: Option<Vec<NamedRuleWithOperations>>,

    /// MatchPolicy defines how the "rules" list is used to match incoming requests
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub match_policy: Option<MatchPolicyType>,
}

/// NamedRuleWithOperations is a tuple of Operations and Resources with ResourceNames
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NamedRuleWithOperations {
    /// ResourceNames is an optional list of resource names that the rule applies to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_names: Option<Vec<String>>,

    /// RuleWithOperations describes operations and resources
    #[serde(flatten)]
    pub rule: RuleWithOperations,
}

/// RuleWithOperations describes what resources and operations match the policy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuleWithOperations {
    /// Operations is the operations the admission hook cares about
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operations: Option<Vec<OperationType>>,

    /// APIGroups is the API groups the resources belong to ('*' means all)
    #[serde(rename = "apiGroups")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_groups: Option<Vec<String>>,

    /// APIVersions is the API versions the resources belong to ('*' means all)
    #[serde(rename = "apiVersions")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_versions: Option<Vec<String>>,

    /// Resources is a list of resources this rule applies to ('*' means all)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<String>>,

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

/// MatchPolicyType describes how the policy matches resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MatchPolicyType {
    /// Exact means that only exact matches are considered
    Exact,
    /// Equivalent means that matches are considered if they are equivalent
    Equivalent,
}

/// Validation describes a validation rule written in CEL
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Validation {
    /// Expression is the CEL expression which is evaluated to validate the request
    pub expression: String,

    /// Message is the message to display when validation fails
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// MessageExpression is a CEL expression that evaluates to the validation failure message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_expression: Option<String>,

    /// Reason is the reason code for validation failure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<StatusReason>,

    /// ValidationActions specify how the validation is enforced
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_actions: Option<Vec<ValidationAction>>,
}

/// StatusReason is a brief CamelCase string that describes why validation failed
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StatusReason {
    Unauthorized,
    Forbidden,
    NotFound,
    AlreadyExists,
    Conflict,
    Invalid,
    BadRequest,
    MethodNotAllowed,
    NotAcceptable,
    Timeout,
    TooManyRequests,
    InternalError,
    ServiceUnavailable,
}

/// ValidationAction specifies the action to take when validation fails
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationAction {
    /// Deny rejects the request
    Deny,
    /// Warn returns a warning but allows the request
    Warn,
    /// Audit records a violation in audit logs but allows the request
    Audit,
}

/// FailurePolicy defines how unrecognized errors from the policy are handled
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FailurePolicy {
    /// Ignore means the error is ignored and the API request is allowed to continue
    Ignore,
    /// Fail means the API request is rejected
    Fail,
}

/// AuditAnnotation describes an audit annotation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AuditAnnotation {
    /// Key is the audit annotation key
    pub key: String,

    /// ValueExpression is a CEL expression which is evaluated to produce an audit annotation value
    pub value_expression: String,
}

/// Variable is a named expression that may be referenced by other expressions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Variable {
    /// Name is the name of the variable
    pub name: String,

    /// Expression is the CEL expression which is evaluated to set the value of the variable
    pub expression: String,
}

/// ValidatingAdmissionPolicyStatus represents the status of an admission validation policy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingAdmissionPolicyStatus {
    /// ObservedGeneration is the generation observed by the controller
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// TypeChecking contains results of type checking the expressions in the policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_checking: Option<TypeChecking>,

    /// Conditions represent the latest available observations of the policy's state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<PolicyCondition>>,
}

/// TypeChecking contains results of type checking the expressions in the ValidatingAdmissionPolicy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TypeChecking {
    /// ExpressionWarnings is a list of warnings for each expression
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_warnings: Option<Vec<ExpressionWarning>>,
}

/// ExpressionWarning is a warning for a specific expression
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionWarning {
    /// FieldRef is a reference to the field containing the expression
    pub field_ref: String,

    /// Warning is the warning message
    pub warning: String,
}

/// PolicyCondition describes a condition of the policy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PolicyCondition {
    /// Type is the type of the condition
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status is the status of the condition (True, False, or Unknown)
    pub status: String,

    /// LastTransitionTime is the last time the condition transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Reason is a brief CamelCase reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Message is a human-readable message indicating details about the transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// ValidatingAdmissionPolicyBinding binds a ValidatingAdmissionPolicy with parameters
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingAdmissionPolicyBinding {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<ValidatingAdmissionPolicyBindingSpec>,
}

impl ValidatingAdmissionPolicyBinding {
    pub fn new(name: &str) -> Self {
        Self {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingAdmissionPolicyBinding".to_string(),
            metadata: ObjectMeta::new(name),
            spec: None,
        }
    }
}

/// ValidatingAdmissionPolicyBindingSpec is the specification of the ValidatingAdmissionPolicyBinding
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ValidatingAdmissionPolicyBindingSpec {
    /// PolicyName references a ValidatingAdmissionPolicy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_name: Option<String>,

    /// ParamRef specifies the parameter resource used to configure the admission control policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param_ref: Option<ParamRef>,

    /// MatchResources declares what resources match this binding
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_resources: Option<MatchResources>,

    /// ValidationActions specifies how validation failures should be handled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_actions: Option<Vec<ValidationAction>>,
}

/// ParamRef describes how to locate the params to be used as input to expressions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ParamRef {
    /// Name is the name of the resource being referenced
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Namespace is the namespace of the referenced resource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Selector is a label selector for filtering which resources to use for params
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,

    /// ParameterNotFoundAction controls the behavior when the param resource is not found
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub parameter_not_found_action: Option<ParameterNotFoundAction>,
}

/// ParameterNotFoundAction defines the action to take when a parameter is not found
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ParameterNotFoundAction {
    /// Allow causes the validation to pass when the parameter is not found
    Allow,
    /// Deny causes the validation to fail when the parameter is not found
    Deny,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validating_admission_policy_creation() {
        let policy = ValidatingAdmissionPolicy::new("test-policy");
        assert_eq!(policy.metadata.name, "test-policy");
        assert_eq!(policy.api_version, "admissionregistration.k8s.io/v1");
        assert_eq!(policy.kind, "ValidatingAdmissionPolicy");
    }

    #[test]
    fn test_validating_admission_policy_binding_creation() {
        let binding = ValidatingAdmissionPolicyBinding::new("test-binding");
        assert_eq!(binding.metadata.name, "test-binding");
        assert_eq!(binding.api_version, "admissionregistration.k8s.io/v1");
        assert_eq!(binding.kind, "ValidatingAdmissionPolicyBinding");
    }

    #[test]
    fn test_validation_action_serialization() {
        let deny = serde_json::to_string(&ValidationAction::Deny).unwrap();
        assert_eq!(deny, r#""Deny""#);

        let warn = serde_json::to_string(&ValidationAction::Warn).unwrap();
        assert_eq!(warn, r#""Warn""#);

        let audit = serde_json::to_string(&ValidationAction::Audit).unwrap();
        assert_eq!(audit, r#""Audit""#);
    }

    #[test]
    fn test_validation_spec() {
        let validation = Validation {
            expression: "object.spec.replicas <= 5".to_string(),
            message: Some("Too many replicas".to_string()),
            message_expression: None,
            reason: Some(StatusReason::Invalid),
            validation_actions: Some(vec![ValidationAction::Deny]),
        };

        assert_eq!(validation.expression, "object.spec.replicas <= 5");
        assert_eq!(validation.message, Some("Too many replicas".to_string()));
    }

    #[test]
    fn test_param_kind() {
        let param_kind = ParamKind {
            api_version: Some("v1".to_string()),
            kind: "ConfigMap".to_string(),
        };

        let json = serde_json::to_string(&param_kind).unwrap();
        let deserialized: ParamKind = serde_json::from_str(&json).unwrap();
        assert_eq!(param_kind, deserialized);
    }

    #[test]
    fn test_match_resources() {
        let match_resources = MatchResources {
            namespace_selector: None,
            object_selector: None,
            resource_rules: Some(vec![NamedRuleWithOperations {
                resource_names: None,
                rule: RuleWithOperations {
                    operations: Some(vec![OperationType::Create, OperationType::Update]),
                    api_groups: Some(vec!["apps".to_string()]),
                    api_versions: Some(vec!["v1".to_string()]),
                    resources: Some(vec!["deployments".to_string()]),
                    scope: None,
                },
            }]),
            exclude_resource_rules: None,
            match_policy: Some(MatchPolicyType::Exact),
        };

        let json = serde_json::to_string(&match_resources).unwrap();
        let deserialized: MatchResources = serde_json::from_str(&json).unwrap();
        assert_eq!(match_resources, deserialized);
    }
}
