use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::ObjectMeta;

// ============================================================================
// SubjectAccessReview (authorization.k8s.io/v1)
// ============================================================================

/// SubjectAccessReview checks whether or not a user or group can perform an action.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectAccessReview {
    #[serde(default = "default_api_version_subject_access_review")]
    pub api_version: String,
    #[serde(default = "default_kind_subject_access_review")]
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: SubjectAccessReviewSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectAccessReviewStatus>,
}

fn default_api_version_subject_access_review() -> String {
    "authorization.k8s.io/v1".to_string()
}

fn default_kind_subject_access_review() -> String {
    "SubjectAccessReview".to_string()
}

/// SubjectAccessReviewSpec is a description of the access request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectAccessReviewSpec {
    /// Extra corresponds to the user.Info.GetExtra() method from the authenticator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<HashMap<String, Vec<String>>>,

    /// Groups is the groups you're testing for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<String>>,

    /// NonResourceAttributes describes information for a non-resource access request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub non_resource_attributes: Option<NonResourceAttributes>,

    /// ResourceAuthorizationAttributes describes information for a resource access request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_attributes: Option<ResourceAttributes>,

    /// UID information about the requesting user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    /// User is the user you're testing for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// NonResourceAttributes includes the authorization attributes available for non-resource requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonResourceAttributes {
    /// Path is the URL path of the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Verb is the standard HTTP verb.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verb: Option<String>,
}

/// ResourceAttributes includes the authorization attributes available for resource requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceAttributes {
    /// FieldSelector describes the limitation on access based on field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_selector: Option<FieldSelectorAttributes>,

    /// Group is the API Group of the Resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,

    /// LabelSelector describes the limitation on access based on labels.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<LabelSelectorAttributes>,

    /// Name is the name of the resource being requested for a "get" or deleted for a "delete".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Namespace is the namespace of the action being requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Resource is one of the existing resource types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,

    /// Subresource is one of the existing resource types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subresource: Option<String>,

    /// Verb is a kubernetes resource API verb.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verb: Option<String>,

    /// Version is the API Version of the Resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// FieldSelectorAttributes indicates a field limited access.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldSelectorAttributes {
    /// RawSelector is the serialized form of the field selector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_selector: Option<String>,

    /// Requirements is the parsed interpretation of a field selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirements: Option<Vec<FieldSelectorRequirement>>,
}

/// FieldSelectorRequirement is a selector that contains values, a key, and an operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldSelectorRequirement {
    /// Key is the field selector key that the requirement applies to.
    pub key: String,

    /// Operator represents a key's relationship to a set of values.
    pub operator: String,

    /// Values is an array of string values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// LabelSelectorAttributes indicates a label limited access.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorAttributes {
    /// RawSelector is the serialized form of the label selector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_selector: Option<String>,

    /// Requirements is the parsed interpretation of a label selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirements: Option<Vec<LabelSelectorRequirement>>,
}

/// LabelSelectorRequirement is a selector that contains values, a key, and an operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorRequirement {
    /// Key is the label key that the selector applies to.
    pub key: String,

    /// Operator represents a key's relationship to a set of values.
    pub operator: String,

    /// Values is an array of string values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// SubjectAccessReviewStatus represents the current state of a SubjectAccessReview.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubjectAccessReviewStatus {
    /// Allowed is required. True if the action would be allowed, false otherwise.
    pub allowed: bool,

    /// Denied is optional. True if the action would be denied, otherwise false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denied: Option<bool>,

    /// EvaluationError is an indication that some error occurred during the authorization check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_error: Option<String>,

    /// Reason is optional. It indicates why a request was allowed or denied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// ============================================================================
// SelfSubjectAccessReview (authorization.k8s.io/v1)
// ============================================================================

/// SelfSubjectAccessReview checks whether or the current user can perform an action.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfSubjectAccessReview {
    #[serde(default = "default_api_version_self_subject_access_review")]
    pub api_version: String,
    #[serde(default = "default_kind_self_subject_access_review")]
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: SelfSubjectAccessReviewSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectAccessReviewStatus>,
}

fn default_api_version_self_subject_access_review() -> String {
    "authorization.k8s.io/v1".to_string()
}

fn default_kind_self_subject_access_review() -> String {
    "SelfSubjectAccessReview".to_string()
}

/// SelfSubjectAccessReviewSpec is a description of the access request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfSubjectAccessReviewSpec {
    /// NonResourceAttributes describes information for a non-resource access request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub non_resource_attributes: Option<NonResourceAttributes>,

    /// ResourceAuthorizationAttributes describes information for a resource access request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_attributes: Option<ResourceAttributes>,
}

// ============================================================================
// LocalSubjectAccessReview (authorization.k8s.io/v1)
// ============================================================================

/// LocalSubjectAccessReview checks whether or not a user or group can perform an action in a given namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalSubjectAccessReview {
    #[serde(default = "default_api_version_local_subject_access_review")]
    pub api_version: String,
    #[serde(default = "default_kind_local_subject_access_review")]
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: SubjectAccessReviewSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectAccessReviewStatus>,
}

fn default_api_version_local_subject_access_review() -> String {
    "authorization.k8s.io/v1".to_string()
}

fn default_kind_local_subject_access_review() -> String {
    "LocalSubjectAccessReview".to_string()
}

// ============================================================================
// SelfSubjectRulesReview (authorization.k8s.io/v1)
// ============================================================================

/// SelfSubjectRulesReview enumerates the set of actions the current user can perform within a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfSubjectRulesReview {
    #[serde(default = "default_api_version_self_subject_rules_review")]
    pub api_version: String,
    #[serde(default = "default_kind_self_subject_rules_review")]
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: SelfSubjectRulesReviewSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectRulesReviewStatus>,
}

fn default_api_version_self_subject_rules_review() -> String {
    "authorization.k8s.io/v1".to_string()
}

fn default_kind_self_subject_rules_review() -> String {
    "SelfSubjectRulesReview".to_string()
}

/// SelfSubjectRulesReviewSpec defines the specification for SelfSubjectRulesReview.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfSubjectRulesReviewSpec {
    /// Namespace to evaluate rules for. Required.
    pub namespace: String,
}

/// SubjectRulesReviewStatus contains the result of a rules check.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubjectRulesReviewStatus {
    /// EvaluationError can appear in combination with Rules. It indicates an error occurred during
    /// rule evaluation, such as an authorizer that doesn't support rule evaluation, and that
    /// ResourceRules and/or NonResourceRules may be incomplete.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_error: Option<String>,

    /// Incomplete is true when the rules returned by this call are incomplete.
    pub incomplete: bool,

    /// NonResourceRules is the list of actions the subject is allowed to perform on non-resources.
    pub non_resource_rules: Vec<NonResourceRule>,

    /// ResourceRules is the list of actions the subject is allowed to perform on resources.
    pub resource_rules: Vec<ResourceRule>,
}

/// NonResourceRule holds information that describes a rule for the non-resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonResourceRule {
    /// NonResourceURLs is a set of partial urls that a user should have access to.
    #[serde(
        rename = "nonResourceURLs",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub non_resource_urls: Option<Vec<String>>,

    /// Verb is a list of kubernetes non-resource API verbs.
    pub verbs: Vec<String>,
}

/// ResourceRule is the list of actions the subject is allowed to perform on resources.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRule {
    /// APIGroups is the name of the APIGroup that contains the resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_groups: Option<Vec<String>>,

    /// ResourceNames is an optional white list of names that the rule applies to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_names: Option<Vec<String>>,

    /// Resources is a list of resources this rule applies to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<String>>,

    /// Verb is a list of kubernetes resource API verbs.
    pub verbs: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_access_review_serialization() {
        let sar = SubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SubjectAccessReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: SubjectAccessReviewSpec {
                user: Some("admin".to_string()),
                groups: Some(vec!["system:masters".to_string()]),
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("default".to_string()),
                    verb: Some("get".to_string()),
                    group: Some("".to_string()),
                    version: Some("v1".to_string()),
                    resource: Some("pods".to_string()),
                    subresource: None,
                    name: None,
                    field_selector: None,
                    label_selector: None,
                }),
                non_resource_attributes: None,
                extra: None,
                uid: None,
            },
            status: Some(SubjectAccessReviewStatus {
                allowed: true,
                denied: Some(false),
                evaluation_error: None,
                reason: Some("RBAC allows".to_string()),
            }),
        };

        let json = serde_json::to_string(&sar).unwrap();
        assert!(json.contains("authorization.k8s.io/v1"));
        assert!(json.contains("SubjectAccessReview"));
        assert!(json.contains("admin"));
    }

    #[test]
    fn test_self_subject_access_review_serialization() {
        let ssar = SelfSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectAccessReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("default".to_string()),
                    verb: Some("create".to_string()),
                    group: Some("".to_string()),
                    version: Some("v1".to_string()),
                    resource: Some("pods".to_string()),
                    subresource: None,
                    name: None,
                    field_selector: None,
                    label_selector: None,
                }),
                non_resource_attributes: None,
            },
            status: None,
        };

        let json = serde_json::to_string(&ssar).unwrap();
        assert!(json.contains("authorization.k8s.io/v1"));
        assert!(json.contains("SelfSubjectAccessReview"));
    }

    #[test]
    fn test_local_subject_access_review_serialization() {
        let lsar = LocalSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "LocalSubjectAccessReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: SubjectAccessReviewSpec {
                user: Some("user1".to_string()),
                groups: None,
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("default".to_string()),
                    verb: Some("delete".to_string()),
                    group: Some("".to_string()),
                    version: Some("v1".to_string()),
                    resource: Some("services".to_string()),
                    subresource: None,
                    name: None,
                    field_selector: None,
                    label_selector: None,
                }),
                non_resource_attributes: None,
                extra: None,
                uid: None,
            },
            status: None,
        };

        let json = serde_json::to_string(&lsar).unwrap();
        assert!(json.contains("authorization.k8s.io/v1"));
        assert!(json.contains("LocalSubjectAccessReview"));
    }

    #[test]
    fn test_self_subject_rules_review_serialization() {
        let ssrr = SelfSubjectRulesReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectRulesReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: SelfSubjectRulesReviewSpec {
                namespace: "default".to_string(),
            },
            status: Some(SubjectRulesReviewStatus {
                incomplete: false,
                non_resource_rules: vec![],
                resource_rules: vec![ResourceRule {
                    verbs: vec!["get".to_string(), "list".to_string()],
                    api_groups: Some(vec!["".to_string()]),
                    resources: Some(vec!["pods".to_string()]),
                    resource_names: None,
                }],
                evaluation_error: None,
            }),
        };

        let json = serde_json::to_string(&ssrr).unwrap();
        assert!(json.contains("authorization.k8s.io/v1"));
        assert!(json.contains("SelfSubjectRulesReview"));
    }
}
