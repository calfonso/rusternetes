use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// Role is a namespaced, logical grouping of PolicyRules that can be referenced as a unit by a RoleBinding
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Role {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    /// Rules holds all the PolicyRules for this Role
    pub rules: Vec<PolicyRule>,
}

impl Role {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Role".to_string(),
                api_version: "rbac.authorization.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            rules: vec![],
        }
    }

    pub fn with_rules(mut self, rules: Vec<PolicyRule>) -> Self {
        self.rules = rules;
        self
    }
}

/// ClusterRole is a cluster level, logical grouping of PolicyRules that can be referenced as a unit by a RoleBinding or ClusterRoleBinding
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRole {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    /// Rules holds all the PolicyRules for this ClusterRole
    pub rules: Vec<PolicyRule>,

    /// AggregationRule is an optional field that describes how to build the Rules for this ClusterRole
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregation_rule: Option<AggregationRule>,
}

impl ClusterRole {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ClusterRole".to_string(),
                api_version: "rbac.authorization.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            rules: vec![],
            aggregation_rule: None,
        }
    }

    pub fn with_rules(mut self, rules: Vec<PolicyRule>) -> Self {
        self.rules = rules;
        self
    }
}

/// PolicyRule holds information that describes a policy rule, but does not contain information about who the rule applies to or which namespace the rule applies to
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRule {
    /// Verbs is a list of Verbs that apply to ALL the ResourceKinds and AttributeRestrictions contained in this rule
    /// Examples: get, list, watch, create, update, patch, delete
    pub verbs: Vec<String>,

    /// APIGroups is the name of the APIGroup that contains the resources
    /// If multiple API groups are specified, any action requested against one of the enumerated resources in any API group will be allowed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_groups: Option<Vec<String>>,

    /// Resources is a list of resources this rule applies to
    /// Examples: pods, services, deployments
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<String>>,

    /// ResourceNames is an optional white list of names that the rule applies to
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_names: Option<Vec<String>>,

    /// NonResourceURLs is a set of partial urls that a user should have access to
    /// *s are allowed, but only as the full, final step in the path
    #[serde(
        rename = "nonResourceURLs",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub non_resource_urls: Option<Vec<String>>,
}

impl PolicyRule {
    pub fn new(verbs: Vec<String>) -> Self {
        Self {
            verbs,
            api_groups: None,
            resources: None,
            resource_names: None,
            non_resource_urls: None,
        }
    }

    pub fn with_api_groups(mut self, api_groups: Vec<String>) -> Self {
        self.api_groups = Some(api_groups);
        self
    }

    pub fn with_resources(mut self, resources: Vec<String>) -> Self {
        self.resources = Some(resources);
        self
    }
}

/// RoleBinding references a role, but does not contain it. It can reference a Role in the same namespace or a ClusterRole in the global namespace
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RoleBinding {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    /// Subjects holds references to the objects the role applies to
    pub subjects: Vec<Subject>,

    /// RoleRef can reference a Role in the current namespace or a ClusterRole in the global namespace
    #[serde(alias = "roleRef")]
    pub role_ref: RoleRef,
}

impl RoleBinding {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "RoleBinding".to_string(),
                api_version: "rbac.authorization.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            subjects: vec![],
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "Role".to_string(),
                name: String::new(),
            },
        }
    }

    pub fn with_subjects(mut self, subjects: Vec<Subject>) -> Self {
        self.subjects = subjects;
        self
    }

    pub fn with_role_ref(mut self, role_ref: RoleRef) -> Self {
        self.role_ref = role_ref;
        self
    }
}

/// ClusterRoleBinding references a ClusterRole, but not contain it
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRoleBinding {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    /// Subjects holds references to the objects the role applies to
    pub subjects: Vec<Subject>,

    /// RoleRef can only reference a ClusterRole in the global namespace
    #[serde(alias = "roleRef")]
    pub role_ref: RoleRef,
}

impl ClusterRoleBinding {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ClusterRoleBinding".to_string(),
                api_version: "rbac.authorization.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            subjects: vec![],
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "ClusterRole".to_string(),
                name: String::new(),
            },
        }
    }

    pub fn with_subjects(mut self, subjects: Vec<Subject>) -> Self {
        self.subjects = subjects;
        self
    }

    pub fn with_role_ref(mut self, role_ref: RoleRef) -> Self {
        self.role_ref = role_ref;
        self
    }
}

/// Subject contains a reference to the object or user identities a role binding applies to
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Subject {
    /// Kind of object being referenced. Values defined by this API group are "User", "Group", and "ServiceAccount"
    pub kind: String,

    /// Name of the object being referenced
    pub name: String,

    /// Namespace of the referenced object. If the object kind is non-namespace, such as "User" or "Group", this field should be empty
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// APIGroup holds the API group of the referenced subject
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,
}

impl Subject {
    pub fn service_account(name: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            kind: "ServiceAccount".to_string(),
            name: name.into(),
            namespace: Some(namespace.into()),
            api_group: None,
        }
    }

    pub fn user(name: impl Into<String>) -> Self {
        Self {
            kind: "User".to_string(),
            name: name.into(),
            namespace: None,
            api_group: Some("rbac.authorization.k8s.io".to_string()),
        }
    }

    pub fn group(name: impl Into<String>) -> Self {
        Self {
            kind: "Group".to_string(),
            name: name.into(),
            namespace: None,
            api_group: Some("rbac.authorization.k8s.io".to_string()),
        }
    }
}

/// RoleRef contains information that points to the role being used
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RoleRef {
    /// APIGroup is the group for the resource being referenced
    #[serde(alias = "apiGroup")]
    pub api_group: String,

    /// Kind is the type of resource being referenced
    pub kind: String,

    /// Name is the name of resource being referenced
    pub name: String,
}

impl RoleRef {
    pub fn role(name: impl Into<String>) -> Self {
        Self {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "Role".to_string(),
            name: name.into(),
        }
    }

    pub fn cluster_role(name: impl Into<String>) -> Self {
        Self {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "ClusterRole".to_string(),
            name: name.into(),
        }
    }
}

/// AggregationRule describes how to locate ClusterRoles to aggregate into the ClusterRole
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AggregationRule {
    /// ClusterRoleSelectors holds a list of selectors which will be used to find ClusterRoles and create the rules
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_role_selectors: Option<Vec<crate::types::LabelSelector>>,
}
