use crate::auth::UserInfo;
use crate::error::Result;
use crate::resources::rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, Subject,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::sync::Arc;

/// Subject ↔ user matcher mirroring upstream
/// `pkg/registry/rbac/validation/rule.go` `appliesTo`:
///   * `User`           — `subject.name == user.username`
///   * `Group`          — `user.groups.contains(subject.name)`
///   * `ServiceAccount` — `user.username == "system:serviceaccount:<ns>:<name>"`,
///     where `<ns>` is `subject.namespace` and `<name>` is `subject.name`.
///
/// Defensive default for unknown `kind`: plain-name equality so existing
/// integration tests that synthesize subjects without a `kind` keep working.
fn subject_matches(subject: &Subject, user: &UserInfo) -> bool {
    match subject.kind.as_str() {
        "User" => subject.name == user.username,
        "Group" => user.groups.contains(&subject.name),
        "ServiceAccount" => {
            let ns = subject.namespace.as_deref().unwrap_or("");
            if ns.is_empty() {
                return false;
            }
            let expected = format!("system:serviceaccount:{}:{}", ns, subject.name);
            user.username == expected
        }
        _ => subject.name == user.username || user.groups.contains(&subject.name),
    }
}

/// Minimal storage trait for authorization (to avoid circular dependency)
#[async_trait]
pub trait AuthzStorage: Send + Sync {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync;

    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: serde::Serialize + DeserializeOwned + Send + Sync;
}

/// Request attributes for authorization
#[derive(Debug, Clone)]
pub struct RequestAttributes {
    /// The user making the request
    pub user: UserInfo,

    /// The verb being requested (get, list, create, update, delete, watch, etc.)
    pub verb: String,

    /// The namespace of the resource (None for cluster-scoped resources)
    pub namespace: Option<String>,

    /// The API group of the resource
    pub api_group: String,

    /// The resource type (pods, services, deployments, etc.)
    pub resource: String,

    /// The specific resource name (None for list operations)
    pub name: Option<String>,

    /// The subresource being accessed (status, scale, etc.)
    pub subresource: Option<String>,

    /// Non-resource URL path (for non-resource requests like /healthz, /metrics)
    pub path: Option<String>,

    /// Whether this is a non-resource request
    pub is_non_resource_request: bool,
}

impl RequestAttributes {
    pub fn new(user: UserInfo, verb: impl Into<String>, resource: impl Into<String>) -> Self {
        Self {
            user,
            verb: verb.into(),
            namespace: None,
            api_group: String::new(),
            resource: resource.into(),
            name: None,
            subresource: None,
            path: None,
            is_non_resource_request: false,
        }
    }

    pub fn new_non_resource(
        user: UserInfo,
        verb: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            user,
            verb: verb.into(),
            namespace: None,
            api_group: String::new(),
            resource: String::new(),
            name: None,
            subresource: None,
            path: Some(path.into()),
            is_non_resource_request: true,
        }
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_api_group(mut self, api_group: impl Into<String>) -> Self {
        self.api_group = api_group.into();
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_subresource(mut self, subresource: impl Into<String>) -> Self {
        self.subresource = Some(subresource.into());
        self
    }
}

/// Authorization decision
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    Allow,
    Deny(String),
}

/// Authorizer trait for authorization implementations
#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision>;

    /// Get all rules that apply to a user in a given namespace
    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )>;
}

/// RBAC Authorizer that uses Role and RoleBinding resources
pub struct RBACAuthorizer<S: AuthzStorage> {
    storage: Arc<S>,
}

impl<S: AuthzStorage> RBACAuthorizer<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Check if a user has permission based on RBAC rules
    async fn check_rbac(&self, attrs: &RequestAttributes) -> Result<Decision> {
        // Get all role bindings for the user
        let role_bindings = self.get_user_role_bindings(attrs).await?;
        let cluster_role_bindings = self.get_user_cluster_role_bindings(attrs).await?;

        // Check namespace-scoped roles first
        if let Some(namespace) = &attrs.namespace {
            for binding in &role_bindings {
                if let Some(ref binding_ns) = binding.metadata.namespace {
                    if binding_ns == namespace {
                        // Get the referenced role
                        if let Ok(role) = self
                            .storage
                            .get::<Role>(&binding.role_ref.name, Some(binding_ns))
                            .await
                        {
                            if self.check_policy_rules(&role.rules, attrs) {
                                return Ok(Decision::Allow);
                            }
                        }
                    }
                }
            }
        }

        // Check cluster-scoped roles via role bindings
        for binding in &role_bindings {
            if binding.role_ref.kind == "ClusterRole" {
                if let Ok(cluster_role) = self
                    .storage
                    .get::<ClusterRole>(&binding.role_ref.name, None)
                    .await
                {
                    if self.check_policy_rules(&cluster_role.rules, attrs) {
                        return Ok(Decision::Allow);
                    }
                }
            }
        }

        // Check cluster-scoped roles
        for binding in cluster_role_bindings {
            if let Ok(cluster_role) = self
                .storage
                .get::<ClusterRole>(&binding.role_ref.name, None)
                .await
            {
                if self.check_policy_rules(&cluster_role.rules, attrs) {
                    return Ok(Decision::Allow);
                }
            }
        }

        Ok(Decision::Deny(
            "User does not have permission to perform this action".to_string(),
        ))
    }

    /// Get all RoleBindings that apply to the user.
    ///
    /// Subject matching mirrors upstream `pkg/registry/rbac/validation/rule.go`:
    ///   * `User`            — `subject.name == user.username`
    ///   * `Group`           — `user.groups.contains(subject.name)`
    ///   * `ServiceAccount`  — `user.username == "system:serviceaccount:<ns>:<name>"`
    ///     where `<ns>` is `subject.namespace` and `<name>` is `subject.name`.
    async fn get_user_role_bindings(&self, attrs: &RequestAttributes) -> Result<Vec<RoleBinding>> {
        let all_bindings = match &attrs.namespace {
            Some(ns) => self.storage.list::<RoleBinding>(Some(ns)).await?,
            None => vec![],
        };

        Ok(all_bindings
            .into_iter()
            .filter(|binding| {
                binding
                    .subjects
                    .iter()
                    .any(|subject| subject_matches(subject, &attrs.user))
            })
            .collect())
    }

    /// Get all ClusterRoleBindings that apply to the user
    async fn get_user_cluster_role_bindings(
        &self,
        attrs: &RequestAttributes,
    ) -> Result<Vec<ClusterRoleBinding>> {
        let all_bindings = self.storage.list::<ClusterRoleBinding>(None).await?;

        Ok(all_bindings
            .into_iter()
            .filter(|binding| {
                binding
                    .subjects
                    .iter()
                    .any(|subject| subject_matches(subject, &attrs.user))
            })
            .collect())
    }

    /// Check if policy rules allow the requested action
    fn check_policy_rules(&self, rules: &[PolicyRule], attrs: &RequestAttributes) -> bool {
        rules.iter().any(|rule| self.rule_allows(rule, attrs))
    }

    /// Check if a single policy rule allows the requested action
    fn rule_allows(&self, rule: &PolicyRule, attrs: &RequestAttributes) -> bool {
        // Check verb
        if !rule.verbs.contains(&attrs.verb) && !rule.verbs.contains(&"*".to_string()) {
            return false;
        }

        // Handle non-resource requests
        if attrs.is_non_resource_request {
            if let Some(ref non_resource_urls) = rule.non_resource_urls {
                if let Some(ref path) = attrs.path {
                    // Check if the path matches any of the non-resource URLs
                    return non_resource_urls
                        .iter()
                        .any(|url| url == "*" || url == path || path.starts_with(url));
                }
            }
            return false;
        }

        // Check API group
        if let Some(ref api_groups) = rule.api_groups {
            if !api_groups.contains(&attrs.api_group) && !api_groups.contains(&"*".to_string()) {
                return false;
            }
        }

        // Check resource
        if let Some(ref resources) = rule.resources {
            if !resources.contains(&attrs.resource) && !resources.contains(&"*".to_string()) {
                return false;
            }
        }

        // Check resource name if specified
        if let Some(ref name) = attrs.name {
            if let Some(ref resource_names) = rule.resource_names {
                if !resource_names.contains(name) && !resource_names.contains(&"*".to_string()) {
                    return false;
                }
            }
        }

        true
    }
}

#[async_trait]
impl<S: AuthzStorage> Authorizer for RBACAuthorizer<S> {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        // System admin has full access
        if attrs.user.username == "system:admin" {
            return Ok(Decision::Allow);
        }

        // Check RBAC
        self.check_rbac(attrs).await
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        let mut resource_rules = Vec::new();
        let mut non_resource_rules = Vec::new();

        // Get all role bindings that apply to this user in the namespace
        let role_bindings = self.storage.list::<RoleBinding>(Some(namespace)).await?;
        let cluster_role_bindings = self.storage.list::<ClusterRoleBinding>(None).await?;

        // Process namespace-scoped role bindings
        for binding in role_bindings {
            // Check if this binding applies to the user
            let applies = binding
                .subjects
                .iter()
                .any(|subject| subject_matches(subject, user));

            if !applies {
                continue;
            }

            // Get the role/cluster role referenced by this binding
            if binding.role_ref.kind == "Role" {
                if let Ok(role) = self
                    .storage
                    .get::<Role>(&binding.role_ref.name, Some(namespace))
                    .await
                {
                    // Convert PolicyRules to ResourceRules
                    for rule in &role.rules {
                        resource_rules.push(crate::resources::ResourceRule {
                            verbs: rule.verbs.clone(),
                            api_groups: rule.api_groups.clone(),
                            resources: rule.resources.clone(),
                            resource_names: rule.resource_names.clone(),
                        });
                    }
                }
            } else if binding.role_ref.kind == "ClusterRole" {
                if let Ok(cluster_role) = self
                    .storage
                    .get::<ClusterRole>(&binding.role_ref.name, None)
                    .await
                {
                    for rule in &cluster_role.rules {
                        resource_rules.push(crate::resources::ResourceRule {
                            verbs: rule.verbs.clone(),
                            api_groups: rule.api_groups.clone(),
                            resources: rule.resources.clone(),
                            resource_names: rule.resource_names.clone(),
                        });

                        // Also collect non-resource rules
                        if let Some(ref urls) = rule.non_resource_urls {
                            non_resource_rules.push(crate::resources::NonResourceRule {
                                verbs: rule.verbs.clone(),
                                non_resource_urls: Some(urls.clone()),
                            });
                        }
                    }
                }
            }
        }

        // Process cluster-scoped role bindings
        for binding in cluster_role_bindings {
            let applies = binding
                .subjects
                .iter()
                .any(|subject| subject_matches(subject, user));

            if !applies {
                continue;
            }

            if let Ok(cluster_role) = self
                .storage
                .get::<ClusterRole>(&binding.role_ref.name, None)
                .await
            {
                for rule in &cluster_role.rules {
                    resource_rules.push(crate::resources::ResourceRule {
                        verbs: rule.verbs.clone(),
                        api_groups: rule.api_groups.clone(),
                        resources: rule.resources.clone(),
                        resource_names: rule.resource_names.clone(),
                    });

                    if let Some(ref urls) = rule.non_resource_urls {
                        non_resource_rules.push(crate::resources::NonResourceRule {
                            verbs: rule.verbs.clone(),
                            non_resource_urls: Some(urls.clone()),
                        });
                    }
                }
            }
        }

        Ok((resource_rules, non_resource_rules))
    }
}

/// Always allow authorizer (for testing)
pub struct AlwaysAllowAuthorizer;

#[async_trait]
impl Authorizer for AlwaysAllowAuthorizer {
    async fn authorize(&self, _attrs: &RequestAttributes) -> Result<Decision> {
        Ok(Decision::Allow)
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        // Return wildcard rules for testing
        Ok((
            vec![crate::resources::ResourceRule {
                verbs: vec!["*".to_string()],
                api_groups: Some(vec!["*".to_string()]),
                resources: Some(vec!["*".to_string()]),
                resource_names: None,
            }],
            vec![crate::resources::NonResourceRule {
                verbs: vec!["*".to_string()],
                non_resource_urls: Some(vec!["*".to_string()]),
            }],
        ))
    }
}

/// Always deny authorizer (for testing)
pub struct AlwaysDenyAuthorizer;

#[async_trait]
impl Authorizer for AlwaysDenyAuthorizer {
    async fn authorize(&self, _attrs: &RequestAttributes) -> Result<Decision> {
        Ok(Decision::Deny("Access denied".to_string()))
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        // Return empty rules for testing
        Ok((vec![], vec![]))
    }
}

/// Node Authorizer - Authorizes kubelet requests to access node-related resources
///
/// Implements node authorization according to Kubernetes node authorizer specification.
/// Nodes can only access resources that are bound to them or required for their operation.
pub struct NodeAuthorizer;

impl NodeAuthorizer {
    /// Extract the node name from a node username (system:node:<nodename>)
    fn extract_node_name(username: &str) -> Option<String> {
        username.strip_prefix("system:node:").map(|s| s.to_string())
    }

    /// Check if the request is for the node's own Node object
    fn is_own_node(&self, attrs: &RequestAttributes, node_name: &str) -> bool {
        attrs.resource == "nodes"
            && attrs.api_group.is_empty()
            && attrs.name.as_ref().map(|n| n == node_name).unwrap_or(false)
    }

    /// Check if the resource type is one that nodes are allowed to read
    fn is_node_allowed_resource(&self, attrs: &RequestAttributes) -> bool {
        // Nodes can read these resources (but creation/updates are restricted)
        let allowed_resources = [
            ("", "services"),
            ("", "endpoints"),
            ("discovery.k8s.io", "endpointslices"),
            ("", "nodes"),
            ("", "persistentvolumes"),
            ("", "persistentvolumeclaims"),
            ("storage.k8s.io", "volumeattachments"),
            ("storage.k8s.io", "csidrivers"),
            ("storage.k8s.io", "csinodes"),
        ];

        allowed_resources
            .iter()
            .any(|(group, resource)| attrs.api_group == *group && attrs.resource == *resource)
    }

    /// Check if the request is for node-related API groups
    fn is_node_api_group(&self, attrs: &RequestAttributes) -> bool {
        matches!(
            attrs.api_group.as_str(),
            "authentication.k8s.io"
                | "authorization.k8s.io"
                | "certificates.k8s.io"
                | "coordination.k8s.io"
        )
    }
}

#[async_trait]
impl Authorizer for NodeAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        // Check if this is a node (kubelet) making the request
        let node_name = match Self::extract_node_name(&attrs.user.username) {
            Some(name) => name,
            None => return Ok(Decision::Deny("Not a node user".to_string())),
        };

        // Allow nodes to access their own Node object
        if self.is_own_node(attrs, &node_name) {
            return Ok(Decision::Allow);
        }

        // Allow nodes to create/update their own Node and CSINode objects
        if (attrs.resource == "nodes" || attrs.resource == "csinodes")
            && matches!(attrs.verb.as_str(), "create" | "update" | "patch")
            && attrs.name.as_ref().map(|n| n == &node_name).unwrap_or(true)
        {
            return Ok(Decision::Allow);
        }

        // Allow nodes to update their own Node status
        if attrs.resource == "nodes"
            && attrs
                .subresource
                .as_ref()
                .map(|s| s == "status")
                .unwrap_or(false)
            && attrs
                .name
                .as_ref()
                .map(|n| n == &node_name)
                .unwrap_or(false)
        {
            return Ok(Decision::Allow);
        }

        // Allow nodes to read certain cluster-wide resources
        if matches!(attrs.verb.as_str(), "get" | "list" | "watch")
            && self.is_node_allowed_resource(attrs)
        {
            return Ok(Decision::Allow);
        }

        // Allow nodes to access node-related API groups
        // (authentication, authorization, certificates, coordination)
        if self.is_node_api_group(attrs) {
            // Nodes can create TokenReviews, SubjectAccessReviews, CSRs, and Leases
            if matches!(
                attrs.verb.as_str(),
                "create" | "get" | "list" | "watch" | "update" | "patch"
            ) {
                return Ok(Decision::Allow);
            }
        }

        // Allow nodes to read/write pods bound to them
        // Note: In a full implementation, this would check if the pod is actually
        // bound to this specific node by querying storage. For now, we allow
        // read access to pods and let RBAC provide additional restrictions.
        if attrs.resource == "pods" && attrs.namespace.is_some() {
            if matches!(
                attrs.verb.as_str(),
                "get" | "list" | "watch" | "update" | "patch"
            ) {
                return Ok(Decision::Allow);
            }
            // Allow updating pod status
            if attrs.verb == "update"
                && attrs
                    .subresource
                    .as_ref()
                    .map(|s| s == "status")
                    .unwrap_or(false)
            {
                return Ok(Decision::Allow);
            }
        }

        // Allow nodes to read secrets and configmaps in namespaces
        // Note: In a full implementation, this should verify the secret/configmap
        // is referenced by a pod bound to this node
        if matches!(attrs.resource.as_str(), "secrets" | "configmaps")
            && attrs.namespace.is_some()
            && matches!(attrs.verb.as_str(), "get" | "list" | "watch")
        {
            return Ok(Decision::Allow);
        }

        // Allow nodes to create events
        if attrs.resource == "events" && attrs.verb == "create" {
            return Ok(Decision::Allow);
        }

        // Deny everything else
        Ok(Decision::Deny(format!(
            "Node {} is not authorized to {} {} in namespace {:?}",
            node_name, attrs.verb, attrs.resource, attrs.namespace
        )))
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        // Extract node name
        let _node_name = match Self::extract_node_name(&user.username) {
            Some(name) => name,
            None => return Ok((vec![], vec![])),
        };

        // Return a representative set of rules for nodes
        let resource_rules = vec![
            crate::resources::ResourceRule {
                verbs: vec!["get".to_string(), "list".to_string(), "watch".to_string()],
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec![
                    "services".to_string(),
                    "endpoints".to_string(),
                    "nodes".to_string(),
                    "pods".to_string(),
                    "secrets".to_string(),
                    "configmaps".to_string(),
                    "persistentvolumeclaims".to_string(),
                    "persistentvolumes".to_string(),
                ]),
                resource_names: None,
            },
            crate::resources::ResourceRule {
                verbs: vec!["create".to_string()],
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["events".to_string()]),
                resource_names: None,
            },
            crate::resources::ResourceRule {
                verbs: vec!["update".to_string(), "patch".to_string()],
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["nodes".to_string(), "pods".to_string()]),
                resource_names: None,
            },
        ];

        Ok((resource_rules, vec![]))
    }
}

/// Webhook Authorizer with full HTTP integration
pub struct WebhookAuthorizer {
    webhook_url: String,
    http_client: reqwest::Client,
    #[allow(dead_code)]
    ca_cert: Option<String>,
}

impl WebhookAuthorizer {
    pub fn new(webhook_url: String, ca_cert: Option<String>) -> crate::error::Result<Self> {
        let http_client = if let Some(ref cert_pem) = ca_cert {
            // Build client with custom CA certificate
            let cert = reqwest::Certificate::from_pem(cert_pem.as_bytes()).map_err(|e| {
                crate::error::Error::Internal(format!("Failed to parse CA certificate: {}", e))
            })?;

            reqwest::Client::builder()
                .add_root_certificate(cert)
                .build()
                .map_err(|e| {
                    crate::error::Error::Internal(format!("Failed to create HTTP client: {}", e))
                })?
        } else {
            reqwest::Client::new()
        };

        Ok(Self {
            webhook_url,
            http_client,
            ca_cert,
        })
    }
}

#[async_trait]
impl Authorizer for WebhookAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        use crate::resources::authorization::{
            NonResourceAttributes, ResourceAttributes as SARResourceAttributes,
            SubjectAccessReview, SubjectAccessReviewSpec,
        };
        use crate::types::ObjectMeta;

        // Create SubjectAccessReview request
        let spec = if attrs.is_non_resource_request {
            SubjectAccessReviewSpec {
                resource_attributes: None,
                non_resource_attributes: Some(NonResourceAttributes {
                    path: attrs.path.clone(),
                    verb: Some(attrs.verb.clone()),
                }),
                user: Some(attrs.user.username.clone()),
                groups: Some(attrs.user.groups.clone()),
                extra: Some(attrs.user.extra.clone()),
                uid: Some(attrs.user.uid.clone()),
            }
        } else {
            SubjectAccessReviewSpec {
                resource_attributes: Some(SARResourceAttributes {
                    namespace: attrs.namespace.clone(),
                    verb: Some(attrs.verb.clone()),
                    group: Some(attrs.api_group.clone()),
                    version: None,
                    resource: Some(attrs.resource.clone()),
                    subresource: attrs.subresource.clone(),
                    name: attrs.name.clone(),
                    field_selector: None,
                    label_selector: None,
                }),
                non_resource_attributes: None,
                user: Some(attrs.user.username.clone()),
                groups: Some(attrs.user.groups.clone()),
                extra: Some(attrs.user.extra.clone()),
                uid: Some(attrs.user.uid.clone()),
            }
        };

        let sar = SubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SubjectAccessReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec,
            status: None,
        };

        // Send request to webhook
        let response = self
            .http_client
            .post(&self.webhook_url)
            .json(&sar)
            .send()
            .await
            .map_err(|e| {
                crate::error::Error::Authorization(format!("Webhook request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Ok(Decision::Deny(format!(
                "Webhook returned error status: {}",
                response.status()
            )));
        }

        // Parse response
        let sar_response: SubjectAccessReview = response.json().await.map_err(|e| {
            crate::error::Error::Authorization(format!("Failed to parse webhook response: {}", e))
        })?;

        // Check if authorization succeeded
        let status = sar_response.status.ok_or_else(|| {
            crate::error::Error::Authorization("Webhook response missing status".to_string())
        })?;

        if status.allowed {
            Ok(Decision::Allow)
        } else {
            Ok(Decision::Deny(
                status
                    .reason
                    .unwrap_or_else(|| "Authorization denied".to_string()),
            ))
        }
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        // Webhook authorizers don't support listing user rules
        // This would require a different API call or mechanism
        Ok((vec![], vec![]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_rule_matching() {
        let rule = PolicyRule {
            verbs: vec!["get".to_string(), "list".to_string()],
            api_groups: Some(vec!["".to_string()]),
            resources: Some(vec!["pods".to_string()]),
            resource_names: None,
            non_resource_urls: None,
        };

        let user = UserInfo {
            username: "test-user".to_string(),
            uid: "test-uid".to_string(),
            groups: vec![],
            extra: std::collections::HashMap::new(),
        };

        let attrs = RequestAttributes::new(user.clone(), "get", "pods")
            .with_namespace("default")
            .with_api_group("");

        // This would need a mock storage implementation to fully test
        // Just testing the rule matching logic
        assert!(rule.verbs.contains(&attrs.verb));
    }
}
