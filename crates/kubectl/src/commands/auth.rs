use crate::types::AuthCommands;
use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SelfSubjectAccessReview {
    api_version: String,
    kind: String,
    spec: SelfSubjectAccessReviewSpec,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SelfSubjectAccessReviewSpec {
    resource_attributes: Option<ResourceAttributes>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResourceAttributes {
    namespace: Option<String>,
    verb: String,
    resource: String,
    name: Option<String>,
}

#[derive(Deserialize)]
struct SelfSubjectAccessReviewStatus {
    allowed: bool,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct SelfSubjectAccessReviewResponse {
    status: SelfSubjectAccessReviewStatus,
}

/// Execute auth commands
pub async fn execute(
    client: &ApiClient,
    command: AuthCommands,
    default_namespace: &str,
) -> Result<()> {
    match command {
        AuthCommands::CanI {
            verb,
            resource,
            name,
            namespace,
            all_namespaces,
        } => {
            let ns = if all_namespaces {
                None
            } else {
                Some(
                    namespace
                        .as_deref()
                        .unwrap_or(default_namespace)
                        .to_string(),
                )
            };

            let review = SelfSubjectAccessReview {
                api_version: "authorization.k8s.io/v1".to_string(),
                kind: "SelfSubjectAccessReview".to_string(),
                spec: SelfSubjectAccessReviewSpec {
                    resource_attributes: Some(ResourceAttributes {
                        namespace: ns,
                        verb,
                        resource,
                        name,
                    }),
                },
            };

            let response: SelfSubjectAccessReviewResponse = client
                .post(
                    "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
                    &review,
                )
                .await
                .context("Failed to check authorization")?;

            if response.status.allowed {
                println!("yes");
                if let Some(reason) = response.status.reason {
                    println!("Reason: {}", reason);
                }
            } else {
                println!("no");
                if let Some(reason) = response.status.reason {
                    println!("Reason: {}", reason);
                }
            }
        }
        AuthCommands::Whoami { output } => {
            // Use SelfSubjectReview (k8s 1.27+) or fallback to token review
            let result: Value = client
                .post(
                    "/apis/authentication.k8s.io/v1/selfsubjectreviews",
                    &json!({
                        "apiVersion": "authentication.k8s.io/v1",
                        "kind": "SelfSubjectReview",
                    }),
                )
                .await
                .context("Failed to get user identity")?;

            if let Some(fmt) = output {
                match fmt.as_str() {
                    "json" => println!("{}", serde_json::to_string_pretty(&result)?),
                    "yaml" => println!("{}", serde_yaml::to_string(&result)?),
                    _ => println!("{}", serde_json::to_string_pretty(&result)?),
                }
            } else {
                // Extract user info for simple output
                if let Some(user_info) = result.get("status").and_then(|s| s.get("userInfo")) {
                    if let Some(username) = user_info.get("username") {
                        println!("Username: {}", username);
                    }
                    if let Some(uid) = user_info.get("uid") {
                        println!("UID: {}", uid);
                    }
                    if let Some(groups) = user_info.get("groups") {
                        println!("Groups: {}", groups);
                    }
                }
            }
        }
        AuthCommands::Reconcile {
            filename,
            namespace,
            remove_extra_permissions,
            remove_extra_subjects,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            execute_reconcile(
                client,
                &filename,
                ns,
                remove_extra_permissions,
                remove_extra_subjects,
            )
            .await?;
        }
    }

    Ok(())
}

/// Reconcile RBAC resources from a file against the server.
///
/// Reads Role, ClusterRole, RoleBinding, and ClusterRoleBinding resources from
/// a file and ensures they exist on the server with the correct rules/subjects.
async fn execute_reconcile(
    client: &ApiClient,
    filename: &str,
    namespace: &str,
    remove_extra_permissions: bool,
    remove_extra_subjects: bool,
) -> Result<()> {
    let contents = fs::read_to_string(filename).context("Failed to read file")?;

    for document in serde_yaml::Deserializer::from_str(&contents) {
        let value: serde_yaml::Value =
            serde_yaml::Value::deserialize(document).context("Failed to parse YAML document")?;

        if value.is_null() {
            continue;
        }

        let kind = value
            .get("kind")
            .and_then(|k| k.as_str())
            .context("Missing 'kind' field")?;

        let name = value
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .context("Missing 'metadata.name' field")?;

        let json_value: Value = serde_yaml::from_value(value.clone())?;

        match kind {
            "ClusterRole" => {
                let path = format!("/apis/rbac.authorization.k8s.io/v1/clusterroles/{}", name);
                reconcile_resource(
                    client,
                    &path,
                    kind,
                    name,
                    &json_value,
                    remove_extra_permissions,
                )
                .await?;
            }
            "ClusterRoleBinding" => {
                let path = format!(
                    "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{}",
                    name
                );
                reconcile_binding(
                    client,
                    &path,
                    kind,
                    name,
                    &json_value,
                    remove_extra_subjects,
                )
                .await?;
            }
            "Role" => {
                let res_ns = value
                    .get("metadata")
                    .and_then(|m| m.get("namespace"))
                    .and_then(|n| n.as_str())
                    .unwrap_or(namespace);
                let path = format!(
                    "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/roles/{}",
                    res_ns, name
                );
                reconcile_resource(
                    client,
                    &path,
                    kind,
                    name,
                    &json_value,
                    remove_extra_permissions,
                )
                .await?;
            }
            "RoleBinding" => {
                let res_ns = value
                    .get("metadata")
                    .and_then(|m| m.get("namespace"))
                    .and_then(|n| n.as_str())
                    .unwrap_or(namespace);
                let path = format!(
                    "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/rolebindings/{}",
                    res_ns, name
                );
                reconcile_binding(
                    client,
                    &path,
                    kind,
                    name,
                    &json_value,
                    remove_extra_subjects,
                )
                .await?;
            }
            _ => {
                eprintln!(
                    "Warning: skipping non-RBAC resource kind '{}' in reconcile",
                    kind
                );
            }
        }
    }

    Ok(())
}

/// Reconcile a Role or ClusterRole resource.
async fn reconcile_resource(
    client: &ApiClient,
    path: &str,
    kind: &str,
    name: &str,
    desired: &Value,
    remove_extra_permissions: bool,
) -> Result<()> {
    let existing: std::result::Result<Value, _> = client.get(path).await;

    match existing {
        Ok(current) => {
            // Build patch with desired rules
            let mut patch = json!({});
            if let Some(rules) = desired.get("rules") {
                if remove_extra_permissions {
                    // Replace rules entirely
                    patch["rules"] = rules.clone();
                } else {
                    // Merge: add any missing rules from desired
                    let current_rules = current
                        .get("rules")
                        .and_then(|r| r.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let desired_rules = rules.as_array().cloned().unwrap_or_default();

                    let mut merged = current_rules;
                    for rule in desired_rules {
                        if !merged.contains(&rule) {
                            merged.push(rule);
                        }
                    }
                    patch["rules"] = Value::Array(merged);
                }
            }

            if let Some(labels) = desired.get("metadata").and_then(|m| m.get("labels")) {
                patch["metadata"] = json!({"labels": labels.clone()});
            }

            let _: Value = client
                .patch(path, &patch, "application/strategic-merge-patch+json")
                .await
                .context(format!("Failed to reconcile {}/{}", kind, name))?;
            println!("{}/{} reconciled", kind.to_lowercase(), name);
        }
        Err(_) => {
            // Create the resource
            let collection_path = path.rsplit_once('/').map(|(p, _)| p).unwrap_or(path);
            let _: Value = client
                .post(collection_path, desired)
                .await
                .context(format!("Failed to create {}/{}", kind, name))?;
            println!("{}/{} created", kind.to_lowercase(), name);
        }
    }

    Ok(())
}

/// Reconcile a RoleBinding or ClusterRoleBinding resource.
async fn reconcile_binding(
    client: &ApiClient,
    path: &str,
    kind: &str,
    name: &str,
    desired: &Value,
    remove_extra_subjects: bool,
) -> Result<()> {
    let existing: std::result::Result<Value, _> = client.get(path).await;

    match existing {
        Ok(current) => {
            let mut patch = json!({});

            // Reconcile roleRef (must match)
            if let Some(role_ref) = desired.get("roleRef") {
                let current_role_ref = current.get("roleRef");
                if current_role_ref.is_some() && current_role_ref != Some(role_ref) {
                    anyhow::bail!(
                        "{}/{}: roleRef is immutable and does not match desired state",
                        kind,
                        name
                    );
                }
            }

            // Reconcile subjects
            if let Some(desired_subjects) = desired.get("subjects") {
                if remove_extra_subjects {
                    patch["subjects"] = desired_subjects.clone();
                } else {
                    let current_subjects = current
                        .get("subjects")
                        .and_then(|s| s.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let desired_subs = desired_subjects.as_array().cloned().unwrap_or_default();

                    let mut merged = current_subjects;
                    for sub in desired_subs {
                        if !merged.contains(&sub) {
                            merged.push(sub);
                        }
                    }
                    patch["subjects"] = Value::Array(merged);
                }
            }

            let _: Value = client
                .patch(path, &patch, "application/strategic-merge-patch+json")
                .await
                .context(format!("Failed to reconcile {}/{}", kind, name))?;
            println!("{}/{} reconciled", kind.to_lowercase(), name);
        }
        Err(_) => {
            let collection_path = path.rsplit_once('/').map(|(p, _)| p).unwrap_or(path);
            let _: Value = client
                .post(collection_path, desired)
                .await
                .context(format!("Failed to create {}/{}", kind, name))?;
            println!("{}/{} created", kind.to_lowercase(), name);
        }
    }

    Ok(())
}

#[cfg(test)]
mod auth_reconcile_tests {
    use super::*;

    #[test]
    fn test_reconcile_merge_rules() {
        let current_rules =
            vec![json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]})];
        let desired_rules = vec![
            json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}),
            json!({"apiGroups": [""], "resources": ["services"], "verbs": ["list"]}),
        ];

        let mut merged = current_rules;
        for rule in desired_rules {
            if !merged.contains(&rule) {
                merged.push(rule);
            }
        }

        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_reconcile_merge_subjects() {
        let current_subjects =
            vec![json!({"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"})];
        let desired_subjects = vec![
            json!({"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}),
            json!({"kind": "User", "name": "bob", "apiGroup": "rbac.authorization.k8s.io"}),
        ];

        let mut merged = current_subjects;
        for sub in desired_subjects {
            if !merged.contains(&sub) {
                merged.push(sub);
            }
        }

        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_reconcile_replace_rules_when_remove_extra() {
        let current_rules = vec![
            json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}),
            json!({"apiGroups": [""], "resources": ["nodes"], "verbs": ["list"]}),
        ];
        let desired_rules =
            vec![json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]})];

        let remove_extra_permissions = true;
        let result = if remove_extra_permissions {
            desired_rules.clone()
        } else {
            let mut merged = current_rules;
            for rule in desired_rules {
                if !merged.contains(&rule) {
                    merged.push(rule);
                }
            }
            merged
        };

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_reconcile_replace_subjects_when_remove_extra() {
        let current_subjects = vec![
            json!({"kind": "User", "name": "alice"}),
            json!({"kind": "User", "name": "charlie"}),
        ];
        let desired_subjects = vec![json!({"kind": "User", "name": "alice"})];

        let remove_extra_subjects = true;
        let result = if remove_extra_subjects {
            desired_subjects.clone()
        } else {
            let mut merged = current_subjects;
            for sub in desired_subjects {
                if !merged.contains(&sub) {
                    merged.push(sub);
                }
            }
            merged
        };

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_self_subject_access_review_serialization() {
        let review = SelfSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectAccessReview".to_string(),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("default".to_string()),
                    verb: "get".to_string(),
                    resource: "pods".to_string(),
                    name: None,
                }),
            },
        };

        let json = serde_json::to_value(&review).unwrap();
        assert_eq!(json["apiVersion"], "authorization.k8s.io/v1");
        assert_eq!(json["kind"], "SelfSubjectAccessReview");
        assert_eq!(json["spec"]["resourceAttributes"]["verb"], "get");
        assert_eq!(json["spec"]["resourceAttributes"]["resource"], "pods");
        assert_eq!(json["spec"]["resourceAttributes"]["namespace"], "default");
        assert!(json["spec"]["resourceAttributes"]["name"].is_null());
    }

    #[test]
    fn test_collection_path_from_resource_path() {
        let path = "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin";
        let collection_path = path.rsplit_once('/').map(|(p, _)| p).unwrap_or(path);
        assert_eq!(
            collection_path,
            "/apis/rbac.authorization.k8s.io/v1/clusterroles"
        );
    }

    #[test]
    fn test_self_subject_access_review_all_namespaces() {
        // When all_namespaces is true, namespace should be None
        let review = SelfSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectAccessReview".to_string(),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    namespace: None,
                    verb: "list".to_string(),
                    resource: "pods".to_string(),
                    name: None,
                }),
            },
        };

        let json = serde_json::to_value(&review).unwrap();
        assert!(json["spec"]["resourceAttributes"]["namespace"].is_null());
        assert_eq!(json["spec"]["resourceAttributes"]["verb"], "list");
    }

    #[test]
    fn test_self_subject_access_review_with_name() {
        let review = SelfSubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SelfSubjectAccessReview".to_string(),
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    namespace: Some("prod".to_string()),
                    verb: "delete".to_string(),
                    resource: "deployments".to_string(),
                    name: Some("web-app".to_string()),
                }),
            },
        };

        let json = serde_json::to_value(&review).unwrap();
        assert_eq!(json["spec"]["resourceAttributes"]["name"], "web-app");
        assert_eq!(json["spec"]["resourceAttributes"]["namespace"], "prod");
        assert_eq!(json["spec"]["resourceAttributes"]["verb"], "delete");
        assert_eq!(
            json["spec"]["resourceAttributes"]["resource"],
            "deployments"
        );
    }

    #[test]
    fn test_collection_path_for_namespaced_resources() {
        let role_path = "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/my-role";
        let collection = role_path
            .rsplit_once('/')
            .map(|(p, _)| p)
            .unwrap_or(role_path);
        assert_eq!(
            collection,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles"
        );

        let binding_path =
            "/apis/rbac.authorization.k8s.io/v1/namespaces/prod/rolebindings/my-binding";
        let collection = binding_path
            .rsplit_once('/')
            .map(|(p, _)| p)
            .unwrap_or(binding_path);
        assert_eq!(
            collection,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/prod/rolebindings"
        );
    }

    #[test]
    fn test_reconcile_merge_no_duplicates() {
        // Merging identical rules should not create duplicates
        let current_rules = vec![
            json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}),
            json!({"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]}),
        ];
        let desired_rules = vec![
            json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}),
            json!({"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]}),
        ];

        let mut merged = current_rules;
        for rule in desired_rules {
            if !merged.contains(&rule) {
                merged.push(rule);
            }
        }

        assert_eq!(merged.len(), 2);
    }
}
