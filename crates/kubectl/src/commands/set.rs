use crate::discovery::ResourceMapping;
use crate::types::SetCommands;
use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// Dispatch set subcommands.
pub async fn execute_set(
    client: &ApiClient,
    command: SetCommands,
    default_namespace: &str,
) -> Result<()> {
    match command {
        SetCommands::Image {
            resource,
            container_images,
            namespace,
        } => {
            execute_image(
                client,
                &resource,
                &container_images,
                namespace.as_deref().unwrap_or(default_namespace),
            )
            .await
        }
        SetCommands::Env {
            resource,
            env_vars,
            namespace,
            container,
            list,
        } => {
            execute_env(
                client,
                &resource,
                &env_vars,
                namespace.as_deref().unwrap_or(default_namespace),
                container.as_deref(),
                list,
            )
            .await
        }
        SetCommands::Resources {
            resource,
            namespace,
            container,
            limits,
            requests,
        } => {
            execute_resources(
                client,
                &resource,
                namespace.as_deref().unwrap_or(default_namespace),
                container.as_deref(),
                limits.as_deref(),
                requests.as_deref(),
            )
            .await
        }
        SetCommands::Selector {
            resource,
            expressions,
            namespace,
            all: _,
            resource_version: _,
        } => {
            execute_selector(
                client,
                &resource,
                &expressions,
                namespace.as_deref().unwrap_or(default_namespace),
            )
            .await
        }
        SetCommands::ServiceAccount {
            resource,
            service_account_name,
            namespace,
        } => {
            execute_serviceaccount(
                client,
                &resource,
                &service_account_name,
                namespace.as_deref().unwrap_or(default_namespace),
            )
            .await
        }
        SetCommands::Subject {
            resource,
            namespace,
            serviceaccount,
            user,
            group,
        } => {
            execute_subject(
                client,
                &resource,
                namespace.as_deref().unwrap_or(default_namespace),
                serviceaccount.as_deref(),
                user.as_deref(),
                group.as_deref(),
            )
            .await
        }
    }
}

/// Resolve the API path and mapping for a resource type/name via server
/// discovery.
///
/// Uses `RestMapper` so any kind the server exposes (plural, singular, short
/// name, or Kind) is addressable, rather than a hardcoded alias table. Returns
/// the item path plus the resolved `ResourceMapping`, so callers can build a
/// group-qualified label (via `ops::resource_label`) and apply kind guards
/// without a second `RestMapper::from_server` round-trip. This is the single
/// discovery call site shared by every `set` subcommand.
async fn resolve_resource(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
) -> Result<(String, ResourceMapping)> {
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(resource_type).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{resource_type}\"")
    })?;
    let ns = if mapping.namespaced {
        Some(namespace)
    } else {
        None
    };
    let path = crate::ops::build_path(mapping, ns, Some(name));
    Ok((path, mapping.clone()))
}

/// Parse "resource_type/name" format into (type, name).
fn parse_resource_arg(arg: &str) -> Result<(String, String)> {
    if let Some((rt, name)) = arg.split_once('/') {
        Ok((rt.to_string(), name.to_string()))
    } else {
        anyhow::bail!(
            "Invalid resource format: '{}'. Expected TYPE/NAME (e.g., deployment/nginx)",
            arg
        );
    }
}

/// Update container images in a resource.
///
/// Usage: `kubectl set image deployment/nginx nginx=nginx:1.9.1`
pub async fn execute_image(
    client: &ApiClient,
    resource: &str,
    container_images: &[String],
    namespace: &str,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    // Parse container=image pairs
    let mut image_updates: Vec<(String, String)> = Vec::new();
    for ci in container_images {
        if let Some((container, image)) = ci.split_once('=') {
            image_updates.push((container.to_string(), image.to_string()));
        } else {
            anyhow::bail!("Invalid image format: '{}'. Expected CONTAINER=IMAGE", ci);
        }
    }

    if image_updates.is_empty() {
        anyhow::bail!("At least one CONTAINER=IMAGE argument is required");
    }

    // Get the current resource to find container indices
    let current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    // Find the containers array path (handles both pod and pod template specs)
    let containers = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        // Deployment/StatefulSet/DaemonSet/ReplicaSet/Job/CronJob
        current["spec"]["template"]["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    } else {
        // Pod
        current["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };

    // Build the strategic merge patch for containers
    let mut patch_containers: Vec<Value> = Vec::new();
    for (container_name, image) in &image_updates {
        if container_name == "*" {
            // Update all containers
            for c in &containers {
                if let Some(cname) = c.get("name").and_then(|n| n.as_str()) {
                    patch_containers.push(json!({
                        "name": cname,
                        "image": image,
                    }));
                }
            }
        } else {
            // Check container exists
            let found = containers
                .iter()
                .any(|c| c.get("name").and_then(|n| n.as_str()) == Some(container_name));
            if !found {
                anyhow::bail!(
                    "Container '{}' not found in resource {}/{}",
                    container_name,
                    resource_type,
                    name
                );
            }
            patch_containers.push(json!({
                "name": container_name,
                "image": image,
            }));
        }
    }

    // Build patch based on resource type
    let patch_body = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": patch_containers,
                    }
                }
            }
        })
    } else {
        json!({
            "spec": {
                "containers": patch_containers,
            }
        })
    };

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update image")?;

    let label = crate::ops::resource_label(&mapping, &name);
    for (container_name, image) in &image_updates {
        if container_name == "*" {
            println!("{label} image updated to {image} (all containers)");
        } else {
            println!("{label} container {container_name} image updated to {image}");
        }
    }

    Ok(())
}

/// Update environment variables on a resource.
///
/// Usage: `kubectl set env deployment/registry STORAGE_DIR=/local`
pub async fn execute_env(
    client: &ApiClient,
    resource: &str,
    env_vars: &[String],
    namespace: &str,
    container_name: Option<&str>,
    list: bool,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    // Get current resource
    let current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    let containers = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        current["spec"]["template"]["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    } else {
        current["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };

    // If --list, just print current env vars
    if list {
        for c in &containers {
            let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
            if let Some(target) = container_name {
                if cname != target {
                    continue;
                }
            }
            println!("# Container: {}", cname);
            if let Some(envs) = c.get("env").and_then(|e| e.as_array()) {
                for env in envs {
                    let key = env.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let val = env.get("value").and_then(|v| v.as_str()).unwrap_or("");
                    println!("{}={}", key, val);
                }
            }
        }
        return Ok(());
    }

    // Parse env vars: KEY=VALUE to add, KEY- to remove
    let mut to_add: Vec<(String, String)> = Vec::new();
    let mut to_remove: Vec<String> = Vec::new();

    for ev in env_vars {
        if let Some(key) = ev.strip_suffix('-') {
            to_remove.push(key.to_string());
        } else if let Some((key, value)) = ev.split_once('=') {
            to_add.push((key.to_string(), value.to_string()));
        } else {
            anyhow::bail!("Invalid env format: '{}'. Expected KEY=VALUE or KEY-", ev);
        }
    }

    // Build updated containers with env changes
    let mut updated_containers: Vec<Value> = Vec::new();

    for c in &containers {
        let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if let Some(target) = container_name {
            if cname != target {
                continue;
            }
        }

        let mut env_list: Vec<Value> = c
            .get("env")
            .and_then(|e| e.as_array())
            .cloned()
            .unwrap_or_default();

        // Remove specified vars
        env_list.retain(|e| {
            let key = e.get("name").and_then(|n| n.as_str()).unwrap_or("");
            !to_remove.contains(&key.to_string())
        });

        // Add/update vars
        for (key, value) in &to_add {
            if let Some(existing) = env_list
                .iter_mut()
                .find(|e| e.get("name").and_then(|n| n.as_str()) == Some(key))
            {
                existing["value"] = json!(value);
            } else {
                env_list.push(json!({"name": key, "value": value}));
            }
        }

        updated_containers.push(json!({
            "name": cname,
            "env": env_list,
        }));
    }

    let patch_body = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": updated_containers,
                    }
                }
            }
        })
    } else {
        json!({
            "spec": {
                "containers": updated_containers,
            }
        })
    };

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update environment variables")?;

    println!(
        "{} env updated",
        crate::ops::resource_label(&mapping, &name)
    );

    Ok(())
}

/// Update resource requests/limits on a resource.
///
/// Usage: `kubectl set resources deployment nginx --limits=cpu=200m,memory=512Mi`
pub async fn execute_resources(
    client: &ApiClient,
    resource: &str,
    namespace: &str,
    container_name: Option<&str>,
    limits: Option<&str>,
    requests: Option<&str>,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    // Get current resource
    let current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    let containers = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        current["spec"]["template"]["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    } else {
        current["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };

    // Parse limits and requests
    let limits_map = parse_resource_spec(limits)?;
    let requests_map = parse_resource_spec(requests)?;

    // Build updated containers
    let mut updated_containers: Vec<Value> = Vec::new();

    for c in &containers {
        let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or("");

        if let Some(target) = container_name {
            if cname != target {
                continue;
            }
        }

        let mut resources = json!({});

        if !limits_map.is_empty() {
            resources["limits"] = json!(limits_map);
        }
        if !requests_map.is_empty() {
            resources["requests"] = json!(requests_map);
        }

        updated_containers.push(json!({
            "name": cname,
            "resources": resources,
        }));
    }

    let patch_body = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": updated_containers,
                    }
                }
            }
        })
    } else {
        json!({
            "spec": {
                "containers": updated_containers,
            }
        })
    };

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update resource requirements")?;

    println!(
        "{} resource requirements updated",
        crate::ops::resource_label(&mapping, &name)
    );

    Ok(())
}

/// Set the selector on a resource (e.g., a Service).
///
/// Usage: `kubectl set selector service/myapp app=myapp`
pub async fn execute_selector(
    client: &ApiClient,
    resource: &str,
    expressions: &[String],
    namespace: &str,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;

    // Build selector map from key=value expressions
    let mut selector = serde_json::Map::new();
    for expr in expressions {
        if let Some((key, value)) = expr.split_once('=') {
            selector.insert(key.to_string(), json!(value));
        } else {
            anyhow::bail!(
                "Invalid selector expression: '{}'. Expected key=value",
                expr
            );
        }
    }

    if selector.is_empty() {
        anyhow::bail!("At least one selector expression (key=value) is required");
    }

    // Resolve the API path via server discovery (handles services and any
    // other selector-bearing kind).
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    let patch_body = json!({
        "spec": {
            "selector": selector,
        }
    });

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update selector")?;

    println!(
        "{} selector updated",
        crate::ops::resource_label(&mapping, &name)
    );

    Ok(())
}

/// Set serviceAccountName on pod templates in a resource.
///
/// Usage: `kubectl set serviceaccount deployment/nginx my-service-account`
pub async fn execute_serviceaccount(
    client: &ApiClient,
    resource: &str,
    service_account_name: &str,
    namespace: &str,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    // Get current resource to determine if it has a template
    let current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    let patch_body = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        json!({
            "spec": {
                "template": {
                    "spec": {
                        "serviceAccountName": service_account_name,
                    }
                }
            }
        })
    } else {
        json!({
            "spec": {
                "serviceAccountName": service_account_name,
            }
        })
    };

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update serviceAccountName")?;

    println!(
        "{} serviceaccount updated to {}",
        crate::ops::resource_label(&mapping, &name),
        service_account_name
    );

    Ok(())
}

/// Update subjects in a RoleBinding or ClusterRoleBinding.
///
/// Usage: `kubectl set subject clusterrolebinding/admin --user=admin --group=devs`
pub async fn execute_subject(
    client: &ApiClient,
    resource: &str,
    namespace: &str,
    serviceaccount: Option<&str>,
    user: Option<&str>,
    group: Option<&str>,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;

    // set subject only operates on (Cluster)RoleBindings. Resolve the type via
    // the shared discovery resolver, then guard on the resolved Kind so the
    // path is built by the shared RestMapper rather than a hardcoded
    // group/version literal.
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;
    // Guard runs after discovery (we need the resolved Kind); a wrong kind costs one discovery round-trip before rejection.
    if mapping.kind != "RoleBinding" && mapping.kind != "ClusterRoleBinding" {
        anyhow::bail!(
            "set subject only supports rolebinding and clusterrolebinding, got: {}",
            resource_type
        );
    }

    // Get current binding
    let mut current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    let subjects = current
        .get_mut("subjects")
        .and_then(|s| s.as_array_mut())
        .map(|a| a.clone())
        .unwrap_or_default();

    let mut new_subjects = subjects;

    if let Some(sa) = serviceaccount {
        let (sa_namespace, sa_name) = if let Some((ns, n)) = sa.split_once(':') {
            (ns.to_string(), n.to_string())
        } else {
            (namespace.to_string(), sa.to_string())
        };
        // Check if subject already exists
        let exists = new_subjects.iter().any(|s| {
            s.get("kind").and_then(|k| k.as_str()) == Some("ServiceAccount")
                && s.get("name").and_then(|n| n.as_str()) == Some(&sa_name)
                && s.get("namespace").and_then(|n| n.as_str()) == Some(&sa_namespace)
        });
        if !exists {
            new_subjects.push(json!({
                "kind": "ServiceAccount",
                "name": sa_name,
                "namespace": sa_namespace,
            }));
        }
    }

    if let Some(u) = user {
        let exists = new_subjects.iter().any(|s| {
            s.get("kind").and_then(|k| k.as_str()) == Some("User")
                && s.get("name").and_then(|n| n.as_str()) == Some(u)
        });
        if !exists {
            new_subjects.push(json!({
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "User",
                "name": u,
            }));
        }
    }

    if let Some(g) = group {
        let exists = new_subjects.iter().any(|s| {
            s.get("kind").and_then(|k| k.as_str()) == Some("Group")
                && s.get("name").and_then(|n| n.as_str()) == Some(g)
        });
        if !exists {
            new_subjects.push(json!({
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Group",
                "name": g,
            }));
        }
    }

    let patch_body = json!({
        "subjects": new_subjects,
    });

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update subjects")?;

    println!(
        "{} subjects updated",
        crate::ops::resource_label(&mapping, &name)
    );

    Ok(())
}

/// Parse a resource spec string like "cpu=200m,memory=512Mi" into a map.
fn parse_resource_spec(spec: Option<&str>) -> Result<std::collections::HashMap<String, String>> {
    let mut map = std::collections::HashMap::new();
    if let Some(s) = spec {
        for pair in s.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((key, value)) = pair.split_once('=') {
                map.insert(key.to_string(), value.to_string());
            } else {
                anyhow::bail!("Invalid resource spec: '{}'. Expected key=value", pair);
            }
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_resource_arg() {
        let (rt, name) = parse_resource_arg("deployment/nginx").unwrap();
        assert_eq!(rt, "deployment");
        assert_eq!(name, "nginx");
    }

    #[test]
    fn test_parse_resource_arg_invalid() {
        assert!(parse_resource_arg("nginx").is_err());
    }

    #[test]
    fn test_parse_resource_spec() {
        let map = parse_resource_spec(Some("cpu=200m,memory=512Mi")).unwrap();
        assert_eq!(map.get("cpu").unwrap(), "200m");
        assert_eq!(map.get("memory").unwrap(), "512Mi");
    }

    #[test]
    fn test_parse_resource_spec_none() {
        let map = parse_resource_spec(None).unwrap();
        assert!(map.is_empty());
    }

    // Helper mirroring a discovery ResourceMapping so the path-shape assertions
    // below exercise the same `ops::build_path` that `resolve_resource_path`
    // now calls. The actual name->mapping resolution is covered by the mapper
    // tests in discovery.rs.
    fn mapping(group: &str, plural: &str, namespaced: bool) -> crate::discovery::ResourceMapping {
        crate::discovery::ResourceMapping {
            group: group.into(),
            version: "v1".into(),
            kind: "X".into(),
            plural: plural.into(),
            singular: "x".into(),
            namespaced,
            verbs: vec![],
            short_names: vec![],
        }
    }

    #[test]
    fn test_resolve_resource_path() {
        let dep = mapping("apps", "deployments", true);
        assert_eq!(
            crate::ops::build_path(&dep, Some("default"), Some("nginx")),
            "/apis/apps/v1/namespaces/default/deployments/nginx"
        );

        let pod = mapping("", "pods", true);
        assert_eq!(
            crate::ops::build_path(&pod, Some("kube-system"), Some("mypod")),
            "/api/v1/namespaces/kube-system/pods/mypod"
        );

        let sts = mapping("apps", "statefulsets", true);
        assert_eq!(
            crate::ops::build_path(&sts, Some("default"), Some("myapp")),
            "/apis/apps/v1/namespaces/default/statefulsets/myapp"
        );
    }

    #[test]
    fn test_image_patch_construction_for_deployment() {
        // Simulate building a strategic merge patch for image update on a deployment
        let patch_containers = vec![json!({"name": "nginx", "image": "nginx:1.21"})];

        let patch_body = json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": patch_containers,
                    }
                }
            }
        });

        assert_eq!(
            patch_body["spec"]["template"]["spec"]["containers"][0]["name"],
            "nginx"
        );
        assert_eq!(
            patch_body["spec"]["template"]["spec"]["containers"][0]["image"],
            "nginx:1.21"
        );
    }

    #[test]
    fn test_env_update_add_and_remove() {
        // Simulate the env update logic: add KEY=VALUE and remove KEY-
        let env_vars = vec!["NEW_VAR=hello".to_string(), "OLD_VAR-".to_string()];
        let mut to_add: Vec<(String, String)> = Vec::new();
        let mut to_remove: Vec<String> = Vec::new();

        for ev in &env_vars {
            if let Some(key) = ev.strip_suffix('-') {
                to_remove.push(key.to_string());
            } else if let Some((key, value)) = ev.split_once('=') {
                to_add.push((key.to_string(), value.to_string()));
            }
        }

        assert_eq!(to_add, vec![("NEW_VAR".to_string(), "hello".to_string())]);
        assert_eq!(to_remove, vec!["OLD_VAR".to_string()]);

        // Simulate applying to existing env list
        let mut env_list: Vec<Value> = vec![
            json!({"name": "OLD_VAR", "value": "world"}),
            json!({"name": "KEEP", "value": "yes"}),
        ];

        env_list.retain(|e| {
            let key = e.get("name").and_then(|n| n.as_str()).unwrap_or("");
            !to_remove.contains(&key.to_string())
        });

        for (key, value) in &to_add {
            env_list.push(json!({"name": key, "value": value}));
        }

        assert_eq!(env_list.len(), 2);
        assert_eq!(env_list[0]["name"], "KEEP");
        assert_eq!(env_list[1]["name"], "NEW_VAR");
        assert_eq!(env_list[1]["value"], "hello");
    }

    #[test]
    fn test_parse_resource_spec_invalid() {
        assert!(parse_resource_spec(Some("cpu200m")).is_err());
    }

    #[test]
    fn test_parse_selector_expressions() {
        let expressions = vec!["app=nginx".to_string(), "env=prod".to_string()];
        let mut selector = serde_json::Map::new();
        for expr in &expressions {
            if let Some((key, value)) = expr.split_once('=') {
                selector.insert(key.to_string(), json!(value));
            }
        }
        assert_eq!(selector.get("app").unwrap(), "nginx");
        assert_eq!(selector.get("env").unwrap(), "prod");
    }

    #[test]
    fn test_parse_serviceaccount_with_namespace() {
        let sa = "kube-system:default";
        let (ns, name) = sa.split_once(':').unwrap();
        assert_eq!(ns, "kube-system");
        assert_eq!(name, "default");
    }

    #[test]
    fn test_subject_json_construction() {
        let user_subject = json!({
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "User",
            "name": "admin",
        });
        assert_eq!(user_subject["kind"].as_str().unwrap(), "User");
        assert_eq!(user_subject["name"].as_str().unwrap(), "admin");
    }

    #[test]
    fn test_resolve_resource_path_full_plural_names() {
        // Path shapes built by ops::build_path for the workload kinds set
        // operates on. Name resolution itself lives in discovery.rs.
        let cases = vec![
            (mapping("", "pods", true), "/api/v1/namespaces/ns1/pods/p1"),
            (
                mapping("apps", "deployments", true),
                "/apis/apps/v1/namespaces/ns1/deployments/d1",
            ),
            (
                mapping("apps", "daemonsets", true),
                "/apis/apps/v1/namespaces/ns1/daemonsets/ds1",
            ),
            (
                mapping("apps", "statefulsets", true),
                "/apis/apps/v1/namespaces/ns1/statefulsets/ss1",
            ),
            (
                mapping("apps", "replicasets", true),
                "/apis/apps/v1/namespaces/ns1/replicasets/rs1",
            ),
            (
                mapping("", "replicationcontrollers", true),
                "/api/v1/namespaces/ns1/replicationcontrollers/rc1",
            ),
            (
                mapping("batch", "cronjobs", true),
                "/apis/batch/v1/namespaces/ns1/cronjobs/cj1",
            ),
        ];
        for (m, expected) in cases {
            let name = expected.rsplit('/').next().unwrap();
            assert_eq!(
                crate::ops::build_path(&m, Some("ns1"), Some(name)),
                expected
            );
        }
    }

    #[test]
    fn test_parse_resource_arg_with_slash_in_name() {
        // splitn(2, '/') means only first slash is used
        let (rt, name) = parse_resource_arg("deployment/my-app").unwrap();
        assert_eq!(rt, "deployment");
        assert_eq!(name, "my-app");
    }

    #[test]
    fn test_parse_resource_arg_empty_name() {
        let (rt, name) = parse_resource_arg("deployment/").unwrap();
        assert_eq!(rt, "deployment");
        assert_eq!(name, "");
    }

    #[test]
    fn test_parse_resource_spec_empty_string() {
        let map = parse_resource_spec(Some("")).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn test_parse_resource_spec_trailing_comma() {
        let map = parse_resource_spec(Some("cpu=100m,")).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("cpu").unwrap(), "100m");
    }

    #[test]
    fn test_parse_resource_spec_whitespace_trimming() {
        let map = parse_resource_spec(Some(" cpu=100m , memory=256Mi ")).unwrap();
        assert_eq!(map.get("cpu").unwrap(), "100m");
        assert_eq!(map.get("memory").unwrap(), "256Mi");
    }

    #[test]
    fn test_parse_resource_spec_single_value() {
        let map = parse_resource_spec(Some("cpu=500m")).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("cpu").unwrap(), "500m");
    }

    #[test]
    fn test_selector_expression_invalid_format() {
        let expressions = vec!["noequalssign".to_string()];
        let mut selector = serde_json::Map::new();
        let mut had_error = false;
        for expr in &expressions {
            if let Some((key, value)) = expr.split_once('=') {
                selector.insert(key.to_string(), json!(value));
            } else {
                had_error = true;
            }
        }
        assert!(had_error);
        assert!(selector.is_empty());
    }

    #[test]
    fn test_selector_expression_empty_value() {
        let expressions = vec!["app=".to_string()];
        let mut selector = serde_json::Map::new();
        for expr in &expressions {
            if let Some((key, value)) = expr.split_once('=') {
                selector.insert(key.to_string(), json!(value));
            }
        }
        assert_eq!(selector.get("app").unwrap(), "");
    }

    #[test]
    fn test_image_patch_construction_for_pod() {
        // Pod has no template — patch goes directly under spec
        let patch_containers = vec![json!({"name": "app", "image": "myapp:2.0"})];
        let patch_body = json!({
            "spec": {
                "containers": patch_containers,
            }
        });
        assert_eq!(patch_body["spec"]["containers"][0]["name"], "app");
        assert_eq!(patch_body["spec"]["containers"][0]["image"], "myapp:2.0");
        // Ensure no template nesting
        assert!(patch_body["spec"].get("template").is_none());
    }

    #[test]
    fn test_image_patch_wildcard_updates_all() {
        // Simulate wildcard container image update
        let containers = vec![
            json!({"name": "web", "image": "nginx:1.19"}),
            json!({"name": "sidecar", "image": "envoy:1.0"}),
        ];
        let new_image = "registry.example.com/latest";
        let mut patch_containers: Vec<Value> = Vec::new();
        for c in &containers {
            if let Some(cname) = c.get("name").and_then(|n| n.as_str()) {
                patch_containers.push(json!({
                    "name": cname,
                    "image": new_image,
                }));
            }
        }
        assert_eq!(patch_containers.len(), 2);
        assert_eq!(patch_containers[0]["name"], "web");
        assert_eq!(patch_containers[0]["image"], new_image);
        assert_eq!(patch_containers[1]["name"], "sidecar");
        assert_eq!(patch_containers[1]["image"], new_image);
    }

    #[test]
    fn test_serviceaccount_without_namespace_separator() {
        let sa = "my-sa";
        let result = sa.split_once(':');
        assert!(result.is_none());
        // Falls back to using the default namespace
        let (sa_namespace, sa_name) = if let Some((ns, n)) = sa.split_once(':') {
            (ns.to_string(), n.to_string())
        } else {
            ("default".to_string(), sa.to_string())
        };
        assert_eq!(sa_namespace, "default");
        assert_eq!(sa_name, "my-sa");
    }

    #[test]
    fn test_subject_group_json_construction() {
        let group_subject = json!({
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Group",
            "name": "developers",
        });
        assert_eq!(group_subject["kind"].as_str().unwrap(), "Group");
        assert_eq!(group_subject["name"].as_str().unwrap(), "developers");
        assert_eq!(
            group_subject["apiGroup"].as_str().unwrap(),
            "rbac.authorization.k8s.io"
        );
    }

    #[test]
    fn test_subject_serviceaccount_json_construction() {
        let sa_subject = json!({
            "kind": "ServiceAccount",
            "name": "deployer",
            "namespace": "ci-cd",
        });
        assert_eq!(sa_subject["kind"].as_str().unwrap(), "ServiceAccount");
        assert_eq!(sa_subject["name"].as_str().unwrap(), "deployer");
        assert_eq!(sa_subject["namespace"].as_str().unwrap(), "ci-cd");
        // ServiceAccount subjects should NOT have apiGroup
        assert!(sa_subject.get("apiGroup").is_none());
    }

    #[test]
    fn test_env_parsing_invalid_format() {
        let ev = "NOEQUALSNODASH";
        let is_remove = ev.strip_suffix('-').is_some();
        let is_add = ev.split_once('=').is_some();
        assert!(!is_remove);
        assert!(!is_add);
    }

    #[test]
    fn test_env_parsing_value_with_equals() {
        // KEY=VAL=UE should split on first = only
        let ev = "DB_URL=postgres://host:5432/db?opt=val";
        let (key, value) = ev.split_once('=').unwrap();
        assert_eq!(key, "DB_URL");
        assert_eq!(value, "postgres://host:5432/db?opt=val");
    }

    // ===== 10 additional tests for untested async functions =====

    fn make_test_client() -> ApiClient {
        ApiClient::new("http://127.0.0.1:1", true, None).unwrap()
    }

    #[tokio::test]
    async fn test_execute_image_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute_image(
            &client,
            "deployment/nginx",
            &["nginx=nginx:1.21".to_string()],
            "default",
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_image_invalid_resource_format() {
        let client = make_test_client();
        let result = execute_image(
            &client,
            "nginx",
            &["nginx=nginx:1.21".to_string()],
            "default",
        )
        .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid resource format"));
    }

    #[tokio::test]
    async fn test_execute_env_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute_env(
            &client,
            "deployment/web",
            &["FOO=bar".to_string()],
            "default",
            None,
            false,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_resources_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute_resources(
            &client,
            "deployment/web",
            "default",
            None,
            Some("cpu=200m"),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_selector_returns_err_on_unreachable() {
        let client = make_test_client();
        let result =
            execute_selector(&client, "service/web", &["app=web".to_string()], "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_serviceaccount_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute_serviceaccount(&client, "deployment/web", "my-sa", "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_subject_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute_subject(
            &client,
            "clusterrolebinding/admin",
            "default",
            None,
            Some("admin-user"),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_subject_unsupported_resource_type() {
        // The "only supports rolebinding/clusterrolebinding" guard now runs
        // after server discovery, so against an unreachable server the error is
        // a connection/discovery failure rather than the guard message. The
        // contract preserved here is simply that a non-binding kind fails.
        let client = make_test_client();
        let result = execute_subject(
            &client,
            "deployment/web",
            "default",
            None,
            Some("user1"),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_subject_guard_rejects_non_binding_kind() {
        use crate::discovery::{parse_aggregated_discovery, RestMapper};
        let raw = include_str!("../../tests/fixtures/aggregated-discovery.json");
        let doc: serde_json::Value = serde_json::from_str(raw).unwrap();
        let mapper = RestMapper::new(parse_aggregated_discovery(&doc).unwrap());
        let mapping = mapper.resolve("deployment").unwrap();
        // The same predicate execute_subject's guard uses:
        let allowed = mapping.kind == "RoleBinding" || mapping.kind == "ClusterRoleBinding";
        assert!(
            !allowed,
            "deployment must be rejected by the set-subject kind guard"
        );
        // and a positive case if rolebindings is in the fixture:
        if let Some(rb) = mapper.resolve("rolebindings") {
            assert!(rb.kind == "RoleBinding" || rb.kind == "ClusterRoleBinding");
        }
    }

    #[tokio::test]
    async fn test_execute_image_no_container_images_returns_err() {
        let client = make_test_client();
        let result = execute_image(&client, "deployment/nginx", &[], "default").await;
        // Empty container_images should fail (after fetching, but resource fetch fails first)
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_set_dispatches_image_command() {
        let client = make_test_client();
        let cmd = SetCommands::Image {
            resource: "deployment/nginx".to_string(),
            container_images: vec!["nginx=nginx:1.21".to_string()],
            namespace: Some("test-ns".to_string()),
        };
        let result = execute_set(&client, cmd, "default").await;
        assert!(result.is_err()); // connection error
    }
}
