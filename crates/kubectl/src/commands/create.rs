use crate::client::ApiClient;
use crate::types::{CreateCommands, SecretCommands, ServiceCommands};
use anyhow::{Context, Result};
use base64::Engine;
use rusternetes_common::resources::{
    Deployment, Endpoints, LimitRange, Namespace, Node, Pod, PriorityClass, ResourceQuota, Service,
    StorageClass, VolumeSnapshot, VolumeSnapshotClass,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

// ── Subcommand dispatch ─────────────────────────────────────────────────────

pub async fn execute_subcommand(
    client: &ApiClient,
    cmd: &CreateCommands,
    default_namespace: &str,
) -> Result<()> {
    match cmd {
        CreateCommands::ClusterRole {
            name,
            verb,
            resource,
            resource_name,
            non_resource_url,
            aggregation_rule,
        } => {
            let body = build_cluster_role(
                name,
                verb,
                resource,
                resource_name,
                non_resource_url,
                aggregation_rule,
            )?;
            let _: Value = client
                .post("/apis/rbac.authorization.k8s.io/v1/clusterroles", &body)
                .await?;
            println!("clusterrole/{} created", name);
        }
        CreateCommands::ClusterRoleBinding {
            name,
            clusterrole,
            user,
            group,
            serviceaccount,
        } => {
            let body = build_cluster_role_binding(name, clusterrole, user, group, serviceaccount)?;
            let _: Value = client
                .post(
                    "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
                    &body,
                )
                .await?;
            println!("clusterrolebinding/{} created", name);
        }
        CreateCommands::ConfigMap {
            name,
            from_literal,
            from_file,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_configmap(name, ns, from_literal, from_file)?;
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/configmaps", ns), &body)
                .await?;
            println!("configmap/{} created", name);
        }
        CreateCommands::CronJob {
            name,
            image,
            schedule,
            restart,
            namespace,
            command,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_cronjob(name, ns, image, schedule, restart, command);
            let _: Value = client
                .post(&format!("/apis/batch/v1/namespaces/{}/cronjobs", ns), &body)
                .await?;
            println!("cronjob.batch/{} created", name);
        }
        CreateCommands::Ingress {
            name,
            ingress_class,
            rule,
            default_backend,
            annotation,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_ingress(
                name,
                ns,
                ingress_class.as_deref(),
                rule,
                default_backend.as_deref(),
                annotation,
            )?;
            let _: Value = client
                .post(
                    &format!("/apis/networking.k8s.io/v1/namespaces/{}/ingresses", ns),
                    &body,
                )
                .await?;
            println!("ingress.networking.k8s.io/{} created", name);
        }
        CreateCommands::Job {
            name,
            image,
            from,
            namespace,
            command,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_job(name, ns, image.as_deref(), from.as_deref(), command)?;
            let _: Value = client
                .post(&format!("/apis/batch/v1/namespaces/{}/jobs", ns), &body)
                .await?;
            println!("job.batch/{} created", name);
        }
        CreateCommands::Pdb {
            name,
            selector,
            min_available,
            max_unavailable,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_pdb(
                name,
                ns,
                selector,
                min_available.as_deref(),
                max_unavailable.as_deref(),
            )?;
            let _: Value = client
                .post(
                    &format!("/apis/policy/v1/namespaces/{}/poddisruptionbudgets", ns),
                    &body,
                )
                .await?;
            println!("poddisruptionbudget.policy/{} created", name);
        }
        CreateCommands::Role {
            name,
            verb,
            resource,
            resource_name,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_role(name, ns, verb, resource, resource_name)?;
            let _: Value = client
                .post(
                    &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{}/roles", ns),
                    &body,
                )
                .await?;
            println!("role.rbac.authorization.k8s.io/{} created", name);
        }
        CreateCommands::RoleBinding {
            name,
            clusterrole,
            role,
            user,
            group,
            serviceaccount,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_role_binding(
                name,
                ns,
                clusterrole.as_deref(),
                role.as_deref(),
                user,
                group,
                serviceaccount,
            )?;
            let _: Value = client
                .post(
                    &format!(
                        "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/rolebindings",
                        ns
                    ),
                    &body,
                )
                .await?;
            println!("rolebinding.rbac.authorization.k8s.io/{} created", name);
        }
        CreateCommands::Secret { subcommand } => {
            execute_secret_subcommand(client, subcommand, default_namespace).await?;
        }
        CreateCommands::ServiceAccount { name, namespace } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_service_account(name, ns);
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/serviceaccounts", ns), &body)
                .await?;
            println!("serviceaccount/{} created", name);
        }
        CreateCommands::Token {
            name,
            audience,
            duration,
            bound_object_kind,
            bound_object_name,
            bound_object_uid,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_token_request(
                audience,
                duration.as_deref(),
                bound_object_kind.as_deref(),
                bound_object_name.as_deref(),
                bound_object_uid.as_deref(),
            )?;
            let response: Value = client
                .post(
                    &format!("/api/v1/namespaces/{}/serviceaccounts/{}/token", ns, name),
                    &body,
                )
                .await?;
            if let Some(token) = response
                .get("status")
                .and_then(|s| s.get("token"))
                .and_then(|t| t.as_str())
            {
                println!("{}", token);
            } else {
                anyhow::bail!("No token in server response");
            }
        }
        CreateCommands::Namespace { name } => {
            let body = build_namespace(name);
            let _: Value = client.post("/api/v1/namespaces", &body).await?;
            println!("namespace/{} created", name);
        }
        CreateCommands::Deployment {
            name,
            image,
            replicas,
            port,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_deployment(name, ns, image, *replicas, *port);
            let _: Value = client
                .post(
                    &format!("/apis/apps/v1/namespaces/{}/deployments", ns),
                    &body,
                )
                .await?;
            println!("deployment.apps/{} created", name);
        }
        CreateCommands::Service { subcommand } => {
            execute_service_subcommand(client, subcommand, default_namespace).await?;
        }
        CreateCommands::PriorityClass {
            name,
            value,
            global_default,
            preemption_policy,
            description,
        } => {
            let body = build_priority_class(
                name,
                *value,
                *global_default,
                preemption_policy,
                description.as_deref(),
            );
            let _: Value = client
                .post("/apis/scheduling.k8s.io/v1/priorityclasses", &body)
                .await?;
            println!("priorityclass.scheduling.k8s.io/{} created", name);
        }
        CreateCommands::Quota {
            name,
            hard,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_quota(name, ns, hard.as_deref())?;
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/resourcequotas", ns), &body)
                .await?;
            println!("resourcequota/{} created", name);
        }
    }
    Ok(())
}

async fn execute_secret_subcommand(
    client: &ApiClient,
    cmd: &SecretCommands,
    default_namespace: &str,
) -> Result<()> {
    match cmd {
        SecretCommands::Generic {
            name,
            from_literal,
            from_file,
            secret_type,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body =
                build_secret_generic(name, ns, from_literal, from_file, secret_type.as_deref())?;
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/secrets", ns), &body)
                .await?;
            println!("secret/{} created", name);
        }
        SecretCommands::DockerRegistry {
            name,
            docker_server,
            docker_username,
            docker_password,
            docker_email,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_secret_docker_registry(
                name,
                ns,
                docker_server,
                docker_username.as_deref(),
                docker_password.as_deref(),
                docker_email.as_deref(),
            )?;
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/secrets", ns), &body)
                .await?;
            println!("secret/{} created", name);
        }
        SecretCommands::Tls {
            name,
            cert,
            key,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_secret_tls(name, ns, cert, key)?;
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/secrets", ns), &body)
                .await?;
            println!("secret/{} created", name);
        }
    }
    Ok(())
}

// ── JSON builders (public for testing) ──────────────────────────────────────

pub fn build_cluster_role(
    name: &str,
    verbs: &[String],
    resources: &[String],
    resource_names: &[String],
    non_resource_urls: &[String],
    aggregation_rule: &[String],
) -> Result<Value> {
    let mut cr = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": { "name": name },
    });

    if !aggregation_rule.is_empty() {
        let mut match_labels = serde_json::Map::new();
        for item in aggregation_rule {
            let parts: Vec<&str> = item.splitn(2, '=').collect();
            if parts.len() == 2 {
                match_labels.insert(parts[0].to_string(), json!(parts[1]));
            }
        }
        cr["aggregationRule"] = json!({
            "clusterRoleSelectors": [{ "matchLabels": match_labels }]
        });
    } else {
        let rules = build_policy_rules(verbs, resources, resource_names, non_resource_urls);
        cr["rules"] = rules;
    }

    Ok(cr)
}

pub fn build_cluster_role_binding(
    name: &str,
    clusterrole: &str,
    users: &[String],
    groups: &[String],
    service_accounts: &[String],
) -> Result<Value> {
    let subjects = build_subjects(users, groups, service_accounts)?;
    Ok(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": { "name": name },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": clusterrole,
        },
        "subjects": subjects,
    }))
}

pub fn build_configmap(
    name: &str,
    namespace: &str,
    from_literal: &[String],
    from_file: &[String],
) -> Result<Value> {
    let mut data = serde_json::Map::new();

    for literal in from_literal {
        let (key, value) = parse_key_value(literal)
            .with_context(|| format!("Invalid --from-literal: {}", literal))?;
        data.insert(key, json!(value));
    }

    for file_src in from_file {
        let (key, path) = parse_file_source(file_src)?;
        let content =
            fs::read_to_string(&path).with_context(|| format!("Failed to read file: {}", path))?;
        data.insert(key, json!(content));
    }

    Ok(json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "data": data,
    }))
}

pub fn build_cronjob(
    name: &str,
    namespace: &str,
    image: &str,
    schedule: &str,
    restart: &str,
    command: &[String],
) -> Value {
    let mut container = json!({
        "name": name,
        "image": image,
    });
    if !command.is_empty() {
        container["command"] = json!(command);
    }

    json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "schedule": schedule,
            "jobTemplate": {
                "metadata": { "name": name },
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [container],
                            "restartPolicy": restart,
                        }
                    }
                }
            }
        }
    })
}

pub fn build_ingress(
    name: &str,
    namespace: &str,
    ingress_class: Option<&str>,
    rules: &[String],
    default_backend: Option<&str>,
    annotations: &[String],
) -> Result<Value> {
    let mut spec = serde_json::Map::new();

    if let Some(class) = ingress_class {
        spec.insert("ingressClassName".to_string(), json!(class));
    }

    if let Some(backend) = default_backend {
        let parts: Vec<&str> = backend.split(':').collect();
        if parts.len() != 2 {
            anyhow::bail!("default-backend should be in format servicename:serviceport");
        }
        spec.insert(
            "defaultBackend".to_string(),
            build_ingress_backend(parts[0], parts[1]),
        );
    }

    // Build rules
    let mut ingress_rules: Vec<Value> = Vec::new();
    let mut tls_entries: Vec<Value> = Vec::new();

    for rule_str in rules {
        let (rule_part, tls_part) = if let Some(idx) = rule_str.find(",tls") {
            (&rule_str[..idx], Some(&rule_str[idx + 1..]))
        } else {
            (rule_str.as_str(), None)
        };

        let host_path_svc: Vec<&str> = rule_part.splitn(2, '/').collect();
        let host = host_path_svc[0];
        let path_svc = if host_path_svc.len() > 1 {
            host_path_svc[1]
        } else {
            anyhow::bail!("Rule must contain a path: {}", rule_str);
        };

        let eq_parts: Vec<&str> = path_svc.splitn(2, '=').collect();
        if eq_parts.len() != 2 {
            anyhow::bail!("Rule must be in format host/path=svc:port: {}", rule_str);
        }
        let raw_path = eq_parts[0];
        let svc_port = eq_parts[1];

        let (path, path_type) = if let Some(stripped) = raw_path.strip_suffix('*') {
            (format!("/{}", stripped), "Prefix")
        } else {
            (format!("/{}", raw_path), "Exact")
        };

        let svc_parts: Vec<&str> = svc_port.split(':').collect();
        if svc_parts.len() != 2 {
            anyhow::bail!("Service must be svc:port in rule: {}", rule_str);
        }

        let http_path = json!({
            "path": path,
            "pathType": path_type,
            "backend": build_ingress_backend(svc_parts[0], svc_parts[1]),
        });

        // Check if host already exists in rules
        let mut found = false;
        for existing in &mut ingress_rules {
            if existing.get("host").and_then(|h| h.as_str()).unwrap_or("") == host {
                if let Some(http) = existing.get_mut("http") {
                    if let Some(paths) = http.get_mut("paths") {
                        if let Some(arr) = paths.as_array_mut() {
                            arr.push(http_path.clone());
                            found = true;
                            break;
                        }
                    }
                }
            }
        }

        if !found {
            let mut rule = json!({
                "http": { "paths": [http_path] }
            });
            if !host.is_empty() {
                rule["host"] = json!(host);
            }
            ingress_rules.push(rule);
        }

        // Handle TLS
        if let Some(tls_str) = tls_part {
            let secret_parts: Vec<&str> = tls_str.splitn(2, '=').collect();
            let secret_name = if secret_parts.len() > 1 {
                secret_parts[1]
            } else {
                ""
            };
            let mut tls = json!({});
            if !host.is_empty() {
                tls["hosts"] = json!([host]);
            }
            if !secret_name.is_empty() {
                tls["secretName"] = json!(secret_name);
            }
            if tls.as_object().is_some_and(|o| !o.is_empty()) {
                tls_entries.push(tls);
            }
        }
    }

    if !ingress_rules.is_empty() {
        spec.insert("rules".to_string(), json!(ingress_rules));
    }
    if !tls_entries.is_empty() {
        spec.insert("tls".to_string(), json!(tls_entries));
    }

    let mut annot_map = serde_json::Map::new();
    for ann in annotations {
        let parts: Vec<&str> = ann.splitn(2, '=').collect();
        if parts.len() == 2 {
            annot_map.insert(parts[0].to_string(), json!(parts[1]));
        }
    }

    let mut metadata = json!({
        "name": name,
        "namespace": namespace,
    });
    if !annot_map.is_empty() {
        metadata["annotations"] = json!(annot_map);
    }

    Ok(json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": metadata,
        "spec": spec,
    }))
}

pub fn build_job(
    name: &str,
    namespace: &str,
    image: Option<&str>,
    from: Option<&str>,
    command: &[String],
) -> Result<Value> {
    if image.is_none() && from.is_none() {
        anyhow::bail!("Either --image or --from must be specified");
    }
    if image.is_some() && from.is_some() {
        anyhow::bail!("Either --image or --from must be specified, not both");
    }

    if let Some(from_ref) = from {
        // For --from, we just create a minimal job referencing the intent;
        // the server doesn't support this natively, so we create a basic job
        // and annotate it. In real kubectl, this fetches the CronJob first.
        let parts: Vec<&str> = from_ref.splitn(2, '/').collect();
        if parts.len() != 2 || parts[0] != "cronjob" {
            anyhow::bail!("--from must be in format cronjob/name");
        }
        // Build a placeholder job; in a real impl we'd GET the CronJob first
        return Ok(json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "annotations": {
                    "cronjob.kubernetes.io/instantiate": "manual"
                }
            },
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": name,
                            "image": "placeholder",
                        }],
                        "restartPolicy": "Never",
                    }
                }
            }
        }));
    }

    let image = image.unwrap();
    let mut container = json!({
        "name": name,
        "image": image,
    });
    if !command.is_empty() {
        container["command"] = json!(command);
    }

    Ok(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [container],
                    "restartPolicy": "Never",
                }
            }
        }
    }))
}

pub fn build_pdb(
    name: &str,
    namespace: &str,
    selector: &str,
    min_available: Option<&str>,
    max_unavailable: Option<&str>,
) -> Result<Value> {
    if min_available.is_none() && max_unavailable.is_none() {
        anyhow::bail!("One of --min-available or --max-unavailable must be specified");
    }
    if min_available.is_some() && max_unavailable.is_some() {
        anyhow::bail!("--min-available and --max-unavailable cannot both be specified");
    }

    let label_selector = parse_label_selector(selector);

    let mut spec = json!({
        "selector": label_selector,
    });

    if let Some(min) = min_available {
        spec["minAvailable"] = parse_int_or_string(min);
    }
    if let Some(max) = max_unavailable {
        spec["maxUnavailable"] = parse_int_or_string(max);
    }

    Ok(json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": spec,
    }))
}

pub fn build_role(
    name: &str,
    namespace: &str,
    verbs: &[String],
    resources: &[String],
    resource_names: &[String],
) -> Result<Value> {
    if verbs.is_empty() {
        anyhow::bail!("At least one --verb must be specified");
    }
    if resources.is_empty() {
        anyhow::bail!("At least one --resource must be specified");
    }

    let rules = build_policy_rules(verbs, resources, resource_names, &[]);

    Ok(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "rules": rules,
    }))
}

pub fn build_role_binding(
    name: &str,
    namespace: &str,
    clusterrole: Option<&str>,
    role: Option<&str>,
    users: &[String],
    groups: &[String],
    service_accounts: &[String],
) -> Result<Value> {
    if clusterrole.is_none() && role.is_none() {
        anyhow::bail!("Exactly one of --clusterrole or --role must be specified");
    }
    if clusterrole.is_some() && role.is_some() {
        anyhow::bail!("Exactly one of --clusterrole or --role must be specified");
    }

    let role_ref = if let Some(cr) = clusterrole {
        json!({
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": cr,
        })
    } else {
        json!({
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": role.unwrap(),
        })
    };

    let subjects = build_subjects(users, groups, service_accounts)?;

    Ok(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "roleRef": role_ref,
        "subjects": subjects,
    }))
}

pub fn build_secret_generic(
    name: &str,
    namespace: &str,
    from_literal: &[String],
    from_file: &[String],
    secret_type: Option<&str>,
) -> Result<Value> {
    let mut data = serde_json::Map::new();

    for literal in from_literal {
        let (key, value) = parse_key_value(literal)
            .with_context(|| format!("Invalid --from-literal: {}", literal))?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(value.as_bytes());
        data.insert(key, json!(encoded));
    }

    for file_src in from_file {
        let (key, path) = parse_file_source(file_src)?;
        let content = fs::read(&path).with_context(|| format!("Failed to read file: {}", path))?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&content);
        data.insert(key, json!(encoded));
    }

    let stype = secret_type.unwrap_or("Opaque");

    Ok(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "type": stype,
        "data": data,
    }))
}

pub fn build_secret_docker_registry(
    name: &str,
    namespace: &str,
    server: &str,
    username: Option<&str>,
    password: Option<&str>,
    email: Option<&str>,
) -> Result<Value> {
    let username = username.unwrap_or("");
    let password = password.unwrap_or("");

    if username.is_empty() || password.is_empty() {
        anyhow::bail!("--docker-username and --docker-password are required");
    }

    let auth = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{}", username, password).as_bytes());

    let mut docker_entry = json!({
        "username": username,
        "password": password,
        "auth": auth,
    });
    if let Some(e) = email {
        docker_entry["email"] = json!(e);
    }

    let docker_config = json!({
        "auths": {
            server: docker_entry,
        }
    });

    let config_bytes = serde_json::to_vec(&docker_config)?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(&config_bytes);

    Ok(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "type": "kubernetes.io/dockerconfigjson",
        "data": {
            ".dockerconfigjson": encoded,
        }
    }))
}

pub fn build_secret_tls(
    name: &str,
    namespace: &str,
    cert_path: &str,
    key_path: &str,
) -> Result<Value> {
    let cert_data =
        fs::read(cert_path).with_context(|| format!("Failed to read cert file: {}", cert_path))?;
    let key_data =
        fs::read(key_path).with_context(|| format!("Failed to read key file: {}", key_path))?;

    let cert_encoded = base64::engine::general_purpose::STANDARD.encode(&cert_data);
    let key_encoded = base64::engine::general_purpose::STANDARD.encode(&key_data);

    Ok(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "type": "kubernetes.io/tls",
        "data": {
            "tls.crt": cert_encoded,
            "tls.key": key_encoded,
        }
    }))
}

pub fn build_service_account(name: &str, namespace: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": name,
            "namespace": namespace,
        }
    })
}

pub fn build_token_request(
    audiences: &[String],
    duration: Option<&str>,
    bound_object_kind: Option<&str>,
    bound_object_name: Option<&str>,
    bound_object_uid: Option<&str>,
) -> Result<Value> {
    let mut spec = json!({});

    if !audiences.is_empty() {
        spec["audiences"] = json!(audiences);
    }

    if let Some(dur) = duration {
        let seconds = parse_duration_to_seconds(dur)?;
        spec["expirationSeconds"] = json!(seconds);
    }

    if let Some(kind) = bound_object_kind {
        let obj_name = bound_object_name.unwrap_or("");
        if obj_name.is_empty() {
            anyhow::bail!("--bound-object-name is required when --bound-object-kind is set");
        }
        let api_version = match kind {
            "Pod" | "Secret" | "Node" => "v1",
            _ => anyhow::bail!("Unsupported --bound-object-kind: {}", kind),
        };
        let mut bound_ref = json!({
            "kind": kind,
            "apiVersion": api_version,
            "name": obj_name,
        });
        if let Some(uid) = bound_object_uid {
            bound_ref["uid"] = json!(uid);
        }
        spec["boundObjectRef"] = bound_ref;
    }

    Ok(json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest",
        "spec": spec,
    }))
}

async fn execute_service_subcommand(
    client: &ApiClient,
    cmd: &ServiceCommands,
    default_namespace: &str,
) -> Result<()> {
    match cmd {
        ServiceCommands::ClusterIP {
            name,
            tcp,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_service(name, ns, "ClusterIP", tcp, None)?;
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/services", ns), &body)
                .await?;
            println!("service/{} created", name);
        }
        ServiceCommands::NodePort {
            name,
            tcp,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_service(name, ns, "NodePort", tcp, None)?;
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/services", ns), &body)
                .await?;
            println!("service/{} created", name);
        }
        ServiceCommands::LoadBalancer {
            name,
            tcp,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_service(name, ns, "LoadBalancer", tcp, None)?;
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/services", ns), &body)
                .await?;
            println!("service/{} created", name);
        }
        ServiceCommands::ExternalName {
            name,
            external_name,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let body = build_service_external_name(name, ns, external_name);
            let _: Value = client
                .post(&format!("/api/v1/namespaces/{}/services", ns), &body)
                .await?;
            println!("service/{} created", name);
        }
    }
    Ok(())
}

// ── New JSON builders ───────────────────────────────────────────────────────

pub fn build_namespace(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
        }
    })
}

pub fn build_deployment(
    name: &str,
    namespace: &str,
    image: &str,
    replicas: i32,
    port: Option<i32>,
) -> Value {
    let mut container = json!({
        "name": name,
        "image": image,
    });
    if let Some(p) = port {
        container["ports"] = json!([{ "containerPort": p }]);
    }

    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": { "app": name },
        },
        "spec": {
            "replicas": replicas,
            "selector": {
                "matchLabels": { "app": name },
            },
            "template": {
                "metadata": {
                    "labels": { "app": name },
                },
                "spec": {
                    "containers": [container],
                }
            }
        }
    })
}

pub fn build_service(
    name: &str,
    namespace: &str,
    service_type: &str,
    tcp: &[String],
    _selector: Option<&str>,
) -> Result<Value> {
    let mut ports: Vec<Value> = Vec::new();
    for tcp_spec in tcp {
        let parts: Vec<&str> = tcp_spec.split(':').collect();
        if parts.len() != 2 {
            anyhow::bail!("--tcp must be in format port:targetPort, got: {}", tcp_spec);
        }
        let port: i32 = parts[0]
            .parse()
            .with_context(|| format!("Invalid port number: {}", parts[0]))?;
        let target_port: i32 = parts[1]
            .parse()
            .with_context(|| format!("Invalid targetPort number: {}", parts[1]))?;
        ports.push(json!({
            "port": port,
            "targetPort": target_port,
            "protocol": "TCP",
        }));
    }

    let mut spec = json!({
        "type": service_type,
        "selector": { "app": name },
    });
    if !ports.is_empty() {
        spec["ports"] = json!(ports);
    }

    Ok(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": { "app": name },
        },
        "spec": spec,
    }))
}

pub fn build_service_external_name(name: &str, namespace: &str, external_name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "type": "ExternalName",
            "externalName": external_name,
        }
    })
}

pub fn build_priority_class(
    name: &str,
    value: i32,
    global_default: bool,
    preemption_policy: &str,
    description: Option<&str>,
) -> Value {
    let mut pc = json!({
        "apiVersion": "scheduling.k8s.io/v1",
        "kind": "PriorityClass",
        "metadata": { "name": name },
        "value": value,
        "globalDefault": global_default,
        "preemptionPolicy": preemption_policy,
    });
    if let Some(desc) = description {
        pc["description"] = json!(desc);
    }
    pc
}

pub fn build_quota(name: &str, namespace: &str, hard: Option<&str>) -> Result<Value> {
    let mut hard_map = serde_json::Map::new();
    if let Some(h) = hard {
        for pair in h.split(',') {
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() != 2 {
                anyhow::bail!("--hard must be in format resource=value, got: {}", pair);
            }
            hard_map.insert(parts[0].trim().to_string(), json!(parts[1].trim()));
        }
    }

    Ok(json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "hard": hard_map,
        }
    }))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn build_policy_rules(
    verbs: &[String],
    resources: &[String],
    resource_names: &[String],
    non_resource_urls: &[String],
) -> Value {
    // Group resources by API group
    let mut group_map: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for res in resources {
        let (resource, group) = parse_resource_group(res);
        group_map.entry(group).or_default().push(resource);
    }

    let verb_list: Vec<&str> = verbs.iter().map(|s| s.as_str()).collect();
    let mut rules: Vec<Value> = Vec::new();

    for (group, res_list) in &group_map {
        let mut rule = json!({
            "apiGroups": [group],
            "resources": res_list,
            "verbs": verb_list,
        });
        if !resource_names.is_empty() {
            rule["resourceNames"] = json!(resource_names);
        }
        rules.push(rule);
    }

    if !non_resource_urls.is_empty() {
        rules.push(json!({
            "nonResourceURLs": non_resource_urls,
            "verbs": verb_list,
        }));
    }

    json!(rules)
}

fn build_subjects(
    users: &[String],
    groups: &[String],
    service_accounts: &[String],
) -> Result<Value> {
    let mut subjects: Vec<Value> = Vec::new();

    for user in users {
        subjects.push(json!({
            "kind": "User",
            "apiGroup": "rbac.authorization.k8s.io",
            "name": user,
        }));
    }

    for group in groups {
        subjects.push(json!({
            "kind": "Group",
            "apiGroup": "rbac.authorization.k8s.io",
            "name": group,
        }));
    }

    for sa in service_accounts {
        let parts: Vec<&str> = sa.split(':').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            anyhow::bail!("serviceaccount must be <namespace>:<name>, got: {}", sa);
        }
        subjects.push(json!({
            "kind": "ServiceAccount",
            "name": parts[1],
            "namespace": parts[0],
        }));
    }

    Ok(json!(subjects))
}

/// Parse "resource.group/subresource" into (resource_with_sub, api_group)
pub fn parse_resource_group(input: &str) -> (String, String) {
    let (main, sub) = if let Some(idx) = input.find('/') {
        (&input[..idx], Some(&input[idx + 1..]))
    } else {
        (input, None)
    };

    let parts: Vec<&str> = main.splitn(2, '.').collect();
    let (resource, group) = if parts.len() == 2 {
        (parts[0].to_string(), parts[1].to_string())
    } else {
        (parts[0].to_string(), String::new())
    };

    let full_resource = if let Some(s) = sub {
        format!("{}/{}", resource, s)
    } else {
        resource
    };

    (full_resource, group)
}

fn parse_key_value(s: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        anyhow::bail!("Expected key=value, got: {}", s);
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

fn parse_file_source(s: &str) -> Result<(String, String)> {
    if s.contains('=') {
        let parts: Vec<&str> = s.splitn(2, '=').collect();
        Ok((parts[0].to_string(), parts[1].to_string()))
    } else {
        let path = Path::new(s);
        let key = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(s)
            .to_string();
        Ok((key, s.to_string()))
    }
}

fn parse_label_selector(selector: &str) -> Value {
    let mut match_labels = serde_json::Map::new();
    for part in selector.split(',') {
        let kv: Vec<&str> = part.splitn(2, '=').collect();
        if kv.len() == 2 {
            match_labels.insert(kv[0].trim().to_string(), json!(kv[1].trim()));
        }
    }
    json!({ "matchLabels": match_labels })
}

fn parse_int_or_string(s: &str) -> Value {
    if s.ends_with('%') {
        json!(s)
    } else if let Ok(n) = s.parse::<i64>() {
        json!(n)
    } else {
        json!(s)
    }
}

fn parse_duration_to_seconds(s: &str) -> Result<i64> {
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix('s') {
        Ok(stripped.parse::<i64>()?)
    } else if let Some(stripped) = s.strip_suffix('m') {
        Ok(stripped.parse::<i64>()? * 60)
    } else if let Some(stripped) = s.strip_suffix('h') {
        Ok(stripped.parse::<i64>()? * 3600)
    } else {
        // Assume seconds
        Ok(s.parse::<i64>()?)
    }
}

fn build_ingress_backend(svc_name: &str, svc_port: &str) -> Value {
    let port = if let Ok(num) = svc_port.parse::<i32>() {
        json!({ "number": num })
    } else {
        json!({ "name": svc_port })
    };

    json!({
        "service": {
            "name": svc_name,
            "port": port,
        }
    })
}

// ── Legacy inline creation ──────────────────────────────────────────────────

/// Execute inline resource creation (e.g., kubectl create namespace foo)
pub async fn execute_inline(_client: &ApiClient, args: &[String], namespace: &str) -> Result<()> {
    if args.is_empty() {
        anyhow::bail!("Resource type required");
    }

    let resource_type = &args[0];
    match resource_type.as_str() {
        "namespace" | "ns" => {
            if args.len() < 2 {
                anyhow::bail!("Namespace name required");
            }
            let name = &args[1];
            println!("Creating namespace: {}", name);
            println!("Note: Inline resource creation not yet fully implemented");
        }
        _ => {
            println!("Creating {} in namespace {}", resource_type, namespace);
            println!("Note: Inline resource creation not yet fully implemented");
        }
    }

    Ok(())
}

pub async fn execute(client: &ApiClient, file: &str) -> Result<()> {
    let contents = fs::read_to_string(file).context("Failed to read file")?;

    // Support for multi-document YAML files
    for document in serde_yaml::Deserializer::from_str(&contents) {
        let value = serde_yaml::Value::deserialize(document)?;

        // Skip empty documents
        if value.is_null() {
            continue;
        }

        create_resource(client, &value).await?;
    }

    Ok(())
}

async fn create_resource(client: &ApiClient, value: &serde_yaml::Value) -> Result<()> {
    // Get the kind field
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field")?;

    let yaml_str = serde_yaml::to_string(value)?;

    match kind {
        "Pod" => {
            let pod: Pod = serde_yaml::from_str(&yaml_str)?;
            let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
            let _result: Pod = client
                .post(&format!("/api/v1/namespaces/{}/pods", namespace), &pod)
                .await?;
            println!("Pod '{}' created", pod.metadata.name);
        }
        "Service" => {
            let service: Service = serde_yaml::from_str(&yaml_str)?;
            let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
            let _result: Service = client
                .post(
                    &format!("/api/v1/namespaces/{}/services", namespace),
                    &service,
                )
                .await?;
            println!("Service '{}' created", service.metadata.name);
        }
        "Deployment" => {
            let deployment: Deployment = serde_yaml::from_str(&yaml_str)?;
            let namespace = deployment
                .metadata
                .namespace
                .as_deref()
                .unwrap_or("default");
            let _result: Deployment = client
                .post(
                    &format!("/apis/apps/v1/namespaces/{}/deployments", namespace),
                    &deployment,
                )
                .await?;
            println!("Deployment '{}' created", deployment.metadata.name);
        }
        "Node" => {
            let node: Node = serde_yaml::from_str(&yaml_str)?;
            let _result: Node = client.post("/api/v1/nodes", &node).await?;
            println!("Node '{}' created", node.metadata.name);
        }
        "Namespace" => {
            let namespace: Namespace = serde_yaml::from_str(&yaml_str)?;
            let _result: Namespace = client.post("/api/v1/namespaces", &namespace).await?;
            println!("Namespace '{}' created", namespace.metadata.name);
        }
        "StorageClass" => {
            let sc: StorageClass = serde_yaml::from_str(&yaml_str)?;
            let _result: StorageClass = client
                .post("/apis/storage.k8s.io/v1/storageclasses", &sc)
                .await?;
            println!("StorageClass '{}' created", sc.metadata.name);
        }
        "VolumeSnapshot" => {
            let vs: VolumeSnapshot = serde_yaml::from_str(&yaml_str)?;
            let namespace = vs.metadata.namespace.as_deref().unwrap_or("default");
            let _result: VolumeSnapshot = client
                .post(
                    &format!(
                        "/apis/snapshot.storage.k8s.io/v1/namespaces/{}/volumesnapshots",
                        namespace
                    ),
                    &vs,
                )
                .await?;
            println!("VolumeSnapshot '{}' created", vs.metadata.name);
        }
        "VolumeSnapshotClass" => {
            let vsc: VolumeSnapshotClass = serde_yaml::from_str(&yaml_str)?;
            let _result: VolumeSnapshotClass = client
                .post(
                    "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses",
                    &vsc,
                )
                .await?;
            println!("VolumeSnapshotClass '{}' created", vsc.metadata.name);
        }
        "Endpoints" => {
            let ep: Endpoints = serde_yaml::from_str(&yaml_str)?;
            let namespace = ep.metadata.namespace.as_deref().unwrap_or("default");
            let _result: Endpoints = client
                .post(&format!("/api/v1/namespaces/{}/endpoints", namespace), &ep)
                .await?;
            println!("Endpoints '{}' created", ep.metadata.name);
        }
        "ResourceQuota" => {
            let rq: ResourceQuota = serde_yaml::from_str(&yaml_str)?;
            let namespace = rq.metadata.namespace.as_deref().unwrap_or("default");
            let _result: ResourceQuota = client
                .post(
                    &format!("/api/v1/namespaces/{}/resourcequotas", namespace),
                    &rq,
                )
                .await?;
            println!("ResourceQuota '{}' created", rq.metadata.name);
        }
        "LimitRange" => {
            let lr: LimitRange = serde_yaml::from_str(&yaml_str)?;
            let namespace = lr.metadata.namespace.as_deref().unwrap_or("default");
            let _result: LimitRange = client
                .post(
                    &format!("/api/v1/namespaces/{}/limitranges", namespace),
                    &lr,
                )
                .await?;
            println!("LimitRange '{}' created", lr.metadata.name);
        }
        "PriorityClass" => {
            let pc: PriorityClass = serde_yaml::from_str(&yaml_str)?;
            let _result: PriorityClass = client
                .post("/apis/scheduling.k8s.io/v1/priorityclasses", &pc)
                .await?;
            println!("PriorityClass '{}' created", pc.metadata.name);
        }
        _ => anyhow::bail!("Unsupported resource kind: {}", kind),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // 1. build_cluster_role — basic with verbs/resources
    #[test]
    fn test_build_cluster_role_basic() {
        let verbs = vec!["get".to_string(), "list".to_string()];
        let resources = vec!["pods".to_string()];
        let result = build_cluster_role("test-cr", &verbs, &resources, &[], &[], &[]).unwrap();
        assert_eq!(result["kind"], "ClusterRole");
        assert_eq!(result["metadata"]["name"], "test-cr");
        assert!(result["rules"].is_array());
    }

    // 2. build_cluster_role — with aggregation rule
    #[test]
    fn test_build_cluster_role_aggregation() {
        let agg = vec!["rbac.example.com/aggregate-to-admin=true".to_string()];
        let result = build_cluster_role("agg-cr", &[], &[], &[], &[], &agg).unwrap();
        assert!(result["aggregationRule"]["clusterRoleSelectors"].is_array());
        assert!(result.get("rules").is_none() || result["rules"].is_null());
    }

    // 3. build_cluster_role_binding
    #[test]
    fn test_build_cluster_role_binding() {
        let users = vec!["admin-user".to_string()];
        let result =
            build_cluster_role_binding("test-crb", "cluster-admin", &users, &[], &[]).unwrap();
        assert_eq!(result["kind"], "ClusterRoleBinding");
        assert_eq!(result["roleRef"]["name"], "cluster-admin");
        assert_eq!(result["subjects"][0]["name"], "admin-user");
    }

    // 4. build_configmap — from literals
    #[test]
    fn test_build_configmap_from_literal() {
        let literals = vec!["key1=value1".to_string(), "key2=value2".to_string()];
        let result = build_configmap("test-cm", "default", &literals, &[]).unwrap();
        assert_eq!(result["kind"], "ConfigMap");
        assert_eq!(result["data"]["key1"], "value1");
        assert_eq!(result["data"]["key2"], "value2");
    }

    // 5. build_configmap — from file
    #[test]
    fn test_build_configmap_from_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let file_path = dir.path().join("config.txt");
        std::fs::write(&file_path, "file-content").unwrap();
        let from_file = vec![file_path.to_string_lossy().to_string()];
        let result = build_configmap("file-cm", "default", &[], &from_file).unwrap();
        assert_eq!(result["data"]["config.txt"], "file-content");
    }

    // 6. build_cronjob — basic
    #[test]
    fn test_build_cronjob_basic() {
        let result = build_cronjob("my-cj", "default", "busybox", "*/5 * * * *", "Never", &[]);
        assert_eq!(result["kind"], "CronJob");
        assert_eq!(result["spec"]["schedule"], "*/5 * * * *");
    }

    // 7. build_cronjob — with command
    #[test]
    fn test_build_cronjob_with_command() {
        let cmd = vec!["echo".to_string(), "hello".to_string()];
        let result = build_cronjob("cj2", "default", "busybox", "0 * * * *", "Never", &cmd);
        let container = &result["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(container["command"][0], "echo");
    }

    // 8. build_ingress — basic rule
    #[test]
    fn test_build_ingress_basic() {
        let rules = vec!["example.com/path*=my-svc:80".to_string()];
        let result =
            build_ingress("test-ing", "default", Some("nginx"), &rules, None, &[]).unwrap();
        assert_eq!(result["kind"], "Ingress");
        assert_eq!(result["spec"]["ingressClassName"], "nginx");
        assert!(result["spec"]["rules"].is_array());
    }

    // 9. build_ingress — with default backend
    #[test]
    fn test_build_ingress_default_backend() {
        let result = build_ingress(
            "test-ing2",
            "default",
            None,
            &[],
            Some("default-svc:80"),
            &[],
        )
        .unwrap();
        assert_eq!(
            result["spec"]["defaultBackend"]["service"]["name"],
            "default-svc"
        );
    }

    // 10. build_ingress — with annotations
    #[test]
    fn test_build_ingress_annotations() {
        let annotations = vec!["nginx.ingress.kubernetes.io/rewrite-target=/".to_string()];
        let result = build_ingress("ann-ing", "default", None, &[], None, &annotations).unwrap();
        assert!(result["metadata"]["annotations"]
            .as_object()
            .unwrap()
            .contains_key("nginx.ingress.kubernetes.io/rewrite-target"));
    }

    // 11. build_job — with image
    #[test]
    fn test_build_job_with_image() {
        let result = build_job("test-job", "default", Some("busybox"), None, &[]).unwrap();
        assert_eq!(result["kind"], "Job");
        assert_eq!(
            result["spec"]["template"]["spec"]["containers"][0]["image"],
            "busybox"
        );
    }

    // 12. build_job — with from cronjob
    #[test]
    fn test_build_job_from_cronjob() {
        let result = build_job("manual-job", "default", None, Some("cronjob/my-cj"), &[]).unwrap();
        assert_eq!(
            result["metadata"]["annotations"]["cronjob.kubernetes.io/instantiate"],
            "manual"
        );
    }

    // 13. build_job — error: neither image nor from
    #[test]
    fn test_build_job_no_image_no_from() {
        let result = build_job("bad-job", "default", None, None, &[]);
        assert!(result.is_err());
    }

    // 14. build_job — error: both image and from
    #[test]
    fn test_build_job_both_image_and_from() {
        let result = build_job("bad-job", "default", Some("img"), Some("cronjob/cj"), &[]);
        assert!(result.is_err());
    }

    // 15. build_pdb — with min_available
    #[test]
    fn test_build_pdb_min_available() {
        let result = build_pdb("test-pdb", "default", "app=nginx", Some("2"), None).unwrap();
        assert_eq!(result["kind"], "PodDisruptionBudget");
        assert_eq!(result["spec"]["minAvailable"], 2);
    }

    // 16. build_pdb — error: neither min nor max
    #[test]
    fn test_build_pdb_no_min_no_max() {
        let result = build_pdb("bad-pdb", "default", "app=nginx", None, None);
        assert!(result.is_err());
    }

    // 17. build_role — basic
    #[test]
    fn test_build_role_basic() {
        let verbs = vec!["get".to_string()];
        let resources = vec!["pods".to_string()];
        let result = build_role("test-role", "default", &verbs, &resources, &[]).unwrap();
        assert_eq!(result["kind"], "Role");
        assert_eq!(result["metadata"]["namespace"], "default");
    }

    // 18. build_role — error: no verbs
    #[test]
    fn test_build_role_no_verbs() {
        let result = build_role("bad-role", "default", &[], &["pods".to_string()], &[]);
        assert!(result.is_err());
    }

    // 19. build_role_binding — with clusterrole ref
    #[test]
    fn test_build_role_binding_clusterrole() {
        let users = vec!["user1".to_string()];
        let result =
            build_role_binding("test-rb", "default", Some("admin"), None, &users, &[], &[])
                .unwrap();
        assert_eq!(result["kind"], "RoleBinding");
        assert_eq!(result["roleRef"]["kind"], "ClusterRole");
    }

    // 20. build_role_binding — with role ref
    #[test]
    fn test_build_role_binding_role() {
        let groups = vec!["devs".to_string()];
        let result = build_role_binding(
            "test-rb2",
            "default",
            None,
            Some("my-role"),
            &[],
            &groups,
            &[],
        )
        .unwrap();
        assert_eq!(result["roleRef"]["kind"], "Role");
        assert_eq!(result["roleRef"]["name"], "my-role");
    }

    // 21. build_secret_generic — from literals
    #[test]
    fn test_build_secret_generic() {
        let literals = vec!["password=s3cr3t".to_string()];
        let result = build_secret_generic("test-secret", "default", &literals, &[], None).unwrap();
        assert_eq!(result["kind"], "Secret");
        assert_eq!(result["type"], "Opaque");
        // data should be base64-encoded
        let encoded = result["data"]["password"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "s3cr3t");
    }

    // 22. build_secret_docker_registry — valid
    #[test]
    fn test_build_secret_docker_registry_valid() {
        let result = build_secret_docker_registry(
            "my-reg",
            "default",
            "https://index.docker.io/v1/",
            Some("user"),
            Some("pass"),
            Some("user@example.com"),
        )
        .unwrap();
        assert_eq!(result["type"], "kubernetes.io/dockerconfigjson");
        assert!(result["data"][".dockerconfigjson"].is_string());
    }

    // 23. build_secret_docker_registry — missing credentials
    #[test]
    fn test_build_secret_docker_registry_missing_creds() {
        let result = build_secret_docker_registry("bad-reg", "default", "server", None, None, None);
        assert!(result.is_err());
    }

    // 24. build_service_account
    #[test]
    fn test_build_service_account() {
        let result = build_service_account("test-sa", "kube-system");
        assert_eq!(result["kind"], "ServiceAccount");
        assert_eq!(result["metadata"]["name"], "test-sa");
        assert_eq!(result["metadata"]["namespace"], "kube-system");
    }

    // 25. build_token_request — basic with audiences
    #[test]
    fn test_build_token_request_basic() {
        let audiences = vec!["api".to_string()];
        let result = build_token_request(&audiences, None, None, None, None).unwrap();
        assert_eq!(result["kind"], "TokenRequest");
        assert_eq!(result["spec"]["audiences"][0], "api");
    }

    // 26. build_token_request — with duration
    #[test]
    fn test_build_token_request_duration() {
        let result = build_token_request(&[], Some("1h"), None, None, None).unwrap();
        assert_eq!(result["spec"]["expirationSeconds"], 3600);
    }

    // 27. parse_resource_group — with group
    #[test]
    fn test_parse_resource_group_with_group() {
        let (resource, group) = parse_resource_group("deployments.apps");
        assert_eq!(resource, "deployments");
        assert_eq!(group, "apps");
    }

    // 28. parse_resource_group — without group
    #[test]
    fn test_parse_resource_group_without_group() {
        let (resource, group) = parse_resource_group("pods");
        assert_eq!(resource, "pods");
        assert_eq!(group, "");
    }

    // Additional helpers that are private but callable from tests module
    #[test]
    fn test_parse_key_value_valid() {
        let (k, v) = parse_key_value("foo=bar").unwrap();
        assert_eq!(k, "foo");
        assert_eq!(v, "bar");
    }

    #[test]
    fn test_parse_key_value_invalid() {
        assert!(parse_key_value("noequalssign").is_err());
    }

    #[test]
    fn test_parse_file_source_with_key() {
        let (k, p) = parse_file_source("mykey=/path/to/file").unwrap();
        assert_eq!(k, "mykey");
        assert_eq!(p, "/path/to/file");
    }

    #[test]
    fn test_parse_file_source_without_key() {
        let (k, p) = parse_file_source("/path/to/config.txt").unwrap();
        assert_eq!(k, "config.txt");
        assert_eq!(p, "/path/to/config.txt");
    }

    #[test]
    fn test_parse_label_selector() {
        let result = parse_label_selector("app=nginx,env=prod");
        let labels = result["matchLabels"].as_object().unwrap();
        assert_eq!(labels["app"], "nginx");
        assert_eq!(labels["env"], "prod");
    }

    #[test]
    fn test_parse_int_or_string_integer() {
        assert_eq!(parse_int_or_string("5"), serde_json::json!(5));
    }

    #[test]
    fn test_parse_int_or_string_percentage() {
        assert_eq!(parse_int_or_string("50%"), serde_json::json!("50%"));
    }

    #[test]
    fn test_parse_duration_to_seconds_seconds() {
        assert_eq!(parse_duration_to_seconds("300s").unwrap(), 300);
    }

    #[test]
    fn test_parse_duration_to_seconds_minutes() {
        assert_eq!(parse_duration_to_seconds("5m").unwrap(), 300);
    }

    #[test]
    fn test_parse_duration_to_seconds_hours() {
        assert_eq!(parse_duration_to_seconds("2h").unwrap(), 7200);
    }

    #[test]
    fn test_parse_duration_to_seconds_bare_number() {
        assert_eq!(parse_duration_to_seconds("120").unwrap(), 120);
    }

    #[test]
    fn test_build_ingress_backend_numeric_port() {
        let backend = build_ingress_backend("my-svc", "8080");
        assert_eq!(backend["service"]["name"], "my-svc");
        assert_eq!(backend["service"]["port"]["number"], 8080);
    }

    #[test]
    fn test_build_ingress_backend_named_port() {
        let backend = build_ingress_backend("my-svc", "http");
        assert_eq!(backend["service"]["port"]["name"], "http");
    }

    #[test]
    fn test_build_policy_rules_basic() {
        let verbs = vec!["get".to_string(), "list".to_string()];
        let resources = vec!["pods".to_string()];
        let result = build_policy_rules(&verbs, &resources, &[], &[]);
        assert!(result.is_array());
        let rules = result.as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["verbs"][0], "get");
    }

    #[test]
    fn test_build_policy_rules_non_resource_urls() {
        let verbs = vec!["get".to_string()];
        let urls = vec!["/healthz".to_string()];
        let result = build_policy_rules(&verbs, &[], &[], &urls);
        let rules = result.as_array().unwrap();
        assert_eq!(rules[0]["nonResourceURLs"][0], "/healthz");
    }

    #[test]
    fn test_build_subjects_user() {
        let users = vec!["alice".to_string()];
        let result = build_subjects(&users, &[], &[]).unwrap();
        assert_eq!(result[0]["kind"], "User");
        assert_eq!(result[0]["name"], "alice");
    }

    #[test]
    fn test_build_subjects_group() {
        let groups = vec!["developers".to_string()];
        let result = build_subjects(&[], &groups, &[]).unwrap();
        assert_eq!(result[0]["kind"], "Group");
    }

    #[test]
    fn test_build_subjects_service_account() {
        let sas = vec!["kube-system:default".to_string()];
        let result = build_subjects(&[], &[], &sas).unwrap();
        assert_eq!(result[0]["kind"], "ServiceAccount");
        assert_eq!(result[0]["namespace"], "kube-system");
        assert_eq!(result[0]["name"], "default");
    }

    #[test]
    fn test_build_subjects_invalid_sa() {
        let sas = vec!["invalid-format".to_string()];
        assert!(build_subjects(&[], &[], &sas).is_err());
    }

    #[tokio::test]
    async fn test_execute_inline_namespace() {
        let client = ApiClient::new("https://127.0.0.1:1", false, None).unwrap();
        let args = vec!["namespace".to_string(), "test-ns".to_string()];
        // This doesn't make API calls, just prints
        let result = execute_inline(&client, &args, "default").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_inline_empty_args() {
        let client = ApiClient::new("https://127.0.0.1:1", false, None).unwrap();
        let result = execute_inline(&client, &[], "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_inline_unknown_resource() {
        let client = ApiClient::new("https://127.0.0.1:1", false, None).unwrap();
        let args = vec!["deployment".to_string()];
        // This doesn't fail, just prints a message
        let result = execute_inline(&client, &args, "default").await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_secret_tls_missing_cert() {
        let result = build_secret_tls(
            "tls-secret",
            "default",
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_build_pdb_max_unavailable_percentage() {
        let result = build_pdb("pdb-pct", "default", "app=web", None, Some("25%")).unwrap();
        assert_eq!(result["spec"]["maxUnavailable"], "25%");
    }

    #[test]
    fn test_parse_resource_group_with_subresource() {
        let (resource, group) = parse_resource_group("pods/status");
        assert_eq!(resource, "pods/status");
        assert_eq!(group, "");
    }

    #[test]
    fn test_build_token_request_bound_object() {
        let result =
            build_token_request(&[], None, Some("Pod"), Some("my-pod"), Some("abc-123")).unwrap();
        let bound = &result["spec"]["boundObjectRef"];
        assert_eq!(bound["kind"], "Pod");
        assert_eq!(bound["name"], "my-pod");
        assert_eq!(bound["uid"], "abc-123");
    }

    // ── Tests for new subcommands ───────────────────────────────────────

    #[test]
    fn test_build_namespace() {
        let result = build_namespace("test-ns");
        assert_eq!(result["apiVersion"], "v1");
        assert_eq!(result["kind"], "Namespace");
        assert_eq!(result["metadata"]["name"], "test-ns");
    }

    #[test]
    fn test_build_deployment_basic() {
        let result = build_deployment("nginx", "default", "nginx:1.25", 3, None);
        assert_eq!(result["apiVersion"], "apps/v1");
        assert_eq!(result["kind"], "Deployment");
        assert_eq!(result["metadata"]["name"], "nginx");
        assert_eq!(result["metadata"]["namespace"], "default");
        assert_eq!(result["spec"]["replicas"], 3);
        let container = &result["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(container["name"], "nginx");
        assert_eq!(container["image"], "nginx:1.25");
        assert!(container.get("ports").is_none() || container["ports"].is_null());
    }

    #[test]
    fn test_build_deployment_with_port() {
        let result = build_deployment("web", "prod", "nginx:latest", 2, Some(80));
        let container = &result["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(container["ports"][0]["containerPort"], 80);
        assert_eq!(result["spec"]["selector"]["matchLabels"]["app"], "web");
        assert_eq!(
            result["spec"]["template"]["metadata"]["labels"]["app"],
            "web"
        );
    }

    #[test]
    fn test_build_service_clusterip() {
        let tcp = vec!["80:8080".to_string()];
        let result = build_service("my-svc", "default", "ClusterIP", &tcp, None).unwrap();
        assert_eq!(result["apiVersion"], "v1");
        assert_eq!(result["kind"], "Service");
        assert_eq!(result["spec"]["type"], "ClusterIP");
        assert_eq!(result["spec"]["ports"][0]["port"], 80);
        assert_eq!(result["spec"]["ports"][0]["targetPort"], 8080);
        assert_eq!(result["spec"]["ports"][0]["protocol"], "TCP");
    }

    #[test]
    fn test_build_service_nodeport() {
        let tcp = vec!["80:8080".to_string()];
        let result = build_service("my-svc", "default", "NodePort", &tcp, None).unwrap();
        assert_eq!(result["spec"]["type"], "NodePort");
    }

    #[test]
    fn test_build_service_loadbalancer() {
        let tcp = vec!["443:8443".to_string()];
        let result = build_service("lb-svc", "prod", "LoadBalancer", &tcp, None).unwrap();
        assert_eq!(result["spec"]["type"], "LoadBalancer");
        assert_eq!(result["spec"]["ports"][0]["port"], 443);
        assert_eq!(result["spec"]["ports"][0]["targetPort"], 8443);
    }

    #[test]
    fn test_build_service_multiple_ports() {
        let tcp = vec!["80:8080".to_string(), "443:8443".to_string()];
        let result = build_service("multi-svc", "default", "ClusterIP", &tcp, None).unwrap();
        let ports = result["spec"]["ports"].as_array().unwrap();
        assert_eq!(ports.len(), 2);
    }

    #[test]
    fn test_build_service_invalid_tcp() {
        let tcp = vec!["invalid".to_string()];
        let result = build_service("bad-svc", "default", "ClusterIP", &tcp, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_service_external_name() {
        let result = build_service_external_name("ext-svc", "default", "my.database.example.com");
        assert_eq!(result["apiVersion"], "v1");
        assert_eq!(result["kind"], "Service");
        assert_eq!(result["spec"]["type"], "ExternalName");
        assert_eq!(result["spec"]["externalName"], "my.database.example.com");
    }

    #[test]
    fn test_build_priority_class_basic() {
        let result = build_priority_class(
            "high-priority",
            1000000,
            false,
            "PreemptLowerPriority",
            None,
        );
        assert_eq!(result["apiVersion"], "scheduling.k8s.io/v1");
        assert_eq!(result["kind"], "PriorityClass");
        assert_eq!(result["metadata"]["name"], "high-priority");
        assert_eq!(result["value"], 1000000);
        assert_eq!(result["globalDefault"], false);
        assert_eq!(result["preemptionPolicy"], "PreemptLowerPriority");
        assert!(result.get("description").is_none() || result["description"].is_null());
    }

    #[test]
    fn test_build_priority_class_with_description() {
        let result = build_priority_class(
            "critical",
            2000000,
            true,
            "Never",
            Some("Critical priority class"),
        );
        assert_eq!(result["globalDefault"], true);
        assert_eq!(result["preemptionPolicy"], "Never");
        assert_eq!(result["description"], "Critical priority class");
    }

    #[test]
    fn test_build_quota_basic() {
        let result = build_quota("my-quota", "default", Some("pods=10,services=5")).unwrap();
        assert_eq!(result["apiVersion"], "v1");
        assert_eq!(result["kind"], "ResourceQuota");
        assert_eq!(result["metadata"]["name"], "my-quota");
        assert_eq!(result["metadata"]["namespace"], "default");
        assert_eq!(result["spec"]["hard"]["pods"], "10");
        assert_eq!(result["spec"]["hard"]["services"], "5");
    }

    #[test]
    fn test_build_quota_empty_hard() {
        let result = build_quota("empty-quota", "default", None).unwrap();
        assert_eq!(result["kind"], "ResourceQuota");
        assert!(result["spec"]["hard"].as_object().unwrap().is_empty());
    }

    #[test]
    fn test_build_quota_invalid_hard() {
        let result = build_quota("bad-quota", "default", Some("invalid-format"));
        assert!(result.is_err());
    }
}
