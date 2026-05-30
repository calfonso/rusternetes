use crate::client::{ApiClient, GetError};
use crate::discovery::RestMapper;
use crate::ops::build_path;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::fs;
use std::io::{self, Read};

/// Show diff between current and applied configuration
pub async fn execute(client: &ApiClient, file: &str, namespace: &str) -> Result<()> {
    let contents = if file == "-" {
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .context("Failed to read from stdin")?;
        buffer
    } else {
        fs::read_to_string(file).context("Failed to read file")?
    };

    // Build the REST mapper once for all documents in this file.
    let mapper = RestMapper::from_server(client).await?;

    // Support for multi-document YAML files
    for document in serde_yaml::Deserializer::from_str(&contents) {
        let value = serde_yaml::Value::deserialize(document)?;

        if value.is_null() {
            continue;
        }

        diff_resource(client, &mapper, &value, namespace).await?;
    }

    Ok(())
}

async fn diff_resource(
    client: &ApiClient,
    mapper: &RestMapper,
    value: &serde_yaml::Value,
    default_namespace: &str,
) -> Result<()> {
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field")?;

    let metadata = value.get("metadata").context("Missing 'metadata' field")?;
    let name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .context("Missing 'name' in metadata")?;

    // Resolve the kind via the server-side REST mapper.
    let mapping = mapper.resolve(kind).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{kind}\"")
    })?;

    // Determine the effective namespace.
    let ns: Option<String> = if mapping.namespaced {
        let from_body = metadata.get("namespace").and_then(|n| n.as_str());
        Some(from_body.unwrap_or(default_namespace).to_string())
    } else {
        None
    };

    // Build the item path and fetch the live object.
    let api_path = build_path(mapping, ns.as_deref(), Some(name));

    // Fetch the live object. We match on GetError so we can cleanly distinguish
    // a genuine 404 (resource doesn't exist yet → show a creation diff) from
    // any other error (connection failure, permission denied, etc.) which we
    // propagate to the caller.
    // Can't use ops::get_value here — it erases GetError::NotFound, which we need
    // for the creation-diff branch.
    let current_yaml = match client.get::<Value>(&api_path).await {
        Ok(mut current) => {
            // Strip server-managed fields the manifest never carries so the diff
            // shows only meaningful changes.
            strip_server_managed(&mut current);
            // Convert to YAML for diffing
            serde_yaml::to_string(&current)?
        }
        Err(GetError::NotFound) => {
            println!("--- /dev/null");
            println!("+++ {}/{} (create)", kind, name);
            // Round-trip through serde_json::Value so keys sort the same way the
            // live side does (see the changed-resource branch below).
            let mut new_json: serde_json::Value = serde_yaml::from_value(value.clone())?;
            strip_server_managed(&mut new_json);
            let new_yaml = serde_yaml::to_string(&new_json)?;
            for line in new_yaml.lines() {
                println!("+{}", line);
            }
            println!();
            return Ok(());
        }
        Err(GetError::Other(e)) => {
            return Err(e);
        }
    };

    // Prepare new resource YAML. Round-trip the desired object through
    // serde_json::Value so its keys sort identically to the live side (which is
    // a serde_json::Value → sorted via BTreeMap). Without this, manifests whose
    // keys aren't already alphabetical would show spurious diffs every run.
    let mut new_json: serde_json::Value = serde_yaml::from_value(value.clone())?;
    // Strip the same server-managed fields from the desired side so a field that
    // exists only on the live side never renders as a deletion.
    strip_server_managed(&mut new_json);
    let new_yaml = serde_yaml::to_string(&new_json)?;

    // Calculate and display diff
    if current_yaml.trim() == new_yaml.trim() {
        println!("No changes for {}/{}", kind, name);
        println!();
        return Ok(());
    }

    println!("--- {}/{} (current)", kind, name);
    println!("+++ {}/{} (new)", kind, name);

    // Simple line-by-line diff
    let current_lines: Vec<&str> = current_yaml.lines().collect();
    let new_lines: Vec<&str> = new_yaml.lines().collect();

    let max_len = current_lines.len().max(new_lines.len());
    for i in 0..max_len {
        let current_line = current_lines.get(i).copied();
        let new_line = new_lines.get(i).copied();

        match (current_line, new_line) {
            (Some(c), Some(n)) if c == n => println!(" {}", c),
            (Some(c), Some(n)) => {
                println!("-{}", c);
                println!("+{}", n);
            }
            (Some(c), None) => println!("-{}", c),
            (None, Some(n)) => println!("+{}", n),
            (None, None) => unreachable!(),
        }
    }
    println!();

    Ok(())
}

/// Remove server-managed fields that the user's manifest never sets, so `diff`
/// shows only meaningful changes (approximation of kubectl's server-side-apply
/// dry-run diff).
fn strip_server_managed(v: &mut serde_json::Value) {
    if let Some(meta) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        for k in [
            "uid",
            "resourceVersion",
            "generation",
            "creationTimestamp",
            "managedFields",
            "selfLink",
            "ownerReferences",
        ] {
            meta.remove(k);
        }
        if let Some(ann) = meta.get_mut("annotations").and_then(|a| a.as_object_mut()) {
            ann.remove("kubectl.kubernetes.io/last-applied-configuration");
            // drop now-empty annotations to avoid an empty-map diff
            let empty = ann.is_empty();
            if empty {
                meta.remove("annotations");
            }
        }
    }
    if let Some(obj) = v.as_object_mut() {
        obj.remove("status");
    }
}

#[cfg(test)]
mod tests {
    use super::strip_server_managed;
    use crate::discovery::{ResourceMapping, RestMapper};
    use crate::ops::build_path;
    use serde_json::json;

    fn mapping(
        group: &str,
        version: &str,
        kind: &str,
        plural: &str,
        namespaced: bool,
    ) -> ResourceMapping {
        ResourceMapping {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
            plural: plural.into(),
            singular: kind.to_lowercase(),
            namespaced,
            verbs: vec![],
            short_names: vec![],
        }
    }

    fn mapper_with(mappings: Vec<ResourceMapping>) -> RestMapper {
        RestMapper::new(mappings)
    }

    // ---------------------------------------------------------------------------
    // Path-building tests: verify that build_path (used by diff_resource) produces
    // the correct API paths for a variety of resource kinds via the mapper.
    // ---------------------------------------------------------------------------

    #[test]
    fn strip_server_managed_removes_managed_fields_keeps_real_data() {
        let mut v = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "f2-cm",
                "namespace": "default",
                "labels": { "app": "demo" },
                "uid": "abc-123",
                "resourceVersion": "4711",
                "generation": 3,
                "creationTimestamp": "2026-05-29T00:00:00Z",
                "managedFields": [{ "manager": "kubectl" }],
                "selfLink": "/api/v1/namespaces/default/configmaps/f2-cm",
                "ownerReferences": [{ "name": "owner" }],
                "annotations": {
                    "kubectl.kubernetes.io/last-applied-configuration": "{}"
                }
            },
            "data": { "a": "1", "b": "2" },
            "status": { "phase": "Active" }
        });

        strip_server_managed(&mut v);

        let meta = v.get("metadata").unwrap().as_object().unwrap();
        for k in [
            "uid",
            "resourceVersion",
            "generation",
            "creationTimestamp",
            "managedFields",
            "selfLink",
            "ownerReferences",
        ] {
            assert!(!meta.contains_key(k), "expected {k} to be stripped");
        }
        // last-applied-configuration was the only annotation → annotations map dropped
        assert!(!meta.contains_key("annotations"));
        // status is server-populated → stripped
        assert!(v.get("status").is_none());

        // user-meaningful fields survive
        assert_eq!(meta.get("name").unwrap(), "f2-cm");
        assert_eq!(meta.get("namespace").unwrap(), "default");
        assert_eq!(meta.get("labels").unwrap(), &json!({ "app": "demo" }));
        assert_eq!(v.get("data").unwrap(), &json!({ "a": "1", "b": "2" }));
        assert_eq!(v.get("kind").unwrap(), "ConfigMap");
    }

    #[test]
    fn strip_server_managed_keeps_other_annotations() {
        let mut v = json!({
            "metadata": {
                "name": "x",
                "annotations": {
                    "kubectl.kubernetes.io/last-applied-configuration": "{}",
                    "team": "infra"
                }
            }
        });
        strip_server_managed(&mut v);
        let ann = v
            .get("metadata")
            .unwrap()
            .get("annotations")
            .unwrap()
            .as_object()
            .unwrap();
        assert!(!ann.contains_key("kubectl.kubernetes.io/last-applied-configuration"));
        assert_eq!(ann.get("team").unwrap(), "infra");
    }

    #[test]
    fn pod_path_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "Pod", "pods", true)]);
        let pod = m.resolve("Pod").unwrap();
        assert_eq!(
            build_path(pod, Some("default"), Some("nginx")),
            "/api/v1/namespaces/default/pods/nginx"
        );
    }

    #[test]
    fn deployment_path_via_mapper() {
        let m = mapper_with(vec![mapping(
            "apps",
            "v1",
            "Deployment",
            "deployments",
            true,
        )]);
        let dep = m.resolve("Deployment").unwrap();
        assert_eq!(
            build_path(dep, Some("prod"), Some("web")),
            "/apis/apps/v1/namespaces/prod/deployments/web"
        );
    }

    #[test]
    fn persistent_volume_cluster_scoped() {
        let m = mapper_with(vec![mapping(
            "",
            "v1",
            "PersistentVolume",
            "persistentvolumes",
            false,
        )]);
        let pv = m.resolve("PersistentVolume").unwrap();
        // namespace argument is ignored for cluster-scoped resources
        assert_eq!(
            build_path(pv, Some("ignored"), Some("pv-1")),
            "/api/v1/persistentvolumes/pv-1"
        );
    }

    #[test]
    fn crd_path_via_mapper() {
        let m = mapper_with(vec![mapping(
            "apiextensions.k8s.io",
            "v1",
            "CustomResourceDefinition",
            "customresourcedefinitions",
            false,
        )]);
        let crd = m.resolve("CustomResourceDefinition").unwrap();
        assert_eq!(
            build_path(crd, Some("ignored"), Some("foos.example.com")),
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com"
        );
    }

    #[test]
    fn unknown_kind_resolves_to_none() {
        let m = mapper_with(vec![mapping("", "v1", "Pod", "pods", true)]);
        assert!(m.resolve("Unknown").is_none());
    }

    #[test]
    fn service_path_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "Service", "services", true)]);
        let svc = m.resolve("Service").unwrap();
        assert_eq!(
            build_path(svc, Some("prod"), Some("my-svc")),
            "/api/v1/namespaces/prod/services/my-svc"
        );
    }

    #[test]
    fn configmap_path_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "ConfigMap", "configmaps", true)]);
        let cm = m.resolve("ConfigMap").unwrap();
        assert_eq!(
            build_path(cm, Some("default"), Some("cfg")),
            "/api/v1/namespaces/default/configmaps/cfg"
        );
    }

    #[test]
    fn namespace_cluster_scoped_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "Namespace", "namespaces", false)]);
        let ns = m.resolve("Namespace").unwrap();
        assert_eq!(
            build_path(ns, Some("ignored"), Some("kube-system")),
            "/api/v1/namespaces/kube-system"
        );
    }

    #[test]
    fn statefulset_path_via_mapper() {
        let m = mapper_with(vec![mapping(
            "apps",
            "v1",
            "StatefulSet",
            "statefulsets",
            true,
        )]);
        let ss = m.resolve("StatefulSet").unwrap();
        assert_eq!(
            build_path(ss, Some("staging"), Some("mysql")),
            "/apis/apps/v1/namespaces/staging/statefulsets/mysql"
        );
    }

    #[test]
    fn node_cluster_scoped_via_mapper() {
        let m = mapper_with(vec![mapping("", "v1", "Node", "nodes", false)]);
        let node = m.resolve("Node").unwrap();
        assert_eq!(
            build_path(node, Some("ignored"), Some("worker-1")),
            "/api/v1/nodes/worker-1"
        );
    }

    #[test]
    fn rbac_resources_via_mapper() {
        let mappings = vec![
            mapping(
                "rbac.authorization.k8s.io",
                "v1",
                "ClusterRole",
                "clusterroles",
                false,
            ),
            mapping(
                "rbac.authorization.k8s.io",
                "v1",
                "ClusterRoleBinding",
                "clusterrolebindings",
                false,
            ),
            mapping("rbac.authorization.k8s.io", "v1", "Role", "roles", true),
        ];
        let m = mapper_with(mappings);

        let cr = m.resolve("ClusterRole").unwrap();
        assert_eq!(
            build_path(cr, Some("ignored"), Some("admin")),
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin"
        );

        let crb = m.resolve("ClusterRoleBinding").unwrap();
        assert_eq!(
            build_path(crb, Some("ignored"), Some("admin-binding")),
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/admin-binding"
        );

        let role = m.resolve("Role").unwrap();
        assert_eq!(
            build_path(role, Some("default"), Some("pod-reader")),
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/pod-reader"
        );
    }
}
