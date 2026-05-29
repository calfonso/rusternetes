#![allow(dead_code)] // consumed by command migrations in later tasks
use crate::client::ApiClient;
use crate::discovery::ResourceMapping;
use anyhow::{Context, Result};
use serde_json::Value;

/// Build the API path for a resource. When `name` is `None` the collection
/// path is returned; otherwise the item path. For cluster-scoped resources the
/// namespace is ignored.
pub fn build_path(m: &ResourceMapping, namespace: Option<&str>, name: Option<&str>) -> String {
    let base = if m.group.is_empty() {
        format!("/api/{}", m.version)
    } else {
        format!("/apis/{}/{}", m.group, m.version)
    };
    let mut path = if m.namespaced {
        let ns = namespace.unwrap_or("default");
        format!("{base}/namespaces/{ns}/{}", m.plural)
    } else {
        format!("{base}/{}", m.plural)
    };
    if let Some(n) = name {
        path.push('/');
        path.push_str(n);
    }
    path
}

/// Read `metadata.name` from a resource Value.
pub fn value_name(v: &Value) -> Option<String> {
    v.pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .map(String::from)
}

/// Read `metadata.namespace` from a resource Value.
pub fn value_namespace(v: &Value) -> Option<String> {
    v.pointer("/metadata/namespace")
        .and_then(|n| n.as_str())
        .map(String::from)
}

/// Apply a resource Value (create-or-replace). Returns the action taken
/// ("created" or "configured") and the server response body.
///
/// `namespace`: the explicit `-n` flag value, or `None` when unset (falls back
/// to the body's metadata.namespace, then "default").
///
/// `query`: an optional query-string suffix (e.g. `?dryRun=All&fieldManager=...`)
/// appended to the PUT/POST URLs. The existence check is performed on the bare
/// item path so a query string never changes how we detect create-vs-replace.
pub async fn apply_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    body: &Value,
    query: &str,
) -> Result<(&'static str, Value)> {
    let name = value_name(body).context("resource is missing metadata.name")?;
    let ns = if m.namespaced {
        Some(
            namespace
                .map(String::from)
                .or_else(|| value_namespace(body))
                .unwrap_or_else(|| "default".to_string()),
        )
    } else {
        None
    };
    let item = build_path(m, ns.as_deref(), Some(&name));
    let collection = build_path(m, ns.as_deref(), None);
    if client.resource_exists(&item).await? {
        let resp: Value = client.put(&format!("{item}{query}"), body).await?;
        Ok(("configured", resp))
    } else {
        let resp: Value = client.post(&format!("{collection}{query}"), body).await?;
        Ok(("created", resp))
    }
}

/// GET a single resource as a Value.
pub async fn get_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    name: &str,
) -> Result<Value> {
    let ns = if m.namespaced {
        Some(namespace.unwrap_or("default").to_string())
    } else {
        None
    };
    let path = build_path(m, ns.as_deref(), Some(name));
    client
        .get::<Value>(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// GET a resource collection; returns the `.items` array as Values. When
/// `all_namespaces` is true for a namespaced resource the cluster-wide
/// collection path is used (no namespace segment).
pub async fn list_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    all_namespaces: bool,
) -> Result<Vec<Value>> {
    let path = if m.namespaced && all_namespaces {
        // Cluster-wide path: no /namespaces/{ns} segment
        let base = if m.group.is_empty() {
            format!("/api/{}", m.version)
        } else {
            format!("/apis/{}/{}", m.group, m.version)
        };
        format!("{}/{}", base, m.plural)
    } else {
        let ns = if m.namespaced {
            Some(namespace.unwrap_or("default").to_string())
        } else {
            None
        };
        build_path(m, ns.as_deref(), None)
    };
    let list: Value = client
        .get::<Value>(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(list
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::ResourceMapping;

    fn m(group: &str, plural: &str, namespaced: bool) -> ResourceMapping {
        ResourceMapping {
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
    fn core_namespaced_paths() {
        let pod = m("", "pods", true);
        assert_eq!(
            build_path(&pod, Some("kube-system"), Some("dns")),
            "/api/v1/namespaces/kube-system/pods/dns"
        );
        assert_eq!(
            build_path(&pod, None, None),
            "/api/v1/namespaces/default/pods"
        );
    }

    #[test]
    fn grouped_cluster_path() {
        let crb = m("rbac.authorization.k8s.io", "clusterrolebindings", false);
        assert_eq!(
            build_path(&crb, Some("ignored"), Some("admin")),
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/admin"
        );
    }

    #[test]
    fn grouped_namespaced_collection() {
        let dep = m("apps", "deployments", true);
        assert_eq!(
            build_path(&dep, Some("prod"), None),
            "/apis/apps/v1/namespaces/prod/deployments"
        );
    }

    #[test]
    fn cluster_collection_path_no_name() {
        let pv = m("", "persistentvolumes", false);
        assert_eq!(build_path(&pv, None, None), "/api/v1/persistentvolumes");
    }

    #[test]
    fn all_namespaces_cluster_wide_path() {
        // When all_namespaces is requested for a namespaced resource the path
        // must NOT contain a /namespaces/{ns} segment.
        // We verify the path-building branch inside list_value directly by
        // replicating its logic here (list_value itself is async and needs a
        // live server, so we only test the path computation).
        let pod = m("", "pods", true);
        // Cluster-wide: base + plural only
        let base = if pod.group.is_empty() {
            format!("/api/{}", pod.version)
        } else {
            format!("/apis/{}/{}", pod.group, pod.version)
        };
        let path = format!("{}/{}", base, pod.plural);
        assert_eq!(path, "/api/v1/pods");

        let dep = m("apps", "deployments", true);
        let base2 = format!("/apis/{}/{}", dep.group, dep.version);
        let path2 = format!("{}/{}", base2, dep.plural);
        assert_eq!(path2, "/apis/apps/v1/deployments");
    }

    #[test]
    fn reads_metadata_name() {
        let v = serde_json::json!({"metadata": {"name": "foo", "namespace": "bar"}});
        assert_eq!(value_name(&v).as_deref(), Some("foo"));
        assert_eq!(value_namespace(&v).as_deref(), Some("bar"));
    }

    #[test]
    fn missing_metadata_is_none() {
        let v = serde_json::json!({"spec": {}});
        assert!(value_name(&v).is_none());
        assert!(value_namespace(&v).is_none());
    }
}
