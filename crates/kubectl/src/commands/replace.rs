use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde::Deserialize;
use std::fs;
use std::io::{self, Read};

/// Compute the API path for a given resource kind, apiVersion, namespace, and name.
/// Returns the full path including the resource name (for PUT requests).
fn resource_path(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> Result<String> {
    let (base, resource) = match kind {
        // Core v1 namespaced resources
        "Pod" => ("/api/v1", "pods"),
        "Service" => ("/api/v1", "services"),
        "ConfigMap" => ("/api/v1", "configmaps"),
        "Secret" => ("/api/v1", "secrets"),
        "ServiceAccount" => ("/api/v1", "serviceaccounts"),
        "Endpoints" => ("/api/v1", "endpoints"),
        "PersistentVolumeClaim" => ("/api/v1", "persistentvolumeclaims"),
        "ResourceQuota" => ("/api/v1", "resourcequotas"),
        "LimitRange" => ("/api/v1", "limitranges"),
        "ReplicationController" => ("/api/v1", "replicationcontrollers"),
        "Event" => ("/api/v1", "events"),

        // Core v1 cluster-scoped resources
        "Namespace" => {
            return Ok(format!("/api/v1/namespaces/{}", name));
        }
        "Node" => {
            return Ok(format!("/api/v1/nodes/{}", name));
        }
        "PersistentVolume" => {
            return Ok(format!("/api/v1/persistentvolumes/{}", name));
        }

        // apps/v1
        "Deployment" => ("/apis/apps/v1", "deployments"),
        "StatefulSet" => ("/apis/apps/v1", "statefulsets"),
        "DaemonSet" => ("/apis/apps/v1", "daemonsets"),
        "ReplicaSet" => ("/apis/apps/v1", "replicasets"),

        // batch/v1
        "Job" => ("/apis/batch/v1", "jobs"),
        "CronJob" => ("/apis/batch/v1", "cronjobs"),

        // networking.k8s.io/v1
        "Ingress" => ("/apis/networking.k8s.io/v1", "ingresses"),
        "NetworkPolicy" => ("/apis/networking.k8s.io/v1", "networkpolicies"),

        // rbac.authorization.k8s.io/v1 namespaced
        "Role" => ("/apis/rbac.authorization.k8s.io/v1", "roles"),
        "RoleBinding" => ("/apis/rbac.authorization.k8s.io/v1", "rolebindings"),

        // rbac.authorization.k8s.io/v1 cluster-scoped
        "ClusterRole" => {
            return Ok(format!(
                "/apis/rbac.authorization.k8s.io/v1/clusterroles/{}",
                name
            ));
        }
        "ClusterRoleBinding" => {
            return Ok(format!(
                "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{}",
                name
            ));
        }

        // storage.k8s.io/v1
        "StorageClass" => {
            return Ok(format!("/apis/storage.k8s.io/v1/storageclasses/{}", name));
        }

        // scheduling.k8s.io/v1
        "PriorityClass" => {
            return Ok(format!(
                "/apis/scheduling.k8s.io/v1/priorityclasses/{}",
                name
            ));
        }

        // apiextensions.k8s.io/v1
        "CustomResourceDefinition" => {
            return Ok(format!(
                "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{}",
                name
            ));
        }

        // snapshot.storage.k8s.io/v1
        "VolumeSnapshot" => ("/apis/snapshot.storage.k8s.io/v1", "volumesnapshots"),
        "VolumeSnapshotClass" => {
            return Ok(format!(
                "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses/{}",
                name
            ));
        }

        // autoscaling/v1 or v2
        "HorizontalPodAutoscaler" => {
            let base = format!("/apis/{}", api_version);
            let ns = namespace.unwrap_or("default");
            return Ok(format!(
                "{}/namespaces/{}/horizontalpodautoscalers/{}",
                base, ns, name
            ));
        }

        // policy/v1
        "PodDisruptionBudget" => ("/apis/policy/v1", "poddisruptionbudgets"),

        // For unknown kinds, try to construct the path from apiVersion
        _ => {
            // If apiVersion contains a slash (e.g., "apps/v1"), it's an API group
            if api_version.contains('/') {
                let base = format!("/apis/{}", api_version);
                let plural = kind_to_plural(kind);
                if let Some(ns) = namespace {
                    return Ok(format!("{}/namespaces/{}/{}/{}", base, ns, plural, name));
                } else {
                    return Ok(format!("{}/{}/{}", base, plural, name));
                }
            } else {
                let plural = kind_to_plural(kind);
                if let Some(ns) = namespace {
                    return Ok(format!("/api/v1/namespaces/{}/{}/{}", ns, plural, name));
                } else {
                    return Ok(format!("/api/v1/{}/{}", plural, name));
                }
            }
        }
    };

    // Namespaced resource
    let ns = namespace.unwrap_or("default");
    Ok(format!("{}/namespaces/{}/{}/{}", base, ns, resource, name))
}

/// Simple heuristic to pluralize a Kind name.
fn kind_to_plural(kind: &str) -> String {
    let lower = kind.to_lowercase();
    if lower.ends_with("ss")
        || lower.ends_with("ch")
        || lower.ends_with("sh")
        || lower.ends_with('x')
    {
        format!("{}es", lower)
    } else if lower.ends_with('s') {
        lower
    } else if lower.ends_with('y')
        && !lower.ends_with("ey")
        && !lower.ends_with("ay")
        && !lower.ends_with("oy")
    {
        format!("{}ies", &lower[..lower.len() - 1])
    } else {
        format!("{}s", lower)
    }
}

/// Kind to lowercase resource name for output (e.g., "Deployment" -> "deployment.apps")
fn kind_to_output_name(kind: &str, api_version: &str) -> String {
    let lower = kind.to_lowercase();
    if api_version == "v1" || api_version.is_empty() {
        lower
    } else if let Some(group) = api_version.split('/').next() {
        if group == "v1" || group == api_version {
            lower
        } else {
            format!("{}.{}", lower, group)
        }
    } else {
        lower
    }
}

pub async fn execute(client: &ApiClient, file: &str) -> Result<()> {
    let contents = if file == "-" {
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .context("Failed to read from stdin")?;
        buffer
    } else {
        fs::read_to_string(file).context("Failed to read file")?
    };

    // Support multi-document YAML files
    for document in serde_yaml::Deserializer::from_str(&contents) {
        let value = serde_yaml::Value::deserialize(document)?;

        if value.is_null() {
            continue;
        }

        replace_resource(client, &value).await?;
    }

    Ok(())
}

async fn replace_resource(client: &ApiClient, value: &serde_yaml::Value) -> Result<()> {
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field in resource")?;

    let api_version = value
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .context("Missing 'apiVersion' field in resource")?;

    let metadata = value
        .get("metadata")
        .context("Missing 'metadata' field in resource")?;

    let name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .context("Missing 'metadata.name' field in resource")?;

    let namespace = metadata.get("namespace").and_then(|n| n.as_str());

    let path = resource_path(api_version, kind, namespace, name)?;

    // Convert to JSON for the PUT request
    let json_value: serde_json::Value = serde_yaml::from_value(value.clone())?;

    let _result: serde_json::Value = client.put(&path, &json_value).await?;

    let output_name = kind_to_output_name(kind, api_version);
    println!("{}/{} replaced", output_name, name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_path_core_namespaced() {
        let path = resource_path("v1", "Pod", Some("default"), "nginx").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");

        let path = resource_path("v1", "Service", Some("kube-system"), "coredns").unwrap();
        assert_eq!(path, "/api/v1/namespaces/kube-system/services/coredns");

        let path = resource_path("v1", "ConfigMap", Some("default"), "my-config").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/configmaps/my-config");

        let path = resource_path("v1", "Secret", Some("default"), "my-secret").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/secrets/my-secret");

        let path = resource_path("v1", "ServiceAccount", Some("default"), "my-sa").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/serviceaccounts/my-sa");
    }

    #[test]
    fn test_resource_path_core_cluster_scoped() {
        let path = resource_path("v1", "Namespace", None, "my-ns").unwrap();
        assert_eq!(path, "/api/v1/namespaces/my-ns");

        let path = resource_path("v1", "Node", None, "node-1").unwrap();
        assert_eq!(path, "/api/v1/nodes/node-1");

        let path = resource_path("v1", "PersistentVolume", None, "pv-1").unwrap();
        assert_eq!(path, "/api/v1/persistentvolumes/pv-1");
    }

    #[test]
    fn test_resource_path_apps_v1() {
        let path = resource_path("apps/v1", "Deployment", Some("default"), "nginx").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/deployments/nginx");

        let path = resource_path("apps/v1", "StatefulSet", Some("default"), "redis").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/statefulsets/redis");

        let path = resource_path("apps/v1", "DaemonSet", Some("kube-system"), "proxy").unwrap();
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/kube-system/daemonsets/proxy"
        );
    }

    #[test]
    fn test_resource_path_batch_v1() {
        let path = resource_path("batch/v1", "Job", Some("default"), "my-job").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/jobs/my-job");

        let path = resource_path("batch/v1", "CronJob", Some("default"), "my-cron").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/cronjobs/my-cron");
    }

    #[test]
    fn test_resource_path_rbac() {
        let path = resource_path(
            "rbac.authorization.k8s.io/v1",
            "Role",
            Some("default"),
            "my-role",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/my-role"
        );

        let path =
            resource_path("rbac.authorization.k8s.io/v1", "ClusterRole", None, "admin").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin"
        );

        let path = resource_path(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "admin-binding",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/admin-binding"
        );
    }

    #[test]
    fn test_resource_path_cluster_scoped_special() {
        let path = resource_path("storage.k8s.io/v1", "StorageClass", None, "standard").unwrap();
        assert_eq!(path, "/apis/storage.k8s.io/v1/storageclasses/standard");

        let path = resource_path(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            "high-priority",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/scheduling.k8s.io/v1/priorityclasses/high-priority"
        );

        let path = resource_path(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "foos.example.com",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com"
        );
    }

    #[test]
    fn test_resource_path_default_namespace() {
        // When namespace is None for a namespaced resource, defaults to "default"
        let path = resource_path("v1", "Pod", None, "nginx").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");
    }

    #[test]
    fn test_resource_path_networking() {
        let path = resource_path(
            "networking.k8s.io/v1",
            "Ingress",
            Some("default"),
            "my-ingress",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/networking.k8s.io/v1/namespaces/default/ingresses/my-ingress"
        );

        let path = resource_path(
            "networking.k8s.io/v1",
            "NetworkPolicy",
            Some("default"),
            "deny-all",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies/deny-all"
        );
    }

    #[test]
    fn test_resource_path_unknown_kind_with_group() {
        // Unknown kind with API group should still construct a reasonable path
        let path = resource_path(
            "custom.example.com/v1",
            "Widget",
            Some("default"),
            "my-widget",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/custom.example.com/v1/namespaces/default/widgets/my-widget"
        );
    }

    #[test]
    fn test_kind_to_plural() {
        assert_eq!(kind_to_plural("Pod"), "pods");
        assert_eq!(kind_to_plural("Service"), "services");
        assert_eq!(kind_to_plural("Ingress"), "ingresses");
        assert_eq!(kind_to_plural("NetworkPolicy"), "networkpolicies");
        assert_eq!(kind_to_plural("Endpoints"), "endpoints");
    }

    #[test]
    fn test_kind_to_output_name() {
        assert_eq!(kind_to_output_name("Pod", "v1"), "pod");
        assert_eq!(
            kind_to_output_name("Deployment", "apps/v1"),
            "deployment.apps"
        );
        assert_eq!(
            kind_to_output_name("ClusterRole", "rbac.authorization.k8s.io/v1"),
            "clusterrole.rbac.authorization.k8s.io"
        );
        assert_eq!(kind_to_output_name("Service", "v1"), "service");
    }

    #[test]
    fn test_resource_path_pvc() {
        let path = resource_path("v1", "PersistentVolumeClaim", Some("default"), "my-pvc").unwrap();
        assert_eq!(
            path,
            "/api/v1/namespaces/default/persistentvolumeclaims/my-pvc"
        );
    }

    #[test]
    fn test_resource_path_policy() {
        let path = resource_path(
            "policy/v1",
            "PodDisruptionBudget",
            Some("default"),
            "my-pdb",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets/my-pdb"
        );
    }

    #[test]
    fn test_resource_path_hpa() {
        let path = resource_path(
            "autoscaling/v2",
            "HorizontalPodAutoscaler",
            Some("prod"),
            "web-hpa",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/autoscaling/v2/namespaces/prod/horizontalpodautoscalers/web-hpa"
        );
    }

    #[test]
    fn test_resource_path_volume_snapshot() {
        let path = resource_path(
            "snapshot.storage.k8s.io/v1",
            "VolumeSnapshot",
            Some("default"),
            "snap-1",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/snapshot.storage.k8s.io/v1/namespaces/default/volumesnapshots/snap-1"
        );
    }

    #[test]
    fn test_resource_path_volume_snapshot_class() {
        let path = resource_path(
            "snapshot.storage.k8s.io/v1",
            "VolumeSnapshotClass",
            None,
            "csi-snap",
        )
        .unwrap();
        assert_eq!(
            path,
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses/csi-snap"
        );
    }

    #[test]
    fn test_resource_path_unknown_kind_no_group() {
        let path = resource_path("v1", "Widget", Some("default"), "w1").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/widgets/w1");
    }

    #[test]
    fn test_resource_path_unknown_cluster_scoped_no_group() {
        let path = resource_path("v1", "Widget", None, "w1").unwrap();
        assert_eq!(path, "/api/v1/widgets/w1");
    }

    #[test]
    fn test_kind_to_plural_special_endings() {
        // Ends with "ch" -> add "es"
        assert_eq!(kind_to_plural("Batch"), "batches");
        // Ends with "sh" -> add "es"
        assert_eq!(kind_to_plural("Mesh"), "meshes");
        // Ends with "x" -> add "es"
        assert_eq!(kind_to_plural("Box"), "boxes");
        // Ends with "ss" -> add "es"
        assert_eq!(kind_to_plural("Class"), "classes");
        // Ends with "y" but not "ey"/"ay"/"oy" -> "ies"
        assert_eq!(kind_to_plural("Policy"), "policies");
    }

    #[test]
    fn test_kind_to_plural_y_exceptions() {
        // "ey" ending -> just add "s"
        assert_eq!(kind_to_plural("Key"), "keys");
        // "ay" ending -> just add "s"
        assert_eq!(kind_to_plural("Gateway"), "gateways");
        // "oy" ending -> just add "s"
        assert_eq!(kind_to_plural("Decoy"), "decoys");
    }

    #[test]
    fn test_kind_to_output_name_empty_api_version() {
        assert_eq!(kind_to_output_name("Pod", ""), "pod");
    }

    #[test]
    fn test_resource_path_remaining_core_types() {
        let path = resource_path("v1", "Endpoints", Some("default"), "my-ep").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/endpoints/my-ep");

        let path = resource_path("v1", "ResourceQuota", Some("default"), "rq").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/resourcequotas/rq");

        let path = resource_path("v1", "LimitRange", Some("default"), "lr").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/limitranges/lr");

        let path = resource_path("v1", "ReplicationController", Some("default"), "rc").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/replicationcontrollers/rc");

        let path = resource_path("v1", "Event", Some("default"), "ev").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/events/ev");
    }

    #[test]
    fn test_resource_path_unknown_cluster_scoped_with_group() {
        let path = resource_path("custom.example.com/v1", "Widget", None, "w1").unwrap();
        assert_eq!(path, "/apis/custom.example.com/v1/widgets/w1");
    }
}
