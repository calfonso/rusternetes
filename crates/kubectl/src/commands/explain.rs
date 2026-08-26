use anyhow::Result;
use std::collections::HashMap;

/// Explain Kubernetes resource documentation
pub async fn execute(resource: &str, api_version: Option<&str>, recursive: bool) -> Result<()> {
    // Parse resource path (e.g., "pod", "pod.spec", "pod.spec.containers")
    let parts: Vec<&str> = resource.split('.').collect();
    let resource_type = parts[0];

    // Hardcoded resource documentation
    let docs = get_resource_docs();

    if let Some(doc) = docs.get(resource_type) {
        println!("KIND:     {}", doc.kind);
        println!("VERSION:  {}", api_version.unwrap_or(&doc.version));
        println!();
        println!("DESCRIPTION:");
        println!("{}", doc.description);
        println!();

        if parts.len() == 1 {
            // Show top-level fields
            println!("FIELDS:");
            for (field_name, field_doc) in &doc.fields {
                println!("   {}   <{}>", field_name, field_doc.type_info);
                if !field_doc.required {
                    println!("      (optional)");
                }
                println!("      {}", field_doc.description);
                println!();
            }
        } else {
            // Show nested field
            let field_path = parts[1..].join(".");
            if let Some(field_doc) = doc.fields.get(field_path.as_str()) {
                println!("FIELD:    {}", field_path);
                println!("TYPE:     {}", field_doc.type_info);
                println!();
                println!("DESCRIPTION:");
                println!("{}", field_doc.description);
            } else {
                anyhow::bail!("Field '{}' not found in {}", field_path, resource_type);
            }
        }

        if recursive && parts.len() == 1 {
            println!(
                "\nNote: Recursive display is limited. Use '{}.<field>' to explore nested fields.",
                resource_type
            );
        }
    } else {
        anyhow::bail!(
            "Resource type '{}' not found. Common types: pod, service, deployment, node",
            resource_type
        );
    }

    Ok(())
}

struct ResourceDoc {
    kind: String,
    version: String,
    description: String,
    fields: HashMap<String, FieldDoc>,
}

struct FieldDoc {
    type_info: String,
    description: String,
    required: bool,
}

fn get_resource_docs() -> HashMap<&'static str, ResourceDoc> {
    let mut docs = HashMap::new();

    // Pod documentation
    let mut pod_fields = HashMap::new();
    pod_fields.insert(
        "apiVersion".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "APIVersion defines the versioned schema of this representation of an object."
                    .to_string(),
            required: false,
        },
    );
    pod_fields.insert(
        "kind".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "Kind is a string value representing the REST resource this object represents."
                    .to_string(),
            required: false,
        },
    );
    pod_fields.insert("metadata".to_string(), FieldDoc {
        type_info: "ObjectMeta".to_string(),
        description: "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata".to_string(),
        required: false,
    });
    pod_fields.insert("spec".to_string(), FieldDoc {
        type_info: "PodSpec".to_string(),
        description: "Specification of the desired behavior of the pod. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status".to_string(),
        required: false,
    });
    pod_fields.insert("status".to_string(), FieldDoc {
        type_info: "PodStatus".to_string(),
        description: "Most recently observed status of the pod. This data may not be up to date. Populated by the system. Read-only.".to_string(),
        required: false,
    });

    docs.insert("pod", ResourceDoc {
        kind: "Pod".to_string(),
        version: "v1".to_string(),
        description: "Pod is a collection of containers that can run on a host. This resource is created by clients and scheduled onto hosts.".to_string(),
        fields: pod_fields,
    });

    // Service documentation
    let mut service_fields = HashMap::new();
    service_fields.insert(
        "apiVersion".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "APIVersion defines the versioned schema of this representation of an object."
                    .to_string(),
            required: false,
        },
    );
    service_fields.insert(
        "kind".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "Kind is a string value representing the REST resource this object represents."
                    .to_string(),
            required: false,
        },
    );
    service_fields.insert(
        "metadata".to_string(),
        FieldDoc {
            type_info: "ObjectMeta".to_string(),
            description: "Standard object's metadata.".to_string(),
            required: false,
        },
    );
    service_fields.insert(
        "spec".to_string(),
        FieldDoc {
            type_info: "ServiceSpec".to_string(),
            description: "Spec defines the behavior of a service.".to_string(),
            required: false,
        },
    );
    service_fields.insert(
        "status".to_string(),
        FieldDoc {
            type_info: "ServiceStatus".to_string(),
            description: "Most recently observed status of the service.".to_string(),
            required: false,
        },
    );

    docs.insert("service", ResourceDoc {
        kind: "Service".to_string(),
        version: "v1".to_string(),
        description: "Service is a named abstraction of software service (for example, mysql) consisting of local port (for example 3306) that the proxy listens on, and the selector that determines which pods will answer requests sent through the proxy.".to_string(),
        fields: service_fields,
    });

    // Deployment documentation
    let mut deployment_fields = HashMap::new();
    deployment_fields.insert(
        "apiVersion".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "APIVersion defines the versioned schema of this representation of an object."
                    .to_string(),
            required: false,
        },
    );
    deployment_fields.insert(
        "kind".to_string(),
        FieldDoc {
            type_info: "string".to_string(),
            description:
                "Kind is a string value representing the REST resource this object represents."
                    .to_string(),
            required: false,
        },
    );
    deployment_fields.insert(
        "metadata".to_string(),
        FieldDoc {
            type_info: "ObjectMeta".to_string(),
            description: "Standard object metadata.".to_string(),
            required: false,
        },
    );
    deployment_fields.insert(
        "spec".to_string(),
        FieldDoc {
            type_info: "DeploymentSpec".to_string(),
            description: "Specification of the desired behavior of the Deployment.".to_string(),
            required: false,
        },
    );
    deployment_fields.insert(
        "status".to_string(),
        FieldDoc {
            type_info: "DeploymentStatus".to_string(),
            description: "Most recently observed status of the Deployment.".to_string(),
            required: false,
        },
    );

    docs.insert(
        "deployment",
        ResourceDoc {
            kind: "Deployment".to_string(),
            version: "apps/v1".to_string(),
            description: "Deployment enables declarative updates for Pods and ReplicaSets."
                .to_string(),
            fields: deployment_fields,
        },
    );

    docs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_resource_docs_has_pod() {
        let docs = get_resource_docs();
        assert!(docs.contains_key("pod"));
        let pod_doc = &docs["pod"];
        assert_eq!(pod_doc.kind, "Pod");
        assert_eq!(pod_doc.version, "v1");
    }

    #[test]
    fn test_get_resource_docs_has_service() {
        let docs = get_resource_docs();
        assert!(docs.contains_key("service"));
        assert_eq!(docs["service"].kind, "Service");
    }

    #[test]
    fn test_get_resource_docs_has_deployment() {
        let docs = get_resource_docs();
        assert!(docs.contains_key("deployment"));
        assert_eq!(docs["deployment"].version, "apps/v1");
    }

    #[test]
    fn test_pod_fields_contain_spec() {
        let docs = get_resource_docs();
        let pod_doc = &docs["pod"];
        assert!(pod_doc.fields.contains_key("spec"));
        assert_eq!(pod_doc.fields["spec"].type_info, "PodSpec");
    }

    #[tokio::test]
    async fn test_execute_unknown_resource_fails() {
        let result = execute("nonexistent", None, false).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_get_resource_docs_has_standard_fields() {
        let docs = get_resource_docs();
        for doc in docs.values() {
            assert!(doc.fields.contains_key("apiVersion"));
            assert!(doc.fields.contains_key("kind"));
            assert!(doc.fields.contains_key("metadata"));
        }
    }

    #[tokio::test]
    async fn test_execute_pod_succeeds() {
        let result = execute("pod", None, false).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_pod_field_path() {
        let result = execute("pod.spec", None, false).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_unknown_field_path_fails() {
        let result = execute("pod.nonexistentfield", None, false).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_resource_doc_fields_have_descriptions() {
        let docs = get_resource_docs();
        for doc in docs.values() {
            assert!(!doc.description.is_empty());
            for field_doc in doc.fields.values() {
                assert!(!field_doc.description.is_empty());
                assert!(!field_doc.type_info.is_empty());
            }
        }
    }
}
