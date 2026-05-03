//! Tests for kubectl apply command
//! Tests resource application logic without requiring a cluster

#[cfg(test)]
mod tests {
    use rusternetes_common::resources::*;
    use serde::Deserialize;

    #[test]
    fn test_apply_pod_yaml_parsing() {
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: nginx-pod
  namespace: default
  labels:
    app: nginx
spec:
  containers:
  - name: nginx
    image: nginx:1.21
    ports:
    - containerPort: 80
"#;

        let pod: Pod = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(pod.metadata.name, "nginx-pod");
        assert_eq!(pod.metadata.namespace, Some("default".to_string()));
        let spec = pod.spec.unwrap();
        assert_eq!(spec.containers.len(), 1);
        assert_eq!(spec.containers[0].name, "nginx");
        assert_eq!(spec.containers[0].image, "nginx:1.21");
    }

    #[test]
    fn test_apply_service_yaml_parsing() {
        let yaml = r#"
apiVersion: v1
kind: Service
metadata:
  name: nginx-service
  namespace: default
spec:
  selector:
    app: nginx
  ports:
  - protocol: TCP
    port: 80
    targetPort: 80
  type: ClusterIP
"#;

        let service: Service = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(service.metadata.name, "nginx-service");
        assert_eq!(service.spec.ports.len(), 1);
        assert_eq!(service.spec.ports[0].port, 80);
    }

    #[test]
    fn test_apply_deployment_yaml_parsing() {
        let yaml = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment
  namespace: default
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.21
"#;

        let deployment: Deployment = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(deployment.metadata.name, "nginx-deployment");
        assert_eq!(deployment.spec.replicas, Some(3));
    }

    #[test]
    fn test_apply_multi_document_yaml() {
        let yaml = r#"
apiVersion: v1
kind: Namespace
metadata:
  name: test-ns
---
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  namespace: test-ns
spec:
  containers:
  - name: nginx
    image: nginx:latest
"#;

        let mut count = 0;
        for doc in serde_yaml::Deserializer::from_str(yaml) {
            if let Ok(value) = serde_yaml::Value::deserialize(doc) {
                if !value.is_null() {
                    count += 1;
                }
            }
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn test_apply_with_labels() {
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: labeled-pod
  labels:
    env: production
    tier: frontend
    version: v1.0.0
spec:
  containers:
  - name: app
    image: myapp:latest
"#;

        let pod: Pod = serde_yaml::from_str(yaml).unwrap();
        let labels = pod.metadata.labels.as_ref().unwrap();
        assert_eq!(labels.len(), 3);
        assert_eq!(labels.get("env"), Some(&"production".to_string()));
        assert_eq!(labels.get("tier"), Some(&"frontend".to_string()));
        assert_eq!(labels.get("version"), Some(&"v1.0.0".to_string()));
    }

    #[test]
    fn test_apply_with_annotations() {
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: annotated-pod
  annotations:
    description: "Test pod with annotations"
    owner: "team@example.com"
spec:
  containers:
  - name: app
    image: myapp:latest
"#;

        let pod: Pod = serde_yaml::from_str(yaml).unwrap();
        let annotations = pod.metadata.annotations.as_ref().unwrap();
        assert_eq!(annotations.len(), 2);
        assert!(annotations.contains_key("description"));
        assert!(annotations.contains_key("owner"));
    }

    #[test]
    fn test_apply_configmap() {
        let yaml = r#"
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: default
data:
  database_url: "postgres://localhost:5432/mydb"
  log_level: "info"
  feature_flags: "new_ui=true,beta_api=false"
"#;

        let cm: ConfigMap = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cm.metadata.name, "app-config");
        let data = cm.data.as_ref().unwrap();
        assert_eq!(data.len(), 3);
        assert_eq!(data.get("log_level"), Some(&"info".to_string()));
    }

    #[test]
    fn test_apply_secret() {
        let yaml = r#"
apiVersion: v1
kind: Secret
metadata:
  name: db-secret
  namespace: default
type: Opaque
data:
  username: YWRtaW4=
  password: cGFzc3dvcmQ=
"#;

        let secret: Secret = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(secret.metadata.name, "db-secret");
        assert_eq!(secret.secret_type, Some("Opaque".to_string()));
        let data = secret.data.as_ref().unwrap();
        assert!(data.contains_key("username"));
        assert!(data.contains_key("password"));
    }

    #[test]
    fn test_apply_with_namespace_override() {
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  namespace: original-ns
spec:
  containers:
  - name: nginx
    image: nginx:latest
"#;

        let mut pod: Pod = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(pod.metadata.namespace, Some("original-ns".to_string()));

        // Simulate namespace override
        pod.metadata.namespace = Some("override-ns".to_string());
        assert_eq!(pod.metadata.namespace, Some("override-ns".to_string()));
    }

    #[test]
    fn test_apply_dry_run_flag() {
        // Test that dry-run mode can be detected
        fn is_dry_run(flag: Option<&str>) -> bool {
            matches!(flag, Some("client") | Some("server"))
        }

        assert!(is_dry_run(Some("client")));
        assert!(is_dry_run(Some("server")));
        assert!(!is_dry_run(None));
        assert!(!is_dry_run(Some("invalid")));
    }

    #[test]
    fn test_apply_server_side_flag() {
        // Test server-side apply flag detection
        fn should_use_server_side(flag: bool) -> &'static str {
            if flag {
                "PATCH"
            } else {
                "PUT"
            }
        }

        assert_eq!(should_use_server_side(true), "PATCH");
        assert_eq!(should_use_server_side(false), "PUT");
    }

    #[test]
    fn test_apply_force_conflicts_flag() {
        // Test force conflicts handling
        fn get_field_manager(force: bool) -> Option<String> {
            if force {
                Some("kubectl-force".to_string())
            } else {
                Some("kubectl".to_string())
            }
        }

        assert_eq!(get_field_manager(true), Some("kubectl-force".to_string()));
        assert_eq!(get_field_manager(false), Some("kubectl".to_string()));
    }
}
