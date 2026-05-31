use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// ControllerRevision represents an immutable snapshot of state data
/// Used by controllers like StatefulSet and DaemonSet to track rollback history
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerRevision {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    // ObjectMeta is `omitempty` in upstream Kubernetes and the apiserver fills in
    // defaults, so tolerate a body that omits it rather than rejecting with a 422.
    // `#[serde(default)]` also guards against the serde-flatten quirk where a
    // failure in a sibling field gets misreported as `missing field metadata`.
    #[serde(default)]
    pub metadata: ObjectMeta,

    /// Data is the serialized representation of the state
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,

    /// Revision indicates the revision of the state represented by Data
    #[serde(default)]
    pub revision: i64,
}

impl ControllerRevision {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>, revision: i64) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ControllerRevision".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            data: None,
            revision,
        }
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_controller_revision_creation() {
        let revision_data = json!({
            "spec": {
                "replicas": 3,
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "nginx"
                        }
                    }
                }
            }
        });

        let cr = ControllerRevision::new("nginx-revision-1", "default", 1).with_data(revision_data);

        assert_eq!(cr.metadata.name, "nginx-revision-1");
        assert_eq!(cr.metadata.namespace, Some("default".to_string()));
        assert_eq!(cr.type_meta.kind, "ControllerRevision");
        assert_eq!(cr.type_meta.api_version, "apps/v1");
        assert_eq!(cr.revision, 1);
        assert!(cr.data.is_some());
    }

    #[test]
    fn test_controller_revision_without_data() {
        let cr = ControllerRevision::new("test-revision", "default", 5);

        assert_eq!(cr.revision, 5);
        assert!(cr.data.is_none());
    }

    #[test]
    fn test_controller_revision_serialization_camel_case() {
        let cr = ControllerRevision::new("my-rev", "default", 3).with_data(json!({"key": "value"}));

        let json_str = serde_json::to_string(&cr).expect("serialize");

        // TypeMeta fields should be present (flattened)
        assert!(json_str.contains(r#""kind":"ControllerRevision""#));
        assert!(json_str.contains(r#""apiVersion":"apps/v1""#));

        // revision field should be camelCase (it already is lowercase, but verify presence)
        assert!(json_str.contains(r#""revision":3"#));

        // data should be present
        assert!(json_str.contains(r#""data":{"key":"value"}"#));
    }

    #[test]
    fn test_controller_revision_data_omitted_when_none() {
        let cr = ControllerRevision::new("no-data-rev", "default", 1);
        let json_str = serde_json::to_string(&cr).expect("serialize");

        // skip_serializing_if means data should not appear
        assert!(
            !json_str.contains("\"data\""),
            "data field should be omitted when None"
        );
    }

    #[test]
    fn test_controller_revision_roundtrip_serialization() {
        let data = json!({
            "spec": {
                "containers": [
                    {"name": "nginx", "image": "nginx:1.21"}
                ]
            }
        });

        let original =
            ControllerRevision::new("roundtrip-rev", "kube-system", 7).with_data(data.clone());

        let json_str = serde_json::to_string(&original).expect("serialize");
        let restored: ControllerRevision = serde_json::from_str(&json_str).expect("deserialize");

        assert_eq!(restored.type_meta.kind, "ControllerRevision");
        assert_eq!(restored.type_meta.api_version, "apps/v1");
        assert_eq!(restored.metadata.name, "roundtrip-rev");
        assert_eq!(restored.metadata.namespace, Some("kube-system".to_string()));
        assert_eq!(restored.revision, 7);
        assert_eq!(restored.data, Some(data));
    }

    #[test]
    fn test_controller_revision_deserialization_from_json() {
        let json_str = r#"{
            "kind": "ControllerRevision",
            "apiVersion": "apps/v1",
            "metadata": {
                "name": "from-json-rev",
                "namespace": "production"
            },
            "revision": 42,
            "data": {"template": {"spec": {}}}
        }"#;

        let cr: ControllerRevision =
            serde_json::from_str(json_str).expect("deserialize from raw JSON");

        assert_eq!(cr.type_meta.kind, "ControllerRevision");
        assert_eq!(cr.type_meta.api_version, "apps/v1");
        assert_eq!(cr.metadata.name, "from-json-rev");
        assert_eq!(cr.metadata.namespace, Some("production".to_string()));
        assert_eq!(cr.revision, 42);
        assert!(cr.data.is_some());
    }

    #[test]
    fn test_controller_revision_with_owner_references() {
        use crate::types::OwnerReference;

        let mut cr = ControllerRevision::new("owned-rev", "default", 1);
        cr.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "DaemonSet".to_string(),
            name: "my-daemonset".to_string(),
            uid: "abc-123".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);

        let json_str = serde_json::to_string(&cr).expect("serialize");
        let restored: ControllerRevision = serde_json::from_str(&json_str).expect("deserialize");

        let refs = restored.metadata.owner_references.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].kind, "DaemonSet");
        assert_eq!(refs[0].name, "my-daemonset");
        assert_eq!(refs[0].uid, "abc-123");
        assert_eq!(refs[0].controller, Some(true));
    }

    #[test]
    fn test_controller_revision_deserialize_without_metadata() {
        // ObjectMeta is omitempty upstream; a body that omits it must not 422.
        let json_str = r#"{
            "kind": "ControllerRevision",
            "apiVersion": "apps/v1",
            "data": {"spec": {"template": {"metadata": {"labels": {"app": "x"}}}}},
            "revision": 2
        }"#;
        let cr: ControllerRevision =
            serde_json::from_str(json_str).expect("metadata should default");
        assert_eq!(cr.revision, 2);
        assert!(cr.metadata.name.is_empty());
        assert!(cr.data.is_some());
    }

    #[test]
    fn test_controller_revision_deserialize_without_revision() {
        let json_str = r#"{
            "kind": "ControllerRevision",
            "apiVersion": "apps/v1",
            "metadata": {"name": "r", "namespace": "n"},
            "data": {"x": 1}
        }"#;
        let cr: ControllerRevision =
            serde_json::from_str(json_str).expect("revision should default to 0");
        assert_eq!(cr.revision, 0);
        assert_eq!(cr.metadata.name, "r");
    }

    #[test]
    fn test_controller_revision_with_labels() {
        let mut cr = ControllerRevision::new("labeled-rev", "default", 2);

        let mut labels = std::collections::HashMap::new();
        labels.insert(
            "controller-revision-hash".to_string(),
            "abc123def4".to_string(),
        );
        labels.insert("app".to_string(), "nginx".to_string());
        cr.metadata.labels = Some(labels);

        let json_str = serde_json::to_string(&cr).expect("serialize");
        let restored: ControllerRevision = serde_json::from_str(&json_str).expect("deserialize");

        let restored_labels = restored.metadata.labels.unwrap();
        assert_eq!(
            restored_labels.get("controller-revision-hash"),
            Some(&"abc123def4".to_string())
        );
        assert_eq!(restored_labels.get("app"), Some(&"nginx".to_string()));
    }
}
