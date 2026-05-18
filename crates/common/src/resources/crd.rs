// Custom Resource Definitions (CRDs) implementation
//
// This module implements Kubernetes CustomResourceDefinition support,
// allowing users to extend the API with custom resource types.

use crate::types::ObjectMeta;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Skip serializing Option<bool> when None or Some(false).
/// K8s omits x-kubernetes-* boolean extensions when false (the default).
fn skip_false_or_none(v: &Option<bool>) -> bool {
    !matches!(v, Some(true))
}

/// Skip serializing Option<String> when None or Some("").
/// K8s uses omitempty which skips empty strings.
fn skip_empty_string(v: &Option<String>) -> bool {
    v.as_ref().map(|s| s.is_empty()).unwrap_or(true)
}

/// Skip serializing Option<Vec<T>> when None or Some(empty vec).
/// K8s uses omitempty which skips nil and empty slices.
fn skip_empty_vec<T>(v: &Option<Vec<T>>) -> bool {
    v.as_ref().map(|v| v.is_empty()).unwrap_or(true)
}

/// Skip serializing Option<HashMap<K,V>> when None or Some(empty map).
fn skip_empty_map<K, V>(v: &Option<std::collections::HashMap<K, V>>) -> bool {
    v.as_ref().map(|m| m.is_empty()).unwrap_or(true)
}

/// CustomResourceDefinition defines a new custom resource type in the cluster
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceDefinition {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: CustomResourceDefinitionSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<CustomResourceDefinitionStatus>,
}

impl CustomResourceDefinition {
    /// Create a new CRD with minimal required fields
    pub fn new(name: &str, group: &str, kind: &str, plural: &str) -> Self {
        Self {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: ObjectMeta::new(format!("{}.{}", plural, group)),
            spec: CustomResourceDefinitionSpec {
                group: group.to_string(),
                names: CustomResourceDefinitionNames {
                    plural: plural.to_string(),
                    singular: Some(name.to_string()),
                    kind: kind.to_string(),
                    short_names: None,
                    categories: None,
                    list_kind: Some(format!("{}List", kind)),
                },
                scope: ResourceScope::Namespaced,
                versions: vec![],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }
}

/// CustomResourceDefinitionSpec describes the desired state of a CRD
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceDefinitionSpec {
    /// Group is the API group of the custom resource
    pub group: String,

    /// Names specify the resource and kind names for the custom resource
    pub names: CustomResourceDefinitionNames,

    /// Scope indicates whether the resource is cluster-scoped or namespace-scoped
    pub scope: ResourceScope,

    /// Versions is the list of versions for this custom resource
    pub versions: Vec<CustomResourceDefinitionVersion>,

    /// Conversion defines conversion settings for the CRD
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversion: Option<CustomResourceConversion>,

    /// PreserveUnknownFields indicates that object fields not specified in schema should be preserved
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preserve_unknown_fields: Option<bool>,
}

/// CustomResourceDefinitionNames indicates the names to use for this resource
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceDefinitionNames {
    /// Plural is the plural name of the resource (used in URLs: /apis/<group>/<version>/<plural>)
    pub plural: String,

    /// Singular is the singular name of the resource (used as an alias on CLI)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub singular: Option<String>,

    /// Kind is the serialized kind of the resource (PascalCase)
    pub kind: String,

    /// ShortNames are short names for the resource (used as aliases on CLI)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_names: Option<Vec<String>>,

    /// Categories is a list of grouped resources this custom resource belongs to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub categories: Option<Vec<String>>,

    /// ListKind is the serialized kind of the list for this resource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_kind: Option<String>,
}

/// ResourceScope indicates whether a resource is cluster-scoped or namespace-scoped
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ResourceScope {
    Namespaced,
    Cluster,
}

/// CustomResourceDefinitionVersion describes a version for a CRD
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceDefinitionVersion {
    /// Name is the version name (e.g., "v1", "v1beta1")
    pub name: String,

    /// Served indicates whether this version is served by the API server
    pub served: bool,

    /// Storage indicates whether this version should be used when persisting to storage
    /// Only one version can be marked as storage version
    pub storage: bool,

    /// Deprecated indicates this version is deprecated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated: Option<bool>,

    /// DeprecationWarning is shown in API responses when using this version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecation_warning: Option<String>,

    /// Schema describes the schema for this version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<CustomResourceValidation>,

    /// Subresources describes the subresources for this version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subresources: Option<CustomResourceSubresources>,

    /// AdditionalPrinterColumns specifies additional columns for kubectl get
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_printer_columns: Option<Vec<CustomResourceColumnDefinition>>,

    /// SelectableFields lists the JSONPaths exposed to `?fieldSelector=...`
    /// list/watch filtering, per `x-kubernetes-selectable-fields`.
    /// JSONPath uses dot-notation rooted at the CR (e.g. `.spec.color`).
    #[serde(skip_serializing_if = "Option::is_none", rename = "selectableFields")]
    pub selectable_fields: Option<Vec<SelectableField>>,
}

/// SelectableField declares a JSONPath that may be used as a field selector
/// in list/watch requests against custom resources of this CRD version.
///
/// See:
/// https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#selectable-fields
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SelectableField {
    /// JSONPath of the field, rooted at the custom resource. Must start
    /// with `.` (e.g. `.spec.color`).
    pub json_path: String,
}

/// CustomResourceValidation is a set of validation rules for a custom resource
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceValidation {
    /// OpenAPIV3Schema is the OpenAPI v3 schema to validate against
    #[serde(rename = "openAPIV3Schema")]
    pub open_apiv3_schema: JSONSchemaProps,
}

/// JSONSchemaProps is a JSON-Schema that validates a JSON object
/// This is a simplified implementation of OpenAPI v3 schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub struct JSONSchemaProps {
    #[serde(skip_serializing_if = "skip_empty_string")]
    pub id: Option<String>,

    #[serde(skip_serializing_if = "skip_empty_string", rename = "$schema")]
    pub schema: Option<String>,

    #[serde(skip_serializing_if = "skip_empty_string", rename = "$ref")]
    pub ref_path: Option<String>,

    #[serde(skip_serializing_if = "skip_empty_string")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "skip_empty_string", rename = "type")]
    pub type_: Option<String>,

    #[serde(skip_serializing_if = "skip_empty_string")]
    pub format: Option<String>,

    #[serde(skip_serializing_if = "skip_empty_string")]
    pub title: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,

    #[serde(skip_serializing_if = "skip_false_or_none")]
    pub exclusive_maximum: Option<bool>,

    #[serde(skip_serializing_if = "skip_false_or_none")]
    pub exclusive_minimum: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_length: Option<i64>,

    #[serde(skip_serializing_if = "skip_empty_string")]
    pub pattern: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_items: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_items: Option<i64>,

    #[serde(skip_serializing_if = "skip_false_or_none")]
    pub unique_items: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiple_of: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_properties: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_properties: Option<i64>,

    #[serde(skip_serializing_if = "skip_empty_vec")]
    pub required: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<JSONSchemaPropsOrArray>>,

    #[serde(skip_serializing_if = "skip_empty_vec")]
    pub all_of: Option<Vec<JSONSchemaProps>>,

    #[serde(skip_serializing_if = "skip_empty_vec")]
    pub one_of: Option<Vec<JSONSchemaProps>>,

    #[serde(skip_serializing_if = "skip_empty_vec")]
    pub any_of: Option<Vec<JSONSchemaProps>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub not: Option<Box<JSONSchemaProps>>,

    #[serde(skip_serializing_if = "skip_empty_map")]
    pub properties: Option<HashMap<String, JSONSchemaProps>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<Box<JSONSchemaPropsOrBool>>,

    #[serde(skip_serializing_if = "skip_empty_map")]
    pub pattern_properties: Option<HashMap<String, JSONSchemaProps>>,

    #[serde(skip_serializing_if = "skip_empty_map")]
    pub dependencies: Option<HashMap<String, JSONSchemaPropsOrStringArray>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_items: Option<Box<JSONSchemaPropsOrBool>>,

    #[serde(skip_serializing_if = "skip_empty_map")]
    pub definitions: Option<HashMap<String, JSONSchemaProps>>,

    #[serde(rename = "enum", skip_serializing_if = "skip_empty_vec")]
    pub enum_: Option<Vec<serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub example: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_docs: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "skip_false_or_none")]
    pub nullable: Option<bool>,

    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "x-kubernetes-preserve-unknown-fields"
    )]
    pub x_kubernetes_preserve_unknown_fields: Option<bool>,

    #[serde(
        skip_serializing_if = "skip_false_or_none",
        rename = "x-kubernetes-embedded-resource"
    )]
    pub x_kubernetes_embedded_resource: Option<bool>,

    #[serde(
        skip_serializing_if = "skip_false_or_none",
        rename = "x-kubernetes-int-or-string"
    )]
    pub x_kubernetes_int_or_string: Option<bool>,

    #[serde(
        skip_serializing_if = "skip_empty_vec",
        rename = "x-kubernetes-list-map-keys"
    )]
    pub x_kubernetes_list_map_keys: Option<Vec<String>>,

    #[serde(
        skip_serializing_if = "skip_empty_string",
        rename = "x-kubernetes-list-type"
    )]
    pub x_kubernetes_list_type: Option<String>,

    #[serde(
        skip_serializing_if = "skip_empty_string",
        rename = "x-kubernetes-map-type"
    )]
    pub x_kubernetes_map_type: Option<String>,

    #[serde(
        skip_serializing_if = "skip_empty_vec",
        rename = "x-kubernetes-validations"
    )]
    pub x_kubernetes_validations: Option<Vec<serde_json::Value>>,
}

/// JSONSchemaPropsOrArray represents a value that can be either a JSONSchemaProps or an array of them
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum JSONSchemaPropsOrArray {
    Schema(JSONSchemaProps),
    Schemas(Vec<JSONSchemaProps>),
}

/// JSONSchemaPropsOrBool represents a value that can be either a JSONSchemaProps or a boolean
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum JSONSchemaPropsOrBool {
    Schema(JSONSchemaProps),
    Bool(bool),
}

/// JSONSchemaPropsOrStringArray represents a value that can be either a JSONSchemaProps or a string array
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum JSONSchemaPropsOrStringArray {
    Schema(JSONSchemaProps),
    Strings(Vec<String>),
}

/// CustomResourceSubresources defines the status and scale subresources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceSubresources {
    /// Status indicates the custom resource should have a /status subresource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<CustomResourceSubresourceStatus>,

    /// Scale indicates the custom resource should have a /scale subresource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<CustomResourceSubresourceScale>,
}

/// CustomResourceSubresourceStatus defines how to serve the status subresource
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceSubresourceStatus {}

/// CustomResourceSubresourceScale defines how to serve the scale subresource
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceSubresourceScale {
    /// SpecReplicasPath is the JSON path in the custom resource for the replica count
    pub spec_replicas_path: String,

    /// StatusReplicasPath is the JSON path in the custom resource for the status replica count
    pub status_replicas_path: String,

    /// LabelSelectorPath is the JSON path for the label selector
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_selector_path: Option<String>,
}

/// CustomResourceColumnDefinition defines a column for kubectl get
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceColumnDefinition {
    /// Name is the name of the column
    #[serde(default)]
    pub name: String,

    /// Type is the OpenAPI type of the column data
    #[serde(rename = "type", default)]
    pub type_: String,

    /// Format is the optional OpenAPI format of the column data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Description is a human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Priority indicates the column's importance (0 = default view, 1+ = wide view)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,

    /// JSONPath is the JSON path to the field in the custom resource
    #[serde(rename = "jsonPath")]
    pub json_path: String,
}

/// CustomResourceConversion describes how to convert between different versions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceConversion {
    /// Strategy specifies how to convert between versions
    pub strategy: ConversionStrategyType,

    /// Webhook describes how to call the conversion webhook (if strategy is Webhook)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookConversion>,
}

/// ConversionStrategyType describes different conversion strategies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConversionStrategyType {
    /// None conversion assumes the same schema for all versions
    None,

    /// Webhook conversion calls an external webhook
    Webhook,
}

/// WebhookConversion describes how to call a conversion webhook
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WebhookConversion {
    /// ClientConfig describes how to connect to the webhook
    pub client_config: WebhookClientConfig,

    /// ConversionReviewVersions is the ordered list of API versions the webhook accepts
    pub conversion_review_versions: Vec<String>,
}

/// WebhookClientConfig contains information for connecting to a webhook
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WebhookClientConfig {
    /// URL is the webhook URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Service is a reference to a Kubernetes service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<ServiceReference>,

    /// CABundle is a PEM-encoded CA bundle for verifying the webhook's certificate
    #[serde(skip_serializing_if = "Option::is_none", rename = "caBundle")]
    pub ca_bundle: Option<String>,
}

/// ServiceReference holds a reference to a Kubernetes Service
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceReference {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
}

/// CustomResourceDefinitionStatus describes the observed state of a CRD
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceDefinitionStatus {
    /// Conditions indicate the state of the CRD
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<CustomResourceDefinitionCondition>>,

    /// AcceptedNames are the names actually being used to serve the CRD
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_names: Option<CustomResourceDefinitionNames>,

    /// StoredVersions lists all versions that have ever been persisted
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_versions: Option<Vec<String>>,
}

/// CustomResourceDefinitionCondition describes a condition of a CRD
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceDefinitionCondition {
    /// Type is the type of condition
    #[serde(rename = "type", default)]
    pub type_: String,

    /// Status is the status of the condition (True, False, Unknown)
    #[serde(default)]
    pub status: String,

    /// LastTransitionTime is when the condition last transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Reason is a brief reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Message is a human-readable message indicating details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// CustomResource represents a generic custom resource instance
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomResource {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,

    /// Spec is the custom resource's specification (schema-validated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<serde_json::Value>,

    /// Status is the custom resource's status (if status subresource is enabled)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<serde_json::Value>,

    /// Extra fields — CRDs with x-kubernetes-preserve-unknown-fields can have
    /// arbitrary top-level fields beyond spec/status. This catches all unknown fields.
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crd_creation() {
        let crd =
            CustomResourceDefinition::new("crontab", "stable.example.com", "CronTab", "crontabs");

        assert_eq!(crd.spec.group, "stable.example.com");
        assert_eq!(crd.spec.names.kind, "CronTab");
        assert_eq!(crd.spec.names.plural, "crontabs");
        assert_eq!(crd.metadata.name, "crontabs.stable.example.com");
    }

    #[test]
    fn test_crd_with_version() {
        let mut crd =
            CustomResourceDefinition::new("crontab", "stable.example.com", "CronTab", "crontabs");

        crd.spec.versions.push(CustomResourceDefinitionVersion {
            name: "v1".to_string(),
            served: true,
            storage: true,
            deprecated: None,
            deprecation_warning: None,
            schema: None,
            subresources: None,
            additional_printer_columns: None,
            selectable_fields: None,
        });

        assert_eq!(crd.spec.versions.len(), 1);
        assert_eq!(crd.spec.versions[0].name, "v1");
        assert!(crd.spec.versions[0].storage);
    }

    #[test]
    fn test_json_schema_simple() {
        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(HashMap::from([(
                "spec".to_string(),
                JSONSchemaProps {
                    type_: Some("object".to_string()),
                    properties: Some(HashMap::from([
                        (
                            "cronSpec".to_string(),
                            JSONSchemaProps {
                                type_: Some("string".to_string()),
                                ..Default::default()
                            },
                        ),
                        (
                            "image".to_string(),
                            JSONSchemaProps {
                                type_: Some("string".to_string()),
                                ..Default::default()
                            },
                        ),
                    ])),
                    ..Default::default()
                },
            )])),
            ..Default::default()
        };

        assert_eq!(schema.type_, Some("object".to_string()));
        assert!(schema.properties.is_some());
    }

    #[test]
    fn test_resource_scope_serialization() {
        let scoped = serde_json::to_string(&ResourceScope::Namespaced).unwrap();
        assert_eq!(scoped, r#""Namespaced""#);

        let cluster = serde_json::to_string(&ResourceScope::Cluster).unwrap();
        assert_eq!(cluster, r#""Cluster""#);
    }

    /// Test that enum values survive a JSON round-trip through the typed struct.
    /// This verifies the CRD schema with nested items + enum can be stored as
    /// raw JSON and deserialized back without losing enum values.
    #[test]
    fn test_enum_survives_json_roundtrip() {
        let crd_json = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "foos.tests.example.com"},
            "spec": {
                "group": "tests.example.com",
                "names": {
                    "plural": "foos",
                    "kind": "Foo"
                },
                "scope": "Namespaced",
                "versions": [{
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "spec": {
                                    "type": "object",
                                    "properties": {
                                        "bars": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "required": ["name"],
                                                "properties": {
                                                    "name": { "type": "string" },
                                                    "feeling": {
                                                        "type": "string",
                                                        "enum": ["Great", "Down"]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }]
            }
        });

        // Deserialize to typed struct
        let crd: super::super::CustomResourceDefinition =
            serde_json::from_value(crd_json).expect("Failed to deserialize CRD");

        // Check that enum values are preserved in the typed struct
        let version = &crd.spec.versions[0];
        let schema = version.schema.as_ref().expect("no schema");
        let spec_schema = schema
            .open_apiv3_schema
            .properties
            .as_ref()
            .unwrap()
            .get("spec")
            .unwrap();
        let bars_schema = spec_schema
            .properties
            .as_ref()
            .unwrap()
            .get("bars")
            .unwrap();

        match bars_schema.items.as_ref() {
            Some(items) => match items.as_ref() {
                JSONSchemaPropsOrArray::Schema(item_schema) => {
                    let feeling = item_schema
                        .properties
                        .as_ref()
                        .unwrap()
                        .get("feeling")
                        .unwrap();
                    assert_eq!(
                        feeling.enum_,
                        Some(vec![serde_json::json!("Great"), serde_json::json!("Down")]),
                        "enum values should survive deserialization"
                    );
                }
                JSONSchemaPropsOrArray::Schemas(_) => {
                    panic!("items should be Schema variant, not Schemas");
                }
            },
            None => panic!("items should not be None"),
        }

        // Serialize back and check again
        let roundtripped = serde_json::to_value(&crd).expect("Failed to serialize CRD");
        let feeling_enum = &roundtripped["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
            ["properties"]["spec"]["properties"]["bars"]["items"]["properties"]["feeling"]["enum"];
        assert_eq!(
            feeling_enum,
            &serde_json::json!(["Great", "Down"]),
            "enum values should survive full JSON round-trip"
        );
    }
}
