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
    ///
    /// `#[serde(default)]` mirrors Go's `json.Unmarshal`: a missing scalar
    /// decodes to its zero value rather than erroring. This struct is reused for
    /// `status.acceptedNames`, which clients send empty/partial (no `plural`) on
    /// create — that must decode, with `spec.names.plural` emptiness enforced by
    /// validation (see handlers::crd) rather than at the decode layer.
    #[serde(default)]
    pub plural: String,

    /// Singular is the singular name of the resource (used as an alias on CLI)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub singular: Option<String>,

    /// Kind is the serialized kind of the resource (PascalCase)
    #[serde(default)]
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

/// JSONSchemaPropsOrArray represents a value that can be either a JSONSchemaProps or an array of them.
///
/// Serializes inline (matching upstream's JSON `MarshalJSON`). Deserialization
/// accepts BOTH the inline JSON form (`{type: object, ...}` / `[ ... ]`) AND
/// the struct form (`{schema: {...}}` / `{jSONSchemas: [...]}`) that
/// vnd.kubernetes CBOR clients emit — k8s CBOR-encodes this type as its Go
/// struct (protobuf field names `schema` / `jSONSchemas`) rather than inlining
/// it, and rusternetes transcodes that CBOR to JSON before typed decode. See
/// [`deserialize_schema_or_array`].
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum JSONSchemaPropsOrArray {
    Schema(JSONSchemaProps),
    Schemas(Vec<JSONSchemaProps>),
}

/// JSONSchemaPropsOrBool represents a value that can be either a JSONSchemaProps or a boolean.
///
/// Like [`JSONSchemaPropsOrArray`], deserialization also accepts the CBOR
/// struct form `{allows: <bool>, schema: {...}}`.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum JSONSchemaPropsOrBool {
    Schema(JSONSchemaProps),
    Bool(bool),
}

/// JSONSchemaPropsOrStringArray represents a value that can be either a JSONSchemaProps or a string array.
///
/// Like [`JSONSchemaPropsOrArray`], deserialization also accepts the CBOR
/// struct form `{schema: {...}}` / `{property: [...]}`.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum JSONSchemaPropsOrStringArray {
    Schema(JSONSchemaProps),
    Strings(Vec<String>),
}

// ---------------------------------------------------------------------------
// Custom Deserialize for the JSONSchemaProps "or X" union types.
//
// Upstream defines custom JSON marshalers that *inline* the value, but its CBOR
// marshalers (pre-1.36) fall back to the Go struct shape, so a CBOR request
// body carries e.g. `items: {schema: {...}}` instead of `items: {type: ...}`.
// rusternetes transcodes incoming CBOR to JSON, so by the time we decode we may
// see either shape. A plain `#[serde(untagged)]` derive only understood the
// inline shape and silently parsed the struct shape as an (empty) inline
// schema, which then tripped strict field validation with a spurious
// `unknown field "...items.schema"` and broke CRD creation over CBOR
// (CustomResourcePublishOpenAPI conformance specs).
//
// The struct shape is unambiguous: an inline JSONSchemaProps never has a
// top-level `schema` / `jSONSchemas` / `allows` / `property` key (those are not
// JSONSchemaProps fields), so their presence selects the struct form.
// ---------------------------------------------------------------------------

fn json_props_from_value<E: serde::de::Error>(v: serde_json::Value) -> Result<JSONSchemaProps, E> {
    serde_json::from_value(v).map_err(E::custom)
}

impl<'de> Deserialize<'de> for JSONSchemaPropsOrArray {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let v = serde_json::Value::deserialize(deserializer)?;
        match v {
            serde_json::Value::Array(_) => Ok(Self::Schemas(
                serde_json::from_value(v).map_err(D::Error::custom)?,
            )),
            serde_json::Value::Object(ref m)
                if m.contains_key("schema") || m.contains_key("jSONSchemas") =>
            {
                if let Some(s) = m.get("schema").filter(|s| !s.is_null()) {
                    Ok(Self::Schema(json_props_from_value(s.clone())?))
                } else if let Some(a) = m.get("jSONSchemas").filter(|a| !a.is_null()) {
                    Ok(Self::Schemas(
                        serde_json::from_value(a.clone()).map_err(D::Error::custom)?,
                    ))
                } else {
                    Ok(Self::Schema(JSONSchemaProps::default()))
                }
            }
            other => Ok(Self::Schema(json_props_from_value(other)?)),
        }
    }
}

impl<'de> Deserialize<'de> for JSONSchemaPropsOrBool {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = serde_json::Value::deserialize(deserializer)?;
        match v {
            serde_json::Value::Bool(b) => Ok(Self::Bool(b)),
            serde_json::Value::Object(ref m)
                if m.contains_key("schema") || m.contains_key("allows") =>
            {
                if let Some(s) = m.get("schema").filter(|s| !s.is_null()) {
                    Ok(Self::Schema(json_props_from_value(s.clone())?))
                } else {
                    Ok(Self::Bool(
                        m.get("allows").and_then(|a| a.as_bool()).unwrap_or(false),
                    ))
                }
            }
            other => Ok(Self::Schema(json_props_from_value(other)?)),
        }
    }
}

impl<'de> Deserialize<'de> for JSONSchemaPropsOrStringArray {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let v = serde_json::Value::deserialize(deserializer)?;
        match v {
            serde_json::Value::Array(_) => Ok(Self::Strings(
                serde_json::from_value(v).map_err(D::Error::custom)?,
            )),
            serde_json::Value::Object(ref m)
                if m.contains_key("schema") || m.contains_key("property") =>
            {
                if let Some(s) = m.get("schema").filter(|s| !s.is_null()) {
                    Ok(Self::Schema(json_props_from_value(s.clone())?))
                } else if let Some(p) = m.get("property").filter(|p| !p.is_null()) {
                    Ok(Self::Strings(
                        serde_json::from_value(p.clone()).map_err(D::Error::custom)?,
                    ))
                } else {
                    Ok(Self::Schema(JSONSchemaProps::default()))
                }
            }
            other => Ok(Self::Schema(json_props_from_value(other)?)),
        }
    }
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

// ---------------------------------------------------------------------------
// Structural-schema pruning helpers
//
// Used by both the api-server handlers (post-mutating-webhook persistence path)
// and the admission-webhook crate (pre-persistence pruning of raw JSON values).
//
// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/prune.go
// ---------------------------------------------------------------------------

/// Recursively prune unknown fields from a JSON [`Value`] against a [`JSONSchemaProps`].
///
/// Fields not declared as a property are removed unless the schema (or the
/// enclosing object) carries `x-kubernetes-preserve-unknown-fields`. Array
/// items are pruned element-wise when `items` is a single schema.
///
/// K8s ref: `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/prune.go`
pub fn prune_value_against_schema(
    value: &mut serde_json::Value,
    schema: &JSONSchemaProps,
    path: &str,
) {
    if schema.x_kubernetes_preserve_unknown_fields == Some(true) {
        return;
    }
    match value {
        serde_json::Value::Object(obj) => {
            let Some(props) = schema.properties.as_ref() else {
                return;
            };
            obj.retain(|k, _| {
                let keep = props.contains_key(k);
                if !keep {
                    tracing::debug!("Pruning unknown field '{}.{}'", path, k);
                }
                keep
            });
            for (k, child_schema) in props {
                if let Some(child_val) = obj.get_mut(k) {
                    prune_value_against_schema(child_val, child_schema, &format!("{}.{}", path, k));
                }
            }
        }
        serde_json::Value::Array(arr) => {
            if let Some(JSONSchemaPropsOrArray::Schema(item_schema)) = schema.items.as_deref() {
                for (i, item) in arr.iter_mut().enumerate() {
                    prune_value_against_schema(item, item_schema, &format!("{}[{}]", path, i));
                }
            }
        }
        _ => {}
    }
}

/// Prune a CR represented as a raw JSON [`Value`] against its CRD's structural schema.
///
/// Used by the admission webhook pipeline after mutating webhooks apply JSON
/// patches — any unknown field a webhook injects is stripped before the value
/// is persisted (mirrors the in-handler `prune_custom_resource` path on the
/// `CustomResource` struct).
///
/// Top-level `apiVersion`, `kind`, and `metadata` are never pruned — they are
/// owned by the Kubernetes object meta, not the structural schema.
///
/// K8s ref: `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/prune.go`
pub fn prune_custom_resource_value(
    crd: &CustomResourceDefinition,
    version: &str,
    value: &mut serde_json::Value,
) {
    let Some(crd_version) = crd.spec.versions.iter().find(|v| v.name == version) else {
        return;
    };

    // Spec-level preserveUnknownFields disables structural-schema pruning.
    if crd.spec.preserve_unknown_fields == Some(true) {
        return;
    }

    let Some(validation) = &crd_version.schema else {
        return;
    };
    let schema = &validation.open_apiv3_schema;

    // Root-level preserveUnknownFields disables pruning for the whole object.
    if schema.x_kubernetes_preserve_unknown_fields == Some(true) {
        return;
    }

    let Some(obj) = value.as_object_mut() else {
        return;
    };

    // Top-level keys allowed even without an explicit schema entry — these
    // are k8s object meta, not part of the CRD's structural schema.
    const META_KEYS: &[&str] = &["apiVersion", "kind", "metadata"];

    let schema_props = schema.properties.as_ref();
    let allowed_top: std::collections::HashSet<&str> = schema_props
        .map(|p| p.keys().map(String::as_str).collect())
        .unwrap_or_default();

    obj.retain(|k, _| META_KEYS.contains(&k.as_str()) || allowed_top.contains(k.as_str()));

    if let Some(props) = schema_props {
        for (k, child_schema) in props {
            if let Some(child_val) = obj.get_mut(k) {
                prune_value_against_schema(child_val, child_schema, k);
            }
        }
    }
}

/// Prune a [`CustomResource`] struct in-place against its CRD's structural schema.
///
/// Removes fields from `cr.extra`, `cr.spec`, and `cr.status` that are not
/// declared in the CRD's structural schema (unless `x-kubernetes-preserve-unknown-fields`
/// is set). Mirrors the post-webhook pruning the create/update handlers perform.
///
/// K8s ref: `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/prune.go`
pub fn prune_custom_resource(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &mut CustomResource,
) {
    let crd_version = match crd.spec.versions.iter().find(|v| v.name == version) {
        Some(v) => v,
        None => return,
    };

    // Check if the CRD preserves unknown fields globally
    if crd.spec.preserve_unknown_fields == Some(true) {
        return;
    }

    let schema = match &crd_version.schema {
        Some(s) => &s.open_apiv3_schema,
        None => return,
    };

    // Check if root schema preserves unknown fields
    if schema.x_kubernetes_preserve_unknown_fields == Some(true) {
        return;
    }

    // Get the schema properties for the "data" field (or whatever top-level fields exist)
    // K8s prunes against spec/status/metadata + any additional properties
    let schema_properties: std::collections::HashSet<String> = schema
        .properties
        .as_ref()
        .map(|props| props.keys().cloned().collect())
        .unwrap_or_default();

    // Prune extra fields on the CR that aren't in the schema
    // K8s preserves: apiVersion, kind, metadata (always)
    // Everything else is checked against schema properties
    let known_top_level: std::collections::HashSet<&str> =
        ["apiVersion", "kind", "metadata"].iter().copied().collect();

    let cr_keys: Vec<String> = cr.extra.keys().cloned().collect();
    for key in cr_keys {
        if !known_top_level.contains(key.as_str()) && !schema_properties.contains(&key) {
            tracing::debug!("Pruning unknown field '{}' from CR", key);
            cr.extra.remove(&key);
        }
    }

    // Also prune within known fields like "data", "spec", "status"
    // by checking their nested schema properties.
    if let Some(schema_props) = &schema.properties {
        for (field_name, field_schema) in schema_props {
            // Check if this field preserves unknown fields
            if field_schema.x_kubernetes_preserve_unknown_fields == Some(true) {
                continue;
            }
            // The CustomResource type lifts "spec" and "status" out of the flat
            // map and stores them on their own fields. Walk those as well so
            // webhook-injected unknown fields under spec/status get pruned.
            if field_name == "spec" {
                if let Some(spec_val) = cr.spec.as_mut() {
                    prune_value_against_schema(spec_val, field_schema, "spec");
                }
                continue;
            }
            if field_name == "status" {
                if let Some(status_val) = cr.status.as_mut() {
                    prune_value_against_schema(status_val, field_schema, "status");
                }
                continue;
            }
            if let Some(field_props) = &field_schema.properties {
                let allowed_keys: std::collections::HashSet<String> =
                    field_props.keys().cloned().collect();

                // Prune from cr.extra if this field is there
                if let Some(field_val) = cr.extra.get_mut(field_name) {
                    if let Some(obj) = field_val.as_object_mut() {
                        let obj_keys: Vec<String> = obj.keys().cloned().collect();
                        for k in obj_keys {
                            if !allowed_keys.contains(&k) {
                                tracing::debug!(
                                    "Pruning unknown field '{}.{}' from CR",
                                    field_name,
                                    k
                                );
                                obj.remove(&k);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jsonschema_items_accepts_cbor_struct_form() {
        // A vnd.kubernetes CBOR client (1.35) sends `items` in the Go struct
        // form `{"schema": {...}}` (transcoded to JSON by rusternetes), not the
        // JSON-inlined form. Both must decode to the same Schema, and the inner
        // schema's fields must be preserved (regression: the old untagged derive
        // parsed it as an empty schema and strict validation rejected
        // `items.schema`).
        let inline: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "array",
            "items": { "type": "object", "properties": { "name": { "type": "string" } } }
        }))
        .unwrap();
        let struct_form: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "array",
            "items": { "schema": { "type": "object", "properties": { "name": { "type": "string" } } } }
        }))
        .unwrap();

        for (label, props) in [("inline", &inline), ("struct", &struct_form)] {
            match props.items.as_deref() {
                Some(JSONSchemaPropsOrArray::Schema(s)) => {
                    assert_eq!(
                        s.type_.as_deref(),
                        Some("object"),
                        "{label}: items elem type"
                    );
                    assert!(
                        s.properties
                            .as_ref()
                            .is_some_and(|p| p.contains_key("name")),
                        "{label}: items elem must keep its properties"
                    );
                }
                other => panic!("{label}: expected Schema variant, got {other:?}"),
            }
        }
        // Inline and struct forms must be equivalent after decode.
        assert_eq!(inline.items, struct_form.items);
    }

    #[test]
    fn test_jsonschema_or_bool_and_stringarray_cbor_struct_form() {
        // additionalProperties: CBOR struct form {allows, schema} and {schema}.
        let ap_bool: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "object",
            "additionalProperties": { "allows": false }
        }))
        .unwrap();
        assert_eq!(
            ap_bool.additional_properties.as_deref(),
            Some(&JSONSchemaPropsOrBool::Bool(false))
        );
        let ap_schema: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "object",
            "additionalProperties": { "schema": { "type": "string" } }
        }))
        .unwrap();
        match ap_schema.additional_properties.as_deref() {
            Some(JSONSchemaPropsOrBool::Schema(s)) => {
                assert_eq!(s.type_.as_deref(), Some("string"))
            }
            other => panic!("expected Schema, got {other:?}"),
        }
        // inline bool still works
        let ap_inline: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "object", "additionalProperties": true
        }))
        .unwrap();
        assert_eq!(
            ap_inline.additional_properties.as_deref(),
            Some(&JSONSchemaPropsOrBool::Bool(true))
        );
    }

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
