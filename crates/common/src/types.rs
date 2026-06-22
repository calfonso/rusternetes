use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Deserialize null or missing String as empty string (matching Go's zero value behavior)
pub fn deserialize_null_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

/// Serde for `metav1.Time` fields (creationTimestamp, deletionTimestamp, ...).
///
/// Upstream `metav1.Time.MarshalJSON` formats as RFC3339 with **second**
/// precision (`time.RFC3339`, no fractional seconds), e.g. `2026-05-31T08:00:43Z`.
/// We emit the same so responses match k8s exactly (sub-second precision is a
/// non-conformance and breaks proto Timestamp round-tripping). Deserialize is
/// lenient and accepts timestamps with or without fractional seconds.
pub mod k8s_time {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(dt) => serializer.serialize_str(&dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                    return Ok(Some(dt.with_timezone(&Utc)));
                }
                s.parse::<DateTime<Utc>>()
                    .map(Some)
                    .map_err(serde::de::Error::custom)
            }
            None => Ok(None),
        }
    }
}

/// Serde for RFC3339 timestamp fields that are stored as `Option<String>`
/// rather than a typed `DateTime` (e.g. `ContainerState` `startedAt` /
/// `finishedAt`, which the kubelet writes as preformatted strings). Normalises
/// to whole-second precision on serialize so JSON matches the protobuf /
/// typed-client representation (`metav1.Time` is second-precision). A value
/// that does not parse as RFC3339 is emitted unchanged.
pub mod k8s_time_str {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(raw) => {
                let normalized = DateTime::parse_from_rfc3339(raw)
                    .map(|dt| {
                        dt.with_timezone(&Utc)
                            .format("%Y-%m-%dT%H:%M:%SZ")
                            .to_string()
                    })
                    .unwrap_or_else(|_| raw.clone());
                serializer.serialize_str(&normalized)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)
    }
}

/// Serde for `map[string]metav1.Time` fields (e.g. `PodDisruptionBudgetStatus.disruptedPods`).
///
/// Upstream `metav1.Time` normally marshals to an RFC3339 string, but apply
/// configurations and some clients emit a *zero* `metav1.Time` as an empty JSON
/// object `{}` (and a zero time marshals to `null`). chrono's `DateTime`
/// deserializer rejects both the object form and `null`, which made the
/// PodDisruptionBudget `UpdateStatus` path reject otherwise-valid requests with
/// a 422. This module accepts a string, an object, or null per value, mirroring
/// `metav1.Time.UnmarshalJSON`'s leniency.
pub mod k8s_time_map {
    use chrono::{DateTime, Utc};
    use serde::de::{MapAccess, Visitor};
    use serde::{self, ser::SerializeMap, Deserialize, Deserializer, Serializer};
    use std::collections::HashMap;
    use std::fmt;

    pub fn serialize<S>(
        map: &Option<HashMap<String, DateTime<Utc>>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match map {
            Some(m) => {
                let mut ser = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m {
                    ser.serialize_entry(k, &v.format("%Y-%m-%dT%H:%M:%SZ").to_string())?;
                }
                ser.end()
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<HashMap<String, DateTime<Utc>>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Accepts a string (RFC3339), an object (zero `metav1.Time` -> now), or
        /// null (zero time -> now), matching upstream leniency.
        struct TimeValue(DateTime<Utc>);

        impl<'de> serde::Deserialize<'de> for TimeValue {
            fn deserialize<D>(d: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = DateTime<Utc>;
                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        f.write_str("an RFC3339 time string, an object, or null")
                    }
                    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        DateTime::parse_from_rfc3339(s)
                            .map(|dt| dt.with_timezone(&Utc))
                            .or_else(|_| s.parse::<DateTime<Utc>>())
                            .map_err(serde::de::Error::custom)
                    }
                    fn visit_unit<E>(self) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(Utc::now())
                    }
                    fn visit_none<E>(self) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(Utc::now())
                    }
                    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                    where
                        A: MapAccess<'de>,
                    {
                        // Drain the (empty) object form of a zero metav1.Time.
                        while map
                            .next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
                            .is_some()
                        {}
                        Ok(Utc::now())
                    }
                }
                d.deserialize_any(V).map(TimeValue)
            }
        }

        let opt: Option<HashMap<String, TimeValue>> =
            Option::deserialize(deserializer).map_err(serde::de::Error::custom)?;
        Ok(opt.map(|m| m.into_iter().map(|(k, v)| (k, v.0)).collect()))
    }
}

/// ObjectMeta is metadata that all persisted resources must have
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub struct ObjectMeta {
    /// Name must be unique within a namespace
    #[serde(default, deserialize_with = "deserialize_null_string")]
    pub name: String,

    /// GenerateName is an optional prefix used to generate a unique name when Name is empty.
    /// The server will append a random suffix to this prefix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generate_name: Option<String>,

    /// Namespace defines the space within which the resource name must be unique
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// UID is a unique identifier for the resource (auto-generated if not provided)
    #[serde(default = "generate_uid", skip_serializing_if = "String::is_empty")]
    pub uid: String,

    /// Generation is a sequence number representing a specific generation of the desired state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,

    /// ResourceVersion is an opaque value for concurrency control
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,

    /// ManagedFields maps workflow-id and version to the set of fields that are managed by that workflow
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_fields: Option<Vec<ManagedFieldsEntry>>,

    /// CreationTimestamp is the creation time
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize",
        default
    )]
    pub creation_timestamp: Option<DateTime<Utc>>,

    /// DeletionTimestamp is the time when the resource will be deleted
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize",
        default
    )]
    pub deletion_timestamp: Option<DateTime<Utc>>,

    /// DeletionGracePeriodSeconds is the number of seconds before the object should be deleted
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_grace_period_seconds: Option<i64>,

    /// Labels are key-value pairs for categorization
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<HashMap<String, String>>,

    /// Annotations are key-value pairs for arbitrary metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<HashMap<String, String>>,

    /// Finalizers are pre-deletion hooks that must complete before deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalizers: Option<Vec<String>>,

    /// OwnerReferences are references to objects that own this object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_references: Option<Vec<OwnerReference>>,
}

fn generate_uid() -> String {
    String::new()
}

impl ObjectMeta {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            generate_name: None,
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            generation: None,
            managed_fields: None,
            resource_version: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            deletion_grace_period_seconds: None,
            labels: None,
            annotations: None,
            finalizers: None,
            owner_references: None,
        }
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = Some(labels);
        self
    }

    pub fn with_annotations(mut self, annotations: HashMap<String, String>) -> Self {
        self.annotations = Some(annotations);
        self
    }

    pub fn with_owner_reference(mut self, owner: OwnerReference) -> Self {
        self.owner_references = Some(vec![owner]);
        self
    }

    pub fn with_finalizers(mut self, finalizers: Vec<String>) -> Self {
        self.finalizers = Some(finalizers);
        self
    }

    /// Resolve `generateName` into a concrete name. If `name` is empty and
    /// `generateName` is a non-empty prefix, append a random 5-character suffix.
    ///
    /// When neither is set the name is left empty so downstream validation can
    /// reject the object — this must NOT fabricate an `auto-<id>` name, which is
    /// non-conformant (a create with no name and no generateName is invalid).
    /// Server-side name generation is normally handled centrally by
    /// `generate_name_middleware` before any handler runs; this remains for
    /// in-process callers that build objects directly.
    pub fn ensure_name(&mut self) {
        if self.name.is_empty() {
            if let Some(prefix) = self.generate_name.as_deref().filter(|p| !p.is_empty()) {
                let suffix = uuid::Uuid::new_v4().simple().to_string()[..5].to_string();
                self.name = format!("{prefix}{suffix}");
            }
        }
    }

    /// Ensure uid is populated (generate if empty). Also resolves generateName if name is empty.
    pub fn ensure_uid(&mut self) {
        self.ensure_name();
        if self.uid.is_empty() {
            self.uid = uuid::Uuid::new_v4().to_string();
        }
    }

    /// Ensure creation timestamp is set
    pub fn ensure_creation_timestamp(&mut self) {
        if self.creation_timestamp.is_none() {
            self.creation_timestamp = Some(Utc::now());
        }
    }

    /// Ensure generation is set (defaults to 1 for new resources)
    pub fn ensure_generation(&mut self) {
        if self.generation.is_none() {
            self.generation = Some(1);
        }
    }

    /// Check if the object is being deleted (has deletion timestamp)
    pub fn is_being_deleted(&self) -> bool {
        self.deletion_timestamp.is_some()
    }

    /// Add a finalizer to the object
    pub fn add_finalizer(&mut self, finalizer: String) {
        let finalizers = self.finalizers.get_or_insert_with(Vec::new);
        if !finalizers.contains(&finalizer) {
            finalizers.push(finalizer);
        }
    }

    /// Remove a finalizer from the object
    pub fn remove_finalizer(&mut self, finalizer: &str) {
        if let Some(finalizers) = &mut self.finalizers {
            finalizers.retain(|f| f != finalizer);
        }
    }

    /// Check if the object has any finalizers
    pub fn has_finalizers(&self) -> bool {
        self.finalizers.as_ref().is_some_and(|f| !f.is_empty())
    }
}

/// TypeMeta describes the type of the object
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TypeMeta {
    /// Kind is the object's type
    #[serde(default)]
    pub kind: String,

    /// APIVersion defines the versioned schema of the object
    #[serde(default)]
    pub api_version: String,
}

/// Resource status phase
///
/// `Unknown` carries `#[serde(alias = "")]` because kubectl / client-go send
/// `status: { phase: "" }` on CREATE for resources where the server assigns
/// the phase (namespace, pod, etc.). Upstream Kubernetes accepts the empty
/// string and overwrites it; without this alias `Option<Phase>` rejects the
/// empty inner string and the api-server returns 422 before any handler
/// runs, breaking every kubectl-driven CREATE including the one hydrophone
/// + sonobuoy issue to bootstrap their test namespace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Phase {
    Pending,
    Running,
    Succeeded,
    Failed,
    #[serde(alias = "")]
    Unknown,
    Active,
    Terminating,
}

/// Label selector for matching resources
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<LabelSelectorRequirement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorRequirement {
    pub key: String,
    pub operator: String, // In, NotIn, Exists, DoesNotExist
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

impl LabelSelector {
    /// Whether `labels` satisfies this selector (both `matchLabels` and
    /// `matchExpressions`). Mirrors upstream
    /// `apimachinery/pkg/apis/meta/v1.LabelSelectorAsSelector` +
    /// `Selector.Matches`.
    ///
    /// An *empty* selector (no `matchLabels` and no `matchExpressions`) matches
    /// nothing here. This mirrors the call sites that treat an empty selector as
    /// "does not apply" — e.g. eviction's PDB lookup and the ClusterRole
    /// aggregation controller both skip empty selectors rather than selecting
    /// everything (upstream's `selector.Empty()` guard).
    pub fn matches_labels(&self, labels: &HashMap<String, String>) -> bool {
        let has_match_labels = self.match_labels.as_ref().is_some_and(|m| !m.is_empty());
        let has_match_exprs = self
            .match_expressions
            .as_ref()
            .is_some_and(|e| !e.is_empty());
        if !has_match_labels && !has_match_exprs {
            return false;
        }

        if let Some(match_labels) = &self.match_labels {
            for (k, v) in match_labels {
                if labels.get(k) != Some(v) {
                    return false;
                }
            }
        }

        if let Some(exprs) = &self.match_expressions {
            for req in exprs {
                let present = labels.contains_key(&req.key);
                let matched =
                    match req.operator.as_str() {
                        "In" => req.values.as_ref().is_some_and(|vals| {
                            labels.get(&req.key).is_some_and(|v| vals.contains(v))
                        }),
                        "NotIn" => !req.values.as_ref().is_some_and(|vals| {
                            labels.get(&req.key).is_some_and(|v| vals.contains(v))
                        }),
                        "Exists" => present,
                        "DoesNotExist" => !present,
                        _ => false,
                    };
                if !matched {
                    return false;
                }
            }
        }

        true
    }
}

/// Tolerant deserializer for `HashMap<String, String>` maps whose
/// values represent Kubernetes `resource.Quantity` wire-form values.
///
/// Upstream `Quantity` round-trips as a JSON string (`"100m"`, `"1Gi"`),
/// but the Go marshaller routes values through `inf.Dec` and several
/// code paths emit a bare JSON number rather than a quoted string —
/// notably zero-valued quantities defaulted from `int64` fields, and
/// some legacy clients. K8s' Go deserialiser tolerates all three forms
/// (`UnmarshalJSON` in `pkg/api/resource/quantity.go`).
///
/// Our previous plain `HashMap<String, String>` deserialiser rejected
/// every pod-update body that carried a numeric `0` in
/// `containers[*].resources.{requests,limits}` (column 942 of the
/// canonical service-account-injected pod body). This function accepts
/// `String`, integer, and float JSON values and renders each into the
/// canonical string form so existing string-typed maps round-trip
/// without forcing every caller to switch to a newtype.
pub fn deserialize_quantity_map<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{MapAccess, Visitor};
    use std::fmt;

    struct OuterVisitor;
    impl<'de> Visitor<'de> for OuterVisitor {
        type Value = Option<HashMap<String, String>>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("null or a map<string, Quantity>")
        }
        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D2>(self, d: D2) -> Result<Self::Value, D2::Error>
        where
            D2: Deserializer<'de>,
        {
            d.deserialize_map(InnerVisitor).map(Some)
        }
        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            InnerVisitor.visit_map(map).map(Some)
        }
    }

    struct InnerVisitor;
    impl<'de> Visitor<'de> for InnerVisitor {
        type Value = HashMap<String, String>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a map<string, Quantity>")
        }
        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut out = HashMap::new();
            while let Some(key) = map.next_key::<String>()? {
                let v: serde_json::Value = map.next_value()?;
                let s = match v {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Null => continue,
                    other => {
                        return Err(serde::de::Error::custom(format!(
                            "Quantity value must be a string or number, got {}",
                            other
                        )));
                    }
                };
                crate::quantity::Quantity::parse(&s).map_err(|e| {
                    serde::de::Error::custom(format!("invalid Quantity value for {key:?}: {e}"))
                })?;
                out.insert(key, s);
            }
            Ok(out)
        }
    }

    deserializer.deserialize_option(OuterVisitor)
}

/// Resource requirements for compute resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_quantity_map"
    )]
    pub limits: Option<HashMap<String, String>>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_quantity_map"
    )]
    pub requests: Option<HashMap<String, String>>,

    /// Claims lists the names of resources, defined in spec.resourceClaims, that are used by this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claims: Option<Vec<ResourceClaim>>,
}

/// ResourceClaim references one entry in PodSpec.resourceClaims
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceClaim {
    /// Name must match the name of one entry in pod.spec.resourceClaims of the Pod
    pub name: String,

    /// Request is the name chosen for a request in the referenced claim
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<String>,
}

/// OwnerReference contains information about an object that owns another object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReference {
    /// API version of the referent
    pub api_version: String,

    /// Kind of the referent
    pub kind: String,

    /// Name of the referent
    pub name: String,

    /// UID of the referent
    pub uid: String,

    /// If true, AND if the owner has the "foregroundDeletion" finalizer, then
    /// the owner cannot be deleted from the key-value store until this
    /// reference is removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_owner_deletion: Option<bool>,

    /// If true, this reference points to the managing controller
    #[serde(skip_serializing_if = "Option::is_none")]
    pub controller: Option<bool>,
}

impl OwnerReference {
    /// Create a new owner reference
    pub fn new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            name: name.into(),
            uid: uid.into(),
            block_owner_deletion: None,
            controller: None,
        }
    }

    /// Mark this reference as the managing controller
    pub fn with_controller(mut self, is_controller: bool) -> Self {
        self.controller = Some(is_controller);
        self
    }

    /// Set whether this reference blocks owner deletion
    pub fn with_block_owner_deletion(mut self, block: bool) -> Self {
        self.block_owner_deletion = Some(block);
        self
    }
}

/// Deletion propagation policy
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum DeletionPropagation {
    /// Orphan dependents (default for some resources)
    Orphan,
    /// Foreground deletion - delete dependents first
    Foreground,
    /// Background deletion - delete owner first, let GC clean up dependents
    Background,
}

/// ListMeta describes metadata for list objects
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListMeta {
    /// resourceVersion is a version string for the list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,

    /// continue token for pagination
    #[serde(skip_serializing_if = "Option::is_none", rename = "continue")]
    pub continue_token: Option<String>,

    /// remainingItemCount is the number of subsequent items in the list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_item_count: Option<i64>,
}

impl Default for ListMeta {
    fn default() -> Self {
        Self {
            // Use "0" as default. Handlers should call with_resource_version()
            // to set the actual value from storage. DO NOT use timestamps here
            // as they create revision space mismatches with etcd mod_revisions.
            resource_version: Some("0".to_string()),
            continue_token: None,
            remaining_item_count: None,
        }
    }
}

/// List is a generic wrapper for Kubernetes list responses
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct List<T> {
    /// APIVersion defines the versioned schema of this representation
    pub api_version: String,

    /// Kind is a string value representing the REST resource
    pub kind: String,

    /// Standard list metadata
    pub metadata: ListMeta,

    /// List of objects
    pub items: Vec<T>,
}

impl<T> List<T> {
    /// Create a new List with the specified kind and API version.
    /// Automatically sets resourceVersion from the highest item's resourceVersion.
    pub fn new(kind: impl Into<String>, api_version: impl Into<String>, items: Vec<T>) -> Self
    where
        T: serde::Serialize,
    {
        let mut list = Self {
            kind: kind.into(),
            api_version: api_version.into(),
            metadata: ListMeta::default(),
            items,
        };
        list.set_resource_version_from_items();
        list
    }

    /// Set the list's resourceVersion from the highest item resourceVersion.
    /// This ensures clients get a valid revision for subsequent watches.
    pub fn set_resource_version_from_items(&mut self)
    where
        T: serde::Serialize,
    {
        let mut max_rv: i64 = 0;
        for item in &self.items {
            if let Ok(v) = serde_json::to_value(item) {
                if let Some(rv_str) = v
                    .get("metadata")
                    .and_then(|m| m.get("resourceVersion"))
                    .and_then(|r| r.as_str())
                {
                    if let Ok(rv) = rv_str.parse::<i64>() {
                        if rv > max_rv {
                            max_rv = rv;
                        }
                    }
                }
            }
        }
        // Always set a resource version — "0" or empty causes client-go to fail
        // with "initial RV '0' is not supported". Use max item RV or "1" as fallback.
        self.metadata.resource_version = Some(if max_rv > 0 {
            max_rv.to_string()
        } else {
            "1".to_string()
        });
    }

    /// Create a new List with resource version
    pub fn with_resource_version(mut self, resource_version: impl Into<String>) -> Self {
        self.metadata.resource_version = Some(resource_version.into());
        self
    }
}

/// Condition contains details for one aspect of the current state of an API Resource
/// This is the standard Kubernetes condition type (metav1.Condition)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Type of condition in CamelCase
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition: True, False, or Unknown
    pub status: String,

    /// ObservedGeneration represents the .metadata.generation that the condition was set based upon
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// LastTransitionTime is the last time the condition transitioned from one status to another
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,

    /// Reason contains a programmatic identifier indicating the reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Message is a human readable message indicating details about the transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Status is a return value for calls that don't return other objects (metav1.Status).
/// This is the standard Kubernetes error/status response type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    /// Kind is always "Status"
    #[serde(default = "default_status_kind")]
    pub kind: String,

    /// APIVersion is always "v1"
    #[serde(default = "default_status_api_version")]
    pub api_version: String,

    /// Standard list metadata (usually empty for Status)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ListMeta>,

    /// Status of the operation: "Success" or "Failure"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,

    /// A human-readable description of the status of this operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// A machine-readable description of why this operation is in the "Failure" status.
    /// E.g., "NotFound", "AlreadyExists", "Conflict", "Invalid", "Forbidden", "Unauthorized"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Extended data associated with the reason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<StatusDetails>,

    /// Suggested HTTP return code for this status (0 if not set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u16>,
}

fn default_status_kind() -> String {
    "Status".to_string()
}

fn default_status_api_version() -> String {
    "v1".to_string()
}

impl Status {
    /// Create a failure Status response
    pub fn failure(message: impl Into<String>, reason: impl Into<String>, code: u16) -> Self {
        Self {
            kind: "Status".to_string(),
            api_version: "v1".to_string(),
            metadata: None,
            status: Some("Failure".to_string()),
            message: Some(message.into()),
            reason: Some(reason.into()),
            details: None,
            code: Some(code),
        }
    }

    /// Create a failure Status with details
    pub fn failure_with_details(
        message: impl Into<String>,
        reason: impl Into<String>,
        code: u16,
        details: StatusDetails,
    ) -> Self {
        Self {
            kind: "Status".to_string(),
            api_version: "v1".to_string(),
            metadata: None,
            status: Some("Failure".to_string()),
            message: Some(message.into()),
            reason: Some(reason.into()),
            details: Some(details),
            code: Some(code),
        }
    }

    /// Create a success Status response
    pub fn success() -> Self {
        Self {
            kind: "Status".to_string(),
            api_version: "v1".to_string(),
            metadata: None,
            status: Some("Success".to_string()),
            message: None,
            reason: None,
            details: None,
            code: Some(200),
        }
    }
}

/// StatusDetails is a set of additional properties that MAY be set by the server
/// to provide additional information about a response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatusDetails {
    /// The name attribute of the resource associated with the status StatusReason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The group attribute of the resource associated with the status StatusReason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,

    /// The kind attribute of the resource associated with the status StatusReason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,

    /// UID of the resource (when there is a single resource which can be described)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    /// The Causes array includes more details associated with the StatusReason failure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causes: Option<Vec<StatusCause>>,

    /// If specified, the time in seconds before the operation should be retried
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<i32>,
}

/// StatusCause provides more information about an api.Status failure, including
/// cases when multiple errors are encountered.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StatusCause {
    /// A machine-readable description of the cause of the error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// A human-readable description of the cause of the error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// The field of the resource that has caused this error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// ManagedFieldsEntry is a workflow-id, a FieldSet and the group version of the resource
/// that the fieldset applies to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedFieldsEntry {
    /// Manager is an identifier of the workflow managing these fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manager: Option<String>,

    /// Operation is the type of operation which lead to this ManagedFieldsEntry (Apply or Update)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,

    /// APIVersion defines the version of the resource that this field set applies to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,

    /// Time is the timestamp of when the ManagedFields entry was added
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<DateTime<Utc>>,

    /// FieldsType is the discriminator for the different fields format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields_type: Option<String>,

    /// FieldsV1 stores a JSON representation of the fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields_v1: Option<serde_json::Value>,

    /// Subresource is the name of the subresource used to update that object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subresource: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sel(json: serde_json::Value) -> LabelSelector {
        serde_json::from_value(json).unwrap()
    }

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn matches_labels_empty_selector_matches_nothing() {
        assert!(!LabelSelector::default().matches_labels(&labels(&[("a", "b")])));
        assert!(!sel(serde_json::json!({})).matches_labels(&labels(&[("a", "b")])));
    }

    #[test]
    fn matches_labels_match_labels() {
        let s = sel(serde_json::json!({"matchLabels": {"app": "web"}}));
        assert!(s.matches_labels(&labels(&[("app", "web"), ("x", "y")])));
        assert!(!s.matches_labels(&labels(&[("app", "db")])));
        assert!(!s.matches_labels(&labels(&[("x", "y")])));
    }

    #[test]
    fn matches_labels_match_expressions() {
        // In
        let s = sel(serde_json::json!({
            "matchExpressions": [{"key": "tier", "operator": "In", "values": ["fe", "be"]}]
        }));
        assert!(s.matches_labels(&labels(&[("tier", "fe")])));
        assert!(!s.matches_labels(&labels(&[("tier", "cache")])));
        assert!(!s.matches_labels(&labels(&[("other", "x")])));

        // Exists / DoesNotExist
        let exists = sel(serde_json::json!({
            "matchExpressions": [{"key": "tier", "operator": "Exists"}]
        }));
        assert!(exists.matches_labels(&labels(&[("tier", "fe")])));
        assert!(!exists.matches_labels(&labels(&[("other", "x")])));

        let dne = sel(serde_json::json!({
            "matchExpressions": [{"key": "tier", "operator": "DoesNotExist"}]
        }));
        assert!(dne.matches_labels(&labels(&[("other", "x")])));
        assert!(!dne.matches_labels(&labels(&[("tier", "fe")])));

        // NotIn (also matches when key absent)
        let notin = sel(serde_json::json!({
            "matchExpressions": [{"key": "tier", "operator": "NotIn", "values": ["fe"]}]
        }));
        assert!(notin.matches_labels(&labels(&[("tier", "be")])));
        assert!(notin.matches_labels(&labels(&[("other", "x")])));
        assert!(!notin.matches_labels(&labels(&[("tier", "fe")])));
    }

    #[test]
    fn matches_labels_both_must_hold() {
        let s = sel(serde_json::json!({
            "matchLabels": {"app": "web"},
            "matchExpressions": [{"key": "tier", "operator": "Exists"}]
        }));
        assert!(s.matches_labels(&labels(&[("app", "web"), ("tier", "fe")])));
        assert!(!s.matches_labels(&labels(&[("app", "web")])));
        assert!(!s.matches_labels(&labels(&[("tier", "fe")])));
    }

    #[test]
    fn test_status_failure() {
        let status = Status::failure("pod not found", "NotFound", 404);
        assert_eq!(status.kind, "Status");
        assert_eq!(status.api_version, "v1");
        assert_eq!(status.status, Some("Failure".to_string()));
        assert_eq!(status.message, Some("pod not found".to_string()));
        assert_eq!(status.reason, Some("NotFound".to_string()));
        assert_eq!(status.code, Some(404));
        assert!(status.details.is_none());
    }

    #[test]
    fn test_status_failure_with_details() {
        let details = StatusDetails {
            name: Some("my-pod".to_string()),
            group: None,
            kind: Some("Pod".to_string()),
            uid: None,
            causes: Some(vec![StatusCause {
                reason: Some("FieldValueInvalid".to_string()),
                message: Some("spec.containers[0].image is required".to_string()),
                field: Some("spec.containers[0].image".to_string()),
            }]),
            retry_after_seconds: None,
        };

        let status =
            Status::failure_with_details("Pod \"my-pod\" is invalid", "Invalid", 422, details);

        assert_eq!(status.reason, Some("Invalid".to_string()));
        assert_eq!(status.code, Some(422));
        let d = status.details.unwrap();
        assert_eq!(d.name, Some("my-pod".to_string()));
        assert_eq!(d.kind, Some("Pod".to_string()));
        assert_eq!(d.causes.as_ref().unwrap().len(), 1);
        assert_eq!(
            d.causes.unwrap()[0].field,
            Some("spec.containers[0].image".to_string())
        );
    }

    #[test]
    fn test_status_success() {
        let status = Status::success();
        assert_eq!(status.status, Some("Success".to_string()));
        assert_eq!(status.code, Some(200));
        assert!(status.reason.is_none());
    }

    #[test]
    fn test_status_serialization() {
        let status = Status::failure("resource not found", "NotFound", 404);
        let json = serde_json::to_string(&status).unwrap();

        assert!(json.contains("\"kind\":\"Status\""));
        assert!(json.contains("\"apiVersion\":\"v1\""));
        assert!(json.contains("\"status\":\"Failure\""));
        assert!(json.contains("\"reason\":\"NotFound\""));
        assert!(json.contains("\"code\":404"));

        let deserialized: Status = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, status);
    }

    #[test]
    fn test_status_deserialization_from_kubernetes() {
        // Simulate a Status response as returned by a real Kubernetes API
        let json = r#"{
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": "pods \"nonexistent\" not found",
            "reason": "NotFound",
            "details": {
                "name": "nonexistent",
                "kind": "pods"
            },
            "code": 404
        }"#;

        let status: Status = serde_json::from_str(json).unwrap();
        assert_eq!(status.reason, Some("NotFound".to_string()));
        assert_eq!(status.code, Some(404));
        let d = status.details.unwrap();
        assert_eq!(d.name, Some("nonexistent".to_string()));
    }

    #[test]
    fn test_condition_serialization() {
        let condition = Condition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            observed_generation: Some(5),
            last_transition_time: None,
            reason: Some("PodReady".to_string()),
            message: Some("Pod is ready".to_string()),
        };

        let json = serde_json::to_string(&condition).unwrap();
        assert!(json.contains("\"type\":\"Ready\""));
        assert!(json.contains("\"observedGeneration\":5"));

        let deserialized: Condition = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.condition_type, "Ready");
        assert_eq!(deserialized.observed_generation, Some(5));
    }

    #[test]
    fn test_list_meta_with_pagination() {
        let meta = ListMeta {
            resource_version: Some("12345".to_string()),
            continue_token: Some("abc123".to_string()),
            remaining_item_count: Some(42),
        };

        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("\"continue\":\"abc123\""));
        assert!(json.contains("\"remainingItemCount\":42"));
    }

    #[test]
    fn test_creation_timestamp_serializes_at_second_precision() {
        use chrono::{DateTime, Utc};
        // Upstream metav1.Time.MarshalJSON formats at SECOND precision
        // (time.RFC3339, no fractional seconds). A client may *send* sub-second
        // precision, but we must serialize responses at second precision to
        // match k8s — emitting nanoseconds is a non-conformance and breaks
        // proto Timestamp round-tripping.
        let ts: DateTime<Utc> = "2026-03-29T21:05:27.270173921Z".parse().unwrap();
        let meta = ObjectMeta {
            name: "test".to_string(),
            creation_timestamp: Some(ts),
            ..Default::default()
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(
            json.contains("\"2026-03-29T21:05:27Z\""),
            "creationTimestamp must serialize at second precision: {}",
            json
        );
        assert!(
            !json.contains(".270"),
            "must not emit sub-second precision: {}",
            json
        );
        // Lenient deserialize still accepts sub-second input; re-serialize is stable.
        let meta2: ObjectMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(json, serde_json::to_string(&meta2).unwrap());
    }

    /// Regression: kubectl / client-go send `status: { phase: "" }` on CREATE
    /// for namespace/pod/etc. — server is meant to assign the phase. Without
    /// `#[serde(alias = "")]` on `Phase::Unknown` the api-server returned 422
    /// before any handler ran and `kubectl create ns <X>` was uniformly broken,
    /// taking hydrophone + sonobuoy down with it.
    #[test]
    fn phase_accepts_empty_string_on_input() {
        let p: Phase = serde_json::from_str(r#""""#).unwrap();
        assert_eq!(p, Phase::Unknown);
    }

    #[test]
    fn phase_named_variants_still_deserialize() {
        for (input, expected) in [
            (r#""Pending""#, Phase::Pending),
            (r#""Running""#, Phase::Running),
            (r#""Active""#, Phase::Active),
            (r#""Terminating""#, Phase::Terminating),
            (r#""Unknown""#, Phase::Unknown),
        ] {
            let parsed: Phase = serde_json::from_str(input).unwrap();
            assert_eq!(
                parsed, expected,
                "input {} should yield {:?}",
                input, expected
            );
        }
    }

    /// Output wire format must NOT change: `Phase::Unknown` still serializes as
    /// `"Unknown"`, not the empty string. The alias is an input-only synonym so
    /// upstream-shape persistence is preserved.
    #[test]
    fn phase_unknown_serializes_as_unknown_not_empty() {
        let json = serde_json::to_string(&Phase::Unknown).unwrap();
        assert_eq!(json, r#""Unknown""#);
    }

    /// Genuinely invalid variants must still fail loudly — the alias is only
    /// for the empty string, not a catch-all.
    #[test]
    fn phase_rejects_unknown_non_empty_variants() {
        let r: Result<Phase, _> = serde_json::from_str(r#""NotAPhase""#);
        assert!(r.is_err(), "unknown non-empty variant must error");
    }
}
