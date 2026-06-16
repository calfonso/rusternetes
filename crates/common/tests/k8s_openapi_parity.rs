//! Structural parity test: Rusternetes resource structs vs the upstream
//! `k8s-openapi` crate (the canonical Rust mirror of the Kubernetes OpenAPI
//! spec).
//!
//! Rust has no runtime reflection, so we cannot enumerate struct fields
//! directly. Instead we use `k8s-openapi`'s `schemars` JSON Schema as the
//! source of truth:
//!
//!   1. `schemars::schema_for!(Upstream)` -> draft-07 JSON Schema.
//!   2. Walk that schema and synthesize a *maximal* JSON instance in which
//!      every property of every (sub)object is populated.
//!   3. Deserialize that JSON into the corresponding Rusternetes struct, then
//!      re-serialize it.
//!   4. Any object key present in the upstream-maximal JSON but missing from
//!      our round-tripped JSON is a field we drop on decode -> a parity gap.
//!
//! We only compare object KEY SETS (recursively, descending into arrays), not
//! scalar values: the dummy values are arbitrary; only field presence matters.
//!
//! Known, intentional gaps are listed in `KNOWN_GAPS` so the test stays green
//! while documenting exactly what diverges. A gap path looks like
//! `spec.containers[].restartPolicyRules` (property names joined by `.`, with
//! `[]` marking array descent).

use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Maximal-instance synthesis from a draft-07 JSON Schema (as serde_json::Value)
// ---------------------------------------------------------------------------

const MAX_DEPTH: usize = 40;

/// Some upstream fields are open `string`s in the OpenAPI schema but are
/// modelled as strict Rust enums in our types, so the generic `"x"` dummy is
/// rejected on decode. Supply a representative valid token, keyed by the JSON
/// property name (and, where the same name means different enums, the enclosing
/// schema definition `ctx`). Extend as new strict-enum fields surface.
fn enum_token(key: &str, ctx: Option<&str>) -> Option<&'static str> {
    let ctx = ctx.unwrap_or("");
    Some(match key {
        "accessModes" => "ReadWriteOnce",
        "volumeMode" => "Filesystem",
        "internalTrafficPolicy" | "externalTrafficPolicy" => "Cluster",
        "ipFamilies" => "IPv4",
        "ipFamilyPolicy" => "SingleStack",
        "type" if ctx.contains("ServiceSpec") => "ClusterIP",
        "type" if ctx.contains("HostPath") => "Directory",
        "persistentVolumeReclaimPolicy" | "reclaimPolicy" => "Retain",
        "volumeBindingMode" => "Immediate",
        "fsGroupPolicy" => "File",
        "volumeLifecycleModes" => "Persistent",
        // `phase` is a different closed enum per resource.
        "phase" if ctx.contains("Namespace") => "Active",
        "phase" if ctx.contains("PersistentVolume") => "Bound", // PV and PVC
        "phase" => "Pending",                                   // Pod
        _ => return None,
    })
}

/// Upstream models these as objects with several optional fields; our types
/// model them as externally-tagged Rust enums (a single-key map). Collapse the
/// generated object to one key so decode succeeds.
fn is_single_key_union(def_name: &str) -> bool {
    // schemars def names are fully qualified (e.g. io.k8s.api.core.v1.ContainerState).
    let short = def_name.rsplit('.').next().unwrap_or(def_name);
    matches!(short, "ContainerState")
}

/// Build a JSON value that populates every property of `schema`.
/// `defs` is the `definitions` map for `$ref` resolution. `stack` tracks the
/// `$ref` names currently being expanded so we break recursive types.
/// `key_hint` is the property name this schema sits under (propagated through
/// arrays) so strict-enum string leaves can be given a valid token.
fn build_instance(
    schema: &Value,
    defs: &Value,
    stack: &mut Vec<String>,
    depth: usize,
    key_hint: Option<&str>,
    ctx: Option<&str>,
) -> Value {
    if depth > MAX_DEPTH {
        return Value::Null;
    }
    let obj = match schema.as_object() {
        Some(o) => o,
        None => return Value::Null, // `true`/`false` schema
    };

    // $ref resolution with cycle guard. schemars 1.x emits `#/$defs/Name`
    // (draft 2020-12); older spec dumps use `#/definitions/Name`. Take the
    // last path segment either way.
    if let Some(reference) = obj.get("$ref").and_then(Value::as_str) {
        let name = reference.rsplit('/').next().unwrap_or(reference);
        if stack.iter().any(|n| n == name) {
            return Value::Null; // recursive type -> stop
        }
        if let Some(target) = defs.get(name) {
            stack.push(name.to_string());
            // Children of this $ref are described by `name` — use it as context.
            let mut out = build_instance(target, defs, stack, depth + 1, key_hint, Some(name));
            stack.pop();
            if is_single_key_union(name) {
                if let Value::Object(m) = &out {
                    if let Some((k, v)) = m.iter().next() {
                        out = json!({ k.clone(): v.clone() });
                    }
                }
            }
            return out;
        }
        return Value::Null;
    }

    // allOf: merge all subschemas (k8s wraps a single $ref + description here).
    if let Some(all) = obj.get("allOf").and_then(Value::as_array) {
        let mut merged = serde_json::Map::new();
        let mut non_obj: Option<Value> = None;
        for sub in all {
            match build_instance(sub, defs, stack, depth + 1, key_hint, ctx) {
                Value::Object(m) => merged.extend(m),
                other if !other.is_null() && non_obj.is_none() => non_obj = Some(other),
                _ => {}
            }
        }
        if !merged.is_empty() {
            return Value::Object(merged);
        }
        if let Some(v) = non_obj {
            return v;
        }
    }

    // anyOf / oneOf: take the first variant that yields a value.
    for key in ["anyOf", "oneOf"] {
        if let Some(arr) = obj.get(key).and_then(Value::as_array) {
            for sub in arr {
                let v = build_instance(sub, defs, stack, depth + 1, key_hint, ctx);
                if !v.is_null() {
                    return v;
                }
            }
        }
    }

    // enum / const: pick a concrete allowed value.
    if let Some(c) = obj.get("const") {
        return c.clone();
    }
    if let Some(e) = obj.get("enum").and_then(Value::as_array) {
        if let Some(first) = e.first() {
            return first.clone();
        }
    }

    // Dispatch on `type` (may be a string or an array of strings).
    let ty = match obj.get("type") {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).find(|t| *t != "null"),
        _ => None,
    };

    match ty {
        Some("object") | None if obj.contains_key("properties") => {
            let mut out = serde_json::Map::new();
            if let Some(props) = obj.get("properties").and_then(Value::as_object) {
                for (k, v) in props {
                    out.insert(
                        k.clone(),
                        build_instance(v, defs, stack, depth + 1, Some(k), ctx),
                    );
                }
            }
            Value::Object(out)
        }
        Some("object") => {
            // Map type (additionalProperties), no fixed keys.
            Value::Object(serde_json::Map::new())
        }
        Some("array") => {
            let items = obj.get("items").cloned().unwrap_or(Value::Bool(true));
            let elem = match &items {
                // tuple form: take the first
                Value::Array(a) => a
                    .first()
                    .map(|s| build_instance(s, defs, stack, depth + 1, key_hint, ctx))
                    .unwrap_or(Value::Null),
                other => build_instance(other, defs, stack, depth + 1, key_hint, ctx),
            };
            Value::Array(vec![elem])
        }
        Some("string") => {
            // Honour `format` so our strict custom deserializers (timestamps,
            // base64 byte strings, …) accept the dummy value.
            if let Some(tok) = key_hint.and_then(|k| enum_token(k, ctx)) {
                return Value::String(tok.to_string());
            }
            let fmt = obj.get("format").and_then(Value::as_str).unwrap_or("");
            let s = match fmt {
                "date-time" => "2024-01-01T00:00:00Z",
                "date" => "2024-01-01",
                "byte" => "eA==", // base64("x")
                _ => "x",
            };
            Value::String(s.to_string())
        }
        Some("integer") => json!(1),
        Some("number") => json!(1.0),
        Some("boolean") => Value::Bool(true),
        Some("null") => Value::Null,
        // Unconstrained schema: `x-kubernetes-int-or-string`, RawExtension,
        // arbitrary JSON. A string satisfies IntOrString and any permissive
        // `serde_json::Value` field.
        _ => Value::String("x".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Key-set diff
// ---------------------------------------------------------------------------

/// Record every object key present in `expected` but absent from `actual`.
/// Paths use `.` between property names and `[]` for array descent.
fn diff_keys(expected: &Value, actual: &Value, path: &str, dropped: &mut Vec<String>) {
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => {
            for (k, ev) in e {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                match a.get(k) {
                    None => dropped.push(child),
                    Some(av) => diff_keys(ev, av, &child, dropped),
                }
            }
        }
        (Value::Array(e), Value::Array(a)) => {
            if let (Some(ev), Some(av)) = (e.first(), a.first()) {
                diff_keys(ev, av, &format!("{path}[]"), dropped);
            }
        }
        // Object dropped to non-object (rename / type mismatch): flag every key.
        (Value::Object(e), _) => {
            for k in e.keys() {
                dropped.push(if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                });
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Known intentional gaps (field path -> reason). Keep this honest & small.
// ---------------------------------------------------------------------------

/// Documented parity gaps. Each entry is `(type_name, path_suffix)`; a gap is
/// allowed when its full JSON path equals the suffix or ends with `.<suffix>`,
/// so one PodSpec-relative suffix covers Pod, Deployment, Job, … (which embed
/// the same `PodSpec` at different prefixes). `type_name == "*"` matches every
/// resource.
///
/// Keep this list HONEST: it is the canonical inventory of where we diverge
/// from upstream. Entries fall into two buckets — intentional omissions and
/// tracked gaps (open a GitHub issue and reference it). Do NOT add an entry to
/// silence a casing/serialization bug; fix the struct instead.
const KNOWN_GAPS: &[(&str, &str)] = &[
    // ---- Intentional omissions ------------------------------------------
    // selfLink: deprecated k8s 1.16, removed from server responses.
    ("*", "metadata.selfLink"),
    // Legacy / removed in-tree volume plugins. Rusternetes does not implement
    // these provider-specific sources (most are removed upstream too). Bare
    // suffixes cover both the pod `volumes[]` form and the inline
    // PersistentVolume `spec.<plugin>` form.
    ("*", "awsElasticBlockStore"),
    ("*", "azureDisk"),
    ("*", "azureFile"),
    ("*", "cephfs"),
    ("*", "cinder"),
    ("*", "fc"),
    ("*", "flexVolume"),
    ("*", "flocker"),
    ("*", "gcePersistentDisk"),
    ("*", "gitRepo"),
    ("*", "glusterfs"),
    ("*", "photonPersistentDisk"),
    ("*", "portworxVolume"),
    ("*", "quobyte"),
    ("*", "rbd"),
    ("*", "scaleIO"),
    ("*", "storageos"),
    ("*", "vsphereVolume"),
    // VolumeAttachment: deprecated inline PV spec not modelled.
    (
        "VolumeAttachment",
        "spec.source.inlineVolumeSpec.accessModes",
    ),
    ("VolumeAttachment", "spec.source.inlineVolumeSpec.capacity"),
    ("VolumeAttachment", "spec.source.inlineVolumeSpec.claimRef"),
    ("VolumeAttachment", "spec.source.inlineVolumeSpec.hostPath"),
    // inline csi secret refs (csi::CSIVolumeSource) — part of deprecated inline spec
    (
        "VolumeAttachment",
        "inlineVolumeSpec.csi.controllerExpandSecretRef",
    ),
    (
        "VolumeAttachment",
        "inlineVolumeSpec.csi.controllerPublishSecretRef",
    ),
    (
        "VolumeAttachment",
        "inlineVolumeSpec.csi.nodeExpandSecretRef",
    ),
    (
        "VolumeAttachment",
        "inlineVolumeSpec.csi.nodeStageSecretRef",
    ),
    ("VolumeAttachment", "spec.source.inlineVolumeSpec.iscsi"),
    ("VolumeAttachment", "spec.source.inlineVolumeSpec.local"),
    (
        "VolumeAttachment",
        "spec.source.inlineVolumeSpec.mountOptions",
    ),
    ("VolumeAttachment", "spec.source.inlineVolumeSpec.nfs"),
    (
        "VolumeAttachment",
        "spec.source.inlineVolumeSpec.nodeAffinity",
    ),
    (
        "VolumeAttachment",
        "spec.source.inlineVolumeSpec.persistentVolumeReclaimPolicy",
    ),
    (
        "VolumeAttachment",
        "spec.source.inlineVolumeSpec.storageClassName",
    ),
    (
        "VolumeAttachment",
        "spec.source.inlineVolumeSpec.volumeAttributesClassName",
    ),
    (
        "VolumeAttachment",
        "spec.source.inlineVolumeSpec.volumeMode",
    ),
    // Node: deprecated fields omitted on purpose.
    ("Node", "spec.configSource"),
    ("Node", "spec.externalID"),
    ("Node", "status.phase"),
    // PersistentVolume.spec.capacity is a `HashMap` with
    // `skip_serializing_if = "is_empty"`; our maximal instance leaves the map
    // empty (additionalProperties have no fixed keys), so it round-trips to
    // absent. Presence is modelled; this is a serialization-skip artifact.
    ("PersistentVolume", "spec.capacity"),
];

fn check(name: &str, upstream_schema: Value, parse_roundtrip: impl Fn(Value) -> Value) {
    // schemars 1.x nests subschemas under `$defs`; older dumps use `definitions`.
    let mut defs = serde_json::Map::new();
    for key in ["$defs", "definitions"] {
        if let Some(m) = upstream_schema.get(key).and_then(Value::as_object) {
            defs.extend(m.clone());
        }
    }
    let defs = Value::Object(defs);
    let mut stack = Vec::new();
    let full = build_instance(&upstream_schema, &defs, &mut stack, 0, None, None);

    let back = parse_roundtrip(full.clone());

    let mut dropped = Vec::new();
    diff_keys(&full, &back, "", &mut dropped);

    let patterns: Vec<&str> = KNOWN_GAPS
        .iter()
        .filter(|(t, _)| *t == name || *t == "*")
        .map(|(_, p)| *p)
        .collect();
    let allowed = |path: &str| {
        patterns
            .iter()
            .any(|p| path == *p || path.ends_with(&format!(".{p}")))
    };

    let unexpected: Vec<&String> = dropped.iter().filter(|p| !allowed(p)).collect();

    assert!(
        unexpected.is_empty(),
        "{name}: {} field(s) present in k8s-openapi but dropped by our type:\n  {}",
        unexpected.len(),
        unexpected
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// `parity!(name, UpstreamType, OurType)` — generates the schema, round-trips
/// through our type, and asserts no non-allowlisted field is dropped.
macro_rules! parity {
    ($name:literal, $upstream:ty, $ours:ty) => {{
        let schema =
            serde_json::to_value(schemars::schema_for!($upstream)).expect("schema serializes");
        check($name, schema, |full| {
            let ours: $ours = serde_json::from_value(full.clone()).unwrap_or_else(|e| {
                panic!(
                    "{}: our type failed to deserialize the upstream-maximal JSON: {e}",
                    $name
                )
            });
            serde_json::to_value(&ours).expect("our type serializes")
        });
    }};
}

use k8s_openapi::api;
use rusternetes_common::resources as ours;

#[test]
fn core_v1_parity() {
    parity!("Pod", api::core::v1::Pod, ours::Pod);
    parity!("Service", api::core::v1::Service, ours::Service);
    parity!("ConfigMap", api::core::v1::ConfigMap, ours::ConfigMap);
    parity!("Secret", api::core::v1::Secret, ours::Secret);
    parity!("Namespace", api::core::v1::Namespace, ours::Namespace);
    parity!("Node", api::core::v1::Node, ours::Node);
    parity!(
        "PersistentVolumeClaim",
        api::core::v1::PersistentVolumeClaim,
        ours::PersistentVolumeClaim
    );
    parity!(
        "PersistentVolume",
        api::core::v1::PersistentVolume,
        ours::PersistentVolume
    );
    parity!(
        "ServiceAccount",
        api::core::v1::ServiceAccount,
        ours::ServiceAccount
    );
    parity!("Endpoints", api::core::v1::Endpoints, ours::Endpoints);
}

#[test]
fn apps_v1_parity() {
    parity!("Deployment", api::apps::v1::Deployment, ours::Deployment);
    parity!("ReplicaSet", api::apps::v1::ReplicaSet, ours::ReplicaSet);
    parity!("StatefulSet", api::apps::v1::StatefulSet, ours::StatefulSet);
    parity!("DaemonSet", api::apps::v1::DaemonSet, ours::DaemonSet);
}

#[test]
fn batch_v1_parity() {
    parity!("Job", api::batch::v1::Job, ours::Job);
    parity!("CronJob", api::batch::v1::CronJob, ours::CronJob);
}

#[test]
fn rbac_v1_parity() {
    parity!("Role", api::rbac::v1::Role, ours::Role);
    parity!("RoleBinding", api::rbac::v1::RoleBinding, ours::RoleBinding);
    parity!("ClusterRole", api::rbac::v1::ClusterRole, ours::ClusterRole);
    parity!(
        "ClusterRoleBinding",
        api::rbac::v1::ClusterRoleBinding,
        ours::ClusterRoleBinding
    );
}

#[test]
fn networking_v1_parity() {
    parity!("Ingress", api::networking::v1::Ingress, ours::Ingress);
    parity!(
        "IngressClass",
        api::networking::v1::IngressClass,
        ours::IngressClass
    );
    parity!(
        "NetworkPolicy",
        api::networking::v1::NetworkPolicy,
        ours::NetworkPolicy
    );
    parity!(
        "EndpointSlice",
        api::discovery::v1::EndpointSlice,
        ours::EndpointSlice
    );
}

#[test]
fn other_groups_parity() {
    parity!("Lease", api::coordination::v1::Lease, ours::Lease);
    parity!(
        "PodDisruptionBudget",
        api::policy::v1::PodDisruptionBudget,
        ours::PodDisruptionBudget
    );
    parity!(
        "PriorityClass",
        api::scheduling::v1::PriorityClass,
        ours::PriorityClass
    );
    parity!(
        "HorizontalPodAutoscaler",
        api::autoscaling::v2::HorizontalPodAutoscaler,
        ours::HorizontalPodAutoscaler
    );
}

#[test]
fn storage_v1_parity() {
    parity!(
        "StorageClass",
        api::storage::v1::StorageClass,
        ours::StorageClass
    );
    parity!("CSIDriver", api::storage::v1::CSIDriver, ours::CSIDriver);
    parity!(
        "VolumeAttachment",
        api::storage::v1::VolumeAttachment,
        ours::VolumeAttachment
    );
}
