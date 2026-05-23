//! Structural merge primitives for Server-Side Apply (SSA).
//!
//! This module implements the smallest viable subset of K8s SSA: a JSON-Pointer
//! based ownership tracker for string-map fields (`data`, `binaryData`,
//! `metadata.labels`, `metadata.annotations`) and the scalar leaves under
//! `metadata` that an Apply request is allowed to touch.
//!
//! The full upstream merge algorithm in
//! `staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/typeconverter.go`
//! relies on the OpenAPI schema to know whether each field is `granular`
//! (atomic / merge / set). ConfigMap is the smallest possible case — every
//! managed field is a granular string leaf — so we hard-code the schema here
//! and leave a TODO for schema-driven generalisation.
//!
//! # Ownership representation
//!
//! Each manager owns a set of JSON-Pointers. For ConfigMap that means paths of
//! the form:
//!
//! ```text
//! /data/<key>
//! /binaryData/<key>
//! /metadata/labels/<key>
//! /metadata/annotations/<key>
//! ```
//!
//! Encoded as a `fieldsV1` v1-style tree the same paths look like:
//!
//! ```json
//! {
//!   "f:data": { "f:<key>": {} },
//!   "f:metadata": {
//!     "f:labels":      { "f:<key>": {} },
//!     "f:annotations": { "f:<key>": {} }
//!   }
//! }
//! ```
//!
//! We expose both forms: the flat `OwnedPaths` set for internal merge logic,
//! and a `to_fields_v1` / `from_fields_v1` codec for persistence in
//! `metadata.managedFields[*].fieldsV1`.

use serde_json::{json, Map, Value};
use std::collections::BTreeSet;

/// A flat set of JSON-Pointers owned by a single field manager.
///
/// JSON-Pointers are stored without the leading `/` for compactness:
/// `data/foo`, `metadata/labels/app`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnedPaths(pub BTreeSet<String>);

impl OwnedPaths {
    /// Create an empty path set.
    pub fn new() -> Self {
        Self(BTreeSet::new())
    }

    /// Insert a JSON-Pointer (without leading `/`).
    pub fn insert(&mut self, path: impl Into<String>) {
        self.0.insert(path.into());
    }

    /// Whether the manager owns the given path.
    pub fn contains(&self, path: &str) -> bool {
        self.0.contains(path)
    }

    /// Iterate owned paths in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }

    /// Number of owned paths.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// True if no paths are owned.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Encode this path set as the `fieldsV1` v1 tree used by upstream
    /// Kubernetes for `metadata.managedFields[*].fieldsV1`.
    pub fn to_fields_v1(&self) -> Value {
        let mut root = Map::new();
        for path in &self.0 {
            let segments: Vec<&str> = path.split('/').collect();
            insert_segment(&mut root, &segments);
        }
        Value::Object(root)
    }

    /// Decode a `fieldsV1` v1 tree into a flat owned-paths set.
    ///
    /// Leaves are recognised by an empty object `{}` at the bottom of the
    /// `f:` prefix chain — upstream calls these `PathElement{FieldName}`
    /// leaves. Any non-`f:` keys (e.g. `v:`, `i:`, `k:`) are ignored at the
    /// scaffold level — those are required for list/set/map-of-struct
    /// semantics we don't yet support.
    pub fn from_fields_v1(value: &Value) -> Self {
        let mut out = OwnedPaths::new();
        if let Some(obj) = value.as_object() {
            collect_paths(obj, &mut Vec::new(), &mut out);
        }
        out
    }
}

fn insert_segment(node: &mut Map<String, Value>, segments: &[&str]) {
    if segments.is_empty() {
        return;
    }
    let key = format!("f:{}", segments[0]);
    let rest = &segments[1..];
    let entry = node.entry(key).or_insert_with(|| Value::Object(Map::new()));
    if rest.is_empty() {
        // Leaf — upstream encodes leaves as empty objects.
        *entry = Value::Object(Map::new());
    } else if let Value::Object(child) = entry {
        insert_segment(child, rest);
    } else {
        // The previous entry was a leaf but we are extending it — promote
        // it back to an object. This can only happen if the caller stored
        // both `a/b` and `a/b/c`; upstream never does that for ConfigMap.
        let mut child = Map::new();
        insert_segment(&mut child, rest);
        *entry = Value::Object(child);
    }
}

fn collect_paths(node: &Map<String, Value>, stack: &mut Vec<String>, out: &mut OwnedPaths) {
    if node.is_empty() && !stack.is_empty() {
        out.insert(stack.join("/"));
        return;
    }
    for (key, child) in node {
        // Only `f:` (FieldName) keys describe ownership at the scaffold
        // level. Skip `v:`, `i:`, `k:` (value/index/key set discriminators)
        // — those require the schema-driven Forge.
        let Some(field) = key.strip_prefix("f:") else {
            continue;
        };
        stack.push(field.to_string());
        if let Some(child_obj) = child.as_object() {
            if child_obj.is_empty() {
                out.insert(stack.join("/"));
            } else {
                collect_paths(child_obj, stack, out);
            }
        } else {
            // Non-object child — treat as a leaf.
            out.insert(stack.join("/"));
        }
        stack.pop();
    }
}

/// A single conflict between the applying manager and an existing owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathConflict {
    /// The dotted JSON-Pointer (without leading `/`) of the disputed leaf.
    pub path: String,
    /// The manager that currently owns the leaf.
    pub current_manager: String,
    /// The manager trying to apply.
    pub applying_manager: String,
}

/// The four leaf groups SSA recognises on ConfigMap.
///
/// Each group is a flat string→string map. Server-Side Apply on these is
/// always a granular merge: keys present in `desired` are claimed by the
/// applying manager; keys absent from `desired` but previously owned by
/// the applying manager are removed; keys owned by some other manager are
/// left untouched (or trigger a conflict if `desired` tries to set them).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafGroup {
    Data,
    BinaryData,
    Labels,
    Annotations,
}

impl LeafGroup {
    /// JSON-Pointer prefix (no leading `/`) for this group.
    pub fn pointer_prefix(self) -> &'static str {
        match self {
            LeafGroup::Data => "data",
            LeafGroup::BinaryData => "binaryData",
            LeafGroup::Labels => "metadata/labels",
            LeafGroup::Annotations => "metadata/annotations",
        }
    }

    /// Walk the resource and return the inner string→Value map for this
    /// group, or an empty map if the parent path doesn't exist.
    pub fn extract_map(self, resource: &Value) -> Map<String, Value> {
        match self {
            LeafGroup::Data => resource
                .get("data")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            LeafGroup::BinaryData => resource
                .get("binaryData")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            LeafGroup::Labels => resource
                .get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            LeafGroup::Annotations => resource
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
        }
    }

    /// Replace the inner map for this group on `resource`.
    pub fn set_map(self, resource: &mut Value, map: Map<String, Value>) {
        let obj = match resource.as_object_mut() {
            Some(o) => o,
            None => return,
        };
        match self {
            LeafGroup::Data => {
                if map.is_empty() {
                    obj.remove("data");
                } else {
                    obj.insert("data".to_string(), Value::Object(map));
                }
            }
            LeafGroup::BinaryData => {
                if map.is_empty() {
                    obj.remove("binaryData");
                } else {
                    obj.insert("binaryData".to_string(), Value::Object(map));
                }
            }
            LeafGroup::Labels => {
                let meta = obj
                    .entry("metadata".to_string())
                    .or_insert_with(|| json!({}));
                if let Some(meta_obj) = meta.as_object_mut() {
                    if map.is_empty() {
                        meta_obj.remove("labels");
                    } else {
                        meta_obj.insert("labels".to_string(), Value::Object(map));
                    }
                }
            }
            LeafGroup::Annotations => {
                let meta = obj
                    .entry("metadata".to_string())
                    .or_insert_with(|| json!({}));
                if let Some(meta_obj) = meta.as_object_mut() {
                    if map.is_empty() {
                        meta_obj.remove("annotations");
                    } else {
                        meta_obj.insert("annotations".to_string(), Value::Object(map));
                    }
                }
            }
        }
    }
}

/// Per-group structural merge result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupMergeOutcome {
    /// JSON-Pointers (no leading `/`) the applying manager now owns.
    pub claimed: OwnedPaths,
    /// JSON-Pointers (no leading `/`) the applying manager has released
    /// (i.e. were previously owned by the manager but absent from `desired`).
    pub released: OwnedPaths,
    /// Conflicts encountered when `force=false`.
    pub conflicts: Vec<PathConflict>,
}

/// Merge a single string-map leaf group, returning the merged map plus
/// claimed / released / conflict bookkeeping.
///
/// # Semantics
///
/// 1. For every key present in `desired_map`:
///    - If no other manager owns the path, take the value from `desired_map`
///      and mark the path as claimed by `applying_manager`.
///    - If another manager owns the path and the value is unchanged, leave
///      it alone and re-claim it (shared ownership is allowed when values
///      match).
///    - If another manager owns the path and the value differs, record a
///      `PathConflict`. When `force=true` overwrite anyway and transfer
///      ownership; when `force=false` keep the current value (the caller
///      will surface the conflict as a 409).
/// 2. For every key in `current_map` not present in `desired_map`:
///    - If `applying_manager` is the sole previous owner, drop the key and
///      mark the path as released.
///    - Otherwise leave it alone.
///
/// `other_owners` maps JSON-Pointer → manager-name for every leaf currently
/// owned by a *different* manager than `applying_manager`. Paths that the
/// applying manager already owned are simply absent from `other_owners`.
pub fn merge_string_map_group(
    group: LeafGroup,
    current_map: &Map<String, Value>,
    desired_map: &Map<String, Value>,
    applying_manager: &str,
    other_owners: &std::collections::BTreeMap<String, String>,
    previously_owned_by_applier: &OwnedPaths,
    force: bool,
) -> (Map<String, Value>, GroupMergeOutcome) {
    let prefix = group.pointer_prefix();
    let mut merged = current_map.clone();
    let mut outcome = GroupMergeOutcome::default();

    // Pass 1 — keys in desired.
    for (key, desired_value) in desired_map {
        let path = format!("{}/{}", prefix, key);
        let current_value = current_map.get(key);
        let other_owner = other_owners.get(&path);

        match other_owner {
            Some(owner) if current_value != Some(desired_value) => {
                // Conflict: another manager owns a leaf and applier wants to
                // change it.
                outcome.conflicts.push(PathConflict {
                    path: path.clone(),
                    current_manager: owner.clone(),
                    applying_manager: applying_manager.to_string(),
                });
                if force {
                    merged.insert(key.clone(), desired_value.clone());
                    outcome.claimed.insert(path);
                }
            }
            _ => {
                // Either no other owner, or other owner with identical value.
                merged.insert(key.clone(), desired_value.clone());
                outcome.claimed.insert(path);
            }
        }
    }

    // Pass 2 — keys in current but not in desired.
    for key in current_map.keys() {
        if desired_map.contains_key(key) {
            continue;
        }
        let path = format!("{}/{}", prefix, key);
        // Drop only if the applying manager previously owned this leaf AND
        // no other manager currently owns it.
        let applier_owned = previously_owned_by_applier.contains(&path);
        let foreign_owned = other_owners.contains_key(&path);
        if applier_owned && !foreign_owned {
            merged.remove(key);
            outcome.released.insert(path);
        }
    }

    (merged, outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fields_v1_roundtrip_string_keys() {
        let mut paths = OwnedPaths::new();
        paths.insert("data/foo");
        paths.insert("data/bar");
        paths.insert("metadata/labels/app");
        paths.insert("metadata/annotations/note");

        let encoded = paths.to_fields_v1();
        // Expected shape mirrors upstream fieldsV1.
        assert_eq!(
            encoded,
            json!({
                "f:data": { "f:foo": {}, "f:bar": {} },
                "f:metadata": {
                    "f:labels":      { "f:app": {} },
                    "f:annotations": { "f:note": {} }
                }
            })
        );

        let decoded = OwnedPaths::from_fields_v1(&encoded);
        assert_eq!(decoded, paths);
    }

    #[test]
    fn fields_v1_skips_non_fieldname_discriminators() {
        // `v:` is a value-set discriminator we don't support yet. It must
        // not contaminate the OwnedPaths flat set.
        let encoded = json!({
            "f:spec": {
                "f:containers": {
                    "k:{\"name\":\"nginx\"}": {
                        "f:image": {}
                    }
                }
            }
        });
        let decoded = OwnedPaths::from_fields_v1(&encoded);
        // Nothing under the unsupported `k:` key should appear.
        assert!(decoded.is_empty(), "got: {:?}", decoded);
    }

    #[test]
    fn merge_claims_new_key_when_unowned() {
        let current = Map::new();
        let mut desired = Map::new();
        desired.insert("foo".to_string(), json!("hello"));

        let (merged, outcome) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &Default::default(),
            &OwnedPaths::new(),
            false,
        );

        assert_eq!(merged.get("foo").unwrap(), &json!("hello"));
        assert!(outcome.conflicts.is_empty());
        assert!(outcome.claimed.contains("data/foo"));
        assert!(outcome.released.is_empty());
    }

    #[test]
    fn merge_releases_key_dropped_by_owning_applier() {
        let mut current = Map::new();
        current.insert("foo".to_string(), json!("old"));
        let desired = Map::new();

        let mut previously_owned = OwnedPaths::new();
        previously_owned.insert("data/foo");

        let (merged, outcome) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &Default::default(),
            &previously_owned,
            false,
        );

        assert!(!merged.contains_key("foo"));
        assert!(outcome.released.contains("data/foo"));
        assert!(outcome.claimed.is_empty());
    }

    #[test]
    fn merge_conflict_when_other_owner_and_value_differs() {
        let mut current = Map::new();
        current.insert("foo".to_string(), json!("controller-set"));
        let mut desired = Map::new();
        desired.insert("foo".to_string(), json!("kubectl-set"));

        let mut others = std::collections::BTreeMap::new();
        others.insert("data/foo".to_string(), "controller".to_string());

        // No force.
        let (merged, outcome) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &others,
            &OwnedPaths::new(),
            false,
        );

        assert_eq!(merged.get("foo").unwrap(), &json!("controller-set"));
        assert_eq!(outcome.conflicts.len(), 1);
        assert_eq!(outcome.conflicts[0].current_manager, "controller");
        assert_eq!(outcome.conflicts[0].applying_manager, "kubectl");
        assert!(outcome.claimed.is_empty());

        // Force.
        let (merged_forced, outcome_forced) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &others,
            &OwnedPaths::new(),
            true,
        );
        assert_eq!(merged_forced.get("foo").unwrap(), &json!("kubectl-set"));
        // Conflict still reported even under force so the caller can audit.
        assert_eq!(outcome_forced.conflicts.len(), 1);
        assert!(outcome_forced.claimed.contains("data/foo"));
    }

    #[test]
    fn merge_shared_ownership_when_value_matches() {
        let mut current = Map::new();
        current.insert("foo".to_string(), json!("same"));
        let mut desired = Map::new();
        desired.insert("foo".to_string(), json!("same"));

        let mut others = std::collections::BTreeMap::new();
        others.insert("data/foo".to_string(), "controller".to_string());

        let (merged, outcome) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &others,
            &OwnedPaths::new(),
            false,
        );
        assert_eq!(merged.get("foo").unwrap(), &json!("same"));
        assert!(outcome.conflicts.is_empty());
        assert!(outcome.claimed.contains("data/foo"));
    }
}
