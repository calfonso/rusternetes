//! Server-Side Apply (SSA) scaffold.
//!
//! This is the in-tree SSA implementation that the `apply-patch+yaml` and
//! `apply-patch+json` PATCH paths route to. It is **schema-driven**: each
//! supported resource registers a [`merge::ResourceLeafSchema`] describing
//! which string-map groups exist on it (`data`, `binaryData`, …) and which
//! top-level scalar leaves an Apply may touch (e.g. `Secret.type`). The
//! merge driver iterates the schema rather than hard-coding per-kind logic.
//!
//! Two resource schemas are registered today:
//!
//! - [`merge::CONFIGMAP_SCHEMA`] — `data`, `binaryData`,
//!   `metadata.labels`, `metadata.annotations`. No atomic leaves outside
//!   `metadata.*`.
//! - [`merge::SECRET_SCHEMA`]    — `data`, `stringData`,
//!   `metadata.labels`, `metadata.annotations`. Atomic leaf: `type`.
//!
//! Both schemas cover:
//!
//! - granular string-map merge with per-key ownership
//! - `metadata.managedFields[*]` ownership bookkeeping with a `fieldsV1` tree
//! - conflict detection vs. `?force=true` resolution
//! - `immutable: true` post-create fence (enforced by the HTTP handler that
//!   calls SSA, not by the merge core itself).
//!
//! The PATCH handlers for Pod / Deployment / Service / etc. continue to
//! delegate to the legacy [`rusternetes_common::server_side_apply`] codepath
//! which uses top-level string-keyed ownership. The schema mechanism is
//! deliberately a thin curated alternative to the upstream OpenAPI-driven
//! type converter
//! (`staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/typeconverter.go`)
//! — we ship a hand-written leaf schema per supported resource instead of
//! shipping the full schema-walk machinery.
//!
//! # Upstream references
//!
//! - `staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/`
//! - `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go`
//! - `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go`
//!   (the `mutateObjectUpdateFn` that runs SSA before storage Update.)
//! - `pkg/registry/core/secret/strategy.go::ValidateUpdate` — pin for the
//!   `Secret.type` immutability fence.
//!
//! # Wire format
//!
//! Two content-types map onto this module:
//!
//! | Content-Type                       | Body shape          |
//! |------------------------------------|---------------------|
//! | `application/apply-patch+yaml`     | YAML document       |
//! | `application/apply-patch+json`     | JSON document       |
//!
//! Both are decoded into `serde_json::Value` and handed to one of the
//! resource-specific shims ([`apply_configmap`], [`apply_secret`]) which
//! delegate to [`apply_via_schema`].

pub mod merge;

use chrono::Utc;
use rusternetes_common::resources::{ConfigMap, Secret};
use rusternetes_common::types::ManagedFieldsEntry;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use thiserror::Error;

use self::merge::{
    merge_string_map_group, LeafGroup, OwnedPaths, PathConflict, ResourceLeafSchema,
    CONFIGMAP_SCHEMA, SECRET_SCHEMA,
};

/// Per-request SSA options sourced from query parameters.
///
/// `fieldManager` is required by upstream when `Content-Type` is
/// `apply-patch+*`; the handler should reject the request with HTTP 400
/// before constructing this struct if it is missing.
#[derive(Debug, Clone)]
pub struct ApplyOptions {
    /// The manager name claiming ownership (`?fieldManager=`).
    pub field_manager: String,
    /// Whether to force-resolve conflicts (`?force=true`).
    pub force: bool,
}

impl ApplyOptions {
    pub fn new(field_manager: impl Into<String>) -> Self {
        Self {
            field_manager: field_manager.into(),
            force: false,
        }
    }

    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }
}

/// Outcome of a server-side apply against an existing or new object.
///
/// The object is boxed to keep the enum compact: a typed Kubernetes resource
/// (TypeMeta + ObjectMeta + payload + managedFields) is hundreds of bytes,
/// so an unboxed variant would make every `Conflicts` value carry the same
/// dead weight and trip clippy's `large_enum_variant`.
#[derive(Debug)]
pub enum ApplyOutcome<T> {
    /// The merge succeeded. The contained object is ready for persistence.
    /// `created` is true when there was no previous object — the caller
    /// should return HTTP 201; otherwise HTTP 200.
    Applied { object: Box<T>, created: bool },

    /// One or more leaves are owned by other managers and `force` was not
    /// set. The caller should translate this into HTTP 409 with an Apply
    /// conflict status body.
    Conflicts(Vec<PathConflict>),
}

/// Errors that can be returned by the `apply_*` family.
#[derive(Debug, Error)]
pub enum ApplyError {
    #[error("apply body is not a valid {kind}: {message}")]
    InvalidBody { kind: &'static str, message: String },

    #[error("internal serialisation error: {0}")]
    Internal(String),
}

impl ApplyError {
    fn invalid_body(kind: &'static str, message: impl Into<String>) -> Self {
        ApplyError::InvalidBody {
            kind,
            message: message.into(),
        }
    }
}

/// Apply a desired ConfigMap on top of an optional current ConfigMap.
///
/// Thin shim — see [`apply_via_schema`] for the actual algorithm. Retained
/// as a named entry-point so callers (and tests) that pre-date the
/// schema-driven refactor keep working.
pub fn apply_configmap(
    current: Option<&ConfigMap>,
    desired: &Value,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome<ConfigMap>, ApplyError> {
    apply_via_schema::<ConfigMap>(current, desired, &CONFIGMAP_SCHEMA, opts)
}

/// Apply a desired Secret on top of an optional current Secret.
///
/// Thin shim — see [`apply_via_schema`] for the actual algorithm. The
/// caller is responsible for the post-merge immutability fence (parity
/// with [`apply_configmap`]'s call sites) and for running
/// [`Secret::normalize`] which folds `stringData` into `data` for storage.
pub fn apply_secret(
    current: Option<&Secret>,
    desired: &Value,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome<Secret>, ApplyError> {
    apply_via_schema::<Secret>(current, desired, &SECRET_SCHEMA, opts)
}

/// Apply a desired object on top of an optional current object, driven by
/// the resource's declarative [`ResourceLeafSchema`].
///
/// `T` is the concrete Kubernetes resource type — used only for the final
/// JSON → struct decode so the handler gets a typed object back.
///
/// # Algorithm
///
/// 1. Decode `current` (if any) into a JSON tree and pull out the existing
///    `metadata.managedFields` array.
/// 2. Build a per-manager ownership index. For every leaf currently owned
///    by a manager *other* than `opts.field_manager`, record (path →
///    manager) so the merge primitive can detect conflicts.
/// 3. For each string-map group in the schema, call
///    [`merge::merge_string_map_group`] to compute the merged map plus the
///    claimed / released / conflict deltas. Then run the same merge for
///    each atomic leaf in `schema.atomic_leaves`.
/// 4. If any group produced conflicts and `force=false`, return
///    `ApplyOutcome::Conflicts` without mutating anything.
/// 5. Otherwise rebuild the JSON tree: replace each merged map / atomic,
///    then rewrite `metadata.managedFields` so that:
///      - `opts.field_manager` now owns the union of paths it owned before
///        minus released paths plus newly-claimed paths;
///      - other managers lose ownership of any path the applier claimed
///        under `force=true`;
///      - the applier's entry has `operation=Apply`, `apiVersion=v1`,
///        `time=now`.
/// 6. Decode the rebuilt JSON tree back into the typed `T`.
pub fn apply_via_schema<T>(
    current: Option<&T>,
    desired: &Value,
    schema: &ResourceLeafSchema,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome<T>, ApplyError>
where
    T: Serialize + DeserializeOwned,
{
    // --- 0. Sanity-check the desired body. It must at least be an object.
    let desired_obj = desired
        .as_object()
        .ok_or_else(|| ApplyError::invalid_body(schema.kind, "expected a JSON object"))?;
    if desired_obj.is_empty() {
        return Err(ApplyError::invalid_body(
            schema.kind,
            "apply body must not be empty",
        ));
    }

    // --- 1. Lift `current` into a mutable JSON tree we can rebuild from.
    let mut working: Value = match current {
        Some(c) => serde_json::to_value(c)
            .map_err(|e| ApplyError::Internal(format!("encode current: {e}")))?,
        None => {
            // Bootstrap: copy desired wholesale. This is the upstream
            // "CreateOnApply" branch.
            let mut obj = desired.clone();
            ensure_apiversion_kind(&mut obj, schema);
            // Strip any client-sent managedFields — we set it ourselves.
            if let Some(meta) = obj
                .as_object_mut()
                .and_then(|o| o.get_mut("metadata"))
                .and_then(|m| m.as_object_mut())
            {
                meta.remove("managedFields");
            }
            return finalise_initial_apply::<T>(obj, desired, schema, opts);
        }
    };

    // --- 2. Build per-leaf ownership map for managers other than the applier.
    let existing_entries = extract_managed_fields(&working);
    let mut other_owners: BTreeMap<String, String> = BTreeMap::new();
    let mut previously_owned_by_applier = OwnedPaths::new();
    for entry in &existing_entries {
        let manager = entry.manager.clone().unwrap_or_default();
        let Some(fields_v1) = &entry.fields_v1 else {
            continue;
        };
        let paths = OwnedPaths::from_fields_v1(fields_v1);
        if manager == opts.field_manager {
            for p in paths.iter() {
                previously_owned_by_applier.insert(p.clone());
            }
        } else {
            for p in paths.iter() {
                // First-writer wins for duplicate ownership of the same path.
                // Upstream allows shared ownership when values match; we
                // model that by only recording one "other" owner per path.
                other_owners
                    .entry(p.clone())
                    .or_insert_with(|| manager.clone());
            }
        }
    }

    // --- 3a. Merge each string-map group declared by the schema.
    let mut all_conflicts: Vec<PathConflict> = Vec::new();
    let mut all_claimed = OwnedPaths::new();
    let mut all_released = OwnedPaths::new();

    let mut new_maps: Vec<(LeafGroup, Map<String, Value>)> =
        Vec::with_capacity(schema.string_map_groups.len());
    for &group in schema.string_map_groups {
        let current_map = group.extract_map(&working);
        let desired_map = group.extract_map(desired);
        let (merged, outcome) = merge_string_map_group(
            group,
            &current_map,
            &desired_map,
            &opts.field_manager,
            &other_owners,
            &previously_owned_by_applier,
            opts.force,
        );
        all_conflicts.extend(outcome.conflicts);
        for p in outcome.claimed.iter() {
            all_claimed.insert(p.clone());
        }
        for p in outcome.released.iter() {
            all_released.insert(p.clone());
        }
        new_maps.push((group, merged));
    }

    // --- 3b. Merge each atomic top-level scalar leaf in the schema.
    let mut atomic_overrides: Vec<(&'static str, Option<Value>)> = Vec::new();
    for &leaf in schema.atomic_leaves {
        let current_value = working.get(leaf).cloned();
        let desired_value = desired.get(leaf).cloned();
        let outcome = merge_atomic_leaf(
            leaf,
            current_value.as_ref(),
            desired_value.as_ref(),
            &opts.field_manager,
            &other_owners,
            &previously_owned_by_applier,
            opts.force,
        );
        all_conflicts.extend(outcome.conflicts);
        for p in outcome.claimed.iter() {
            all_claimed.insert(p.clone());
        }
        for p in outcome.released.iter() {
            all_released.insert(p.clone());
        }
        if let Some(new_value) = outcome.new_value {
            atomic_overrides.push((leaf, new_value));
        }
    }

    // --- 4. Bail out on conflicts unless force is set.
    if !all_conflicts.is_empty() && !opts.force {
        return Ok(ApplyOutcome::Conflicts(all_conflicts));
    }

    // --- 5. Commit merged maps and rebuild managedFields.
    for (group, map) in new_maps {
        group.set_map(&mut working, map);
    }
    if let Some(working_obj) = working.as_object_mut() {
        for (leaf, new_value) in atomic_overrides {
            match new_value {
                Some(v) => {
                    working_obj.insert(leaf.to_string(), v);
                }
                None => {
                    working_obj.remove(leaf);
                }
            }
        }
    }
    // When force=true and we did override another manager's leaves, strip
    // those leaves from the other manager's ownership.
    let claimed_paths: std::collections::BTreeSet<&String> = all_claimed.iter().collect();
    let updated_entries = rewrite_managed_fields(
        &existing_entries,
        &opts.field_manager,
        &all_claimed,
        &all_released,
        &previously_owned_by_applier,
        opts.force,
        &claimed_paths,
    );
    set_managed_fields(&mut working, &updated_entries)
        .map_err(|e| ApplyError::Internal(format!("set managedFields: {e}")))?;

    // --- 6. Decode back into the typed resource.
    let object: T = serde_json::from_value(working)
        .map_err(|e| ApplyError::Internal(format!("decode merged {}: {e}", schema.kind)))?;
    Ok(ApplyOutcome::Applied {
        object: Box::new(object),
        created: false,
    })
}

fn finalise_initial_apply<T>(
    mut working: Value,
    desired: &Value,
    schema: &ResourceLeafSchema,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome<T>, ApplyError>
where
    T: DeserializeOwned,
{
    // For a brand-new object, the applier owns every leaf it set — both
    // string-map keys and any atomic leaves declared by the schema that
    // the desired body actually populated.
    let mut claimed = OwnedPaths::new();
    for &group in schema.string_map_groups {
        let desired_map = group.extract_map(desired);
        let prefix = group.pointer_prefix();
        for key in desired_map.keys() {
            claimed.insert(format!("{}/{}", prefix, key));
        }
    }
    for &leaf in schema.atomic_leaves {
        if desired.get(leaf).is_some() {
            claimed.insert(leaf.to_string());
        }
    }

    let entry = ManagedFieldsEntry {
        manager: Some(opts.field_manager.clone()),
        operation: Some("Apply".to_string()),
        api_version: Some("v1".to_string()),
        time: Some(Utc::now()),
        fields_type: Some("FieldsV1".to_string()),
        fields_v1: Some(claimed.to_fields_v1()),
        subresource: None,
    };
    set_managed_fields(&mut working, &[entry])
        .map_err(|e| ApplyError::Internal(format!("set managedFields: {e}")))?;

    let object: T = serde_json::from_value(working)
        .map_err(|e| ApplyError::Internal(format!("decode applied {}: {e}", schema.kind)))?;
    Ok(ApplyOutcome::Applied {
        object: Box::new(object),
        created: true,
    })
}

fn ensure_apiversion_kind(obj: &mut Value, schema: &ResourceLeafSchema) {
    let Some(map) = obj.as_object_mut() else {
        return;
    };
    map.entry("apiVersion".to_string())
        .or_insert_with(|| json!("v1"));
    map.entry("kind".to_string())
        .or_insert_with(|| json!(schema.kind));
}

/// Per-leaf merge outcome for a top-level atomic scalar field.
#[derive(Debug, Default)]
struct AtomicMergeOutcome {
    /// `Some(Some(v))` → set leaf to v. `Some(None)` → delete leaf.
    /// `None` → leave as-is.
    new_value: Option<Option<Value>>,
    claimed: OwnedPaths,
    released: OwnedPaths,
    conflicts: Vec<PathConflict>,
}

/// Merge a single top-level atomic leaf (e.g. `Secret.type`).
///
/// Atomic semantics: the leaf is either owned in full by one manager or not
/// owned at all. There's no per-byte ownership inside the value. If the
/// applier sets a value:
///
/// - no other owner → claim it.
/// - another owner, same value → shared ownership (claim, no conflict).
/// - another owner, different value → conflict (force=true overrides).
///
/// If the applier previously owned the leaf and `desired` omits it, the
/// leaf is released and removed from the working object.
fn merge_atomic_leaf(
    leaf: &str,
    current_value: Option<&Value>,
    desired_value: Option<&Value>,
    applying_manager: &str,
    other_owners: &BTreeMap<String, String>,
    previously_owned_by_applier: &OwnedPaths,
    force: bool,
) -> AtomicMergeOutcome {
    let path = leaf.to_string();
    let other_owner = other_owners.get(&path);
    let mut outcome = AtomicMergeOutcome::default();

    match desired_value {
        Some(dv) => match other_owner {
            Some(owner) if current_value != Some(dv) => {
                outcome.conflicts.push(PathConflict {
                    path: path.clone(),
                    current_manager: owner.clone(),
                    applying_manager: applying_manager.to_string(),
                });
                if force {
                    outcome.new_value = Some(Some(dv.clone()));
                    outcome.claimed.insert(path);
                }
            }
            _ => {
                outcome.new_value = Some(Some(dv.clone()));
                outcome.claimed.insert(path);
            }
        },
        None => {
            // Applier dropped the leaf. Only act if the applier was the
            // sole previous owner — never silently remove a leaf owned by
            // someone else.
            let applier_owned = previously_owned_by_applier.contains(&path);
            let foreign_owned = other_owners.contains_key(&path);
            if applier_owned && !foreign_owned {
                outcome.new_value = Some(None);
                outcome.released.insert(path);
            }
        }
    }

    outcome
}

fn extract_managed_fields(resource: &Value) -> Vec<ManagedFieldsEntry> {
    let raw = resource
        .get("metadata")
        .and_then(|m| m.get("managedFields"));
    let Some(raw) = raw else {
        return Vec::new();
    };
    serde_json::from_value(raw.clone()).unwrap_or_default()
}

fn set_managed_fields(
    resource: &mut Value,
    entries: &[ManagedFieldsEntry],
) -> Result<(), serde_json::Error> {
    let Some(obj) = resource.as_object_mut() else {
        return Ok(());
    };
    let meta = obj
        .entry("metadata".to_string())
        .or_insert_with(|| json!({}));
    let Some(meta_obj) = meta.as_object_mut() else {
        return Ok(());
    };
    if entries.is_empty() {
        meta_obj.remove("managedFields");
    } else {
        meta_obj.insert("managedFields".to_string(), serde_json::to_value(entries)?);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn rewrite_managed_fields(
    existing: &[ManagedFieldsEntry],
    applying_manager: &str,
    newly_claimed: &OwnedPaths,
    released: &OwnedPaths,
    previously_owned_by_applier: &OwnedPaths,
    force: bool,
    forced_paths: &std::collections::BTreeSet<&String>,
) -> Vec<ManagedFieldsEntry> {
    let mut out: Vec<ManagedFieldsEntry> = Vec::new();
    let mut applier_seen = false;

    for entry in existing {
        let manager = entry.manager.clone().unwrap_or_default();
        let mut paths = entry
            .fields_v1
            .as_ref()
            .map(OwnedPaths::from_fields_v1)
            .unwrap_or_default();

        if manager == applying_manager {
            applier_seen = true;
            // Recompute applier ownership from scratch: previously owned
            // minus released plus newly-claimed.
            let mut next = OwnedPaths::new();
            for p in previously_owned_by_applier.iter() {
                if !released.contains(p) {
                    next.insert(p.clone());
                }
            }
            for p in newly_claimed.iter() {
                next.insert(p.clone());
            }
            paths = next;
        } else if force {
            // Strip any leaves the applier just claimed under force.
            let stripped: std::collections::BTreeSet<String> = paths
                .iter()
                .filter(|p| !forced_paths.contains(p))
                .cloned()
                .collect();
            paths = OwnedPaths(stripped);
        }

        // Drop the entry entirely if it owns nothing.
        if paths.is_empty() {
            if manager == applying_manager {
                // Applier should still show up unless it owns truly nothing
                // — but the upstream behaviour is to drop empty entries.
                continue;
            } else {
                continue;
            }
        }

        let mut next_entry = entry.clone();
        next_entry.fields_v1 = Some(paths.to_fields_v1());
        if manager == applying_manager {
            next_entry.operation = Some("Apply".to_string());
            next_entry.api_version = Some("v1".to_string());
            next_entry.time = Some(Utc::now());
            next_entry.fields_type = Some("FieldsV1".to_string());
            next_entry.manager = Some(applying_manager.to_string());
        }
        out.push(next_entry);
    }

    if !applier_seen && !newly_claimed.is_empty() {
        out.push(ManagedFieldsEntry {
            manager: Some(applying_manager.to_string()),
            operation: Some("Apply".to_string()),
            api_version: Some("v1".to_string()),
            time: Some(Utc::now()),
            fields_type: Some("FieldsV1".to_string()),
            fields_v1: Some(newly_claimed.to_fields_v1()),
            subresource: None,
        });
    }

    out
}

/// Parse an `apply-patch+yaml` or `apply-patch+json` request body into a
/// generic JSON tree. This is the single entry point the HTTP handler should
/// use — it accepts either format transparently.
///
/// YAML is decoded via `serde_yaml` (already an indirect dependency through
/// `rusternetes-common`). JSON is decoded directly.
pub fn decode_apply_body(content_type: &str, body: &[u8]) -> Result<Value, ApplyError> {
    if content_type.contains("apply-patch+yaml") || content_type.contains("+yaml") {
        serde_yaml::from_slice::<Value>(body)
            .map_err(|e| ApplyError::invalid_body("Resource", format!("invalid YAML: {e}")))
    } else {
        serde_json::from_slice::<Value>(body)
            .map_err(|e| ApplyError::invalid_body("Resource", format!("invalid JSON: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_yaml_body() {
        let body = b"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: foo\n";
        let v = decode_apply_body("application/apply-patch+yaml", body).unwrap();
        assert_eq!(v["metadata"]["name"], "foo");
    }

    #[test]
    fn decode_json_body() {
        let body = br#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"foo"}}"#;
        let v = decode_apply_body("application/apply-patch+json", body).unwrap();
        assert_eq!(v["metadata"]["name"], "foo");
    }

    #[test]
    fn empty_body_rejected() {
        let result = apply_configmap(None, &json!({}), &ApplyOptions::new("kubectl"));
        assert!(matches!(result, Err(ApplyError::InvalidBody { .. })));
    }

    #[test]
    fn empty_secret_body_rejected() {
        let result = apply_secret(None, &json!({}), &ApplyOptions::new("kubectl"));
        assert!(matches!(result, Err(ApplyError::InvalidBody { .. })));
    }
}
