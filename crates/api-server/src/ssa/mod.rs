//! Server-Side Apply (SSA) scaffold.
//!
//! This is the in-tree SSA implementation that the `apply-patch+yaml` and
//! `apply-patch+json` PATCH paths route to. Today it only knows how to apply
//! a single resource type — **ConfigMap** — because that is the smallest
//! resource that fully exercises:
//!
//! - granular string-map merge (`data`, `binaryData`, `metadata.labels`,
//!   `metadata.annotations`)
//! - `metadata.managedFields[*]` ownership bookkeeping with a `fieldsV1`
//!   tree
//! - conflict detection vs. `?force=true` resolution
//!
//! Everything else is intentionally a TODO. The PATCH handlers for Pod /
//! Deployment / Service / etc. continue to delegate to the legacy
//! [`rusternetes_common::server_side_apply`] codepath which uses top-level
//! string-keyed ownership.
//!
//! # Upstream references
//!
//! - `staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/`
//! - `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go`
//! - `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go`
//!   (the `mutateObjectUpdateFn` that runs SSA before storage Update.)
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
//! Both are decoded into `serde_json::Value` and handed to
//! [`apply_configmap`].

pub mod merge;

use chrono::Utc;
use rusternetes_common::resources::ConfigMap;
use rusternetes_common::types::ManagedFieldsEntry;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use thiserror::Error;

use self::merge::{merge_string_map_group, LeafGroup, OwnedPaths, PathConflict};

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
/// `ConfigMap` carries ~500 bytes inline (TypeMeta + ObjectMeta + ManagedFields
/// vec headers), so [`ApplyOutcome::Applied`] boxes it to keep the enum small —
/// otherwise every `Conflicts` variant carries that dead weight and clippy
/// (rightly) trips `large_enum_variant`.
#[derive(Debug)]
pub enum ApplyOutcome {
    /// The merge succeeded. The contained ConfigMap is ready for persistence.
    /// `created` is true when there was no previous object — the caller
    /// should return HTTP 201; otherwise HTTP 200.
    Applied {
        object: Box<ConfigMap>,
        created: bool,
    },

    /// One or more leaves are owned by other managers and `force` was not
    /// set. The caller should translate this into HTTP 409 with an Apply
    /// conflict status body.
    Conflicts(Vec<PathConflict>),
}

/// Errors that can be returned by [`apply_configmap`].
#[derive(Debug, Error)]
pub enum ApplyError {
    #[error("apply body is not a valid ConfigMap: {0}")]
    InvalidBody(String),

    #[error("internal serialisation error: {0}")]
    Internal(String),
}

/// Apply a desired ConfigMap on top of an optional current ConfigMap.
///
/// `current` is `None` when the object does not yet exist — in that case the
/// outcome is always [`ApplyOutcome::Applied { created: true, .. }`].
///
/// # Algorithm
///
/// 1. Decode `current` (if any) into a JSON tree and pull out the existing
///    `metadata.managedFields` array.
/// 2. Build a per-manager ownership index. For every leaf currently owned
///    by a manager *other* than `opts.field_manager`, record (path →
///    manager) so the merge primitive can detect conflicts.
/// 3. For each of the four ConfigMap leaf groups (`data`, `binaryData`,
///    `metadata.labels`, `metadata.annotations`), call
///    [`merge::merge_string_map_group`] to compute the merged map plus the
///    claimed / released / conflict deltas.
/// 4. If any group produced conflicts and `force=false`, return
///    `ApplyOutcome::Conflicts` without mutating anything.
/// 5. Otherwise rebuild the JSON tree: replace each merged map, then
///    rewrite `metadata.managedFields` so that:
///      - `opts.field_manager` now owns the union of paths it owned before
///        minus released paths plus newly-claimed paths;
///      - other managers lose ownership of any path the applier claimed
///        under `force=true`;
///      - the applier's entry has `operation=Apply`, `apiVersion=v1`,
///        `time=now`.
/// 6. Decode the rebuilt JSON tree back into a `ConfigMap`.
pub fn apply_configmap(
    current: Option<&ConfigMap>,
    desired: &Value,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome, ApplyError> {
    // --- 0. Sanity-check the desired body. It must at least be an object.
    let desired_obj = desired
        .as_object()
        .ok_or_else(|| ApplyError::InvalidBody("expected a JSON object".to_string()))?;
    if desired_obj.is_empty() {
        return Err(ApplyError::InvalidBody(
            "apply body must not be empty".to_string(),
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
            ensure_apiversion_kind(&mut obj);
            // Strip any client-sent managedFields — we set it ourselves.
            if let Some(meta) = obj
                .as_object_mut()
                .and_then(|o| o.get_mut("metadata"))
                .and_then(|m| m.as_object_mut())
            {
                meta.remove("managedFields");
            }
            return finalise_initial_apply(obj, desired, opts);
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

    // --- 3. Merge each leaf group.
    let mut all_conflicts: Vec<PathConflict> = Vec::new();
    let mut all_claimed = OwnedPaths::new();
    let mut all_released = OwnedPaths::new();

    let groups = [
        LeafGroup::Data,
        LeafGroup::BinaryData,
        LeafGroup::Labels,
        LeafGroup::Annotations,
    ];

    let mut new_maps: Vec<(LeafGroup, Map<String, Value>)> = Vec::with_capacity(groups.len());
    for group in groups {
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

    // --- 4. Bail out on conflicts unless force is set.
    if !all_conflicts.is_empty() && !opts.force {
        return Ok(ApplyOutcome::Conflicts(all_conflicts));
    }

    // --- 5. Commit merged maps and rebuild managedFields.
    for (group, map) in new_maps {
        group.set_map(&mut working, map);
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

    // --- 6. Decode back into a typed ConfigMap.
    let object: ConfigMap = serde_json::from_value(working)
        .map_err(|e| ApplyError::Internal(format!("decode merged ConfigMap: {e}")))?;
    Ok(ApplyOutcome::Applied {
        object: Box::new(object),
        created: false,
    })
}

fn finalise_initial_apply(
    mut working: Value,
    desired: &Value,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome, ApplyError> {
    // For a brand-new object, the applier owns every leaf it set.
    let mut claimed = OwnedPaths::new();
    for group in [
        LeafGroup::Data,
        LeafGroup::BinaryData,
        LeafGroup::Labels,
        LeafGroup::Annotations,
    ] {
        let desired_map = group.extract_map(desired);
        let prefix = group.pointer_prefix();
        for key in desired_map.keys() {
            claimed.insert(format!("{}/{}", prefix, key));
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

    let object: ConfigMap = serde_json::from_value(working)
        .map_err(|e| ApplyError::Internal(format!("decode applied ConfigMap: {e}")))?;
    Ok(ApplyOutcome::Applied {
        object: Box::new(object),
        created: true,
    })
}

fn ensure_apiversion_kind(obj: &mut Value) {
    let Some(map) = obj.as_object_mut() else {
        return;
    };
    map.entry("apiVersion".to_string())
        .or_insert_with(|| json!("v1"));
    map.entry("kind".to_string())
        .or_insert_with(|| json!("ConfigMap"));
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
            .map_err(|e| ApplyError::InvalidBody(format!("invalid YAML: {e}")))
    } else {
        serde_json::from_slice::<Value>(body)
            .map_err(|e| ApplyError::InvalidBody(format!("invalid JSON: {e}")))
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
        assert!(matches!(result, Err(ApplyError::InvalidBody(_))));
    }
}
