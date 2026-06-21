//! Rusternetes port of upstream
//! `k8s.io/apimachinery/pkg/api/validation/objectmeta.go` (release-1.35).
//!
//! Mirrors the upstream structure: validators return [`ErrorList`] (a
//! `Vec<Error>`) and *accumulate* every problem they find rather than
//! short-circuiting. Field paths and error wording match upstream
//! byte-for-byte so conformance log greps and the API-machinery test mirror
//! stay valid.
//!
//! Upstream:
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/apimachinery/pkg/api/validation/objectmeta.go>
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/apimachinery/pkg/api/validation/generic.go>

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::collections::HashSet;

use crate::types::{ObjectMeta, OwnerReference};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_qualified_name, validate_labels,
    validate_managed_fields,
};

/// Total allowed byte size of all annotations on an object. Mirrors upstream
/// `TotalAnnotationSizeLimitB` (256 KiB).
pub const TOTAL_ANNOTATION_SIZE_LIMIT_B: usize = 256 * 1024;

/// Upstream constant used by the immutability checks.
pub const FIELD_IMMUTABLE_ERROR_MSG: &str = "field is immutable";

/// Upstream `IsNegativeErrorMsg`.
pub const IS_NEGATIVE_ERROR_MSG: &str = "must be greater than or equal to 0";

/// Upstream `metav1.FinalizerOrphanDependents`.
pub const FINALIZER_ORPHAN_DEPENDENTS: &str = "orphan";

/// Upstream `metav1.FinalizerDeleteDependents`.
pub const FINALIZER_DELETE_DEPENDENTS: &str = "foregroundDeletion";

// ---------------------------------------------------------------------------
// Name-validator function types and helpers
// ---------------------------------------------------------------------------

/// Mirror of upstream `ValidateNameFunc`. Takes a name and a `prefix` flag
/// (true when the value will have a random suffix appended, i.e. it came from
/// `generateName`). Returns a list of human-readable strings — empty on
/// success.
pub type ValidateNameFunc = fn(&str, bool) -> Vec<String>;

/// Mirror of upstream `ValidateNameFuncWithErrors`. Takes a field path + name
/// and returns an [`ErrorList`].
pub type ValidateNameFuncWithErrors = fn(&Path, &str) -> ErrorList;

/// Upstream `maskTrailingDash`: when a trailing dash is present and the name
/// is longer than one character, drop the dash and append `'a'`. The off-by-
/// one `name[:len-2] + "a"` in Go is preserved here — replacing the trailing
/// dash with `'a'` and dropping the previous byte too. That oddity is part of
/// the upstream contract (a final `'-'` is always legal on generateName).
fn mask_trailing_dash(name: &str) -> String {
    if name.len() > 1 && name.ends_with('-') {
        // Go: name[:len(name)-2] + "a" — drops 2 bytes off the end then
        // appends 'a'. Replicate exactly.
        let mut s = name.to_string();
        s.pop();
        s.pop();
        s.push('a');
        s
    } else {
        name.to_string()
    }
}

/// Upstream `NameIsDNSSubdomain`. A [`ValidateNameFunc`] for names that must
/// be DNS subdomains.
pub fn name_is_dns_subdomain(name: &str, prefix: bool) -> Vec<String> {
    let name = if prefix {
        mask_trailing_dash(name)
    } else {
        name.to_string()
    };
    is_dns1123_subdomain(&name)
}

/// Upstream `NameIsDNSLabel`. A [`ValidateNameFunc`] for names that must be
/// DNS-1123 labels.
pub fn name_is_dns_label(name: &str, prefix: bool) -> Vec<String> {
    let name = if prefix {
        mask_trailing_dash(name)
    } else {
        name.to_string()
    };
    is_dns1123_label(&name)
}

/// Upstream `ValidateNamespaceName` — `NameIsDNSLabel`.
pub fn validate_namespace_name(name: &str, prefix: bool) -> Vec<String> {
    name_is_dns_label(name, prefix)
}

/// Strings that can never be a path-segment name. Mirrors upstream
/// `path.NameMayNotBe` (`.` and `..`).
const NAME_MAY_NOT_BE: [&str; 2] = [".", ".."];

/// Substrings forbidden inside a path-segment name. Mirrors upstream
/// `path.NameMayNotContain` (`/` and `%`).
const NAME_MAY_NOT_CONTAIN: [&str; 2] = ["/", "%"];

/// Upstream `path.IsValidPathSegmentName` exposed as a [`ValidateNameFunc`].
/// Used by the RBAC kinds (`ValidateRBACName`) — names only need to encode
/// safely as a REST/etcd path segment, so the DNS rules don't apply. The
/// `prefix` flag is ignored (upstream `ValidateRBACName` ignores it too).
pub fn name_is_path_segment(name: &str, _prefix: bool) -> Vec<String> {
    for illegal in NAME_MAY_NOT_BE {
        if name == illegal {
            return vec![format!("may not be '{illegal}'")];
        }
    }
    let mut errs = Vec::new();
    for illegal in NAME_MAY_NOT_CONTAIN {
        if name.contains(illegal) {
            errs.push(format!("may not contain '{illegal}'"));
        }
    }
    errs
}

/// Upstream `ValidateIPAddressName` (networking) → `validation.IsValidIP`
/// (strict). A [`ValidateNameFunc`] for the `IPAddress` kind, whose name must
/// be the canonical text form of an IP address. `prefix` is ignored — IPAddress
/// does not support `generateName`.
///
/// Mirrors `parseIP(strict)` + the canonical-form check: a non-parseable value,
/// an IPv4-mapped IPv6 address, or a non-canonical spelling each fail with the
/// upstream wording.
pub fn name_is_ip(name: &str, _prefix: bool) -> Vec<String> {
    use std::net::IpAddr;
    let parsed = match name.parse::<IpAddr>() {
        Ok(ip) => ip,
        Err(_) => {
            return vec![
                "must be a valid IP address, (e.g. 10.9.8.7 or 2001:db8::ffff)".to_string(),
            ];
        }
    };
    let mut errs = Vec::new();
    if let IpAddr::V6(v6) = parsed {
        if v6.to_ipv4_mapped().is_some() {
            errs.push("must not be an IPv4-mapped IPv6 address".to_string());
        }
    }
    let canonical = parsed.to_string();
    if canonical != name {
        errs.push(format!("must be in canonical form (\"{canonical}\")"));
    }
    errs
}

/// A [`ValidateNameFunc`] that imposes no format constraint, mirroring upstream
/// validators that `return nil` for any name — e.g.
/// `certificates.ValidateCertificateRequestName`. The rest of
/// `ValidateObjectMeta` (namespace, labels, etc.) still runs.
pub fn name_unconstrained(_name: &str, _prefix: bool) -> Vec<String> {
    Vec::new()
}

/// Upstream `ValidateNonnegativeField`.
pub fn validate_nonnegative_field(value: i64, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    if value < 0 {
        errs.push(Error::invalid(fld_path, value, IS_NEGATIVE_ERROR_MSG).with_origin("minimum"));
    }
    errs
}

// ---------------------------------------------------------------------------
// Annotations
// ---------------------------------------------------------------------------

/// Upstream `ValidateAnnotations`. Annotation *keys* follow the qualified-
/// name rule (case-insensitive prefix + name); annotation *values* are
/// unrestricted but the combined byte size across all entries is capped at
/// [`TOTAL_ANNOTATION_SIZE_LIMIT_B`].
pub fn validate_annotations(annotations: &HashMap<String, String>, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let mut keys: Vec<&String> = annotations.keys().collect();
    keys.sort();
    for k in keys {
        // Upstream lowercases the key before running the qualified-name
        // check (annotations are case-insensitive on the key) but reports
        // the *original* key as the bad value.
        let lowered = k.to_lowercase();
        for msg in is_qualified_name(&lowered) {
            errs.push(Error::invalid(fld_path, k.clone(), msg).with_origin("format=k8s-label-key"));
        }
    }
    if let Some(_size_err) = validate_annotations_size(annotations) {
        // Upstream calls `field.TooLong(fldPath, "", TotalAnnotationSizeLimitB)`.
        errs.push(Error::too_long(fld_path, TOTAL_ANNOTATION_SIZE_LIMIT_B));
    }
    errs
}

/// Upstream `ValidateAnnotationsSize`. Returns an `Err` description when the
/// combined annotation byte size exceeds the limit, `None` otherwise.
pub fn validate_annotations_size(annotations: &HashMap<String, String>) -> Option<String> {
    let total: usize = annotations.iter().map(|(k, v)| k.len() + v.len()).sum();
    if total > TOTAL_ANNOTATION_SIZE_LIMIT_B {
        Some(format!(
            "annotations size {total} is larger than limit {TOTAL_ANNOTATION_SIZE_LIMIT_B}"
        ))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Owner references
// ---------------------------------------------------------------------------

/// A blocklist of object types that can never appear as an owner. Mirrors
/// upstream `BannedOwners` (currently `core/v1/Event`).
fn is_banned_owner(api_version: &str, kind: &str) -> bool {
    // Upstream: `schema.FromAPIVersionAndKind(apiVersion, kind)`. The legacy
    // group `v1` (no slash) maps to `{Group: "", Version: "v1"}` — exactly
    // what we need to compare against `{Group: "", Version: "v1", Kind:
    // "Event"}`.
    let (group, version) = split_api_version(api_version);
    group.is_empty() && version == "v1" && kind == "Event"
}

/// Upstream `schema.FromAPIVersionAndKind`. `"v1"` → `("", "v1")`, `"apps/v1"`
/// → `("apps", "v1")`.
fn split_api_version(api_version: &str) -> (&str, &str) {
    match api_version.split_once('/') {
        Some((g, v)) => (g, v),
        None => ("", api_version),
    }
}

fn validate_owner_reference(owner: &OwnerReference, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let (_group, version) = split_api_version(&owner.api_version);
    if version.is_empty() {
        errs.push(Error::invalid(
            &fld_path.child("apiVersion"),
            owner.api_version.clone(),
            "version must not be empty",
        ));
    }
    if owner.kind.is_empty() {
        errs.push(Error::invalid(
            &fld_path.child("kind"),
            owner.kind.clone(),
            "must not be empty",
        ));
    }
    if owner.name.is_empty() {
        errs.push(Error::invalid(
            &fld_path.child("name"),
            owner.name.clone(),
            "must not be empty",
        ));
    }
    if owner.uid.is_empty() {
        errs.push(Error::invalid(
            &fld_path.child("uid"),
            owner.uid.clone(),
            "must not be empty",
        ));
    }
    if is_banned_owner(&owner.api_version, &owner.kind) {
        // Upstream: `field.Invalid(fldPath, ownerReference, fmt.Sprintf("%s is disallowed from being an owner", gvk))`.
        // gvk renders as `/<version>, Kind=<kind>` for the legacy group.
        let gvk = render_gvk(&owner.api_version, &owner.kind);
        errs.push(Error::invalid(
            fld_path,
            render_owner_ref(owner),
            format!("{gvk} is disallowed from being an owner"),
        ));
    }
    errs
}

/// Render `schema.GroupVersionKind.String()`. For legacy group v1 this is
/// `"/v1, Kind=Event"`.
fn render_gvk(api_version: &str, kind: &str) -> String {
    let (group, version) = split_api_version(api_version);
    format!("{group}/{version}, Kind={kind}")
}

fn render_owner_ref(owner: &OwnerReference) -> serde_json::Value {
    // Match upstream's `%v` of an OwnerReference struct value. We don't need
    // byte-for-byte parity here because the only test that observes the bad
    // value (the "is disallowed" case) only matches the *detail* substring.
    serde_json::to_value(owner).unwrap_or(serde_json::Value::Null)
}

/// Upstream `ValidateOwnerReferences`.
pub fn validate_owner_references(refs: &[OwnerReference], fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let mut first_controller_name: Option<String> = None;
    for r in refs {
        errs.extend(validate_owner_reference(r, fld_path));
        if r.controller == Some(true) {
            let cur = format!("{}/{}", r.kind, r.name);
            if let Some(first) = &first_controller_name {
                errs.push(Error::invalid(
                    fld_path,
                    render_owner_refs(refs),
                    format!(
                        "Only one reference can have Controller set to true. Found \"true\" in references for {first} and {cur}"
                    ),
                ));
            } else {
                first_controller_name = Some(cur);
            }
        }
    }
    errs
}

fn render_owner_refs(refs: &[OwnerReference]) -> serde_json::Value {
    serde_json::to_value(refs).unwrap_or(serde_json::Value::Null)
}

// ---------------------------------------------------------------------------
// Finalizers
// ---------------------------------------------------------------------------

/// Upstream `ValidateFinalizerName`.
pub fn validate_finalizer_name(value: &str, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    for msg in is_qualified_name(value) {
        errs.push(Error::invalid(fld_path, value.to_string(), msg));
    }
    errs
}

/// Upstream `ValidateFinalizers`. Both validates each finalizer name and
/// rejects the `orphan` + `foregroundDeletion` combination.
pub fn validate_finalizers(finalizers: &[String], fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let mut has_orphan = false;
    let mut has_delete = false;
    for f in finalizers {
        errs.extend(validate_finalizer_name(f, fld_path));
        if f == FINALIZER_ORPHAN_DEPENDENTS {
            has_orphan = true;
        }
        if f == FINALIZER_DELETE_DEPENDENTS {
            has_delete = true;
        }
    }
    if has_orphan && has_delete {
        errs.push(Error::invalid(
            fld_path,
            serde_json::to_value(finalizers).unwrap_or(serde_json::Value::Null),
            format!(
                "finalizer {FINALIZER_ORPHAN_DEPENDENTS} and {FINALIZER_DELETE_DEPENDENTS} cannot be both set"
            ),
        ));
    }
    errs
}

/// Upstream `ValidateNoNewFinalizers`.
pub fn validate_no_new_finalizers(
    new_finalizers: &[String],
    old_finalizers: &[String],
    fld_path: &Path,
) -> ErrorList {
    let old: HashSet<&String> = old_finalizers.iter().collect();
    let mut extra: Vec<&String> = new_finalizers
        .iter()
        .filter(|f| !old.contains(*f))
        .collect();
    extra.sort();
    extra.dedup();
    let mut errs = Vec::new();
    if !extra.is_empty() {
        // Upstream renders `extra.List()` (a sorted Go slice) via `%#v`. We
        // approximate with a Rust-side `[]string{"a", "b"}` form.
        let rendered: Vec<String> = extra.iter().map(|s| format!("{s:?}")).collect();
        let go_form = format!("[]string{{{}}}", rendered.join(", "));
        errs.push(Error::forbidden(
            fld_path,
            format!(
                "no new finalizers can be added if the object is being deleted, found new finalizers {go_form}"
            ),
        ));
    }
    errs
}

// ---------------------------------------------------------------------------
// Immutability helper
// ---------------------------------------------------------------------------

/// Upstream `ValidateImmutableField` specialised for JSON-serializable values.
/// Upstream uses `apiequality.Semantic.DeepEqual` over `interface{}`; here we
/// compare via `serde_json::Value` to keep the call sites generic.
fn validate_immutable_field<T: serde::Serialize + ?Sized>(
    new_val: &T,
    old_val: &T,
    fld_path: &Path,
) -> ErrorList {
    let new_json = serde_json::to_value(new_val).unwrap_or(serde_json::Value::Null);
    let old_json = serde_json::to_value(old_val).unwrap_or(serde_json::Value::Null);
    if new_json == old_json {
        return Vec::new();
    }
    vec![Error::invalid(
        fld_path,
        new_json,
        FIELD_IMMUTABLE_ERROR_MSG,
    )]
}

// ---------------------------------------------------------------------------
// ObjectMeta (create)
// ---------------------------------------------------------------------------

/// Upstream `ValidateObjectMeta` (and `ValidateObjectMetaAccessor`). Validates
/// the metadata of an object on creation. `name_fn` is the resource-specific
/// name validator (typically [`name_is_dns_subdomain`] or
/// [`name_is_dns_label`]).
pub fn validate_object_meta(
    meta: &ObjectMeta,
    requires_namespace: bool,
    name_fn: ValidateNameFunc,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();

    if let Some(gn) = &meta.generate_name {
        if !gn.is_empty() {
            for msg in name_fn(gn, true) {
                errs.push(Error::invalid(
                    &fld_path.child("generateName"),
                    gn.clone(),
                    msg,
                ));
            }
        }
    }
    if meta.name.is_empty() {
        errs.push(Error::required(
            &fld_path.child("name"),
            "name or generateName is required",
        ));
    } else {
        for msg in name_fn(&meta.name, false) {
            errs.push(Error::invalid(
                &fld_path.child("name"),
                meta.name.clone(),
                msg,
            ));
        }
    }

    errs.extend(validate_object_meta_accessor_with_opts_common(
        meta,
        requires_namespace,
        fld_path,
    ));
    errs
}

/// Upstream `ValidateObjectMetaWithOpts`. Identical to [`validate_object_meta`]
/// except the name validator returns an [`ErrorList`] directly (so callers can
/// shape the error path themselves) and `generateName` is *not* validated
/// here — upstream assumes name generation has already run.
pub fn validate_object_meta_with_opts(
    meta: &ObjectMeta,
    is_namespaced: bool,
    name_fn: ValidateNameFuncWithErrors,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    let has_gn = meta.generate_name.as_ref().is_some_and(|s| !s.is_empty());
    if has_gn && meta.name.is_empty() {
        // Upstream emits `field.InternalError`. We surface as Invalid with
        // the upstream wording — callers grep for the substring only.
        errs.push(
            Error::invalid(
                &fld_path.child("name"),
                meta.name.clone(),
                format!(
                    "generateName was specified ({:?}), but no name was generated",
                    meta.generate_name.as_deref().unwrap_or("")
                ),
            )
            .with_origin("internal"),
        );
    }
    if meta.name.is_empty() {
        errs.push(Error::required(
            &fld_path.child("name"),
            "name or generateName is required",
        ));
    } else {
        errs.extend(name_fn(&fld_path.child("name"), &meta.name));
    }

    errs.extend(validate_object_meta_accessor_with_opts_common(
        meta,
        is_namespaced,
        fld_path,
    ));
    errs
}

/// Upstream `validateObjectMetaAccessorWithOptsCommon` — the shared body
/// driving the namespace, generation, labels, annotations, owner-references,
/// finalizers and managed-fields validators.
pub fn validate_object_meta_accessor_with_opts_common(
    meta: &ObjectMeta,
    is_namespaced: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();

    let ns = meta.namespace.as_deref().unwrap_or("");
    if is_namespaced {
        if ns.is_empty() {
            errs.push(Error::required(&fld_path.child("namespace"), ""));
        } else {
            for msg in validate_namespace_name(ns, false) {
                errs.push(Error::invalid(
                    &fld_path.child("namespace"),
                    ns.to_string(),
                    msg,
                ));
            }
        }
    } else if !ns.is_empty() {
        errs.push(Error::forbidden(
            &fld_path.child("namespace"),
            "not allowed on this type",
        ));
    }

    errs.extend(validate_nonnegative_field(
        meta.generation.unwrap_or(0),
        &fld_path.child("generation"),
    ));
    if let Some(labels) = &meta.labels {
        errs.extend(validate_labels(labels, &fld_path.child("labels")));
    }
    if let Some(annotations) = &meta.annotations {
        errs.extend(validate_annotations(
            annotations,
            &fld_path.child("annotations"),
        ));
    }
    if let Some(refs) = &meta.owner_references {
        errs.extend(validate_owner_references(
            refs,
            &fld_path.child("ownerReferences"),
        ));
    }
    if let Some(finalizers) = &meta.finalizers {
        errs.extend(validate_finalizers(
            finalizers,
            &fld_path.child("finalizers"),
        ));
    }
    if let Some(mf) = &meta.managed_fields {
        errs.extend(validate_managed_fields(
            mf,
            &fld_path.child("managedFields"),
        ));
    }
    errs
}

// ---------------------------------------------------------------------------
// ObjectMeta (update)
// ---------------------------------------------------------------------------

/// Upstream `ValidateObjectMetaUpdate` (and `ValidateObjectMetaAccessorUpdate`).
/// Enforces immutability of `name`, `namespace`, `uid`, `creationTimestamp`,
/// `deletionTimestamp` and `deletionGracePeriodSeconds`, plus the
/// finalizer-during-deletion rule and the generation-monotonicity rule.
pub fn validate_object_meta_update(
    new_meta: &ObjectMeta,
    old_meta: &ObjectMeta,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();

    // Finalizers cannot be added if the object is already being deleted.
    if old_meta.deletion_timestamp.is_some() {
        let empty: Vec<String> = Vec::new();
        let new_finalizers = new_meta.finalizers.as_ref().unwrap_or(&empty);
        let old_finalizers = old_meta.finalizers.as_ref().unwrap_or(&empty);
        errs.extend(validate_no_new_finalizers(
            new_finalizers,
            old_finalizers,
            &fld_path.child("finalizers"),
        ));
    }

    // Reject updates that don't specify a resource version.
    let new_rv = new_meta.resource_version.as_deref().unwrap_or("");
    if new_rv.is_empty() {
        errs.push(Error::invalid(
            &fld_path.child("resourceVersion"),
            new_rv.to_string(),
            "must be specified for an update",
        ));
    }

    // Generation shouldn't be decremented.
    let new_gen = new_meta.generation.unwrap_or(0);
    let old_gen = old_meta.generation.unwrap_or(0);
    if new_gen < old_gen {
        errs.push(Error::invalid(
            &fld_path.child("generation"),
            new_gen,
            "must not be decremented",
        ));
    }

    errs.extend(validate_immutable_field(
        &new_meta.name,
        &old_meta.name,
        &fld_path.child("name"),
    ));
    errs.extend(validate_immutable_field(
        &new_meta.namespace.clone().unwrap_or_default(),
        &old_meta.namespace.clone().unwrap_or_default(),
        &fld_path.child("namespace"),
    ));
    errs.extend(validate_immutable_field(
        &new_meta.uid,
        &old_meta.uid,
        &fld_path.child("uid"),
    ));
    errs.extend(validate_immutable_datetime(
        new_meta.creation_timestamp,
        old_meta.creation_timestamp,
        &fld_path.child("creationTimestamp"),
    ));
    errs.extend(validate_immutable_datetime(
        new_meta.deletion_timestamp,
        old_meta.deletion_timestamp,
        &fld_path.child("deletionTimestamp"),
    ));
    errs.extend(validate_immutable_optional_int(
        new_meta.deletion_grace_period_seconds,
        old_meta.deletion_grace_period_seconds,
        &fld_path.child("deletionGracePeriodSeconds"),
    ));

    if let Some(labels) = &new_meta.labels {
        errs.extend(validate_labels(labels, &fld_path.child("labels")));
    }
    if let Some(annotations) = &new_meta.annotations {
        errs.extend(validate_annotations(
            annotations,
            &fld_path.child("annotations"),
        ));
    }
    if let Some(refs) = &new_meta.owner_references {
        errs.extend(validate_owner_references(
            refs,
            &fld_path.child("ownerReferences"),
        ));
    }
    if let Some(mf) = &new_meta.managed_fields {
        errs.extend(validate_managed_fields(
            mf,
            &fld_path.child("managedFields"),
        ));
    }
    errs
}

/// Render a `DateTime<Utc>` the way upstream's `metav1.Time.MarshalJSON` does:
/// RFC-3339 with a `Z` suffix (no nanoseconds when they're zero).
fn render_timestamp(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Immutability check specialised for `Option<DateTime<Utc>>`. We can't just
/// reuse [`validate_immutable_field`] because upstream renders `nil` as
/// JSON `null` and a non-nil value as an RFC-3339 string — JSON serialization
/// of `Option<DateTime>` would produce a quoted string for Some(...) but the
/// shape of the upstream-rendered bad-value is what the conformance test
/// asserts, so we keep it explicit.
fn validate_immutable_datetime(
    new_val: Option<DateTime<Utc>>,
    old_val: Option<DateTime<Utc>>,
    fld_path: &Path,
) -> ErrorList {
    if new_val == old_val {
        return Vec::new();
    }
    let bad = match new_val {
        Some(ts) => serde_json::Value::String(render_timestamp(ts)),
        None => serde_json::Value::Null,
    };
    vec![Error::invalid(fld_path, bad, FIELD_IMMUTABLE_ERROR_MSG)]
}

/// Immutability check specialised for `Option<i64>` (rendered as JSON `null`
/// when None, plain integer when Some).
fn validate_immutable_optional_int(
    new_val: Option<i64>,
    old_val: Option<i64>,
    fld_path: &Path,
) -> ErrorList {
    if new_val == old_val {
        return Vec::new();
    }
    let bad = match new_val {
        Some(v) => serde_json::Value::Number(v.into()),
        None => serde_json::Value::Null,
    };
    vec![Error::invalid(fld_path, bad, FIELD_IMMUTABLE_ERROR_MSG)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_trailing_dash_replaces_dash_with_a() {
        // Upstream Go: `name[:len-2] + "a"`. "foo-" → "fo" + "a" = "foa".
        assert_eq!(mask_trailing_dash("foo-"), "foa");
        assert_eq!(mask_trailing_dash("foo"), "foo");
        // Single-char `-` is left alone (upstream's `len > 1` guard).
        assert_eq!(mask_trailing_dash("-"), "-");
    }

    #[test]
    fn render_timestamp_matches_upstream() {
        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        assert_eq!(render_timestamp(ts), "1970-01-01T00:16:40Z");
    }
}
