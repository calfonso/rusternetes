//! PersistentVolumeClaim validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePersistentVolumeClaim`,
//! `ValidatePersistentVolumeClaimSpec`, and `ValidatePersistentVolumeClaimUpdate`
//! (release-1.35).
//!
//! Covers the field-level checks that don't need cluster state: access modes,
//! the storage request, `storageClassName`, the `selector`, `dataSource` /
//! `dataSourceRef` consistency, and `volumeAttributesClassName`. The
//! `accessModes` and `volumeMode` enums are closed Rust enums, so an
//! out-of-range value is rejected at deserialization (matching upstream's
//! `NotSupported` set membership check).

use crate::quantity::Quantity;
use crate::resources::volume::{
    LabelSelector as VolumeLabelSelector, PersistentVolumeAccessMode, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, TypedLocalObjectReference, TypedObjectReference,
};
use crate::types::{
    LabelSelector as MetaLabelSelector, LabelSelectorRequirement as MetaLabelSelectorRequirement,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_subdomain, validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::validate_namespace_name;

/// Convert the PVC-spec `volume::LabelSelector` to the structurally-identical
/// `types::LabelSelector` that `validate_label_selector` consumes.
fn to_meta_label_selector(sel: &VolumeLabelSelector) -> MetaLabelSelector {
    MetaLabelSelector {
        match_labels: sel.match_labels.clone(),
        match_expressions: sel.match_expressions.as_ref().map(|reqs| {
            reqs.iter()
                .map(|r| MetaLabelSelectorRequirement {
                    key: r.key.clone(),
                    operator: r.operator.clone(),
                    values: r.values.clone(),
                })
                .collect()
        }),
    }
}

/// Mirrors upstream `validateDataSource`. `dataSource` is a
/// `TypedLocalObjectReference` (no namespace).
fn validate_data_source(ds: &TypedLocalObjectReference, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if ds.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    }
    if ds.kind.is_empty() {
        errs.push(Error::required(&fld_path.child("kind"), ""));
    }
    let api_group = ds.api_group.as_deref().unwrap_or("");
    if api_group.is_empty() && ds.kind != "PersistentVolumeClaim" {
        errs.push(Error::invalid(
            fld_path,
            ds.kind.clone(),
            "must be 'PersistentVolumeClaim' when referencing the default apiGroup",
        ));
    }
    if !api_group.is_empty() {
        for msg in is_dns1123_subdomain(api_group) {
            errs.push(Error::invalid(
                &fld_path.child("apiGroup"),
                api_group.to_string(),
                msg,
            ));
        }
    }
    errs
}

/// Mirrors upstream `validateDataSourceRef`. `dataSourceRef` is a
/// `TypedObjectReference` (may carry a cross-namespace reference).
fn validate_data_source_ref(dsr: &TypedObjectReference, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if dsr.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    }
    if dsr.kind.is_empty() {
        errs.push(Error::required(&fld_path.child("kind"), ""));
    }
    let api_group = dsr.api_group.as_deref().unwrap_or("");
    if api_group.is_empty() && dsr.kind != "PersistentVolumeClaim" {
        errs.push(Error::invalid(
            fld_path,
            dsr.kind.clone(),
            "must be 'PersistentVolumeClaim' when referencing the default apiGroup",
        ));
    }
    if !api_group.is_empty() {
        for msg in is_dns1123_subdomain(api_group) {
            errs.push(Error::invalid(
                &fld_path.child("apiGroup"),
                api_group.to_string(),
                msg,
            ));
        }
    }
    if let Some(ns) = &dsr.namespace {
        if !ns.is_empty() {
            for msg in validate_namespace_name(ns, false) {
                errs.push(Error::invalid(
                    &fld_path.child("namespace"),
                    ns.clone(),
                    msg,
                ));
            }
        }
    }
    errs
}

/// Upstream `isDataSourceEqualDataSourceRef`: a `dataSource` and `dataSourceRef`
/// are equivalent when apiGroup, kind, and name all match.
fn is_data_source_equal_data_source_ref(
    ds: &TypedLocalObjectReference,
    dsr: &TypedObjectReference,
) -> bool {
    ds.api_group == dsr.api_group && ds.kind == dsr.kind && ds.name == dsr.name
}

/// Validate a `PersistentVolumeClaimSpec`. Mirrors upstream
/// `ValidatePersistentVolumeClaimSpec`.
pub fn validate_persistent_volume_claim_spec(
    spec: &PersistentVolumeClaimSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // accessModes: at least one is required. (Individual values are an enum, so
    // their validity is enforced at deserialization.)
    if spec.access_modes.is_empty() {
        errs.push(Error::required(
            &fld_path.child("accessModes"),
            "at least 1 access mode is required",
        ));
    }

    // selector: when present, validate as a label selector (upstream
    // `ValidateLabelSelector`).
    if let Some(selector) = &spec.selector {
        errs.extend(validate_label_selector(
            &to_meta_label_selector(selector),
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
    }

    // ReadWriteOncePod may not be combined with any other access mode.
    let has_rwop = spec
        .access_modes
        .iter()
        .any(|m| matches!(m, PersistentVolumeAccessMode::ReadWriteOncePod));
    let has_other = spec
        .access_modes
        .iter()
        .any(|m| !matches!(m, PersistentVolumeAccessMode::ReadWriteOncePod));
    if has_rwop && has_other {
        errs.push(Error::forbidden(
            &fld_path.child("accessModes"),
            "may not use ReadWriteOncePod with other access modes",
        ));
    }

    // resources.requests[storage] is required and must be a positive quantity.
    let storage_path = fld_path
        .child("resources")
        .child("requests")
        .child("storage");
    match spec
        .resources
        .requests
        .as_ref()
        .and_then(|r| r.get("storage"))
    {
        None => errs.push(Error::required(&storage_path, "")),
        Some(val) => match Quantity::parse(val) {
            Err(_) => errs.push(Error::invalid(
                &storage_path,
                val.clone(),
                "must be a valid resource quantity",
            )),
            Ok(q) => {
                if q.is_negative() || q.is_zero() {
                    errs.push(Error::invalid(
                        &storage_path,
                        val.clone(),
                        "must be greater than 0",
                    ));
                }
            }
        },
    }

    // storageClassName, when set, must be a DNS-1123 subdomain (upstream
    // `ValidateClassName`).
    if let Some(scn) = &spec.storage_class_name {
        if !scn.is_empty() {
            for msg in is_dns1123_subdomain(scn) {
                errs.push(Error::invalid(
                    &fld_path.child("storageClassName"),
                    scn.clone(),
                    msg,
                ));
            }
        }
    }

    // volumeMode validity is enforced at deserialization (closed Rust enum,
    // upstream's `supportedVolumeModes` NotSupported check).

    // dataSource / dataSourceRef field-level validation.
    if let Some(ds) = &spec.data_source {
        errs.extend(validate_data_source(ds, &fld_path.child("dataSource")));
    }
    if let Some(dsr) = &spec.data_source_ref {
        errs.extend(validate_data_source_ref(
            dsr,
            &fld_path.child("dataSourceRef"),
        ));
    }

    // dataSource / dataSourceRef interaction (upstream block at validation.go
    // ~2514): if dataSourceRef carries a namespace, dataSource may not also be
    // set; otherwise if both are set they must be equal.
    let dsr_has_namespace = spec
        .data_source_ref
        .as_ref()
        .and_then(|r| r.namespace.as_ref())
        .is_some_and(|ns| !ns.is_empty());
    if dsr_has_namespace {
        if spec.data_source.is_some() {
            errs.push(Error::invalid(
                fld_path,
                fld_path.child("dataSource").to_string(),
                "may not be specified when dataSourceRef.namespace is specified",
            ));
        }
    } else if let (Some(ds), Some(dsr)) = (&spec.data_source, &spec.data_source_ref) {
        if !is_data_source_equal_data_source_ref(ds, dsr) {
            errs.push(Error::invalid(
                fld_path,
                fld_path.child("dataSource").to_string(),
                "must match dataSourceRef",
            ));
        }
    }

    // volumeAttributesClassName, when set, must be a DNS-1123 subdomain
    // (upstream `ValidateClassName`). The upstream feature-gate guard is always
    // open here.
    if let Some(vacn) = &spec.volume_attributes_class_name {
        if !vacn.is_empty() {
            for msg in is_dns1123_subdomain(vacn) {
                errs.push(Error::invalid(
                    &fld_path.child("volumeAttributesClassName"),
                    vacn.clone(),
                    msg,
                ));
            }
        }
    }

    errs
}

/// Validate a new `PersistentVolumeClaim`. Mirrors upstream
/// `ValidatePersistentVolumeClaim`.
pub fn validate_persistent_volume_claim(pvc: &PersistentVolumeClaim) -> ErrorList {
    validate_persistent_volume_claim_spec(&pvc.spec, &Path::new("spec"))
}

/// Validate a `PersistentVolumeClaim` update. Ports the conformance-relevant
/// subset of upstream `ValidatePersistentVolumeClaimUpdate`: `volumeMode` is
/// immutable, and the storage request may not shrink. The broad "spec is
/// immutable except resources.requests" deep-equal check (which needs careful
/// per-field normalization to avoid false positives during binding) is left as
/// a follow-up.
pub fn validate_persistent_volume_claim_update(
    new_pvc: &PersistentVolumeClaim,
    old_pvc: &PersistentVolumeClaim,
) -> ErrorList {
    // The new object must still satisfy the create-time spec rules.
    let mut errs = validate_persistent_volume_claim(new_pvc);

    // volumeMode is immutable.
    if new_pvc.spec.volume_mode != old_pvc.spec.volume_mode {
        errs.push(Error::forbidden(
            &Path::new("volumeMode"),
            "field is immutable",
        ));
    }

    // resources.requests["storage"] may not decrease (Kubernetes supports growth
    // only, not shrinking).
    let storage = |spec: &PersistentVolumeClaimSpec| -> Option<String> {
        spec.resources
            .requests
            .as_ref()
            .and_then(|m| m.get("storage"))
            .cloned()
    };
    if let (Some(old_s), Some(new_s)) = (storage(&old_pvc.spec), storage(&new_pvc.spec)) {
        if let (Ok(o), Ok(n)) = (Quantity::parse(&old_s), Quantity::parse(&new_s)) {
            if n.cmp_value(&o) == std::cmp::Ordering::Less {
                errs.push(Error::forbidden(
                    &Path::new("spec")
                        .child("resources")
                        .child("requests")
                        .child("storage"),
                    "field can not be less than previous value",
                ));
            }
        }
    }

    errs
}
