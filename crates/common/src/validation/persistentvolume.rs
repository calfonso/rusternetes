//! PersistentVolume validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePersistentVolume` (release-1.35).
//!
//! Scope: capacity (storage required, storage-only, non-negative), access modes
//! (≥1 + ReadWriteOncePod exclusivity), exactly one volume source,
//! nodeAffinity-required-for-Local, the hostPath-'/'-with-Recycle prohibition,
//! and storageClassName. The exhaustive per-source field validation (each
//! `validate*VolumeSource`) is left as a follow-up.
//!
//! reclaimPolicy / volumeMode / accessModes enum-membership (upstream
//! `supportedReclaimPolicy` / `supportedVolumeModes` / `supportedAccessModes`)
//! is enforced upstream-of-validation by Rusternetes' typed enums: an unknown
//! string fails to deserialize before this validator runs, so no explicit
//! `NotSupported` check is reproduced here.

use crate::quantity::Quantity;
use crate::resources::volume::{
    PersistentVolume, PersistentVolumeAccessMode, PersistentVolumeMode,
    PersistentVolumeReclaimPolicy, PersistentVolumeSpec,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;

/// Lexically normalise a path, mirroring Go's `path.Clean` for the cases that
/// matter to the hostPath-root check: collapse repeated slashes and resolve
/// `.` / `..` elements, returning "/" for any path that reduces to the root.
fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if let Some(&last) = out.last() {
                    if last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !rooted {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    let joined = out.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

/// Validate a `PersistentVolumeSpec`. Mirrors the core of upstream
/// `ValidatePersistentVolume`.
pub fn validate_persistent_volume_spec(
    spec: &PersistentVolumeSpec,
    inline: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let capacity_path = fld_path.child("capacity");

    // Inline-volume-only deltas (upstream `validateInlinePersistentVolumeSpec`,
    // core validation.go:1968-1978): claimRef + capacity are forbidden and a CSI
    // source is required, because an inline PV (VolumeAttachment.inlineVolumeSpec)
    // is not a standalone object.
    if inline {
        if spec.claim_ref.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("claimRef"),
                "may not be specified in the context of inline volumes",
            ));
        }
        if !spec.capacity.is_empty() {
            errs.push(Error::forbidden(
                &capacity_path,
                "may not be specified in the context of inline volumes",
            ));
        }
        if spec.csi.is_none() {
            errs.push(Error::required(
                &fld_path.child("csi"),
                "has to be specified in the context of inline volumes",
            ));
        }
    }

    // capacity is required (upstream line ~2002). Then it must hold exactly the
    // `storage` resource and nothing else (upstream line ~2005-2007).
    // Upstream uses two independent `if`s (not else-if): an empty capacity is
    // both Required AND NotSupported (storage absent). Match that exactly.
    // These run only for standalone PVs — inline volumes forbid capacity above.
    if !inline {
        if spec.capacity.is_empty() {
            errs.push(Error::required(&capacity_path, ""));
        }
        if !spec.capacity.contains_key("storage") || spec.capacity.len() > 1 {
            errs.push(Error::not_supported(
                &capacity_path,
                "<capacity>",
                &["storage"],
            ));
        }

        // Every capacity quantity must parse and be a non-negative value
        // (upstream validateBasicResource + ValidatePositiveQuantityValue, ~2009-2012).
        for (resource, value) in &spec.capacity {
            let key_path = capacity_path.key(resource.clone());
            match Quantity::parse(value) {
                Err(_) => errs.push(Error::invalid(
                    &key_path,
                    value.clone(),
                    "must be a valid resource quantity",
                )),
                Ok(q) => {
                    if q.is_negative() {
                        errs.push(Error::invalid(
                            &key_path,
                            value.clone(),
                            "must be greater than or equal to 0",
                        ));
                    }
                }
            }
        }
    }

    // accessModes: at least one; ReadWriteOncePod may not combine with others.
    if spec.access_modes.is_empty() {
        errs.push(Error::required(
            &fld_path.child("accessModes"),
            "at least 1 access mode is required",
        ));
    }
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

    // Exactly one volume source must be specified.
    let num_volumes = [
        spec.host_path.is_some(),
        spec.nfs.is_some(),
        spec.iscsi.is_some(),
        spec.local.is_some(),
        spec.csi.is_some(),
    ]
    .iter()
    .filter(|x| **x)
    .count();
    if num_volumes == 0 {
        errs.push(Error::required(fld_path, "must specify a volume type"));
    } else if num_volumes > 1 {
        errs.push(Error::forbidden(
            fld_path,
            "may not specify more than 1 volume type",
        ));
    }

    // A Local volume requires node affinity (upstream line ~2194-2197).
    if spec.local.is_some() && spec.node_affinity.is_none() {
        errs.push(Error::required(
            &fld_path.child("nodeAffinity"),
            "Local volume requires node affinity",
        ));
    }

    // A hostPath mount of '/' may not use the Recycle reclaim policy
    // (upstream line ~2222-2225).
    if let Some(hp) = &spec.host_path {
        if clean_path(&hp.path) == "/"
            && spec.persistent_volume_reclaim_policy == Some(PersistentVolumeReclaimPolicy::Recycle)
        {
            errs.push(Error::forbidden(
                &fld_path.child("persistentVolumeReclaimPolicy"),
                "may not be 'recycle' for a hostPath mount of '/'",
            ));
        }
    }

    // reclaimPolicy: inline volumes may only use Retain (upstream
    // validation.go:~2018). Standalone PVs accept the full supported set
    // (enum validation handled elsewhere); the hostPath '/' Recycle case above
    // is independent.
    if inline {
        if let Some(policy) = &spec.persistent_volume_reclaim_policy {
            if *policy != PersistentVolumeReclaimPolicy::Retain {
                errs.push(Error::forbidden(
                    &fld_path.child("persistentVolumeReclaimPolicy"),
                    "may only be Retain in the context of inline volumes",
                ));
            }
        }
        // nodeAffinity may not be specified for inline volumes (validation.go:~2228).
        if spec.node_affinity.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("nodeAffinity"),
                "may not be specified in the context of inline volumes",
            ));
        }
        // volumeMode, when set, must be Filesystem for inline volumes
        // (validation.go:~2237).
        if let Some(mode) = &spec.volume_mode {
            if *mode != PersistentVolumeMode::Filesystem {
                errs.push(Error::forbidden(
                    &fld_path.child("volumeMode"),
                    "may not specify volumeMode other than Filesystem in the context of inline volumes",
                ));
            }
        }
    }

    // storageClassName: forbidden for inline volumes; otherwise, when set, must
    // be a DNS-1123 subdomain (upstream validation.go:~2228-2233).
    if let Some(scn) = &spec.storage_class_name {
        if inline {
            if !scn.is_empty() {
                errs.push(Error::forbidden(
                    &fld_path.child("storageClassName"),
                    "may not be specified in the context of inline volumes",
                ));
            }
        } else if !scn.is_empty() {
            for msg in is_dns1123_subdomain(scn) {
                errs.push(Error::invalid(
                    &fld_path.child("storageClassName"),
                    scn.clone(),
                    msg,
                ));
            }
        }
    }

    errs
}

/// Validate a new `PersistentVolume`. Mirrors upstream `ValidatePersistentVolume`.
pub fn validate_persistent_volume(pv: &PersistentVolume) -> ErrorList {
    validate_persistent_volume_spec(&pv.spec, false, &Path::new("spec"))
}

/// JSON view of just the volume-source union of a `PersistentVolumeSpec`
/// (`PersistentVolumeSource` upstream) — the fields that are immutable after
/// creation. Capacity / accessModes / reclaimPolicy etc. are intentionally
/// excluded.
fn persistent_volume_source(spec: &PersistentVolumeSpec) -> serde_json::Value {
    serde_json::json!({
        "hostPath": spec.host_path,
        "nfs": spec.nfs,
        "iscsi": spec.iscsi,
        "local": spec.local,
        "csi": spec.csi,
    })
}

/// Validate a `PersistentVolume` on update. Mirrors upstream
/// `ValidatePersistentVolumeUpdate`: re-run create validation, then enforce that
/// the volume source and `volumeMode` are immutable. The CSI
/// `controllerExpandSecretRef` may be set when it was previously unset (allowed
/// for volume expansion), so it is excluded from the source-immutability diff in
/// that case.
pub fn validate_persistent_volume_update(
    new: &PersistentVolume,
    old: &PersistentVolume,
) -> ErrorList {
    let mut errs = validate_persistent_volume(new);

    // Allow first-time setting of csi.controllerExpandSecretRef: normalise the
    // new spec to drop it before the source diff when old had none.
    let mut new_spec = new.spec.clone();
    let old_had_expand_ref = old
        .spec
        .csi
        .as_ref()
        .map(|c| c.controller_expand_secret_ref.is_some())
        .unwrap_or(false);
    if !old_had_expand_ref {
        if let Some(csi) = new_spec.csi.as_mut() {
            csi.controller_expand_secret_ref = None;
        }
    }

    if persistent_volume_source(&new_spec) != persistent_volume_source(&old.spec) {
        errs.push(Error::forbidden(
            &Path::new("spec").child("persistentvolumesource"),
            "spec.persistentvolumesource is immutable after creation",
        ));
    }

    if new.spec.volume_mode != old.spec.volume_mode {
        errs.push(Error::invalid(
            &Path::new("spec").child("volumeMode"),
            format!("{:?}", new.spec.volume_mode),
            "field is immutable",
        ));
    }

    // nodeAffinity: immutable once set (upstream validatePvNodeAffinity, with the
    // default-off MutablePVNodeAffinity gate). A nil → set transition is allowed.
    // The beta→GA topology-label masking carve-out is not modelled.
    if old.spec.node_affinity.is_some() {
        let na_eq = serde_json::to_value(&new.spec.node_affinity).ok()
            == serde_json::to_value(&old.spec.node_affinity).ok();
        if !na_eq {
            errs.push(Error::invalid(
                &Path::new("spec").child("nodeAffinity"),
                "<nodeAffinity>".to_string(),
                "field is immutable",
            ));
        }
    }

    // volumeAttributesClassName: with the VolumeAttributesClass feature enabled
    // (beta-on by 1.35), an existing class may be changed but not cleared.
    if old.spec.volume_attributes_class_name.is_some()
        && new.spec.volume_attributes_class_name.is_none()
    {
        errs.push(Error::forbidden(
            &Path::new("spec").child("volumeAttributesClassName"),
            "update from non-nil value to nil is forbidden",
        ));
    }

    errs
}

#[cfg(test)]
mod create_tests {
    use super::*;

    fn pv(json: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(json).unwrap()
    }

    fn valid_hostpath() -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Retain",
                "hostPath": {"path": "/data"}
            }
        })
    }

    #[test]
    fn clean_path_resolves_root() {
        assert_eq!(clean_path("/"), "/");
        assert_eq!(clean_path("//"), "/");
        assert_eq!(clean_path("/."), "/");
        assert_eq!(clean_path("/foo/.."), "/");
        assert_eq!(clean_path("/foo/../bar"), "/bar");
        assert_eq!(clean_path("/data"), "/data");
    }

    #[test]
    fn valid_pv_passes() {
        assert!(validate_persistent_volume(&pv(valid_hostpath())).is_empty());
    }

    #[test]
    fn capacity_required() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("capacity")),
            "{errs:?}"
        );
    }

    #[test]
    fn capacity_must_be_storage_only() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"storage": "1Gi", "cpu": "1"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("capacity") && e.detail.contains("supported values")),
            "{errs:?}"
        );
    }

    #[test]
    fn capacity_missing_storage_key_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"cpu": "1"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("capacity")),
            "{errs:?}"
        );
    }

    #[test]
    fn negative_capacity_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"storage": "-1Gi"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.detail.contains("greater than or equal to 0")),
            "{errs:?}"
        );
    }

    #[test]
    fn access_modes_required() {
        let mut v = valid_hostpath();
        v["spec"]["accessModes"] = serde_json::json!([]);
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("accessModes")),
            "{errs:?}"
        );
    }

    #[test]
    fn rwop_with_other_modes_forbidden() {
        let mut v = valid_hostpath();
        v["spec"]["accessModes"] = serde_json::json!(["ReadWriteOncePod", "ReadWriteOnce"]);
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not use ReadWriteOncePod with other access modes")),
            "{errs:?}"
        );
    }

    #[test]
    fn no_volume_source_rejected() {
        let mut v = valid_hostpath();
        v["spec"].as_object_mut().unwrap().remove("hostPath");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("must specify a volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn more_than_one_volume_source_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["nfs"] = serde_json::json!({"server": "1.2.3.4", "path": "/exports"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not specify more than 1 volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn local_requires_node_affinity() {
        let v = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "local": {"path": "/mnt/disk"}
            }
        }));
        let errs = validate_persistent_volume(&v);
        assert!(
            errs.iter().any(|e| e.field.ends_with("nodeAffinity")
                && e.to_string()
                    .contains("Local volume requires node affinity")),
            "{errs:?}"
        );
    }

    #[test]
    fn local_with_node_affinity_passes() {
        let v = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "local": {"path": "/mnt/disk"},
                "nodeAffinity": {"required": {"nodeSelectorTerms": [
                    {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n1"]}]}
                ]}}
            }
        }));
        assert!(validate_persistent_volume(&v).is_empty());
    }

    #[test]
    fn hostpath_root_with_recycle_forbidden() {
        let mut v = valid_hostpath();
        v["spec"]["hostPath"]["path"] = serde_json::json!("/");
        v["spec"]["persistentVolumeReclaimPolicy"] = serde_json::json!("Recycle");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not be 'recycle' for a hostPath mount of '/'")),
            "{errs:?}"
        );
    }

    #[test]
    fn hostpath_nonroot_with_recycle_ok() {
        let mut v = valid_hostpath();
        v["spec"]["persistentVolumeReclaimPolicy"] = serde_json::json!("Recycle");
        // path is /data, not '/', so Recycle is allowed
        assert!(validate_persistent_volume(&pv(v)).is_empty());
    }

    #[test]
    fn invalid_storage_class_name_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["storageClassName"] = serde_json::json!("Bad_Name");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("storageClassName")),
            "{errs:?}"
        );
    }
}

#[cfg(test)]
mod update_tests {
    use super::*;

    fn pv(json: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(json).unwrap()
    }

    fn hostpath_pv(path: &str) -> PersistentVolume {
        pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Retain",
                "hostPath": {"path": path}
            }
        }))
    }

    #[test]
    fn unchanged_passes() {
        let old = hostpath_pv("/data");
        let new = hostpath_pv("/data");
        assert!(validate_persistent_volume_update(&new, &old).is_empty());
    }

    #[test]
    fn changed_source_rejected() {
        let old = hostpath_pv("/data");
        let new = hostpath_pv("/other");
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("persistentvolumesource is immutable")),
            "{errs:?}"
        );
    }

    #[test]
    fn changed_volume_mode_rejected() {
        let mut old = hostpath_pv("/data");
        old.spec.volume_mode = Some(crate::resources::volume::PersistentVolumeMode::Filesystem);
        let mut new = hostpath_pv("/data");
        new.spec.volume_mode = Some(crate::resources::volume::PersistentVolumeMode::Block);
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("volumeMode") && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }

    #[test]
    fn first_time_csi_expand_secret_ref_allowed() {
        let old = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Delete",
                "csi": {"driver": "csi.example.com", "volumeHandle": "vol-1"}}
        }));
        let new = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Delete",
                "csi": {"driver": "csi.example.com", "volumeHandle": "vol-1",
                    "controllerExpandSecretRef": {"name": "s", "namespace": "ns"}}}
        }));
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            !errs
                .iter()
                .any(|e| e.to_string().contains("persistentvolumesource")),
            "{errs:?}"
        );
    }

    #[test]
    fn node_affinity_immutable_once_set() {
        let na = serde_json::json!({"required": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n1"]}]}
        ]}});
        let mut old = hostpath_pv("/data");
        old.spec.node_affinity = serde_json::from_value(na.clone()).unwrap();
        // unchanged -> ok
        let mut same = hostpath_pv("/data");
        same.spec.node_affinity = serde_json::from_value(na).unwrap();
        assert!(validate_persistent_volume_update(&same, &old).is_empty());
        // changed -> immutable
        let mut changed = hostpath_pv("/data");
        changed.spec.node_affinity = serde_json::from_value(serde_json::json!({"required": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n2"]}]}
        ]}})).unwrap();
        let errs = validate_persistent_volume_update(&changed, &old);
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("nodeAffinity") && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }

    #[test]
    fn node_affinity_may_be_set_when_old_nil() {
        let old = hostpath_pv("/data"); // no nodeAffinity
        let mut new = hostpath_pv("/data");
        new.spec.node_affinity =
            serde_json::from_value(serde_json::json!({"required": {"nodeSelectorTerms": [
                {"matchExpressions": [{"key": "k", "operator": "Exists"}]}
            ]}}))
            .unwrap();
        assert!(validate_persistent_volume_update(&new, &old).is_empty());
    }

    #[test]
    fn vac_name_may_not_be_cleared() {
        let mut old = hostpath_pv("/data");
        old.spec.volume_attributes_class_name = Some("gold".to_string());
        let new = hostpath_pv("/data"); // VAC cleared
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("non-nil value to nil is forbidden")),
            "{errs:?}"
        );
        // changing to another class is allowed
        let mut changed = hostpath_pv("/data");
        changed.spec.volume_attributes_class_name = Some("silver".to_string());
        assert!(!validate_persistent_volume_update(&changed, &old)
            .iter()
            .any(|e| e.field.ends_with("volumeAttributesClassName")));
    }
}
