//! ConfigMap validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateConfigMap` and
//! `ValidateConfigMapUpdate` (release-1.35).
//!
//! Scope: key validity (`IsConfigMapKey`) for `data` and `binaryData`, the
//! cross-bag duplicate-key check, the `MaxSecretSize` (1 MiB) total-size cap,
//! and the update-time immutability enforcement (`immutable: true` freezes
//! `immutable`, `data`, and `binaryData`).

use crate::resources::ConfigMap;
use crate::validation::field::{Error, ErrorList, Path};

/// Upstream `core.MaxSecretSize` (`pkg/apis/core/types.go`): the combined byte
/// length of a ConfigMap's `data` + `binaryData` (and a Secret's `data`) values
/// may not exceed 1 MiB.
pub const MAX_SECRET_SIZE: usize = 1024 * 1024;

/// Port of upstream `IsConfigMapKey`
/// (`staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go`):
/// ≤253 chars (`DNS1123SubdomainMaxLength`), matching `[-._a-zA-Z0-9]+`, and
/// not a chdir-prefix key (`.`, `..`, or anything starting with `..`). Returns
/// the upstream-style messages in upstream order. Shared with Secret data-key
/// validation (the same `IsConfigMapKey` rule applies there).
pub fn config_map_key_errors(key: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if key.len() > 253 {
        errs.push("must be no more than 253 characters".to_string());
    }
    if key.is_empty()
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
    {
        errs.push(
            "a valid config key must consist of alphanumeric characters, '-', '_' or '.'"
                .to_string(),
        );
    }
    // Upstream `hasChDirPrefix`: distinct messages for `.`, `..`, and a `..`
    // prefix. The bare `.`/`..` cases and the leading-`..` case are mutually
    // exclusive (`starts_with("..")` only fires for strings longer than two
    // dots once `..` itself is handled).
    if key == "." {
        errs.push("must not be '.'".to_string());
    } else if key == ".." {
        errs.push("must not be '..'".to_string());
    } else if key.starts_with("..") {
        errs.push("must not start with '..'".to_string());
    }
    errs
}

/// Validate a `ConfigMap`. Mirrors the core of upstream `ValidateConfigMap`.
pub fn validate_config_map(cfg: &ConfigMap) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut total_size: usize = 0;

    if let Some(data) = &cfg.data {
        let data_path = Path::new("data");
        for (key, value) in data {
            for msg in config_map_key_errors(key) {
                errs.push(Error::invalid(&data_path.key(key), key.clone(), msg));
            }
            if cfg
                .binary_data
                .as_ref()
                .is_some_and(|b| b.contains_key(key))
            {
                errs.push(Error::invalid(
                    &data_path.key(key),
                    key.clone(),
                    "duplicate of key present in binaryData",
                ));
            }
            total_size += value.len();
        }
    }
    if let Some(binary) = &cfg.binary_data {
        let bin_path = Path::new("binaryData");
        for (key, value) in binary {
            for msg in config_map_key_errors(key) {
                errs.push(Error::invalid(&bin_path.key(key), key.clone(), msg));
            }
            total_size += value.len();
        }
    }

    // Upstream emits `field.TooLong(field.NewPath(""), "", MaxSecretSize)` —
    // the empty path indicates the error refers to the whole object.
    if total_size > MAX_SECRET_SIZE {
        errs.push(Error::too_long(&Path::new(""), MAX_SECRET_SIZE));
    }

    errs
}

/// Validate a `ConfigMap` update. Mirrors upstream `ValidateConfigMapUpdate`:
/// when the **old** object is immutable (`immutable: true`), the `immutable`
/// flag, `data`, and `binaryData` may not change; then the full
/// `ValidateConfigMap` checks run against the new object.
///
/// ObjectMeta-update validation (`ValidateObjectMetaUpdate`) is enforced by the
/// generic api-server update path, so it is not re-run here.
pub fn validate_config_map_update(old: &ConfigMap, new: &ConfigMap) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if old.immutable == Some(true) {
        if new.immutable != Some(true) {
            errs.push(Error::forbidden(
                &Path::new("immutable"),
                "field is immutable when `immutable` is set",
            ));
        }
        if new.data != old.data {
            errs.push(Error::forbidden(
                &Path::new("data"),
                "field is immutable when `immutable` is set",
            ));
        }
        if new.binary_data != old.binary_data {
            errs.push(Error::forbidden(
                &Path::new("binaryData"),
                "field is immutable when `immutable` is set",
            ));
        }
    }

    errs.extend(validate_config_map(new));
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::ConfigMap;
    use std::collections::HashMap;

    fn cm() -> ConfigMap {
        ConfigMap {
            type_meta: Default::default(),
            metadata: crate::types::ObjectMeta {
                name: "cm".to_string(),
                ..Default::default()
            },
            data: None,
            binary_data: None,
            immutable: None,
        }
    }

    #[test]
    fn config_map_valid_keys_pass() {
        let mut c = cm();
        let mut data = HashMap::new();
        data.insert("good.key-1_2".to_string(), "v".to_string());
        c.data = Some(data);
        assert!(validate_config_map(&c).is_empty());
    }

    #[test]
    fn config_map_invalid_key_char_fails() {
        let mut c = cm();
        let mut data = HashMap::new();
        data.insert("bad key!".to_string(), "v".to_string());
        c.data = Some(data);
        let errs = validate_config_map(&c);
        assert!(errs
            .iter()
            .any(|e| e.detail.contains("a valid config key must consist")));
    }

    #[test]
    fn config_map_chdir_keys_fail_with_distinct_messages() {
        assert_eq!(config_map_key_errors("."), vec!["must not be '.'"]);
        assert_eq!(config_map_key_errors(".."), vec!["must not be '..'"]);
        assert_eq!(
            config_map_key_errors("..foo"),
            vec!["must not start with '..'"]
        );
        // A single leading dot is allowed (e.g. `.dockercfg`).
        assert!(config_map_key_errors(".foo").is_empty());
    }

    #[test]
    fn config_map_long_key_fails() {
        let key = "a".repeat(254);
        let errs = config_map_key_errors(&key);
        assert!(errs.iter().any(|m| m.contains("no more than 253")));
    }

    #[test]
    fn config_map_duplicate_key_across_bags_fails() {
        let mut c = cm();
        let mut data = HashMap::new();
        data.insert("dup".to_string(), "v".to_string());
        c.data = Some(data);
        let mut bin = HashMap::new();
        bin.insert("dup".to_string(), vec![1u8, 2, 3]);
        c.binary_data = Some(bin);
        let errs = validate_config_map(&c);
        assert!(errs
            .iter()
            .any(|e| e.detail.contains("duplicate of key present in binaryData")));
    }

    #[test]
    fn config_map_oversize_fails() {
        let mut c = cm();
        let mut data = HashMap::new();
        data.insert("big".to_string(), "x".repeat(MAX_SECRET_SIZE + 1));
        c.data = Some(data);
        let errs = validate_config_map(&c);
        assert!(!errs.is_empty());
    }

    #[test]
    fn config_map_at_size_limit_passes() {
        let mut c = cm();
        let mut data = HashMap::new();
        // key length counts toward IsConfigMapKey only, not total size; total
        // size is the sum of value lengths.
        data.insert("k".to_string(), "x".repeat(MAX_SECRET_SIZE));
        c.data = Some(data);
        assert!(validate_config_map(&c).is_empty());
    }

    #[test]
    fn config_map_update_mutable_allows_data_change() {
        let mut old = cm();
        let mut new = cm();
        let mut d1 = HashMap::new();
        d1.insert("a".to_string(), "1".to_string());
        old.data = Some(d1);
        let mut d2 = HashMap::new();
        d2.insert("a".to_string(), "2".to_string());
        new.data = Some(d2);
        assert!(validate_config_map_update(&old, &new).is_empty());
    }

    #[test]
    fn config_map_update_immutable_forbids_data_change() {
        let mut old = cm();
        old.immutable = Some(true);
        let mut d1 = HashMap::new();
        d1.insert("a".to_string(), "1".to_string());
        old.data = Some(d1);

        let mut new = cm();
        new.immutable = Some(true);
        let mut d2 = HashMap::new();
        d2.insert("a".to_string(), "2".to_string());
        new.data = Some(d2);

        let errs = validate_config_map_update(&old, &new);
        assert!(errs.iter().any(|e| e.field == "data"
            && e.detail
                .contains("field is immutable when `immutable` is set")));
    }

    #[test]
    fn config_map_update_immutable_forbids_clearing_immutable_flag() {
        let mut old = cm();
        old.immutable = Some(true);
        let mut new = cm();
        new.immutable = Some(false);
        let errs = validate_config_map_update(&old, &new);
        assert!(errs.iter().any(|e| e.field == "immutable"));
        // Clearing immutable to None is also forbidden.
        let mut new2 = cm();
        new2.immutable = None;
        let errs2 = validate_config_map_update(&old, &new2);
        assert!(errs2.iter().any(|e| e.field == "immutable"));
    }

    #[test]
    fn config_map_update_immutable_forbids_binary_data_change() {
        let mut old = cm();
        old.immutable = Some(true);
        let mut b1 = HashMap::new();
        b1.insert("k".to_string(), vec![1u8]);
        old.binary_data = Some(b1);

        let mut new = cm();
        new.immutable = Some(true);
        let mut b2 = HashMap::new();
        b2.insert("k".to_string(), vec![2u8]);
        new.binary_data = Some(b2);

        let errs = validate_config_map_update(&old, &new);
        assert!(errs.iter().any(|e| e.field == "binaryData"));
    }

    #[test]
    fn config_map_update_immutable_unchanged_passes() {
        let mut old = cm();
        old.immutable = Some(true);
        let mut d = HashMap::new();
        d.insert("a".to_string(), "1".to_string());
        old.data = Some(d.clone());
        let mut new = cm();
        new.immutable = Some(true);
        new.data = Some(d);
        // Metadata (labels) may still change without tripping immutability.
        let mut labels = HashMap::new();
        labels.insert("x".to_string(), "y".to_string());
        new.metadata.labels = Some(labels);
        assert!(validate_config_map_update(&old, &new).is_empty());
    }
}
