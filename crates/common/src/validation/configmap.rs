//! ConfigMap validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateConfigMap` (release-1.35).
//!
//! Scope: key validity (`IsConfigMapKey`) for `data` and `binaryData`, the
//! cross-bag duplicate-key check, and the `MaxSecretSize` (1 MiB) total-size cap.

use crate::resources::ConfigMap;
use crate::validation::field::{Error, ErrorList, Path};

/// Upstream `core.MaxSecretSize` (`pkg/apis/core/types.go`): the combined byte
/// length of a ConfigMap's `data` + `binaryData` (and a Secret's `data`) values
/// may not exceed 1 MiB.
pub const MAX_SECRET_SIZE: usize = 1024 * 1024;

/// Port of upstream `IsConfigMapKey`: ≤253 chars, matching `[-._a-zA-Z0-9]+`,
/// and not `.`/`..`. Returns the upstream-style messages.
fn config_map_key_errors(key: &str) -> Vec<String> {
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
    if key == "." || key == ".." {
        errs.push("must not be '.' or '..'".to_string());
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
                errs.push(Error::invalid(&data_path.child(key), key.clone(), msg));
            }
            if cfg
                .binary_data
                .as_ref()
                .is_some_and(|b| b.contains_key(key))
            {
                errs.push(Error::invalid(
                    &data_path.child(key),
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
                errs.push(Error::invalid(&bin_path.child(key), key.clone(), msg));
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
