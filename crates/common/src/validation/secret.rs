//! Secret validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateSecret` and
//! `ValidateSecretUpdate` (release-1.35).
//!
//! [`validate_secret`] covers the full upstream `ValidateSecret`:
//!   * object metadata (`ValidateObjectMeta` with `ValidateSecretName` ==
//!     `NameIsDNSSubdomain`),
//!   * `data` key validity (`IsConfigMapKey`) and the `MaxSecretSize` (1 MiB)
//!     total-size cap,
//!   * the per-type required-key constraints (the `switch secret.Type` block):
//!     service-account-token, dockercfg, dockerconfigjson, basic-auth, ssh-auth
//!     and tls.
//!
//! [`validate_secret_update`] adds the update-only rules: `type` is immutable,
//! and when the *old* secret has `immutable: true`, `immutable` may not be
//! cleared and `data` may not change.
//!
//! Both must be called **after** `stringData` has been merged into `data` (the
//! handler's `Secret::normalize`), so a secret supplying its keys via
//! `stringData` is neither falsely rejected nor falsely sized — upstream
//! validates the post-conversion `Data` map only ("We don't validate
//! StringData, as it was already converted back to Data before validation").

use crate::resources::Secret;
use crate::validation::configmap::{config_map_key_errors, MAX_SECRET_SIZE};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    FIELD_IMMUTABLE_ERROR_MSG,
};

// Secret type strings (core/v1 SecretType constants).
const SECRET_TYPE_SERVICE_ACCOUNT_TOKEN: &str = "kubernetes.io/service-account-token";
const SECRET_TYPE_DOCKERCFG: &str = "kubernetes.io/dockercfg";
const SECRET_TYPE_DOCKER_CONFIG_JSON: &str = "kubernetes.io/dockerconfigjson";
const SECRET_TYPE_BASIC_AUTH: &str = "kubernetes.io/basic-auth";
const SECRET_TYPE_SSH_AUTH: &str = "kubernetes.io/ssh-auth";
const SECRET_TYPE_TLS: &str = "kubernetes.io/tls";

// Default type applied by upstream `SetDefaults_Secret` when `.type` is unset.
const SECRET_TYPE_OPAQUE: &str = "Opaque";

// Well-known data / annotation keys.
const SERVICE_ACCOUNT_NAME_KEY: &str = "kubernetes.io/service-account.name";
const DOCKER_CONFIG_KEY: &str = ".dockercfg";
const DOCKER_CONFIG_JSON_KEY: &str = ".dockerconfigjson";
const BASIC_AUTH_USERNAME_KEY: &str = "username";
const BASIC_AUTH_PASSWORD_KEY: &str = "password";
const SSH_AUTH_PRIVATE_KEY: &str = "ssh-privatekey";
const TLS_CERT_KEY: &str = "tls.crt";
const TLS_PRIVATE_KEY_KEY: &str = "tls.key";

/// Full port of upstream `ValidateSecret`.
///
/// Validates metadata, `data` key format + total size, and the type-specific
/// required keys. Field paths and error wording mirror upstream so wire errors
/// match real Kubernetes.
pub fn validate_secret(secret: &Secret) -> ErrorList {
    // `ValidateObjectMeta(&secret.ObjectMeta, true, ValidateSecretName, ...)`.
    // ValidateSecretName == apimachineryvalidation.NameIsDNSSubdomain.
    let mut errs = validate_object_meta(
        &secret.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );

    // `data` key format (IsConfigMapKey) + MaxSecretSize total-size cap.
    let data_path = Path::new("data");
    let mut total_size: usize = 0;
    if let Some(data) = &secret.data {
        for (key, value) in data {
            for msg in config_map_key_errors(key) {
                errs.push(Error::invalid(&data_path.key(key), key.clone(), msg));
            }
            total_size += value.len();
        }
    }
    if total_size > MAX_SECRET_SIZE {
        // Upstream: `field.TooLong(dataPath, "" /*unused*/, MaxSecretSize)`.
        errs.push(Error::too_long(&data_path, MAX_SECRET_SIZE));
    }

    // Per-type required-key constraints (`switch secret.Type`).
    errs.extend(validate_secret_type(secret));

    errs
}

/// Full port of upstream `ValidateSecretUpdate`.
///
/// Appends the update-only rules to [`validate_secret`]: object-meta update
/// rules, immutable `type`, and (when the old secret is `immutable: true`) the
/// forbidden `immutable`-clear and `data`-change checks.
pub fn validate_secret_update(old: &Secret, new: &Secret) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));

    // `ValidateImmutableField(newSecret.Type, oldSecret.Type, NewPath("type"))`.
    // Both sides default to "Opaque" to mirror `SetDefaults_Secret`, so an
    // update body that omits `.type` (or sends "") matches a server-defaulted
    // existing secret instead of spuriously tripping the immutability check.
    let old_type = default_type(old);
    let new_type = default_type(new);
    if new_type != old_type {
        errs.push(Error::invalid(
            &Path::new("type"),
            new_type.to_string(),
            FIELD_IMMUTABLE_ERROR_MSG,
        ));
    }

    // `if oldSecret.Immutable != nil && *oldSecret.Immutable { ... }`.
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
        // StringData is intentionally not validated here — upstream relies on
        // it already having been converted back to Data before validation.
    }

    errs.extend(validate_secret(new));
    errs
}

/// `secret.Type` defaulted to `Opaque` when unset or empty, mirroring upstream
/// `pkg/apis/core/v1/defaults.go::SetDefaults_Secret`.
fn default_type(secret: &Secret) -> &str {
    match secret.secret_type.as_deref() {
        None | Some("") => SECRET_TYPE_OPAQUE,
        Some(t) => t,
    }
}

/// Validate the type-specific required keys of a Secret — upstream
/// `ValidateSecret`'s `switch secret.Type`.
pub fn validate_secret_type(secret: &Secret) -> ErrorList {
    let mut errs = ErrorList::new();
    let data_path = Path::new("data");
    let empty = std::collections::HashMap::new();
    let data = secret.data.as_ref().unwrap_or(&empty);
    let has = |k: &str| data.contains_key(k);

    match secret.secret_type.as_deref().unwrap_or("") {
        SECRET_TYPE_SERVICE_ACCOUNT_TOKEN => {
            let name = secret
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(SERVICE_ACCOUNT_NAME_KEY));
            if name.map(|v| v.is_empty()).unwrap_or(true) {
                errs.push(Error::required(
                    &Path::new("metadata")
                        .child("annotations")
                        .key(SERVICE_ACCOUNT_NAME_KEY),
                    "",
                ));
            }
        }
        // Opaque / "" — no constraints.
        "" | "Opaque" => {}
        SECRET_TYPE_DOCKERCFG => match data.get(DOCKER_CONFIG_KEY) {
            None => errs.push(Error::required(&data_path.key(DOCKER_CONFIG_KEY), "")),
            Some(bytes) if serde_json::from_slice::<serde_json::Value>(bytes).is_err() => {
                errs.push(Error::invalid(
                    &data_path.key(DOCKER_CONFIG_KEY),
                    "<secret contents redacted>".to_string(),
                    "must be a valid JSON document",
                ));
            }
            Some(_) => {}
        },
        SECRET_TYPE_DOCKER_CONFIG_JSON => match data.get(DOCKER_CONFIG_JSON_KEY) {
            None => errs.push(Error::required(&data_path.key(DOCKER_CONFIG_JSON_KEY), "")),
            Some(bytes) if serde_json::from_slice::<serde_json::Value>(bytes).is_err() => {
                errs.push(Error::invalid(
                    &data_path.key(DOCKER_CONFIG_JSON_KEY),
                    "<secret contents redacted>".to_string(),
                    "must be a valid JSON document",
                ));
            }
            Some(_) => {}
        },
        // basic-auth: username or password may be empty, but at least one
        // field must be present.
        SECRET_TYPE_BASIC_AUTH
            if !has(BASIC_AUTH_USERNAME_KEY) && !has(BASIC_AUTH_PASSWORD_KEY) =>
        {
            errs.push(Error::required(&data_path.key(BASIC_AUTH_USERNAME_KEY), ""));
            errs.push(Error::required(&data_path.key(BASIC_AUTH_PASSWORD_KEY), ""));
        }
        SECRET_TYPE_SSH_AUTH
            if data
                .get(SSH_AUTH_PRIVATE_KEY)
                .map(|v| v.is_empty())
                .unwrap_or(true) =>
        {
            errs.push(Error::required(&data_path.key(SSH_AUTH_PRIVATE_KEY), ""));
        }
        SECRET_TYPE_TLS => {
            if !has(TLS_CERT_KEY) {
                errs.push(Error::required(&data_path.key(TLS_CERT_KEY), ""));
            }
            if !has(TLS_PRIVATE_KEY_KEY) {
                errs.push(Error::required(&data_path.key(TLS_PRIVATE_KEY_KEY), ""));
            }
        }
        // Any other (custom) type — no-op, matching upstream's default.
        _ => {}
    }

    errs
}
