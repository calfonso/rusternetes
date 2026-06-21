//! Type-specific Secret validation, ported from the `switch secret.Type`
//! block of upstream `ValidateSecret`
//! (`pkg/apis/core/validation/validation.go`).
//!
//! The data-key format and `MaxSecretSize` checks from `ValidateSecret` live
//! elsewhere; this module covers only the per-type required-key constraints:
//! service-account-token, dockercfg, dockerconfigjson, basic-auth, ssh-auth
//! and tls.
//!
//! Call this **after** `stringData` has been merged into `data` (the create
//! handler's `Secret::normalize`), so a secret supplying its keys via
//! `stringData` is not falsely rejected.

use crate::resources::Secret;
use crate::validation::field::{Error, ErrorList, Path};

// Secret type strings (core/v1 SecretType constants).
const SECRET_TYPE_SERVICE_ACCOUNT_TOKEN: &str = "kubernetes.io/service-account-token";
const SECRET_TYPE_DOCKERCFG: &str = "kubernetes.io/dockercfg";
const SECRET_TYPE_DOCKER_CONFIG_JSON: &str = "kubernetes.io/dockerconfigjson";
const SECRET_TYPE_BASIC_AUTH: &str = "kubernetes.io/basic-auth";
const SECRET_TYPE_SSH_AUTH: &str = "kubernetes.io/ssh-auth";
const SECRET_TYPE_TLS: &str = "kubernetes.io/tls";

// Well-known data / annotation keys.
const SERVICE_ACCOUNT_NAME_KEY: &str = "kubernetes.io/service-account.name";
const DOCKER_CONFIG_KEY: &str = ".dockercfg";
const DOCKER_CONFIG_JSON_KEY: &str = ".dockerconfigjson";
const BASIC_AUTH_USERNAME_KEY: &str = "username";
const BASIC_AUTH_PASSWORD_KEY: &str = "password";
const SSH_AUTH_PRIVATE_KEY: &str = "ssh-privatekey";
const TLS_CERT_KEY: &str = "tls.crt";
const TLS_PRIVATE_KEY_KEY: &str = "tls.key";

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
