//! Conformance: Secret resource + the three Pod-side consumption shapes.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/secrets.go` (env var + envFrom paths)
//!   - `test/e2e/common/storage/secrets_volume.go` (volume path)
//!   - `staging/src/k8s.io/api/core/v1/types.go::Secret`,
//!     `SecretVolumeSource`, `SecretKeySelector`, `SecretEnvSource`
//!
//! Pins:
//!   - Secret round-trips through `MemoryStorage` under
//!     `/registry/secrets/<ns>/<name>` (the same keying the kubelet's
//!     secret lookup uses).
//!   - Each of `env.valueFrom.secretKeyRef`, `envFrom.secretRef`, and
//!     `volumes[*].secret` round-trips through serde with camelCase keys.
//!
//! No runtime / container creation — those paths live in node-conformance
//! e2e and are out of unit-test scope.

use rusternetes_common::resources::{
    EnvFromSource, EnvVar, EnvVarSource, Secret, SecretEnvSource, SecretKeySelector,
    SecretVolumeSource, Volume,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// MemoryStorage round-trip — keying must match what kubelet looks up
// ---------------------------------------------------------------------------

#[tokio::test]
async fn secret_round_trips_under_registry_secrets_namespace_name_key() {
    let storage = MemoryStorage::new();
    let mut data = HashMap::new();
    data.insert("username".to_string(), b"admin".to_vec());
    data.insert("password".to_string(), b"hunter2".to_vec());
    let secret = Secret::new("db-creds", "default").with_data(data);

    let key = build_key("secrets", Some("default"), "db-creds");
    assert_eq!(key, "/registry/secrets/default/db-creds");
    storage.create(&key, &secret).await.unwrap();

    let fetched: Secret = storage.get(&key).await.unwrap();
    assert_eq!(fetched.metadata.name, "db-creds");
    assert_eq!(fetched.metadata.namespace.as_deref(), Some("default"));
    let data = fetched.data.unwrap();
    assert_eq!(
        data.get("username").map(|v| v.as_slice()),
        Some(b"admin".as_slice())
    );
    assert_eq!(
        data.get("password").map(|v| v.as_slice()),
        Some(b"hunter2".as_slice())
    );
}

// ---------------------------------------------------------------------------
// Consumption shape #1 — env.valueFrom.secretKeyRef
// ---------------------------------------------------------------------------

#[test]
fn env_var_secret_key_ref_serializes_camel_case() {
    let env = EnvVar {
        name: "DB_PASS".to_string(),
        value: None,
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: "db-creds".to_string(),
                key: "password".to_string(),
                ..Default::default()
            }),
            config_map_key_ref: None,
            field_ref: None,
            resource_field_ref: None,
            file_key_ref: None,
        }),
    };
    let v = serde_json::to_value(&env).unwrap();
    assert_eq!(v["name"], "DB_PASS");
    assert_eq!(v["valueFrom"]["secretKeyRef"]["name"], "db-creds");
    assert_eq!(v["valueFrom"]["secretKeyRef"]["key"], "password");
}

// ---------------------------------------------------------------------------
// Consumption shape #2 — envFrom.secretRef
// ---------------------------------------------------------------------------

#[test]
fn env_from_secret_ref_with_optional_flag_round_trips() {
    let ef = EnvFromSource {
        prefix: Some("APP_".to_string()),
        config_map_ref: None,
        secret_ref: Some(SecretEnvSource {
            name: "shared-env".to_string(),
            optional: Some(true),
        }),
    };
    let v = serde_json::to_value(&ef).unwrap();
    assert_eq!(v["prefix"], "APP_");
    assert_eq!(v["secretRef"]["name"], "shared-env");
    assert_eq!(v["secretRef"]["optional"], true);

    let decoded: EnvFromSource = serde_json::from_value(v).unwrap();
    let sr = decoded.secret_ref.unwrap();
    assert_eq!(sr.name, "shared-env");
    assert_eq!(sr.optional, Some(true));
}

// ---------------------------------------------------------------------------
// Consumption shape #3 — volumes[*].secret
// ---------------------------------------------------------------------------

#[test]
fn secret_volume_source_uses_camel_case_keys() {
    let vol = Volume {
        name: "creds".to_string(),
        secret: Some(SecretVolumeSource {
            secret_name: Some("db-creds".to_string()),
            items: None,
            default_mode: Some(0o400),
            optional: Some(false),
        }),
        empty_dir: None,
        host_path: None,
        config_map: None,
        persistent_volume_claim: None,
        downward_api: None,
        csi: None,
        ephemeral: None,
        nfs: None,
        iscsi: None,
        projected: None,
        image: None,
    };
    let v = serde_json::to_value(&vol).unwrap();
    assert_eq!(v["secret"]["secretName"], "db-creds");
    assert_eq!(v["secret"]["defaultMode"], 0o400);
    assert_eq!(v["secret"]["optional"], false);
}
