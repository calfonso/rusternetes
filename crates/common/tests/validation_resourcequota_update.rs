use rusternetes_common::resources::ResourceQuota;
use rusternetes_common::validation::resourcequota::validate_resource_quota_update;
use serde_json::json;

fn rq(scopes: serde_json::Value) -> ResourceQuota {
    let mut spec = json!({"hard": {"pods": "10"}});
    if !scopes.is_null() {
        spec["scopes"] = scopes;
    }
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "rq", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

#[test]
fn unchanged_scopes_pass() {
    let old = rq(json!(["BestEffort"]));
    let new = rq(json!(["BestEffort"]));
    assert!(validate_resource_quota_update(&new, &old).is_empty());
}

#[test]
fn scopes_order_insensitive() {
    let old = rq(json!(["BestEffort", "NotTerminating"]));
    let new = rq(json!(["NotTerminating", "BestEffort"]));
    assert!(
        validate_resource_quota_update(&new, &old).is_empty(),
        "scopes compared as a set"
    );
}

#[test]
fn changing_scopes_is_immutable() {
    let old = rq(json!(["BestEffort"]));
    let new = rq(json!(["NotBestEffort"]));
    let errs = validate_resource_quota_update(&new, &old);
    assert!(
        errs.iter().any(|e| e.to_string().contains("scopes")),
        "{errs:?}"
    );
}

#[test]
fn adding_scope_is_immutable() {
    let old = rq(json!(null));
    let new = rq(json!(["BestEffort"]));
    let errs = validate_resource_quota_update(&new, &old);
    assert!(
        errs.iter().any(|e| e.to_string().contains("scopes")),
        "{errs:?}"
    );
}
