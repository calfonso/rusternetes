use rusternetes_common::resources::admission_webhook::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use rusternetes_common::validation::webhookconfiguration::{
    validate_mutating_webhook_configuration, validate_validating_webhook_configuration,
};
use serde_json::json;

fn vwc(webhook: serde_json::Value) -> ValidatingWebhookConfiguration {
    serde_json::from_value(json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "cfg"},
        "webhooks": [webhook]
    }))
    .unwrap()
}

fn valid_webhook() -> serde_json::Value {
    json!({
        "name": "deny.example.com",
        "clientConfig": {"url": "https://example.com/hook"},
        "sideEffects": "None",
        "admissionReviewVersions": ["v1"],
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["pods"]
        }]
    })
}

#[test]
fn valid_config_has_no_errors() {
    assert!(validate_validating_webhook_configuration(&vwc(valid_webhook())).is_empty());
}

#[test]
fn side_effects_must_be_none_or_none_on_dry_run() {
    let mut w = valid_webhook();
    w["sideEffects"] = json!("Unknown");
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter().any(|e| e.to_string().contains("sideEffects")),
        "{errs:?}"
    );
}

#[test]
fn name_must_be_fully_qualified() {
    let mut w = valid_webhook();
    w["name"] = json!("foo");
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter().any(|e| e.to_string().contains("name")),
        "{errs:?}"
    );
}

#[test]
fn admission_review_versions_required_and_recognized() {
    let mut w = valid_webhook();
    w["admissionReviewVersions"] = json!([]);
    assert!(!validate_validating_webhook_configuration(&vwc(w)).is_empty());

    let mut w2 = valid_webhook();
    w2["admissionReviewVersions"] = json!(["v2"]);
    let errs = validate_validating_webhook_configuration(&vwc(w2));
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("admissionReviewVersions")),
        "{errs:?}"
    );
}

#[test]
fn client_config_exactly_one_of_url_or_service() {
    let mut w = valid_webhook();
    w["clientConfig"] = json!({
        "url": "https://example.com/hook",
        "service": {"namespace": "ns", "name": "svc"}
    });
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter().any(|e| e.to_string().contains("clientConfig")),
        "{errs:?}"
    );
}

#[test]
fn timeout_seconds_range() {
    let mut w = valid_webhook();
    w["timeoutSeconds"] = json!(60);
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("timeoutSeconds")),
        "{errs:?}"
    );
}

#[test]
fn duplicate_webhook_names() {
    let cfg: ValidatingWebhookConfiguration = serde_json::from_value(json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "cfg"},
        "webhooks": [valid_webhook(), valid_webhook()]
    }))
    .unwrap();
    let errs = validate_validating_webhook_configuration(&cfg);
    assert!(
        errs.iter().any(|e| e.to_string().contains("name")),
        "{errs:?}"
    );
}

#[test]
fn rule_wildcard_operation_exclusive() {
    let mut w = valid_webhook();
    w["rules"][0]["operations"] = json!(["*", "CREATE"]);
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter().any(|e| e.to_string().contains("operations")),
        "{errs:?}"
    );
}

#[test]
fn mutating_valid_config_has_no_errors() {
    let cfg: MutatingWebhookConfiguration = serde_json::from_value(json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "cfg"},
        "webhooks": [valid_webhook()]
    }))
    .unwrap();
    assert!(validate_mutating_webhook_configuration(&cfg).is_empty());
}
