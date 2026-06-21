//! Validation for admissionregistration.k8s.io webhook configurations,
//! ported from upstream `pkg/apis/admissionregistration/validation/validation.go`
//! (`ValidateValidatingWebhookConfiguration` / `ValidateMutatingWebhookConfiguration`).
//!
//! Scope: the behaviour-significant create-time checks — fully-qualified +
//! unique webhook names, `admissionReviewVersions` (required + recognized),
//! `sideEffects` (v1 requires None/NoneOnDryRun), `timeoutSeconds` range,
//! `clientConfig` url-XOR-service, and `rules` (operations/apiGroups/
//! apiVersions/resources required + wildcard exclusivity + scope enum).
//!
//! `failurePolicy`/`matchPolicy`/`sideEffects`/operations are typed enums in
//! the resource model, so an out-of-range *string* is already rejected at
//! decode time; here we enforce the value constraints decode can't (e.g. v1's
//! no-side-effects rule). Deep `clientConfig.url`/`service` URL validation and
//! `namespaceSelector`/`objectSelector` label-selector validation are not yet
//! ported.

use crate::resources::admission_webhook::{
    MutatingWebhook, MutatingWebhookConfiguration, RuleWithOperations, SideEffectClass,
    ValidatingWebhook, ValidatingWebhookConfiguration,
};
use crate::resources::WebhookClientConfig;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;
use std::collections::HashSet;

/// Versions of the AdmissionReview object this apiserver accepts. Mirrors
/// upstream `AcceptedAdmissionReviewVersions`.
const ACCEPTED_ADMISSION_REVIEW_VERSIONS: &[&str] = &["v1", "v1beta1"];

const VALID_SCOPES: &[&str] = &["Cluster", "Namespaced", "*"];

/// Upstream `IsFullyQualifiedName`: required, a valid DNS1123 subdomain, with at
/// least three dot-separated segments.
fn validate_fully_qualified_name(path: &Path, name: &str) -> ErrorList {
    let mut errs = ErrorList::new();
    if name.is_empty() {
        errs.push(Error::required(path, ""));
        return errs;
    }
    let sub_errs = is_dns1123_subdomain(name);
    if !sub_errs.is_empty() {
        errs.push(Error::invalid(path, name.to_string(), sub_errs.join(",")));
        return errs;
    }
    if name.split('.').count() < 3 {
        errs.push(Error::invalid(
            path,
            name.to_string(),
            "should be a domain with at least three segments separated by dots",
        ));
    }
    errs
}

/// Upstream `validateAdmissionReviewVersions` with
/// `requireRecognizedAdmissionReviewVersion = true` (the create path).
fn validate_admission_review_versions(versions: &[String], path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    if versions.is_empty() {
        errs.push(Error::required(
            path,
            format!(
                "must specify one of {}",
                ACCEPTED_ADMISSION_REVIEW_VERSIONS.join(", ")
            ),
        ));
        return errs;
    }
    let mut seen: HashSet<&str> = HashSet::new();
    let mut has_accepted = false;
    for (i, v) in versions.iter().enumerate() {
        if !seen.insert(v.as_str()) {
            errs.push(Error::invalid(
                &path.index(i),
                v.clone(),
                "duplicate version",
            ));
            continue;
        }
        if ACCEPTED_ADMISSION_REVIEW_VERSIONS.contains(&v.as_str()) {
            has_accepted = true;
        }
    }
    if !has_accepted {
        errs.push(Error::invalid(
            path,
            versions.join(","),
            format!(
                "must include at least one of {}",
                ACCEPTED_ADMISSION_REVIEW_VERSIONS.join(", ")
            ),
        ));
    }
    errs
}

fn has_wildcard(values: &[String]) -> bool {
    values.iter().any(|v| v == "*")
}

/// Upstream `validateRuleWithOperations` + `validateRule` (allowSubResource).
fn validate_rule_with_operations(rule: &RuleWithOperations, path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();

    if rule.operations.is_empty() {
        errs.push(Error::required(&path.child("operations"), ""));
    }
    let has_all = rule
        .operations
        .iter()
        .any(|o| matches!(o, crate::resources::admission_webhook::OperationType::All));
    if rule.operations.len() > 1 && has_all {
        errs.push(Error::invalid(
            &path.child("operations"),
            "*".to_string(),
            "if '*' is present, must not specify other operations",
        ));
    }

    let r = &rule.rule;
    if r.api_groups.is_empty() {
        errs.push(Error::required(&path.child("apiGroups"), ""));
    }
    if r.api_groups.len() > 1 && has_wildcard(&r.api_groups) {
        errs.push(Error::invalid(
            &path.child("apiGroups"),
            "*".to_string(),
            "if '*' is present, must not specify other API groups",
        ));
    }
    if r.api_versions.is_empty() {
        errs.push(Error::required(&path.child("apiVersions"), ""));
    }
    if r.api_versions.len() > 1 && has_wildcard(&r.api_versions) {
        errs.push(Error::invalid(
            &path.child("apiVersions"),
            "*".to_string(),
            "if '*' is present, must not specify other API versions",
        ));
    }
    for (i, v) in r.api_versions.iter().enumerate() {
        if v.is_empty() {
            errs.push(Error::required(&path.child("apiVersions").index(i), ""));
        }
    }
    if r.resources.is_empty() {
        errs.push(Error::required(&path.child("resources"), ""));
    }
    if r.resources.len() > 1 && has_wildcard(&r.resources) {
        errs.push(Error::invalid(
            &path.child("resources"),
            "*".to_string(),
            "if '*' is present, must not specify other resources",
        ));
    }
    if let Some(scope) = &r.scope {
        if !VALID_SCOPES.contains(&scope.as_str()) {
            errs.push(Error::not_supported(
                &path.child("scope"),
                scope.clone(),
                VALID_SCOPES,
            ));
        }
    }
    errs
}

fn side_effect_str(s: &SideEffectClass) -> &'static str {
    match s {
        SideEffectClass::Unknown => "Unknown",
        SideEffectClass::None => "None",
        SideEffectClass::Some => "Some",
        SideEffectClass::NoneOnDryRun => "NoneOnDryRun",
    }
}

/// v1 webhooks require `sideEffects` to be `None` or `NoneOnDryRun`
/// (`requireNoSideEffects`).
fn validate_no_side_effects(side_effects: &SideEffectClass, path: &Path) -> Option<Error> {
    match side_effects {
        SideEffectClass::None | SideEffectClass::NoneOnDryRun => None,
        other => Some(Error::not_supported(
            path,
            side_effect_str(other).to_string(),
            &["None", "NoneOnDryRun"],
        )),
    }
}

fn validate_client_config(cc: &WebhookClientConfig, path: &Path) -> Option<Error> {
    // exactly one of url or service
    if cc.url.is_none() == cc.service.is_none() {
        Some(Error::required(
            path,
            "exactly one of url or service is required",
        ))
    } else {
        None
    }
}

fn validate_timeout_seconds(timeout: Option<i32>, path: &Path) -> Option<Error> {
    match timeout {
        Some(t) if !(1..=30).contains(&t) => Some(Error::invalid(
            path,
            t,
            "the timeout value must be between 1 and 30 seconds",
        )),
        _ => None,
    }
}

/// Validate a `ValidatingWebhookConfiguration` (create path) — upstream
/// `ValidateValidatingWebhookConfiguration`.
pub fn validate_validating_webhook_configuration(
    cfg: &ValidatingWebhookConfiguration,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut names: HashSet<String> = HashSet::new();
    if let Some(webhooks) = &cfg.webhooks {
        for (i, hook) in webhooks.iter().enumerate() {
            let path = Path::new("webhooks").index(i);
            errs.extend(validate_validating_webhook(hook, &path));
            errs.extend(validate_admission_review_versions(
                &hook.admission_review_versions,
                &path.child("admissionReviewVersions"),
            ));
            if !hook.name.is_empty() && !names.insert(hook.name.clone()) {
                errs.push(Error::duplicate(&path.child("name"), hook.name.clone()));
            }
        }
    }
    errs
}

/// Validate a `MutatingWebhookConfiguration` (create path) — upstream
/// `ValidateMutatingWebhookConfiguration`.
pub fn validate_mutating_webhook_configuration(cfg: &MutatingWebhookConfiguration) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut names: HashSet<String> = HashSet::new();
    if let Some(webhooks) = &cfg.webhooks {
        for (i, hook) in webhooks.iter().enumerate() {
            let path = Path::new("webhooks").index(i);
            errs.extend(validate_mutating_webhook(hook, &path));
            errs.extend(validate_admission_review_versions(
                &hook.admission_review_versions,
                &path.child("admissionReviewVersions"),
            ));
            if !hook.name.is_empty() && !names.insert(hook.name.clone()) {
                errs.push(Error::duplicate(&path.child("name"), hook.name.clone()));
            }
        }
    }
    errs
}

fn validate_validating_webhook(hook: &ValidatingWebhook, path: &Path) -> ErrorList {
    let mut errs = validate_fully_qualified_name(&path.child("name"), &hook.name);
    for (i, rule) in hook.rules.iter().enumerate() {
        errs.extend(validate_rule_with_operations(
            rule,
            &path.child("rules").index(i),
        ));
    }
    if let Some(e) = validate_no_side_effects(&hook.side_effects, &path.child("sideEffects")) {
        errs.push(e);
    }
    if let Some(e) = validate_timeout_seconds(hook.timeout_seconds, &path.child("timeoutSeconds")) {
        errs.push(e);
    }
    if let Some(e) = validate_client_config(&hook.client_config, &path.child("clientConfig")) {
        errs.push(e);
    }
    errs
}

fn validate_mutating_webhook(hook: &MutatingWebhook, path: &Path) -> ErrorList {
    let mut errs = validate_fully_qualified_name(&path.child("name"), &hook.name);
    for (i, rule) in hook.rules.iter().enumerate() {
        errs.extend(validate_rule_with_operations(
            rule,
            &path.child("rules").index(i),
        ));
    }
    if let Some(e) = validate_no_side_effects(&hook.side_effects, &path.child("sideEffects")) {
        errs.push(e);
    }
    if let Some(e) = validate_timeout_seconds(hook.timeout_seconds, &path.child("timeoutSeconds")) {
        errs.push(e);
    }
    if let Some(e) = validate_client_config(&hook.client_config, &path.child("clientConfig")) {
        errs.push(e);
    }
    errs
}
