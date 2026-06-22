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
//! no-side-effects rule). Deep `clientConfig.url`/`service` validation
//! (`ValidateWebhookURL`/`ValidateWebhookService`) and
//! `namespaceSelector`/`objectSelector` label-selector validation are included.

use crate::resources::admission_webhook::{
    LabelSelector as WebhookLabelSelector, MutatingWebhook, MutatingWebhookConfiguration,
    RuleWithOperations, SideEffectClass, ValidatingWebhook, ValidatingWebhookConfiguration,
};
use crate::resources::WebhookClientConfig;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1035_label, is_dns1123_subdomain, validate_label_selector, LabelSelectorValidationOptions,
};
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
        // Upstream runs IsDNS1035Label on each version.
        for msg in is_dns1035_label(v) {
            errs.push(Error::invalid(&path.index(i), v.clone(), msg));
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

/// Port of upstream `webhook.ValidateWebhookURL` (forceHttps=true): scheme must
/// be `https`, host present, no user-info / fragment / query.
fn validate_webhook_url(url: &str, path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    const FORM: &str = "; desired format: https://host[/path]";
    let parsed = match ::url::Url::parse(url) {
        Ok(u) => u,
        Err(e) => {
            errs.push(Error::required(
                path,
                format!("url must be a valid URL: {e}{FORM}"),
            ));
            return errs;
        }
    };
    if parsed.scheme() != "https" {
        errs.push(Error::invalid(
            path,
            parsed.scheme().to_string(),
            format!("'https' is the only allowed URL scheme{FORM}"),
        ));
    }
    if parsed.host_str().unwrap_or("").is_empty() {
        errs.push(Error::invalid(
            path,
            String::new(),
            format!("host must be specified{FORM}"),
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        errs.push(Error::invalid(
            path,
            parsed.username().to_string(),
            "user information is not permitted in the URL",
        ));
    }
    if parsed.fragment().is_some() {
        errs.push(Error::invalid(
            path,
            parsed.fragment().unwrap_or("").to_string(),
            "fragments are not permitted in the URL",
        ));
    }
    if parsed.query().is_some() {
        errs.push(Error::invalid(
            path,
            parsed.query().unwrap_or("").to_string(),
            "query parameters are not permitted in the URL",
        ));
    }
    errs
}

/// Port of upstream `webhook.ValidateWebhookService`: name/namespace required,
/// port in 1..=65535, and the path (when set) a valid `/`-rooted URL path whose
/// non-empty segments are DNS1123 subdomains.
fn validate_webhook_service(
    name: &str,
    namespace: &str,
    svc_path: Option<&str>,
    port: Option<i32>,
    path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    if name.is_empty() {
        errs.push(Error::required(&path.child("name"), ""));
    }
    if namespace.is_empty() {
        errs.push(Error::required(&path.child("namespace"), ""));
    }
    // Port defaults to 443 when unset (SetDefaults), which is valid; only check
    // an explicitly-set value.
    if let Some(p) = port {
        if !(1..=65535).contains(&p) {
            errs.push(Error::invalid(
                &path.child("port"),
                p,
                "port is not valid: must be between 1 and 65535, inclusive",
            ));
        }
    }
    let Some(url_path) = svc_path else {
        return errs;
    };
    if url_path == "/" || url_path.is_empty() {
        return errs;
    }
    if url_path == "//" {
        errs.push(Error::invalid(
            &path.child("path"),
            url_path.to_string(),
            "segment[0] may not be empty",
        ));
        return errs;
    }
    if !url_path.starts_with('/') {
        errs.push(Error::invalid(
            &path.child("path"),
            url_path.to_string(),
            "must start with a '/'",
        ));
    }
    let mut to_check = &url_path[1..];
    if let Some(stripped) = to_check.strip_suffix('/') {
        to_check = stripped;
    }
    for (i, step) in to_check.split('/').enumerate() {
        if step.is_empty() {
            errs.push(Error::invalid(
                &path.child("path"),
                url_path.to_string(),
                format!("segment[{i}] may not be empty"),
            ));
            continue;
        }
        for failure in is_dns1123_subdomain(step) {
            errs.push(Error::invalid(
                &path.child("path"),
                url_path.to_string(),
                format!("segment[{i}]: {failure}"),
            ));
        }
    }
    errs
}

fn validate_client_config(cc: &WebhookClientConfig, path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    // exactly one of url or service
    if cc.url.is_none() == cc.service.is_none() {
        errs.push(Error::required(
            &path.child("url"),
            "exactly one of url or service is required",
        ));
    }
    if let Some(url) = &cc.url {
        errs.extend(validate_webhook_url(url, &path.child("url")));
    }
    if let Some(svc) = &cc.service {
        errs.extend(validate_webhook_service(
            &svc.name,
            &svc.namespace,
            svc.path.as_deref(),
            svc.port,
            &path.child("service"),
        ));
    }
    errs
}

/// Convert the webhook resource's `LabelSelector` to the shared
/// `types::LabelSelector` so the common `validate_label_selector` can run.
fn to_metav1_selector(s: &WebhookLabelSelector) -> crate::types::LabelSelector {
    use crate::resources::admission_webhook::LabelSelectorOperator as Op;
    crate::types::LabelSelector {
        match_labels: s.match_labels.clone(),
        match_expressions: s.match_expressions.as_ref().map(|reqs| {
            reqs.iter()
                .map(|r| crate::types::LabelSelectorRequirement {
                    key: r.key.clone(),
                    operator: match r.operator {
                        Op::In => "In",
                        Op::NotIn => "NotIn",
                        Op::Exists => "Exists",
                        Op::DoesNotExist => "DoesNotExist",
                    }
                    .to_string(),
                    values: r.values.clone(),
                })
                .collect()
        }),
    }
}

/// Validate `namespaceSelector` / `objectSelector` via the shared
/// `ValidateLabelSelector` (upstream runs it on both when present).
fn validate_webhook_selectors(
    namespace_selector: Option<&WebhookLabelSelector>,
    object_selector: Option<&WebhookLabelSelector>,
    path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let opts = LabelSelectorValidationOptions::default();
    if let Some(ns) = namespace_selector {
        errs.extend(validate_label_selector(
            &to_metav1_selector(ns),
            opts,
            &path.child("namespaceSelector"),
        ));
    }
    if let Some(os) = object_selector {
        errs.extend(validate_label_selector(
            &to_metav1_selector(os),
            opts,
            &path.child("objectSelector"),
        ));
    }
    errs
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
    errs.extend(validate_client_config(
        &hook.client_config,
        &path.child("clientConfig"),
    ));
    errs.extend(validate_webhook_selectors(
        hook.namespace_selector.as_ref(),
        hook.object_selector.as_ref(),
        path,
    ));
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
    errs.extend(validate_client_config(
        &hook.client_config,
        &path.child("clientConfig"),
    ));
    errs.extend(validate_webhook_selectors(
        hook.namespace_selector.as_ref(),
        hook.object_selector.as_ref(),
        path,
    ));
    errs
}

#[cfg(test)]
mod deep_validation_tests {
    use super::*;

    fn p() -> Path {
        Path::new("clientConfig")
    }

    #[test]
    fn valid_https_url_passes() {
        assert!(validate_webhook_url("https://example.com/hook", &p().child("url")).is_empty());
    }

    #[test]
    fn http_scheme_rejected() {
        let errs = validate_webhook_url("http://example.com", &p().child("url"));
        assert!(errs.iter().any(|e| e.detail.contains("https")), "{errs:?}");
    }

    #[test]
    fn url_with_query_and_fragment_rejected() {
        let errs = validate_webhook_url("https://example.com/h?a=1#frag", &p().child("url"));
        assert!(errs.iter().any(|e| e.detail.contains("query")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.detail.contains("fragment")),
            "{errs:?}"
        );
    }

    #[test]
    fn service_requires_name_and_namespace() {
        let errs = validate_webhook_service("", "", None, None, &p().child("service"));
        assert!(errs.iter().any(|e| e.field.ends_with("name")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field.ends_with("namespace")),
            "{errs:?}"
        );
    }

    #[test]
    fn service_port_out_of_range_rejected() {
        let errs = validate_webhook_service("svc", "ns", None, Some(0), &p().child("service"));
        assert!(
            errs.iter().any(|e| e.detail.contains("port is not valid")),
            "{errs:?}"
        );
        assert!(
            validate_webhook_service("svc", "ns", None, Some(443), &p().child("service"))
                .is_empty()
        );
    }

    #[test]
    fn service_path_empty_segment_rejected() {
        let errs =
            validate_webhook_service("svc", "ns", Some("/a//b"), None, &p().child("service"));
        assert!(
            errs.iter().any(|e| e.detail.contains("may not be empty")),
            "{errs:?}"
        );
        assert!(validate_webhook_service(
            "svc",
            "ns",
            Some("/mutate"),
            None,
            &p().child("service")
        )
        .is_empty());
    }

    #[test]
    fn admission_review_version_must_be_dns1035() {
        // Leading digit is valid DNS1123 but not DNS1035.
        let errs = validate_admission_review_versions(
            &["1v".to_string(), "v1".to_string()],
            &Path::new("admissionReviewVersions"),
        );
        assert!(
            errs.iter().any(|e| e.detail.contains("DNS-1035")),
            "{errs:?}"
        );
    }
}
