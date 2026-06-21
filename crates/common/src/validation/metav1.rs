//! Rusternetes port of upstream
//! `k8s.io/apimachinery/pkg/apis/meta/v1/validation/validation.go`
//! (release-1.35).
//!
//! Mirrors upstream structure: validators return [`ErrorList`] (a
//! `Vec<Error>`) and *accumulate* every problem they find rather than
//! short-circuiting on the first failure. Field paths and error wording match
//! upstream byte-for-byte so conformance log greps stay valid.
//!
//! Upstream:
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/validation/validation.go>

use std::collections::HashMap;

use crate::deletion::DeleteOptions;
use crate::types::{
    Condition, DeletionPropagation, LabelSelector, LabelSelectorRequirement, ManagedFieldsEntry,
};
use crate::validation::field::{Error, ErrorList, Path};
use once_cell::sync::Lazy;
use regex::Regex;

// -- low-level "is X" helpers (port of k8s.io/apimachinery/pkg/util/validation) ----

const LABEL_KEY_FMT: &str = "([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]";
const LABEL_KEY_ERR_MSG: &str =
    "must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character";
const LABEL_KEY_MAX_LENGTH: usize = 63;

const LABEL_VALUE_ERR_MSG: &str =
    "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character";
const LABEL_VALUE_MAX_LENGTH: usize = 63;

const DNS1123_LABEL_FMT: &str = "[a-z0-9]([-a-z0-9]*[a-z0-9])?";
const DNS1123_LABEL_ERR_MSG: &str =
    "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character";
const DNS1123_LABEL_MAX_LENGTH: usize = 63;

const DNS1123_SUBDOMAIN_FMT: &str =
    "[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*";
const DNS1123_SUBDOMAIN_ERR_MSG: &str =
    "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character";
const DNS1123_SUBDOMAIN_MAX_LENGTH: usize = 253;

// Underscore-permissive subdomain — mirrors upstream
// `dns1123SubdomainFmtWithUnderscore` in
// `staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go`. Gated
// by the `RelaxedDNSSearchValidation` feature in pod DNS search validation.
// Each label may carry one leading underscore (`_sip`, `_tcp`, etc.), and
// label-interior dashes/underscores are allowed; the label must still start
// and end with an alphanumeric character (after the optional leading `_`).
const DNS1123_SUBDOMAIN_FMT_WITH_UNDERSCORE: &str =
    "_?[a-z0-9]([-_a-z0-9]*[a-z0-9])?(\\._?[a-z0-9]([-_a-z0-9]*[a-z0-9])?)*";
const DNS1123_SUBDOMAIN_ERR_MSG_FG: &str =
    "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '_', '-' or '.', and must start and end with an alphanumeric character";

static LABEL_KEY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(&format!("^{LABEL_KEY_FMT}$")).expect("label key regex"));
// Label value upstream is `(labelKeyFmt)?` — i.e. either empty or a label-key.
static LABEL_VALUE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(&format!("^({LABEL_KEY_FMT})?$")).expect("label value regex"));
static DNS1123_LABEL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(&format!("^{DNS1123_LABEL_FMT}$")).expect("dns1123 label regex"));
static DNS1123_SUBDOMAIN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!("^{DNS1123_SUBDOMAIN_FMT}$")).expect("dns1123 subdomain regex")
});
static DNS1123_SUBDOMAIN_WITH_UNDERSCORE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!("^{DNS1123_SUBDOMAIN_FMT_WITH_UNDERSCORE}$"))
        .expect("dns1123 subdomain with underscore regex")
});
static CONDITION_REASON_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$").expect("condition reason regex")
});

const CONDITION_REASON_FMT: &str = "[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?";
const CONDITION_REASON_ERR_MSG: &str = "a condition reason must start with alphabetic character, optionally followed by a string of alphanumeric characters or '_,:', and must end with an alphanumeric character or '_'";

const MAX_REASON_LEN: usize = 1024;
const MAX_MESSAGE_LEN: usize = 32 * 1024;

/// Upstream `validation.RegexError(msg, fmt, examples...)` — formats the
/// canonical "regex used for validation" tail.
fn regex_error(msg: &str, fmt: &str, examples: &[&str]) -> String {
    if examples.is_empty() {
        return format!("{msg} (regex used for validation is '{fmt}')");
    }
    let mut out = String::from(msg);
    out.push_str(" (e.g. ");
    for (i, ex) in examples.iter().enumerate() {
        if i > 0 {
            out.push_str(" or ");
        }
        out.push('\'');
        out.push_str(ex);
        out.push_str("', ");
    }
    out.push_str("regex used for validation is '");
    out.push_str(fmt);
    out.push_str("')");
    out
}

fn max_len_error(length: usize) -> String {
    format!("must be no more than {length} bytes")
}

fn empty_error() -> &'static str {
    "must be non-empty"
}

/// Upstream `validation.IsDNS1123Subdomain`.
pub fn is_dns1123_subdomain(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > DNS1123_SUBDOMAIN_MAX_LENGTH {
        errs.push(max_len_error(DNS1123_SUBDOMAIN_MAX_LENGTH));
    }
    if !DNS1123_SUBDOMAIN_RE.is_match(value) {
        errs.push(regex_error(
            DNS1123_SUBDOMAIN_ERR_MSG,
            DNS1123_SUBDOMAIN_FMT,
            &["example.com"],
        ));
    }
    errs
}

/// Upstream `validation.IsDNS1123SubdomainWithUnderScore` — the relaxed
/// variant that allows a single leading underscore per label. Used by pod
/// `dnsConfig.searches` when the `RelaxedDNSSearchValidation` feature gate
/// is enabled. Source:
/// `staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go`.
pub fn is_dns1123_subdomain_with_underscore(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > DNS1123_SUBDOMAIN_MAX_LENGTH {
        errs.push(max_len_error(DNS1123_SUBDOMAIN_MAX_LENGTH));
    }
    if !DNS1123_SUBDOMAIN_WITH_UNDERSCORE_RE.is_match(value) {
        errs.push(regex_error(
            DNS1123_SUBDOMAIN_ERR_MSG_FG,
            DNS1123_SUBDOMAIN_FMT_WITH_UNDERSCORE,
            &["example.com"],
        ));
    }
    errs
}

/// Upstream `validation.IsDNS1123Label`.
pub fn is_dns1123_label(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > DNS1123_LABEL_MAX_LENGTH {
        errs.push(max_len_error(DNS1123_LABEL_MAX_LENGTH));
    }
    if !DNS1123_LABEL_RE.is_match(value) {
        if DNS1123_SUBDOMAIN_RE.is_match(value) {
            // It was a valid subdomain and not a valid label.  Since we
            // already checked length, it must be dots.
            errs.push("must not contain dots".to_string());
        } else {
            errs.push(regex_error(
                DNS1123_LABEL_ERR_MSG,
                DNS1123_LABEL_FMT,
                &["my-name", "123-abc"],
            ));
        }
    }
    errs
}

/// Upstream `content.IsLabelKey` (a.k.a. `IsQualifiedName`).
pub fn is_qualified_name(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    let parts: Vec<&str> = value.split('/').collect();
    let name: &str;
    match parts.len() {
        1 => name = parts[0],
        2 => {
            let prefix = parts[0];
            name = parts[1];
            if prefix.is_empty() {
                errs.push(format!("prefix part {}", empty_error()));
            } else {
                for msg in is_dns1123_subdomain(prefix) {
                    errs.push(format!("prefix part {msg}"));
                }
            }
        }
        _ => {
            errs.push(format!(
                "a valid label key {} with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyName')",
                regex_error(LABEL_KEY_ERR_MSG, LABEL_KEY_FMT, &["MyName", "my.name", "123-abc"])
            ));
            return errs;
        }
    }
    if name.is_empty() {
        errs.push(format!("name part {}", empty_error()));
    } else if name.len() > LABEL_KEY_MAX_LENGTH {
        errs.push(format!("name part {}", max_len_error(LABEL_KEY_MAX_LENGTH)));
    }
    if !LABEL_KEY_RE.is_match(name) {
        errs.push(format!(
            "name part {}",
            regex_error(
                LABEL_KEY_ERR_MSG,
                LABEL_KEY_FMT,
                &["MyName", "my.name", "123-abc"]
            )
        ));
    }
    errs
}

/// Upstream `content.IsLabelValue`.
pub fn is_valid_label_value(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > LABEL_VALUE_MAX_LENGTH {
        errs.push(max_len_error(LABEL_VALUE_MAX_LENGTH));
    }
    if !LABEL_VALUE_RE.is_match(value) {
        errs.push(regex_error(
            LABEL_VALUE_ERR_MSG,
            // Upstream renders the inner labelKeyFmt-based labelValueFmt.
            &format!("({LABEL_KEY_FMT})?"),
            &["MyValue", "my_value", "12345"],
        ));
    }
    errs
}

// -- public validators -------------------------------------------------------

/// Upstream `ValidateLabelName`.
pub fn validate_label_name(label_name: &str, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    for msg in is_qualified_name(label_name) {
        errs.push(Error::invalid(fld_path, label_name, msg).with_origin("format=k8s-label-key"));
    }
    errs
}

/// Upstream `ValidateLabels`.
///
/// Note: upstream iterates a Go `map[string]string` whose order is
/// unspecified. Rust's `HashMap` shares that property. For deterministic
/// reporting we sort keys before iterating.
pub fn validate_labels(labels: &HashMap<String, String>, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let mut keys: Vec<&String> = labels.keys().collect();
    keys.sort();
    for k in keys {
        let v = &labels[k];
        errs.extend(validate_label_name(k, fld_path));
        for msg in is_valid_label_value(v) {
            errs.push(
                Error::invalid(fld_path, v.clone(), msg).with_origin("format=k8s-label-value"),
            );
        }
    }
    errs
}

/// Options for [`validate_label_selector`]. Mirrors upstream
/// `LabelSelectorValidationOptions`.
#[derive(Debug, Clone, Copy, Default)]
pub struct LabelSelectorValidationOptions {
    pub allow_invalid_label_value_in_selector: bool,
    pub allow_unknown_operator_in_requirement: bool,
}

/// Upstream `ValidateLabelSelector`.
pub fn validate_label_selector(
    selector: &LabelSelector,
    opts: LabelSelectorValidationOptions,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    if let Some(match_labels) = &selector.match_labels {
        errs.extend(validate_labels(
            match_labels,
            &fld_path.child("matchLabels"),
        ));
    }
    if let Some(reqs) = &selector.match_expressions {
        for (i, expr) in reqs.iter().enumerate() {
            errs.extend(validate_label_selector_requirement(
                expr,
                opts,
                &fld_path.child("matchExpressions").index(i),
            ));
        }
    }
    errs
}

/// Upstream `ValidateLabelSelectorRequirement`.
pub fn validate_label_selector_requirement(
    req: &LabelSelectorRequirement,
    opts: LabelSelectorValidationOptions,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    match req.operator.as_str() {
        "In" | "NotIn" => {
            if req.values.as_ref().is_none_or(|v| v.is_empty()) {
                errs.push(Error::required(
                    &fld_path.child("values"),
                    "must be specified when `operator` is 'In' or 'NotIn'",
                ));
            }
        }
        "Exists" | "DoesNotExist" => {
            if req.values.as_ref().is_some_and(|v| !v.is_empty()) {
                errs.push(Error::forbidden(
                    &fld_path.child("values"),
                    "may not be specified when `operator` is 'Exists' or 'DoesNotExist'",
                ));
            }
        }
        other => {
            if !opts.allow_unknown_operator_in_requirement {
                errs.push(Error::invalid(
                    &fld_path.child("operator"),
                    other.to_string(),
                    "not a valid selector operator",
                ));
            }
        }
    }
    errs.extend(validate_label_name(&req.key, &fld_path.child("key")));
    if !opts.allow_invalid_label_value_in_selector {
        if let Some(values) = &req.values {
            for (value_index, value) in values.iter().enumerate() {
                for msg in is_valid_label_value(value) {
                    errs.push(Error::invalid(
                        &fld_path.child("values").index(value_index),
                        value.clone(),
                        msg,
                    ));
                }
            }
        }
    }
    errs
}

/// Allowed dry-run values. Upstream lives in
/// `metav1.DryRunAll` = `"All"`.
const ALLOWED_DRY_RUN_VALUES: &[&str] = &["All"];

/// Upstream `ValidateDryRun`.
pub fn validate_dry_run(fld_path: &Path, dry_run: &[String]) -> ErrorList {
    let mut errs = Vec::new();
    let bad = dry_run
        .iter()
        .any(|v| !ALLOWED_DRY_RUN_VALUES.contains(&v.as_str()));
    if bad {
        errs.push(Error::not_supported(
            fld_path,
            dry_run.to_vec(),
            ALLOWED_DRY_RUN_VALUES,
        ));
    }
    errs
}

/// Maximum length of a field manager identifier. Mirrors upstream
/// `FieldManagerMaxLength`.
pub const FIELD_MANAGER_MAX_LENGTH: usize = 128;

/// Upstream `ValidateFieldManager`. Each non-printable character produces an
/// individual `Invalid` error so the position of every offender is reported.
pub fn validate_field_manager(field_manager: &str, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    if field_manager.chars().count() > FIELD_MANAGER_MAX_LENGTH {
        errs.push(Error::too_long(fld_path, FIELD_MANAGER_MAX_LENGTH));
    }
    // Walk byte positions like upstream's `for i, r := range fieldManager`.
    for (i, ch) in field_manager.char_indices() {
        if !is_printable(ch) {
            errs.push(Error::invalid(
                fld_path,
                field_manager.to_string(),
                format!("invalid character {} (at position {i})", format_rune(ch)),
            ));
        }
    }
    errs
}

/// Upstream Go's `unicode.IsPrint` covers letters, marks, numbers,
/// punctuation, symbols, and the ASCII space. Newlines, tabs, carriage
/// returns, and other control characters are NOT printable.
fn is_printable(ch: char) -> bool {
    // Treat U+0020 (space) as printable like Go does.
    if ch == ' ' {
        return true;
    }
    // Go's IsPrint excludes the other whitespace runes (\n, \t, \r, \v, \f).
    if ch.is_whitespace() {
        return false;
    }
    !ch.is_control()
}

/// Mirror Go's `fmt.Sprintf("%#U", r)` — `U+XXXX 'r'`.
fn format_rune(ch: char) -> String {
    let cp = ch as u32;
    // Match upstream rendering: `U+000A '\n'` for newline, otherwise `U+%04X 'r'`.
    // Go's `%#U` actually renders unprintable runes without the trailing rune.
    if is_printable(ch) {
        format!("U+{:04X} '{}'", cp, ch)
    } else {
        format!("U+{:04X}", cp)
    }
}

/// Maximum subresource name length. Mirrors `MaxSubresourceNameLength`.
pub const MAX_SUBRESOURCE_NAME_LENGTH: usize = 256;

/// Upstream `ValidateManagedFields`.
pub fn validate_managed_fields(fields_list: &[ManagedFieldsEntry], fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    for (i, fields) in fields_list.iter().enumerate() {
        let entry_path = fld_path.index(i);

        let operation = fields.operation.as_deref().unwrap_or("");
        match operation {
            "Apply" | "Update" => {}
            _ => {
                errs.push(Error::invalid(
                    &entry_path.child("operation"),
                    operation.to_string(),
                    "must be `Apply` or `Update`",
                ));
            }
        }

        if let Some(ft) = &fields.fields_type {
            if !ft.is_empty() && ft != "FieldsV1" {
                errs.push(Error::invalid(
                    &entry_path.child("fieldsType"),
                    ft.clone(),
                    "must be `FieldsV1`",
                ));
            }
        }

        let manager = fields.manager.as_deref().unwrap_or("");
        errs.extend(validate_field_manager(
            manager,
            &entry_path.child("manager"),
        ));

        if let Some(sub) = &fields.subresource {
            if sub.len() > MAX_SUBRESOURCE_NAME_LENGTH {
                errs.push(Error::too_long(
                    &entry_path.child("subresource"),
                    MAX_SUBRESOURCE_NAME_LENGTH,
                ));
            }
        }
    }
    errs
}

/// Upstream `ValidateConditions`.
pub fn validate_conditions(conditions: &[Condition], fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let mut first_index: HashMap<String, usize> = HashMap::new();
    for (i, condition) in conditions.iter().enumerate() {
        if first_index.contains_key(&condition.condition_type) {
            errs.push(Error::duplicate(
                &fld_path.index(i).child("type"),
                condition.condition_type.clone(),
            ));
        } else {
            first_index.insert(condition.condition_type.clone(), i);
        }
        errs.extend(validate_condition(condition, &fld_path.index(i)));
    }
    errs
}

/// Upstream `ValidateCondition`.
pub fn validate_condition(condition: &Condition, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();

    // type is set and is a valid format
    errs.extend(validate_label_name(
        &condition.condition_type,
        &fld_path.child("type"),
    ));

    // status is set and is an accepted value
    let valid_statuses = ["False", "True", "Unknown"];
    if !valid_statuses.contains(&condition.status.as_str()) {
        errs.push(Error::not_supported(
            &fld_path.child("status"),
            condition.status.clone(),
            &valid_statuses,
        ));
    }

    if let Some(gen) = condition.observed_generation {
        if gen < 0 {
            errs.push(Error::invalid(
                &fld_path.child("observedGeneration"),
                gen,
                "must be greater than or equal to zero",
            ));
        }
    }

    // Upstream treats a zero metav1.Time as "missing". Our Rust type uses
    // Option, so None == missing.
    if condition.last_transition_time.is_none() {
        errs.push(Error::required(&fld_path.child("lastTransitionTime"), ""));
    }

    match &condition.reason {
        None => {
            errs.push(Error::required(&fld_path.child("reason"), ""));
        }
        Some(reason) if reason.is_empty() => {
            errs.push(Error::required(&fld_path.child("reason"), ""));
        }
        Some(reason) => {
            for msg in is_valid_condition_reason(reason) {
                errs.push(Error::invalid(
                    &fld_path.child("reason"),
                    reason.clone(),
                    msg,
                ));
            }
            if reason.len() > MAX_REASON_LEN {
                errs.push(Error::too_long(&fld_path.child("reason"), MAX_REASON_LEN));
            }
        }
    }

    if let Some(message) = &condition.message {
        if message.len() > MAX_MESSAGE_LEN {
            errs.push(Error::too_long(&fld_path.child("message"), MAX_MESSAGE_LEN));
        }
    }

    errs
}

fn is_valid_condition_reason(value: &str) -> Vec<String> {
    if !CONDITION_REASON_RE.is_match(value) {
        return vec![regex_error(
            CONDITION_REASON_ERR_MSG,
            CONDITION_REASON_FMT,
            &[
                "my_name",
                "MY_NAME",
                "MyName",
                "ReasonA,ReasonB",
                "ReasonA:ReasonB",
            ],
        )];
    }
    Vec::new()
}

/// Upstream `ValidateDeleteOptions`.
pub fn validate_delete_options(options: &DeleteOptions) -> ErrorList {
    let mut errs = Vec::new();

    if let (Some(_), Some(policy)) = (options.orphan_dependents, options.propagation_policy) {
        // Upstream renders the propagationPolicy enum as `&str` via Go's
        // `*metav1.DeletionPropagation` pointer; we render the Rust enum name
        // (matches "Background"/"Foreground"/"Orphan") which is what JSON
        // serialises to.
        errs.push(Error::invalid(
            &Path::new("propagationPolicy"),
            propagation_policy_name(policy),
            "orphanDependents and deletionPropagation cannot be both set",
        ));
    }
    if let Some(policy) = options.propagation_policy {
        match policy {
            DeletionPropagation::Foreground
            | DeletionPropagation::Background
            | DeletionPropagation::Orphan => {}
        }
        // No NotSupported branch needed — the Rust enum is exhaustive over the
        // three allowed values; an invalid policy can't be constructed.
    }

    errs.extend(validate_dry_run(
        &Path::new("dryRun"),
        options.dry_run.as_deref().unwrap_or(&[]),
    ));
    errs.extend(validate_ignore_store_read_error(
        &Path::new("ignoreStoreReadErrorWithClusterBreakingPotential"),
        options,
    ));
    errs
}

fn propagation_policy_name(p: DeletionPropagation) -> String {
    match p {
        DeletionPropagation::Background => "Background",
        DeletionPropagation::Foreground => "Foreground",
        DeletionPropagation::Orphan => "Orphan",
    }
    .to_string()
}

/// Upstream `ValidateIgnoreStoreReadError`.
pub fn validate_ignore_store_read_error(fld_path: &Path, options: &DeleteOptions) -> ErrorList {
    let mut errs = Vec::new();
    let enabled = options
        .ignore_store_read_error_with_cluster_breaking_potential
        .unwrap_or(false);
    if !enabled {
        return errs;
    }

    if options.dry_run.as_ref().is_some_and(|v| !v.is_empty()) {
        errs.push(Error::invalid(
            fld_path,
            true,
            "cannot be set together with .dryRun",
        ));
    }
    if options.propagation_policy.is_some() {
        errs.push(Error::invalid(
            fld_path,
            true,
            "cannot be set together with .propagationPolicy",
        ));
    }
    if options.orphan_dependents.is_some() {
        errs.push(Error::invalid(
            fld_path,
            true,
            "cannot be set together with .orphanDependents",
        ));
    }
    if options.grace_period_seconds.is_some() {
        errs.push(Error::invalid(
            fld_path,
            true,
            "cannot be set together with .gracePeriodSeconds",
        ));
    }
    if options.preconditions.is_some() {
        errs.push(Error::invalid(
            fld_path,
            true,
            "cannot be set together with .preconditions",
        ));
    }
    errs
}

/// Apply-patch content types upstream uses. Exposed so callers passing a
/// concrete `Content-Type` header can match against the same constants.
pub const APPLY_YAML_PATCH_TYPE: &str = "application/apply-patch+yaml";
pub const APPLY_CBOR_PATCH_TYPE: &str = "application/apply-patch+cbor";

/// Subset of upstream `metav1.PatchOptions` exercised by `validate_patch_options`.
#[derive(Debug, Clone, Default)]
pub struct PatchOptions {
    pub field_manager: Option<String>,
    pub force: Option<bool>,
    pub dry_run: Option<Vec<String>>,
    pub field_validation: Option<String>,
}

/// Upstream `ValidatePatchOptions`.
///
/// `patch_type` is the MIME type carried by the `Content-Type` header
/// (`APPLY_YAML_PATCH_TYPE`, `APPLY_CBOR_PATCH_TYPE`, etc.).
pub fn validate_patch_options(options: &PatchOptions, patch_type: &str) -> ErrorList {
    let mut errs = Vec::new();
    match patch_type {
        APPLY_YAML_PATCH_TYPE | APPLY_CBOR_PATCH_TYPE => {
            let fm_empty = options
                .field_manager
                .as_deref()
                .map(|s| s.is_empty())
                .unwrap_or(true);
            if fm_empty {
                errs.push(Error::required(
                    &Path::new("fieldManager"),
                    "is required for apply patch",
                ));
            }
        }
        _ => {
            if options.force.is_some() {
                errs.push(Error::forbidden(
                    &Path::new("force"),
                    "may not be specified for non-apply patch",
                ));
            }
        }
    }
    errs.extend(validate_field_manager(
        options.field_manager.as_deref().unwrap_or(""),
        &Path::new("fieldManager"),
    ));
    errs.extend(validate_dry_run(
        &Path::new("dryRun"),
        options.dry_run.as_deref().unwrap_or(&[]),
    ));
    errs.extend(validate_field_validation(
        &Path::new("fieldValidation"),
        options.field_validation.as_deref().unwrap_or(""),
    ));
    errs
}

const ALLOWED_FIELD_VALIDATION: &[&str] = &["", "Ignore", "Warn", "Strict"];

/// Upstream `ValidateFieldValidation`.
pub fn validate_field_validation(fld_path: &Path, field_validation: &str) -> ErrorList {
    let mut errs = Vec::new();
    if !ALLOWED_FIELD_VALIDATION.contains(&field_validation) {
        errs.push(Error::not_supported(
            fld_path,
            field_validation.to_string(),
            ALLOWED_FIELD_VALIDATION,
        ));
    }
    errs
}
