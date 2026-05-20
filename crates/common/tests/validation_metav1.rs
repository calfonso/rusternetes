//! Mirror of upstream `metav1` validation tests for `rusternetes-common`.
//!
//! Source (release-1.35): <https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/validation/validation_test.go>
//!
//! Each `#[test]` mirrors one `func Test*` from the upstream Go file and
//! keeps the original name so the table of contents lines up 1:1. The
//! validator entry points live in `rusternetes_common::validation::metav1`
//! and the field-path / error types in `rusternetes_common::validation::field`.
//!
//! Originally landed RED (see PR #168). Flipped GREEN once the validators
//! arrived under `crates/common/src/validation/`.

// The fixtures intentionally mirror the upstream Go literal style: every test
// builds a `vec![...]` of cases (even single-entry ones) so that, when the
// matching validator lands, dropping the `#[ignore]` and replacing the
// placeholder `TODO` comment with a real call is a one-line edit. Clippy's
// `useless_vec` and "iter().any()" suggestions would flatten that structure
// and obscure the parity with upstream.
#![allow(clippy::useless_vec)]

use std::collections::HashMap;

use rusternetes_common::deletion::{DeleteOptions, Preconditions};
use rusternetes_common::types::{
    Condition, DeletionPropagation, LabelSelector, LabelSelectorRequirement, ManagedFieldsEntry,
};
use rusternetes_common::validation::field::Path;
use rusternetes_common::validation::metav1::{
    validate_conditions, validate_delete_options, validate_dry_run, validate_field_manager,
    validate_label_selector, validate_labels, validate_managed_fields, validate_patch_options,
    LabelSelectorValidationOptions, PatchOptions, APPLY_CBOR_PATCH_TYPE, APPLY_YAML_PATCH_TYPE,
};

const MERGE_PATCH_TYPE: &str = "application/merge-patch+json";

// -- upstream Go: TestValidateLabels (line 33) --------------------------------

#[test]
fn test_validate_labels() {
    let success_cases: Vec<HashMap<&'static str, &'static str>> = vec![
        [("simple", "bar")].into_iter().collect(),
        [("now-with-dashes", "bar")].into_iter().collect(),
        [("1-starts-with-num", "bar")].into_iter().collect(),
        [("1234", "bar")].into_iter().collect(),
        [("simple/simple", "bar")].into_iter().collect(),
        [("now-with-dashes/simple", "bar")].into_iter().collect(),
        [("now-with-dashes/now-with-dashes", "bar")]
            .into_iter()
            .collect(),
        [("now.with.dots/simple", "bar")].into_iter().collect(),
        [("now-with.dashes-and.dots/simple", "bar")]
            .into_iter()
            .collect(),
        [("1-num.2-num/3-num", "bar")].into_iter().collect(),
        [("1234/5678", "bar")].into_iter().collect(),
        [("1.2.3.4/5678", "bar")].into_iter().collect(),
        [("UpperCaseAreOK123", "bar")].into_iter().collect(),
        [("goodvalue", "123_-.BaR")].into_iter().collect(),
    ];
    for case in &success_cases {
        let labels: HashMap<String, String> = case
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let errs = validate_labels(&labels, &Path::new("field"));
        assert!(
            errs.is_empty(),
            "expected no errors for {case:?}, got: {errs:?}",
        );
    }

    let too_long_key = "a".repeat(254);
    let label_name_error_cases: Vec<(HashMap<String, String>, &'static str)> = vec![
        (
            [("nospecialchars^=@".to_string(), "bar".to_string())]
                .into_iter()
                .collect(),
            "name part must consist of",
        ),
        (
            [("cantendwithadash-".to_string(), "bar".to_string())]
                .into_iter()
                .collect(),
            "name part must consist of",
        ),
        (
            [("only/one/slash".to_string(), "bar".to_string())]
                .into_iter()
                .collect(),
            "a valid label key must consist of",
        ),
        (
            [(too_long_key, "bar".to_string())].into_iter().collect(),
            "must be no more than",
        ),
    ];
    for (labels, needle) in &label_name_error_cases {
        let errs = validate_labels(labels, &Path::new("field"));
        assert!(
            errs.iter().any(|e| e.to_string().contains(needle)),
            "expected an error containing {needle:?} for {labels:?}, got: {errs:?}",
        );
    }

    let too_long_value = "a".repeat(64);
    let label_value_error_cases: Vec<(HashMap<String, String>, &'static str)> = vec![
        (
            [("toolongvalue".to_string(), too_long_value)]
                .into_iter()
                .collect(),
            "must be no more than",
        ),
        (
            [(
                "backslashesinvalue".to_string(),
                "some\\bad\\value".to_string(),
            )]
            .into_iter()
            .collect(),
            "a valid label must be an empty string or consist of",
        ),
        (
            [("nocommasallowed".to_string(), "bad,value".to_string())]
                .into_iter()
                .collect(),
            "a valid label must be an empty string or consist of",
        ),
        (
            [(
                "strangecharsinvalue".to_string(),
                "?#$notsogood".to_string(),
            )]
            .into_iter()
            .collect(),
            "a valid label must be an empty string or consist of",
        ),
    ];
    for (labels, needle) in &label_value_error_cases {
        let errs = validate_labels(labels, &Path::new("field"));
        assert!(
            errs.iter().any(|e| e.to_string().contains(needle)),
            "expected an error containing {needle:?} for {labels:?}, got: {errs:?}",
        );
    }
}

// -- upstream Go: TestValidDryRun (line 103) ----------------------------------

#[test]
fn test_valid_dry_run() {
    let tests: Vec<Vec<&'static str>> = vec![vec![], vec!["All"], vec!["All", "All"]];
    for case in &tests {
        let dr: Vec<String> = case.iter().map(|s| (*s).to_string()).collect();
        let errs = validate_dry_run(&Path::new("dryRun"), &dr);
        assert!(
            errs.is_empty(),
            "expected no errors for {case:?}, got: {errs:?}"
        );
    }
}

// -- upstream Go: TestInvalidDryRun (line 119) --------------------------------

#[test]
fn test_invalid_dry_run() {
    let tests: Vec<Vec<&'static str>> = vec![vec!["False"], vec!["All", "False"]];
    for case in &tests {
        let dr: Vec<String> = case.iter().map(|s| (*s).to_string()).collect();
        let errs = validate_dry_run(&Path::new("dryRun"), &dr);
        assert!(!errs.is_empty(), "expected errors for {case:?}, got none");
    }
}

// -- upstream Go: TestValidateDeleteOptionsWithIgnoreStoreReadError (line 135)

#[test]
fn test_validate_delete_options_with_ignore_store_read_error() {
    // Case 1: option is nil — DryRun set, expect no errors.
    let opts_nil = DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: None,
        preconditions: None,
        orphan_dependents: None,
        dry_run: Some(vec!["All".to_string()]),
        ignore_store_read_error_with_cluster_breaking_potential: None,
    };
    let errs = validate_delete_options(&opts_nil);
    assert!(errs.is_empty(), "case 1: expected no errors, got: {errs:?}");

    // Case 2: option is false, PropagationPolicy is set — expect no errors.
    let opts_false_propagation = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Background),
        grace_period_seconds: Some(0),
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: None,
        }),
        orphan_dependents: None,
        dry_run: Some(vec!["All".to_string()]),
        ignore_store_read_error_with_cluster_breaking_potential: Some(false),
    };
    let errs = validate_delete_options(&opts_false_propagation);
    assert!(errs.is_empty(), "case 2: expected no errors, got: {errs:?}");

    // Case 3: option is false, OrphanDependents is set — expect no errors.
    let opts_false_orphan = DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: Some(0),
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: None,
        }),
        orphan_dependents: Some(true),
        dry_run: Some(vec!["All".to_string()]),
        ignore_store_read_error_with_cluster_breaking_potential: Some(false),
    };
    let errs = validate_delete_options(&opts_false_orphan);
    assert!(errs.is_empty(), "case 3: expected no errors, got: {errs:?}");

    // Case 4: option is true, PropagationPolicy is set — expect 4 errors:
    //   - cannot be set together with .dryRun
    //   - cannot be set together with .propagationPolicy
    //   - cannot be set together with .gracePeriodSeconds
    //   - cannot be set together with .preconditions
    let opts_true_propagation = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Background),
        grace_period_seconds: Some(0),
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: None,
        }),
        orphan_dependents: None,
        dry_run: Some(vec!["All".to_string()]),
        ignore_store_read_error_with_cluster_breaking_potential: Some(true),
    };
    let errs = validate_delete_options(&opts_true_propagation);
    let needles = [
        "cannot be set together with .dryRun",
        "cannot be set together with .propagationPolicy",
        "cannot be set together with .gracePeriodSeconds",
        "cannot be set together with .preconditions",
    ];
    for needle in needles {
        assert!(
            errs.iter().any(|e| e.to_string().contains(needle)),
            "case 4: missing {needle:?} in {errs:?}",
        );
    }

    // Case 5: option is true, OrphanDependents is set — expect 4 errors:
    //   - cannot be set together with .dryRun
    //   - cannot be set together with .orphanDependents
    //   - cannot be set together with .gracePeriodSeconds
    //   - cannot be set together with .preconditions
    let opts_true_orphan = DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: Some(0),
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: None,
        }),
        orphan_dependents: Some(true),
        dry_run: Some(vec!["All".to_string()]),
        ignore_store_read_error_with_cluster_breaking_potential: Some(true),
    };
    let errs = validate_delete_options(&opts_true_orphan);
    let needles = [
        "cannot be set together with .dryRun",
        "cannot be set together with .orphanDependents",
        "cannot be set together with .gracePeriodSeconds",
        "cannot be set together with .preconditions",
    ];
    for needle in needles {
        assert!(
            errs.iter().any(|e| e.to_string().contains(needle)),
            "case 5: missing {needle:?} in {errs:?}",
        );
    }

    // Case 6: option is true, no other option set — expect no errors.
    let opts_true_only = DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: None,
        preconditions: None,
        orphan_dependents: None,
        dry_run: None,
        ignore_store_read_error_with_cluster_breaking_potential: Some(false),
    };
    let errs = validate_delete_options(&opts_true_only);
    assert!(errs.is_empty(), "case 6: expected no errors, got: {errs:?}");
}

// -- upstream Go: TestValidPatchOptions (line 225) ----------------------------

#[test]
fn test_valid_patch_options() {
    // Upstream success cases (fieldManager, force, patchType):
    let cases: Vec<(Option<&'static str>, Option<bool>, &'static str)> = vec![
        (Some("kubectl"), Some(true), APPLY_YAML_PATCH_TYPE),
        (Some("kubectl"), None, APPLY_YAML_PATCH_TYPE),
        (Some("kubectl"), Some(true), APPLY_CBOR_PATCH_TYPE),
        (Some("kubectl"), None, APPLY_CBOR_PATCH_TYPE),
        (None, None, MERGE_PATCH_TYPE),
        (Some("patcher"), None, MERGE_PATCH_TYPE),
    ];
    for (field_manager, force, patch_type) in cases {
        let opts = PatchOptions {
            field_manager: field_manager.map(str::to_string),
            force,
            dry_run: None,
            field_validation: None,
        };
        let errs = validate_patch_options(&opts, patch_type);
        assert!(
            errs.is_empty(),
            "expected no errors for ({field_manager:?}, {force:?}, {patch_type}), got: {errs:?}",
        );
    }
}

// -- upstream Go: TestInvalidPatchOptions (line 271) --------------------------

#[test]
fn test_invalid_patch_options() {
    let cases: Vec<(Option<&'static str>, Option<bool>, &'static str)> = vec![
        (None, None, APPLY_YAML_PATCH_TYPE),
        (None, None, APPLY_CBOR_PATCH_TYPE),
        (None, Some(true), MERGE_PATCH_TYPE),
        (Some("kubectl"), Some(false), MERGE_PATCH_TYPE),
    ];
    for (field_manager, force, patch_type) in cases {
        let opts = PatchOptions {
            field_manager: field_manager.map(str::to_string),
            force,
            dry_run: None,
            field_validation: None,
        };
        let errs = validate_patch_options(&opts, patch_type);
        assert!(
            !errs.is_empty(),
            "expected at least one error for ({field_manager:?}, {force:?}, {patch_type})",
        );
    }
}

// -- upstream Go: TestValidateFieldManagerValid (line 313) --------------------

#[test]
fn test_validate_field_manager_valid() {
    let valid: Vec<&'static str> = vec!["filedManager", "你好", "🍔"];
    for name in &valid {
        let errs = validate_field_manager(name, &Path::new("fieldManager"));
        assert!(errs.is_empty(), "expected {name:?} valid, got: {errs:?}");
    }
}

// -- upstream Go: TestValidateFieldManagerInvalid (line 330) ------------------

#[test]
fn test_validate_field_manager_invalid() {
    let invalid: Vec<String> = vec!["field\nmanager".to_string(), "f".repeat(129)];
    for name in &invalid {
        let errs = validate_field_manager(name, &Path::new("fieldManager"));
        assert!(!errs.is_empty(), "expected {name:?} invalid, got no errors");
    }
}

// -- upstream Go: TestValidateManagedFieldsInvalid (line 346) -----------------

#[test]
fn test_validate_managed_fields_invalid() {
    let too_long_subresource = "TooLong".repeat(40);
    let cases: Vec<ManagedFieldsEntry> = vec![
        ManagedFieldsEntry {
            manager: None,
            operation: Some("Update".to_string()),
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: Some("RandomVersion".to_string()),
            fields_v1: None,
            subresource: None,
        },
        ManagedFieldsEntry {
            manager: None,
            operation: Some("RandomOperation".to_string()),
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: Some("FieldsV1".to_string()),
            fields_v1: None,
            subresource: None,
        },
        ManagedFieldsEntry {
            manager: None,
            operation: None,
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: Some("FieldsV1".to_string()),
            fields_v1: None,
            subresource: None,
        },
        ManagedFieldsEntry {
            manager: Some("field\nmanager".to_string()),
            operation: Some("Update".to_string()),
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: Some("FieldsV1".to_string()),
            fields_v1: None,
            subresource: None,
        },
        ManagedFieldsEntry {
            manager: None,
            operation: Some("Apply".to_string()),
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: Some("FieldsV1".to_string()),
            fields_v1: None,
            subresource: Some(too_long_subresource),
        },
    ];
    for case in cases {
        let errs =
            validate_managed_fields(std::slice::from_ref(&case), &Path::new("managedFields"));
        assert!(
            !errs.is_empty(),
            "expected at least one error for {case:?}, got none",
        );
    }
}

// -- upstream Go: TestValidateMangedFieldsValid (line 382) --------------------
// Note: upstream typo preserved ("Manged"). Renamed to the corrected form here.

#[test]
fn test_validate_managed_fields_valid() {
    let cases: Vec<ManagedFieldsEntry> = vec![
        ManagedFieldsEntry {
            manager: None,
            operation: Some("Update".to_string()),
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: None,
            fields_v1: None,
            subresource: None,
        },
        ManagedFieldsEntry {
            manager: None,
            operation: Some("Update".to_string()),
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: Some("FieldsV1".to_string()),
            fields_v1: None,
            subresource: None,
        },
        ManagedFieldsEntry {
            manager: None,
            operation: Some("Apply".to_string()),
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: Some("FieldsV1".to_string()),
            fields_v1: None,
            subresource: Some("scale".to_string()),
        },
        ManagedFieldsEntry {
            manager: Some("🍔".to_string()),
            operation: Some("Apply".to_string()),
            api_version: Some("v1".to_string()),
            time: None,
            fields_type: Some("FieldsV1".to_string()),
            fields_v1: None,
            subresource: None,
        },
    ];
    for case in cases {
        let errs =
            validate_managed_fields(std::slice::from_ref(&case), &Path::new("managedFields"));
        assert!(
            errs.is_empty(),
            "expected no errors for {case:?}, got: {errs:?}",
        );
    }
}

// -- upstream Go: TestValidateConditions (line 413) ---------------------------

fn conditions_path() -> Path {
    Path::new("status").child("conditions")
}

#[test]
fn test_validate_conditions_bunch_of_invalid_fields() {
    let conditions = vec![Condition {
        condition_type: ":invalid".to_string(),
        status: "unknown".to_string(),
        observed_generation: Some(-1),
        last_transition_time: None,
        reason: Some("invalid;val".to_string()),
        message: Some(String::new()),
    }];
    let errs = validate_conditions(&conditions, &conditions_path());
    let needles = [
        "status.conditions[0].type: Invalid value: \":invalid\"",
        "status.conditions[0].status: Unsupported value: \"unknown\"",
        "status.conditions[0].observedGeneration: Invalid value: -1",
        "status.conditions[0].lastTransitionTime: Required value",
        "status.conditions[0].reason: Invalid value: \"invalid;val\"",
    ];
    for needle in needles {
        assert!(
            errs.iter().any(|e| e.to_string().contains(needle)),
            "missing {needle:?} in {errs:?}",
        );
    }
}

#[test]
fn test_validate_conditions_duplicates() {
    let conditions = vec![
        Condition {
            condition_type: "First".to_string(),
            status: String::new(),
            observed_generation: None,
            last_transition_time: None,
            reason: None,
            message: None,
        },
        Condition {
            condition_type: "Second".to_string(),
            status: String::new(),
            observed_generation: None,
            last_transition_time: None,
            reason: None,
            message: None,
        },
        Condition {
            condition_type: "First".to_string(),
            status: String::new(),
            observed_generation: None,
            last_transition_time: None,
            reason: None,
            message: None,
        },
    ];
    let errs = validate_conditions(&conditions, &conditions_path());
    let needle = "status.conditions[2].type: Duplicate value: \"First\"";
    assert!(
        errs.iter().any(|e| e.to_string().contains(needle)),
        "missing {needle:?} in {errs:?}",
    );
}

#[test]
fn test_validate_conditions_colon_allowed_in_reason() {
    let conditions = vec![Condition {
        condition_type: "First".to_string(),
        status: String::new(),
        observed_generation: None,
        last_transition_time: None,
        reason: Some("valid:val".to_string()),
        message: None,
    }];
    let errs = validate_conditions(&conditions, &conditions_path());
    let prefix = "status.conditions[0].reason";
    assert!(
        !errs.iter().any(|e| e.to_string().starts_with(prefix)),
        "expected no error with prefix {prefix:?} in {errs:?}",
    );
}

#[test]
fn test_validate_conditions_comma_allowed_in_reason() {
    let conditions = vec![Condition {
        condition_type: "First".to_string(),
        status: String::new(),
        observed_generation: None,
        last_transition_time: None,
        reason: Some("valid,val".to_string()),
        message: None,
    }];
    let errs = validate_conditions(&conditions, &conditions_path());
    let prefix = "status.conditions[0].reason";
    assert!(
        !errs.iter().any(|e| e.to_string().starts_with(prefix)),
        "expected no error with prefix {prefix:?} in {errs:?}",
    );
}

#[test]
fn test_validate_conditions_reason_does_not_end_in_delimiter() {
    let conditions = vec![Condition {
        condition_type: "First".to_string(),
        status: String::new(),
        observed_generation: None,
        last_transition_time: None,
        reason: Some("valid,val:".to_string()),
        message: None,
    }];
    let errs = validate_conditions(&conditions, &conditions_path());
    let needle = "status.conditions[0].reason: Invalid value: \"valid,val:\"";
    assert!(
        errs.iter().any(|e| e.to_string().contains(needle)),
        "missing {needle:?} in {errs:?}",
    );
}

// -- upstream Go: TestLabelSelectorMatchExpression (line 511) -----------------

#[test]
fn test_label_selector_match_expression_valid() {
    let sel = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "key".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["value".to_string()]),
        }]),
    };
    let errs = validate_label_selector(
        &sel,
        LabelSelectorValidationOptions::default(),
        &Path::new("labelSelector"),
    );
    assert!(errs.is_empty(), "expected no errors, got: {errs:?}");
}

#[test]
fn test_label_selector_match_expression_invalid_key() {
    let sel = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "-key".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["value".to_string()]),
        }]),
    };
    let errs = validate_label_selector(
        &sel,
        LabelSelectorValidationOptions::default(),
        &Path::new("labelSelector"),
    );
    let needle = "name part must consist of alphanumeric characters";
    assert!(
        errs.iter().any(|e| e.to_string().contains(needle)),
        "missing {needle:?} in {errs:?}",
    );
    assert_eq!(errs.len(), 1, "expected exactly one error, got {errs:?}");
}

#[test]
fn test_label_selector_match_expression_invalid_operator() {
    let sel = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "key".to_string(),
            operator: "abc".to_string(),
            values: Some(vec!["value".to_string()]),
        }]),
    };
    let errs = validate_label_selector(
        &sel,
        LabelSelectorValidationOptions::default(),
        &Path::new("labelSelector"),
    );
    let needle = "not a valid selector operator";
    assert!(
        errs.iter().any(|e| e.to_string().contains(needle)),
        "missing {needle:?} in {errs:?}",
    );
    assert_eq!(errs.len(), 1, "expected exactly one error, got {errs:?}");
}

#[test]
fn test_label_selector_match_expression_invalid_value() {
    let sel = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "key".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["-value".to_string()]),
        }]),
    };
    let errs = validate_label_selector(
        &sel,
        LabelSelectorValidationOptions::default(),
        &Path::new("labelSelector"),
    );
    let needle = "a valid label must be an empty string or consist of";
    assert!(
        errs.iter().any(|e| e.to_string().contains(needle)),
        "missing {needle:?} in {errs:?}",
    );
    assert_eq!(errs.len(), 1, "expected exactly one error, got {errs:?}");
}
