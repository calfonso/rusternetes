//! RED-state mirror of upstream `metav1` validation tests for `rusternetes-common`.
//!
//! Source (release-1.35): <https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/validation/validation_test.go>
//!
//! Each `#[test]` mirrors one `func Test*` from the upstream Go file and keeps
//! the original name so the table of contents lines up 1:1. The bodies build
//! the exact fixtures the upstream cases exercise, then call the rusternetes
//! equivalents of the upstream validators (`ValidateLabels`, `ValidateDryRun`,
//! `ValidatePatchOptions`, `ValidateFieldManager`, `ValidateManagedFields`,
//! `ValidateConditions`, `ValidateLabelSelector`, `ValidateDeleteOptions`).
//!
//! None of those entry points exist in `rusternetes-common` today, so the
//! tests that depend on them are marked `#[ignore = "upstream-mirror: TODO
//! when <function> exists"]`. The fixture construction itself stays compiled
//! and clippy-clean so any drift in the `types`/`deletion`/`server_side_apply`
//! shapes is caught immediately. When a validator lands, drop the matching
//! `#[ignore]` and the assertion becomes a live red-or-green pin.
//!
//! Part of the /batch landing upstream integration-test mirrors as RED-state
//! TDD pins.

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

// -- upstream Go: TestValidateLabels (line 33) --------------------------------

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_labels exists"]
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

    // Upstream pins (label-name error cases):
    //   {"nospecialchars^=@": "bar"}     -> "name part must consist of"
    //   {"cantendwithadash-": "bar"}     -> "name part must consist of"
    //   {"only/one/slash": "bar"}        -> "a valid label key must consist of"
    //   {strings.Repeat("a", 254): "bar"}-> "must be no more than"
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

    // Upstream pins (label-value error cases):
    //   {"toolongvalue": strings.Repeat("a", 64)} -> "must be no more than"
    //   {"backslashesinvalue": "some\\bad\\value"} -> "a valid label must be ..."
    //   {"nocommasallowed": "bad,value"}           -> "a valid label must be ..."
    //   {"strangecharsinvalue": "?#$notsogood"}    -> "a valid label must be ..."
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

    // TODO: call `rusternetes_common::validation::validate_labels(&case, "field")`
    // and assert success/failure. The function does not exist yet, so this test
    // is `#[ignore]` and only the fixtures are exercised here.
    assert!(!success_cases.is_empty());
    assert_eq!(label_name_error_cases.len(), 4);
    assert_eq!(label_value_error_cases.len(), 4);
}

// -- upstream Go: TestValidDryRun (line 103) ----------------------------------

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_dry_run exists"]
fn test_valid_dry_run() {
    // Upstream cases: {}, {"All"}, {"All", "All"} — all must be valid dry-run values.
    let tests: Vec<Vec<&'static str>> = vec![vec![], vec!["All"], vec!["All", "All"]];
    for case in &tests {
        // TODO: assert validate_dry_run(case).is_empty()
        assert!(case.iter().all(|s| *s == "All"));
    }
}

// -- upstream Go: TestInvalidDryRun (line 119) --------------------------------

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_dry_run exists"]
fn test_invalid_dry_run() {
    // Upstream cases: {"False"}, {"All", "False"} — both must FAIL validation.
    let tests: Vec<Vec<&'static str>> = vec![vec!["False"], vec!["All", "False"]];
    for case in &tests {
        // TODO: assert !validate_dry_run(case).is_empty()
        assert!(case.contains(&"False"));
    }
}

// -- upstream Go: TestValidateDeleteOptionsWithIgnoreStoreReadError (line 135)
// The upstream `metav1.DeleteOptions.IgnoreStoreReadErrorWithClusterBreakingPotential`
// field is not modelled by `rusternetes_common::deletion::DeleteOptions` yet, so
// the cases that depend on it are pinned for when the field lands.

#[test]
#[ignore = "upstream-mirror: TODO when DeleteOptions.ignore_store_read_error_with_cluster_breaking_potential exists"]
fn test_validate_delete_options_with_ignore_store_read_error() {
    // Case 1: option is nil — DryRun set, expect no errors.
    let _opts_nil = DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: None,
        preconditions: None,
        orphan_dependents: None,
        dry_run: Some(vec!["All".to_string()]),
    };

    // Case 2: option is false, PropagationPolicy is set — expect no errors.
    let _opts_false_propagation = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Background),
        grace_period_seconds: Some(0),
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: None,
        }),
        orphan_dependents: None,
        dry_run: Some(vec!["All".to_string()]),
    };

    // Case 3: option is false, OrphanDependents is set — expect no errors.
    let _opts_false_orphan = DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: Some(0),
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: None,
        }),
        orphan_dependents: Some(true),
        dry_run: Some(vec!["All".to_string()]),
    };

    // Case 4: option is true, PropagationPolicy is set — expect 4 errors:
    //   - cannot be set together with .dryRun
    //   - cannot be set together with .propagationPolicy
    //   - cannot be set together with .gracePeriodSeconds
    //   - cannot be set together with .preconditions
    let _opts_true_propagation = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Background),
        grace_period_seconds: Some(0),
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: None,
        }),
        orphan_dependents: None,
        dry_run: Some(vec!["All".to_string()]),
    };

    // Case 5: option is true, OrphanDependents is set — expect 4 errors:
    //   - cannot be set together with .dryRun
    //   - cannot be set together with .orphanDependents
    //   - cannot be set together with .gracePeriodSeconds
    //   - cannot be set together with .preconditions
    let _opts_true_orphan = DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: Some(0),
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: None,
        }),
        orphan_dependents: Some(true),
        dry_run: Some(vec!["All".to_string()]),
    };

    // Case 6: option is true, no other option set — expect no errors.
    let _opts_true_only = DeleteOptions {
        propagation_policy: None,
        grace_period_seconds: None,
        preconditions: None,
        orphan_dependents: None,
        dry_run: None,
    };

    // TODO: when validate_delete_options exists, drive every case above through
    // it and assert the error list matches the upstream expectations.
}

// -- upstream Go: TestValidPatchOptions (line 225) ----------------------------
// `rusternetes_common` has no `PatchOptions` struct nor `validate_patch_options`
// function yet; this test pins the fixtures from the upstream success table.

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common PatchOptions + validate_patch_options exist"]
fn test_valid_patch_options() {
    // Upstream success cases (opts, patchType):
    //   {Force: true, FieldManager: "kubectl"}, ApplyYAMLPatchType
    //   {FieldManager: "kubectl"},              ApplyYAMLPatchType
    //   {Force: true, FieldManager: "kubectl"}, ApplyCBORPatchType
    //   {FieldManager: "kubectl"},              ApplyCBORPatchType
    //   {},                                     MergePatchType
    //   {FieldManager: "patcher"},              MergePatchType
    let _cases: Vec<(Option<&'static str>, Option<bool>, &'static str)> = vec![
        (Some("kubectl"), Some(true), "application/apply-patch+yaml"),
        (Some("kubectl"), None, "application/apply-patch+yaml"),
        (Some("kubectl"), Some(true), "application/apply-patch+cbor"),
        (Some("kubectl"), None, "application/apply-patch+cbor"),
        (None, None, "application/merge-patch+json"),
        (Some("patcher"), None, "application/merge-patch+json"),
    ];
    // TODO: when PatchOptions + validate_patch_options exist, replace the
    // sentinel tuples with real fixtures and assert no errors.
}

// -- upstream Go: TestInvalidPatchOptions (line 271) --------------------------

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common PatchOptions + validate_patch_options exist"]
fn test_invalid_patch_options() {
    // Upstream failure cases (opts, patchType):
    //   {},                                       ApplyYAMLPatchType  (missing manager)
    //   {},                                       ApplyCBORPatchType  (missing manager)
    //   {Force: true},                            MergePatchType      (force on non-apply)
    //   {FieldManager: "kubectl", Force: false},  MergePatchType      (force on non-apply)
    let _cases: Vec<(Option<&'static str>, Option<bool>, &'static str)> = vec![
        (None, None, "application/apply-patch+yaml"),
        (None, None, "application/apply-patch+cbor"),
        (None, Some(true), "application/merge-patch+json"),
        (Some("kubectl"), Some(false), "application/merge-patch+json"),
    ];
    // TODO: when PatchOptions + validate_patch_options exist, replace the
    // sentinel tuples with real fixtures and assert at least one error.
}

// -- upstream Go: TestValidateFieldManagerValid (line 313) --------------------

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_field_manager exists"]
fn test_validate_field_manager_valid() {
    // Upstream: "filedManager", "你好" (Hello), "🍔" — all valid.
    let valid: Vec<&'static str> = vec!["filedManager", "你好", "🍔"];
    for name in &valid {
        // TODO: assert validate_field_manager(name, "fieldManager").is_empty()
        assert!(!name.is_empty());
    }
}

// -- upstream Go: TestValidateFieldManagerInvalid (line 330) ------------------

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_field_manager exists"]
fn test_validate_field_manager_invalid() {
    // Upstream: "field\nmanager" (newline), 129-char "f...f" (too long).
    let invalid: Vec<String> = vec!["field\nmanager".to_string(), "f".repeat(129)];
    for name in &invalid {
        // TODO: assert !validate_field_manager(name, "fieldManager").is_empty()
        assert!(!name.is_empty());
    }
}

// -- upstream Go: TestValidateManagedFieldsInvalid (line 346) -----------------

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_managed_fields exists"]
fn test_validate_managed_fields_invalid() {
    // Upstream invalid entries (each individually should fail validation):
    //   { Operation: Update,  FieldsType: "RandomVersion", APIVersion: "v1" }
    //   { Operation: "RandomOperation", FieldsType: "FieldsV1", APIVersion: "v1" }
    //   { /* missing operation */ FieldsType: "FieldsV1", APIVersion: "v1" }
    //   { Operation: Update, FieldsType: "FieldsV1", APIVersion: "v1",
    //     Manager: "field\nmanager" }
    //   { Operation: Apply, FieldsType: "FieldsV1", APIVersion: "v1",
    //     Subresource: <256-char string> }
    let too_long_subresource = "TooLong".repeat(40);
    let _cases: Vec<ManagedFieldsEntry> = vec![
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
    // TODO: when validate_managed_fields exists, run each case through it and
    // assert at least one error.
}

// -- upstream Go: TestValidateMangedFieldsValid (line 382) --------------------
// Note: upstream typo preserved ("Manged"). Renamed to the corrected form here.

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_managed_fields exists"]
fn test_validate_managed_fields_valid() {
    // Upstream valid entries:
    //   { Operation: Update, APIVersion: "v1" /* FieldsType missing OK */ }
    //   { Operation: Update, FieldsType: "FieldsV1", APIVersion: "v1" }
    //   { Operation: Apply,  FieldsType: "FieldsV1", APIVersion: "v1",
    //     Subresource: "scale" }
    //   { Operation: Apply,  FieldsType: "FieldsV1", APIVersion: "v1",
    //     Manager: "🍔" }
    let _cases: Vec<ManagedFieldsEntry> = vec![
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
    // TODO: when validate_managed_fields exists, run each case through it and
    // assert no errors.
}

// -- upstream Go: TestValidateConditions (line 413) ---------------------------
// Split per upstream subtest for finer-grained RED pinning.

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_conditions exists"]
fn test_validate_conditions_bunch_of_invalid_fields() {
    // Upstream invalid condition (one entry):
    //   Type: ":invalid", Status: "unknown", ObservedGeneration: -1,
    //   LastTransitionTime: <zero>, Reason: "invalid;val", Message: ""
    // Expected error needles:
    //   status.conditions[0].type: Invalid value ":invalid"
    //   status.conditions[0].status: Unsupported value "unknown"
    //   status.conditions[0].observedGeneration: Invalid value -1
    //   status.conditions[0].lastTransitionTime: Required value
    //   status.conditions[0].reason: Invalid value "invalid;val"
    let _conditions = vec![Condition {
        condition_type: ":invalid".to_string(),
        status: "unknown".to_string(),
        observed_generation: Some(-1),
        last_transition_time: None,
        reason: Some("invalid;val".to_string()),
        message: Some(String::new()),
    }];
    // TODO: assert validate_conditions(_conditions, "status.conditions") emits
    // all five upstream error needles.
}

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_conditions exists"]
fn test_validate_conditions_duplicates() {
    // Upstream: ["First", "Second", "First"] — error at index 2 must contain
    //   `status.conditions[2].type: Duplicate value: "First"`
    let _conditions = vec![
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
    // TODO: assert validate_conditions emits the Duplicate value error at [2].
}

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_conditions exists"]
fn test_validate_conditions_colon_allowed_in_reason() {
    // Upstream: reason "valid:val" must NOT produce a `.reason` error.
    let _conditions = vec![Condition {
        condition_type: "First".to_string(),
        status: String::new(),
        observed_generation: None,
        last_transition_time: None,
        reason: Some("valid:val".to_string()),
        message: None,
    }];
    // TODO: assert no error whose prefix is `status.conditions[0].reason`.
}

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_conditions exists"]
fn test_validate_conditions_comma_allowed_in_reason() {
    // Upstream: reason "valid,val" must NOT produce a `.reason` error.
    let _conditions = vec![Condition {
        condition_type: "First".to_string(),
        status: String::new(),
        observed_generation: None,
        last_transition_time: None,
        reason: Some("valid,val".to_string()),
        message: None,
    }];
    // TODO: assert no error whose prefix is `status.conditions[0].reason`.
}

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_conditions exists"]
fn test_validate_conditions_reason_does_not_end_in_delimiter() {
    // Upstream: reason "valid,val:" MUST produce
    //   `status.conditions[0].reason: Invalid value: "valid,val:"`
    let _conditions = vec![Condition {
        condition_type: "First".to_string(),
        status: String::new(),
        observed_generation: None,
        last_transition_time: None,
        reason: Some("valid,val:".to_string()),
        message: None,
    }];
    // TODO: assert validate_conditions emits the invalid-reason error.
}

// -- upstream Go: TestLabelSelectorMatchExpression (line 511) -----------------
// Upstream uses a single test with subtests; mirror each as its own #[test].

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_label_selector exists"]
fn test_label_selector_match_expression_valid() {
    // Upstream: valid selector — Key "key", Operator In, Values ["value"].
    let _sel = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "key".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["value".to_string()]),
        }]),
    };
    // TODO: assert validate_label_selector(_sel, opts{AllowInvalidLabelValueInSelector: false},
    //   "labelSelector").is_empty()
}

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_label_selector exists"]
fn test_label_selector_match_expression_invalid_key() {
    // Upstream: Key "-key" — expect 1 error containing
    //   "name part must consist of alphanumeric characters".
    let _sel = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "-key".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["value".to_string()]),
        }]),
    };
    // TODO: assert single error containing the upstream message.
}

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_label_selector exists"]
fn test_label_selector_match_expression_invalid_operator() {
    // Upstream: Operator "abc" — expect 1 error containing
    //   "not a valid selector operator".
    let _sel = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "key".to_string(),
            operator: "abc".to_string(),
            values: Some(vec!["value".to_string()]),
        }]),
    };
    // TODO: assert single error containing the upstream message.
}

#[test]
#[ignore = "upstream-mirror: TODO when rusternetes_common::validation::validate_label_selector exists"]
fn test_label_selector_match_expression_invalid_value() {
    // Upstream: Values ["-value"] — expect 1 error containing
    //   "a valid label must be an empty string or consist of".
    let _sel = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "key".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["-value".to_string()]),
        }]),
    };
    // TODO: assert single error containing the upstream message.
}
