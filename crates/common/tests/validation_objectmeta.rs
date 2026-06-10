//! Scoped mirror of Kubernetes v1.35 apimachinery validation tests for
//! `ObjectMeta`.
//!
//! Source: https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/apimachinery/pkg/api/validation/objectmeta_test.go
//!
//! Each test mirrors a single `func Test*` block from upstream
//! `objectmeta_test.go` (function name preserved). Tests construct
//! `rusternetes_common::types::ObjectMeta` fixtures the same way upstream
//! builds `metav1.ObjectMeta`, then drive validation through whatever public
//! API the `rusternetes_common` validation surface exposes today.
//!
//! RED-STATE TDD: the underlying validation primitives (`ValidateObjectMeta`,
//! `ValidateObjectMetaUpdate`, `ValidateObjectMetaWithOpts`,
//! `validateObjectMetaAccessorWithOptsCommon`, `NameIsDNSSubdomain`,
//! `ValidateAnnotations`, `TotalAnnotationSizeLimitB`) have no Rust analogue
//! in `rusternetes-common` yet. Each test below is `#[ignore]` with a TODO
//! marker pointing at the missing function. They are checked-in pins that
//! will switch from `#[ignore]` to live the moment the matching function
//! lands.
//!
//! `never_loop` is allowed module-wide because every red-state test follows
//! the same shape: build a table of fixtures, iterate, then `panic!` inside
//! the body with the missing-API hint. Once the underlying validators land
//! and the bodies grow real assertions, the lint stops firing on its own.

#![allow(clippy::never_loop)]

use rusternetes_common::types::{ObjectMeta, OwnerReference};
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_annotations, validate_object_meta,
    validate_object_meta_accessor_with_opts_common, validate_object_meta_update,
    validate_object_meta_with_opts,
};
use std::collections::HashMap;

fn aggregate(errs: &ErrorList) -> String {
    errs.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Fixture helpers — these mirror metav1.ObjectMeta{...} composite literals.
// They are intentionally tiny and verbose because upstream's table-driven
// tests inline their fixtures the same way.
// ---------------------------------------------------------------------------

fn meta_named_gen(name: &str, generate_name: &str) -> ObjectMeta {
    ObjectMeta {
        name: name.to_string(),
        generate_name: if generate_name.is_empty() {
            None
        } else {
            Some(generate_name.to_string())
        },
        ..ObjectMeta::default()
    }
}

fn meta_with_ns(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
        ..ObjectMeta::default()
    }
}

fn meta_with_owners(refs: Vec<OwnerReference>) -> ObjectMeta {
    ObjectMeta {
        name: "test".to_string(),
        namespace: Some("test".to_string()),
        owner_references: Some(refs),
        ..ObjectMeta::default()
    }
}

fn owner_ref(api_version: &str, kind: &str, uid: &str, controller: Option<bool>) -> OwnerReference {
    OwnerReference {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        name: "name".to_string(),
        uid: uid.to_string(),
        block_owner_deletion: None,
        controller,
    }
}

// ===========================================================================
// TestValidateObjectMetaCustomName
// Upstream: lines 37-94
// Drives ValidateObjectMeta with a custom NameGenerator that returns
// ["wrong value"] for any input != "test". Both Name and GenerateName flow
// through the validator, so an "invalid"/"invalid" pair produces 2 errors.
// ===========================================================================
#[test]
fn test_validate_object_meta_custom_name() {
    // Table of (input, expected_n_errs, expected_err_substr).
    let cases: Vec<(ObjectMeta, usize, &'static str)> = vec![
        (meta_named_gen("test", ""), 0, ""),
        (meta_named_gen("test", "test"), 0, ""),
        (meta_named_gen("invalid", ""), 1, "wrong value"),
        (meta_named_gen("invalid", "test"), 1, "wrong value"),
        (meta_named_gen("invalid", "invalid"), 2, "wrong value"),
    ];

    fn name_fn(s: &str, _prefix: bool) -> Vec<String> {
        if s == "test" {
            Vec::new()
        } else {
            vec!["wrong value".to_string()]
        }
    }

    for (meta, n_errs, substr) in cases {
        let errs = validate_object_meta(&meta, false, name_fn, &Path::new("field"));
        if substr.is_empty() {
            assert!(errs.is_empty(), "expected no errors, got: {errs:?}");
        } else {
            assert_eq!(
                errs.len(),
                n_errs,
                "expected {n_errs} errors, got: {errs:?}"
            );
            assert!(
                errs[0].to_string().contains(substr),
                "expected substring {substr:?} in first error, got: {}",
                errs[0]
            );
        }
    }
}

// ===========================================================================
// TestValidateObjectMetaWithOptsName
// Upstream: lines 97-149
// Variant that uses ValidateNameFunc returning a field.ErrorList instead of
// []string. Always exactly 1 error for the failure cases — upstream collapses
// generateName errors when name itself is already invalid.
// ===========================================================================
#[test]
fn test_validate_object_meta_with_opts_name() {
    let cases: Vec<(ObjectMeta, &'static str)> = vec![
        (meta_named_gen("test", ""), ""),
        (meta_named_gen("test", "test"), ""),
        (meta_named_gen("invalid", ""), "wrong value"),
        (meta_named_gen("invalid", "test"), "wrong value"),
        (meta_named_gen("invalid", "invalid"), "wrong value"),
    ];

    fn name_fn(fld_path: &Path, s: &str) -> ErrorList {
        if s == "test" {
            Vec::new()
        } else {
            vec![FieldError::invalid(fld_path, s.to_string(), "wrong value")]
        }
    }

    for (meta, expected_substr) in cases {
        let errs = validate_object_meta_with_opts(&meta, false, name_fn, &Path::new("field"));
        if expected_substr.is_empty() {
            assert!(errs.is_empty(), "expected no errors, got: {errs:?}");
        } else {
            assert_eq!(
                errs.len(),
                1,
                "expected exactly 1 error, got {}: {errs:?}",
                errs.len()
            );
            assert!(
                errs[0].to_string().contains(expected_substr),
                "expected substring {expected_substr:?} in error, got: {}",
                errs[0]
            );
        }
    }
}

// ===========================================================================
// TestValidateObjectMetaNamespaces
// Upstream: lines 152-177
// Drives validateObjectMetaAccessorWithOptsCommon. Asserts:
//   - "foo.bar" namespace yields exactly 1 error containing `Invalid value: "foo.bar"`
//   - 64-rune (over 63 max) random namespace yields exactly 2 errors, both
//     containing "Invalid value"
// ===========================================================================
#[test]
fn test_validate_object_meta_namespaces() {
    // Case 1: a dot in the namespace is forbidden for DNS-label namespaces.
    let bad_dot = meta_with_ns("test", "foo.bar");
    let errs = validate_object_meta_accessor_with_opts_common(&bad_dot, true, &Path::new("field"));
    assert_eq!(errs.len(), 1, "unexpected errors: {errs:?}");
    assert!(
        errs[0].to_string().contains(r#"Invalid value: "foo.bar""#),
        "unexpected error message: {}",
        aggregate(&errs)
    );

    // Case 2: namespace longer than 63 chars (DNS-label max).
    const MAX_LENGTH: usize = 63;
    let letters: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        .chars()
        .collect();
    // Deterministic 64-char namespace — upstream uses rand.Intn but the rule
    // tested here only cares about length, not content.
    let long_ns: String = (0..MAX_LENGTH + 1)
        .map(|i| letters[i % letters.len()])
        .collect();
    let bad_long = meta_with_ns("test", &long_ns);
    let errs = validate_object_meta_accessor_with_opts_common(&bad_long, true, &Path::new("field"));
    assert_eq!(errs.len(), 2, "unexpected errors: {errs:?}");
    assert!(
        errs[0].to_string().contains("Invalid value")
            && errs[1].to_string().contains("Invalid value"),
        "unexpected error message: {}",
        aggregate(&errs)
    );
}

// ===========================================================================
// TestValidateObjectMetaOwnerReferences
// Upstream: lines 179-298
// Four cases:
//   1. single third-party owner ref → ok
//   2. Event kind as owner → "is disallowed from being an owner"
//   3. exactly one ref with Controller=true → ok
//   4. two refs with Controller=true → "Only one reference can have Controller set to true..."
// ===========================================================================
#[test]
fn test_validate_object_meta_owner_references() {
    let cases: Vec<(&'static str, ObjectMeta, bool, &'static str)> = vec![
        (
            "simple success - third party extension",
            meta_with_owners(vec![owner_ref(
                "customresourceVersion",
                "customresourceKind",
                "1",
                None,
            )]),
            false,
            "",
        ),
        (
            "simple failures - event shouldn't be set as an owner",
            meta_with_owners(vec![owner_ref("v1", "Event", "1", None)]),
            true,
            "is disallowed from being an owner",
        ),
        (
            "simple controller ref success - one reference with Controller set",
            meta_with_owners(vec![
                owner_ref("customresourceVersion", "customresourceKind", "1", Some(false)),
                owner_ref("customresourceVersion", "customresourceKind", "2", Some(true)),
                owner_ref("customresourceVersion", "customresourceKind", "3", Some(false)),
                owner_ref("customresourceVersion", "customresourceKind", "4", None),
            ]),
            false,
            "",
        ),
        (
            "simple controller ref failure - two references with Controller set",
            meta_with_owners(vec![
                owner_ref("customresourceVersion", "customresourceKind1", "1", Some(false)),
                owner_ref("customresourceVersion", "customresourceKind2", "2", Some(true)),
                owner_ref("customresourceVersion", "customresourceKind3", "3", Some(true)),
                owner_ref("customresourceVersion", "customresourceKind4", "4", None),
            ]),
            true,
            "Only one reference can have Controller set to true. \
             Found \"true\" in references for customresourceKind2/name and customresourceKind3/name",
        ),
    ];

    for (desc, meta, expect_err, err_substr) in cases {
        let errs = validate_object_meta_accessor_with_opts_common(&meta, true, &Path::new("field"));
        if !errs.is_empty() && !expect_err {
            panic!("unexpected error: {errs:?} in test case {desc}");
        }
        if errs.is_empty() && expect_err {
            panic!("expected error in test case {desc}");
        }
        if !errs.is_empty() && !errs[0].to_string().contains(err_substr) {
            panic!(
                "unexpected error message: {} in test case {desc}",
                aggregate(&errs)
            );
        }
    }
}

// ===========================================================================
// TestValidateObjectMetaUpdateIgnoresCreationTimestamp
// Upstream: lines 300-322
// CreationTimestamp on the *update* path is silently ignored — upstream
// asserts that any mutation of CreationTimestamp produces exactly 1 error
// (the timestamp delta itself is normalised, so the single error tests the
// "metadata.name immutable" path; this is a regression pin for the trio of
// add/clear/change scenarios).
// ===========================================================================
#[test]
fn test_validate_object_meta_update_ignores_creation_timestamp() {
    let old_no_ts = ObjectMeta {
        name: "test".into(),
        resource_version: Some("1".into()),
        ..ObjectMeta::default()
    };
    let new_no_ts = old_no_ts.clone();
    let ts10 = chrono::DateTime::from_timestamp(10, 0).unwrap();
    let ts11 = chrono::DateTime::from_timestamp(11, 0).unwrap();

    let mut new_with_ts = old_no_ts.clone();
    new_with_ts.creation_timestamp = Some(ts10);

    let mut old_with_ts = old_no_ts.clone();
    old_with_ts.creation_timestamp = Some(ts10);

    let mut new_with_ts11 = old_no_ts.clone();
    new_with_ts11.creation_timestamp = Some(ts11);

    // Three scenarios, each expecting exactly 1 error from ValidateObjectMetaUpdate.
    let cases: Vec<(ObjectMeta, ObjectMeta)> = vec![
        (new_no_ts.clone(), new_with_ts.clone()),
        (new_with_ts.clone(), new_no_ts),
        (old_with_ts, new_with_ts11),
    ];

    for (old, new) in cases {
        let errs = validate_object_meta_update(&new, &old, &Path::new("field"));
        assert_eq!(errs.len(), 1, "unexpected errors: {errs:?}");
    }
}

// ===========================================================================
// TestValidateFinalizersUpdate
// Upstream: lines 324-361
// Adding finalizers while DeletionTimestamp is set is forbidden, but
// removing them is allowed. Adding finalizers when no deletion is in
// progress is also allowed.
// ===========================================================================
#[test]
fn test_validate_finalizers_update() {
    let deletion_ts = Some(chrono::DateTime::from_timestamp(0, 0).unwrap());

    let mk = |finalizers: Vec<&str>, with_deletion: bool| ObjectMeta {
        name: "test".into(),
        resource_version: Some("1".into()),
        deletion_timestamp: if with_deletion { deletion_ts } else { None },
        finalizers: Some(finalizers.into_iter().map(String::from).collect()),
        ..ObjectMeta::default()
    };

    let cases: Vec<(&'static str, ObjectMeta, ObjectMeta, &'static str)> = vec![
        (
            "invalid adding finalizers",
            mk(vec!["x/a"], true),
            mk(vec!["x/a", "y/b"], true),
            "y/b",
        ),
        (
            "invalid changing finalizers",
            mk(vec!["x/a"], true),
            mk(vec!["x/b"], true),
            "x/b",
        ),
        (
            "valid removing finalizers",
            mk(vec!["x/a", "y/b"], true),
            mk(vec!["x/a"], true),
            "",
        ),
        (
            "valid adding finalizers for objects not being deleted",
            mk(vec!["x/a"], false),
            mk(vec!["x/a", "y/b"], false),
            "",
        ),
    ];

    for (name, old, new, expected_substr) in cases {
        let errs = validate_object_meta_update(&new, &old, &Path::new("field"));
        let agg = aggregate(&errs);
        if errs.is_empty() {
            assert!(
                expected_substr.is_empty(),
                "case {name}: expected error to contain {expected_substr:?}",
            );
        } else {
            assert!(
                agg.contains(expected_substr),
                "case {name}: expected error to contain {expected_substr:?}, got {agg:?}",
            );
        }
    }
}

// ===========================================================================
// TestValidateFinalizersPreventConflictingFinalizers
// Upstream: lines 363-383
// `orphan` and `foregroundDeletion` finalizers cannot coexist on the same
// object — validateObjectMetaAccessorWithOptsCommon must reject the combo
// with "cannot be both set".
// ===========================================================================
#[test]
fn test_validate_finalizers_prevent_conflicting_finalizers() {
    // Upstream uses metav1.FinalizerOrphanDependents + metav1.FinalizerDeleteDependents.
    let meta = ObjectMeta {
        name: "test".into(),
        resource_version: Some("1".into()),
        finalizers: Some(vec!["orphan".into(), "foregroundDeletion".into()]),
        ..ObjectMeta::default()
    };
    let errs = validate_object_meta_accessor_with_opts_common(&meta, false, &Path::new("field"));
    let agg = aggregate(&errs);
    assert!(
        agg.contains("cannot be both set"),
        "expected error containing 'cannot be both set', got: {agg}"
    );
}

// ===========================================================================
// TestValidateObjectMetaUpdatePreventsDeletionFieldMutation
// Upstream: lines 385-466
// DeletionTimestamp and DeletionGracePeriodSeconds are immutable once set.
// Eight test cases covering set/clear/change for each field.
// ===========================================================================
#[test]
fn test_validate_object_meta_update_prevents_deletion_field_mutation() {
    let now = chrono::DateTime::from_timestamp(1000, 0).unwrap();
    let later = chrono::DateTime::from_timestamp(2000, 0).unwrap();
    let grace_short: i64 = 30;
    let grace_long: i64 = 40;

    let base = ObjectMeta {
        name: "test".into(),
        resource_version: Some("1".into()),
        ..ObjectMeta::default()
    };

    let mut with_now_short = base.clone();
    with_now_short.deletion_timestamp = Some(now);
    with_now_short.deletion_grace_period_seconds = Some(grace_short);

    let mut with_now = base.clone();
    with_now.deletion_timestamp = Some(now);

    let mut with_later = base.clone();
    with_later.deletion_timestamp = Some(later);

    let mut with_short = base.clone();
    with_short.deletion_grace_period_seconds = Some(grace_short);

    let mut with_long = base.clone();
    with_long.deletion_grace_period_seconds = Some(grace_long);

    // (case_name, old, new, expected_errs)
    let cases: Vec<(&'static str, ObjectMeta, ObjectMeta, Vec<&'static str>)> = vec![
        ("valid without deletion fields", base.clone(), base.clone(), vec![]),
        (
            "valid with deletion fields",
            with_now_short.clone(),
            with_now_short.clone(),
            vec![],
        ),
        (
            "invalid set deletionTimestamp",
            base.clone(),
            with_now.clone(),
            vec!["field.deletionTimestamp: Invalid value: \"1970-01-01T00:16:40Z\": field is immutable"],
        ),
        (
            "invalid clear deletionTimestamp",
            with_now.clone(),
            base.clone(),
            vec!["field.deletionTimestamp: Invalid value: null: field is immutable"],
        ),
        (
            "invalid change deletionTimestamp",
            with_now,
            with_later,
            vec!["field.deletionTimestamp: Invalid value: \"1970-01-01T00:33:20Z\": field is immutable"],
        ),
        (
            "invalid set deletionGracePeriodSeconds",
            base.clone(),
            with_short.clone(),
            vec!["field.deletionGracePeriodSeconds: Invalid value: 30: field is immutable"],
        ),
        (
            "invalid clear deletionGracePeriodSeconds",
            with_short.clone(),
            base,
            vec!["field.deletionGracePeriodSeconds: Invalid value: null: field is immutable"],
        ),
        (
            "invalid change deletionGracePeriodSeconds",
            with_short,
            with_long,
            vec!["field.deletionGracePeriodSeconds: Invalid value: 40: field is immutable"],
        ),
    ];

    for (name, old, new, expected) in cases {
        let errs = validate_object_meta_update(&new, &old, &Path::new("field"));
        assert_eq!(
            errs.len(),
            expected.len(),
            "case {name}: expected {} errors, got {}: {errs:?}",
            expected.len(),
            errs.len()
        );
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(
                errs[i].to_string(),
                *want,
                "case {name}: error #{i} mismatch"
            );
        }
    }
}

// ===========================================================================
// TestObjectMetaGenerationUpdate
// Upstream: lines 468-509
// Generation must never decrement. Incrementing or leaving it unchanged is
// allowed.
// ===========================================================================
#[test]
fn test_object_meta_generation_update() {
    let mk = |gen_val: i64| ObjectMeta {
        name: "test".into(),
        resource_version: Some("1".into()),
        generation: Some(gen_val),
        ..ObjectMeta::default()
    };

    let cases: Vec<(&'static str, ObjectMeta, ObjectMeta, Vec<&'static str>)> = vec![
        (
            "invalid generation change - decremented",
            mk(5),
            mk(4),
            vec!["field.generation: Invalid value: 4: must not be decremented"],
        ),
        (
            "valid generation change - incremented by one",
            mk(1),
            mk(2),
            vec![],
        ),
        ("valid generation field - not updated", mk(5), mk(5), vec![]),
    ];

    for (name, old, new, expected) in cases {
        let errs = validate_object_meta_update(&new, &old, &Path::new("field"));
        assert_eq!(
            errs.len(),
            expected.len(),
            "case {name}: expected {} errors, got {}: {errs:?}",
            expected.len(),
            errs.len()
        );
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(
                errs[i].to_string(),
                *want,
                "case {name}: error #{i} mismatch"
            );
        }
    }
}

// ===========================================================================
// TestValidateObjectMetaTrimsTrailingDash
// Upstream: lines 511-521
// A trailing dash on generateName is legal because the server appends a
// random suffix before persisting — the dash never reaches the name validator.
// ===========================================================================
#[test]
fn test_validate_object_meta_trims_trailing_dash() {
    let meta = ObjectMeta {
        name: "test".into(),
        generate_name: Some("foo-".into()),
        ..ObjectMeta::default()
    };
    let errs = validate_object_meta(&meta, false, name_is_dns_subdomain, &Path::new("field"));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

// ===========================================================================
// TestValidateAnnotations
// Upstream: lines 523-583
// Annotation keys follow the same rules as label keys (DNS-1123 subdomain
// prefix + name part). Annotation values are unrestricted in content but
// the total annotation byte-size across all key/value pairs is bounded by
// TotalAnnotationSizeLimitB.
// ===========================================================================
#[test]
fn test_validate_annotations() {
    // Upstream constant value — declared here so the fixture compiles even
    // though rusternetes-common does not yet expose the symbol.
    const TOTAL_ANNOTATION_SIZE_LIMIT_B: usize = 256 * 1024;

    let mk = |pairs: &[(&str, String)]| -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    };
    let mk_owned =
        |pairs: Vec<(String, String)>| -> HashMap<String, String> { pairs.into_iter().collect() };

    let success_cases: Vec<HashMap<String, String>> = vec![
        mk(&[("simple", "bar".into())]),
        mk(&[("now-with-dashes", "bar".into())]),
        mk(&[("1-starts-with-num", "bar".into())]),
        mk(&[("1234", "bar".into())]),
        mk(&[("simple/simple", "bar".into())]),
        mk(&[("now-with-dashes/simple", "bar".into())]),
        mk(&[("now-with-dashes/now-with-dashes", "bar".into())]),
        mk(&[("now.with.dots/simple", "bar".into())]),
        mk(&[("now-with.dashes-and.dots/simple", "bar".into())]),
        mk(&[("1-num.2-num/3-num", "bar".into())]),
        mk(&[("1234/5678", "bar".into())]),
        mk(&[("1.2.3.4/5678", "bar".into())]),
        mk(&[("UpperCase123", "bar".into())]),
        mk(&[("a", "b".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B - 1))]),
        mk(&[
            ("a", "b".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B / 2 - 1)),
            ("c", "d".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B / 2 - 1)),
        ]),
    ];

    let name_part_err = "name part must consist of";
    let name_err = "a valid label key must consist of";
    let max_length_err = "must be no more than";

    let name_error_cases: Vec<(HashMap<String, String>, &'static str)> = vec![
        (mk(&[("nospecialchars^=@", "bar".into())]), name_part_err),
        (mk(&[("cantendwithadash-", "bar".into())]), name_part_err),
        (mk(&[("only/one/slash", "bar".into())]), name_err),
        // Owned-key variant — upstream uses strings.Repeat("a", 254) inline, but
        // Rust can't take `&str` from a temporary `String`, so the key is owned.
        (
            mk_owned(vec![("a".repeat(254), "bar".into())]),
            max_length_err,
        ),
    ];

    let size_error_cases: Vec<HashMap<String, String>> = vec![
        mk(&[("a", "b".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B))]),
        mk(&[
            ("a", "b".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B / 2)),
            ("c", "d".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B / 2)),
        ]),
    ];

    for (i, annotations) in success_cases.iter().enumerate() {
        let errs = validate_annotations(annotations, &Path::new("field"));
        assert!(errs.is_empty(), "case[{i}] expected success, got {errs:?}");
    }
    for (i, (annotations, expect)) in name_error_cases.iter().enumerate() {
        let errs = validate_annotations(annotations, &Path::new("field"));
        assert_eq!(errs.len(), 1, "case[{i}]: expected failure, got {errs:?}");
        assert!(
            errs[0].detail.contains(expect),
            "case[{i}]: error detail does not include {expect:?}: {:?}",
            errs[0].detail
        );
    }
    for (i, annotations) in size_error_cases.iter().enumerate() {
        let errs = validate_annotations(annotations, &Path::new("field"));
        assert_eq!(errs.len(), 1, "case[{i}] expected failure, got {errs:?}");
    }
}

// ===========================================================================
// Bonus pin: ObjectMeta::ensure_name() — a small piece of the upstream
// generateName contract that DOES exist in rusternetes-common today. This
// test is intentionally NOT #[ignore]d so the file produces at least one
// green pin while the rest stays red.
// ===========================================================================
#[test]
fn test_ensure_name_resolves_generate_name() {
    let mut meta = ObjectMeta {
        name: String::new(),
        generate_name: Some("foo-".into()),
        ..ObjectMeta::default()
    };
    meta.ensure_name();
    assert!(
        meta.name.starts_with("foo-"),
        "expected ensure_name to keep the generateName prefix, got {:?}",
        meta.name
    );
    assert!(
        meta.name.len() > "foo-".len(),
        "expected ensure_name to append a suffix, got {:?}",
        meta.name
    );
}

#[test]
fn test_ensure_name_does_not_fabricate_without_generate_name() {
    // With neither name nor generateName, ensure_name must leave the name
    // empty (so name validation can reject the object) rather than inventing
    // an `auto-<id>` name — which is non-conformant (#1063).
    let mut meta = ObjectMeta {
        name: String::new(),
        generate_name: None,
        ..ObjectMeta::default()
    };
    meta.ensure_name();
    assert!(
        meta.name.is_empty(),
        "ensure_name must not fabricate a name without generateName, got {:?}",
        meta.name
    );

    // An empty generateName is equivalent to none.
    let mut meta = ObjectMeta {
        name: String::new(),
        generate_name: Some(String::new()),
        ..ObjectMeta::default()
    };
    meta.ensure_name();
    assert!(meta.name.is_empty(), "got {:?}", meta.name);
}

// ===========================================================================
// Bonus pin: ObjectMeta::has_finalizers() — a tiny green pin that exercises
// the finalizer accessor used as a precondition by several of the upstream
// finalizer tests above.
// ===========================================================================
#[test]
fn test_has_finalizers_accessor() {
    let mut meta = ObjectMeta::new("test");
    assert!(!meta.has_finalizers());
    meta.add_finalizer("kubernetes.io/pv-protection".into());
    assert!(meta.has_finalizers());
    meta.remove_finalizer("kubernetes.io/pv-protection");
    assert!(!meta.has_finalizers());
}
