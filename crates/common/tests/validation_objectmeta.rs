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
use std::collections::HashMap;

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
#[ignore = "upstream-mirror: TODO when rusternetes_common exposes ValidateObjectMeta(meta, requires_namespace, name_fn, field_path)"]
fn test_validate_object_meta_custom_name() {
    // Table of (input, expected_n_errs, expected_err_substr).
    let cases: Vec<(ObjectMeta, usize, &'static str)> = vec![
        (meta_named_gen("test", ""), 0, ""),
        (meta_named_gen("test", "test"), 0, ""),
        (meta_named_gen("invalid", ""), 1, "wrong value"),
        (meta_named_gen("invalid", "test"), 1, "wrong value"),
        (meta_named_gen("invalid", "invalid"), 2, "wrong value"),
    ];

    for (_meta, _n_errs, _substr) in cases {
        // TODO: call rusternetes_common::validation::validate_object_meta(
        //   &meta, /*requires_namespace=*/ false, &|s, _prefix| if s == "test" { vec![] } else { vec!["wrong value".into()] },
        //   FieldPath::new("field"),
        // ) and assert error count + substring.
        panic!(
            "validate_object_meta is not implemented in rusternetes-common yet \
             (upstream: ValidateObjectMeta with custom NameGenerator)"
        );
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
#[ignore = "upstream-mirror: TODO when ValidateObjectMetaWithOpts (errlist-returning name fn) lands"]
fn test_validate_object_meta_with_opts_name() {
    let cases: Vec<(ObjectMeta, &'static str)> = vec![
        (meta_named_gen("test", ""), ""),
        (meta_named_gen("test", "test"), ""),
        (meta_named_gen("invalid", ""), "wrong value"),
        (meta_named_gen("invalid", "test"), "wrong value"),
        (meta_named_gen("invalid", "invalid"), "wrong value"),
    ];

    for (_meta, _expected_substr) in cases {
        // TODO: drive ValidateObjectMetaWithOpts. For non-empty expected_substr
        // expect exactly 1 error whose Display contains the substring.
        panic!(
            "validate_object_meta_with_opts is not implemented in \
             rusternetes-common yet"
        );
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
#[ignore = "upstream-mirror: TODO when validate_object_meta_accessor_with_opts_common (namespace DNS-label rules) exists"]
fn test_validate_object_meta_namespaces() {
    // Case 1: a dot in the namespace is forbidden for DNS-label namespaces.
    let _bad_dot = meta_with_ns("test", "foo.bar");

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
    let _bad_long = meta_with_ns("test", &long_ns);

    // TODO: call validate_object_meta_accessor_with_opts_common(meta, /*requires_namespace=*/ true, ...).
    // Assert error counts (1 and 2 respectively) and substring "Invalid value".
    panic!(
        "validate_object_meta_accessor_with_opts_common is not implemented in \
         rusternetes-common yet"
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
#[ignore = "upstream-mirror: TODO when owner-reference validation (Event blacklist + single-controller rule) exists"]
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

    for (_desc, _meta, _expect_err, _err_substr) in cases {
        // TODO: drive owner-reference validation.
        panic!("owner-reference validation is not implemented in rusternetes-common yet");
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
#[ignore = "upstream-mirror: TODO when validate_object_meta_update (immutability checks) exists"]
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

    for (_old, _new) in cases {
        // TODO: call validate_object_meta_update(&new, &old, FieldPath::new("field"))
        // and assert exactly 1 error.
        panic!("validate_object_meta_update is not implemented in rusternetes-common yet");
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
#[ignore = "upstream-mirror: TODO when validate_object_meta_update enforces finalizer-during-deletion rules"]
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

    for (_name, _old, _new, _expected_substr) in cases {
        // TODO: call validate_object_meta_update(&new, &old, FieldPath::new("field"))
        // and check whether expected_substr appears in the aggregated error.
        panic!(
            "validate_object_meta_update finalizer-during-deletion check is \
             not implemented in rusternetes-common yet"
        );
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
#[ignore = "upstream-mirror: TODO when conflicting-finalizer detection lands"]
fn test_validate_finalizers_prevent_conflicting_finalizers() {
    // Upstream uses metav1.FinalizerOrphanDependents + metav1.FinalizerDeleteDependents.
    let _meta = ObjectMeta {
        name: "test".into(),
        resource_version: Some("1".into()),
        finalizers: Some(vec!["orphan".into(), "foregroundDeletion".into()]),
        ..ObjectMeta::default()
    };

    // TODO: call validate_object_meta_accessor_with_opts_common(&meta, false, ...)
    // and assert error contains "cannot be both set".
    panic!(
        "conflicting-finalizer detection (orphan + foregroundDeletion) is not \
         implemented in rusternetes-common yet"
    );
}

// ===========================================================================
// TestValidateObjectMetaUpdatePreventsDeletionFieldMutation
// Upstream: lines 385-466
// DeletionTimestamp and DeletionGracePeriodSeconds are immutable once set.
// Eight test cases covering set/clear/change for each field.
// ===========================================================================
#[test]
#[ignore = "upstream-mirror: TODO when validate_object_meta_update treats deletion fields as immutable"]
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

    for (_name, _old, _new, _expected) in cases {
        // TODO: drive validate_object_meta_update and compare errors element-wise.
        panic!(
            "deletion-field immutability checks are not implemented in \
             rusternetes-common yet"
        );
    }
}

// ===========================================================================
// TestObjectMetaGenerationUpdate
// Upstream: lines 468-509
// Generation must never decrement. Incrementing or leaving it unchanged is
// allowed.
// ===========================================================================
#[test]
#[ignore = "upstream-mirror: TODO when generation-monotonicity check lands in validate_object_meta_update"]
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

    for (_name, _old, _new, _expected) in cases {
        // TODO: drive validate_object_meta_update; assert error list equality.
        panic!(
            "generation monotonicity check is not implemented in \
             rusternetes-common yet"
        );
    }
}

// ===========================================================================
// TestValidateObjectMetaTrimsTrailingDash
// Upstream: lines 511-521
// A trailing dash on generateName is legal because the server appends a
// random suffix before persisting — the dash never reaches the name validator.
// ===========================================================================
#[test]
#[ignore = "upstream-mirror: TODO when ValidateObjectMeta + NameIsDNSSubdomain accept trailing-dash generateName"]
fn test_validate_object_meta_trims_trailing_dash() {
    let _meta = ObjectMeta {
        name: "test".into(),
        generate_name: Some("foo-".into()),
        ..ObjectMeta::default()
    };

    // TODO: call validate_object_meta(&meta, /*requires_namespace=*/ false,
    //   NameIsDNSSubdomain, FieldPath::new("field"))
    // and assert zero errors.
    panic!(
        "ValidateObjectMeta + NameIsDNSSubdomain (with trailing-dash trim) is \
         not implemented in rusternetes-common yet"
    );
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
#[ignore = "upstream-mirror: TODO when validate_annotations + TotalAnnotationSizeLimitB exist in rusternetes-common"]
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

    let _success_cases: Vec<HashMap<String, String>> = vec![
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

    let _name_error_cases: Vec<(HashMap<String, String>, &'static str)> = vec![
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

    let _size_error_cases: Vec<HashMap<String, String>> = vec![
        mk(&[("a", "b".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B))]),
        mk(&[
            ("a", "b".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B / 2)),
            ("c", "d".repeat(TOTAL_ANNOTATION_SIZE_LIMIT_B / 2)),
        ]),
    ];

    // TODO: call validate_annotations(&annotations, FieldPath::new("field"))
    // and assert zero errors on success cases, exactly 1 error containing the
    // matching substring on the name error cases, and exactly 1 error on the
    // size error cases.
    panic!(
        "validate_annotations + TotalAnnotationSizeLimitB are not implemented \
         in rusternetes-common yet"
    );
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
