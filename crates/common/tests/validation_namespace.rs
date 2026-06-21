//! Tests for Namespace validation (port of upstream ValidateNamespace).

use rusternetes_common::resources::namespace::NamespaceSpec;
use rusternetes_common::resources::Namespace;
use rusternetes_common::validation::namespace::validate_namespace;

fn ns(finalizers: Option<Vec<&str>>) -> Namespace {
    let mut n = Namespace {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec: finalizers.map(|f| NamespaceSpec {
            finalizers: Some(f.into_iter().map(|s| s.to_string()).collect()),
        }),
        status: None,
    };
    n.metadata.name = "test-ns".to_string();
    n
}

fn has(errs: &[rusternetes_common::validation::field::Error], substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(substr))
}

#[test]
fn no_spec_ok() {
    assert!(validate_namespace(&ns(None)).is_empty());
}

#[test]
fn standard_finalizer_ok() {
    assert!(validate_namespace(&ns(Some(vec!["kubernetes"]))).is_empty());
    assert!(validate_namespace(&ns(Some(vec!["orphan"]))).is_empty());
    assert!(validate_namespace(&ns(Some(vec!["foregroundDeletion"]))).is_empty());
}

#[test]
fn qualified_finalizer_ok() {
    assert!(validate_namespace(&ns(Some(vec!["example.com/my-finalizer"]))).is_empty());
}

#[test]
fn unqualified_nonstandard_finalizer_rejected() {
    let errs = validate_namespace(&ns(Some(vec!["myfinalizer"])));
    assert!(errs
        .iter()
        .any(|e| e.detail.contains("neither a standard finalizer")));
}

#[test]
fn invalid_qualified_name_rejected() {
    // a "/"-containing value with an invalid prefix/name fails IsQualifiedName
    assert!(has(
        &validate_namespace(&ns(Some(vec!["bad prefix/name"]))),
        "spec.finalizers"
    ));
}
