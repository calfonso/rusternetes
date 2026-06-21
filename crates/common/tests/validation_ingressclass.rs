//! Tests for IngressClass validation (port of upstream `ValidateIngressClass`).

use rusternetes_common::resources::ingressclass::{
    IngressClass, IngressClassParametersReference, IngressClassSpec,
};
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::ingressclass::validate_ingress_class;

fn ic(controller: &str, parameters: Option<IngressClassParametersReference>) -> IngressClass {
    let mut x = IngressClass {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec: Some(IngressClassSpec {
            controller: controller.to_string(),
            parameters,
        }),
    };
    x.metadata.name = "nginx".to_string();
    x
}

fn params(scope: Option<&str>, namespace: Option<&str>) -> IngressClassParametersReference {
    IngressClassParametersReference {
        api_group: Some("k8s.example.com".to_string()),
        kind: "IngressParameters".to_string(),
        name: "external-config".to_string(),
        namespace: namespace.map(|s| s.to_string()),
        scope: scope.map(|s| s.to_string()),
    }
}

fn has(errs: &[Error], field_substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field_substr))
}

#[test]
fn valid_ingressclass_passes() {
    assert!(validate_ingress_class(&ic("acme.io/ingress-controller", None)).is_empty());
}

#[test]
fn empty_controller_rejected() {
    let errs = validate_ingress_class(&ic("", None));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.controller" && e.error_type == ErrorType::Required));
}

#[test]
fn non_domain_prefixed_controller_rejected() {
    // No "/" → not a domain-prefixed path.
    assert!(has(
        &validate_ingress_class(&ic("nginx", None)),
        "spec.controller"
    ));
}

#[test]
fn controller_bad_host_rejected() {
    assert!(has(
        &validate_ingress_class(&ic("Bad_Host/ingress", None)),
        "spec.controller"
    ));
}

#[test]
fn valid_cluster_scope_parameters_passes() {
    let p = params(Some("Cluster"), None);
    assert!(validate_ingress_class(&ic("acme.io/ic", Some(p))).is_empty());
}

#[test]
fn valid_namespace_scope_parameters_passes() {
    let p = params(Some("Namespace"), Some("kube-system"));
    let errs = validate_ingress_class(&ic("acme.io/ic", Some(p)));
    assert!(errs.is_empty(), "{:?}", errs);
}

#[test]
fn parameters_missing_scope_rejected() {
    let p = params(None, None);
    let errs = validate_ingress_class(&ic("acme.io/ic", Some(p)));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.parameters.scope" && e.error_type == ErrorType::Required));
}

#[test]
fn parameters_bad_scope_rejected() {
    let p = params(Some("Galaxy"), None);
    let errs = validate_ingress_class(&ic("acme.io/ic", Some(p)));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.parameters.scope" && e.error_type == ErrorType::NotSupported));
}

#[test]
fn namespace_scope_without_namespace_rejected() {
    let p = params(Some("Namespace"), None);
    assert!(has(
        &validate_ingress_class(&ic("acme.io/ic", Some(p))),
        "spec.parameters.namespace"
    ));
}

#[test]
fn cluster_scope_with_namespace_rejected() {
    let p = params(Some("Cluster"), Some("kube-system"));
    let errs = validate_ingress_class(&ic("acme.io/ic", Some(p)));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.parameters.namespace" && e.error_type == ErrorType::Forbidden));
}

#[test]
fn parameters_missing_kind_rejected() {
    let mut p = params(Some("Cluster"), None);
    p.kind = String::new();
    assert!(has(
        &validate_ingress_class(&ic("acme.io/ic", Some(p))),
        "spec.parameters.kind"
    ));
}
