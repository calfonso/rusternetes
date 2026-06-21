//! Tests for PodTemplate validation (port of upstream ValidatePodTemplate).

use rusternetes_common::resources::pod::{Container, PodSpec};
use rusternetes_common::resources::workloads::{PodTemplate, PodTemplateSpec};
use rusternetes_common::types::ObjectMeta;
use rusternetes_common::validation::podtemplate::validate_pod_template;

fn container(name: &str) -> Container {
    Container {
        name: name.to_string(),
        image: "nginx".to_string(),
        ..Default::default()
    }
}

fn pt(containers: Vec<Container>) -> PodTemplate {
    let mut p = PodTemplate {
        type_meta: Default::default(),
        metadata: Default::default(),
        template: PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers,
                ..Default::default()
            },
        },
    };
    p.metadata.name = "pt-1".to_string();
    p
}

fn has(errs: &[rusternetes_common::validation::field::Error], field_substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field_substr))
}

#[test]
fn valid_pod_template_passes() {
    let errs = validate_pod_template(&pt(vec![container("web")]));
    assert!(errs.is_empty(), "{:?}", errs);
}

#[test]
fn empty_containers_rejected() {
    assert!(has(
        &validate_pod_template(&pt(vec![])),
        "template.spec.containers"
    ));
}

#[test]
fn bad_container_name_rejected() {
    assert!(has(
        &validate_pod_template(&pt(vec![container("Bad_Name")])),
        "template.spec.containers[0].name"
    ));
}

#[test]
fn empty_container_image_rejected() {
    let mut c = container("web");
    c.image = String::new();
    assert!(has(
        &validate_pod_template(&pt(vec![c])),
        "template.spec.containers[0].image"
    ));
}

#[test]
fn bad_template_label_rejected() {
    let mut p = pt(vec![container("web")]);
    let mut labels = std::collections::HashMap::new();
    labels.insert("bad label!".to_string(), "x".to_string());
    p.template.metadata = Some(ObjectMeta {
        labels: Some(labels),
        ..Default::default()
    });
    assert!(has(&validate_pod_template(&p), "template.labels"));
}

#[test]
fn ephemeral_containers_forbidden() {
    let mut p = pt(vec![container("web")]);
    p.template.spec.ephemeral_containers = Some(vec![
        rusternetes_common::resources::pod::EphemeralContainer {
            name: "debug".to_string(),
            ..Default::default()
        },
    ]);
    let errs = validate_pod_template(&p);
    assert!(
        errs.iter().any(|e| e.field.contains("ephemeralContainers")),
        "{:?}",
        errs
    );
}
