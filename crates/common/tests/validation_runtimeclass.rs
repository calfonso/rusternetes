//! Tests for RuntimeClass validation (port of upstream `ValidateRuntimeClass`).

use rusternetes_common::resources::runtimeclass::{Overhead, Scheduling};
use rusternetes_common::resources::RuntimeClass;
use rusternetes_common::validation::runtimeclass::validate_runtime_class;
use std::collections::HashMap;

fn rc(handler: &str) -> RuntimeClass {
    let mut r = RuntimeClass {
        type_meta: Default::default(),
        metadata: Default::default(),
        handler: handler.to_string(),
        overhead: None,
        scheduling: None,
    };
    r.metadata.name = "myclass".to_string();
    r
}

fn has(errs: &[rusternetes_common::validation::field::Error], field_substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field_substr))
}

#[test]
fn valid_runtime_class_passes() {
    assert!(validate_runtime_class(&rc("runc")).is_empty());
}

#[test]
fn empty_handler_rejected() {
    assert!(has(&validate_runtime_class(&rc("")), "handler"));
}

#[test]
fn uppercase_handler_rejected() {
    // DNS-1123 labels are lowercase only.
    assert!(has(&validate_runtime_class(&rc("RunC")), "handler"));
}

#[test]
fn valid_overhead_passes() {
    let mut r = rc("runc");
    let mut pf = HashMap::new();
    pf.insert("cpu".to_string(), "250m".to_string());
    pf.insert("memory".to_string(), "64Mi".to_string());
    r.overhead = Some(Overhead {
        pod_fixed: Some(pf),
    });
    assert!(
        validate_runtime_class(&r).is_empty(),
        "{:?}",
        validate_runtime_class(&r)
    );
}

#[test]
fn negative_overhead_rejected() {
    let mut r = rc("runc");
    let mut pf = HashMap::new();
    pf.insert("cpu".to_string(), "-1".to_string());
    r.overhead = Some(Overhead {
        pod_fixed: Some(pf),
    });
    assert!(has(&validate_runtime_class(&r), "overhead.podFixed"));
}

#[test]
fn unparseable_overhead_rejected() {
    let mut r = rc("runc");
    let mut pf = HashMap::new();
    pf.insert("memory".to_string(), "not-a-qty".to_string());
    r.overhead = Some(Overhead {
        pod_fixed: Some(pf),
    });
    assert!(has(&validate_runtime_class(&r), "overhead.podFixed"));
}

#[test]
fn valid_scheduling_passes() {
    let mut r = rc("runc");
    let mut ns = HashMap::new();
    ns.insert("kubernetes.io/os".to_string(), "linux".to_string());
    r.scheduling = Some(Scheduling {
        node_selector: Some(ns),
        tolerations: None,
    });
    assert!(
        validate_runtime_class(&r).is_empty(),
        "{:?}",
        validate_runtime_class(&r)
    );
}

#[test]
fn invalid_node_selector_label_rejected() {
    let mut r = rc("runc");
    let mut ns = HashMap::new();
    ns.insert("bad label!".to_string(), "x".to_string());
    r.scheduling = Some(Scheduling {
        node_selector: Some(ns),
        tolerations: None,
    });
    assert!(has(&validate_runtime_class(&r), "scheduling.nodeSelector"));
}
