//! Tests for FlowSchema validation (upstream APF ValidateFlowSchemaSpec).

use rusternetes_common::resources::flowcontrol::{
    FlowSchema, FlowSchemaSpec, FlowSchemaSubject, NonResourcePolicyRule, PolicyRulesWithSubjects,
    PriorityLevelConfigurationReference, ResourcePolicyRule, ServiceAccountSubject, SubjectKind,
    UserSubject,
};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::flowschema::validate_flow_schema;

fn user_subject(name: &str) -> FlowSchemaSubject {
    FlowSchemaSubject {
        kind: SubjectKind::User,
        user: Some(UserSubject {
            name: name.to_string(),
        }),
        group: None,
        service_account: None,
    }
}

fn resource_rule() -> ResourcePolicyRule {
    ResourcePolicyRule {
        verbs: vec!["get".to_string()],
        api_groups: vec!["".to_string()],
        resources: vec!["pods".to_string()],
        cluster_scope: None,
        namespaces: Some(vec!["default".to_string()]),
    }
}

fn fs(name: &str, precedence: i32, plc: &str, rules: Vec<PolicyRulesWithSubjects>) -> FlowSchema {
    let mut f = FlowSchema {
        api_version: "flowcontrol.apiserver.k8s.io/v1".to_string(),
        kind: "FlowSchema".to_string(),
        metadata: Default::default(),
        spec: FlowSchemaSpec {
            priority_level_configuration: PriorityLevelConfigurationReference {
                name: plc.to_string(),
            },
            matching_precedence: precedence,
            distinguisher_method: None,
            rules: Some(rules),
        },
        status: None,
    };
    f.metadata.name = name.to_string();
    f
}

fn one_rule() -> PolicyRulesWithSubjects {
    PolicyRulesWithSubjects {
        subjects: vec![user_subject("alice")],
        resource_rules: Some(vec![resource_rule()]),
        non_resource_rules: None,
    }
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field))
}

#[test]
fn valid_flowschema_passes() {
    let errs = validate_flow_schema(&fs("fs1", 1000, "workload", vec![one_rule()]));
    assert!(errs.is_empty(), "{:?}", errs);
}

#[test]
fn nonpositive_precedence_rejected() {
    assert!(has(
        &validate_flow_schema(&fs("fs1", 0, "workload", vec![one_rule()])),
        "spec.matchingPrecedence"
    ));
}

#[test]
fn precedence_above_max_rejected() {
    assert!(has(
        &validate_flow_schema(&fs("fs1", 10001, "workload", vec![one_rule()])),
        "spec.matchingPrecedence"
    ));
}

#[test]
fn precedence_one_only_for_exempt() {
    assert!(has(
        &validate_flow_schema(&fs("notexempt", 1, "workload", vec![one_rule()])),
        "spec.matchingPrecedence"
    ));
    // name 'exempt' with precedence 1 is allowed
    assert!(!has(
        &validate_flow_schema(&fs("exempt", 1, "exempt", vec![one_rule()])),
        "spec.matchingPrecedence"
    ));
}

#[test]
fn missing_plc_name_rejected() {
    let errs = validate_flow_schema(&fs("fs1", 1000, "", vec![one_rule()]));
    assert!(errs
        .iter()
        .any(|e| e.field.contains("priorityLevelConfiguration.name")
            && e.error_type == ErrorType::Required));
}

#[test]
fn rule_without_subjects_rejected() {
    let mut r = one_rule();
    r.subjects = vec![];
    assert!(has(
        &validate_flow_schema(&fs("fs1", 1000, "workload", vec![r])),
        "subjects"
    ));
}

#[test]
fn rule_without_any_rules_rejected() {
    let r = PolicyRulesWithSubjects {
        subjects: vec![user_subject("a")],
        resource_rules: None,
        non_resource_rules: None,
    };
    let errs = validate_flow_schema(&fs("fs1", 1000, "workload", vec![r]));
    assert!(errs.iter().any(|e| e
        .detail
        .contains("at least one of resourceRules and nonResourceRules")));
}

#[test]
fn sa_subject_without_namespace_rejected() {
    let subj = FlowSchemaSubject {
        kind: SubjectKind::ServiceAccount,
        user: None,
        group: None,
        service_account: Some(ServiceAccountSubject {
            namespace: String::new(),
            name: "sa".to_string(),
        }),
    };
    let r = PolicyRulesWithSubjects {
        subjects: vec![subj],
        resource_rules: Some(vec![resource_rule()]),
        non_resource_rules: None,
    };
    assert!(has(
        &validate_flow_schema(&fs("fs1", 1000, "workload", vec![r])),
        "serviceAccount.namespace"
    ));
}

#[test]
fn bad_verb_rejected() {
    let mut rr = resource_rule();
    rr.verbs = vec!["frobnicate".to_string()];
    let r = PolicyRulesWithSubjects {
        subjects: vec![user_subject("a")],
        resource_rules: Some(vec![rr]),
        non_resource_rules: None,
    };
    let errs = validate_flow_schema(&fs("fs1", 1000, "workload", vec![r]));
    assert!(errs
        .iter()
        .any(|e| e.field.contains("verbs") && e.error_type == ErrorType::NotSupported));
}

#[test]
fn nonresource_rule_without_urls_rejected() {
    let nr = NonResourcePolicyRule {
        verbs: vec!["get".to_string()],
        non_resource_urls: vec![],
    };
    let r = PolicyRulesWithSubjects {
        subjects: vec![user_subject("a")],
        resource_rules: None,
        non_resource_rules: Some(vec![nr]),
    };
    assert!(has(
        &validate_flow_schema(&fs("fs1", 1000, "workload", vec![r])),
        "nonResourceURLs"
    ));
}

#[test]
fn user_subject_with_group_forbidden() {
    let subj = FlowSchemaSubject {
        kind: SubjectKind::User,
        user: Some(UserSubject {
            name: "a".to_string(),
        }),
        group: Some(rusternetes_common::resources::flowcontrol::GroupSubject {
            name: "g".to_string(),
        }),
        service_account: None,
    };
    let r = PolicyRulesWithSubjects {
        subjects: vec![subj],
        resource_rules: Some(vec![resource_rule()]),
        non_resource_rules: None,
    };
    let errs = validate_flow_schema(&fs("fs1", 1000, "workload", vec![r]));
    assert!(errs
        .iter()
        .any(|e| e.field.contains("group") && e.error_type == ErrorType::Forbidden));
}
