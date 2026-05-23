//! Integration tests for `ValidatingAdmissionPolicy.spec.matchConditions[*]`
//! CEL expression evaluation.
//!
//! These tests exercise [`rusternetes_api_server::cel::MatchConditionEvaluator`]
//! directly, the same surface that
//! `crate::admission_webhook::run_validating_admission_policies_ext` calls into
//! before deciding whether a policy applies to an admission request.

use rusternetes_api_server::cel::{MatchConditionEvaluator, MatchOutcome};
use rusternetes_common::admission::{AdmissionRequest, Operation, UserInfo};
use rusternetes_common::resources::MatchCondition;
use serde_json::json;

/// Build an `AdmissionRequest` for a ConfigMap CREATE in `namespace`.
fn create_configmap_request(namespace: &str, name: &str) -> AdmissionRequest {
    AdmissionRequest {
        operation: Operation::Create,
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some(namespace.to_string()),
        name: name.to_string(),
        object: json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
            },
            "data": {
                "key": "value",
            },
        }),
        old_object: None,
        user_info: UserInfo {
            username: "system:admin".to_string(),
            uid: "uid-admin".to_string(),
            groups: vec!["system:masters".to_string()],
        },
    }
}

#[test]
fn matchcondition_matches_when_namespace_is_kube_system() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("kube-system", "leader-election");

    let conditions = vec![MatchCondition {
        name: "kube-system-only".to_string(),
        expression: "object.metadata.namespace == 'kube-system'".to_string(),
    }];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, None),
        MatchOutcome::Matched,
        "expected the matchCondition to match kube-system"
    );
}

#[test]
fn matchcondition_skips_policy_when_namespace_does_not_match() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("default", "app-config");

    let conditions = vec![MatchCondition {
        name: "kube-system-only".to_string(),
        expression: "object.metadata.namespace == 'kube-system'".to_string(),
    }];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, None),
        MatchOutcome::NotMatched,
        "expected the matchCondition to skip non-kube-system namespaces"
    );
}

#[test]
fn matchcondition_short_circuits_on_first_false() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("default", "app-config");

    let conditions = vec![
        MatchCondition {
            name: "first-true".to_string(),
            expression: "object.kind == 'ConfigMap'".to_string(),
        },
        MatchCondition {
            name: "second-false".to_string(),
            expression: "object.metadata.namespace == 'kube-system'".to_string(),
        },
        // This third condition would also be false, but we should never reach it
        // because the second already short-circuits. We use a bogus expression
        // to prove the short-circuit: if it were evaluated, the outcome would
        // be Error rather than NotMatched.
        MatchCondition {
            name: "never-evaluated".to_string(),
            expression: "this is not valid CEL syntax !!!".to_string(),
        },
    ];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, None),
        MatchOutcome::NotMatched,
        "evaluator must short-circuit on the second false condition"
    );
}

#[test]
fn matchcondition_can_reference_request_operation() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("kube-system", "any");

    let conditions = vec![MatchCondition {
        name: "creates-only".to_string(),
        expression: "request.operation == 'CREATE'".to_string(),
    }];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, None),
        MatchOutcome::Matched,
    );
}

#[test]
fn matchcondition_oldobject_is_null_on_create() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("kube-system", "any");

    let conditions = vec![MatchCondition {
        name: "is-create".to_string(),
        expression: "oldObject == null".to_string(),
    }];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, None),
        MatchOutcome::Matched,
    );
}

#[test]
fn matchcondition_can_reference_params_when_provided() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("any-ns", "any-name");

    let params = json!({
        "metadata": { "name": "policy-params" },
        "spec": { "enabled": true },
    });

    let conditions = vec![MatchCondition {
        name: "params-enabled".to_string(),
        expression: "params.spec.enabled == true".to_string(),
    }];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, Some(&params)),
        MatchOutcome::Matched,
    );
}

#[test]
fn matchcondition_params_is_null_when_no_paramref() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("any-ns", "any-name");

    let conditions = vec![MatchCondition {
        name: "params-null".to_string(),
        expression: "params == null".to_string(),
    }];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, None),
        MatchOutcome::Matched,
    );
}

#[test]
fn matchcondition_error_on_compile_failure() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("kube-system", "any");

    let conditions = vec![MatchCondition {
        name: "broken".to_string(),
        expression: "this is not valid CEL @@".to_string(),
    }];

    match evaluator.evaluate(&conditions, &request, None) {
        MatchOutcome::Error(msg) => {
            assert!(
                msg.contains("broken"),
                "error message should reference condition name 'broken': {}",
                msg
            );
        }
        other => panic!("expected Error outcome, got {:?}", other),
    }
}

#[test]
fn matchcondition_empty_list_matches() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("kube-system", "any");

    assert_eq!(
        evaluator.evaluate(&[], &request, None),
        MatchOutcome::Matched,
        "empty match conditions list must match (policy always applies)"
    );
}

#[test]
fn matchcondition_user_info_visible() {
    let mut evaluator = MatchConditionEvaluator::new();
    let request = create_configmap_request("default", "x");

    let conditions = vec![MatchCondition {
        name: "is-admin".to_string(),
        expression: "request.userInfo.username == 'system:admin'".to_string(),
    }];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, None),
        MatchOutcome::Matched,
    );
}

#[test]
fn matchcondition_request_kind_exposes_group_and_version() {
    // Verifies Work Unit A: AdmissionRequest now carries group/version, and
    // build_context surfaces them inside `request.kind.{group,version,kind}`
    // exactly like the K8s admission webhook envelope.
    let mut evaluator = MatchConditionEvaluator::new();
    let mut request = create_configmap_request("default", "x");
    // Swap to a non-core resource so group/version are non-trivial.
    request.group = "apps".to_string();
    request.version = "v1".to_string();
    request.kind = "Deployment".to_string();

    let conditions = vec![MatchCondition {
        name: "deployment-only".to_string(),
        expression: "request.kind.group == 'apps' \
                     && request.kind.version == 'v1' \
                     && request.kind.kind == 'Deployment'"
            .to_string(),
    }];

    assert_eq!(
        evaluator.evaluate(&conditions, &request, None),
        MatchOutcome::Matched,
        "request.kind.{{group,version,kind}} must reflect the AdmissionRequest"
    );
}
