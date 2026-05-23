//! End-to-end test that fires a `ValidatingAdmissionPolicy` through the full
//! admission path with:
//!
//!   * a `matchConditions[]` entry (must allow the policy to apply)
//!   * a `validations[]` entry that returns `false` with a `messageExpression`
//!     that renders a dynamic rejection message
//!   * an `auditAnnotations[]` entry that emits a string value
//!
//! This exercises the wiring done in:
//!   * `crate::cel::MatchConditionEvaluator`
//!   * `crate::cel::ValidationEvaluator`
//!   * `crate::cel::AuditAnnotationEvaluator`
//!
//! by stuffing a real VAP + binding into MemoryStorage and invoking
//! `AdmissionWebhookManager::run_validating_admission_policies_ext`.

use rusternetes_api_server::admission_webhook::AdmissionWebhookManager;
use rusternetes_common::admission::{GroupVersionKind, Operation};
use rusternetes_storage::{memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;

/// Insert a policy and binding pair into storage. `binding_age_secs` controls
/// the binding's age (VAP requires the binding to be "ready" — older than the
/// policy by some margin — before it applies; we set 10s for safety).
async fn install_policy(
    storage: &Arc<MemoryStorage>,
    policy_name: &str,
    binding_name: &str,
    policy: serde_json::Value,
    binding_extra: serde_json::Value,
    binding_age_secs: i64,
) {
    let policy_key = format!("/registry/validatingadmissionpolicies/{}", policy_name);
    storage
        .create::<serde_json::Value>(&policy_key, &policy)
        .await
        .unwrap();

    let old_time = (chrono::Utc::now() - chrono::Duration::seconds(binding_age_secs)).to_rfc3339();
    let mut binding = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {
            "name": binding_name,
            "creationTimestamp": old_time,
        },
        "spec": {
            "policyName": policy_name,
            "validationActions": ["Deny", "Audit"],
        }
    });
    // Merge binding_extra into the binding's spec.
    if let Some(extra) = binding_extra.as_object() {
        let spec = binding["spec"].as_object_mut().unwrap();
        for (k, v) in extra {
            spec.insert(k.clone(), v.clone());
        }
    }

    let binding_key = format!(
        "/registry/validatingadmissionpolicybindings/{}",
        binding_name
    );
    storage
        .create::<serde_json::Value>(&binding_key, &binding)
        .await
        .unwrap();
}

#[tokio::test]
async fn vap_end_to_end_match_validate_with_message_expression_and_audit_annotation() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    // ---- The policy ----
    //
    // matchConditions: only apply to ConfigMaps in the "production" namespace
    //                  whose group is the core API ("")
    // validations:     deny when the configmap's name starts with "deny-"
    //                  with a dynamic messageExpression
    // auditAnnotations: emit the requested configmap name
    let policy = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {
            "name": "production-configmap-policy",
            "creationTimestamp": chrono::Utc::now().to_rfc3339(),
        },
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": [""],
                    "apiVersions": ["v1"],
                    "resources": ["configmaps"],
                    "operations": ["CREATE"],
                }]
            },
            "matchConditions": [
                {
                    "name": "production-only",
                    "expression": "request.namespace == 'production'",
                },
                {
                    "name": "core-group-only",
                    "expression": "request.kind.group == '' && request.kind.kind == 'ConfigMap'",
                }
            ],
            "validations": [{
                "expression": "!object.metadata.name.startsWith('deny-')",
                "messageExpression":
                    "'configmap ' + object.metadata.name + ' is forbidden in ' + request.namespace",
                "message": "static fallback should NOT be used here",
            }],
            "auditAnnotations": [{
                "key": "configmap-name",
                "valueExpression": "object.metadata.name",
            }]
        }
    });

    install_policy(
        &storage,
        "production-configmap-policy",
        "production-configmap-policy-binding",
        policy,
        json!({}),
        10,
    )
    .await;

    let gvk = GroupVersionKind {
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
    };

    // Case 1: matchCondition not met (wrong namespace) → policy is silently
    // skipped, so the "always-false" validation does NOT trigger.
    let cm = json!({
        "metadata": { "name": "deny-thing", "namespace": "default" },
        "data": {},
    });
    let result = manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &gvk,
            Some(&cm),
            None,
            Some("configmaps"),
            Some("default"),
        )
        .await;
    assert!(
        result.is_ok(),
        "matchCondition (wrong namespace) must skip policy entirely, got: {:?}",
        result
    );

    // Case 2: matchConditions met but validation passes → admit.
    let cm = json!({
        "metadata": { "name": "allowed-thing", "namespace": "production" },
        "data": {},
    });
    let result = manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &gvk,
            Some(&cm),
            None,
            Some("configmaps"),
            Some("production"),
        )
        .await;
    assert!(
        result.is_ok(),
        "matchConditions met + validation passes must admit, got: {:?}",
        result
    );

    // Case 3: matchConditions met AND validation fails — messageExpression is
    // used (dynamic), the static `message` is NOT used as a fallback.
    let cm = json!({
        "metadata": { "name": "deny-secret", "namespace": "production" },
        "data": {},
    });
    let result = manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &gvk,
            Some(&cm),
            None,
            Some("configmaps"),
            Some("production"),
        )
        .await;
    let err = result.expect_err("must be denied by the VAP");
    let msg = err.to_string();
    assert!(
        msg.contains("configmap deny-secret is forbidden in production"),
        "expected dynamic messageExpression in error; got: {}",
        msg
    );
    assert!(
        !msg.contains("static fallback should NOT be used"),
        "static message must NOT be used when messageExpression succeeds; got: {}",
        msg
    );
}

#[tokio::test]
async fn vap_messageexpression_falls_back_to_static_when_cel_errors() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    // Validation that fires, with a deliberately broken messageExpression so we
    // exercise the fallback path inside `ValidationEvaluator::evaluate_one`.
    let policy = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {
            "name": "bad-msg-expr",
            "creationTimestamp": chrono::Utc::now().to_rfc3339(),
        },
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": [""],
                    "resources": ["configmaps"],
                    "operations": ["CREATE"],
                }]
            },
            "validations": [{
                "expression": "false",
                // Reference a field that doesn't exist on the object → runtime error
                "messageExpression": "object.metadata.nonexistent.deeply.nested",
                "message": "static-fallback-used",
            }]
        }
    });
    install_policy(
        &storage,
        "bad-msg-expr",
        "bad-msg-expr-binding",
        policy,
        json!({}),
        10,
    )
    .await;

    let gvk = GroupVersionKind {
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
    };
    let cm = json!({"metadata": {"name": "x"}});
    let result = manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &gvk,
            Some(&cm),
            None,
            Some("configmaps"),
            Some("default"),
        )
        .await;
    let err = result.expect_err("validation must fire");
    assert!(
        err.to_string().contains("static-fallback-used"),
        "must fall back to static message when messageExpression errors: {}",
        err
    );
}

#[tokio::test]
async fn vap_matchcondition_uses_group_and_version() {
    // Verifies that the VAP path's matchCondition (which now goes through the
    // shared MatchConditionEvaluator) sees `request.kind.{group,version,kind}`
    // — the new fields plumbed into AdmissionRequest in Work Unit A.
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    let policy = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {
            "name": "apps-v1-deployments-only",
            "creationTimestamp": chrono::Utc::now().to_rfc3339(),
        },
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": ["apps"],
                    "apiVersions": ["v1"],
                    "resources": ["deployments"],
                    "operations": ["CREATE"],
                }]
            },
            "matchConditions": [{
                "name": "apps-v1-deployment",
                "expression":
                    "request.kind.group == 'apps' && \
                     request.kind.version == 'v1' && \
                     request.kind.kind == 'Deployment'",
            }],
            "validations": [{
                "expression": "false",
                "message": "blocked by apps/v1/Deployment policy",
            }]
        }
    });
    install_policy(
        &storage,
        "apps-v1-deployments-only",
        "apps-v1-deployments-only-binding",
        policy,
        json!({}),
        10,
    )
    .await;

    // CASE A: request matches the matchCondition's GVK → validation fires → denied
    let apps_gvk = GroupVersionKind {
        group: "apps".to_string(),
        version: "v1".to_string(),
        kind: "Deployment".to_string(),
    };
    let deployment = json!({"metadata": {"name": "x", "namespace": "default"}});
    let result = manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &apps_gvk,
            Some(&deployment),
            None,
            Some("deployments"),
            Some("default"),
        )
        .await;
    let err = result.expect_err("apps/v1/Deployment must hit the policy");
    assert!(
        err.to_string().contains("blocked by apps/v1/Deployment"),
        "got: {}",
        err
    );

    // CASE B: a request with a different GVK (e.g. extensions/v1beta1) does not
    // satisfy the matchCondition — policy skipped, no denial. We have to also
    // align matchConstraints to allow extensions/v1beta1/Deployment so the
    // request reaches matchConditions. To keep this test focused, install a
    // second permissive policy that ONLY differs in matchCondition.
    let permissive_policy = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {
            "name": "wildcard-deployment-policy",
            "creationTimestamp": chrono::Utc::now().to_rfc3339(),
        },
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": ["*"],
                    "apiVersions": ["*"],
                    "resources": ["deployments"],
                    "operations": ["CREATE"],
                }]
            },
            "matchConditions": [{
                "name": "apps-v1-only",
                "expression":
                    "request.kind.group == 'apps' && request.kind.version == 'v1'",
            }],
            "validations": [{
                "expression": "false",
                "message": "wildcard-policy-fired",
            }]
        }
    });
    install_policy(
        &storage,
        "wildcard-deployment-policy",
        "wildcard-deployment-policy-binding",
        permissive_policy,
        json!({}),
        10,
    )
    .await;

    let ext_gvk = GroupVersionKind {
        group: "extensions".to_string(),
        version: "v1beta1".to_string(),
        kind: "Deployment".to_string(),
    };
    let result = manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &ext_gvk,
            Some(&deployment),
            None,
            Some("deployments"),
            Some("default"),
        )
        .await;
    // The wildcard policy's matchCondition rejects extensions/v1beta1, so it's
    // skipped. The apps/v1-only policy's matchConstraints reject extensions, so
    // it's skipped too. Result: admitted.
    assert!(
        result.is_ok(),
        "extensions/v1beta1/Deployment should NOT hit either policy; got: {:?}",
        result
    );
}
