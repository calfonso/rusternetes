//! Thin CEL (Common Expression Language) evaluator wrapper for the API server.
//!
//! This module focuses on the **`ValidatingAdmissionPolicy.spec.matchConditions[*]`**
//! surface: each match condition is a CEL expression that gates whether a policy
//! applies to an admission request. If any matchCondition evaluates to `false`,
//! the policy is skipped for that request.
//!
//! Upstream reference (Go):
//!   staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/
//!   staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/matching/matcher.go
//!
//! The heavy-lifting CEL primitives (`CELEvaluator`, `CELContext`, JSON→CEL value
//! conversion) live in [`rusternetes_common::cel`]. This module exposes a small,
//! purpose-built API that takes an [`AdmissionRequest`] (and optional `params`
//! object loaded from the binding's `paramRef`) and returns whether the policy
//! should run.
//!
//! # Activation variables
//!
//! Per the [VAP spec], matchCondition expressions can reference:
//!
//! * `object`       - the new object being admitted (the request body)
//! * `oldObject`    - the prior object on UPDATE; `null` otherwise
//! * `request`      - the [`AdmissionRequest`] metadata (operation, kind, etc.)
//! * `params`       - the resource pointed to by the binding's `paramRef`, or `null`
//!
//! Other CEL surfaces (`spec.validations[*].expression`,
//! `spec.auditAnnotations[*].valueExpression`, CRD
//! `x-kubernetes-validations[*].rule`) are **out of scope** for this module and
//! are handled inline in [`crate::admission_webhook`] today. New evaluators for
//! those surfaces should live next to this one in follow-up work; see the
//! `TODO` markers in the public API below.
//!
//! [VAP spec]: https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/

use rusternetes_common::admission::AdmissionRequest;
use rusternetes_common::cel::{CELContext, CELEvaluator};
use rusternetes_common::resources::MatchCondition;
use serde_json::Value;

/// Outcome of evaluating a policy's `matchConditions[]` against a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchOutcome {
    /// Every match condition evaluated to `true` (or the list was empty). The
    /// policy applies and its validations should run.
    Matched,
    /// At least one match condition evaluated to `false`. The policy is silently
    /// skipped for this request — upstream calls this "not matched".
    NotMatched,
    /// A match condition expression failed to compile or returned a non-bool.
    /// Caller decides whether to honour the policy's `failurePolicy` here.
    Error(String),
}

/// Evaluator dedicated to `ValidatingAdmissionPolicy.spec.matchConditions[*]`.
///
/// Reuses the program cache exposed by [`rusternetes_common::cel::CELEvaluator`]
/// so repeated admission of objects against the same policy avoids re-compiling
/// the CEL programs.
pub struct MatchConditionEvaluator {
    inner: CELEvaluator,
}

impl MatchConditionEvaluator {
    pub fn new() -> Self {
        Self {
            inner: CELEvaluator::new(),
        }
    }

    /// Evaluate every match condition in `conditions` against `request`/`params`.
    ///
    /// Conditions are evaluated **in order** and short-circuit on the first
    /// non-matching condition, mirroring upstream `matcher.go`.
    ///
    /// Returns:
    /// * [`MatchOutcome::Matched`]    — `conditions` is empty or all returned `true`
    /// * [`MatchOutcome::NotMatched`] — some condition returned `false`
    /// * [`MatchOutcome::Error`]      — some condition errored (compile or runtime)
    pub fn evaluate(
        &mut self,
        conditions: &[MatchCondition],
        request: &AdmissionRequest,
        params: Option<&Value>,
    ) -> MatchOutcome {
        if conditions.is_empty() {
            return MatchOutcome::Matched;
        }

        let context = match build_context(request, params) {
            Ok(ctx) => ctx,
            Err(e) => return MatchOutcome::Error(format!("failed to build CEL context: {}", e)),
        };

        for cond in conditions {
            if cond.expression.trim().is_empty() {
                continue;
            }
            match self.inner.evaluate(&cond.expression, &context) {
                Ok(true) => {}
                Ok(false) => return MatchOutcome::NotMatched,
                Err(e) => {
                    return MatchOutcome::Error(format!(
                        "matchCondition '{}' (expression: {}) failed: {}",
                        cond.name, cond.expression, e
                    ));
                }
            }
        }

        MatchOutcome::Matched
    }
}

impl Default for MatchConditionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

// TODO(cel): wire up dedicated wrappers for these surfaces in follow-up PRs.
// They currently live inline inside `crate::admission_webhook`:
//
//   * `ValidatingAdmissionPolicy.spec.validations[*].expression`         (boolean)
//   * `ValidatingAdmissionPolicy.spec.validations[*].messageExpression`  (string)
//   * `ValidatingAdmissionPolicy.spec.auditAnnotations[*].valueExpression`
//   * CRD `x-kubernetes-validations[*].rule`
//
// Until those wrappers exist, the inline call sites are the source of truth.

/// Build a CEL activation context with the standard VAP variables:
/// `object`, `oldObject`, `request`, and `params`.
///
/// Mirrors upstream `admission/plugin/cel/compile.go`'s activation, minus the
/// `namespaceObject` variable which is handled in the broader VAP path because
/// it requires storage access.
fn build_context(
    request: &AdmissionRequest,
    params: Option<&Value>,
) -> Result<CELContext, anyhow::Error> {
    let mut context = CELContext::new();

    // object
    context.add_json_variable("object", &request.object)?;

    // oldObject — null on non-UPDATE
    let old = request.old_object.clone().unwrap_or(Value::Null);
    context.add_json_variable("oldObject", &old)?;

    // request — slim AdmissionRequest projection that's stable to construct
    // from the `AdmissionRequest` struct in `rusternetes_common::admission`.
    // Upstream populates more fields (uid, dryRun, options) but matchConditions
    // expressions in the wild use operation/kind/namespace/name/userInfo.
    let op_str = match request.operation {
        rusternetes_common::admission::Operation::Create => "CREATE",
        rusternetes_common::admission::Operation::Update => "UPDATE",
        rusternetes_common::admission::Operation::Delete => "DELETE",
        rusternetes_common::admission::Operation::Connect => "CONNECT",
    };
    let request_val = serde_json::json!({
        "operation": op_str,
        "kind": {
            // The AdmissionRequest only carries the kind name; group/version are
            // not stored on the struct. Match conditions that need group/version
            // should use the inline path in admission_webhook.rs.
            "kind": request.kind,
        },
        "namespace": request.namespace.clone().unwrap_or_default(),
        "name": request.name,
        "userInfo": {
            "username": request.user_info.username,
            "uid": request.user_info.uid,
            "groups": request.user_info.groups,
        },
    });
    context.add_json_variable("request", &request_val)?;

    // params — null when the binding has no paramRef or the param resource
    // wasn't found in storage. Matches upstream behaviour.
    let params_val = params.cloned().unwrap_or(Value::Null);
    context.add_json_variable("params", &params_val)?;

    Ok(context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::admission::{Operation, UserInfo};

    fn req(namespace: Option<&str>) -> AdmissionRequest {
        AdmissionRequest {
            operation: Operation::Create,
            kind: "ConfigMap".to_string(),
            namespace: namespace.map(|s| s.to_string()),
            name: "test-cm".to_string(),
            object: serde_json::json!({
                "metadata": {
                    "name": "test-cm",
                    "namespace": namespace.unwrap_or(""),
                },
            }),
            old_object: None,
            user_info: UserInfo {
                username: "alice".to_string(),
                uid: "uid-1".to_string(),
                groups: vec!["system:masters".to_string()],
            },
        }
    }

    #[test]
    fn empty_conditions_match() {
        let mut e = MatchConditionEvaluator::new();
        let r = req(Some("kube-system"));
        assert_eq!(e.evaluate(&[], &r, None), MatchOutcome::Matched);
    }

    #[test]
    fn skips_blank_expression() {
        let mut e = MatchConditionEvaluator::new();
        let r = req(Some("kube-system"));
        let cond = MatchCondition {
            name: "blank".to_string(),
            expression: "   ".to_string(),
        };
        assert_eq!(e.evaluate(&[cond], &r, None), MatchOutcome::Matched);
    }

    #[test]
    fn errors_surface_with_condition_name() {
        let mut e = MatchConditionEvaluator::new();
        let r = req(Some("kube-system"));
        let cond = MatchCondition {
            name: "bogus".to_string(),
            // not a boolean expression — should yield an Error outcome
            expression: "object.metadata.name".to_string(),
        };
        match e.evaluate(&[cond], &r, None) {
            MatchOutcome::Error(msg) => assert!(msg.contains("bogus"), "msg: {}", msg),
            other => panic!("expected Error, got {:?}", other),
        }
    }
}
