//! Dedicated CEL (Common Expression Language) evaluators for the admission path.
//!
//! This module owns the **VAP** (`ValidatingAdmissionPolicy`) and admission
//! webhook CEL surfaces and offers a single, well-documented place to fix
//! parity issues against upstream Kubernetes.
//!
//! Upstream reference (Go):
//!   `staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/`
//!     - `compile.go`  — defines the activation variables every CEL surface sees
//!     - `filter.go`   — `Filter.ForInput` driver that evaluates expressions
//!     - `validator.go`— evaluates `validations[*].expression` &
//!                       `messageExpression`
//!   `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/matching/matcher.go`
//!     — short-circuits matchConditions in order
//!
//! The heavy-lifting CEL primitives (`CELEvaluator`, `CELContext`, JSON→CEL
//! value conversion) live in [`rusternetes_common::cel`]. This module exposes
//! purpose-built APIs that take a typed [`AdmissionRequest`] and return one of
//! a small set of outcomes per surface.
//!
//! # Surfaces in this module
//!
//! | Type                       | What it evaluates                                                |
//! |----------------------------|------------------------------------------------------------------|
//! | [`MatchConditionEvaluator`]| `Webhook.matchConditions[*]` & `VAP.spec.matchConditions[*]`     |
//! | [`ValidationEvaluator`]    | `VAP.spec.validations[*].expression` (+ optional messageExpression)|
//! | [`AuditAnnotationEvaluator`]| `VAP.spec.auditAnnotations[*].valueExpression`                  |
//!
//! # Activation variables
//!
//! Per the [VAP spec], the CEL expressions can reference:
//!
//! * `object`         - the new object being admitted (the request body)
//! * `oldObject`      - the prior object on UPDATE; `null` otherwise
//! * `request`        - the [`AdmissionRequest`] metadata (operation, kind, etc.)
//! * `params`         - the resource pointed to by the binding's `paramRef`, or `null`
//! * `variables`      - lazily-evaluated `spec.variables` map (callers supply)
//! * `namespaceObject`- the Namespace object (callers supply when available)
//!
//! `authorizer` and request-options (`dryRun`, `options`) are **not** populated
//! by these evaluators; callers that need them should layer them on top of the
//! [`CELContext`] returned from [`build_context`] before invoking the evaluator.
//!
//! # Out of scope (tracked separately)
//!
//! * CRD `x-kubernetes-validations[*].rule` — that surface evaluates per-CR
//!   inside a different code path (CRD validation, not admission) and will be
//!   wired in a follow-up PR.
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

/// Outcome of evaluating a single `VAP.spec.validations[*]` entry.
///
/// Upstream Go reference: `admission/plugin/cel/validator.go::Validate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationOutcome {
    /// The expression returned `true` — validation passed.
    Pass,
    /// The expression returned `false`. `message` is the resolved message:
    /// the result of `messageExpression` when present and the static `message`
    /// otherwise (mirrors upstream).
    Fail { message: String },
    /// The expression failed to compile or errored at runtime. Caller honours
    /// the policy's `failurePolicy` here.
    Error { message: String },
}

/// Outcome of evaluating a single `VAP.spec.auditAnnotations[*]` entry.
///
/// Upstream parity (`admission/plugin/cel/validator.go::auditAnnotation`):
///
/// * a value expression returning a string → emit the annotation
/// * a value expression returning `null`   → **skip** the annotation
/// * a runtime/compile error               → caller honours `failurePolicy`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditAnnotationOutcome {
    /// Emit `{key: value}` on the audit event. `key` is the annotation's
    /// declared `key`, prefixed by the policy name on the caller side.
    Emit { key: String, value: String },
    /// `valueExpression` returned `null` — upstream drops the annotation.
    Skip,
    /// `valueExpression` errored. Caller honours `failurePolicy`.
    Error { message: String },
}

/// Evaluator dedicated to webhook & VAP `matchConditions[*]`.
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

        self.evaluate_with_context(conditions, &context)
    }

    /// Same as [`Self::evaluate`] but with a pre-built [`CELContext`] — used by
    /// the VAP path that needs to layer `variables` / `namespaceObject` onto
    /// the base activation before evaluating.
    pub fn evaluate_with_context(
        &mut self,
        conditions: &[MatchCondition],
        context: &CELContext,
    ) -> MatchOutcome {
        for cond in conditions {
            if cond.expression.trim().is_empty() {
                continue;
            }
            match self.inner.evaluate(&cond.expression, context) {
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

/// Evaluator for `VAP.spec.validations[*]`.
///
/// One call to [`Self::evaluate_one`] returns whether the validation passed
/// and, when it failed, resolves the message via `messageExpression` (CEL) if
/// present, falling back to the static `message` field. This matches the
/// upstream `validator.go::Validate` path exactly.
pub struct ValidationEvaluator;

impl ValidationEvaluator {
    /// Evaluate a single `validations[*]` entry.
    ///
    /// * `expression` — required boolean CEL expression.
    /// * `message_expression` — optional CEL expression returning a string,
    ///   consulted only when `expression` returns false.
    /// * `static_message` — fallback message when `messageExpression` is
    ///   absent or errors.
    /// * `evaluator` — caller-owned `CELEvaluator` (shared so the program cache
    ///   spans the policy's validations, variables, and messageExpressions).
    /// * `context` — caller-built [`CELContext`] populated with
    ///   `object` / `oldObject` / `request` / `params` /
    ///   `variables` / `namespaceObject`.
    pub fn evaluate_one(
        expression: &str,
        message_expression: Option<&str>,
        static_message: Option<&str>,
        evaluator: &mut CELEvaluator,
        context: &CELContext,
    ) -> ValidationOutcome {
        let trimmed = expression.trim();
        if trimmed.is_empty() {
            // An empty expression cannot fail — upstream treats it as no-op (Pass).
            return ValidationOutcome::Pass;
        }

        match evaluator.evaluate(trimmed, context) {
            Ok(true) => ValidationOutcome::Pass,
            Ok(false) => {
                let message =
                    resolve_message(message_expression, static_message, evaluator, context);
                ValidationOutcome::Fail { message }
            }
            Err(e) => ValidationOutcome::Error {
                message: format!("validation expression '{}' failed: {}", trimmed, e),
            },
        }
    }
}

/// Evaluator for `VAP.spec.auditAnnotations[*]`.
///
/// Upstream returns `nil` to drop the annotation, otherwise produces a string.
/// Non-string / non-null results are rejected at admission registration time
/// in upstream — here we coerce them to their CEL `Debug` form to surface
/// misconfiguration rather than silently drop.
pub struct AuditAnnotationEvaluator;

impl AuditAnnotationEvaluator {
    /// Evaluate one `auditAnnotations[*]` entry.
    ///
    /// `key` is the annotation key (caller is expected to prefix with the
    /// policy name per upstream `validator.go`).
    pub fn evaluate_one(
        key: &str,
        value_expression: &str,
        evaluator: &mut CELEvaluator,
        context: &CELContext,
    ) -> AuditAnnotationOutcome {
        let trimmed = value_expression.trim();
        if trimmed.is_empty() {
            // No expression means nothing to emit — upstream treats as Skip.
            return AuditAnnotationOutcome::Skip;
        }

        match evaluator.evaluate_to_value(trimmed, context) {
            Ok(cel::objects::Value::Null) => AuditAnnotationOutcome::Skip,
            Ok(cel::objects::Value::String(s)) => AuditAnnotationOutcome::Emit {
                key: key.to_string(),
                value: s.to_string(),
            },
            Ok(other) => AuditAnnotationOutcome::Emit {
                key: key.to_string(),
                value: format!("{:?}", other),
            },
            Err(e) => AuditAnnotationOutcome::Error {
                message: format!(
                    "auditAnnotation '{}' valueExpression '{}' failed: {}",
                    key, trimmed, e
                ),
            },
        }
    }
}

/// Build a CEL activation context with the standard VAP variables:
/// `object`, `oldObject`, `request`, and `params`.
///
/// Mirrors upstream `admission/plugin/cel/compile.go`'s activation. Callers
/// that need `variables` or `namespaceObject` should call
/// [`CELContext::add_variable`] / [`CELContext::add_json_variable`] on the
/// returned context. `authorizer` is not modelled here (no rusternetes
/// equivalent yet) and is therefore documented as out-of-scope.
pub fn build_context(
    request: &AdmissionRequest,
    params: Option<&Value>,
) -> Result<CELContext, anyhow::Error> {
    let mut context = CELContext::new();

    // object
    context.add_json_variable("object", &request.object)?;

    // oldObject — null on non-UPDATE
    let old = request.old_object.clone().unwrap_or(Value::Null);
    context.add_json_variable("oldObject", &old)?;

    // request — slim AdmissionRequest projection that mirrors upstream
    // `admission/plugin/cel/request.go` for the fields VAP / matchCondition
    // expressions actually use in the wild (operation/kind/namespace/name/userInfo).
    let op_str = operation_as_str(&request.operation);
    let request_val = serde_json::json!({
        "operation": op_str,
        "kind": {
            "group": request.group,
            "version": request.version,
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

fn operation_as_str(op: &rusternetes_common::admission::Operation) -> &'static str {
    use rusternetes_common::admission::Operation;
    match op {
        Operation::Create => "CREATE",
        Operation::Update => "UPDATE",
        Operation::Delete => "DELETE",
        Operation::Connect => "CONNECT",
    }
}

/// Resolve the rejection message for a failing validation, preferring
/// `messageExpression` (CEL → string) over the static `message`. Mirrors
/// upstream `validator.go::Validate` exactly: if the CEL message expression
/// errors or returns a non-string, fall back to the static message.
fn resolve_message(
    message_expression: Option<&str>,
    static_message: Option<&str>,
    evaluator: &mut CELEvaluator,
    context: &CELContext,
) -> String {
    if let Some(expr) = message_expression {
        let trimmed = expr.trim();
        if !trimmed.is_empty() {
            if let Ok(cel::objects::Value::String(s)) =
                evaluator.evaluate_to_value(trimmed, context)
            {
                return s.to_string();
            }
        }
    }
    static_message.unwrap_or("Validation failed").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::admission::{Operation, UserInfo};

    fn req(namespace: Option<&str>) -> AdmissionRequest {
        AdmissionRequest {
            operation: Operation::Create,
            group: "".to_string(),
            version: "v1".to_string(),
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

    #[test]
    fn request_kind_carries_group_and_version() {
        let mut e = MatchConditionEvaluator::new();
        let mut r = req(Some("ns"));
        r.group = "apps".to_string();
        r.version = "v1".to_string();
        r.kind = "Deployment".to_string();

        let cond = MatchCondition {
            name: "gvk-matches".to_string(),
            expression: "request.kind.group == 'apps' && request.kind.version == 'v1' \
                    && request.kind.kind == 'Deployment'"
                .to_string(),
        };
        assert_eq!(e.evaluate(&[cond], &r, None), MatchOutcome::Matched);
    }

    // ===== ValidationEvaluator =====

    #[test]
    fn validation_pass() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "object.metadata.name == 'test-cm'",
            None,
            None,
            &mut ev,
            &ctx,
        );
        assert_eq!(outcome, ValidationOutcome::Pass);
    }

    #[test]
    fn validation_fail_uses_static_message_when_no_message_expression() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "object.metadata.name == 'nope'",
            None,
            Some("name must be 'nope'"),
            &mut ev,
            &ctx,
        );
        assert_eq!(
            outcome,
            ValidationOutcome::Fail {
                message: "name must be 'nope'".to_string()
            }
        );
    }

    #[test]
    fn validation_fail_uses_message_expression_when_present() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "object.metadata.name == 'nope'",
            Some("'got name: ' + object.metadata.name"),
            Some("fallback"),
            &mut ev,
            &ctx,
        );
        assert_eq!(
            outcome,
            ValidationOutcome::Fail {
                message: "got name: test-cm".to_string()
            }
        );
    }

    #[test]
    fn validation_fail_falls_back_when_message_expression_errors() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "object.metadata.name == 'nope'",
            Some("this @@ is bogus"),
            Some("fallback used"),
            &mut ev,
            &ctx,
        );
        assert_eq!(
            outcome,
            ValidationOutcome::Fail {
                message: "fallback used".to_string()
            }
        );
    }

    #[test]
    fn validation_error_on_compile_failure() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome = ValidationEvaluator::evaluate_one(
            "this is not valid CEL @@",
            None,
            None,
            &mut ev,
            &ctx,
        );
        match outcome {
            ValidationOutcome::Error { message } => {
                assert!(
                    message.contains("this is not valid CEL"),
                    "msg: {}",
                    message
                )
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn validation_empty_expression_is_pass() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        assert_eq!(
            ValidationEvaluator::evaluate_one("   ", None, None, &mut ev, &ctx),
            ValidationOutcome::Pass
        );
    }

    // ===== AuditAnnotationEvaluator =====

    #[test]
    fn audit_annotation_emits_string() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome =
            AuditAnnotationEvaluator::evaluate_one("name", "object.metadata.name", &mut ev, &ctx);
        assert_eq!(
            outcome,
            AuditAnnotationOutcome::Emit {
                key: "name".to_string(),
                value: "test-cm".to_string(),
            }
        );
    }

    #[test]
    fn audit_annotation_null_skips() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        // CEL expression returning null
        let outcome = AuditAnnotationEvaluator::evaluate_one("only", "null", &mut ev, &ctx);
        assert_eq!(outcome, AuditAnnotationOutcome::Skip);
    }

    #[test]
    fn audit_annotation_error_on_compile_failure() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        let outcome =
            AuditAnnotationEvaluator::evaluate_one("bad", "this @@ is bogus", &mut ev, &ctx);
        match outcome {
            AuditAnnotationOutcome::Error { message } => {
                assert!(message.contains("bad"), "msg: {}", message)
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn audit_annotation_empty_expression_skips() {
        let r = req(Some("ns"));
        let ctx = build_context(&r, None).unwrap();
        let mut ev = CELEvaluator::new();
        assert_eq!(
            AuditAnnotationEvaluator::evaluate_one("k", "   ", &mut ev, &ctx),
            AuditAnnotationOutcome::Skip
        );
    }
}
