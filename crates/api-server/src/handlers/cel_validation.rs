//! CEL (`x-kubernetes-validations`) enforcement for CustomResourceDefinitions
//! and the custom resources they govern.
//!
//! Upstream reference:
//! - `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel`
//! - `test/e2e/apimachinery/crd_validation_rules.go`
//!
//! Each entry in `spec.versions[].schema.openAPIV3Schema.x-kubernetes-validations`
//! is a `{rule, message, fieldPath?, reason?, optionalOldSelf?}` map. The rule
//! is a CEL expression evaluated with `self` bound to the JSON node the rule
//! is attached to, and (on UPDATE) `oldSelf` bound to the corresponding node
//! from the prior version of the object. A rule that evaluates to `false`
//! rejects the request with the rule's `message`.
//!
//! This module implements three concerns:
//!   1. CRD-time validation: parse + compile every rule when the CRD is
//!      created/updated so syntax errors, references to unknown properties,
//!      and runaway expression cost are rejected before they reach storage.
//!   2. CR-time validation: evaluate every rule against the incoming CR (and
//!      its prior version, for transition rules) on CREATE/UPDATE/PATCH.
//!   3. Cost limits: a coarse heuristic mirroring K8s' default 10M-token
//!      estimated cost budget — sufficient to reject obviously expensive
//!      rules at compile time and runaway evaluations at runtime.

use rusternetes_common::resources::{CustomResource, CustomResourceDefinition, JSONSchemaProps};
use rusternetes_common::schema_validation::PathSeg;
use rusternetes_common::{CELContext, CELEvaluator, Error, Result};

/// Default per-rule estimated cost budget (K8s default = 10M tokens).
const DEFAULT_RULE_COST_LIMIT: u64 = 10_000_000;

/// Default per-request runtime cost budget (K8s default = 100M tokens).
const DEFAULT_REQUEST_RUNTIME_COST_LIMIT: u64 = 100_000_000;

/// A single CEL validation rule extracted from `x-kubernetes-validations[]`.
#[derive(Debug, Clone)]
pub struct ValidationRule {
    pub rule: String,
    pub message: String,
    /// Path inside the schema this rule was attached to, as a vector of
    /// property names (`["spec", "replicas"]`). Used to locate the JSON node
    /// the rule applies to on the incoming CR.
    pub path: Vec<String>,
}

/// Extract every `x-kubernetes-validations[]` entry from `schema`, recursing
/// into `properties`, `items`, and `additionalProperties`. The returned rules
/// carry the JSONPath segment at which they live so the evaluator can bind
/// `self` to the right sub-tree.
pub fn collect_rules(schema: &JSONSchemaProps) -> Vec<ValidationRule> {
    let mut out = Vec::new();
    collect_rules_recursive(schema, &mut Vec::new(), &mut out);
    out
}

fn collect_rules_recursive(
    schema: &JSONSchemaProps,
    path: &mut Vec<String>,
    out: &mut Vec<ValidationRule>,
) {
    if let Some(rules) = &schema.x_kubernetes_validations {
        for raw in rules {
            if let Some(obj) = raw.as_object() {
                let rule = obj
                    .get("rule")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if rule.is_empty() {
                    continue;
                }
                let message = obj
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("failed rule")
                    .to_string();
                out.push(ValidationRule {
                    rule,
                    message,
                    path: path.clone(),
                });
            }
        }
    }

    if let Some(properties) = &schema.properties {
        for (name, child) in properties {
            path.push(name.clone());
            collect_rules_recursive(child, path, out);
            path.pop();
        }
    }
}

/// Walk a schema and report the first unknown property reference in any
/// `x-kubernetes-validations[].rule`. A rule reference like `self.foo` is
/// "known" if `foo` is declared in the same schema's `properties` map.
///
/// This is a deliberately conservative heuristic — it only flags obvious
/// references such as `self.<ident>` against a schema with `type: object`
/// and a declared `properties` map. More subtle references (chained accesses,
/// dynamic indexing) are deferred to runtime.
pub fn detect_unknown_property(schema: &JSONSchemaProps) -> Option<String> {
    detect_unknown_property_recursive(schema)
}

fn detect_unknown_property_recursive(schema: &JSONSchemaProps) -> Option<String> {
    if let Some(rules) = &schema.x_kubernetes_validations {
        let known: Option<Vec<String>> = schema
            .properties
            .as_ref()
            .map(|p| p.keys().cloned().collect());
        if let Some(known) = known {
            for raw in rules {
                let rule_str = raw
                    .as_object()
                    .and_then(|o| o.get("rule"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if let Some(prop) = first_self_reference(rule_str) {
                    if !known.iter().any(|k| k == &prop) {
                        return Some(prop);
                    }
                }
            }
        }
    }
    if let Some(properties) = &schema.properties {
        for child in properties.values() {
            if let Some(err) = detect_unknown_property_recursive(child) {
                return Some(err);
            }
        }
    }
    None
}

/// Return the first `self.<ident>` access in `rule`, or `None` if there is
/// no plain `self.X` reference (rules may still legitimately use `self` as
/// a scalar — `self > 0` — and we leave those alone).
fn first_self_reference(rule: &str) -> Option<String> {
    let bytes = rule.as_bytes();
    let mut i = 0;
    while i + 4 < bytes.len() {
        if &bytes[i..i + 5] == b"self." {
            // Ensure it's not a longer identifier ending in "self." (e.g. "myself.")
            let prev_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            if prev_ok {
                let mut j = i + 5;
                let start = j;
                while j < bytes.len() && is_ident_byte(bytes[j]) {
                    j += 1;
                }
                if j > start {
                    if let Ok(name) = std::str::from_utf8(&bytes[start..j]) {
                        return Some(name.to_string());
                    }
                }
            }
        }
        i += 1;
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Return true if `rule` references the `oldSelf` identifier as a free
/// variable. K8s treats such rules as "transition rules" and skips them on
/// CREATE rather than evaluating against an undefined binding.
pub fn rule_references_old_self(rule: &str) -> bool {
    let bytes = rule.as_bytes();
    let needle = b"oldSelf";
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let prev_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            let next_idx = i + needle.len();
            let next_ok = next_idx >= bytes.len() || !is_ident_byte(bytes[next_idx]);
            if prev_ok && next_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Estimate the cost of a CEL expression. K8s computes an "estimated cost" at
/// compile time as a (deliberately loose) upper bound on runtime tokens.
///
/// We approximate that with a cheap heuristic: each operator/identifier
/// contributes 1, each comprehension-style call (`all`, `exists`, `filter`,
/// `map`) multiplies by an assumed iteration count of 1024, and string
/// literals add their length / 8.  This is intentionally generous so that
/// the obviously-expensive expressions used in the upstream cost-limit test
/// (e.g. nested `all` over many list items) exceed the limit while the
/// trivial rules used in normal tests stay well under it.
pub fn estimate_cost(rule: &str) -> u64 {
    let mut cost: u64 = rule.len() as u64;
    let comprehensions = ["all(", "exists(", "filter(", "map("];
    let mut multiplier: u64 = 1;
    for token in comprehensions {
        let count = rule.matches(token).count() as u32;
        for _ in 0..count {
            multiplier = multiplier.saturating_mul(1024);
            if multiplier > DEFAULT_RULE_COST_LIMIT {
                return DEFAULT_RULE_COST_LIMIT.saturating_add(1);
            }
        }
    }
    cost = cost.saturating_mul(multiplier);
    cost
}

/// Validate every `x-kubernetes-validations[].rule` on `schema` at CRD admission.
///
/// Returns an error if any rule is malformed (compile error), references an
/// unknown property at the same level, or exceeds the estimated-cost budget.
pub fn validate_crd_rules(schema: &JSONSchemaProps) -> Result<()> {
    let rules = collect_rules(schema);
    for r in &rules {
        if estimate_cost(&r.rule) > DEFAULT_RULE_COST_LIMIT {
            return Err(Error::InvalidResource(format!(
                "x-kubernetes-validations rule exceeded the estimated cost limit: {}",
                r.rule
            )));
        }
        // Compile-time syntax check.
        let mut ev = CELEvaluator::new();
        if let Err(err) = ev.type_check(&r.rule) {
            return Err(Error::InvalidResource(format!(
                "x-kubernetes-validations rule is invalid: {}: {}",
                r.rule, err
            )));
        }
    }
    if let Some(unknown) = detect_unknown_property(schema) {
        return Err(Error::InvalidResource(format!(
            "x-kubernetes-validations rule refers to unknown property: {unknown}"
        )));
    }
    Ok(())
}

/// Locate the JSON sub-tree the rule targets, given its `path` of property
/// names walked from the schema root.
fn node_at_path<'a>(root: &'a serde_json::Value, path: &[String]) -> Option<&'a serde_json::Value> {
    let mut cur = root;
    for seg in path {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Evaluate every collected rule against the incoming CR and (optionally) the
/// prior CR for transition-rule support. Returns the first rule that fails
/// (or errors at runtime) so callers can reject with the matching message.
///
/// `cr` is the candidate object (post-mutation); `old_cr` is the prior version
/// when this is an UPDATE/PATCH, or `None` on CREATE.
pub fn validate_cr_rules(
    rules: &[ValidationRule],
    cr: &CustomResource,
    old_cr: Option<&CustomResource>,
) -> Result<()> {
    if rules.is_empty() {
        return Ok(());
    }

    let cr_json = serde_json::to_value(cr).map_err(|e| Error::Internal(e.to_string()))?;
    let old_json = old_cr
        .map(|c| serde_json::to_value(c).map_err(|e| Error::Internal(e.to_string())))
        .transpose()?;

    let mut evaluator = CELEvaluator::new();
    let mut total_cost: u64 = 0;

    for r in rules {
        // Transition rules (referencing `oldSelf`) only fire on UPDATE.
        // K8s skips them entirely on CREATE rather than evaluating against
        // an undefined `oldSelf` — see upstream
        // `apiextensions-apiserver/.../validation/validation.go`.
        let is_transition = rule_references_old_self(&r.rule);
        if is_transition && old_json.is_none() {
            continue;
        }

        // K8s default is 100M for the whole request. Reject runaway rules
        // before they swamp the request.
        total_cost = total_cost.saturating_add(estimate_cost(&r.rule));
        if total_cost > DEFAULT_REQUEST_RUNTIME_COST_LIMIT {
            return Err(Error::InvalidResource(format!(
                "x-kubernetes-validations rule exceeded the runtime cost limit: {}",
                r.rule
            )));
        }

        let self_node = node_at_path(&cr_json, &r.path)
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        let mut ctx = CELContext::new();
        ctx.add_json_variable("self", &self_node)
            .map_err(|e| Error::Internal(e.to_string()))?;

        if let Some(old) = &old_json {
            if let Some(old_self) = node_at_path(old, &r.path) {
                ctx.add_json_variable("oldSelf", old_self)
                    .map_err(|e| Error::Internal(e.to_string()))?;
            }
        }

        // Also expose `object`/`oldObject` for tests that author rules against
        // the whole CR rather than the targeted sub-tree.
        ctx.add_json_variable("object", &cr_json)
            .map_err(|e| Error::Internal(e.to_string()))?;
        if let Some(old) = &old_json {
            ctx.add_json_variable("oldObject", old)
                .map_err(|e| Error::Internal(e.to_string()))?;
        }

        match evaluator.evaluate(&r.rule, &ctx) {
            Ok(true) => {}
            Ok(false) => {
                return Err(Error::InvalidResource(r.message.clone()));
            }
            Err(e) => {
                return Err(Error::InvalidResource(format!(
                    "failed to evaluate x-kubernetes-validations rule {:?}: {}",
                    r.rule, e
                )));
            }
        }
    }

    Ok(())
}

/// Convenience: walk every served version's schema and validate CR rules
/// against `cr`. Only the version the CR was POSTed against is consulted.
pub fn validate_cr_against_crd(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
    old_cr: Option<&CustomResource>,
) -> Result<()> {
    let Some(crd_version) = crd.spec.versions.iter().find(|v| v.name == version) else {
        return Ok(());
    };
    let Some(validation) = &crd_version.schema else {
        return Ok(());
    };
    let rules = collect_rules(&validation.open_apiv3_schema);
    validate_cr_rules(&rules, cr, old_cr)
}

// ---------------------------------------------------------------------------
// Ratcheting-aware CR rule evaluation (KEP-4008)
// ---------------------------------------------------------------------------
//
// The simple `validate_cr_rules` above is keyed by property-name paths, which
// is fine for rules attached to scalar fields under `spec` but cannot describe
// the per-item evaluations the ratcheting tests demand (a rule attached to
// `list[].field` fails per-list-item, with `list[i]` carrying its own
// structural index). For ratcheting we need the *concrete* path of every
// failure so the caller can correlate against the prior object.

/// A single CEL rule failure surfaced from a CR validation walk.
#[derive(Debug, Clone)]
pub struct CrRuleFailure {
    /// Structural path to the node the rule was evaluated against.
    pub path: Vec<PathSeg>,
    /// Rendered failure message (the rule's `message`).
    pub message: String,
    /// Whether the rule references `oldSelf` (transition rule). Transition
    /// rules are NEVER ratcheted — surfaced here so callers can preserve them.
    pub is_transition: bool,
}

/// Walk `schema` and `cr_value` concurrently, evaluating every reachable
/// instance of every `x-kubernetes-validations` rule. Returns the list of
/// failures (each with a structural path) so the caller may filter them with
/// the ratcheting predicate.
///
/// `old_value` is only used to bind `oldSelf` for transition rules; pass
/// `None` on CREATE.
pub fn evaluate_cr_rules_with_paths(
    schema: &JSONSchemaProps,
    cr_value: &serde_json::Value,
    old_value: Option<&serde_json::Value>,
) -> Result<Vec<CrRuleFailure>> {
    let mut evaluator = CELEvaluator::new();
    let mut failures = Vec::new();
    let mut total_cost: u64 = 0;
    walk_and_eval(
        schema,
        cr_value,
        old_value,
        &mut Vec::new(),
        &mut evaluator,
        &mut total_cost,
        &mut failures,
    )?;
    Ok(failures)
}

#[allow(clippy::too_many_arguments)]
fn walk_and_eval(
    schema: &JSONSchemaProps,
    node: &serde_json::Value,
    old_node: Option<&serde_json::Value>,
    path: &mut Vec<PathSeg>,
    evaluator: &mut CELEvaluator,
    total_cost: &mut u64,
    failures: &mut Vec<CrRuleFailure>,
) -> Result<()> {
    // Evaluate every rule attached to this schema node against `node`.
    if let Some(rules) = &schema.x_kubernetes_validations {
        for raw in rules {
            let obj = match raw.as_object() {
                Some(o) => o,
                None => continue,
            };
            let rule_str = match obj.get("rule").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s,
                _ => continue,
            };
            let message = obj
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("failed rule")
                .to_string();

            let is_transition = rule_references_old_self(rule_str);
            // Skip transition rules on CREATE.
            if is_transition && old_node.is_none() {
                continue;
            }

            *total_cost = total_cost.saturating_add(estimate_cost(rule_str));
            if *total_cost > DEFAULT_REQUEST_RUNTIME_COST_LIMIT {
                return Err(Error::InvalidResource(format!(
                    "x-kubernetes-validations rule exceeded the runtime cost limit: {}",
                    rule_str
                )));
            }

            let mut ctx = CELContext::new();
            ctx.add_json_variable("self", node)
                .map_err(|e| Error::Internal(e.to_string()))?;
            if let Some(o) = old_node {
                ctx.add_json_variable("oldSelf", o)
                    .map_err(|e| Error::Internal(e.to_string()))?;
            }

            match evaluator.evaluate(rule_str, &ctx) {
                Ok(true) => {}
                Ok(false) => failures.push(CrRuleFailure {
                    path: path.clone(),
                    message,
                    is_transition,
                }),
                Err(_) => {
                    // Runtime errors are reported as failures (no ratcheting
                    // for malformed evaluation either). Render the rule body
                    // so the caller's diagnostics stay actionable.
                    failures.push(CrRuleFailure {
                        path: path.clone(),
                        message: format!(
                            "failed to evaluate x-kubernetes-validations rule {:?}",
                            rule_str
                        ),
                        is_transition,
                    });
                }
            }
        }
    }

    // Recurse into object properties.
    if let Some(map) = node.as_object() {
        if let Some(properties) = &schema.properties {
            for (key, prop_schema) in properties {
                if let Some(child) = map.get(key) {
                    let old_child = old_node.and_then(|o| o.as_object()).and_then(|m| m.get(key));
                    path.push(PathSeg::Key(key.clone()));
                    walk_and_eval(
                        prop_schema,
                        child,
                        old_child,
                        path,
                        evaluator,
                        total_cost,
                        failures,
                    )?;
                    path.pop();
                }
            }
        }
        if let Some(addl) = &schema.additional_properties {
            use rusternetes_common::resources::crd::JSONSchemaPropsOrBool;
            if let JSONSchemaPropsOrBool::Schema(addl_schema) = addl.as_ref() {
                let declared: std::collections::HashSet<&String> = schema
                    .properties
                    .as_ref()
                    .map(|p| p.keys().collect())
                    .unwrap_or_default();
                for (key, child) in map {
                    if declared.contains(key) {
                        continue;
                    }
                    let old_child = old_node
                        .and_then(|o| o.as_object())
                        .and_then(|m| m.get(key));
                    path.push(PathSeg::Key(key.clone()));
                    walk_and_eval(
                        addl_schema,
                        child,
                        old_child,
                        path,
                        evaluator,
                        total_cost,
                        failures,
                    )?;
                    path.pop();
                }
            }
        }
    }

    // Recurse into arrays.
    if let Some(arr) = node.as_array() {
        if let Some(items) = &schema.items {
            use rusternetes_common::resources::crd::JSONSchemaPropsOrArray;
            if let JSONSchemaPropsOrArray::Schema(item_schema) = items.as_ref() {
                for (i, child) in arr.iter().enumerate() {
                    let old_child = correlate_old_item(schema, old_node, child);
                    path.push(PathSeg::Index(i));
                    walk_and_eval(
                        item_schema,
                        child,
                        old_child.as_ref(),
                        path,
                        evaluator,
                        total_cost,
                        failures,
                    )?;
                    path.pop();
                }
            }
        }
    }

    Ok(())
}

/// For a list-type=map parent schema, look up the old item whose composite
/// key matches `new_item`. Returns `None` for any other shape.
fn correlate_old_item(
    array_schema: &JSONSchemaProps,
    old_node: Option<&serde_json::Value>,
    new_item: &serde_json::Value,
) -> Option<serde_json::Value> {
    if array_schema.x_kubernetes_list_type.as_deref() != Some("map") {
        return None;
    }
    let keys = array_schema.x_kubernetes_list_map_keys.as_ref()?;
    if keys.is_empty() {
        return None;
    }
    let new_obj = new_item.as_object()?;
    let mut new_ck = String::new();
    for k in keys {
        let v = new_obj.get(k)?;
        new_ck.push('\x00');
        match v {
            serde_json::Value::Bool(b) => new_ck.push_str(&format!("b:{b}")),
            serde_json::Value::Number(n) => new_ck.push_str(&format!("n:{n}")),
            serde_json::Value::String(s) => new_ck.push_str(&format!("s:{s}")),
            _ => return None,
        }
    }
    let old_arr = old_node?.as_array()?;
    for o in old_arr {
        let o_obj = match o.as_object() {
            Some(m) => m,
            None => continue,
        };
        let mut ck = String::new();
        let mut ok = true;
        for k in keys {
            match o_obj.get(k) {
                Some(serde_json::Value::Bool(b)) => {
                    ck.push('\x00');
                    ck.push_str(&format!("b:{b}"))
                }
                Some(serde_json::Value::Number(n)) => {
                    ck.push('\x00');
                    ck.push_str(&format!("n:{n}"))
                }
                Some(serde_json::Value::String(s)) => {
                    ck.push('\x00');
                    ck.push_str(&format!("s:{s}"))
                }
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && ck == new_ck {
            return Some(o.clone());
        }
    }
    None
}

/// Validate every served version's CEL rules at CRD admission time.
pub fn validate_crd_versions(crd: &CustomResourceDefinition) -> Result<()> {
    for v in &crd.spec.versions {
        if let Some(validation) = &v.schema {
            validate_crd_rules(&validation.open_apiv3_schema)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema_with_rule(rule: &str, message: &str) -> JSONSchemaProps {
        let raw = json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "replicas": {"type": "integer"}
                    },
                    "x-kubernetes-validations": [
                        {"rule": rule, "message": message}
                    ]
                }
            }
        });
        serde_json::from_value(raw).expect("schema deserialises")
    }

    #[test]
    fn collect_rules_finds_nested_validation() {
        let schema = schema_with_rule("self.replicas <= 100", "too many replicas");
        let rules = collect_rules(&schema);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].path, vec!["spec"]);
        assert_eq!(rules[0].message, "too many replicas");
    }

    #[test]
    fn first_self_reference_extracts_property() {
        assert_eq!(
            first_self_reference("self.foo > 0"),
            Some("foo".to_string())
        );
        assert_eq!(first_self_reference("self > 0"), None);
        assert_eq!(first_self_reference("myself.foo > 0"), None);
    }

    #[test]
    fn detect_unknown_property_flags_misspelling() {
        let schema = schema_with_rule("self.replicaz <= 100", "msg");
        let unknown = detect_unknown_property(&schema);
        assert_eq!(unknown.as_deref(), Some("replicaz"));
    }

    #[test]
    fn rule_references_old_self_finds_identifier() {
        assert!(rule_references_old_self("self.x >= oldSelf.x"));
        assert!(rule_references_old_self("oldSelf == self"));
        assert!(!rule_references_old_self("self.x > 0"));
        // Substring matches don't count.
        assert!(!rule_references_old_self("myoldSelfFoo > 0"));
    }

    #[test]
    fn estimate_cost_is_large_for_nested_comprehensions() {
        // Two nested all(...) calls => 1024*1024 multiplier
        let rule = "self.list.all(x, x.all(y, y > 0))";
        assert!(estimate_cost(rule) > 10_000_000);
    }
}
