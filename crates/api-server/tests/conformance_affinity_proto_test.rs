//! Wire-format regression tests for the full `Affinity` message tree on
//! `PodSpec.affinity` (`k8s.io/api/core/v1/generated.proto`, release-1.35).
//!
//! This covers the three top-level sub-messages — `NodeAffinity`,
//! `PodAffinity`, `PodAntiAffinity` — and every nested wire shape they
//! delegate to:
//!
//! - `NodeAffinity.requiredDuringSchedulingIgnoredDuringExecution`
//!   (`NodeSelector`) → `nodeSelectorTerms[*]` → `NodeSelectorTerm` with
//!   both `matchExpressions` and `matchFields` of `NodeSelectorRequirement
//!   {key, operator, values[]}`.
//! - `NodeAffinity.preferredDuringSchedulingIgnoredDuringExecution`
//!   → repeated `PreferredSchedulingTerm{weight, preference}`.
//! - `PodAffinity.requiredDuringSchedulingIgnoredDuringExecution`
//!   → repeated `PodAffinityTerm{labelSelector, namespaces, topologyKey,
//!   namespaceSelector, matchLabelKeys, mismatchLabelKeys}`.
//! - `PodAffinity.preferredDuringSchedulingIgnoredDuringExecution`
//!   → repeated `WeightedPodAffinityTerm{weight, podAffinityTerm}`.
//! - `PodAntiAffinity` — identical shape to `PodAffinity`.
//!
//! These types were previously registered as opaque (empty) schemas in
//! `ProtoRegistry::new`, so any protobuf-encoded Pod carrying an `affinity`
//! block had every nested field silently dropped during the proto→JSON
//! middleware step. This test pins the field numbers and the nested
//! decode contract so future schema edits cannot regress the tree.
//!
//! Wire bytes are hand-crafted to keep the assertions auditable against
//! upstream `generated.proto` field numbers, mirroring the style used in
//! `conformance_configmap_envfrom_prefixes_test.rs`.

use rusternetes_api_server::protobuf::ProtoRegistry;
use serde_json::Value;

// --- low-level wire-format helpers --------------------------------------

/// Encode a varint per protobuf spec (7 bits per byte, MSB = continuation).
fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Emit a length-delimited (wire type 2) field: tag, length, bytes.
fn write_length_delimited(buf: &mut Vec<u8>, field_number: u32, payload: &[u8]) {
    write_varint(buf, ((field_number as u64) << 3) | 2);
    write_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

/// Emit a varint (wire type 0) field: tag, value.
fn write_varint_field(buf: &mut Vec<u8>, field_number: u32, value: u64) {
    write_varint(buf, (field_number as u64) << 3);
    write_varint(buf, value);
}

/// Emit a UTF-8 string at the given field number.
fn write_string(buf: &mut Vec<u8>, field_number: u32, s: &str) {
    write_length_delimited(buf, field_number, s.as_bytes());
}

// --- domain message builders --------------------------------------------

/// `NodeSelectorRequirement{key, operator, values[]}` per
/// `generated.proto` (field 1 = key, 2 = operator, 3 = repeated values).
fn node_selector_requirement(key: &str, operator: &str, values: &[&str]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_string(&mut buf, 1, key);
    write_string(&mut buf, 2, operator);
    for v in values {
        write_string(&mut buf, 3, v);
    }
    buf
}

/// `LabelSelectorRequirement{key, operator, values[]}` — same wire layout
/// as `NodeSelectorRequirement` (1=key, 2=operator, 3=values).
fn label_selector_requirement(key: &str, operator: &str, values: &[&str]) -> Vec<u8> {
    node_selector_requirement(key, operator, values)
}

/// `LabelSelector{matchExpressions[*]}` — only `matchExpressions` (field 2)
/// is populated here; the parity bug we're guarding lives in the
/// recursive nesting, so a non-trivial expression list is what matters.
fn label_selector(reqs: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for r in reqs {
        write_length_delimited(&mut buf, 2, r);
    }
    buf
}

/// `NodeSelectorTerm{matchExpressions[*], matchFields[*]}` per
/// generated.proto field numbers (1 = matchExpressions, 2 = matchFields).
fn node_selector_term(match_expressions: &[Vec<u8>], match_fields: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for r in match_expressions {
        write_length_delimited(&mut buf, 1, r);
    }
    for r in match_fields {
        write_length_delimited(&mut buf, 2, r);
    }
    buf
}

/// `NodeSelector{nodeSelectorTerms[*]}` — field 1 is repeated.
fn node_selector(terms: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for t in terms {
        write_length_delimited(&mut buf, 1, t);
    }
    buf
}

/// `PreferredSchedulingTerm{weight, preference}` (1 = weight int32,
/// 2 = preference NodeSelectorTerm).
fn preferred_scheduling_term(weight: i64, preference: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_varint_field(&mut buf, 1, weight as u64);
    write_length_delimited(&mut buf, 2, preference);
    buf
}

/// `PodAffinityTerm{labelSelector, namespaces[], topologyKey,
/// namespaceSelector, matchLabelKeys[], mismatchLabelKeys[]}`. Field
/// numbers per generated.proto: 1=labelSelector, 2=namespaces (repeated),
/// 3=topologyKey, 4=namespaceSelector, 5=matchLabelKeys, 6=mismatchLabelKeys.
#[allow(clippy::too_many_arguments)]
fn pod_affinity_term(
    label_selector_bytes: Option<&[u8]>,
    namespaces: &[&str],
    topology_key: &str,
    namespace_selector_bytes: Option<&[u8]>,
    match_label_keys: &[&str],
    mismatch_label_keys: &[&str],
) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Some(ls) = label_selector_bytes {
        write_length_delimited(&mut buf, 1, ls);
    }
    for ns in namespaces {
        write_string(&mut buf, 2, ns);
    }
    write_string(&mut buf, 3, topology_key);
    if let Some(ns_sel) = namespace_selector_bytes {
        write_length_delimited(&mut buf, 4, ns_sel);
    }
    for k in match_label_keys {
        write_string(&mut buf, 5, k);
    }
    for k in mismatch_label_keys {
        write_string(&mut buf, 6, k);
    }
    buf
}

/// `WeightedPodAffinityTerm{weight, podAffinityTerm}` (1=weight,
/// 2=podAffinityTerm).
fn weighted_pod_affinity_term(weight: i64, term_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_varint_field(&mut buf, 1, weight as u64);
    write_length_delimited(&mut buf, 2, term_bytes);
    buf
}

/// `NodeAffinity{required, preferred[]}` — 1=required, 2=preferred (repeated).
fn node_affinity(required_bytes: Option<&[u8]>, preferred_terms: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Some(req) = required_bytes {
        write_length_delimited(&mut buf, 1, req);
    }
    for p in preferred_terms {
        write_length_delimited(&mut buf, 2, p);
    }
    buf
}

/// `PodAffinity{required[], preferred[]}` — 1=required (repeated
/// PodAffinityTerm), 2=preferred (repeated WeightedPodAffinityTerm).
/// `PodAntiAffinity` has the identical wire layout.
fn pod_affinity_msg(required_terms: &[Vec<u8>], preferred_terms: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for r in required_terms {
        write_length_delimited(&mut buf, 1, r);
    }
    for p in preferred_terms {
        write_length_delimited(&mut buf, 2, p);
    }
    buf
}

/// `Affinity{nodeAffinity, podAffinity, podAntiAffinity}` — top-level
/// field numbers 1, 2, 3 respectively.
fn affinity(
    node_affinity_bytes: Option<&[u8]>,
    pod_affinity_bytes: Option<&[u8]>,
    pod_anti_affinity_bytes: Option<&[u8]>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Some(na) = node_affinity_bytes {
        write_length_delimited(&mut buf, 1, na);
    }
    if let Some(pa) = pod_affinity_bytes {
        write_length_delimited(&mut buf, 2, pa);
    }
    if let Some(paa) = pod_anti_affinity_bytes {
        write_length_delimited(&mut buf, 3, paa);
    }
    buf
}

// --- assertion helpers --------------------------------------------------

fn obj_get<'a>(value: &'a Value, key: &str) -> &'a Value {
    value
        .get(key)
        .unwrap_or_else(|| panic!("expected key {key:?} in {value}"))
}

fn arr<'a>(value: &'a Value, key: &str) -> &'a Vec<Value> {
    obj_get(value, key)
        .as_array()
        .unwrap_or_else(|| panic!("expected {key:?} to be array, got {value}"))
}

fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    obj_get(value, key)
        .as_str()
        .unwrap_or_else(|| panic!("expected {key:?} to be string, got {value}"))
}

fn i64_field(value: &Value, key: &str) -> i64 {
    obj_get(value, key)
        .as_i64()
        .unwrap_or_else(|| panic!("expected {key:?} to be int, got {value}"))
}

// --- tests --------------------------------------------------------------

/// Build a single comprehensive `Affinity` wire-format payload exercising
/// every nested level — `Affinity` → `PodAffinity` → `PodAffinityTerm` →
/// `LabelSelector` → `matchExpressions[*]` — plus parallel `NodeAffinity`
/// (with both required `NodeSelector` and preferred `PreferredSchedulingTerm`)
/// and `PodAntiAffinity` branches. Decode once and walk every nested field
/// to prove the recursive selector tree round-trips through the proto→JSON
/// decoder.
#[test]
fn test_affinity_full_tree_decodes_all_nested_selectors() {
    // --- NodeAffinity: required + preferred --------------------------
    let na_req_match_expr =
        node_selector_requirement("kubernetes.io/os", "In", &["linux", "windows"]);
    let na_req_match_field = node_selector_requirement("metadata.name", "NotIn", &["controlplane"]);
    let na_required_term = node_selector_term(&[na_req_match_expr], &[na_req_match_field]);
    let na_required = node_selector(&[na_required_term]);

    let na_pref_inner_expr =
        node_selector_requirement("topology.kubernetes.io/zone", "Exists", &[]);
    let na_pref_inner_term = node_selector_term(&[na_pref_inner_expr], &[]);
    let na_preferred = preferred_scheduling_term(42, &na_pref_inner_term);

    let na_bytes = node_affinity(Some(&na_required), &[na_preferred]);

    // --- PodAffinity: required + preferred ---------------------------
    let pa_req_inner_expr_a = label_selector_requirement("app", "In", &["web", "api"]);
    let pa_req_inner_expr_b = label_selector_requirement("tier", "NotIn", &["batch"]);
    let pa_req_label_selector = label_selector(&[pa_req_inner_expr_a, pa_req_inner_expr_b]);

    let pa_req_ns_sel_expr = label_selector_requirement("project", "Exists", &[]);
    let pa_req_namespace_selector = label_selector(&[pa_req_ns_sel_expr]);

    let pa_required_term = pod_affinity_term(
        Some(&pa_req_label_selector),
        &["ns-a", "ns-b"],
        "kubernetes.io/hostname",
        Some(&pa_req_namespace_selector),
        &["pod-template-hash"],
        &["release"],
    );

    let pa_pref_inner_expr = label_selector_requirement("affinity", "In", &["yes"]);
    let pa_pref_label_selector = label_selector(&[pa_pref_inner_expr]);
    let pa_pref_term = pod_affinity_term(
        Some(&pa_pref_label_selector),
        &[],
        "topology.kubernetes.io/zone",
        None,
        &[],
        &[],
    );
    let pa_preferred = weighted_pod_affinity_term(75, &pa_pref_term);

    let pa_bytes = pod_affinity_msg(&[pa_required_term], &[pa_preferred]);

    // --- PodAntiAffinity: only preferred -----------------------------
    let paa_pref_inner_expr = label_selector_requirement("conflicts", "NotIn", &["never"]);
    let paa_pref_label_selector = label_selector(&[paa_pref_inner_expr]);
    let paa_pref_term = pod_affinity_term(
        Some(&paa_pref_label_selector),
        &["other-ns"],
        "kubernetes.io/region",
        None,
        &[],
        &["mismatch-key"],
    );
    let paa_preferred = weighted_pod_affinity_term(10, &paa_pref_term);
    let paa_bytes = pod_affinity_msg(&[], &[paa_preferred]);

    // --- top-level Affinity ------------------------------------------
    let bytes = affinity(Some(&na_bytes), Some(&pa_bytes), Some(&paa_bytes));

    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("Affinity", &bytes)
        .expect("Affinity schema must be registered");

    // ===== NodeAffinity =====
    let node_affinity_json = obj_get(&decoded, "nodeAffinity");

    let required = obj_get(
        node_affinity_json,
        "requiredDuringSchedulingIgnoredDuringExecution",
    );
    let terms = arr(required, "nodeSelectorTerms");
    assert_eq!(terms.len(), 1, "exactly one NodeSelectorTerm");
    let term0 = &terms[0];

    let match_exprs = arr(term0, "matchExpressions");
    assert_eq!(match_exprs.len(), 1);
    assert_eq!(str_field(&match_exprs[0], "key"), "kubernetes.io/os");
    assert_eq!(str_field(&match_exprs[0], "operator"), "In");
    let values: Vec<&str> = arr(&match_exprs[0], "values")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(values, vec!["linux", "windows"]);

    let match_fields = arr(term0, "matchFields");
    assert_eq!(match_fields.len(), 1);
    assert_eq!(str_field(&match_fields[0], "key"), "metadata.name");
    assert_eq!(str_field(&match_fields[0], "operator"), "NotIn");

    let preferred = arr(
        node_affinity_json,
        "preferredDuringSchedulingIgnoredDuringExecution",
    );
    assert_eq!(preferred.len(), 1);
    assert_eq!(i64_field(&preferred[0], "weight"), 42);
    let pref_term = obj_get(&preferred[0], "preference");
    let pref_match_exprs = arr(pref_term, "matchExpressions");
    assert_eq!(pref_match_exprs.len(), 1);
    assert_eq!(
        str_field(&pref_match_exprs[0], "key"),
        "topology.kubernetes.io/zone"
    );
    assert_eq!(str_field(&pref_match_exprs[0], "operator"), "Exists");

    // ===== PodAffinity =====
    let pod_affinity_json = obj_get(&decoded, "podAffinity");

    let pa_required = arr(
        pod_affinity_json,
        "requiredDuringSchedulingIgnoredDuringExecution",
    );
    assert_eq!(pa_required.len(), 1);
    let pa_term0 = &pa_required[0];

    // PodAffinityTerm fields
    assert_eq!(str_field(pa_term0, "topologyKey"), "kubernetes.io/hostname");
    let pa_namespaces: Vec<&str> = arr(pa_term0, "namespaces")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(pa_namespaces, vec!["ns-a", "ns-b"]);
    let pa_match_label_keys: Vec<&str> = arr(pa_term0, "matchLabelKeys")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(pa_match_label_keys, vec!["pod-template-hash"]);
    let pa_mismatch_label_keys: Vec<&str> = arr(pa_term0, "mismatchLabelKeys")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(pa_mismatch_label_keys, vec!["release"]);

    // The deepest level: PodAffinityTerm.labelSelector.matchExpressions[*]
    let pa_label_sel = obj_get(pa_term0, "labelSelector");
    let pa_label_sel_exprs = arr(pa_label_sel, "matchExpressions");
    assert_eq!(
        pa_label_sel_exprs.len(),
        2,
        "labelSelector.matchExpressions must round-trip both entries"
    );
    assert_eq!(str_field(&pa_label_sel_exprs[0], "key"), "app");
    assert_eq!(str_field(&pa_label_sel_exprs[0], "operator"), "In");
    let pa_lse0_vals: Vec<&str> = arr(&pa_label_sel_exprs[0], "values")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(pa_lse0_vals, vec!["web", "api"]);
    assert_eq!(str_field(&pa_label_sel_exprs[1], "key"), "tier");
    assert_eq!(str_field(&pa_label_sel_exprs[1], "operator"), "NotIn");

    // PodAffinityTerm.namespaceSelector (sibling LabelSelector)
    let pa_ns_sel = obj_get(pa_term0, "namespaceSelector");
    let pa_ns_sel_exprs = arr(pa_ns_sel, "matchExpressions");
    assert_eq!(pa_ns_sel_exprs.len(), 1);
    assert_eq!(str_field(&pa_ns_sel_exprs[0], "key"), "project");
    assert_eq!(str_field(&pa_ns_sel_exprs[0], "operator"), "Exists");

    // PodAffinity.preferred → WeightedPodAffinityTerm{weight, podAffinityTerm}
    let pa_preferred_arr = arr(
        pod_affinity_json,
        "preferredDuringSchedulingIgnoredDuringExecution",
    );
    assert_eq!(pa_preferred_arr.len(), 1);
    assert_eq!(i64_field(&pa_preferred_arr[0], "weight"), 75);
    let wpat = obj_get(&pa_preferred_arr[0], "podAffinityTerm");
    assert_eq!(
        str_field(wpat, "topologyKey"),
        "topology.kubernetes.io/zone"
    );
    let wpat_ls_exprs = arr(obj_get(wpat, "labelSelector"), "matchExpressions");
    assert_eq!(wpat_ls_exprs.len(), 1);
    assert_eq!(str_field(&wpat_ls_exprs[0], "key"), "affinity");
    assert_eq!(str_field(&wpat_ls_exprs[0], "operator"), "In");

    // ===== PodAntiAffinity (same shape as PodAffinity) =====
    let pod_anti_affinity_json = obj_get(&decoded, "podAntiAffinity");

    // No required terms on this branch.
    assert!(
        pod_anti_affinity_json
            .get("requiredDuringSchedulingIgnoredDuringExecution")
            .and_then(|v| v.as_array())
            .map(|a| a.is_empty())
            .unwrap_or(true),
        "podAntiAffinity must have no required terms; got {pod_anti_affinity_json}"
    );

    let paa_preferred_arr = arr(
        pod_anti_affinity_json,
        "preferredDuringSchedulingIgnoredDuringExecution",
    );
    assert_eq!(paa_preferred_arr.len(), 1);
    assert_eq!(i64_field(&paa_preferred_arr[0], "weight"), 10);
    let paa_wpat = obj_get(&paa_preferred_arr[0], "podAffinityTerm");
    assert_eq!(str_field(paa_wpat, "topologyKey"), "kubernetes.io/region");
    let paa_ns: Vec<&str> = arr(paa_wpat, "namespaces")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(paa_ns, vec!["other-ns"]);
    let paa_mismatch: Vec<&str> = arr(paa_wpat, "mismatchLabelKeys")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(paa_mismatch, vec!["mismatch-key"]);
    let paa_ls_exprs = arr(obj_get(paa_wpat, "labelSelector"), "matchExpressions");
    assert_eq!(paa_ls_exprs.len(), 1);
    assert_eq!(str_field(&paa_ls_exprs[0], "key"), "conflicts");
    assert_eq!(str_field(&paa_ls_exprs[0], "operator"), "NotIn");
}

/// Spot-check that `Affinity` is registered (regression for a future
/// refactor that drops one of the three top-level sub-message schemas —
/// `NodeAffinity` / `PodAffinity` / `PodAntiAffinity` — and silently
/// reverts the tree to opaque decoding).
#[test]
fn test_affinity_subtype_schemas_are_registered() {
    let registry = ProtoRegistry::new();
    for name in ["Affinity", "NodeAffinity", "PodAffinity", "PodAntiAffinity"] {
        assert!(
            registry.decode_message(name, &[]).is_some(),
            "{name} schema must be registered (decoder returned None for empty bytes)"
        );
    }
}
