//! Upstream-parity corpus for `resource.Quantity`, ported from
//! `staging/src/k8s.io/apimachinery/pkg/api/resource/quantity_test.go`
//! in the Kubernetes Go tree.
//!
//! Background
//! ----------
//! Upstream `resource.Quantity` is a parsed numeric type with a chosen
//! `Format` (`DecimalSI`, `BinarySI`, `DecimalExponent`). On encode it
//! emits the canonical-form string for the stored Format, simplifying
//! where possible (`"1024Mi"` -> `"1Gi"`, `"1000m"` -> `"1"`) and
//! always producing `"0"` for the zero value regardless of the suffix
//! used on input.
//!
//! Rusternetes now ships a [`Quantity`] newtype that mirrors that
//! behaviour exactly. The tests below exercise the parser and
//! canonical encoder directly. Pod-routed cases stay because the
//! decode path (`deserialize_quantity_map`) shares the same parser
//! for input validation, even though it preserves the original input
//! string in the resulting map (callers that want canonical form ask
//! for it explicitly via `Quantity::canonical_string`).
//!
//! Categories covered (matches the unit brief):
//!   1. Canonical-form normalization on encode
//!   2. Suffix coverage — DecimalSI / BinarySI / DecimalExponent
//!   3. Suffix equivalence (`"1Ki"` and `"1024"` carry the same value)
//!   4. Zero quantity forms
//!   5. Format preservation across round-trip
//!   6. Negative quantities
//!   7. Very large / very small / boundary values
//!   8. Error cases — bad suffix / empty / mixed letters
//!
//! Anchor for upstream cross-reference:
//!   k8s.io/apimachinery/pkg/api/resource/quantity_test.go
//!   TestQuantityParse, TestQuantityCanonicalize, TestQuantityRoundTrip

use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::Pod;
use serde_json::{json, Value};

// ---- helpers ---------------------------------------------------------

fn pod_with_request(key: &str, value: Value) -> Result<Pod, serde_json::Error> {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p", "namespace": "default" },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": { "requests": { key: value } }
            }]
        }
    }))
}

fn pod_request(pod: &Pod, key: &str) -> Option<String> {
    pod.spec.as_ref()?.containers[0]
        .resources
        .as_ref()?
        .requests
        .as_ref()?
        .get(key)
        .cloned()
}

fn assert_canonical(input: &str, expected: &str) {
    let q = Quantity::parse(input)
        .unwrap_or_else(|e| panic!("expected parse({input:?}) to succeed: {e}"));
    assert_eq!(
        q.canonical_string(),
        expected,
        "canonical mismatch for {input:?}"
    );
}

fn assert_pod_accepts(input: Value) {
    let pod = pod_with_request("cpu", input.clone())
        .unwrap_or_else(|e| panic!("expected accept for {input:?}: {e}"));
    assert!(
        pod_request(&pod, "cpu").is_some(),
        "decoded pod is missing the cpu request entry for {input:?}"
    );
}

fn assert_pod_rejects(input: Value, reason: &str) {
    if let Ok(pod) = pod_with_request("cpu", input.clone()) {
        panic!(
            "expected reject for {input:?} ({reason}); decoder accepted with stored {:?}",
            pod_request(&pod, "cpu")
        );
    }
}

// ====================================================================
// Category 1: Canonical-form normalization on encode.
// ====================================================================

#[test]
fn millis_round_trip_preserves_canonical_form() {
    // `"1024m"` is already canonical — 1024 has no factor of 10 to
    // simplify away.
    assert_canonical("1024m", "1024m");
}

#[test]
fn thousand_millis_simplifies_to_one() {
    // `"1000m"` is integer one cpu — upstream emits `"1"`.
    assert_canonical("1000m", "1");
}

#[test]
fn half_simplifies_to_500m() {
    // `"0.5"` cpu encodes as `"500m"` in DecimalSI form.
    assert_canonical("0.5", "500m");
}

#[test]
fn binary_si_simplifies_within_format() {
    // 1024Mi == 1Gi; canonical BinarySI form is `"1Gi"`.
    assert_canonical("1024Mi", "1Gi");
}

#[test]
fn decimal_si_keeps_format_when_value_has_no_factor_of_1000() {
    // `"1024M"` is 1024 * 10^6 — 1024 has no trailing zeros so the
    // canonical DecimalSI form stays at the M suffix. Upstream emits
    // `"1024M"`, NOT `"1.024G"` (DecimalSI canonical mantissas are
    // always integer).
    assert_canonical("1024M", "1024M");
}

#[test]
fn decimal_exponent_strips_trailing_zeros_from_mantissa() {
    // `"2.5e3"` canonical: mantissa stripped of trailing zeros gives
    // 25, scale 2 — output is `"25e2"`.
    assert_canonical("2.5e3", "25e2");
}

#[test]
fn decimal_exponent_lowercases_e() {
    // `"1E6"` is DecimalExponent (suffix `E6` has digits). Canonical
    // output always uses lowercase `e`.
    assert_canonical("1E6", "1e6");
}

// ====================================================================
// Category 2: Suffix coverage — DecimalSI / BinarySI / DecimalExponent.
// Each suffix decodes via the Pod path and round-trips through the
// canonical encoder.
// ====================================================================

#[test]
fn decimal_si_suffixes_decode_and_canonicalise_to_self() {
    // n (10^-9), u (10^-6), m (10^-3), "" (10^0), k (10^3), M (10^6),
    // G (10^9), T (10^12), P (10^15), E (10^18).
    for s in [
        "5n", "10u", "100m", "1", "5k", "1M", "10G", "1T", "1P", "1E",
    ] {
        assert_pod_accepts(json!(s));
        assert_canonical(s, s);
    }
}

#[test]
fn binary_si_suffixes_decode_and_canonicalise_to_self() {
    for s in ["1Ki", "1Mi", "1Gi", "1Ti", "1Pi", "1Ei"] {
        assert_pod_accepts(json!(s));
        assert_canonical(s, s);
        assert_eq!(Quantity::parse(s).unwrap().format(), Format::BinarySI);
    }
}

#[test]
fn decimal_exponent_suffixes_decode() {
    // Direct canonical assertions cover the case-folding and trailing
    // zero strip; here we only need to confirm the decode path
    // accepts each.
    for s in ["1e0", "1e3", "1e-3", "1E6", "2.5e3"] {
        assert_pod_accepts(json!(s));
        let q = Quantity::parse(s).unwrap();
        assert_eq!(q.format(), Format::DecimalExponent);
    }
}

#[test]
fn fractional_decimal_decode() {
    for s in ["0.5", "1.5", "0.001", "100.5m"] {
        assert_pod_accepts(json!(s));
    }
}

// ====================================================================
// Category 3: Suffix equivalence.
// Two quantities written with different suffixes can carry the same
// numeric value; canonical form depends on the chosen Format, but
// `value_eq` compares numerically.
// ====================================================================

#[test]
fn ki_and_1024_are_value_equal() {
    let a = Quantity::parse("1Ki").unwrap();
    let b = Quantity::parse("1024").unwrap();
    assert!(a.value_eq(&b));
    // Canonical string still preserves format.
    assert_eq!(a.canonical_string(), "1Ki");
    assert_eq!(b.canonical_string(), "1024");
}

#[test]
fn one_mega_and_million_are_value_equal() {
    let a = Quantity::parse("1M").unwrap();
    let b = Quantity::parse("1000000").unwrap();
    assert!(a.value_eq(&b));
    // Both canonicalise to `"1M"` because upstream picks the largest
    // valid suffix that keeps the mantissa integer.
    assert_eq!(a.canonical_string(), "1M");
    assert_eq!(b.canonical_string(), "1M");
}

#[test]
fn one_gi_and_1024_mi_are_value_equal() {
    let a = Quantity::parse("1Gi").unwrap();
    let b = Quantity::parse("1024Mi").unwrap();
    assert!(a.value_eq(&b));
}

// ====================================================================
// Category 4: Zero quantity forms.
// Every zero form canonicalises to bare `"0"`, regardless of input
// suffix.
// ====================================================================

#[test]
fn zero_forms_decode() {
    for s in ["0", "0.0", "0Ki", "0m", "0n", "0Mi", "0e0", "-0", "-0m"] {
        assert_pod_accepts(json!(s));
    }
}

#[test]
fn zero_integer_decodes_to_zero_string() {
    // Numeric `0` arrives via the `serde_json::Value::Number` arm
    // and is rendered as the canonical string `"0"`.
    let pod = pod_with_request("cpu", json!(0)).expect("numeric 0 decodes");
    assert_eq!(pod_request(&pod, "cpu").as_deref(), Some("0"));
}

#[test]
fn zero_forms_canonicalise_to_bare_zero() {
    for s in [
        "0", "0.0", "0Ki", "0m", "0n", "0Mi", "0e0", "-0", "-0m", "-0e0",
    ] {
        assert_canonical(s, "0");
    }
}

// ====================================================================
// Category 5: Format preservation across round-trip.
// Upstream stores the chosen Format on the Quantity and emits the
// simplified canonical form within that Format.
// ====================================================================

#[test]
fn format_is_preserved_across_parse() {
    assert_eq!(Quantity::parse("1").unwrap().format(), Format::DecimalSI);
    assert_eq!(Quantity::parse("1Ki").unwrap().format(), Format::BinarySI);
    assert_eq!(
        Quantity::parse("1e3").unwrap().format(),
        Format::DecimalExponent
    );
    // Bare `E` is the DecimalSI exa suffix, not DecimalExponent.
    assert_eq!(Quantity::parse("1E").unwrap().format(), Format::DecimalSI);
}

#[test]
fn binary_si_simplifies_within_binary_format() {
    // 2048Mi simplifies to 2Gi.
    assert_canonical("2048Mi", "2Gi");
}

// ====================================================================
// Category 6: Negative quantities.
// The Quantity type itself accepts negatives; ResourceList non-negative
// rejection lives in validation, not in decode.
// ====================================================================

#[test]
fn negative_quantities_decode() {
    for s in ["-100m", "-1Ki", "-1", "-2.5", "-1e3"] {
        assert_pod_accepts(json!(s));
    }
}

#[test]
fn negative_quantities_canonicalise() {
    // Inputs that are already canonical round-trip to themselves.
    assert_canonical("-100m", "-100m");
    assert_canonical("-1Ki", "-1Ki");
    assert_canonical("-1", "-1");
    assert_canonical("-1e3", "-1e3");
    // `-2.5` is DecimalSI with fractional scale -1; canonical form
    // shifts mantissa down to the milli scale: `-2500m`.
    assert_canonical("-2.5", "-2500m");
}

#[test]
fn negative_numeric_decodes() {
    let pod = pod_with_request("cpu", json!(-1)).expect("numeric -1 decodes");
    assert_eq!(pod_request(&pod, "cpu").as_deref(), Some("-1"));
}

// ====================================================================
// Category 7: Boundary / very large / very small values.
// ====================================================================

#[test]
fn boundary_values_decode() {
    for s in [
        "8E",                  // 8 * 10^18, fits in i128 comfortably
        "1Ei",                 // 2^60
        "1n",                  // smallest practical decimal SI
        "0.000000001",         // sub-nano fractional input
        "9223372036854775807", // i64::MAX as bare integer
    ] {
        assert_pod_accepts(json!(s));
    }
}

#[test]
fn boundary_values_canonicalise() {
    assert_canonical("8E", "8E");
    assert_canonical("1Ei", "1Ei");
    assert_canonical("1n", "1n");
    // `"0.000000001"` is the literal 1 * 10^-9 — canonical form is
    // `"1n"`.
    assert_canonical("0.000000001", "1n");
    assert_canonical("9223372036854775807", "9223372036854775807");
}

#[test]
fn very_large_numeric_decodes() {
    let pod = pod_with_request("cpu", json!(i64::MAX)).expect("i64::MAX decodes");
    assert_eq!(
        pod_request(&pod, "cpu").as_deref(),
        Some(i64::MAX.to_string().as_str())
    );
}

// ====================================================================
// Category 8: Error cases — invalid quantities.
// The decode path (`deserialize_quantity_map`) routes every string
// through `Quantity::parse`, so every upstream-rejected input is also
// rejected here.
// ====================================================================

#[test]
fn empty_string_is_rejected() {
    assert_pod_rejects(json!(""), "empty");
    assert!(Quantity::parse("").is_err());
}

#[test]
fn whitespace_only_is_rejected() {
    assert_pod_rejects(json!("   "), "whitespace only");
    assert!(Quantity::parse("   ").is_err());
}

#[test]
fn only_suffix_is_rejected() {
    assert_pod_rejects(json!("Ki"), "suffix without number");
    assert!(Quantity::parse("Ki").is_err());
}

#[test]
fn unknown_suffix_is_rejected() {
    assert_pod_rejects(json!("1Q"), "unknown suffix Q");
    assert!(Quantity::parse("1Q").is_err());
}

#[test]
fn trailing_garbage_is_rejected() {
    assert_pod_rejects(json!("1ki "), "trailing whitespace, wrong-case suffix");
    assert!(Quantity::parse("1ki ").is_err());
    assert!(Quantity::parse(" 1").is_err());
    assert!(Quantity::parse("1 ").is_err());
}

#[test]
fn mixed_letters_are_rejected() {
    // Upstream accepts `Ki` (binary kilobyte) but not `KiB`.
    assert_pod_rejects(json!("1KiB"), "trailing B after Ki");
    assert!(Quantity::parse("1KiB").is_err());
}

#[test]
fn malformed_decimal_is_rejected() {
    // Upstream's documented invalid corpus: double-dot, `+` mid-number,
    // exponent with no digits, lowercase `mi`, etc.
    for s in ["1.1.M", "1+1.0M", "0.1mi", "-3.01e-", ".5i", "-3.01i", "1i"] {
        assert!(
            Quantity::parse(s).is_err(),
            "expected reject for {s:?} but it parsed"
        );
    }
}

#[test]
fn boolean_value_is_rejected_today() {
    // The decoder's `other` arm rejects anything that isn't a string,
    // number, or null. Pin so a future parser refactor doesn't
    // regress.
    let err = serde_json::from_value::<Pod>(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p", "namespace": "default" },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": { "requests": { "cpu": true } }
            }]
        }
    }))
    .expect_err("Quantity must reject boolean");
    let msg = err.to_string();
    assert!(
        msg.contains("Quantity value must be a string or number"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn array_value_is_rejected_today() {
    let err = serde_json::from_value::<Pod>(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p", "namespace": "default" },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": { "requests": { "cpu": [1, 2] } }
            }]
        }
    }))
    .expect_err("Quantity must reject array");
    let msg = err.to_string();
    assert!(
        msg.contains("Quantity value must be a string or number"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn null_quantity_value_is_dropped() {
    // The decoder treats a `null` value as "skip this key" — pin it.
    let pod = pod_with_request("cpu", json!(null)).expect("null quantity must decode");
    assert!(
        pod_request(&pod, "cpu").is_none(),
        "null Quantity should be dropped from the map, got {:?}",
        pod_request(&pod, "cpu")
    );
}
