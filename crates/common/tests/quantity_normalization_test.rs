//! Upstream-parity corpus for `resource.Quantity` canonical-form
//! normalization, ported from
//! `staging/src/k8s.io/apimachinery/pkg/api/resource/quantity_test.go` in
//! the Kubernetes Go tree.
//!
//! Background
//! ----------
//! Upstream `resource.Quantity` is a parsed numeric type with a chosen
//! Format (`DecimalSI`, `BinarySI`, `DecimalExponent`). On encode it
//! emits the canonical-form string for the stored Format, simplifying
//! where possible (`"1024Mi"` -> `"1Gi"`, `"1000m"` -> `"1"`) and
//! always producing `"0"` for the zero value regardless of the suffix
//! used on input.
//!
//! Rusternetes currently stores every quantity as a raw `String` value
//! inside `ResourceRequirements::{requests,limits}` (see
//! `deserialize_quantity_map` in `crates/common/src/types.rs`). There
//! is no parsing, no Format tracking, and no canonical-form encoder.
//! As a result, every case that requires _normalization on output_
//! cannot pass today and is gated with
//!   `#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]`.
//! Those ignored tests stand as the spec for a future
//! normalization-aware `Quantity` newtype.
//!
//! Categories covered (matches the unit brief):
//!   1. Canonical-form normalization on encode (all ignored)
//!   2. Suffix coverage — DecimalSI / BinarySI / DecimalExponent decode
//!   3. Suffix equivalence (`"1Ki" == "1024"`) — ignored, requires parsing
//!   4. Zero quantity forms — partially ignored (canonical `"0"` output)
//!   5. Format preservation across round-trip — ignored, requires Format tracking
//!   6. Negative quantities decode (Quantity type accepts; ResourceList
//!      rejection lives in validation, not decode)
//!   7. Very large / very small / boundary values decode
//!   8. Error cases — bad suffix / empty / mixed letters — ignored,
//!      current decoder accepts any string
//!
//! Anchor for upstream cross-reference:
//!   k8s.io/apimachinery/pkg/api/resource/quantity_test.go
//!   TestQuantityParse, TestQuantityCanonicalize, TestQuantityRoundTrip
//!
//! Note: this file targets the `ResourceRequirements`-via-`Pod` decode
//! path (the only externally observable Quantity round-trip rusternetes
//! ships today). When the `Quantity` newtype lands the ignored tests
//! should be rewritten to call its parser directly instead of routing
//! through a Pod body, and un-ignored as the implementation supports
//! each category.

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

fn request(pod: &Pod, key: &str) -> Option<String> {
    pod.spec.as_ref()?.containers[0]
        .resources
        .as_ref()?
        .requests
        .as_ref()?
        .get(key)
        .cloned()
}

fn assert_accepts(input: Value) {
    let pod = pod_with_request("cpu", input.clone())
        .unwrap_or_else(|e| panic!("expected accept for {input:?}: {e}"));
    assert!(
        request(&pod, "cpu").is_some(),
        "decoded pod is missing the cpu request entry for {input:?}"
    );
}

fn assert_round_trip(input: Value, expected: &str) {
    let pod = pod_with_request("cpu", input.clone())
        .unwrap_or_else(|e| panic!("expected accept for {input:?}: {e}"));
    let got = request(&pod, "cpu").unwrap_or_else(|| panic!("missing cpu for {input:?}"));
    assert_eq!(got, expected, "round-trip mismatch for {input:?}");
}

// ====================================================================
// Category 2: Suffix coverage — DecimalSI / BinarySI / DecimalExponent.
// All of these strings must decode without error today.
// We verify the stored string equals the input (no normalization).
// ====================================================================

#[test]
fn decimal_si_suffixes_decode() {
    // n (10^-9), u (10^-6), m (10^-3), "" (10^0), k (10^3), M (10^6),
    // G (10^9), T (10^12), P (10^15), E (10^18).
    for s in [
        "5n", "10u", "100m", "1", "5k", "1M", "10G", "1T", "1P", "1E",
    ] {
        assert_round_trip(json!(s), s);
    }
}

#[test]
fn binary_si_suffixes_decode() {
    for s in ["1Ki", "1Mi", "1Gi", "1Ti", "1Pi", "1Ei"] {
        assert_round_trip(json!(s), s);
    }
}

#[test]
fn decimal_exponent_suffixes_decode() {
    for s in ["1e0", "1e3", "1e-3", "1E6", "2.5e3"] {
        assert_round_trip(json!(s), s);
    }
}

#[test]
fn fractional_decimal_decode() {
    for s in ["0.5", "1.5", "0.001", "100.5m"] {
        // 100.5m is unusual but upstream accepts fractional within suffix.
        // We assert acceptance; some forms get canonicalised upstream
        // (covered by ignored category-1 tests).
        assert_accepts(json!(s));
    }
}

// ====================================================================
// Category 4: Zero quantity forms.
// All decode without error; current raw-string decoder preserves the
// suffix. Upstream canonicalises every zero form to bare `"0"`.
// ====================================================================

#[test]
fn zero_forms_decode() {
    for s in ["0", "0.0", "0Ki", "0m", "0n", "0Mi", "0e0", "-0", "-0m"] {
        assert_accepts(json!(s));
    }
}

#[test]
fn zero_integer_decodes_to_zero_string() {
    // Numeric `0` is the column-942 case already pinned by
    // `quantity_decode_test.rs`, mirrored here so the canonical-form
    // corpus stands on its own.
    assert_round_trip(json!(0), "0");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn zero_forms_canonicalise_to_bare_zero() {
    // Upstream: every zero form encodes back as `"0"`.
    // Current rusternetes: stores raw input. `"0Ki"` stays `"0Ki"`.
    for s in ["0.0", "0Ki", "0m", "0n", "0Mi", "0e0"] {
        assert_round_trip(json!(s), "0");
    }
}

// ====================================================================
// Category 6: Negative quantities.
// The Quantity type itself accepts negatives; ResourceList non-negative
// rejection lives in validation, not in decode.
// ====================================================================

#[test]
fn negative_quantities_decode() {
    for s in ["-100m", "-1Ki", "-1", "-2.5", "-1e3"] {
        assert_round_trip(json!(s), s);
    }
}

#[test]
fn negative_numeric_decodes() {
    assert_round_trip(json!(-1), "-1");
    assert_round_trip(json!(-1.5), "-1.5");
}

// ====================================================================
// Category 7: Very large / very small / boundary values decode.
// Tests upstream pins on the i64 boundary near the SI prefix limits.
// ====================================================================

#[test]
fn boundary_values_decode() {
    // ~ i64::MAX is ~9.2 * 10^18; both 8E and 1Ei comfortably fit.
    // 1Ei = 2^60 ~= 1.15 * 10^18.
    for s in [
        "8E",                  // 8 * 10^18
        "1Ei",                 // 2^60
        "1n",                  // smallest practical decimal SI
        "0.000000001",         // sub-nano fractional
        "9223372036854775807", // i64::MAX as bare integer
    ] {
        assert_round_trip(json!(s), s);
    }
}

#[test]
fn very_large_numeric_decodes() {
    // Bare numeric ints up to i64::MAX must decode through the
    // serde_json::Value::Number arm and become the canonical string.
    assert_round_trip(json!(i64::MAX), &i64::MAX.to_string());
}

// ====================================================================
// Category 1: Canonical-form normalization on ENCODE.
// All ignored — rusternetes stores raw strings, no Format-aware
// encoder exists. These tests are the spec for a future normalising
// Quantity newtype.
// ====================================================================

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn millis_round_trip_preserves_canonical_form() {
    // `"1024m"` is already canonical — must stay `"1024m"` after round-trip.
    assert_round_trip(json!("1024m"), "1024m");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn thousand_millis_simplifies_to_one() {
    // `"1000m"` is the integer one cpu — upstream emits `"1"`.
    assert_round_trip(json!("1000m"), "1");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn half_simplifies_to_500m() {
    // `"0.5"` cpu encodes as `"500m"` in DecimalSI form.
    assert_round_trip(json!("0.5"), "500m");
}

// ====================================================================
// Category 3: Suffix-equivalence.
// `"1Ki"` and `"1024"` are the same value; canonical form depends on
// the chosen Format. All ignored — current decoder preserves raw input.
// ====================================================================

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn ki_equals_1024_decimal_si() {
    // If we asked for DecimalSI on output, "1Ki" round-trips to "1024".
    assert_round_trip(json!("1Ki"), "1024");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn one_mega_equals_million_decimal_si() {
    // `"1M"` -> `"1000000"` when output Format is DecimalSI bare.
    assert_round_trip(json!("1M"), "1000000");
}

// ====================================================================
// Category 5: Format preservation across round-trip.
// Upstream stores the chosen Format on the Quantity and emits the
// simplified canonical form within that Format.
// ====================================================================

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn binary_si_simplifies_within_format() {
    // 1024Mi == 1Gi; canonical BinarySI form is `"1Gi"`.
    assert_round_trip(json!("1024Mi"), "1Gi");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn decimal_si_simplifies_within_format() {
    // 1024M stays DecimalSI on output. Upstream emits `"1.024G"`.
    assert_round_trip(json!("1024M"), "1.024G");
}

// ====================================================================
// Category 8: Error cases — invalid quantities.
// Upstream `resource.ParseQuantity` rejects all of these. The current
// rusternetes decoder treats anything that lands in
// `serde_json::Value::String` as valid (it never parses the value), so
// every test below is ignored until a parser is wired in.
// ====================================================================

fn assert_rejects(input: Value, reason: &str) {
    // Helper so the un-ignored impl path is trivial: when the parser
    // lands, drop the #[ignore] and these calls will start enforcing.
    if let Ok(pod) = pod_with_request("cpu", input.clone()) {
        panic!(
            "expected reject for {input:?} ({reason}); decoder accepted with stored {:?}",
            request(&pod, "cpu")
        );
    }
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn empty_string_is_rejected() {
    assert_rejects(json!(""), "empty");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn whitespace_only_is_rejected() {
    assert_rejects(json!("   "), "whitespace only");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn only_suffix_is_rejected() {
    assert_rejects(json!("Ki"), "suffix without number");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn unknown_suffix_is_rejected() {
    assert_rejects(json!("1Q"), "unknown suffix Q");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn trailing_garbage_is_rejected() {
    assert_rejects(json!("1ki "), "trailing whitespace, wrong-case suffix");
}

#[test]
#[ignore = "blocked on issue #TBD: Quantity stored as raw String without canonical normalization"]
fn mixed_letters_are_rejected() {
    // Upstream accepts `Ki` (binary kilobyte) but not `KiB`.
    assert_rejects(json!("1KiB"), "trailing B after Ki");
}

#[test]
fn boolean_value_is_rejected_today() {
    // The current decoder's `other` arm rejects anything that isn't a
    // string, number, or null. This is one error case the decoder
    // already enforces — pin it so a future parser refactor doesn't
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
        request(&pod, "cpu").is_none(),
        "null Quantity should be dropped from the map, got {:?}",
        request(&pod, "cpu")
    );
}
