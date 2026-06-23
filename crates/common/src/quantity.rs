//! `Quantity` newtype matching Kubernetes `apimachinery`
//! `pkg/api/resource/quantity.go`.
//!
//! Provides upstream-parity parsing and canonical-form encoding for
//! resource quantity values such as `"100m"`, `"1Gi"`, `"2.5e3"`.
//!
//! Internal representation is `mantissa * 10^scale` for all three
//! formats. `BinarySI` inputs are converted to that representation on
//! parse (multiplying mantissa by `2^suffix_exp`) and re-encoded by
//! factoring out powers of 1024 on canonicalisation.

use std::fmt;

/// The chosen output format for a [`Quantity`]. Mirrors upstream
/// `resource.Format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    /// `n`, `u`, `m`, `""`, `k`, `M`, `G`, `T`, `P`, `E` (base 10).
    DecimalSI,
    /// `Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei` (base 2, exponents are
    /// multiples of 10 with suffixes spaced every factor of 1024).
    BinarySI,
    /// `e<int>` / `E<int>` with arbitrary signed integer exponent.
    DecimalExponent,
}

/// Parsed resource quantity. Construct via [`Quantity::parse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantity {
    mantissa: i128,
    scale: i32,
    format: Format,
}

/// Error returned by [`Quantity::parse`] for any input that upstream
/// `resource.ParseQuantity` rejects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseQuantityError(String);

impl ParseQuantityError {
    fn new(message: impl Into<String>) -> Self {
        ParseQuantityError(message.into())
    }
}

impl fmt::Display for ParseQuantityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseQuantityError {}

impl Quantity {
    /// Parse a quantity string. Rejects empty input, whitespace,
    /// unknown suffixes, mixed suffix letters and bare suffixes with
    /// no mantissa.
    pub fn parse(input: &str) -> Result<Quantity, ParseQuantityError> {
        if input.is_empty() {
            return Err(ParseQuantityError::new(
                "quantities must match the regular expression '^[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+|[numkMGTPE]|Ki|Mi|Gi|Ti|Pi|Ei)?$': empty string",
            ));
        }
        let bytes = input.as_bytes();
        let mut pos = 0usize;

        // Optional sign.
        let mut sign: i128 = 1;
        match bytes[pos] {
            b'+' => pos += 1,
            b'-' => {
                sign = -1;
                pos += 1;
            }
            _ => {}
        }

        // Whole-digit run.
        let whole_start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        let whole_end = pos;

        // Optional fractional part introduced by `.`.
        let mut frac_start = pos;
        let mut frac_end = pos;
        if pos < bytes.len() && bytes[pos] == b'.' {
            pos += 1;
            frac_start = pos;
            while pos < bytes.len() && bytes[pos].is_ascii_digit() {
                pos += 1;
            }
            frac_end = pos;
        }

        if whole_start == whole_end && frac_start == frac_end {
            return Err(ParseQuantityError::new(format!(
                "quantity must have at least one digit: {input:?}"
            )));
        }

        let whole = &input[whole_start..whole_end];
        let frac = &input[frac_start..frac_end];
        let suffix = &input[pos..];

        // Build mantissa from concatenated whole+frac digits and parse
        // as `i128`. Empty whole becomes `0` to support `".5"` shape
        // (though upstream rejects bare `".5i"`; the suffix check
        // below catches that).
        let mut combined = String::with_capacity(whole.len() + frac.len());
        if whole.is_empty() {
            combined.push('0');
        } else {
            combined.push_str(whole);
        }
        combined.push_str(frac);
        let mut mantissa = combined.parse::<i128>().map_err(|_| {
            ParseQuantityError::new(format!("quantity mantissa overflows i128: {input:?}"))
        })?;
        mantissa *= sign;
        let mut scale = -(frac.len() as i32);

        let kind = interpret_suffix(suffix).ok_or_else(|| {
            ParseQuantityError::new(format!("unable to parse quantity suffix in {input:?}"))
        })?;
        let format = match kind {
            SuffixKind::DecimalSI(e) => {
                scale = scale.checked_add(e).ok_or_else(|| {
                    ParseQuantityError::new(format!("quantity scale overflow: {input:?}"))
                })?;
                Format::DecimalSI
            }
            SuffixKind::DecimalExponent(e) => {
                scale = scale.checked_add(e).ok_or_else(|| {
                    ParseQuantityError::new(format!("quantity scale overflow: {input:?}"))
                })?;
                Format::DecimalExponent
            }
            SuffixKind::BinarySI(e) => {
                if !(0..=120).contains(&e) {
                    return Err(ParseQuantityError::new(format!(
                        "invalid binary exponent in {input:?}"
                    )));
                }
                let mul = 1i128.checked_shl(e as u32).ok_or_else(|| {
                    ParseQuantityError::new(format!("binary multiplier overflow: {input:?}"))
                })?;
                mantissa = mantissa.checked_mul(mul).ok_or_else(|| {
                    ParseQuantityError::new(format!("quantity value overflow: {input:?}"))
                })?;
                Format::BinarySI
            }
        };

        Ok(Quantity {
            mantissa,
            scale,
            format,
        })
    }

    /// Format. Preserved from parsed suffix.
    pub fn format(&self) -> Format {
        self.format
    }

    /// True when the underlying value is zero regardless of format /
    /// scale.
    pub fn is_zero(&self) -> bool {
        self.mantissa == 0
    }

    /// Compare two quantities by their numeric value, ignoring
    /// format. Mirrors upstream `Quantity.Cmp`.
    pub fn cmp_value(&self, other: &Quantity) -> std::cmp::Ordering {
        // a*10^sa  vs  b*10^sb
        let (a, sa) = (self.mantissa, self.scale);
        let (b, sb) = (other.mantissa, other.scale);
        if sa == sb {
            return a.cmp(&b);
        }
        // Bring to common scale = min(sa, sb).
        let common = sa.min(sb);
        let shift_a = (sa - common) as u32;
        let shift_b = (sb - common) as u32;
        let a_scaled =
            a.checked_mul(10i128.pow(shift_a))
                .unwrap_or(if a < 0 { i128::MIN } else { i128::MAX });
        let b_scaled =
            b.checked_mul(10i128.pow(shift_b))
                .unwrap_or(if b < 0 { i128::MIN } else { i128::MAX });
        a_scaled.cmp(&b_scaled)
    }

    /// Two quantities are value-equal when they represent the same
    /// numeric value, regardless of format.
    pub fn value_eq(&self, other: &Quantity) -> bool {
        self.cmp_value(other) == std::cmp::Ordering::Equal
    }

    /// Subtract `other` from `self`, returning a new `Quantity`.
    ///
    /// The result inherits the format of `self` (the "capacity" side)
    /// so that `8Gi - 1Gi` canonicalises as `7Gi`.  Returns `None` on
    /// overflow (i128 mantissa).
    ///
    /// Used by `NodeController` to compute `allocatable = capacity - reserved`.
    pub fn sub(&self, other: &Quantity) -> Option<Quantity> {
        // Bring both operands to a common scale = min(self.scale, other.scale)
        // so the mantissas can be subtracted directly.
        let common = self.scale.min(other.scale);
        let shift_a = (self.scale - common) as u32;
        let shift_b = (other.scale - common) as u32;
        let a = self.mantissa.checked_mul(10i128.checked_pow(shift_a)?)?;
        let b = other.mantissa.checked_mul(10i128.checked_pow(shift_b)?)?;
        let result = a.checked_sub(b)?;
        Some(Quantity {
            mantissa: result,
            scale: common,
            format: self.format,
        })
    }

    /// True when this quantity represents a whole-number value (no fractional
    /// part). The internal value is `mantissa * 10^scale`, so a non-negative
    /// scale is always integral; a negative scale is integral only when the
    /// mantissa is divisible by `10^(-scale)`.
    ///
    /// Mirrors the integer-resource check in upstream
    /// `ValidateResourceQuantityValue` (`value.MilliValue()%1000 == 0`):
    /// extended (integer) resources must not carry a fractional quantity.
    pub fn is_integer(&self) -> bool {
        if self.scale >= 0 {
            return true;
        }
        let divisor = 10i128.checked_pow((-self.scale) as u32);
        match divisor {
            Some(d) => self.mantissa % d == 0,
            // Scale so negative that 10^(-scale) overflows i128: the magnitude
            // is far below 1, so it cannot be a non-zero integer.
            None => self.mantissa == 0,
        }
    }

    /// True when this quantity represents a negative value. Used to clamp
    /// `allocatable = capacity - reserved` to zero when a node reserves more
    /// than its capacity (upstream `getNodeAllocatableAbsolute` clamps to 0).
    pub fn is_negative(&self) -> bool {
        self.mantissa < 0
    }

    /// Value of `ceil(self / 10^target_scale)` as `i128`. Mirrors upstream
    /// `Quantity.ScaledValue(scale)` (rounding up, away from zero). The
    /// internal representation is `mantissa * 10^self.scale`, so this is
    /// `ceil(mantissa * 10^(self.scale - target_scale))`.
    fn scaled_value(&self, target_scale: i32) -> i128 {
        let delta = self.scale - target_scale;
        match delta.cmp(&0) {
            std::cmp::Ordering::Equal => self.mantissa,
            std::cmp::Ordering::Greater => {
                // Multiply up — exact, no rounding needed.
                self.mantissa
                    .checked_mul(10i128.pow(delta as u32))
                    .unwrap_or(if self.mantissa < 0 {
                        i128::MIN
                    } else {
                        i128::MAX
                    })
            }
            std::cmp::Ordering::Less => {
                // Divide by 10^(-delta), rounding up away from zero to match
                // upstream `ScaledValue` ceiling semantics.
                let divisor = 10i128.pow((-delta) as u32);
                let q = self.mantissa / divisor;
                let r = self.mantissa % divisor;
                if r > 0 {
                    q + 1
                } else if r < 0 {
                    q - 1
                } else {
                    q
                }
            }
        }
    }

    /// Unscaled value rounded up to the nearest integer away from zero.
    /// Mirrors upstream `Quantity.Value()`.
    pub fn value(&self) -> i128 {
        self.scaled_value(0)
    }

    /// `ceil(self * 1000)`. Mirrors upstream `Quantity.MilliValue()`.
    pub fn milli_value(&self) -> i128 {
        self.scaled_value(-3)
    }

    /// Canonical-form string for this quantity. Matches upstream
    /// `Quantity.String()`.
    pub fn canonical_string(&self) -> String {
        if self.mantissa == 0 {
            return "0".to_string();
        }
        match self.format {
            Format::DecimalSI => canonical_decimal_si(self.mantissa, self.scale),
            Format::DecimalExponent => canonical_decimal_exponent(self.mantissa, self.scale),
            Format::BinarySI => canonical_binary_si(self.mantissa, self.scale),
        }
    }
}

impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical_string())
    }
}

/// Strip trailing factors of 10 from `mantissa`, lifting the scale
/// the same number of steps. Leaves the numeric value unchanged.
fn strip_trailing_zeros(mut mantissa: i128, mut scale: i32) -> (i128, i32) {
    if mantissa == 0 {
        return (0, scale);
    }
    while mantissa % 10 == 0 {
        mantissa /= 10;
        scale = scale.saturating_add(1);
    }
    (mantissa, scale)
}

fn canonical_decimal_si(mantissa: i128, scale: i32) -> String {
    let (m, s) = strip_trailing_zeros(mantissa, scale);
    const VALID: [i32; 10] = [-9, -6, -3, 0, 3, 6, 9, 12, 15, 18];
    // Pick the largest valid suffix-scale `t` with `t <= s` and shift
    // mantissa up by `10^(s - t)` so the value is preserved.
    if let Some(t) = VALID.iter().rev().find(|&&v| v <= s).copied() {
        let shift = (s - t) as u32;
        let factor = 10i128
            .checked_pow(shift)
            .expect("decimal SI shift factor overflow");
        let final_m = m.checked_mul(factor).expect("decimal SI mantissa overflow");
        format!("{}{}", final_m, decimal_si_suffix(t))
    } else if let Some(top) = VALID.last().copied().filter(|&top| s > top) {
        // scale > 18: clamp to E and absorb the excess into mantissa.
        let shift = (s - top) as u32;
        let factor = 10i128
            .checked_pow(shift)
            .expect("decimal SI shift factor overflow");
        let final_m = m.checked_mul(factor).expect("decimal SI mantissa overflow");
        format!("{}{}", final_m, decimal_si_suffix(top))
    } else {
        // scale < -9: try to divide mantissa cleanly down to scale=-9;
        // otherwise emit as DecimalExponent (no valid SI suffix).
        let target = -9i32;
        let shift = (target - s) as u32;
        let factor = 10i128
            .checked_pow(shift)
            .expect("decimal SI shift factor overflow");
        if m % factor == 0 {
            format!("{}{}", m / factor, decimal_si_suffix(target))
        } else {
            canonical_decimal_exponent(mantissa, scale)
        }
    }
}

fn decimal_si_suffix(scale: i32) -> &'static str {
    match scale {
        -9 => "n",
        -6 => "u",
        -3 => "m",
        0 => "",
        3 => "k",
        6 => "M",
        9 => "G",
        12 => "T",
        15 => "P",
        18 => "E",
        _ => unreachable!("decimal_si_suffix called with invalid scale {scale}"),
    }
}

fn canonical_decimal_exponent(mantissa: i128, scale: i32) -> String {
    let (m, s) = strip_trailing_zeros(mantissa, scale);
    format!("{m}e{s}")
}

fn canonical_binary_si(mantissa: i128, scale: i32) -> String {
    // BinarySI must represent an integer value. If the parsed
    // mantissa/scale cannot be reduced to an integer, fall back to
    // DecimalSI canonical form.
    let value = match scale.cmp(&0) {
        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
            let factor = match 10i128.checked_pow(scale as u32) {
                Some(f) => f,
                None => return canonical_decimal_si(mantissa, scale),
            };
            match mantissa.checked_mul(factor) {
                Some(v) => v,
                None => return canonical_decimal_si(mantissa, scale),
            }
        }
        std::cmp::Ordering::Less => {
            let factor = match 10i128.checked_pow((-scale) as u32) {
                Some(f) => f,
                None => return canonical_decimal_si(mantissa, scale),
            };
            if mantissa % factor != 0 {
                return canonical_decimal_si(mantissa, scale);
            }
            mantissa / factor
        }
    };

    if value.unsigned_abs() < 1024 {
        return canonical_decimal_si(mantissa, scale);
    }

    let mut v = value;
    let mut k: u32 = 0;
    while k < 6 && v % 1024 == 0 {
        v /= 1024;
        k += 1;
    }
    if k == 0 {
        return canonical_decimal_si(mantissa, scale);
    }
    let suffix = binary_si_suffix(k);
    format!("{v}{suffix}")
}

fn binary_si_suffix(k: u32) -> &'static str {
    match k {
        1 => "Ki",
        2 => "Mi",
        3 => "Gi",
        4 => "Ti",
        5 => "Pi",
        6 => "Ei",
        _ => unreachable!("binary_si_suffix called with invalid k {k}"),
    }
}

enum SuffixKind {
    DecimalSI(i32),
    DecimalExponent(i32),
    BinarySI(i32),
}

fn interpret_suffix(suffix: &str) -> Option<SuffixKind> {
    if suffix.is_empty() {
        return Some(SuffixKind::DecimalSI(0));
    }
    // DecimalExponent: `e`/`E` followed by an optional sign and at
    // least one digit. The rest must parse as a signed integer; if
    // not, fall through to the static suffix table (so `Ei` / `E`
    // resolve correctly).
    if let Some(rest) = suffix.strip_prefix(['e', 'E']) {
        if !rest.is_empty() {
            if let Ok(exp) = rest.parse::<i32>() {
                return Some(SuffixKind::DecimalExponent(exp));
            }
        }
    }
    if suffix == "E" {
        return Some(SuffixKind::DecimalSI(18));
    }
    match suffix {
        "n" => Some(SuffixKind::DecimalSI(-9)),
        "u" => Some(SuffixKind::DecimalSI(-6)),
        "m" => Some(SuffixKind::DecimalSI(-3)),
        "k" => Some(SuffixKind::DecimalSI(3)),
        "M" => Some(SuffixKind::DecimalSI(6)),
        "G" => Some(SuffixKind::DecimalSI(9)),
        "T" => Some(SuffixKind::DecimalSI(12)),
        "P" => Some(SuffixKind::DecimalSI(15)),
        "Ki" => Some(SuffixKind::BinarySI(10)),
        "Mi" => Some(SuffixKind::BinarySI(20)),
        "Gi" => Some(SuffixKind::BinarySI(30)),
        "Ti" => Some(SuffixKind::BinarySI(40)),
        "Pi" => Some(SuffixKind::BinarySI(50)),
        "Ei" => Some(SuffixKind::BinarySI(60)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Quantity {
        Quantity::parse(s).unwrap_or_else(|e| panic!("parse({s:?}) failed: {e}"))
    }

    #[test]
    fn decimal_si_suffix_table_canonical_self() {
        for s in [
            "5n", "10u", "100m", "1", "5k", "1M", "10G", "1T", "1P", "1E",
        ] {
            assert_eq!(parse(s).canonical_string(), s);
        }
    }

    #[test]
    fn binary_si_suffix_table_canonical_self() {
        for s in ["1Ki", "1Mi", "1Gi", "1Ti", "1Pi", "1Ei"] {
            assert_eq!(parse(s).canonical_string(), s);
        }
    }

    #[test]
    fn decimal_exponent_canonical_form() {
        assert_eq!(parse("1e0").canonical_string(), "1e0");
        assert_eq!(parse("1e3").canonical_string(), "1e3");
        assert_eq!(parse("1e-3").canonical_string(), "1e-3");
        // Upstream lowercases `E` -> `e` in DecimalExponent canonical.
        assert_eq!(parse("1E6").canonical_string(), "1e6");
        // Mantissa is canonicalised by stripping trailing zeros, so
        // `2.5e3` becomes `25e2`.
        assert_eq!(parse("2.5e3").canonical_string(), "25e2");
    }

    #[test]
    fn fractional_decimal_simplifies_to_millis() {
        assert_eq!(parse("0.5").canonical_string(), "500m");
        assert_eq!(parse("1.5").canonical_string(), "1500m");
        assert_eq!(parse("0.001").canonical_string(), "1m");
    }

    #[test]
    fn thousand_millis_simplifies_to_one() {
        assert_eq!(parse("1000m").canonical_string(), "1");
    }

    #[test]
    fn millis_with_remainder_stays_in_milli_scale() {
        assert_eq!(parse("1024m").canonical_string(), "1024m");
    }

    #[test]
    fn binary_si_factors_out_1024s() {
        // 1024Mi == 1Gi.
        assert_eq!(parse("1024Mi").canonical_string(), "1Gi");
        // 2048Mi == 2Gi.
        assert_eq!(parse("2048Mi").canonical_string(), "2Gi");
    }

    #[test]
    fn binary_si_keeps_format_when_value_is_not_power_of_1024() {
        // 100Mi has 100 left over after factoring 1024^2.
        assert_eq!(parse("100Mi").canonical_string(), "100Mi");
    }

    #[test]
    fn decimal_si_keeps_format_when_value_is_not_power_of_1000() {
        // 1024M is 1024 * 10^6, the canonical DecimalSI form because
        // 1024 has no trailing zeros.
        assert_eq!(parse("1024M").canonical_string(), "1024M");
    }

    #[test]
    fn zero_in_any_format_is_bare_zero() {
        for s in ["0", "0.0", "0Ki", "0m", "0n", "0Mi", "0e0", "-0", "-0m"] {
            assert_eq!(parse(s).canonical_string(), "0", "for {s:?}");
        }
    }

    #[test]
    fn negative_values_canonical() {
        assert_eq!(parse("-100m").canonical_string(), "-100m");
        assert_eq!(parse("-1Ki").canonical_string(), "-1Ki");
        assert_eq!(parse("-1").canonical_string(), "-1");
        assert_eq!(parse("-2.5").canonical_string(), "-2500m");
        assert_eq!(parse("-1e3").canonical_string(), "-1e3");
    }

    #[test]
    fn value_equality_across_formats() {
        // "1Ki" parses to the same numeric value as "1024".
        assert!(parse("1Ki").value_eq(&parse("1024")));
        // "1M" == "1000000".
        assert!(parse("1M").value_eq(&parse("1000000")));
        // "1024Mi" == "1Gi".
        assert!(parse("1024Mi").value_eq(&parse("1Gi")));
    }

    #[test]
    fn very_small_canonicalises_to_nanos() {
        assert_eq!(parse("0.000000001").canonical_string(), "1n");
    }

    #[test]
    fn very_large_boundary_decimal_si() {
        assert_eq!(parse("8E").canonical_string(), "8E");
    }

    #[test]
    fn very_large_boundary_binary_si() {
        assert_eq!(parse("1Ei").canonical_string(), "1Ei");
    }

    #[test]
    fn i64_max_decodes_to_self() {
        let q = parse(&i64::MAX.to_string());
        assert_eq!(q.canonical_string(), i64::MAX.to_string());
    }

    #[test]
    fn rejects_empty() {
        assert!(Quantity::parse("").is_err());
    }

    #[test]
    fn rejects_whitespace_only() {
        assert!(Quantity::parse("   ").is_err());
    }

    #[test]
    fn rejects_suffix_without_mantissa() {
        assert!(Quantity::parse("Ki").is_err());
        assert!(Quantity::parse("m").is_err());
        assert!(Quantity::parse("E").is_err());
    }

    #[test]
    fn rejects_unknown_suffix() {
        for s in ["1Q", "1A", "1B", "1KiB", "1ki", "1MIB"] {
            assert!(
                Quantity::parse(s).is_err(),
                "expected reject for {s:?} but it parsed"
            );
        }
    }

    #[test]
    fn rejects_trailing_whitespace() {
        assert!(Quantity::parse("1ki ").is_err());
        assert!(Quantity::parse("1 ").is_err());
        assert!(Quantity::parse(" 1").is_err());
    }

    #[test]
    fn rejects_dotted_garbage() {
        // ".5i" — i alone is not a valid suffix.
        assert!(Quantity::parse(".5i").is_err());
        // "1.1.M" — second `.` ends the parse with leftover garbage.
        assert!(Quantity::parse("1.1.M").is_err());
        // "1+1.0M" — `+` mid-mantissa is not allowed.
        assert!(Quantity::parse("1+1.0M").is_err());
        // "-3.01e-" — exponent must have at least one digit.
        assert!(Quantity::parse("-3.01e-").is_err());
        // "0.1mi" — lowercase `mi` is not a suffix.
        assert!(Quantity::parse("0.1mi").is_err());
    }

    #[test]
    fn sub_value_and_clamp() {
        let q = |s: &str| Quantity::parse(s).unwrap();
        // Same binary unit.
        assert!(q("8Gi").sub(&q("1Gi")).unwrap().value_eq(&q("7Gi")));
        // Different decimal scales (cores minus millicores).
        assert!(q("4").sub(&q("500m")).unwrap().value_eq(&q("3500m")));
        // Cross binary units.
        assert!(q("1Gi").sub(&q("512Mi")).unwrap().value_eq(&q("512Mi")));
        // Subtracting zero is identity; equal operands net to zero.
        assert!(q("4").sub(&q("0")).unwrap().value_eq(&q("4")));
        assert!(q("1Gi").sub(&q("1Gi")).unwrap().value_eq(&q("0")));
        // Reserved exceeding capacity yields a negative quantity (callers clamp).
        let neg = q("100m").sub(&q("500m")).unwrap();
        assert!(neg.is_negative());
        assert!(!q("500m").sub(&q("100m")).unwrap().is_negative());
    }
}
