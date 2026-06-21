use fixed::types::I64F64;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Div, Mul, Sub};

// ---------------------------------------------------------------------------
// Rounding mode
// ---------------------------------------------------------------------------

/// Determines how fractional results are rounded after division.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoundingMode {
    /// Round to nearest; ties round away from zero (commercial billing default).
    RoundHalfUp,
    /// Round to nearest; ties round to even (banker's rounding, settlement).
    Bankers,
    /// Always round toward zero (truncate).
    Truncate,
}

// ---------------------------------------------------------------------------
// DecimalCommodity – saturating newtype over I64F64
// ---------------------------------------------------------------------------

/// A saturating fixed-point wrapper around [`I64F64`] for analytics and
/// non-billing code paths that must never panic on overflow.
///
/// Saturates at `I64F64::MAX` / `I64F64::MIN` on arithmetic overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DecimalCommodity(pub I64F64);

impl DecimalCommodity {
    /// Create from a raw `I64F64` value.
    #[inline]
    pub fn new(inner: I64F64) -> Self {
        Self(inner)
    }

    /// Create from an `f64`, saturating at the representable bounds.
    #[inline]
    pub fn from_num(num: f64) -> Self {
        Self(I64F64::from_num(num))
    }

    /// Return the inner `I64F64`.
    #[inline]
    pub fn into_inner(self) -> I64F64 {
        self.0
    }

    /// Saturating addition.
    #[inline]
    pub fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating subtraction.
    #[inline]
    pub fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    /// Saturating multiplication.
    #[inline]
    pub fn saturating_mul(self, rhs: Self) -> Self {
        Self(self.0.saturating_mul(rhs.0))
    }

    /// Saturating division — returns `MAX` on division by zero.
    #[inline]
    pub fn saturating_div(self, rhs: Self) -> Self {
        if rhs.0 == I64F64::from_num(0) {
            Self(I64F64::MAX)
        } else {
            Self(self.0.saturating_div(rhs.0))
        }
    }
}

// Operator overloads delegate to saturating arithmetic.

impl Add for DecimalCommodity {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Self) -> Self {
        self.saturating_add(rhs)
    }
}

impl Sub for DecimalCommodity {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: Self) -> Self {
        self.saturating_sub(rhs)
    }
}

impl Mul for DecimalCommodity {
    type Output = Self;
    #[inline]
    fn mul(self, rhs: Self) -> Self {
        self.saturating_mul(rhs)
    }
}

impl Div for DecimalCommodity {
    type Output = Self;
    #[inline]
    fn div(self, rhs: Self) -> Self {
        self.saturating_div(rhs)
    }
}

impl fmt::Display for DecimalCommodity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<f64> for DecimalCommodity {
    fn from(v: f64) -> Self {
        Self::from_num(v)
    }
}

impl From<DecimalCommodity> for f64 {
    fn from(v: DecimalCommodity) -> Self {
        v.0.to_num::<f64>()
    }
}

/// Legacy CommodityAmount — kept for backward compatibility with
/// callers that serialize/deserialize this shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct CommodityAmount {
    pub integral: i64,
    pub fractional: u64,
}

// ---------------------------------------------------------------------------
// Checked arithmetic helpers
// ---------------------------------------------------------------------------

/// Maximum value representable by `I64F64` (≈ 9.22 × 10¹⁸).
const I64F64_MAX_F64: f64 = 9.22e18;
/// Minimum value representable by `I64F64`.
const I64F64_MIN_F64: f64 = -9.22e18;

/// Returns `Some(())` when `amount` is a finite `f64` within `I64F64`'s safe range.
fn validate_f64_for_fixed(amount: f64) -> Result<(), &'static str> {
    if amount.is_nan() {
        return Err("invalid float: NaN");
    }
    if amount.is_infinite() {
        return Err("invalid float: infinite");
    }
    if amount > I64F64_MAX_F64 || amount < I64F64_MIN_F64 {
        return Err("overflow: value outside I64F64 representable range");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Core public API
// ---------------------------------------------------------------------------

/// Scale a commodity amount (in floating-point) to a fixed-point `I64F64` value.
///
/// Returns `Err` on NaN, infinity, or values outside the representable range
/// of `I64F64`.
pub fn scale_commodity(amount: f64, _decimals: u8) -> Result<I64F64, &'static str> {
    validate_f64_for_fixed(amount)?;
    Ok(I64F64::from_num(amount))
}

/// Convert a commodity value between units.
///
/// All arithmetic uses checked operations — overflow returns `Err`.
/// The `rounding` parameter is available for callers that need to apply a
/// rounding mode downstream; the raw conversion result is returned untouched
/// to preserve full fixed-point precision.
pub fn convert_units(
    value: I64F64,
    from_unit: &str,
    to_unit: &str,
    _rounding: RoundingMode,
) -> Result<I64F64, &'static str> {
    let result = match (from_unit, to_unit) {
        ("kWh", "MWh") => value.checked_div(I64F64::from_num(1000)),
        ("gal", "CCF") => value.checked_div(I64F64::from_num(748)),
        ("m3", "L") => value.checked_mul(I64F64::from_num(1000)),
        ("MWh", "kWh") => value.checked_mul(I64F64::from_num(1000)),
        ("CCF", "gal") => value.checked_mul(I64F64::from_num(748)),
        ("L", "m3") => value.checked_div(I64F64::from_num(1000)),
        _ => return Err("unsupported unit conversion pair"),
    };

    result.ok_or("arithmetic overflow during unit conversion")
}

/// Apply a rounding mode to a fixed-point value, rounding to the nearest integer.
///
/// Implements three distinct rounding strategies:
///
/// | Mode          | Behaviour             | Example               |
/// |---------------|-----------------------|-----------------------|
/// | `Truncate`    | Toward zero           | 2.9 → 2, -2.9 → -2   |
/// | `Bankers`     | Ties to even          | 2.5 → 2, 3.5 → 4     |
/// | `RoundHalfUp` | Ties away from zero   | 2.5 → 3, -2.5 → -3   |
pub fn apply_rounding(value: I64F64, mode: RoundingMode) -> I64F64 {
    let zero = I64F64::from_num(0);
    let one = I64F64::from_num(1);
    let half = I64F64::from_num(0.5);

    /// Truncate toward zero: floor for positive, ceil for negative.
    fn trunc_toward_zero(v: I64F64) -> I64F64 {
        if v >= I64F64::from_num(0) {
            v.floor()
        } else {
            v.ceil()
        }
    }

    match mode {
        RoundingMode::Truncate => trunc_toward_zero(value),

        RoundingMode::RoundHalfUp => {
            if value >= zero {
                trunc_toward_zero(value + half)
            } else {
                trunc_toward_zero(value - half)
            }
        }

        RoundingMode::Bankers => {
            let integer = trunc_toward_zero(value);
            let fractional = value - integer; // keeps sign of value
            let abs_frac = if fractional < zero {
                -fractional
            } else {
                fractional
            };

            if abs_frac > half {
                // Round away from zero.
                if value >= zero {
                    integer + one
                } else {
                    integer - one
                }
            } else if abs_frac < half {
                // Round toward zero.
                integer
            } else {
                // Tie: round to even integer.
                // Use floor() / ceil() to get integer value as i64.
                let int_val = if integer >= zero {
                    integer.floor().to_num::<i64>()
                } else {
                    integer.ceil().to_num::<i64>()
                };
                let is_even = int_val.abs() % 2 == 0;
                if is_even {
                    integer
                } else if value >= zero {
                    integer + one
                } else {
                    integer - one
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // -- basic correctness --

    #[test]
    fn convert_kwh_to_mwh() {
        let val = I64F64::from_num(1000);
        let result = convert_units(val, "kWh", "MWh", RoundingMode::RoundHalfUp).unwrap();
        assert!((result.to_num::<f64>() - 1.0).abs() < 0.0001);
    }

    #[test]
    fn convert_mwh_to_kwh() {
        let val = I64F64::from_num(1);
        let result = convert_units(val, "MWh", "kWh", RoundingMode::RoundHalfUp).unwrap();
        assert!((result.to_num::<f64>() - 1000.0).abs() < 0.0001);
    }

    #[test]
    fn convert_gal_to_ccf() {
        let val = I64F64::from_num(748);
        let result = convert_units(val, "gal", "CCF", RoundingMode::RoundHalfUp).unwrap();
        assert!((result.to_num::<f64>() - 1.0).abs() < 0.0001);
    }

    #[test]
    fn convert_ccf_to_gal() {
        let val = I64F64::from_num(1);
        let result = convert_units(val, "CCF", "gal", RoundingMode::RoundHalfUp).unwrap();
        assert!((result.to_num::<f64>() - 748.0).abs() < 0.0001);
    }

    #[test]
    fn convert_m3_to_l() {
        let val = I64F64::from_num(1);
        let result = convert_units(val, "m3", "L", RoundingMode::RoundHalfUp).unwrap();
        assert!((result.to_num::<f64>() - 1000.0).abs() < 0.0001);
    }

    #[test]
    fn unsupported_conversion_is_error() {
        let val = I64F64::from_num(100);
        assert!(convert_units(val, "kWh", "barrel", RoundingMode::Truncate).is_err());
    }

    // -- overflow / edge-case protection --

    #[test]
    fn scale_nan_is_error() {
        assert!(scale_commodity(f64::NAN, 7).is_err());
    }

    #[test]
    fn scale_infinity_is_error() {
        assert!(scale_commodity(f64::INFINITY, 7).is_err());
        assert!(scale_commodity(f64::NEG_INFINITY, 7).is_err());
    }

    #[test]
    fn scale_out_of_bounds_is_error() {
        assert!(scale_commodity(1e20, 7).is_err());
        assert!(scale_commodity(-1e20, 7).is_err());
    }

    #[test]
    fn scale_normal_value_is_ok() {
        assert!(scale_commodity(42.0, 7).is_ok());
        assert!(scale_commodity(0.0, 7).is_ok());
        assert!(scale_commodity(-1.5, 7).is_ok());
    }

    // -- DecimalCommodity saturating arithmetic --

    #[test]
    fn decimal_commodity_add_normal() {
        let a = DecimalCommodity::from_num(100.0);
        let b = DecimalCommodity::from_num(50.0);
        assert_eq!(f64::from(a + b), 150.0);
    }

    #[test]
    fn decimal_commodity_sub_normal() {
        let a = DecimalCommodity::from_num(100.0);
        let b = DecimalCommodity::from_num(50.0);
        assert_eq!(f64::from(a - b), 50.0);
    }

    #[test]
    fn decimal_commodity_mul_normal() {
        let a = DecimalCommodity::from_num(10.0);
        let b = DecimalCommodity::from_num(3.0);
        assert!((f64::from(a * b) - 30.0).abs() < 0.0001);
    }

    #[test]
    fn decimal_commodity_div_normal() {
        let a = DecimalCommodity::from_num(100.0);
        let b = DecimalCommodity::from_num(4.0);
        assert!((f64::from(a / b) - 25.0).abs() < 0.0001);
    }

    #[test]
    fn decimal_commodity_saturates_on_overflow() {
        let max = DecimalCommodity(I64F64::MAX);
        let one = DecimalCommodity(I64F64::from_num(1));
        let result = max + one;
        assert_eq!(result, DecimalCommodity(I64F64::MAX));
    }

    #[test]
    fn decimal_commodity_saturates_on_underflow() {
        let min = DecimalCommodity(I64F64::MIN);
        let one = DecimalCommodity(I64F64::from_num(1));
        let result = min - one;
        assert_eq!(result, DecimalCommodity(I64F64::MIN));
    }

    #[test]
    fn decimal_commodity_div_by_zero_saturates() {
        let a = DecimalCommodity::from_num(100.0);
        let zero = DecimalCommodity(I64F64::from_num(0));
        let result = a / zero;
        assert_eq!(result, DecimalCommodity(I64F64::MAX));
    }

    // -- rounding modes --

    #[test]
    fn round_half_up_positive() {
        // 2.5 → 3 (ties away from zero)
        let val = I64F64::from_num(2.5);
        let result = apply_rounding(val, RoundingMode::RoundHalfUp);
        assert!((result.to_num::<f64>() - 3.0).abs() < 0.0001);
    }

    #[test]
    fn round_half_up_negative() {
        // -2.5 → -3 (ties away from zero)
        let val = I64F64::from_num(-2.5);
        let result = apply_rounding(val, RoundingMode::RoundHalfUp);
        assert!((result.to_num::<f64>() - (-3.0)).abs() < 0.0001);
    }

    #[test]
    fn round_half_up_non_tie() {
        // 2.3 → 2, 2.7 → 3
        assert!((apply_rounding(I64F64::from_num(2.3), RoundingMode::RoundHalfUp)
            .to_num::<f64>()
            - 2.0)
            .abs()
            < 0.0001);
        assert!((apply_rounding(I64F64::from_num(2.7), RoundingMode::RoundHalfUp)
            .to_num::<f64>()
            - 3.0)
            .abs()
            < 0.0001);
    }

    #[test]
    fn truncate_mode() {
        let val = I64F64::from_num(2.9);
        let result = apply_rounding(val, RoundingMode::Truncate);
        assert!((result.to_num::<f64>() - 2.0).abs() < 0.0001);
    }

    #[test]
    fn truncate_mode_negative() {
        let val = I64F64::from_num(-2.9);
        let result = apply_rounding(val, RoundingMode::Truncate);
        assert!((result.to_num::<f64>() - (-2.0)).abs() < 0.0001);
    }

    #[test]
    fn bankers_mode_tie_to_even() {
        // bank's rounding: 2.5 → 2 (ties to even)
        let val = I64F64::from_num(2.5);
        let result = apply_rounding(val, RoundingMode::Bankers);
        assert!((result.to_num::<f64>() - 2.0).abs() < 0.0001);
    }

    #[test]
    fn bankers_mode_tie_to_even_odd() {
        // bank's rounding: 3.5 → 4 (ties to even)
        let val = I64F64::from_num(3.5);
        let result = apply_rounding(val, RoundingMode::Bankers);
        assert!((result.to_num::<f64>() - 4.0).abs() < 0.0001);
    }

    #[test]
    fn bankers_mode_negative() {
        // -2.5 → -2 (ties to even integer, -2 is even)
        let val = I64F64::from_num(-2.5);
        let result = apply_rounding(val, RoundingMode::Bankers);
        assert!((result.to_num::<f64>() - (-2.0)).abs() < 0.0001);
    }

    #[test]
    fn bankers_mode_non_tie() {
        // 2.3 → 2, 2.7 → 3
        assert!((apply_rounding(I64F64::from_num(2.3), RoundingMode::Bankers)
            .to_num::<f64>()
            - 2.0)
            .abs()
            < 0.0001);
        assert!((apply_rounding(I64F64::from_num(2.7), RoundingMode::Bankers)
            .to_num::<f64>()
            - 3.0)
            .abs()
            < 0.0001);
    }

    #[test]
    fn bankers_differs_from_half_up() {
        let val = I64F64::from_num(2.5);
        let half_up = apply_rounding(val, RoundingMode::RoundHalfUp);
        let bankers = apply_rounding(val, RoundingMode::Bankers);
        assert!(
            half_up > bankers,
            "round-half-up (3) should differ from bank's (2) at 2.5"
        );
    }

    // -- property-based tests --

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        /// scale_commodity must never panic for any f64 value, including NaN and ±∞.
        #[test]
        fn no_panic_on_extreme_values(v in any::<f64>()) {
            let _ = scale_commodity(v, 7);
        }

        /// (a + b) - b == a for valid I64F64 values that don't overflow.
        #[test]
        fn identity_add_sub(
            a in -1e9f64..1e9f64,
            b in -1e9f64..1e9f64,
        ) {
            let fa = I64F64::from_num(a);
            let fb = I64F64::from_num(b);
            let sum = match fa.checked_add(fb) {
                Some(s) => s,
                None => return Ok(()),
            };
            let result = match sum.checked_sub(fb) {
                Some(r) => r,
                None => return Ok(()),
            };
            let diff = (result - fa).abs();
            prop_assert!(diff < I64F64::from_num(1e-6), "identity failed: a={a}, b={b}, diff={diff}");
        }

        /// scale_commodity returns Ok for all finite f64 values within safe range.
        #[test]
        fn scale_finite_in_range_is_ok(v in -9e18f64..9e18f64) {
            let result = scale_commodity(v, 7);
            prop_assert!(result.is_ok(), "scale_commodity failed for {v}: {result:?}");
        }

        /// convert_units (kWh ↔ MWh round-trip) preserves value within epsilon.
        #[test]
        fn convert_kwh_mwh_roundtrip(v in -1e6f64..1e6f64) {
            let val = I64F64::from_num(v);
            let kwh_to_mwh = match convert_units(val, "kWh", "MWh", RoundingMode::RoundHalfUp) {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
            let roundtrip = match convert_units(kwh_to_mwh, "MWh", "kWh", RoundingMode::RoundHalfUp) {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
            let diff = (roundtrip - val).abs();
            prop_assert!(diff < I64F64::from_num(1e-3), "round-trip failed: v={v}, roundtrip={roundtrip}");
        }

        /// DecimalCommodity add-then-sub identity.
        #[test]
        fn decimal_commodity_add_sub_identity(
            a in -1e9f64..1e9f64,
            b in -1e9f64..1e9f64,
        ) {
            let da = DecimalCommodity::from_num(a);
            let db = DecimalCommodity::from_num(b);
            let result = (da + db) - db;
            let diff = f64::from(result) - a;
            prop_assert!(diff.abs() < 1e-6, "DecimalCommodity add-sub identity failed: a={a}, b={b}");
        }
    }
}
