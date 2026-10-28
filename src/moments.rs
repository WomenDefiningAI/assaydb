//! Second-moment statistics (VARIANCE/STDDEV/COVAR/CORR/REGR_*) with fixed,
//! deterministic accumulation and no partial merges.
//!
//! * Exact inputs (INTEGER/DECIMAL) keep exact integer power sums: n, Σx, Σx²
//!   (and Σy, Σy², Σxy). Results are derived from exact central sums such as
//!   n·Σx² − (Σx)², so the only roundings are the final binary64 conversions.
//! * REAL inputs keep double-double sums of the exact deviations from the
//!   group's first value (and their squares/products), applied in canonical
//!   input order: about 106 bits of working precision with no division per
//!   row. Squares are of deviations, never of raw values, so the range matches
//!   a Welford update. The state is never merged, so the bits are independent
//!   of workers, batches and spill.
//!
//! Degenerate and nonfinite cases follow DuckDB 1.5.6 and PostgreSQL 17 where
//! they agree and are refused where they differ (see SEMANTICS-LOG.md).
use crate::{Result, bind::Moment};

/// Fixed-width two's-complement integer, little-endian 64-bit limbs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Wide<const N: usize>([u64; N]);

impl<const N: usize> Default for Wide<N> {
    fn default() -> Self {
        Self([0; N])
    }
}

impl<const N: usize> Wide<N> {
    pub(crate) fn from_i128(value: i128) -> Self {
        let mut limbs = [if value < 0 { u64::MAX } else { 0 }; N];
        limbs[0] = value as u64;
        if N > 1 {
            limbs[1] = (value >> 64) as u64;
        }
        Self(limbs)
    }
    fn from_magnitude(magnitude: [u64; N], negative: bool) -> Self {
        let value = Self(magnitude);
        if negative { value.negate() } else { value }
    }
    pub(crate) fn is_negative(&self) -> bool {
        self.0[N - 1] >> 63 == 1
    }
    pub(crate) fn is_zero(&self) -> bool {
        self.0.iter().all(|limb| *limb == 0)
    }
    fn negate(&self) -> Self {
        let mut out = [0; N];
        let mut carry = 1_u64;
        for (o, limb) in out.iter_mut().zip(self.0) {
            let (sum, overflow) = (!limb).overflowing_add(carry);
            *o = sum;
            carry = u64::from(overflow);
        }
        Self(out)
    }
    fn magnitude(&self) -> [u64; N] {
        if self.is_negative() {
            self.negate().0
        } else {
            self.0
        }
    }
    /// Wrapping addition; every caller sizes N so the exact sum fits.
    pub(crate) fn add(&mut self, other: &Self) {
        let mut carry = false;
        for (limb, addend) in self.0.iter_mut().zip(other.0) {
            let (sum, first) = limb.overflowing_add(addend);
            let (sum, second) = sum.overflowing_add(u64::from(carry));
            *limb = sum;
            carry = first || second;
        }
    }
    pub(crate) fn sub(&self, other: &Self) -> Self {
        let mut out = *self;
        out.add(&other.negate());
        out
    }
    pub(crate) fn add_i128(&mut self, value: i128) {
        self.add(&Self::from_i128(value));
    }
    /// Adds the exact 256-bit product a·b.
    pub(crate) fn add_product(&mut self, a: i128, b: i128) {
        let (x, y) = (a.unsigned_abs(), b.unsigned_abs());
        let (x0, x1) = (x as u64 as u128, (x >> 64) as u64 as u128);
        let (y0, y1) = (y as u64 as u128, (y >> 64) as u64 as u128);
        let mut limbs = [0_u64; N];
        // Schoolbook 2x2 limb product into four limbs.
        let mut accumulate = |index: usize, value: u128| {
            let mut carry = value;
            let mut i = index;
            while carry != 0 && i < N {
                let sum = u128::from(limbs[i]) + (carry & u128::from(u64::MAX));
                limbs[i] = sum as u64;
                carry = (carry >> 64) + (sum >> 64);
                i += 1;
            }
        };
        accumulate(0, x0 * y0);
        accumulate(1, x0 * y1);
        accumulate(1, x1 * y0);
        accumulate(2, x1 * y1);
        self.add(&Self::from_magnitude(limbs, (a < 0) != (b < 0)));
    }
    /// Exact product when it fits R limbs (callers size R accordingly).
    pub(crate) fn mul<const M: usize, const R: usize>(&self, other: &Wide<M>) -> Wide<R> {
        let (a, b) = (self.magnitude(), other.magnitude());
        let mut out = [0_u64; R];
        for (i, x) in a.iter().enumerate() {
            if *x == 0 {
                continue;
            }
            let mut carry = 0_u128;
            for (j, y) in b.iter().enumerate() {
                if i + j >= R {
                    break;
                }
                let t = u128::from(*x) * u128::from(*y) + u128::from(out[i + j]) + carry;
                out[i + j] = t as u64;
                carry = t >> 64;
            }
            let mut k = i + M;
            while carry != 0 && k < R {
                let t = u128::from(out[k]) + carry;
                out[k] = t as u64;
                carry = t >> 64;
                k += 1;
            }
        }
        Wide::from_magnitude(out, self.is_negative() != other.is_negative())
    }
    /// Nearest binary64 (ties to even) of the exact value.
    pub(crate) fn to_f64(self) -> f64 {
        let magnitude = self.magnitude();
        let Some(top) = magnitude.iter().rposition(|limb| *limb != 0) else {
            return 0.0;
        };
        let high = u128::from(magnitude[top]) << 64
            | if top > 0 {
                u128::from(magnitude[top - 1])
            } else {
                0
            };
        let sticky = top > 1 && magnitude[..top - 1].iter().any(|limb| *limb != 0);
        let zeros = high.leading_zeros();
        let shifted = high << zeros;
        let mut bits = (shifted >> 64) as u64;
        if shifted as u64 != 0 || sticky {
            // Below the rounding position for any 64-bit value with its top bit
            // set, so one integer-to-float conversion rounds correctly.
            bits |= 1;
        }
        let exponent = 64 * top as i32 - zeros as i32;
        let value = scale_by_power_of_two(bits as f64, exponent);
        if self.is_negative() { -value } else { value }
    }
}

fn scale_by_power_of_two(mut value: f64, mut exponent: i32) -> f64 {
    // Each step multiplies by an exact power of two inside binary64's range.
    while exponent > 1000 {
        value *= 2f64.powi(1000);
        exponent -= 1000;
    }
    while exponent < -1000 {
        value *= 2f64.powi(-1000);
        exponent += 1000;
    }
    value * 2f64.powi(exponent)
}

/// Unevaluated sum hi + lo with |lo| <= ulp(hi)/2.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Dd {
    hi: f64,
    lo: f64,
}

#[inline]
fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    let bb = s - a;
    (s, (a - (s - bb)) + (b - bb))
}
#[inline]
fn quick_two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    (s, b - (s - a))
}
#[inline]
fn two_prod(a: f64, b: f64) -> (f64, f64) {
    let p = a * b;
    (p, a.mul_add(b, -p))
}

impl Dd {
    pub(crate) fn from_f64(value: f64) -> Self {
        Self { hi: value, lo: 0.0 }
    }
    pub(crate) fn value(self) -> f64 {
        self.hi + self.lo
    }
    fn is_finite(self) -> bool {
        self.hi.is_finite() && self.lo.is_finite()
    }
    pub(crate) fn add(self, other: Self) -> Self {
        let (s, e) = two_sum(self.hi, other.hi);
        let (t, f) = two_sum(self.lo, other.lo);
        let (s, e) = quick_two_sum(s, e + t);
        let (hi, lo) = quick_two_sum(s, e + f);
        Self { hi, lo }
    }
    pub(crate) fn neg(self) -> Self {
        Self {
            hi: -self.hi,
            lo: -self.lo,
        }
    }
    pub(crate) fn sub(self, other: Self) -> Self {
        self.add(other.neg())
    }
    pub(crate) fn mul(self, other: Self) -> Self {
        let (p, e) = two_prod(self.hi, other.hi);
        let e = e + (self.hi * other.lo + self.lo * other.hi);
        let (hi, lo) = quick_two_sum(p, e);
        Self { hi, lo }
    }
    pub(crate) fn div(self, other: Self) -> Self {
        let q1 = self.hi / other.hi;
        let r = self.sub(other.mul(Self::from_f64(q1)));
        let q2 = r.hi / other.hi;
        let r = r.sub(other.mul(Self::from_f64(q2)));
        let q3 = r.hi / other.hi;
        let (hi, lo) = quick_two_sum(q1, q2);
        Self { hi, lo }.add(Self::from_f64(q3))
    }
}

/// Exact one-variable power sums; the count lives in the aggregate state.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Exact1 {
    sx: Wide<4>,
    sxx: Wide<5>,
}
impl Exact1 {
    #[inline]
    pub(crate) fn update(&mut self, x: i128) {
        self.sx.add_i128(x);
        self.sxx.add_product(x, x);
    }
}

/// Exact two-variable power sums for (y, x) pairs with both values present.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Exact2 {
    sy: Wide<4>,
    sx: Wide<4>,
    syy: Wide<5>,
    sxx: Wide<5>,
    sxy: Wide<5>,
}
impl Exact2 {
    #[inline]
    pub(crate) fn update(&mut self, y: i128, x: i128) {
        self.sy.add_i128(y);
        self.sx.add_i128(x);
        self.syy.add_product(y, y);
        self.sxx.add_product(x, x);
        self.sxy.add_product(x, y);
    }
}

fn nonfinite(function: Moment) -> crate::Error {
    format!(
        "unsupported: {} over a NaN or infinite REAL input in this group (DuckDB 1.5.6 and \
         PostgreSQL disagree; see SEMANTICS-LOG)",
        function.sql_name()
    )
    .into()
}
fn overflow() -> crate::Error {
    "statistical aggregate out of range: intermediate value exceeds DOUBLE".into()
}

/// The exact difference x − shift as a double-double (TwoSum is exact).
#[inline]
fn deviation(x: f64, shift: f64) -> Dd {
    let (hi, lo) = two_sum(x, -shift);
    Dd { hi, lo }
}

/// NaN/infinity classes seen in one argument (bit set).
const NAN: u8 = 1;
const POSITIVE_INFINITY: u8 = 2;
const NEGATIVE_INFINITY: u8 = 4;

/// One REAL argument: double-double sums of the exact deviations d = v − K
/// of its finite values from the first finite value K (canonical order), and
/// the classes of its nonfinite values, which are never accumulated.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Axis {
    shift: f64,
    s1: Dd,
    s2: Dd,
    started: bool,
    nonfinite: u8,
}
impl Axis {
    /// The exact deviation of a finite value, or None after recording a
    /// nonfinite one.
    #[inline]
    fn deviation(&mut self, v: f64) -> Option<Dd> {
        if !v.is_finite() {
            self.nonfinite |= if v.is_nan() {
                NAN
            } else if v > 0.0 {
                POSITIVE_INFINITY
            } else {
                NEGATIVE_INFINITY
            };
            return None;
        }
        if !self.started {
            (self.shift, self.started) = (v, true);
        }
        let d = deviation(v, self.shift);
        self.s1 = self.s1.add(d);
        self.s2 = self.s2.add(d.mul(d));
        Some(d)
    }
    fn finite(&self) -> bool {
        self.s1.is_finite() && self.s2.is_finite()
    }
    /// IEEE sum class of the nonfinite values (what AVG would return), or
    /// None when every value was finite.
    fn nonfinite_class(&self) -> Option<f64> {
        match self.nonfinite {
            0 => None,
            POSITIVE_INFINITY => Some(f64::INFINITY),
            NEGATIVE_INFINITY => Some(f64::NEG_INFINITY),
            _ => Some(f64::NAN),
        }
    }
    /// (mean, central sum, exactly constant) over `n` values, all finite.
    fn central(&self, n: u64) -> (Dd, f64, bool) {
        let (mean, central) = real_central(n, self.shift, self.s1, self.s1, self.s2);
        // Every deviation was exactly zero iff Σd² is zero.
        let constant = self.s2.hi == 0.0;
        let central = if constant {
            0.0
        } else {
            central.value().max(0.0)
        };
        (mean, central, constant)
    }
}

/// REAL one-variable state.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Real1 {
    x: Axis,
}
impl Real1 {
    #[inline]
    pub(crate) fn update(&mut self, x: f64) -> Result<()> {
        self.x.deviation(x);
        if !self.x.finite() {
            return Err(overflow());
        }
        Ok(())
    }
}

/// REAL two-variable state for (y, x) pairs; Σd_x·d_y covers pairs whose
/// values are both finite (it is used only when every value was finite).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Real2 {
    y: Axis,
    x: Axis,
    sxy: Dd,
}
impl Real2 {
    #[inline]
    pub(crate) fn update(&mut self, y: f64, x: f64) -> Result<()> {
        let dy = self.y.deviation(y);
        let dx = self.x.deviation(x);
        if let (Some(dy), Some(dx)) = (dy, dx) {
            self.sxy = self.sxy.add(dx.mul(dy));
        }
        if !self.y.finite() || !self.x.finite() || !self.sxy.is_finite() {
            return Err(overflow());
        }
        Ok(())
    }
}

/// Mean and central sum Σ(a−ā)(b−b̄) in double-double from shifted sums.
fn real_central(n: u64, shift: f64, sum: Dd, other: Dd, product: Dd) -> (Dd, Dd) {
    let count = Dd::from_f64(n as f64);
    let mean = Dd::from_f64(shift).add(sum.div(count));
    (mean, product.sub(sum.mul(other).div(count)))
}

/// Central quantities of a completed group, each within a few ulps.
/// `sxx` = Σ(x−x̄)², `syy` = Σ(y−ȳ)², `sxy` = Σ(x−x̄)(y−ȳ); the constant flags
/// are exact (the central sum is exactly zero).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Finished {
    n: u64,
    mean_x: f64,
    mean_y: f64,
    sxx: f64,
    syy: f64,
    sxy: f64,
    x_constant: bool,
    y_constant: bool,
    /// ȳ − slope·x̄, computed before rounding; None when x is constant.
    intercept: Option<f64>,
    /// IEEE sum class (NaN/±inf) of nonfinite REAL values of x / y, if any;
    /// the central quantities of that argument are then meaningless.
    x_nonfinite: Option<f64>,
    y_nonfinite: Option<f64>,
}

fn power_of_ten(scale: i32) -> f64 {
    10f64.powi(scale)
}

impl Finished {
    /// One-variable exact state. `scale` is the decimal scale of x.
    pub(crate) fn exact1(n: u64, state: &Exact1, scale: i8) -> Self {
        let (central, mean) = exact_central(n, &state.sx, &state.sxx, scale);
        Self::single(n, mean, central)
    }
    pub(crate) fn real1(n: u64, state: &Real1) -> Self {
        if n == 0 {
            return Self::single(0, 0.0, (0.0, true));
        }
        let nonfinite = state.x.nonfinite_class();
        let (mean, central, constant) = if nonfinite.is_some() {
            (Dd::default(), 0.0, false)
        } else {
            state.x.central(n)
        };
        Self {
            x_nonfinite: nonfinite,
            y_nonfinite: nonfinite,
            ..Self::single(n, mean.value(), (central, constant))
        }
    }
    fn single(n: u64, mean: f64, (sxx, constant): (f64, bool)) -> Self {
        Self {
            n,
            mean_x: mean,
            mean_y: mean,
            sxx,
            syy: sxx,
            sxy: sxx,
            x_constant: constant,
            y_constant: constant,
            intercept: None,
            x_nonfinite: None,
            y_nonfinite: None,
        }
    }
    /// Two-variable exact state; scales of y and x.
    pub(crate) fn exact2(n: u64, state: &Exact2, scale_y: i8, scale_x: i8) -> Self {
        let (sxx, mean_x) = exact_central(n, &state.sx, &state.sxx, scale_x);
        let (syy, mean_y) = exact_central(n, &state.sy, &state.syy, scale_y);
        let count = Wide::<4>::from_i128(i128::from(n));
        // n·Σxy − Σx·Σy, scale sx+sy.
        let cxy: Wide<8> = count
            .mul::<5, 8>(&state.sxy)
            .sub(&state.sx.mul::<4, 8>(&state.sy));
        let cxx: Wide<8> = count
            .mul::<5, 8>(&state.sxx)
            .sub(&state.sx.mul::<4, 8>(&state.sx));
        let n_f = n as f64;
        let sxy = if n == 0 {
            0.0
        } else {
            cxy.to_f64() / n_f / power_of_ten(i32::from(scale_x) + i32::from(scale_y))
        };
        let intercept = (!sxx.1 && n > 0).then(|| {
            // (Σy·Cxx − Σx·Cxy) / (n·Cxx), exact numerator at scale 2sx+sy.
            let numerator: Wide<13> = state
                .sy
                .mul::<8, 13>(&cxx)
                .sub(&state.sx.mul::<8, 13>(&cxy));
            numerator.to_f64() / cxx.to_f64() / n_f / power_of_ten(i32::from(scale_y))
        });
        Self {
            n,
            mean_x,
            mean_y,
            sxx: sxx.0,
            syy: syy.0,
            sxy,
            x_constant: sxx.1,
            y_constant: syy.1,
            intercept,
            x_nonfinite: None,
            y_nonfinite: None,
        }
    }
    pub(crate) fn real2(n: u64, state: &Real2) -> Self {
        let empty = Self::exact2(0, &Exact2::default(), 0, 0);
        if n == 0 {
            return empty;
        }
        let (x_nonfinite, y_nonfinite) = (state.x.nonfinite_class(), state.y.nonfinite_class());
        // Each argument's own quantities are valid when that argument is finite.
        let (mean_x, sxx, x_constant) = match x_nonfinite {
            None => state.x.central(n),
            Some(_) => (Dd::default(), 0.0, false),
        };
        let (mean_y, syy, y_constant) = match y_nonfinite {
            None => state.y.central(n),
            Some(_) => (Dd::default(), 0.0, false),
        };
        let mut finished = Self {
            n,
            mean_x: mean_x.value(),
            mean_y: mean_y.value(),
            sxx,
            syy,
            x_constant,
            y_constant,
            x_nonfinite,
            y_nonfinite,
            ..empty
        };
        if x_nonfinite.is_none() && y_nonfinite.is_none() {
            let (_, sxy) = real_central(n, state.x.shift, state.x.s1, state.y.s1, state.sxy);
            finished.sxy = if x_constant || y_constant {
                0.0
            } else {
                sxy.value()
            };
            if !x_constant {
                let (_, sxx) = real_central(n, state.x.shift, state.x.s1, state.x.s1, state.x.s2);
                finished.intercept = Some(mean_y.sub(sxy.div(sxx).mul(mean_x)).value());
            }
        }
        finished
    }
}

/// ((Cxx/n)/10^(2s), exactly zero) and the mean, from exact power sums.
fn exact_central(n: u64, sum: &Wide<4>, squares: &Wide<5>, scale: i8) -> ((f64, bool), f64) {
    if n == 0 {
        return ((0.0, true), 0.0);
    }
    let count = Wide::<4>::from_i128(i128::from(n));
    let central: Wide<8> = count.mul::<5, 8>(squares).sub(&sum.mul::<4, 8>(sum));
    let n_f = n as f64;
    let scale = i32::from(scale);
    (
        (
            central.to_f64() / n_f / power_of_ten(2 * scale),
            central.is_zero(),
        ),
        sum.to_f64() / n_f / power_of_ten(scale),
    )
}

impl Moment {
    pub(crate) fn sql_name(self) -> &'static str {
        match self {
            Self::VarSamp => "VAR_SAMP",
            Self::VarPop => "VAR_POP",
            Self::StddevSamp => "STDDEV_SAMP",
            Self::StddevPop => "STDDEV_POP",
            Self::CovarSamp => "COVAR_SAMP",
            Self::CovarPop => "COVAR_POP",
            Self::Corr => "CORR",
            Self::RegrSlope => "REGR_SLOPE",
            Self::RegrIntercept => "REGR_INTERCEPT",
            Self::RegrR2 => "REGR_R2",
            Self::RegrSxx => "REGR_SXX",
            Self::RegrSyy => "REGR_SYY",
            Self::RegrSxy => "REGR_SXY",
            Self::RegrAvgX => "REGR_AVGX",
            Self::RegrAvgY => "REGR_AVGY",
        }
    }
}

/// Groups with a NaN or infinite REAL value: only the outcomes on which
/// DuckDB 1.5.6 and PostgreSQL 17 agree (or the October 17 decision for
/// degenerate CORR/REGR_SLOPE groups) are returned; everything else is
/// refused. `f.n >= 1` here.
fn evaluate_nonfinite(function: Moment, f: &Finished) -> Result<Option<f64>> {
    let x_finite = f.x_nonfinite.is_none();
    let y_finite = f.y_nonfinite.is_none();
    let single = f.n == 1;
    Ok(match function {
        // One row: the sample forms have no value in either reference.
        Moment::VarSamp | Moment::StddevSamp | Moment::CovarSamp if single => None,
        // One row with a nonfinite argument: (v − v̄) is NaN in both.
        Moment::CovarPop | Moment::RegrSxy if single => Some(f64::NAN),
        // One row: both NaN when both arguments are nonfinite; otherwise
        // DuckDB NaN, PostgreSQL NULL, and NULL by decision.
        Moment::Corr if single && !x_finite && !y_finite => Some(f64::NAN),
        Moment::Corr if single => None,
        // Constant finite X (including one row): PostgreSQL NULL; DuckDB NaN
        // for the slope (NULL by decision) and NULL for intercept and R².
        Moment::RegrSlope | Moment::RegrIntercept | Moment::RegrR2 if x_finite && f.x_constant => {
            None
        }
        Moment::RegrSlope if single => Some(f64::NAN),
        // Quantities of one argument only follow from that argument.
        Moment::RegrSxx if x_finite => Some(f.sxx),
        Moment::RegrSyy if y_finite => Some(f.syy),
        // Both references divide an IEEE sum: ±inf or NaN by value classes.
        Moment::RegrAvgX => Some(f.x_nonfinite.unwrap_or(f.mean_x)),
        Moment::RegrAvgY => Some(f.y_nonfinite.unwrap_or(f.mean_y)),
        _ => return Err(nonfinite(function)),
    })
}

/// The SQL result for one completed group; Err for reference disagreements.
pub(crate) fn evaluate(function: Moment, f: &Finished) -> Result<Option<f64>> {
    if f.n > 0 && (f.x_nonfinite.is_some() || f.y_nonfinite.is_some()) {
        return evaluate_nonfinite(function, f);
    }
    let n = f.n as f64;
    let result = match function {
        Moment::VarPop | Moment::StddevPop | Moment::CovarPop if f.n == 0 => return Ok(None),
        Moment::VarSamp | Moment::StddevSamp | Moment::CovarSamp if f.n < 2 => return Ok(None),
        Moment::VarPop => f.sxx / n,
        Moment::VarSamp => f.sxx / (n - 1.0),
        Moment::StddevPop => (f.sxx / n).sqrt(),
        Moment::StddevSamp => (f.sxx / (n - 1.0)).sqrt(),
        Moment::CovarPop => f.sxy / n,
        Moment::CovarSamp => f.sxy / (n - 1.0),
        _ if f.n == 0 => return Ok(None),
        // Constant argument or one row: PostgreSQL and the SQL standard
        // return NULL, DuckDB NaN; NULL by decision (October 17, 2026).
        Moment::Corr if f.x_constant || f.y_constant => return Ok(None),
        Moment::Corr => {
            // One rounding before the square root when the product is normal.
            let product = f.sxx * f.syy;
            if product.is_normal() {
                f.sxy / product.sqrt()
            } else {
                f.sxy / (f.sxx.sqrt() * f.syy.sqrt())
            }
        }
        Moment::RegrSlope if f.x_constant => return Ok(None),
        Moment::RegrSlope => f.sxy / f.sxx,
        Moment::RegrIntercept => match f.intercept {
            None => return Ok(None),
            Some(value) => value,
        },
        Moment::RegrR2 if f.x_constant => return Ok(None),
        Moment::RegrR2 if f.y_constant => 1.0,
        Moment::RegrR2 => (f.sxy / f.sxx) * (f.sxy / f.syy),
        Moment::RegrSxx => f.sxx,
        Moment::RegrSyy => f.syy,
        Moment::RegrSxy => f.sxy,
        Moment::RegrAvgX => f.mean_x,
        Moment::RegrAvgY => f.mean_y,
    };
    if !result.is_finite() {
        return Err(overflow());
    }
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact_variance(values: &[i128], scale: i8) -> Option<f64> {
        let mut state = Exact1::default();
        for v in values {
            state.update(*v);
        }
        evaluate(
            Moment::VarSamp,
            &Finished::exact1(values.len() as u64, &state, scale),
        )
        .unwrap()
    }
    fn real_variance(values: &[f64]) -> Option<f64> {
        let mut state = Real1::default();
        for v in values {
            state.update(*v).unwrap();
        }
        evaluate(
            Moment::VarSamp,
            &Finished::real1(values.len() as u64, &state),
        )
        .unwrap()
    }

    #[test]
    fn wide_arithmetic_matches_i128_and_extends_beyond_it() {
        let values = [
            0_i128,
            1,
            -1,
            7,
            -9,
            i64::MAX as i128,
            i128::MIN + 1,
            i128::MAX,
        ];
        for a in values {
            for b in values {
                let mut sum = Wide::<5>::from_i128(a);
                sum.add_i128(b);
                if let Some(exact) = a.checked_add(b) {
                    assert_eq!(sum, Wide::<5>::from_i128(exact));
                }
                let mut product = Wide::<5>::default();
                product.add_product(a, b);
                if let Some(exact) = a.checked_mul(b) {
                    assert_eq!(product, Wide::<5>::from_i128(exact));
                    let narrow = Wide::<2>::from_i128(a).mul::<2, 4>(&Wide::<2>::from_i128(b));
                    assert_eq!(narrow, Wide::<4>::from_i128(exact));
                }
                if a.unsigned_abs() < 1 << 26 && b.unsigned_abs() < 1 << 26 {
                    assert_eq!(product.to_f64(), (a as f64) * (b as f64));
                }
            }
        }
        // (2^127 - 1)^2 needs 254 bits; the 5-limb state holds 2^64 of them.
        let mut squares = Wide::<5>::default();
        for _ in 0..4 {
            squares.add_product(i128::MAX, i128::MAX);
        }
        assert_eq!(squares.to_f64(), 4.0 * (i128::MAX as f64).powi(2));
        assert!(!squares.is_negative());
        assert_eq!(
            Wide::<3>::from_i128(-5).sub(&Wide::from_i128(-5)),
            Wide::default()
        );
    }

    #[test]
    fn wide_to_f64_rounds_ties_to_even_with_sticky_bits() {
        // 2^53 + 1 is a tie between 2^53 and 2^53 + 2: even wins.
        assert_eq!(Wide::<2>::from_i128((1 << 53) + 1).to_f64(), 2f64.powi(53));
        // A low sticky bit far below breaks the tie upward.
        let mut v = Wide::<4>::from_i128((1 << 53) + 1);
        v = v.mul::<4, 4>(&Wide::from_i128(1 << 100));
        v.add_i128(1);
        assert_eq!(v.to_f64(), (2f64.powi(53) + 2.0) * 2f64.powi(100));
        assert_eq!(Wide::<4>::from_i128(-3).to_f64(), -3.0);
    }

    #[test]
    fn exact_and_real_variance_agree_with_closed_forms() {
        assert_eq!(exact_variance(&[1, 2, 3, 4], 0), Some(5.0 / 3.0));
        assert_eq!(exact_variance(&[150, 250], 2), Some(0.5));
        assert_eq!(exact_variance(&[5], 0), None);
        assert_eq!(real_variance(&[1.0, 2.0, 3.0, 4.0]), Some(5.0 / 3.0));
        // Catastrophic cancellation for the textbook formula: 1e9 + {4,7,13,16}.
        let shifted = [1e9 + 4.0, 1e9 + 7.0, 1e9 + 13.0, 1e9 + 16.0];
        assert_eq!(real_variance(&shifted), Some(30.0));
        let exact: Vec<i128> = shifted.iter().map(|v| *v as i128).collect();
        assert_eq!(exact_variance(&exact, 0), Some(30.0));
    }

    #[test]
    fn degenerate_groups_follow_postgresql_null() {
        let mut state = Exact2::default();
        state.update(1, 1);
        let one = Finished::exact2(1, &state, 0, 0);
        assert_eq!(evaluate(Moment::CovarPop, &one).unwrap(), Some(0.0));
        assert_eq!(evaluate(Moment::CovarSamp, &one).unwrap(), None);
        assert_eq!(evaluate(Moment::RegrIntercept, &one).unwrap(), None);
        assert_eq!(evaluate(Moment::RegrR2, &one).unwrap(), None);
        assert_eq!(evaluate(Moment::RegrSxx, &one).unwrap(), Some(0.0));
        // DuckDB NaN, PostgreSQL NULL: NULL by decision (October 17, 2026).
        assert_eq!(evaluate(Moment::Corr, &one).unwrap(), None);
        assert_eq!(evaluate(Moment::RegrSlope, &one).unwrap(), None);
        state.update(1, 2); // y constant, x varies
        let flat = Finished::exact2(2, &state, 0, 0);
        assert_eq!(evaluate(Moment::RegrR2, &flat).unwrap(), Some(1.0));
        assert_eq!(evaluate(Moment::RegrSlope, &flat).unwrap(), Some(0.0));
        assert_eq!(evaluate(Moment::RegrIntercept, &flat).unwrap(), Some(1.0));
        let empty = Finished::exact2(0, &Exact2::default(), 0, 0);
        for function in [Moment::Corr, Moment::RegrSlope, Moment::RegrAvgX] {
            assert_eq!(evaluate(function, &empty).unwrap(), None);
        }
    }

    #[test]
    fn real_two_variable_state_matches_exact_state() {
        let pairs = [(3_i128, 1_i128), (5, 2), (4, 3), (8, 4), (10, 5)];
        let mut exact = Exact2::default();
        let mut real = Real2::default();
        for (y, x) in pairs.iter() {
            exact.update(*y, *x);
            real.update(*y as f64, *x as f64).unwrap();
        }
        let a = Finished::exact2(5, &exact, 0, 0);
        let b = Finished::real2(5, &real);
        for function in [
            Moment::Corr,
            Moment::CovarSamp,
            Moment::RegrSlope,
            Moment::RegrIntercept,
            Moment::RegrR2,
            Moment::RegrSxy,
        ] {
            let (x, y) = (
                evaluate(function, &a).unwrap().unwrap(),
                evaluate(function, &b).unwrap().unwrap(),
            );
            assert!(
                (x - y).abs() <= 1e-15 * x.abs().max(1.0),
                "{function:?} {x} {y}"
            );
        }
        assert_eq!(evaluate(Moment::RegrSlope, &a).unwrap(), Some(1.7));
        assert_eq!(evaluate(Moment::RegrIntercept, &a).unwrap(), Some(0.9));
    }

    fn real2(pairs: &[(f64, f64)]) -> Finished {
        let mut state = Real2::default();
        for (y, x) in pairs {
            state.update(*y, *x).unwrap();
        }
        Finished::real2(pairs.len() as u64, &state)
    }

    #[test]
    fn nonfinite_inputs_follow_agreed_outcomes_and_refuse_the_rest() {
        let (inf, nan) = (f64::INFINITY, f64::NAN);
        let mut one = Real1::default();
        one.update(inf).unwrap();
        let one = Finished::real1(1, &one);
        assert_eq!(evaluate(Moment::StddevSamp, &one).unwrap(), None);
        assert!(
            evaluate(Moment::VarPop, &one)
                .unwrap_err()
                .starts_with("unsupported:")
        );
        // y = inf, x = 1: X is finite and constant.
        let f = real2(&[(inf, 1.0)]);
        assert_eq!(evaluate(Moment::Corr, &f).unwrap(), None);
        assert_eq!(evaluate(Moment::RegrSlope, &f).unwrap(), None);
        assert_eq!(evaluate(Moment::RegrIntercept, &f).unwrap(), None);
        assert_eq!(evaluate(Moment::RegrSxx, &f).unwrap(), Some(0.0));
        assert_eq!(evaluate(Moment::RegrAvgY, &f).unwrap(), Some(inf));
        assert!(evaluate(Moment::CovarPop, &f).unwrap().unwrap().is_nan());
        assert!(
            evaluate(Moment::RegrSyy, &f)
                .unwrap_err()
                .starts_with("unsupported:")
        );
        let f = real2(&[(inf, inf)]);
        assert!(evaluate(Moment::Corr, &f).unwrap().unwrap().is_nan());
        assert!(evaluate(Moment::RegrSlope, &f).unwrap().unwrap().is_nan());
        let f = real2(&[(1.0, f64::NEG_INFINITY), (2.0, inf), (nan, 3.0)]);
        assert!(evaluate(Moment::RegrAvgX, &f).unwrap().unwrap().is_nan());
        assert!(evaluate(Moment::RegrAvgY, &f).unwrap().unwrap().is_nan());
        assert!(
            evaluate(Moment::CovarPop, &f)
                .unwrap_err()
                .starts_with("unsupported:")
        );
        let f = real2(&[(1.0, 2.0), (2.0, inf), (4.0, 3.0)]);
        assert_eq!(evaluate(Moment::RegrSyy, &f).unwrap(), Some(14.0 / 3.0));
        assert_eq!(evaluate(Moment::RegrAvgY, &f).unwrap(), Some(7.0 / 3.0));
    }

    #[test]
    fn finite_overflow_errors() {
        let mut state = Real1::default();
        state.update(1e200).unwrap();
        assert_eq!(
            state.update(-1e200).unwrap_err(),
            "statistical aggregate out of range: intermediate value exceeds DOUBLE"
        );
        // Deviations, not raw squares: large equal values stay finite (DuckDB 0.0).
        let mut state = Real1::default();
        state.update(1e308).unwrap();
        state.update(1e308).unwrap();
        assert_eq!(
            evaluate(Moment::VarPop, &Finished::real1(2, &state)).unwrap(),
            Some(0.0)
        );
    }
}
