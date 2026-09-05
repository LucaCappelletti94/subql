//! The running total the engine holds for one aggregate subscription.
//!
//! A total starts out unseeded, which is not the same as zero. The caller
//! learns its seed query from the registration, so its read necessarily
//! happens after the engine has begun folding changes, and a change committed
//! inside that window is in both the read and the fold. Adding both would
//! double count it, permanently. So an unseeded total folds nothing and
//! instead records each change against the stream position it arrived at, and
//! the install keeps only the changes the read could not have seen.

use crate::backend::{Backend, Value};
use crate::checkpoint::Checkpoint;
use crate::compiler::AggSpec;
use crate::{AggValue, AggregateInstallError, IdTypes, SubscriptionId};
use alloc::string::ToString;
use alloc::vec::Vec;
use hashbrown::HashMap;

/// Typed signed change to one aggregate subscription's running value.
///
/// Internal: the engine holds the running value itself, so a delta never
/// reaches a caller. [`AggValue`] is what a caller sees.
#[derive(Clone, Debug, PartialEq)]
pub enum AggDelta {
    /// COUNT(*) / COUNT(column) delta: always +/-1 per matching (non-NULL) row.
    Count(i64),
    /// SUM(column) or AVG(column) delta: the signed change in the column
    /// total, and the signed change in how many rows contribute one.
    ///
    /// One variant for both, because both read the same pair. `AVG`
    /// divides the total by the count; `SUM` reports NULL when the count
    /// is zero, since measured, every engine answers NULL for a sum over
    /// no contributing rows and `0` for a sum over one row worth zero, so
    /// a total alone cannot tell those apart. Which function reads the
    /// pair is [`AggAccumulator`]'s to know.
    Totalled { value: TotalDelta, count_delta: i64 },
    /// VAR_POP / VAR_SAMP / STDDEV_POP / STDDEV_SAMP delta, carrying the three
    /// components all four derive their value from. See [`AggAccumulator`].
    Stats {
        /// The exact contribution to the total, because a widened seed
        /// still projects the function's own total and a `SUM` beside a
        /// sibling `HAVING` answers it exactly.
        value: TotalDelta,
        /// The rows joining, summarised.
        added: Spread,
        /// The rows leaving, summarised.
        removed: Spread,
    },
}

/// One row's signed contribution to a running total, as exactly as the
/// row carried it.
///
/// No engine sums in `f64`: measured, a single row of `9007199254740993`
/// is itself on all three, where `f64` answers `9007199254740992`. So a
/// delta keeps the cell's own kind and the accumulator decides what to add
/// it into.
#[derive(Clone, Debug, PartialEq)]
pub enum TotalDelta {
    /// An integer cell, widened to 128 bits so that merging a batch of
    /// them cannot overflow before the accumulator applies its engine's
    /// own boundary.
    Integer(i128),
    /// An exact decimal cell.
    Decimal(bigdecimal::BigDecimal),
    /// A floating change, held as its parts so that it can be undone.
    ///
    /// Not one `f64`. A removal reaches the accumulator as the negated
    /// value, and negating an infinity produces the other infinity, so
    /// `Infinity + -Infinity` is `NaN` and nothing later escapes it.
    /// Measured, PostgreSQL simply answers over the rows that remain: an
    /// `Infinity` deleted from `Infinity, 1, 2` leaves `3`. Counting the
    /// live non-finite contributions instead makes a removal ordinary
    /// arithmetic, and makes the delta and the total members of the same
    /// commutative group, added componentwise.
    Real(FloatParts),
}

/// A floating quantity as its parts: what the finite contributions sum
/// to, and how many non-finite ones are live.
///
/// Signed counts, because a delta removing an infinity carries `-1`
/// where a total holding one carries `1`. Both are the same type so that
/// applying a delta is componentwise addition and needs no case
/// analysis.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FloatParts {
    finite: f64,
    positive_infinities: i64,
    negative_infinities: i64,
    nans: i64,
}

impl FloatParts {
    /// Nothing contributed.
    pub const EMPTY: Self = Self {
        finite: 0.0,
        positive_infinities: 0,
        negative_infinities: 0,
        nans: 0,
    };

    /// One cell contributing with `weight`, which is `1` for a row
    /// arriving and `-1` for one leaving.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub(crate) fn of(value: f64, weight: i64) -> Self {
        if value.is_nan() {
            return Self {
                nans: weight,
                ..Self::EMPTY
            };
        }
        if value.is_infinite() {
            return if value.is_sign_positive() {
                Self {
                    positive_infinities: weight,
                    ..Self::EMPTY
                }
            } else {
                Self {
                    negative_infinities: weight,
                    ..Self::EMPTY
                }
            };
        }
        Self {
            finite: value * weight as f64,
            ..Self::EMPTY
        }
    }

    /// A quantity that is only its finite part, for a seed or a cast.
    const fn finite(value: f64) -> Self {
        Self {
            finite: value,
            ..Self::EMPTY
        }
    }

    /// Add `other` in, componentwise.
    fn add(&mut self, other: Self) {
        self.finite += other.finite;
        self.positive_infinities += other.positive_infinities;
        self.negative_infinities += other.negative_infinities;
        self.nans += other.nans;
    }

    /// Whether this moves nothing.
    fn is_zero(&self) -> bool {
        self.finite == 0.0
            && self.positive_infinities == 0
            && self.negative_infinities == 0
            && self.nans == 0
    }

    /// The number these parts describe, which is what a caller reads.
    ///
    /// Measured on PostgreSQL 16.15: a `NaN` anywhere makes the sum
    /// `NaN`, infinities of both signs together make it `NaN`, one sign
    /// alone makes it that infinity however many finite rows there are,
    /// and otherwise it is the finite sum.
    const fn value(self) -> f64 {
        if self.nans != 0 || (self.positive_infinities != 0 && self.negative_infinities != 0) {
            return f64::NAN;
        }
        if self.positive_infinities != 0 {
            return f64::INFINITY;
        }
        if self.negative_infinities != 0 {
            return f64::NEG_INFINITY;
        }
        self.finite
    }
}

impl TotalDelta {
    /// This change as `f64`, for the functions that still average or
    /// square it. Lossy above `2^53`, which Phase D2 removes for `AVG`.
    #[allow(clippy::cast_precision_loss)]
    fn as_f64(&self) -> f64 {
        match self {
            Self::Integer(value) => *value as f64,
            Self::Decimal(value) => {
                <bigdecimal::BigDecimal as bigdecimal::ToPrimitive>::to_f64(value).unwrap_or(0.0)
            }
            Self::Real(parts) => parts.value(),
        }
    }

    /// This change as floating parts, for a total that accumulates in
    /// `f64`. An exact carrier contributes only a finite part.
    fn float_parts(&self) -> FloatParts {
        match self {
            Self::Real(parts) => *parts,
            other => FloatParts::finite(other.as_f64()),
        }
    }

    /// Add `other` into this change, keeping exactness where both sides
    /// have it and falling back to `f64` only where one side never had
    /// any.
    fn merge(&mut self, other: &Self) {
        match (&mut *self, other) {
            (Self::Integer(a), Self::Integer(b)) => *a += b,
            (Self::Decimal(a), Self::Decimal(b)) => *a += b,
            (Self::Decimal(a), Self::Integer(b)) => *a += bigdecimal::BigDecimal::from(*b),
            (Self::Integer(a), Self::Decimal(b)) => {
                *self = Self::Decimal(bigdecimal::BigDecimal::from(*a) + b);
            }
            // Two floating changes merge as their parts, so a batch that
            // both adds and removes an infinity nets to neither.
            (Self::Real(a), Self::Real(b)) => a.add(*b),
            (left, right) => {
                *left = Self::Real(FloatParts::finite(left.as_f64() + right.as_f64()));
            }
        }
    }

    /// Whether this change moves nothing.
    fn is_zero(&self) -> bool {
        match self {
            Self::Integer(value) => *value == 0,
            Self::Decimal(value) => value == &bigdecimal::BigDecimal::from(0),
            Self::Real(parts) => parts.is_zero(),
        }
    }
}

/// A set of rows summarised as its count, its sum, and its sum of
/// squared deviations from its own mean.
///
/// The last one is what makes a variance stable. `sum_sq / n - (sum / n)^2`
/// subtracts two large numbers, so at a large mean it keeps almost no
/// significant digits: measured over `100000000.0`, `100000001.0` and
/// `100000002.0`, `sum(x*x)` is `3.0000000600000004e+16` and that identity
/// answers `2.0` where PostgreSQL and MySQL both answer
/// `0.6666666666666666`. Accumulating the deviations instead reproduces
/// them digit for digit.
///
/// Summarised rather than kept row by row so that several rows at one
/// stream position merge into one delta without allocating, which is what
/// [`Spread::combine`] is for.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Spread {
    /// Rows in this set whose value is finite.
    pub rows: i64,
    /// Their sum.
    pub sum: f64,
    /// Their sum of squared deviations from their own mean.
    pub squared_deviations: f64,
    /// How many rows in this set hold a non-finite value.
    ///
    /// Counted apart rather than accumulated, for the reason the total
    /// keeps its parts apart: a squared deviation from a non-finite row
    /// is non-finite, so it destroys every finite deviation already
    /// accumulated and no removal can bring them back. Measured on
    /// PostgreSQL 16.15, `VAR_POP` over `Infinity, 1, 2` is `NaN` and,
    /// once the infinity is deleted, `0.25`, which is the spread of the
    /// two rows that remain.
    pub non_finite: i64,
}

impl Spread {
    /// The empty set.
    pub const EMPTY: Self = Self {
        rows: 0,
        sum: 0.0,
        squared_deviations: 0.0,
        non_finite: 0,
    };

    /// One row on its own, which deviates from its own mean by nothing.
    ///
    /// A non-finite row joins the count instead of the sum, so that
    /// taking it back out is subtraction rather than an attempt to undo
    /// an arithmetic that has no inverse.
    pub const fn of_one(value: f64) -> Self {
        if value.is_finite() {
            Self {
                rows: 1,
                sum: value,
                squared_deviations: 0.0,
                non_finite: 0,
            }
        } else {
            Self {
                non_finite: 1,
                ..Self::EMPTY
            }
        }
    }

    /// Whether any row here holds a non-finite value, in which case every
    /// deviation is non-finite and so is the answer.
    #[must_use]
    pub const fn has_non_finite(&self) -> bool {
        self.non_finite != 0
    }

    /// Two sets read as one.
    ///
    /// One row joining takes the engines' own per-row step, which
    /// subtracts before it divides. That is not a rearrangement for
    /// taste: with an infinity in play, `value * rows - sum` is
    /// `inf - inf` and the spread becomes `NaN`, which is what PostgreSQL
    /// answers, while dividing first would answer `Infinity`. A genuine
    /// batch, two sets each of several rows, takes the engines' combine
    /// step instead, whose term is the spread between the two means
    /// weighted by their sizes.
    ///
    /// The subtraction is deliberately unfused. A fused multiply-add is
    /// more accurate, and that is the problem: the engines compute
    /// `value * rows - sum` as two operations, and reproducing their
    /// answer means reproducing their rounding rather than improving on
    /// it.
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub fn combine(self, other: Self) -> Self {
        let non_finite = self.non_finite + other.non_finite;
        if self.rows == 0 {
            return Self {
                non_finite,
                ..other
            };
        }
        if other.rows == 0 {
            return Self { non_finite, ..self };
        }
        let rows = self.rows + other.rows;
        let sum = self.sum + other.sum;
        if other.rows == 1 {
            let deviation = other.sum * rows as f64 - sum;
            return Self {
                rows,
                sum,
                squared_deviations: self.squared_deviations
                    + deviation * deviation / (rows as f64 * self.rows as f64),
                non_finite,
            };
        }
        let between = self.sum / self.rows as f64 - other.sum / other.rows as f64;
        Self {
            rows,
            sum,
            squared_deviations: self.squared_deviations
                + other.squared_deviations
                + between * between * (self.rows as f64) * (other.rows as f64) / rows as f64,
            non_finite,
        }
    }

    /// `other` taken back out of this set, which is [`Spread::combine`]
    /// read backwards.
    ///
    /// One row leaving inverts the per-row step exactly, so adding a row
    /// and taking it back out returns the spread it started from.
    /// Emptying the set answers the empty one rather than a residue of
    /// rounding, which is what the engines answer for no rows.
    ///
    /// The subtraction is deliberately unfused. A fused multiply-add is
    /// more accurate, and that is the problem: the engines compute
    /// `value * rows - sum` as two operations, and reproducing their
    /// answer means reproducing their rounding rather than improving on
    /// it.
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    pub fn without(self, other: Self) -> Self {
        let non_finite = self.non_finite - other.non_finite;
        if other.rows == 0 {
            return Self { non_finite, ..self };
        }
        let rows = self.rows - other.rows;
        if rows <= 0 {
            return Self {
                non_finite,
                ..Self::EMPTY
            };
        }
        let sum = self.sum - other.sum;
        if other.rows == 1 {
            let deviation = other.sum * self.rows as f64 - self.sum;
            return Self {
                rows,
                sum,
                squared_deviations: self.squared_deviations
                    - deviation * deviation / (self.rows as f64 * rows as f64),
                non_finite,
            };
        }
        let between = sum / rows as f64 - other.sum / other.rows as f64;
        Self {
            rows,
            sum,
            squared_deviations: self.squared_deviations
                - other.squared_deviations
                - between * between * (rows as f64) * (other.rows as f64) / self.rows as f64,
            non_finite,
        }
    }
}

impl AggDelta {
    /// Add `other` into `self`. Both come from one subscription, so both are
    /// the same variant and a mismatch cannot happen.
    pub fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => *a += b,
            (
                Self::Totalled { value, count_delta },
                Self::Totalled {
                    value: v,
                    count_delta: c,
                },
            ) => {
                value.merge(v);
                *count_delta += c;
            }
            (
                Self::Stats {
                    value,
                    added,
                    removed,
                },
                Self::Stats {
                    value: v,
                    added: a,
                    removed: r,
                },
            ) => {
                value.merge(v);
                *added = added.combine(*a);
                *removed = removed.combine(*r);
            }
            // One subscription folds one delta shape, kept by dispatch
            // handing every event the same spec. A mismatch reaching here
            // would silently drop a contribution and corrupt the total, so
            // it fails loudly where tests run rather than being ignored.
            _ => debug_assert!(false, "AggDelta::merge got mismatched variants"),
        }
    }

    /// Whether every component is zero, meaning the value did not move.
    pub fn is_zero(&self) -> bool {
        match self {
            Self::Count(n) => *n == 0,
            Self::Totalled { value, count_delta } => value.is_zero() && *count_delta == 0,
            Self::Stats {
                value,
                added,
                removed,
            } => value.is_zero() && added.rows == 0 && removed.rows == 0,
        }
    }
}

/// Which aggregate a running value is read through.
///
/// Eight, not seven: `COUNT(*)` and `COUNT(col)` read different counters,
/// rows matched against rows whose value is not NULL, so collapsing them
/// would put the choice back into a field that means two things.
/// [`crate::HavingFunction`] is the seven of these that aggregate a
/// column, and maps into this by [`crate::HavingFunction::kind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggKind {
    /// `COUNT(*)`, over rows matched.
    CountStar,
    /// `COUNT(col)`, over rows whose value is not NULL.
    CountColumn,
    /// `SUM(col)`.
    Sum,
    /// `AVG(col)`.
    Avg,
    /// `VAR_POP(col)`.
    VarPop,
    /// `VAR_SAMP(col)`.
    VarSamp,
    /// `STDDEV_POP(col)`.
    StddevPop,
    /// `STDDEV_SAMP(col)`.
    StddevSamp,
}

/// How one subscription's fold answers, resolved once at registration.
///
/// Three facts that all follow from the summed column and the engine, and
/// that a running value needs together: what a total accumulates in, what
/// a mean answers, and how this engine divides. Carried as one descriptor
/// because an accumulator is not generic over its backend.
#[derive(Clone, Copy, Debug)]
pub struct FoldRule {
    /// What a total accumulates in.
    pub total: crate::backend::SumRule,
    /// What a mean answers when the total is exact.
    pub mean: crate::backend::MeanRule,
    /// How this engine divides, which is what a mean is. Never read by a
    /// fold that computes no mean, and registration refuses a mean whose
    /// engine needs a setting it was not given.
    pub quotient: crate::compiler::bytecode::Quotient,
    /// Which shape this engine's variance seed arrives in.
    pub variance_seed: crate::backend::VarianceSeed,
    /// What this engine answers when a floating total leaves its range.
    pub float_overflow: crate::backend::FloatSumOverflow,
}

/// A running total, in the type its engine sums into.
///
/// Built from [`SumRule`], so the boundary each engine raises at travels
/// with the number rather than being checked somewhere central.
#[derive(Clone, Debug)]
enum Total {
    /// Exact in 64 bits. `promotes` is SQLite's rule: a non-integer value
    /// joining turns the total into a double, where PostgreSQL's
    /// `sum(int)` can never see one.
    ///
    /// `reals` holds what those non-integer values contribute, kept apart
    /// from `value` rather than folded into it. The promotion is a
    /// property of the rows present, not of the rows that have ever been
    /// present: measured on SQLite 3.51.1, an `INTEGER` column holding
    /// `i64::MAX` and `0.5` sums to `9.223372036854776e+18` typed real,
    /// and once the `0.5` is deleted it sums to `9223372036854775807`
    /// typed integer, and two rows of `i64::MAX` then answer `integer
    /// overflow` again. Replacing the accumulator threw the integer away,
    /// so nothing could restore it, and a later overflow rounded in
    /// silence where the engine raises.
    Integer {
        value: i64,
        promotes: bool,
        reals: FloatParts,
    },
    /// Exact decimal, bounded by the engine's integer-digit ceiling when
    /// it has a reachable one.
    Decimal {
        value: bigdecimal::BigDecimal,
        integer_digits: Option<u32>,
    },
    /// Single precision, which is what PostgreSQL's `sum(real)`
    /// accumulates in. Every addition rounds the finite part, so the
    /// total is held at that width rather than rounded once on the way
    /// out.
    Single(FloatParts, FloatBoundary),
    /// A double.
    Double(FloatParts, FloatBoundary),
}

/// What a floating accumulator does when its finite part leaves range.
///
/// Carried beside the parts rather than checked centrally, so the
/// boundary travels with the number exactly as the exact accumulators'
/// boundaries do.
#[derive(Clone, Copy, Debug)]
struct FloatBoundary(crate::backend::FloatSumOverflow);

impl FloatBoundary {
    /// Apply this engine's rule to a finite part that has just left
    /// range, or leave it alone when it has not.
    ///
    /// Only the finite part is passed, and only ever finite values are
    /// added to it: a non-finite contribution is counted apart, so it
    /// cannot reach here. The mutation battery is what established that,
    /// by surviving a guard against non-finite operands, which was
    /// unreachable and is gone.
    const fn settle(self, total: f64) -> Result<f64, SumOutOfRange> {
        if total.is_finite() {
            return Ok(total);
        }
        match self.0 {
            // Both stop, for different reasons that reach the caller
            // the same way: PostgreSQL has no answer to give, and
            // MySQL's answer is not one a running total can hold.
            crate::backend::FloatSumOverflow::Raises
            | crate::backend::FloatSumOverflow::Unmaintainable => Err(SumOutOfRange),
            crate::backend::FloatSumOverflow::Saturates => Ok(total),
        }
    }
}

/// The total cannot be represented as this engine would represent it, so
/// there is no answer to report.
///
/// Measured: SQLite answers `integer overflow` past 64 bits and
/// PostgreSQL answers `value overflows numeric format` past 131072
/// integer digits, both reachable from two rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SumOutOfRange;

impl Total {
    /// An empty total under `rule`.
    fn empty(rule: crate::backend::SumRule, boundary: FloatBoundary) -> Self {
        match rule {
            crate::backend::SumRule::Integer => Self::Integer {
                value: 0,
                promotes: false,
                reals: FloatParts::EMPTY,
            },
            crate::backend::SumRule::IntegerPromotingToDouble => Self::Integer {
                value: 0,
                promotes: true,
                reals: FloatParts::EMPTY,
            },
            crate::backend::SumRule::Decimal { integer_digits } => Self::Decimal {
                value: bigdecimal::BigDecimal::from(0),
                integer_digits,
            },
            crate::backend::SumRule::Single => Self::Single(FloatParts::EMPTY, boundary),
            crate::backend::SumRule::Double => Self::Double(FloatParts::EMPTY, boundary),
        }
    }

    /// Add one row's contribution.
    ///
    /// # Errors
    ///
    /// [`SumOutOfRange`] where the engine itself would raise.
    fn add(&mut self, delta: &TotalDelta) -> Result<(), SumOutOfRange> {
        match self {
            Self::Integer {
                value,
                promotes,
                reals,
            } => match delta {
                TotalDelta::Integer(change) => {
                    let total = i128::from(*value) + change;
                    // The exact half keeps its own boundary even while a
                    // real value is live, so the boundary is back the
                    // moment the last one leaves rather than gone for
                    // good.
                    *value = i64::try_from(total).map_err(|_| SumOutOfRange)?;
                    Ok(())
                }
                // A non-integer value joined. SQLite turns the total real
                // while one is live; an engine whose integer sum cannot
                // see one has a catalog and a stream that disagree, and no
                // answer to give.
                _ if *promotes => {
                    reals.add(delta.float_parts());
                    Ok(())
                }
                _ => Err(SumOutOfRange),
            },
            Self::Decimal {
                value,
                integer_digits,
            } => {
                *value += match delta {
                    TotalDelta::Integer(change) => bigdecimal::BigDecimal::from(*change),
                    TotalDelta::Decimal(change) => change.clone(),
                    // A floating change joining an exact total has to be
                    // representable as a decimal, which a non-finite one
                    // is not: that is the engine's own out-of-range, not
                    // a rounding choice.
                    TotalDelta::Real(change) => {
                        <bigdecimal::BigDecimal as bigdecimal::FromPrimitive>::from_f64(
                            change.value(),
                        )
                        .ok_or(SumOutOfRange)?
                    }
                };
                match integer_digits {
                    Some(limit) if integer_digits_of(value) > u64::from(*limit) => {
                        Err(SumOutOfRange)
                    }
                    _ => Ok(()),
                }
            }
            // Rounded per addition, because the engine's accumulator is
            // this wide and a total rounded only once at the end is a
            // different number: measured, `16777216 + 1 + 1` keeps both
            // units in double and loses both in single.
            Self::Single(parts, boundary) => {
                parts.add(delta.float_parts());
                #[allow(clippy::cast_possible_truncation)]
                let rounded = f64::from(parts.finite as f32);
                parts.finite = boundary.settle(rounded)?;
                Ok(())
            }
            Self::Double(parts, boundary) => {
                parts.add(delta.float_parts());
                parts.finite = boundary.settle(parts.finite)?;
                Ok(())
            }
        }
    }

    /// Seed this total from a decoded component cell.
    fn seed<B: Backend>(&mut self, cell: &Value<B>) {
        use core::any::Any;
        match self {
            Self::Integer { value, reals, .. } => {
                *reals = FloatParts::EMPTY;
                match cell {
                    Value::Int(int) => {
                        if let Some(int) = (int as &dyn Any).downcast_ref::<i64>() {
                            *value = *int;
                        }
                    }
                    // A seed read back as a real is an engine reporting a
                    // sum that has already promoted, so it seeds the real
                    // half. Measured, SQLite answers `typeof real` for
                    // exactly that set.
                    Value::Float(_) => {
                        *value = 0;
                        *reals = FloatParts::finite(AggAccumulator::seed_f64(cell).unwrap_or(0.0));
                    }
                    _ => {}
                }
            }
            Self::Decimal { value, .. } => match cell {
                Value::Decimal(decimal) => {
                    if let Some(decimal) =
                        (decimal as &dyn Any).downcast_ref::<bigdecimal::BigDecimal>()
                    {
                        *value = decimal.clone();
                    }
                }
                Value::Int(int) => {
                    if let Some(int) = (int as &dyn Any).downcast_ref::<i64>() {
                        *value = bigdecimal::BigDecimal::from(*int);
                    }
                }
                _ => {}
            },
            Self::Single(parts, _) => {
                #[allow(clippy::cast_possible_truncation)]
                let seeded = AggAccumulator::seed_f64(cell).unwrap_or(0.0) as f32;
                *parts = FloatParts::finite(f64::from(seeded));
            }
            Self::Double(parts, _) => {
                *parts = FloatParts::finite(AggAccumulator::seed_f64(cell).unwrap_or(0.0));
            }
        }
    }

    /// Empty this total, keeping the rule it was built with.
    ///
    /// In place rather than rebuilt. Reconstructing it from a rule read
    /// back off the current state is how a `TRUNCATE` used to cost
    /// SQLite its integer boundary: the state after a promotion reported
    /// the promoted rule, not the declared one. Nothing infers a rule
    /// from a value any more.
    fn clear(&mut self) {
        match self {
            Self::Integer { value, reals, .. } => {
                *value = 0;
                *reals = FloatParts::EMPTY;
            }
            Self::Decimal { value, .. } => *value = bigdecimal::BigDecimal::from(0),
            Self::Single(parts, _) | Self::Double(parts, _) => *parts = FloatParts::EMPTY,
        }
    }

    /// The total as a caller reads it.
    fn value(&self) -> crate::NumericValue {
        match self {
            // Real while a non-integer contribution is live, exact
            // again once none is, which is what SQLite answers for the
            // rows present.
            #[allow(clippy::cast_precision_loss)]
            Self::Integer { value, reals, .. } if !reals.is_zero() => {
                crate::NumericValue::Double(*value as f64 + reals.value())
            }
            Self::Integer { value, .. } => crate::NumericValue::Integer(*value),
            Self::Decimal { value, .. } => crate::NumericValue::Decimal(value.clone()),
            // Reported as a double, because that is the carrier a caller
            // reads a floating total through. The width belongs to the
            // accumulation, and widening an `f32` is exact.
            Self::Single(parts, _) | Self::Double(parts, _) => {
                crate::NumericValue::Double(parts.value())
            }
        }
    }
}

/// A reported total or mean as `f64`, for the one comparison that still needs
/// one. Lossy above `2^53`, which Phase D2 removes.
#[allow(clippy::cast_precision_loss)]
fn numeric_as_f64(total: &crate::NumericValue) -> Option<f64> {
    match total {
        crate::NumericValue::Integer(value) => Some(*value as f64),
        crate::NumericValue::Decimal(value) => {
            <bigdecimal::BigDecimal as bigdecimal::ToPrimitive>::to_f64(value)
        }
        crate::NumericValue::Double(value) => Some(*value),
    }
}

/// Digits `value` carries ahead of the decimal point.
fn integer_digits_of(value: &bigdecimal::BigDecimal) -> u64 {
    let (digits, scale) = value.as_bigint_and_exponent();
    let spelled = <bigdecimal::num_bigint::BigInt as Clone>::clone(&digits)
        .magnitude()
        .to_string();
    let length = i128::try_from(spelled.len()).unwrap_or(i128::MAX);
    u64::try_from(length - i128::from(scale)).unwrap_or(0)
}

/// Running value of one aggregate subscription: three sums plus the function
/// they are read through.
///
/// Internal. The engine holds one of these per aggregate subscription and
/// reports [`AggValue`], so a caller never folds anything itself.
#[derive(Clone, Debug)]
pub struct AggAccumulator {
    kind: AggKind,
    /// Rows matched, whatever their value, which is what `COUNT(*)`
    /// counts.
    ///
    /// Maintained only for [`AggKind::CountStar`], which is the only
    /// aggregate that reads it and the only one whose bootstrap row
    /// carries it: a `SUM` subscription is seeded from `COUNT(col)`, so
    /// this crate cannot know how many rows it matched without asking a
    /// different question of the database. Zero for every other kind, and
    /// no other kind reads it.
    ///
    /// Separate from `contributions` regardless, because one field served
    /// both before, chosen by which spec was registered, so its meaning
    /// depended on state elsewhere and no reader could tell which it held.
    rows_matched: i64,
    /// Rows whose value is not NULL, which is what every aggregate but
    /// `COUNT(*)` counts, and what decides whether one answers NULL.
    contributions: i64,
    sum: f64,
    /// The variance family's own state: the rows so far, their sum, and
    /// their sum of squared deviations. Held beside `count` and `sum`
    /// because a `COUNT` or a `SUM` maintains no spread at all.
    spread: Spread,
    /// `SUM`'s own running total, exact in the type its engine sums into.
    /// `sum` above stays beside it because the variance family and a
    /// widened `HAVING` still read squares from it, which Phase D4 takes
    /// up.
    total: Total,
    /// What a mean answers on this engine.
    mean_rule: crate::backend::MeanRule,
    /// Which shape a variance seed arrives in on this engine.
    variance_seed: crate::backend::VarianceSeed,
    /// How this engine divides, for the mean.
    quotient_rule: crate::compiler::bytecode::Quotient,
}

impl AggAccumulator {
    /// An empty accumulator for the aggregate described by `spec`,
    /// answering under `rule`.
    pub fn from_spec(spec: &AggSpec, rule: FoldRule) -> Self {
        use AggSpec as S;
        let kind = match spec {
            S::CountStar => AggKind::CountStar,
            S::CountColumn { .. } => AggKind::CountColumn,
            S::Sum { .. } => AggKind::Sum,
            S::Avg { .. } => AggKind::Avg,
            S::VarPop { .. } => AggKind::VarPop,
            S::VarSamp { .. } => AggKind::VarSamp,
            S::StddevPop { .. } => AggKind::StddevPop,
            S::StddevSamp { .. } => AggKind::StddevSamp,
        };
        Self {
            kind,
            rows_matched: 0,
            contributions: 0,
            sum: 0.0,
            spread: Spread::EMPTY,
            total: Total::empty(rule.total, FloatBoundary(rule.float_overflow)),
            mean_rule: rule.mean,
            variance_seed: rule.variance_seed,
            quotient_rule: rule.quotient,
        }
    }

    /// Seed an accumulator from a bootstrap component row produced by
    /// [`AggregateBootstrap`](crate::AggregateBootstrap).
    ///
    /// Consumes the components in the documented column order: `[c]` for
    /// COUNT, `[s, c]` for SUM, `[s, c]` for AVG, and `[s, sq, c]` for the
    /// variance and stddev family. A zero-row result (COUNT `0`, NULL
    /// sum components) seeds the empty-aggregate state, matching the
    /// "set went empty" semantics of the re-execution family.
    pub fn seed_from_row<B: Backend>(spec: &AggSpec, rule: FoldRule, row: &[Value<B>]) -> Self {
        use AggSpec as S;
        let mut acc = Self::from_spec(spec, rule);
        match spec {
            S::CountStar => {
                acc.rows_matched = row.first().and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
            S::CountColumn { .. } => {
                acc.contributions = row.first().and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
            S::Sum { .. } => {
                if let Some(cell) = row.first() {
                    acc.total.seed(cell);
                }
                acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
                acc.contributions = row.get(1).and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
            S::Avg { .. } => {
                if let Some(cell) = row.first() {
                    acc.total.seed(cell);
                }
                acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
                acc.contributions = row.get(1).and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
            S::VarPop { .. } | S::VarSamp { .. } | S::StddevPop { .. } | S::StddevSamp { .. } => {
                acc.seed_spread(row);
            }
        }
        acc
    }

    /// Seed from the complete component layout (`sum`, `sum_sq`, `count`),
    /// used when a sibling `HAVING` widens the seed regardless of the
    /// projected function.
    pub fn seed_stats_row<B: Backend>(spec: &AggSpec, rule: FoldRule, row: &[Value<B>]) -> Self {
        let mut acc = Self::from_spec(spec, rule);
        if let Some(cell) = row.first() {
            acc.total.seed(cell);
        }
        acc.seed_spread(row);
        acc
    }

    /// Seed the variance family's state from `[sum, squared_deviations,
    /// count]`.
    ///
    /// The middle component is the engine's own sum of squared
    /// deviations, which it computes stably, rather than a sum of
    /// squares: re-deriving the deviations from squares would put the
    /// cancellation this phase removed back at seed time.
    #[allow(clippy::cast_precision_loss)]
    fn seed_spread<B: Backend>(&mut self, row: &[Value<B>]) {
        self.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
        self.contributions = row.get(2).and_then(|v| Self::seed_i64(v)).unwrap_or(0);
        let component = row.get(1).and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
        let squared_deviations = match self.variance_seed {
            crate::backend::VarianceSeed::EnginesOwn => component,
            // Derived, because this engine has no variance function to
            // ask: the one place the cancellation survives, and only for
            // an engine that cannot express the stable answer at all.
            crate::backend::VarianceSeed::SumOfSquares if self.contributions > 0 => {
                component - self.sum * self.sum / self.contributions as f64
            }
            crate::backend::VarianceSeed::SumOfSquares => 0.0,
        };
        self.spread = Spread {
            rows: self.contributions,
            sum: self.sum,
            squared_deviations,
            // A seed read from the engine reports over rows the engine
            // has already folded, so nothing here is pending as
            // non-finite: whatever it answered is the answer.
            non_finite: 0,
        };
    }

    /// Decode a numeric component cell to `f64`. NULL/Missing/non-numeric
    /// cells return `None`, and the caller defaults them to `0.0`.
    ///
    /// That default is safe because every function here reports through its
    /// count, so a zero-count accumulator reports the empty state whatever
    /// its sum holds. `SUM` was the exception until it maintained a count
    /// of its own: a NULL seed became `0.0` and was reported as a total of
    /// zero, where all three engines answer NULL.
    #[allow(clippy::cast_precision_loss)]
    fn seed_f64<B: Backend>(v: &Value<B>) -> Option<f64> {
        use core::any::Any;
        match v {
            // i64 -> f64 loses precision above 2^53; the seed path accepts
            // the same bounded loss the delta path (`probe_column_for_agg`)
            // already does for realistic aggregate magnitudes.
            Value::Int(i) => (i as &dyn Any).downcast_ref::<i64>().map(|x| *x as f64),
            Value::Float(f) => (f as &dyn Any)
                .downcast_ref::<f64>()
                .copied()
                .filter(|x| x.is_finite()),
            // NUMERIC/DECIMAL sums (e.g. Postgres `SUM(int_col)`) arrive as
            // BigDecimal; parse through its decimal string to avoid a
            // num-traits import.
            Value::Decimal(d) => (d as &dyn Any)
                .downcast_ref::<bigdecimal::BigDecimal>()
                .and_then(|x| sql_scalar_text::parse_f64(&x.to_string())),
            _ => None,
        }
    }

    /// Decode the COUNT component cell to `i64`. COUNT is exact and integer
    /// on every backend, so only `Value::Int` is accepted.
    fn seed_i64<B: Backend>(v: &Value<B>) -> Option<i64> {
        use core::any::Any;
        match v {
            Value::Int(i) => (i as &dyn Any).downcast_ref::<i64>().copied(),
            _ => None,
        }
    }

    /// Fold one delta into the running value.
    ///
    /// # Errors
    ///
    /// [`SumOutOfRange`] when the total leaves what its engine can
    /// represent, which is the point the engine itself raises.
    pub fn apply(&mut self, delta: &AggDelta) -> Result<(), SumOutOfRange> {
        match delta {
            // `COUNT(*)` counts rows, every other count counts values.
            // The producer already applied the NULL filter for the
            // latter, emitting no delta at all for an absent cell, so the
            // only thing left to decide here is which counter it lands in.
            AggDelta::Count(delta) => match self.kind {
                AggKind::CountStar => self.rows_matched += delta,
                _ => self.contributions += delta,
            },
            AggDelta::Totalled { value, count_delta } => {
                self.total.add(value)?;
                self.sum += value.as_f64();
                self.contributions += count_delta;
            }
            AggDelta::Stats {
                value,
                added,
                removed,
            } => {
                self.total.add(value)?;
                self.sum += value.as_f64();
                self.contributions += added.rows - removed.rows;
                self.spread = self.spread.combine(*added).without(*removed);
            }
        }
        Ok(())
    }

    /// Empty the running value, which is exactly the state a re-read of an
    /// emptied table would seed.
    pub fn clear(&mut self) {
        // Both counters, because an emptied table matched no rows and
        // received no values. Resetting only the contributions left a
        // `COUNT(*)` reporting its old row count after a truncate, which
        // `a_truncate_zeroes_a_held_total_and_reports_it` caught.
        self.rows_matched = 0;
        self.contributions = 0;
        self.sum = 0.0;
        self.spread = Spread::EMPTY;
        self.total.clear();
    }

    /// Current aggregate value.
    pub fn value(&self) -> AggValue {
        self.value_of(self.kind)
    }

    /// The value this held state renders as, read through `kind`.
    ///
    /// One renderer, because the projected value and the value a sibling
    /// `HAVING` function reads are the same formulas over the same held
    /// components. Two copies could only ever differ by mistake, and the
    /// mistake would be silent: both answers are plausible.
    fn value_of(&self, kind: AggKind) -> AggValue {
        match kind {
            AggKind::CountStar => AggValue::CountStar(self.rows_matched),
            AggKind::CountColumn => AggValue::CountColumn(self.contributions),
            AggKind::Sum => AggValue::Sum(self.reported_total()),
            AggKind::Avg => AggValue::Avg(self.mean()),
            AggKind::VarPop => AggValue::VarPop(self.var_pop()),
            AggKind::VarSamp => AggValue::VarSamp(self.var_samp()),
            AggKind::StddevPop => AggValue::StddevPop(self.var_pop().map(f64::sqrt)),
            AggKind::StddevSamp => AggValue::StddevSamp(self.var_samp().map(f64::sqrt)),
        }
    }

    /// The total, or `None` when no row contributes one.
    ///
    /// Measured on all three engines: `SUM` over no rows, and over rows
    /// whose value is NULL, answers NULL, while one row worth zero answers
    /// `0`. The count is what separates them, so the total is read through
    /// it rather than reported raw.
    fn reported_total(&self) -> Option<crate::NumericValue> {
        (self.contributions > 0).then(|| self.total.value())
    }

    /// The mean, or `None` when no row contributes one.
    ///
    /// A mean is a quotient, so it is the engine's own division of the
    /// exact total by the count: measured, PostgreSQL answers
    /// `1.5000000000000000` for 1 and 2 over an `int` column and MySQL
    /// compares `1.666666666` for 1, 2 and 2, each being that engine's
    /// `/` applied to the pair. SQLite answers a real, and a floating
    /// total is a double everywhere.
    #[allow(clippy::cast_precision_loss)]
    fn mean(&self) -> Option<crate::NumericValue> {
        if self.contributions <= 0 {
            return None;
        }
        let double = || crate::NumericValue::Double(self.sum / self.contributions as f64);
        match (&self.total, self.mean_rule) {
            // A mean never holds a single-precision total, because
            // `catalog_helpers::total_rule` gives one only to a `SUM`:
            // measured, PostgreSQL's `avg(real)` is `double precision`
            // and answers the double quotient of the double sum. Read
            // through the widened total, which is what that quotient is,
            // so both widths answer alike here.
            (Total::Double(total, _) | Total::Single(total, _), _) => Some(
                crate::NumericValue::Double(total.value() / self.contributions as f64),
            ),
            (_, crate::backend::MeanRule::Double) => Some(double()),
            (Total::Integer { value, .. }, crate::backend::MeanRule::Exact) => Some(
                crate::NumericValue::Decimal(self.quotient(&bigdecimal::BigDecimal::from(*value))),
            ),
            (Total::Decimal { value, .. }, crate::backend::MeanRule::Exact) => {
                Some(crate::NumericValue::Decimal(self.quotient(value)))
            }
        }
    }

    /// `total / count` as this engine divides, which registration resolved
    /// into the quotient rule this carries.
    fn quotient(&self, total: &bigdecimal::BigDecimal) -> bigdecimal::BigDecimal {
        match self.quotient_rule {
            crate::compiler::bytecode::Quotient::InWordsAt(increment) => {
                crate::compiler::vm::arithmetic::quotient_in_words(
                    total,
                    &bigdecimal::BigDecimal::from(self.contributions),
                    increment,
                )
            }
            crate::compiler::bytecode::Quotient::FromTheOperands => {
                crate::compiler::vm::arithmetic::quotient_at_significant_digits(
                    total,
                    &bigdecimal::BigDecimal::from(self.contributions),
                )
            }
        }
    }

    /// The value a sibling family function reads from the held components.
    /// Meaningful only when the components are maintained in full, which the
    /// widened delta and seed paths guarantee.
    pub fn value_as(&self, function: crate::HavingFunction) -> AggValue {
        self.value_of(function.kind())
    }

    /// The population variance, which is the spread divided by the rows.
    ///
    /// A live non-finite row answers `NaN` whatever the finite rows
    /// hold, because every deviation from a non-finite mean is
    /// non-finite: measured, PostgreSQL's `var_pop` over `1.0` and
    /// `Infinity` is `NaN`. Those rows are counted rather than
    /// accumulated, so the answer asks the count and the finite spread
    /// stays intact underneath, ready for when the last one leaves.
    #[allow(clippy::cast_precision_loss)]
    fn var_pop(&self) -> Option<f64> {
        if self.spread.has_non_finite() {
            return Some(f64::NAN);
        }
        (self.spread.rows > 0).then(|| self.deviations() / self.spread.rows as f64)
    }

    /// The sample variance, undefined below two rows, which is the NULL
    /// every engine answers there.
    #[allow(clippy::cast_precision_loss)]
    fn var_samp(&self) -> Option<f64> {
        if self.spread.has_non_finite() {
            // Two rows are still needed for the answer to exist at all,
            // and a non-finite row is one of them.
            return (self.spread.rows + self.spread.non_finite >= 2).then_some(f64::NAN);
        }
        (self.spread.rows >= 2).then(|| self.deviations() / (self.spread.rows - 1) as f64)
    }

    /// The accumulated deviations, with rounding's negatives read as
    /// zero.
    ///
    /// The sum of squared deviations of a real set cannot be negative, so
    /// a negative here is rounding left over from taking rows back out,
    /// and a standard deviation must not answer `NaN` for a set whose
    /// spread has merely gone.
    ///
    /// Written as a comparison rather than `f64::max`, which answers the
    /// other operand for a `NaN` and would swallow the one case where
    /// `NaN` is the engine's own answer: measured, PostgreSQL's `var_pop`
    /// over `1.0` and `Infinity` is `NaN`. `NaN < 0.0` is false, so it
    /// travels through.
    fn deviations(&self) -> f64 {
        let deviations = self.spread.squared_deviations;
        if deviations < 0.0 {
            0.0
        } else {
            deviations
        }
    }
}

/// Default ceiling on the changes one aggregate subscription holds while its
/// starting numbers are being read.
///
/// The engine keeps a change per stream position until the numbers land, so it
/// can drop the ones the read already saw instead of counting them twice. The
/// list is bounded because it grows with how long the read takes on a busy
/// table. Past the ceiling the seed is refused and the caller reads again, so
/// the cost of this number being too small is a retry, never a wrong total.
pub const DEFAULT_MAX_CHANGES_DURING_AGGREGATE_READ: usize = 4096;

/// Default maximum live groups for one grouped aggregate subscription.
pub const DEFAULT_MAX_GROUPS_PER_AGGREGATE: usize = 1024;

/// What happened to a total at one stream position, held only until the
/// starting numbers arrive.
#[derive(Clone, Debug)]
enum PendingChange {
    /// A matched row change, already folded to a delta.
    Fold(AggDelta),
    /// The table was emptied, which voids every number read before it.
    Emptied,
}

/// Changes seen while waiting for the starting numbers.
struct Pending<C: Checkpoint> {
    /// In arrival order, which is non-decreasing in position.
    changes: Vec<(Option<C>, PendingChange)>,
    /// Set once the list hit its ceiling, at which point the changes the read
    /// already saw can no longer be identified and the seed must be refused.
    overflowed: bool,
}

impl<C: Checkpoint> Pending<C> {
    const fn new() -> Self {
        Self {
            changes: Vec::new(),
            overflowed: false,
        }
    }

    /// Record `change` at `at`, merging into the last entry when the position
    /// repeats so several changes inside one transaction cost one slot.
    fn push(&mut self, at: Option<&C>, change: PendingChange, cap: usize) {
        if let (Some((last_at, PendingChange::Fold(held))), PendingChange::Fold(delta)) =
            (self.changes.last_mut(), &change)
        {
            if last_at.is_some() && last_at.as_ref() == at {
                held.merge(delta);
                return;
            }
        }
        if self.changes.len() >= cap {
            self.overflowed = true;
            return;
        }
        self.changes.push((at.cloned(), change));
    }
}

/// One aggregate subscription's running value.
pub struct AggregateTotal<I: IdTypes, C: Checkpoint> {
    /// The consumer the registration belongs to, reported alongside the value.
    consumer: I::ConsumerId,
    /// Which function is maintained, needed to read the sums and to decode a
    /// seed row's components.
    spec: AggSpec,
    /// How this subscription's fold answers, resolved at registration.
    rule: FoldRule,
    accumulator: AggAccumulator,
    /// `None` once the starting numbers have landed.
    pending: Option<Pending<C>>,
}

impl<I: IdTypes, C: Checkpoint> AggregateTotal<I, C> {
    pub fn new(consumer: I::ConsumerId, spec: AggSpec, rule: FoldRule) -> Self {
        Self {
            consumer,
            accumulator: AggAccumulator::from_spec(&spec, rule),
            spec,
            rule,
            pending: Some(Pending::new()),
        }
    }

    pub const fn consumer(&self) -> I::ConsumerId {
        self.consumer
    }

    /// The value held right now, or `None` while the starting numbers are
    /// still missing.
    pub fn value(&self) -> Option<AggValue> {
        self.pending.is_none().then(|| self.accumulator.value())
    }

    /// Fold one change. Answers with the new value when the total is seeded,
    /// and with nothing while it is not, since a total covering only the last
    /// few seconds is worse than silence.
    pub fn fold(
        &mut self,
        delta: AggDelta,
        at: Option<&C>,
        cap: usize,
    ) -> Result<Option<AggValue>, SumOutOfRange> {
        if let Some(pending) = &mut self.pending {
            pending.push(at, PendingChange::Fold(delta), cap);
            return Ok(None);
        }
        self.accumulator.apply(&delta)?;
        Ok(Some(self.accumulator.value()))
    }

    /// Empty the total because the table was truncated. Answers with the new
    /// value when that moved it, and with nothing when it was already empty or
    /// the total is unseeded.
    ///
    /// No re-read is needed: an emptied table's components are all zero, which
    /// is what a seed over it would decode to.
    pub fn empty(&mut self, at: Option<&C>, cap: usize) -> Option<AggValue> {
        if let Some(pending) = &mut self.pending {
            pending.push(at, PendingChange::Emptied, cap);
            return None;
        }
        let before = self.accumulator.value();
        self.accumulator.clear();
        let after = self.accumulator.value();
        (before != after).then_some(after)
    }

    /// Empty the total and mark it unseeded again, for a permission change the
    /// engine cannot see.
    pub fn reset(&mut self) {
        self.accumulator.clear();
        self.pending = Some(Pending::new());
    }

    /// Adopt `row` as the starting numbers, read at `read_at`.
    ///
    /// Every change recorded at or before `read_at` is already inside `row`
    /// and is dropped. `read_at` is taken before the read's snapshot opens, so
    /// that direction is the safe one: it can only keep a change the numbers
    /// already hold, which the position comparison then removes.
    pub fn install<B: Backend>(
        &mut self,
        subscription: SubscriptionId,
        row: &[Value<B>],
        read_at: Option<&C>,
        cap: usize,
    ) -> Result<AggValue, AggregateInstallError> {
        let Some(pending) = self.pending.as_ref() else {
            return Err(AggregateInstallError::AlreadySeeded(subscription));
        };
        if pending.overflowed {
            return Err(AggregateInstallError::TooManyChangesDuringRead { subscription, cap });
        }
        if !pending.changes.is_empty()
            && (read_at.is_none() || pending.changes.iter().any(|(at, _)| at.is_none()))
        {
            return Err(AggregateInstallError::PositionUnknown(subscription));
        }

        let mut accumulator = AggAccumulator::seed_from_row(&self.spec, self.rule, row);
        for (at, change) in &pending.changes {
            if at.as_ref() <= read_at {
                continue;
            }
            match change {
                // A change held through the read window can put the total
                // out of range exactly as a live one can, and the caller
                // learns the same way: the install fails and the tier is
                // re-read.
                PendingChange::Fold(delta) => accumulator
                    .apply(delta)
                    .map_err(|_| AggregateInstallError::SumOutOfRange(subscription))?,
                PendingChange::Emptied => accumulator.clear(),
            }
        }

        self.accumulator = accumulator;
        self.pending = None;
        Ok(self.accumulator.value())
    }
}

/// One live grouped SQL result row.
struct GroupValue<B: Backend> {
    values: Vec<Value<B>>,
    accumulator: AggAccumulator,
    rows: i64,
    /// Whether the group is currently announced to the consumer.
    announced: bool,
}

impl<B: Backend> GroupValue<B> {
    fn identity(&self, key: &[u8]) -> crate::GroupIdentity<B> {
        crate::GroupIdentity {
            key: key.to_vec(),
            values: self.values.clone(),
        }
    }

    fn into_identity(self, key: Vec<u8>) -> crate::GroupIdentity<B> {
        crate::GroupIdentity {
            key,
            values: self.values,
        }
    }
}

/// A fold's `HAVING`, compiled once at registration: the subject to read
/// and the parsed numeric threshold.
struct GroupHaving {
    subject: crate::HavingSubject,
    op: crate::HavingOp,
    threshold: f64,
}

impl GroupHaving {
    /// Whether the group currently belongs to the announced result. An
    /// unknown value (an empty average) never passes, matching SQL's
    /// UNKNOWN.
    #[allow(
        clippy::cast_precision_loss,
        reason = "counts beyond 2^53 accept the same loss the delta path does"
    )]
    fn passes<B: Backend>(&self, group: &GroupValue<B>) -> bool {
        let value = match self.subject {
            crate::HavingSubject::RowCount => Some(group.rows as f64),
            // A `SUM` over zero contributions is NULL and a comparison
            // over NULL is UNKNOWN, which the value itself now says: this
            // used to re-derive it here from the contribution count.
            crate::HavingSubject::Aggregate(function) => {
                match group.accumulator.value_as(function) {
                    AggValue::CountStar(count) | AggValue::CountColumn(count) => Some(count as f64),
                    // A `HAVING` compares in `f64`, which Phase D2 makes
                    // exact along with `AVG`.
                    AggValue::Sum(value) | AggValue::Avg(value) => {
                        value.as_ref().and_then(numeric_as_f64)
                    }
                    AggValue::VarPop(real)
                    | AggValue::VarSamp(real)
                    | AggValue::StddevPop(real)
                    | AggValue::StddevSamp(real) => real,
                }
            }
        };
        value
            .and_then(|value| value.partial_cmp(&self.threshold))
            .is_some_and(|ordering| self.op.admits(ordering))
    }
}

/// One grouped change held until all seed rows arrive.
#[derive(Clone, Debug)]
enum PendingGroupChange<B: Backend> {
    Fold {
        key: Vec<u8>,
        values: Vec<Value<B>>,
        delta: Option<AggDelta>,
        rows: i64,
    },
    Emptied,
}

/// Grouped changes seen while the database seed read is in flight.
struct PendingGroups<B: Backend, C: Checkpoint> {
    changes: Vec<(Option<C>, PendingGroupChange<B>)>,
    overflowed: bool,
}

impl<B: Backend, C: Checkpoint> PendingGroups<B, C> {
    const fn new() -> Self {
        Self {
            changes: Vec::new(),
            overflowed: false,
        }
    }

    /// Record `change` at `at`, merging into the last entry when both the
    /// position and the group repeat, so a transaction's many rows into one
    /// group cost one slot, mirroring the ungrouped buffer.
    fn push(&mut self, at: Option<&C>, change: PendingGroupChange<B>, cap: usize) {
        if let (
            Some((
                last_at,
                PendingGroupChange::Fold {
                    key: held_key,
                    values: _,
                    delta: held_delta,
                    rows: held_rows,
                },
            )),
            PendingGroupChange::Fold {
                key,
                values: _,
                delta,
                rows,
            },
        ) = (self.changes.last_mut(), &change)
        {
            if last_at.is_some() && last_at.as_ref() == at && held_key == key {
                match (held_delta, delta) {
                    (Some(held), Some(delta)) => held.merge(delta),
                    (held @ None, delta) => held.clone_from(delta),
                    (_, None) => {}
                }
                *held_rows += rows;
                return;
            }
        }
        if self.changes.len() >= cap {
            self.overflowed = true;
            return;
        }
        self.changes.push((at.cloned(), change));
    }
}

type GroupedValueChanges<B> = Vec<(crate::GroupIdentity<B>, crate::AggregateValueChange<B>)>;

/// Result of applying one grouped CDC change.
pub enum GroupedFoldOutcome<B: Backend> {
    /// Aggregate value and group existence did not change.
    Unchanged,
    /// Emit this write or removal.
    Change(crate::GroupIdentity<B>, crate::AggregateValueChange<B>),
    /// A new group would exceed the configured limit.
    GroupLimit,
    /// One group's total left what its engine can represent.
    SumOutOfRange,
}

/// What one grouped fold needs to know about its subscription, which is
/// the same three facts however the fold was reached.
struct GroupedFold<'a> {
    spec: &'a AggSpec,
    rule: FoldRule,
    having: Option<&'a GroupHaving>,
}

/// One group's write or removal, or nothing when its value did not move.
type GroupChange<B> = Option<(crate::GroupIdentity<B>, crate::AggregateValueChange<B>)>;

pub struct GroupedAggregateTotal<I: IdTypes, B: Backend, C: Checkpoint> {
    consumer: I::ConsumerId,
    spec: AggSpec,
    group_columns: usize,
    group_key_encoder: crate::backend::GroupKeyEncoder<B>,
    groups: HashMap<Vec<u8>, GroupValue<B>>,
    pending: Option<PendingGroups<B, C>>,
    having: Option<GroupHaving>,
    /// Whether the seed carries components needed only by `HAVING`.
    widened: bool,
    /// How this subscription's fold answers, shared by every group.
    rule: FoldRule,
}

impl<I: IdTypes, B: Backend, C: Checkpoint> GroupedAggregateTotal<I, B, C> {
    pub fn new(
        consumer: I::ConsumerId,
        spec: AggSpec,
        group_columns: usize,
        having: Option<&crate::AggHaving>,
        group_key_encoder: crate::backend::GroupKeyEncoder<B>,
        rule: FoldRule,
    ) -> Self {
        let widened = having.is_some_and(|having| having.widens(&spec));
        debug_assert!(
            having.is_none_or(|having| sql_scalar_text::parse_f64(&having.threshold).is_some()),
            "a HAVING threshold parses at registration, so this one is corrupt"
        );
        let having = having.map(|having| GroupHaving {
            subject: having.subject,
            op: having.op,
            // Validated at registration. An unparseable threshold would
            // compare as NaN and never pass, failing closed.
            threshold: sql_scalar_text::parse_f64(&having.threshold).unwrap_or(f64::NAN),
        });
        Self {
            consumer,
            spec,
            rule,
            group_columns,
            group_key_encoder,
            groups: HashMap::new(),
            pending: Some(PendingGroups::new()),
            having,
            widened,
        }
    }

    pub const fn consumer(&self) -> I::ConsumerId {
        self.consumer
    }

    pub const fn is_seeded(&self) -> bool {
        self.pending.is_none()
    }

    pub const fn group_columns(&self) -> usize {
        self.group_columns
    }

    /// Fold one group change. `rows` counts source rows, independently of
    /// whether the aggregate column itself is NULL or contributes zero.
    pub fn fold(
        &mut self,
        group: crate::GroupIdentity<B>,
        delta: Option<AggDelta>,
        rows: i64,
        at: Option<&C>,
        cap: usize,
        group_limit: usize,
    ) -> GroupedFoldOutcome<B> {
        let crate::GroupIdentity { key, values } = group;
        if let Some(pending) = &mut self.pending {
            pending.push(
                at,
                PendingGroupChange::Fold {
                    key,
                    values,
                    delta,
                    rows,
                },
                cap,
            );
            return GroupedFoldOutcome::Unchanged;
        }
        if !self.groups.contains_key(&key) && rows > 0 && self.groups.len() >= group_limit {
            return GroupedFoldOutcome::GroupLimit;
        }
        Self::apply_change(
            &GroupedFold {
                spec: &self.spec,
                rule: self.rule,
                having: self.having.as_ref(),
            },
            &mut self.groups,
            &key,
            &values,
            delta.as_ref(),
            rows,
        )
        .map_or(GroupedFoldOutcome::SumOutOfRange, |change| {
            change.map_or(GroupedFoldOutcome::Unchanged, |(identity, change)| {
                GroupedFoldOutcome::Change(identity, change)
            })
        })
    }

    fn apply_change(
        fold: &GroupedFold<'_>,
        groups: &mut HashMap<Vec<u8>, GroupValue<B>>,
        key: &[u8],
        values: &[Value<B>],
        delta: Option<&AggDelta>,
        rows: i64,
    ) -> Result<GroupChange<B>, SumOutOfRange> {
        let group = groups.entry(key.to_vec()).or_insert_with(|| GroupValue {
            values: values.to_vec(),
            accumulator: AggAccumulator::from_spec(fold.spec, fold.rule),
            rows: 0,
            announced: false,
        });
        let was_announced = group.announced;
        let before = group.accumulator.value();
        group.rows += rows;
        if let Some(delta) = delta {
            group.accumulator.apply(delta)?;
        }
        if group.rows <= 0 {
            let group = groups.remove(key).expect("the group was inserted above");
            return Ok(was_announced.then(|| {
                (
                    group.into_identity(key.to_vec()),
                    crate::AggregateValueChange::Remove,
                )
            }));
        }
        let after = group.accumulator.value();
        let passes = fold.having.is_none_or(|having| having.passes(group));
        group.announced = passes;
        let change = if passes && (!was_announced || before != after) {
            Some(crate::AggregateValueChange::Set(
                crate::AggregateResultValue::Folded(after),
            ))
        } else {
            (was_announced && !passes).then_some(crate::AggregateValueChange::Remove)
        };
        Ok(change.map(|change| (group.identity(key), change)))
    }

    /// Empty every group after `TRUNCATE`.
    pub fn empty(&mut self, at: Option<&C>, cap: usize) -> GroupedValueChanges<B> {
        if let Some(pending) = &mut self.pending {
            pending.push(at, PendingGroupChange::Emptied, cap);
            return Vec::new();
        }
        let mut removed: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, group)| group.announced)
            .map(|(key, group)| (group.identity(key), crate::AggregateValueChange::Remove))
            .collect();
        removed.sort_unstable_by(|a, b| a.0.key.cmp(&b.0.key));
        self.groups.clear();
        removed
    }

    pub fn reset(&mut self) {
        self.groups.clear();
        self.pending = Some(PendingGroups::new());
    }

    fn seed_group(
        &self,
        subscription: SubscriptionId,
        group_columns: usize,
        components_len: usize,
        row: &[Value<B>],
    ) -> Result<(Vec<u8>, GroupValue<B>), AggregateInstallError> {
        if row.len() != group_columns + components_len + 1 {
            return Err(AggregateInstallError::GroupedRowArity {
                subscription,
                expected: group_columns + components_len + 1,
                got: row.len(),
            });
        }
        let values = row[..group_columns].to_vec();
        let key = self
            .group_key_encoder
            .encode(&values)
            .ok_or(AggregateInstallError::GroupKeyUnencodable(subscription))?;
        let rows = row
            .last()
            .and_then(AggAccumulator::seed_i64)
            .filter(|count| *count > 0)
            .ok_or(AggregateInstallError::GroupedRowCount(subscription))?;
        let components = &row[group_columns..row.len() - 1];
        let accumulator = if self.widened {
            AggAccumulator::seed_stats_row(&self.spec, self.rule, components)
        } else {
            AggAccumulator::seed_from_row(&self.spec, self.rule, components)
        };
        Ok((
            key,
            GroupValue {
                values,
                accumulator,
                rows,
                announced: false,
            },
        ))
    }

    /// Install every grouped seed row as one atomic result.
    pub fn install(
        &mut self,
        subscription: SubscriptionId,
        group_columns: usize,
        rows: &[Vec<Value<B>>],
        read_at: Option<&C>,
        cap: usize,
        group_limit: usize,
    ) -> Result<GroupedValueChanges<B>, AggregateInstallError> {
        let Some(pending) = self.pending.as_ref() else {
            return Err(AggregateInstallError::AlreadySeeded(subscription));
        };
        if pending.overflowed {
            return Err(AggregateInstallError::TooManyChangesDuringRead { subscription, cap });
        }
        if !pending.changes.is_empty()
            && (read_at.is_none() || pending.changes.iter().any(|(at, _)| at.is_none()))
        {
            return Err(AggregateInstallError::PositionUnknown(subscription));
        }

        // A short row would silently seed zeroed components (a widened seed
        // missing its count column would hide the group forever), so the
        // arity is exact: group values, the component set, the row count.
        let components_len = if self.widened {
            3
        } else {
            crate::compiler::sql_shape::aggregate_bootstrap_kinds(&self.spec, self.rule.total).len()
        };
        let mut groups = HashMap::with_capacity(rows.len());
        for row in rows {
            let (key, value) = self.seed_group(subscription, group_columns, components_len, row)?;
            if !groups.contains_key(&key) && groups.len() >= group_limit {
                return Err(AggregateInstallError::GroupLimit {
                    subscription,
                    limit: group_limit,
                });
            }
            if groups.insert(key, value).is_some() {
                return Err(AggregateInstallError::DuplicateGroup(subscription));
            }
        }

        for (at, change) in &pending.changes {
            if at.as_ref() <= read_at {
                continue;
            }
            match change {
                PendingGroupChange::Fold {
                    key,
                    values,
                    delta,
                    rows,
                } => {
                    if !groups.contains_key(key) && *rows > 0 && groups.len() >= group_limit {
                        return Err(AggregateInstallError::GroupLimit {
                            subscription,
                            limit: group_limit,
                        });
                    }
                    Self::apply_change(
                        &GroupedFold {
                            spec: &self.spec,
                            rule: self.rule,
                            having: self.having.as_ref(),
                        },
                        &mut groups,
                        key,
                        values,
                        delta.as_ref(),
                        *rows,
                    )
                    .map_err(|_| AggregateInstallError::SumOutOfRange(subscription))?;
                }
                PendingGroupChange::Emptied => groups.clear(),
            }
        }

        // Announce only the groups that pass the condition. The rest install
        // silently and are already current the moment they cross in.
        let mut opening: Vec<_> = groups
            .iter_mut()
            .filter_map(|(key, group)| {
                group.announced = self
                    .having
                    .as_ref()
                    .is_none_or(|having| having.passes(group));
                group.announced.then(|| {
                    (
                        group.identity(key),
                        crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                            group.accumulator.value(),
                        )),
                    )
                })
            })
            .collect();
        opening.sort_unstable_by(|a, b| a.0.key.cmp(&b.0.key));
        self.groups = groups;
        self.pending = None;
        Ok(opening)
    }
}

#[cfg(test)]
mod tests {
    use super::{AggAccumulator, AggDelta, FoldRule, Spread};
    use crate::compiler::sql_shape::{AggSpec, HavingFunction};

    /// Three rows, each as the exact value the total takes and the real
    /// the spread takes.
    ///
    /// Spelled both ways rather than cast, so the test states the two
    /// numbers it means. A count of 3, a total of 9 and a mean of 3 are
    /// all different, which is what makes a wrong pairing visible.
    const ROWS: [(i128, f64); 3] = [(1, 1.0), (2, 2.0), (6, 6.0)];

    /// A fold rule that maintains every component, so both renderers have
    /// something to disagree about.
    const RULE: FoldRule = FoldRule {
        total: crate::backend::SumRule::Integer,
        mean: crate::backend::MeanRule::Double,
        quotient: crate::compiler::bytecode::Quotient::FromTheOperands,
        variance_seed: crate::backend::VarianceSeed::EnginesOwn,
        float_overflow: crate::backend::FloatSumOverflow::Raises,
    };

    /// The `AggSpec` a `HavingFunction` names, which is the pairing both
    /// renderers have to agree about.
    fn spec_of(function: HavingFunction) -> AggSpec {
        let column: crate::ColumnId = 0;
        match function {
            HavingFunction::CountColumn => AggSpec::CountColumn { column },
            HavingFunction::Sum => AggSpec::Sum { column },
            HavingFunction::Avg => AggSpec::Avg { column },
            HavingFunction::VarPop => AggSpec::VarPop { column },
            HavingFunction::VarSamp => AggSpec::VarSamp { column },
            HavingFunction::StddevPop => AggSpec::StddevPop { column },
            HavingFunction::StddevSamp => AggSpec::StddevSamp { column },
        }
    }

    /// The two renderers answer the same thing for every function.
    ///
    /// A guard rather than a failing test: nothing is wrong with either
    /// answer today, only with there being two places to change. Both
    /// read the same held components through the same formulas, so the
    /// only way they can part is by pairing a function with the wrong
    /// kind, and that is what this asserts. The state is chosen so every
    /// kind renders differently: three rows worth 1, 2 and 6 give a count
    /// of 3, a total of 9, a mean of 3 and a spread nobody else reports.
    #[test]
    fn having_and_projected_renderers_agree_for_every_kind() {
        const FUNCTIONS: [HavingFunction; 7] = [
            HavingFunction::CountColumn,
            HavingFunction::Sum,
            HavingFunction::Avg,
            HavingFunction::VarPop,
            HavingFunction::VarSamp,
            HavingFunction::StddevPop,
            HavingFunction::StddevSamp,
        ];
        for function in FUNCTIONS {
            let mut accumulator = AggAccumulator::from_spec(&spec_of(function), RULE);
            for (exact, real) in ROWS {
                accumulator
                    .apply(&AggDelta::Stats {
                        value: super::TotalDelta::Integer(exact),
                        added: Spread::of_one(real),
                        removed: Spread::EMPTY,
                    })
                    .expect("three small integers stay in range");
            }
            assert_eq!(
                accumulator.value(),
                accumulator.value_as(function),
                "the projected renderer and the HAVING renderer disagree for {function:?}"
            );
        }
    }

    /// And they disagree when the pairing is wrong, so the test above is
    /// not vacuous.
    ///
    /// Without this, a mapping that sent every function to one kind would
    /// pass: each accumulator would be built from that kind's spec and
    /// both renderers would agree on the wrong answer. Asserting that a
    /// deliberately mismatched pair differs is what makes the agreement
    /// above mean something.
    #[test]
    fn a_mismatched_pairing_is_visible() {
        let mut accumulator = AggAccumulator::from_spec(&AggSpec::Sum { column: 0 }, RULE);
        for (exact, real) in ROWS {
            accumulator
                .apply(&AggDelta::Stats {
                    value: super::TotalDelta::Integer(exact),
                    added: Spread::of_one(real),
                    removed: Spread::EMPTY,
                })
                .expect("three small integers stay in range");
        }
        assert_ne!(
            accumulator.value(),
            accumulator.value_as(HavingFunction::Avg),
            "a total of 9 and a mean of 3 are not the same value"
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod reported_kind_tests {
    use super::{AggAccumulator, AggDelta, FoldRule, Spread};
    use crate::compiler::sql_shape::AggSpec;

    const RULE: FoldRule = FoldRule {
        total: crate::backend::SumRule::Integer,
        mean: crate::backend::MeanRule::Double,
        quotient: crate::compiler::bytecode::Quotient::FromTheOperands,
        variance_seed: crate::backend::VarianceSeed::EnginesOwn,
        float_overflow: crate::backend::FloatSumOverflow::Raises,
    };

    /// An accumulator of `spec` holding the two rows 5 and 7.
    ///
    /// Chosen because it is the row set where the defect is visible:
    /// measured on PostgreSQL 16, MySQL 8.4 and SQLite 3.51.1, its
    /// population variance is 1 and its population standard deviation is
    /// also 1, since the square root of one is one.
    fn over_five_and_seven(spec: &AggSpec) -> AggAccumulator {
        let mut accumulator = AggAccumulator::from_spec(spec, RULE);
        for (exact, real) in [(5_i128, 5.0), (7, 7.0)] {
            accumulator
                .apply(&AggDelta::Stats {
                    value: super::TotalDelta::Integer(exact),
                    added: Spread::of_one(real),
                    removed: Spread::EMPTY,
                })
                .unwrap();
        }
        accumulator
    }

    /// A reported value says which aggregate produced it.
    ///
    /// `AggValue::Real` lumps all four of `VAR_POP`, `VAR_SAMP`,
    /// `STDDEV_POP` and `STDDEV_SAMP`, so a consumer holding one cannot
    /// tell a variance from a standard deviation. That is not a matter of
    /// taste: for these two rows both answer 1, so the reported values are
    /// byte-identical and no amount of care at the call site recovers
    /// which was asked for.
    #[test]
    fn a_reported_value_names_the_aggregate_that_produced_it() {
        let variance = over_five_and_seven(&AggSpec::VarPop { column: 0 }).value();
        let deviation = over_five_and_seven(&AggSpec::StddevPop { column: 0 }).value();
        assert_ne!(
            variance, deviation,
            "a population variance and a population standard deviation are \
             different aggregates and must not report as the same value"
        );
    }
}
