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
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggDelta {
    /// COUNT(*) / COUNT(column) delta: always +/-1 per matching (non-NULL) row.
    Count(i64),
    /// SUM(column) delta: signed change in the column sum.
    Sum(f64),
    /// AVG(column) delta: the two components of a running average.
    Avg { sum_delta: f64, count_delta: i64 },
    /// VAR_POP / VAR_SAMP / STDDEV_POP / STDDEV_SAMP delta, carrying the three
    /// components all four derive their value from. See [`AggAccumulator`].
    Stats {
        sum_delta: f64,
        sum_sq_delta: f64,
        count_delta: i64,
    },
}

impl AggDelta {
    /// Add `other` into `self`. Both come from one subscription, so both are
    /// the same variant and a mismatch cannot happen.
    pub fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => *a += b,
            (Self::Sum(a), Self::Sum(b)) => *a += b,
            (
                Self::Avg {
                    sum_delta,
                    count_delta,
                },
                Self::Avg {
                    sum_delta: s,
                    count_delta: c,
                },
            ) => {
                *sum_delta += s;
                *count_delta += c;
            }
            (
                Self::Stats {
                    sum_delta,
                    sum_sq_delta,
                    count_delta,
                },
                Self::Stats {
                    sum_delta: s,
                    sum_sq_delta: sq,
                    count_delta: c,
                },
            ) => {
                *sum_delta += s;
                *sum_sq_delta += sq;
                *count_delta += c;
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
            Self::Sum(v) => *v == 0.0,
            Self::Avg {
                sum_delta,
                count_delta,
            } => *sum_delta == 0.0 && *count_delta == 0,
            Self::Stats {
                sum_delta,
                sum_sq_delta,
                count_delta,
            } => *sum_delta == 0.0 && *sum_sq_delta == 0.0 && *count_delta == 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum AggKind {
    Count,
    Sum,
    Avg,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
}

/// Running value of one aggregate subscription: three sums plus the function
/// they are read through.
///
/// Internal. The engine holds one of these per aggregate subscription and
/// reports [`AggValue`], so a caller never folds anything itself.
#[derive(Clone, Debug)]
pub struct AggAccumulator {
    kind: AggKind,
    count: i64,
    sum: f64,
    sum_sq: f64,
}

impl AggAccumulator {
    /// An empty accumulator for the aggregate described by `spec`.
    pub const fn from_spec(spec: &AggSpec) -> Self {
        use AggSpec as S;
        let kind = match spec {
            S::CountStar | S::CountColumn { .. } => AggKind::Count,
            S::Sum { .. } => AggKind::Sum,
            S::Avg { .. } => AggKind::Avg,
            S::VarPop { .. } => AggKind::VarPop,
            S::VarSamp { .. } => AggKind::VarSamp,
            S::StddevPop { .. } => AggKind::StddevPop,
            S::StddevSamp { .. } => AggKind::StddevSamp,
        };
        Self {
            kind,
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    /// Seed an accumulator from a bootstrap component row produced by
    /// [`AggregateBootstrap`](crate::AggregateBootstrap).
    ///
    /// Consumes the components in the documented column order: `[c]` for
    /// COUNT, `[s]` for SUM, `[s, c]` for AVG, and `[s, sq, c]` for the
    /// variance and stddev family. A zero-row result (COUNT `0`, NULL
    /// sum components) seeds the empty-aggregate state, matching the
    /// "set went empty" semantics of the re-execution family.
    pub fn seed_from_row<B: Backend>(spec: &AggSpec, row: &[Value<B>]) -> Self {
        use AggSpec as S;
        let mut acc = Self::from_spec(spec);
        match spec {
            S::CountStar | S::CountColumn { .. } => {
                acc.count = row.first().and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
            S::Sum { .. } => {
                acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
            }
            S::Avg { .. } => {
                acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
                acc.count = row.get(1).and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
            S::VarPop { .. } | S::VarSamp { .. } | S::StddevPop { .. } | S::StddevSamp { .. } => {
                acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
                acc.sum_sq = row.get(1).and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
                acc.count = row.get(2).and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
        }
        acc
    }

    /// Seed from the complete component layout (`sum`, `sum_sq`, `count`),
    /// used when a sibling `HAVING` widens the seed regardless of the
    /// projected function.
    pub fn seed_stats_row<B: Backend>(spec: &AggSpec, row: &[Value<B>]) -> Self {
        let mut acc = Self::from_spec(spec);
        acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
        acc.sum_sq = row.get(1).and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
        acc.count = row.get(2).and_then(|v| Self::seed_i64(v)).unwrap_or(0);
        acc
    }

    /// Decode a numeric component cell to `f64`. NULL/Missing/non-numeric
    /// cells return `None` (the caller defaults them to `0.0`, safe because
    /// a zero-count accumulator reports the empty state regardless of sum).
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
                .and_then(|x| x.to_string().parse::<f64>().ok()),
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
    pub fn apply(&mut self, delta: &AggDelta) {
        match delta {
            AggDelta::Count(d) => self.count += d,
            AggDelta::Sum(d) => self.sum += d,
            AggDelta::Avg {
                sum_delta,
                count_delta,
            } => {
                self.sum += sum_delta;
                self.count += count_delta;
            }
            AggDelta::Stats {
                sum_delta,
                sum_sq_delta,
                count_delta,
            } => {
                self.sum += sum_delta;
                self.sum_sq += sum_sq_delta;
                self.count += count_delta;
            }
        }
    }

    /// Empty the running value, which is exactly the state a re-read of an
    /// emptied table would seed.
    pub const fn clear(&mut self) {
        self.count = 0;
        self.sum = 0.0;
        self.sum_sq = 0.0;
    }

    /// Current aggregate value.
    pub fn value(&self) -> AggValue {
        match self.kind {
            AggKind::Count => AggValue::Count(self.count),
            AggKind::Sum => AggValue::Sum(self.sum),
            AggKind::Avg => AggValue::Real(self.mean()),
            AggKind::VarPop => AggValue::Real(self.var_pop()),
            AggKind::VarSamp => AggValue::Real(self.var_samp()),
            AggKind::StddevPop => AggValue::Real(self.var_pop().map(f64::sqrt)),
            AggKind::StddevSamp => AggValue::Real(self.var_samp().map(f64::sqrt)),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn mean(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }

    /// The value a sibling family function reads from the held components.
    /// Meaningful only when the components are maintained in full, which the
    /// widened delta and seed paths guarantee.
    pub fn value_as(&self, function: crate::HavingFunction) -> AggValue {
        match function {
            crate::HavingFunction::CountColumn => AggValue::Count(self.count),
            crate::HavingFunction::Sum => AggValue::Sum(self.sum),
            crate::HavingFunction::Avg => AggValue::Real(self.mean()),
            crate::HavingFunction::VarPop => AggValue::Real(self.var_pop()),
            crate::HavingFunction::VarSamp => AggValue::Real(self.var_samp()),
            crate::HavingFunction::StddevPop => AggValue::Real(self.var_pop().map(f64::sqrt)),
            crate::HavingFunction::StddevSamp => AggValue::Real(self.var_samp().map(f64::sqrt)),
        }
    }

    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    fn var_pop(&self) -> Option<f64> {
        (self.count > 0).then(|| {
            let n = self.count as f64;
            self.sum_sq / n - (self.sum / n).powi(2)
        })
    }

    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    fn var_samp(&self) -> Option<f64> {
        (self.count >= 2).then(|| {
            let n = self.count as f64;
            (self.sum_sq - self.sum.powi(2) / n) / (n - 1.0)
        })
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
#[derive(Clone, Copy, Debug)]
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
            (self.changes.last_mut(), change)
        {
            if last_at.is_some() && last_at.as_ref() == at {
                held.merge(&delta);
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
    accumulator: AggAccumulator,
    /// `None` once the starting numbers have landed.
    pending: Option<Pending<C>>,
}

impl<I: IdTypes, C: Checkpoint> AggregateTotal<I, C> {
    pub const fn new(consumer: I::ConsumerId, spec: AggSpec) -> Self {
        Self {
            consumer,
            accumulator: AggAccumulator::from_spec(&spec),
            spec,
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
    pub fn fold(&mut self, delta: AggDelta, at: Option<&C>, cap: usize) -> Option<AggValue> {
        if let Some(pending) = &mut self.pending {
            pending.push(at, PendingChange::Fold(delta), cap);
            return None;
        }
        self.accumulator.apply(&delta);
        Some(self.accumulator.value())
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

        let mut accumulator = AggAccumulator::seed_from_row(&self.spec, row);
        for (at, change) in &pending.changes {
            if at.as_ref() <= read_at {
                continue;
            }
            match change {
                PendingChange::Fold(delta) => accumulator.apply(delta),
                PendingChange::Emptied => accumulator.clear(),
            }
        }

        self.accumulator = accumulator;
        self.pending = None;
        Ok(self.accumulator.value())
    }
}

/// One live grouped SQL result row.
struct GroupValue {
    accumulator: AggAccumulator,
    rows: i64,
    /// Whether the group is currently announced to the consumer. Always
    /// true without a `HAVING`. A hidden group folds silently and its
    /// removal announces nothing.
    announced: bool,
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
    fn passes(&self, group: &GroupValue) -> bool {
        let value = match self.subject {
            crate::HavingSubject::RowCount => Some(group.rows as f64),
            // SQL's `SUM` over zero contributions is NULL, and a comparison
            // over NULL is UNKNOWN. The subject widens at registration, so
            // the contribution count here is real.
            crate::HavingSubject::Aggregate(crate::HavingFunction::Sum) => {
                (group.accumulator.count > 0).then_some(group.accumulator.sum)
            }
            crate::HavingSubject::Aggregate(function) => {
                match group.accumulator.value_as(function) {
                    AggValue::Count(count) => Some(count as f64),
                    AggValue::Sum(sum) => Some(sum),
                    AggValue::Real(real) => real,
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
enum PendingGroupChange {
    Fold {
        key: Vec<u8>,
        delta: Option<AggDelta>,
        rows: i64,
    },
    Emptied,
}

/// Grouped changes seen while the database seed read is in flight.
struct PendingGroups<C: Checkpoint> {
    changes: Vec<(Option<C>, PendingGroupChange)>,
    overflowed: bool,
}

impl<C: Checkpoint> PendingGroups<C> {
    const fn new() -> Self {
        Self {
            changes: Vec::new(),
            overflowed: false,
        }
    }

    /// Record `change` at `at`, merging into the last entry when both the
    /// position and the group repeat, so a transaction's many rows into one
    /// group cost one slot, mirroring the ungrouped buffer.
    fn push(&mut self, at: Option<&C>, change: PendingGroupChange, cap: usize) {
        if let (
            Some((
                last_at,
                PendingGroupChange::Fold {
                    key: held_key,
                    delta: held_delta,
                    rows: held_rows,
                },
            )),
            PendingGroupChange::Fold { key, delta, rows },
        ) = (self.changes.last_mut(), &change)
        {
            if last_at.is_some() && last_at.as_ref() == at && held_key == key {
                match (held_delta, delta) {
                    (Some(held), Some(delta)) => held.merge(delta),
                    (held @ None, delta) => *held = *delta,
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

type GroupedValueChanges<B> = Vec<(Vec<u8>, crate::AggregateValueChange<B>)>;

/// Result of applying one grouped CDC change.
pub enum GroupedFoldOutcome<B: Backend> {
    /// Aggregate value and group existence did not change.
    Unchanged,
    /// Emit this write or removal.
    Change(crate::AggregateValueChange<B>),
    /// A new group would exceed the configured limit.
    GroupLimit,
}

pub struct GroupedAggregateTotal<I: IdTypes, C: Checkpoint> {
    consumer: I::ConsumerId,
    spec: AggSpec,
    group_columns: usize,
    groups: HashMap<Vec<u8>, GroupValue>,
    pending: Option<PendingGroups<C>>,
    having: Option<GroupHaving>,
    /// Whether the seed carries the complete component layout because a
    /// sibling `HAVING` reads more than the projected function maintains.
    widened: bool,
}

impl<I: IdTypes, C: Checkpoint> GroupedAggregateTotal<I, C> {
    pub fn new(
        consumer: I::ConsumerId,
        spec: AggSpec,
        group_columns: usize,
        having: Option<&crate::AggHaving>,
    ) -> Self {
        let widened = having.is_some_and(|having| having.widens(&spec));
        debug_assert!(
            having.is_none_or(|having| having.threshold.parse::<f64>().is_ok()),
            "a HAVING threshold parses at registration, so this one is corrupt"
        );
        let having = having.map(|having| GroupHaving {
            subject: having.subject,
            op: having.op,
            // Validated at registration. An unparseable threshold would
            // compare as NaN and never pass, failing closed.
            threshold: having.threshold.parse().unwrap_or(f64::NAN),
        });
        Self {
            consumer,
            spec,
            group_columns,
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
    pub fn fold<B: Backend>(
        &mut self,
        key: Vec<u8>,
        delta: Option<AggDelta>,
        rows: i64,
        at: Option<&C>,
        cap: usize,
        group_limit: usize,
    ) -> GroupedFoldOutcome<B> {
        if let Some(pending) = &mut self.pending {
            pending.push(at, PendingGroupChange::Fold { key, delta, rows }, cap);
            return GroupedFoldOutcome::Unchanged;
        }
        if !self.groups.contains_key(&key) && rows > 0 && self.groups.len() >= group_limit {
            return GroupedFoldOutcome::GroupLimit;
        }
        Self::apply_change(
            &self.spec,
            self.having.as_ref(),
            &mut self.groups,
            &key,
            delta,
            rows,
        )
        .map_or(GroupedFoldOutcome::Unchanged, GroupedFoldOutcome::Change)
    }

    fn apply_change<B: Backend>(
        spec: &AggSpec,
        having: Option<&GroupHaving>,
        groups: &mut HashMap<Vec<u8>, GroupValue>,
        key: &[u8],
        delta: Option<AggDelta>,
        rows: i64,
    ) -> Option<crate::AggregateValueChange<B>> {
        let group = groups.entry(key.to_vec()).or_insert_with(|| GroupValue {
            accumulator: AggAccumulator::from_spec(spec),
            rows: 0,
            announced: false,
        });
        let was_announced = group.announced;
        let before = group.accumulator.value();
        group.rows += rows;
        if let Some(delta) = delta {
            group.accumulator.apply(&delta);
        }
        if group.rows <= 0 {
            groups.remove(key);
            return was_announced.then_some(crate::AggregateValueChange::Remove);
        }
        let after = group.accumulator.value();
        let passes = having.is_none_or(|having| having.passes(group));
        group.announced = passes;
        if passes && (!was_announced || before != after) {
            return Some(crate::AggregateValueChange::Set(
                crate::AggregateResultValue::Folded(after),
            ));
        }
        (was_announced && !passes).then_some(crate::AggregateValueChange::Remove)
    }

    /// Empty every group after `TRUNCATE`.
    pub fn empty<B: Backend>(&mut self, at: Option<&C>, cap: usize) -> GroupedValueChanges<B> {
        if let Some(pending) = &mut self.pending {
            pending.push(at, PendingGroupChange::Emptied, cap);
            return Vec::new();
        }
        let mut removed: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, group)| group.announced)
            .map(|(key, _)| (key.clone(), crate::AggregateValueChange::Remove))
            .collect();
        removed.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.groups.clear();
        removed
    }

    pub fn reset(&mut self) {
        self.groups.clear();
        self.pending = Some(PendingGroups::new());
    }

    /// Install every grouped seed row as one atomic result.
    pub fn install<B: Backend>(
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
            crate::compiler::sql_shape::aggregate_bootstrap_kinds(&self.spec).len()
        };
        let mut groups = HashMap::with_capacity(rows.len());
        for row in rows {
            if row.len() != group_columns + components_len + 1 {
                return Err(AggregateInstallError::GroupedRowArity {
                    subscription,
                    expected: group_columns + components_len + 1,
                    got: row.len(),
                });
            }
            let key = crate::backend::encode_value_key(&row[..group_columns])
                .ok_or(AggregateInstallError::GroupKeyUnencodable(subscription))?;
            let row_count = row
                .last()
                .and_then(AggAccumulator::seed_i64)
                .filter(|count| *count > 0)
                .ok_or(AggregateInstallError::GroupedRowCount(subscription))?;
            let components = &row[group_columns..row.len() - 1];
            let accumulator = if self.widened {
                AggAccumulator::seed_stats_row(&self.spec, components)
            } else {
                AggAccumulator::seed_from_row(&self.spec, components)
            };
            let value = GroupValue {
                accumulator,
                rows: row_count,
                announced: false,
            };
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
                PendingGroupChange::Fold { key, delta, rows } => {
                    if !groups.contains_key(key) && *rows > 0 && groups.len() >= group_limit {
                        return Err(AggregateInstallError::GroupLimit {
                            subscription,
                            limit: group_limit,
                        });
                    }
                    let _ = Self::apply_change::<B>(
                        &self.spec,
                        self.having.as_ref(),
                        &mut groups,
                        key,
                        *delta,
                        *rows,
                    );
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
                        key.clone(),
                        crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                            group.accumulator.value(),
                        )),
                    )
                })
            })
            .collect();
        opening.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.groups = groups;
        self.pending = None;
        Ok(opening)
    }
}
