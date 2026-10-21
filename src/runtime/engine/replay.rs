#![doc = include_str!("../../../REPLAY_STATE.md")]

use alloc::{
    borrow::Cow,
    collections::{btree_map::Entry, BTreeMap},
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};

use bigdecimal::BigDecimal;
use core::{
    any::Any,
    hash::{Hash, Hasher},
};
use hashbrown::{HashMap, HashSet};
use rpds::{RedBlackTreeMapSync as RedBlackTreeMap, VectorSync};
use serde::{Deserialize, Serialize};
use sql_traits::prelude::DatabaseLike;

use super::SubscriptionEngine;
use crate::backend::{
    Backend, CdcEvent, ColumnComparisonOf, ComparisonContext, DivisionPrecisionIncrement,
    GroupKeyEncoder, RowKind, ScalarFamily, TextOperation, TextRule, Value,
};
use crate::checkpoint::NoCheckpoint;
use crate::compiler::{
    literals::SqlLiteralParse,
    sql_shape::{AggSpec, QueryProjection, ScalarAggKind},
    value_cmp::{compare_ordered_values, values_equal},
    BytecodeProgram, Tri, Vm,
};
use crate::reexec::{
    plan::{build_plan, GroupedHavingCheck, QueryPlan},
    BoundQuery,
};
use crate::runtime::agg::{agg_delta_for_row, AggCellRead, DeltaSpec};
use crate::runtime::aggregate::{AggAccumulator, AggDelta, FoldRule, FoldSnapshot, GroupHaving};
use crate::term::{TermCaller, TermKey, TermLookup, TermPlan};
use crate::types::{AggValue, EventKind, GroupIdentity};
use crate::{catalog_helpers, ColumnId, IdTypes, RegisterError, SubscriptionRequest, TableId};

/// The checkpoint format this module writes and verifies.
const WIRE_FORMAT: u32 = 1;

/// One complete captured dependency input at the bootstrap boundary.
pub struct ReplayTable<B: Backend> {
    pub table_id: TableId,
    pub rows: Vec<Vec<Value<B>>>,
}

impl<B: Backend> core::fmt::Debug for ReplayTable<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ReplayTable")
            .field("table_id", &self.table_id)
            .field("rows", &self.rows)
            .finish()
    }
}

/// A rejected durable replay state.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReplayError {
    /// The statement is outside the shapes this crate replays in process.
    #[error("replay refuses the statement: {0}")]
    Unsupported(String),
    /// The retained inputs cannot answer the statement exactly.
    #[error("replay cannot answer exactly: {0}")]
    InvalidState(String),
    /// A bootstrap input or source event omits state the detector requires.
    #[error("replay input is incomplete: {0}")]
    IncompleteInput(String),
    /// A source event contradicts the retained rows.
    #[error("replay input conflicts with the retained rows: {0}")]
    InputConflict(String),
    /// A retained cell cannot be evaluated.
    #[error("replay evaluation failed: {0}")]
    Evaluation(String),
    /// The checkpoint encoding exceeds the byte budget.
    #[error("checkpoint exceeds the byte budget of {0}")]
    Capacity(usize),
    /// A checkpointed operator state fails verification on restore.
    #[error("checkpointed replay state fails verification: {0}")]
    Restore(String),
}

/// One value of a scalar replay answer.
pub enum ReplayValue<B: Backend> {
    /// A folded aggregate value.
    Folded(AggValue),
    /// A plain scalar cell, `NULL` for an empty extreme.
    Scalar(Value<B>),
}

/// The selected result a detector maintains exactly.
pub enum ReplayAnswer<B: Backend> {
    /// Distinct projected rows with their multiplicities, unordered.
    Rows(Vec<(Vec<Value<B>>, u64)>),
    /// One scalar value.
    Scalar(ReplayValue<B>),
    /// One value per surviving group, in group-key order.
    Grouped(Vec<(GroupIdentity<B>, ReplayValue<B>)>),
}

impl<B: Backend> Clone for ReplayTable<B> {
    fn clone(&self) -> Self {
        Self {
            table_id: self.table_id,
            rows: self.rows.clone(),
        }
    }
}

impl<B: Backend> Clone for ReplayValue<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Folded(value) => Self::Folded(value.clone()),
            Self::Scalar(value) => Self::Scalar(value.clone()),
        }
    }
}

impl<B: Backend> Clone for ReplayAnswer<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Rows(rows) => Self::Rows(rows.clone()),
            Self::Scalar(value) => Self::Scalar(value.clone()),
            Self::Grouped(groups) => Self::Grouped(
                groups
                    .iter()
                    .map(|(group, value)| {
                        (
                            GroupIdentity {
                                key: group.key.clone(),
                                values: group.values.clone(),
                            },
                            value.clone(),
                        )
                    })
                    .collect(),
            ),
        }
    }
}

impl<B: Backend> core::fmt::Debug for ReplayValue<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Folded(value) => f.debug_tuple("Folded").field(value).finish(),
            Self::Scalar(value) => f.debug_tuple("Scalar").field(value).finish(),
        }
    }
}

impl<B: Backend> core::fmt::Debug for ReplayAnswer<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Rows(rows) => f.debug_tuple("Rows").field(rows).finish(),
            Self::Scalar(value) => f.debug_tuple("Scalar").field(value).finish(),
            Self::Grouped(groups) => f.debug_tuple("Grouped").field(groups).finish(),
        }
    }
}

impl<B: Backend> PartialEq for ReplayValue<B> {
    fn eq(&self, other: &Self) -> bool {
        replay_values_match(self, other)
    }
}

impl<B: Backend> PartialEq for ReplayAnswer<B> {
    fn eq(&self, other: &Self) -> bool {
        answers_match(self, other)
    }
}

struct OwnedRow<B: Backend>(Vec<Value<B>>);

struct RowView<'a, B: Backend> {
    row: &'a [Value<B>],
    columns: Option<&'a [ColumnId]>,
}

impl<B: Backend> RowView<'_, B> {
    fn len(&self) -> usize {
        self.columns.map_or(self.row.len(), <[ColumnId]>::len)
    }

    fn cells(&self) -> impl Iterator<Item = &Value<B>> {
        self.row
            .iter()
            .take(self.columns.map_or(self.row.len(), |_| 0))
            .chain(
                self.columns
                    .into_iter()
                    .flatten()
                    .map(|column| &self.row[usize::from(*column)]),
            )
    }
}

impl<B: Backend> From<&RowView<'_, B>> for OwnedRow<B> {
    fn from(view: &RowView<'_, B>) -> Self {
        Self(view.cells().cloned().collect())
    }
}

impl<B: Backend> PartialEq for RowView<'_, B> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .cells()
                .zip(other.cells())
                .all(|(left, right)| cells_match(left, right))
    }
}

impl<B: Backend> Eq for RowView<'_, B> {}

impl<B: Backend> Hash for RowView<'_, B> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.len().hash(state);
        for value in self.cells() {
            core::mem::discriminant(value).hash(state);
            match value {
                Value::Missing | Value::Null => {}
                Value::Bool(value) => value.hash(state),
                Value::Int(value) => value.hash(state),
                Value::String(value) => value.hash(state),
                Value::Bytes(value) => value.hash(state),
                Value::Uuid(value) => value.hash(state),
                Value::Timestamp(value) => value.hash(state),
                Value::TimestampTz(value) => value.hash(state),
                Value::Date(value) => value.hash(state),
                Value::Time(value) => value.hash(state),
                Value::Decimal(value) => value.hash(state),
                Value::Custom(value) => value.hash(state),
                Value::Float(value) => {
                    if let Some(value) = (value as &dyn Any).downcast_ref::<f64>() {
                        canonical_float_bits(*value).hash(state);
                    }
                }
                Value::Json(value) => {
                    if let Some(value) = (value as &dyn Any).downcast_ref::<serde_json::Value>() {
                        value.hash(state);
                    }
                }
                Value::Jsonb(value) => {
                    if let Some(value) = (value as &dyn Any).downcast_ref::<serde_json::Value>() {
                        match postgres_jsonb_canonical::encode::<B::JsonbVersion>(value) {
                            Ok(bytes) => bytes.hash(state),
                            Err(_) => value.hash(state),
                        }
                    }
                }
            }
        }
    }
}

impl<B: Backend> PartialEq for OwnedRow<B> {
    fn eq(&self, other: &Self) -> bool {
        RowView {
            row: &self.0,
            columns: None,
        } == RowView {
            row: &other.0,
            columns: None,
        }
    }
}

impl<B: Backend> Eq for OwnedRow<B> {}

impl<B: Backend> Hash for OwnedRow<B> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        RowView {
            row: &self.0,
            columns: None,
        }
        .hash(state);
    }
}

impl<B: Backend> hashbrown::Equivalent<OwnedRow<B>> for RowView<'_, B> {
    fn equivalent(&self, key: &OwnedRow<B>) -> bool {
        *self
            == RowView {
                row: &key.0,
                columns: None,
            }
    }
}

fn cells_match<B: Backend>(left: &Value<B>, right: &Value<B>) -> bool {
    left == right
        || matches!((left, right), (Value::Float(left), Value::Float(right))
        if left.partial_cmp(left).is_none() && right.partial_cmp(right).is_none())
}

fn canonical_float_bits(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

type CkRow<B> = Arc<Vec<Value<B>>>;

struct InputBag<B: Backend> {
    rows: VectorSync<CkRow<B>>,
    sizes: VectorSync<usize>,
    bytes: usize,
    keys: RedBlackTreeMap<Vec<u8>, usize>,
    encoder: Option<GroupKeyEncoder<B>>,
}

impl<B: Backend> Clone for InputBag<B> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            sizes: self.sizes.clone(),
            bytes: self.bytes,
            keys: self.keys.clone(),
            encoder: self.encoder.clone(),
        }
    }
}

impl<B: Backend> InputBag<B> {
    fn new<DB: DatabaseLike>(
        rows: VectorSync<CkRow<B>>,
        pk: &[ColumnId],
        table: TableId,
        db: &DB,
        budget: usize,
    ) -> Result<Self, ReplayError> {
        let encoder = if pk.is_empty() {
            None
        } else {
            let columns = pk
                .iter()
                .map(|column| catalog_helpers::column_comparison::<B, DB>(db, table, *column))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    ReplayError::Unsupported("the primary key has no comparison contract".into())
                })?;
            Some(B::group_key_encoder(columns).ok_or_else(|| {
                ReplayError::Unsupported("the primary key has no canonical encoder".into())
            })?)
        };
        let mut bag = Self {
            rows,
            sizes: VectorSync::new_sync(),
            bytes: 0,
            keys: RedBlackTreeMap::new_sync(),
            encoder,
        };
        for (position, row) in bag.rows.iter().enumerate() {
            let size = encoded_row_size(row, budget)?;
            bag.bytes = bag.bytes.saturating_add(size);
            if bag.bytes > budget {
                return Err(ReplayError::Capacity(budget));
            }
            bag.sizes.push_back_mut(size);
            if let Some(key) = bag.row_key(pk, row)? {
                if bag.keys.contains_key(&key) {
                    return Err(ReplayError::InputConflict(
                        "duplicate captured primary key".into(),
                    ));
                }
                bag.keys.insert_mut(key, position);
            }
        }
        Ok(bag)
    }

    fn row_key(&self, pk: &[ColumnId], row: &[Value<B>]) -> Result<Option<Vec<u8>>, ReplayError> {
        self.encoder
            .as_ref()
            .map(|encoder| {
                let values: Vec<_> = pk
                    .iter()
                    .map(|column| row[usize::from(*column)].clone())
                    .collect();
                encoder.encode(&values).ok_or_else(|| {
                    ReplayError::IncompleteInput("the primary key cannot be encoded".into())
                })
            })
            .transpose()
    }

    fn insert(&mut self, row: CkRow<B>, pk: &[ColumnId], budget: usize) -> Result<(), ReplayError> {
        let size = encoded_row_size(&row, budget)?;
        let bytes = self.bytes.saturating_add(size);
        if bytes > budget {
            return Err(ReplayError::Capacity(budget));
        }
        if let Some(key) = self.row_key(pk, &row)? {
            if self.keys.contains_key(&key) {
                return Err(ReplayError::InputConflict(
                    "the inserted primary key is retained".into(),
                ));
            }
            self.keys.insert_mut(key, self.rows.len());
        }
        self.rows.push_back_mut(row);
        self.sizes.push_back_mut(size);
        self.bytes = bytes;
        Ok(())
    }

    fn replace(
        &mut self,
        position: usize,
        row: CkRow<B>,
        pk: &[ColumnId],
        budget: usize,
    ) -> Result<(), ReplayError> {
        let size = encoded_row_size(&row, budget)?;
        let bytes = (self.bytes - self.sizes[position]).saturating_add(size);
        if bytes > budget {
            return Err(ReplayError::Capacity(budget));
        }
        if let Some(key) = self.row_key(pk, &row)? {
            if self.keys.get(&key).is_some_and(|other| *other != position) {
                return Err(ReplayError::InputConflict(
                    "the updated primary key is retained".into(),
                ));
            }
            let old_key = self
                .row_key(pk, &self.rows[position])?
                .expect("the input has a primary key");
            self.keys.remove_mut(&old_key);
            self.keys.insert_mut(key, position);
        }
        self.rows.set_mut(position, row);
        self.sizes.set_mut(position, size);
        self.bytes = bytes;
        Ok(())
    }

    fn remove(&mut self, position: usize, pk: &[ColumnId]) -> Result<CkRow<B>, ReplayError> {
        let old = Arc::clone(&self.rows[position]);
        if let Some(key) = self.row_key(pk, &old)? {
            self.keys.remove_mut(&key);
        }
        self.bytes -= self.sizes[position];
        let last = self.rows.len() - 1;
        if position != last {
            let moved = Arc::clone(&self.rows[last]);
            if let Some(key) = self.row_key(pk, &moved)? {
                self.keys.insert_mut(key, position);
            }
            self.rows.set_mut(position, moved);
            self.sizes.set_mut(position, self.sizes[last]);
        }
        self.rows.drop_last_mut();
        self.sizes.drop_last_mut();
        Ok(old)
    }

    fn clear(&mut self) {
        self.rows = VectorSync::new_sync();
        self.sizes = VectorSync::new_sync();
        self.keys = RedBlackTreeMap::new_sync();
        self.bytes = 0;
    }
}

fn encoded_row_size<B: Backend>(row: &[Value<B>], budget: usize) -> Result<usize, ReplayError> {
    super::state_codec::encoded_size(row, budget).map_err(|error| match error {
        super::state_codec::StateCodecError::OverBudget(_) => ReplayError::Capacity(budget),
        error => ReplayError::InvalidState(error.to_string()),
    })
}

struct PendingFold<B: Backend> {
    values: Vec<Value<B>>,
    rows: i64,
    cells: BTreeMap<Vec<u8>, (CkRow<B>, i64)>,
}

impl<B: Backend> PendingFold<B> {
    fn delta(&self, spec: &AggSpec, widens: bool) -> Result<Option<AggDelta>, ReplayError> {
        let mut merged: Option<AggDelta> = None;
        for (row, weight) in self.cells.values() {
            let stats = widens && spec.column().is_some();
            let repetitions = if stats { weight.unsigned_abs() } else { 1 };
            let weight = if stats { weight.signum() } else { *weight };
            for _ in 0..repetitions {
                if let Some(delta) = row_delta(row, delta_spec(spec, widens), weight)? {
                    if let Some(held) = &mut merged {
                        held.merge(&delta);
                    } else {
                        merged = Some(delta);
                    }
                }
            }
        }
        Ok(merged.filter(|delta| !delta.is_zero()))
    }
}

/// The one shape the subscription replays in process.
enum Tier<B: Backend> {
    Rows {
        projection: QueryProjection,
    },
    ScalarFold {
        spec: AggSpec,
        widens: bool,
    },
    GroupedFold {
        groups: Vec<ColumnId>,
        spec: AggSpec,
        rule: FoldRule,
        having: Option<GroupHaving>,
        widens: bool,
        encoder: GroupKeyEncoder<B>,
    },
    ScalarExtreme {
        kind: ScalarAggKind,
        column: ColumnId,
    },
    GroupedExtreme {
        kind: ScalarAggKind,
        column: ColumnId,
        groups: Vec<ColumnId>,
        encoder: GroupKeyEncoder<B>,
        having: Option<GroupedHavingCheck<B>>,
    },
}

#[derive(Serialize)]
#[serde(bound = "")]
enum TierSig<'a, B: Backend> {
    Rows {
        projection: &'a QueryProjection,
    },
    ScalarFold {
        spec: &'a AggSpec,
        widens: bool,
    },
    GroupedFold {
        groups: &'a [ColumnId],
        spec: &'a AggSpec,
        having: Option<&'a GroupHaving>,
        widens: bool,
    },
    ScalarExtreme {
        kind: u8,
        column: ColumnId,
    },
    GroupedExtreme {
        kind: u8,
        column: ColumnId,
        groups: &'a [ColumnId],
        having: Option<&'a GroupedHavingCheck<B>>,
    },
}

/// Everything `restore` and `apply` re-derive must agree with at byte level.
#[derive(Serialize)]
#[serde(bound = "")]
struct PlanSig<'a, B: Backend> {
    bytecode: &'a BytecodeProgram<B>,
    table: TableId,
    pks: &'a [Vec<ColumnId>],
    terms: TermPlans<'a>,
    tier: TierSig<'a, B>,
    extreme_comparison: Option<&'a ExtremeComparison<B>>,
    increment: Option<DivisionPrecisionIncrement>,
}

struct TermPlans<'a>(&'a [TermPlan]);

impl Serialize for TermPlans<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|plan| {
            (
                plan.slot,
                &plan.columns,
                match plan.caller {
                    TermCaller::Identity => 0u8,
                    TermCaller::Subjects => 1u8,
                },
                &plan.moved_by,
            )
        }))
    }
}

const fn kind_tag(kind: ScalarAggKind) -> u8 {
    match kind {
        ScalarAggKind::Min => 0,
        ScalarAggKind::Max => 1,
    }
}

fn tier_sig<B: Backend>(tier: &Tier<B>) -> TierSig<'_, B> {
    match tier {
        Tier::Rows { projection } => TierSig::Rows { projection },
        Tier::ScalarFold { spec, widens } => TierSig::ScalarFold {
            spec,
            widens: *widens,
        },
        Tier::GroupedFold {
            groups,
            spec,
            having,
            widens,
            ..
        } => TierSig::GroupedFold {
            groups,
            spec,
            having: having.as_ref(),
            widens: *widens,
        },
        Tier::ScalarExtreme { kind, column } => TierSig::ScalarExtreme {
            kind: kind_tag(*kind),
            column: *column,
        },
        Tier::GroupedExtreme {
            kind,
            column,
            groups,
            having,
            ..
        } => TierSig::GroupedExtreme {
            kind: kind_tag(*kind),
            column: *column,
            groups,
            having: having.as_ref(),
        },
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireFp {
    table: TableId,
    algorithm: String,
    version: u16,
    profile: u16,
    digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct WireInput<'a, B: Backend> {
    table: TableId,
    rows: Cow<'a, VectorSync<CkRow<B>>>,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct WireGroup<'a, B: Backend> {
    key: Cow<'a, [u8]>,
    values: Cow<'a, [Value<B>]>,
    rows: i64,
    acc: FoldSnapshot,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct WireExtremeGroup<'a, B: Backend> {
    key: Cow<'a, [u8]>,
    values: Cow<'a, [Value<B>]>,
    extreme: Cow<'a, Option<Value<B>>>,
    rows: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
enum WireState<'a, B: Backend> {
    Rows(Cow<'a, [(Vec<Value<B>>, u64)]>),
    Empty,
    ScalarExtreme(Cow<'a, Option<Value<B>>>),
    GroupedExtreme(Vec<WireExtremeGroup<'a, B>>),
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct Wire<'a, B: Backend> {
    format: u32,
    backend: Cow<'a, str>,
    fingerprints: Cow<'a, [WireFp]>,
    plan_signature: Cow<'a, [u8]>,
    inputs: Vec<WireInput<'a, B>>,
    fold: Option<FoldSnapshot>,
    groups: Vec<WireGroup<'a, B>>,
    state: WireState<'a, B>,
}

struct GroupState<B: Backend> {
    values: Vec<Value<B>>,
    acc: AggAccumulator,
    rows: i64,
}

struct ExtremeGroupState<B: Backend> {
    values: Vec<Value<B>>,
    extreme: Option<Value<B>>,
    rows: i64,
}

impl<B: Backend> Clone for GroupState<B> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            acc: self.acc.clone(),
            rows: self.rows,
        }
    }
}

impl<B: Backend> Clone for ExtremeGroupState<B> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            extreme: self.extreme.clone(),
            rows: self.rows,
        }
    }
}

struct ReplayPlan<B: Backend> {
    backend: String,
    fingerprints: Vec<WireFp>,
    plan_signature: Vec<u8>,
    program: Arc<BytecodeProgram<B>>,
    table: TableId,
    deps: Vec<TableId>,
    pks: Vec<Vec<ColumnId>>,
    arity: Vec<usize>,
    term_plans: Vec<TermPlan>,
    term_subjects: Vec<HashSet<TermKey<B>>>,
    tier: Tier<B>,
    increment: Option<DivisionPrecisionIncrement>,
    extreme_comparison: Option<ExtremeComparison<B>>,
}

impl<B: Backend> ReplayPlan<B> {
    const fn comparison(&self) -> &ExtremeComparison<B> {
        self.extreme_comparison
            .as_ref()
            .expect("an extreme tier carries its comparison contract")
    }
}

#[derive(Serialize)]
#[serde(bound = "")]
struct ExtremeComparison<B: Backend> {
    column: ColumnComparisonOf<B>,
    ordering_text: Option<TextRule>,
    equality_text: Option<TextRule>,
    having_text: Option<TextRule>,
}

impl<B: Backend> ExtremeComparison<B> {
    const fn ordering(&self) -> ComparisonContext<'_, B> {
        ComparisonContext {
            left: Some(&self.column),
            right: Some(&self.column),
            text: self.ordering_text,
        }
    }

    const fn equality(&self) -> ComparisonContext<'_, B> {
        ComparisonContext {
            left: Some(&self.column),
            right: Some(&self.column),
            text: self.equality_text,
        }
    }

    const fn having(&self) -> ComparisonContext<'_, B> {
        ComparisonContext {
            left: Some(&self.column),
            right: None,
            text: self.having_text,
        }
    }
}

fn extreme_comparison<B: Backend, DB: DatabaseLike>(
    database: &DB,
    table: TableId,
    column: ColumnId,
    has_having: bool,
) -> Result<ExtremeComparison<B>, ReplayError> {
    let column =
        catalog_helpers::column_comparison::<B, DB>(database, table, column).ok_or_else(|| {
            ReplayError::Unsupported("the extreme column has no comparison contract".into())
        })?;
    if matches!(
        column.kind.family(),
        None | Some(ScalarFamily::Json | ScalarFamily::Jsonb)
    ) {
        return Err(ReplayError::Unsupported(
            "the extreme ordering is not reproducible in process".into(),
        ));
    }
    let mut comparison = ExtremeComparison {
        column,
        ordering_text: None,
        equality_text: None,
        having_text: None,
    };
    if comparison.column.kind.family() == Some(ScalarFamily::String) {
        let rule = |context, operation| {
            B::text_rule(&context, operation).rule().ok_or_else(|| {
                ReplayError::Unsupported(
                    "the extreme text comparison is not reproducible in process".into(),
                )
            })
        };
        let ordering_text = rule(comparison.ordering(), TextOperation::Ordering)?;
        let equality_text = rule(comparison.equality(), TextOperation::Equality)?;
        let having_text = has_having
            .then(|| rule(comparison.having(), TextOperation::Ordering))
            .transpose()?;
        comparison.ordering_text = Some(ordering_text);
        comparison.equality_text = Some(equality_text);
        comparison.having_text = having_text;
    }
    Ok(comparison)
}

/// A compiled subscription and its transactionally retained inputs and result.
pub struct ReplayDetector<B: Backend> {
    plan: Arc<ReplayPlan<B>>,
    bags: Vec<InputBag<B>>,
    grants: Arc<Vec<HashSet<Vec<TermKey<B>>>>>,
    fold: Option<AggAccumulator>,
    groups: RedBlackTreeMap<Vec<u8>, GroupState<B>>,
    extreme: Option<Value<B>>,
    extreme_groups: RedBlackTreeMap<Vec<u8>, ExtremeGroupState<B>>,
    answer: Arc<ReplayAnswer<B>>,
}

impl<B: Backend> Clone for ReplayDetector<B> {
    fn clone(&self) -> Self {
        Self {
            plan: Arc::clone(&self.plan),
            bags: self.bags.clone(),
            grants: Arc::clone(&self.grants),
            fold: self.fold.clone(),
            groups: self.groups.clone(),
            extreme: self.extreme.clone(),
            extreme_groups: self.extreme_groups.clone(),
            answer: Arc::clone(&self.answer),
        }
    }
}

impl<B: Backend> core::fmt::Debug for ReplayDetector<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ReplayDetector")
            .field("dependencies", &self.plan.deps)
            .field("answer", &self.answer)
            .finish_non_exhaustive()
    }
}

struct RowImage<'a, B: Backend> {
    table: TableId,
    cells: &'a [Value<B>],
    pk: &'a [ColumnId],
}

impl<B: Backend> CdcEvent for RowImage<'_, B> {
    type Backend = B;
    type Checkpoint = NoCheckpoint;

    fn kind(&self) -> EventKind {
        EventKind::Insert
    }

    fn table_id<DB: DatabaseLike>(&self, _db: &DB) -> TableId {
        self.table
    }

    fn checkpoint(&self) -> Option<NoCheckpoint> {
        Some(NoCheckpoint)
    }

    fn pk_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
        self.pk.to_vec()
    }

    fn changed_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
        Vec::new()
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        _db: &DB,
        _row: RowKind,
        col: ColumnId,
    ) -> Result<Value<B>, crate::ValueError> {
        Ok(self
            .cells
            .get(usize::from(col))
            .cloned()
            .unwrap_or(Value::Missing))
    }
}

struct ResolvedPlan<B: Backend> {
    tier: Tier<B>,
    extreme_comparison: Option<ExtremeComparison<B>>,
    program: Arc<BytecodeProgram<B>>,
    table: TableId,
    deps: Vec<TableId>,
    pks: Vec<Vec<ColumnId>>,
    term_plans: Vec<TermPlan>,
}

impl<E, I, DB> SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Build a detector from complete dependency inputs at the acknowledged boundary.
    ///
    /// # Errors
    /// Refuses unsupported statements and duplicate, missing, or incomplete inputs.
    pub fn replay_detector(
        &self,
        spec: &SubscriptionRequest<I, E::Backend>,
        tables: Vec<ReplayTable<E::Backend>>,
    ) -> Result<ReplayDetector<E::Backend>, ReplayError> {
        self.replay_detector_bounded(spec, tables, usize::MAX)
    }

    pub(crate) fn replay_detector_bounded(
        &self,
        spec: &SubscriptionRequest<I, E::Backend>,
        tables: Vec<ReplayTable<E::Backend>>,
        budget: usize,
    ) -> Result<ReplayDetector<E::Backend>, ReplayError> {
        let plan = self.resolve(spec)?;
        let plan_signature = plan_signature_bytes(&plan, spec, self.division_increment, budget)?;
        let (bags, arity) = validate_inputs(&self.database, &plan.deps, &plan.pks, tables, budget)?;
        let term_subjects = term_subjects(spec, &plan.term_plans)?;
        let grants = grant_sets(&term_subjects, &plan.term_plans, &bags, &plan.deps)?;
        let fingerprints = fingerprints_of(&self.database, &plan.deps)?;
        let mut detector = ReplayDetector {
            plan: Arc::new(ReplayPlan {
                backend: core::any::type_name::<E::Backend>().to_string(),
                fingerprints,
                plan_signature,
                program: plan.program,
                table: plan.table,
                deps: plan.deps,
                pks: plan.pks,
                arity,
                term_plans: plan.term_plans,
                term_subjects,
                tier: plan.tier,
                extreme_comparison: plan.extreme_comparison,
                increment: self.division_increment,
            }),
            bags,
            grants: Arc::new(grants),
            fold: None,
            groups: RedBlackTreeMap::new_sync(),
            extreme: None,
            extreme_groups: RedBlackTreeMap::new_sync(),
            answer: Arc::new(ReplayAnswer::Scalar(ReplayValue::Folded(
                AggValue::CountStar(0),
            ))),
        };
        detector.seed_folds(&self.database)?;
        detector.recompute_answer(&self.database)?;
        Ok(detector)
    }

    /// Restore and verify checkpointed operator state against the configured plan and catalog.
    ///
    /// # Errors
    /// Returns [`ReplayError::Restore`] for incompatible bytes, plans, or catalogs.
    pub fn restore_replay_detector(
        &self,
        spec: &SubscriptionRequest<I, E::Backend>,
        operator: &[u8],
    ) -> Result<ReplayDetector<E::Backend>, ReplayError> {
        let plan = self.resolve(spec)?;
        let wire = super::state_codec::decode::<Wire<'static, E::Backend>>(operator)
            .map_err(|error| ReplayError::Restore(error.to_string()))?;
        let backend = core::any::type_name::<E::Backend>().to_string();
        if wire.format != WIRE_FORMAT || wire.backend != backend.as_str() {
            return Err(ReplayError::Restore(
                "the operator state is not for this backend".into(),
            ));
        }
        let plan_signature =
            plan_signature_bytes(&plan, spec, self.division_increment, operator.len())
                .map_err(|error| ReplayError::Restore(error.to_string()))?;
        if plan_signature.as_slice() != wire.plan_signature.as_ref() {
            return Err(ReplayError::Restore(
                "the plan no longer matches the operator state".into(),
            ));
        }
        let fingerprints = fingerprints_of(&self.database, &plan.deps)?;
        if fingerprints.as_slice() != wire.fingerprints.as_ref() {
            return Err(ReplayError::Restore(
                "the catalog no longer matches the operator state".into(),
            ));
        }
        if wire.inputs.len() != plan.deps.len() {
            return Err(ReplayError::Restore(
                "the operator inputs no longer match the dependencies".into(),
            ));
        }
        let mut bags = Vec::with_capacity(plan.deps.len());
        let mut arity = Vec::with_capacity(plan.deps.len());
        for (input, dep) in wire.inputs.iter().zip(&plan.deps) {
            if input.table != *dep {
                return Err(ReplayError::Restore(
                    "an operator input is in the wrong order".into(),
                ));
            }
            let width = catalog_helpers::table_arity(&self.database, *dep)
                .map_err(|error| ReplayError::InvalidState(error.to_string()))?;
            arity.push(width);
            for row in input.rows.as_ref() {
                if row.len() != width || row.iter().any(Value::is_missing) {
                    return Err(ReplayError::Restore(
                        "an operator input row is incomplete".into(),
                    ));
                }
            }
            bags.push(InputBag::new(
                input.rows.as_ref().clone(),
                &plan.pks[bags.len()],
                *dep,
                &self.database,
                operator.len(),
            )?);
        }
        let term_subjects = term_subjects(spec, &plan.term_plans)?;
        let grants = grant_sets(&term_subjects, &plan.term_plans, &bags, &plan.deps)?;
        let mut detector = ReplayDetector {
            plan: Arc::new(ReplayPlan {
                backend,
                fingerprints,
                plan_signature,
                program: plan.program,
                table: plan.table,
                deps: plan.deps,
                pks: plan.pks,
                arity,
                term_plans: plan.term_plans,
                term_subjects,
                tier: plan.tier,
                extreme_comparison: plan.extreme_comparison,
                increment: self.division_increment,
            }),
            bags,
            grants: Arc::new(grants),
            fold: None,
            groups: RedBlackTreeMap::new_sync(),
            extreme: None,
            extreme_groups: RedBlackTreeMap::new_sync(),
            answer: Arc::new(ReplayAnswer::Scalar(ReplayValue::Folded(
                AggValue::CountStar(0),
            ))),
        };
        detector.restore_state(&self.database, wire)?;
        Ok(detector)
    }

    fn resolve(
        &self,
        spec: &SubscriptionRequest<I, E::Backend>,
    ) -> Result<ResolvedPlan<E::Backend>, ReplayError> {
        match self.compile_spec(spec.clone(), false) {
            Ok(compiled) => self.resolve_compiled(compiled),
            Err(RegisterError::UnsupportedSql(_) | RegisterError::NotServedInProcess(_)) => {
                self.resolve_reread(spec)
            }
            Err(error) => Err(ReplayError::InvalidState(error.to_string())),
        }
    }

    fn resolve_compiled(
        &self,
        compiled: super::CompiledSpec<I, E::Backend>,
    ) -> Result<ResolvedPlan<E::Backend>, ReplayError> {
        let table = compiled.table_id;
        if catalog_helpers::table_has_rls(&self.database, table)
            .map_err(|error| ReplayError::InvalidState(error.to_string()))?
            && !compiled.read_rule_folded
        {
            return Err(ReplayError::Unsupported(
                "the read rule is not folded into the predicate".into(),
            ));
        }
        let program = Arc::new(compiled.bytecode);
        let tier = match &compiled.projection {
            QueryProjection::Rows | QueryProjection::Columns { .. } => Tier::Rows {
                projection: compiled.projection.clone(),
            },
            QueryProjection::Aggregate(agg) => match agg {
                AggSpec::CountStar | AggSpec::CountColumn { .. } | AggSpec::Sum { .. } => {
                    Tier::ScalarFold {
                        spec: agg.clone(),
                        widens: false,
                    }
                }
                _ => {
                    return Err(ReplayError::Unsupported(
                        "only COUNT and SUM folds are replayed in process".into(),
                    ))
                }
            },
            QueryProjection::GroupedAggregate {
                groups,
                agg,
                having,
                ..
            } => {
                if !matches!(
                    agg,
                    AggSpec::CountStar | AggSpec::CountColumn { .. } | AggSpec::Sum { .. }
                ) {
                    return Err(ReplayError::Unsupported(
                        "only COUNT and SUM folds are replayed in process".into(),
                    ));
                }
                let comparisons: Option<Vec<crate::backend::ColumnComparisonOf<E::Backend>>> =
                    groups
                        .iter()
                        .map(|column| {
                            catalog_helpers::column_comparison::<E::Backend, DB>(
                                &self.database,
                                table,
                                *column,
                            )
                        })
                        .collect();
                let Some(encoder) = comparisons.and_then(E::Backend::group_key_encoder) else {
                    return Err(ReplayError::Unsupported(
                        "the group key cannot be encoded canonically".into(),
                    ));
                };
                let rule = catalog_helpers::grouped_fold_rule::<E::Backend, DB>(
                    agg,
                    having.as_ref(),
                    &self.database,
                    table,
                    self.division_increment,
                );
                let check = compile_having::<E::Backend>(having.as_ref(), rule)?;
                Tier::GroupedFold {
                    groups: groups.clone(),
                    spec: agg.clone(),
                    rule,
                    having: check,
                    widens: having.as_ref().is_some_and(|having| having.widens(agg)),
                    encoder,
                }
            }
        };
        match &tier {
            Tier::ScalarFold { spec, widens } | Tier::GroupedFold { spec, widens, .. } => {
                validate_fold_carrier::<E::Backend, DB>(spec, *widens, &self.database, table)?;
            }
            _ => {}
        }
        let deps = dependency_tables(table, &compiled.term_plans);
        let pks = primary_keys(&self.database, &deps)?;
        Ok(ResolvedPlan {
            tier,
            extreme_comparison: None,
            program,
            table,
            deps,
            pks,
            term_plans: compiled.term_plans,
        })
    }

    fn resolve_reread(
        &self,
        spec: &SubscriptionRequest<I, E::Backend>,
    ) -> Result<ResolvedPlan<E::Backend>, ReplayError> {
        let query = BoundQuery::new(spec.sql.clone(), spec.binds.clone());
        let plan = build_plan(
            &query,
            self.dialect(),
            &self.database,
            self.division_increment,
        )
        .map_err(|error| match error {
            RegisterError::UnsupportedSql(prose) => ReplayError::Unsupported(prose),
            other => ReplayError::InvalidState(other.to_string()),
        })?;
        match plan {
            QueryPlan::Partial(plan) if plan.where_program.term_columns.is_empty() => {
                let table = plan.table_id;
                let rls = catalog_helpers::table_has_rls(&self.database, table)
                    .map_err(|error| ReplayError::InvalidState(error.to_string()))?;
                if rls {
                    return Err(ReplayError::Unsupported(
                        "row-level security keeps the extreme per consumer".into(),
                    ));
                }
                let deps = vec![table];
                Ok(ResolvedPlan {
                    extreme_comparison: Some(extreme_comparison::<E::Backend, DB>(
                        &self.database,
                        table,
                        plan.agg_column,
                        false,
                    )?),
                    tier: Tier::ScalarExtreme {
                        kind: plan.kind,
                        column: plan.agg_column,
                    },
                    program: plan.where_program,
                    table,
                    pks: primary_keys(&self.database, &deps)?,
                    deps,
                    term_plans: Vec::new(),
                })
            }
            QueryPlan::GroupedPartial(plan) if plan.where_program.term_columns.is_empty() => {
                let table = plan.table_id;
                let rls = catalog_helpers::table_has_rls(&self.database, table)
                    .map_err(|error| ReplayError::InvalidState(error.to_string()))?;
                if rls {
                    return Err(ReplayError::Unsupported(
                        "row-level security keeps the extreme per consumer".into(),
                    ));
                }
                let deps = vec![table];
                Ok(ResolvedPlan {
                    extreme_comparison: Some(extreme_comparison::<E::Backend, DB>(
                        &self.database,
                        table,
                        plan.agg_column,
                        matches!(plan.having, Some(GroupedHavingCheck::Extreme { .. })),
                    )?),
                    tier: Tier::GroupedExtreme {
                        kind: plan.kind,
                        column: plan.agg_column,
                        groups: plan.group_columns,
                        encoder: plan.group_key_encoder,
                        having: plan.having,
                    },
                    program: plan.where_program,
                    table,
                    pks: primary_keys(&self.database, &deps)?,
                    deps,
                    term_plans: Vec::new(),
                })
            }
            QueryPlan::Partial(_) | QueryPlan::GroupedPartial(_) => Err(ReplayError::Unsupported(
                "an extreme over a membership term is not replayed in process".into(),
            )),
            QueryPlan::Total(_) | QueryPlan::Keyed(_) => Err(ReplayError::Unsupported(
                "the statement is not maintainable in process".into(),
            )),
        }
    }
}

fn compile_having<B: Backend>(
    having: Option<&crate::AggHaving>,
    rule: FoldRule,
) -> Result<Option<GroupHaving>, ReplayError> {
    let check = having
        .map(|having| {
            GroupHaving::new::<B>(having).ok_or_else(|| {
                ReplayError::InvalidState(
                    "the `HAVING` threshold does not parse as a number".into(),
                )
            })
        })
        .transpose()?;
    if check
        .as_ref()
        .is_some_and(|check| !check.comparisons_served::<B>(rule))
    {
        return Err(ReplayError::Unsupported(
            "the backend does not serve the `HAVING` numeric comparison".into(),
        ));
    }
    Ok(check)
}

fn dependency_tables(table: TableId, term_plans: &[TermPlan]) -> Vec<TableId> {
    let mut deps = vec![table];
    for plan in term_plans {
        if let Some(movement) = &plan.moved_by {
            if !deps.contains(&movement.member_table) {
                deps.push(movement.member_table);
            }
        }
    }
    deps
}

fn primary_keys<DB2: DatabaseLike>(
    database: &DB2,
    deps: &[TableId],
) -> Result<Vec<Vec<ColumnId>>, ReplayError> {
    deps.iter()
        .map(|dep| {
            catalog_helpers::primary_key_columns(database, *dep)
                .map_err(|error| ReplayError::InvalidState(error.to_string()))
        })
        .collect()
}

fn plan_signature_bytes<I: IdTypes, B: Backend>(
    plan: &ResolvedPlan<B>,
    spec: &SubscriptionRequest<I, B>,
    increment: Option<DivisionPrecisionIncrement>,
    budget: usize,
) -> Result<Vec<u8>, ReplayError> {
    let signature = PlanSig {
        bytecode: &plan.program,
        table: plan.table,
        pks: &plan.pks,
        terms: TermPlans(&plan.term_plans),
        tier: tier_sig(&plan.tier),
        extreme_comparison: plan.extreme_comparison.as_ref(),
        increment,
    };
    super::state_codec::encode_limited(&(signature, spec), budget).map_err(|error| match error {
        super::state_codec::StateCodecError::OverBudget(_) => ReplayError::Capacity(budget),
        error => ReplayError::InvalidState(error.to_string()),
    })
}

fn fingerprints_of<DB2: DatabaseLike>(
    database: &DB2,
    deps: &[TableId],
) -> Result<Vec<WireFp>, ReplayError> {
    deps.iter()
        .map(|dep| {
            let Some(fingerprint) = catalog_helpers::schema_fingerprint(database, *dep)
                .map_err(|error| ReplayError::InvalidState(error.to_string()))?
            else {
                return Err(ReplayError::InvalidState(
                    "a dependency table is not in the catalog".into(),
                ));
            };
            Ok(WireFp {
                table: *dep,
                algorithm: fingerprint.algorithm_id().as_str().to_string(),
                version: fingerprint.canonicalization_version(),
                profile: fingerprint.profile_id(),
                digest: fingerprint.fingerprint256(),
            })
        })
        .collect()
}

fn validate_inputs<B: Backend, DB2: DatabaseLike>(
    database: &DB2,
    deps: &[TableId],
    pks: &[Vec<ColumnId>],
    mut tables: Vec<ReplayTable<B>>,
    budget: usize,
) -> Result<(Vec<InputBag<B>>, Vec<usize>), ReplayError> {
    let mut seen = HashSet::new();
    for input in &tables {
        if !seen.insert(input.table_id) {
            return Err(ReplayError::InvalidState(
                "a dependency table is supplied twice".into(),
            ));
        }
        if !deps.contains(&input.table_id) {
            return Err(ReplayError::InvalidState(
                "an input table is not a dependency".into(),
            ));
        }
    }
    if tables.len() != deps.len() {
        return Err(ReplayError::InvalidState(
            "a dependency table has no input".into(),
        ));
    }
    let mut bags = Vec::with_capacity(deps.len());
    let mut arity = Vec::with_capacity(deps.len());
    let mut remaining = budget;
    for dep in deps {
        let position = tables
            .iter()
            .position(|table| table.table_id == *dep)
            .expect("the input set was checked complete above");
        let input = tables.swap_remove(position);
        let width = catalog_helpers::table_arity(database, *dep)
            .map_err(|error| ReplayError::InvalidState(error.to_string()))?;
        arity.push(width);
        let mut bag = InputBag::new(
            VectorSync::new_sync(),
            &pks[bags.len()],
            *dep,
            database,
            remaining,
        )?;
        for row in input.rows {
            if row.len() != width {
                return Err(ReplayError::IncompleteInput(
                    "an input row has the wrong width".into(),
                ));
            }
            if row.iter().any(Value::is_missing) {
                return Err(ReplayError::IncompleteInput(
                    "an input row omits a cell".into(),
                ));
            }
            bag.insert(Arc::new(row), &pks[bags.len()], remaining)?;
        }
        remaining -= bag.bytes;
        bags.push(bag);
    }
    Ok((bags, arity))
}

fn term_subjects<I: IdTypes, B: Backend>(
    spec: &SubscriptionRequest<I, B>,
    plans: &[TermPlan],
) -> Result<Vec<HashSet<TermKey<B>>>, ReplayError> {
    plans
        .iter()
        .map(|plan| {
            let values: &[Value<B>] = match plan.caller {
                TermCaller::Identity => spec.subscriber.as_slice(),
                TermCaller::Subjects => &spec.subjects,
            };
            values
                .iter()
                .map(|value| match TermLookup::of(value.clone()) {
                    TermLookup::Key(key) => Ok(key),
                    _ => Err(ReplayError::InvalidState(
                        "a stated subject is not a term key".into(),
                    )),
                })
                .collect()
        })
        .collect()
}

fn grant_sets<B: Backend>(
    subjects: &[HashSet<TermKey<B>>],
    term_plans: &[TermPlan],
    bags: &[InputBag<B>],
    deps: &[TableId],
) -> Result<Vec<HashSet<Vec<TermKey<B>>>>, ReplayError> {
    term_plans
        .iter()
        .zip(subjects)
        .map(|(plan, subjects)| match &plan.moved_by {
            None => Ok(subjects.iter().cloned().map(|key| vec![key]).collect()),
            Some(movement) => {
                let Some(member) = deps.iter().position(|dep| *dep == movement.member_table) else {
                    return Err(ReplayError::InvalidState(
                        "a membership table is not a dependency".into(),
                    ));
                };
                let mut grants = HashSet::new();
                for row in &bags[member].rows {
                    let subject = row[usize::from(movement.member_subject)].clone();
                    let TermLookup::Key(subject) = TermLookup::of(subject) else {
                        continue;
                    };
                    if !subjects.contains(&subject) {
                        continue;
                    }
                    let mut key = Vec::with_capacity(movement.member_keys.len());
                    let mut complete = true;
                    for column in &movement.member_keys {
                        if let TermLookup::Key(value) =
                            TermLookup::of(row[usize::from(*column)].clone())
                        {
                            key.push(value);
                        } else {
                            complete = false;
                            break;
                        }
                    }
                    if complete {
                        grants.insert(key);
                    }
                }
                Ok(grants)
            }
        })
        .collect()
}

fn cell_read(value: &Value<impl Backend>) -> Result<AggCellRead, ReplayError> {
    let unsupported = || {
        ReplayError::Unsupported(
            "the backend's numeric carrier is unsupported by the fold kernel".into(),
        )
    };
    Ok(match value {
        Value::Int(value) => AggCellRead::Integer(
            *(value as &dyn Any)
                .downcast_ref::<i64>()
                .ok_or_else(unsupported)?,
        ),
        Value::Float(value) => AggCellRead::Real(
            *(value as &dyn Any)
                .downcast_ref::<f64>()
                .ok_or_else(unsupported)?,
        ),
        Value::Decimal(value) => AggCellRead::Decimal(
            (value as &dyn Any)
                .downcast_ref::<BigDecimal>()
                .ok_or_else(unsupported)?
                .clone(),
        ),
        Value::Missing => AggCellRead::Missing,
        Value::Null => AggCellRead::Null,
        _ => AggCellRead::NonNumeric,
    })
}

fn validate_fold_carrier<B: Backend, DB: DatabaseLike>(
    spec: &AggSpec,
    widens: bool,
    database: &DB,
    table: TableId,
) -> Result<(), ReplayError> {
    if !matches!(spec, AggSpec::Sum { .. }) && !widens {
        return Ok(());
    }
    let Some(column) = spec.column() else {
        return Ok(());
    };
    let kind = catalog_helpers::column_comparison::<B, DB>(database, table, column)
        .and_then(|column| column.kind.family());
    let supported = match kind {
        Some(ScalarFamily::Int) => {
            core::any::TypeId::of::<B::Int>() == core::any::TypeId::of::<i64>()
        }
        Some(ScalarFamily::Float) => {
            core::any::TypeId::of::<B::Float>() == core::any::TypeId::of::<f64>()
        }
        Some(ScalarFamily::Decimal) => {
            core::any::TypeId::of::<B::Decimal>() == core::any::TypeId::of::<BigDecimal>()
        }
        _ => false,
    };
    if supported {
        Ok(())
    } else {
        Err(ReplayError::Unsupported(
            "the backend's numeric carrier is unsupported by the fold kernel".into(),
        ))
    }
}

fn contribution_key<B: Backend>(cell: &Value<B>) -> Result<Vec<u8>, ReplayError> {
    let encoded = match cell {
        Value::Float(value) => {
            let value = (value as &dyn Any).downcast_ref::<f64>().ok_or_else(|| {
                ReplayError::Unsupported("the fold kernel requires an f64 carrier".into())
            })?;
            super::state_codec::encode(&canonical_float_bits(*value))
        }
        Value::Decimal(value) => {
            let value = (value as &dyn Any)
                .downcast_ref::<BigDecimal>()
                .ok_or_else(|| {
                    ReplayError::Unsupported("the fold kernel requires a BigDecimal carrier".into())
                })?;
            super::state_codec::encode(&value.normalized())
        }
        _ => super::state_codec::encode(cell),
    };
    encoded.map_err(|error| ReplayError::InvalidState(error.to_string()))
}

const fn delta_spec(spec: &AggSpec, widens: bool) -> DeltaSpec<'_> {
    match (widens, spec.column()) {
        (true, Some(column)) => DeltaSpec::FullStats(column),
        _ => DeltaSpec::Projected(spec),
    }
}

fn row_delta(
    cells: &[Value<impl Backend>],
    spec: DeltaSpec<'_>,
    weight: i64,
) -> Result<Option<AggDelta>, ReplayError> {
    let mut cell = match spec {
        DeltaSpec::Projected(AggSpec::CountStar) => None,
        DeltaSpec::Projected(AggSpec::CountColumn { column }) => {
            Some(match &cells[usize::from(*column)] {
                Value::Null => AggCellRead::Null,
                Value::Missing => AggCellRead::Missing,
                _ => AggCellRead::NonNumeric,
            })
        }
        _ => {
            let column = match spec {
                DeltaSpec::FullStats(column) => column,
                DeltaSpec::Projected(spec) => {
                    spec.column().expect("a numeric aggregate names a column")
                }
            };
            let cell = cell_read(&cells[usize::from(column)])?;
            if matches!(cell, AggCellRead::NonNumeric | AggCellRead::Missing) {
                return Err(ReplayError::IncompleteInput(
                    "a numeric aggregate received a nonnumeric cell".into(),
                ));
            }
            Some(cell)
        }
    };
    Ok(agg_delta_for_row(spec, weight, |_| {
        cell.take().expect("the delta kernel reads its cell once")
    }))
}

fn apply_delta(
    cells: &[Value<impl Backend>],
    spec: DeltaSpec<'_>,
    weight: i64,
    acc: &mut AggAccumulator,
) -> Result<(), ReplayError> {
    let Some(delta) = row_delta(cells, spec, weight)? else {
        return Ok(());
    };
    acc.apply(&delta)
        .map_err(|error| ReplayError::Evaluation(format!("{error:?}")))
}

fn rows_match<B: Backend>(a: &[Value<B>], b: &[Value<B>]) -> bool {
    RowView {
        row: a,
        columns: None,
    } == RowView {
        row: b,
        columns: None,
    }
}

fn option_values_match<B: Backend>(a: Option<&Value<B>>, b: Option<&Value<B>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => cells_match(a, b),
        _ => false,
    }
}

fn replay_values_match<B: Backend>(a: &ReplayValue<B>, b: &ReplayValue<B>) -> bool {
    match (a, b) {
        (ReplayValue::Scalar(a), ReplayValue::Scalar(b)) => cells_match(a, b),
        (ReplayValue::Folded(a), ReplayValue::Folded(b)) => {
            if let (
                AggValue::Sum(Some(crate::NumericValue::Double(a))),
                AggValue::Sum(Some(crate::NumericValue::Double(b))),
            ) = (a, b)
            {
                return canonical_float_bits(*a) == canonical_float_bits(*b);
            }
            a == b
        }
        _ => false,
    }
}

fn bags_match<B: Backend>(a: &[(Vec<Value<B>>, u64)], b: &[(Vec<Value<B>>, u64)]) -> bool {
    let mut counts: HashMap<RowView<'_, B>, u128> = HashMap::new();
    for (row, count) in a {
        if *count != 0 {
            *counts.entry(RowView { row, columns: None }).or_default() += u128::from(*count);
        }
    }
    for (row, count) in b {
        if *count == 0 {
            continue;
        }
        let view = RowView { row, columns: None };
        let Some(held) = counts.get_mut(&view) else {
            return false;
        };
        let Some(remaining) = held.checked_sub(u128::from(*count)) else {
            return false;
        };
        if remaining == 0 {
            counts.remove(&view);
        } else {
            *held = remaining;
        }
    }
    counts.is_empty()
}

fn answers_match<B: Backend>(a: &ReplayAnswer<B>, b: &ReplayAnswer<B>) -> bool {
    match (a, b) {
        (ReplayAnswer::Scalar(a), ReplayAnswer::Scalar(b)) => replay_values_match(a, b),
        (ReplayAnswer::Rows(a), ReplayAnswer::Rows(b)) => bags_match(a, b),
        (ReplayAnswer::Grouped(a), ReplayAnswer::Grouped(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|((a, x), (b, y))| a.key == b.key && replay_values_match(x, y))
        }
        _ => false,
    }
}

impl<B: Backend> ReplayDetector<B> {
    /// The selected result at the last applied boundary.
    #[must_use]
    pub fn answer(&self) -> &ReplayAnswer<B> {
        &self.answer
    }

    /// Every table whose committed transactions this detector applies.
    #[must_use]
    pub fn dependencies(&self) -> &[TableId] {
        &self.plan.deps
    }

    /// The complete checkpointed state.
    ///
    /// # Errors
    /// Returns [`ReplayError::InvalidState`] if the encoding fails.
    pub fn checkpoint(&self) -> Result<Vec<u8>, ReplayError> {
        self.wire_bytes()
    }

    /// Apply one complete source transaction.
    ///
    /// # Errors
    /// Evaluation and input errors preserve the published detector.
    pub fn apply_transaction<E2, DB2>(
        &mut self,
        events: &[E2],
        database: &DB2,
    ) -> Result<bool, ReplayError>
    where
        E2: CdcEvent<Backend = B>,
        DB2: DatabaseLike,
    {
        let (staged, changed) = self.stage_transaction(events, database, usize::MAX)?;
        *self = staged;
        Ok(changed)
    }

    /// Apply one complete source transaction atomically under a byte budget.
    ///
    /// # Errors
    /// Refuses inconsistent inputs, evaluation failures, or retained state exceeding `budget`.
    pub fn apply_transaction_limited<E2, DB2>(
        &mut self,
        events: &[E2],
        database: &DB2,
        budget: usize,
    ) -> Result<(bool, Vec<u8>), ReplayError>
    where
        E2: CdcEvent<Backend = B>,
        DB2: DatabaseLike,
    {
        let (staged, changed) = self.stage_transaction(events, database, budget)?;
        let bytes = staged.wire_bytes_limited(budget)?;
        *self = staged;
        Ok((changed, bytes))
    }

    fn stage_transaction<E2, DB2>(
        &self,
        events: &[E2],
        database: &DB2,
        budget: usize,
    ) -> Result<(Self, bool), ReplayError>
    where
        E2: CdcEvent<Backend = B>,
        DB2: DatabaseLike,
    {
        let mut staged = self.clone();
        let mut folds = BTreeMap::new();
        for event in events {
            staged.apply_event(event, database, budget, &mut folds)?;
            if budget != usize::MAX
                && staged
                    .bags
                    .iter()
                    .fold(0usize, |bytes, bag| bytes.saturating_add(bag.bytes))
                    > budget
            {
                return Err(ReplayError::Capacity(budget));
            }
        }
        staged.finish_folds(folds)?;
        staged.recompute_answer(database)?;
        let changed = !answers_match(&self.answer, &staged.answer);
        Ok((staged, changed))
    }

    fn wire_bytes(&self) -> Result<Vec<u8>, ReplayError> {
        self.wire_bytes_limited(usize::MAX)
    }

    fn wire_bytes_limited(&self, budget: usize) -> Result<Vec<u8>, ReplayError> {
        let wire = Wire {
            format: WIRE_FORMAT,
            backend: Cow::Borrowed(&self.plan.backend),
            fingerprints: Cow::Borrowed(&self.plan.fingerprints),
            plan_signature: Cow::Borrowed(&self.plan.plan_signature),
            inputs: self
                .bags
                .iter()
                .zip(&self.plan.deps)
                .map(|(rows, table)| WireInput {
                    table: *table,
                    rows: Cow::Borrowed(&rows.rows),
                })
                .collect(),
            fold: match &self.plan.tier {
                Tier::ScalarFold { .. } => self.fold.as_ref().map(AggAccumulator::snapshot),
                _ => None,
            },
            groups: match &self.plan.tier {
                Tier::GroupedFold { .. } => self
                    .groups
                    .iter()
                    .map(|(key, group)| WireGroup {
                        key: Cow::Borrowed(key),
                        values: Cow::Borrowed(&group.values),
                        rows: group.rows,
                        acc: group.acc.snapshot(),
                    })
                    .collect(),
                _ => Vec::new(),
            },
            state: match &self.plan.tier {
                Tier::Rows { .. } => {
                    let ReplayAnswer::Rows(rows) = self.answer.as_ref() else {
                        unreachable!("a rows tier recomputes a rows answer")
                    };
                    WireState::Rows(Cow::Borrowed(rows))
                }
                Tier::ScalarExtreme { .. } => {
                    WireState::ScalarExtreme(Cow::Borrowed(&self.extreme))
                }
                Tier::GroupedExtreme { .. } => WireState::GroupedExtreme(
                    self.extreme_groups
                        .iter()
                        .map(|(key, group)| WireExtremeGroup {
                            key: Cow::Borrowed(key),
                            values: Cow::Borrowed(&group.values),
                            extreme: Cow::Borrowed(&group.extreme),
                            rows: group.rows,
                        })
                        .collect(),
                ),
                _ => WireState::Empty,
            },
        };
        super::state_codec::encode_limited(&wire, budget).map_err(|error| match error {
            super::state_codec::StateCodecError::OverBudget(_) => ReplayError::Capacity(budget),
            error => ReplayError::InvalidState(error.to_string()),
        })
    }

    fn apply_event<E2, DB2>(
        &mut self,
        event: &E2,
        db: &DB2,
        budget: usize,
        folds: &mut BTreeMap<Vec<u8>, PendingFold<B>>,
    ) -> Result<(), ReplayError>
    where
        E2: CdcEvent<Backend = B>,
        DB2: DatabaseLike,
    {
        let table = event.table_id(db);
        let Some(dep) = self.plan.deps.iter().position(|t| *t == table) else {
            return Ok(());
        };
        if event.kind() == EventKind::Truncate {
            self.bags[dep].clear();
            if dep == 0 {
                folds.clear();
                if let Some(acc) = &mut self.fold {
                    acc.clear();
                }
                self.groups = RedBlackTreeMap::new_sync();
                self.extreme = None;
                self.extreme_groups = RedBlackTreeMap::new_sync();
            } else {
                self.grants = Arc::new(grant_sets(
                    &self.plan.term_subjects,
                    &self.plan.term_plans,
                    &self.bags,
                    &self.plan.deps,
                )?);
            }
            Ok(())
        } else {
            let (old, new) = self.hydrate(event, db, dep, budget)?;
            if dep != 0 {
                self.grants = Arc::new(grant_sets(
                    &self.plan.term_subjects,
                    &self.plan.term_plans,
                    &self.bags,
                    &self.plan.deps,
                )?);
                return Ok(());
            }
            if matches!(
                self.plan.tier,
                Tier::ScalarFold { .. } | Tier::GroupedFold { .. }
            ) {
                if let Some(row) = old {
                    self.stage_fold_row(row, -1, folds, db)?;
                }
                if let Some(row) = new {
                    self.stage_fold_row(row, 1, folds, db)?;
                }
                Ok(())
            } else {
                self.apply_row_change(
                    old.as_ref().map(|row| row.as_slice()),
                    new.as_ref().map(|row| row.as_slice()),
                    db,
                )
            }
        }
    }

    fn stage_fold_row<DB: DatabaseLike>(
        &self,
        row: CkRow<B>,
        weight: i64,
        folds: &mut BTreeMap<Vec<u8>, PendingFold<B>>,
        db: &DB,
    ) -> Result<(), ReplayError> {
        if !self.matches_where(&row, db)? {
            return Ok(());
        }
        let (spec, widens, group) = match &self.plan.tier {
            Tier::ScalarFold { spec, widens } => (spec, *widens, None),
            Tier::GroupedFold {
                spec,
                widens,
                groups,
                encoder,
                ..
            } => (spec, *widens, Some((groups, encoder))),
            _ => return Ok(()),
        };
        let (key, values) = if let Some((columns, encoder)) = group {
            let values: Vec<_> = columns
                .iter()
                .map(|column| row[usize::from(*column)].clone())
                .collect();
            let key = encoder.encode(&values).ok_or_else(|| {
                ReplayError::Evaluation("the input group cannot be encoded".into())
            })?;
            (key, values)
        } else {
            (Vec::new(), Vec::new())
        };
        let group = folds.entry(key).or_insert_with(|| PendingFold {
            values,
            rows: 0,
            cells: BTreeMap::new(),
        });
        group.rows += weight;
        let key = match spec {
            AggSpec::CountStar => Vec::new(),
            AggSpec::CountColumn { column } if !widens => {
                if row[usize::from(*column)].is_absent() {
                    return Ok(());
                }
                Vec::new()
            }
            _ => {
                let cell =
                    &row[usize::from(spec.column().expect("a numeric aggregate names a column"))];
                if cell.is_absent() {
                    return Ok(());
                }
                contribution_key(cell)?
            }
        };
        match group.cells.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert((row, weight));
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().1 += weight;
                if entry.get().1 == 0 {
                    entry.remove();
                }
            }
        }
        Ok(())
    }

    fn finish_folds(
        &mut self,
        folds: BTreeMap<Vec<u8>, PendingFold<B>>,
    ) -> Result<(), ReplayError> {
        let plan = Arc::clone(&self.plan);
        let (spec, widens, rule) = match &plan.tier {
            Tier::ScalarFold { spec, widens } => (spec, *widens, None),
            Tier::GroupedFold {
                spec, widens, rule, ..
            } => (spec, *widens, Some(rule)),
            _ => return Ok(()),
        };
        for (key, pending) in folds {
            if pending.rows == 0 && pending.cells.is_empty() {
                continue;
            }
            let Some(rule) = rule else {
                if let Some(delta) = pending.delta(spec, widens)? {
                    self.fold
                        .as_mut()
                        .expect("the scalar fold holds an accumulator")
                        .apply(&delta)
                        .map_err(|error| ReplayError::Evaluation(format!("{error:?}")))?;
                }
                continue;
            };
            let rows = self.groups.get(&key).map_or(0, |group| group.rows) + pending.rows;
            if rows < 0 {
                return Err(ReplayError::InputConflict(
                    "a group loses more rows than it retains".into(),
                ));
            }
            if rows == 0 {
                self.groups.remove_mut(&key);
                continue;
            }
            let delta = pending.delta(spec, widens)?;
            if !self.groups.contains_key(&key) {
                self.groups.insert_mut(
                    key.clone(),
                    GroupState {
                        values: pending.values,
                        acc: AggAccumulator::from_spec(spec, *rule),
                        rows: 0,
                    },
                );
            }
            let group = self.groups.get_mut(&key).expect("the fold group exists");
            group.rows = rows;
            if let Some(delta) = delta {
                group
                    .acc
                    .apply(&delta)
                    .map_err(|error| ReplayError::Evaluation(format!("{error:?}")))?;
            }
        }
        Ok(())
    }

    fn hydrate<E2, DB2>(
        &mut self,
        event: &E2,
        db: &DB2,
        dep: usize,
        budget: usize,
    ) -> Result<(Option<CkRow<B>>, Option<CkRow<B>>), ReplayError>
    where
        E2: CdcEvent<Backend = B>,
        DB2: DatabaseLike,
    {
        let arity = self.plan.arity[dep];
        let pk = &self.plan.pks[dep];
        let bag = &mut self.bags[dep];
        match event.kind() {
            EventKind::Insert => {
                let row = Arc::new(read_complete(event, db, RowKind::New, arity)?);
                bag.insert(Arc::clone(&row), pk, budget)?;
                Ok((None, Some(row)))
            }
            EventKind::Update | EventKind::Delete => {
                let position = if pk.is_empty() {
                    let old = read_complete(event, db, RowKind::Old, arity)?;
                    no_pk_position(&bag.rows, &old)?
                } else {
                    let values = pk_key(event, db, pk)?;
                    let key = bag
                        .encoder
                        .as_ref()
                        .expect("the input has a primary key")
                        .encode(&values)
                        .ok_or_else(|| {
                            ReplayError::IncompleteInput("the event key cannot be encoded".into())
                        })?;
                    *bag.keys.get(&key).ok_or_else(|| {
                        ReplayError::InputConflict(
                            "the previous primary key is not retained".into(),
                        )
                    })?
                };
                let old = Arc::clone(&bag.rows[position]);
                for (index, retained) in old.iter().enumerate() {
                    let column = u16::try_from(index).map_err(|_| {
                        ReplayError::InvalidState("the table is wider than a column id".into())
                    })?;
                    let supplied = event
                        .value_at(db, RowKind::Old, column)
                        .map_err(|error| ReplayError::IncompleteInput(error.to_string()))?;
                    if !supplied.is_missing() && !cells_match(&supplied, retained) {
                        return Err(ReplayError::InputConflict(
                            "the previous image contradicts its retained row".into(),
                        ));
                    }
                }
                if event.kind() == EventKind::Delete {
                    bag.remove(position, pk)?;
                    Ok((Some(old), None))
                } else {
                    let new = Arc::new(merge_image(event, db, RowKind::New, &old, arity)?);
                    bag.replace(position, Arc::clone(&new), pk, budget)?;
                    Ok((Some(old), Some(new)))
                }
            }
            EventKind::Truncate => Ok((None, None)),
        }
    }

    fn apply_row_change<DB2: DatabaseLike>(
        &mut self,
        old: Option<&[Value<B>]>,
        new: Option<&[Value<B>]>,
        db: &DB2,
    ) -> Result<(), ReplayError> {
        let plan = Arc::clone(&self.plan);
        match &plan.tier {
            Tier::ScalarFold { spec, widens } => {
                let widens = *widens;
                let old_match = match old {
                    Some(cells) => self.matches_where(cells, db)?,
                    None => false,
                };
                let new_match = match new {
                    Some(cells) => self.matches_where(cells, db)?,
                    None => false,
                };
                let acc = self
                    .fold
                    .as_mut()
                    .expect("the fold tier holds an accumulator");
                if let Some(cells) = old.filter(|_| old_match) {
                    apply_delta(cells, delta_spec(spec, widens), -1, acc)?;
                }
                if let Some(cells) = new.filter(|_| new_match) {
                    apply_delta(cells, delta_spec(spec, widens), 1, acc)?;
                }
                Ok(())
            }
            Tier::GroupedFold { .. } => {
                let old_match = match old {
                    Some(cells) => self.matches_where(cells, db)?,
                    None => false,
                };
                let new_match = match new {
                    Some(cells) => self.matches_where(cells, db)?,
                    None => false,
                };
                if let Some(cells) = old.filter(|_| old_match) {
                    self.group_apply(cells, -1)?;
                }
                if let Some(cells) = new.filter(|_| new_match) {
                    self.group_apply(cells, 1)?;
                }
                Ok(())
            }
            Tier::ScalarExtreme { kind, column } => {
                let (kind, column) = (*kind, *column);
                if let Some(cells) = old {
                    if self.matches_where(cells, db)? {
                        self.extreme_remove(kind, column, cells, db)?;
                    }
                }
                if let Some(cells) = new {
                    if self.matches_where(cells, db)? {
                        self.extreme_add(kind, column, cells)?;
                    }
                }
                Ok(())
            }
            Tier::GroupedExtreme {
                kind,
                column,
                groups,
                encoder,
                ..
            } => {
                let (kind, column) = (*kind, *column);
                if let Some(cells) = old {
                    if self.matches_where(cells, db)? {
                        self.extreme_group_remove(kind, groups, encoder, column, cells, db)?;
                    }
                }
                if let Some(cells) = new {
                    if self.matches_where(cells, db)? {
                        self.extreme_group_add(kind, groups, encoder, column, cells)?;
                    }
                }
                Ok(())
            }
            Tier::Rows { .. } => Ok(()),
        }
    }

    fn seed_folds<DB2: DatabaseLike>(&mut self, db: &DB2) -> Result<(), ReplayError> {
        if let Tier::ScalarFold { spec, .. } = &self.plan.tier {
            let rule = catalog_helpers::fold_rule::<B, DB2>(
                spec,
                db,
                self.plan.table,
                self.plan.increment,
            );
            self.fold = Some(AggAccumulator::from_spec(spec, rule));
        }
        let rows = self.bags[0].rows.clone();
        for row in &rows {
            self.apply_row_change(None, Some(row), db)?;
        }
        Ok(())
    }

    fn recompute_answer<DB2: DatabaseLike>(&mut self, db: &DB2) -> Result<(), ReplayError> {
        self.answer = Arc::new(match &self.plan.tier {
            Tier::ScalarFold { .. } => ReplayAnswer::Scalar(ReplayValue::Folded(
                self.fold
                    .as_ref()
                    .expect("the fold tier holds an accumulator")
                    .value(),
            )),
            Tier::GroupedFold { having, .. } => {
                let mut rows = Vec::new();
                for (key, group) in &self.groups {
                    let passes = match having {
                        Some(check) => check.passes::<B>(group.rows, &group.acc).map_err(|_| {
                            ReplayError::Evaluation(
                                "the `HAVING` comparison exceeds native numeric range".into(),
                            )
                        })?,
                        None => true,
                    };
                    if passes {
                        rows.push((
                            GroupIdentity {
                                key: key.clone(),
                                values: group.values.clone(),
                            },
                            ReplayValue::Folded(group.acc.value()),
                        ));
                    }
                }
                ReplayAnswer::Grouped(rows)
            }
            Tier::Rows { .. } => self.recompute_rows_answer(db)?,
            Tier::ScalarExtreme { .. } => ReplayAnswer::Scalar(ReplayValue::Scalar(
                self.extreme.clone().unwrap_or(Value::Null),
            )),
            Tier::GroupedExtreme { having, .. } => {
                let mut rows = Vec::new();
                for (key, group) in &self.extreme_groups {
                    if self.extreme_group_visible(having.as_ref(), group)? {
                        rows.push((
                            GroupIdentity {
                                key: key.clone(),
                                values: group.values.clone(),
                            },
                            ReplayValue::Scalar(group.extreme.clone().unwrap_or(Value::Null)),
                        ));
                    }
                }
                ReplayAnswer::Grouped(rows)
            }
        });
        Ok(())
    }

    fn recompute_rows_answer<DB2: DatabaseLike>(
        &self,
        db: &DB2,
    ) -> Result<ReplayAnswer<B>, ReplayError> {
        let Tier::Rows { projection } = &self.plan.tier else {
            unreachable!("only a rows tier recomputes its answer by scan");
        };
        let mut vm = Vm::<B>::new();
        let mut multiplicity: HashMap<OwnedRow<B>, u64> = HashMap::new();
        for row in &self.bags[0].rows {
            let values = row.as_slice();
            let image = RowImage {
                table: self.plan.table,
                cells: values,
                pk: &self.plan.pks[0],
            };
            if self.verdict(&mut vm, &image, db)? != Tri::True {
                continue;
            }
            let columns = match projection {
                QueryProjection::Rows => None,
                QueryProjection::Columns { columns, .. } => Some(columns.as_slice()),
                _ => unreachable!("only a rows tier recomputes its answer by scan"),
            };
            *multiplicity
                .entry_ref(&RowView {
                    row: values,
                    columns,
                })
                .or_insert(0) += 1;
        }
        Ok(ReplayAnswer::Rows(
            multiplicity
                .into_iter()
                .map(|(row, count)| (row.0, count))
                .collect(),
        ))
    }

    /// The where verdict of one retained row image, with each term slot's
    /// truth read from the recomputed grants.
    fn verdict<DB2: DatabaseLike>(
        &self,
        vm: &mut Vm<B>,
        image: &RowImage<'_, B>,
        db: &DB2,
    ) -> Result<Tri, ReplayError> {
        if self.plan.program.term_columns.is_empty() {
            return vm
                .eval(&self.plan.program, image, RowKind::New, db)
                .map_err(|error| ReplayError::Evaluation(format!("{error:?}")));
        }
        let mut truths = Vec::new();
        for (slot, columns) in self.plan.program.term_columns.iter().enumerate() {
            let mut key = Vec::with_capacity(columns.len());
            for column in columns {
                match TermLookup::of(image.cells[usize::from(*column)].clone()) {
                    TermLookup::Unknown => {
                        truths.push(Tri::Unknown);
                        break;
                    }
                    TermLookup::Nobody => {
                        truths.push(Tri::False);
                        break;
                    }
                    TermLookup::Key(value) => key.push(value),
                }
            }
            if truths.len() <= slot {
                truths.push(if self.grants[slot].contains(&key) {
                    Tri::True
                } else {
                    Tri::False
                });
            }
        }
        vm.eval_with_terms(&self.plan.program, image, RowKind::New, db, &truths, None)
            .map_err(|error| ReplayError::Evaluation(format!("{error:?}")))
    }

    fn matches_where<DB2: DatabaseLike>(
        &self,
        cells: &[Value<B>],
        db: &DB2,
    ) -> Result<bool, ReplayError> {
        let mut vm = Vm::<B>::new();
        let image = RowImage {
            table: self.plan.table,
            cells,
            pk: &self.plan.pks[0],
        };
        Ok(self.verdict(&mut vm, &image, db)? == Tri::True)
    }

    fn better(
        context: ComparisonContext<'_, B>,
        kind: ScalarAggKind,
        cand: &Value<B>,
        cur: &Value<B>,
    ) -> Result<bool, ReplayError> {
        let predicate = match kind {
            ScalarAggKind::Min => core::cmp::Ordering::is_lt,
            ScalarAggKind::Max => core::cmp::Ordering::is_gt,
        };
        compare_ordered_values(context, cand, cur, predicate)
            .map_err(|error| ReplayError::Evaluation(format!("{error:?}")))
            .map(|verdict| verdict == Tri::True)
    }

    fn extreme_remove<DB2: DatabaseLike>(
        &mut self,
        kind: ScalarAggKind,
        column: ColumnId,
        cells: &[Value<B>],
        db: &DB2,
    ) -> Result<(), ReplayError> {
        let cand = &cells[usize::from(column)];
        let Some(cur) = self.extreme.as_ref() else {
            return Ok(());
        };
        if cand.is_absent() {
            return Ok(());
        }
        let equal = values_equal(self.plan.comparison().equality(), cand, cur)
            .map_err(|error| ReplayError::Evaluation(format!("{error:?}")))?;
        if !equal {
            return Ok(());
        }
        self.extreme = self.scan_extreme(kind, column, db)?;
        Ok(())
    }

    fn extreme_add(
        &mut self,
        kind: ScalarAggKind,
        column: ColumnId,
        cells: &[Value<B>],
    ) -> Result<(), ReplayError> {
        let cand = &cells[usize::from(column)];
        if cand.is_absent() {
            return Ok(());
        }
        let better = match self.extreme.as_ref() {
            None => true,
            Some(cur) => Self::better(self.plan.comparison().ordering(), kind, cand, cur)?,
        };
        if better {
            self.extreme = Some(cand.clone());
        }
        Ok(())
    }

    fn scan_extreme<DB2: DatabaseLike>(
        &self,
        kind: ScalarAggKind,
        column: ColumnId,
        db: &DB2,
    ) -> Result<Option<Value<B>>, ReplayError> {
        let mut vm = Vm::<B>::new();
        let mut best: Option<Value<B>> = None;
        for row in &self.bags[0].rows {
            let values = row.as_slice();
            let image = RowImage {
                table: self.plan.table,
                cells: values,
                pk: &self.plan.pks[0],
            };
            if self.verdict(&mut vm, &image, db)? != Tri::True {
                continue;
            }
            let cand = &values[usize::from(column)];
            if cand.is_absent() {
                continue;
            }
            let replace = match best.as_ref() {
                None => true,
                Some(cur) => Self::better(self.plan.comparison().ordering(), kind, cand, cur)?,
            };
            if replace {
                best = Some(cand.clone());
            }
        }
        Ok(best)
    }

    fn extreme_group_add(
        &mut self,
        kind: ScalarAggKind,
        groups: &[ColumnId],
        encoder: &GroupKeyEncoder<B>,
        column: ColumnId,
        cells: &[Value<B>],
    ) -> Result<(), ReplayError> {
        let group_values: Vec<Value<B>> = groups
            .iter()
            .map(|c| cells[usize::from(*c)].clone())
            .collect();
        let Some(key) = encoder.encode(&group_values) else {
            return Err(ReplayError::Evaluation(
                "the group key encoder refuses a retained row".into(),
            ));
        };
        if !self.extreme_groups.contains_key(&key) {
            self.extreme_groups.insert_mut(
                key.clone(),
                ExtremeGroupState {
                    values: group_values,
                    extreme: None,
                    rows: 0,
                },
            );
        }
        let entry = self
            .extreme_groups
            .get_mut(&key)
            .expect("the group was inserted");
        entry.rows += 1;
        let cand = &cells[usize::from(column)];
        if cand.is_absent() {
            return Ok(());
        }
        let better = match entry.extreme.as_ref() {
            None => true,
            Some(cur) => Self::better(self.plan.comparison().ordering(), kind, cand, cur)?,
        };
        if better {
            entry.extreme = Some(cand.clone());
        }
        Ok(())
    }

    fn extreme_group_remove<DB2: DatabaseLike>(
        &mut self,
        kind: ScalarAggKind,
        groups: &[ColumnId],
        encoder: &GroupKeyEncoder<B>,
        column: ColumnId,
        cells: &[Value<B>],
        db: &DB2,
    ) -> Result<(), ReplayError> {
        let group_values: Vec<Value<B>> = groups
            .iter()
            .map(|c| cells[usize::from(*c)].clone())
            .collect();
        let Some(key) = encoder.encode(&group_values) else {
            return Err(ReplayError::Evaluation(
                "the group key encoder refuses a retained row".into(),
            ));
        };
        let Some(entry) = self.extreme_groups.get_mut(&key) else {
            return Ok(());
        };
        let cand = &cells[usize::from(column)];
        let holds_extreme = match &entry.extreme {
            Some(cur) => {
                !cand.is_absent()
                    && values_equal(self.plan.comparison().equality(), cand, cur)
                        .map_err(|error| ReplayError::Evaluation(format!("{error:?}")))?
            }
            None => false,
        };
        entry.rows -= 1;
        if entry.rows == 0 {
            self.extreme_groups.remove_mut(&key);
            return Ok(());
        }
        if holds_extreme {
            let _ = entry;
            let mut vm = Vm::<B>::new();
            let mut best: Option<Value<B>> = None;
            for row in &self.bags[0].rows {
                let values = row.as_slice();
                let image = RowImage {
                    table: self.plan.table,
                    cells: values,
                    pk: &self.plan.pks[0],
                };
                if self.verdict(&mut vm, &image, db)? != Tri::True {
                    continue;
                }
                let row_values: Vec<Value<B>> = groups
                    .iter()
                    .map(|c| values[usize::from(*c)].clone())
                    .collect();
                let Some(row_key) = encoder.encode(&row_values) else {
                    continue;
                };
                if row_key != key {
                    continue;
                }
                let cand = &values[usize::from(column)];
                if cand.is_absent() {
                    continue;
                }
                let replace = match best.as_ref() {
                    None => true,
                    Some(cur) => Self::better(self.plan.comparison().ordering(), kind, cand, cur)?,
                };
                if replace {
                    best = Some(cand.clone());
                }
            }
            let entry = self
                .extreme_groups
                .get_mut(&key)
                .expect("the group remains populated");
            entry.extreme = best;
        }
        Ok(())
    }

    fn extreme_group_visible(
        &self,
        having: Option<&GroupedHavingCheck<B>>,
        group: &ExtremeGroupState<B>,
    ) -> Result<bool, ReplayError> {
        let Some(check) = having else {
            return Ok(true);
        };
        match check {
            GroupedHavingCheck::RowCount { op, threshold } => {
                Ok(op.admits(group.rows.cmp(threshold)))
            }
            GroupedHavingCheck::Extreme { op, threshold } => {
                let Some(extreme) = &group.extreme else {
                    return Ok(false);
                };
                Ok(compare_ordered_values(
                    self.plan.comparison().having(),
                    extreme,
                    threshold,
                    |ordering| op.admits(ordering),
                )
                .map_err(|error| ReplayError::Evaluation(format!("{error:?}")))?
                    == Tri::True)
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive tier match validates native folds and retained candidates before publication"
    )]
    fn restore_state<DB2: DatabaseLike>(
        &mut self,
        db: &DB2,
        wire: Wire<'_, B>,
    ) -> Result<(), ReplayError> {
        let plan = Arc::clone(&self.plan);
        if wire.fold.is_some() != matches!(plan.tier, Tier::ScalarFold { .. })
            || (!wire.groups.is_empty() && !matches!(plan.tier, Tier::GroupedFold { .. }))
        {
            return Err(ReplayError::Restore(
                "the fold state does not match the compiled tier".into(),
            ));
        }
        if let Tier::ScalarFold { spec, .. } = &plan.tier {
            let snapshot = wire
                .fold
                .as_ref()
                .ok_or_else(|| ReplayError::Restore("the fold snapshot is missing".into()))?;
            let rule = catalog_helpers::fold_rule::<B, DB2>(spec, db, plan.table, plan.increment);
            self.fold = Some(
                AggAccumulator::from_snapshot(snapshot, spec, rule)
                    .map_err(|error| ReplayError::Restore(error.into()))?,
            );
        }
        if let Tier::GroupedFold {
            groups,
            spec,
            rule,
            encoder,
            ..
        } = &plan.tier
        {
            for group in wire.groups {
                let values = group.values.into_owned();
                let key = group.key.into_owned();
                if values.len() != groups.len()
                    || encoder.encode(&values).as_ref() != Some(&key)
                    || group.rows <= 0
                    || self.groups.contains_key(&key)
                {
                    return Err(ReplayError::Restore(
                        "invalid or duplicate fold group".into(),
                    ));
                }
                self.groups.insert_mut(
                    key,
                    GroupState {
                        values,
                        rows: group.rows,
                        acc: AggAccumulator::from_snapshot(&group.acc, spec, *rule)
                            .map_err(|error| ReplayError::Restore(error.into()))?,
                    },
                );
            }
        }
        match (&plan.tier, wire.state) {
            (Tier::Rows { .. }, WireState::Rows(rows)) => {
                let saved = ReplayAnswer::Rows(rows.into_owned());
                if let ReplayAnswer::Rows(rows) = &saved {
                    if rows.iter().any(|(_, count)| *count == 0) {
                        return Err(ReplayError::Restore(
                            "a projected row has zero multiplicity".into(),
                        ));
                    }
                }
                let answer = self.recompute_rows_answer(db)?;
                if !answers_match(&saved, &answer) {
                    return Err(ReplayError::Restore(
                        "the projected bag contradicts its inputs".into(),
                    ));
                }
                self.answer = Arc::new(answer);
                return Ok(());
            }
            (Tier::ScalarFold { .. } | Tier::GroupedFold { .. }, WireState::Empty) => {
                self.verify_fold_inputs(db)?;
            }
            (Tier::ScalarExtreme { kind, column }, WireState::ScalarExtreme(value)) => {
                self.extreme = value.into_owned();
                if !option_values_match(
                    self.extreme.as_ref(),
                    self.scan_extreme(*kind, *column, db)?.as_ref(),
                ) {
                    return Err(ReplayError::Restore(
                        "the extreme contradicts its retained candidates".into(),
                    ));
                }
            }
            (
                Tier::GroupedExtreme {
                    groups, encoder, ..
                },
                WireState::GroupedExtreme(saved),
            ) => {
                for group in saved {
                    let key = group.key.into_owned();
                    let values = group.values.into_owned();
                    if values.len() != groups.len()
                        || encoder.encode(&values).as_ref() != Some(&key)
                        || group.rows <= 0
                        || self.extreme_groups.contains_key(&key)
                    {
                        return Err(ReplayError::Restore(
                            "invalid or duplicate extreme group".into(),
                        ));
                    }
                    self.extreme_groups.insert_mut(
                        key,
                        ExtremeGroupState {
                            values,
                            extreme: group.extreme.into_owned(),
                            rows: group.rows,
                        },
                    );
                }
                let saved = self.extreme_groups.clone();
                self.extreme_groups = RedBlackTreeMap::new_sync();
                self.seed_folds(db)?;
                if saved.size() != self.extreme_groups.size() {
                    return Err(ReplayError::Restore(
                        "the extreme groups contradict their inputs".into(),
                    ));
                }
                for (key, group) in &saved {
                    let Some(actual) = self.extreme_groups.get(key) else {
                        return Err(ReplayError::Restore(
                            "an extreme group has no retained inputs".into(),
                        ));
                    };
                    if group.rows != actual.rows
                        || !option_values_match(group.extreme.as_ref(), actual.extreme.as_ref())
                    {
                        return Err(ReplayError::Restore(
                            "an extreme group contradicts its inputs".into(),
                        ));
                    }
                }
                self.extreme_groups = saved;
            }
            _ => {
                return Err(ReplayError::Restore(
                    "the result state does not match the compiled tier".into(),
                ))
            }
        }
        self.recompute_answer(db)
    }

    fn verify_fold_inputs<DB2: DatabaseLike>(&self, db: &DB2) -> Result<(), ReplayError> {
        let (spec, grouped) = match &self.plan.tier {
            Tier::ScalarFold { spec, .. } => (spec, None),
            Tier::GroupedFold {
                spec,
                groups,
                encoder,
                ..
            } => (spec, Some((groups, encoder))),
            _ => return Ok(()),
        };
        let mut counts = HashMap::<Vec<u8>, (i64, i64)>::new();
        let mut scalar = (0i64, 0i64);
        for row in &self.bags[0].rows {
            if !self.matches_where(row, db)? {
                continue;
            }
            let contributes = spec
                .column()
                .is_none_or(|column| !row[usize::from(column)].is_absent());
            let entry = if let Some((groups, encoder)) = grouped {
                let values: Vec<_> = groups
                    .iter()
                    .map(|column| row[usize::from(*column)].clone())
                    .collect();
                let key = encoder.encode(&values).ok_or_else(|| {
                    ReplayError::Restore("an input group cannot be encoded".into())
                })?;
                counts.entry(key).or_default()
            } else {
                &mut scalar
            };
            entry.0 += 1;
            entry.1 += i64::from(contributes);
        }
        if grouped.is_some() {
            if counts.len() != self.groups.size() {
                return Err(ReplayError::Restore(
                    "the fold groups contradict their inputs".into(),
                ));
            }
            for (key, (rows, contributions)) in counts {
                let Some(group) = self.groups.get(&key) else {
                    return Err(ReplayError::Restore(
                        "a retained fold group is missing".into(),
                    ));
                };
                if group.rows != rows || !group.acc.counts_match(rows, contributions) {
                    return Err(ReplayError::Restore(
                        "a fold group's counters contradict its inputs".into(),
                    ));
                }
            }
        } else if !self
            .fold
            .as_ref()
            .is_some_and(|acc| acc.counts_match(scalar.0, scalar.1))
        {
            return Err(ReplayError::Restore(
                "the fold counters contradict their inputs".into(),
            ));
        }
        Ok(())
    }

    fn group_apply(&mut self, cells: &[Value<B>], weight: i64) -> Result<(), ReplayError> {
        let plan = Arc::clone(&self.plan);
        let Tier::GroupedFold {
            groups,
            spec,
            rule,
            encoder,
            widens,
            ..
        } = &plan.tier
        else {
            unreachable!("only a grouped fold applies a group delta");
        };
        let widens = *widens;
        let group_values: Vec<Value<B>> = groups
            .iter()
            .map(|column| cells[usize::from(*column)].clone())
            .collect();
        let Some(key) = encoder.encode(&group_values) else {
            return Err(ReplayError::Evaluation(
                "the group key encoder refuses a retained row".into(),
            ));
        };
        if !self.groups.contains_key(&key) {
            if weight < 0 {
                return Err(ReplayError::InputConflict(
                    "the removed group is not retained".into(),
                ));
            }
            self.groups.insert_mut(
                key.clone(),
                GroupState {
                    values: group_values,
                    acc: AggAccumulator::from_spec(spec, *rule),
                    rows: 0,
                },
            );
        }
        let entry = self.groups.get_mut(&key).expect("the group was inserted");
        apply_delta(cells, delta_spec(spec, widens), weight, &mut entry.acc)?;
        entry.rows += weight;
        if entry.rows == 0 {
            self.groups.remove_mut(&key);
        }
        Ok(())
    }
}

fn read_complete<E2, B, DB2>(
    event: &E2,
    db: &DB2,
    row: RowKind,
    arity: usize,
) -> Result<Vec<Value<B>>, ReplayError>
where
    E2: CdcEvent<Backend = B>,
    B: Backend,
    DB2: DatabaseLike,
{
    let mut values = Vec::with_capacity(arity);
    for index in 0..arity {
        let column = u16::try_from(index)
            .map_err(|_| ReplayError::InvalidState("the table is wider than a column id".into()))?;
        let cell = event
            .value_at(db, row, column)
            .map_err(|error| ReplayError::IncompleteInput(error.to_string()))?;
        if cell.is_missing() {
            return Err(ReplayError::IncompleteInput(
                "a required cell is missing from the event image".into(),
            ));
        }
        values.push(cell);
    }
    Ok(values)
}

fn merge_image<E2, B, DB2>(
    event: &E2,
    db: &DB2,
    row: RowKind,
    retained: &CkRow<B>,
    arity: usize,
) -> Result<Vec<Value<B>>, ReplayError>
where
    E2: CdcEvent<Backend = B>,
    B: Backend,
    DB2: DatabaseLike,
{
    (0..arity)
        .map(|index| {
            let column = u16::try_from(index).map_err(|_| {
                ReplayError::InvalidState("the table is wider than a column id".into())
            })?;
            match event
                .value_at(db, row, column)
                .map_err(|error| ReplayError::IncompleteInput(error.to_string()))?
            {
                Value::Missing => Ok(retained[index].clone()),
                cell => Ok(cell),
            }
        })
        .collect()
}

fn pk_key<E2, B, DB2>(event: &E2, db: &DB2, pk: &[ColumnId]) -> Result<Vec<Value<B>>, ReplayError>
where
    E2: CdcEvent<Backend = B>,
    B: Backend,
    DB2: DatabaseLike,
{
    let mut key = Vec::with_capacity(pk.len());
    for column in pk {
        let previous = event
            .value_at(db, RowKind::Old, *column)
            .map_err(|error| ReplayError::IncompleteInput(error.to_string()))?;
        let cell = if previous.is_missing() {
            event
                .value_at(db, RowKind::Pk, *column)
                .map_err(|error| ReplayError::IncompleteInput(error.to_string()))?
        } else {
            previous
        };
        if cell.is_missing() {
            return Err(ReplayError::IncompleteInput(
                "a primary-key cell is missing from the event".into(),
            ));
        }
        key.push(cell);
    }
    Ok(key)
}

fn no_pk_position<B: Backend>(
    bag: &VectorSync<CkRow<B>>,
    old: &[Value<B>],
) -> Result<usize, ReplayError> {
    for (position, row) in bag.iter().enumerate() {
        if rows_match(row, old) {
            return Ok(position);
        }
    }
    Err(ReplayError::InputConflict(
        "the old image is not retained".into(),
    ))
}
