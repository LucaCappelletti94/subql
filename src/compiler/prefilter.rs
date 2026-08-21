//! Prefilter planning for candidate pruning before VM execution.
//!
//! The planner is intentionally conservative: it may return false
//! positives, but it must not return false negatives. The plan feeds
//! two consumers. `trigger_atoms` is the set of indexable atoms the
//! bitmap indexes ingest for candidate triggering. `scan_required`
//! marks predicates that must be considered even when no index atom
//! fires (SQL that the analyser could not reduce to a purely indexable
//! form).

use super::literals::SqlLiteralParse;
use crate::backend::{Backend, Value};
use crate::{catalog_helpers, ColumnId, TableId};
use alloc::collections::BTreeSet;
use alloc::sync::Arc;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};

/// A normalized prefilter plan used by runtime dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrefilterPlan {
    /// Atoms inserted into bitmap indexes for candidate triggering.
    pub trigger_atoms: Arc<[PlannerAtom]>,
    /// If true, this predicate must be considered even without index hits.
    pub scan_required: bool,
}

impl Default for PrefilterPlan {
    fn default() -> Self {
        Self {
            trigger_atoms: Arc::from([]),
            scan_required: true,
        }
    }
}

/// Planner value used in indexable equality atoms.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PlannerValue {
    Bool(bool),
    Int(i64),
    Float(u64), // f64::to_bits()
    String(Arc<str>),
}

impl PlannerValue {
    /// The index key for a typed cell, or `None` for a payload the bitmap
    /// indexes cannot hold.
    ///
    /// Runtime dispatch derives a row cell's probe key through this same
    /// function (see `IndexableCell::from_value`), which is what keeps a
    /// filed atom reachable. Two derivations of one key is how an atom
    /// becomes unprobeable and its predicate silently missed.
    ///
    /// The payload types are checked rather than the variants, because a
    /// backend chooses them: SQLite spells `Bool` as `i64`, so its boolean
    /// cell keys as the integer the row actually carries.
    #[must_use]
    pub fn from_value<B: Backend>(v: &Value<B>) -> Option<Self> {
        use core::any::Any;
        match v {
            Value::Bool(x) => (x as &dyn Any)
                .downcast_ref::<bool>()
                .map(|b| Self::Bool(*b))
                .or_else(|| (x as &dyn Any).downcast_ref::<i64>().map(|i| Self::Int(*i))),
            Value::Int(x) => (x as &dyn Any).downcast_ref::<i64>().map(|i| Self::Int(*i)),
            Value::Float(x) => (x as &dyn Any)
                .downcast_ref::<f64>()
                .map(|f| Self::Float(f.to_bits())),
            Value::String(x) => (x as &dyn Any)
                .downcast_ref::<alloc::string::String>()
                .map(|s| Self::String(Arc::from(s.as_str()))),
            _ => None,
        }
    }
}

/// Indexable planner atom.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PlannerAtom {
    /// col = value
    Equality {
        column_id: ColumnId,
        value: PlannerValue,
    },
    /// col within [lower, upper] bounds
    Range {
        column_id: ColumnId,
        lower: Option<i64>,
        upper: Option<i64>,
    },
    /// col IS NULL / IS NOT NULL
    Null { column_id: ColumnId, is_null: bool },
}

#[derive(Clone, Debug)]
struct Analysis {
    trigger_atoms: BTreeSet<PlannerAtom>,
    hit_guaranteed_if_true: bool,
    true_possible: bool,
}

impl Analysis {
    const fn constant(value: bool) -> Self {
        Self {
            trigger_atoms: BTreeSet::new(),
            // If the expression cannot be true, implication is vacuously
            // true.
            hit_guaranteed_if_true: !value,
            true_possible: value,
        }
    }

    const fn unknown() -> Self {
        Self {
            trigger_atoms: BTreeSet::new(),
            hit_guaranteed_if_true: false,
            true_possible: true,
        }
    }

    fn indexed_atom(atom: PlannerAtom) -> Self {
        let mut trigger_atoms = BTreeSet::new();
        trigger_atoms.insert(atom);
        Self {
            trigger_atoms,
            hit_guaranteed_if_true: true,
            true_possible: true,
        }
    }

    fn and(lhs: Self, rhs: Self) -> Self {
        let mut trigger_atoms = lhs.trigger_atoms;
        trigger_atoms.extend(rhs.trigger_atoms);

        let true_possible = lhs.true_possible && rhs.true_possible;
        let hit_guaranteed_if_true = if true_possible {
            lhs.hit_guaranteed_if_true || rhs.hit_guaranteed_if_true
        } else {
            true
        };

        Self {
            trigger_atoms,
            hit_guaranteed_if_true,
            true_possible,
        }
    }

    fn or(lhs: Self, rhs: Self) -> Self {
        let mut trigger_atoms = lhs.trigger_atoms;
        trigger_atoms.extend(rhs.trigger_atoms);

        let true_possible = lhs.true_possible || rhs.true_possible;
        let hit_guaranteed_if_true = if true_possible {
            lhs.hit_guaranteed_if_true && rhs.hit_guaranteed_if_true
        } else {
            true
        };

        Self {
            trigger_atoms,
            hit_guaranteed_if_true,
            true_possible,
        }
    }
}

/// Build prefilter plan from WHERE clause expression.
#[must_use]
pub fn build_prefilter_plan<B: SqlLiteralParse, DB: DatabaseLike>(
    where_clause: Option<&Expr>,
    table_id: TableId,
    database: &DB,
) -> PrefilterPlan {
    let analysis = where_clause.map_or_else(
        || Analysis::constant(true),
        |expr| analyze_expr::<B, DB>(expr, table_id, database, false),
    );

    let scan_required = analysis.true_possible && !analysis.hit_guaranteed_if_true;
    let trigger_atoms = Arc::from(analysis.trigger_atoms.into_iter().collect::<Vec<_>>());

    PrefilterPlan {
        trigger_atoms,
        scan_required,
    }
}

fn analyze_expr<B: SqlLiteralParse, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    negated: bool,
) -> Analysis {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                // De Morgan: negating (A AND B) flips to OR of negated
                // terms, negating (A OR B) flips to AND of negated terms.
                let child_negated = negated;
                let effective_or = matches!(op, BinaryOperator::Or) ^ negated;

                let lhs = analyze_expr::<B, DB>(left, table_id, database, child_negated);
                let rhs = analyze_expr::<B, DB>(right, table_id, database, child_negated);
                if effective_or {
                    Analysis::or(lhs, rhs)
                } else {
                    Analysis::and(lhs, rhs)
                }
            }
            _ => analyze_comparison::<B, DB>(left, op.clone(), right, table_id, database, negated),
        },

        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => analyze_expr::<B, DB>(expr, table_id, database, !negated),

        Expr::Nested(expr) => analyze_expr::<B, DB>(expr, table_id, database, negated),

        Expr::InList {
            expr,
            list,
            negated: list_negated,
        } => analyze_in_list::<B, DB>(expr, list, *list_negated ^ negated, table_id, database),

        Expr::Between {
            expr,
            low,
            high,
            negated: between_negated,
        } => analyze_between(
            expr,
            low,
            high,
            *between_negated ^ negated,
            table_id,
            database,
        ),

        Expr::IsNull(expr) => analyze_null_check(expr, true ^ negated, table_id, database),
        Expr::IsNotNull(expr) => analyze_null_check(expr, false ^ negated, table_id, database),

        Expr::Value(val) => match &val.value {
            SqlValue::Boolean(b) => Analysis::constant(*b ^ negated),
            _ => Analysis::unknown(),
        },

        _ => Analysis::unknown(),
    }
}

fn analyze_null_check<DB: DatabaseLike>(
    expr: &Expr,
    is_null: bool,
    table_id: TableId,
    database: &DB,
) -> Analysis {
    resolve_column(expr, table_id, database).map_or_else(Analysis::unknown, |column_id| {
        Analysis::indexed_atom(PlannerAtom::Null { column_id, is_null })
    })
}

fn analyze_in_list<B: SqlLiteralParse, DB: DatabaseLike>(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    table_id: TableId,
    database: &DB,
) -> Analysis {
    if negated {
        return Analysis::unknown();
    }

    let Some(column_id) = resolve_column(expr, table_id, database) else {
        return Analysis::unknown();
    };

    if list.is_empty() {
        return Analysis::constant(false);
    }

    // Track indexable literal branches plus whether any branch could be
    // true without an index trigger (e.g. non-literal IN members).
    let mut disjuncts: Vec<Analysis> = Vec::new();
    let mut has_non_indexable_true_path = false;

    for item in list {
        match item {
            Expr::Value(v) => {
                if let Some(value) = literal_index_key::<B, DB>(item, table_id, column_id, database)
                {
                    disjuncts.push(Analysis::indexed_atom(PlannerAtom::Equality {
                        column_id,
                        value,
                    }));
                } else if !matches!(v.value, SqlValue::Null) {
                    // NULL in IN-list cannot make expression TRUE by
                    // itself, but other unsupported literal forms
                    // might.
                    has_non_indexable_true_path = true;
                }
            }
            _ => has_non_indexable_true_path = true,
        }
    }

    let mut disjunct_iter = disjuncts.into_iter();
    let mut out = disjunct_iter.next().unwrap_or_else(|| {
        if has_non_indexable_true_path {
            Analysis::unknown()
        } else {
            // Example: `col IN (NULL)` can never evaluate to TRUE.
            Analysis::constant(false)
        }
    });

    for branch in disjunct_iter {
        out = Analysis::or(out, branch);
    }

    if has_non_indexable_true_path {
        Analysis::or(out, Analysis::unknown())
    } else {
        out
    }
}

fn analyze_between<DB: DatabaseLike>(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    table_id: TableId,
    database: &DB,
) -> Analysis {
    let Some(column_id) = resolve_column(expr, table_id, database) else {
        return Analysis::unknown();
    };

    let Some(low_int) = literal_int_from_expr(low) else {
        return Analysis::unknown();
    };
    let Some(high_int) = literal_int_from_expr(high) else {
        return Analysis::unknown();
    };

    if !negated {
        return Analysis::indexed_atom(PlannerAtom::Range {
            column_id,
            lower: Some(low_int),
            upper: Some(high_int),
        });
    }

    let below = Analysis::indexed_atom(PlannerAtom::Range {
        column_id,
        lower: None,
        upper: Some(low_int.saturating_sub(1)),
    });
    let above = Analysis::indexed_atom(PlannerAtom::Range {
        column_id,
        lower: Some(high_int.saturating_add(1)),
        upper: None,
    });
    Analysis::or(below, above)
}

/// Split a comparison into the column it names, the literal it compares
/// against, and the operator oriented so the column reads on the left.
fn column_and_literal<'e, DB: DatabaseLike>(
    left: &'e Expr,
    op: BinaryOperator,
    right: &'e Expr,
    table_id: TableId,
    database: &DB,
) -> Option<(ColumnId, &'e Expr, BinaryOperator)> {
    if is_literal(right) {
        if let Some(column_id) = resolve_column(left, table_id, database) {
            return Some((column_id, right, op));
        }
    }
    if is_literal(left) {
        if let Some(column_id) = resolve_column(right, table_id, database) {
            return Some((column_id, left, flip_comparison(op)));
        }
    }
    None
}

fn analyze_comparison<B: SqlLiteralParse, DB: DatabaseLike>(
    left: &Expr,
    op: BinaryOperator,
    right: &Expr,
    table_id: TableId,
    database: &DB,
    negated: bool,
) -> Analysis {
    let Some((column_id, literal, normalized_op)) =
        column_and_literal(left, op, right, table_id, database)
    else {
        return Analysis::unknown();
    };

    match apply_negation_to_comparison(normalized_op, negated) {
        BinaryOperator::Eq => literal_index_key::<B, DB>(literal, table_id, column_id, database)
            .map_or_else(Analysis::unknown, |value| {
                Analysis::indexed_atom(PlannerAtom::Equality { column_id, value })
            }),

        BinaryOperator::Gt => literal_int_from_expr(literal).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: Some(v.saturating_add(1)),
                upper: None,
            })
        }),

        BinaryOperator::GtEq => {
            literal_int_from_expr(literal).map_or_else(Analysis::unknown, |v| {
                Analysis::indexed_atom(PlannerAtom::Range {
                    column_id,
                    lower: Some(v),
                    upper: None,
                })
            })
        }

        BinaryOperator::Lt => literal_int_from_expr(literal).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: None,
                upper: Some(v.saturating_sub(1)),
            })
        }),

        BinaryOperator::LtEq => {
            literal_int_from_expr(literal).map_or_else(Analysis::unknown, |v| {
                Analysis::indexed_atom(PlannerAtom::Range {
                    column_id,
                    lower: None,
                    upper: Some(v),
                })
            })
        }

        _ => Analysis::unknown(),
    }
}

fn apply_negation_to_comparison(op: BinaryOperator, negated: bool) -> BinaryOperator {
    if !negated {
        return op;
    }

    match op {
        BinaryOperator::Eq => BinaryOperator::NotEq,
        BinaryOperator::NotEq => BinaryOperator::Eq,
        BinaryOperator::Gt => BinaryOperator::LtEq,
        BinaryOperator::GtEq => BinaryOperator::Lt,
        BinaryOperator::Lt => BinaryOperator::GtEq,
        BinaryOperator::LtEq => BinaryOperator::Gt,
        other => other,
    }
}

fn flip_comparison(op: BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        other => other,
    }
}

fn resolve_column<DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> Option<ColumnId> {
    super::literals::resolve_column_ref(expr, table_id, database)
}

/// The index key for the literal in `expr` as compared against `column_id`,
/// or `None` when it has none, which sends the comparison to the scan set
/// for the VM to evaluate on typed values.
///
/// The literal is parsed to the column's own kind first, so the key is
/// derived from the same typed value a row cell would carry. Deriving it
/// from the SQL text instead is what made an atom unreachable: a UUID
/// literal read as text filed a String key, while the row's `Value::Uuid`
/// probed with no key at all.
fn literal_index_key<B: SqlLiteralParse, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    column_id: ColumnId,
    database: &DB,
) -> Option<PlannerValue> {
    let Expr::Value(value) = expr else {
        return None;
    };
    let kind = catalog_helpers::column_scalar_kind::<B, DB>(database, table_id, column_id)?;
    PlannerValue::from_value(&B::parse_literal(&value.value, kind).ok()?)
}

/// An integer bound for a range atom. Range entries hold `i64` bounds and
/// are probed numerically rather than by key equality, so unlike an
/// equality key this reads the number the SQL names and needs no column
/// kind.
fn literal_int_from_expr(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(value) => match &value.value {
            SqlValue::Number(n, _) => n.parse::<i64>().ok(),
            _ => None,
        },
        _ => None,
    }
}

const fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(_))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::{Postgres, SQLite};

    /// The four payloads the bitmap indexes can hold keep a key, and every
    /// other payload has none, so its comparison reaches the VM instead of
    /// an index entry nothing can probe.
    #[test]
    fn only_the_indexable_payloads_have_a_key() {
        assert_eq!(
            PlannerValue::from_value(&Value::<Postgres>::Int(42)),
            Some(PlannerValue::Int(42))
        );
        assert_eq!(
            PlannerValue::from_value(&Value::<Postgres>::Bool(true)),
            Some(PlannerValue::Bool(true))
        );
        assert!(matches!(
            PlannerValue::from_value(&Value::<Postgres>::String("x".to_string())),
            Some(PlannerValue::String(_))
        ));
        assert_eq!(
            PlannerValue::from_value(&Value::<Postgres>::Float(1.5)),
            Some(PlannerValue::Float(1.5f64.to_bits()))
        );
        for value in [
            Value::<Postgres>::Bytes(alloc::vec![0xde, 0xad]),
            Value::<Postgres>::Uuid(uuid::Uuid::nil()),
            Value::<Postgres>::Json(serde_json::Value::Null),
            Value::<Postgres>::Null,
            Value::<Postgres>::Missing,
        ] {
            assert_eq!(PlannerValue::from_value(&value), None, "{value:?}");
        }
    }

    /// SQLite spells a boolean cell as an integer, so its key is that
    /// integer. Filing a `Bool` key for a `flag = true` filter would leave
    /// the subscriber unreachable, since the row probes with `Int`.
    #[test]
    fn a_sqlite_bool_keys_as_the_integer_it_is_stored_as() {
        assert_eq!(
            PlannerValue::from_value(&Value::<SQLite>::Bool(1)),
            Some(PlannerValue::Int(1))
        );
    }
}
