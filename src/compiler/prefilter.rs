//! Prefilter planning for candidate pruning before VM execution.
//!
//! The planner is intentionally conservative: it may return false
//! positives, but it must not return false negatives. The plan feeds
//! two consumers. `trigger_atoms` is the set of indexable atoms the
//! bitmap indexes ingest for candidate triggering. `scan_required`
//! marks predicates that must be considered even when no index atom
//! fires (SQL that the analyser could not reduce to a purely indexable
//! form).

use crate::{ColumnId, TableId};
use alloc::collections::BTreeSet;
use alloc::sync::Arc;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};

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
pub fn build_prefilter_plan<DB: DatabaseLike>(
    where_clause: Option<&Expr>,
    table_id: TableId,
    database: &DB,
) -> PrefilterPlan {
    let analysis = where_clause.map_or_else(
        || Analysis::constant(true),
        |expr| analyze_expr(expr, table_id, database, false),
    );

    let scan_required = analysis.true_possible && !analysis.hit_guaranteed_if_true;
    let trigger_atoms = Arc::from(analysis.trigger_atoms.into_iter().collect::<Vec<_>>());

    PrefilterPlan {
        trigger_atoms,
        scan_required,
    }
}

fn analyze_expr<DB: DatabaseLike>(
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

                let lhs = analyze_expr(left, table_id, database, child_negated);
                let rhs = analyze_expr(right, table_id, database, child_negated);
                if effective_or {
                    Analysis::or(lhs, rhs)
                } else {
                    Analysis::and(lhs, rhs)
                }
            }
            _ => analyze_comparison(left, op.clone(), right, table_id, database, negated),
        },

        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => analyze_expr(expr, table_id, database, !negated),

        Expr::Nested(expr) => analyze_expr(expr, table_id, database, negated),

        Expr::InList {
            expr,
            list,
            negated: list_negated,
        } => analyze_in_list(expr, list, *list_negated ^ negated, table_id, database),

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
            Value::Boolean(b) => Analysis::constant(*b ^ negated),
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

fn analyze_in_list<DB: DatabaseLike>(
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
                if let Some(value) = planner_value_from_sql_value(&v.value) {
                    disjuncts.push(Analysis::indexed_atom(PlannerAtom::Equality {
                        column_id,
                        value,
                    }));
                } else if !matches!(v.value, Value::Null) {
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

fn analyze_comparison<DB: DatabaseLike>(
    left: &Expr,
    op: BinaryOperator,
    right: &Expr,
    table_id: TableId,
    database: &DB,
    negated: bool,
) -> Analysis {
    let mut normalized_op = op.clone();

    let comparison = if let (Some(column_id), Some(lit)) = (
        resolve_column(left, table_id, database),
        literal_planner_value(right),
    ) {
        Some((column_id, lit))
    } else if let (Some(lit), Some(column_id)) = (
        literal_planner_value(left),
        resolve_column(right, table_id, database),
    ) {
        normalized_op = flip_comparison(op);
        Some((column_id, lit))
    } else {
        None
    };

    let Some((column_id, lit)) = comparison else {
        return Analysis::unknown();
    };

    let effective_op = apply_negation_to_comparison(normalized_op, negated);

    match effective_op {
        BinaryOperator::Eq => Analysis::indexed_atom(PlannerAtom::Equality {
            column_id,
            value: lit,
        }),

        BinaryOperator::Gt => planner_value_as_int(&lit).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: Some(v.saturating_add(1)),
                upper: None,
            })
        }),

        BinaryOperator::GtEq => planner_value_as_int(&lit).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: Some(v),
                upper: None,
            })
        }),

        BinaryOperator::Lt => planner_value_as_int(&lit).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: None,
                upper: Some(v.saturating_sub(1)),
            })
        }),

        BinaryOperator::LtEq => planner_value_as_int(&lit).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: None,
                upper: Some(v),
            })
        }),

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

fn literal_planner_value(expr: &Expr) -> Option<PlannerValue> {
    if let Expr::Value(v) = expr {
        planner_value_from_sql_value(&v.value)
    } else {
        None
    }
}

fn literal_int_from_expr(expr: &Expr) -> Option<i64> {
    match literal_planner_value(expr)? {
        PlannerValue::Int(v) => Some(v),
        _ => None,
    }
}

const fn planner_value_as_int(value: &PlannerValue) -> Option<i64> {
    match value {
        PlannerValue::Int(v) => Some(*v),
        _ => None,
    }
}

fn planner_value_from_sql_value(val: &Value) -> Option<PlannerValue> {
    match val {
        Value::Boolean(b) => Some(PlannerValue::Bool(*b)),
        Value::Number(n, _) => n
            .parse::<i64>()
            .map(PlannerValue::Int)
            .ok()
            .or_else(|| n.parse::<f64>().map(|f| PlannerValue::Float(f.to_bits())).ok()),
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::NationalStringLiteral(s)
        | Value::HexStringLiteral(s) => Some(PlannerValue::String(Arc::from(s.as_str()))),
        _ => None,
    }
}
