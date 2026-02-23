//! Prefilter planning for candidate pruning before VM execution.
//!
//! This planner is intentionally conservative: it may return false positives,
//! but it must not return false negatives.

use super::Tri;
use crate::{Cell, ColumnId, RowImage, SchemaCatalog, TableId};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};
use std::collections::BTreeSet;
use std::sync::Arc;

/// A normalized prefilter plan used by runtime dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrefilterPlan {
    /// Boolean expression over indexable atoms + Unknown leaves.
    pub expr: PrefilterExpr,
    /// Atoms inserted into bitmap indexes for candidate triggering.
    pub trigger_atoms: Arc<[PlannerAtom]>,
    /// If true, this predicate must be considered even without index hits.
    pub scan_required: bool,
    /// If true, evaluate prefilter expression before VM to prune obvious misses.
    pub requires_prefilter_eval: bool,
}

impl PrefilterPlan {
    /// Returns true if this predicate can still evaluate to TRUE for this row.
    #[must_use]
    pub fn may_match(&self, row: &RowImage) -> bool {
        self.expr.eval_may_true(row)
    }
}

impl Default for PrefilterPlan {
    fn default() -> Self {
        Self {
            expr: PrefilterExpr::Const(true),
            trigger_atoms: Arc::from([]),
            scan_required: true,
            requires_prefilter_eval: true,
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

/// Prefilter expression in NNF (no NOT nodes).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PrefilterExpr {
    Const(bool),
    /// Unknown / non-indexable leaf.
    Unknown,
    Atom(PlannerAtom),
    And(Vec<PrefilterExpr>),
    Or(Vec<PrefilterExpr>),
}

impl PrefilterExpr {
    #[must_use]
    fn eval_may_true(&self, row: &RowImage) -> bool {
        self.eval_possibility(row).can_true
    }

    fn eval_possibility(&self, row: &RowImage) -> TriPossibility {
        match self {
            Self::Const(true) => TriPossibility::from_tri(Tri::True),
            Self::Const(false) => TriPossibility::from_tri(Tri::False),
            Self::Unknown => TriPossibility::any(),
            Self::Atom(atom) => TriPossibility::from_tri(eval_atom(atom, row)),
            Self::And(parts) => {
                let mut acc = TriPossibility::from_tri(Tri::True);
                for part in parts {
                    acc = acc.and(part.eval_possibility(row));
                }
                acc
            }
            Self::Or(parts) => {
                let mut acc = TriPossibility::from_tri(Tri::False);
                for part in parts {
                    acc = acc.or(part.eval_possibility(row));
                }
                acc
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct TriPossibility {
    can_true: bool,
    can_false: bool,
    can_unknown: bool,
}

impl TriPossibility {
    const fn from_tri(tri: Tri) -> Self {
        match tri {
            Tri::True => Self {
                can_true: true,
                can_false: false,
                can_unknown: false,
            },
            Tri::False => Self {
                can_true: false,
                can_false: true,
                can_unknown: false,
            },
            Tri::Unknown => Self {
                can_true: false,
                can_false: false,
                can_unknown: true,
            },
        }
    }

    const fn any() -> Self {
        Self {
            can_true: true,
            can_false: true,
            can_unknown: true,
        }
    }

    fn and(self, other: Self) -> Self {
        combine_possibilities(self, other, Tri::and)
    }

    fn or(self, other: Self) -> Self {
        combine_possibilities(self, other, Tri::or)
    }
}

fn combine_possibilities(
    lhs: TriPossibility,
    rhs: TriPossibility,
    op: fn(Tri, Tri) -> Tri,
) -> TriPossibility {
    let mut out = TriPossibility {
        can_true: false,
        can_false: false,
        can_unknown: false,
    };

    for a in possible_tris(lhs) {
        for b in possible_tris(rhs) {
            match op(a, b) {
                Tri::True => out.can_true = true,
                Tri::False => out.can_false = true,
                Tri::Unknown => out.can_unknown = true,
            }
        }
    }

    out
}

fn possible_tris(poss: TriPossibility) -> Vec<Tri> {
    let mut vals = Vec::with_capacity(3);
    if poss.can_true {
        vals.push(Tri::True);
    }
    if poss.can_false {
        vals.push(Tri::False);
    }
    if poss.can_unknown {
        vals.push(Tri::Unknown);
    }
    vals
}

fn eval_atom(atom: &PlannerAtom, row: &RowImage) -> Tri {
    match atom {
        PlannerAtom::Equality { column_id, value } => row
            .get(*column_id)
            .map_or(Tri::Unknown, |cell| eval_equality(cell, value)),
        PlannerAtom::Range {
            column_id,
            lower,
            upper,
        } => row
            .get(*column_id)
            .map_or(Tri::Unknown, |cell| eval_range(cell, *lower, *upper)),
        PlannerAtom::Null { column_id, is_null } => {
            row.get(*column_id).map_or(Tri::Unknown, |cell| {
                if *is_null {
                    if cell.is_null() {
                        Tri::True
                    } else {
                        Tri::False
                    }
                } else if cell.is_missing() {
                    Tri::Unknown
                } else if cell.is_null() {
                    Tri::False
                } else {
                    Tri::True
                }
            })
        }
    }
}

fn eval_equality(cell: &Cell, value: &PlannerValue) -> Tri {
    match (cell, value) {
        (Cell::Null | Cell::Missing, _) => Tri::Unknown,
        (Cell::Bool(lhs), PlannerValue::Bool(rhs)) => Tri::from_option(Some(lhs == rhs)),
        (Cell::Int(lhs), PlannerValue::Int(rhs)) => Tri::from_option(Some(lhs == rhs)),
        (Cell::Float(lhs), PlannerValue::Float(rhs_bits)) => {
            let rhs = f64::from_bits(*rhs_bits);
            if lhs.is_nan() || rhs.is_nan() {
                Tri::Unknown
            } else {
                Tri::from_option(Some(*lhs == rhs))
            }
        }
        (Cell::String(lhs), PlannerValue::String(rhs)) => Tri::from_option(Some(lhs == rhs)),
        _ => Tri::Unknown,
    }
}

fn eval_range(cell: &Cell, lower: Option<i64>, upper: Option<i64>) -> Tri {
    match cell {
        Cell::Null | Cell::Missing => Tri::Unknown,
        Cell::Int(v) => {
            let lower_ok = lower.map_or(true, |l| *v >= l);
            let upper_ok = upper.map_or(true, |u| *v <= u);
            Tri::from_option(Some(lower_ok && upper_ok))
        }
        Cell::Float(v) => {
            if v.is_nan() {
                return Tri::Unknown;
            }
            let lower_ok = lower.map_or(true, |l| *v >= l as f64);
            let upper_ok = upper.map_or(true, |u| *v <= u as f64);
            Tri::from_option(Some(lower_ok && upper_ok))
        }
        _ => Tri::Unknown,
    }
}

#[derive(Clone, Debug)]
struct Analysis {
    expr: PrefilterExpr,
    trigger_atoms: BTreeSet<PlannerAtom>,
    hit_guaranteed_if_true: bool,
    true_possible: bool,
}

impl Analysis {
    fn constant(value: bool) -> Self {
        Self {
            expr: PrefilterExpr::Const(value),
            trigger_atoms: BTreeSet::new(),
            // If expression cannot be true, implication is vacuously true.
            hit_guaranteed_if_true: !value,
            true_possible: value,
        }
    }

    fn unknown() -> Self {
        Self {
            expr: PrefilterExpr::Unknown,
            trigger_atoms: BTreeSet::new(),
            hit_guaranteed_if_true: false,
            true_possible: true,
        }
    }

    fn indexed_atom(atom: PlannerAtom) -> Self {
        let mut trigger_atoms = BTreeSet::new();
        trigger_atoms.insert(atom.clone());
        Self {
            expr: PrefilterExpr::Atom(atom),
            trigger_atoms,
            hit_guaranteed_if_true: true,
            true_possible: true,
        }
    }

    fn and(lhs: Self, rhs: Self) -> Self {
        let expr = merge_expr(PrefilterExpr::And, lhs.expr, rhs.expr);
        let mut trigger_atoms = lhs.trigger_atoms;
        trigger_atoms.extend(rhs.trigger_atoms);

        let true_possible = lhs.true_possible && rhs.true_possible;
        let hit_guaranteed_if_true = if !true_possible {
            true
        } else {
            lhs.hit_guaranteed_if_true || rhs.hit_guaranteed_if_true
        };

        Self {
            expr,
            trigger_atoms,
            hit_guaranteed_if_true,
            true_possible,
        }
    }

    fn or(lhs: Self, rhs: Self) -> Self {
        let expr = merge_expr(PrefilterExpr::Or, lhs.expr, rhs.expr);
        let mut trigger_atoms = lhs.trigger_atoms;
        trigger_atoms.extend(rhs.trigger_atoms);

        let true_possible = lhs.true_possible || rhs.true_possible;
        let hit_guaranteed_if_true = if !true_possible {
            true
        } else {
            lhs.hit_guaranteed_if_true && rhs.hit_guaranteed_if_true
        };

        Self {
            expr,
            trigger_atoms,
            hit_guaranteed_if_true,
            true_possible,
        }
    }
}

fn merge_expr(
    make: fn(Vec<PrefilterExpr>) -> PrefilterExpr,
    lhs: PrefilterExpr,
    rhs: PrefilterExpr,
) -> PrefilterExpr {
    let mut out = Vec::new();
    match lhs {
        PrefilterExpr::And(parts) if matches!(make(Vec::new()), PrefilterExpr::And(_)) => {
            out.extend(parts)
        }
        PrefilterExpr::Or(parts) if matches!(make(Vec::new()), PrefilterExpr::Or(_)) => {
            out.extend(parts)
        }
        other => out.push(other),
    }
    match rhs {
        PrefilterExpr::And(parts) if matches!(make(Vec::new()), PrefilterExpr::And(_)) => {
            out.extend(parts)
        }
        PrefilterExpr::Or(parts) if matches!(make(Vec::new()), PrefilterExpr::Or(_)) => {
            out.extend(parts)
        }
        other => out.push(other),
    }
    make(out)
}

/// Build prefilter plan from WHERE clause expression.
#[must_use]
pub fn build_prefilter_plan(
    where_clause: Option<&Expr>,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> PrefilterPlan {
    let analysis = match where_clause {
        Some(expr) => analyze_expr(expr, table_id, catalog, false),
        None => Analysis::constant(true),
    };

    let scan_required = analysis.true_possible && !analysis.hit_guaranteed_if_true;
    let trigger_atoms = Arc::from(analysis.trigger_atoms.into_iter().collect::<Vec<_>>());

    PrefilterPlan {
        expr: analysis.expr,
        trigger_atoms,
        scan_required,
        requires_prefilter_eval: scan_required,
    }
}

fn analyze_expr(
    expr: &Expr,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    negated: bool,
) -> Analysis {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                let (lhs_neg, rhs_neg, effective_or) = match (op, negated) {
                    (BinaryOperator::And, false) => (false, false, false),
                    (BinaryOperator::Or, false) => (false, false, true),
                    // De Morgan when negated.
                    (BinaryOperator::And, true) => (true, true, true),
                    (BinaryOperator::Or, true) => (true, true, false),
                    _ => unreachable!(),
                };

                let lhs = analyze_expr(left, table_id, catalog, lhs_neg);
                let rhs = analyze_expr(right, table_id, catalog, rhs_neg);
                if effective_or {
                    Analysis::or(lhs, rhs)
                } else {
                    Analysis::and(lhs, rhs)
                }
            }
            _ => analyze_comparison(left, op.clone(), right, table_id, catalog, negated),
        },

        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => analyze_expr(expr, table_id, catalog, !negated),

        Expr::UnaryOp { .. } => Analysis::unknown(),

        Expr::Nested(expr) => analyze_expr(expr, table_id, catalog, negated),

        Expr::InList {
            expr,
            list,
            negated: list_negated,
        } => analyze_in_list(expr, list, *list_negated ^ negated, table_id, catalog),

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
            catalog,
        ),

        Expr::IsNull(expr) => analyze_null_check(expr, true ^ negated, table_id, catalog),
        Expr::IsNotNull(expr) => analyze_null_check(expr, false ^ negated, table_id, catalog),

        Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::RLike { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. } => Analysis::unknown(),

        Expr::Value(val) => match &val.value {
            Value::Boolean(b) => Analysis::constant(*b ^ negated),
            _ => Analysis::unknown(),
        },

        _ => Analysis::unknown(),
    }
}

fn analyze_null_check(
    expr: &Expr,
    is_null: bool,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Analysis {
    resolve_column(expr, table_id, catalog).map_or_else(Analysis::unknown, |column_id| {
        Analysis::indexed_atom(PlannerAtom::Null { column_id, is_null })
    })
}

fn analyze_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Analysis {
    if negated {
        return Analysis::unknown();
    }

    let Some(column_id) = resolve_column(expr, table_id, catalog) else {
        return Analysis::unknown();
    };

    if list.is_empty() {
        return Analysis::constant(false);
    }

    // Track indexable literal branches plus whether any branch could be true
    // without an index trigger (e.g. non-literal IN members).
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
                    // NULL in IN-list cannot make expression TRUE by itself, but
                    // other unsupported literal forms might.
                    has_non_indexable_true_path = true;
                }
            }
            _ => has_non_indexable_true_path = true,
        }
    }

    let mut disjunct_iter = disjuncts.into_iter();
    let mut out = if let Some(first) = disjunct_iter.next() {
        first
    } else if has_non_indexable_true_path {
        Analysis::unknown()
    } else {
        // Example: `col IN (NULL)` can never evaluate to TRUE.
        Analysis::constant(false)
    };

    for branch in disjunct_iter {
        out = Analysis::or(out, branch);
    }

    if has_non_indexable_true_path {
        Analysis::or(out, Analysis::unknown())
    } else {
        out
    }
}

fn analyze_between(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Analysis {
    let Some(column_id) = resolve_column(expr, table_id, catalog) else {
        return Analysis::unknown();
    };

    let Some(low_int) = literal_int(low) else {
        return Analysis::unknown();
    };
    let Some(high_int) = literal_int(high) else {
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

fn analyze_comparison(
    left: &Expr,
    op: BinaryOperator,
    right: &Expr,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    negated: bool,
) -> Analysis {
    let mut normalized_op = op.clone();

    let comparison = if let (Some(column_id), Some(lit)) =
        (resolve_column(left, table_id, catalog), literal_cell(right))
    {
        Some((column_id, lit))
    } else if let (Some(lit), Some(column_id)) =
        (literal_cell(left), resolve_column(right, table_id, catalog))
    {
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
        BinaryOperator::Eq => planner_value_from_cell(&lit)
            .map_or_else(Analysis::unknown, |value| {
                Analysis::indexed_atom(PlannerAtom::Equality { column_id, value })
            }),

        BinaryOperator::Gt => literal_int_cell(&lit).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: Some(v.saturating_add(1)),
                upper: None,
            })
        }),

        BinaryOperator::GtEq => literal_int_cell(&lit).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: Some(v),
                upper: None,
            })
        }),

        BinaryOperator::Lt => literal_int_cell(&lit).map_or_else(Analysis::unknown, |v| {
            Analysis::indexed_atom(PlannerAtom::Range {
                column_id,
                lower: None,
                upper: Some(v.saturating_sub(1)),
            })
        }),

        BinaryOperator::LtEq => literal_int_cell(&lit).map_or_else(Analysis::unknown, |v| {
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

fn resolve_column(expr: &Expr, table_id: TableId, catalog: &dyn SchemaCatalog) -> Option<ColumnId> {
    match expr {
        Expr::Identifier(ident) => catalog.column_id(table_id, &ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            catalog.column_id(table_id, &parts[1].value)
        }
        _ => None,
    }
}

fn literal_cell(expr: &Expr) -> Option<Cell> {
    if let Expr::Value(v) = expr {
        literal_cell_from_sql_value(&v.value)
    } else {
        None
    }
}

fn literal_int(expr: &Expr) -> Option<i64> {
    literal_cell(expr).as_ref().and_then(literal_int_cell)
}

fn literal_int_cell(cell: &Cell) -> Option<i64> {
    if let Cell::Int(i) = cell {
        Some(*i)
    } else {
        None
    }
}

fn literal_cell_from_sql_value(val: &Value) -> Option<Cell> {
    match val {
        Value::Null => Some(Cell::Null),
        Value::Boolean(b) => Some(Cell::Bool(*b)),
        Value::Number(n, _long) => {
            if let Ok(i) = n.parse::<i64>() {
                Some(Cell::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Some(Cell::Float(f))
            } else {
                None
            }
        }
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::NationalStringLiteral(s)
        | Value::HexStringLiteral(s) => Some(Cell::String(s.as_str().into())),
        _ => None,
    }
}

fn planner_value_from_sql_value(val: &Value) -> Option<PlannerValue> {
    literal_cell_from_sql_value(val)
        .as_ref()
        .and_then(planner_value_from_cell)
}

fn planner_value_from_cell(cell: &Cell) -> Option<PlannerValue> {
    match cell {
        Cell::Bool(b) => Some(PlannerValue::Bool(*b)),
        Cell::Int(i) => Some(PlannerValue::Int(*i)),
        Cell::Float(f) => Some(PlannerValue::Float(f.to_bits())),
        Cell::String(s) => Some(PlannerValue::String(Arc::clone(s))),
        Cell::Null | Cell::Missing => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::compiler::parse_compile_normalize_and_prefilter;
    use crate::testing::MockCatalog;
    use sqlparser::{
        ast::{SetExpr, Statement},
        dialect::PostgreSqlDialect,
        parser::Parser,
    };
    use std::collections::HashMap;

    fn make_catalog() -> MockCatalog {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), (1, 8));

        let mut columns = HashMap::new();
        columns.insert((1, "id".to_string()), 0);
        columns.insert((1, "amount".to_string()), 1);
        columns.insert((1, "status".to_string()), 2);
        columns.insert((1, "priority".to_string()), 3);
        columns.insert((1, "created_at".to_string()), 4);

        MockCatalog { tables, columns }
    }

    #[test]
    fn test_or_of_indexable_not_scan_required() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM orders WHERE amount = 10 OR amount = 20";
        let (_, _, _, plan) =
            parse_compile_normalize_and_prefilter(sql, &dialect, &catalog).unwrap();

        assert!(!plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 2);
    }

    #[test]
    fn test_or_with_unknown_becomes_scan_required() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM orders WHERE amount = 10 OR status LIKE 'a%'";
        let (_, _, _, plan) =
            parse_compile_normalize_and_prefilter(sql, &dialect, &catalog).unwrap();

        assert!(plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 1);
    }

    #[test]
    fn test_not_between_gets_indexable_ranges() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM orders WHERE amount NOT BETWEEN 10 AND 20";
        let (_, _, _, plan) =
            parse_compile_normalize_and_prefilter(sql, &dialect, &catalog).unwrap();

        assert!(!plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 2);
    }

    #[test]
    fn test_in_list_with_non_literal_branch_requires_scan() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM orders WHERE amount IN (10, priority)";
        let stmts = Parser::parse_sql(&dialect, sql).unwrap();
        let expr = match &stmts[0] {
            Statement::Query(q) => match q.body.as_ref() {
                SetExpr::Select(s) => s.selection.clone().unwrap(),
                _ => panic!("unexpected non-select body"),
            },
            _ => panic!("unexpected non-query statement"),
        };
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 1);
    }

    #[test]
    fn test_in_list_null_only_is_constant_false() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM orders WHERE amount IN (NULL)";
        let (_, _, _, plan) =
            parse_compile_normalize_and_prefilter(sql, &dialect, &catalog).unwrap();

        assert!(!plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 0);
        assert!(!plan.may_match(&RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Int(10), Cell::String("pending".into())]),
        }));
    }

    #[test]
    fn test_plan_may_match_prunes_definitely_false() {
        let atom = PlannerAtom::Equality {
            column_id: 1,
            value: PlannerValue::Int(42),
        };
        let plan = PrefilterPlan {
            expr: PrefilterExpr::Atom(atom),
            trigger_atoms: Arc::from([]),
            scan_required: true,
            requires_prefilter_eval: true,
        };

        let row = RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Int(7)]),
        };
        assert!(!plan.may_match(&row));
    }
}
