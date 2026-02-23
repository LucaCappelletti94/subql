//! Prefilter planning for candidate pruning before VM execution.
//!
//! This planner is intentionally conservative: it may return false positives,
//! but it must not return false negatives.

use super::Tri;
use crate::{Cell, ColumnId, RowImage, SchemaCatalog, TableId};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};
use std::cmp::Ordering;
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
    And(Vec<Self>),
    Or(Vec<Self>),
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
        (Cell::Bool(lhs), PlannerValue::Bool(rhs)) => Tri::from_option(Some(lhs == rhs)),
        (Cell::Int(lhs), PlannerValue::Int(rhs)) => Tri::from_option(Some(lhs == rhs)),
        (Cell::Float(lhs), PlannerValue::Float(rhs_bits)) => {
            let rhs = f64::from_bits(*rhs_bits);
            match lhs.partial_cmp(&rhs) {
                Some(Ordering::Equal) => Tri::True,
                Some(Ordering::Less | Ordering::Greater) => Tri::False,
                None => Tri::Unknown,
            }
        }
        (Cell::String(lhs), PlannerValue::String(rhs)) => Tri::from_option(Some(lhs == rhs)),
        _ => Tri::Unknown,
    }
}

fn eval_range(cell: &Cell, lower: Option<i64>, upper: Option<i64>) -> Tri {
    match cell {
        Cell::Int(v) => {
            let lower_ok = lower.is_none_or(|l| *v >= l);
            let upper_ok = upper.is_none_or(|u| *v <= u);
            Tri::from_option(Some(lower_ok && upper_ok))
        }
        Cell::Float(v) => {
            if v.is_nan() {
                return Tri::Unknown;
            }
            #[allow(clippy::cast_precision_loss)]
            let lower_ok = lower.is_none_or(|l| *v >= l as f64);
            #[allow(clippy::cast_precision_loss)]
            let upper_ok = upper.is_none_or(|u| *v <= u as f64);
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
    const fn constant(value: bool) -> Self {
        Self {
            expr: PrefilterExpr::Const(value),
            trigger_atoms: BTreeSet::new(),
            // If expression cannot be true, implication is vacuously true.
            hit_guaranteed_if_true: !value,
            true_possible: value,
        }
    }

    const fn unknown() -> Self {
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
        let hit_guaranteed_if_true = if true_possible {
            lhs.hit_guaranteed_if_true || rhs.hit_guaranteed_if_true
        } else {
            true
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
        let hit_guaranteed_if_true = if true_possible {
            lhs.hit_guaranteed_if_true && rhs.hit_guaranteed_if_true
        } else {
            true
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
            out.extend(parts);
        }
        PrefilterExpr::Or(parts) if matches!(make(Vec::new()), PrefilterExpr::Or(_)) => {
            out.extend(parts);
        }
        other => out.push(other),
    }
    match rhs {
        PrefilterExpr::And(parts) if matches!(make(Vec::new()), PrefilterExpr::And(_)) => {
            out.extend(parts);
        }
        PrefilterExpr::Or(parts) if matches!(make(Vec::new()), PrefilterExpr::Or(_)) => {
            out.extend(parts);
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
    let analysis = where_clause.map_or_else(
        || Analysis::constant(true),
        |expr| analyze_expr(expr, table_id, catalog, false),
    );

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
                // De Morgan: negating (A AND B) flips to OR of negated terms;
                // negating (A OR B) flips to AND of negated terms.
                let child_negated = negated;
                let effective_or = matches!(op, BinaryOperator::Or) ^ negated;

                let lhs = analyze_expr(left, table_id, catalog, child_negated);
                let rhs = analyze_expr(right, table_id, catalog, child_negated);
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

const fn literal_int_cell(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Int(i) => Some(*i),
        _ => None,
    }
}

fn literal_cell_from_sql_value(val: &Value) -> Option<Cell> {
    match val {
        Value::Null => Some(Cell::Null),
        Value::Boolean(b) => Some(Cell::Bool(*b)),
        Value::Number(n, _long) => n
            .parse::<i64>()
            .map(Cell::Int)
            .or_else(|_| n.parse::<f64>().map(Cell::Float))
            .ok(),
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
        ast::{Expr, SetExpr, Statement},
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

    fn parse_where_expr(sql: &str) -> Expr {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).unwrap();
        match &stmts[0] {
            Statement::Query(q) => match q.body.as_ref() {
                SetExpr::Select(s) => s.selection.clone().unwrap(),
                _ => panic!("unexpected non-select body"),
            },
            _ => panic!("unexpected non-query statement"),
        }
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

    #[test]
    fn test_not_in_requires_scan_and_no_atoms() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE amount NOT IN (10, 20)");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(plan.scan_required);
        assert!(plan.requires_prefilter_eval);
        assert_eq!(plan.trigger_atoms.len(), 0);
        assert!(matches!(plan.expr, PrefilterExpr::Unknown));
    }

    #[test]
    fn test_not_not_equal_becomes_equality_atom() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE NOT (amount <> 10)");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(!plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 1);
        assert!(matches!(
            plan.trigger_atoms[0],
            PlannerAtom::Equality {
                column_id: 1,
                value: PlannerValue::Int(10)
            }
        ));
    }

    #[test]
    fn test_operand_flipped_comparison_builds_expected_range() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE 10 < amount");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(!plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 1);
        assert!(matches!(
            plan.trigger_atoms[0],
            PlannerAtom::Range {
                column_id: 1,
                lower: Some(11),
                upper: None
            }
        ));
    }

    #[test]
    fn test_not_greater_than_becomes_lte_range() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE NOT (amount > 10)");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(!plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 1);
        assert!(matches!(
            plan.trigger_atoms[0],
            PlannerAtom::Range {
                column_id: 1,
                lower: None,
                upper: Some(10)
            }
        ));
    }

    #[test]
    fn test_column_to_column_comparison_is_unknown() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE amount > priority");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 0);
        assert!(matches!(plan.expr, PrefilterExpr::Unknown));
    }

    #[test]
    fn test_compound_identifier_two_parts_is_indexed() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE orders.amount = 10");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(!plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 1);
        assert!(matches!(
            plan.trigger_atoms[0],
            PlannerAtom::Equality {
                column_id: 1,
                value: PlannerValue::Int(10)
            }
        ));
    }

    #[test]
    fn test_compound_identifier_three_parts_is_not_indexed() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE public.orders.amount = 10");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(plan.scan_required);
        assert_eq!(plan.trigger_atoms.len(), 0);
        assert!(matches!(plan.expr, PrefilterExpr::Unknown));
    }

    #[test]
    fn test_is_null_and_is_not_null_eval_paths() {
        let catalog = make_catalog();
        let is_null_expr = parse_where_expr("SELECT * FROM orders WHERE amount IS NULL");
        let is_not_null_expr = parse_where_expr("SELECT * FROM orders WHERE amount IS NOT NULL");

        let is_null_plan = build_prefilter_plan(Some(&is_null_expr), 1, &catalog);
        let is_not_null_plan = build_prefilter_plan(Some(&is_not_null_expr), 1, &catalog);

        let row_null = RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Null]),
        };
        let row_missing = RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Missing]),
        };
        let row_present = RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Int(7)]),
        };

        assert!(is_null_plan.may_match(&row_null));
        assert!(!is_null_plan.may_match(&row_missing));
        assert!(!is_null_plan.may_match(&row_present));

        assert!(!is_not_null_plan.may_match(&row_null));
        assert!(!is_not_null_plan.may_match(&row_missing));
        assert!(is_not_null_plan.may_match(&row_present));
    }

    #[test]
    fn test_constant_true_and_false_plans() {
        let catalog = make_catalog();
        let expr_true = parse_where_expr("SELECT * FROM orders WHERE TRUE");
        let expr_false = parse_where_expr("SELECT * FROM orders WHERE FALSE");

        let true_plan = build_prefilter_plan(Some(&expr_true), 1, &catalog);
        let false_plan = build_prefilter_plan(Some(&expr_false), 1, &catalog);
        let row = RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Int(2)]),
        };

        assert!(true_plan.scan_required);
        assert!(true_plan.may_match(&row));
        assert_eq!(true_plan.trigger_atoms.len(), 0);

        assert!(!false_plan.scan_required);
        assert!(!false_plan.may_match(&row));
        assert_eq!(false_plan.trigger_atoms.len(), 0);
    }

    #[test]
    fn test_and_or_expression_flattening() {
        let catalog = make_catalog();
        let and_expr =
            parse_where_expr("SELECT * FROM orders WHERE amount = 1 AND priority = 2 AND id = 3");
        let or_expr =
            parse_where_expr("SELECT * FROM orders WHERE amount = 1 OR priority = 2 OR id = 3");

        let and_plan = build_prefilter_plan(Some(&and_expr), 1, &catalog);
        let or_plan = build_prefilter_plan(Some(&or_expr), 1, &catalog);

        assert!(matches!(and_plan.expr, PrefilterExpr::And(ref parts) if parts.len() == 3));
        assert!(matches!(or_plan.expr, PrefilterExpr::Or(ref parts) if parts.len() == 3));
    }

    #[test]
    fn test_float_nan_paths_do_not_report_may_match() {
        let row = RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Float(f64::NAN)]),
        };

        let eq_nan_plan = PrefilterPlan {
            expr: PrefilterExpr::Atom(PlannerAtom::Equality {
                column_id: 1,
                value: PlannerValue::Float(f64::NAN.to_bits()),
            }),
            trigger_atoms: Arc::from([]),
            scan_required: true,
            requires_prefilter_eval: true,
        };
        let range_nan_plan = PrefilterPlan {
            expr: PrefilterExpr::Atom(PlannerAtom::Range {
                column_id: 1,
                lower: Some(0),
                upper: Some(10),
            }),
            trigger_atoms: Arc::from([]),
            scan_required: true,
            requires_prefilter_eval: true,
        };

        assert!(!eq_nan_plan.may_match(&row));
        assert!(!range_nan_plan.may_match(&row));
    }

    #[test]
    fn test_tri_possibility_any_and_combine_helpers() {
        let any = TriPossibility::any();
        let vals = possible_tris(any);
        assert!(vals.contains(&Tri::True));
        assert!(vals.contains(&Tri::False));
        assert!(vals.contains(&Tri::Unknown));

        let and_poss = combine_possibilities(any, any, Tri::and);
        assert!(and_poss.can_true);
        assert!(and_poss.can_false);
        assert!(and_poss.can_unknown);

        let or_poss = combine_possibilities(any, any, Tri::or);
        assert!(or_poss.can_true);
        assert!(or_poss.can_false);
        assert!(or_poss.can_unknown);
    }

    #[test]
    fn test_prefilter_expr_eval_possibility_paths() {
        let row = RowImage {
            cells: Arc::from([Cell::Int(7)]),
        };

        assert!(PrefilterExpr::Unknown.eval_may_true(&row));
        assert!(
            PrefilterExpr::And(vec![PrefilterExpr::Const(true), PrefilterExpr::Unknown])
                .eval_may_true(&row)
        );
        assert!(
            PrefilterExpr::Or(vec![PrefilterExpr::Const(false), PrefilterExpr::Unknown])
                .eval_may_true(&row)
        );
    }

    #[test]
    fn test_eval_atom_null_and_out_of_bounds_paths() {
        let row = RowImage {
            cells: Arc::from([Cell::Null, Cell::Missing, Cell::Int(1)]),
        };

        assert_eq!(
            eval_atom(
                &PlannerAtom::Null {
                    column_id: 0,
                    is_null: true
                },
                &row
            ),
            Tri::True
        );
        assert_eq!(
            eval_atom(
                &PlannerAtom::Null {
                    column_id: 2,
                    is_null: true
                },
                &row
            ),
            Tri::False
        );
        assert_eq!(
            eval_atom(
                &PlannerAtom::Null {
                    column_id: 1,
                    is_null: false
                },
                &row
            ),
            Tri::Unknown
        );
        assert_eq!(
            eval_atom(
                &PlannerAtom::Null {
                    column_id: 0,
                    is_null: false
                },
                &row
            ),
            Tri::False
        );
        assert_eq!(
            eval_atom(
                &PlannerAtom::Null {
                    column_id: 2,
                    is_null: false
                },
                &row
            ),
            Tri::True
        );
        assert_eq!(
            eval_atom(
                &PlannerAtom::Null {
                    column_id: 9,
                    is_null: true
                },
                &row
            ),
            Tri::Unknown
        );
    }

    #[test]
    fn test_eval_equality_and_range_unknown_paths() {
        assert_eq!(
            eval_equality(&Cell::Bool(true), &PlannerValue::Bool(true)),
            Tri::True
        );
        assert_eq!(
            eval_equality(&Cell::Float(1.0), &PlannerValue::Float(2.0_f64.to_bits())),
            Tri::False
        );
        assert_eq!(
            eval_equality(&Cell::String("x".into()), &PlannerValue::String("x".into())),
            Tri::True
        );
        assert_eq!(
            eval_equality(&Cell::Int(1), &PlannerValue::Bool(true)),
            Tri::Unknown
        );

        assert_eq!(eval_range(&Cell::Int(5), Some(0), Some(10)), Tri::True);
        assert_eq!(eval_range(&Cell::Float(5.5), Some(0), Some(5)), Tri::False);
        assert_eq!(
            eval_range(&Cell::Bool(true), Some(0), Some(1)),
            Tri::Unknown
        );
    }

    #[test]
    fn test_analysis_boolean_algebra_helpers() {
        let eq = PlannerAtom::Equality {
            column_id: 1,
            value: PlannerValue::Int(1),
        };

        let both_false = Analysis::or(Analysis::constant(false), Analysis::constant(false));
        assert!(!both_false.true_possible);
        assert!(both_false.hit_guaranteed_if_true);

        let and_with_false = Analysis::and(Analysis::constant(false), Analysis::indexed_atom(eq));
        assert!(!and_with_false.true_possible);
        assert!(and_with_false.hit_guaranteed_if_true);
    }

    #[test]
    fn test_apply_negation_and_flip_helpers() {
        assert!(matches!(
            apply_negation_to_comparison(BinaryOperator::Eq, true),
            BinaryOperator::NotEq
        ));
        assert!(matches!(
            apply_negation_to_comparison(BinaryOperator::NotEq, true),
            BinaryOperator::Eq
        ));
        assert!(matches!(
            apply_negation_to_comparison(BinaryOperator::Gt, true),
            BinaryOperator::LtEq
        ));
        assert!(matches!(
            apply_negation_to_comparison(BinaryOperator::GtEq, true),
            BinaryOperator::Lt
        ));
        assert!(matches!(
            apply_negation_to_comparison(BinaryOperator::Lt, true),
            BinaryOperator::GtEq
        ));
        assert!(matches!(
            apply_negation_to_comparison(BinaryOperator::LtEq, true),
            BinaryOperator::Gt
        ));
        assert!(matches!(
            apply_negation_to_comparison(BinaryOperator::Plus, true),
            BinaryOperator::Plus
        ));

        assert!(matches!(
            flip_comparison(BinaryOperator::Gt),
            BinaryOperator::Lt
        ));
        assert!(matches!(
            flip_comparison(BinaryOperator::GtEq),
            BinaryOperator::LtEq
        ));
        assert!(matches!(
            flip_comparison(BinaryOperator::Lt),
            BinaryOperator::Gt
        ));
        assert!(matches!(
            flip_comparison(BinaryOperator::LtEq),
            BinaryOperator::GtEq
        ));
        assert!(matches!(
            flip_comparison(BinaryOperator::Eq),
            BinaryOperator::Eq
        ));
    }

    #[test]
    fn test_literal_and_planner_value_helpers() {
        assert_eq!(literal_int_cell(&Cell::Int(9)), Some(9));
        assert_eq!(literal_int_cell(&Cell::Float(9.0)), None);

        assert_eq!(literal_cell_from_sql_value(&Value::Null), Some(Cell::Null));
        assert_eq!(
            literal_cell_from_sql_value(&Value::Boolean(true)),
            Some(Cell::Bool(true))
        );
        assert_eq!(
            literal_cell_from_sql_value(&Value::Number("123".to_string(), false)),
            Some(Cell::Int(123))
        );
        assert_eq!(
            literal_cell_from_sql_value(&Value::Number("1.5".to_string(), false)),
            Some(Cell::Float(1.5))
        );
        assert_eq!(
            literal_cell_from_sql_value(&Value::SingleQuotedString("x".to_string())),
            Some(Cell::String("x".into()))
        );
        assert_eq!(
            literal_cell_from_sql_value(&Value::DoubleQuotedString("x".to_string())),
            Some(Cell::String("x".into()))
        );
        assert_eq!(
            literal_cell_from_sql_value(&Value::NationalStringLiteral("x".to_string())),
            Some(Cell::String("x".into()))
        );
        assert_eq!(
            literal_cell_from_sql_value(&Value::HexStringLiteral("DEAD".to_string())),
            Some(Cell::String("DEAD".into()))
        );

        assert_eq!(
            planner_value_from_cell(&Cell::Bool(true)),
            Some(PlannerValue::Bool(true))
        );
        assert_eq!(
            planner_value_from_cell(&Cell::Int(7)),
            Some(PlannerValue::Int(7))
        );
        assert_eq!(
            planner_value_from_cell(&Cell::Float(3.0)),
            Some(PlannerValue::Float(3.0_f64.to_bits()))
        );
        assert_eq!(
            planner_value_from_cell(&Cell::String("abc".into())),
            Some(PlannerValue::String("abc".into()))
        );
        assert_eq!(planner_value_from_cell(&Cell::Null), None);
        assert_eq!(planner_value_from_cell(&Cell::Missing), None);
    }

    #[test]
    fn test_in_list_empty_and_unknown_column_paths() {
        let catalog = make_catalog();
        let mut expr = parse_where_expr("SELECT * FROM orders WHERE amount IN (1)");

        if let Expr::InList { list, .. } = &mut expr {
            list.clear();
        } else {
            panic!("expected IN list expression");
        }

        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);
        assert!(!plan.scan_required);
        assert!(!plan.may_match(&RowImage {
            cells: Arc::from([Cell::Int(1)])
        }));

        let unknown_col = parse_where_expr("SELECT * FROM orders WHERE nope IN (1)");
        let unknown_plan = build_prefilter_plan(Some(&unknown_col), 1, &catalog);
        assert!(unknown_plan.scan_required);
        assert!(matches!(unknown_plan.expr, PrefilterExpr::Unknown));
    }

    #[test]
    fn test_between_non_numeric_bounds_is_unknown() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE amount BETWEEN 'a' AND 10");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(plan.scan_required);
        assert!(matches!(plan.expr, PrefilterExpr::Unknown));
    }

    #[test]
    fn test_boolean_negation_path_in_analyze_expr() {
        let catalog = make_catalog();
        let expr = parse_where_expr("SELECT * FROM orders WHERE NOT TRUE");
        let plan = build_prefilter_plan(Some(&expr), 1, &catalog);

        assert!(!plan.scan_required);
        assert!(!plan.may_match(&RowImage {
            cells: Arc::from([Cell::Int(1)])
        }));
    }
}
