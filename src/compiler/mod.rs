//! SQL compilation pipeline: parse -> normalize -> compile to VM bytecode

pub mod bytecode;
pub mod canonicalize;
pub mod literals;
pub mod parser;
pub mod prefilter;
pub mod sql_shape;
pub mod tristate;
pub(crate) mod value_cmp;
pub mod vm;

pub use bytecode::{BytecodeProgram, Instruction};
pub use canonicalize::{hash_sql, normalize_sql, PredicateHash};
pub use literals::{parse_custom_literal, SqlLiteralParse};
pub use parser::{
    derive_update_follow_select, derive_update_follow_select_with_set_binds, parse_and_compile,
    parse_and_resolve_hash, parse_compile_and_normalize, parse_compile_normalize_and_prefilter,
    parse_compile_normalize_and_prefilter_with_binds, CompiledQuery, MAX_TERMS_PER_FILTER,
};
pub use prefilter::{PlannerAtom, PlannerValue, PrefilterPlan};
pub use sql_shape::{AggHaving, AggSpec, HavingFunction, HavingOp, HavingSubject, QueryProjection};
pub use tristate::Tri;
pub use vm::{Vm, VmError};

/// Delimit `name` the way `dialect` does, leaving it bare for a dialect that
/// names no quoting style.
///
/// The one place subql decides how a runtime-supplied identifier is spelled.
/// A delimited identifier escapes its own delimiter by doubling it, and
/// sqlparser's [`Ident`](sqlparser::ast::Ident) renderer is what knows that, so
/// this returns the node rather than text and lets the caller render it. Every
/// hand-built variant of this that subql used to carry got the doubling either
/// right by accident or wrong by omission.
pub(crate) fn quoted_ident(
    dialect: &dyn sqlparser::dialect::Dialect,
    name: &str,
) -> sqlparser::ast::Ident {
    use sqlparser::ast::Ident;

    dialect
        .identifier_quote_style(name)
        .map_or_else(|| Ident::new(name), |quote| Ident::with_quote(quote, name))
}

/// Which placeholder form a dialect's server takes for an ordered, unnamed bind
/// list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BindPlaceholder {
    /// `$1`, `$2`, numbered from one. Postgres.
    Numbered,
    /// One `?` per bind, consumed left to right. MySQL and SQLite.
    Positional,
}

/// The bind form `dialect`'s server takes.
///
/// **Interim.** The right home for this is sqlparser, beside
/// `identifier_quote_style`, which is the same kind of consumer-facing rendering
/// hint and has no call site inside the parser either. The finding is written up
/// at `upstream/sqlparser-placeholder-dialect.md`. This mapping goes away when
/// that method lands.
///
/// **Do not reach for `Dialect::supports_dollar_placeholder`.** It reads like
/// this predicate and is not: it means "this dialect allows `$var` named
/// placeholders", it is true only for `SQLiteDialect`, and substituting it hands
/// `$1` to SQLite and `?` to Postgres, which is the exact inversion this
/// function exists to avoid.
///
/// The three supported backends answer explicitly, so adding a fourth has to
/// say which form it wants. A dialect subql does not ship a backend for falls
/// back to asking whether the dialect treats `$` as an identifier character,
/// which is what this call site did for every dialect before.
pub(crate) fn bind_placeholder(dialect: &dyn sqlparser::dialect::Dialect) -> BindPlaceholder {
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};

    // The three backends subql ships answer explicitly.
    if dialect.is::<PostgreSqlDialect>() {
        return BindPlaceholder::Numbered;
    }
    if dialect.is::<MySqlDialect>() || dialect.is::<SQLiteDialect>() {
        return BindPlaceholder::Positional;
    }
    // A dialect subql ships no backend for keeps what this call site did for
    // every dialect before: ask whether `$` starts an identifier there.
    if dialect.is_identifier_start('$') {
        BindPlaceholder::Positional
    } else {
        BindPlaceholder::Numbered
    }
}

#[cfg(test)]
mod tests {
    use super::{bind_placeholder, quoted_ident};
    use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
    use sqlparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};

    /// A name carrying the dialect's own delimiter survives a render and a
    /// re-parse unchanged, on every dialect subql supports.
    ///
    /// Exhaustive over the three deliberately, in the style of the gate test in
    /// `crate::term`, so adding a fourth backend has to say what its delimiter
    /// is rather than inherit an answer.
    #[test]
    fn quoting_round_trips_every_dialect_delimiter() {
        let dialects: [&dyn Dialect; 3] =
            [&PostgreSqlDialect {}, &MySqlDialect {}, &SQLiteDialect {}];
        for dialect in dialects {
            let delimiter = dialect
                .identifier_quote_style("x")
                .expect("every supported dialect delimits identifiers");
            let name = alloc::format!("a{delimiter}b");
            let rendered = quoted_ident(dialect, &name).to_string();

            let sql = alloc::format!("SELECT {rendered} FROM t");
            let mut parsed = sqlparser::parser::Parser::parse_sql(dialect, &sql)
                .unwrap_or_else(|e| panic!("`{sql}` must re-parse, got {e}"));
            let Statement::Query(query) = parsed.remove(0) else {
                panic!("a SELECT");
            };
            let SetExpr::Select(select) = *query.body else {
                panic!("a plain SELECT");
            };
            let Some(SelectItem::UnnamedExpr(Expr::Identifier(ident))) = select.projection.first()
            else {
                panic!("expected a bare identifier, got {:?}", select.projection);
            };
            assert_eq!(
                ident.value, name,
                "delimiter {delimiter:?} did not round trip, rendered as `{rendered}`"
            );
        }
    }

    /// Each supported dialect's server is asked for the bind form it actually
    /// takes, not for something correlated with it.
    ///
    /// Exhaustive over the three on purpose: a fourth backend has to say which
    /// form it wants rather than inherit one.
    #[test]
    fn bind_placeholder_syntax_per_dialect() {
        use super::BindPlaceholder;

        assert_eq!(
            bind_placeholder(&PostgreSqlDialect {}),
            BindPlaceholder::Numbered
        );
        assert_eq!(
            bind_placeholder(&MySqlDialect {}),
            BindPlaceholder::Positional
        );
        assert_eq!(
            bind_placeholder(&SQLiteDialect {}),
            BindPlaceholder::Positional
        );
    }
}
