//! Execute a re-execution query against a caller-supplied database connection
//! and decode the scalar result into a subql [`Cell`].
//!
//! The wrapper is connection-agnostic: callers plug in their own pooling by
//! implementing [`ConnectionProvider`]. A ready-made r2d2 implementation is
//! available under the `reexec-r2d2` feature.

use crate::{Cell, ColumnType};
use alloc::string::String;
use alloc::sync::Arc;
use diesel::query_builder::SqlQuery;
use diesel::query_dsl::LoadQuery;
use diesel::sql_types::{BigInt, Double, Nullable, Text};
use diesel::{sql_query, Connection, QueryResult, RunQueryDsl};

/// Source of database connections for re-execution.
///
/// Implement this over a connection pool (r2d2, deadpool, bb8) or a single
/// connection. The wrapper calls [`with_connection`](Self::with_connection)
/// whenever a routed CDC event requires re-running a query.
pub trait ConnectionProvider {
    /// The diesel connection type handed to the closure.
    type Connection: Connection;
    /// Error returned when a connection cannot be obtained (e.g. pool timeout).
    type Error: core::fmt::Display;

    /// Acquire a connection and run `f` with it.
    fn with_connection<R>(
        &self,
        f: impl FnOnce(&mut Self::Connection) -> R,
    ) -> Result<R, Self::Error>;
}

/// Failure modes of a scalar re-execution.
#[derive(Debug, thiserror::Error)]
pub enum ReExecError<E> {
    /// The connection provider could not hand out a connection.
    #[error("could not acquire a database connection: {0}")]
    Connection(E),
    /// The database rejected or failed the query.
    #[error("re-execution query failed: {0}")]
    Query(diesel::result::Error),
}

// Row types are `pub` (rather than `pub(crate)`) only so the `ScalarSource`
// blanket impl's bounds, which name them, do not trip `private_bounds`. The
// `executor` module is private and these are not re-exported, so they remain
// effectively crate-internal.
#[derive(diesel::QueryableByName)]
pub struct IntRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    v: Option<i64>,
}

#[derive(diesel::QueryableByName)]
pub struct FloatRow {
    #[diesel(sql_type = Nullable<Double>)]
    v: Option<f64>,
}

#[derive(diesel::QueryableByName)]
pub struct TextRow {
    #[diesel(sql_type = Nullable<Text>)]
    v: Option<String>,
}

// FUTURE (single-table row re-execution): a row-loading extension trait
// parallel to `ScalarSource`. A query whose projection is a single base table
// (a join/subquery in the filter, output rows of one table) re-executes to the
// matching rows of that *known* base table, keyed by its primary key; the
// engine diffs the PK set against the cached one and emits per-table row
// deltas. The result schema is the catalog-known base table - not an arbitrary
// result set - so the loader is bounded by that table's columns. Not
// implemented in v1 (the wrapper serves single-table scalar MIN/MAX).

/// A [`ConnectionProvider`] whose backend can decode the scalar row types
/// re-execution needs.
///
/// Blanket-implemented for every provider whose connection satisfies diesel's
/// loading bounds, so the engine can bound on `P: ScalarSource` without those
/// bounds (and the private row types they name) leaking into its public API.
/// Not implemented manually.
pub trait ScalarSource: ConnectionProvider {
    /// Run `sql` (which must project a single column aliased `v`) and decode
    /// the scalar into a [`Cell`] according to `column_type`.
    ///
    /// A SQL NULL decodes to [`Cell::Null`]; this includes the empty-input
    /// case, since `MIN`/`MAX` over no matching rows yields a single NULL row.
    /// Integer columns load as [`Cell::Int`], float columns as [`Cell::Float`],
    /// and everything else (text, bool, unknown) as [`Cell::String`].
    fn load_scalar(
        &self,
        sql: &str,
        column_type: ColumnType,
    ) -> Result<Cell, ReExecError<Self::Error>>;
}

impl<P> ScalarSource for P
where
    P: ConnectionProvider,
    for<'q> SqlQuery: LoadQuery<'q, P::Connection, IntRow>
        + LoadQuery<'q, P::Connection, FloatRow>
        + LoadQuery<'q, P::Connection, TextRow>,
{
    fn load_scalar(
        &self,
        sql: &str,
        column_type: ColumnType,
    ) -> Result<Cell, ReExecError<Self::Error>> {
        self.with_connection(|conn| load_cell(conn, sql, column_type))
            .map_err(ReExecError::Connection)?
            .map_err(ReExecError::Query)
    }
}

fn load_cell<C>(conn: &mut C, sql: &str, column_type: ColumnType) -> QueryResult<Cell>
where
    for<'q> SqlQuery:
        LoadQuery<'q, C, IntRow> + LoadQuery<'q, C, FloatRow> + LoadQuery<'q, C, TextRow>,
{
    let cell = match column_type {
        ColumnType::Int => sql_query(sql)
            .get_result::<IntRow>(conn)?
            .v
            .map_or(Cell::Null, Cell::Int),
        ColumnType::Float => sql_query(sql)
            .get_result::<FloatRow>(conn)?
            .v
            .map_or(Cell::Null, Cell::Float),
        ColumnType::Bool | ColumnType::String | ColumnType::Unknown => sql_query(sql)
            .get_result::<TextRow>(conn)?
            .v
            .map_or(Cell::Null, |s| Cell::String(Arc::from(s))),
    };
    Ok(cell)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use core::cell::RefCell;
    use core::convert::Infallible;
    use diesel::{Connection, SqliteConnection};

    /// Minimal single-connection provider for tests (no pooling). `RefCell`
    /// gives the `&self` -> `&mut Conn` interior mutability the trait needs;
    /// fine for single-threaded tests.
    struct TestProvider {
        conn: RefCell<SqliteConnection>,
    }

    impl ConnectionProvider for TestProvider {
        type Connection = SqliteConnection;
        type Error = Infallible;

        fn with_connection<R>(
            &self,
            f: impl FnOnce(&mut SqliteConnection) -> R,
        ) -> Result<R, Self::Error> {
            Ok(f(&mut self.conn.borrow_mut()))
        }
    }

    fn provider_with_orders() -> TestProvider {
        let mut conn = SqliteConnection::establish(":memory:").unwrap();
        sql_query(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, price REAL, \
             quantity INTEGER, status TEXT)",
        )
        .execute(&mut conn)
        .unwrap();
        sql_query(
            "INSERT INTO orders (price, quantity, status) VALUES \
             (10.5, 3, 'paid'), (4.0, 7, 'paid'), (8.0, 1, 'open')",
        )
        .execute(&mut conn)
        .unwrap();
        TestProvider {
            conn: RefCell::new(conn),
        }
    }

    fn run(provider: &TestProvider, sql: &str, ty: ColumnType) -> Cell {
        provider.load_scalar(sql, ty).unwrap()
    }

    #[test]
    fn min_float() {
        let p = provider_with_orders();
        assert_eq!(
            run(&p, "SELECT MIN(price) AS v FROM orders", ColumnType::Float),
            Cell::Float(4.0)
        );
    }

    #[test]
    fn max_int() {
        let p = provider_with_orders();
        assert_eq!(
            run(&p, "SELECT MAX(quantity) AS v FROM orders", ColumnType::Int),
            Cell::Int(7)
        );
    }

    #[test]
    fn min_with_where() {
        let p = provider_with_orders();
        assert_eq!(
            run(
                &p,
                "SELECT MIN(price) AS v FROM orders WHERE status = 'open'",
                ColumnType::Float,
            ),
            Cell::Float(8.0)
        );
    }

    #[test]
    fn min_text() {
        let p = provider_with_orders();
        // Lexicographic MIN over {'paid','paid','open'} is 'open'.
        assert_eq!(
            run(
                &p,
                "SELECT MIN(status) AS v FROM orders",
                ColumnType::String
            ),
            Cell::String(Arc::from("open"))
        );
    }

    #[test]
    fn empty_input_is_null() {
        let p = provider_with_orders();
        // No matching rows: MIN/MAX yields a single NULL row, not zero rows.
        assert_eq!(
            run(
                &p,
                "SELECT MIN(price) AS v FROM orders WHERE status = 'missing'",
                ColumnType::Float,
            ),
            Cell::Null
        );
    }

    #[test]
    fn query_error_surfaces() {
        let p = provider_with_orders();
        // Nonexistent column -> diesel error, mapped to ReExecError::Query.
        let err = p
            .load_scalar("SELECT MIN(nope) AS v FROM orders", ColumnType::Float)
            .unwrap_err();
        assert!(matches!(err, ReExecError::Query(_)));
    }
}
