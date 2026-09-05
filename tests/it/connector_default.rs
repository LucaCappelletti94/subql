//! The default `Connector::execute_scalar_row` rejects the multi-column
//! seed with `ScalarRowError::Unsupported`, so an external connector that
//! implements only the required methods keeps compiling and reports cleanly
//! that it cannot seed multi-column aggregates. No shipped connector
//! exercises the default (they all override it), so this pins it directly.

#![allow(clippy::unwrap_used)]

use subql::backend::{Postgres, ScalarFamily, Value};
use subql::reexec::{Connector, ReadQuery, RowPage, ScalarRowError, Snapshot};
use subql::NoCheckpoint;

/// A connector implementing only the required trait methods, leaving
/// `execute_scalar_row` at its trait default.
struct MinimalConnector;

impl Connector for MinimalConnector {
    type AuthContext = ();
    type Error = String;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> Result<(Value<Postgres>, Option<NoCheckpoint>), String> {
        Ok((Value::Int(0), None))
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, String> {
        Err("this connector reads no rows".to_string())
    }
}

#[test]
fn read_query_carries_sql_and_typed_binds() {
    let query = ReadQuery::<Postgres>::owned(
        "SELECT value FROM t WHERE id = $1".to_string(),
        vec![Value::Int(7)],
    );
    assert_eq!(query.sql(), "SELECT value FROM t WHERE id = $1");
    assert_eq!(query.binds(), &[Value::Int(7)]);

    let borrowed = ReadQuery::borrowed(query.sql(), query.binds());
    assert_eq!(borrowed.sql(), query.sql());
    assert_eq!(borrowed.binds(), query.binds());
}

#[test]
fn default_execute_scalar_row_is_unsupported() {
    let connector = MinimalConnector;
    let result = connector.execute_scalar_row(
        &subql::reexec::ReadQuery::without_binds(
            "SELECT SUM(amount) AS c0, COUNT(amount) AS c1 FROM t",
        ),
        &[ScalarFamily::Float, ScalarFamily::Int],
        &(),
    );
    assert!(matches!(result, Err(ScalarRowError::Unsupported)));
}

#[test]
fn execute_scalar_still_works_without_overriding_the_row_method() {
    // The required scalar path is unaffected by the added default method.
    let connector = MinimalConnector;
    let (value, checkpoint) = connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds("SELECT COUNT(*) AS v FROM t"),
            ScalarFamily::Int,
            &(),
        )
        .unwrap();
    assert_eq!(value, Value::Int(0));
    assert!(checkpoint.is_none());
}
