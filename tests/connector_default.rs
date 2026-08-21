//! The default `Connector::execute_scalar_row` rejects the multi-column
//! seed with `ScalarRowError::Unsupported`, so an external connector that
//! implements only the required methods keeps compiling and reports cleanly
//! that it cannot seed multi-column aggregates. No shipped connector
//! exercises the default (they all override it), so this pins it directly.

#![allow(clippy::unwrap_used)]

use subql::backend::{BuiltinKind, Postgres, ScalarKind, Value};
use subql::reexec::{Connector, ScalarRowError, Snapshot};
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
        _sql: &str,
        _kind: BuiltinKind,
        _auth: &(),
    ) -> Result<(Value<Postgres>, Option<NoCheckpoint>), String> {
        Ok((Value::Int(0), None))
    }

    fn execute_rows(
        &self,
        _sql: &str,
        _auth: &(),
    ) -> Result<Snapshot<Vec<Vec<Value<Postgres>>>, NoCheckpoint>, String> {
        Err("rows unsupported".to_string())
    }
}

#[test]
fn default_execute_scalar_row_is_unsupported() {
    let connector = MinimalConnector;
    let result = connector.execute_scalar_row(
        "SELECT SUM(amount) AS c0, COUNT(amount) AS c1 FROM t",
        &[ScalarKind::Float, ScalarKind::Int],
        &(),
    );
    assert!(matches!(result, Err(ScalarRowError::Unsupported)));
}

#[test]
fn execute_scalar_still_works_without_overriding_the_row_method() {
    // The required scalar path is unaffected by the added default method.
    let connector = MinimalConnector;
    let (value, checkpoint) = connector
        .execute_scalar("SELECT COUNT(*) AS v FROM t", ScalarKind::Int, &())
        .unwrap();
    assert_eq!(value, Value::Int(0));
    assert!(checkpoint.is_none());
}
