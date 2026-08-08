//! Docker-backed check that [`subql::REPLICA_IDENTITY_AUDIT_SQL`] names
//! exactly the tables whose change stream omits the previous row.
//!
//! The constant is what a consumer runs once at startup to refuse a
//! misconfigured database, so its value is only as good as what real
//! Postgres answers. Asserting on the string itself would test nothing.
//!
//! Requires Docker. `#[ignore]`d so default `cargo test` starts no
//! containers. Run with:
//!
//! ```sh
//! cargo test --test replica_identity_audit \
//!     --features executor-diesel-postgres -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used)]

mod common;

use diesel::sql_types::Text;
use diesel::{sql_query, PgConnection, QueryableByName, RunQueryDsl};

/// One table the audit is unhappy about.
///
/// The audit reads `pg_class` and `pg_namespace`, which no `table!` of
/// ours describes, so the statement stays raw. The result does not:
/// naming the columns and their SQL types here is what makes a change to
/// the query a compile error rather than a runtime surprise.
#[derive(QueryableByName, Debug, PartialEq, Eq)]
struct Offender {
    #[diesel(sql_type = Text)]
    nspname: String,
    #[diesel(sql_type = Text)]
    relname: String,
}

fn audit(conn: &mut PgConnection) -> Vec<Offender> {
    sql_query(subql::REPLICA_IDENTITY_AUDIT_SQL)
        .load(conn)
        .expect("the audit query runs")
}

fn ddl(conn: &mut PgConnection, statement: &str) {
    sql_query(statement).execute(conn).expect(statement);
}

#[test]
#[ignore = "requires Docker"]
fn the_audit_names_every_table_that_omits_the_previous_row() {
    let container = common::pg_with_wal2json();
    let mut conn = common::pg_connect(common::pg_port(&container));

    // A fresh database has no user tables, so nothing to report. This is
    // the assertion that catches a query matching the system catalogs.
    assert_eq!(audit(&mut conn), vec![], "a clean database is quiet");

    ddl(
        &mut conn,
        "CREATE TABLE lax (id INT PRIMARY KEY, owner TEXT)",
    );
    ddl(
        &mut conn,
        "CREATE TABLE strict_table (id INT PRIMARY KEY, owner TEXT)",
    );
    ddl(&mut conn, "ALTER TABLE strict_table REPLICA IDENTITY FULL");

    // Only the table left on the default identity is reported, and it is
    // reported by name so an operator knows what to fix.
    assert_eq!(
        audit(&mut conn),
        vec![Offender {
            nspname: "public".into(),
            relname: "lax".into(),
        }]
    );

    // Fixing it clears the report, which is what a startup gate reads.
    ddl(&mut conn, "ALTER TABLE lax REPLICA IDENTITY FULL");
    assert_eq!(audit(&mut conn), vec![]);

    // A table in another schema is still found, since replication does
    // not stop at `public`.
    ddl(&mut conn, "CREATE SCHEMA other");
    ddl(
        &mut conn,
        "CREATE TABLE other.also_lax (id INT PRIMARY KEY)",
    );
    assert_eq!(
        audit(&mut conn),
        vec![Offender {
            nspname: "other".into(),
            relname: "also_lax".into(),
        }]
    );

    // `NOTHING` and `USING INDEX` are not `FULL` either, and both leave
    // an update's old image short.
    ddl(
        &mut conn,
        "ALTER TABLE other.also_lax REPLICA IDENTITY NOTHING",
    );
    assert_eq!(audit(&mut conn).len(), 1, "NOTHING is not FULL");

    ddl(
        &mut conn,
        "CREATE UNIQUE INDEX also_lax_pk ON other.also_lax (id)",
    );
    ddl(
        &mut conn,
        "ALTER TABLE other.also_lax REPLICA IDENTITY USING INDEX also_lax_pk",
    );
    assert_eq!(audit(&mut conn).len(), 1, "USING INDEX is not FULL");
}
