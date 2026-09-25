//! A projection naming a subset of the table's columns, served in process.
//!
//! Filtering is the same as `SELECT *`. What differs is what a change means
//! to the result: an UPDATE that changes no projected column changes nothing
//! a consumer holds, so it is not reported, where today such a statement is
//! a keyed read per change delivering the unchanged row. The registration
//! names the projected columns so the consumer trims its own events.
//!
//! A subset need not carry the key. A consumer holding a keyless result
//! removes a row by its projected values, so an UPDATE or DELETE whose old
//! image lacks one of them is unanswered rather than reported. A subset
//! carrying the key is applied by key, which every old image carries, so
//! there it is reported, as it must be under PostgreSQL's default replica
//! identity, whose old image is the key alone.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, NotServed, QueryProjection, SubscriptionEngine,
    SubscriptionRequest, Tier,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT, amount INT, note TEXT); \
                   CREATE TABLE other (id INT PRIMARY KEY, status TEXT); \
                   CREATE TABLE bag (a INT, b INT)";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> (Engine, subql::TableId) {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let table = catalog_helpers::table_id::<Postgres, _>(&database, "orders").unwrap();
    (
        SubscriptionEngine::new(database, PostgreSqlDialect {}),
        table,
    )
}

fn projection_of(sql: &str) -> Result<QueryProjection, Option<NotServed<Postgres>>> {
    let (mut engine, _) = engine();
    let registered = engine
        .register(SubscriptionRequest::new(1u64, sql))
        .unwrap_or_else(|error| panic!("{sql} registers on some tier, got {error:?}"));
    match registered.tier {
        Tier::InProcess(served) => Ok(served.projection),
        _ => Err(registered.not_served_because),
    }
}

fn row(id: i64, status: &str, amount: i64, note: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(status.into()),
        Value::Int(amount),
        Value::String(note.into()),
    ]
}

/// The columns are named in the order written, bare, qualified by the table
/// or renamed, and whether they carry the key is resolved with them. A table
/// with no key has none to carry.
#[test]
fn a_column_subset_is_served_naming_its_columns() {
    for (sql, columns, carries_key) in [
        (
            "SELECT id, status FROM orders WHERE amount > 3",
            vec![0, 1],
            true,
        ),
        (
            "SELECT status, id FROM orders WHERE amount > 3",
            vec![1, 0],
            true,
        ),
        (
            "SELECT orders.status AS s FROM orders WHERE amount > 3",
            vec![1],
            false,
        ),
        (
            "SELECT o.note FROM orders o WHERE amount > 3",
            vec![3],
            false,
        ),
        ("SELECT note FROM orders", vec![3], false),
        ("SELECT a FROM bag WHERE b > 1", vec![0], false),
    ] {
        assert_eq!(
            projection_of(sql),
            Ok(QueryProjection::Columns {
                columns,
                carries_key
            }),
            "{sql}"
        );
    }
    assert_eq!(
        projection_of("SELECT id, status, amount, note FROM orders WHERE amount > 3"),
        Ok(QueryProjection::Rows),
        "every column is the row"
    );
}

/// Anything that is not a column of the table read is left to the engine.
#[test]
fn a_projection_of_something_else_is_routed() {
    for sql in [
        "SELECT status, amount + 1 FROM orders WHERE amount > 3",
        "SELECT other.status FROM orders WHERE amount > 3",
        "SELECT orders.status FROM orders o WHERE amount > 3",
        "SELECT status, 1 FROM orders WHERE amount > 3",
    ] {
        let outcome = projection_of(sql);
        assert!(
            matches!(&outcome, Err(Some(NotServed::UnsupportedSql(_)))),
            "{sql}: {outcome:?}"
        );
    }
}

/// What each change reports, beside a `SELECT *` over the same filter.
#[test]
fn an_update_of_unprojected_columns_only_is_not_reported() {
    let (mut engine, table) = engine();
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT id, status FROM orders WHERE amount > 3",
        ))
        .unwrap();
    engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE amount > 3",
        ))
        .unwrap();
    let mut changes = |event: TestEvent<Postgres>| {
        let notifications = engine.consumers(&event).unwrap();
        (
            notifications.inserted().to_vec(),
            notifications.updated().to_vec(),
            notifications.deleted().to_vec(),
        )
    };
    assert_eq!(
        changes(TestEvent::insert(table, row(1, "a", 5, "x"))),
        (vec![1, 2], vec![], vec![])
    );
    assert_eq!(
        changes(TestEvent::update(
            table,
            row(1, "a", 5, "x"),
            row(1, "a", 5, "y")
        )),
        (vec![], vec![2], vec![]),
        "only the unprojected note changed"
    );
    assert_eq!(
        changes(TestEvent::update(
            table,
            row(1, "a", 5, "x"),
            row(1, "a", 6, "x")
        )),
        (vec![], vec![2], vec![]),
        "the filtered but unprojected amount changed, and the row stays"
    );
    assert_eq!(
        changes(TestEvent::update(
            table,
            row(1, "a", 5, "x"),
            row(1, "b", 5, "x")
        )),
        (vec![], vec![1, 2], vec![]),
        "a projected column changed"
    );
    assert_eq!(
        changes(TestEvent::update(
            table,
            row(1, "a", 5, "x"),
            row(1, "a", 1, "x")
        )),
        (vec![], vec![], vec![1, 2]),
        "the row left the result"
    );
    assert_eq!(
        changes(TestEvent::update(
            table,
            row(1, "a", 1, "x"),
            row(1, "a", 5, "x")
        )),
        (vec![1, 2], vec![], vec![]),
        "the row entered the result"
    );
    assert_eq!(
        changes(TestEvent::delete(table, row(1, "a", 5, "x"))),
        (vec![], vec![], vec![1, 2])
    );
}

/// What a subset reports for each image, `(updated, deleted, unanswered)`.
fn image_outcomes(
    sql: &str,
    events: &[TestEvent<Postgres>],
) -> Vec<(Vec<u64>, Vec<u64>, Vec<u16>)> {
    let (mut engine, _) = engine();
    engine
        .register(SubscriptionRequest::new(1u64, sql))
        .unwrap();
    events
        .iter()
        .map(|event| {
            let notifications = engine.consumers(event).unwrap();
            (
                notifications.updated().to_vec(),
                notifications.deleted().to_vec(),
                notifications
                    .unanswered()
                    .iter()
                    .map(|entry| entry.column)
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

fn without(mut cells: Vec<Value<Postgres>>, missing: &[usize]) -> Vec<Value<Postgres>> {
    for &column in missing {
        cells[column] = Value::Missing;
    }
    cells
}

/// A keyless subset is removed from by its old projected values, so an old
/// image lacking one leaves the change unanswered, whether it stays, leaves
/// or is deleted.
#[test]
fn a_keyless_subset_leaves_a_change_it_cannot_remove_by_unanswered() {
    let (_, table) = engine();
    let old = without(row(1, "a", 5, "x"), &[1]);
    assert_eq!(
        image_outcomes(
            "SELECT status FROM orders WHERE amount > 3",
            &[
                TestEvent::update(table, old.clone(), row(1, "b", 5, "x")),
                TestEvent::update(table, old.clone(), row(1, "a", 1, "x")),
                TestEvent::delete(table, old),
            ],
        ),
        vec![(vec![], vec![], vec![1]); 3]
    );
}

/// A keyed subset is applied by key, so an old image that is the key alone,
/// as PostgreSQL's default replica identity sends it, is reported as usual,
/// without the unchanged-row suppression it cannot decide.
/// The filter reads the key, which that image carries, so the subset's rule
/// is the only one deciding. The same keyless subset is unanswered.
#[test]
fn a_keyed_subset_is_reported_from_an_old_image_that_is_the_key_alone() {
    let (_, table) = engine();
    let key_only = without(row(1, "a", 5, "x"), &[1, 2, 3]);
    assert_eq!(
        image_outcomes(
            "SELECT id, status FROM orders WHERE id > 0",
            &[
                TestEvent::update(table, key_only.clone(), row(1, "a", 5, "y")),
                TestEvent::delete(table, key_only),
            ],
        ),
        vec![(vec![1], vec![], vec![]), (vec![], vec![1], vec![])]
    );
    assert_eq!(
        image_outcomes(
            "SELECT status FROM orders WHERE id > 0",
            &[TestEvent::delete(
                table,
                without(row(1, "a", 5, "x"), &[1, 2, 3])
            )],
        ),
        vec![(vec![], vec![], vec![1])]
    );
}

/// An image lacking only an unprojected cell is answered as usual, and a new
/// image lacking a projected cell is reported, as `SELECT *` reports it.
#[test]
fn an_image_lacking_other_cells_is_answered() {
    let (_, table) = engine();
    assert_eq!(
        image_outcomes(
            "SELECT status FROM orders WHERE amount > 3",
            &[
                TestEvent::update(
                    table,
                    without(row(1, "a", 5, "x"), &[3]),
                    row(1, "b", 5, "x")
                ),
                TestEvent::update(
                    table,
                    without(row(1, "a", 5, "x"), &[3]),
                    row(1, "a", 5, "y")
                ),
                TestEvent::update(
                    table,
                    row(1, "a", 5, "x"),
                    without(row(1, "a", 5, "x"), &[1])
                ),
            ],
        ),
        vec![
            (vec![1], vec![], vec![]),
            (vec![], vec![], vec![]),
            (vec![1], vec![], vec![]),
        ]
    );
}

/// A truncate removes every row of every subset.
#[test]
fn a_truncate_empties_a_column_subset() {
    let (mut engine, table) = engine();
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT status FROM orders WHERE amount > 3",
        ))
        .unwrap();
    let notifications = engine.consumers(&TestEvent::truncate(table)).unwrap();
    assert_eq!(notifications.deleted(), [1u64]);
}

/// Two subsets over one filter answer different questions, so they are two
/// predicates, and one subset registered twice shares one.
#[test]
fn a_subset_is_part_of_what_a_subscription_asks() {
    let (mut engine, _) = engine();
    let mut created = Vec::new();
    for (consumer, sql) in [
        (1u64, "SELECT status FROM orders WHERE amount > 3"),
        (2u64, "SELECT note FROM orders WHERE amount > 3"),
        (3u64, "SELECT status FROM orders WHERE amount > 3"),
        (4u64, "SELECT * FROM orders WHERE amount > 3"),
    ] {
        let Tier::InProcess(served) = engine
            .register(SubscriptionRequest::new(consumer, sql))
            .unwrap()
            .tier
        else {
            panic!("{sql} is served in process");
        };
        created.push(served.created_new_predicate);
    }
    assert_eq!(created, vec![true, true, false, true]);
}
