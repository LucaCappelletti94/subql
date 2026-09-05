//! Two layers, and why one of them cannot be the only one.
//!
//! Every test in Parts II and III, and both phases of this harness so
//! far, hands subql a [`TestEvent`]: a change event constructed in Rust.
//! That exercises the compiler and the VM, which is most of what those
//! parts corrected, and it bypasses the wire decoders entirely. Two of
//! the worst defects in the series live exactly where a constructed
//! event cannot reach:
//!
//! ```text
//! commit    defect                              why a TestEvent misses it
//! d4e07bc   a cell the event did not carry      a constructed row always
//!           reported a subscription unnotified  carries every cell
//! 9a59536   a float decoded at the wrong width  the decoder is what picks
//!                                               the width
//! ```
//!
//! So there are two layers. The outer one drives real DML against a real
//! server, reads the change event off the replication slot, and feeds
//! *that* event to the engine: it is the only layer that covers wire
//! decoding, unchanged TOAST and the [`CellPresence::Missing`] path. The
//! inner one is the constructed event, fast enough for thousands of
//! generated cases, and never the only layer.
//! [`inner_layer_cannot_stand_alone`](tests::inner_layer_cannot_stand_alone)
//! pins that rule against a real stream rather than leaving it as a
//! comment.
//!
//! # Three assertions, not one
//!
//! An end-to-end comparison that only asks "did subql notify" cannot
//! tell a decode bug from a comparison bug from a fold bug, and Part II
//! and Part III together prove all three families exist. So each case
//! asks three separate questions of the same event:
//!
//! ```text
//! assertion         what it defends            defect it would have caught
//! decoded cell      the wire decoders          9a59536, d4e07bc
//! dispatch verdict  the comparison semantics   537ea04 .. 7e80b5a
//! folded aggregate  the fold arithmetic        c87c343 .. bfbd7dc
//! ```
//!
//! The aggregate is compared against the engine's own `SUM`, which is
//! the same rule Phase D1b had to correct: a sum answers in the type the
//! engine sums into.
#![allow(clippy::unwrap_used)]

use bigdecimal::BigDecimal;
#[cfg(feature = "pg-streaming")]
use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use subql::backend::{Backend, CdcEvent, CellPresence, RowKind, Value};
use subql::compiler::{SqlLiteralParse, Tri};
use subql::{
    catalog_helpers, AggValue, AggregateSeedInstall, DefaultIds, Install, NumericValue,
    SubscriptionEngine, SubscriptionRequest, Tier,
};

use super::oracle::{OracleCase, OracleVerdict};

/// What subql answered for one case, read from one event.
///
/// Every field is an answer to a different question, so a divergence
/// names the layer it came from instead of just failing.
#[derive(Debug)]
pub struct Answers<B: Backend> {
    /// What the event says about each column, by name, in schema order.
    pub cells: Vec<(String, CellPresence)>,
    /// The integer column's decoded value, which is the one carrier that
    /// compares exactly against what the server returns without any
    /// formatting in between.
    pub wide: Option<Value<B>>,
    /// Whether the predicate selected the row.
    pub dispatch: Option<bool>,
    /// What the aggregate folded to, or why it did not.
    pub folded: Folded,
}

/// What came of feeding the event to an aggregate subscription.
///
/// Three outcomes rather than an `Option`, because "not served in
/// process" is a legitimate skip and "served, seeded, and the event
/// moved nothing" is a divergence: the row the event describes is in the
/// table, so the aggregate over it has an answer. Folding those two into
/// one `None` is how the defect `f0148c9` pinned went unnoticed, where a
/// served aggregate reported its seed forever.
#[derive(Debug, PartialEq)]
pub enum Folded {
    /// Routed to a database read, so there is no in-process value.
    NotServed,
    /// Served, and the event folded to this.
    Value(AggValue),
    /// Served and seeded, and the event produced no update at all.
    NoUpdate,
}

/// Ask subql all three questions about one event.
///
/// `catalog_ddl` is the schema as text, `predicate` and `aggregate` are
/// the two subscriptions, and `event` is the event under test, which is
/// the whole point of the parameter: the same function answers for a
/// streamed event and for a constructed one, so the two layers differ in
/// exactly one thing.
pub fn answers_for<E>(
    catalog_ddl: &str,
    dialect: <E::Backend as Backend>::Dialect,
    predicate: &str,
    aggregate: &str,
    event: &E,
) -> Answers<E::Backend>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse + 'static,
    <E::Backend as Backend>::Dialect: sqlparser::dialect::Dialect + Default + Clone,
{
    let database = ParserDB::parse::<<E::Backend as Backend>::Dialect>(catalog_ddl)
        .expect("the catalog DDL parses");
    let table = catalog_helpers::table_id(&database, "t").expect("the table is in the catalog");

    let mut cells = Vec::new();
    let mut wide = None;
    let arity = catalog_helpers::table_arity(&database, table).expect("the table has an arity");
    for ordinal in 0..arity {
        let column = subql::ColumnId::from(u16::try_from(ordinal).expect("arity fits a u16"));
        let name = catalog_helpers::column_name(&database, table, column)
            .expect("a column in range has a name");
        cells.push((
            name.clone(),
            event.presence_at(&database, RowKind::New, column),
        ));
        if name == "wide" {
            wide = event.value_at(&database, RowKind::New, column).ok();
        }
    }

    let dispatch = {
        let mut engine: SubscriptionEngine<E, DefaultIds, ParserDB> = SubscriptionEngine::new(
            ParserDB::parse::<<E::Backend as Backend>::Dialect>(catalog_ddl).expect("parses"),
            dialect.clone(),
        );
        engine
            .register(SubscriptionRequest::new(
                1u64,
                format!("SELECT * FROM t WHERE {predicate}"),
            ))
            .ok()
            .filter(|registered| registered.not_served_because.is_none())
            .and_then(|_| engine.consumers(event).ok())
            .map(|notifications| !notifications.inserted().is_empty())
    };

    let folded = {
        let mut engine: SubscriptionEngine<E, DefaultIds, ParserDB> = SubscriptionEngine::new(
            ParserDB::parse::<<E::Backend as Backend>::Dialect>(catalog_ddl).expect("parses"),
            dialect,
        );
        let registered = engine
            .register(SubscriptionRequest::new(2u64, aggregate))
            .expect("the aggregate registers");
        if registered.not_served_because.is_some() || !matches!(registered.tier, Tier::InProcess(_))
        {
            Folded::NotServed
        } else {
            // Seeded with one empty group, so every number the
            // comparison sees came from folding this event rather than
            // from the seed, and the seed's position is behind the
            // event's so maintenance does not skip it as already
            // counted.
            Install::install(
                &mut engine,
                registered.subscription_id,
                AggregateSeedInstall::<E::Backend, E::Checkpoint> {
                    rows: vec![Vec::new()],
                    read_at: None,
                },
            )
            .expect("the empty seed lands");
            engine
                .aggregate_updates(event)
                .expect("the event folds")
                .updates
                .first()
                .and_then(subql::AggregateValueUpdate::folded_value)
                .map_or(Folded::NoUpdate, Folded::Value)
        }
    };

    Answers {
        cells,
        wide,
        dispatch,
        folded,
    }
}

#[cfg(feature = "pg-streaming")]
/// A nullable `bigint` read back from the server.
#[derive(diesel::QueryableByName)]
struct NullableInt {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    value: Option<i64>,
}

#[cfg(feature = "pg-streaming")]
/// A nullable boolean read back from the server.
#[derive(diesel::QueryableByName)]
struct NullableFlag {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Bool>)]
    value: Option<bool>,
}

#[cfg(feature = "pg-streaming")]
/// A nullable `numeric`, which is what PostgreSQL answers for a sum over
/// a `bigint` column: measured, `pg_typeof(SUM(wide))` is `numeric`, and
/// that is the type Phase D1b had to make subql sum into.
#[derive(diesel::QueryableByName)]
struct NullableNumeric {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Numeric>)]
    value: Option<BigDecimal>,
}

/// What the server itself says about the row the event described.
///
/// Read back from the table rather than assumed from the statement that
/// wrote it, because the question is what the engine holds and not what
/// the test meant to write.
pub struct ServerRow {
    /// Which columns hold SQL `NULL`, by name.
    pub nulls: Vec<(String, bool)>,
    pub wide: Option<i64>,
    /// The engine's own sum, in the type the engine sums into. Read as
    /// `numeric` rather than cast to an integer, because the cast would
    /// hide precisely the type question Phase D1b was about.
    pub sum_wide: Option<BigDecimal>,
}

// Reading a row back needs a live connection, which only the streaming
// feature brings in. The struct itself stays: `assert_agrees` compares
// against one however it was built.
#[cfg(feature = "pg-streaming")]
impl ServerRow {
    /// Read it off a live PostgreSQL connection.
    ///
    /// Every column comes back in its own declared type, with no cast in
    /// the SQL: a cast is the server answering about a converted value,
    /// and the whole point is what it answers about the real one.
    pub fn read_pg(connection: &mut diesel::PgConnection, columns: &[&str]) -> Self {
        let mut nulls = Vec::new();
        for column in columns {
            let row = sql_query(format!("SELECT {column} IS NULL AS value FROM t"))
                .get_result::<NullableFlag>(connection)
                .expect("the server reads the column back");
            nulls.push(((*column).to_string(), row.value == Some(true)));
        }
        let wide = sql_query("SELECT wide AS value FROM t")
            .get_result::<NullableInt>(connection)
            .expect("the server reads `wide` back")
            .value;
        let sum_wide = sql_query("SELECT SUM(wide) AS value FROM t")
            .get_result::<NullableNumeric>(connection)
            .expect("the server sums `wide`")
            .value;
        Self {
            nulls,
            wide,
            sum_wide,
        }
    }
}

/// Compare one layer's answers against the server's, and say which
/// question diverged.
///
/// A refusal is not compared: when the engine raised there is no answer
/// to hold subql to, which is the distinction Phase E1 built
/// [`OracleVerdict`] to keep.
pub fn assert_agrees<B: Backend>(
    answers: &Answers<B>,
    server: &ServerRow,
    verdict: &OracleVerdict,
    case: &OracleCase<'_>,
) where
    B::Int: Copy,
    i64: From<B::Int>,
{
    for (name, null) in &server.nulls {
        let presence = answers
            .cells
            .iter()
            .find(|(column, _)| column == name)
            .map_or_else(
                || panic!("the event has no cell for `{name}`"),
                |(_, presence)| *presence,
            );
        let expected = if *null {
            CellPresence::Null
        } else {
            CellPresence::Present
        };
        assert_eq!(
            presence, expected,
            "the decoded cell for `{name}` disagrees with the server: an INSERT under \
             REPLICA IDENTITY FULL carries every column, so `Missing` here is the defect \
             d4e07bc corrected and `Null` for a value is a decode bug"
        );
    }

    match (&answers.wide, server.wide) {
        (Some(Value::Int(decoded)), Some(read)) => assert_eq!(
            i64::from(*decoded),
            read,
            "`wide` decoded off the wire is not the integer the server holds"
        ),
        (Some(Value::Null), None) => {}
        (decoded, read) => panic!("`wide` decoded as {decoded:?} where the server holds {read:?}"),
    }

    if let (Some(dispatch), Some(answered)) = (answers.dispatch, verdict.answered()) {
        assert_eq!(
            dispatch,
            answered == Tri::True,
            "subql {} the row for `{}` where the engine answered {answered:?}: only TRUE \
             selects a row, and NULL is not TRUE",
            if dispatch {
                "selected"
            } else {
                "did not select"
            },
            case.predicate
        );
    }

    match &answers.folded {
        Folded::NotServed => {}
        Folded::NoUpdate => panic!(
            "`SUM(wide)` is served and seeded empty, and the event moved it nowhere, while \
             the engine answers {:?} over the same row",
            server.sum_wide
        ),
        Folded::Value(AggValue::Sum(total)) => {
            // The variant is part of the answer, not packaging. PostgreSQL
            // sums a `bigint` into `numeric`, measured, so a sum that came
            // back as a machine integer would be the exact type
            // divergence Phase D1b corrected even when the digits agree.
            let actual = match total {
                Some(NumericValue::Decimal(value)) => Some(value.clone()),
                None => None,
                other => panic!(
                    "`SUM(wide)` over a BIGINT column folded to {other:?}, where the engine \
                     answers in `numeric`"
                ),
            };
            match (&actual, &server.sum_wide) {
                (Some(folded), Some(engine)) => assert_eq!(
                    folded.normalized(),
                    engine.normalized(),
                    "the folded sum disagrees with the engine's own SUM(wide)"
                ),
                (folded, engine) => assert_eq!(
                    folded.is_some(),
                    engine.is_some(),
                    "the folded sum is {folded:?} where the engine answers {engine:?}"
                ),
            }
        }
        Folded::Value(other) => panic!("`SUM(wide)` folded to {other:?}, which is not a sum"),
    }
}

#[cfg(test)]
mod tests {
    use super::super::oracle::OracleCase;
    use super::{answers_for, assert_agrees, Answers, Folded, ServerRow};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::backend::{CellPresence, Postgres, Value};
    use subql::testing::TestEvent;
    use subql::{catalog_helpers, PgLsn};

    /// The columns this phase's cases read. `wide` is the integer the
    /// comparison holds exactly, `padded` is a `char(n)`, and `bulky` is
    /// large enough to be TOASTed, which is what makes the `Missing`
    /// path reachable at all.
    pub(super) const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, wide BIGINT, \
                       padded CHAR(5), bulky TEXT)";
    pub(super) const COLUMNS: &[&str] = &["id", "wide", "padded", "bulky"];

    /// The three answers are read from the same event, so a divergence
    /// names one layer rather than the pair.
    #[test]
    fn the_three_answers_come_from_one_event() {
        let database = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("the DDL parses");
        let table = catalog_helpers::table_id(&database, "t").expect("the table is in the catalog");
        let event: TestEvent<Postgres, PgLsn> = TestEvent::insert(
            table,
            vec![
                Value::Int(1),
                Value::Int(9_007_199_254_740_993),
                Value::String("ab   ".to_string()),
                Value::String("x".to_string()),
            ],
        )
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(9));
        let answers: Answers<Postgres> = answers_for(
            DDL,
            PostgreSqlDialect {},
            "wide > 0",
            "SELECT SUM(wide) FROM t",
            &event,
        );
        assert_eq!(
            answers.wide,
            Some(Value::Int(9_007_199_254_740_993)),
            "the decoded cell is read from the event under test"
        );
        assert_eq!(
            answers.dispatch,
            Some(true),
            "the dispatch verdict is read from the same event"
        );
        assert!(
            matches!(&answers.folded, Folded::Value(_)),
            "the folded aggregate is read from the same event, so all three describe one \
             row, got {:?}",
            answers.folded
        );
        assert_eq!(
            answers.cells.len(),
            COLUMNS.len(),
            "every column of the schema is reported, by name"
        );
    }

    /// One row, agreeing with itself, as the baseline the three
    /// divergence tests below perturb.
    ///
    /// Built by hand rather than streamed, because what is under test
    /// here is the comparison and not the layer: a divergence has to be
    /// caught whichever layer produced it.
    fn agreeing_pair() -> (Answers<Postgres>, ServerRow) {
        let answers = Answers {
            cells: COLUMNS
                .iter()
                .map(|name| ((*name).to_string(), CellPresence::Present))
                .collect(),
            wide: Some(Value::Int(7)),
            dispatch: Some(true),
            folded: Folded::Value(subql::AggValue::Sum(Some(subql::NumericValue::Decimal(
                bigdecimal::BigDecimal::from(7),
            )))),
        };
        let server = ServerRow {
            nulls: COLUMNS
                .iter()
                .map(|name| ((*name).to_string(), false))
                .collect(),
            wide: Some(7),
            sum_wide: Some(bigdecimal::BigDecimal::from(7)),
        };
        (answers, server)
    }

    /// The agreeing pair agrees, so the three tests below fail for the
    /// perturbation and not for the fixture.
    #[test]
    fn an_agreeing_pair_agrees() {
        let (answers, server) = agreeing_pair();
        assert_agrees(&answers, &server, &verdict(true), &case());
    }

    fn verdict(answer: bool) -> crate::differential::oracle::OracleVerdict {
        crate::differential::oracle::OracleVerdict::Answered(if answer {
            subql::compiler::Tri::True
        } else {
            subql::compiler::Tri::False
        })
    }

    fn case() -> OracleCase<'static> {
        OracleCase {
            ddl: &[DDL],
            insert: "",
            predicate: "wide > 0",
        }
    }

    /// A cell the event did not carry, where the server holds a value,
    /// is a divergence.
    ///
    /// The mutation battery is why this test exists: deleting the
    /// cell-decode comparison entirely changed no test, because every
    /// case above happens to agree about presence. Nothing was driving
    /// the comparison, so it was defending nothing. This drives it, and
    /// it is the exact shape of `d4e07bc`.
    #[test]
    #[should_panic(expected = "the decoded cell for `bulky` disagrees with the server")]
    fn a_missing_cell_is_a_divergence() {
        let (mut answers, server) = agreeing_pair();
        for (name, presence) in &mut answers.cells {
            if name == "bulky" {
                *presence = CellPresence::Missing;
            }
        }
        assert_agrees(&answers, &server, &verdict(true), &case());
    }

    /// A decoded integer that is not the one the server holds is a
    /// divergence, which is the shape of `9a59536`.
    #[test]
    #[should_panic(expected = "is not the integer the server holds")]
    fn a_misdecoded_integer_is_a_divergence() {
        let (mut answers, server) = agreeing_pair();
        answers.wide = Some(Value::Int(8));
        assert_agrees(&answers, &server, &verdict(true), &case());
    }

    /// Selecting a row the engine answered `FALSE` for is a divergence,
    /// which is the whole of Part II.
    #[test]
    #[should_panic(expected = "subql selected the row")]
    fn a_dispatch_disagreement_is_a_divergence() {
        let (answers, server) = agreeing_pair();
        assert_agrees(&answers, &server, &verdict(false), &case());
    }

    /// A fold that answers a different number than the engine's own
    /// `SUM` is a divergence, which is the whole of Part III.
    #[test]
    #[should_panic(expected = "the folded sum disagrees with the engine's own SUM(wide)")]
    fn a_folded_disagreement_is_a_divergence() {
        let (mut answers, server) = agreeing_pair();
        answers.folded = Folded::Value(subql::AggValue::Sum(Some(subql::NumericValue::Decimal(
            bigdecimal::BigDecimal::from(8),
        ))));
        assert_agrees(&answers, &server, &verdict(true), &case());
    }

    /// A served aggregate that the event moved nowhere is a divergence
    /// too, not a skip: the row is in the table, so the sum over it has
    /// an answer. This is the shape `f0148c9` pinned, where a served
    /// aggregate reported its seed forever.
    #[test]
    #[should_panic(expected = "the event moved it nowhere")]
    fn a_served_aggregate_that_never_moved_is_a_divergence() {
        let (mut answers, server) = agreeing_pair();
        answers.folded = Folded::NoUpdate;
        assert_agrees(&answers, &server, &verdict(true), &case());
    }
}

/// The outer layer, which only exists when the streaming source is
/// compiled in.
///
/// `pg-streaming` is what brings [`PgStreamingCdcSource`] into the build,
/// and without it there is no replication slot to read: the inner-layer
/// tests above still run, which is exactly the asymmetry
/// [`inner_layer_cannot_stand_alone`] is about.
#[cfg(all(test, feature = "pg-streaming"))]
mod streamed_tests {
    use super::tests::{COLUMNS, DDL};
    use super::{answers_for, assert_agrees, Folded, ServerRow};
    use crate::common;
    use crate::differential::oracle::{Oracle as _, OracleCase, PgOracle};
    use diesel::{sql_query, RunQueryDsl};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::backend::{CdcEvent as _, CellPresence, Postgres, RowKind, Value};
    use subql::testing::TestEvent;
    use subql::{
        catalog_helpers, CdcSource as _, EventKind, PgLsn, PgStreamingCdcSource, PgStreamingConfig,
    };

    type Container = testcontainers::Container<testcontainers::GenericImage>;
    fn current_thread_rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds")
    }
    /// Stand up PostgreSQL, create the table with a full replica
    /// identity and a slot, run `dml`, and return the change event the
    /// server streamed for it.
    ///
    /// Everything about this is the outer layer: the event is decoded
    /// from the wire by the same code a deployment runs, not built in
    /// the test.
    fn streamed(slot: &str, dml: &str) -> (Container, diesel::PgConnection, subql::ChangeEvent) {
        common::assert_docker_available();
        let container = common::pg_with_wal2json();
        let port = common::pg_port(&container);
        let mut setup = common::pg_connect(port);
        let mut writer = common::pg_connect(port);

        sql_query(DDL)
            .execute(&mut setup)
            .expect("create the table");
        sql_query("ALTER TABLE t REPLICA IDENTITY FULL")
            .execute(&mut setup)
            .expect("a full replica identity");
        let publication = format!("{slot}_pub");
        common::create_publication(&mut setup, &publication, "t");
        common::create_pgoutput_slot(&mut setup, slot);

        let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("the DDL parses");
        let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, &publication);

        let event = current_thread_rt().block_on(async move {
            let mut source = PgStreamingCdcSource::connect(config, catalog)
                .await
                .expect("the replication connection opens");
            sql_query(dml).execute(&mut writer).expect("the DML lands");
            tokio::time::timeout(core::time::Duration::from_secs(10), source.next_event())
                .await
                .expect("the server streams the event")
                .expect("the stream does not error")
                .expect("the source is still open")
        });
        (container, setup, event)
    }
    /// The outer layer reads a real event off the replication slot, and
    /// what arrives is the INSERT that was issued.
    ///
    /// The weakest of the four and still worth its place: if this fails,
    /// every assertion below is comparing against nothing.
    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn outer_layer_reads_the_event_off_the_stream() {
        let (_container, mut setup, event) = streamed(
            "sdr_outer_reads",
            "INSERT INTO t (id, wide, padded, bulky) VALUES (1, 42, 'ab', 'x')",
        );
        assert_eq!(
            event.kind(),
            EventKind::Insert,
            "the event off the slot is the INSERT that was issued"
        );
        assert!(
            event.checkpoint().is_some(),
            "a streamed event carries the LSN it was read at, which is what an ack advances"
        );
        common::drop_slot(&mut setup, "sdr_outer_reads");
    }
    /// Every cell the event carries says about the row what the server
    /// says about it.
    ///
    /// This is the assertion no constructed event can make, because a
    /// constructed event is built from the values the test already knows.
    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn decoded_cell_matches_the_engine_value() {
        let (_container, mut setup, event) = streamed(
            "sdr_decoded_cell",
            "INSERT INTO t (id, wide, padded, bulky) VALUES (1, 9007199254740993, 'ab', NULL)",
        );
        let answers = answers_for(
            DDL,
            PostgreSqlDialect {},
            "wide = 9007199254740993",
            "SELECT SUM(wide) FROM t",
            &event,
        );
        let server = ServerRow::read_pg(&mut setup, COLUMNS);
        assert_agrees(
            &answers,
            &server,
            &crate::differential::oracle::OracleVerdict::Answered(subql::compiler::Tri::True),
            &OracleCase {
                ddl: &[DDL],
                insert: "",
                predicate: "wide = 9007199254740993",
            },
        );
        assert_eq!(
            answers.wide,
            Some(Value::Int(9_007_199_254_740_993)),
            "a value one past 2^53 survives the wire, which is the boundary D1b had to correct"
        );
        common::drop_slot(&mut setup, "sdr_decoded_cell");
    }
    /// Whether subql selects the row is what the engine answers for the
    /// same predicate over the same row.
    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn dispatch_verdict_matches_the_oracle() {
        let (container, mut setup, event) = streamed(
            "sdr_dispatch",
            "INSERT INTO t (id, wide, padded, bulky) VALUES (1, 7, 'ab   ', 'x')",
        );
        // A `char(5)` holding 'ab' padded to width: measured in Phase
        // C5, PostgreSQL compares it equal to 'ab' because the
        // comparison ignores the padding, and comparing the decoded
        // text byte for byte does not.
        let predicate = "padded = 'ab'";
        let port = common::pg_port(&container);
        let mut oracle = PgOracle {
            connection: common::pg_connect(port),
        };
        let verdict = oracle.answer(&OracleCase {
            ddl: &[DDL],
            insert: "INSERT INTO t (id, wide, padded, bulky) VALUES (1, 7, 'ab   ', 'x')",
            predicate,
        });
        let answers = answers_for(
            DDL,
            PostgreSqlDialect {},
            predicate,
            "SELECT SUM(wide) FROM t",
            &event,
        );
        assert!(
            answers.dispatch.is_some(),
            "a `char(n)` comparison is served in process, so there is a verdict to compare"
        );
        let server = ServerRow::read_pg(&mut setup, COLUMNS);
        assert_agrees(
            &answers,
            &server,
            &verdict,
            &OracleCase {
                ddl: &[DDL],
                insert: "",
                predicate,
            },
        );
        common::drop_slot(&mut setup, "sdr_dispatch");
    }
    /// The folded aggregate is what the engine's own `SUM` answers over
    /// the same row.
    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn folded_aggregate_matches_the_engine() {
        let (_container, mut setup, event) = streamed(
            "sdr_folded",
            "INSERT INTO t (id, wide, padded, bulky) VALUES (1, 9007199254740993, 'ab', 'x')",
        );
        let answers = answers_for(
            DDL,
            PostgreSqlDialect {},
            "wide > 0",
            "SELECT SUM(wide) FROM t",
            &event,
        );
        assert!(
            matches!(&answers.folded, Folded::Value(_)),
            "`SUM(wide)` over a BIGINT column is served and folded in process, got {:?}",
            answers.folded
        );
        let server = ServerRow::read_pg(&mut setup, COLUMNS);
        assert_agrees(
            &answers,
            &server,
            &crate::differential::oracle::OracleVerdict::Answered(subql::compiler::Tri::True),
            &OracleCase {
                ddl: &[DDL],
                insert: "",
                predicate: "wide > 0",
            },
        );
        common::drop_slot(&mut setup, "sdr_folded");
    }
    /// The inner layer cannot answer for a cell the stream did not
    /// carry, so it cannot be the only layer.
    ///
    /// An `UPDATE` that leaves a TOASTed column alone is streamed without
    /// that column's value: the server sends what changed, and a
    /// megabyte of unchanged text is not resent. The outer layer sees
    /// [`CellPresence::Missing`] and subql re-executes. A constructed
    /// event built from the same table sees a value, because a
    /// constructed row is written from what the test already read. That
    /// gap is the defect `d4e07bc` corrected, and this test is the
    /// standing proof that deleting the outer layer would hide it again.
    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn inner_layer_cannot_stand_alone() {
        let bulky = "z".repeat(64 * 1024);
        let slot = "sdr_toast";
        common::assert_docker_available();
        let container = common::pg_with_wal2json();
        let port = common::pg_port(&container);
        let mut setup = common::pg_connect(port);
        let mut writer = common::pg_connect(port);

        sql_query(DDL)
            .execute(&mut setup)
            .expect("create the table");
        // Size alone does not make a column TOASTed, which is what the
        // first attempt at this test got wrong. Measured on PostgreSQL
        // 16: 64 KiB of one repeated character compresses to 762 bytes
        // and stays inline, so the server resends it on every UPDATE and
        // the Missing path is never reached. `SET STORAGE EXTERNAL` turns
        // compression off, the same 64 KiB then occupies 65536 bytes out
        // of line, and an UPDATE that leaves it alone omits it.
        sql_query("ALTER TABLE t ALTER COLUMN bulky SET STORAGE EXTERNAL")
            .execute(&mut setup)
            .expect("the column stores out of line, uncompressed");
        // DEFAULT, not FULL: a full identity resends the old row, and the
        // point here is the column the server declines to resend.
        sql_query("ALTER TABLE t REPLICA IDENTITY DEFAULT")
            .execute(&mut setup)
            .expect("the default replica identity");
        sql_query(format!(
            "INSERT INTO t (id, wide, padded, bulky) VALUES (1, 1, 'ab', '{bulky}')"
        ))
        .execute(&mut setup)
        .expect("the wide row lands");
        let publication = format!("{slot}_pub");
        common::create_publication(&mut setup, &publication, "t");
        common::create_pgoutput_slot(&mut setup, slot);

        let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("the DDL parses");
        let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, &publication);
        let event = current_thread_rt().block_on(async move {
            let mut source = PgStreamingCdcSource::connect(config, catalog)
                .await
                .expect("the replication connection opens");
            sql_query("UPDATE t SET wide = 2 WHERE id = 1")
                .execute(&mut writer)
                .expect("the update lands");
            tokio::time::timeout(core::time::Duration::from_secs(10), source.next_event())
                .await
                .expect("the server streams the event")
                .expect("the stream does not error")
                .expect("the source is still open")
        });

        let database = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("the DDL parses");
        let table = catalog_helpers::table_id(&database, "t").expect("the table is in the catalog");
        let bulky_column =
            catalog_helpers::column_id(&database, table, "bulky").expect("`bulky` is a column");

        assert_eq!(
            event.presence_at(&database, RowKind::New, bulky_column),
            CellPresence::Missing,
            "an UPDATE that leaves a TOASTed column alone streams without it, which is the \
             only reason the Missing path exists"
        );

        // The same row, as the inner layer would build it: every cell
        // written from what the test knows, so nothing is missing and the
        // Missing path is unreachable from here.
        let constructed: TestEvent<Postgres, PgLsn> = TestEvent::update(
            table,
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::String("ab   ".to_string()),
                Value::String(bulky.clone()),
            ],
            vec![
                Value::Int(1),
                Value::Int(2),
                Value::String("ab   ".to_string()),
                Value::String(bulky),
            ],
        )
        .with_pk_columns([0u16]);
        assert_eq!(
            constructed.presence_at(&database, RowKind::New, bulky_column),
            CellPresence::Present,
            "a constructed event carries every cell, so the inner layer can never produce \
             the case the outer layer just produced: it cannot stand alone"
        );

        common::drop_slot(&mut setup, slot);
    }
}
