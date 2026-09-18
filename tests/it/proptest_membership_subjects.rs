#![allow(clippy::unwrap_used)]
//! The admission index against its own definition.
//!
//! `TermMembers` keeps `by_value` as the set dispatch reads and the granting
//! subjects beside it, and four paths maintain the pair: seeding, a membership
//! row appearing, one disappearing, and a truncate. A miss in any of them is
//! silent, so the whole index is driven here against the naive reading of what
//! it means, that a caller is admitted to a value exactly while one of the
//! subjects it holds grants that value.

use proptest::prelude::*;
use rls2fga::translator::{Translator, TranslatorBuilder};
use rls2fga::types::ConfidenceLevel;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use std::collections::BTreeSet;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

const DDL: &str = "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
     CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY(project_id, user_id));
     CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER, title TEXT, score DOUBLE PRECISION);";

const TERM: &str = "SELECT * FROM docs WHERE project_id IN \
     (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";

const SUBJECTS: usize = 3;
const PROJECTS: i64 = 3;

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

/// One caller: the subjects it holds, and the grants it states at registration.
#[derive(Debug, Clone)]
struct Caller {
    consumer: u64,
    subjects: Vec<usize>,
    stated: Vec<(usize, i64)>,
}

/// One membership change arriving after everybody is registered.
#[derive(Debug, Clone)]
enum Op {
    Insert(usize, i64),
    Delete(usize, i64),
    Truncate,
}

fn translator() -> Translator {
    TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .build()
}

fn engine() -> (Engine, TableId, TableId) {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let docs = catalog_helpers::table_id::<Postgres, _>(&database, "docs").expect("docs");
    let members =
        catalog_helpers::table_id::<Postgres, _>(&database, "project_members").expect("members");
    let engine =
        SubscriptionEngine::new(database, PostgreSqlDialect {}).with_translator(translator());
    (engine, docs, members)
}

fn subject(index: usize) -> Value<Postgres> {
    Value::String(format!("key:{index}"))
}

fn caller_strategy(consumer: u64) -> impl Strategy<Value = Caller> {
    proptest::collection::vec(0..SUBJECTS, 1..=SUBJECTS).prop_flat_map(move |drawn| {
        let mut subjects = Vec::new();
        for held in drawn {
            if !subjects.contains(&held) {
                subjects.push(held);
            }
        }
        let held = subjects.clone();
        proptest::collection::vec((0..held.len(), 1..=PROJECTS), 0..4).prop_map(move |rows| {
            Caller {
                consumer,
                subjects: held.clone(),
                stated: rows
                    .iter()
                    .map(|&(position, project)| (held[position], project))
                    .collect(),
            }
        })
    })
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (0..SUBJECTS, 1..=PROJECTS).prop_map(|(s, p)| Op::Insert(s, p)),
        8 => (0..SUBJECTS, 1..=PROJECTS).prop_map(|(s, p)| Op::Delete(s, p)),
        2 => Just(Op::Truncate),
    ]
}

/// Which consumers the engine delivers a document in `project` to.
fn delivered(engine: &mut Engine, docs: TableId, project: i64) -> BTreeSet<u64> {
    let row = vec![
        Value::Int(1000 + project),
        Value::Int(project),
        Value::String("spec".into()),
        Value::Float(1.0),
    ];
    engine
        .consumers(&TestEvent::insert(docs, row))
        .unwrap()
        .inserted()
        .iter()
        .copied()
        .collect()
}

/// The naive reading: a caller is admitted to a project exactly while one of
/// the subjects it holds grants it.
fn admits(grants: &BTreeSet<(usize, i64)>, caller: &Caller, project: i64) -> bool {
    caller
        .subjects
        .iter()
        .any(|&held| grants.contains(&(held, project)))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// Seeding, a membership row appearing, one disappearing and a truncate,
    /// in any order, leave the engine delivering exactly what the definition
    /// says.
    #[test]
    fn admissions_follow_the_subjects_that_grant_them(
        callers in (1u64..=3).prop_flat_map(|count| {
            (1..=count).map(caller_strategy).collect::<Vec<_>>()
        }),
        ops in proptest::collection::vec(op_strategy(), 0..14),
    ) {
        let (mut engine, docs, members) = engine();
        let mut model: Vec<(Caller, BTreeSet<(usize, i64)>)> = Vec::new();
        for caller in callers {
            let request = SubscriptionRequest::new(caller.consumer, TERM)
                .subjects(caller.subjects.iter().map(|&held| subject(held)))
                .term_values(
                    vec!["project_id"],
                    caller
                        .stated
                        .iter()
                        .map(|&(held, project)| (subject(held), vec![Value::Int(project)]))
                        .collect(),
                );
            engine.register(request).unwrap();
            let grants = caller.stated.iter().copied().collect();
            model.push((caller, grants));
        }

        for op in ops {
            match op {
                Op::Insert(held, project) => {
                    engine
                        .consumers(&TestEvent::insert(
                            members,
                            vec![Value::Int(project), subject(held)],
                        ))
                        .unwrap();
                    for (caller, grants) in &mut model {
                        if caller.subjects.contains(&held) {
                            grants.insert((held, project));
                        }
                    }
                }
                Op::Delete(held, project) => {
                    engine
                        .consumers(&TestEvent::delete(
                            members,
                            vec![Value::Int(project), subject(held)],
                        ))
                        .unwrap();
                    for (_, grants) in &mut model {
                        grants.remove(&(held, project));
                    }
                }
                Op::Truncate => {
                    engine.consumers(&TestEvent::truncate(members)).unwrap();
                    for (_, grants) in &mut model {
                        grants.clear();
                    }
                }
            }

            for project in 1..=PROJECTS {
                let expected: BTreeSet<u64> = model
                    .iter()
                    .filter(|(caller, grants)| admits(grants, caller, project))
                    .map(|(caller, _)| caller.consumer)
                    .collect();
                prop_assert_eq!(
                    delivered(&mut engine, docs, project),
                    expected,
                    "project {} after {:?}",
                    project,
                    op
                );
            }
        }
    }
}
