//! Grouped extreme updates match a reference multiset across random row changes.
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::reexec::ReExecutionRead;
use subql::testing::TestEvent;
use subql::{
    AggregateResultValue, AggregateValueChange, DefaultIds, GroupedScalarInstall,
    GroupedScalarSeedInstall, Install, NoCheckpoint, SubscriptionEngine, SubscriptionRequest,
};

fn row(id: i64, group: &str, amount: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(group.into()),
        Value::Int(amount),
    ]
}

fn apply_updates(
    observed: &mut BTreeMap<Vec<u8>, i64>,
    updates: &[subql::AggregateValueUpdate<DefaultIds, Postgres>],
) {
    for update in updates {
        let key = update.group.clone().expect("group key");
        match &update.change {
            AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(value))) => {
                observed.insert(key, *value);
            }
            AggregateValueChange::Remove => {
                observed.remove(&key);
            }
            other @ AggregateValueChange::Set(_) => {
                panic!("unexpected grouped extreme update {other:?}")
            }
        }
    }
}

proptest! {
    #[test]
    fn grouped_min_matches_a_reference_map(
        operations in prop::collection::vec((0u8..3, 0u8..12, any::<bool>(), -50i16..50), 1..100)
    ) {
        let database = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT);",
        )
        .expect("parse DDL");
        let table = subql::catalog_helpers::table_id(&database, "orders").expect("orders");
        let mut engine = SubscriptionEngine::<
            TestEvent<Postgres>,
            DefaultIds,
            ParserDB,
        >::new(database, PostgreSqlDialect {});
        let subscription = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT region, MIN(amount) FROM orders GROUP BY region",
            ))
            .expect("grouped minimum registers")
            .subscription_id;
        Install::install(
            &mut engine,
            subscription,
            GroupedScalarSeedInstall {
                rows: Vec::new(),
                read_at: None::<NoCheckpoint>,
            },
        )
        .expect("empty seed installs");

        let mut rows = HashMap::<i64, (String, i64)>::new();
        let mut observed = BTreeMap::<Vec<u8>, i64>::new();
        let mut group_keys = HashMap::<String, Vec<u8>>::new();
        for (kind, raw_id, second_group, raw_amount) in operations {
            let id = i64::from(raw_id);
            let group = if second_group { "south" } else { "north" };
            let amount = i64::from(raw_amount);
            let event = match kind {
                0 if !rows.contains_key(&id) => {
                    rows.insert(id, (group.to_owned(), amount));
                    Some((
                        TestEvent::insert(table, row(id, group, amount)).with_pk_columns([0u16]),
                        Some(group.to_owned()),
                    ))
                }
                1 => rows.remove(&id).map(|(old_group, old_amount)| {
                    (
                        TestEvent::delete(table, row(id, &old_group, old_amount))
                            .with_pk_columns([0u16]),
                        None,
                    )
                }),
                2 => rows.get_mut(&id).map(|held| {
                    let old = row(id, &held.0, held.1);
                    *held = (group.to_owned(), amount);
                    (
                        TestEvent::update(table, old, row(id, group, amount))
                            .with_pk_columns([0u16])
                            .with_changed_columns([1u16, 2u16]),
                        Some(group.to_owned()),
                    )
                }),
                _ => None,
            };
            let Some((event, new_group)) = event else {
                continue;
            };
            let output = engine.dispatch(&event).expect("event dispatches");
            prop_assert!(output.transitions().is_empty());
            if let Some(group) = new_group.filter(|group| !group_keys.contains_key(group)) {
                let key = output
                    .aggregate_updates()
                    .iter()
                    .find_map(|update| {
                        matches!(update.change, AggregateValueChange::Set(_))
                            .then(|| update.group.clone())
                            .flatten()
                    })
                    .expect("a new group emits its key");
                group_keys.insert(group, key);
            }
            apply_updates(&mut observed, output.aggregate_updates());

            let reference = rows.values().fold(
                BTreeMap::<String, Vec<i64>>::new(),
                |mut grouped, (group, value)| {
                    grouped.entry(group.clone()).or_default().push(*value);
                    grouped
                },
            );
            for trigger in output.triggers() {
                let ReExecutionRead::GroupedScalar { group, .. } = &trigger.read else {
                    prop_assert!(false, "unexpected trigger scope");
                    continue;
                };
                let name = group_keys
                    .iter()
                    .find_map(|(name, known)| (known == group).then_some(name))
                    .expect("every live or removed group has a known key");
                let (minimum, count) = reference
                    .get(name)
                    .map(|values| {
                        (*values.iter().min().expect("non-empty group"), values.len())
                    })
                    .map_or((0, 0), |(minimum, count)| (minimum, count));
                let installed = Install::install(
                    &mut engine,
                    subscription,
                    GroupedScalarInstall {
                        group: group.clone(),
                        row: vec![
                            if count == 0 { Value::Null } else { Value::Int(minimum) },
                            Value::Int(i64::try_from(count).expect("test group count fits i64")),
                        ],
                        checkpoint: None::<NoCheckpoint>,
                    },
                )
                .expect("group read installs");
                apply_updates(&mut observed, &installed.updates);
            }

            let expected: BTreeMap<Vec<u8>, i64> = reference
                .into_iter()
                .map(|(group, values)| {
                    (
                        group_keys.get(&group).expect("group key was emitted").clone(),
                        *values.iter().min().expect("non-empty group"),
                    )
                })
                .collect();
            prop_assert_eq!(&observed, &expected);
        }
    }
}

proptest! {
    #[test]
    fn grouped_min_with_a_threshold_matches_a_filtered_reference_map(
        operations in prop::collection::vec((0u8..3, 0u8..12, any::<bool>(), -50i16..50), 1..100)
    ) {
        let database = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT);",
        )
        .expect("parse DDL");
        let table = subql::catalog_helpers::table_id(&database, "orders").expect("orders");
        let mut engine = SubscriptionEngine::<
            TestEvent<Postgres>,
            DefaultIds,
            ParserDB,
        >::new(database, PostgreSqlDialect {});
        let subscription = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT region, MIN(amount) FROM orders GROUP BY region \
                 HAVING MIN(amount) < 0",
            ))
            .expect("grouped minimum with a threshold registers")
            .subscription_id;
        Install::install(
            &mut engine,
            subscription,
            GroupedScalarSeedInstall {
                rows: Vec::new(),
                read_at: None::<NoCheckpoint>,
            },
        )
        .expect("empty seed installs");

        // Hidden groups never announce a key, so the keys are computed the
        // way the engine encodes them rather than discovered from updates.
        let group_keys: HashMap<String, Vec<u8>> = ["north", "south"]
            .into_iter()
            .map(|name| {
                let values = [Value::<Postgres>::String(name.into())];
                (
                    name.to_owned(),
                    subql::backend::encode_value_key(&values).expect("text encodes"),
                )
            })
            .collect();

        let mut rows = HashMap::<i64, (String, i64)>::new();
        let mut observed = BTreeMap::<Vec<u8>, i64>::new();
        for (kind, raw_id, second_group, raw_amount) in operations {
            let id = i64::from(raw_id);
            let group = if second_group { "south" } else { "north" };
            let amount = i64::from(raw_amount);
            let event = match kind {
                0 if !rows.contains_key(&id) => {
                    rows.insert(id, (group.to_owned(), amount));
                    Some(TestEvent::insert(table, row(id, group, amount)).with_pk_columns([0u16]))
                }
                1 => rows.remove(&id).map(|(old_group, old_amount)| {
                    TestEvent::delete(table, row(id, &old_group, old_amount))
                        .with_pk_columns([0u16])
                }),
                2 => rows.get_mut(&id).map(|held| {
                    let old = row(id, &held.0, held.1);
                    *held = (group.to_owned(), amount);
                    TestEvent::update(table, old, row(id, group, amount))
                        .with_pk_columns([0u16])
                        .with_changed_columns([1u16, 2u16])
                }),
                _ => None,
            };
            let Some(event) = event else {
                continue;
            };
            let output = engine.dispatch(&event).expect("event dispatches");
            prop_assert!(output.transitions().is_empty());
            apply_updates(&mut observed, output.aggregate_updates());

            let reference = rows.values().fold(
                BTreeMap::<String, Vec<i64>>::new(),
                |mut grouped, (group, value)| {
                    grouped.entry(group.clone()).or_default().push(*value);
                    grouped
                },
            );
            for trigger in output.triggers() {
                let ReExecutionRead::GroupedScalar { group, .. } = &trigger.read else {
                    prop_assert!(false, "unexpected trigger scope");
                    continue;
                };
                let name = group_keys
                    .iter()
                    .find_map(|(name, known)| (known == group).then_some(name))
                    .expect("every read names one of the two groups");
                let (minimum, count) = reference
                    .get(name)
                    .map(|values| {
                        (*values.iter().min().expect("non-empty group"), values.len())
                    })
                    .map_or((0, 0), |(minimum, count)| (minimum, count));
                let installed = Install::install(
                    &mut engine,
                    subscription,
                    GroupedScalarInstall {
                        group: group.clone(),
                        row: vec![
                            if count == 0 { Value::Null } else { Value::Int(minimum) },
                            Value::Int(i64::try_from(count).expect("test group count fits i64")),
                        ],
                        checkpoint: None::<NoCheckpoint>,
                    },
                )
                .expect("group read installs");
                apply_updates(&mut observed, &installed.updates);
            }

            // The announced result is the reference filtered by the condition.
            let expected: BTreeMap<Vec<u8>, i64> = reference
                .into_iter()
                .filter_map(|(group, values)| {
                    let minimum = *values.iter().min().expect("non-empty group");
                    (minimum < 0).then(|| {
                        (
                            group_keys.get(&group).expect("both keys precomputed").clone(),
                            minimum,
                        )
                    })
                })
                .collect();
            prop_assert_eq!(&observed, &expected);
        }
    }
}
