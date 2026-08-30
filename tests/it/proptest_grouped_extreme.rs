//! Grouped extreme updates match a reference multiset across random row changes.
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{
    Backend, BuiltinKind, GroupKeyCollation, GroupKeyColumn, NoCustom, Postgres, Value,
};
use subql::reexec::ReExecutionRead;
use subql::testing::TestEvent;
use subql::{
    AggregateResultValue, AggregateValueChange, DefaultIds, GroupIdentity, GroupedScalarInstall,
    GroupedScalarSeedInstall, Install, NoCheckpoint, SubscriptionEngine, SubscriptionRequest,
};

fn row(id: i64, group: &str, amount: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(group.into()),
        Value::Int(amount),
    ]
}

fn identity_group_name(identity: &GroupIdentity<Postgres>) -> &str {
    let [Value::String(name)] = identity.values.as_slice() else {
        panic!("group identity must carry one string value")
    };
    name
}

fn text_group_key(name: &str) -> Vec<u8> {
    let encoder = Postgres::group_key_encoder(vec![GroupKeyColumn::<NoCustom> {
        kind: BuiltinKind::String.into(),
        declared_type: String::from("TEXT"),
        collation: GroupKeyCollation::DatabaseDefault,
    }])
    .expect("Postgres default text has a canonical key");
    encoder
        .encode(&[Value::String(name.into())])
        .expect("text matches the encoder")
}

fn apply_updates(
    observed: &mut BTreeMap<Vec<u8>, i64>,
    group_keys: &HashMap<String, Vec<u8>>,
    updates: &[subql::AggregateValueUpdate<DefaultIds, Postgres>],
) {
    for update in updates {
        let identity = update.group.as_ref().expect("group identity");
        let name = identity_group_name(identity);
        assert_eq!(
            group_keys.get(name),
            Some(&identity.key),
            "identity values must name the updated reference group"
        );
        match &update.change {
            AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(value))) => {
                observed.insert(identity.key.clone(), *value);
            }
            AggregateValueChange::Remove => {
                observed.remove(&identity.key);
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
                        let identity = update.group.as_ref()?;
                        (matches!(update.change, AggregateValueChange::Set(_))
                            && identity_group_name(identity) == group)
                            .then(|| identity.key.clone())
                    })
                    .expect("a new group emits its identity");
                group_keys.insert(group, key);
            }
            apply_updates(&mut observed, &group_keys, output.aggregate_updates());

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
                    .map_or((0, 0), |values| {
                        (*values.iter().min().expect("non-empty group"), values.len())
                    });
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
                apply_updates(&mut observed, &group_keys, &installed.updates);
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
                (
                    name.to_owned(),
                    text_group_key(name),
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
            apply_updates(&mut observed, &group_keys, output.aggregate_updates());

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
                    .map_or((0, 0), |values| {
                        (*values.iter().min().expect("non-empty group"), values.len())
                    });
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
                apply_updates(&mut observed, &group_keys, &installed.updates);
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
