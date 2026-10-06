//! Current term audiences and membership watches in durable shards.

use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use sql_traits::prelude::DatabaseLike;

use crate::backend::{Backend, CdcEvent, ScalarKindOf, Value, ValueKindOf};
use crate::catalog_helpers;
use crate::compiler::literals::SqlLiteralParse;
use crate::persistence::{
    codec,
    shard::{ShardPayload, TermSeedData},
};
use crate::runtime::dispatch::ConsumerDictionary;
use crate::runtime::ids::{ConsumerOrdinal, PredicateId};
use crate::runtime::partition::TablePartition;
use crate::runtime::predicate::PredicateStore;
use crate::term::{kind_can_key, TermKey, TermLookup, TermMovement, TermSeed};
use crate::{ColumnId, IdTypes, StorageError, TableId};

use super::{RebuildPayloadError, SubscriptionEngine, TermWatch};

type StoredSeed<B> = (Vec<Value<B>>, Vec<(Value<B>, Vec<Value<B>>)>);
type DecodedSeeds<I, B> = HashMap<(u128, <I as IdTypes>::ConsumerId, u16), TermSeed<B>>;

fn seed_key<B: Backend>(value: Value<B>) -> Result<TermKey<B>, RebuildPayloadError> {
    match TermLookup::of(value) {
        TermLookup::Key(key) => Ok(key),
        TermLookup::Nobody | TermLookup::Unknown => Err(RebuildPayloadError::Corrupt(
            "term seed contains a value that cannot key a term".into(),
        )),
    }
}

fn encode_term_seed<B: Backend>(seed: TermSeed<B>) -> Result<Vec<u8>, StorageError> {
    let mut subjects = seed
        .subjects
        .into_iter()
        .map(|key| {
            let value = key.into_value();
            codec::serialize(&value).map(|bytes| (bytes, value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    subjects.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let subjects = subjects
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    let mut rows = seed
        .rows
        .into_iter()
        .map(|(subject, values)| {
            let pair = (
                subject.into_value(),
                values
                    .into_iter()
                    .map(TermKey::into_value)
                    .collect::<Vec<_>>(),
            );
            codec::serialize(&pair).map(|bytes| (bytes, pair))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let rows = rows.into_iter().map(|(_, pair)| pair).collect::<Vec<_>>();
    codec::serialize(&(subjects, rows))
}

fn column_kind<B: Backend, DB: DatabaseLike>(
    database: &DB,
    table: TableId,
    column: ColumnId,
) -> Result<ScalarKindOf<B>, RebuildPayloadError> {
    match catalog_helpers::column_scalar_kind::<B, DB>(database, table, column) {
        Some(kind) if kind_can_key::<B>(kind) => Ok(kind),
        _ => Err(RebuildPayloadError::Corrupt(format!(
            "term column {column} of table {table} has no keyable catalog kind"
        ))),
    }
}

fn decode_term_seeds<B: Backend, I: IdTypes>(
    table_id: TableId,
    payload: &ShardPayload<I>,
    term_slots: &HashMap<u128, &[Vec<ColumnId>]>,
    consumer_dict: &ConsumerDictionary<I>,
    expected_kind: &mut impl FnMut(TableId, ColumnId) -> Result<ValueKindOf<B>, RebuildPayloadError>,
) -> Result<DecodedSeeds<I, B>, RebuildPayloadError> {
    let live_pairs: HashSet<(u128, I::ConsumerId)> = payload
        .bindings
        .iter()
        .map(|binding| (binding.predicate_hash, binding.consumer_id))
        .collect();
    let mut decoded = HashMap::new();
    for record in &payload.term_seeds {
        if !live_pairs.contains(&(record.predicate_hash, record.consumer_id))
            || consumer_dict.get(record.consumer_id).is_none()
        {
            return Err(RebuildPayloadError::Corrupt(
                "term seed has no live consumer binding".into(),
            ));
        }
        let columns = term_slots
            .get(&record.predicate_hash)
            .and_then(|slots| slots.get(usize::from(record.slot)))
            .filter(|columns| !columns.is_empty())
            .ok_or_else(|| {
                RebuildPayloadError::Corrupt(
                    "term seed references an unknown predicate or slot".into(),
                )
            })?;
        let (subjects, rows): StoredSeed<B> = codec::deserialize(&record.seed)
            .map_err(|error| RebuildPayloadError::Codec(error.to_string()))?;
        let subjects = subjects
            .into_iter()
            .map(seed_key)
            .collect::<Result<Vec<_>, _>>()?;
        let keys = {
            let claimed: HashSet<&TermKey<B>> = subjects.iter().collect();
            let mut keys = Vec::with_capacity(rows.len());
            for (subject, values) in rows {
                let subject = seed_key(subject)?;
                if !claimed.contains(&subject) || values.len() != columns.len() {
                    return Err(RebuildPayloadError::Corrupt(
                        "term seed row has an unclaimed grant or invalid width".into(),
                    ));
                }
                let values = values
                    .into_iter()
                    .map(seed_key)
                    .collect::<Result<Vec<_>, _>>()?;
                for (value, column) in values.iter().zip(columns) {
                    if value.scalar_kind() != expected_kind(table_id, *column)? {
                        return Err(RebuildPayloadError::Corrupt(
                            "term seed value disagrees with its column kind".into(),
                        ));
                    }
                }
                keys.push((subject, values));
            }
            keys
        };
        let key = (record.predicate_hash, record.consumer_id, record.slot);
        if decoded
            .insert(
                key,
                TermSeed {
                    subjects,
                    rows: keys,
                },
            )
            .is_some()
        {
            return Err(RebuildPayloadError::Corrupt(
                "duplicate term seed record".into(),
            ));
        }
    }
    for (hash, consumer) in &live_pairs {
        let slots = term_slots.get(hash).ok_or_else(|| {
            RebuildPayloadError::Corrupt("term binding names an absent predicate".into())
        })?;
        for (slot, _) in slots.iter().enumerate() {
            let slot = u16::try_from(slot).map_err(|_| {
                RebuildPayloadError::Corrupt("predicate has too many term slots".into())
            })?;
            if !decoded.contains_key(&(*hash, *consumer, slot)) {
                return Err(RebuildPayloadError::Corrupt(
                    "bound term has no seed record".into(),
                ));
            }
        }
    }
    Ok(decoded)
}

fn rebuild_term_watches<B: Backend, I: IdTypes>(
    table_id: TableId,
    payload: &ShardPayload<I>,
    store: &PredicateStore<I, B>,
    term_slots: &HashMap<u128, &[Vec<ColumnId>]>,
    decoded: &DecodedSeeds<I, B>,
    expected_kind: &mut impl FnMut(TableId, ColumnId) -> Result<ValueKindOf<B>, RebuildPayloadError>,
) -> Result<Vec<TermWatch>, RebuildPayloadError> {
    let mut watches = Vec::new();
    let mut movements: HashMap<(u128, u16), &TermMovement> = HashMap::new();
    for predicate in &payload.predicates {
        let Some(slots) = term_slots.get(&predicate.hash) else {
            continue;
        };
        let mut seen_slots = HashSet::new();
        for (slot, movement) in &predicate.term_movements {
            if !seen_slots.insert(*slot) {
                return Err(RebuildPayloadError::Corrupt(
                    "duplicate term movement slot".into(),
                ));
            }
            let columns = slots
                .get(usize::from(*slot))
                .filter(|columns| !columns.is_empty())
                .ok_or_else(|| {
                    RebuildPayloadError::Corrupt("term movement references an absent slot".into())
                })?;
            if movement.member_keys.len() != columns.len() {
                return Err(RebuildPayloadError::Corrupt(
                    "term movement has an invalid column width".into(),
                ));
            }
            for (column, member) in columns.iter().zip(&movement.member_keys) {
                let compared = expected_kind(table_id, *column)?;
                let member = expected_kind(movement.member_table, *member)?;
                if compared != member {
                    return Err(RebuildPayloadError::Corrupt(
                        "term movement columns have incompatible kinds".into(),
                    ));
                }
            }
            expected_kind(movement.member_table, movement.member_subject)?;
            if movements
                .insert((predicate.hash, *slot), movement)
                .is_none()
            {
                let predicate_id = store.find_by_hash(predicate.hash).ok_or_else(|| {
                    RebuildPayloadError::Corrupt("term movement has no live predicate".into())
                })?;
                watches.push(TermWatch {
                    subscribed: table_id,
                    member_table: movement.member_table,
                    predicate: predicate_id,
                    slot: *slot,
                    columns: columns.clone(),
                    member_keys: movement.member_keys.clone(),
                    member_subject: movement.member_subject,
                });
            }
        }
    }
    for ((hash, _, slot), seed) in decoded {
        let columns = &term_slots[hash][usize::from(*slot)];
        let (table, column) = movements
            .get(&(*hash, *slot))
            .map_or((table_id, columns[0]), |movement| {
                (movement.member_table, movement.member_subject)
            });
        let expected = expected_kind(table, column)?;
        if seed
            .subjects
            .iter()
            .any(|subject| subject.scalar_kind() != expected)
        {
            return Err(RebuildPayloadError::Corrupt(
                "term subject disagrees with its column kind".into(),
            ));
        }
    }
    watches.sort_unstable_by_key(|watch| (watch.predicate, watch.slot));
    Ok(watches)
}

impl<E: CdcEvent, I: IdTypes, DB: DatabaseLike> SubscriptionEngine<E, I, DB>
where
    E::Backend: SqlLiteralParse,
{
    pub(super) fn snapshot_term_seeds(
        table_id: TableId,
        store: &PredicateStore<I, E::Backend>,
        consumer_dict: &ConsumerDictionary<I>,
    ) -> Result<Vec<TermSeedData<I>>, StorageError> {
        let mut records = Vec::new();
        for ((pred_id, slot), members) in store.term_members.iter() {
            let predicate = store.get_predicate(*pred_id).ok_or_else(|| {
                StorageError::Corrupt(format!("term slot in table {table_id} has no predicate"))
            })?;
            for (ordinal, seed) in members.snapshot_seeds() {
                let consumer_id = consumer_dict.get_consumer(ordinal).ok_or_else(|| {
                    StorageError::Corrupt(format!(
                        "term seed ordinal {} in table {table_id} has no consumer",
                        ordinal.get()
                    ))
                })?;
                records.push(TermSeedData {
                    predicate_hash: predicate.hash,
                    consumer_id,
                    slot: *slot,
                    seed: encode_term_seed(seed)?,
                });
            }
        }
        records.sort_unstable_by_key(|record| {
            (record.predicate_hash, record.consumer_id, record.slot)
        });
        Ok(records)
    }

    pub(super) fn snapshot_term_movements(
        &self,
        table_id: TableId,
    ) -> HashMap<PredicateId, Vec<(u16, TermMovement)>> {
        let mut movements: HashMap<PredicateId, Vec<(u16, TermMovement)>> = HashMap::new();
        for watch in self
            .term_watch
            .values()
            .flatten()
            .filter(|watch| watch.subscribed == table_id)
        {
            movements.entry(watch.predicate).or_default().push((
                watch.slot,
                TermMovement {
                    member_table: watch.member_table,
                    member_keys: watch.member_keys.clone(),
                    member_subject: watch.member_subject,
                },
            ));
        }
        for slots in movements.values_mut() {
            slots.sort_unstable_by_key(|(slot, _)| *slot);
        }
        movements
    }

    pub(super) fn rebuild_term_state(
        &self,
        table_id: TableId,
        payload: &ShardPayload<I>,
        consumer_dict: &ConsumerDictionary<I>,
        partition: &mut TablePartition<I, E::Backend>,
    ) -> Result<Vec<TermWatch>, RebuildPayloadError> {
        let snapshot = partition.load_snapshot();
        let store = &snapshot.predicates;
        let term_slots: HashMap<u128, &[Vec<ColumnId>]> = store
            .predicates
            .iter()
            .map(|(_, predicate)| (predicate.hash, predicate.bytecode.term_columns.as_slice()))
            .collect();
        let mut kinds: HashMap<(TableId, ColumnId), ValueKindOf<E::Backend>> = HashMap::new();
        let mut expected_kind = |table, column| {
            if let Some(kind) = kinds.get(&(table, column)) {
                return Ok(*kind);
            }
            let kind = column_kind::<E::Backend, _>(&self.database, table, column)?.value_kind();
            kinds.insert((table, column), kind);
            Ok::<_, RebuildPayloadError>(kind)
        };
        let decoded = decode_term_seeds(
            table_id,
            payload,
            &term_slots,
            consumer_dict,
            &mut expected_kind,
        )?;

        let watches = rebuild_term_watches(
            table_id,
            payload,
            store,
            &term_slots,
            &decoded,
            &mut expected_kind,
        )?;
        if decoded.is_empty() {
            return Ok(watches);
        }

        let mut ordered = decoded.into_iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|(key, _)| *key);
        let mut grouped: HashMap<(PredicateId, ConsumerOrdinal), Vec<TermSeed<E::Backend>>> =
            HashMap::new();
        for ((hash, consumer, _), seed) in ordered {
            let predicate = store.find_by_hash(hash).ok_or_else(|| {
                RebuildPayloadError::Corrupt("term seed has no live predicate".into())
            })?;
            let ordinal = consumer_dict.get(consumer).ok_or_else(|| {
                RebuildPayloadError::Corrupt("term seed has no live consumer".into())
            })?;
            grouped.entry((predicate, ordinal)).or_default().push(seed);
        }
        partition.mutate(|txn| {
            for ((predicate, ordinal), seeds) in &grouped {
                txn.seed_terms(*predicate, *ordinal, seeds);
            }
        });
        Ok(watches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Postgres;
    use crate::compiler::sql_shape::QueryProjection;
    use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan};
    use crate::persistence::codec;
    use crate::persistence::shard::{
        BindingData, ConsumerDictData, PredicateData, ShardPayload, TermSeedData,
    };
    use crate::persistence::test_support::make_catalog;
    use crate::term::{TermKey, TermMovement, TermSeed};
    use crate::testing::TestEvent;
    use crate::{ColumnId, DefaultIds, SubscriptionScope, TableId};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

    const ORDERS: TableId = 1;
    const AMOUNT: ColumnId = 1;
    const STATUS: ColumnId = 2;
    const TERM_HASH: u128 = 0x0100;
    const CONSUMER: u64 = 7;

    fn engine() -> Engine {
        SubscriptionEngine::new(make_catalog(), PostgreSqlDialect {})
    }

    fn unkeyable_amount_engine() -> Engine {
        let database = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE _persistence_pad (id INT);
             CREATE TABLE orders (id INT PRIMARY KEY, amount DOUBLE PRECISION, status TEXT);",
        )
        .expect("the floating-point catalog parses");
        SubscriptionEngine::new(database, PostgreSqlDialect {})
    }
    fn term_program(columns: Vec<Vec<ColumnId>>) -> Vec<u8> {
        codec::serialize(&BytecodeProgram::<Postgres>::with_terms(
            alloc::vec![Instruction::TermTruth(0)],
            columns,
        ))
        .expect("the program encodes")
    }

    fn movement(subject: ColumnId, keys: Vec<ColumnId>) -> TermMovement {
        TermMovement {
            member_table: ORDERS,
            member_keys: keys,
            member_subject: subject,
        }
    }

    fn term_predicate(
        hash: u128,
        columns: Vec<Vec<ColumnId>>,
        movements: Vec<(u16, TermMovement)>,
    ) -> PredicateData {
        PredicateData {
            hash,
            normalized_sql: "amount = 5".to_string(),
            bytecode_instructions: term_program(columns),
            prefilter_plan: codec::serialize(&PrefilterPlan::default()).expect("the plan encodes"),
            dependency_columns: alloc::vec![AMOUNT],
            projection: QueryProjection::Rows,
            refcount: 1,
            updated_at_unix_ms: 0,
            term_movements: movements,
        }
    }

    fn binding(hash: u128, consumer: u64) -> BindingData<DefaultIds> {
        BindingData {
            subscription_id: 1,
            predicate_hash: hash,
            consumer_id: consumer,
            scope: SubscriptionScope::Durable,
            updated_at_unix_ms: 0,
        }
    }

    fn seed_bytes(
        subjects: Vec<TermKey<Postgres>>,
        rows: Vec<(TermKey<Postgres>, Vec<TermKey<Postgres>>)>,
    ) -> Vec<u8> {
        encode_term_seed(TermSeed { subjects, rows }).expect("the seed encodes")
    }

    fn grant(subject: i64, value: i64) -> Vec<u8> {
        seed_bytes(
            alloc::vec![TermKey::Int(subject)],
            alloc::vec![(TermKey::Int(subject), alloc::vec![TermKey::Int(value)])],
        )
    }

    fn seed_record(slot: u16, seed: Vec<u8>) -> TermSeedData<DefaultIds> {
        TermSeedData {
            predicate_hash: TERM_HASH,
            consumer_id: CONSUMER,
            slot,
            seed,
        }
    }

    fn shard(
        predicates: Vec<PredicateData>,
        bindings: Vec<BindingData<DefaultIds>>,
        consumers: Vec<u64>,
        seeds: Vec<TermSeedData<DefaultIds>>,
    ) -> ShardPayload<DefaultIds> {
        ShardPayload {
            predicates,
            bindings,
            consumer_dict: ConsumerDictData {
                ordinal_to_consumer: consumers,
            },
            term_seeds: seeds,
            created_at_unix_ms: 0,
        }
    }

    fn standard_payload() -> ShardPayload<DefaultIds> {
        shard(
            vec![term_predicate(
                TERM_HASH,
                vec![vec![AMOUNT]],
                vec![(0, movement(0, vec![AMOUNT]))],
            )],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(42, 5))],
        )
    }

    fn order_row(id: i64, amount: i64) -> Vec<Value<Postgres>> {
        vec![
            Value::Int(id),
            Value::Int(amount),
            Value::String("paid".into()),
        ]
    }

    fn assert_seeded(engine: &mut Engine) {
        let row = |amount| {
            vec![
                Value::Int(101),
                Value::Int(amount),
                Value::String("paid".into()),
            ]
        };
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(ORDERS, row(5)))
                .expect("the granted row dispatches")
                .inserted(),
            &[CONSUMER]
        );
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(ORDERS, row(6)))
                .expect("the ungranted row dispatches")
                .inserted(),
            &[] as &[u64]
        );
    }

    fn assert_no_state_restored(engine: &Engine) {
        assert!(
            engine.partitions.get(&ORDERS).is_none(),
            "a refused shard must not install a partition"
        );
        assert!(
            engine.consumer_dictionaries.get(&ORDERS).is_none(),
            "nor name its consumers"
        );
        assert!(
            engine.term_watch.get(&ORDERS).is_none(),
            "nor watch its terms"
        );
    }

    fn refused_corrupt(engine: Engine, payload: &ShardPayload<DefaultIds>) {
        let mut engine = engine;
        let error = engine
            .rebuild_and_replace_table_state(ORDERS, payload)
            .expect_err("the shard must be refused");
        assert!(
            matches!(error, RebuildPayloadError::Corrupt(_)),
            "the refusal names the corruption rather than a codec failure"
        );
        assert_no_state_restored(&engine);
    }

    #[test]
    fn the_agreeing_seed_and_movement_restore_the_audience() {
        let mut engine = engine();
        engine
            .rebuild_and_replace_table_state(ORDERS, &standard_payload())
            .expect("a valid shard restores");

        assert_seeded(&mut engine);
        let withdrawn = engine
            .consumers(&TestEvent::delete(
                ORDERS,
                vec![Value::Int(42), Value::Int(5), Value::String("paid".into())],
            ))
            .expect("the restored watch withdraws the grant");
        assert_eq!(withdrawn.narrowings().len(), 1);
        assert!(!withdrawn.narrowings()[0].entered);
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(
                    ORDERS,
                    vec![Value::Int(101), Value::Int(5), Value::String("paid".into())],
                ))
                .expect("the withdrawn row dispatches")
                .inserted(),
            &[] as &[u64]
        );
    }

    #[test]
    fn the_seed_without_a_movement_restores_without_watching() {
        let mut engine = engine();
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(5, 5))],
        );
        engine
            .rebuild_and_replace_table_state(ORDERS, &payload)
            .expect("a seed without a movement still restores");

        let removed = engine
            .consumers(&TestEvent::delete(
                ORDERS,
                vec![Value::Int(5), Value::Int(5), Value::String("paid".into())],
            ))
            .expect("the ordinary row deletion dispatches");
        assert_eq!(removed.narrowings().len(), 0);
        assert_seeded(&mut engine);
    }

    #[test]
    fn a_cloned_shard_keeps_its_claims_after_the_original_is_withdrawn() {
        let mut original = standard_payload();
        let copied = original.clone();
        original.term_seeds[0].seed = seed_bytes(vec![], vec![]);

        let mut withdrawn = engine();
        withdrawn
            .rebuild_and_replace_table_state(ORDERS, &original)
            .expect("the explicit empty audience restores");
        let mut retained = engine();
        retained
            .rebuild_and_replace_table_state(ORDERS, &copied)
            .expect("the independent copied audience restores");

        assert_eq!(
            withdrawn
                .consumers(&TestEvent::insert(ORDERS, order_row(101, 5)))
                .expect("the withdrawn audience dispatches")
                .inserted(),
            &[] as &[u64]
        );
        assert_seeded(&mut retained);

        let granted = retained
            .consumers(&TestEvent::insert(ORDERS, order_row(42, 9)))
            .expect("the copied claim accepts a new grant");
        assert_eq!(granted.narrowings().len(), 1);
        assert!(granted.narrowings()[0].entered);
        assert_eq!(
            retained
                .consumers(&TestEvent::insert(ORDERS, order_row(101, 9)))
                .expect("the copied audience dispatches")
                .inserted(),
            &[CONSUMER]
        );
        let unclaimed = withdrawn
            .consumers(&TestEvent::insert(ORDERS, order_row(42, 9)))
            .expect("the empty audience ignores the grant");
        assert_eq!(unclaimed.narrowings().len(), 0);
        assert_eq!(
            withdrawn
                .consumers(&TestEvent::insert(ORDERS, order_row(101, 9)))
                .expect("the empty audience stays closed")
                .inserted(),
            &[] as &[u64]
        );
    }

    #[test]
    fn replacing_a_watched_term_with_a_static_term_removes_the_old_watch() {
        let mut engine = engine();
        engine
            .rebuild_and_replace_table_state(ORDERS, &standard_payload())
            .expect("the watched audience restores");
        let replacement = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(5, 5))],
        );
        engine
            .rebuild_and_replace_table_state(ORDERS, &replacement)
            .expect("the static audience replaces it");

        let ordinary = engine
            .consumers(&TestEvent::insert(ORDERS, order_row(5, 9)))
            .expect("the ordinary row dispatches");
        assert_eq!(ordinary.narrowings().len(), 0);
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(ORDERS, order_row(101, 9)))
                .expect("the ungranted row dispatches")
                .inserted(),
            &[] as &[u64]
        );
        assert_seeded(&mut engine);
    }

    #[test]
    fn replacing_one_table_preserves_another_tables_membership_watch() {
        const OTHER: TableId = 0;
        let database = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE _persistence_pad (id INT PRIMARY KEY, amount INT, status TEXT);
             CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
        )
        .expect("both watched tables parse");
        let mut engine: Engine = SubscriptionEngine::new(database, PostgreSqlDialect {});
        let mut watched = standard_payload();
        watched.predicates[0].term_movements[0].1.member_table = OTHER;
        engine
            .rebuild_and_replace_table_state(ORDERS, &watched)
            .expect("the first watched audience restores");
        let mut other = watched.clone();
        other.bindings[0].subscription_id = 2;
        other.bindings[0].consumer_id = 8;
        other.consumer_dict.ordinal_to_consumer[0] = 8;
        other.term_seeds[0].consumer_id = 8;
        engine
            .rebuild_and_replace_table_state(OTHER, &other)
            .expect("the other watched audience restores");

        let replacement = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(5, 5))],
        );
        engine
            .rebuild_and_replace_table_state(ORDERS, &replacement)
            .expect("only the first audience is replaced");

        let granted = engine
            .consumers(&TestEvent::insert(OTHER, order_row(42, 9)))
            .expect("the surviving watch receives its grant");
        let [narrowing] = granted.narrowings() else {
            panic!("one surviving watch must change its audience")
        };
        assert_eq!(narrowing.table, OTHER);
        assert_eq!(narrowing.subscription, 2);
        assert!(narrowing.entered);
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(OTHER, order_row(101, 9)))
                .expect("the surviving audience dispatches")
                .inserted(),
            &[8]
        );
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(ORDERS, order_row(101, 9)))
                .expect("the replaced audience dispatches")
                .inserted(),
            &[] as &[u64]
        );
        assert_seeded(&mut engine);
    }

    #[test]
    fn refusing_a_corrupt_replacement_keeps_the_current_audience_and_watch() {
        let mut engine = engine();
        engine
            .rebuild_and_replace_table_state(ORDERS, &standard_payload())
            .expect("the current audience restores");
        let mut corrupt = standard_payload();
        corrupt.term_seeds[0].seed = b"not a term seed".to_vec();
        assert!(matches!(
            engine.rebuild_and_replace_table_state(ORDERS, &corrupt),
            Err(RebuildPayloadError::Codec(_))
        ));
        assert_seeded(&mut engine);

        let withdrawn = engine
            .consumers(&TestEvent::delete(ORDERS, order_row(42, 5)))
            .expect("the original watch still withdraws its grant");
        assert_eq!(withdrawn.narrowings().len(), 1);
        assert!(!withdrawn.narrowings()[0].entered);
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(ORDERS, order_row(101, 5)))
                .expect("the withdrawal changes the original audience")
                .inserted(),
            &[] as &[u64]
        );
    }

    #[test]
    fn canonical_seed_order_preserves_each_subjects_grants() {
        let subjects = vec![TermKey::Int(99), TermKey::Int(7), TermKey::Int(42)];
        let rows = vec![
            (TermKey::Int(99), vec![TermKey::Int(9)]),
            (TermKey::Int(7), vec![TermKey::Int(6)]),
            (TermKey::Int(42), vec![TermKey::Int(5)]),
        ];
        let encoded = seed_bytes(subjects.clone(), rows.clone());
        assert_eq!(
            encoded,
            seed_bytes(
                subjects.into_iter().rev().collect(),
                rows.into_iter().rev().collect(),
            )
        );
        let mut payload = standard_payload();
        payload.term_seeds[0].seed = encoded;
        let mut engine = engine();
        engine
            .rebuild_and_replace_table_state(ORDERS, &payload)
            .expect("the canonical multi-subject audience restores");

        for amount in [5, 6, 9] {
            assert_eq!(
                engine
                    .consumers(&TestEvent::insert(ORDERS, order_row(101, amount)))
                    .expect("each distinct grant dispatches")
                    .inserted(),
                &[CONSUMER]
            );
        }
        let withdrawn = engine
            .consumers(&TestEvent::delete(ORDERS, order_row(42, 5)))
            .expect("only the named subject loses its grant");
        assert_eq!(withdrawn.narrowings().len(), 1);
        assert!(!withdrawn.narrowings()[0].entered);
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(ORDERS, order_row(101, 5)))
                .expect("the withdrawn grant dispatches")
                .inserted(),
            &[] as &[u64]
        );
        for amount in [6, 9] {
            assert_eq!(
                engine
                    .consumers(&TestEvent::insert(ORDERS, order_row(101, amount)))
                    .expect("another subject retains its grant")
                    .inserted(),
                &[CONSUMER]
            );
        }
    }

    #[test]
    fn a_term_on_an_unkeyable_catalog_column_is_refused() {
        refused_corrupt(unkeyable_amount_engine(), &standard_payload());
    }

    #[test]
    fn an_empty_seed_still_requires_a_keyable_compared_column() {
        for moving in [false, true] {
            let mut payload = standard_payload();
            payload.term_seeds[0].seed = seed_bytes(vec![TermKey::Int(42)], Vec::new());
            if !moving {
                payload.predicates[0].term_movements.clear();
            }
            refused_corrupt(unkeyable_amount_engine(), &payload);
        }
    }

    #[test]
    fn a_movement_from_an_unkeyable_subject_column_is_refused() {
        let mut payload = standard_payload();
        payload.predicates[0] = term_predicate(
            TERM_HASH,
            vec![vec![0]],
            vec![(0, movement(AMOUNT, vec![0]))],
        );
        refused_corrupt(unkeyable_amount_engine(), &payload);
    }

    #[test]
    fn a_movement_to_an_absent_catalog_table_is_refused() {
        let mut payload = standard_payload();
        payload.predicates[0].term_movements[0].1.member_table = 99;
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn a_grant_from_an_unkeyable_subject_is_refused() {
        let subjects: Vec<Value<Postgres>> = vec![Value::Int(42)];
        let rows: Vec<(Value<Postgres>, Vec<Value<Postgres>>)> =
            vec![(Value::Null, vec![Value::Int(5)])];
        let mut payload = standard_payload();
        payload.term_seeds[0].seed =
            codec::serialize(&(subjects, rows)).expect("the malformed subject encodes");
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn an_unkeyable_claimed_subject_is_refused() {
        let subjects: Vec<Value<Postgres>> = vec![Value::Null];
        let rows: Vec<(Value<Postgres>, Vec<Value<Postgres>>)> = Vec::new();
        let mut payload = standard_payload();
        payload.term_seeds[0].seed =
            codec::serialize(&(subjects, rows)).expect("the malformed claim encodes");
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn duplicate_predicate_records_restore_one_watch_per_audience() {
        const OTHER_HASH: u128 = 0x0200;
        let mut other_binding = binding(OTHER_HASH, 8);
        other_binding.subscription_id = 2;
        let payload = shard(
            vec![
                term_predicate(
                    OTHER_HASH,
                    vec![vec![AMOUNT]],
                    vec![(0, movement(0, vec![AMOUNT]))],
                ),
                term_predicate(
                    TERM_HASH,
                    vec![vec![AMOUNT]],
                    vec![(0, movement(0, vec![AMOUNT]))],
                ),
                term_predicate(
                    TERM_HASH,
                    vec![vec![AMOUNT]],
                    vec![(0, movement(0, vec![AMOUNT]))],
                ),
            ],
            vec![other_binding, binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER, 8],
            vec![
                TermSeedData {
                    predicate_hash: OTHER_HASH,
                    consumer_id: 8,
                    slot: 0,
                    seed: grant(42, 9),
                },
                seed_record(0, grant(42, 5)),
            ],
        );
        let mut engine = engine();
        engine
            .rebuild_and_replace_table_state(ORDERS, &payload)
            .expect("the two audiences restore without duplicate watches");
        for (amount, consumer) in [(5, CONSUMER), (9, 8)] {
            assert_eq!(
                engine
                    .consumers(&TestEvent::insert(ORDERS, order_row(101, amount)))
                    .expect("each restored audience dispatches")
                    .inserted(),
                &[consumer]
            );
        }

        let granted = engine
            .consumers(&TestEvent::insert(ORDERS, order_row(42, 11)))
            .expect("the shared membership reaches both audiences");
        assert_eq!(granted.narrowings().len(), 2);
        assert!(granted.narrowings().iter().all(|change| change.entered));
        assert_eq!(
            engine
                .consumers(&TestEvent::insert(ORDERS, order_row(101, 11)))
                .expect("both audiences admit the new grant")
                .inserted(),
            &[CONSUMER, 8]
        );
    }

    #[test]
    fn the_seed_outside_any_live_binding_is_refused() {
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![],
            vec![],
            vec![seed_record(0, grant(42, 5))],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_seed_for_a_slot_the_predicate_does_not_carry_is_refused() {
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(1, grant(42, 5))],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_seed_for_an_empty_slot_is_refused() {
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(42, 5))],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_seed_carrying_a_value_that_cannot_key_a_term_is_refused() {
        let subjects: Vec<Value<Postgres>> = alloc::vec![Value::Int(42)];
        let rows: Vec<(Value<Postgres>, Vec<Value<Postgres>>)> =
            alloc::vec![(Value::Int(42), alloc::vec![Value::Float(1.5)])];
        let seed = codec::serialize(&(subjects, rows)).expect("the fixture encodes");
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, seed)],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_seed_value_of_the_wrong_kind_is_refused() {
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(
                0,
                seed_bytes(
                    alloc::vec![TermKey::Int(42)],
                    alloc::vec![(
                        TermKey::Int(42),
                        alloc::vec![TermKey::String("paid".into())],
                    )],
                ),
            )],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_seed_granted_by_an_unclaimed_subject_is_refused() {
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(
                0,
                seed_bytes(
                    alloc::vec![TermKey::Int(42)],
                    alloc::vec![(TermKey::Int(99), alloc::vec![TermKey::Int(5)])],
                ),
            )],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_seed_row_of_the_wrong_width_is_refused() {
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(
                0,
                seed_bytes(
                    alloc::vec![TermKey::Int(42)],
                    alloc::vec![(
                        TermKey::Int(42),
                        alloc::vec![TermKey::Int(5), TermKey::Int(6)],
                    )],
                ),
            )],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_duplicate_seed_record_is_refused() {
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(42, 5)), seed_record(0, grant(42, 6))],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_bound_term_without_a_seed_record_is_refused() {
        let payload = shard(
            vec![term_predicate(TERM_HASH, vec![vec![AMOUNT]], vec![])],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_subject_of_the_wrong_kind_is_refused() {
        let payload = shard(
            vec![term_predicate(
                TERM_HASH,
                vec![vec![AMOUNT]],
                vec![(0, movement(AMOUNT, vec![AMOUNT]))],
            )],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(
                0,
                seed_bytes(
                    alloc::vec![TermKey::String("alice".into())],
                    alloc::vec![(
                        TermKey::String("alice".into()),
                        alloc::vec![TermKey::Int(5)],
                    )],
                ),
            )],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_duplicate_movement_slot_is_refused() {
        let payload = shard(
            vec![term_predicate(
                TERM_HASH,
                vec![vec![AMOUNT]],
                vec![
                    (0, movement(AMOUNT, vec![AMOUNT])),
                    (0, movement(AMOUNT, vec![AMOUNT])),
                ],
            )],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(42, 5))],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_movement_outside_the_term_slots_is_refused() {
        let payload = shard(
            vec![term_predicate(
                TERM_HASH,
                vec![vec![AMOUNT]],
                vec![(5, movement(AMOUNT, vec![AMOUNT]))],
            )],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(42, 5))],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_movement_of_the_wrong_width_is_refused() {
        let payload = shard(
            vec![term_predicate(
                TERM_HASH,
                vec![vec![AMOUNT]],
                vec![(0, movement(AMOUNT, vec![AMOUNT, STATUS]))],
            )],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(42, 5))],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_movement_between_incompatible_kinds_is_refused() {
        let payload = shard(
            vec![term_predicate(
                TERM_HASH,
                vec![vec![AMOUNT]],
                vec![(0, movement(AMOUNT, vec![STATUS]))],
            )],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, grant(42, 5))],
        );
        refused_corrupt(engine(), &payload);
    }

    #[test]
    fn the_seed_bytes_that_do_not_encode_a_seed_are_refused() {
        let payload = shard(
            vec![term_predicate(
                TERM_HASH,
                vec![vec![AMOUNT]],
                vec![(0, movement(AMOUNT, vec![AMOUNT]))],
            )],
            vec![binding(TERM_HASH, CONSUMER)],
            vec![CONSUMER],
            vec![seed_record(0, b"not a term seed".to_vec())],
        );
        let mut engine = engine();
        let error = engine
            .rebuild_and_replace_table_state(ORDERS, &payload)
            .expect_err("the shard must be refused");
        assert!(
            matches!(error, RebuildPayloadError::Codec(_)),
            "undecodable bytes are a codec failure, not a corruption"
        );
        assert_no_state_restored(&engine);
    }
}
