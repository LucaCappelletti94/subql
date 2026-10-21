//! Replay admission, transaction boundaries, and checkpoint verification.

use std::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::TimeZone;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, NumericValue, ReplayAnswer, ReplayDetector, ReplayError,
    ReplayTable, ReplayValue, SubscriptionEngine, SubscriptionRequest, TableId,
};

type Event = TestEvent<Postgres>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

const WIDE_DDL: &str = "CREATE TABLE wide (
    id INT PRIMARY KEY,
    flag BOOL,
    f DOUBLE PRECISION,
    s TEXT,
    b BYTEA,
    u UUID,
    ts TIMESTAMP,
    tstz TIMESTAMPTZ,
    d DATE,
    t TIME,
    num NUMERIC,
    j JSON,
    jb JSONB
);";

const WIDE_WRONG_TYPE_DDL: &str = "CREATE TABLE wide (
    id INT PRIMARY KEY,
    flag BOOL,
    f DOUBLE PRECISION,
    s BIGINT,
    b BYTEA,
    u UUID,
    ts TIMESTAMP,
    tstz TIMESTAMPTZ,
    d DATE,
    t TIME,
    num NUMERIC,
    j JSON,
    jb JSONB
);";

const READINGS_DDL: &str = "CREATE TABLE readings (id INT PRIMARY KEY, grp INT, qty INT, \
                             price DOUBLE PRECISION, note TEXT, amount NUMERIC);";

const NAN_DDL: &str = "CREATE TABLE readings (id INT PRIMARY KEY, grp INT, qty INT, \
                       price DOUBLE PRECISION, note TEXT, amount NUMERIC);
                       CREATE TABLE unrelated (id INT PRIMARY KEY, x INT);";

fn parse(ddl: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(ddl).expect("the DDL parses")
}

fn table_of(db: &ParserDB, name: &str) -> TableId {
    catalog_helpers::table_id::<Postgres, _>(db, name).expect("the table is in the catalog")
}

fn engine_of(db: ParserDB) -> Engine {
    SubscriptionEngine::new(db, PostgreSqlDialect {})
}

fn insert(table: TableId, row: Vec<Value<Postgres>>) -> Event {
    Event::insert(table, row).with_pk_columns([0u16])
}

fn update(table: TableId, old: Vec<Value<Postgres>>, new: Vec<Value<Postgres>>) -> Event {
    Event::update(table, old, new).with_pk_columns([0u16])
}

fn delete(table: TableId, row: Vec<Value<Postgres>>) -> Event {
    Event::delete(table, row).with_pk_columns([0u16])
}

fn detector_of(
    engine: &Engine,
    sql: &str,
    table: TableId,
    rows: Vec<Vec<Value<Postgres>>>,
) -> ReplayDetector<Postgres> {
    engine
        .replay_detector(
            &SubscriptionRequest::new(1, sql),
            vec![ReplayTable {
                table_id: table,
                rows,
            }],
        )
        .expect("the statement replays in process")
}

fn restored_of(engine: &Engine, sql: &str, bytes: &[u8]) -> ReplayDetector<Postgres> {
    engine
        .restore_replay_detector(&SubscriptionRequest::new(1, sql), bytes)
        .expect("the checkpoint verifies against the same plan and catalog")
}

fn rows_of(answer: &ReplayAnswer<Postgres>) -> Vec<(Vec<Value<Postgres>>, u64)> {
    match answer {
        ReplayAnswer::Rows(rows) => rows.clone(),
        other => panic!("the rows tier answers rows, got {other:?}"),
    }
}

fn grouped_of<F>(answer: &ReplayAnswer<Postgres>, extract: F) -> Vec<(i64, i64)>
where
    F: Fn(&ReplayValue<Postgres>) -> i64,
{
    match answer {
        ReplayAnswer::Grouped(rows) => {
            let mut out = Vec::new();
            for (group, value) in rows {
                let grp = match group.values.first().expect("one group column") {
                    Value::Int(g) => *g,
                    other => panic!("the group key is an int, got {other:?}"),
                };
                out.push((grp, extract(value)));
            }
            out.sort_unstable();
            out
        }
        other => panic!("a grouped query answers groups, got {other:?}"),
    }
}

fn grouped_fold_of(answer: &ReplayAnswer<Postgres>) -> Vec<(i64, i64)> {
    grouped_of(answer, |value| match value {
        ReplayValue::Folded(AggValue::Sum(Some(NumericValue::Integer(s)))) => *s,
        other => panic!("the group total is an exact integer, got {other:?}"),
    })
}

fn grouped_extreme_of(answer: &ReplayAnswer<Postgres>) -> Vec<(i64, f64)> {
    match answer {
        ReplayAnswer::Grouped(rows) => {
            let mut out = Vec::new();
            for (group, value) in rows {
                let grp = match group.values.first().expect("one group column") {
                    Value::Int(g) => *g,
                    other => panic!("the group key is an int, got {other:?}"),
                };
                let max = match value {
                    ReplayValue::Scalar(Value::Float(f)) => *f,
                    other => panic!("the group extreme is a float, got {other:?}"),
                };
                out.push((grp, max));
            }
            out.sort_by_key(|(group, _)| *group);
            out
        }
        other => panic!("a grouped extreme answers groups, got {other:?}"),
    }
}

fn extreme_of(answer: &ReplayAnswer<Postgres>) -> Value<Postgres> {
    match answer {
        ReplayAnswer::Scalar(ReplayValue::Scalar(value)) => value.clone(),
        other => panic!("a scalar extreme answers one scalar, got {other:?}"),
    }
}

fn count_star_of(answer: &ReplayAnswer<Postgres>) -> i64 {
    match answer {
        ReplayAnswer::Scalar(ReplayValue::Folded(AggValue::CountStar(n))) => *n,
        other => panic!("count(*) answers a row count, got {other:?}"),
    }
}

fn count_column_of(answer: &ReplayAnswer<Postgres>) -> i64 {
    match answer {
        ReplayAnswer::Scalar(ReplayValue::Folded(AggValue::CountColumn(n))) => *n,
        other => panic!("count(col) answers a non-null count, got {other:?}"),
    }
}

fn sum_integer_of(answer: &ReplayAnswer<Postgres>) -> i64 {
    match answer {
        ReplayAnswer::Scalar(ReplayValue::Folded(AggValue::Sum(Some(NumericValue::Integer(
            n,
        ))))) => *n,
        other => panic!("sum(qty) answers an exact integer, got {other:?}"),
    }
}

fn sum_decimal_of(answer: &ReplayAnswer<Postgres>) -> BigDecimal {
    match answer {
        ReplayAnswer::Scalar(ReplayValue::Folded(AggValue::Sum(Some(NumericValue::Decimal(
            n,
        ))))) => n.clone(),
        other => panic!("sum(amount) answers an exact decimal, got {other:?}"),
    }
}

/// A retained row of `wide` carrying one edge value per scalar family.
fn wide_row_a() -> Vec<Value<Postgres>> {
    vec![
        Value::Int(1),
        Value::Bool(true),
        Value::Float(1.5),
        Value::String("a".into()),
        Value::Bytes(vec![0, 1, 2]),
        Value::Uuid(
            uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .expect("a fixed literal parses"),
        ),
        Value::Timestamp(
            chrono::NaiveDate::from_ymd_opt(2024, 3, 1)
                .expect("a fixed date")
                .and_hms_milli_opt(12, 34, 56, 789)
                .expect("a fixed timestamp"),
        ),
        Value::TimestampTz(
            chrono::Utc
                .timestamp_opt(1_709_296_096, 500_000_000)
                .single()
                .expect("a fixed instant"),
        ),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 3, 1).expect("a fixed date")),
        Value::Time(
            chrono::NaiveTime::from_hms_milli_opt(13, 14, 15, 123).expect("a fixed time of day"),
        ),
        Value::Decimal(BigDecimal::from_str("0.30").expect("a fixed decimal")),
        Value::Json(serde_json::json!({ "k": 1 })),
        Value::Jsonb(serde_json::json!([1, 2.5, { "x": "y" }])),
    ]
}

/// Structured `JSON` nulls remain distinct from `SQL` nulls.
fn wide_row_b() -> Vec<Value<Postgres>> {
    let mut row = wide_row_a();
    row[0] = Value::Int(2);
    row[1] = Value::Bool(false);
    row[2] = Value::Float(f64::INFINITY);
    row[3] = Value::String(String::new());
    row[4] = Value::Bytes(Vec::new());
    row[5] = Value::Uuid(
        uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")
            .expect("a fixed literal parses"),
    );
    row[6] = Value::Timestamp(
        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("a fixed date")
            .and_hms_opt(0, 0, 1)
            .expect("a fixed timestamp"),
    );
    row[7] = Value::TimestampTz(
        chrono::Utc
            .timestamp_opt(-62_135_596, 250_000_000)
            .single()
            .expect("a fixed instant"),
    );
    row[8] = Value::Date(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("a fixed date"));
    row[9] = Value::Time(chrono::NaiveTime::from_hms_opt(23, 59, 59).expect("a fixed time"));
    row[10] = Value::Decimal(
        BigDecimal::from_str("-12345678901234567890.123000").expect("a fixed decimal"),
    );
    row[11] = Value::Json(serde_json::Value::Null);
    row[12] = Value::Jsonb(serde_json::json!({ "x": true }));
    row
}

#[test]
fn rows_tier_round_trips_every_scalar_family() {
    let db = parse(WIDE_DDL);
    let wide = table_of(&db, "wide");
    let sql = "SELECT * FROM wide";
    let rows = vec![wide_row_a(), wide_row_b()];
    let engine = engine_of(db.clone());
    let detector = detector_of(&engine, sql, wide, rows.clone());

    let mut got = rows_of(detector.answer());
    got.sort_by_key(|(row, _)| match &row[0] {
        Value::Int(id) => *id,
        other => panic!("the key is an int, got {other:?}"),
    });
    assert_eq!(got, vec![(wide_row_a(), 1), (wide_row_b(), 1)]);

    let bytes = detector.checkpoint().expect("the checkpoint encodes");
    let restored = restored_of(&engine, sql, &bytes);
    assert_eq!(
        restored.answer(),
        detector.answer(),
        "restore answers what the detector answered"
    );

    let mut twin = detector_of(&engine, sql, wide, rows);
    let event = update(wide, wide_row_a(), {
        let mut edited = wide_row_a();
        edited[3] = Value::String("b".into());
        edited
    });
    let mut restored = restored;
    restored
        .apply_transaction(core::slice::from_ref(&event), &db)
        .expect("the update applies to the restored detector");
    twin.apply_transaction(&[event], &db)
        .expect("the update applies to the twin");
    assert_eq!(restored.answer(), twin.answer());
}

#[test]
fn rows_tier_restore_refuses_mismatched_state() {
    let db = parse(WIDE_DDL);
    let wide = table_of(&db, "wide");
    let engine = engine_of(db);
    let detector = detector_of(
        &engine,
        "SELECT * FROM wide",
        wide,
        vec![wide_row_a(), wide_row_b()],
    );
    let bytes = detector.checkpoint().expect("the checkpoint encodes");

    let wrong_plan = engine
        .restore_replay_detector(&SubscriptionRequest::new(1, "SELECT s FROM wide"), &bytes)
        .expect_err("another projection is another plan signature");
    assert!(
        matches!(wrong_plan, ReplayError::Restore(_)),
        "the refusal is a restore failure, got {wrong_plan:?}"
    );

    let mut trailing = bytes.clone();
    trailing.push(0xAB);
    let wrong_length = engine
        .restore_replay_detector(
            &SubscriptionRequest::new(1, "SELECT * FROM wide"),
            &trailing,
        )
        .expect_err("bytes past the state are not a state");
    assert!(
        matches!(wrong_length, ReplayError::Restore(_)),
        "the refusal is a restore failure, got {wrong_length:?}"
    );

    let garbage = engine
        .restore_replay_detector(
            &SubscriptionRequest::new(1, "SELECT * FROM wide"),
            &[0x01, 0x02, 0x03],
        )
        .expect_err("undecodable bytes are not a state");
    assert!(
        matches!(garbage, ReplayError::Restore(_)),
        "the refusal is a restore failure, got {garbage:?}"
    );

    let other_db = parse(WIDE_WRONG_TYPE_DDL);
    let other_engine = engine_of(other_db);
    let wrong_catalog = other_engine
        .restore_replay_detector(&SubscriptionRequest::new(1, "SELECT * FROM wide"), &bytes)
        .expect_err("a changed column type is a changed fingerprint");
    assert!(
        matches!(wrong_catalog, ReplayError::Restore(_)),
        "the refusal is a restore failure, got {wrong_catalog:?}"
    );
}

fn reading(
    id: i64,
    grp: i64,
    qty: Value<Postgres>,
    price: f64,
    note: &str,
    amount: &str,
) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::Int(grp),
        qty,
        Value::Float(price),
        Value::String(note.into()),
        Value::Decimal(BigDecimal::from_str(amount).expect("a fixed decimal")),
    ]
}

fn readings_seed() -> Vec<Vec<Value<Postgres>>> {
    vec![
        reading(1, 1, Value::Int(100), 1.5, "a", "0.30"),
        reading(2, 1, Value::Null, 2.5, "b", "0.10"),
        reading(3, 2, Value::Int(9_007_199_254_740_991), 3.5, "c", "0.001"),
    ]
}

fn reading_four() -> Vec<Value<Postgres>> {
    reading(4, 2, Value::Int(7), 4.5, "d", "0.50")
}

#[test]
fn scalar_fold_round_trips_count_and_exact_sums() {
    let db = parse(READINGS_DDL);
    let readings = table_of(&db, "readings");
    let engine = engine_of(db.clone());

    let mut count_star = detector_of(
        &engine,
        "SELECT COUNT(*) FROM readings",
        readings,
        readings_seed(),
    );
    assert_eq!(count_star_of(count_star.answer()), 3);
    count_star
        .apply_transaction(&[insert(readings, reading_four())], &db)
        .expect("the insert applies");
    let bytes = count_star.checkpoint().expect("the checkpoint encodes");
    let restored = restored_of(&engine, "SELECT COUNT(*) FROM readings", &bytes);
    assert_eq!(count_star_of(restored.answer()), 4);

    let mut count_col = detector_of(
        &engine,
        "SELECT COUNT(qty) FROM readings",
        readings,
        readings_seed(),
    );
    assert_eq!(count_column_of(count_col.answer()), 2);
    count_col
        .apply_transaction(&[insert(readings, reading_four())], &db)
        .expect("the insert applies");
    let bytes = count_col.checkpoint().expect("the checkpoint encodes");
    let restored = restored_of(&engine, "SELECT COUNT(qty) FROM readings", &bytes);
    assert_eq!(count_column_of(restored.answer()), 3);

    let mut sum_int = detector_of(
        &engine,
        "SELECT SUM(qty) FROM readings",
        readings,
        readings_seed(),
    );
    assert_eq!(sum_integer_of(sum_int.answer()), 9_007_199_254_741_091);
    sum_int
        .apply_transaction(&[insert(readings, reading_four())], &db)
        .expect("the insert applies");
    let bytes = sum_int.checkpoint().expect("the checkpoint encodes");
    let restored = restored_of(&engine, "SELECT SUM(qty) FROM readings", &bytes);
    assert_eq!(sum_integer_of(restored.answer()), 9_007_199_254_741_098);

    let mut sum_decimal = detector_of(
        &engine,
        "SELECT SUM(amount) FROM readings",
        readings,
        readings_seed(),
    );
    assert_eq!(
        sum_decimal_of(sum_decimal.answer()),
        BigDecimal::from_str("0.401").expect("a fixed decimal")
    );
    sum_decimal
        .apply_transaction(&[insert(readings, reading_four())], &db)
        .expect("the insert applies");
    let bytes = sum_decimal.checkpoint().expect("the checkpoint encodes");
    let restored = restored_of(&engine, "SELECT SUM(amount) FROM readings", &bytes);
    assert_eq!(
        sum_decimal_of(restored.answer()),
        BigDecimal::from_str("0.901").expect("a fixed decimal")
    );
}

#[test]
fn grouped_fold_round_trips_a_having_threshold() {
    let db = parse(READINGS_DDL);
    let readings = table_of(&db, "readings");
    let sql = "SELECT grp, SUM(qty) FROM readings GROUP BY grp HAVING COUNT(*) >= 2";
    let engine = engine_of(db.clone());
    let seed = vec![
        reading(1, 1, Value::Int(1), 0.0, "a", "0.0"),
        reading(2, 1, Value::Int(2), 0.0, "b", "0.0"),
        reading(3, 2, Value::Int(5), 0.0, "c", "0.0"),
    ];
    let mut detector = detector_of(&engine, sql, readings, seed);
    assert_eq!(grouped_fold_of(detector.answer()), vec![(1, 3)]);

    detector
        .apply_transaction(
            &[insert(
                readings,
                reading(4, 2, Value::Int(4), 0.0, "d", "0.0"),
            )],
            &db,
        )
        .expect("the insert applies");
    assert_eq!(
        grouped_fold_of(detector.answer()),
        vec![(1, 3), (2, 9)],
        "the entering group appears at its total"
    );

    let bytes = detector.checkpoint().expect("the checkpoint encodes");
    let mut restored = restored_of(&engine, sql, &bytes);
    assert_eq!(grouped_fold_of(restored.answer()), vec![(1, 3), (2, 9)]);
    restored
        .apply_transaction(
            &[delete(
                readings,
                reading(4, 2, Value::Int(4), 0.0, "d", "0.0"),
            )],
            &db,
        )
        .expect("the delete applies");
    assert_eq!(
        grouped_fold_of(restored.answer()),
        vec![(1, 3)],
        "the leaving group disappears again"
    );
}

#[test]
fn scalar_extreme_round_trips_min_and_max() {
    let db = parse(READINGS_DDL);
    let readings = table_of(&db, "readings");
    let engine = engine_of(db.clone());

    let seed = vec![
        reading(1, 1, Value::Null, 1.5, "a", "0.0"),
        reading(2, 1, Value::Null, 2.5, "b", "0.0"),
        reading(3, 1, Value::Null, 2.5, "c", "0.0"),
    ];
    let max = detector_of(&engine, "SELECT MAX(price) FROM readings", readings, seed);
    assert_eq!(extreme_of(max.answer()), Value::Float(2.5));
    let bytes = max.checkpoint().expect("the checkpoint encodes");
    let mut max = restored_of(&engine, "SELECT MAX(price) FROM readings", &bytes);
    assert_eq!(extreme_of(max.answer()), Value::Float(2.5));
    assert!(
        !max.apply_transaction(
            &[delete(
                readings,
                reading(2, 1, Value::Null, 2.5, "b", "0.0")
            )],
            &db,
        )
        .expect("the delete applies"),
        "a tied maximum reports no change"
    );
    assert!(
        max.apply_transaction(
            &[delete(
                readings,
                reading(3, 1, Value::Null, 2.5, "c", "0.0")
            )],
            &db,
        )
        .expect("the delete applies"),
        "the last maximum reports a change"
    );
    assert_eq!(extreme_of(max.answer()), Value::Float(1.5));

    let min = detector_of(
        &engine,
        "SELECT MIN(price) FROM readings",
        readings,
        vec![
            reading(1, 1, Value::Null, 1.5, "a", "0.0"),
            reading(2, 1, Value::Null, 2.5, "b", "0.0"),
        ],
    );
    assert_eq!(extreme_of(min.answer()), Value::Float(1.5));
    let bytes = min.checkpoint().expect("the checkpoint encodes");
    let mut min = restored_of(&engine, "SELECT MIN(price) FROM readings", &bytes);
    assert_eq!(extreme_of(min.answer()), Value::Float(1.5));
    min.apply_transaction(
        &[delete(
            readings,
            reading(1, 1, Value::Null, 1.5, "a", "0.0"),
        )],
        &db,
    )
    .expect("the delete applies");
    assert_eq!(extreme_of(min.answer()), Value::Float(2.5));
    min.apply_transaction(
        &[delete(
            readings,
            reading(2, 1, Value::Null, 2.5, "b", "0.0"),
        )],
        &db,
    )
    .expect("the delete applies");
    assert_eq!(
        extreme_of(min.answer()),
        Value::Null,
        "an empty extreme is NULL"
    );
}

#[test]
fn grouped_extreme_round_trips_a_having_threshold() {
    let db = parse(READINGS_DDL);
    let readings = table_of(&db, "readings");
    let sql = "SELECT grp, MAX(price) FROM readings GROUP BY grp HAVING COUNT(*) > 1";
    let engine = engine_of(db.clone());
    let seed = vec![
        reading(1, 1, Value::Null, 1.0, "a", "0.0"),
        reading(2, 1, Value::Null, 2.0, "b", "0.0"),
        reading(3, 2, Value::Null, 9.0, "c", "0.0"),
    ];
    let mut detector = detector_of(&engine, sql, readings, seed);
    assert_eq!(grouped_extreme_of(detector.answer()), vec![(1, 2.0)]);

    detector
        .apply_transaction(
            &[insert(
                readings,
                reading(4, 2, Value::Null, 10.0, "d", "0.0"),
            )],
            &db,
        )
        .expect("the insert applies");
    assert_eq!(
        grouped_extreme_of(detector.answer()),
        vec![(1, 2.0), (2, 10.0)]
    );

    let bytes = detector.checkpoint().expect("the checkpoint encodes");
    let mut restored = restored_of(&engine, sql, &bytes);
    assert_eq!(
        grouped_extreme_of(restored.answer()),
        vec![(1, 2.0), (2, 10.0)]
    );
    restored
        .apply_transaction(
            &[delete(
                readings,
                reading(4, 2, Value::Null, 10.0, "d", "0.0"),
            )],
            &db,
        )
        .expect("the delete applies");
    assert_eq!(grouped_extreme_of(restored.answer()), vec![(1, 2.0)]);
}

fn sum_is_nan(detector: &ReplayDetector<Postgres>) -> bool {
    matches!(
        detector.answer(),
        ReplayAnswer::Scalar(ReplayValue::Folded(AggValue::Sum(Some(NumericValue::Double(x)))))
            if x.is_nan()
    )
}

fn extreme_is_nan(detector: &ReplayDetector<Postgres>) -> bool {
    matches!(
        detector.answer(),
        ReplayAnswer::Scalar(ReplayValue::Scalar(Value::Float(x))) if x.is_nan()
    )
}

#[test]
fn nan_totals_report_no_change_for_irrelevant_and_noop_transactions() {
    let db = parse(NAN_DDL);
    let readings = table_of(&db, "readings");
    let unrelated = table_of(&db, "unrelated");
    let engine = engine_of(db.clone());
    let seed = vec![reading(1, 1, Value::Int(5), f64::NAN, "a", "1.00")];
    let noop = update(readings, seed[0].clone(), {
        let mut edited = seed[0].clone();
        edited[4] = Value::String("b".into());
        edited
    });
    let foreign = insert(unrelated, vec![Value::Int(9), Value::Int(7)]);

    let mut sum = detector_of(
        &engine,
        "SELECT SUM(price) FROM readings",
        readings,
        seed.clone(),
    );
    assert!(sum_is_nan(&sum), "a NaN price folds to a NaN sum");
    assert!(
        !sum.apply_transaction(core::slice::from_ref(&foreign), &db)
            .expect("a transaction on an unrelated table is ignored"),
        "the unrelated transaction changes nothing"
    );
    assert!(
        !sum.apply_transaction(core::slice::from_ref(&noop), &db)
            .expect("the price cell did not move"),
        "the projected no-op changes nothing"
    );
    assert!(sum_is_nan(&sum), "the sum is still NaN");

    let mut min = detector_of(&engine, "SELECT MIN(price) FROM readings", readings, seed);
    assert!(extreme_is_nan(&min), "a lone NaN is the MIN of the table");
    assert!(
        !min.apply_transaction(&[foreign], &db)
            .expect("a transaction on an unrelated table is ignored"),
        "the unrelated transaction changes nothing"
    );
    assert!(
        !min.apply_transaction(&[noop], &db)
            .expect("the price cell did not move"),
        "the projected no-op changes nothing"
    );
    assert!(extreme_is_nan(&min), "the extreme is still NaN");
}

#[cfg(feature = "membership-term")]
#[test]
fn an_unfolded_read_policy_refuses_replay_admission() {
    use rls2fga::classifier::function_registry::{SessionAttribute, SessionAttributeKind};
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga::types::ConfidenceLevel;
    // Unregistered `app.tenant_id` prevents the read rule from folding.
    const DDL: &str = "CREATE TABLE rls_items (id INT PRIMARY KEY, owner TEXT, price DOUBLE PRECISION);
        ALTER TABLE rls_items ENABLE ROW LEVEL SECURITY;
        CREATE POLICY rls_items_p ON rls_items FOR ALL USING (owner = current_setting('app.tenant_id', true));";

    let translator = TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .with_session_attributes([SessionAttribute::setting(
            "app.subjects",
            SessionAttributeKind::SetAttribute,
        )])
        .build();
    let rows = vec![vec![
        Value::Int(1),
        Value::String("one".into()),
        Value::Float(2.5),
    ]];

    let db = parse(DDL);
    let table = table_of(&db, "rls_items");
    let engine = engine_of(db).with_translator(translator);

    for sql in [
        "SELECT id FROM rls_items",
        "SELECT MAX(price) FROM rls_items",
    ] {
        let error = engine
            .replay_detector(
                &SubscriptionRequest::new(5, sql),
                vec![ReplayTable {
                    table_id: table,
                    rows: rows.clone(),
                }],
            )
            .expect_err("an unclassifiable read policy cannot admit replay");
        assert!(
            matches!(error, ReplayError::Unsupported(_)),
            "{sql} returned {error:?}"
        );
    }
}

#[test]
fn failed_transactions_and_capacity_keep_the_published_operator() {
    let db = parse(READINGS_DDL);
    let table = table_of(&db, "readings");
    let engine = engine_of(db.clone());
    let mut detector = detector_of(
        &engine,
        "SELECT COUNT(*) FROM readings",
        table,
        readings_seed(),
    );
    let before = detector.checkpoint().expect("the initial state encodes");
    let missing = reading(99, 1, Value::Int(1), 1.0, "missing", "1");
    let events = [insert(table, reading_four()), delete(table, missing)];
    assert!(matches!(
        detector.apply_transaction(&events, &db),
        Err(ReplayError::InputConflict(_))
    ));
    assert_eq!(count_star_of(detector.answer()), 3);
    assert_eq!(
        detector.checkpoint().expect("the rejected state encodes"),
        before
    );
    assert!(matches!(
        detector.apply_transaction_limited(&[insert(table, reading_four())], &db, before.len()),
        Err(ReplayError::Capacity(_))
    ));
    assert_eq!(count_star_of(detector.answer()), 3);
    assert_eq!(
        detector.checkpoint().expect("capacity preserves state"),
        before
    );
    assert!(detector
        .apply_transaction(&[insert(table, reading_four())], &db)
        .expect("the retry applies"));
    assert_eq!(count_star_of(detector.answer()), 4);
}

#[test]
fn duplicate_rows_without_a_primary_key_keep_multiplicity() {
    let db = parse("CREATE TABLE readings (qty INT);");
    let table = table_of(&db, "readings");
    let engine = engine_of(db.clone());
    let row = vec![Value::Int(7)];
    let mut detector = detector_of(
        &engine,
        "SELECT qty FROM readings",
        table,
        vec![row.clone(), row.clone()],
    );
    assert_eq!(rows_of(detector.answer()), vec![(row.clone(), 2)]);
    assert!(detector
        .apply_transaction(&[Event::delete(table, row.clone())], &db)
        .expect("one duplicate leaves"));
    assert_eq!(rows_of(detector.answer()), vec![(row.clone(), 1)]);
    let mut restored = restored_of(
        &engine,
        "SELECT qty FROM readings",
        &detector
            .checkpoint()
            .expect("the remaining duplicate encodes"),
    );
    assert!(restored
        .apply_transaction(&[Event::delete(table, row)], &db)
        .expect("the final duplicate leaves"));
    assert_eq!(rows_of(restored.answer()), []);
}

#[test]
fn partial_updates_verify_previous_cells_and_primary_key_changes() {
    let db = parse("CREATE TABLE readings (id INT PRIMARY KEY, qty INT);");
    let table = table_of(&db, "readings");
    let engine = engine_of(db.clone());
    let mut detector = detector_of(
        &engine,
        "SELECT SUM(qty) FROM readings",
        table,
        vec![
            vec![Value::Int(1), Value::Int(5)],
            vec![Value::Int(2), Value::Int(10)],
        ],
    );
    let contradictory = update(
        table,
        vec![Value::Int(1), Value::Int(99)],
        vec![Value::Int(1), Value::Int(6)],
    );
    assert!(matches!(
        detector.apply_transaction(&[contradictory], &db),
        Err(ReplayError::InputConflict(_))
    ));
    assert_eq!(sum_integer_of(detector.answer()), 15);
    let conflict = update(
        table,
        vec![Value::Int(1), Value::Missing],
        vec![Value::Int(2), Value::Missing],
    );
    assert!(matches!(
        detector.apply_transaction(&[conflict], &db),
        Err(ReplayError::InputConflict(_))
    ));
    let changed_key = update(
        table,
        vec![Value::Int(1), Value::Missing],
        vec![Value::Int(3), Value::Int(7)],
    );
    assert!(detector
        .apply_transaction(&[changed_key], &db)
        .expect("the old key locates the row"));
    assert_eq!(sum_integer_of(detector.answer()), 17);
    assert!(detector
        .apply_transaction(&[delete(table, vec![Value::Int(3), Value::Missing])], &db)
        .expect("the new key locates its hydrated row"));
    assert_eq!(sum_integer_of(detector.answer()), 10);
}

#[test]
fn restored_floating_totals_preserve_accumulation_history() {
    let db = parse("CREATE TABLE readings (id INT PRIMARY KEY, price DOUBLE PRECISION);");
    let table = table_of(&db, "readings");
    let engine = engine_of(db.clone());
    let row = |id, price| vec![Value::Int(id), Value::Float(price)];
    let sql = "SELECT SUM(price) FROM readings";
    let mut detector = detector_of(&engine, sql, table, vec![row(1, 1e16), row(2, 1.0)]);
    detector
        .apply_transaction(&[delete(table, row(1, 1e16))], &db)
        .expect("the large value leaves");
    let mut restored = restored_of(
        &engine,
        sql,
        &detector.checkpoint().expect("the historical total encodes"),
    );
    let value = |answer: &ReplayAnswer<Postgres>| match answer {
        ReplayAnswer::Scalar(ReplayValue::Folded(AggValue::Sum(Some(NumericValue::Double(
            value,
        ))))) => *value,
        other => panic!("expected a floating sum, got {other:?}"),
    };
    assert_eq!(
        value(detector.answer()).partial_cmp(&0.0),
        Some(core::cmp::Ordering::Equal)
    );
    assert_eq!(
        value(restored.answer()).partial_cmp(&0.0),
        Some(core::cmp::Ordering::Equal)
    );
    let change = insert(table, row(3, 2.0));
    detector
        .apply_transaction(core::slice::from_ref(&change), &db)
        .expect("the live total continues");
    restored
        .apply_transaction(&[change], &db)
        .expect("the restored total continues");
    assert_eq!(
        value(detector.answer()).partial_cmp(&2.0),
        Some(core::cmp::Ordering::Equal)
    );
    assert_eq!(
        value(restored.answer()).partial_cmp(&2.0),
        Some(core::cmp::Ordering::Equal)
    );
}

#[test]
fn zero_net_fold_contributions_preserve_floating_results() {
    let db = parse(READINGS_DDL);
    let table = table_of(&db, "readings");
    let engine = engine_of(db.clone());
    let first = reading(1, 1, Value::Int(1), 1e16, "a", "1");
    let mut edited = first.clone();
    edited[4] = Value::String("edited".into());
    let mut doubled = edited.clone();
    doubled[3] = Value::Float(2e16);
    let seed = vec![
        first.clone(),
        reading(2, 1, Value::Int(1), -1e16, "b", "1"),
        reading(3, 1, Value::Int(1), 1.0, "c", "1"),
    ];
    for sql in [
        "SELECT SUM(price) FROM readings",
        "SELECT grp, SUM(price) FROM readings GROUP BY grp HAVING AVG(price) > 0",
    ] {
        let mut detector = detector_of(&engine, sql, table, seed.clone());
        let before = detector.answer().clone();
        assert!(
            !detector
                .apply_transaction(&[update(table, first.clone(), edited.clone())], &db)
                .expect("an unrelated cell changes without touching the total"),
            "{sql}"
        );
        let compensating = [
            update(table, edited.clone(), doubled.clone()),
            update(table, doubled.clone(), edited.clone()),
        ];
        assert!(
            !detector
                .apply_transaction(&compensating, &db)
                .expect("one transaction restores its contribution"),
            "{sql}"
        );
        assert_eq!(detector.answer(), &before);
        let restored = restored_of(
            &engine,
            sql,
            &detector.checkpoint().expect("the cancelled fold encodes"),
        );
        assert_eq!(restored.answer(), &before);
    }
}

#[test]
fn grouped_having_preserves_exact_thresholds_across_restore() {
    const BEFORE: i64 = 9_007_199_254_740_992;
    const AFTER: i64 = BEFORE + 1;
    let db = parse("CREATE TABLE readings (id INT PRIMARY KEY, grp INT, qty BIGINT);");
    let table = table_of(&db, "readings");
    let engine = engine_of(db.clone());
    let row = |qty| vec![Value::Int(1), Value::Int(1), Value::Int(qty)];
    let total = |value: &ReplayValue<Postgres>| match value {
        ReplayValue::Folded(AggValue::Sum(Some(NumericValue::Decimal(sum)))) => {
            sum.to_string().parse().expect("the total fits an integer")
        }
        other => panic!("expected an exact decimal sum, got {other:?}"),
    };
    for threshold in [
        "9007199254740992",
        "9007199254740992.5",
        "9.0071992547409925e15",
    ] {
        let sql = format!(
            "SELECT grp, SUM(qty) FROM readings GROUP BY grp HAVING SUM(qty) > {threshold}"
        );
        let detector = detector_of(&engine, &sql, table, vec![row(BEFORE)]);
        assert_eq!(grouped_of(detector.answer(), total), []);
        let mut restored = restored_of(
            &engine,
            &sql,
            &detector.checkpoint().expect("the hidden group encodes"),
        );
        assert!(restored
            .apply_transaction(&[update(table, row(BEFORE), row(AFTER))], &db)
            .expect("the sum crosses the exact threshold"));
        assert_eq!(grouped_of(restored.answer(), total), [(1, AFTER)]);
        let mut continued = restored_of(
            &engine,
            &sql,
            &restored.checkpoint().expect("the visible group encodes"),
        );
        assert!(continued
            .apply_transaction(&[update(table, row(AFTER), row(BEFORE))], &db)
            .expect("the sum returns below the exact threshold"));
        assert_eq!(grouped_of(continued.answer(), total), []);
    }
}

#[test]
fn projected_json_changes_need_no_sql_equality_operator() {
    let db = parse(WIDE_DDL);
    let table = table_of(&db, "wide");
    let engine = engine_of(db.clone());
    let initial = wide_row_a();
    let mut changed = initial.clone();
    changed[11] = Value::Json(serde_json::json!({"different": true}));
    let mut detector = detector_of(&engine, "SELECT j FROM wide", table, vec![initial.clone()]);
    assert!(detector
        .apply_transaction(&[update(table, initial, changed.clone())], &db)
        .expect("a projected structured value changes"));
    assert_eq!(
        rows_of(detector.answer()),
        vec![(vec![changed[11].clone()], 1)]
    );
}

fn zero_bag(answer: &ReplayAnswer<Postgres>) -> Vec<(f64, u64)> {
    let mut out = Vec::new();
    for (row, multiplicity) in rows_of(answer) {
        let [Value::Float(value)] = row.as_slice() else {
            panic!("one projected float column, got {row:?}")
        };
        out.push((if *value == 0.0 { 0.0 } else { *value }, multiplicity));
    }
    out.sort_by_key(|(value, _)| value.to_bits());
    out
}

#[test]
fn projected_rows_keep_zero_multiplicity_across_the_sign() {
    let db = parse(READINGS_DDL);
    let readings = table_of(&db, "readings");
    let sql = "SELECT price FROM readings";
    let engine = engine_of(db.clone());
    let zero_a = reading(1, 1, Value::Int(1), 0.0, "a", "0.00");
    let zero_b = reading(2, 1, Value::Int(2), -0.0, "b", "0.00");
    let other = reading(3, 1, Value::Int(3), 1.5, "c", "0.00");
    let detector = detector_of(&engine, sql, readings, vec![zero_a, zero_b.clone(), other]);
    assert_eq!(
        zero_bag(detector.answer()),
        vec![(0.0, 2), (1.5, 1)],
        "the equal floats are one row held twice"
    );

    let mut restored = restored_of(
        &engine,
        sql,
        &detector.checkpoint().expect("the zero pair encodes"),
    );
    assert_eq!(
        zero_bag(restored.answer()),
        vec![(0.0, 2), (1.5, 1)],
        "restore answers the same logical bag"
    );

    let mut flipped = zero_b.clone();
    flipped[3] = Value::Float(0.0);
    let event = update(readings, zero_b, flipped);
    assert!(
        !restored
            .apply_transaction(&[event], &db)
            .expect("the rewrite applies"),
        "the sign of zero is not a value change"
    );
    assert_eq!(
        zero_bag(restored.answer()),
        vec![(0.0, 2), (1.5, 1)],
        "the bag keeps its multiplicity"
    );
}

#[test]
fn extreme_admission_refuses_a_locale_ordered_text_column() {
    const DEFAULT_DDL: &str = "CREATE TABLE labels (id INT PRIMARY KEY, label TEXT);";
    const ICU_DDL: &str = "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', \
                          deterministic = false);
                          CREATE TABLE labels (id INT PRIMARY KEY, label TEXT COLLATE ci);";
    for (ddl, sql) in [
        (DEFAULT_DDL, "SELECT MIN(label) FROM labels"),
        (DEFAULT_DDL, "SELECT MAX(label) FROM labels"),
        (ICU_DDL, "SELECT MIN(label) FROM labels"),
        (ICU_DDL, "SELECT MAX(label) FROM labels"),
    ] {
        let db = parse(ddl);
        let labels = table_of(&db, "labels");
        let engine = engine_of(db);
        let error = engine
            .replay_detector(
                &SubscriptionRequest::new(1, sql),
                vec![ReplayTable {
                    table_id: labels,
                    rows: vec![],
                }],
            )
            .expect_err(
                "the locale ordering is not reproducible, so replay cannot admit the extreme",
            );
        assert!(
            matches!(
                error,
                ReplayError::Unsupported(_) | ReplayError::InvalidState(_)
            ),
            "the statement is refused at admission, got {error:?}"
        );
    }
}

#[test]
fn byte_ordered_text_columns_admit_the_extreme() {
    const C_DDL: &str = "CREATE TABLE labels (id INT PRIMARY KEY, label TEXT COLLATE \"C\");";
    const POSIX_DDL: &str =
        "CREATE TABLE labels (id INT PRIMARY KEY, label TEXT COLLATE \"POSIX\");";
    for ddl in [C_DDL, POSIX_DDL] {
        let db = parse(ddl);
        let labels = table_of(&db, "labels");
        let engine = engine_of(db.clone());
        let rows = vec![
            vec![Value::Int(1), Value::String("B".into())],
            vec![Value::Int(2), Value::String("a".into())],
        ];
        let max = detector_of(
            &engine,
            "SELECT MAX(label) FROM labels",
            labels,
            rows.clone(),
        );
        assert_eq!(
            extreme_of(max.answer()),
            Value::String("a".into()),
            "byte order puts 'a' above 'B'"
        );
        let mut min = detector_of(
            &engine,
            "SELECT MIN(label) FROM labels",
            labels,
            rows.clone(),
        );
        assert_eq!(
            extreme_of(min.answer()),
            Value::String("B".into()),
            "byte order puts 'B' below 'a'"
        );
        min.apply_transaction(
            &[insert(
                labels,
                vec![Value::Int(3), Value::String("z".into())],
            )],
            &db,
        )
        .expect("the insert applies");
        assert_eq!(
            extreme_of(min.answer()),
            Value::String("B".into()),
            "the byte-larger 'z' does not displace the minimum"
        );
    }
}

/// One decoded checkpoint, with its wire fields addressable by name.
fn decoded_state(bytes: &[u8]) -> ciborium::Value {
    ciborium::from_reader(bytes).expect("the checkpoint decodes as CBOR")
}

fn reencoded_state(state: &ciborium::Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(state, &mut out).expect("the mutated state re-encodes");
    out
}

/// One field of a decoded `CBOR` map.
fn wire_field<'a>(state: &'a mut ciborium::Value, name: &str) -> &'a mut ciborium::Value {
    let ciborium::Value::Map(entries) = state else {
        panic!("the checkpoint decodes to a map, got {state:?}")
    };
    entries
        .iter_mut()
        .find(|(key, _)| matches!(key, ciborium::Value::Text(key) if key == name))
        .map_or_else(
            || panic!("the checkpoint carries a {name} field"),
            |(_, value)| value,
        )
}

#[test]
fn restore_refuses_a_result_state_from_another_tier() {
    let db = parse(READINGS_DDL);
    let readings = table_of(&db, "readings");
    let engine = engine_of(db);

    let rows_sql = "SELECT qty FROM readings";
    let rows_bytes = detector_of(&engine, rows_sql, readings, readings_seed())
        .checkpoint()
        .expect("the rows state encodes");
    let mut state = decoded_state(&rows_bytes);
    *wire_field(&mut state, "state") = ciborium::Value::Map(vec![(
        ciborium::Value::Text("ScalarExtreme".into()),
        ciborium::Value::Null,
    )]);
    let refused = engine
        .restore_replay_detector(
            &SubscriptionRequest::new(1, rows_sql),
            &reencoded_state(&state),
        )
        .expect_err("an extreme state is no rows state");
    assert!(
        matches!(refused, ReplayError::Restore(_)),
        "got {refused:?}"
    );

    let extreme_sql = "SELECT MAX(price) FROM readings";
    let extreme_bytes = detector_of(
        &engine,
        extreme_sql,
        readings,
        vec![
            reading(1, 1, Value::Null, 1.5, "a", "0.0"),
            reading(2, 1, Value::Null, 2.5, "b", "0.0"),
        ],
    )
    .checkpoint()
    .expect("the extreme state encodes");
    let mut state = decoded_state(&extreme_bytes);
    *wire_field(&mut state, "state") = ciborium::Value::Map(vec![(
        ciborium::Value::Text("Rows".into()),
        ciborium::Value::Array(Vec::new()),
    )]);
    let refused = engine
        .restore_replay_detector(
            &SubscriptionRequest::new(1, extreme_sql),
            &reencoded_state(&state),
        )
        .expect_err("a rows state is no extreme state");
    assert!(
        matches!(refused, ReplayError::Restore(_)),
        "got {refused:?}"
    );
}

#[test]
fn restore_refuses_fold_counters_the_inputs_do_not_carry() {
    let db = parse(READINGS_DDL);
    let readings = table_of(&db, "readings");
    let sql = "SELECT COUNT(*) FROM readings";
    let engine = engine_of(db);
    let bytes = detector_of(&engine, sql, readings, readings_seed())
        .checkpoint()
        .expect("the fold state encodes");

    for counter in [-1i64, 99] {
        let mut state = decoded_state(&bytes);
        *wire_field(wire_field(&mut state, "fold"), "rows_matched") =
            ciborium::Value::Integer(counter.into());
        let refused = engine
            .restore_replay_detector(&SubscriptionRequest::new(1, sql), &reencoded_state(&state))
            .expect_err("the counter cannot be restored");
        assert!(
            matches!(refused, ReplayError::Restore(_)),
            "got {refused:?}"
        );
    }
}

#[test]
fn restore_refuses_fold_groups_the_inputs_do_not_carry() {
    let db = parse(READINGS_DDL);
    let readings = table_of(&db, "readings");
    let sql = "SELECT grp, SUM(qty) FROM readings GROUP BY grp";
    let engine = engine_of(db);
    let bytes = detector_of(&engine, sql, readings, readings_seed())
        .checkpoint()
        .expect("the grouped state encodes");

    let mut state = decoded_state(&bytes);
    let groups = wire_field(&mut state, "groups");
    let ciborium::Value::Array(groups) = groups else {
        panic!("the groups decode to an array, got {groups:?}")
    };
    let group = groups.first_mut().expect("the seed carries fold groups");
    let ciborium::Value::Map(_) = group else {
        panic!("a group decodes to a map, got {group:?}")
    };
    let values = wire_field(group, "values");
    let ciborium::Value::Array(cells) = values else {
        panic!("the group values decode to an array, got {values:?}")
    };
    cells
        .first_mut()
        .expect("the group names one column")
        .clone_from(&ciborium::Value::Map(vec![(
            ciborium::Value::Text("Int".into()),
            ciborium::Value::Integer(9.into()),
        )]));
    let refused = engine
        .restore_replay_detector(&SubscriptionRequest::new(1, sql), &reencoded_state(&state))
        .expect_err("the values no longer encode their key");
    assert!(
        matches!(refused, ReplayError::Restore(_)),
        "got {refused:?}"
    );

    let mut state = decoded_state(&bytes);
    let groups = wire_field(&mut state, "groups");
    let ciborium::Value::Array(groups) = groups else {
        panic!("the groups decode to an array, got {groups:?}")
    };
    let group = groups.first_mut().expect("the seed carries fold groups");
    let ciborium::Value::Map(_) = group else {
        panic!("a group decodes to a map, got {group:?}")
    };
    *wire_field(group, "rows") = ciborium::Value::Integer(0.into());
    let refused = engine
        .restore_replay_detector(&SubscriptionRequest::new(1, sql), &reencoded_state(&state))
        .expect_err("the group holds no rows");
    assert!(
        matches!(refused, ReplayError::Restore(_)),
        "got {refused:?}"
    );
}

#[test]
fn truncate_discards_pending_deltas_and_replays_later_rows() {
    let db = parse(READINGS_DDL);
    let table = table_of(&db, "readings");
    let engine = engine_of(db.clone());
    let first = reading(1, 1, Value::Int(6), 6.0, "a", "6");
    let second = reading(2, 1, Value::Int(8), 8.0, "b", "8");
    let pending = reading(3, 1, Value::Int(100), 100.0, "c", "100");
    let retained = reading(4, 1, Value::Int(4), 4.0, "d", "4");
    for sql in [
        "SELECT qty FROM readings",
        "SELECT COUNT(*) FROM readings",
        "SELECT SUM(qty) FROM readings",
        "SELECT grp, SUM(qty) FROM readings GROUP BY grp HAVING SUM(qty) > 0",
        "SELECT MIN(price) FROM readings",
        "SELECT MAX(price) FROM readings",
        "SELECT grp, MIN(price) FROM readings GROUP BY grp",
        "SELECT grp, MAX(price) FROM readings GROUP BY grp",
    ] {
        let mut detector = detector_of(&engine, sql, table, vec![first.clone(), second.clone()]);
        let expected = detector_of(&engine, sql, table, vec![retained.clone()]);
        assert!(
            detector
                .apply_transaction(
                    &[
                        insert(table, pending.clone()),
                        Event::truncate(table),
                        insert(table, retained.clone()),
                    ],
                    &db
                )
                .expect("the transaction applies"),
            "{sql}"
        );
        assert_eq!(detector.answer(), expected.answer(), "{sql}");
        let restored = restored_of(
            &engine,
            sql,
            &detector.checkpoint().expect("the result encodes"),
        );
        assert_eq!(restored.answer(), expected.answer(), "{sql}");
    }
}

#[cfg(feature = "membership-term")]
#[test]
fn self_referential_membership_is_refused_before_replay() {
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga::types::ConfidenceLevel;
    let db = parse("CREATE TABLE members (id INT PRIMARY KEY, project_id INT, user_id TEXT);");
    let table = table_of(&db, "members");
    let engine = engine_of(db).with_translator(
        TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build(),
    );
    let request = SubscriptionRequest::new(1,
        "SELECT id FROM members WHERE project_id IN (SELECT project_id FROM members WHERE user_id = current_setting('app.user_id', true))")
        .subscriber(Value::String("alice".into()))
        .term_values(vec!["project_id"], vec![(Value::String("alice".into()), vec![Value::Int(1)])]);
    let error = engine
        .replay_detector(
            &request,
            vec![ReplayTable {
                table_id: table,
                rows: vec![],
            }],
        )
        .expect_err("self-referential membership is outside the admitted surface");
    assert!(matches!(error, ReplayError::InvalidState(_)));
}

#[test]
fn projected_rows_merge_equal_decimal_jsonb_and_nan_values() {
    let db = parse("CREATE TABLE cells (f DOUBLE PRECISION, n NUMERIC, j JSONB);");
    let table = table_of(&db, "cells");
    let engine = engine_of(db.clone());
    let first = vec![
        Value::Float(f64::NAN),
        Value::Decimal(BigDecimal::from_str("0.30").expect("decimal")),
        Value::Jsonb(serde_json::json!({"n": 1})),
    ];
    let second = vec![
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::Decimal(BigDecimal::from_str("0.3").expect("decimal")),
        Value::Jsonb(serde_json::json!({"n": 1.0})),
    ];
    let mut detector = detector_of(
        &engine,
        "SELECT * FROM cells",
        table,
        vec![first.clone(), second.clone()],
    );
    assert_eq!(
        detector.answer(),
        &ReplayAnswer::Rows(vec![(first.clone(), 2)])
    );
    assert!(!detector
        .apply_transaction(&[Event::update(table, second, first)], &db)
        .expect("the logical row stays equal"));
    let restored = restored_of(
        &engine,
        "SELECT * FROM cells",
        &detector.checkpoint().expect("the bag encodes"),
    );
    assert_eq!(restored.answer(), detector.answer());
}

#[test]
fn grouped_having_uses_each_real_aggregate_width() {
    const TENTH: f64 = 0.100_000_001_490_116_12;
    let db = parse("CREATE TABLE readings (id INT PRIMARY KEY, grp INT, price REAL);");
    let table = table_of(&db, "readings");
    let engine = engine_of(db.clone());
    let row = |id| vec![Value::Int(id), Value::Int(1), Value::Float(TENTH)];
    let count_sql =
        "SELECT grp, COUNT(price) FROM readings GROUP BY grp HAVING SUM(price) > 0.30000001";
    let count = detector_of(&engine, count_sql, table, vec![row(1), row(2)]);
    let mut count = restored_of(
        &engine,
        count_sql,
        &count.checkpoint().expect("the hidden count group encodes"),
    );
    assert!(count
        .apply_transaction(&[insert(table, row(3))], &db)
        .expect("the native real sum crosses the threshold"));
    assert_eq!(
        grouped_of(count.answer(), |value| match value {
            ReplayValue::Folded(AggValue::CountColumn(count)) => *count,
            other => panic!("expected a column count, got {other:?}"),
        }),
        [(1, 3)]
    );

    let sum_sql =
        "SELECT grp, SUM(price) FROM readings GROUP BY grp HAVING AVG(price) > 0.100000003";
    let sum = detector_of(&engine, sum_sql, table, vec![row(1), row(2)]);
    let mut sum = restored_of(
        &engine,
        sum_sql,
        &sum.checkpoint().expect("the hidden sum group encodes"),
    );
    assert!(!sum
        .apply_transaction(&[insert(table, row(3))], &db)
        .expect("the double precision mean stays below the threshold"));
    assert!(matches!(sum.answer(), ReplayAnswer::Grouped(groups) if groups.is_empty()));
}
