#![allow(clippy::unwrap_used)]

use crate::common;
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-mysql"
))]
use subql::backend::Pg18;

use bigdecimal::BigDecimal;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sql_traits::traits::MySqlCollationPadding;
use std::str::FromStr;
use subql::backend::{
    Backend, BuiltinKind, GroupKeyCollation, GroupKeyCollationName, GroupKeyColumn, MySql,
    NoCustom, Postgres, SQLite, SqliteJson, Value,
};
#[cfg(feature = "executor-diesel-mysql")]
use subql::reexec::MysqlDieselConnector;
#[cfg(feature = "executor-diesel-postgres")]
use subql::reexec::PgDieselConnector;
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-mysql"
))]
use subql::reexec::{Connector, ReadQuery};

mod schema {
    diesel::table! {
        pg_groups (id) {
            id -> Integer,
            float_value -> Double,
            text_value -> Text,
            exact_text -> Text,
            jsonb_value -> Jsonb,
        }
    }

    diesel::table! {
        mysql_groups (id) {
            id -> Integer,
            padded -> Text,
            unpadded -> Text,
            decimal_value -> Numeric,
            double_value -> Double,
            single_value -> Float,
        }
    }

    diesel::table! {
        sq_groups (id) {
            id -> Integer,
            nocase_value -> Text,
            rtrim_value -> Text,
        }
    }
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

fn column(
    kind: subql::backend::BuiltinKind,
    collation: GroupKeyCollation,
) -> GroupKeyColumn<NoCustom> {
    GroupKeyColumn {
        kind: kind.into(),
        declared_type: String::from("test"),
        collation,
    }
}

fn named(
    name: &str,
    postgres_deterministic: Option<bool>,
    mysql_padding: Option<MySqlCollationPadding>,
) -> GroupKeyCollation {
    GroupKeyCollation::Named {
        name: GroupKeyCollationName {
            name: String::from(name),
            name_is_quoted: false,
            schema: None,
            schema_is_quoted: false,
        },
        postgres_deterministic,
        mysql_padding,
    }
}

#[derive(Insertable)]
#[diesel(table_name = schema::pg_groups)]
struct NewPgGroup {
    id: i32,
    float_value: f64,
    text_value: String,
    exact_text: String,
    jsonb_value: serde_json::Value,
}

fn pg_rows() -> [NewPgGroup; 12] {
    [
        NewPgGroup {
            id: 1,
            float_value: 0.0,
            text_value: String::from("A"),
            exact_text: String::from("A"),
            jsonb_value: serde_json::from_str(r#"{"a":1.0,"b":true}"#).unwrap(),
        },
        NewPgGroup {
            id: 2,
            float_value: -0.0,
            text_value: String::from("a"),
            exact_text: String::from("a"),
            jsonb_value: serde_json::from_str(r#"{"b":true,"a":1.00}"#).unwrap(),
        },
        NewPgGroup {
            id: 3,
            float_value: f64::NAN,
            text_value: String::from("B"),
            exact_text: String::from("B"),
            jsonb_value: serde_json::from_str(r#"{"a":2,"b":true}"#).unwrap(),
        },
        NewPgGroup {
            id: 4,
            float_value: f64::from_bits(0x7ff0_0000_0000_0001),
            text_value: String::from("b"),
            exact_text: String::from("b"),
            jsonb_value: serde_json::from_str(r#"{"b":true,"a":2.0}"#).unwrap(),
        },
        NewPgGroup {
            id: 5,
            float_value: 0.0,
            text_value: String::from("C"),
            exact_text: String::from("C"),
            jsonb_value: serde_json::from_str(r#"{"a":1e2,"big":1234567890123456789012345.0}"#)
                .unwrap(),
        },
        NewPgGroup {
            id: 6,
            float_value: -0.0,
            text_value: String::from("c"),
            exact_text: String::from("c"),
            jsonb_value: serde_json::from_str(r#"{"big":1234567890123456789012345.00,"a":100}"#)
                .unwrap(),
        },
        NewPgGroup {
            id: 7,
            float_value: 0.0,
            text_value: String::from("D"),
            exact_text: String::from("D"),
            jsonb_value: serde_json::from_str(r#"{"duplicate":1,"duplicate":2}"#).unwrap(),
        },
        NewPgGroup {
            id: 8,
            float_value: -0.0,
            text_value: String::from("d"),
            exact_text: String::from("d"),
            jsonb_value: serde_json::from_str(r#"{"duplicate":2}"#).unwrap(),
        },
        // Exponent spellings of one value. `1e-3` and `0.001` group together, and both
        // differ from the neighbouring pair below.
        NewPgGroup {
            id: 9,
            float_value: 0.0,
            text_value: String::from("E"),
            exact_text: String::from("E"),
            jsonb_value: serde_json::from_str(r#"{"e":1e-3,"f":1E2}"#).unwrap(),
        },
        NewPgGroup {
            id: 10,
            float_value: -0.0,
            text_value: String::from("e"),
            exact_text: String::from("e"),
            jsonb_value: serde_json::from_str(r#"{"f":100.000,"e":0.001}"#).unwrap(),
        },
        // Differ in the twenty-fifth significant digit, which an f64 cannot hold, so a
        // lossy path would group these together and PostgreSQL does not.
        NewPgGroup {
            id: 11,
            float_value: 0.0,
            text_value: String::from("F"),
            exact_text: String::from("F"),
            jsonb_value: serde_json::from_str(r#"{"n":1234567890123456789012345}"#).unwrap(),
        },
        NewPgGroup {
            id: 12,
            float_value: -0.0,
            text_value: String::from("f"),
            exact_text: String::from("f"),
            jsonb_value: serde_json::from_str(r#"{"n":1234567890123456789012346}"#).unwrap(),
        },
    ]
}

#[cfg(feature = "executor-diesel-postgres")]
#[test]
#[ignore = "requires Docker"]
fn postgres_keys_match_group_by_equality() {
    use schema::pg_groups::dsl as groups;

    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut connection = common::pg_connect(port);
    connection
        .batch_execute(
            "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
             CREATE TABLE pg_groups (
                 id INTEGER PRIMARY KEY,
                 float_value DOUBLE PRECISION NOT NULL,
                 text_value TEXT COLLATE ci NOT NULL,
                 exact_text TEXT COLLATE \"C\" NOT NULL,
                 jsonb_value JSONB NOT NULL
             );",
        )
        .unwrap();
    let rows = pg_rows();
    diesel::insert_into(groups::pg_groups)
        .values(&rows)
        .execute(&mut connection)
        .unwrap();

    let float_groups = groups::pg_groups
        .group_by(groups::float_value)
        .select((groups::float_value, diesel::dsl::count_star()))
        .load::<(f64, i64)>(&mut connection)
        .unwrap();
    assert_eq!(float_groups.len(), 2);
    let float_encoder = Postgres::<Pg18>::group_key_encoder(vec![column(
        BuiltinKind::Float,
        GroupKeyCollation::DatabaseDefault,
    )])
    .unwrap();
    assert_eq!(
        float_encoder.encode(&[Value::Float(0.0)]),
        float_encoder.encode(&[Value::Float(-0.0)])
    );
    assert_eq!(
        float_encoder.encode(&[Value::Float(f64::NAN)]),
        float_encoder.encode(&[Value::Float(f64::from_bits(0x7ff0_0000_0000_0001))])
    );

    let text_groups = groups::pg_groups
        .group_by(groups::text_value)
        .select((groups::text_value, diesel::dsl::count_star()))
        .load::<(String, i64)>(&mut connection)
        .unwrap();
    assert_eq!(text_groups.len(), 6);
    assert!(Postgres::<Pg18>::group_key_encoder(vec![column(
        BuiltinKind::String,
        named("ci", Some(false), None),
    )])
    .is_none());

    let exact_groups = groups::pg_groups
        .group_by(groups::exact_text)
        .select((groups::exact_text, diesel::dsl::count_star()))
        .load::<(String, i64)>(&mut connection)
        .unwrap();
    assert_eq!(exact_groups.len(), 12);
    assert!(Postgres::<Pg18>::group_key_encoder(vec![column(
        BuiltinKind::String,
        named("C", Some(true), None),
    )])
    .is_some());

    let jsonb_groups = groups::pg_groups
        .group_by(groups::jsonb_value)
        .select((groups::jsonb_value, diesel::dsl::count_star()))
        .load::<(serde_json::Value, i64)>(&mut connection)
        .unwrap();
    assert_eq!(jsonb_groups.len(), 7);
    let jsonb_encoder = Postgres::<Pg18>::group_key_encoder(vec![column(
        BuiltinKind::Jsonb,
        GroupKeyCollation::DatabaseDefault,
    )])
    .unwrap();
    assert_eq!(
        jsonb_encoder.encode(&[Value::Jsonb(rows[0].jsonb_value.clone())]),
        jsonb_encoder.encode(&[Value::Jsonb(rows[1].jsonb_value.clone())])
    );
    assert_eq!(
        jsonb_encoder.encode(&[Value::Jsonb(rows[4].jsonb_value.clone())]),
        jsonb_encoder.encode(&[Value::Jsonb(rows[5].jsonb_value.clone())])
    );
    assert_eq!(
        jsonb_encoder.encode(&[Value::Jsonb(rows[6].jsonb_value.clone())]),
        jsonb_encoder.encode(&[Value::Jsonb(rows[7].jsonb_value.clone())])
    );
    // Exponent spellings of one value fold together.
    assert_eq!(
        jsonb_encoder.encode(&[Value::Jsonb(rows[8].jsonb_value.clone())]),
        jsonb_encoder.encode(&[Value::Jsonb(rows[9].jsonb_value.clone())])
    );
    // Differing in the twenty-fifth significant digit, which an f64 cannot hold, so a
    // lossy path would fold these and PostgreSQL does not.
    assert_ne!(
        jsonb_encoder.encode(&[Value::Jsonb(rows[10].jsonb_value.clone())]),
        jsonb_encoder.encode(&[Value::Jsonb(rows[11].jsonb_value.clone())])
    );

    let connector = PgDieselConnector::new(common::pg_connect(port));
    let query = ReadQuery::owned(
        String::from("SELECT id FROM pg_groups WHERE jsonb_value = $1 ORDER BY id"),
        vec![Value::Jsonb(rows[0].jsonb_value.clone())],
    );
    let page = connector.read_page(&query, 4096, &()).unwrap();
    assert_eq!(page.value.rows.len(), 2);
}

#[derive(Insertable)]
#[diesel(table_name = schema::mysql_groups)]
struct NewMysqlGroup {
    id: i32,
    padded: String,
    unpadded: String,
    decimal_value: BigDecimal,
    double_value: f64,
    single_value: f32,
}

#[cfg(feature = "executor-diesel-mysql")]
#[test]
#[ignore = "requires Docker"]
fn mysql_keys_match_binary_collations_and_decimal_equality() {
    use schema::mysql_groups::dsl as groups;

    let container = common::mysql_8();
    let port = common::mysql_port(&container);
    let mut connection = common::mysql_connect(port);
    connection
        .batch_execute(
            "CREATE TABLE mysql_groups (
                 id INTEGER PRIMARY KEY,
                 padded VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                 unpadded VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
                 decimal_value DECIMAL(20, 4) NOT NULL,
                 double_value DOUBLE NOT NULL,
                 single_value FLOAT NOT NULL
             );",
        )
        .unwrap();
    let rows = [
        NewMysqlGroup {
            id: 1,
            padded: String::from("value"),
            unpadded: String::from("value"),
            decimal_value: BigDecimal::from_str("1.0").unwrap(),
            double_value: 0.0,
            single_value: 0.0,
        },
        NewMysqlGroup {
            id: 2,
            padded: String::from("value  "),
            unpadded: String::from("value  "),
            decimal_value: BigDecimal::from_str("1.00").unwrap(),
            double_value: -0.0,
            single_value: -0.0,
        },
    ];
    diesel::insert_into(groups::mysql_groups)
        .values(&rows)
        .execute(&mut connection)
        .unwrap();

    let padded_groups = groups::mysql_groups
        .group_by(groups::padded)
        .select((groups::padded, diesel::dsl::count_star()))
        .load::<(String, i64)>(&mut connection)
        .unwrap();
    assert_eq!(padded_groups.len(), 1);
    let padded_encoder = MySql::group_key_encoder(vec![column(
        BuiltinKind::String,
        named("utf8mb4_bin", None, Some(MySqlCollationPadding::PadSpace)),
    )])
    .unwrap();
    assert_eq!(
        padded_encoder.encode(&[Value::String(String::from("value"))]),
        padded_encoder.encode(&[Value::String(String::from("value  "))])
    );

    let unpadded_groups = groups::mysql_groups
        .group_by(groups::unpadded)
        .select((groups::unpadded, diesel::dsl::count_star()))
        .load::<(String, i64)>(&mut connection)
        .unwrap();
    assert_eq!(unpadded_groups.len(), 2);

    let decimal_groups = groups::mysql_groups
        .group_by(groups::decimal_value)
        .select((groups::decimal_value, diesel::dsl::count_star()))
        .load::<(BigDecimal, i64)>(&mut connection)
        .unwrap();
    assert_eq!(decimal_groups.len(), 1);

    let double_groups = groups::mysql_groups
        .group_by(groups::double_value)
        .select((groups::double_value, diesel::dsl::count_star()))
        .load::<(f64, i64)>(&mut connection)
        .unwrap();
    assert_eq!(double_groups.len(), 2);
    let single_groups = groups::mysql_groups
        .group_by(groups::single_value)
        .select((groups::single_value, diesel::dsl::count_star()))
        .load::<(f32, i64)>(&mut connection)
        .unwrap();
    assert_eq!(single_groups.len(), 2);
    assert!(MySql::group_key_encoder(vec![column(
        BuiltinKind::Float,
        GroupKeyCollation::DatabaseDefault,
    )])
    .is_none());

    let connector = MysqlDieselConnector::new(common::mysql_connect(port));
    let query = ReadQuery::owned(
        String::from("SELECT id FROM mysql_groups WHERE decimal_value = ? ORDER BY id"),
        vec![Value::Decimal(BigDecimal::from_str("1.00").unwrap())],
    );
    let page = connector.read_page(&query, 4096, &()).unwrap();
    assert_eq!(page.value.rows.len(), 2);
}

#[derive(Insertable)]
#[diesel(table_name = schema::sq_groups)]
struct NewSqliteGroup {
    id: i32,
    nocase_value: String,
    rtrim_value: String,
}

#[test]
fn sqlite_keys_match_builtin_collations_and_dynamic_numeric_equality() {
    use schema::sq_groups::dsl as groups;

    let mut connection = diesel::sqlite::SqliteConnection::establish(":memory:").unwrap();
    connection
        .batch_execute(
            "CREATE TABLE sq_groups (
                 id INTEGER PRIMARY KEY,
                 nocase_value TEXT COLLATE NOCASE NOT NULL,
                 rtrim_value TEXT COLLATE RTRIM NOT NULL,
                 json_value JSON
             );",
        )
        .unwrap();
    let rows = [
        NewSqliteGroup {
            id: 1,
            nocase_value: String::from("A"),
            rtrim_value: String::from("value"),
        },
        NewSqliteGroup {
            id: 2,
            nocase_value: String::from("a"),
            rtrim_value: String::from("value  "),
        },
        NewSqliteGroup {
            id: 3,
            nocase_value: String::from("A\0LEFT"),
            rtrim_value: String::from("value"),
        },
        NewSqliteGroup {
            id: 4,
            nocase_value: String::from("a\0left"),
            rtrim_value: String::from("value  "),
        },
    ];
    diesel::insert_into(groups::sq_groups)
        .values(&rows)
        .execute(&mut connection)
        .unwrap();

    let nocase_groups = groups::sq_groups
        .group_by(groups::nocase_value)
        .select((groups::nocase_value, diesel::dsl::count_star()))
        .load::<(String, i64)>(&mut connection)
        .unwrap();
    assert_eq!(nocase_groups.len(), 2);
    let nocase_encoder = SQLite::group_key_encoder(vec![column(
        BuiltinKind::String,
        named("NOCASE", None, None),
    )])
    .unwrap();
    assert_eq!(
        nocase_encoder.encode(&[Value::String(String::from("A"))]),
        nocase_encoder.encode(&[Value::String(String::from("a"))])
    );
    assert_eq!(
        nocase_encoder.encode(&[Value::String(String::from("A\0LEFT"))]),
        nocase_encoder.encode(&[Value::String(String::from("a\0left"))])
    );

    let rtrim_groups = groups::sq_groups
        .group_by(groups::rtrim_value)
        .select((groups::rtrim_value, diesel::dsl::count_star()))
        .load::<(String, i64)>(&mut connection)
        .unwrap();
    assert_eq!(rtrim_groups.len(), 1);

    // Diesel cannot assign two SQL storage types to one dynamic SQLite column.
    diesel::sql_query("INSERT INTO sq_groups (id, nocase_value, rtrim_value, json_value) VALUES (5, 'x', 'x', 1), (6, 'y', 'y', 1.0)")
        .execute(&mut connection)
        .unwrap();
    let groups = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM (SELECT json_value FROM sq_groups WHERE id >= 5 GROUP BY json_value)",
    )
    .get_result::<CountRow>(&mut connection)
    .unwrap();
    assert_eq!(groups.count, 1);
    let json_encoder = SQLite::group_key_encoder(vec![column(
        BuiltinKind::Json,
        GroupKeyCollation::DatabaseDefault,
    )])
    .unwrap();
    assert_eq!(
        json_encoder.encode(&[Value::Json(SqliteJson::integer(1))]),
        json_encoder.encode(&[Value::Json(SqliteJson::real(1.0))])
    );
}
