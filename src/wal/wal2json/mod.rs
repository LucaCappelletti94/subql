//! [`CdcEvent`] for the `wal2json-events` message types.
//!
//! subql parses wal2json JSON with `wal2json_events::{parse_v2,
//! parse_v1}` and views the resulting [`MessageV2`] and [`ChangeV1`] as
//! [`CdcEvent`]s, resolving table and column names to catalog ordinals and
//! decoding each cell against the catalog on demand. This replaces the former
//! bespoke `Wal2JsonV{1,2}Parser` and `Wal2JsonV{1,2}Event`.
//!
//! v2 carries the stream LSN (with `include-lsn=true`) and surfaces it as a
//! [`PgLsn`](crate::PgLsn) checkpoint. v1 batches a transaction and has no
//! per-change LSN, so it uses [`NoCheckpoint`](crate::NoCheckpoint).

mod decode_helpers;
mod parse_helpers;
mod v1;
mod v2;

pub use parse_helpers::{parse_wal2json_v1, parse_wal2json_v2};

#[cfg(test)]
mod tests {
    use super::{parse_wal2json_v1, parse_wal2json_v2};
    use crate::backend::{CdcEvent, RowKind, Value};
    use crate::types::EventKind;
    use crate::PgLsn;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use wal2json_events::MessageV2;

    fn orders() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer INT, amount INT, status TEXT);",
        )
        .expect("parse DDL")
    }

    fn one_v2(bytes: &[u8]) -> MessageV2 {
        let mut msgs = parse_wal2json_v2(bytes).expect("parse succeeds");
        assert_eq!(msgs.len(), 1);
        msgs.remove(0)
    }

    #[test]
    fn v2_insert_pk_and_cells_and_lsn() {
        let db = orders();
        let ev = one_v2(
            br#"{"action":"I","schema":"public","table":"orders","lsn":"0/16B2270",
                 "columns":[{"name":"id","type":"integer","value":7},
                            {"name":"amount","type":"integer","value":250},
                            {"name":"status","type":"text","value":"paid"}]}"#,
        );
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.pk_columns(&db), alloc::vec![0u16]);
        assert_eq!(ev.checkpoint(), PgLsn::parse("0/16B2270"));
        assert!(ev.checkpoint().is_some());
        assert_eq!(
            ev.value_at(&db, RowKind::New, 2).expect("amount present"),
            Value::Int(250)
        );
        assert_eq!(
            ev.value_at(&db, RowKind::New, 3).expect("status present"),
            Value::String("paid".into())
        );
        assert_eq!(
            ev.value_at(&db, RowKind::Pk, 0).expect("pk present"),
            Value::Int(7)
        );
        assert_eq!(
            ev.value_at(&db, RowKind::Pk, 2).expect("non-pk missing"),
            Value::Missing
        );
    }

    #[test]
    fn v2_full_identity_derives_changed_columns() {
        let db = orders();
        let ev = one_v2(
            br#"{"action":"U","schema":"public","table":"orders",
                 "columns":[{"name":"id","type":"integer","value":8},
                            {"name":"customer","type":"integer","value":3},
                            {"name":"amount","type":"integer","value":250},
                            {"name":"status","type":"text","value":"paid"}],
                 "identity":[{"name":"id","type":"integer","value":7},
                             {"name":"customer","type":"integer","value":3},
                             {"name":"amount","type":"integer","value":100},
                             {"name":"status","type":"text","value":"pending"}]}"#,
        );
        let _ = super::decode_helpers::take_index_hashes();
        let mut changed = ev.changed_columns(&db);
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![0u16, 2u16, 3u16]);
        assert_eq!(super::decode_helpers::take_index_hashes(), 16);
        assert_eq!(
            ev.value_at(&db, RowKind::Old, 2).expect("old amount"),
            Value::Int(100)
        );
        assert_eq!(
            ev.value_at(&db, RowKind::New, 2).expect("new amount"),
            Value::Int(250)
        );
        let resolved = crate::backend::ResolvedEvent::new(&ev, &db);
        assert_eq!(
            resolved.value_at_known_pk(&db, 0).expect("old primary key"),
            Value::Int(7)
        );
    }

    #[test]
    fn v2_boundary_messages_drop() {
        assert_eq!(
            parse_wal2json_v2(br#"{"action":"B"}"#).expect("begin parses"),
            []
        );
        assert_eq!(
            parse_wal2json_v2(br#"{"action":"C"}"#).expect("commit parses"),
            []
        );
    }

    #[test]
    fn v1_delete_reads_oldkeys() {
        let db = orders();
        let mut changes = parse_wal2json_v1(
            br#"{"change":[{"kind":"delete","schema":"public","table":"orders",
                 "oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[42]}}]}"#,
        )
        .expect("parse");
        assert_eq!(changes.len(), 1);
        let ev = changes.remove(0);
        assert_eq!(ev.kind(), EventKind::Delete);
        assert_eq!(ev.pk_columns(&db), alloc::vec![0u16]);
        assert_eq!(
            ev.value_at(&db, RowKind::Old, 0).expect("old key"),
            Value::Int(42)
        );
        assert_eq!(
            ev.value_at(&db, RowKind::Pk, 0).expect("pk value"),
            Value::Int(42)
        );
        assert_eq!(ev.checkpoint(), None);
    }

    #[test]
    fn v1_full_identity_derives_changed_columns() {
        let db = orders();
        let mut events = parse_wal2json_v1(
            br#"{"change":[{"kind":"update","schema":"public","table":"orders",
                 "columnnames":["id","customer","amount","status"],
                 "columntypes":["integer","integer","integer","text"],
                 "columnvalues":[8,3,250,"paid"],
                 "oldkeys":{"keynames":["id","customer","amount","status"],
                            "keytypes":["integer","integer","integer","text"],
                            "keyvalues":[7,3,100,"pending"]}}]}"#,
        )
        .expect("parse");
        let ev = events.remove(0);
        let _ = super::decode_helpers::take_index_hashes();
        let mut changed = ev.changed_columns(&db);
        changed.sort_unstable();
        assert_eq!(changed, alloc::vec![0u16, 2u16, 3u16]);
        assert_eq!(super::decode_helpers::take_index_hashes(), 16);
        let resolved = crate::backend::ResolvedEvent::new(&ev, &db);
        assert_eq!(
            resolved.value_at_known_pk(&db, 0).expect("old primary key"),
            Value::Int(7)
        );
    }

    #[test]
    fn v1_multi_change_transaction_splits() {
        let db = orders();
        let changes = parse_wal2json_v1(
            br#"{"change":[
                 {"kind":"insert","schema":"public","table":"orders",
                  "columnnames":["id","amount"],"columntypes":["integer","integer"],
                  "columnvalues":[7,250]},
                 {"kind":"delete","schema":"public","table":"orders",
                  "oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[9]}}]}"#,
        )
        .expect("parse");
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].kind(), EventKind::Insert);
        assert_eq!(
            changes[0]
                .value_at(&db, RowKind::New, 0)
                .expect("insert id"),
            Value::Int(7)
        );
        assert_eq!(changes[1].kind(), EventKind::Delete);
        assert_eq!(
            changes[1]
                .value_at(&db, RowKind::Old, 0)
                .expect("delete id"),
            Value::Int(9)
        );
    }
}
