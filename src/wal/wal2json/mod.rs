//! [`CdcEvent`] for the `wal2json-events` message types.
//!
//! subql parses wal2json JSON with `wal2json_events::{parse_v2,
//! parse_v1}` and views the resulting [`MessageV2`] and [`ChangeV1`] as
//! [`CdcEvent`]s, resolving table and column names to catalog ordinals and
//! decoding each cell against the catalog on demand. This replaces the former
//! bespoke `Wal2JsonV{1,2}Parser` and `Wal2JsonV{1,2}Event`.
//!
//! v2 is read through [`Wal2JsonV2Reader`], which follows the transaction
//! boundaries to place each row at its [`PgCommitPosition`](crate::PgCommitPosition)
//! (with `include-lsn=true`). v1 batches a transaction and has no per-change
//! position, so it uses [`NoCheckpoint`](crate::NoCheckpoint).

mod decode_helpers;
mod parse_helpers;
mod v1;
mod v2;

pub use parse_helpers::parse_wal2json_v1;
pub use v2::{Wal2JsonV2Event, Wal2JsonV2Reader};

#[cfg(test)]
mod tests {
    use super::{parse_wal2json_v1, Wal2JsonV2Event, Wal2JsonV2Reader};
    use crate::backend::{CdcEvent, RowKind, Value};
    use crate::types::EventKind;
    use crate::wal::{TransactionOrderError, WalParseError};
    use crate::{PgCommitPosition, PgLsn};
    use alloc::vec::Vec;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn orders() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer INT, amount INT, status TEXT);",
        )
        .expect("parse DDL")
    }

    /// The row `bytes` carries, read as the first row of a transaction
    /// committing at `0/16B2300`.
    fn one_v2(bytes: &[u8]) -> Wal2JsonV2Event {
        let mut reader = Wal2JsonV2Reader::new();
        let begin = reader.parse(br#"{"action":"B","lsn":"0/16B2300"}"#);
        assert!(begin.expect("begin parses").is_none());
        reader
            .parse(bytes)
            .expect("parse succeeds")
            .expect("a row action carries an event")
    }

    /// Checkpoints of the row events `lines` carry, in order.
    fn checkpoints(lines: &[&[u8]]) -> Result<Vec<Option<PgCommitPosition>>, WalParseError> {
        let mut reader = Wal2JsonV2Reader::new();
        let mut out = Vec::new();
        for line in lines {
            if let Some(event) = reader.parse(line)? {
                out.push(event.checkpoint());
            }
        }
        Ok(out)
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
        assert_eq!(
            ev.checkpoint(),
            Some(PgCommitPosition::new(PgLsn(0x16B_2300), 1)),
            "the checkpoint is the commit position the begin named, not the row's own lsn"
        );
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

    /// Each row image keeps its own decoded cells, whichever is read first.
    #[test]
    fn a_resolved_update_keeps_old_and_new_cells_apart() {
        let db = orders();
        let ev = one_v2(
            br#"{"action":"U","schema":"public","table":"orders",
                 "columns":[{"name":"id","type":"integer","value":7},
                            {"name":"amount","type":"integer","value":250}],
                 "identity":[{"name":"id","type":"integer","value":7},
                             {"name":"amount","type":"integer","value":100}]}"#,
        );
        for first in [RowKind::Old, RowKind::New] {
            let resolved = crate::backend::ResolvedEvent::new(&ev, &db);
            let _ = resolved.value_at(&db, first, 2);
            assert_eq!(resolved.value_at(&db, RowKind::Old, 2), Ok(Value::Int(100)));
            assert_eq!(resolved.value_at(&db, RowKind::New, 2), Ok(Value::Int(250)));
        }
    }

    /// T2, a row at 1200 committing at 1300, arrives before T1, rows at 1000
    /// and 1100 committing at 1500.
    #[test]
    fn v2_rows_order_by_commit_then_place_in_transaction() {
        let placed = checkpoints(&[
            br#"{"action":"B","lsn":"0/514"}"#,
            br#"{"action":"I","schema":"public","table":"orders","lsn":"0/4B0","columns":[]}"#,
            br#"{"action":"C","lsn":"0/514"}"#,
            br#"{"action":"B","lsn":"0/5DC"}"#,
            br#"{"action":"M","transactional":true,"prefix":"p","content":"c"}"#,
            br#"{"action":"I","schema":"public","table":"orders","lsn":"0/3E8","columns":[]}"#,
            br#"{"action":"T","schema":"public","table":"orders","lsn":"0/44C"}"#,
            br#"{"action":"C","lsn":"0/5DC"}"#,
        ]);
        assert_eq!(
            placed.expect("frames in place"),
            vec![
                Some(PgCommitPosition::new(PgLsn(1300), 1)),
                Some(PgCommitPosition::new(PgLsn(1500), 1)),
                Some(PgCommitPosition::new(PgLsn(1500), 2)),
            ]
        );
    }

    /// Without `include-lsn` the begin names no commit, so the rows carry no
    /// position rather than a made-up one.
    #[test]
    fn v2_without_commit_positions_carries_none() {
        let placed = checkpoints(&[
            br#"{"action":"B"}"#,
            br#"{"action":"I","schema":"public","table":"orders","columns":[]}"#,
            br#"{"action":"C"}"#,
        ]);
        assert_eq!(placed.expect("frames in place"), vec![None]);
    }

    #[test]
    fn v2_boundaries_out_of_place_are_refused() {
        let row: &[u8] = br#"{"action":"I","schema":"public","table":"orders","columns":[]}"#;
        assert!(matches!(
            checkpoints(&[row]),
            Err(WalParseError::TransactionOrder(
                TransactionOrderError::RowOutsideTransaction
            ))
        ));
        assert!(matches!(
            checkpoints(&[br#"{"action":"C"}"#]),
            Err(WalParseError::TransactionOrder(
                TransactionOrderError::CommitOutsideTransaction
            ))
        ));
        assert!(matches!(
            checkpoints(&[br#"{"action":"B"}"#, br#"{"action":"B"}"#]),
            Err(WalParseError::TransactionOrder(
                TransactionOrderError::NestedBegin
            ))
        ));
        assert!(matches!(
            checkpoints(&[
                br#"{"action":"B","lsn":"0/5DC"}"#,
                br#"{"action":"C","lsn":"0/640"}"#
            ]),
            Err(WalParseError::TransactionOrder(
                TransactionOrderError::CommitMismatch { .. }
            ))
        ));
        assert!(matches!(
            checkpoints(&[br#"{"action":"B","lsn":"not-an-lsn"}"#]),
            Err(WalParseError::MalformedPayload(_))
        ));
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
