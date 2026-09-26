//! [`CdcEvent`] for pgoutput row changes.
//!
//! subql's Postgres CDC sources decode the wire with
//! [`pg_walstream::PgOutputDecoder`] into the ecosystem [`ChangeEvent`] and
//! yield each row change as a [`PgChangeEvent`], the change together with its
//! [`PgCommitPosition`]. This module is the subql-side view over it, which
//! resolves the change's table and column names to catalog ordinals and
//! decodes each cell against the catalog scalar kind on demand.
//!
//! [`ChangeEvent`] carries more than the row events subql dispatches
//! (transaction boundaries, relation definitions, streaming markers). The
//! sources reduce a raw stream to row events with [`PgOutputOrder`], which
//! reads the transaction frames to place each row, before handing anything
//! to the engine, so [`CdcEvent::kind`] is only ever called on an Insert,
//! Update, Delete, or Truncate.

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
use alloc::sync::Arc;
use alloc::vec::Vec;

use pg_walstream::{ChangeEvent, ColumnValue, EventType, RowData};
use sql_traits::prelude::DatabaseLike;

use super::pg_type::text_to_pg_value_by_kind;
use super::resolve_table;
#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
use super::transaction_order::{TransactionOrder, TransactionOrderError};
use crate::backend::{Postgres, RowKind, Value};
use crate::catalog_helpers;
use crate::types::{ColumnId, EventKind, TableId};
use crate::wal::wire_event::{wire_cdc_event, WireEvent};
use crate::PgCommitPosition;
#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
use crate::PgLsn;

/// A pgoutput row change and where it falls in commit order.
///
/// The change's own `lsn` is the position of its WAL record, which does not
/// follow commit order when transactions interleave. The checkpoint this
/// event carries is its [`PgCommitPosition`].
#[derive(Clone, Debug)]
pub struct PgChangeEvent {
    change: ChangeEvent,
    position: PgCommitPosition,
}

impl PgChangeEvent {
    /// The row change `change` at `position`.
    ///
    /// `change` is an Insert, Update, Delete, or single-table Truncate. Any
    /// other variant is not a row event, and reading its kind panics.
    #[must_use]
    pub const fn new(change: ChangeEvent, position: PgCommitPosition) -> Self {
        Self { change, position }
    }

    /// Where the change falls in commit order.
    #[must_use]
    pub const fn position(&self) -> PgCommitPosition {
        self.position
    }

    /// The decoded change.
    #[must_use]
    pub const fn change(&self) -> &ChangeEvent {
        &self.change
    }

    /// The decoded change, without its position.
    #[must_use]
    pub fn into_change(self) -> ChangeEvent {
        self.change
    }
}

/// The DML or truncate kind of `event`, or `None` for the non-row events
/// (`Begin`, `Commit`, `Relation`, streaming and two-phase markers) that a
/// pg_walstream stream also carries.
const fn dml_kind(event: &ChangeEvent) -> Option<EventKind> {
    match event.event_type {
        EventType::Insert { .. } => Some(EventKind::Insert),
        EventType::Update { .. } => Some(EventKind::Update),
        EventType::Delete { .. } => Some(EventKind::Delete),
        EventType::Truncate(_) => Some(EventKind::Truncate),
        _ => None,
    }
}

/// A transaction a pgoutput stream committed.
#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedTransaction {
    /// The position of its last row event, or [`PgCommitPosition::before_commit`]
    /// of its commit when it carried none.
    pub last: PgCommitPosition,
    /// Where its commit record ends, the flush position that releases it.
    pub end_lsn: PgLsn,
}

/// Delivered transactions the slot still holds, and how far the consumer has
/// acknowledged.
///
/// The flush position moves only to the end of a transaction whose every row
/// the consumer acknowledged, since a flush past a transaction's rows tells
/// the server they were received.
#[cfg(feature = "pg-streaming")]
pub struct ReleaseQueue {
    /// In commit order, so the releasable ones are a prefix.
    held: alloc::collections::VecDeque<CommittedTransaction>,
    acknowledged: Option<PgCommitPosition>,
}

#[cfg(feature = "pg-streaming")]
impl ReleaseQueue {
    pub const fn new() -> Self {
        Self {
            held: alloc::collections::VecDeque::new(),
            acknowledged: None,
        }
    }

    /// Hold `transaction`, which delivered rows the consumer has not yet
    /// acknowledged.
    pub fn committed(&mut self, transaction: CommittedTransaction) {
        self.held.push_back(transaction);
    }

    /// Record that the consumer applied every event up to `upto`.
    pub fn acknowledge(&mut self, upto: PgCommitPosition) {
        self.acknowledged = self.acknowledged.max(Some(upto));
    }

    /// Let go of every held transaction acknowledged in full, returning the
    /// flush position that releases them, or `None` when there is none.
    pub fn release(&mut self) -> Option<PgLsn> {
        let acknowledged = self.acknowledged?;
        let released = self
            .held
            .iter()
            .take_while(|transaction| transaction.last <= acknowledged)
            .count();
        self.held
            .drain(..released)
            .next_back()
            .map(|transaction| transaction.end_lsn)
    }
}

/// Reduces a decoded pgoutput stream to the row events subql's engine
/// consumes, each at its [`PgCommitPosition`].
///
/// Insert, Update, and Delete become one event each. A `Truncate` naming
/// several tables fans out into one single-table `Truncate` per table, since
/// a subql event is always about one table, and each takes its own ordinal.
/// Relation, type, origin and logical messages yield nothing. Streaming and
/// two-phase messages are refused, since the sources decode protocol 1
/// without either.
#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
pub struct PgOutputOrder {
    order: TransactionOrder<PgLsn>,
}

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
impl PgOutputOrder {
    pub const fn new() -> Self {
        Self {
            order: TransactionOrder::new(),
        }
    }

    /// Place `change`, appending the row events it carries to `rows`, and
    /// report the transaction when `change` is its commit.
    pub fn apply(
        &mut self,
        change: ChangeEvent,
        rows: &mut impl Extend<PgChangeEvent>,
    ) -> Result<Option<CommittedTransaction>, TransactionOrderError> {
        match &change.event_type {
            EventType::Insert { .. } | EventType::Update { .. } | EventType::Delete { .. } => {
                let (commit, ordinal) = self.order.next_row()?;
                rows.extend([PgChangeEvent::new(
                    change,
                    PgCommitPosition::new(commit, ordinal),
                )]);
            }
            EventType::Truncate(names) => {
                for name in names {
                    let (commit, ordinal) = self.order.next_row()?;
                    let single = ChangeEvent {
                        event_type: EventType::Truncate(vec![Arc::clone(name)]),
                        lsn: change.lsn,
                        metadata: change.metadata.clone(),
                    };
                    rows.extend([PgChangeEvent::new(
                        single,
                        PgCommitPosition::new(commit, ordinal),
                    )]);
                }
            }
            EventType::Begin { final_lsn, .. } => {
                self.order.begin(PgLsn(final_lsn.value()))?;
            }
            EventType::Commit {
                commit_lsn,
                end_lsn,
                ..
            } => {
                let (began, placed) = self.order.commit()?;
                let committed = PgLsn(commit_lsn.value());
                if committed != began {
                    return Err(TransactionOrderError::CommitMismatch { began, committed });
                }
                return Ok(Some(CommittedTransaction {
                    last: PgCommitPosition::new(began, placed),
                    end_lsn: PgLsn(end_lsn.value()),
                }));
            }
            EventType::Relation { .. }
            | EventType::Type { .. }
            | EventType::Origin { .. }
            | EventType::Message { .. } => {}
            EventType::StreamStart { .. }
            | EventType::StreamStop
            | EventType::StreamCommit { .. }
            | EventType::StreamAbort { .. }
            | EventType::BeginPrepare { .. }
            | EventType::Prepare { .. }
            | EventType::CommitPrepared { .. }
            | EventType::RollbackPrepared { .. }
            | EventType::StreamPrepare { .. } => {
                return Err(TransactionOrderError::UnexpectedMessage(
                    change.event_type_str().into(),
                ));
            }
        }
        Ok(None)
    }
}

/// Resolve the observed table for `event` to a subql [`TableId`], or
/// `None` when the event carries no table (a non-row event) or the name is
/// not in `db`.
fn event_table_id<DB: DatabaseLike>(event: &ChangeEvent, db: &DB) -> Option<TableId> {
    match &event.event_type {
        EventType::Insert { schema, table, .. }
        | EventType::Update { schema, table, .. }
        | EventType::Delete { schema, table, .. } => {
            resolve_table::<crate::backend::Postgres, DB>(schema, table, db).ok()
        }
        EventType::Truncate(names) => {
            let full = names.first()?.as_ref();
            let (schema, table) = full.rsplit_once('.').unwrap_or(("", full));
            resolve_table::<crate::backend::Postgres, DB>(schema, table, db).ok()
        }
        _ => None,
    }
}

/// The row image `row` selects for `event`, or `None` when the event does
/// not carry that image. Matches the pgoutput access rules: Insert exposes
/// its row as New and Pk, Delete its old row as Old and Pk, and Update its
/// new row as New and its old row as Old and Pk.
const fn image_for(event: &ChangeEvent, row: RowKind) -> Option<&RowData> {
    match (&event.event_type, row) {
        (EventType::Insert { data, .. }, RowKind::New | RowKind::Pk) => Some(data),
        (EventType::Delete { old_data, .. }, RowKind::Old | RowKind::Pk) => Some(old_data),
        (EventType::Update { new_data, .. }, RowKind::New) => Some(new_data),
        (EventType::Update { old_data, .. }, RowKind::Old | RowKind::Pk) => old_data.as_ref(),
        _ => None,
    }
}

wire_cdc_event!(PgChangeEvent, Postgres, PgCommitPosition);

impl WireEvent for PgChangeEvent {
    type Backend = Postgres;
    type Checkpoint = PgCommitPosition;

    fn wire_kind(&self) -> EventKind {
        dml_kind(&self.change).expect(
            "CdcEvent::kind called on a non-row PgChangeEvent. Build one from an Insert, Update, Delete, or single-table Truncate",
        )
    }

    fn wire_table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId {
        // Infallible in the trait, so an unresolved name yields the
        // `u32` sentinel, which the engine reports as an unknown table.
        event_table_id(&self.change, db).unwrap_or(TableId::MAX)
    }

    fn wire_checkpoint(&self) -> Option<Self::Checkpoint> {
        Some(self.position)
    }

    fn wire_pk_columns<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        match &self.change.event_type {
            EventType::Insert { .. } | EventType::Update { .. } | EventType::Delete { .. } => {
                catalog_helpers::primary_key_columns(db, table_id).unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    fn wire_changed_columns<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        let EventType::Update {
            old_data: Some(old),
            new_data,
            ..
        } = &self.change.event_type
        else {
            return Vec::new();
        };
        let Ok(arity) = catalog_helpers::table_arity(db, table_id) else {
            return Vec::new();
        };
        if old.len() != arity || new_data.len() != arity {
            return Vec::new();
        }
        super::changed_columns_by_name(db, table_id, arity, |name| {
            (old.get(name), new_data.get(name))
        })
    }

    fn wire_value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Postgres>, crate::ValueError> {
        if row == RowKind::Pk && !WireEvent::wire_pk_columns(self, db, table_id).contains(&col) {
            return Ok(Value::Missing);
        }
        let Some(image) = image_for(&self.change, row) else {
            return Ok(Value::Missing);
        };
        let Some(name) = catalog_helpers::column_name(db, table_id, col) else {
            return Ok(Value::Missing);
        };
        match image.get(&name) {
            // A column the wire did not carry, and binary-format cells
            // (which do not appear on subql's text-mode proto v1 streams),
            // are both Missing here: they escalate to re-execution rather
            // than surfacing as a decode error.
            None | Some(ColumnValue::Binary(_)) => Ok(Value::Missing),
            Some(ColumnValue::Null) => Ok(Value::Null),
            Some(ColumnValue::Text(bytes)) => {
                catalog_helpers::column_scalar_kind::<Postgres, DB>(db, table_id, col).map_or(
                    Ok(Value::Missing),
                    |kind| {
                        // Non-UTF-8 bytes fail before any kind is consulted,
                        // so they are reported against the kind the column
                        // declares, custom or not.
                        let Ok(text) = core::str::from_utf8(bytes) else {
                            return Err(kind.family().map_or_else(
                                || crate::ValueError::Custom {
                                    column: col,
                                    custom: alloc::format!("{kind:?}"),
                                },
                                |builtin| crate::ValueError::Builtin {
                                    column: col,
                                    kind: builtin,
                                },
                            ));
                        };
                        crate::backend::decode_cell(col, kind, |builtin| {
                            text_to_pg_value_by_kind(text, builtin)
                        })
                    },
                )
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::CdcEvent;
    use crate::PgLsn;
    use pg_walstream::{Lsn, ReplicaIdentity};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn orders() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, customer INT, amount INT, status TEXT);",
        )
        .expect("parse DDL")
    }

    fn row(pairs: Vec<(&str, ColumnValue)>) -> RowData {
        RowData::from_pairs(pairs)
    }

    /// `change` as the first row of a transaction committing at `0x10`.
    fn at(change: ChangeEvent) -> PgChangeEvent {
        PgChangeEvent::new(change, PgCommitPosition::new(PgLsn(0x10), 1))
    }

    #[test]
    fn insert_exposes_pk_and_typed_cells() {
        let db = orders();
        let ev = at(ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                data: row(vec![
                    ("id", ColumnValue::text("7")),
                    ("customer", ColumnValue::text("3")),
                    ("amount", ColumnValue::text("250")),
                    ("status", ColumnValue::text("paid")),
                ]),
            },
            lsn: Lsn::new(0x10),
            metadata: None,
        });
        assert_eq!(ev.kind(), EventKind::Insert);
        assert_eq!(ev.pk_columns(&db), vec![0u16]);
        assert!(
            ev.changed_columns(&db).is_empty(),
            "insert has no old image so no changed columns arise"
        );
        assert_eq!(
            ev.checkpoint(),
            Some(PgCommitPosition::new(PgLsn(0x10), 1)),
            "the checkpoint is the commit position, whatever the record position"
        );
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Int(7));
        assert_eq!(
            ev.value_at(&db, RowKind::New, 3).unwrap(),
            Value::String("paid".into())
        );
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
        // Non-PK column read through Pk is Missing.
        assert_eq!(ev.value_at(&db, RowKind::Pk, 2).unwrap(), Value::Missing);
        // Insert carries no old image.
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Missing);
    }

    #[test]
    fn update_full_identity_derives_changed_columns() {
        let db = orders();
        let ev = at(ChangeEvent {
            event_type: EventType::Update {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                old_data: Some(row(vec![
                    ("id", ColumnValue::text("7")),
                    ("customer", ColumnValue::text("3")),
                    ("amount", ColumnValue::text("100")),
                    ("status", ColumnValue::text("pending")),
                ])),
                new_data: row(vec![
                    ("id", ColumnValue::text("8")),
                    ("customer", ColumnValue::text("3")),
                    ("amount", ColumnValue::text("250")),
                    ("status", ColumnValue::text("paid")),
                ]),
                replica_identity: ReplicaIdentity::Full,
                key_columns: vec!["id".into()],
            },
            lsn: Lsn::new(0),
            metadata: None,
        });
        assert_eq!(ev.kind(), EventKind::Update);
        assert_eq!(ev.pk_columns(&db), vec![0u16]);
        let mut changed = ev.changed_columns(&db);
        changed.sort_unstable();
        assert_eq!(changed, vec![0u16, 2u16, 3u16]);
        assert_eq!(ev.value_at(&db, RowKind::Old, 2).unwrap(), Value::Int(100));
        assert_eq!(ev.value_at(&db, RowKind::New, 2).unwrap(), Value::Int(250));
        let resolved = crate::backend::ResolvedEvent::new(&ev, &db);
        assert_eq!(
            resolved.value_at_known_pk(&db, 0).expect("old primary key"),
            Value::Int(7)
        );
    }

    #[test]
    fn update_sparse_old_image_leaves_changed_empty() {
        let db = orders();
        let ev = at(ChangeEvent {
            event_type: EventType::Update {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                // REPLICA IDENTITY DEFAULT: old image carries the key only.
                old_data: Some(row(vec![("id", ColumnValue::text("7"))])),
                new_data: row(vec![
                    ("id", ColumnValue::text("7")),
                    ("customer", ColumnValue::text("3")),
                    ("amount", ColumnValue::text("250")),
                    ("status", ColumnValue::text("paid")),
                ]),
                replica_identity: ReplicaIdentity::Default,
                key_columns: vec!["id".into()],
            },
            lsn: Lsn::new(0),
            metadata: None,
        });
        assert!(
            ev.changed_columns(&db).is_empty(),
            "old image has only the pk so non-pk changes are indeterminate"
        );
        // Pre-update PK identifies the row through the old image.
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(7));
    }

    #[test]
    fn delete_pk_from_key_columns_and_old_image() {
        let db = orders();
        let ev = at(ChangeEvent {
            event_type: EventType::Delete {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                old_data: row(vec![("id", ColumnValue::text("9"))]),
                replica_identity: ReplicaIdentity::Default,
                key_columns: vec!["id".into()],
            },
            lsn: Lsn::new(0),
            metadata: None,
        });
        assert_eq!(ev.kind(), EventKind::Delete);
        assert_eq!(ev.pk_columns(&db), vec![0u16]);
        assert_eq!(ev.value_at(&db, RowKind::Old, 0).unwrap(), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::Pk, 0).unwrap(), Value::Int(9));
        assert_eq!(ev.value_at(&db, RowKind::New, 0).unwrap(), Value::Missing);
    }

    #[test]
    fn null_is_distinct_from_missing() {
        let db = orders();
        let ev = at(ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                data: row(vec![
                    ("id", ColumnValue::text("7")),
                    ("status", ColumnValue::Null),
                ]),
            },
            lsn: Lsn::new(0),
            metadata: None,
        });
        // Present-but-NULL decodes to Null.
        assert_eq!(ev.value_at(&db, RowKind::New, 3).unwrap(), Value::Null);
        // Column the wire did not carry decodes to Missing.
        assert_eq!(ev.value_at(&db, RowKind::New, 1).unwrap(), Value::Missing);
    }

    #[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
    mod order {
        use super::super::{CommittedTransaction, PgChangeEvent, PgOutputOrder};
        use crate::wal::TransactionOrderError;
        use crate::{PgCommitPosition, PgLsn};
        use bytes::Bytes;
        use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RowData};

        fn begin(commit: u64) -> ChangeEvent {
            ChangeEvent::begin(
                1,
                Lsn::new(commit),
                chrono::DateTime::UNIX_EPOCH,
                Lsn::new(0),
            )
        }

        fn commit(commit: u64, end: u64) -> ChangeEvent {
            ChangeEvent::commit(
                chrono::DateTime::UNIX_EPOCH,
                Lsn::new(end),
                Lsn::new(commit),
                Lsn::new(end),
            )
        }

        /// An insert whose WAL record sits at `record`.
        fn insert(record: u64) -> ChangeEvent {
            ChangeEvent::insert(
                "public",
                "orders",
                1,
                RowData::from_pairs(vec![("id", ColumnValue::text("1"))]),
                Lsn::new(record),
            )
        }

        fn positions(
            order: &mut PgOutputOrder,
            changes: Vec<ChangeEvent>,
        ) -> Vec<PgCommitPosition> {
            let mut rows = Vec::new();
            for change in changes {
                order.apply(change, &mut rows).expect("frames in place");
            }
            rows.iter().map(PgChangeEvent::position).collect()
        }

        /// T1 writes at 1000 and commits at 1500, and T2 writes at 1200 and
        /// commits at 1300, so T2 arrives first.
        #[test]
        fn an_older_transaction_committing_later_orders_later() {
            let mut order = PgOutputOrder::new();
            let placed = positions(
                &mut order,
                vec![
                    begin(1300),
                    insert(1200),
                    commit(1300, 1310),
                    begin(1500),
                    insert(1000),
                    insert(1100),
                    commit(1500, 1510),
                ],
            );
            assert_eq!(
                placed,
                vec![
                    PgCommitPosition::new(PgLsn(1300), 1),
                    PgCommitPosition::new(PgLsn(1500), 1),
                    PgCommitPosition::new(PgLsn(1500), 2),
                ]
            );
        }

        #[test]
        fn a_commit_reports_its_last_row_and_where_it_ends() {
            let mut order = PgOutputOrder::new();
            let mut rows = Vec::new();
            for change in [begin(1500), insert(1000), insert(1100)] {
                assert_eq!(order.apply(change, &mut rows), Ok(None));
            }
            assert_eq!(
                order.apply(commit(1500, 1510), &mut rows),
                Ok(Some(CommittedTransaction {
                    last: PgCommitPosition::new(PgLsn(1500), 2),
                    end_lsn: PgLsn(1510),
                }))
            );
            order.apply(begin(1600), &mut rows).unwrap();
            assert_eq!(
                order.apply(commit(1600, 1610), &mut rows),
                Ok(Some(CommittedTransaction {
                    last: PgCommitPosition::before_commit(PgLsn(1600)),
                    end_lsn: PgLsn(1610),
                })),
                "a transaction without rows reports the position before its commit"
            );
        }

        #[test]
        fn a_truncate_of_several_tables_takes_an_ordinal_per_table() {
            let mut order = PgOutputOrder::new();
            let mut rows = Vec::new();
            for change in [
                begin(1500),
                ChangeEvent::truncate(
                    vec!["public.orders".into(), "public.items".into()],
                    Lsn::new(1000),
                ),
                insert(1100),
            ] {
                order.apply(change, &mut rows).unwrap();
            }
            let tables: Vec<_> = rows
                .iter()
                .map(|row| match &row.change().event_type {
                    pg_walstream::EventType::Truncate(names) => names.clone(),
                    other => panic!("expected a truncate, got {other:?}"),
                })
                .take(2)
                .collect();
            assert_eq!(
                tables,
                vec![vec!["public.orders".into()], vec!["public.items".into()]]
            );
            let ordinals: Vec<u64> = rows.iter().map(|row| row.position().ordinal()).collect();
            assert_eq!(ordinals, vec![1, 2, 3]);
        }

        #[test]
        fn metadata_messages_carry_no_row_and_leave_the_frame_alone() {
            let mut order = PgOutputOrder::new();
            let origin = || ChangeEvent::origin(Lsn::new(1), "upstream", Lsn::new(0));
            let message =
                || ChangeEvent::message(0, Lsn::new(1), "prefix", Bytes::new(), Lsn::new(0));
            let placed = positions(
                &mut order,
                vec![
                    message(),
                    begin(1500),
                    origin(),
                    insert(1000),
                    message(),
                    commit(1500, 1510),
                    origin(),
                ],
            );
            assert_eq!(placed, vec![PgCommitPosition::new(PgLsn(1500), 1)]);
        }

        #[test]
        fn frames_out_of_place_are_refused() {
            let mut rows = Vec::new();
            assert_eq!(
                PgOutputOrder::new().apply(insert(1000), &mut rows),
                Err(TransactionOrderError::RowOutsideTransaction)
            );
            assert_eq!(
                PgOutputOrder::new().apply(commit(1500, 1510), &mut rows),
                Err(TransactionOrderError::CommitOutsideTransaction)
            );
            let mut nested = PgOutputOrder::new();
            nested.apply(begin(1500), &mut rows).unwrap();
            assert_eq!(
                nested.apply(begin(1600), &mut rows),
                Err(TransactionOrderError::NestedBegin)
            );
            let mut mismatched = PgOutputOrder::new();
            mismatched.apply(begin(1500), &mut rows).unwrap();
            assert_eq!(
                mismatched.apply(commit(1600, 1610), &mut rows),
                Err(TransactionOrderError::CommitMismatch {
                    began: PgLsn(1500),
                    committed: PgLsn(1600),
                })
            );
            assert!(rows.is_empty());
        }

        #[test]
        fn streaming_and_two_phase_messages_are_refused() {
            let stop = ChangeEvent {
                event_type: pg_walstream::EventType::StreamStop,
                lsn: Lsn::new(0),
                metadata: None,
            };
            let prepare = ChangeEvent::begin_prepare(
                1,
                Lsn::new(1500),
                Lsn::new(1510),
                chrono::DateTime::UNIX_EPOCH,
                "gid",
                Lsn::new(0),
            );
            for change in [stop, prepare] {
                let kind = change.event_type_str().to_owned();
                assert_eq!(
                    PgOutputOrder::new().apply(change, &mut Vec::new()),
                    Err(TransactionOrderError::UnexpectedMessage(kind))
                );
            }
        }
    }

    #[cfg(feature = "pg-streaming")]
    mod release {
        use super::super::{CommittedTransaction, ReleaseQueue};
        use crate::{PgCommitPosition, PgLsn};

        fn position(commit: u64, ordinal: u64) -> PgCommitPosition {
            PgCommitPosition::new(PgLsn(commit), ordinal)
        }

        /// A transaction committing at `commit` with `rows` rows, ending ten past it.
        fn transaction(commit: u64, rows: u64) -> CommittedTransaction {
            CommittedTransaction {
                last: position(commit, rows),
                end_lsn: PgLsn(commit + 10),
            }
        }

        #[test]
        fn a_transaction_is_released_only_once_every_row_is_acknowledged() {
            let mut queue = ReleaseQueue::new();
            queue.committed(transaction(1300, 1));
            queue.committed(transaction(1500, 2));
            queue.acknowledge(position(1500, 1));
            assert_eq!(queue.release(), Some(PgLsn(1310)), "only T2 is whole");
            assert_eq!(queue.release(), None, "T1 is still half acknowledged");
            queue.acknowledge(position(1500, 2));
            assert_eq!(queue.release(), Some(PgLsn(1510)));
        }

        #[test]
        fn an_acknowledgement_ahead_of_the_commit_releases_it_when_it_arrives() {
            let mut queue = ReleaseQueue::new();
            queue.acknowledge(position(1500, 2));
            assert_eq!(queue.release(), None);
            queue.committed(transaction(1500, 2));
            assert_eq!(queue.release(), Some(PgLsn(1510)));
        }

        #[test]
        fn an_older_acknowledgement_never_takes_the_position_back() {
            let mut queue = ReleaseQueue::new();
            queue.acknowledge(position(1500, 2));
            queue.acknowledge(position(1300, 1));
            queue.committed(transaction(1500, 2));
            assert_eq!(queue.release(), Some(PgLsn(1510)));
        }

        #[test]
        fn nothing_is_released_before_any_acknowledgement() {
            let mut queue = ReleaseQueue::new();
            queue.committed(transaction(1300, 1));
            assert_eq!(queue.release(), None);
        }
    }
}
