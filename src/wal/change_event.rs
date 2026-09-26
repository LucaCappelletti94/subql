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
use crate::{PgCommitPosition, PgLsn};

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
        EventType::Truncate { .. } => Some(EventKind::Truncate),
        _ => None,
    }
}

/// The commit a Postgres source yields after the last row of its transaction.
///
/// Acknowledging its [`position`](Self::position) moves the slot's
/// `confirmed_flush_lsn` to its [`end_lsn`](Self::end_lsn), and acknowledging
/// rows alone never moves it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PgCommit {
    position: PgCommitPosition,
    end_lsn: PgLsn,
}

impl PgCommit {
    /// The commit at `position` whose record ends at `end_lsn`.
    #[must_use]
    pub const fn new(position: PgCommitPosition, end_lsn: PgLsn) -> Self {
        Self { position, end_lsn }
    }

    /// Where the commit falls in commit order, after every row of its
    /// transaction.
    #[must_use]
    pub const fn position(&self) -> PgCommitPosition {
        self.position
    }

    /// Where the commit record ends, the flush position acknowledging this
    /// commit releases the slot to.
    #[must_use]
    pub const fn end_lsn(&self) -> PgLsn {
        self.end_lsn
    }
}

/// Delivered commits the slot still holds, and how far the consumer has
/// acknowledged.
///
/// The flush position moves only to the end of a transaction whose commit
/// the consumer acknowledged, since a flush past a transaction tells the
/// server the consumer holds all of it.
#[cfg(feature = "pg-streaming")]
pub struct ReleaseQueue {
    /// In commit order, so the releasable ones are a prefix.
    held: alloc::collections::VecDeque<PgCommit>,
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

    /// Hold `commit`, delivered and not yet acknowledged.
    pub fn committed(&mut self, commit: PgCommit) {
        self.held.push_back(commit);
    }

    /// Record that the consumer applied every event up to `upto`.
    pub fn acknowledge(&mut self, upto: PgCommitPosition) {
        self.acknowledged = self.acknowledged.max(Some(upto));
    }

    /// Let go of every held commit the consumer acknowledged, returning the
    /// flush position that releases them, or `None` when there is none.
    pub fn release(&mut self) -> Option<PgLsn> {
        let acknowledged = self.acknowledged?;
        let released = self
            .held
            .iter()
            .take_while(|commit| commit.position <= acknowledged)
            .count();
        self.held
            .drain(..released)
            .next_back()
            .map(|commit| commit.end_lsn)
    }
}

/// Reduces a decoded pgoutput stream to the row events subql's engine
/// consumes, each at its [`PgCommitPosition`], and the commits that end
/// them.
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
    /// report the commit when `change` ends a transaction that carried rows.
    pub fn apply(
        &mut self,
        change: ChangeEvent,
        rows: &mut impl Extend<PgChangeEvent>,
    ) -> Result<Option<PgCommit>, TransactionOrderError> {
        match &change.event_type {
            EventType::Insert { .. } | EventType::Update { .. } | EventType::Delete { .. } => {
                let (commit, ordinal) = self.order.next_row()?;
                rows.extend([PgChangeEvent::new(
                    change,
                    PgCommitPosition::new(commit, ordinal),
                )]);
            }
            EventType::Truncate {
                tables,
                cascade,
                restart_identity,
            } => {
                for table in tables {
                    let (commit, ordinal) = self.order.next_row()?;
                    let single = ChangeEvent {
                        event_type: EventType::Truncate {
                            tables: vec![Arc::clone(table)],
                            cascade: *cascade,
                            restart_identity: *restart_identity,
                        },
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
                return Ok((placed > 0).then(|| {
                    PgCommit::new(PgCommitPosition::at_commit(began), PgLsn(end_lsn.value()))
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
        EventType::Truncate { tables, .. } => {
            let full = tables.first()?.as_ref();
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
        use super::super::{PgChangeEvent, PgCommit, PgOutputOrder};
        use crate::wal::TransactionOrderError;
        use crate::{PgCommitPosition, PgLsn};
        use alloc::sync::Arc;
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
        fn a_commit_reports_its_position_after_its_rows_and_where_it_ends() {
            let mut order = PgOutputOrder::new();
            let mut rows = Vec::new();
            for change in [begin(1500), insert(1000), insert(1100)] {
                assert_eq!(order.apply(change, &mut rows), Ok(None));
            }
            let commit = order
                .apply(commit(1500, 1510), &mut rows)
                .expect("frames in place")
                .expect("a transaction with rows reports its commit");
            assert_eq!(
                commit,
                PgCommit::new(PgCommitPosition::at_commit(PgLsn(1500)), PgLsn(1510))
            );
            assert!(rows.iter().all(|row| row.position() < commit.position()));
        }

        #[test]
        fn a_transaction_without_rows_reports_no_commit() {
            let mut order = PgOutputOrder::new();
            let mut rows = Vec::new();
            order.apply(begin(1600), &mut rows).unwrap();
            assert_eq!(order.apply(commit(1600, 1610), &mut rows), Ok(None));
        }

        #[test]
        fn a_truncate_of_several_tables_takes_an_ordinal_per_table() {
            let mut order = PgOutputOrder::new();
            let mut rows = Vec::new();
            for change in [
                begin(1500),
                ChangeEvent::truncate(
                    vec!["public.orders".into(), "public.items".into()],
                    true,
                    false,
                    Lsn::new(1000),
                ),
                insert(1100),
            ] {
                order.apply(change, &mut rows).unwrap();
            }
            let tables: Vec<_> = rows
                .iter()
                .map(|row| match &row.change().event_type {
                    pg_walstream::EventType::Truncate {
                        tables,
                        cascade,
                        restart_identity,
                    } => (tables.clone(), *cascade, *restart_identity),
                    other => panic!("expected a truncate, got {other:?}"),
                })
                .take(2)
                .collect();
            let orders: Arc<str> = "public.orders".into();
            let items: Arc<str> = "public.items".into();
            assert_eq!(
                tables,
                vec![(vec![orders], true, false), (vec![items], true, false)],
                "each table keeps the statement's CASCADE and RESTART IDENTITY"
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
        use super::super::{PgCommit, ReleaseQueue};
        use crate::{PgCommitPosition, PgLsn};

        /// The commit of a transaction committing at `commit`, ending ten past it.
        fn commit(commit: u64) -> PgCommit {
            PgCommit::new(
                PgCommitPosition::at_commit(PgLsn(commit)),
                PgLsn(commit + 10),
            )
        }

        #[test]
        fn acknowledging_every_row_without_the_commit_releases_nothing() {
            let mut queue = ReleaseQueue::new();
            queue.committed(commit(1300));
            queue.acknowledge(PgCommitPosition::new(PgLsn(1300), 2));
            assert_eq!(queue.release(), None);
            queue.acknowledge(PgCommitPosition::at_commit(PgLsn(1300)));
            assert_eq!(queue.release(), Some(PgLsn(1310)));
        }

        #[test]
        fn a_commit_is_released_with_every_commit_before_it_and_none_after() {
            let mut queue = ReleaseQueue::new();
            queue.committed(commit(1300));
            queue.committed(commit(1500));
            queue.committed(commit(1700));
            queue.acknowledge(PgCommitPosition::new(PgLsn(1700), 1));
            assert_eq!(queue.release(), Some(PgLsn(1510)), "T1 and T2 are whole");
            assert_eq!(queue.release(), None, "T3's commit is unacknowledged");
            queue.acknowledge(PgCommitPosition::at_commit(PgLsn(1700)));
            assert_eq!(queue.release(), Some(PgLsn(1710)));
        }

        #[test]
        fn an_acknowledgement_ahead_of_the_commit_releases_it_when_it_arrives() {
            let mut queue = ReleaseQueue::new();
            queue.acknowledge(PgCommitPosition::at_commit(PgLsn(1500)));
            assert_eq!(queue.release(), None);
            queue.committed(commit(1500));
            assert_eq!(queue.release(), Some(PgLsn(1510)));
        }

        #[test]
        fn an_older_acknowledgement_never_takes_the_position_back() {
            let mut queue = ReleaseQueue::new();
            queue.acknowledge(PgCommitPosition::at_commit(PgLsn(1500)));
            queue.acknowledge(PgCommitPosition::at_commit(PgLsn(1300)));
            queue.committed(commit(1500));
            assert_eq!(queue.release(), Some(PgLsn(1510)));
        }

        #[test]
        fn nothing_is_released_before_any_acknowledgement() {
            let mut queue = ReleaseQueue::new();
            queue.committed(commit(1300));
            assert_eq!(queue.release(), None);
        }
    }
}
