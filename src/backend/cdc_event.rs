//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

use crate::backend::{
    Backend, Checkpoint, ColumnId, CustomScalars, DatabaseLike, EventKind, RowKind, ScalarKindOf,
    TableId, Value,
};
use alloc::vec::Vec;

/// What a row image says about one cell.
///
/// `Missing` is the variant that carries weight: a cell the source did not
/// carry cannot be answered from the event, which is a different fact from
/// the cell carrying SQL `NULL`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CellPresence {
    /// The source's row image did not carry this cell.
    Missing,
    /// The cell carries SQL `NULL`.
    Null,
    /// The cell carries a value.
    Present,
    /// The source carried the cell but subql could not decode it to its
    /// declared type. The prefilter cannot index it, so every predicate
    /// depending on the column is selected as a candidate and the VM
    /// surfaces the decode error.
    Undecodable,
}

/// One CDC row event as seen by the engine.
///
/// An impl exposes the event's structure (kind, table, PK layout, changed
/// columns, checkpoint) plus [`value_at`](CdcEvent::value_at), which decodes
/// one cell to a typed [`Value`]. It takes a [`RowKind`] to select the row
/// view and a [`ColumnId`] to address the cell.
///
/// # Access patterns
///
/// * Insert: `new_row` populated for every column, `old_row` empty, `pk_row`
///   populated for PK columns only.
/// * Update: both `new_row` and `old_row` may be populated; `changed_columns`
///   names the columns whose cell changed. Sources that follow
///   `REPLICA IDENTITY DEFAULT` (Postgres) carry only PK columns in `old_row`.
/// * Delete: `old_row` populated (extent depends on the source's replica
///   identity), `new_row` empty, `pk_row` populated.
/// * Truncate: no row images. `pk_columns` is empty; `changed_columns` is
///   empty. Structural event only.
///
/// # PK access
///
/// [`value_at`](CdcEvent::value_at) called with `RowKind::Pk` and a `col`
/// that is not in [`pk_columns`](Self::pk_columns) returns
/// [`Value::Missing`]. Composite PKs are read by iterating `pk_columns()`
/// and calling `value_at` per column.
pub trait CdcEvent {
    /// The database this event observes.
    type Backend: Backend;
    /// The checkpoint type this event carries (LSN, binlog position, ...).
    type Checkpoint: Checkpoint;

    /// Which flavour of event this is.
    fn kind(&self) -> EventKind;

    /// Table the event belongs to.
    ///
    /// `db` is the catalog for the observed database. A raw ecosystem
    /// event knows only the table name, so resolving it to a subql
    /// [`TableId`] needs the catalog.
    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId;

    /// Checkpoint (position in the source stream) when the source carries one.
    ///
    /// Returned owned so an event can bridge a source-native position
    /// type (a `pg_walstream` LSN, say) to a subql
    /// [`Checkpoint`](Self::Checkpoint) on demand.
    fn checkpoint(&self) -> Option<Self::Checkpoint>;

    /// Column ids that make up the primary key, in PK declaration order.
    ///
    /// For a composite PK the returned length is greater than one, and
    /// ordering matches the schema. For a Truncate event the result is
    /// empty. The identity reflects the event's replica identity plus
    /// the catalog, not the catalog alone, so `db` resolves the wire
    /// key layout to subql column ordinals.
    ///
    /// Returned owned as a [`Vec`] because a raw ecosystem event stores
    /// no subql [`ColumnId`] ordinals to borrow. PK arity is small, so
    /// the per-call allocation is cheap. A fill-into-buffer variant can
    /// replace this later if profiling shows it matters.
    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId>;

    #[doc(hidden)]
    fn pk_columns_resolved<DB: DatabaseLike>(&self, db: &DB, table_id: TableId) -> Vec<ColumnId> {
        let _ = table_id;
        self.pk_columns(db)
    }

    #[doc(hidden)]
    fn with_pk_columns<DB: DatabaseLike, R>(
        &self,
        db: &DB,
        read: impl FnOnce(&[ColumnId]) -> R,
    ) -> R {
        let columns = self.pk_columns(db);
        read(&columns)
    }

    /// Column ids whose cells changed on an Update event.
    ///
    /// For non-Update events the result is empty. For Update events
    /// sources vary in whether they populate this: sources that only
    /// carry the changed columns (wal2json v2 with `add-tables`) list
    /// only those. Sources that carry the full row image list every
    /// column whose value differs. Consumers should treat the result as
    /// a hint for optimisation, not as an authoritative diff. `db`
    /// resolves wire names to subql column ordinals.
    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId>;

    #[doc(hidden)]
    fn changed_columns_resolved<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
    ) -> Vec<ColumnId> {
        let _ = table_id;
        self.changed_columns(db)
    }

    #[doc(hidden)]
    fn with_changed_columns<DB: DatabaseLike, R>(
        &self,
        db: &DB,
        read: impl FnOnce(&[ColumnId]) -> R,
    ) -> R {
        let columns = self.changed_columns(db);
        read(&columns)
    }

    /// Decode one cell to an owned [`Value<Self::Backend>`].
    ///
    /// `db` is the catalog for the observed database. Formats whose wire
    /// carries no column type metadata (Maxwell, and positional pgoutput
    /// tuples) resolve the column's type from `db` at decode time. Formats
    /// that already carry their own type state may ignore it.
    ///
    /// Returns `Ok(Value::Missing)` for cells the source did not carry,
    /// `Ok(Value::Null)` for SQL NULL, `Ok(_)` with the matching typed
    /// variant for a present value, and `Err` when the source carried a
    /// cell of a known type that cannot be decoded (for example an
    /// integer above `i64::MAX`).
    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError>;

    /// What the row image says about one cell.
    ///
    /// The distinction [`CellPresence::Missing`] draws is the load-bearing
    /// one: a cell the source did not carry, an unchanged TOAST value or a
    /// column outside the replica identity, is not a SQL `NULL`, and a
    /// predicate reading it cannot be answered from the event at all.
    ///
    /// No caller acts on this yet; the prefilter still classifies inline.
    fn presence_at<DB: DatabaseLike>(&self, db: &DB, row: RowKind, col: ColumnId) -> CellPresence {
        match self.value_at(db, row, col) {
            Ok(Value::Missing) => CellPresence::Missing,
            Ok(Value::Null) => CellPresence::Null,
            Ok(_) => CellPresence::Present,
            Err(_) => CellPresence::Undecodable,
        }
    }

    #[doc(hidden)]
    fn value_at_resolved<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError> {
        let _ = table_id;
        self.value_at(db, row, col)
    }

    #[doc(hidden)]
    fn value_at_known_pk<DB: DatabaseLike>(
        &self,
        db: &DB,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError> {
        self.value_at(db, RowKind::Pk, col)
    }

    #[doc(hidden)]
    fn value_at_known_pk_resolved<DB: DatabaseLike>(
        &self,
        db: &DB,
        table_id: TableId,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError> {
        self.value_at_resolved(db, table_id, RowKind::Pk, col)
    }
}

/// One event paired with its resolved catalog table.
pub struct ResolvedEvent<'a, E: CdcEvent> {
    event: &'a E,
    table_id: TableId,
    pk_columns: core::cell::OnceCell<Vec<ColumnId>>,
    changed_columns: core::cell::OnceCell<Vec<ColumnId>>,
}

impl<'a, E: CdcEvent> ResolvedEvent<'a, E> {
    pub(crate) fn new<DB: DatabaseLike>(event: &'a E, database: &DB) -> Self {
        Self {
            event,
            table_id: event.table_id(database),
            pk_columns: core::cell::OnceCell::new(),
            changed_columns: core::cell::OnceCell::new(),
        }
    }

    pub(crate) const fn table_id(&self) -> TableId {
        self.table_id
    }

    pub(crate) fn pk_columns<DB: DatabaseLike>(&self, database: &DB) -> &[ColumnId] {
        self.pk_columns
            .get_or_init(|| self.event.pk_columns_resolved(database, self.table_id))
            .as_slice()
    }

    pub(crate) fn changed_columns<DB: DatabaseLike>(&self, database: &DB) -> &[ColumnId] {
        self.changed_columns
            .get_or_init(|| self.event.changed_columns_resolved(database, self.table_id))
            .as_slice()
    }

    pub(crate) fn value_at<DB: DatabaseLike>(
        &self,
        database: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<E::Backend>, crate::ValueError> {
        self.event
            .value_at_resolved(database, self.table_id, row, col)
    }
}

impl<E: CdcEvent> CdcEvent for ResolvedEvent<'_, E> {
    type Backend = E::Backend;
    type Checkpoint = E::Checkpoint;

    fn kind(&self) -> EventKind {
        self.event.kind()
    }

    fn table_id<DB: DatabaseLike>(&self, _db: &DB) -> TableId {
        self.table_id
    }

    fn checkpoint(&self) -> Option<Self::Checkpoint> {
        self.event.checkpoint()
    }

    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        ResolvedEvent::pk_columns(self, db).to_vec()
    }

    fn with_pk_columns<DB: DatabaseLike, R>(
        &self,
        db: &DB,
        read: impl FnOnce(&[ColumnId]) -> R,
    ) -> R {
        read(ResolvedEvent::pk_columns(self, db))
    }

    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId> {
        ResolvedEvent::changed_columns(self, db).to_vec()
    }

    fn with_changed_columns<DB: DatabaseLike, R>(
        &self,
        db: &DB,
        read: impl FnOnce(&[ColumnId]) -> R,
    ) -> R {
        read(ResolvedEvent::changed_columns(self, db))
    }

    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError> {
        self.event.value_at_resolved(db, self.table_id, row, col)
    }

    fn value_at_known_pk<DB: DatabaseLike>(
        &self,
        db: &DB,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError> {
        self.event
            .value_at_known_pk_resolved(db, self.table_id, col)
    }
}

/// Decode one cell whose column kind may name a custom type, by handing a
/// builtin kind to `decode` and converting afterwards when it does.
///
/// The four wire decoders stay builtin-only and unchanged: this is the one
/// place that knows a custom column is read by decoding its carrier and then
/// converting, so the four paths cannot drift apart on it.
///
/// # Errors
///
/// [`crate::ValueError::Builtin`] when `decode` could not read the bytes as the
/// kind (or as the carrier, for a custom), and [`crate::ValueError::Custom`] when
/// the carrier read fine and the type's own conversion declined it. Keeping
/// those apart is why this returns a `Result` rather than
/// [`Value::Missing`].
pub fn decode_cell<B, F>(
    column: crate::ColumnId,
    kind: ScalarKindOf<B>,
    decode: F,
) -> Result<Value<B>, crate::ValueError>
where
    B: Backend,
    F: FnOnce(crate::backend::DeclaredType) -> Value<B>,
{
    let Some(custom) = kind.custom().copied() else {
        // Total: `custom()` answered `None`, so `builtin` answers `Some`.
        let builtin = kind
            .declared_type()
            .unwrap_or(crate::backend::DeclaredType::Text(
                crate::backend::TextWidth::Varying,
            ));
        let decoded = decode(builtin);
        return if decoded.is_missing() {
            Err(crate::ValueError::Builtin {
                column,
                kind: builtin.family(),
            })
        } else {
            Ok(decoded)
        };
    };

    let carrier = <B::Custom as CustomScalars>::carrier(custom);
    // A carrier is a family: a custom type declares no width, so the decode
    // reads the common case rather than inventing a refinement.
    let raw = decode(crate::backend::declared_type_of(
        carrier,
        crate::backend::IntWidth::SixtyFour,
        crate::backend::FloatWidth::Double,
        crate::backend::TextWidth::Varying,
    ));
    let Some(view) = raw.as_carried() else {
        return Err(crate::ValueError::Builtin {
            column,
            kind: carrier,
        });
    };
    <B::Custom as CustomScalars>::convert(custom, view)
        .map(Value::Custom)
        .ok_or_else(|| crate::ValueError::Custom {
            column,
            custom: alloc::format!("{custom:?}"),
        })
}

/// Encode a tuple of values into stable bytes usable as a map key.
///
/// [`Value`] carries floats, so it has neither [`Hash`](core::hash::Hash) nor
/// [`Ord`] and cannot key a map directly. Postcard's encoding is
/// length-prefixed per element, so `["a", "b"]` and `["ab", ""]` differ, which
/// a naive concatenation would not.
///
/// This is the transport identity used by keyed row matching. Grouped results
/// use [`crate::backend::GroupKeyEncoder`], whose backend policy follows database equality.
///
/// Returns `None` when postcard cannot encode the tuple.
pub fn encode_value_key<B: Backend>(values: &[Value<B>]) -> Option<alloc::vec::Vec<u8>> {
    postcard::to_allocvec(values).ok()
}

/// The presence a row image reports per cell, which is what tells "the source
/// did not say" apart from "the value is NULL".
#[cfg(all(test, feature = "std"))]
#[allow(clippy::unwrap_used)]
mod cell_presence_tests {
    use crate::backend::{CdcEvent, CellPresence, Postgres, RowKind, Value};
    use crate::testing::TestEvent;
    use alloc::vec;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    #[test]
    fn presence_distinguishes_missing_from_null() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE t (id INT PRIMARY KEY, absent TEXT, nulled TEXT);",
        )
        .expect("the DDL parses");
        let table = crate::catalog_helpers::table_id(&db, "t").expect("t is cataloged");
        let event =
            TestEvent::<Postgres>::insert(table, vec![Value::Int(1), Value::Missing, Value::Null])
                .with_pk_columns([0u16]);

        assert_eq!(
            event.presence_at(&db, RowKind::New, 0),
            CellPresence::Present
        );
        assert_eq!(
            event.presence_at(&db, RowKind::New, 1),
            CellPresence::Missing,
            "a cell the source did not carry is not a NULL"
        );
        assert_eq!(event.presence_at(&db, RowKind::New, 2), CellPresence::Null);
    }

    #[test]
    fn presence_reports_a_cell_the_source_could_not_decode() {
        struct Corrupt(crate::TableId);

        impl CdcEvent for Corrupt {
            type Backend = Postgres;
            type Checkpoint = crate::NoCheckpoint;

            fn kind(&self) -> crate::EventKind {
                crate::EventKind::Insert
            }

            fn table_id<DB: sql_traits::prelude::DatabaseLike>(&self, _db: &DB) -> crate::TableId {
                self.0
            }

            fn checkpoint(&self) -> Option<Self::Checkpoint> {
                None
            }

            fn pk_columns<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                _db: &DB,
            ) -> alloc::vec::Vec<crate::ColumnId> {
                vec![0]
            }

            fn changed_columns<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                _db: &DB,
            ) -> alloc::vec::Vec<crate::ColumnId> {
                alloc::vec::Vec::new()
            }

            fn value_at<DB: sql_traits::prelude::DatabaseLike>(
                &self,
                _db: &DB,
                _row: RowKind,
                col: crate::ColumnId,
            ) -> Result<Value<Self::Backend>, crate::ValueError> {
                Err(crate::ValueError::Builtin {
                    column: col,
                    kind: crate::backend::ScalarFamily::Int,
                })
            }
        }

        let db = ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE t (id INT PRIMARY KEY);")
            .expect("the DDL parses");
        let table = crate::catalog_helpers::table_id(&db, "t").expect("t is cataloged");
        assert_eq!(
            Corrupt(table).presence_at(&db, RowKind::New, 0),
            CellPresence::Undecodable
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod value_key_tests {
    use super::encode_value_key;
    use crate::backend::Postgres;
    use crate::backend::Value;
    use alloc::vec;

    /// The transport encoding remains byte-for-byte stable.
    #[test]
    fn the_encoding_is_frozen() {
        let cases: vec::Vec<(vec::Vec<Value<Postgres>>, &[u8])> = vec![
            (vec![], &[0]),
            (vec![Value::Int(1)], &[1, 3, 2]),
            (vec![Value::String("eu".into())], &[1, 5, 2, b'e', b'u']),
            (
                vec![Value::String("a".into()), Value::String("b".into())],
                &[2, 5, 1, b'a', 5, 1, b'b'],
            ),
            (
                vec![
                    Value::String("ab".into()),
                    Value::String(alloc::string::String::new()),
                ],
                &[2, 5, 2, b'a', b'b', 5, 0],
            ),
            (vec![Value::Null], &[1, 1]),
            (vec![Value::Bool(true)], &[1, 2, 1]),
            (vec![Value::Bytes(vec![1, 2])], &[1, 6, 2, 1, 2]),
            (
                vec![Value::Uuid(
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
                        .expect("valid UUID"),
                )],
                &[
                    1, 7, 16, 85, 14, 132, 0, 226, 155, 65, 212, 167, 22, 68, 102, 85, 68, 0, 0,
                ],
            ),
            (
                vec![Value::Timestamp(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
                        .expect("valid date")
                        .and_hms_opt(3, 4, 5)
                        .expect("valid time"),
                )],
                &[
                    1, 8, 19, 50, 48, 50, 54, 45, 48, 49, 45, 48, 50, 84, 48, 51, 58, 48, 52, 58,
                    48, 53,
                ],
            ),
            (
                vec![Value::TimestampTz(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
                        .expect("valid date")
                        .and_hms_opt(3, 4, 5)
                        .expect("valid time")
                        .and_utc(),
                )],
                &[
                    1, 9, 20, 50, 48, 50, 54, 45, 48, 49, 45, 48, 50, 84, 48, 51, 58, 48, 52, 58,
                    48, 53, 90,
                ],
            ),
            (
                vec![Value::Date(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date"),
                )],
                &[1, 10, 10, 50, 48, 50, 54, 45, 48, 49, 45, 48, 50],
            ),
            (
                vec![Value::Time(
                    chrono::NaiveTime::from_hms_opt(3, 4, 5).expect("valid time"),
                )],
                &[1, 11, 8, 48, 51, 58, 48, 52, 58, 48, 53],
            ),
        ];
        for (tuple, want) in cases {
            let got = encode_value_key(&tuple).expect("every kind here is encodable");
            assert_eq!(
                got, want,
                "the encoding of {tuple:?} changed, which orphans every stored key that used it"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod canonical_group_key_tests {
    use crate::backend::{
        Backend, CollationFacts, CollationName, ColumnComparison, MySql, NoCustom, Pg18, Postgres,
        SQLite, ScalarFamily, SqliteJson, Value,
    };
    use alloc::{string::String, vec};
    use sql_traits::traits::MySqlCollationPadding;

    fn column(kind: ScalarFamily) -> ColumnComparison<NoCustom> {
        column_with_collation(kind, CollationFacts::DatabaseDefault)
    }

    fn column_with_collation(
        kind: ScalarFamily,
        collation: CollationFacts,
    ) -> ColumnComparison<NoCustom> {
        ColumnComparison {
            kind: kind.into(),
            declared_type: String::from("test"),
            collation,
        }
    }

    fn named_collation(
        name: &str,
        postgres_deterministic: Option<bool>,
        padding: Option<MySqlCollationPadding>,
    ) -> CollationFacts {
        CollationFacts::Named {
            name: CollationName {
                name: String::from(name),
                name_is_quoted: false,
                schema: None,
                schema_is_quoted: false,
            },
            postgres_deterministic,
            padding: padding.map(Into::into),
        }
    }

    #[test]
    fn canonical_key_has_one_versioned_tuple_format() {
        let encoder = Postgres::<Pg18>::group_key_encoder(vec![column(ScalarFamily::Int)])
            .expect("integer groups have a canonical encoder");
        let key = encoder
            .encode(&[Value::Int(42)])
            .expect("integer value matches the plan");

        assert_eq!(
            key,
            vec![b'S', b'Q', b'G', b'K', 1, 0, 1, 0, 0, 0, 2, 2, 84]
        );
    }

    #[test]
    fn canonical_key_rejects_values_outside_the_selected_domain() {
        let encoder = Postgres::<Pg18>::group_key_encoder(vec![column(ScalarFamily::Int)])
            .expect("integer groups have a canonical encoder");

        assert!(encoder.encode(&[]).is_none());
        assert!(encoder.encode(&[Value::Missing]).is_none());
        assert!(encoder.encode(&[Value::Null]).is_some());
        assert_ne!(
            encoder.encode(&[Value::Null]),
            encoder.encode(&[Value::Int(0)])
        );
        assert!(encoder
            .encode(&[Value::String(String::from("42"))])
            .is_none());
    }

    #[test]
    fn postgres_float_keys_follow_grouping_equality() {
        let encoder = Postgres::<Pg18>::group_key_encoder(vec![column(ScalarFamily::Float)])
            .expect("Postgres float grouping is canonical");

        let zero = encoder.encode(&[Value::Float(0.0)]).unwrap();
        let negative_zero = encoder.encode(&[Value::Float(-0.0)]).unwrap();
        assert_eq!(zero, negative_zero);

        let nan = encoder.encode(&[Value::Float(f64::NAN)]).unwrap();
        let other_nan = encoder
            .encode(&[Value::Float(f64::from_bits(0x7ff0_0000_0000_0001))])
            .unwrap();
        assert_eq!(nan, other_nan);
        assert_ne!(zero, nan);
    }

    #[test]
    fn postgres_text_requires_deterministic_comparison() {
        assert!(
            Postgres::<Pg18>::group_key_encoder(vec![column(ScalarFamily::String)]).is_some(),
            "the database default is deterministic"
        );
        assert!(
            Postgres::<Pg18>::group_key_encoder(vec![column_with_collation(
                ScalarFamily::String,
                named_collation("unicode", Some(true), None),
            )])
            .is_some()
        );
        assert!(
            Postgres::<Pg18>::group_key_encoder(vec![column_with_collation(
                ScalarFamily::String,
                named_collation("ci", Some(false), None),
            )])
            .is_none()
        );
        assert!(
            Postgres::<Pg18>::group_key_encoder(vec![column_with_collation(
                ScalarFamily::String,
                CollationFacts::Unknown,
            )])
            .is_none()
        );
    }

    #[test]
    fn sqlite_builtin_collations_have_exact_canonical_forms() {
        let nocase = SQLite::group_key_encoder(vec![column_with_collation(
            ScalarFamily::String,
            named_collation("NOCASE", None, None),
        )])
        .unwrap();
        assert_eq!(
            nocase.encode(&[Value::String(String::from("A\0IGNORED"))]),
            nocase.encode(&[Value::String(String::from("a\0ignored"))])
        );
        assert_ne!(
            nocase.encode(&[Value::String(String::from("A\0ignored"))]),
            nocase.encode(&[Value::String(String::from("a\0different"))])
        );
        assert_ne!(
            nocase.encode(&[Value::String(String::from("Æ"))]),
            nocase.encode(&[Value::String(String::from("æ"))])
        );

        let rtrim = SQLite::group_key_encoder(vec![column_with_collation(
            ScalarFamily::String,
            named_collation("RTRIM", None, None),
        )])
        .unwrap();
        assert_eq!(
            rtrim.encode(&[Value::String(String::from("value"))]),
            rtrim.encode(&[Value::String(String::from("value  "))])
        );
    }

    #[test]
    fn mysql_binary_collations_apply_their_padding_rule() {
        assert!(MySql::group_key_encoder(vec![column(ScalarFamily::String)]).is_none());

        let pad = MySql::group_key_encoder(vec![column_with_collation(
            ScalarFamily::String,
            named_collation("utf8mb4_bin", None, Some(MySqlCollationPadding::PadSpace)),
        )])
        .unwrap();
        assert_eq!(
            pad.encode(&[Value::String(String::from("value"))]),
            pad.encode(&[Value::String(String::from("value  "))])
        );

        let no_pad = MySql::group_key_encoder(vec![column_with_collation(
            ScalarFamily::String,
            named_collation("utf8mb4_0900_bin", None, Some(MySqlCollationPadding::NoPad)),
        )])
        .unwrap();
        assert_ne!(
            no_pad.encode(&[Value::String(String::from("value"))]),
            no_pad.encode(&[Value::String(String::from("value  "))])
        );
    }

    #[test]
    fn mysql_decimal_keys_ignore_scale_spelling() {
        let encoder = MySql::group_key_encoder(vec![column(ScalarFamily::Decimal)]).unwrap();
        assert_eq!(
            encoder.encode(&[Value::Decimal("1.0".parse().unwrap())]),
            encoder.encode(&[Value::Decimal("1.00".parse().unwrap())])
        );
    }

    #[test]
    fn postgres_jsonb_keys_follow_structural_equality() {
        let encoder =
            Postgres::<Pg18>::group_key_encoder(vec![column(ScalarFamily::Jsonb)]).unwrap();
        let left: serde_json::Value =
            serde_json::from_str(r#"{"a": 1.0, "b": [true, null]}"#).unwrap();
        let right: serde_json::Value =
            serde_json::from_str(r#"{"b": [true, null], "a": 1.00}"#).unwrap();
        assert_eq!(
            encoder.encode(&[Value::Jsonb(left.clone())]),
            encoder.encode(&[Value::Jsonb(right.clone())])
        );
        assert_eq!(Value::<Postgres>::Jsonb(left), Value::Jsonb(right));
    }

    #[test]
    fn sqlite_json_keys_preserve_storage_equality() {
        let encoder = SQLite::group_key_encoder(vec![column(ScalarFamily::Json)]).unwrap();
        assert_eq!(
            encoder.encode(&[Value::Json(SqliteJson::integer(1))]),
            encoder.encode(&[Value::Json(SqliteJson::real(1.0))])
        );
        assert_ne!(
            encoder.encode(&[Value::Json(SqliteJson::text(String::from("{\"a\":1}")))]),
            encoder.encode(&[Value::Json(SqliteJson::text(String::from("{ \"a\": 1 }")))])
        );
        assert_ne!(
            encoder.encode(&[Value::Json(SqliteJson::blob(vec![1]))]),
            encoder.encode(&[Value::Json(SqliteJson::text(String::from("1")))])
        );
    }

    proptest::proptest! {
        #[test]
        fn sqlite_nocase_folds_every_ascii_case_pair(value in "[A-Za-z0-9]{0,64}") {
            let encoder = SQLite::group_key_encoder(vec![column_with_collation(
                ScalarFamily::String,
                named_collation("NOCASE", None, None),
            )])
            .unwrap();
            proptest::prop_assert_eq!(
                encoder.encode(&[Value::String(value.to_ascii_lowercase())]),
                encoder.encode(&[Value::String(value.to_ascii_uppercase())])
            );
        }

        #[test]
        fn postgres_float_collapses_every_nan_payload(bits in proptest::prelude::any::<u64>()) {
            let value = f64::from_bits(bits);
            if value.is_nan() {
                let encoder = Postgres::<Pg18>::group_key_encoder(vec![column(ScalarFamily::Float)]).unwrap();
                proptest::prop_assert_eq!(
                    encoder.encode(&[Value::Float(value)]),
                    encoder.encode(&[Value::Float(f64::NAN)])
                );
            }
        }
    }
}
