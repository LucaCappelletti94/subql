#![allow(clippy::type_complexity)]

//! Diesel-typed subscription API.
//!
//! Renders a typed diesel query to placeholder SQL plus a list of resolved bind
//! [`Value<B>`](crate::backend::Value)s, so callers get compile-time-checked
//! queries from their diesel `table!` schema while the engine keeps consuming
//! SQL + binds (see [`crate::SubscriptionRequest::binds`]).
//!
//! # Backends
//!
//! Diesel fixes the bind collector per backend, so binds come out in a
//! backend-specific form that each [`BindDecode`] impl turns into
//! [`Value<B>`](crate::backend::Value)s:
//!
//! - Postgres (`diesel-typed`): serialized binary wire bytes plus a type OID,
//!   decoded big-endian by OID. Pure Rust (`postgres_backend`, no libpq).
//! - SQLite (`diesel-typed-sqlite`): already-typed bind values, read directly.
//! - MySQL (`diesel-typed-mysql`): native-endian client buffers tagged with a
//!   `MysqlType`, read by their byte length. Pure Rust (`mysql_backend`, no
//!   libmysqlclient).
//!
//! Typed SELECT and UPDATE-follow work on all three. Follow-insert
//! ([`SubscriptionEngine::register_follow_insert`](crate::SubscriptionEngine::register_follow_insert))
//! covers Postgres and SQLite through `RETURNING`. MySQL has no `RETURNING`,
//! so it takes the DB-minted key from diesel's `execute_returning_id` and
//! follows it with
//! [`SubscriptionEngine::follow_row`](crate::SubscriptionEngine::follow_row).
//!
//! # Caveats
//!
//! - The rendered SQL is the backend's own flavor: `$N` vs `?` placeholders,
//!   and double-quoted vs backtick-quoted identifiers. So the engine's
//!   sqlparser `Dialect` must match the backend the query was rendered for.
//! - SQLite and MySQL have no boolean storage class, so a `bool` bind decodes
//!   to [`Value::Int`] `(0)` or [`Value::Int`] `(1)`, not
//!   [`Value::Bool`] (Postgres yields [`Value::Bool`]).
//!
//! [`Value::Int`]: crate::backend::Value::Int
//! [`Value::Bool`]: crate::backend::Value::Bool
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use diesel::backend::Backend;
use diesel::connection::{DefaultLoadingMode, LoadConnection};
use diesel::expression::QueryMetadata;
use diesel::pg::{Pg, PgMetadataLookup, PgTypeMetadata};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::returning::ReturningClause;
use diesel::query_builder::{InsertStatement, Query, QueryBuilder, QueryFragment, QueryId};
use diesel::row::{Field, Row};
use diesel::Table;

/// A follow-insert query: the caller's insert with a `RETURNING <primary key>`
/// clause that we derive from the table type, so the returned columns are always
/// the table's primary key, in its order (never restated by the caller).
type PkReturningInsert<T, U, Op> =
    InsertStatement<T, U, Op, ReturningClause<<T as Table>::PrimaryKey>>;
use sql_traits::prelude::DatabaseLike;

use crate::backend::{CdcEvent, Postgres, Value};
use crate::compiler::literals::SqlLiteralParse;
#[cfg(feature = "diesel-typed-mysql")]
use crate::diesel_decode::mysql_canonical;
#[cfg(feature = "diesel-typed-sqlite")]
use crate::diesel_decode::owned_sqlite_canonical;
use crate::diesel_decode::pg_canonical;
use crate::diesel_decode::{RowFieldDecode, SpellCanonical};
use crate::{IdTypes, RegisterError, Registered, SubscriptionEngine, SubscriptionRequest};

/// A metadata lookup that resolves nothing. Built-in scalar Postgres types
/// report their OID statically without consulting the lookup, so this suffices
/// for scalar binds without a database connection. A custom type (enum, domain,
/// composite) would consult it; we return sentinel OID `0`, which
/// [`decode_pg_bind`] then rejects as an unsupported bind type.
struct NoLookup;

impl PgMetadataLookup for NoLookup {
    fn lookup_type(&mut self, _type_name: &str, _schema: Option<&str>) -> PgTypeMetadata {
        PgTypeMetadata::new(0, 0)
    }
}

/// Render a typed diesel query to placeholder SQL plus its bind values.
///
/// The binds are decoded to [`Value<B>`](crate::backend::Value)s, in
/// placeholder order, where `B` is the subql
/// [`crate::backend::Backend`] the diesel backend corresponds to.
///
/// Generic over the diesel backend `D` that decodes its own binds
/// ([`BindDecode`]): `Pg` yields `$N` placeholders and decodes the
/// Postgres binary wire format by type OID, `Sqlite` yields `?`
/// placeholders and reads its typed bind values directly.
///
/// # Errors
/// Returns [`RegisterError::UnsupportedSql`] if diesel fails to render
/// the query or a bind uses a type outside the supported scalar set
/// (bool, integer, float, text, uuid).
pub fn render_typed<D, Q>(query: &Q) -> Result<(String, Vec<Value<D::SubqlBackend>>), RegisterError>
where
    D: BindDecode,
    Q: QueryFragment<D>,
{
    D::render_sql_and_binds(query)
}

/// Render only the placeholder SQL skeleton for a backend, no binds. Needs just
/// the backend type, no connection.
fn render_sql<B, Q>(query: &Q) -> Result<String, RegisterError>
where
    B: Backend + Default,
    <B as Backend>::QueryBuilder: Default,
    Q: QueryFragment<B>,
{
    let mut qb = <B as Backend>::QueryBuilder::default();
    query.to_sql(&mut qb, &B::default()).map_err(|e| {
        RegisterError::UnsupportedSql(format!("diesel query rendering failed: {e}"))
    })?;
    Ok(qb.finish())
}

/// Render a typed diesel query to placeholder SQL plus its bind
/// [`Value<B>`](crate::backend::Value)s.
///
/// One diesel backend `D` whose associated
/// [`SubqlBackend`](BindDecode::SubqlBackend) picks the subql
/// [`crate::backend::Backend`] to type the values against. The bind
/// side is backend-specific: Postgres decodes the binary wire format
/// by OID, SQLite reads its typed bind values. This is the input-side
/// counterpart to [`RowFieldDecode`], which reads the values an
/// executed query hands back.
pub trait BindDecode: Backend {
    /// The subql [`crate::backend::Backend`] whose [`Value`] shape this
    /// diesel backend's binds decode to.
    type SubqlBackend: crate::backend::Backend;

    /// Render `query` to `(placeholder SQL, ordered bind values)`.
    ///
    /// # Errors
    /// [`RegisterError::UnsupportedSql`] if rendering fails or a bind
    /// uses a type outside the supported scalar set.
    fn render_sql_and_binds<Q>(
        query: &Q,
    ) -> Result<(String, Vec<Value<Self::SubqlBackend>>), RegisterError>
    where
        Q: QueryFragment<Self>;
}

impl BindDecode for Pg {
    type SubqlBackend = Postgres;

    fn render_sql_and_binds<Q>(query: &Q) -> Result<(String, Vec<Value<Postgres>>), RegisterError>
    where
        Q: QueryFragment<Self>,
    {
        let sql = render_sql::<Self, _>(query)?;

        // Binds as serialized wire bytes + type OIDs, decoded per OID.
        let mut collector = RawBytesBindCollector::<Self>::new();
        let mut lookup = NoLookup;
        query
            .collect_binds(&mut collector, &mut lookup, &Self)
            .map_err(|e| {
                RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
            })?;
        let mut values = Vec::with_capacity(collector.binds.len());
        for (bytes, meta) in collector.binds.iter().zip(collector.metadata.iter()) {
            let type_oid = meta.oid().map_err(|_| {
                RegisterError::UnsupportedSql("diesel bind has an unresolved type OID".to_string())
            })?;
            values.push(Postgres::value_from_canonical(pg_canonical(
                bytes.as_deref(),
                type_oid,
            )?));
        }
        Ok((sql, values))
    }
}

#[cfg(feature = "diesel-typed-sqlite")]
impl BindDecode for diesel::sqlite::Sqlite {
    type SubqlBackend = crate::backend::SQLite;

    fn render_sql_and_binds<Q>(
        query: &Q,
    ) -> Result<(String, Vec<Value<crate::backend::SQLite>>), RegisterError>
    where
        Q: QueryFragment<Self>,
    {
        use diesel::query_builder::MoveableBindCollector;
        use diesel::sqlite::SqliteBindCollector;

        let sql = render_sql::<Self, _>(query)?;

        let mut collector = SqliteBindCollector::default();
        query
            .collect_binds(&mut collector, &mut (), &Self)
            .map_err(|e| {
                RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
            })?;
        let data = collector.moveable();
        let mut values = Vec::with_capacity(data.binds().len());
        for (value, _ty) in data.binds() {
            values.push(crate::backend::SQLite::value_from_canonical(
                owned_sqlite_canonical(value),
            ));
        }
        Ok((sql, values))
    }
}

#[cfg(feature = "diesel-typed-mysql")]
impl BindDecode for diesel::mysql::Mysql {
    type SubqlBackend = crate::backend::MySql;

    fn render_sql_and_binds<Q>(
        query: &Q,
    ) -> Result<(String, Vec<Value<crate::backend::MySql>>), RegisterError>
    where
        Q: QueryFragment<Self>,
    {
        let sql = render_sql::<Self, _>(query)?;

        let mut collector = RawBytesBindCollector::<Self>::new();
        query
            .collect_binds(&mut collector, &mut (), &Self)
            .map_err(|e| {
                RegisterError::UnsupportedSql(format!("diesel bind collection failed: {e}"))
            })?;
        let mut values = Vec::with_capacity(collector.binds.len());
        for (bytes, meta) in collector.binds.iter().zip(collector.metadata.iter()) {
            values.push(crate::backend::MySql::value_from_canonical(
                mysql_canonical(bytes.as_deref(), *meta)?,
            ));
        }
        Ok((sql, values))
    }
}

/// Recover the bare table name an insert targets straight from its diesel table
/// type, so a follow never restates (and so cannot mistype) the table it writes.
///
/// Renders the table's identifier with the backend's own quoting (`"t"` on
/// Postgres/SQLite, `` `t` `` on MySQL), strips that quoting, and takes the final
/// dotted segment so a schema-qualified table (`"public"."users"`) resolves to
/// its bare catalog name (`users`).
fn insert_target_table_name<T, DB>() -> Result<String, RegisterError>
where
    T: Table + QueryFragment<DB> + Default,
    DB: Backend + Default,
    <DB as Backend>::QueryBuilder: Default,
{
    let mut qb = <DB as Backend>::QueryBuilder::default();
    QueryFragment::<DB>::to_sql(&T::default(), &mut qb, &DB::default()).map_err(|e| {
        RegisterError::UnsupportedSql(format!("diesel table rendering failed: {e}"))
    })?;
    let rendered = qb.finish();
    let bare = rendered
        .rsplit('.')
        .next()
        .unwrap_or(rendered.as_str())
        .trim_matches(|c| c == '"' || c == '`')
        .to_string();
    if bare.is_empty() {
        return Err(RegisterError::UnsupportedSql(format!(
            "could not determine the target table name from the diesel insert (rendered {rendered:?})"
        )));
    }
    Ok(bare)
}

/// Error from [`SubscriptionEngine::register_follow_insert`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FollowInsertError {
    /// Executing or reading the `INSERT ... RETURNING` failed.
    #[error("insert execution failed: {0}")]
    Load(#[from] diesel::result::Error),
    /// Building or registering the follow failed (unknown table, no primary key,
    /// key-arity mismatch, or an unsupported returned column type).
    #[error("follow registration failed: {0}")]
    Register(#[from] RegisterError),
}

/// Diesel-typed registration methods on the engine.
impl<E, I, DB> SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Register a subscription from a typed diesel query.
    pub fn register_select_typed<D, Q>(
        &mut self,
        consumer_id: I::ConsumerId,
        query: &Q,
    ) -> Result<Registered, RegisterError>
    where
        D: BindDecode<SubqlBackend = E::Backend>,
        Q: QueryFragment<D>,
    {
        let (sql, binds) = render_typed::<D, _>(query)?;
        self.register(SubscriptionRequest::new(consumer_id, sql).binds(binds))
    }

    /// Register a follow subscription from a typed diesel UPDATE.
    pub fn register_follow_update_typed<D, Q>(
        &mut self,
        consumer_id: I::ConsumerId,
        update: &Q,
    ) -> Result<Registered, RegisterError>
    where
        D: BindDecode<SubqlBackend = E::Backend>,
        Q: QueryFragment<D>,
    {
        let (sql, binds) = render_typed::<D, _>(update)?;
        // Diesel emits binds in placeholder order (SET first, then WHERE).
        // The follow SELECT drops the SET clause, so its `?` positional
        // placeholders bind against the wrong end of the collected list
        // and its `$N` numbered placeholders reference indices that no
        // longer exist after trimming. Ask the derive helper to also report
        // how many SET binds it discarded and to renumber surviving `$N`s
        // so the caller's slice + engine round-trip line up.
        let (select_sql, set_bind_count) =
            crate::compiler::derive_update_follow_select_with_set_binds(&sql, self.dialect())?;
        let where_binds: Vec<Value<E::Backend>> = binds.into_iter().skip(set_bind_count).collect();
        self.register(SubscriptionRequest::new(consumer_id, select_sql).binds(where_binds))
    }

    /// Execute a typed diesel `INSERT ... RETURNING <pk>` and follow the
    /// inserted row(s) by their (possibly DB-minted) primary key.
    pub fn register_follow_insert<T, U, Op, C>(
        &mut self,
        consumer_id: I::ConsumerId,
        insert: InsertStatement<T, U, Op>,
        conn: &mut C,
    ) -> Result<alloc::vec::Vec<Registered>, FollowInsertError>
    where
        C: LoadConnection<DefaultLoadingMode>,
        E::Backend: SpellCanonical,
        C::Backend: RowFieldDecode
            + Default
            + QueryMetadata<<PkReturningInsert<T, U, Op> as Query>::SqlType>,
        <C::Backend as Backend>::QueryBuilder: Default,
        T: Table + QueryFragment<C::Backend> + Default,
        PkReturningInsert<T, U, Op>: Query + QueryFragment<C::Backend> + QueryId,
    {
        let table =
            insert_target_table_name::<T, C::Backend>().map_err(FollowInsertError::Register)?;

        let insert = insert.returning(T::default().primary_key());

        let rows: alloc::vec::Vec<alloc::vec::Vec<Value<E::Backend>>> = {
            let cursor = conn.load(insert)?;
            let mut out = alloc::vec::Vec::new();
            for row in cursor {
                let row = row?;
                let n = row.field_count();
                let mut values = alloc::vec::Vec::with_capacity(n);
                for i in 0..n {
                    let value = match row.get(i) {
                        Some(field) => E::Backend::value_from_canonical(
                            <C::Backend as RowFieldDecode>::field_to_canonical(field.value())
                                .map_err(FollowInsertError::Register)?,
                        ),
                        None => Value::Null,
                    };
                    values.push(value);
                }
                out.push(values);
            }
            out
        };

        let mut results = alloc::vec::Vec::with_capacity(rows.len());
        for pk in rows {
            results.push(
                self.follow_row(consumer_id, &table, pk)
                    .map_err(FollowInsertError::Register)?,
            );
        }
        Ok(results)
    }
}

/// Diesel columns naming the compared columns of one membership term, in the
/// order the stated value rows follow.
///
/// Implemented for tuples of one to eight columns, so a static schema states
/// values as `term_values_for((docs::id,), rows)` or
/// `term_values_for((docs::tenant_id, docs::id), rows)` and can never
/// misspell a name. The one-wide spelling keeps its trailing comma: a bare
/// column cannot carry this trait beside a foreign `Column` bound, since a
/// later diesel could implement `Column` for tuples and the two impls would
/// then overlap.
pub trait TermColumns {
    /// The column names, in tuple order.
    fn names() -> Vec<String>;
}

macro_rules! term_columns_tuple {
    ($($column:ident),+) => {
        impl<$($column: diesel::Column),+> TermColumns for ($($column,)+) {
            fn names() -> Vec<String> {
                alloc::vec![$($column::NAME.to_string()),+]
            }
        }
    };
}
term_columns_tuple!(C1);
term_columns_tuple!(C1, C2);
term_columns_tuple!(C1, C2, C3);
term_columns_tuple!(C1, C2, C3, C4);
term_columns_tuple!(C1, C2, C3, C4, C5);
term_columns_tuple!(C1, C2, C3, C4, C5, C6);
term_columns_tuple!(C1, C2, C3, C4, C5, C6, C7);
term_columns_tuple!(C1, C2, C3, C4, C5, C6, C7, C8);

impl<I: IdTypes, B: crate::backend::Backend> SubscriptionRequest<I, B> {
    /// State the value rows this subscriber currently matches, naming the
    /// compared columns as diesel columns rather than as strings.
    ///
    /// The typed spelling of [`SubscriptionRequest::term_values`], and the
    /// preferred one wherever a static `table!` schema exists: the names come
    /// from the schema and cannot be misspelled. Each row follows the tuple's
    /// order, which may differ from the filter's own, since the engine
    /// matches by name.
    #[must_use]
    pub fn term_values_for<T: TermColumns>(self, _columns: T, rows: Vec<Vec<Value<B>>>) -> Self {
        self.term_values(T::names(), rows)
    }
}

#[cfg(test)]
mod term_columns_tests {
    use super::TermColumns;
    use crate::backend::{Postgres, Value};
    use crate::{DefaultIds, SubscriptionRequest};

    diesel::table! {
        docs (tenant_id, id) {
            tenant_id -> Integer,
            id -> Integer,
            title -> Text,
        }
    }

    /// The typed door and the string door state the same thing: one produces
    /// exactly what the other spells by hand, one-wide and two-wide.
    #[test]
    fn the_typed_columns_spell_the_catalog_names() {
        assert_eq!(
            <(docs::id,) as TermColumns>::names(),
            vec!["id".to_string()],
            "a one-wide tuple spells one name"
        );
        assert_eq!(
            <(docs::tenant_id, docs::id) as TermColumns>::names(),
            vec!["tenant_id".to_string(), "id".to_string()],
            "a tuple keeps its order"
        );

        let rows = vec![vec![Value::<Postgres>::Int(1), Value::<Postgres>::Int(5)]];
        let typed: SubscriptionRequest<DefaultIds, Postgres> =
            SubscriptionRequest::new(1u64, "SELECT 1")
                .term_values_for((docs::tenant_id, docs::id), rows.clone());
        let spelled: SubscriptionRequest<DefaultIds, Postgres> =
            SubscriptionRequest::new(1u64, "SELECT 1").term_values(vec!["tenant_id", "id"], rows);
        assert_eq!(
            typed.term_values, spelled.term_values,
            "the two doors fill the request identically"
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod render_tests {
    use super::render_typed;
    use crate::backend::{Postgres, Value};
    use crate::testing::TestEvent;
    use diesel::pg::Pg;
    use diesel::prelude::*;

    diesel::table! {
        users (id) {
            id -> Integer,
            name -> Text,
            active -> Bool,
        }
    }

    // Table with a `Binary` column, used by the SQLite blob-bind tests.
    #[cfg(feature = "diesel-typed-sqlite")]
    diesel::table! {
        blobs (id) {
            id -> Integer,
            name -> Text,
            payload -> Binary,
        }
    }

    // Every payload `Value` carries that the Postgres decoder used to refuse.
    diesel::table! {
        readings (id) {
            id -> Integer,
            at -> Timestamp,
            at_tz -> Timestamptz,
            on_day -> Date,
            at_time -> Time,
            amount -> Numeric,
            doc -> Json,
            docb -> Jsonb,
            raw -> Binary,
        }
    }

    // `Datetime` is MySQL's own tag, distinct from `Timestamp`, so it needs its
    // own column or the arm that reads it is never exercised.
    #[cfg(feature = "diesel-typed-mysql")]
    diesel::table! {
        stamps (id) {
            id -> Integer,
            at_dt -> Datetime,
        }
    }

    /// A bind of every remaining payload kind round trips: diesel's own
    /// serializer writes the wire bytes, and the decoder reads them back into
    /// the `Value` variant the catalog would name for that column. Hand-written
    /// bytes would only pin subql's guess at each binary format.
    #[test]
    fn every_payload_kind_round_trips_through_a_bind() {
        use bigdecimal::BigDecimal;
        use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
        use core::str::FromStr;

        let at = NaiveDate::from_ymd_opt(2026, 8, 22)
            .unwrap()
            .and_hms_micro_opt(13, 45, 6, 123_456)
            .unwrap();
        let at_tz: DateTime<Utc> = DateTime::from_naive_utc_and_offset(at, Utc);
        let on_day = NaiveDate::from_ymd_opt(2026, 8, 22).unwrap();
        let at_time = NaiveTime::from_hms_micro_opt(13, 45, 6, 123_456).unwrap();
        let amount = BigDecimal::from_str("1234.5678").unwrap();
        let doc = serde_json::json!({"a": 1});
        let docb = serde_json::json!({"b": [true, null]});
        let raw = alloc::vec![0_u8, 1, 2, 255];

        let query = readings::table
            .filter(readings::at.eq(at))
            .filter(readings::at_tz.eq(at_tz))
            .filter(readings::on_day.eq(on_day))
            .filter(readings::at_time.eq(at_time))
            .filter(readings::amount.eq(amount.clone()))
            .filter(readings::doc.eq(doc.clone()))
            .filter(readings::docb.eq(docb.clone()))
            .filter(readings::raw.eq(raw.clone()));

        let (_sql, binds) = render_typed::<Pg, _>(&query).expect("render");
        assert_eq!(
            binds,
            alloc::vec![
                Value::<Postgres>::Timestamp(at),
                Value::TimestampTz(at_tz),
                Value::Date(on_day),
                Value::Time(at_time),
                Value::Decimal(amount),
                Value::Json(doc),
                Value::Jsonb(docb),
                Value::Bytes(raw),
            ]
        );
    }

    /// `numeric` used to be this file's example of an unsupported type. It is
    /// read now, so the case that used to prove the refusal arm would pass for
    /// the wrong reason: malformed bytes rather than an unknown OID.
    #[test]
    fn numeric_is_read_rather_than_refused() {
        use core::str::FromStr;
        use diesel::sql_types;

        let encoded = render_typed::<Pg, _>(
            &diesel::dsl::sql::<sql_types::Bool>("")
                .bind::<sql_types::Numeric, _>(bigdecimal::BigDecimal::from_str("-0.5").unwrap()),
        )
        .expect("render");
        assert_eq!(
            encoded.1,
            alloc::vec![Value::<Postgres>::Decimal(
                bigdecimal::BigDecimal::from_str("-0.5").unwrap()
            )]
        );
    }

    #[test]
    fn render_select_with_binds() {
        let query = users::table
            .filter(users::id.eq(5))
            .filter(users::name.eq("ann"))
            .filter(users::active.eq(true));
        let (sql, binds) = render_typed::<Pg, _>(&query).expect("render");
        assert!(sql.contains("$1"), "sql: {sql}");
        assert_eq!(
            binds,
            alloc::vec![
                Value::Int(5),
                Value::String("ann".into()),
                Value::Bool(true)
            ]
        );
    }

    #[test]
    fn register_typed_row_query_via_engine() {
        use crate::SubscriptionEngine;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::PostgreSqlDialect;

        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        let mut engine = SubscriptionEngine::<TestEvent<Postgres>, crate::DefaultIds, _>::new(
            catalog,
            PostgreSqlDialect {},
        );

        // Diesel renders a row query as a fully-qualified all-columns list plus a
        // parameterized WHERE. The typed path must accept it (complete column
        // list == SELECT *) and resolve the bind.
        let query = users::table.filter(users::id.eq(5));
        let a = engine
            .register_select_typed::<Pg, _>(1, &query)
            .expect("typed register");
        // Deterministic: registering the same typed query dedups to the same id.
        let b = engine
            .register_select_typed::<Pg, _>(1, &query)
            .expect("typed register again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    #[test]
    fn register_typed_update_follow_via_engine() {
        use crate::SubscriptionEngine;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::PostgreSqlDialect;

        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        let mut engine = SubscriptionEngine::<TestEvent<Postgres>, crate::DefaultIds, _>::new(
            catalog,
            PostgreSqlDialect {},
        );

        // A typed diesel UPDATE registers as a follow on its target rows.
        let update = diesel::update(users::table.filter(users::id.eq(5))).set(users::name.eq("x"));
        let a = engine
            .register_follow_update_typed::<Pg, _>(1, &update)
            .expect("typed update follow");
        // Deterministic: the same typed UPDATE dedups to the same subscription.
        let b = engine
            .register_follow_update_typed::<Pg, _>(1, &update)
            .expect("typed update follow again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn render_select_with_binds_sqlite() {
        use diesel::sqlite::Sqlite;

        let query = users::table
            .filter(users::id.eq(5))
            .filter(users::name.eq("ann"))
            .filter(users::active.eq(true));
        let (sql, binds) = render_typed::<Sqlite, _>(&query).expect("render sqlite");
        // SQLite renders positional `?` placeholders, not `$N`.
        assert!(sql.contains('?'), "sql: {sql}");
        // SQLite has no boolean storage class: `true` binds as an integer, so it
        // decodes to `Value::Int(1)` rather than `Value::Bool(true)` (contrast Pg).
        assert_eq!(
            binds,
            alloc::vec![
                Value::<crate::backend::SQLite>::Int(5),
                Value::String("ann".into()),
                Value::Int(1)
            ]
        );
    }

    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn register_typed_select_sqlite_via_engine() {
        use crate::backend::SQLite;
        use crate::SubscriptionEngine;
        use diesel::sqlite::Sqlite;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::SQLiteDialect;

        let catalog = ParserDB::parse::<SQLiteDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        // The rendered SQL is SQLite-flavored (`?` placeholders, `"..."` idents),
        // so the engine parses it with the matching dialect.
        let mut engine = SubscriptionEngine::<TestEvent<SQLite>, crate::DefaultIds, _>::new(
            catalog,
            SQLiteDialect {},
        );

        let query = users::table.filter(users::id.eq(5));
        let a = engine
            .register_select_typed::<Sqlite, _>(1, &query)
            .expect("typed register sqlite");
        let b = engine
            .register_select_typed::<Sqlite, _>(1, &query)
            .expect("typed register sqlite again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    #[cfg(feature = "diesel-typed-mysql")]
    #[test]
    fn render_select_with_binds_mysql() {
        use diesel::mysql::Mysql;

        let query = users::table
            .filter(users::id.eq(5))
            .filter(users::name.eq("ann"))
            .filter(users::active.eq(true));
        let (sql, binds) = render_typed::<Mysql, _>(&query).expect("render mysql");
        // MySQL renders positional `?` placeholders.
        assert!(sql.contains('?'), "sql: {sql}");
        // Like SQLite, MySQL has no boolean type: `true` binds as an integer.
        assert_eq!(
            binds,
            alloc::vec![
                Value::<crate::backend::MySql>::Int(5),
                Value::String("ann".into()),
                Value::Int(1)
            ]
        );
    }

    /// The MySQL peer of the Postgres round trip. The temporal tags carry
    /// diesel's own `MysqlTime` struct rather than text or an integer, so
    /// letting diesel write and read them is what keeps the two ends agreeing.
    #[cfg(feature = "diesel-typed-mysql")]
    #[test]
    fn every_payload_kind_round_trips_through_a_mysql_bind() {
        use bigdecimal::BigDecimal;
        use chrono::{NaiveDate, NaiveTime};
        use core::str::FromStr;
        use diesel::mysql::Mysql;

        let at = NaiveDate::from_ymd_opt(2026, 8, 22)
            .unwrap()
            .and_hms_opt(13, 45, 6)
            .unwrap();
        let on_day = NaiveDate::from_ymd_opt(2026, 8, 22).unwrap();
        let at_time = NaiveTime::from_hms_opt(13, 45, 6).unwrap();
        let amount = BigDecimal::from_str("1234.5678").unwrap();
        let raw = alloc::vec![0_u8, 1, 2, 255];

        let query = readings::table
            .filter(readings::at.eq(at))
            .filter(readings::on_day.eq(on_day))
            .filter(readings::at_time.eq(at_time))
            .filter(readings::amount.eq(amount.clone()))
            .filter(readings::raw.eq(raw.clone()));

        let (_sql, binds) = render_typed::<Mysql, _>(&query).expect("render mysql");
        assert_eq!(
            binds,
            alloc::vec![
                Value::<crate::backend::MySql>::Timestamp(at),
                Value::Date(on_day),
                Value::Time(at_time),
                Value::Decimal(amount),
                Value::Bytes(raw),
            ]
        );

        // MySQL's own `Datetime` tag, which no `Timestamp` column reaches.
        let (_sql, binds) = render_typed::<Mysql, _>(&stamps::table.filter(stamps::at_dt.eq(at)))
            .expect("render mysql datetime");
        assert_eq!(
            binds,
            alloc::vec![Value::<crate::backend::MySql>::Timestamp(at)]
        );
    }

    #[cfg(feature = "diesel-typed-mysql")]
    #[test]
    fn register_typed_select_mysql_via_engine() {
        use crate::backend::MySql;
        use crate::SubscriptionEngine;
        use diesel::mysql::Mysql;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::MySqlDialect;

        let catalog = ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        // MySQL renders `?` placeholders and backtick idents, so parse with the
        // matching dialect.
        let mut engine = SubscriptionEngine::<TestEvent<MySql>, crate::DefaultIds, _>::new(
            catalog,
            MySqlDialect {},
        );

        let query = users::table.filter(users::id.eq(5));
        let a = engine
            .register_select_typed::<Mysql, _>(1, &query)
            .expect("typed register mysql");
        let b = engine
            .register_select_typed::<Mysql, _>(1, &query)
            .expect("typed register mysql again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    #[cfg(feature = "diesel-typed-mysql")]
    #[test]
    fn register_typed_update_follow_mysql_via_engine() {
        use crate::backend::MySql;
        use crate::SubscriptionEngine;
        use diesel::mysql::Mysql;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::MySqlDialect;

        let catalog = ParserDB::parse::<MySqlDialect>(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL);",
        )
        .expect("catalog");
        let mut engine = SubscriptionEngine::<TestEvent<MySql>, crate::DefaultIds, _>::new(
            catalog,
            MySqlDialect {},
        );

        // A typed diesel UPDATE follows its target rows: the rendered MySQL UPDATE
        // (backtick idents, `?` binds) must parse under MySqlDialect and derive the
        // follow SELECT.
        let update = diesel::update(users::table.filter(users::id.eq(5))).set(users::name.eq("x"));
        let a = engine
            .register_follow_update_typed::<Mysql, _>(1, &update)
            .expect("mysql update follow");
        let b = engine
            .register_follow_update_typed::<Mysql, _>(1, &update)
            .expect("mysql update follow again");
        assert_eq!(a.subscription_id, b.subscription_id);
    }

    /// A blob bind renders to a `?` placeholder plus a `Value::Bytes`, not
    /// an inlined literal. Empty blobs round-trip too.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn render_blob_bind_sqlite() {
        use diesel::sqlite::Sqlite;

        let query = blobs::table.filter(blobs::payload.eq(alloc::vec![1u8, 2, 3]));
        let (sql, binds) = render_typed::<Sqlite, _>(&query).expect("render blob");
        assert!(sql.contains('?'), "sql: {sql}");
        // The value rides as a bind, never inlined as a hex literal.
        assert!(!sql.contains("X'"), "blob must not be inlined: {sql}");
        assert_eq!(
            binds,
            alloc::vec![Value::<crate::backend::SQLite>::Bytes(alloc::vec![1, 2, 3])]
        );

        let empty = blobs::table.filter(blobs::payload.eq(alloc::vec::Vec::<u8>::new()));
        let (_, binds) = render_typed::<Sqlite, _>(&empty).expect("render empty blob");
        assert_eq!(
            binds,
            alloc::vec![Value::<crate::backend::SQLite>::Bytes(alloc::vec![])]
        );
    }

    /// Bind order follows placeholder order: text, then blob, then integer
    /// come back in exactly that sequence, so a `?` at position N pairs with
    /// bind N.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn render_blob_bind_order_sqlite() {
        use diesel::sqlite::Sqlite;

        let query = blobs::table
            .filter(blobs::name.eq("ann"))
            .filter(blobs::payload.eq(alloc::vec![0xaau8, 0xbb]))
            .filter(blobs::id.eq(7));
        let (_, binds) = render_typed::<Sqlite, _>(&query).expect("render ordered");
        assert_eq!(
            binds,
            alloc::vec![
                Value::<crate::backend::SQLite>::String("ann".into()),
                Value::Bytes(alloc::vec![0xaa, 0xbb]),
                Value::Int(7),
            ]
        );
    }

    /// Regression guard on the full `OwnedSqliteBindValue` mapping: every
    /// variant decodes as before, and `Binary` now maps to `Value::Bytes`
    /// (empty included) rather than being rejected.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn owned_sqlite_binds_read_every_variant() {
        use crate::diesel_decode::{owned_sqlite_canonical, Canonical};
        use diesel::sqlite::OwnedSqliteBindValue as V;

        // Asserts what was read, not how a backend spells it: the spelling is
        // `SpellCanonical`'s job and is tested with the backend that does it.
        let dec = owned_sqlite_canonical;
        assert_eq!(dec(&V::Null), Canonical::Null);
        assert_eq!(dec(&V::I32(5)), Canonical::Int(5));
        assert_eq!(dec(&V::I64(9)), Canonical::Int(9));
        assert_eq!(dec(&V::F64(1.5)), Canonical::Float(1.5));
        assert_eq!(dec(&V::String("hi".into())), Canonical::Text("hi".into()));
        assert_eq!(
            dec(&V::Binary(alloc::vec![1u8, 2, 3].into())),
            Canonical::Bytes(alloc::vec![1, 2, 3])
        );
        assert_eq!(
            dec(&V::Binary(alloc::vec::Vec::<u8>::new().into())),
            Canonical::Bytes(alloc::vec![])
        );
    }

    /// End to end: a typed diesel query filtering a blob column registers
    /// through `render_typed` plus placeholder resolution and matches a CDC
    /// row carrying the same bytes, while a row with different bytes does
    /// not. Depends on the `Value::Bytes` placeholder-resolution path.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn register_typed_blob_matches_sqlite() {
        use crate::backend::SQLite;
        use crate::SubscriptionEngine;
        use diesel::sqlite::Sqlite;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::SQLiteDialect;

        let catalog = ParserDB::parse::<SQLiteDialect>(
            "CREATE TABLE blobs (id INTEGER PRIMARY KEY, name TEXT, payload BLOB);",
        )
        .expect("catalog");
        let table_id = crate::catalog_helpers::table_id(&catalog, "blobs").expect("blobs table");
        let mut engine = SubscriptionEngine::<TestEvent<SQLite>, crate::DefaultIds, _>::new(
            catalog,
            SQLiteDialect {},
        );

        let bytes = alloc::vec![0xde, 0xad, 0xbe, 0xef];
        let query = blobs::table.filter(blobs::payload.eq(bytes.clone()));
        engine
            .register_select_typed::<Sqlite, _>(1, &query)
            .expect("register typed blob");

        let hit = TestEvent::<SQLite>::insert(
            table_id,
            alloc::vec![Value::Int(1), Value::Null, Value::Bytes(bytes)],
        )
        .with_pk_columns([0u16]);
        assert_eq!(
            engine.consumers(&hit).expect("dispatch hit").inserted(),
            &[1u64]
        );

        let miss = TestEvent::<SQLite>::insert(
            table_id,
            alloc::vec![Value::Int(2), Value::Null, Value::Bytes(alloc::vec![0x00])],
        )
        .with_pk_columns([0u16]);
        assert!(
            engine
                .consumers(&miss)
                .expect("dispatch miss")
                .inserted()
                .is_empty(),
            "wrong payload bytes notified no consumer"
        );
    }

    /// A typed SQLite UPDATE with a blob predicate follows its target rows.
    /// The SET bind (`name = 'x'`) is trimmed from the front while the blob
    /// WHERE bind survives `skip(set_bind_count)` and placeholder
    /// resolution, so the derived follow SELECT matches a row carrying those
    /// bytes and rejects one that does not. Also the only SQLite
    /// update-follow coverage.
    #[cfg(feature = "diesel-typed-sqlite")]
    #[test]
    fn register_typed_update_follow_blob_sqlite() {
        use crate::backend::SQLite;
        use crate::SubscriptionEngine;
        use diesel::sqlite::Sqlite;
        use sql_traits::structs::ParserDB;
        use sqlparser::dialect::SQLiteDialect;

        let catalog = ParserDB::parse::<SQLiteDialect>(
            "CREATE TABLE blobs (id INTEGER PRIMARY KEY, name TEXT, payload BLOB);",
        )
        .expect("catalog");
        let table_id = crate::catalog_helpers::table_id(&catalog, "blobs").expect("blobs table");
        let mut engine = SubscriptionEngine::<TestEvent<SQLite>, crate::DefaultIds, _>::new(
            catalog,
            SQLiteDialect {},
        );

        let bytes = alloc::vec![0xcau8, 0xfe];
        // UPDATE blobs SET name = 'x' WHERE payload = ? derives
        // SELECT * FROM blobs WHERE payload = ?, keeping the blob bind after
        // the SET text bind is trimmed.
        let update = diesel::update(blobs::table.filter(blobs::payload.eq(bytes.clone())))
            .set(blobs::name.eq("x"));
        engine
            .register_follow_update_typed::<Sqlite, _>(1, &update)
            .expect("register update follow");

        let hit = TestEvent::<SQLite>::insert(
            table_id,
            alloc::vec![
                Value::Int(1),
                Value::String("y".into()),
                Value::Bytes(bytes)
            ],
        )
        .with_pk_columns([0u16]);
        assert_eq!(
            engine.consumers(&hit).expect("dispatch hit").inserted(),
            &[1u64]
        );

        let miss = TestEvent::<SQLite>::insert(
            table_id,
            alloc::vec![
                Value::Int(2),
                Value::String("y".into()),
                Value::Bytes(alloc::vec![0x00])
            ],
        )
        .with_pk_columns([0u16]);
        assert!(
            engine
                .consumers(&miss)
                .expect("dispatch miss")
                .inserted()
                .is_empty(),
            "wrong payload bytes notified no consumer"
        );
    }
}
