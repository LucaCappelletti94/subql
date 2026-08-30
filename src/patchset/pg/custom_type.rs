//! Custom user-defined type dispatch for the Postgres patchset adapter.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::marker::PhantomData;

use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::result::QueryResult;
use diesel::serialize::ToSql;
use diesel::sql_types::HasSqlType;
use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::{Adapter, Binder, DefaultBinder, Value};

use super::PgAdapter;

/// A caller-supplied native bind for one Postgres user-defined type,
/// such as an `ENUM` or a `DOMAIN`, that [`PgAdapter`] cannot bind on its
/// own (a text bind to a custom type fails without a SQL cast).
///
/// Register an implementation on a [`CustomTypePgAdapter`] under the
/// type's declared name. When a column of that type is applied, the
/// wrapper calls [`bind`](PgCustomBinder::bind) with the carried wire
/// value. `Value::Null` never reaches here, since the wrapper binds it as
/// a literal NULL before dispatching. The returned [`Binder`] pushes a
/// native bind carrying the type's OID, which diesel resolves from a
/// `SqlType` marked `#[diesel(postgres_type(name = "..."))]`. No SQL cast
/// is emitted.
///
/// Use [`bind_as`] to build the returned binder without hand-writing a
/// [`Binder`] impl.
pub trait PgCustomBinder<S, B>: Send + Sync {
    /// Bind a carried (non-null) wire value as this Postgres type.
    ///
    /// # Errors
    ///
    /// Return an error when the wire value cannot represent the target
    /// type, for example an unknown enum label or a non-text wire shape.
    /// The error rolls the apply transaction back.
    fn bind<'a>(&self, value: &'a Value<S, B>) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>>;
}

/// Box a native diesel bind of `value` as the Postgres `SqlType` `ST`,
/// with no SQL cast.
///
/// The building block for a [`PgCustomBinder`] implementation: given a
/// diesel `SqlType` `ST` (marked `#[diesel(postgres_type(name = "..."))]`)
/// and a value that serializes to it through [`ToSql`], this pushes a
/// bind parameter carrying the type's OID, which diesel resolves from the
/// type name at execute time.
#[must_use]
pub fn bind_as<ST, U>(value: U) -> Box<dyn Binder<Pg> + Send>
where
    ST: 'static,
    U: ToSql<ST, Pg> + Send + 'static,
    Pg: HasSqlType<ST>,
{
    Box::new(CustomBinder::<ST, U> {
        value,
        _marker: PhantomData,
    })
}

/// Binder produced by [`bind_as`]: pushes `value` as the `SqlType` `ST`.
struct CustomBinder<ST, U> {
    value: U,
    _marker: PhantomData<fn() -> ST>,
}

impl<ST, U> Binder<Pg> for CustomBinder<ST, U>
where
    U: ToSql<ST, Pg>,
    Pg: HasSqlType<ST>,
{
    fn walk<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_bind_param::<ST, U>(&self.value)
    }
}

/// [`PgAdapter`] wrapped with dispatch for Postgres user-defined types
/// (`ENUM`s, `DOMAIN`s, and other custom types) that need a
/// caller-supplied native bind.
///
/// [`PgAdapter`] natively binds the built-in scalar families (bool, uuid,
/// numeric, temporal, json), but it cannot bind a custom type, since a
/// native bind needs a diesel `SqlType` and [`ToSql`] the caller owns.
/// This wrapper resolves each column's declared type name from the
/// catalog and, when a [`PgCustomBinder`] is registered under that name
/// (matched case-insensitively, mirroring Postgres unquoted-identifier
/// folding), dispatches the column to it. Every other column, and
/// `Value::Null` on a registered column, delegates to the inner
/// [`PgAdapter`].
///
/// ```
/// use diesel::pg::Pg;
/// use diesel::result::QueryResult;
/// use diesel::serialize::{self, IsNull, Output, ToSql};
/// use std::io::Write;
/// use sql_traits::structs::ParserDB;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use sqlite_diff_rs::{Adapter, Binder, Value};
/// use subql::patchset::{bind_as, CustomTypePgAdapter, PgCustomBinder};
///
/// // A diesel SqlType naming the Postgres enum, so diesel resolves its
/// // OID and binds natively.
/// #[derive(diesel::sql_types::SqlType)]
/// #[diesel(postgres_type(name = "mood"))]
/// struct MoodType;
///
/// // A local value type carrying the enum label, serialized as the raw
/// // bytes Postgres expects for the enum.
/// #[derive(Debug)]
/// struct Mood(String);
/// impl ToSql<MoodType, Pg> for Mood {
///     fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
///         out.write_all(self.0.as_bytes())?;
///         Ok(IsNull::No)
///     }
/// }
///
/// // The registered rule: a mood column's wire text becomes a native bind.
/// struct MoodBinder;
/// impl PgCustomBinder<String, Vec<u8>> for MoodBinder {
///     fn bind<'a>(
///         &self,
///         value: &'a Value<String, Vec<u8>>,
///     ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
///         match value {
///             Value::Text(label) => Ok(bind_as::<MoodType, Mood>(Mood(label.clone()))),
///             _ => Err(diesel::result::Error::QueryBuilderError(
///                 "mood column expects TEXT".into(),
///             )),
///         }
///     }
/// }
///
/// let catalog = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE TABLE t (id INT PRIMARY KEY, feeling mood);",
/// )
/// .expect("valid DDL");
/// let adapter = CustomTypePgAdapter::new(&catalog).register("mood", MoodBinder);
///
/// // The `feeling` column (index 1) routes to the registered binder.
/// assert!(adapter.bind("t", 1, &Value::Text("happy".to_owned())).is_ok());
/// // A non-text wire shape on the enum column is refused.
/// assert!(adapter
///     .bind("t", 1, &Value::<String, Vec<u8>>::Integer(1))
///     .is_err());
/// ```
pub struct CustomTypePgAdapter<'db, DB: DatabaseLike, S, B> {
    inner: PgAdapter<'db, DB>,
    binders: Vec<(String, Box<dyn PgCustomBinder<S, B>>)>,
}

impl<'db, DB: DatabaseLike, S, B> CustomTypePgAdapter<'db, DB, S, B> {
    /// Build a wrapper around a fresh [`PgAdapter`] borrowing `catalog`,
    /// with no custom types registered yet.
    #[must_use]
    pub const fn new(catalog: &'db DB) -> Self {
        Self {
            inner: PgAdapter::new(catalog),
            binders: Vec::new(),
        }
    }

    /// Register `binder` for every column whose declared catalog type
    /// name equals `type_name` (matched case-insensitively). Chainable.
    #[must_use]
    pub fn register(
        mut self,
        type_name: impl Into<String>,
        binder: impl PgCustomBinder<S, B> + 'static,
    ) -> Self {
        self.binders.push((type_name.into(), Box::new(binder)));
        self
    }
}

impl<DB, S, B> Adapter<Pg, S, B> for CustomTypePgAdapter<'_, DB, S, B>
where
    DB: DatabaseLike,
    S: AsRef<str> + Sync,
    B: AsRef<[u8]> + Sync,
{
    fn column_name(&self, table_name: &str, column_index: usize) -> &str {
        <PgAdapter<'_, DB> as Adapter<Pg, S, B>>::column_name(&self.inner, table_name, column_index)
    }

    fn bind<'a>(
        &self,
        table_name: &str,
        column_index: usize,
        value: &'a Value<S, B>,
    ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
        let custom = self
            .inner
            .column_type_name(table_name, column_index)
            .and_then(|type_name| {
                self.binders
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(type_name.as_ref()))
            });
        match custom {
            Some((_, binder)) => match value {
                Value::Null => Ok(Box::new(DefaultBinder::from(value))),
                other => binder.bind(other),
            },
            None => self.inner.bind(table_name, column_index, value),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{bind_as, CustomTypePgAdapter, PgCustomBinder};
    use alloc::borrow::ToOwned;
    use alloc::boxed::Box;
    use alloc::string::String;
    use alloc::vec::Vec;
    use diesel::pg::Pg;
    use diesel::result::{Error as DieselError, QueryResult};
    use diesel::serialize::{self, IsNull, Output, ToSql};
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{Adapter, Binder, Value};
    use sqlparser::dialect::PostgreSqlDialect;
    use std::io::Write;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "mood"))]
    struct MoodType;

    #[derive(Debug)]
    struct Mood(String);

    impl ToSql<MoodType, Pg> for Mood {
        fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
            out.write_all(self.0.as_bytes())?;
            Ok(IsNull::No)
        }
    }

    // Only "happy" is a valid label, so any other text is refused, which
    // proves the wrapper routes the column here rather than to the inner
    // adapter's text fall-through.
    struct MoodBinder;

    impl PgCustomBinder<String, Vec<u8>> for MoodBinder {
        fn bind<'a>(
            &self,
            value: &'a Value<String, Vec<u8>>,
        ) -> QueryResult<Box<dyn Binder<Pg> + Send + 'a>> {
            match value {
                Value::Text(s) if s == "happy" => Ok(bind_as::<MoodType, Mood>(Mood(s.clone()))),
                Value::Text(_) => Err(DieselError::QueryBuilderError("unknown mood label".into())),
                _ => Err(DieselError::QueryBuilderError(
                    "feeling expects TEXT".into(),
                )),
            }
        }
    }

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, active BOOLEAN, feeling mood);",
        )
        .unwrap()
    }

    fn adapter(db: &ParserDB) -> CustomTypePgAdapter<'_, ParserDB, String, Vec<u8>> {
        CustomTypePgAdapter::new(db).register("mood", MoodBinder)
    }

    #[test]
    fn registered_type_routes_to_custom_binder() {
        let db = catalog();
        let a = adapter(&db);
        // feeling is column index 2, declared type `mood`.
        assert!(a
            .bind("orders", 2, &Value::Text("happy".to_owned()))
            .is_ok());
    }

    #[test]
    fn unknown_label_is_refused_by_the_custom_binder() {
        let db = catalog();
        let a = adapter(&db);
        assert!(a
            .bind("orders", 2, &Value::Text("elated".to_owned()))
            .is_err());
    }

    #[test]
    fn null_on_registered_column_binds_as_literal_null() {
        let db = catalog();
        let a = adapter(&db);
        assert!(a.bind("orders", 2, &Value::<String, Vec<u8>>::Null).is_ok());
    }

    #[test]
    fn type_name_match_is_case_insensitive() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, feeling MOOD);",
        )
        .unwrap();
        let a = adapter(&db);
        // feeling is column index 1 here, declared type `MOOD`.
        assert!(a
            .bind("orders", 1, &Value::Text("happy".to_owned()))
            .is_ok());
        assert!(a
            .bind("orders", 1, &Value::Text("elated".to_owned()))
            .is_err());
    }

    #[test]
    fn unregistered_column_delegates_to_inner_adapter() {
        let db = catalog();
        let a = adapter(&db);
        // `active` is BOOLEAN: the inner adapter accepts Integer and
        // refuses TEXT, so delegation is observable.
        assert!(a.bind("orders", 1, &Value::Integer(1)).is_ok());
        assert!(a.bind("orders", 1, &Value::Text("x".to_owned())).is_err());
    }
}
