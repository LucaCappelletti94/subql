//! The membership term's vocabulary: what the compiler lifted out of a filter,
//! and how the subscriber lookup it needs is keyed.
//!
//! A membership subquery answers "which subscribers does this changed row
//! reach" by reading one column off the row and looking the value up. That
//! makes the value a map key, and [`Value`] cannot be one: [`ScalarCore`]
//! bounds equality at `PartialEq` because a float is not equal to itself,
//! and `Missing` and `Null` are not values at all.
//!
//! [`TermKey`] is the subset that can be a key. The three scalars it omits are
//! refused at registration, which is what makes its `Eq` honest rather than
//! asserted: nothing here claims reflexivity for a type that lacks it.
//!
//! [`ScalarCore`]: crate::backend::ScalarCore

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};
use core::mem::discriminant;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::Expr;

use crate::backend::{Backend, ScalarKind, Value};
use crate::{catalog_helpers, ColumnId, RegisterError, TableId};

/// One membership term the compiler lifted out of a filter.
///
/// The compiled program carries only the slot, because that is all the VM
/// needs. Registration needs the expression, to ask whether the relationship
/// can be served at all, and the column, to group the subscriber's own starting
/// values and to read the changed row later.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledTerm {
    /// The slot [`Instruction::TermTruth`] names, and the index into the
    /// program's `term_columns`.
    ///
    /// [`Instruction::TermTruth`]: crate::compiler::Instruction::TermTruth
    pub slot: u16,
    /// The column of the subscribed table the term compares.
    pub column: ColumnId,
    /// The whole `<column> IN (SELECT ...)` expression, which is what decides
    /// whether the relationship it names can be served.
    pub expr: Expr,
}

impl CompiledTerm {
    /// Whether this term compares the caller directly rather than through a
    /// membership subquery.
    ///
    /// The distinction decides how the term seeds: a caller comparison admits
    /// exactly the subscriber the request states, so it seeds itself and takes
    /// no stated values, and nothing ever moves its set.
    #[must_use]
    pub fn compares_the_caller(&self) -> bool {
        crate::compiler::sql_shape::is_caller_comparison(&self.expr)
    }
}

/// What registration settled about one term, beyond what the compiler saw.
///
/// The compiler knows which column the filter compares. Whether the
/// relationship can be served, and which table's rows move it, is what
/// `rls2fga` answers, and this is that answer resolved to subql's own ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TermPlan {
    /// The slot this plan belongs to.
    pub slot: u16,
    /// The column of the subscribed table the term compares.
    pub column: ColumnId,
    /// The rows that move which subscribers the term admits.
    ///
    /// `None` for a caller comparison: its set is the subscriber itself, and an
    /// identity does not change, so no table's rows are watched for it.
    pub moved_by: Option<TermMovement>,
}

/// The table and columns whose changed rows move a membership term's set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TermMovement {
    /// The table whose changed rows move which subscribers the term admits.
    ///
    /// The subscribed table itself when the filter names the caller from its
    /// own rows, and the membership table when it names the caller through a
    /// related row. One walk covers both, because in each case it is the table
    /// the shape that names a caller reads.
    pub member_table: TableId,
    /// The column of `member_table` carrying the value the term's compared
    /// column is compared against.
    pub member_key: ColumnId,
    /// The column of `member_table` naming the subscriber a row admits.
    pub member_subject: ColumnId,
}

/// What a caller has to read, and at which kinds, to seed one membership term
/// before registering the filter that names it.
///
/// [`TermPlan`] in the catalog's own words, plus the seed read itself.
/// Registration consumes the seed and an absent one admits nobody, so the caller
/// needs this before it registers, and deriving it a second time from the SQL
/// leaves two readers of one text that can disagree.
///
/// Answered by
/// [`SubscriptionEngine::describe_terms`](crate::SubscriptionEngine::describe_terms).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TermDescription {
    /// The compared column of the subscribed table, by catalog name.
    ///
    /// The name
    /// [`SubscriptionRequest::term_values`](crate::SubscriptionRequest::term_values)
    /// is keyed by, since the compared column is what both sides share.
    pub column: String,
    /// The table whose changed rows move which subscribers the term admits, by
    /// catalog name.
    pub member_table: String,
    /// The column of `member_table` [`seed_sql`](Self::seed_sql) projects.
    pub member_key: String,
    /// The column of `member_table` naming the subscriber a row admits.
    pub member_subject: String,
    /// The kind the subscriber value has to be built at.
    ///
    /// [`TermKey`] keys a string and a UUID under different variants, so an
    /// identity supplied at another kind matches no membership row and admits
    /// nobody in silence.
    pub subject_kind: ScalarKind,
    /// The kind [`seed_sql`](Self::seed_sql)'s one column decodes as, which is
    /// also the kind [`column`](Self::column) holds.
    pub key_kind: ScalarKind,
    /// The seed read: the membership subquery itself, one column, the values
    /// this subscriber currently matches.
    ///
    /// Run it as the caller, because it names the caller the way the snapshot
    /// does, then state what came back under [`column`](Self::column). Reading
    /// the membership table is what makes the seed complete: a snapshot of the
    /// subscribed table omits every value whose rows do not exist yet, and no
    /// later membership change repairs that, because the membership did not
    /// change.
    pub seed_sql: String,
}

impl TermDescription {
    /// Resolve `plan` and the term it was settled from against the catalog.
    ///
    /// # Errors
    ///
    /// [`RegisterError::MembershipTermRefused`] when the catalog does not name
    /// one of the plan's ids, or when the term was lifted from a shape whose
    /// seed read cannot be named.
    pub(crate) fn resolve<DB: DatabaseLike>(
        plan: &TermPlan,
        term: &CompiledTerm,
        table: TableId,
        database: &DB,
    ) -> Result<Self, RegisterError> {
        // A term compiles from `IN (SELECT ...)` alone, whose inner query the
        // compiler bounds to one projected column. A shape a later compiler
        // lifts a term from is refused rather than served with no seed read,
        // since an unseeded term admits nobody in silence.
        let Expr::InSubquery { subquery, .. } = &term.expr else {
            return Err(RegisterError::MembershipTermRefused(
                "this membership term was not lifted from an IN (SELECT ...), so SubQL cannot say \
                 which read seeds it"
                    .into(),
            ));
        };
        // A caller comparison seeds itself from the subscriber, so no read
        // describes it. `describe_terms` skips such plans, and this is what a
        // later caller of `resolve` gets instead of a fabricated read.
        let Some(movement) = plan.moved_by else {
            return Err(RegisterError::MembershipTermRefused(
                "this membership term seeds itself from the subscriber, so there is no read to \
                 describe"
                    .into(),
            ));
        };

        Ok(Self {
            column: name(database, table, plan.column)?,
            member_table: table_name_of(database, movement.member_table)?,
            member_key: name(database, movement.member_table, movement.member_key)?,
            member_subject: name(database, movement.member_table, movement.member_subject)?,
            subject_kind: kind(database, movement.member_table, movement.member_subject)?,
            key_kind: kind(database, table, plan.column)?,
            seed_sql: subquery.to_string(),
        })
    }
}

/// The tail every refusal below shares.
const CANNOT_SAY: &str = "so SubQL cannot say what seeds the membership subquery comparing it";

/// A table's catalog name, or the refusal for a table the catalog forgot.
fn table_name_of<DB: DatabaseLike>(database: &DB, table: TableId) -> Result<String, RegisterError> {
    catalog_helpers::table_name(database, table).ok_or_else(|| {
        RegisterError::MembershipTermRefused(alloc::format!(
            "table {table} is not in the catalog under a name, {CANNOT_SAY}"
        ))
    })
}

/// A column's catalog name, or the refusal for a column the catalog forgot.
fn name<DB: DatabaseLike>(
    database: &DB,
    table: TableId,
    column: ColumnId,
) -> Result<String, RegisterError> {
    catalog_helpers::column_name(database, table, column).ok_or_else(|| {
        RegisterError::MembershipTermRefused(alloc::format!(
            "column {column} of table {table} is not in the catalog under a name, {CANNOT_SAY}"
        ))
    })
}

/// A column's scalar kind, or the refusal for one the catalog cannot type.
fn kind<DB: DatabaseLike>(
    database: &DB,
    table: TableId,
    column: ColumnId,
) -> Result<ScalarKind, RegisterError> {
    catalog_helpers::column_scalar_kind(database, table, column).ok_or_else(|| {
        RegisterError::MembershipTermRefused(alloc::format!(
            "column {column} of table {table} holds a SQL type SubQL cannot read, {CANNOT_SAY}"
        ))
    })
}

/// The columns `terms` compare, indexed by slot.
///
/// The compiler assigns slots densely from zero in first-occurrence order, so
/// this is a reindexing rather than a search, and it is the table dispatch
/// reads a changed row through.
#[must_use]
pub fn term_columns(terms: &[CompiledTerm]) -> Vec<ColumnId> {
    let mut columns = alloc::vec![0; terms.len()];
    for term in terms {
        if let Some(slot) = columns.get_mut(usize::from(term.slot)) {
            *slot = term.column;
        }
    }
    columns
}

/// A changed row's link value, in the form the subscriber lookup is keyed by.
///
/// Ten of [`Value`]'s fifteen variants. `Float`, `Json` and `Jsonb` are absent
/// because their equality is not reflexive, and `Missing` and `Null` are absent
/// because they are not values (see [`TermLookup`]).
pub enum TermKey<B: Backend> {
    /// [`Backend::Bool`] payload.
    Bool(B::Bool),
    /// [`Backend::Int`] payload.
    Int(B::Int),
    /// [`Backend::String`] payload.
    String(B::String),
    /// [`Backend::Bytes`] payload.
    Bytes(B::Bytes),
    /// [`Backend::Uuid`] payload.
    Uuid(B::Uuid),
    /// [`Backend::Timestamp`] payload.
    Timestamp(B::Timestamp),
    /// [`Backend::TimestampTz`] payload.
    TimestampTz(B::TimestampTz),
    /// [`Backend::Date`] payload.
    Date(B::Date),
    /// [`Backend::Time`] payload.
    Time(B::Time),
    /// [`Backend::Decimal`] payload.
    Decimal(B::Decimal),
}

// `Clone`, `Debug`, `PartialEq`, `Eq` and `Hash` are hand-implemented for the
// same reason `Value<B>`'s are: a derive would put the bound on `B`, the
// backend marker, rather than on the scalar types it names, so a third-party
// marker lacking those derives would silently lose them here.
impl<B: Backend> Clone for TermKey<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Bool(v) => Self::Bool(v.clone()),
            Self::Int(v) => Self::Int(v.clone()),
            Self::String(v) => Self::String(v.clone()),
            Self::Bytes(v) => Self::Bytes(v.clone()),
            Self::Uuid(v) => Self::Uuid(v.clone()),
            Self::Timestamp(v) => Self::Timestamp(v.clone()),
            Self::TimestampTz(v) => Self::TimestampTz(v.clone()),
            Self::Date(v) => Self::Date(v.clone()),
            Self::Time(v) => Self::Time(v.clone()),
            Self::Decimal(v) => Self::Decimal(v.clone()),
        }
    }
}

impl<B: Backend> core::fmt::Debug for TermKey<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Bool(v) => f.debug_tuple("Bool").field(v).finish(),
            Self::Int(v) => f.debug_tuple("Int").field(v).finish(),
            Self::String(v) => f.debug_tuple("String").field(v).finish(),
            Self::Bytes(v) => f.debug_tuple("Bytes").field(v).finish(),
            Self::Uuid(v) => f.debug_tuple("Uuid").field(v).finish(),
            Self::Timestamp(v) => f.debug_tuple("Timestamp").field(v).finish(),
            Self::TimestampTz(v) => f.debug_tuple("TimestampTz").field(v).finish(),
            Self::Date(v) => f.debug_tuple("Date").field(v).finish(),
            Self::Time(v) => f.debug_tuple("Time").field(v).finish(),
            Self::Decimal(v) => f.debug_tuple("Decimal").field(v).finish(),
        }
    }
}

impl<B: Backend> TermKey<B> {
    /// The value this key was read from.
    ///
    /// Lossless, since every variant here is a variant of [`Value`]. Used to
    /// report a value back to the caller, which speaks [`Value`] everywhere
    /// else and would otherwise have to match a second enum to bind one.
    #[must_use]
    pub fn into_value(self) -> Value<B> {
        match self {
            Self::Bool(v) => Value::Bool(v),
            Self::Int(v) => Value::Int(v),
            Self::String(v) => Value::String(v),
            Self::Bytes(v) => Value::Bytes(v),
            Self::Uuid(v) => Value::Uuid(v),
            Self::Timestamp(v) => Value::Timestamp(v),
            Self::TimestampTz(v) => Value::TimestampTz(v),
            Self::Date(v) => Value::Date(v),
            Self::Time(v) => Value::Time(v),
            Self::Decimal(v) => Value::Decimal(v),
        }
    }

    /// The kind this key holds.
    ///
    /// A caller comparison refuses a subscriber whose kind is not the compared
    /// column's: the lookup matches by variant before value, so a mismatched
    /// kind would never find what it stored and the subscription would be
    /// served dead.
    #[must_use]
    pub const fn scalar_kind(&self) -> ScalarKind {
        match self {
            Self::Bool(_) => ScalarKind::Bool,
            Self::Int(_) => ScalarKind::Int,
            Self::String(_) => ScalarKind::String,
            Self::Bytes(_) => ScalarKind::Bytes,
            Self::Uuid(_) => ScalarKind::Uuid,
            Self::Timestamp(_) => ScalarKind::Timestamp,
            Self::TimestampTz(_) => ScalarKind::TimestampTz,
            Self::Date(_) => ScalarKind::Date,
            Self::Time(_) => ScalarKind::Time,
            Self::Decimal(_) => ScalarKind::Decimal,
        }
    }
}

impl<B: Backend> PartialEq for TermKey<B> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
            (Self::Timestamp(a), Self::Timestamp(b)) => a == b,
            (Self::TimestampTz(a), Self::TimestampTz(b)) => a == b,
            (Self::Date(a), Self::Date(b)) => a == b,
            (Self::Time(a), Self::Time(b)) => a == b,
            (Self::Decimal(a), Self::Decimal(b)) => a == b,
            _ => false,
        }
    }
}

// Sound because every scalar named above is `ScalarKey`, which requires `Eq`.
impl<B: Backend> Eq for TermKey<B> {}

impl<B: Backend> Hash for TermKey<B> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // The discriminant is part of the hash because two variants can share a
        // payload type: on MySQL and SQLite, `Uuid` and `String` are both
        // `String`, and the same text under each must not collide.
        discriminant(self).hash(state);
        match self {
            Self::Bool(v) => v.hash(state),
            Self::Int(v) => v.hash(state),
            Self::String(v) => v.hash(state),
            Self::Bytes(v) => v.hash(state),
            Self::Uuid(v) => v.hash(state),
            Self::Timestamp(v) => v.hash(state),
            Self::TimestampTz(v) => v.hash(state),
            Self::Date(v) => v.hash(state),
            Self::Time(v) => v.hash(state),
            Self::Decimal(v) => v.hash(state),
        }
    }
}

/// What a changed row's link value says about the subscribers a term admits.
///
/// Three outcomes rather than two, because `Null` and `Missing` mean different
/// things and collapsing them would turn "cannot say" into "no".
pub enum TermLookup<B: Backend> {
    /// Look the subscribers up under this key.
    Key(TermKey<B>),
    /// SQL `NULL`. `NULL IN (SELECT ...)` is never true, so the term admits
    /// nobody and the row reaches no subscriber through it.
    Nobody,
    /// The term cannot say: either the source did not carry the column, or the
    /// value is of a kind that cannot be a key. Propagates as
    /// [`Tri::Unknown`](crate::compiler::Tri) through the surrounding filter,
    /// which dispatch treats as not matching.
    Unknown,
}

impl<B: Backend> TermLookup<B> {
    /// Read a changed row's link value as a lookup.
    ///
    /// A `Float`, `Json` or `Jsonb` value answers [`TermLookup::Unknown`]. It
    /// cannot arrive, because a term comparing such a column is refused at
    /// registration, and answering "cannot say" rather than panicking keeps a
    /// registration bug from taking the dispatch loop down with it.
    pub fn of(value: Value<B>) -> Self {
        match value {
            Value::Bool(v) => Self::Key(TermKey::Bool(v)),
            Value::Int(v) => Self::Key(TermKey::Int(v)),
            Value::String(v) => Self::Key(TermKey::String(v)),
            Value::Bytes(v) => Self::Key(TermKey::Bytes(v)),
            Value::Uuid(v) => Self::Key(TermKey::Uuid(v)),
            Value::Timestamp(v) => Self::Key(TermKey::Timestamp(v)),
            Value::TimestampTz(v) => Self::Key(TermKey::TimestampTz(v)),
            Value::Date(v) => Self::Key(TermKey::Date(v)),
            Value::Time(v) => Self::Key(TermKey::Time(v)),
            Value::Decimal(v) => Self::Key(TermKey::Decimal(v)),
            Value::Null => Self::Nobody,
            Value::Missing | Value::Float(_) | Value::Json(_) | Value::Jsonb(_) => Self::Unknown,
        }
    }
}

/// Can a column of this kind be the one a membership term compares?
///
/// The three that cannot are exactly those whose equality is not reflexive, so
/// a lookup keyed on one could not find what it stored. Registration refuses a
/// term on such a column rather than serving one that answers inconsistently.
#[must_use]
pub const fn kind_can_key(kind: ScalarKind) -> bool {
    match kind {
        ScalarKind::Bool
        | ScalarKind::Int
        | ScalarKind::String
        | ScalarKind::Bytes
        | ScalarKind::Uuid
        | ScalarKind::Timestamp
        | ScalarKind::TimestampTz
        | ScalarKind::Date
        | ScalarKind::Time
        | ScalarKind::Decimal => true,
        ScalarKind::Float | ScalarKind::Json | ScalarKind::Jsonb => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{kind_can_key, TermKey, TermLookup};
    use crate::backend::{Backend, MySql, Postgres, ScalarKind, Value};
    use hashbrown::HashMap;

    /// Both keys must go through the *same* hasher. `DefaultHashBuilder` is
    /// randomly seeded per instance, so hashing each key with its own builder
    /// compares two unrelated numbers and would pass for identical keys.
    fn hashes_alike<B: Backend>(left: &TermKey<B>, right: &TermKey<B>) -> bool {
        use core::hash::BuildHasher;
        let builder = hashbrown::DefaultHashBuilder::default();
        builder.hash_one(left) == builder.hash_one(right)
    }

    /// The point of the type: it can key a map, which `Value` cannot.
    #[test]
    fn a_term_key_keys_a_map() {
        let mut map: HashMap<TermKey<Postgres>, u32> = HashMap::new();
        map.insert(TermKey::Int(7), 1);
        map.insert(TermKey::String("proj-7".to_string()), 2);
        assert_eq!(map.get(&TermKey::Int(7)), Some(&1));
        assert_eq!(map.get(&TermKey::String("proj-7".to_string())), Some(&2));
        assert_eq!(map.get(&TermKey::Int(8)), None);
    }

    /// The same hasher gives the same answer twice, which is what makes the
    /// inequality asserted below mean something.
    #[test]
    fn the_hash_comparison_is_not_vacuous() {
        let key: TermKey<MySql> = TermKey::String("same".to_string());
        assert!(
            hashes_alike(&key, &key.clone()),
            "equal keys must hash alike, or the comparison proves nothing"
        );
    }

    /// On MySQL a UUID is carried as text, so two variants share one payload
    /// type. The same text under each must be two different keys, or a project
    /// id and a UUID spelled alike would share a subscriber set.
    #[test]
    fn two_variants_sharing_a_payload_type_are_different_keys() {
        let text = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".to_string();
        let as_uuid: TermKey<MySql> = TermKey::Uuid(text.clone());
        let as_string: TermKey<MySql> = TermKey::String(text);
        assert_ne!(as_uuid, as_string, "the variant is part of the identity");
        assert!(
            !hashes_alike(&as_uuid, &as_string),
            "the discriminant has to reach the hash, or the map collides them"
        );
    }

    /// `NULL` and a cell the source omitted are different answers. `NULL IN
    /// (...)` is never true, so it admits nobody; an absent cell cannot say.
    #[test]
    fn null_admits_nobody_and_a_missing_cell_cannot_say() {
        assert!(matches!(
            TermLookup::of(Value::<Postgres>::Null),
            TermLookup::Nobody
        ));
        assert!(matches!(
            TermLookup::of(Value::<Postgres>::Missing),
            TermLookup::Unknown
        ));
    }

    /// A kind that cannot key answers "cannot say" rather than keying on
    /// something whose equality is not reflexive.
    #[test]
    fn a_value_of_an_unkeyable_kind_cannot_say() {
        for value in [
            Value::<Postgres>::Float(1.0),
            Value::<Postgres>::Json(serde_json::Value::Null),
            Value::<Postgres>::Jsonb(serde_json::Value::Null),
        ] {
            assert!(
                matches!(TermLookup::of(value), TermLookup::Unknown),
                "an unkeyable kind must not become a key"
            );
        }
    }

    /// The registration gate and the runtime conversion agree on which kinds
    /// can key, for **every** kind. If they disagreed, a term would be accepted
    /// and then never match, or refused for a column that would have worked.
    ///
    /// Exhaustive on purpose. The `match` inside `kind_can_key` already makes it
    /// impossible to forget a kind, but nothing stops one being classified the
    /// wrong way, and a partial list here would not notice.
    #[test]
    fn the_registration_gate_matches_what_the_conversion_accepts() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).expect("epoch is a valid instant");
        let cases = [
            (ScalarKind::Bool, Value::<Postgres>::Bool(true)),
            (ScalarKind::Int, Value::Int(1)),
            (ScalarKind::Float, Value::Float(1.0)),
            (ScalarKind::String, Value::String("x".to_string())),
            (ScalarKind::Bytes, Value::Bytes(vec![1])),
            (ScalarKind::Uuid, Value::Uuid(uuid::Uuid::nil())),
            (ScalarKind::Timestamp, Value::Timestamp(epoch.naive_utc())),
            (ScalarKind::TimestampTz, Value::TimestampTz(epoch)),
            (ScalarKind::Date, Value::Date(epoch.date_naive())),
            (ScalarKind::Time, Value::Time(epoch.time())),
            (
                ScalarKind::Decimal,
                Value::Decimal(bigdecimal::BigDecimal::from(1)),
            ),
            (ScalarKind::Json, Value::Json(serde_json::Value::Null)),
            (ScalarKind::Jsonb, Value::Jsonb(serde_json::Value::Null)),
        ];
        assert_eq!(cases.len(), 13, "every ScalarKind must appear here");
        for (kind, value) in cases {
            let lookup = TermLookup::of(value);
            let keyed = matches!(lookup, TermLookup::Key(_));
            assert_eq!(
                kind_can_key(kind),
                keyed,
                "{kind:?} disagrees between the gate and the conversion"
            );
            // The key's own kind is the kind the value was built at, which is
            // what the caller-comparison refusal compares against the column.
            if let TermLookup::Key(key) = lookup {
                assert_eq!(key.scalar_kind(), kind, "the key changed kind in transit");
            }
        }
    }

    /// `TermKey`'s `Eq` is sound only because every scalar it carries is
    /// [`ScalarKey`], and `Eq` is a marker trait, so `impl Eq for TermKey<B> {}`
    /// would go on compiling even if that stopped being true. This ties the two
    /// together: drop `Eq` from `ScalarKey` and this stops compiling, which is
    /// the only way a type-level claim can be defended.
    #[test]
    fn scalar_key_still_carries_the_equality_term_key_asserts() {
        fn needs_eq_and_hash<T: Eq + core::hash::Hash>() {}
        fn every_scalar_key<T: crate::backend::ScalarKey>() {
            needs_eq_and_hash::<T>();
        }
        every_scalar_key::<<Postgres as Backend>::Int>();
        every_scalar_key::<<Postgres as Backend>::String>();
        every_scalar_key::<<Postgres as Backend>::Uuid>();
        every_scalar_key::<<Postgres as Backend>::Decimal>();
        every_scalar_key::<<MySql as Backend>::Uuid>();
    }
}
