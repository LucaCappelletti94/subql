//! Keying a membership term's subscriber lookup.
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

use core::hash::{Hash, Hasher};
use core::mem::discriminant;

use crate::backend::{Backend, ScalarKind, Value};

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
            let keyed = matches!(TermLookup::of(value), TermLookup::Key(_));
            assert_eq!(
                kind_can_key(kind),
                keyed,
                "{kind:?} disagrees between the gate and the conversion"
            );
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
