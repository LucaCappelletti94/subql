//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

use super::{Backend, MySql, Postgres, SQLite, SqliteJson, SqliteJsonStorage};
use alloc::string::ToString;

/// Runtime tag naming either one builtin SQL type or one custom type.
///
/// `C` names the embedder's own scalar types. The builtin position carries
/// subql's own [`BuiltinType`], not the family sql-traits classifies into:
/// a family is what three engines share, while a type is what one column
/// declares, and the difference between them is where answers diverge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ScalarKind<C> {
    /// One builtin SQL type, refinements included.
    Builtin(#[serde(with = "builtin_type_serde")] BuiltinType),
    /// One of the embedder's own types.
    Custom(C),
}

/// Builtin scalar *families* are classified and owned by sql-traits. They
/// are the coarse question ("is this column numeric at all"), and an exact
/// type answers it through [`BuiltinType::family`].
pub type BuiltinKind = sql_traits::utils::scalar_family::ScalarFamily;

/// The width a declared floating-point type fixes.
///
/// Measured: PostgreSQL's `real` and MySQL's `FLOAT` are float4, their
/// `double precision` and `DOUBLE` are float8, and SQLite has one `REAL`
/// which is float8. The width decides what the wire text means, what an
/// expression computes in, and what an aggregate accumulates in, so it
/// belongs to the type rather than to each of those layers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum FloatWidth {
    /// `real`, `float4`, MySQL `FLOAT`.
    Single,
    /// `double precision`, `float8`, MySQL `DOUBLE`, SQLite `REAL`.
    Double,
}

/// How wide a declared integer type is.
///
/// It decides what a sum of that column answers, measured on PostgreSQL
/// 16.15: `sum(smallint)` and `sum(int)` are `bigint`, so an exact total
/// fits a 64-bit integer and overflows past it, while `sum(bigint)` is
/// `numeric`, which has no 64-bit boundary at all. MySQL sums every width
/// into a decimal and SQLite has one 64-bit integer, so only one engine
/// reads this, and it reads it for every integer column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IntWidth {
    /// `smallint`, `integer`, and every narrower spelling: at most 32
    /// bits, so a wider accumulator can hold any total of them exactly.
    UpToThirtyTwo,
    /// `bigint`: 64 bits, so no fixed-width integer accumulator can
    /// promise to hold a total of them.
    SixtyFour,
}

/// Whether a declared character type is fixed width.
///
/// A fixed-width column is padded out to its width on write, which decides
/// how its trailing spaces compare. The width itself is not available and
/// not needed: `sql-traits` canonicalizes `CHARACTER(5)` to `CHAR`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TextWidth {
    /// `char(n)`, `character(n)`, `bpchar`, `nchar`.
    Fixed,
    /// `varchar(n)`, `text`, and every other varying spelling.
    Varying,
}

/// One builtin SQL type as a column declares it.
///
/// Exhaustive on purpose. A refinement carried in a variant cannot be
/// forgotten by a new match arm, which is what a width read out of a
/// declared-type string per layer could not promise.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BuiltinType {
    /// Boolean.
    Bool,
    /// Signed or unsigned integer, at the width the declaration fixes.
    Int(IntWidth),
    /// Floating point, at the width the declaration fixes.
    Float(FloatWidth),
    /// Exact decimal.
    Decimal,
    /// Text, fixed or varying width.
    Text(TextWidth),
    /// Binary.
    Bytes,
    /// UUID.
    Uuid,
    /// Calendar date.
    Date,
    /// Time of day.
    Time,
    /// Timestamp without a timezone.
    Timestamp,
    /// Timestamp with a timezone.
    TimestampTz,
    /// JSON.
    Json,
    /// Binary JSON.
    Jsonb,
}

impl BuiltinType {
    /// The family this type belongs to, for the questions that are genuinely
    /// coarse: whether a column can be folded, which wire type it emits.
    #[must_use]
    pub const fn family(self) -> BuiltinKind {
        match self {
            Self::Bool => BuiltinKind::Bool,
            Self::Int(_) => BuiltinKind::Int,
            Self::Float(_) => BuiltinKind::Float,
            Self::Decimal => BuiltinKind::Decimal,
            Self::Text(_) => BuiltinKind::String,
            Self::Bytes => BuiltinKind::Bytes,
            Self::Uuid => BuiltinKind::Uuid,
            Self::Date => BuiltinKind::Date,
            Self::Time => BuiltinKind::Time,
            Self::Timestamp => BuiltinKind::Timestamp,
            Self::TimestampTz => BuiltinKind::TimestampTz,
            Self::Json => BuiltinKind::Json,
            Self::Jsonb => BuiltinKind::Jsonb,
        }
    }

    /// The float width this type fixes, or `None` when it is not a float.
    #[must_use]
    pub const fn float_width(self) -> Option<FloatWidth> {
        match self {
            Self::Float(width) => Some(width),
            _ => None,
        }
    }

    /// The integer width this type fixes, or `None` when it is not an
    /// integer.
    #[must_use]
    pub const fn int_width(self) -> Option<IntWidth> {
        match self {
            Self::Int(width) => Some(width),
            _ => None,
        }
    }

    /// Whether this type is a fixed-width character type.
    #[must_use]
    pub const fn is_fixed_width_text(self) -> bool {
        matches!(self, Self::Text(TextWidth::Fixed))
    }
}

/// Whether a declared integer type is 64 bits wide.
///
/// Shared because the spelling is nearly the same everywhere: PostgreSQL
/// writes `bigint`/`int8`, MySQL `bigint`, and SQLite calls its one
/// 64-bit integer `INTEGER`, which each backend resolves for itself.
#[must_use]
pub fn declares_sixty_four_bit_int(declared_type: &str) -> IntWidth {
    let declared = declared_type.trim();
    if ["bigint", "int8", "bigserial", "serial8"]
        .iter()
        .any(|name| declared.eq_ignore_ascii_case(name))
    {
        IntWidth::SixtyFour
    } else {
        IntWidth::UpToThirtyTwo
    }
}

/// The exact type a family names, given the refinements the caller resolved
/// from the declared type.
///
/// The mechanism every backend's [`Backend::refine_builtin`] shares: only
/// the two refinements differ per engine, never the mapping.
#[must_use]
pub const fn refined_builtin(
    family: BuiltinKind,
    int: IntWidth,
    float: FloatWidth,
    text: TextWidth,
) -> BuiltinType {
    match family {
        BuiltinKind::Bool => BuiltinType::Bool,
        BuiltinKind::Int => BuiltinType::Int(int),
        BuiltinKind::Float => BuiltinType::Float(float),
        BuiltinKind::Decimal => BuiltinType::Decimal,
        BuiltinKind::String => BuiltinType::Text(text),
        BuiltinKind::Bytes => BuiltinType::Bytes,
        BuiltinKind::Uuid => BuiltinType::Uuid,
        BuiltinKind::Date => BuiltinType::Date,
        BuiltinKind::Time => BuiltinType::Time,
        BuiltinKind::Timestamp => BuiltinType::Timestamp,
        BuiltinKind::TimestampTz => BuiltinType::TimestampTz,
        BuiltinKind::Json => BuiltinType::Json,
        BuiltinKind::Jsonb => BuiltinType::Jsonb,
    }
}

/// One float4 value, as an `f64`.
///
/// The narrowing is the point rather than a hazard: a float4 result is held
/// on the float4 grid, and a value computed or parsed in `f64` has to be put
/// back on it to be the number the engine holds. Every float4 is exactly
/// representable as an `f64`, so widening back is lossless.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn at_float4(double: f64) -> f64 {
    f64::from(double as f32)
}

/// Whether a declared type names a fixed-width character type.
///
/// The spellings PostgreSQL and MySQL share. `sql-traits` canonicalizes
/// `CHARACTER(5)` to `CHAR`, so no width is parsed here.
#[must_use]
pub fn declares_fixed_width_text(declared_type: &str) -> TextWidth {
    let declared = declared_type.trim();
    if ["char", "character", "bpchar", "nchar"]
        .iter()
        .any(|name| declared.eq_ignore_ascii_case(name))
    {
        TextWidth::Fixed
    } else {
        TextWidth::Varying
    }
}

/// What a runtime value is: a family, or one custom type.
///
/// Distinct from [`ScalarKind`] because a value carries no declaration and
/// therefore no refinement. `'0.1'` on the wire is a float; only the column
/// it belongs to says whether that float is float4 or float8, which is
/// exactly the fact this type refuses to invent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValueKind<C> {
    /// One builtin family.
    Builtin(BuiltinKind),
    /// One of the embedder's own types.
    Custom(C),
}

impl<C> ValueKind<C> {
    /// The family this value belongs to, or `None` when it is custom.
    #[must_use]
    pub const fn as_builtin(&self) -> Option<BuiltinKind> {
        match self {
            Self::Builtin(family) => Some(*family),
            Self::Custom(_) => None,
        }
    }
}

impl<C> From<BuiltinKind> for ValueKind<C> {
    fn from(family: BuiltinKind) -> Self {
        Self::Builtin(family)
    }
}

/// The kind of a column under backend `B`, custom position included.
pub type ScalarKindOf<B> = ScalarKind<<<B as Backend>::Custom as CustomScalars>::Kind>;

/// What a value of backend `B` is, custom position included.
pub type ValueKindOf<B> = ValueKind<<<B as Backend>::Custom as CustomScalars>::Kind>;

mod builtin_type_serde {
    use super::{BuiltinType, FloatWidth, IntWidth, TextWidth};

    /// One byte per type, refinements included.
    ///
    /// Tags 0 through 12 are the family tags this format has always used, and
    /// they keep their meaning: a type that carries no refinement encodes as
    /// before, and the refinement that used to be implicit keeps the old tag
    /// (a float was read as float8, a text type as varying). The exact cases
    /// the old format could not express take new tags.
    const fn tag(kind: BuiltinType) -> u8 {
        match kind {
            BuiltinType::Bool => 0,
            BuiltinType::Int(IntWidth::SixtyFour) => 1,
            BuiltinType::Float(FloatWidth::Double) => 2,
            BuiltinType::Decimal => 3,
            BuiltinType::Text(TextWidth::Varying) => 4,
            BuiltinType::Bytes => 5,
            BuiltinType::Uuid => 6,
            BuiltinType::Date => 7,
            BuiltinType::Time => 8,
            BuiltinType::Timestamp => 9,
            BuiltinType::TimestampTz => 10,
            BuiltinType::Json => 11,
            BuiltinType::Jsonb => 12,
            BuiltinType::Float(FloatWidth::Single) => 13,
            BuiltinType::Text(TextWidth::Fixed) => 14,
            BuiltinType::Int(IntWidth::UpToThirtyTwo) => 15,
        }
    }

    const fn untag(tag: u8) -> Option<BuiltinType> {
        Some(match tag {
            0 => BuiltinType::Bool,
            1 => BuiltinType::Int(IntWidth::SixtyFour),
            2 => BuiltinType::Float(FloatWidth::Double),
            3 => BuiltinType::Decimal,
            4 => BuiltinType::Text(TextWidth::Varying),
            5 => BuiltinType::Bytes,
            6 => BuiltinType::Uuid,
            7 => BuiltinType::Date,
            8 => BuiltinType::Time,
            9 => BuiltinType::Timestamp,
            10 => BuiltinType::TimestampTz,
            11 => BuiltinType::Json,
            12 => BuiltinType::Jsonb,
            13 => BuiltinType::Float(FloatWidth::Single),
            14 => BuiltinType::Text(TextWidth::Fixed),
            15 => BuiltinType::Int(IntWidth::UpToThirtyTwo),
            _ => return None,
        })
    }

    // serde's `serialize_with` fixes the signature, so the one-byte type
    // arrives by reference whatever clippy would prefer.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(kind: &BuiltinType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(tag(*kind))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BuiltinType, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let tag = <u8 as serde::Deserialize>::deserialize(deserializer)?;
        untag(tag).ok_or_else(|| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(u64::from(tag)),
                &"a builtin type tag from 0 through 15",
            )
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod scalar_kind_serde_tests {
    use super::{BuiltinKind, ScalarKind};

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    enum TestCustom {
        Named,
    }

    #[test]
    fn builtin_and_custom_kinds_round_trip_with_stable_tags() {
        let families = [
            BuiltinKind::Bool,
            BuiltinKind::Int,
            BuiltinKind::Float,
            BuiltinKind::Decimal,
            BuiltinKind::String,
            BuiltinKind::Bytes,
            BuiltinKind::Uuid,
            BuiltinKind::Date,
            BuiltinKind::Time,
            BuiltinKind::Timestamp,
            BuiltinKind::TimestampTz,
            BuiltinKind::Json,
            BuiltinKind::Jsonb,
        ];
        for (tag, family) in (0_u8..).zip(families) {
            let kind = ScalarKind::<TestCustom>::from(family);
            let encoded = postcard::to_allocvec(&kind).unwrap();
            assert_eq!(encoded, [0, tag]);
            assert_eq!(
                postcard::from_bytes::<ScalarKind<TestCustom>>(&encoded),
                Ok(kind)
            );
        }

        let custom = ScalarKind::Custom(TestCustom::Named);
        let encoded = postcard::to_allocvec(&custom).unwrap();
        assert_eq!(encoded, [1, 0]);
        assert_eq!(
            postcard::from_bytes::<ScalarKind<TestCustom>>(&encoded),
            Ok(custom)
        );
    }

    /// The refined cases the family tags could not express take tags of
    /// their own, and the unrefined ones keep the tags they always had, so a
    /// stored `real` reloads as float4 rather than as float8.
    #[test]
    fn refined_types_have_their_own_stable_tags() {
        use super::{BuiltinType, FloatWidth, TextWidth};

        for (tag, kind) in [
            (2_u8, BuiltinType::Float(FloatWidth::Double)),
            (4, BuiltinType::Text(TextWidth::Varying)),
            (13, BuiltinType::Float(FloatWidth::Single)),
            (14, BuiltinType::Text(TextWidth::Fixed)),
        ] {
            let stored = ScalarKind::<TestCustom>::Builtin(kind);
            let encoded = postcard::to_allocvec(&stored).unwrap();
            assert_eq!(encoded, [0, tag], "{kind:?} encodes as tag {tag}");
            assert_eq!(
                postcard::from_bytes::<ScalarKind<TestCustom>>(&encoded),
                Ok(stored)
            );
        }
    }

    #[test]
    fn scalar_kind_rejects_an_unknown_builtin_tag() {
        assert!(postcard::from_bytes::<ScalarKind<TestCustom>>(&[0, 16]).is_err());
    }
}
/// Owned SQL name for a column's declared collation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CollationName {
    /// Identifier without surrounding quotes.
    pub name: alloc::string::String,
    /// Whether the identifier was quoted.
    pub name_is_quoted: bool,
    /// Optional schema identifier without surrounding quotes.
    pub schema: Option<alloc::string::String>,
    /// Whether the schema identifier was quoted.
    pub schema_is_quoted: bool,
}

/// Whether a collation ignores trailing spaces, mirrored from
/// [`sql_traits::traits::MySqlCollationPadding`] so a persisted descriptor
/// carries no foreign type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TrailingSpacePadding {
    /// Comparisons ignore trailing spaces.
    PadSpace,
    /// Comparisons keep trailing spaces significant.
    NoPad,
}

impl From<sql_traits::traits::MySqlCollationPadding> for TrailingSpacePadding {
    fn from(padding: sql_traits::traits::MySqlCollationPadding) -> Self {
        match padding {
            sql_traits::traits::MySqlCollationPadding::PadSpace => Self::PadSpace,
            sql_traits::traits::MySqlCollationPadding::NoPad => Self::NoPad,
        }
    }
}

/// Comparison metadata a column's declared collation carries.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CollationFacts {
    /// The database default applies.
    DatabaseDefault,
    /// The column declares a named collation.
    Named {
        /// Declared collation name.
        name: CollationName,
        /// PostgreSQL determinism when known.
        postgres_deterministic: Option<bool>,
        /// Trailing-space rule when known.
        padding: Option<TrailingSpacePadding>,
    },
    /// Comparison rules changed without a resolved collation name.
    Unknown,
}

/// The catalog facts a comparison of one column depends on.
///
/// Resolved once per registration and carried in the compiled program, so no
/// comparison consults the catalog per row. Also decides whether a column can
/// form a group key, which is the same question asked of the same facts.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(bound = "C: serde::Serialize + for<'d> serde::Deserialize<'d>")]
pub struct ColumnComparison<C> {
    /// Scalar kind used by subql.
    pub kind: ScalarKind<C>,
    /// Canonical declared SQL type.
    pub declared_type: alloc::string::String,
    /// Column comparison metadata.
    pub collation: CollationFacts,
}

impl<C> ColumnComparison<C> {
    /// Whether the declared type is a fixed-width character type.
    ///
    /// PostgreSQL pads such a column out to its width on write and then
    /// ignores those trailing spaces when comparing, so the rule needs the
    /// type and not the width. The width is not available anyway:
    /// `sql-traits` canonicalizes `CHARACTER(5)` to `CHAR`.
    ///
    /// This is only the type fact. Whether it makes a comparison ignore
    /// trailing spaces is the backend's decision, because the engines
    /// differ: PostgreSQL decides on the type, while MySQL decides on the
    /// collation and strips a `CHAR` column's trailing spaces on write, so
    /// no padded cell reaches a comparison at all.
    #[must_use]
    pub fn declares_char_type(&self) -> bool {
        self.kind
            .builtin()
            .is_some_and(BuiltinType::is_fixed_width_text)
    }

    /// Whether the column's collation declares that comparisons ignore
    /// trailing spaces, which is MySQL's `PAD SPACE`.
    #[must_use]
    pub const fn collation_pads_trailing_spaces(&self) -> bool {
        matches!(
            &self.collation,
            CollationFacts::Named {
                padding: Some(TrailingSpacePadding::PadSpace),
                ..
            }
        )
    }
}

/// Comparison facts under backend `B`.
pub type ColumnComparisonOf<B> = ColumnComparison<<<B as Backend>::Custom as CustomScalars>::Kind>;

/// How each side of a text comparison is read, once the backend has
/// decided what the operands' declared types and collations mean.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum TrailingSpaces {
    /// Both sides keep their trailing spaces: the exact-bytes rule.
    BothSignificant,
    /// Neither side's trailing spaces count, which is PostgreSQL comparing
    /// two `char` values, or a `char` against a `varchar` or a literal, and
    /// MySQL under a `PAD SPACE` collation.
    BothIgnored,
    /// Only the left side's trailing spaces are dropped, which is
    /// PostgreSQL comparing a `char` column against a `text` one: the
    /// conversion to `text` strips the padding and the comparison is then
    /// exact.
    LeftStripped,
    /// The mirror of [`Self::LeftStripped`].
    RightStripped,
}

impl TrailingSpaces {
    /// The two operands as this rule reads them.
    #[must_use]
    pub fn apply<'a>(self, left: &'a str, right: &'a str) -> (&'a str, &'a str) {
        let trim = |text: &'a str| text.trim_end_matches(' ');
        match self {
            Self::BothSignificant => (left, right),
            Self::BothIgnored => (trim(left), trim(right)),
            Self::LeftStripped => (trim(left), right),
            Self::RightStripped => (left, trim(right)),
        }
    }
}

/// Which text comparison the in-process comparator performs, once the
/// backend has read the operands' collations.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum TextCase {
    /// Compare the characters as they are.
    Exact,
    /// Fold ASCII case only, which is what SQLite's `NOCASE` does and,
    /// measured, all that it does: it leaves a ligature or a non-ASCII
    /// letter alone.
    AsciiNoCase,
}

/// The operation a text comparison performs, because reproducibility does
/// not factor per column.
///
/// PostgreSQL's default collation is the proof: equality is byte equality,
/// while ordering is the locale's and byte order does not reproduce it. A
/// single answer per column cannot express that, so the backend is asked
/// per operation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TextOperation {
    /// `=`, `<>`, `IN`, and group-key membership, which is equality.
    Equality,
    /// `<`, `<=`, `>`, `>=`, `BETWEEN`, and extreme maintenance.
    Ordering,
    /// `LIKE`. A separate operation because the engines do not answer it
    /// like equality: PostgreSQL's `LIKE` reads a `char(n)` column's
    /// padding where its `=` ignores it. Case handling is the engine's
    /// too: measured, PostgreSQL and MySQL under a binary collation
    /// answer `'ABC' LIKE 'abc'` false while SQLite answers it true.
    Pattern,
    /// `ILIKE`, which only PostgreSQL has: MySQL answers a syntax error
    /// and SQLite has no such keyword.
    ///
    /// Its folding is the locale's, and reproducible only where that
    /// folding is ASCII-only. Measured on a `en_US.utf8` database,
    /// `lower('İ')` is the single character `i`, so a fold that produced
    /// two characters would let `_` match one the server never emitted,
    /// and `'ΣΟΦΟΣ' ILIKE 'σοφος'` is false there where a final-sigma
    /// aware fold answers true.
    CaseInsensitivePattern,
}

/// How one text comparison is answered in process, resolved once at
/// registration from both operands' declared types and collations.
///
/// Carried in the compiled program, so no comparison consults a collation
/// per row. Its absence is not "compare exactly": it means no in-process
/// comparison reproduces the engine, and the statement takes a database
/// read instead.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct TextRule {
    /// Character handling.
    pub case: TextCase,
    /// Trailing-space handling.
    pub spaces: TrailingSpaces,
}

impl TextRule {
    /// Byte comparison: the rule for a binary collation, and for
    /// PostgreSQL equality under any deterministic collation.
    pub const EXACT: Self = Self {
        case: TextCase::Exact,
        spaces: TrailingSpaces::BothSignificant,
    };

    /// This rule with `spaces` replaced, for a backend that resolves the
    /// two facts separately.
    #[must_use]
    pub const fn with_spaces(self, spaces: TrailingSpaces) -> Self {
        Self { spaces, ..self }
    }

    /// Whether two text operands are equal under this rule.
    #[must_use]
    pub fn equal(self, left: &str, right: &str) -> bool {
        let (left, right) = self.spaces.apply(left, right);
        match self.case {
            TextCase::Exact => left == right,
            TextCase::AsciiNoCase => left.eq_ignore_ascii_case(right),
        }
    }

    /// How two text operands order under this rule.
    #[must_use]
    pub fn compare(self, left: &str, right: &str) -> core::cmp::Ordering {
        let (left, right) = self.spaces.apply(left, right);
        match self.case {
            TextCase::Exact => left.cmp(right),
            // Fold each byte as it is compared rather than allocating a
            // lowercased copy of both sides.
            TextCase::AsciiNoCase => left
                .bytes()
                .map(|byte| byte.to_ascii_lowercase())
                .cmp(right.bytes().map(|byte| byte.to_ascii_lowercase())),
        }
    }
}

/// How a comparison reads a numeric pair whose two operands are different
/// scalars.
///
/// Measured 2026-09-04, using `9007199254740993` against
/// `9007199254740992`, the smallest pair `f64` cannot separate:
///
/// ```text
/// pair                 pg      mysql   sqlite
/// integer vs float     lossy   lossy   exact
/// integer vs decimal   exact   exact   no decimal type
/// decimal vs float     lossy   lossy   no decimal type
/// ```
///
/// So there is no single widening. PostgreSQL and MySQL cast the other
/// operand to `double precision` when one side is a float, and compare
/// exactly against a decimal; SQLite compares an integer against a real
/// without rounding either.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NumericWidening {
    /// Compare in `f64`, reproducing a cast to `double precision`. Two
    /// integers `f64` cannot separate then compare equal, which is what
    /// those engines answer.
    AtFloatWidth,
    /// Compare without losing a digit.
    Exact,
}

/// What `/` answers, which is an engine's choice and not one rule.
///
/// Measured 2026-09-05 on PostgreSQL 16.15, MySQL 8.4.11 and SQLite
/// 3.51.1, and read back to both servers' sources:
///
/// ```text
/// expression           pg                      mysql        sqlite
/// 7 / 2                3                       3.5000       3
/// 1 / 3                0                       0.333333333  0
/// 1::numeric / 3       0.33333333333333333333  -            no decimal type
/// ```
///
/// Two independent choices hide in that table. Whether an integer
/// quotient stays an integer: PostgreSQL and SQLite truncate toward zero,
/// MySQL answers a decimal instead, so `7 / 2 > 3` is false on two
/// engines and true on the third. And what scale a decimal quotient
/// carries, since neither engine computes one exactly.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DivisionRule {
    /// Two integers divide to an integer, truncated toward zero, and a
    /// decimal quotient carries the scale that gives sixteen significant
    /// digits, rounded half away from zero.
    ///
    /// PostgreSQL's rule, from `select_div_scale`
    /// (`src/backend/utils/adt/numeric.c`), whose scale is
    /// `max(16 - quotient_weight * 4, dividend_scale, divisor_scale, 0)`
    /// where the weight counts four-digit words. Byte-identical in
    /// PostgreSQL 12 through 17 and driven by no session variable.
    ///
    /// SQLite states this too, and only its integer half ever runs: its
    /// numeric tower is `INTEGER` and `REAL`, so no decimal value reaches
    /// arithmetic.
    IntegersTruncate,
    /// Every quotient is a decimal, even for two integers, and its
    /// fractional digits are quantised to whole nine-digit words and
    /// truncated toward zero.
    ///
    /// MySQL's rule, from `do_div_mod` (`mysys/decimal.cc`):
    ///
    /// ```text
    /// frame1 = ceil(dividend_scale / 9) * 9
    /// frame2 = ceil(divisor_scale  / 9) * 9
    /// adj    = max(0, increment - ((frame1 - dividend_scale) + (frame2 - divisor_scale)))
    /// digits = ceil((frame1 + frame2 + adj) / 9) * 9
    /// ```
    ///
    /// Nine is `DIG_PER_DEC1`, the digits one stored word holds. The
    /// `increment` is a session setting, so this rule needs
    /// [`DivisionPrecisionIncrement`] before it can answer at all.
    /// Byte-identical in MySQL 5.7 through 9.0 and in MariaDB 11.4.
    QuotientsAreDecimalInWords,
}

/// What a `SUM` accumulates in, and therefore what it answers.
///
/// Measured 2026-09-05 on PostgreSQL 16.15, MySQL 8.4.11 and SQLite
/// 3.51.1. No engine sums in `f64`, and no two agree:
///
/// ```text
/// column           pg                    mysql           sqlite
/// smallint, int    bigint                decimal(32,0)   integer
/// bigint           numeric               decimal(41,0)   integer
/// numeric          numeric, scale kept   decimal, kept   no decimal type
/// real, double     double precision      double          real
/// ```
///
/// None of them is unbounded either, and they run out differently, which
/// is why the boundary rides on the variant rather than being checked once
/// somewhere central.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SumRule {
    /// Exact in 64 bits, and the engine raises when a total does not fit.
    ///
    /// PostgreSQL's `sum(smallint)` and `sum(int)`, whose result is
    /// `bigint`.
    Integer,
    /// Exact in 64 bits until a non-integer value joins, at which point
    /// the total becomes a double, and the engine raises on a 64-bit
    /// overflow before that.
    ///
    /// SQLite, measured: two rows of `i64::MAX` answer `integer
    /// overflow`, while `1 + 2 + 0.5` answers `3.5` typed `real`.
    IntegerPromotingToDouble,
    /// Exact decimal, bounded by `integer_digits` digits ahead of the
    /// decimal point when the engine has such a bound.
    ///
    /// PostgreSQL's `numeric` stops at 131072 integer digits and answers
    /// `value overflows numeric format`, which two rows can reach. MySQL
    /// states no bound here because none is reachable: its `SELECT`
    /// widens past `DECIMAL(65)`, answering 68 digits over 200 rows and 70
    /// over 51200, and its internal ceiling needs about 10^16 rows of the
    /// widest column it permits. The out-of-range error MySQL documents
    /// belongs to storing such a total in a column, not to computing it.
    Decimal {
        /// Digits permitted ahead of the decimal point, or `None` when the
        /// engine has no reachable bound.
        integer_digits: Option<u32>,
    },
    /// A double, which is what every engine sums a floating column into.
    Double,
}

/// Which shape a variance seed can read the spread back in.
///
/// A stable fold keeps the sum of squared deviations, and the cheapest
/// way to seed it is to ask the engine for its own: `VAR_POP(x) *
/// COUNT(x)`, which PostgreSQL and MySQL compute stably and which
/// measured exactly `2` over `100000000.0`, `100000001.0` and
/// `100000002.0`.
///
/// SQLite has no variance function at all, so it cannot express that
/// product: the seed query answers `no such function: VAR_POP`. There the
/// only shape a single projection can ask for is a sum of squares, and
/// the deviations are derived from it, which is the cancellation this fold
/// exists to avoid and is therefore as good as that engine gets.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VarianceSeed {
    /// `VAR_POP(x) * COUNT(x)`, the engine's own stable answer.
    EnginesOwn,
    /// `SUM(x * 1.0 * x)`, from which the deviations are derived, for an
    /// engine with no variance function to ask.
    SumOfSquares,
}

/// What `AVG` answers over a column whose total is exact.
///
/// Measured 2026-09-05. PostgreSQL and MySQL both answer an exact
/// decimal, each by its own division rule: `avg(int)` of 1 and 2 is
/// `1.5000000000000000` on PostgreSQL, sixteen significant digits, and
/// `avg` over 1, 2, 2 compares as `1.666666666` on MySQL, nine digits
/// truncated, whatever the `1.6667` it prints. SQLite answers a real for
/// every column, and inexactly: `avg` of one row of `9007199254740993` is
/// `9.00719925474099e+15`.
///
/// A floating column averages into a double everywhere, which follows
/// from its total rather than from this rule.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MeanRule {
    /// The mean of an exact total is an exact decimal, computed the way
    /// this engine computes a decimal quotient. PostgreSQL and MySQL.
    Exact,
    /// Every mean is a double. SQLite.
    Double,
}

/// MySQL's `div_precision_increment`, as the deployment declares it.
///
/// A session setting, default 4, valid from 0 through 30
/// (`Sys_var_ulong Sys_div_precincrement`, `sql/sys_vars.cc`), and it is
/// answer-visible rather than cosmetic. Measured on MySQL 8.4.11, `1 / 3`
/// compares as `0` at increment 0, as `0.333333333` at 4 and 9, and as
/// `0.333333333333333333` at 10, because
/// [`DivisionRule::QuotientsAreDecimalInWords`] spends it a whole word at
/// a time.
///
/// Nothing in a change stream carries it, so an engine that must answer
/// division is told through
/// [`SubscriptionEngine::with_division_precision_increment`](crate::SubscriptionEngine::with_division_precision_increment)
/// and refuses the operator until it is.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct DivisionPrecisionIncrement(u8);

impl DivisionPrecisionIncrement {
    /// The largest increment MySQL accepts, `DECIMAL_MAX_SCALE`.
    const MAX: u8 = 30;

    /// The declared increment, or `None` when the server would reject it.
    ///
    /// Read one from a deployment with
    /// `SELECT @@div_precision_increment`.
    #[must_use]
    pub const fn new(increment: u8) -> Option<Self> {
        if increment > Self::MAX {
            return None;
        }
        Some(Self(increment))
    }

    /// The declared digits, as the quotient formula spends them.
    #[must_use]
    pub const fn digits(self) -> u8 {
        self.0
    }
}

/// The catalog facts of both operands of one comparison.
///
/// Two-sided because a comparison's answer can depend on both columns, not
/// on one: `real` against `double precision` compares at the wider column's
/// width, and two differently collated text columns have no single collation
/// to consult. A side that is a literal carries `None`.
pub struct ComparisonContext<'a, B: Backend> {
    /// Facts for the left operand, or `None` when it is not a column.
    pub left: Option<&'a ColumnComparisonOf<B>>,
    /// Facts for the right operand, or `None` when it is not a column.
    pub right: Option<&'a ColumnComparisonOf<B>>,
    /// The text rule the compiler resolved for this comparison, or `None`
    /// when the comparison is not a text one, or reaches the comparator
    /// from a path that carries no compiled program.
    pub text: Option<TextRule>,
}

// Hand-implemented for the same reason as `Value<B>`: `#[derive]` would
// require `B: Clone`, which `Backend` does not imply. The struct holds two
// shared references and nothing else.
impl<B: Backend> Clone for ComparisonContext<'_, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<B: Backend> Copy for ComparisonContext<'_, B> {}

impl<B: Backend> Default for ComparisonContext<'_, B> {
    fn default() -> Self {
        Self::none()
    }
}

impl<'a, B: Backend> ComparisonContext<'a, B> {
    /// A comparison whose operand facts are both unknown, which is what a
    /// literal-only comparison and a program compiled before the descriptors
    /// existed both carry.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            left: None,
            right: None,
            text: None,
        }
    }

    /// A comparison between a column and a literal.
    #[must_use]
    pub const fn column(facts: &'a ColumnComparisonOf<B>) -> Self {
        Self {
            left: Some(facts),
            right: None,
            text: None,
        }
    }

    /// The facts both sides agree on, or `None` when they differ or either
    /// side is unknown. The one honest single-sided answer.
    #[must_use]
    pub fn agreed(&self) -> Option<&'a ColumnComparisonOf<B>> {
        match (self.left, self.right) {
            (Some(left), None) | (None, Some(left)) => Some(left),
            (Some(left), Some(right)) if left == right => Some(left),
            _ => None,
        }
    }
}

type GroupKeyComponentEncoder<B> =
    fn(&ColumnComparisonOf<B>, &Value<B>, &mut alloc::vec::Vec<u8>) -> bool;

/// Canonical identity encoder selected for one grouped projection.
pub struct GroupKeyEncoder<B: Backend> {
    columns: alloc::sync::Arc<[ColumnComparisonOf<B>]>,
    encode_component: GroupKeyComponentEncoder<B>,
}

impl<B: Backend> GroupKeyEncoder<B> {
    /// Creates an encoder from resolved columns and one backend component writer.
    #[must_use]
    pub fn new(
        columns: alloc::vec::Vec<ColumnComparisonOf<B>>,
        encode_component: GroupKeyComponentEncoder<B>,
    ) -> Self {
        Self {
            columns: columns.into(),
            encode_component,
        }
    }

    /// Encodes one tuple, or refuses values outside the selected column domain.
    #[must_use]
    pub fn encode(&self, values: &[Value<B>]) -> Option<alloc::vec::Vec<u8>> {
        let count = u16::try_from(self.columns.len()).ok()?;
        if values.len() != self.columns.len() {
            return None;
        }
        let mut key = alloc::vec::Vec::new();
        key.extend_from_slice(b"SQGK");
        key.push(1);
        key.extend_from_slice(&count.to_be_bytes());
        for (column, value) in self.columns.iter().zip(values) {
            let length_at = key.len();
            key.extend_from_slice(&[0; 4]);
            let component_at = key.len();
            if !(self.encode_component)(column, value, &mut key) {
                return None;
            }
            let length = u32::try_from(key.len() - component_at).ok()?;
            key[length_at..component_at].copy_from_slice(&length.to_be_bytes());
        }
        Some(key)
    }

    /// Columns whose comparison contract selected this encoder.
    #[must_use]
    pub fn columns(&self) -> &[ColumnComparisonOf<B>] {
        &self.columns
    }
}

impl<B: Backend> Clone for GroupKeyEncoder<B> {
    fn clone(&self) -> Self {
        Self {
            columns: alloc::sync::Arc::clone(&self.columns),
            encode_component: self.encode_component,
        }
    }
}

impl<B: Backend> core::fmt::Debug for GroupKeyEncoder<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GroupKeyEncoder")
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

struct AppendPostcard<'a>(&'a mut alloc::vec::Vec<u8>);

impl postcard::ser_flavors::Flavor for AppendPostcard<'_> {
    type Output = ();

    fn try_extend(&mut self, data: &[u8]) -> postcard::Result<()> {
        self.0.extend_from_slice(data);
        Ok(())
    }

    fn try_push(&mut self, data: u8) -> postcard::Result<()> {
        self.0.push(data);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(())
    }
}

fn append_postcard<T: serde::Serialize + ?Sized>(
    output: &mut alloc::vec::Vec<u8>,
    value: &T,
) -> bool {
    postcard::serialize_with_flavor::<T, AppendPostcard<'_>, ()>(value, AppendPostcard(output))
        .is_ok()
}

fn encode_exact_component<B: Backend>(
    column: &ColumnComparisonOf<B>,
    value: &Value<B>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    macro_rules! tagged {
        ($tag:literal, $value:expr) => {{
            output.push($tag);
            append_postcard(output, $value)
        }};
    }
    if let (ScalarKind::Custom(kind), Value::Custom(custom)) = (&column.kind, value) {
        return *kind == <B::Custom as CustomScalars>::kind_of(custom) && tagged!(14, custom);
    }
    match (column.kind.as_builtin(), value) {
        (_, Value::Null) => {
            output.push(0);
            true
        }
        (Some(BuiltinKind::Bool), Value::Bool(value)) => tagged!(1, value),
        (Some(BuiltinKind::Int), Value::Int(value)) => tagged!(2, value),
        (Some(BuiltinKind::Float), Value::Float(value)) => tagged!(3, value),
        (Some(BuiltinKind::String), Value::String(value)) => tagged!(4, value),
        (Some(BuiltinKind::Bytes), Value::Bytes(value)) => tagged!(5, value),
        (Some(BuiltinKind::Uuid), Value::Uuid(value)) => tagged!(6, value),
        (Some(BuiltinKind::Timestamp), Value::Timestamp(value)) => {
            tagged!(7, value)
        }
        (Some(BuiltinKind::TimestampTz), Value::TimestampTz(value)) => {
            tagged!(8, value)
        }
        (Some(BuiltinKind::Date), Value::Date(value)) => tagged!(9, value),
        (Some(BuiltinKind::Time), Value::Time(value)) => tagged!(10, value),
        (Some(BuiltinKind::Decimal), Value::Decimal(value)) => tagged!(11, value),
        (Some(BuiltinKind::Json), Value::Json(value)) => tagged!(12, value),
        (Some(BuiltinKind::Jsonb), Value::Jsonb(value)) => tagged!(13, value),
        _ => false,
    }
}

pub(super) fn default_group_key_encoder<B: Backend>(
    columns: alloc::vec::Vec<ColumnComparisonOf<B>>,
) -> Option<GroupKeyEncoder<B>> {
    let supported = columns.iter().all(|column| {
        matches!(
            column.kind.as_builtin(),
            Some(
                BuiltinKind::Int
                    | BuiltinKind::Bool
                    | BuiltinKind::Bytes
                    | BuiltinKind::Timestamp
                    | BuiltinKind::TimestampTz
                    | BuiltinKind::Date
                    | BuiltinKind::Time
            )
        )
    });
    supported.then(|| GroupKeyEncoder::new(columns, encode_exact_component::<B>))
}

pub(super) fn decode_exact_group_value<B: Backend>(
    kind: ValueKindOf<B>,
    value: Value<B>,
) -> Option<Value<B>> {
    if value.is_null() || value.scalar_kind() == Some(kind) {
        Some(value)
    } else {
        None
    }
}

fn canonical_f64(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else if value.is_nan() {
        f64::from_bits(0x7ff8_0000_0000_0000)
    } else {
        value
    }
}

#[allow(clippy::cast_precision_loss)]
#[must_use]
pub const fn widen_i64_to_f64(value: i64) -> f64 {
    value as f64 // Deliberate SQL double-precision rounding.
}

fn append_tagged<T: serde::Serialize + ?Sized>(
    output: &mut alloc::vec::Vec<u8>,
    tag: u8,
    value: &T,
) -> bool {
    output.push(tag);
    append_postcard(output, value)
}

/// Write one text component under the rule the comparator uses, so a group
/// key and a predicate never disagree about which values are the same.
fn append_text(output: &mut alloc::vec::Vec<u8>, tag: u8, value: &str, rule: TextRule) -> bool {
    let trimmed = match rule.spaces {
        TrailingSpaces::BothSignificant => value,
        // One column against itself, so the one-sided rules cannot arise.
        TrailingSpaces::BothIgnored
        | TrailingSpaces::LeftStripped
        | TrailingSpaces::RightStripped => value.trim_end_matches(' '),
    };
    match rule.case {
        TextCase::Exact => append_tagged(output, tag, trimmed),
        TextCase::AsciiNoCase => {
            let mut canonical = trimmed.as_bytes().to_vec();
            canonical.make_ascii_lowercase();
            output.push(tag);
            append_postcard(output, canonical.as_slice())
        }
    }
}

/// The rule for one column compared against itself, which is what group-key
/// membership and an index key are: both ask whether two values are the
/// same value.
pub fn single_column_rule<B: Backend>(column: &ColumnComparisonOf<B>) -> Option<TextRule> {
    B::text_rule(
        &ComparisonContext {
            left: Some(column),
            right: Some(column),
            text: None,
        },
        TextOperation::Equality,
    )
}

/// Whether any in-process comparison reproduces this PostgreSQL column for
/// `operation`.
///
/// Equality under a deterministic collation is byte equality, which is what
/// deterministic means, and `CREATE DATABASE` cannot select a
/// nondeterministic collation, so the database default qualifies. Ordering
/// is the locale's, and measured, byte order does not reproduce it: the
/// server answers `'a' < 'B'` true where bytes answer false. Only `C` and
/// `POSIX` order by byte.
///
/// A case-insensitive pattern is the locale's too, and worse: measured on
/// a `en_US.utf8` database, `lower('İ')` is the single character `i`,
/// where Rust's folding gives two, and `'ΣΟΦΟΣ' ILIKE 'σοφος'` is false
/// where a final-sigma aware fold answers true. Only `C` and `POSIX` fold
/// ASCII alone, and only there is `ILIKE` reproducible.
pub(super) fn postgres_reproduces(
    column: &ColumnComparisonOf<Postgres>,
    operation: TextOperation,
) -> bool {
    use TextOperation;

    let byte_ordered = |name: &CollationName| {
        name.name.eq_ignore_ascii_case("C") || name.name.eq_ignore_ascii_case("POSIX")
    };
    match (&column.collation, operation) {
        // A byte-ordered collation reproduces every operation.
        (CollationFacts::Named { name, .. }, _) if byte_ordered(name) => true,
        // Ordering and case folding under any other collation are the
        // locale's.
        (_, TextOperation::Ordering | TextOperation::CaseInsensitivePattern) => false,
        (CollationFacts::DatabaseDefault, _) => true,
        (
            CollationFacts::Named {
                postgres_deterministic: Some(true),
                ..
            },
            _,
        ) => true,
        (CollationFacts::Named { .. } | CollationFacts::Unknown, _) => false,
    }
}

/// The in-process comparison for a MySQL column, or `None` when none
/// reproduces it.
///
/// Only the binary collations qualify. Measured on 8.4.11: the server
/// default folds case and accents, and `utf8mb4_0900_as_cs` reports the NFC
/// and NFD spellings of one letter equal, so case sensitivity is not byte
/// exactness. Padding still differs inside the binary family, which
/// `information_schema.COLLATIONS.PAD_ATTRIBUTE` names: `utf8mb4_bin` is
/// `PAD SPACE` and `utf8mb4_0900_bin` is `NO PAD`, both measured.
pub(super) fn mysql_binary_text_rule(column: &ColumnComparisonOf<MySql>) -> Option<TextRule> {
    let CollationFacts::Named { name, padding, .. } = &column.collation else {
        return None;
    };
    if !name.name.to_ascii_lowercase().ends_with("_bin") {
        return None;
    }
    let spaces = match padding {
        Some(TrailingSpacePadding::PadSpace) => TrailingSpaces::BothIgnored,
        Some(TrailingSpacePadding::NoPad) => TrailingSpaces::BothSignificant,
        None if name.name.eq_ignore_ascii_case("utf8mb4_bin") => TrailingSpaces::BothIgnored,
        None if name.name.eq_ignore_ascii_case("utf8mb4_0900_bin") => {
            TrailingSpaces::BothSignificant
        }
        // A binary collation this build cannot place on either side of the
        // padding question is not reproduced, rather than guessed.
        None => return None,
    };
    Some(TextRule::EXACT.with_spaces(spaces))
}

/// The in-process comparison for a SQLite column.
///
/// All three built-in collations are exactly reproducible, and the same
/// rule serves every operation. `NOCASE` folds ASCII case only, measured:
/// it leaves a ligature and the NFC/NFD distinction alone.
pub(super) fn sqlite_text_rule(column: &ColumnComparisonOf<SQLite>) -> Option<TextRule> {
    match &column.collation {
        CollationFacts::DatabaseDefault => Some(TextRule::EXACT),
        CollationFacts::Named { name, .. } if name.name.eq_ignore_ascii_case("binary") => {
            Some(TextRule::EXACT)
        }
        CollationFacts::Named { name, .. } if name.name.eq_ignore_ascii_case("nocase") => {
            Some(TextRule {
                case: TextCase::AsciiNoCase,
                spaces: TrailingSpaces::BothSignificant,
            })
        }
        CollationFacts::Named { name, .. } if name.name.eq_ignore_ascii_case("rtrim") => {
            Some(TextRule::EXACT.with_spaces(TrailingSpaces::BothIgnored))
        }
        CollationFacts::Named { .. } | CollationFacts::Unknown => None,
    }
}

/// PostgreSQL `jsonb` equality, for values already known to be storable.
///
/// A value refused by the canonical crate cannot have come from a server, and the one path
/// that could introduce one, `SqlLiteralParse::parse_literal`, rejects it before it reaches
/// here. Should one arrive anyway, comparing the payloads directly keeps the relation
/// reflexive, which answering `false` would not.
pub fn jsonb_payloads_equal<B: Backend>(left: &B::Jsonb, right: &B::Jsonb) -> bool {
    let left_json = (left as &dyn core::any::Any).downcast_ref::<serde_json::Value>();
    let right_json = (right as &dyn core::any::Any).downcast_ref::<serde_json::Value>();
    match (left_json, right_json) {
        (Some(left), Some(right)) => {
            postgres_jsonb_canonical::equivalent::<B::JsonbVersion>(left, right)
                .unwrap_or_else(|_| left == right)
        }
        _ => left == right,
    }
}

pub(super) fn encode_postgres_component<V: postgres_jsonb_canonical::PgVersion + 'static>(
    column: &ColumnComparisonOf<Postgres<V>>,
    value: &Value<Postgres<V>>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind.as_builtin(), value) {
        (Some(BuiltinKind::Float), Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (Some(BuiltinKind::String), Value::String(value)) => single_column_rule::<Postgres>(column)
            .is_some_and(|rule| append_text(output, 4, value, rule)),
        (Some(BuiltinKind::Jsonb), Value::Jsonb(value)) => {
            output.push(13);
            // Restores `output` itself on refusal, so a rejected value leaves no partial
            // component behind in the group key.
            postgres_jsonb_canonical::encode_into::<V>(value, output).is_ok()
        }
        _ => encode_exact_component(column, value, output),
    }
}

pub(super) fn encode_mysql_component(
    column: &ColumnComparisonOf<MySql>,
    value: &Value<MySql>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind.as_builtin(), value) {
        (Some(BuiltinKind::Float), Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (Some(BuiltinKind::String), Value::String(value)) => single_column_rule::<MySql>(column)
            .is_some_and(|rule| append_text(output, 4, value, rule)),
        (Some(BuiltinKind::Uuid), Value::Uuid(value)) => single_column_rule::<MySql>(column)
            .is_some_and(|rule| append_text(output, 6, value, rule)),
        (Some(BuiltinKind::Decimal), Value::Decimal(value)) => {
            append_tagged(output, 11, &value.normalized())
        }
        _ => encode_exact_component(column, value, output),
    }
}

fn append_sqlite_json(
    column: &ColumnComparisonOf<SQLite>,
    value: &SqliteJson,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match value.storage() {
        SqliteJsonStorage::Text(value) => single_column_rule::<SQLite>(column)
            .is_some_and(|rule| append_text(output, 0, value, rule)),
        SqliteJsonStorage::Integer(value) => append_tagged(output, 1, value),
        SqliteJsonStorage::Real(value) => {
            let canonical = canonical_f64(*value);
            if canonical.fract() == 0.0 {
                if let Some(integer) = sql_scalar_text::parse_i64(&canonical.to_string()) {
                    return append_tagged(output, 1, &integer);
                }
            }
            append_tagged(output, 2, &canonical)
        }
        SqliteJsonStorage::Blob(value) => append_tagged(output, 3, value),
    }
}

pub(super) fn encode_sqlite_component(
    column: &ColumnComparisonOf<SQLite>,
    value: &Value<SQLite>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind.as_builtin(), value) {
        // SQLite stores NaN as SQL NULL, so only synthetic values reach this arm.
        (Some(BuiltinKind::Float), Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (Some(BuiltinKind::String), Value::String(value)) => single_column_rule::<SQLite>(column)
            .is_some_and(|rule| append_text(output, 4, value, rule)),
        (Some(BuiltinKind::Uuid), Value::Uuid(value)) => single_column_rule::<SQLite>(column)
            .is_some_and(|rule| append_text(output, 6, value, rule)),
        (Some(BuiltinKind::Json), Value::Json(value)) => {
            output.push(12);
            append_sqlite_json(column, value, output)
        }
        (Some(BuiltinKind::Jsonb), Value::Jsonb(value)) => {
            output.push(13);
            append_sqlite_json(column, value, output)
        }
        _ => encode_exact_component(column, value, output),
    }
}

/// The custom scalar set of a backend that has none. Uninhabited, so
/// [`ScalarKind::Custom`] cannot be constructed for such a backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum NoCustom {}

impl<C> From<BuiltinType> for ScalarKind<C> {
    fn from(builtin: BuiltinType) -> Self {
        Self::Builtin(builtin)
    }
}

/// A family widened to a type by taking each refinement's common case: a
/// 64-bit integer, float8, and varying text.
///
/// For fixtures only. Production code classifies through
/// [`Backend::refine_builtin`], because choosing a refinement without
/// reading the declaration is the erasure this type removes.
#[cfg(any(test, feature = "testing"))]
impl<C> From<BuiltinKind> for ScalarKind<C> {
    fn from(family: BuiltinKind) -> Self {
        Self::Builtin(refined_builtin(
            family,
            IntWidth::SixtyFour,
            FloatWidth::Double,
            TextWidth::Varying,
        ))
    }
}

impl<C> ScalarKind<C> {
    /// This kind as a builtin family, or `None` for a custom type.
    #[must_use]
    pub const fn as_builtin(&self) -> Option<BuiltinKind> {
        match self {
            Self::Builtin(builtin) => Some(builtin.family()),
            Self::Custom(_) => None,
        }
    }

    /// This declared type reduced to what a value can carry, so a column
    /// and a value are compared on the fact they share.
    #[must_use]
    pub const fn value_kind(&self) -> ValueKind<C>
    where
        C: Copy,
    {
        match self {
            Self::Builtin(builtin) => ValueKind::Builtin(builtin.family()),
            Self::Custom(custom) => ValueKind::Custom(*custom),
        }
    }

    /// The exact builtin type this kind names, or `None` for a custom type.
    ///
    /// For the questions a family cannot answer: which width a float
    /// declares, whether a text type is fixed width.
    #[must_use]
    pub const fn builtin(&self) -> Option<BuiltinType> {
        match self {
            Self::Builtin(builtin) => Some(*builtin),
            Self::Custom(_) => None,
        }
    }

    /// The custom type this kind names, or `None` for a builtin.
    pub const fn custom(&self) -> Option<&C> {
        match self {
            Self::Custom(custom) => Some(custom),
            Self::Builtin(_) => None,
        }
    }
}

/// What an embedder teaches a [`Backend`] about its own scalar types.
///
/// One implementation describes the whole custom set: [`Self::Kind`] names
/// the types (one variant each, so identity is compile-time and two
/// same-named types in different schemas stay distinct), and
/// [`Self::Value`] carries a decoded one.
///
/// The four methods are the whole surface, and each answers exactly once
/// for the read side, the write side, and SQL literals alike:
///
/// * [`Self::classify`] turns a declared catalog type name into a kind, and
///   is the single rule both directions consult, so a column cannot bind as
///   one type and read back as another.
/// * [`Self::carrier`] names the builtin shape the type travels as on the
///   wire, which lets the existing decoders keep producing what they always
///   did before conversion.
/// * [`Self::convert`] turns that builtin value into a custom one. A change
///   event, a diesel bind and a SQL literal all arrive here, so a filter's
///   value and a row's value cannot disagree about what a spelling means.
/// * [`Self::can_key`] answers whether a membership term may compare the
///   type, the same question [`crate::term::kind_can_key`] answers for
///   builtins.
pub trait CustomScalars: 'static {
    /// The embedder's types, one variant per type. The serde bounds because
    /// a compiled predicate persists the comparison facts of every column it
    /// loads, and those name the column's kind.
    type Kind: Copy
        + Eq
        + core::hash::Hash
        + core::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static;

    /// A decoded custom value. `Eq + Hash` because a membership term keys
    /// its lookup on this value (see [`Self::can_key`]), and the serde
    /// bounds because a literal of this type reaches a persisted predicate.
    type Value: Clone
        + core::fmt::Debug
        + Eq
        + core::hash::Hash
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static;
    /// Which custom type, if any, a declared catalog type name means.
    ///
    /// Receives the name as the catalog spells it. An embedder that wants
    /// two same-named types in different schemas told apart resolves that
    /// here, which a bare name comparison could not.
    fn classify(declared_type: &str) -> Option<Self::Kind>;

    /// The builtin shape `kind` travels as on the wire.
    fn carrier(kind: Self::Kind) -> BuiltinKind;

    /// Turn a wire or literal value of `kind`'s carrier shape into a custom
    /// value, or refuse it.
    ///
    /// Refusing is reported against the column that carried it rather than
    /// silently dropping the cell. The argument is a [`Carried`] rather than
    /// a [`Value`] because a [`Value`] can itself be a custom, and a custom
    /// never carries a custom.
    fn convert(kind: Self::Kind, carried: Carried<'_, Self::Carrier>) -> Option<Self::Value>;

    /// The backend these types extend, named so [`Self::convert`] can read
    /// its payload types.
    type Carrier: Backend;

    /// May a membership term compare a column of this type?
    ///
    /// The builtin rule is that equality must be reflexive, since a lookup
    /// keyed on a value has to find what it stored. `true` requires the same
    /// of [`Self::Value`], which its `Eq` bound already promises, so this
    /// exists for a type that wants to refuse keying for its own reasons.
    fn can_key(kind: Self::Kind) -> bool;

    /// Which custom type a decoded value is.
    ///
    /// Keeps [`Value::scalar_kind`] total: a custom cell can name its own
    /// kind instead of reporting no kind at all, which would read as
    /// "carries no scalar".
    fn kind_of(value: &Self::Value) -> Self::Kind;
}

/// The custom set of a backend serving none.
///
/// Uninhabited [`Kind`](CustomScalars::Kind) and
/// [`Value`](CustomScalars::Value), so [`ScalarKind::Custom`] and
/// [`Value::Custom`] cannot be constructed for such a backend and every
/// method below discharges by matching an impossible value.
pub struct NoCustomScalars<B>(core::marker::PhantomData<fn() -> B>);

impl<B: Backend> CustomScalars for NoCustomScalars<B> {
    type Kind = NoCustom;
    type Value = NoCustom;
    type Carrier = B;

    fn classify(_declared_type: &str) -> Option<Self::Kind> {
        None
    }

    fn carrier(kind: Self::Kind) -> BuiltinKind {
        match kind {}
    }

    fn convert(kind: Self::Kind, _carried: Carried<'_, B>) -> Option<Self::Value> {
        match kind {}
    }

    fn can_key(kind: Self::Kind) -> bool {
        match kind {}
    }

    fn kind_of(value: &Self::Value) -> Self::Kind {
        // No `&NoCustom` can exist, since the type is uninhabited, so this is
        // unreachable. Discharging it by matching through the reference would
        // be a dereference of an uninhabited place.
        let _ = value;
        unreachable!("NoCustom is uninhabited, so no reference to one exists")
    }
}

/// A borrowed builtin payload of `B`, handed to
/// [`CustomScalars::convert`].
///
/// One variant per builtin kind and no custom position, so the type states
/// what the contract needs: whatever carries a custom value is itself never
/// custom. Borrowed rather than owned, since the decoders already hold the
/// value they just built.
#[derive(Debug)]
pub enum Carried<'a, B: Backend> {
    /// [`Backend::Bool`] payload.
    Bool(&'a B::Bool),
    /// [`Backend::Int`] payload.
    Int(&'a B::Int),
    /// [`Backend::Float`] payload.
    Float(&'a B::Float),
    /// [`Backend::String`] payload.
    String(&'a B::String),
    /// [`Backend::Bytes`] payload.
    Bytes(&'a B::Bytes),
    /// [`Backend::Uuid`] payload.
    Uuid(&'a B::Uuid),
    /// [`Backend::Timestamp`] payload.
    Timestamp(&'a B::Timestamp),
    /// [`Backend::TimestampTz`] payload.
    TimestampTz(&'a B::TimestampTz),
    /// [`Backend::Date`] payload.
    Date(&'a B::Date),
    /// [`Backend::Time`] payload.
    Time(&'a B::Time),
    /// [`Backend::Decimal`] payload.
    Decimal(&'a B::Decimal),
    /// [`Backend::Json`] payload.
    Json(&'a B::Json),
    /// [`Backend::Jsonb`] payload.
    Jsonb(&'a B::Jsonb),
}

/// A scalar value carried in the VM's evaluation stack, or as a literal
/// operand in the compiled bytecode.
///
/// One variant per scalar type on the backend, plus [`Value::Missing`] and
/// [`Value::Null`], which name cells the source did not carry and SQL NULL
/// respectively. [`crate::backend::CdcEvent::value_at`] returns this type directly.
///
/// The variants own their payload so a `Value` moved onto the VM stack does
/// not need to keep the event alive. LoadColumn instructions clone the
/// scalar out of the event when they push. String, Bytes, Decimal, Json,
/// and Jsonb pay for that clone; primitive scalars are cheap.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]
pub enum Value<B: Backend> {
    /// Cell not carried by the source's row image.
    Missing,
    /// SQL NULL.
    Null,
    /// [`Backend::Bool`] payload.
    Bool(B::Bool),
    /// [`Backend::Int`] payload.
    Int(B::Int),
    /// [`Backend::Float`] payload.
    Float(B::Float),
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
    /// [`Backend::Json`] payload.
    Json(B::Json),
    /// [`Backend::Jsonb`] payload.
    Jsonb(B::Jsonb),
    /// One of the embedder's own types, decoded through
    /// [`CustomScalars::convert`].
    Custom(<B::Custom as CustomScalars>::Value),
}

impl<B: Backend> Value<B> {
    /// Discriminant tag for this value, or `None` for [`Value::Missing`] and
    /// [`Value::Null`] (which do not correspond to a specific scalar type).
    #[inline]
    pub fn scalar_kind(&self) -> Option<ValueKindOf<B>> {
        Some(match self {
            Self::Missing | Self::Null => return None,
            Self::Bool(_) => ValueKind::Builtin(BuiltinKind::Bool),
            Self::Int(_) => ValueKind::Builtin(BuiltinKind::Int),
            Self::Float(_) => ValueKind::Builtin(BuiltinKind::Float),
            Self::String(_) => ValueKind::Builtin(BuiltinKind::String),
            Self::Bytes(_) => ValueKind::Builtin(BuiltinKind::Bytes),
            Self::Uuid(_) => ValueKind::Builtin(BuiltinKind::Uuid),
            Self::Timestamp(_) => ValueKind::Builtin(BuiltinKind::Timestamp),
            Self::TimestampTz(_) => ValueKind::Builtin(BuiltinKind::TimestampTz),
            Self::Date(_) => ValueKind::Builtin(BuiltinKind::Date),
            Self::Time(_) => ValueKind::Builtin(BuiltinKind::Time),
            Self::Decimal(_) => ValueKind::Builtin(BuiltinKind::Decimal),
            Self::Json(_) => ValueKind::Builtin(BuiltinKind::Json),
            Self::Jsonb(_) => ValueKind::Builtin(BuiltinKind::Jsonb),
            Self::Custom(value) => ValueKind::Custom(<B::Custom as CustomScalars>::kind_of(value)),
        })
    }

    /// This value as a borrowed builtin payload, or `None` when it is
    /// absent or already custom.
    ///
    /// Feeds [`CustomScalars::convert`], which reads what a carrier
    /// delivered and never reads a custom.
    #[must_use]
    pub const fn as_carried(&self) -> Option<Carried<'_, B>> {
        Some(match self {
            Self::Missing | Self::Null | Self::Custom(_) => return None,
            Self::Bool(x) => Carried::Bool(x),
            Self::Int(x) => Carried::Int(x),
            Self::Float(x) => Carried::Float(x),
            Self::String(x) => Carried::String(x),
            Self::Bytes(x) => Carried::Bytes(x),
            Self::Uuid(x) => Carried::Uuid(x),
            Self::Timestamp(x) => Carried::Timestamp(x),
            Self::TimestampTz(x) => Carried::TimestampTz(x),
            Self::Date(x) => Carried::Date(x),
            Self::Time(x) => Carried::Time(x),
            Self::Decimal(x) => Carried::Decimal(x),
            Self::Json(x) => Carried::Json(x),
            Self::Jsonb(x) => Carried::Jsonb(x),
        })
    }

    /// True when this value carries no scalar (`Missing` or `Null`).
    #[inline]
    pub const fn is_absent(&self) -> bool {
        matches!(self, Self::Missing | Self::Null)
    }

    /// True when this value is `Missing`.
    #[inline]
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    /// True when this value is `Null`.
    #[inline]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

// `Clone`, `Debug`, and `PartialEq` on `Value<B>` are hand-implemented so
// their bounds fall on the scalar payloads (all of which are `ScalarCore`
// and therefore `Clone + Debug + PartialEq`) rather than on `B` itself.
// `#[derive(...)]` would defensively add `B: Clone` / `B: Debug` / etc.
// which is not implied by `Backend` and would prevent `Vm<B: Backend>`,
// `Instruction<B: Backend>` etc. from being usable in generic contexts.

impl<B: Backend> Clone for Value<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Missing => Self::Missing,
            Self::Null => Self::Null,
            Self::Bool(x) => Self::Bool(x.clone()),
            Self::Int(x) => Self::Int(x.clone()),
            Self::Float(x) => Self::Float(x.clone()),
            Self::String(x) => Self::String(x.clone()),
            Self::Bytes(x) => Self::Bytes(x.clone()),
            Self::Uuid(x) => Self::Uuid(x.clone()),
            Self::Timestamp(x) => Self::Timestamp(x.clone()),
            Self::TimestampTz(x) => Self::TimestampTz(x.clone()),
            Self::Date(x) => Self::Date(x.clone()),
            Self::Time(x) => Self::Time(x.clone()),
            Self::Decimal(x) => Self::Decimal(x.clone()),
            Self::Json(x) => Self::Json(x.clone()),
            Self::Jsonb(x) => Self::Jsonb(x.clone()),
            Self::Custom(x) => Self::Custom(x.clone()),
        }
    }
}

impl<B: Backend> core::fmt::Debug for Value<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Missing => f.write_str("Missing"),
            Self::Null => f.write_str("Null"),
            Self::Bool(x) => f.debug_tuple("Bool").field(x).finish(),
            Self::Int(x) => f.debug_tuple("Int").field(x).finish(),
            Self::Float(x) => f.debug_tuple("Float").field(x).finish(),
            Self::String(x) => f.debug_tuple("String").field(x).finish(),
            Self::Bytes(x) => f.debug_tuple("Bytes").field(x).finish(),
            Self::Uuid(x) => f.debug_tuple("Uuid").field(x).finish(),
            Self::Timestamp(x) => f.debug_tuple("Timestamp").field(x).finish(),
            Self::TimestampTz(x) => f.debug_tuple("TimestampTz").field(x).finish(),
            Self::Date(x) => f.debug_tuple("Date").field(x).finish(),
            Self::Time(x) => f.debug_tuple("Time").field(x).finish(),
            Self::Decimal(x) => f.debug_tuple("Decimal").field(x).finish(),
            Self::Json(x) => f.debug_tuple("Json").field(x).finish(),
            Self::Custom(x) => f.debug_tuple("Custom").field(x).finish(),
            Self::Jsonb(x) => f.debug_tuple("Jsonb").field(x).finish(),
        }
    }
}

impl<B: Backend> PartialEq for Value<B> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Missing, Self::Missing) => true,
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
            (Self::Timestamp(a), Self::Timestamp(b)) => a == b,
            (Self::TimestampTz(a), Self::TimestampTz(b)) => a == b,
            (Self::Date(a), Self::Date(b)) => a == b,
            (Self::Time(a), Self::Time(b)) => a == b,
            (Self::Decimal(a), Self::Decimal(b)) => a == b,
            (Self::Json(a), Self::Json(b)) => a == b,
            (Self::Jsonb(a), Self::Jsonb(b)) => jsonb_payloads_equal::<B>(a, b),
            // Without this arm the wildcard below answers `false` for two
            // equal custom values, which is the silent wrong answer the
            // wildcard hides from the compiler.
            (Self::Custom(a), Self::Custom(b)) => a == b,
            _ => false,
        }
    }
}

#[cfg(test)]
mod jsonb_order_tests {
    use alloc::vec::Vec;

    /// PostgreSQL 16.15 answering `SELECT left > right` for each pair, as
    /// measured for this crate's `jsonb` work.
    const VECTORS: &[(&str, &str, bool)] = &[
        ("{}", "[1]", true),
        ("[]", "true", false),
        ("[1]", "true", true),
        ("true", "1", true),
        ("false", "1", true),
        ("1", "\"a\"", true),
        ("\"a\"", "null", true),
        ("[1,2]", "[9]", true),
        ("[2]", "[1,9]", false),
        ("{\"a\":1,\"b\":2}", "{\"z\":9}", true),
        ("{\"b\":1}", "{\"a\":9}", true),
        ("1.0", "1", false),
        ("\"a\"", "\"B\"", false),
    ];

    /// The canonical binary form is not ordered the way the server orders
    /// `jsonb`, so comparing encoded bytes is not a cheap way to answer an
    /// ordered `jsonb` comparison in process. This is the evidence behind
    /// classifying the form as a read instead of serving it: five of these
    /// thirteen measured pairs come out backwards.
    ///
    /// The last of them, `"a" > "B"`, is also why an ordering comparator
    /// alone would not settle the question: `jsonb` string ordering follows
    /// the database collation.
    #[test]
    fn canonical_bytes_do_not_order_like_postgres() {
        let disagreements: Vec<&str> = VECTORS
            .iter()
            .filter(|(left, right, postgres)| {
                let decode = |text: &str| -> serde_json::Value {
                    serde_json::from_str(text).expect("vector is valid JSON")
                };
                let encode = |value: &serde_json::Value| -> Vec<u8> {
                    postgres_jsonb_canonical::encode::<postgres_jsonb_canonical::Pg18>(value)
                        .expect("vector is storable")
                };
                (encode(&decode(left)) > encode(&decode(right))) != *postgres
            })
            .map(|(left, _, _)| *left)
            .collect();

        assert_eq!(
            disagreements,
            ["[]", "true", "false", "1", "\"a\""],
            "the pairs whose byte order contradicts the server, keyed by left operand"
        );
    }
}
