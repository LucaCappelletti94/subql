//! Typed row round trips through every CDC decoder subql ships.
//!
//! The harness draws column types and a row of values those columns can
//! hold, writes the row as each source writes it (pgoutput text through
//! `pg_walstream`'s encoder, wal2json v1 and v2, Maxwell), decodes it through
//! subql's `CdcEvent`, and requires `value_at` to hand back each value.
//!
//! Each cell says what a decoder may answer. A value subql represents has to
//! come back exactly, and one it cannot represent, such as a numeric `NaN`,
//! has to be refused as unanswerable. Where the wire writes the same thing
//! for two values, as wal2json's `null` for `NULL` and for a float `NaN`,
//! the decoder may refuse as well. Answering anything else is the bug.
//!
//! The spellings are the sources' own, and `tests/it/decoder_spelling.rs`
//! holds them to what a live PostgreSQL with wal2json and a live Maxwell
//! write for the same values.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Write as _;

use arbitrary::{Arbitrary, Unstructured};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use sql_traits::structs::ParserDB;
use uuid::Uuid;

use crate::backend::{CdcEvent, MySql, Postgres, RowKind, Value};
use crate::{ColumnId, PgCommitPosition, PgLsn, PgXid};

/// What a decoder may answer for one cell.
#[derive(Clone, Debug, PartialEq)]
pub enum Expect<B: crate::backend::Backend<Float = f64>> {
    /// The value, and nothing else.
    Exact(Value<B>),
    /// The value, or a refusal (`Value::Missing` or an error).
    Refusable(Value<B>),
    /// Only a refusal, since subql has no value for the cell.
    Refused,
}

impl<B: crate::backend::Backend<Float = f64>> Expect<B> {
    /// Whether `got` is an answer this cell allows.
    fn allows(&self, got: &Result<Value<B>, crate::ValueError>) -> bool {
        let refused = matches!(got, Ok(Value::Missing) | Err(_));
        match (self, got) {
            (Self::Exact(want), Ok(got)) => same(want, got),
            (Self::Refusable(want), Ok(got)) => refused || same(want, got),
            (Self::Refused, _) | (Self::Refusable(_), Err(_)) => refused,
            (Self::Exact(_), Err(_)) => false,
        }
    }

    /// The same value, which a decoder may now also refuse.
    #[must_use]
    pub fn or_refused(&self) -> Self {
        match self {
            Self::Exact(value) | Self::Refusable(value) => Self::Refusable(value.clone()),
            Self::Refused => Self::Refused,
        }
    }
}

/// Values as a decoder should answer them, with floats compared by value and
/// every `NaN` alike.
fn same<B: crate::backend::Backend<Float = f64>>(want: &Value<B>, got: &Value<B>) -> bool {
    match (want, got) {
        // A JSON number token `-0` is an integer zero to `serde_json`, so
        // wal2json's `-0` reads back unsigned, and SQL compares the two
        // zeros equal.
        (Value::Float(a), Value::Float(b)) => {
            let zero = |x: f64| x.to_bits() << 1 == 0;
            (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits() || (zero(*a) && zero(*b))
        }
        _ => want == got,
    }
}

/// A PostgreSQL column type the harness draws.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Arbitrary)]
pub enum PgColumn {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Numeric20x6,
    Text,
    Varchar40,
    Char8,
    Bytea,
    Uuid,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    TimeTz,
    Json,
    Jsonb,
}

impl PgColumn {
    /// The type as the column's DDL spells it and as `format_type` names it,
    /// which is what wal2json writes.
    const fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::Bool => ("BOOLEAN", "boolean"),
            Self::Int2 => ("SMALLINT", "smallint"),
            Self::Int4 => ("INTEGER", "integer"),
            Self::Int8 => ("BIGINT", "bigint"),
            Self::Float4 => ("REAL", "real"),
            Self::Float8 => ("DOUBLE PRECISION", "double precision"),
            Self::Numeric => ("NUMERIC", "numeric"),
            Self::Numeric20x6 => ("NUMERIC(20,6)", "numeric(20,6)"),
            Self::Text => ("TEXT", "text"),
            Self::Varchar40 => ("VARCHAR(40)", "character varying(40)"),
            Self::Char8 => ("CHAR(8)", "character(8)"),
            Self::Bytea => ("BYTEA", "bytea"),
            Self::Uuid => ("UUID", "uuid"),
            Self::Timestamp => ("TIMESTAMP", "timestamp without time zone"),
            Self::TimestampTz => ("TIMESTAMPTZ", "timestamp with time zone"),
            Self::Date => ("DATE", "date"),
            Self::Time => ("TIME", "time without time zone"),
            Self::TimeTz => ("TIMETZ", "time with time zone"),
            Self::Json => ("JSON", "json"),
            Self::Jsonb => ("JSONB", "jsonb"),
        }
    }

    /// The type as the column's DDL spells it.
    #[must_use]
    pub const fn ddl(self) -> &'static str {
        self.names().0
    }

    /// The type as `format_type` names it, which wal2json writes.
    #[must_use]
    pub const fn format_type(self) -> &'static str {
        self.names().1
    }

    /// The type's OID and modifier, as a `Relation` message carries them.
    const fn oid(self) -> (u32, i32) {
        match self {
            Self::Bool => (16, -1),
            Self::Int2 => (21, -1),
            Self::Int4 => (23, -1),
            Self::Int8 => (20, -1),
            Self::Float4 => (700, -1),
            Self::Float8 => (701, -1),
            Self::Numeric => (1700, -1),
            Self::Numeric20x6 => (1700, (20 << 16 | 6) + 4),
            Self::Text => (25, -1),
            Self::Varchar40 => (1043, 44),
            Self::Char8 => (1042, 12),
            Self::Bytea => (17, -1),
            Self::Uuid => (2950, -1),
            Self::Timestamp => (1114, -1),
            Self::TimestampTz => (1184, -1),
            Self::Date => (1082, -1),
            Self::Time => (1083, -1),
            Self::TimeTz => (1266, -1),
            Self::Json => (114, -1),
            Self::Jsonb => (3802, -1),
        }
    }

    /// Whether wal2json writes the output text as a bare JSON number.
    const fn numeric(self) -> bool {
        matches!(
            self,
            Self::Int2
                | Self::Int4
                | Self::Int8
                | Self::Float4
                | Self::Float8
                | Self::Numeric
                | Self::Numeric20x6
        )
    }
}

/// One PostgreSQL cell, with its output text or `None` for `NULL`, and what a
/// decoder may answer.
#[derive(Clone, Debug)]
pub struct PgCell {
    pub column: PgColumn,
    pub text: Option<String>,
    pub expect: Expect<Postgres>,
}

impl PgCell {
    /// A value `column` can hold, as the type's output function prints it.
    ///
    /// # Errors
    ///
    /// When `u` runs out of bytes.
    pub fn arbitrary(column: PgColumn, u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        if u.ratio(1u8, 10)? {
            return Ok(Self {
                column,
                text: None,
                expect: Expect::Exact(Value::Null),
            });
        }
        let (text, expect) = match column {
            PgColumn::Timestamp
            | PgColumn::TimestampTz
            | PgColumn::Date
            | PgColumn::Time
            | PgColumn::TimeTz
            | PgColumn::Json
            | PgColumn::Jsonb => pg_temporal_or_document(column, u)?,
            _ => pg_scalar(column, u)?,
        };
        Ok(Self {
            column,
            text: Some(text),
            expect,
        })
    }

    /// What a decoder of wal2json may answer, which is a refusal as well
    /// when wal2json writes `null` for a value that need not be `NULL`.
    #[must_use]
    pub fn wal2json_expect(&self) -> Expect<Postgres> {
        if self.column.numeric()
            && !matches!(
                self.column,
                PgColumn::Int2 | PgColumn::Int4 | PgColumn::Int8
            )
            && self.wal2json().is_null()
        {
            self.expect.or_refused()
        } else {
            self.expect.clone()
        }
    }

    /// The cell as wal2json writes it.
    #[must_use]
    pub fn wal2json(&self) -> serde_json::Value {
        let Some(text) = &self.text else {
            return serde_json::Value::Null;
        };
        if self.column == PgColumn::Bool {
            return serde_json::Value::Bool(text == "t");
        }
        if self.column.numeric() {
            // JSON has no NaN or infinity, and wal2json writes `null` for them.
            return serde_json::from_str::<serde_json::Number>(text)
                .map_or(serde_json::Value::Null, serde_json::Value::Number);
        }
        if self.column == PgColumn::Bytea {
            // wal2json drops the `\x` of the hex output.
            return serde_json::Value::String(text.trim_start_matches("\\x").to_string());
        }
        serde_json::Value::String(text.clone())
    }
}

/// A number, string, byte string, boolean or UUID cell's text and expectation.
fn pg_scalar(
    column: PgColumn,
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<(String, Expect<Postgres>)> {
    Ok(match column {
        PgColumn::Bool => {
            let b = bool::arbitrary(u)?;
            (
                String::from(if b { "t" } else { "f" }),
                Expect::Exact(Value::Bool(b)),
            )
        }
        PgColumn::Int2 => int_cell(i64::from(i16::arbitrary(u)?)),
        PgColumn::Int4 => int_cell(i64::from(i32::arbitrary(u)?)),
        PgColumn::Int8 => int_cell(i64::arbitrary(u)?),
        PgColumn::Float4 => {
            let x = special_or(u, |u| f32::arbitrary(u).map(f64::from))?;
            #[allow(clippy::cast_possible_truncation)]
            let single = x as f32;
            (
                pg_float(f64::from(single), true),
                Expect::Exact(Value::Float(f64::from(single))),
            )
        }
        PgColumn::Float8 => {
            let x = special_or(u, f64::arbitrary)?;
            (pg_float(x, false), Expect::Exact(Value::Float(x)))
        }
        PgColumn::Numeric => {
            if u.ratio(1u8, 12)? {
                let text = *u.choose(&["NaN", "Infinity", "-Infinity"])?;
                (String::from(text), Expect::Refused)
            } else {
                let int_digits = u.int_in_range(1..=30)?;
                let scale = u.int_in_range(0..=20)?;
                decimal_cell(u, int_digits, scale)?
            }
        }
        PgColumn::Numeric20x6 => {
            let int_digits = u.int_in_range(1..=14)?;
            decimal_cell(u, int_digits, 6)?
        }
        PgColumn::Text => {
            let text = text_without_nul(u, 64)?;
            (text.clone(), Expect::Exact(Value::String(text)))
        }
        PgColumn::Varchar40 => {
            let text = text_without_nul(u, 40)?;
            (text.clone(), Expect::Exact(Value::String(text)))
        }
        PgColumn::Char8 => {
            let mut text = text_without_nul(u, 8)?;
            // bpchar pads to its length, and the output keeps the padding.
            let pad = 8 - text.chars().count();
            text.extend(core::iter::repeat_n(' ', pad));
            (text.clone(), Expect::Exact(Value::String(text)))
        }
        PgColumn::Bytea => {
            let bytes = Vec::<u8>::arbitrary(u)?;
            let mut text = String::from("\\x");
            for byte in &bytes {
                write!(text, "{byte:02x}").expect("writing to a String cannot fail");
            }
            (text, Expect::Exact(Value::Bytes(bytes)))
        }
        PgColumn::Uuid => {
            let uuid = Uuid::from_bytes(<[u8; 16]>::arbitrary(u)?);
            (
                uuid.hyphenated().to_string(),
                Expect::Exact(Value::Uuid(uuid)),
            )
        }
        _ => unreachable!("the caller routes every other column elsewhere"),
    })
}

/// A temporal or JSON cell's text and expectation.
fn pg_temporal_or_document(
    column: PgColumn,
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<(String, Expect<Postgres>)> {
    Ok(match column {
        PgColumn::Timestamp => {
            let at = pg_datetime(u)?;
            (
                pg_timestamp_text(at, None),
                Expect::Exact(Value::Timestamp(at)),
            )
        }
        PgColumn::TimestampTz => {
            let at = pg_datetime(u)?;
            let offset = *u.choose(&OFFSETS)?;
            let local = at + chrono::TimeDelta::seconds(i64::from(offset));
            let utc = DateTime::<Utc>::from_naive_utc_and_offset(at, Utc);
            (
                pg_timestamp_text(local, Some(offset)),
                Expect::Exact(Value::TimestampTz(utc)),
            )
        }
        PgColumn::Date => {
            let date = pg_date(u)?;
            (pg_date_text(date), Expect::Exact(Value::Date(date)))
        }
        PgColumn::Time => {
            let time = time(u)?;
            (time_text(time), Expect::Exact(Value::Time(time)))
        }
        PgColumn::TimeTz => {
            let time = time(u)?;
            let offset = *u.choose(&OFFSETS)?;
            // A time with a zone is not a time, and subql has no value
            // that keeps the zone.
            (
                format!("{}{}", time_text(time), offset_text(offset)),
                Expect::Refused,
            )
        }
        PgColumn::Json => {
            let document = json_document(u, 3, true)?;
            let text = if u.arbitrary()? {
                serde_json::to_string_pretty(&document)
            } else {
                serde_json::to_string(&document)
            }
            .map_err(|_| arbitrary::Error::IncorrectFormat)?;
            (text, Expect::Exact(Value::Json(document)))
        }
        PgColumn::Jsonb => {
            let document = json_document(u, 3, true)?;
            (jsonb_text(&document), Expect::Exact(Value::Jsonb(document)))
        }
        _ => unreachable!("the caller routes every other column elsewhere"),
    })
}

fn int_cell(value: i64) -> (String, Expect<Postgres>) {
    (value.to_string(), Expect::Exact(Value::Int(value)))
}

/// A decimal with `int_digits` digits before the point and `scale` after,
/// printed as PostgreSQL prints it, which keeps the scale.
fn decimal_cell(
    u: &mut Unstructured<'_>,
    int_digits: usize,
    scale: usize,
) -> arbitrary::Result<(String, Expect<Postgres>)> {
    let (text, value) = decimal_text(u, int_digits, scale)?;
    Ok((text, Expect::Exact(Value::Decimal(value))))
}

/// A decimal and its text, the scale kept and a zero printed unsigned.
fn decimal_text(
    u: &mut Unstructured<'_>,
    int_digits: usize,
    scale: usize,
) -> arbitrary::Result<(String, BigDecimal)> {
    let digit = |u: &mut Unstructured<'_>| u.int_in_range(b'0'..=b'9').map(char::from);
    let mut int_part = String::new();
    for _ in 0..int_digits {
        int_part.push(digit(u)?);
    }
    let int_part = int_part.trim_start_matches('0');
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    let mut text = String::new();
    let negative = bool::arbitrary(u)?;
    text.push_str(int_part);
    if scale > 0 {
        text.push('.');
        for _ in 0..scale {
            text.push(digit(u)?);
        }
    }
    // PostgreSQL prints a zero without its sign.
    let zero = text.chars().all(|c| matches!(c, '0' | '.'));
    if negative && !zero {
        text.insert(0, '-');
    }
    let value: BigDecimal = text
        .parse()
        .map_err(|_| arbitrary::Error::IncorrectFormat)?;
    Ok((text, value))
}

/// A float from `draw`, or one of the values at a float's edges.
fn special_or<'a>(
    u: &mut Unstructured<'a>,
    draw: impl FnOnce(&mut Unstructured<'a>) -> arbitrary::Result<f64>,
) -> arbitrary::Result<f64> {
    if u.ratio(1u8, 6)? {
        Ok(*u.choose(&[
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1e15,
            1e-5,
            123_456.0,
            0.1,
        ])?)
    } else {
        draw(u)
    }
}

/// The shortest significant digits that read back as `x` at its width, its
/// sign, and the decimal exponent of its first digit.
fn shortest_digits(x: f64, single: bool) -> (&'static str, String, i32) {
    #[allow(clippy::cast_possible_truncation)]
    let scientific = if single {
        format!("{:e}", x as f32)
    } else {
        format!("{x:e}")
    };
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("LowerExp writes an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("LowerExp writes an integer exponent");
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |rest| ("-", rest));
    (
        sign,
        mantissa.chars().filter(char::is_ascii_digit).collect(),
        exponent,
    )
}

/// `x` as `float8out` or `float4out` prints it, with the shortest digits that
/// read back as `x`, in positional notation for decimal exponents from -4
/// up to 14 (float8) or 5 (float4) and as `d.ddde+XX` outside them.
#[must_use]
pub fn pg_float(x: f64, single: bool) -> String {
    if x.is_nan() {
        return String::from("NaN");
    }
    if x.is_infinite() {
        return String::from(if x > 0.0 { "Infinity" } else { "-Infinity" });
    }
    if x == 0.0 {
        return String::from(if x.is_sign_negative() { "-0" } else { "0" });
    }
    let (sign, digits, exponent) = shortest_digits(x, single);
    let limit = if single { 6 } else { 15 };
    let body = if (-4..limit).contains(&exponent) {
        positional(&digits, exponent)
    } else {
        let (first, rest) = digits.split_at(1);
        let fraction = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        let exponent_sign = if exponent < 0 { '-' } else { '+' };
        format!(
            "{first}{fraction}e{exponent_sign}{:02}",
            exponent.unsigned_abs()
        )
    };
    format!("{sign}{body}")
}

/// Significant `digits` whose first sits at decimal `exponent`, positionally.
fn positional(digits: &str, exponent: i32) -> String {
    if exponent < 0 {
        let zeros = usize::try_from(-exponent - 1).unwrap_or(0);
        return format!("0.{}{digits}", "0".repeat(zeros));
    }
    let whole = usize::try_from(exponent).unwrap_or(0) + 1;
    if digits.len() <= whole {
        format!("{digits}{}", "0".repeat(whole - digits.len()))
    } else {
        format!("{}.{}", &digits[..whole], &digits[whole..])
    }
}

fn text_without_nul(u: &mut Unstructured<'_>, max_chars: usize) -> arbitrary::Result<String> {
    let text = String::arbitrary(u)?;
    Ok(text
        .chars()
        .filter(|&c| c != '\0')
        .take(max_chars)
        .collect())
}

/// Server `TimeZone` offsets in seconds east of UTC, among them ones with
/// minutes and one with seconds, Amsterdam's local mean time, which
/// PostgreSQL prints as `+00:19:32` in winter through 1937 and in summer
/// before 1916.
const OFFSETS: [i32; 6] = [0, 3600, -18000, 19800, 20700, 1172];

/// A timestamp PostgreSQL stores, from 4713 BC to 294276 AD, mostly this era.
fn pg_datetime(u: &mut Unstructured<'_>) -> arbitrary::Result<NaiveDateTime> {
    Ok(pg_date(u)?.and_time(time(u)?))
}

fn pg_date(u: &mut Unstructured<'_>) -> arbitrary::Result<NaiveDate> {
    let year = if u.ratio(1u8, 5)? {
        u.int_in_range(-4712..=262_000)?
    } else {
        u.int_in_range(1900..=2100)?
    };
    let ordinal = u.int_in_range(1..=365)?;
    NaiveDate::from_yo_opt(year, ordinal).ok_or(arbitrary::Error::IncorrectFormat)
}

fn time(u: &mut Unstructured<'_>) -> arbitrary::Result<NaiveTime> {
    let seconds = u.int_in_range(0..=86_399)?;
    let micros = if u.arbitrary()? {
        0
    } else {
        u.int_in_range(0..=999_999)?
    };
    NaiveTime::from_num_seconds_from_midnight_opt(seconds, micros * 1000)
        .ok_or(arbitrary::Error::IncorrectFormat)
}

/// `HH:MM:SS` with the microseconds PostgreSQL keeps, trailing zeros dropped.
fn time_text(time: NaiveTime) -> String {
    let mut text = format!(
        "{:02}:{:02}:{:02}",
        time.hour(),
        time.minute(),
        time.second()
    );
    let micros = time.nanosecond() / 1000;
    if micros > 0 {
        let fraction = format!("{micros:06}");
        text.push('.');
        text.push_str(fraction.trim_end_matches('0'));
    }
    text
}

/// A year as PostgreSQL prints it, with four digits at least and the era after
/// the whole value when it is before Christ, where year 0 is 1 BC.
fn year_and_era(year: i32) -> (String, &'static str) {
    if year <= 0 {
        (format!("{:04}", 1 - year), " BC")
    } else {
        (format!("{year:04}"), "")
    }
}

fn pg_date_text(date: NaiveDate) -> String {
    let (year, era) = year_and_era(date.year());
    format!("{year}-{:02}-{:02}{era}", date.month(), date.day())
}

/// `+HH`, `+HH:MM` or `+HH:MM:SS`, as PostgreSQL writes a zone offset.
fn offset_text(offset: i32) -> String {
    let sign = if offset < 0 { '-' } else { '+' };
    let offset = offset.unsigned_abs();
    let (hours, minutes, seconds) = (offset / 3600, offset / 60 % 60, offset % 60);
    let mut text = format!("{sign}{hours:02}");
    if minutes != 0 || seconds != 0 {
        write!(text, ":{minutes:02}").expect("writing to a String cannot fail");
    }
    if seconds != 0 {
        write!(text, ":{seconds:02}").expect("writing to a String cannot fail");
    }
    text
}

fn pg_timestamp_text(at: NaiveDateTime, offset: Option<i32>) -> String {
    let (year, era) = year_and_era(at.year());
    let zone = offset.map(offset_text).unwrap_or_default();
    format!(
        "{year}-{:02}-{:02} {}{zone}{era}",
        at.month(),
        at.day(),
        time_text(at.time())
    )
}

/// A JSON document of integers, plain decimals, strings, booleans and nulls,
/// the numbers spelled the way both `json` and `jsonb` print them back.
fn json_document(
    u: &mut Unstructured<'_>,
    depth: u8,
    decimals: bool,
) -> arbitrary::Result<serde_json::Value> {
    let leaf_only = depth == 0;
    Ok(match u.int_in_range(0u8..=if leaf_only { 4 } else { 6 })? {
        0 => serde_json::Value::Null,
        1 => serde_json::Value::Bool(bool::arbitrary(u)?),
        2 => serde_json::Value::from(i64::arbitrary(u)?),
        3 if decimals => {
            let cents = i32::arbitrary(u)?;
            let text = format!(
                "{}{}.{:02}",
                if cents < 0 { "-" } else { "" },
                cents.unsigned_abs() / 100,
                cents.unsigned_abs() % 100
            );
            serde_json::from_str(&text).map_err(|_| arbitrary::Error::IncorrectFormat)?
        }
        3 | 4 => serde_json::Value::String(text_without_nul(u, 16)?),
        5 => {
            let len = u.int_in_range(0..=4)?;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(json_document(u, depth - 1, decimals)?);
            }
            serde_json::Value::Array(items)
        }
        _ => {
            let len = u.int_in_range(0..=4)?;
            let mut map = serde_json::Map::new();
            for _ in 0..len {
                map.insert(
                    text_without_nul(u, 8)?,
                    json_document(u, depth - 1, decimals)?,
                );
            }
            serde_json::Value::Object(map)
        }
    })
}

/// `document` as `jsonb` prints it, with keys ordered by length and then bytes,
/// `", "` between items and `": "` after a key.
#[must_use]
pub fn jsonb_text(document: &serde_json::Value) -> String {
    match document {
        serde_json::Value::Array(items) => {
            let items: Vec<String> = items.iter().map(jsonb_text).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
            let entries: Vec<String> = entries
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}: {}",
                        serde_json::Value::String(key.clone()),
                        jsonb_text(value)
                    )
                })
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        leaf => leaf.to_string(),
    }
}

/// A MySQL column type the harness draws.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Arbitrary)]
pub enum MyColumn {
    Boolean,
    TinyInt,
    Int,
    IntUnsigned,
    BigInt,
    BigIntUnsigned,
    Float,
    Double,
    Decimal20x6,
    Varchar40,
    Text,
    Char8,
    Binary16,
    Varbinary40,
    Blob,
    Datetime,
    Datetime6,
    Timestamp6,
    Date,
    Time6,
    Json,
}

impl MyColumn {
    /// The type as the column's DDL spells it.
    #[must_use]
    pub const fn ddl(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::TinyInt => "TINYINT",
            Self::Int => "INT",
            Self::IntUnsigned => "INT UNSIGNED",
            Self::BigInt => "BIGINT",
            Self::BigIntUnsigned => "BIGINT UNSIGNED",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::Decimal20x6 => "DECIMAL(20,6)",
            Self::Varchar40 => "VARCHAR(40)",
            Self::Text => "TEXT",
            Self::Char8 => "CHAR(8)",
            Self::Binary16 => "BINARY(16)",
            Self::Varbinary40 => "VARBINARY(40)",
            Self::Blob => "BLOB",
            Self::Datetime => "DATETIME",
            Self::Datetime6 => "DATETIME(6)",
            Self::Timestamp6 => "TIMESTAMP(6)",
            Self::Date => "DATE",
            Self::Time6 => "TIME(6)",
            Self::Json => "JSON",
        }
    }
}

/// One MySQL cell, with what Maxwell writes for it, how an `INSERT` spells it,
/// and what a decoder may answer.
#[derive(Clone, Debug)]
pub struct MyCell {
    pub column: MyColumn,
    pub maxwell: serde_json::Value,
    pub literal: String,
    pub expect: Expect<MySql>,
}

impl MyCell {
    /// A value `column` can hold, as Maxwell writes it.
    ///
    /// # Errors
    ///
    /// When `u` runs out of bytes.
    pub fn arbitrary(column: MyColumn, u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        if u.ratio(1u8, 10)? {
            // Maxwell writes a `JSON` column's `NULL` as it writes its `null`
            // document, so a decoder may refuse either.
            let expect = if column == MyColumn::Json {
                Expect::Refusable(Value::Null)
            } else {
                Expect::Exact(Value::Null)
            };
            return Ok(Self {
                column,
                maxwell: serde_json::Value::Null,
                literal: String::from("NULL"),
                expect,
            });
        }
        match column {
            MyColumn::Boolean
            | MyColumn::TinyInt
            | MyColumn::Int
            | MyColumn::IntUnsigned
            | MyColumn::BigInt
            | MyColumn::BigIntUnsigned
            | MyColumn::Float
            | MyColumn::Double
            | MyColumn::Decimal20x6 => my_number(column, u),
            _ => my_other(column, u),
        }
    }
}

/// A numeric or boolean MySQL cell.
fn my_number(column: MyColumn, u: &mut Unstructured<'_>) -> arbitrary::Result<MyCell> {
    Ok(match column {
        MyColumn::Boolean => {
            // `BOOLEAN` is `TINYINT(1)`, which holds any byte.
            let value = if u.ratio(1u8, 4)? {
                i8::arbitrary(u)?
            } else {
                i8::from(bool::arbitrary(u)?)
            };
            let expect = match value {
                0 => Expect::Exact(Value::Bool(false)),
                1 => Expect::Exact(Value::Bool(true)),
                _ => Expect::Refused,
            };
            number_cell(column, i128::from(value), expect)
        }
        MyColumn::TinyInt => my_int(column, i64::from(i8::arbitrary(u)?)),
        MyColumn::Int => my_int(column, i64::from(i32::arbitrary(u)?)),
        MyColumn::IntUnsigned => my_int(column, i64::from(u32::arbitrary(u)?)),
        MyColumn::BigInt => my_int(column, i64::arbitrary(u)?),
        MyColumn::BigIntUnsigned => {
            let value = u64::arbitrary(u)?;
            let expect = i64::try_from(value)
                .map_or(Expect::Refused, |fits| Expect::Exact(Value::Int(fits)));
            number_cell(column, i128::from(value), expect)
        }
        MyColumn::Float => {
            let single = f32::arbitrary(u)?;
            let single = if single.is_finite() { single } else { 1.5 };
            MyCell {
                column,
                maxwell: java_number(&java_float(f64::from(single), true)),
                literal: pg_float(f64::from(single), true),
                expect: Expect::Exact(Value::Float(f64::from(single))),
            }
        }
        MyColumn::Double => {
            let double = f64::arbitrary(u)?;
            let double = if double.is_finite() { double } else { -2.25 };
            MyCell {
                column,
                maxwell: java_number(&java_float(double, false)),
                literal: pg_float(double, false),
                expect: Expect::Exact(Value::Float(double)),
            }
        }
        MyColumn::Decimal20x6 => {
            let int_digits = u.int_in_range(1..=14)?;
            let (text, value) = decimal_text(u, int_digits, 6)?;
            MyCell {
                column,
                maxwell: java_number(&text),
                literal: text,
                expect: Expect::Exact(Value::Decimal(value)),
            }
        }
        _ => unreachable!("the caller routes every other column elsewhere"),
    })
}

/// A string, byte string, temporal or JSON MySQL cell.
fn my_other(column: MyColumn, u: &mut Unstructured<'_>) -> arbitrary::Result<MyCell> {
    Ok(match column {
        MyColumn::Varchar40 | MyColumn::Text => {
            let text = text_without_nul(u, if column == MyColumn::Text { 64 } else { 40 })?;
            string_cell(column, text)
        }
        MyColumn::Char8 => {
            // `CHAR` drops trailing spaces on the way out.
            let text = text_without_nul(u, 8)?.trim_end_matches(' ').to_string();
            string_cell(column, text)
        }
        MyColumn::Binary16 | MyColumn::Varbinary40 | MyColumn::Blob => my_binary(column, u)?,
        MyColumn::Datetime | MyColumn::Datetime6 | MyColumn::Timestamp6 => {
            let (low, high) = if column == MyColumn::Timestamp6 {
                (1971, 2037)
            } else {
                (1000, 9999)
            };
            let date =
                NaiveDate::from_yo_opt(u.int_in_range(low..=high)?, u.int_in_range(1..=365)?)
                    .ok_or(arbitrary::Error::IncorrectFormat)?;
            let mut at = date.and_time(time(u)?);
            if column == MyColumn::Datetime {
                at = at
                    .with_nanosecond(0)
                    .ok_or(arbitrary::Error::IncorrectFormat)?;
            }
            let text = my_datetime_text(at, column != MyColumn::Datetime);
            MyCell {
                column,
                maxwell: serde_json::Value::String(text.clone()),
                literal: format!("'{text}'"),
                expect: Expect::Exact(Value::Timestamp(at)),
            }
        }
        MyColumn::Date => {
            let date =
                NaiveDate::from_yo_opt(u.int_in_range(1000..=9999)?, u.int_in_range(1..=365)?)
                    .ok_or(arbitrary::Error::IncorrectFormat)?;
            let text = format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day());
            MyCell {
                column,
                maxwell: serde_json::Value::String(text.clone()),
                literal: format!("'{text}'"),
                expect: Expect::Exact(Value::Date(date)),
            }
        }
        MyColumn::Time6 => {
            let time = time(u)?;
            let text = format!(
                "{:02}:{:02}:{:02}.{:06}",
                time.hour(),
                time.minute(),
                time.second(),
                time.nanosecond() / 1000
            );
            MyCell {
                column,
                maxwell: serde_json::Value::String(text.clone()),
                literal: format!("'{text}'"),
                expect: Expect::Exact(Value::Time(time)),
            }
        }
        MyColumn::Json => {
            let document = json_document(u, 3, false)?;
            let text =
                serde_json::to_string(&document).map_err(|_| arbitrary::Error::IncorrectFormat)?;
            let expect = if document.is_null() {
                Expect::Refusable(Value::Json(document.clone()))
            } else {
                Expect::Exact(Value::Json(document.clone()))
            };
            MyCell {
                column,
                maxwell: document,
                literal: my_string_literal(&text),
                expect,
            }
        }
        _ => unreachable!("the caller routes every other column elsewhere"),
    })
}

/// A binary MySQL cell, which Maxwell writes in base64.
fn my_binary(column: MyColumn, u: &mut Unstructured<'_>) -> arbitrary::Result<MyCell> {
    let mut bytes = Vec::<u8>::arbitrary(u)?;
    match column {
        MyColumn::Binary16 => bytes.resize(16, 0),
        MyColumn::Varbinary40 => bytes.truncate(40),
        _ => {}
    }
    let mut hex = String::new();
    for byte in &bytes {
        write!(hex, "{byte:02X}").expect("writing to a String cannot fail");
    }
    // The binlog keeps a `BINARY(n)` value without its trailing zero
    // bytes, and Maxwell writes it so, in base64.
    let written = if column == MyColumn::Binary16 {
        let kept = bytes
            .iter()
            .rposition(|&byte| byte != 0)
            .map_or(0, |at| at + 1);
        &bytes[..kept]
    } else {
        &bytes[..]
    };
    Ok(MyCell {
        column,
        maxwell: serde_json::Value::String(base64(written)),
        literal: format!("X'{hex}'"),
        expect: Expect::Exact(Value::Bytes(bytes)),
    })
}

fn my_int(column: MyColumn, value: i64) -> MyCell {
    number_cell(column, i128::from(value), Expect::Exact(Value::Int(value)))
}

fn number_cell(column: MyColumn, value: i128, expect: Expect<MySql>) -> MyCell {
    MyCell {
        column,
        maxwell: java_number(&value.to_string()),
        literal: value.to_string(),
        expect,
    }
}

fn string_cell(column: MyColumn, text: String) -> MyCell {
    MyCell {
        column,
        maxwell: serde_json::Value::String(text.clone()),
        literal: my_string_literal(&text),
        expect: Expect::Exact(Value::String(text)),
    }
}

fn java_number(text: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Number>(text)
        .map_or(serde_json::Value::Null, serde_json::Value::Number)
}

/// A MySQL string literal, backslash and quote escaped.
fn my_string_literal(text: &str) -> String {
    let mut literal = String::from("'");
    for c in text.chars() {
        match c {
            '\'' => literal.push_str("''"),
            '\\' => literal.push_str("\\\\"),
            c => literal.push(c),
        }
    }
    literal.push('\'');
    literal
}

/// `x` as Java's `Double.toString` or `Float.toString` writes it, which is
/// what Jackson prints, positional from `1e-3` up to `1e7` with at least
/// one digit after the point, and `d.dddE-n` outside.
#[must_use]
pub fn java_float(x: f64, single: bool) -> String {
    if x == 0.0 {
        return String::from(if x.is_sign_negative() { "-0.0" } else { "0.0" });
    }
    let (sign, digits, exponent) = shortest_digits(x, single);
    let body = if (-3..7).contains(&exponent) {
        let text = positional(&digits, exponent);
        if text.contains('.') {
            text
        } else {
            format!("{text}.0")
        }
    } else {
        let (first, rest) = digits.split_at(1);
        let rest = if rest.is_empty() { "0" } else { rest };
        format!("{first}.{rest}E{exponent}")
    };
    format!("{sign}{body}")
}

fn my_datetime_text(at: NaiveDateTime, micros: bool) -> String {
    let mut text = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        at.year(),
        at.month(),
        at.day(),
        at.hour(),
        at.minute(),
        at.second()
    );
    if micros {
        write!(text, ".{:06}", at.nanosecond() / 1000).expect("writing to a String cannot fail");
    }
    text
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut text = String::new();
    for chunk in bytes.chunks(3) {
        let word = chunk.iter().enumerate().fold(0u32, |word, (i, &byte)| {
            word | u32::from(byte) << (16 - 8 * i)
        });
        for i in 0..4 {
            if i <= chunk.len() {
                text.push(char::from(ALPHABET[(word >> (18 - 6 * i) & 63) as usize]));
            } else {
                text.push('=');
            }
        }
    }
    text
}

/// The source a row is written as.
#[derive(Clone, Copy, Debug, Arbitrary)]
enum Source {
    PgOutput,
    Wal2JsonV1,
    Wal2JsonV2,
    Maxwell,
}

/// The cells one row checked, by source, so a run that checks nothing, or
/// only `NULL`s, is visible.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecoderCoverage {
    /// Cells decoded from pgoutput.
    pub pgoutput: usize,
    /// Cells decoded from wal2json v1.
    pub wal2json_v1: usize,
    /// Cells decoded from wal2json v2.
    pub wal2json_v2: usize,
    /// Cells decoded from Maxwell.
    pub maxwell: usize,
    /// Of those, cells that had to come back as a value other than `NULL`.
    pub values: usize,
}

impl core::ops::AddAssign for DecoderCoverage {
    fn add_assign(&mut self, other: Self) {
        self.pgoutput += other.pgoutput;
        self.wal2json_v1 += other.wal2json_v1;
        self.wal2json_v2 += other.wal2json_v2;
        self.maxwell += other.maxwell;
        self.values += other.values;
    }
}

/// Whether `expect` demands a value other than `NULL`.
const fn demands_value<B: crate::backend::Backend<Float = f64>>(expect: &Expect<B>) -> bool {
    matches!(expect, Expect::Exact(value) if !matches!(value, Value::Null))
}

/// Write one generated row as a source writes it, decode it through subql,
/// and require every cell to come back as the cell allows.
///
/// # Panics
///
/// When a decoder answers a cell with a value it does not hold, or refuses
/// one it must answer.
pub fn harness_decoder_roundtrip(data: &[u8]) {
    let _ = decoder_roundtrip(data);
}

/// [`harness_decoder_roundtrip`], reporting what the row checked.
///
/// # Panics
///
/// As [`harness_decoder_roundtrip`].
#[must_use]
pub fn decoder_roundtrip(data: &[u8]) -> DecoderCoverage {
    let mut coverage = DecoderCoverage::default();
    let mut u = Unstructured::new(data);
    let Ok(source) = Source::arbitrary(&mut u) else {
        return coverage;
    };
    let Ok(width) = u.int_in_range(1usize..=8) else {
        return coverage;
    };
    match source {
        Source::Maxwell => {
            let mut cells = Vec::with_capacity(width);
            for _ in 0..width {
                let Ok(column) = MyColumn::arbitrary(&mut u) else {
                    return coverage;
                };
                let Ok(cell) = MyCell::arbitrary(column, &mut u) else {
                    return coverage;
                };
                cells.push(cell);
            }
            check_maxwell(&cells);
            coverage.maxwell = cells.len();
            coverage.values = cells
                .iter()
                .filter(|cell| demands_value(&cell.expect))
                .count();
        }
        source => {
            let mut cells = Vec::with_capacity(width);
            for _ in 0..width {
                let Ok(column) = PgColumn::arbitrary(&mut u) else {
                    return coverage;
                };
                let Ok(cell) = PgCell::arbitrary(column, &mut u) else {
                    return coverage;
                };
                cells.push(cell);
            }
            check_postgres(source, &cells);
            let count = cells.len();
            match source {
                Source::PgOutput => coverage.pgoutput = count,
                Source::Wal2JsonV1 => coverage.wal2json_v1 = count,
                _ => coverage.wal2json_v2 = count,
            }
            coverage.values = if matches!(source, Source::PgOutput) {
                cells
                    .iter()
                    .filter(|cell| demands_value(&cell.expect))
                    .count()
            } else {
                cells
                    .iter()
                    .filter(|cell| demands_value(&cell.wal2json_expect()))
                    .count()
            };
        }
    }
    coverage
}

fn ddl<'a>(types: impl Iterator<Item = &'a str>) -> String {
    let columns: Vec<String> = types
        .enumerate()
        .map(|(i, ty)| format!(", c{i} {ty}"))
        .collect();
    format!(
        "CREATE TABLE t (id INTEGER PRIMARY KEY{});",
        columns.concat()
    )
}

/// Require each decoded cell to be one its expectation allows.
fn require<B: crate::backend::Backend<Float = f64> + core::fmt::Debug>(
    source: &str,
    event: &impl CdcEvent<Backend = B>,
    db: &ParserDB,
    cells: &[(&str, &str, &Expect<B>)],
) {
    for (i, (ty, spelled, expect)) in cells.iter().enumerate() {
        let column = ColumnId::try_from(i + 1).expect("at most eight columns");
        let got = event.value_at(db, RowKind::New, column);
        assert!(
            expect.allows(&got),
            "{source} decodes {ty} cell {spelled} as {got:?} where {expect:?} is allowed"
        );
    }
}

fn check_postgres(source: Source, cells: &[PgCell]) {
    let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(&ddl(cells
        .iter()
        .map(|cell| cell.column.ddl())))
    .expect("the generated table parses");
    let spelled: Vec<String> = cells
        .iter()
        .map(|cell| format!("{:?}", cell.text))
        .collect();
    let wal2json_expects: Vec<Expect<Postgres>> =
        cells.iter().map(PgCell::wal2json_expect).collect();
    let expectations: Vec<(&str, &str, &Expect<Postgres>)> = cells
        .iter()
        .zip(&spelled)
        .zip(&wal2json_expects)
        .map(|((cell, spelled), wal2json)| {
            let expect = if matches!(source, Source::PgOutput) {
                &cell.expect
            } else {
                wal2json
            };
            (cell.column.ddl(), spelled.as_str(), expect)
        })
        .collect();
    let names = || (0..cells.len()).map(|i| format!("c{i}"));
    match source {
        Source::PgOutput => {
            let event = pgoutput_insert(cells);
            require("pgoutput", &event, &db, &expectations);
        }
        Source::Wal2JsonV1 => {
            let mut columnnames = vec![serde_json::Value::from("id")];
            columnnames.extend(names().map(serde_json::Value::from));
            let mut columntypes = vec![serde_json::Value::from("integer")];
            columntypes.extend(cells.iter().map(|c| c.column.format_type().into()));
            let mut columnvalues = vec![serde_json::Value::from(1)];
            columnvalues.extend(cells.iter().map(PgCell::wal2json));
            let message = serde_json::json!({
                "xid": 1,
                "change": [{
                    "kind": "insert",
                    "schema": "public",
                    "table": "t",
                    "columnnames": columnnames,
                    "columntypes": columntypes,
                    "columnvalues": columnvalues,
                }],
            });
            let bytes = serde_json::to_vec(&message).expect("the message serializes");
            let changes = crate::parse_wal2json_v1(&bytes).expect("the v1 message parses");
            let [event] = changes.as_slice() else {
                panic!("one v1 insert decodes to {} changes", changes.len());
            };
            require("wal2json v1", event, &db, &expectations);
        }
        _ => {
            let mut columns =
                vec![serde_json::json!({"name": "id", "type": "integer", "value": 1})];
            columns.extend(cells.iter().zip(names()).map(|(cell, name)| {
                serde_json::json!({
                    "name": name,
                    "type": cell.column.format_type(),
                    "value": cell.wal2json(),
                })
            }));
            let message = serde_json::json!({
                "action": "I",
                "schema": "public",
                "table": "t",
                "columns": columns,
            });
            let bytes = serde_json::to_vec(&message).expect("the message serializes");
            let mut reader = crate::Wal2JsonV2Reader::new();
            reader
                .parse(br#"{"action":"B"}"#)
                .expect("a transaction opens");
            let event = reader
                .parse(&bytes)
                .expect("the v2 message parses")
                .expect("an insert is a row event");
            require("wal2json v2", &event, &db, &expectations);
        }
    }
}

/// The row as a `Relation` and an `Insert` message, through
/// `pg_walstream`'s encoder and decoder.
fn pgoutput_insert(cells: &[PgCell]) -> crate::PgChangeEvent {
    use pg_walstream::{
        encode_message, ColumnData, ColumnInfo, LogicalReplicationMessage, Lsn, PgOutputDecoder,
        TupleData,
    };

    let mut columns = vec![ColumnInfo::new(1, String::from("id"), 23, -1)];
    columns.extend(cells.iter().enumerate().map(|(i, cell)| {
        let (oid, modifier) = cell.column.oid();
        ColumnInfo::new(0, format!("c{i}"), oid, modifier)
    }));
    let mut tuple = vec![ColumnData::text(b"1".to_vec())];
    tuple.extend(cells.iter().map(|cell| {
        cell.text.as_ref().map_or_else(ColumnData::null, |text| {
            ColumnData::text(text.as_bytes().to_vec())
        })
    }));
    let mut decoder = PgOutputDecoder::with_protocol_version(1);
    let mut decode = |message: &LogicalReplicationMessage| {
        let mut buf = bytes::BytesMut::new();
        encode_message(message, 1, &mut buf);
        decoder
            .decode_message(buf, Lsn::new(1))
            .expect("pg_walstream decodes what it encodes")
    };
    decode(&LogicalReplicationMessage::Relation {
        relation_id: 16_384,
        namespace: "public".into(),
        relation_name: "t".into(),
        replica_identity: b'd',
        columns,
    });
    let change = decode(&LogicalReplicationMessage::Insert {
        relation_id: 16_384,
        tuple: TupleData::new(tuple),
    })
    .expect("an insert is a change");
    crate::PgChangeEvent::new(change, PgCommitPosition::new(PgLsn(1), PgXid(1), 1))
}

fn check_maxwell(cells: &[MyCell]) {
    let db = ParserDB::parse::<sqlparser::dialect::MySqlDialect>(&ddl(cells
        .iter()
        .map(|cell| cell.column.ddl())))
    .expect("the generated table parses");
    let mut data = serde_json::Map::new();
    data.insert(String::from("id"), serde_json::Value::from(1));
    for (i, cell) in cells.iter().enumerate() {
        data.insert(format!("c{i}"), cell.maxwell.clone());
    }
    let message = serde_json::json!({
        "database": "db",
        "table": "t",
        "type": "insert",
        "ts": 1,
        "xid": 1,
        "commit": true,
        "data": data,
        "primary_key_columns": ["id"],
    });
    let bytes = serde_json::to_vec(&message).expect("the message serializes");
    let messages = crate::parse_maxwell(&bytes).expect("the Maxwell message parses");
    let [message] = <[_; 1]>::try_from(messages).expect("one Maxwell insert decodes to one row");
    let event = crate::MaxwellEvent::<crate::backend::NamesStoredAsWritten>::new(message);
    let spelled: Vec<String> = cells.iter().map(|cell| cell.maxwell.to_string()).collect();
    let expectations: Vec<(&str, &str, &Expect<MySql>)> = cells
        .iter()
        .zip(&spelled)
        .map(|(cell, spelled)| (cell.column.ddl(), spelled.as_str(), &cell.expect))
        .collect();
    require("Maxwell", &event, &db, &expectations);
}

#[cfg(test)]
mod tests {
    use super::{jsonb_text, pg_float};

    #[test]
    fn floats_print_as_float8out_and_float4out() {
        for (x, text) in [
            (1e20, "1e+20"),
            (1.5e-5, "1.5e-05"),
            (123_456_789_012_345.0, "123456789012345"),
            (1e15, "1e+15"),
            (0.0001, "0.0001"),
            (-2.5, "-2.5"),
            (100.0, "100"),
        ] {
            assert_eq!(pg_float(x, false), text);
        }
        assert_eq!(pg_float(f64::from(0.1f32), true), "0.1");
        assert_eq!(pg_float(1e6, true), "1e+06");
        assert_eq!(pg_float(123_456.0, true), "123456");
    }

    #[test]
    fn jsonb_orders_keys_by_length_then_bytes() {
        let document = serde_json::json!({"bb": 1, "a": [true, null], "c": "x"});
        assert_eq!(
            jsonb_text(&document),
            r#"{"a": [true, null], "c": "x", "bb": 1}"#
        );
    }
}
