//! One acceptance set for temporal text.
//!
//! Registration, the Postgres text and JSON wire paths, and the SQLite
//! changeset path all read temporal columns out of text, and a spelling one
//! of them accepts every other has to accept too: a filter naming the text
//! the database just printed must reach the rows carrying it. The set is
//! RFC 3339 plus the forms Postgres prints. Each caller wraps the `None`
//! in what its own position means, a registration type error or a
//! [`crate::backend::Value::Missing`] wire cell.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

/// `TIMESTAMP` text: date and time separated by a space or `T`, with an
/// optional fractional second.
pub fn parse_timestamp(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .ok()
}

/// `TIMESTAMPTZ` text, normalized to UTC: RFC 3339, or the space-separated
/// form Postgres prints, whose offset may be `+hh`, `+hhmm`, `+hh:mm`, or
/// `Z`. An offset is required, since a bare timestamp names no instant.
pub fn parse_timestamp_tz(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M:%S%z",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    None
}

/// `DATE` text.
pub fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// `TIME` text, with an optional fractional second.
pub fn parse_time(s: &str) -> Option<NaiveTime> {
    NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
pub mod corpus {
    //! The temporal text every subql parser must read alike, paired with the
    //! value it means. Each call site loops this set in its own test, so a
    //! parser whose set drifts fails by name where it drifted.

    use alloc::vec::Vec;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

    use crate::backend::{Backend, BuiltinKind, ScalarKind, Value};

    /// The value a corpus entry means. All three shipped backends map the
    /// four temporal kinds to these same `chrono` types, so one shape
    /// serves every call site.
    #[derive(Debug, Clone, Copy)]
    pub enum Temporal {
        Timestamp(NaiveDateTime),
        TimestampTz(DateTime<Utc>),
        Date(NaiveDate),
        Time(NaiveTime),
    }

    impl Temporal {
        pub const fn kind(self) -> BuiltinKind {
            match self {
                Self::Timestamp(_) => ScalarKind::Timestamp,
                Self::TimestampTz(_) => ScalarKind::TimestampTz,
                Self::Date(_) => ScalarKind::Date,
                Self::Time(_) => ScalarKind::Time,
            }
        }

        pub fn value<B>(self) -> Value<B>
        where
            B: Backend<
                Timestamp = NaiveDateTime,
                TimestampTz = DateTime<Utc>,
                Date = NaiveDate,
                Time = NaiveTime,
            >,
        {
            match self {
                Self::Timestamp(v) => Value::Timestamp(v),
                Self::TimestampTz(v) => Value::TimestampTz(v),
                Self::Date(v) => Value::Date(v),
                Self::Time(v) => Value::Time(v),
            }
        }
    }

    fn naive(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, milli: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, mo, d)
            .and_then(|date| date.and_hms_milli_opt(h, mi, s, milli))
            .expect("corpus timestamps are valid")
    }

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, milli: u32) -> DateTime<Utc> {
        naive(y, mo, d, h, mi, s, milli).and_utc()
    }

    fn date(y: i32, mo: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, mo, d).expect("corpus dates are valid")
    }

    fn time(h: u32, mi: u32, s: u32, milli: u32) -> NaiveTime {
        NaiveTime::from_hms_milli_opt(h, mi, s, milli).expect("corpus times are valid")
    }

    /// Text every temporal parser accepts, with the instant it denotes.
    ///
    /// The timestamptz entries all name one instant through the spellings
    /// Postgres prints and the offsets it prints them with, so a parser that
    /// accepts a spelling but reads a different instant fails too.
    pub fn accepted() -> Vec<(&'static str, Temporal)> {
        let midnight = Temporal::TimestampTz(utc(2026, 1, 1, 0, 0, 0, 0));
        let midnight_half = Temporal::TimestampTz(utc(2026, 1, 1, 0, 0, 0, 500));
        let naive_midnight = Temporal::Timestamp(naive(2026, 1, 1, 0, 0, 0, 0));
        let naive_half = Temporal::Timestamp(naive(2026, 1, 1, 0, 0, 0, 500));
        vec![
            // The form `SELECT ts` prints under a UTC server.
            ("2026-01-01 00:00:00+00", midnight),
            ("2026-01-01 00:00:00+00:00", midnight),
            ("2026-01-01 00:00:00+0000", midnight),
            ("2026-01-01 00:00:00Z", midnight),
            ("2026-01-01T00:00:00Z", midnight),
            ("2026-01-01T00:00:00+00:00", midnight),
            // A non-UTC offset names the same instant.
            ("2025-12-31 22:00:00-02", midnight),
            ("2026-01-01 02:30:00+02:30", midnight),
            ("2026-01-01 00:00:00.5+00", midnight_half),
            ("2026-01-01T00:00:00.5Z", midnight_half),
            ("2026-01-01 00:00:00", naive_midnight),
            ("2026-01-01T00:00:00", naive_midnight),
            ("2026-01-01 00:00:00.5", naive_half),
            ("2026-01-01T00:00:00.5", naive_half),
            ("2026-01-01", Temporal::Date(date(2026, 1, 1))),
            ("00:00:00", Temporal::Time(time(0, 0, 0, 0))),
            ("12:34:56.789", Temporal::Time(time(12, 34, 56, 789))),
        ]
    }

    /// Text no temporal parser accepts for the named kind. A timestamptz
    /// without an offset and a naive timestamp carrying one are the two
    /// that matter: each is the other kind's spelling.
    pub fn refused() -> Vec<(&'static str, BuiltinKind)> {
        vec![
            ("2026-01-01 00:00:00", ScalarKind::TimestampTz),
            ("2026-01-01", ScalarKind::TimestampTz),
            ("nope", ScalarKind::TimestampTz),
            ("", ScalarKind::TimestampTz),
            ("2026-01-01 00:00:00+00", ScalarKind::Timestamp),
            ("2026-01-01", ScalarKind::Timestamp),
            ("nope", ScalarKind::Timestamp),
            ("20260101", ScalarKind::Date),
            ("nope", ScalarKind::Date),
            ("12:34", ScalarKind::Time),
            ("nope", ScalarKind::Time),
        ]
    }
}
