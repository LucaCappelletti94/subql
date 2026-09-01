//! The re-read answers' own file.
//!
//! A shard belongs to one table and every subscription in it points at a
//! compiled predicate. An answer that needs a database read has no predicate,
//! and one of them can be woken by several tables, so it is not a property of
//! one table and is not stored as if it were.
//!
//! Layout: a 15-byte header (magic, version, entry count, payload length)
//! followed by the encoded payload. Uncompressed on purpose: this file holds
//! one small record per re-read answer, so compression would buy nothing and
//! cost a dependency on the shard machinery's framing.
//!
//! Each entry carries the fingerprints of the tables it reads, so loading
//! judges every answer on its own tables rather than refusing the whole file
//! because one unrelated table changed.

use alloc::string::String;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

use crate::backend::Backend;
use crate::persistence::shard::ShardFingerprintEnvelope;
use crate::{IdTypes, ReadTier, StorageError, SubscriptionId, TableId};

/// Magic bytes for the re-read file, distinct from a shard's.
const MAGIC: [u8; 5] = *b"SUBQR";

const VERSION: u16 = 4;

/// Header length in bytes.
const HEADER_LEN: usize = 15;

/// Refuse a payload claiming more than this, so a tampered length cannot make
/// the loader allocate wildly. One entry is well under a kilobyte.
const MAX_PAYLOAD_LEN: u64 = 64 * 1024 * 1024;

mod bound_query_wire {
    use super::{Backend, String, Vec};
    use crate::backend::Value;
    use serde::ser::{Error as _, SerializeSeq};
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize)]
    #[serde(bound = "")]
    enum Bind<B: Backend> {
        Missing,
        Null,
        Bool(B::Bool),
        Int(B::Int),
        Float(B::Float),
        String(B::String),
        Bytes(B::Bytes),
        Uuid(B::Uuid),
        Timestamp(B::Timestamp),
        TimestampTz(B::TimestampTz),
        Date(B::Date),
        Time(B::Time),
        Decimal(Vec<u8>),
        Json(Vec<u8>),
        Jsonb(Vec<u8>),
        Custom(<B::Custom as crate::backend::CustomScalars>::Value),
    }

    impl<B: Backend> Bind<B> {
        fn into_value(self) -> Result<Value<B>, serde_json::Error> {
            Ok(match self {
                Self::Missing => Value::Missing,
                Self::Null => Value::Null,
                Self::Bool(value) => Value::Bool(value),
                Self::Int(value) => Value::Int(value),
                Self::Float(value) => Value::Float(value),
                Self::String(value) => Value::String(value),
                Self::Bytes(value) => Value::Bytes(value),
                Self::Uuid(value) => Value::Uuid(value),
                Self::Timestamp(value) => Value::Timestamp(value),
                Self::TimestampTz(value) => Value::TimestampTz(value),
                Self::Date(value) => Value::Date(value),
                Self::Time(value) => Value::Time(value),
                Self::Decimal(bytes) => Value::Decimal(serde_json::from_slice(&bytes)?),
                Self::Json(bytes) => Value::Json(serde_json::from_slice(&bytes)?),
                Self::Jsonb(bytes) => Value::Jsonb(serde_json::from_slice(&bytes)?),
                Self::Custom(value) => Value::Custom(value),
            })
        }
    }

    struct BindRef<'a, B: Backend>(&'a Value<B>);

    impl<B: Backend> Serialize for BindRef<'_, B> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            match self.0 {
                Value::Missing => serializer.serialize_unit_variant("Bind", 0, "Missing"),
                Value::Null => serializer.serialize_unit_variant("Bind", 1, "Null"),
                Value::Bool(value) => {
                    serializer.serialize_newtype_variant("Bind", 2, "Bool", value)
                }
                Value::Int(value) => serializer.serialize_newtype_variant("Bind", 3, "Int", value),
                Value::Float(value) => {
                    serializer.serialize_newtype_variant("Bind", 4, "Float", value)
                }
                Value::String(value) => {
                    serializer.serialize_newtype_variant("Bind", 5, "String", value)
                }
                Value::Bytes(value) => {
                    serializer.serialize_newtype_variant("Bind", 6, "Bytes", value)
                }
                Value::Uuid(value) => {
                    serializer.serialize_newtype_variant("Bind", 7, "Uuid", value)
                }
                Value::Timestamp(value) => {
                    serializer.serialize_newtype_variant("Bind", 8, "Timestamp", value)
                }
                Value::TimestampTz(value) => {
                    serializer.serialize_newtype_variant("Bind", 9, "TimestampTz", value)
                }
                Value::Date(value) => {
                    serializer.serialize_newtype_variant("Bind", 10, "Date", value)
                }
                Value::Time(value) => {
                    serializer.serialize_newtype_variant("Bind", 11, "Time", value)
                }
                Value::Decimal(value) => serializer.serialize_newtype_variant(
                    "Bind",
                    12,
                    "Decimal",
                    &serde_json::to_vec(value).map_err(S::Error::custom)?,
                ),
                Value::Json(value) => serializer.serialize_newtype_variant(
                    "Bind",
                    13,
                    "Json",
                    &serde_json::to_vec(value).map_err(S::Error::custom)?,
                ),
                Value::Jsonb(value) => serializer.serialize_newtype_variant(
                    "Bind",
                    14,
                    "Jsonb",
                    &serde_json::to_vec(value).map_err(S::Error::custom)?,
                ),
                Value::Custom(value) => {
                    serializer.serialize_newtype_variant("Bind", 15, "Custom", value)
                }
            }
        }
    }

    struct BindsRef<'a, B: Backend>(&'a [Value<B>]);

    impl<B: Backend> Serialize for BindsRef<'_, B> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
            for bind in self.0 {
                sequence.serialize_element(&BindRef(bind))?;
            }
            sequence.end()
        }
    }

    #[derive(Serialize)]
    #[serde(bound = "")]
    struct QueryRef<'a, B: Backend> {
        sql: &'a str,
        binds: BindsRef<'a, B>,
    }

    #[derive(Deserialize)]
    #[serde(bound = "")]
    struct Query<B: Backend> {
        sql: String,
        binds: Vec<Bind<B>>,
    }

    pub fn serialize<B: Backend, S: serde::Serializer>(
        query: &crate::reexec::BoundQuery<B>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        QueryRef {
            sql: query.sql(),
            binds: BindsRef(query.binds()),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, B: Backend, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<crate::reexec::BoundQuery<B>, D::Error> {
        let query = Query::<B>::deserialize(deserializer)?;
        let binds = query
            .binds
            .into_iter()
            .map(Bind::into_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::de::Error::custom)?;
        Ok(crate::reexec::BoundQuery::new(query.sql, binds))
    }
}

/// One stored re-read answer.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ReadEntry<I: IdTypes, B: Backend> {
    /// Identity it had, and keeps: a caller holds this id.
    pub subscription_id: SubscriptionId,
    /// Consumer that registered it.
    pub consumer_id: I::ConsumerId,
    /// Durable, or bound to a session.
    pub scope: crate::SubscriptionScope<I>,
    /// Original query used to rebuild the plan.
    #[serde(with = "bound_query_wire")]
    pub source_query: crate::reexec::BoundQuery<B>,
    /// Tables it reads, each with the fingerprint it was saved under.
    pub tables: Vec<(TableId, ShardFingerprintEnvelope)>,
    /// The tier it had when saved.
    pub tier: ReadTier,
    /// Whether each database read runs under the individual consumer's
    /// database identity.
    pub database_reads_per_consumer: bool,
}

impl<I: IdTypes, B: Backend> Clone for ReadEntry<I, B> {
    fn clone(&self) -> Self {
        Self {
            subscription_id: self.subscription_id,
            consumer_id: self.consumer_id,
            scope: self.scope,
            source_query: self.source_query.clone(),
            tables: self.tables.clone(),
            tier: self.tier,
            database_reads_per_consumer: self.database_reads_per_consumer,
        }
    }
}

/// Everything the file holds.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ReadsPayload<I: IdTypes, B: Backend> {
    /// One per re-read answer.
    pub entries: Vec<ReadEntry<I, B>>,
    /// When the file was written, milliseconds since the Unix epoch.
    pub created_at_unix_ms: u64,
}

/// Encode `payload` into the file's bytes.
pub fn serialize<I: IdTypes, B: Backend>(
    payload: &ReadsPayload<I, B>,
) -> Result<Vec<u8>, StorageError> {
    let body = crate::persistence::codec::serialize(payload)
        .map_err(|e| StorageError::Codec(alloc::format!("reads serialize failed: {e}")))?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len());
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    #[allow(clippy::cast_possible_truncation)]
    bytes.extend_from_slice(&(payload.entries.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(body.len() as u64).to_le_bytes()[..4]);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// Decode a file written by [`serialize`].
///
/// Refuses a wrong magic, a version this build does not write, a truncated
/// body, and a length past this module's own ceiling.
pub fn deserialize<I: IdTypes, B: Backend>(
    bytes: &[u8],
) -> Result<ReadsPayload<I, B>, StorageError> {
    if bytes.len() < HEADER_LEN {
        return Err(StorageError::Corrupt(String::from(
            "reads file shorter than its header",
        )));
    }
    if bytes[..5] != MAGIC {
        return Err(StorageError::Corrupt(String::from(
            "reads file has the wrong magic",
        )));
    }
    let version = u16::from_be_bytes([bytes[5], bytes[6]]);
    if version != VERSION {
        return Err(StorageError::Corrupt(alloc::format!(
            "reads file version {version}, this build writes {VERSION}"
        )));
    }
    let body_len = u64::from(u32::from_le_bytes([
        bytes[11], bytes[12], bytes[13], bytes[14],
    ]));
    if body_len > MAX_PAYLOAD_LEN {
        return Err(StorageError::Corrupt(alloc::format!(
            "reads file claims {body_len} bytes of payload"
        )));
    }
    let body = bytes
        .get(HEADER_LEN..)
        .filter(|rest| rest.len() as u64 == body_len)
        .ok_or_else(|| {
            StorageError::Corrupt(String::from("reads file payload length does not match"))
        })?;
    crate::persistence::codec::deserialize(body)
        .map_err(|e| StorageError::Codec(alloc::format!("reads deserialize failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Postgres, Value};
    use crate::DefaultIds;
    use proptest::prelude::*;

    fn payload() -> ReadsPayload<DefaultIds, Postgres> {
        payload_with_binds(alloc::vec![Value::Float(5.0)])
    }

    fn payload_with_binds(binds: Vec<Value<Postgres>>) -> ReadsPayload<DefaultIds, Postgres> {
        ReadsPayload {
            entries: alloc::vec![ReadEntry::<DefaultIds, Postgres> {
                subscription_id: 7,
                consumer_id: 3,
                scope: crate::SubscriptionScope::Durable,
                source_query: crate::reexec::BoundQuery::new(
                    String::from("SELECT MIN(price) FROM orders"),
                    binds,
                ),
                tables: alloc::vec![(
                    1,
                    ShardFingerprintEnvelope {
                        algorithm_id: 1,
                        canonicalization_version: 1,
                        profile_id: 1,
                        digest128: [9u8; 16],
                    },
                )],
                tier: ReadTier::Scalar,
                database_reads_per_consumer: true,
            }],
            created_at_unix_ms: 42,
        }
    }

    fn payloads_equal(
        left: &ReadsPayload<DefaultIds, Postgres>,
        right: &ReadsPayload<DefaultIds, Postgres>,
    ) -> bool {
        left.created_at_unix_ms == right.created_at_unix_ms
            && left.entries.len() == right.entries.len()
            && left
                .entries
                .iter()
                .zip(&right.entries)
                .all(|(left, right)| {
                    left.subscription_id == right.subscription_id
                        && left.consumer_id == right.consumer_id
                        && left.scope == right.scope
                        && left.source_query == right.source_query
                        && left.tables == right.tables
                        && left.tier == right.tier
                        && left.database_reads_per_consumer == right.database_reads_per_consumer
                })
    }

    fn arb_date() -> BoxedStrategy<chrono::NaiveDate> {
        (1970i32..=2100, 1u32..=12, 1u32..=28)
            .prop_map(|(year, month, day)| {
                chrono::NaiveDate::from_ymd_opt(year, month, day).expect("valid date")
            })
            .boxed()
    }

    fn arb_time() -> BoxedStrategy<chrono::NaiveTime> {
        (0u32..24, 0u32..60, 0u32..60, 0u32..1_000_000_000)
            .prop_map(|(hour, minute, second, nanosecond)| {
                chrono::NaiveTime::from_hms_nano_opt(hour, minute, second, nanosecond)
                    .expect("valid time")
            })
            .boxed()
    }

    fn arb_timestamp() -> BoxedStrategy<chrono::NaiveDateTime> {
        (arb_date(), arb_time())
            .prop_map(|(date, time)| date.and_time(time))
            .boxed()
    }

    fn arb_json() -> BoxedStrategy<serde_json::Value> {
        let leaf = prop_oneof![
            Just(serde_json::Value::Null),
            any::<bool>().prop_map(serde_json::Value::Bool),
            (-1_000i64..=1_000).prop_map(|value| serde_json::Value::Number(value.into())),
            "[a-zA-Z0-9 _-]{0,16}".prop_map(serde_json::Value::String),
        ];

        leaf.prop_recursive(3, 32, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..=4).prop_map(serde_json::Value::Array),
                proptest::collection::btree_map("[a-z]{1,6}", inner, 0..=4)
                    .prop_map(|entries| serde_json::Value::Object(entries.into_iter().collect())),
            ]
        })
        .boxed()
    }

    fn arb_bind() -> BoxedStrategy<Value<Postgres>> {
        prop_oneof![
            Just(Value::Missing),
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            (-10_000i32..=10_000).prop_map(|value| Value::Float(f64::from(value) / 10.0)),
            "[a-zA-Z0-9 _-]{0,16}".prop_map(Value::String),
            proptest::collection::vec(any::<u8>(), 0..=16).prop_map(Value::Bytes),
            any::<[u8; 16]>().prop_map(|bytes| Value::Uuid(uuid::Uuid::from_bytes(bytes))),
            arb_timestamp().prop_map(Value::Timestamp),
            arb_timestamp().prop_map(|value| Value::TimestampTz(value.and_utc())),
            arb_date().prop_map(Value::Date),
            arb_time().prop_map(Value::Time),
            any::<i64>().prop_map(|value| Value::Decimal(bigdecimal::BigDecimal::from(value))),
            arb_json().prop_map(Value::Json),
            arb_json().prop_map(Value::Jsonb),
        ]
        .boxed()
    }

    fn every_builtin_bind() -> Vec<Value<Postgres>> {
        let timestamp = chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
            .expect("valid date")
            .and_hms_micro_opt(3, 4, 5, 678_901)
            .expect("valid timestamp");
        alloc::vec![
            Value::Missing,
            Value::Null,
            Value::Bool(true),
            Value::Int(-42),
            Value::Float(-12.5),
            Value::String(String::from("fixed string")),
            Value::Bytes(alloc::vec![0, 1, 127, 255]),
            Value::Uuid(
                uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid UUID"),
            ),
            Value::Timestamp(timestamp),
            Value::TimestampTz(timestamp.and_utc()),
            Value::Date(timestamp.date()),
            Value::Time(timestamp.time()),
            Value::Decimal(
                "1234.5678"
                    .parse::<bigdecimal::BigDecimal>()
                    .expect("valid decimal"),
            ),
            Value::Json(serde_json::json!({"bind": [1, true, null]})),
            Value::Jsonb(serde_json::json!({"bindb": {"nested": "value"}})),
        ]
    }

    #[test]
    fn every_builtin_bind_round_trips() {
        let original = payload_with_binds(every_builtin_bind());
        let encoded = serialize(&original).expect("serialize");
        let decoded: ReadsPayload<DefaultIds, Postgres> =
            deserialize(&encoded).expect("deserialize");

        assert!(payloads_equal(&decoded, &original), "{decoded:?}");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn reads_payload_round_trips_arbitrary_bind_vectors(
            binds in proptest::collection::vec(arb_bind(), 0..=8),
        ) {
            let original = payload_with_binds(binds);
            let encoded = serialize(&original).expect("serialize");
            let decoded: ReadsPayload<DefaultIds, Postgres> =
                deserialize(&encoded).expect("deserialize");

            prop_assert!(payloads_equal(&decoded, &original), "{decoded:?}");
        }
    }

    #[test]
    fn a_written_file_reads_back_the_same() {
        let bytes = serialize(&payload()).expect("serialize");
        let back: ReadsPayload<DefaultIds, Postgres> = deserialize(&bytes).expect("deserialize");
        assert_eq!(back.entries.len(), 1);
        assert_eq!(back.entries[0].subscription_id, 7);
        assert_eq!(back.entries[0].tier, ReadTier::Scalar);
        assert_eq!(
            back.entries[0].source_query.sql(),
            "SELECT MIN(price) FROM orders"
        );
        assert_eq!(back.entries[0].source_query.binds(), &[Value::Float(5.0)]);
        assert_eq!(back.created_at_unix_ms, 42);
    }

    #[test]
    fn a_file_from_another_format_is_refused() {
        let mut bytes = serialize(&payload()).expect("serialize");
        bytes[5..7].copy_from_slice(&99u16.to_be_bytes());
        let refused = deserialize::<DefaultIds, Postgres>(&bytes).expect_err("version is checked");
        assert!(
            alloc::format!("{refused}").contains("version 99"),
            "the refusal names the version it found, got {refused}"
        );
    }

    #[test]
    fn a_truncated_file_is_refused() {
        let bytes = serialize(&payload()).expect("serialize");
        let refused = deserialize::<DefaultIds, Postgres>(&bytes[..bytes.len() - 3])
            .expect_err("length is checked");
        assert!(alloc::format!("{refused}").contains("length"), "{refused}");
    }
}
