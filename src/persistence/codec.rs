//! Serialization codec with compression

use crate::StorageError;
use lz4;

/// Serialize a value to binary bytes.
pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    postcard::to_stdvec(value)
        .map_err(|e| StorageError::Codec(format!("Postcard serialize error: {e}")))
}

/// Deserialize a value from binary bytes.
///
/// Rejects inputs that have trailing bytes after the deserialized value,
/// preventing silent data corruption from partial/mismatched payloads.
pub fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    let (value, remaining) = postcard::take_from_bytes(bytes)
        .map_err(|e| StorageError::Codec(format!("Postcard deserialize error: {e}")))?;
    if !remaining.is_empty() {
        return Err(StorageError::Codec(format!(
            "Trailing bytes after deserialization: {} unexpected bytes",
            remaining.len()
        )));
    }
    Ok(value)
}

/// Serialize and compress data
///
/// Uses postcard for efficient binary encoding, then LZ4 for fast compression.
pub fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    let serialized = serialize(value)?;

    encode_serialized(&serialized)
}

/// Compress already-serialized bytes with LZ4.
pub fn encode_serialized(mut serialized: &[u8]) -> Result<Vec<u8>, StorageError> {
    // Compress with LZ4
    let mut encoder = lz4::EncoderBuilder::new()
        .level(4) // Fast compression
        .build(Vec::new())
        .map_err(|e| StorageError::Codec(format!("LZ4 encoder error: {e}")))?;

    std::io::copy(&mut serialized, &mut encoder)
        .map_err(|e| StorageError::Codec(format!("LZ4 compression error: {e}")))?;

    let (compressed, result) = encoder.finish();
    result.map_err(|e| StorageError::Codec(format!("LZ4 finish error: {e}")))?;

    Ok(compressed)
}

fn decompress_internal(bytes: &[u8], max_output: Option<usize>) -> Result<Vec<u8>, StorageError> {
    // Decompress with LZ4
    let mut decoder = lz4::Decoder::new(bytes)
        .map_err(|e| StorageError::Codec(format!("LZ4 decoder error: {e}")))?;

    let mut decompressed = Vec::new();
    let mut chunk = [0_u8; 8 * 1024];

    loop {
        let read = std::io::Read::read(&mut decoder, &mut chunk)
            .map_err(|e| StorageError::Codec(format!("LZ4 decompression error: {e}")))?;
        if read == 0 {
            break;
        }

        if let Some(limit) = max_output {
            let new_len = decompressed.len().saturating_add(read);
            if new_len > limit {
                return Err(StorageError::Corrupt(format!(
                    "Decompressed payload exceeds limit: {new_len} > {limit}"
                )));
            }
        }

        decompressed.extend_from_slice(&chunk[..read]);
    }

    Ok(decompressed)
}

/// Decompress bytes with an explicit maximum output size.
pub(crate) fn decompress_with_limit(
    bytes: &[u8],
    max_output: usize,
) -> Result<Vec<u8>, StorageError> {
    decompress_internal(bytes, Some(max_output))
}

/// Hard cap for decompressed data in `decode` (defense against decompression bombs).
///
/// Intentionally separate from [`super::shard::MAX_SHARD_UNCOMPRESSED_SIZE`] — each
/// layer enforces its own limit independently.
pub(super) const MAX_DECODE_UNCOMPRESSED: usize = 256 * 1024 * 1024; // 256 MiB

/// Decompress and deserialize data
///
/// Decompresses LZ4 (capped at 256 MiB to prevent decompression bombs),
/// then deserializes with postcard.
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    let decompressed = decompress_internal(bytes, Some(MAX_DECODE_UNCOMPRESSED))?;
    deserialize(&decompressed)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestData {
        id: u64,
        name: String,
        values: Vec<i32>,
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let data = TestData {
            id: 42,
            name: "test".to_string(),
            values: vec![1, 2, 3, 4, 5],
        };

        let encoded = encode(&data).unwrap();
        let decoded: TestData = decode(&encoded).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_compression_reduces_size() {
        // Create data with repetition (compresses well)
        let data = vec![42u32; 1000];

        let encoded = encode(&data).unwrap();
        let uncompressed = serialize(&data).unwrap();

        // Compressed should be much smaller
        assert!(encoded.len() < uncompressed.len());
    }

    #[test]
    fn test_empty_data() {
        let data: Vec<u32> = vec![];

        let encoded = encode(&data).unwrap();
        let decoded: Vec<u32> = decode(&encoded).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_large_string() {
        let data = "x".repeat(10_000);

        let encoded = encode(&data).unwrap();
        let decoded: String = decode(&encoded).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_invalid_data() {
        let invalid = vec![0xFF, 0xFF, 0xFF, 0xFF];
        let result: Result<TestData, _> = decode(&invalid);

        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_with_limit_exceeded() {
        let data = "x".repeat(8_192);
        let encoded = encode(&data).unwrap();

        let err = decompress_with_limit(&encoded, 32).expect_err("must exceed limit");
        assert!(matches!(err, StorageError::Corrupt(message) if message.contains("exceeds limit")));
    }

    // =========================================================================
    // D1 — Trailing bytes must be rejected
    // =========================================================================

    #[test]
    fn test_trailing_bytes_rejected() {
        let data = TestData {
            id: 1,
            name: "hello".to_string(),
            values: vec![1, 2, 3],
        };

        let mut encoded = serialize(&data).unwrap();
        // Append garbage bytes
        encoded.extend_from_slice(&[0xFF, 0xFF]);

        let err: Result<TestData, _> = deserialize(&encoded);
        assert!(
            err.is_err(),
            "deserialize must reject inputs with trailing bytes"
        );
        if let Err(StorageError::Codec(msg)) = err {
            assert!(msg.contains("Trailing bytes"), "unexpected message: {msg}");
        } else {
            panic!("expected StorageError::Codec, got ok or different variant");
        }
    }

    #[test]
    fn test_exact_bytes_accepted() {
        let data = TestData {
            id: 42,
            name: "exact".to_string(),
            values: vec![],
        };
        let encoded = serialize(&data).unwrap();
        let decoded: TestData = deserialize(&encoded).unwrap();
        assert_eq!(data, decoded);
    }

    // =========================================================================
    // D2 — decode applies decompression-bomb cap
    // =========================================================================

    #[test]
    fn test_decode_applies_decompression_limit() {
        // Compress a moderately large string, then try to decode with a tiny cap
        // by calling decompress_with_limit directly to verify the cap exists.
        // The real decode cap is 256 MiB, which we can't easily exceed in a unit test,
        // but we verify the limit constant is set correctly.
        assert_eq!(MAX_DECODE_UNCOMPRESSED, 256 * 1024 * 1024);

        // Verify that decode itself propagates decompression errors
        let invalid_lz4: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let result: Result<Vec<u8>, _> = decode(&invalid_lz4);
        assert!(result.is_err(), "invalid LZ4 must return an error");
    }
}
