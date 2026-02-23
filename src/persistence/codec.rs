//! Serialization codec with compression

use crate::StorageError;
use lz4;

/// Serialize a value to binary bytes.
pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    postcard::to_stdvec(value)
        .map_err(|e| StorageError::Codec(format!("Postcard serialize error: {e}")))
}

/// Deserialize a value from binary bytes.
pub fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    postcard::from_bytes(bytes)
        .map_err(|e| StorageError::Codec(format!("Postcard deserialize error: {e}")))
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

/// Decompress and deserialize data
///
/// Decompresses LZ4, then deserializes with postcard.
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    let decompressed = decompress_internal(bytes, None)?;
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
}
