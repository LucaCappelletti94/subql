//! Strict `CBOR` encoding for portable replay state.

use alloc::string::String;
use alloc::vec::Vec;

use ciborium::de;
use ciborium::ser;

/// A state encoding the codec cannot write or read.
#[derive(Debug, thiserror::Error)]
pub enum StateCodecError {
    /// A value the serializer refused.
    #[error("state encoding refused a value: {0}")]
    Encode(String),
    /// Bytes that do not hold one complete, well-formed value.
    #[error("state decoding refused the bytes: {0}")]
    Decode(String),
    /// One complete value with bytes left behind after it.
    #[error("state bytes trail the decoded value: {0}")]
    Trailing(usize),
    /// An encoding that will not fit its byte budget.
    #[error("state encoding exceeds the budget of {0} bytes")]
    OverBudget(usize),
}

struct StateWriter<'a> {
    buffer: Option<&'a mut Vec<u8>>,
    written: &'a mut usize,
    budget: usize,
}

impl ciborium_io::Write for StateWriter<'_> {
    type Error = StateCodecError;

    fn write_all(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        let total = self.written.saturating_add(data.len());
        if total > self.budget {
            return Err(StateCodecError::OverBudget(self.budget));
        }
        if let Some(buffer) = &mut self.buffer {
            buffer.extend_from_slice(data);
        }
        *self.written = total;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn serialize_into<T: serde::Serialize + ?Sized>(
    value: &T,
    writer: StateWriter<'_>,
) -> Result<(), StateCodecError> {
    ser::into_writer(value, writer).map_err(|error| match error {
        ser::Error::Io(error) => error,
        ser::Error::Value(message) => StateCodecError::Encode(message),
    })
}

/// Encode one value as a complete self-describing state.
///
/// # Errors
/// A value the serializer cannot write.
pub fn encode<T: serde::Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, StateCodecError> {
    encode_limited(value, usize::MAX)
}

/// Encode without writing beyond the byte budget.
///
/// # Errors
/// The encoding crosses `budget` or the serializer refuses a value.
pub fn encode_limited<T: serde::Serialize + ?Sized>(
    value: &T,
    budget: usize,
) -> Result<Vec<u8>, StateCodecError> {
    let mut buffer = Vec::new();
    let mut written = 0;
    serialize_into(
        value,
        StateWriter {
            buffer: Some(&mut buffer),
            written: &mut written,
            budget,
        },
    )?;
    Ok(buffer)
}

/// Count the encoded size of one value without keeping its bytes.
///
/// # Errors
/// The budget is crossed, or a value the serializer cannot write.
pub fn encoded_size<T: serde::Serialize + ?Sized>(
    value: &T,
    budget: usize,
) -> Result<usize, StateCodecError> {
    let mut written = 0;
    serialize_into(
        value,
        StateWriter {
            buffer: None,
            written: &mut written,
            budget,
        },
    )?;
    Ok(written)
}

struct SliceReader<'a, 'b> {
    remaining: &'a mut &'b [u8],
}

impl ciborium_io::Read for SliceReader<'_, '_> {
    type Error = StateCodecError;

    fn read_exact(&mut self, data: &mut [u8]) -> Result<(), Self::Error> {
        let Some((prefix, suffix)) = self.remaining.split_at_checked(data.len()) else {
            return Err(StateCodecError::Decode(
                "the state ends before its value completes".into(),
            ));
        };
        data.copy_from_slice(prefix);
        *self.remaining = suffix;
        Ok(())
    }
}

/// Decode one complete self-describing state, refusing trailing bytes.
///
/// # Errors
/// Bytes that hold no complete value, or a complete value with bytes after it.
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StateCodecError> {
    let mut remaining = bytes;
    let value = de::from_reader(SliceReader {
        remaining: &mut remaining,
    })
    .map_err(|error| match error {
        de::Error::Io(error) => error,
        de::Error::Syntax(offset) => {
            StateCodecError::Decode(format!("the state is malformed at byte {offset}"))
        }
        de::Error::Semantic(_, message) => StateCodecError::Decode(message),
        de::Error::RecursionLimitExceeded => {
            StateCodecError::Decode("the state nests beyond the codec's recursion limit".into())
        }
    })?;
    if !remaining.is_empty() {
        return Err(StateCodecError::Trailing(remaining.len()));
    }
    Ok(value)
}
