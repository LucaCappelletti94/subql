use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

/// Escape a string for embedding as a single-quoted SQL literal.
pub(super) fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Decode lowercase ASCII hex bytes into raw bytes. Rejects odd-length
/// or non-hex inputs with a structured error message.
pub(super) fn hex_decode(bytes: &[u8]) -> Result<Vec<u8>, String> {
    if !bytes.len().is_multiple_of(2) {
        return Err(format!(
            "encode(data, 'hex') returned odd-length output ({} chars)",
            bytes.len()
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for i in 0..bytes.len() / 2 {
        let hi = hex_nibble(bytes[2 * i])?;
        let lo = hex_nibble(bytes[2 * i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        other => Err(format!("invalid hex digit 0x{other:02X}")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{hex_decode, sql_string_literal};
    use alloc::vec::Vec;

    #[test]
    fn hex_decode_round_trip() {
        let bytes = [0x00, 0xFF, 0x42, 0xc0];
        let mut hex = Vec::with_capacity(bytes.len() * 2);
        for b in bytes {
            hex.push(b"0123456789abcdef"[usize::from(b >> 4)]);
            hex.push(b"0123456789abcdef"[usize::from(b & 0x0F)]);
        }
        assert_eq!(hex_decode(&hex).unwrap(), bytes);
    }

    #[test]
    fn hex_decode_rejects_odd_length() {
        assert!(hex_decode(b"abc").is_err());
    }

    #[test]
    fn hex_decode_rejects_non_hex() {
        assert!(hex_decode(b"zz").is_err());
    }

    #[test]
    fn sql_string_literal_escapes_quotes() {
        assert_eq!(sql_string_literal("foo'bar"), "'foo''bar'");
        assert_eq!(sql_string_literal("ok_slot"), "'ok_slot'");
    }
}
