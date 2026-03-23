/// Encode a variable-length integer (PVA size encoding).
///
/// Values 0..253 are stored as a single byte. Values >= 254 are stored as
/// 0xFE followed by a 4-byte u32.
pub fn encode_size(size: usize, is_be: bool) -> Vec<u8> {
    if size == 0 {
        return vec![0x00];
    }
    if size < 254 {
        return vec![size as u8];
    }
    let mut out = vec![0xFE];
    let bytes = if is_be {
        (size as u32).to_be_bytes()
    } else {
        (size as u32).to_le_bytes()
    };
    out.extend_from_slice(&bytes);
    out
}

/// Encode a string as a PVA size-prefixed byte sequence.
pub fn encode_string(value: &str, is_be: bool) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut out = encode_size(bytes.len(), is_be);
    out.extend_from_slice(bytes);
    out
}
