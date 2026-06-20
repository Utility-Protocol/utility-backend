use std::str;

/// A zero-copy view into a raw envelope buffer.
///
/// Lifetime `'a` is tied to the input buffer — no heap allocations are made during parsing.
pub struct CompressedEnvelope<'a> {
    pub meter_id: &'a str,
    pub payload: &'a [u8],
    pub checksum: [u8; 32],
}

/// Parses a compressed envelope from `data` with zero heap allocations.
///
/// Wire format: `[u16 BE meter_id_len][meter_id UTF-8 bytes][payload bytes][32-byte checksum]`
pub fn parse_envelope(data: &[u8]) -> Result<CompressedEnvelope<'_>, &'static str> {
    if data.len() < 40 {
        return Err("envelope too short");
    }
    let meter_id_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    if data.len() < 2 + meter_id_len + 32 {
        return Err("malformed envelope: meter_id truncated");
    }
    let meter_id = str::from_utf8(&data[2..2 + meter_id_len])
        .map_err(|_| "invalid utf-8 meter_id")?;
    let payload_start = 2 + meter_id_len;
    let payload_end = data.len() - 32;
    let payload = &data[payload_start..payload_end];
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&data[payload_end..]);
    Ok(CompressedEnvelope {
        meter_id,
        payload,
        checksum,
    })
}
