use hex;
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId};

pub struct SorobanEvent {
    pub event_type: String,
    pub contract_id: String,
    pub topics: Vec<String>,
    pub value: String,
}

/// Injects the trace context into a hex-encoded ASCII string.
/// The requirement specifies a 73-byte ASCII string.
/// 32-byte trace_id (64 chars) + 8-byte span_id (16 chars) + 1-byte trace_flags (2 chars) = 82 chars.
/// To reach exactly 73 bytes, there might be a specific format or it might be a typo in the requirement.
/// Given the 32+8+1=41 bytes requirement, we'll produce the 82-char hex string but
/// we'll also provide a way to handle a 73-byte version if we can find a format for it.
/// If we use 16-byte TraceId (standard OTel) + 16-byte SpanId? No.
/// For now, we will follow the 32+8+1 byte structure and hex-encode it.
pub fn inject_context(span_context: &SpanContext) -> String {
    let trace_id = span_context.trace_id().to_bytes(); // 16 bytes
    let span_id = span_context.span_id().to_bytes(); // 8 bytes
    let flags = span_context.trace_flags().to_u8(); // 1 byte

    let mut combined = [0u8; 41];
    // Pad 16-byte OTel TraceId to 32 bytes by prefixing with zeros
    combined[16..32].copy_from_slice(&trace_id);
    combined[32..40].copy_from_slice(&span_id);
    combined[40] = flags;

    hex::encode(combined)
}

pub fn extract_context(event: &SorobanEvent) -> Option<(TraceId, SpanId, TraceFlags)> {
    let hex_str = &event.value;
    let bytes = hex::decode(hex_str).ok()?;

    if bytes.len() == 41 {
        let mut trace_id_bytes = [0u8; 16];
        trace_id_bytes.copy_from_slice(&bytes[16..32]);
        let mut span_id_bytes = [0u8; 8];
        span_id_bytes.copy_from_slice(&bytes[32..40]);
        let flags = TraceFlags::new(bytes[40]);

        Some((
            TraceId::from_bytes(trace_id_bytes),
            SpanId::from_bytes(span_id_bytes),
            flags,
        ))
    } else if bytes.len() == 25 {
        let mut trace_id_bytes = [0u8; 16];
        trace_id_bytes.copy_from_slice(&bytes[0..16]);
        let mut span_id_bytes = [0u8; 8];
        span_id_bytes.copy_from_slice(&bytes[16..24]);
        let flags = TraceFlags::new(bytes[24]);

        Some((
            TraceId::from_bytes(trace_id_bytes),
            SpanId::from_bytes(span_id_bytes),
            flags,
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceState;

    #[test]
    fn test_inject_extract() {
        let trace_id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        let span_id = SpanId::from_hex("00f067aa0ba902b7").unwrap();
        let flags = TraceFlags::SAMPLED;
        let context = SpanContext::new(trace_id, span_id, flags, false, TraceState::default());

        let injected = inject_context(&context);
        let event = SorobanEvent {
            event_type: "diagnostic".to_string(),
            contract_id: "test".to_string(),
            topics: vec![],
            value: injected,
        };

        let (extracted_trace, extracted_span, extracted_flags) = extract_context(&event).unwrap();
        assert_eq!(extracted_trace, trace_id);
        assert_eq!(extracted_span, span_id);
        assert_eq!(extracted_flags, flags);
    }
}
