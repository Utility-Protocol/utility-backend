use std::collections::HashMap;
use std::future::Future;

use axum::http::{HeaderMap, HeaderValue};
use uuid::Uuid;

pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";
const MAX_CORRELATION_ID_LEN: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelationId(String);

impl CorrelationId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn parse(value: &str) -> Option<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_CORRELATION_ID_LEN {
            return None;
        }

        if trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        {
            Some(Self(trimmed.to_string()))
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn to_header_value(&self) -> HeaderValue {
        HeaderValue::from_str(self.as_str()).expect("correlation ids are sanitized")
    }
}

tokio::task_local! {
    static CURRENT_CORRELATION_ID: String;
}

pub async fn scope<F, T>(id: CorrelationId, future: F) -> T
where
    F: Future<Output = T>,
{
    CURRENT_CORRELATION_ID.scope(id.0, future).await
}

pub fn current_correlation_id() -> Option<String> {
    CURRENT_CORRELATION_ID.try_with(Clone::clone).ok()
}

pub fn correlation_id_from_headers(headers: &HeaderMap) -> CorrelationId {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(CorrelationId::parse)
        .unwrap_or_else(CorrelationId::generate)
}

pub fn insert_header(headers: &mut HeaderMap, id: &CorrelationId) {
    headers.insert(CORRELATION_ID_HEADER, id.to_header_value());
}

pub fn inject_current_into_message_headers(headers: &mut HashMap<String, String>) {
    if let Some(id) = current_correlation_id() {
        headers.insert(CORRELATION_ID_HEADER.to_string(), id);
    }
}

pub fn inject_into_message_headers(headers: &mut HashMap<String, String>, id: &CorrelationId) {
    headers.insert(CORRELATION_ID_HEADER.to_string(), id.as_str().to_string());
}

pub fn extract_from_message_headers(headers: &HashMap<String, String>) -> Option<CorrelationId> {
    headers
        .get(CORRELATION_ID_HEADER)
        .or_else(|| headers.get("X-Correlation-ID"))
        .and_then(|value| CorrelationId::parse(value))
}

pub fn propagate_reqwest(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match current_correlation_id() {
        Some(id) => builder.header(CORRELATION_ID_HEADER, id),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_safe_correlation_ids() {
        assert_eq!(
            CorrelationId::parse("abc-123_DEF.456").unwrap().as_str(),
            "abc-123_DEF.456"
        );
        assert!(CorrelationId::parse("").is_none());
        assert!(CorrelationId::parse("has spaces").is_none());
        assert!(CorrelationId::parse("secret\nnext").is_none());
        assert!(CorrelationId::parse(&"a".repeat(MAX_CORRELATION_ID_LEN + 1)).is_none());
    }

    #[test]
    fn extracts_existing_header_or_generates_new_id() {
        let mut headers = HeaderMap::new();
        headers.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("incoming-1"));
        assert_eq!(correlation_id_from_headers(&headers).as_str(), "incoming-1");

        headers.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("bad id"));
        let generated = correlation_id_from_headers(&headers);
        assert_ne!(generated.as_str(), "bad id");
        assert!(Uuid::parse_str(generated.as_str()).is_ok());
    }

    #[tokio::test]
    async fn task_scope_exposes_current_correlation_id() {
        let id = CorrelationId::parse("req-123").unwrap();
        let current = scope(id, async { current_correlation_id() }).await;
        assert_eq!(current.as_deref(), Some("req-123"));
        assert!(current_correlation_id().is_none());
    }

    #[tokio::test]
    async fn message_headers_roundtrip_current_id() {
        let id = CorrelationId::parse("event-456").unwrap();
        let mut headers = HashMap::new();
        scope(id, async {
            inject_current_into_message_headers(&mut headers);
        })
        .await;
        assert_eq!(
            extract_from_message_headers(&headers).unwrap().as_str(),
            "event-456"
        );
    }
}
