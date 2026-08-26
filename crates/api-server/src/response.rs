//! HTTP response handling with content negotiation
//!
//! Supports both JSON and Protobuf serialization based on Accept header

use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// API response wrapper that supports content negotiation
pub struct ApiResponse<T> {
    data: T,
    status: StatusCode,
}

impl<T> ApiResponse<T> {
    /// Create a new API response
    pub fn new(data: T) -> Self {
        Self {
            data,
            status: StatusCode::OK,
        }
    }

    /// Create a new API response with a specific status code
    pub fn with_status(data: T, status: StatusCode) -> Self {
        Self { data, status }
    }
}

impl<T> IntoResponse for ApiResponse<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        // For now, default to JSON
        // In full implementation, check Accept header and return protobuf if requested
        match serde_json::to_vec(&self.data) {
            Ok(body) => Response::builder()
                .status(self.status)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
            Err(e) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to serialize response: {}", e)))
                .unwrap(),
        }
    }
}

/// Negotiate content type based on Accept header
pub fn negotiate_content_type(headers: &HeaderMap) -> ContentType {
    if let Some(accept) = headers.get(header::ACCEPT) {
        if let Ok(accept_str) = accept.to_str() {
            if accept_str.contains("application/vnd.kubernetes.protobuf") {
                return ContentType::Protobuf;
            }
        }
    }
    ContentType::Json
}

/// Content type for responses
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Json,
    Protobuf,
}

impl ContentType {
    /// Get the MIME type string
    pub fn mime_type(&self) -> &'static str {
        match self {
            ContentType::Json => "application/json",
            ContentType::Protobuf => "application/vnd.kubernetes.protobuf",
        }
    }
}

/// Create a response with content negotiation
/// Note: Protobuf encoding requires api_version and kind, so this is a simplified version
pub fn create_response<T>(data: T, status: StatusCode, _content_type: ContentType) -> Response
where
    T: Serialize,
{
    // For now, always use JSON since protobuf encoding needs type metadata
    // In full implementation, this would check content_type and encode appropriately
    match serde_json::to_vec(&data) {
        Ok(body) => Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, ContentType::Json.mime_type())
            .body(Body::from(body))
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Failed to serialize: {}", e)))
            .unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_content_type_negotiation() {
        let mut headers = HeaderMap::new();
        assert_eq!(negotiate_content_type(&headers), ContentType::Json);

        headers.insert(header::ACCEPT, "application/json".parse().unwrap());
        assert_eq!(negotiate_content_type(&headers), ContentType::Json);

        headers.insert(
            header::ACCEPT,
            "application/vnd.kubernetes.protobuf".parse().unwrap(),
        );
        assert_eq!(negotiate_content_type(&headers), ContentType::Protobuf);
    }

    #[test]
    fn test_content_type_mime_types() {
        assert_eq!(ContentType::Json.mime_type(), "application/json");
        assert_eq!(
            ContentType::Protobuf.mime_type(),
            "application/vnd.kubernetes.protobuf"
        );
    }

    #[test]
    fn test_api_response_creation() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        let response = ApiResponse::new(data.clone());
        assert_eq!(response.status, StatusCode::OK);

        let response = ApiResponse::with_status(data, StatusCode::CREATED);
        assert_eq!(response.status, StatusCode::CREATED);
    }
}
