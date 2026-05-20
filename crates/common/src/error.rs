use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Resource already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid resource: {0}")]
    InvalidResource(String),

    /// Generic bad-request error mapped to HTTP 400 / reason=BadRequest.
    ///
    /// Upstream Kubernetes uses 400/BadRequest for strict-decode errors and
    /// other "client supplied a syntactically malformed request" cases. Use
    /// `InvalidResource` instead when the request parses cleanly but fails a
    /// field-level validator (that maps to 422/Invalid).
    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Authentication error: {0}")]
    Authentication(String),

    #[error("Authorization error: {0}")]
    Authorization(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Too many requests: {0}")]
    TooManyRequests(String),

    #[error("Gone: {0}")]
    Gone(String),

    #[error("Unsupported media type: {0}")]
    UnsupportedMediaType(String),

    #[error("Not acceptable: {0}")]
    NotAcceptable(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Returns the machine-readable reason string matching Kubernetes StatusReason values
    pub fn reason(&self) -> &str {
        match self {
            Error::NotFound(_) => "NotFound",
            Error::AlreadyExists(_) => "AlreadyExists",
            Error::InvalidResource(_) => "Invalid",
            Error::BadRequest(_) => "BadRequest",
            Error::Serialization(_) => "BadRequest",
            Error::Storage(_) => "InternalError",
            Error::Network(_) => "ServiceUnavailable",
            Error::Authentication(_) => "Unauthorized",
            Error::Authorization(_) => "Forbidden",
            Error::Forbidden(_) => "Forbidden",
            Error::Conflict(_) => "Conflict",
            Error::TooManyRequests(_) => "TooManyRequests",
            Error::Gone(_) => "Gone",
            Error::UnsupportedMediaType(_) => "UnsupportedMediaType",
            Error::NotAcceptable(_) => "NotAcceptable",
            Error::Internal(_) => "InternalError",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "axum-support")]
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        use axum::Json;

        // Extract resource name from error message for StatusDetails
        let (status, message, reason, details) = match self {
            Error::NotFound(msg) => {
                // Sanitize internal storage paths from error messages
                let clean_msg = if msg.starts_with("/registry/") {
                    // Convert /registry/resources/namespace/name to "resources \"name\" not found"
                    let parts: Vec<&str> =
                        msg.trim_start_matches("/registry/").split('/').collect();
                    match parts.len() {
                        3 => format!("{} \"{}\" not found", parts[0], parts[2]),
                        2 => format!("{} \"{}\" not found", parts[0], parts[1]),
                        _ => format!("resource not found: {}", parts.last().unwrap_or(&"")),
                    }
                } else {
                    msg.clone()
                };
                let details = extract_resource_details(&clean_msg);
                (StatusCode::NOT_FOUND, clean_msg, "NotFound", details)
            }
            Error::AlreadyExists(msg) => {
                let details = extract_resource_details(&msg);
                (StatusCode::CONFLICT, msg, "AlreadyExists", details)
            }
            Error::InvalidResource(msg) => {
                let details = extract_resource_details_for_invalid(&msg);
                (StatusCode::UNPROCESSABLE_ENTITY, msg, "Invalid", details)
            }
            // Mirrors upstream k8s apimachinery/pkg/api/errors/errors.go:
            // strict decoding errors and other syntactic / client-malformed
            // requests return HTTP 400 with reason=BadRequest (NOT 422/Invalid,
            // which is reserved for semantic field-validation errors).
            Error::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg, "BadRequest", None),
            Error::Authentication(msg) => (StatusCode::UNAUTHORIZED, msg, "Unauthorized", None),
            Error::Authorization(msg) => (StatusCode::FORBIDDEN, msg, "Forbidden", None),
            Error::Forbidden(msg) => (StatusCode::FORBIDDEN, msg, "Forbidden", None),
            Error::Conflict(msg) => {
                let details = extract_resource_details(&msg);
                (StatusCode::CONFLICT, msg, "Conflict", details)
            }
            Error::TooManyRequests(msg) => {
                (StatusCode::TOO_MANY_REQUESTS, msg, "TooManyRequests", None)
            }
            Error::Gone(msg) => (StatusCode::GONE, msg, "Gone", None),
            Error::Storage(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                msg,
                "InternalError",
                None,
            ),
            Error::Network(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                msg,
                "ServiceUnavailable",
                None,
            ),
            Error::Serialization(e) => (StatusCode::BAD_REQUEST, e.to_string(), "BadRequest", None),
            Error::UnsupportedMediaType(msg) => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                msg,
                "UnsupportedMediaType",
                None,
            ),
            Error::NotAcceptable(msg) => (StatusCode::NOT_ACCEPTABLE, msg, "NotAcceptable", None),
            Error::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                msg,
                "InternalError",
                None,
            ),
        };

        let status_obj = if let Some(details) = details {
            crate::types::Status::failure_with_details(&message, reason, status.as_u16(), details)
        } else {
            crate::types::Status::failure(&message, reason, status.as_u16())
        };

        (status, Json(status_obj)).into_response()
    }
}

/// Extract resource name from error messages and return StatusDetails.
#[cfg(feature = "axum-support")]
fn extract_resource_details(msg: &str) -> Option<crate::types::StatusDetails> {
    let name = if let Some(path) = msg.split(": ").last() {
        if path.starts_with("/registry/") {
            path.rsplit('/').next().unwrap_or(path).to_string()
        } else {
            path.to_string()
        }
    } else {
        return None;
    };

    if name.is_empty() {
        return None;
    }

    Some(crate::types::StatusDetails {
        name: Some(name),
        group: None,
        kind: None,
        uid: None,
        causes: None,
        retry_after_seconds: None,
    })
}

/// Extract resource details for Invalid errors, including causes.
#[cfg(feature = "axum-support")]
fn extract_resource_details_for_invalid(msg: &str) -> Option<crate::types::StatusDetails> {
    Some(crate::types::StatusDetails {
        name: None,
        group: None,
        kind: None,
        uid: None,
        causes: Some(vec![crate::types::StatusCause {
            reason: Some("FieldValueInvalid".to_string()),
            message: Some(msg.to_string()),
            field: Some("metadata.name".to_string()),
        }]),
        retry_after_seconds: None,
    })
}
