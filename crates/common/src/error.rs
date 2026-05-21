use thiserror::Error;

use crate::validation::field::ErrorList;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Resource already exists: {0}")]
    AlreadyExists(String),

    /// Legacy 422 Invalid error carrying only a flat string. Used by handler
    /// paths that have not yet been refactored to thread a structured
    /// `field::ErrorList`. The IntoResponse impl parses the upstream-style
    /// `"<field>: <ErrorType>: <detail>"` wording back into a single
    /// `StatusCause` so the response body still matches upstream shape.
    #[error("Invalid resource: {0}")]
    InvalidResource(String),

    /// Structured 422 Invalid error carrying a `field::ErrorList`. Each entry
    /// maps to one `Status.details.causes[]` entry with the upstream
    /// `FieldValueXxx` reason taxonomy — mirrors `NewInvalid` upstream.
    ///
    /// Validators should accumulate ALL violations into the list rather than
    /// short-circuiting on the first failure; the IntoResponse impl emits
    /// one cause per error in field-path order.
    #[error("Invalid resource: {causes}", causes = format_error_list(.0))]
    Invalid(ErrorList),

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

/// Render an `ErrorList` upstream-style: one error per line joined by `; `.
fn format_error_list(errs: &ErrorList) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

impl Error {
    /// Returns the machine-readable reason string matching Kubernetes StatusReason values
    pub fn reason(&self) -> &str {
        match self {
            Error::NotFound(_) => "NotFound",
            Error::AlreadyExists(_) => "AlreadyExists",
            Error::InvalidResource(_) => "Invalid",
            Error::Invalid(_) => "Invalid",
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
            Error::Invalid(errs) => {
                let msg = format_error_list(&errs);
                let details = build_invalid_details_from_errors(&errs);
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    msg,
                    "Invalid",
                    Some(details),
                )
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

/// Build `Status.details` from a structured `field::ErrorList`, mirroring
/// upstream `apimachinery/pkg/api/errors/errors.go::NewInvalid`. Each
/// `field::Error` becomes one `StatusCause` whose `reason` is the upstream
/// `FieldValueXxx` mapping of the underlying `ErrorType`, `field` is the
/// real field path, and `message` is the full upstream-style error line
/// (`<field>: <ErrorType>: <detail>`).
#[cfg(feature = "axum-support")]
fn build_invalid_details_from_errors(errs: &ErrorList) -> crate::types::StatusDetails {
    let causes: Vec<crate::types::StatusCause> = errs
        .iter()
        .map(|e| crate::types::StatusCause {
            reason: Some(e.error_type.cause_reason().to_string()),
            message: Some(e.to_string()),
            field: Some(e.field.clone()),
        })
        .collect();
    crate::types::StatusDetails {
        name: None,
        group: None,
        kind: None,
        uid: None,
        causes: if causes.is_empty() {
            None
        } else {
            Some(causes)
        },
        retry_after_seconds: None,
    }
}

/// Best-effort parser for legacy `Error::InvalidResource(String)` messages
/// that follow upstream's `"<field>: <ErrorType>: <detail>"` shape (e.g.
/// `"spec.containers: Required value"`,
/// `"spec.containers[1].name: Duplicate value: \"ctr-a\""`,
/// `"spec.nodeName: Invalid value: \"node-2\": field is immutable"`).
///
/// Returns `None` if the message does not look like a field error — callers
/// fall back to a minimal hardcoded cause in that case.
#[cfg(feature = "axum-support")]
fn parse_legacy_field_error(msg: &str) -> Option<(crate::validation::field::ErrorType, String)> {
    use crate::validation::field::ErrorType;

    // Split into `<field>: <rest>` — `rest` carries the ErrorType label and
    // (optionally) the bad value and detail. We only need the type label here.
    let (field, rest) = msg.split_once(": ")?;
    if field.is_empty() || field.contains(' ') {
        return None;
    }
    // Each ErrorType has a canonical leading label (see `ErrorType::as_str`).
    // Match longest-prefix first so `"Invalid value"` is not matched as
    // `"Invalid"` (which is not actually one of upstream's labels).
    let labels: &[(&str, ErrorType)] = &[
        ("Required value", ErrorType::Required),
        ("Duplicate value", ErrorType::Duplicate),
        ("Unsupported value", ErrorType::NotSupported),
        ("Forbidden", ErrorType::Forbidden),
        ("Invalid value", ErrorType::Invalid),
        ("Not found", ErrorType::NotFound),
        ("Too long", ErrorType::TooLong),
        ("Too many", ErrorType::TooMany),
        ("Internal error", ErrorType::Internal),
    ];
    for (label, ty) in labels {
        if rest == *label || rest.starts_with(&format!("{label}:")) {
            return Some((*ty, field.to_string()));
        }
    }
    None
}

/// Extract resource details for Invalid errors, including causes.
#[cfg(feature = "axum-support")]
fn extract_resource_details_for_invalid(msg: &str) -> Option<crate::types::StatusDetails> {
    // Try to parse upstream-shaped legacy messages into a structured cause.
    // Falls back to the previous hardcoded shape only when parsing fails so
    // that older callers that emit free-form text don't suddenly 500.
    let (reason, field) = match parse_legacy_field_error(msg) {
        Some((ty, field)) => (ty.cause_reason().to_string(), field),
        None => ("FieldValueInvalid".to_string(), "metadata.name".to_string()),
    };
    Some(crate::types::StatusDetails {
        name: None,
        group: None,
        kind: None,
        uid: None,
        causes: Some(vec![crate::types::StatusCause {
            reason: Some(reason),
            message: Some(msg.to_string()),
            field: Some(field),
        }]),
        retry_after_seconds: None,
    })
}

#[cfg(all(test, feature = "axum-support"))]
mod tests {
    use super::*;
    use crate::validation::field::{ErrorType, Path};

    #[test]
    fn parse_legacy_required() {
        let (ty, field) = parse_legacy_field_error("spec.containers: Required value").unwrap();
        assert_eq!(ty, ErrorType::Required);
        assert_eq!(field, "spec.containers");
    }

    #[test]
    fn parse_legacy_duplicate_with_value() {
        let (ty, field) =
            parse_legacy_field_error("spec.containers[1].name: Duplicate value: \"ctr-a\"")
                .unwrap();
        assert_eq!(ty, ErrorType::Duplicate);
        assert_eq!(field, "spec.containers[1].name");
    }

    #[test]
    fn parse_legacy_forbidden() {
        let (ty, field) = parse_legacy_field_error(
            "spec.ephemeralContainers: Forbidden: cannot be set on create",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::Forbidden);
        assert_eq!(field, "spec.ephemeralContainers");
    }

    #[test]
    fn parse_legacy_invalid_with_detail() {
        let (ty, field) = parse_legacy_field_error(
            "spec.nodeName: Invalid value: \"node-2\": field is immutable",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::Invalid);
        assert_eq!(field, "spec.nodeName");
    }

    #[test]
    fn parse_legacy_unsupported() {
        let (ty, field) = parse_legacy_field_error(
            "spec.restartPolicy: Unsupported value: \"InvalidPolicy\": supported values: \"Always\", \"OnFailure\", \"Never\"",
        )
        .unwrap();
        assert_eq!(ty, ErrorType::NotSupported);
        assert_eq!(field, "spec.restartPolicy");
    }

    #[test]
    fn build_details_from_error_list_one_cause_per_error() {
        let p = Path::new("spec");
        let errs = vec![
            crate::validation::field::Error::required(&p.child("containers"), ""),
            crate::validation::field::Error::not_supported(
                &p.child("restartPolicy"),
                "InvalidPolicy",
                &["Always", "OnFailure", "Never"],
            ),
        ];
        let details = build_invalid_details_from_errors(&errs);
        let causes = details.causes.unwrap();
        assert_eq!(causes.len(), 2);
        assert_eq!(causes[0].reason.as_deref(), Some("FieldValueRequired"));
        assert_eq!(causes[0].field.as_deref(), Some("spec.containers"));
        assert_eq!(causes[1].reason.as_deref(), Some("FieldValueNotSupported"));
        assert_eq!(causes[1].field.as_deref(), Some("spec.restartPolicy"));
    }
}
