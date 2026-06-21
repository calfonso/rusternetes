//! Rusternetes port of upstream `k8s.io/apimachinery/pkg/util/validation/field`.
//!
//! Provides [`Path`] (a typed breadcrumb like `metadata.labels[foo]`) and
//! [`Error`] (a structured validation error carrying type, field path, bad
//! value and detail). The string rendering matches upstream byte-for-byte so
//! conformance log greps and unit-test needles stay valid.
//!
//! Upstream source (release-1.35): <https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/apimachinery/pkg/util/validation/field/errors.go>

use std::fmt;

/// Typed field path. Mirrors upstream `field.Path`.
///
/// Paths are built incrementally via [`Path::child`] (dotted child) and
/// [`Path::index`] (`[i]` indexer). `Display` renders to the canonical
/// `metadata.labels[foo]` form upstream uses.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Path {
    segments: Vec<Segment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Field(String),
    Index(usize),
    /// Map key, rendered as `[key]` upstream — same as Index but quoted as a
    /// raw string. Kept separate so future tweaks (e.g. quoting) are isolated.
    Key(String),
}

impl Path {
    /// Build a new path starting with the given root segment(s). The first
    /// segment becomes the root; any further arguments are appended as
    /// dot-children. Mirrors upstream `field.NewPath(name, moreNames...)`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            segments: vec![Segment::Field(name.into())],
        }
    }

    /// Append a dotted child segment. Mirrors `Path.Child`.
    pub fn child(&self, name: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Field(name.into()));
        next
    }

    /// Append an `[i]` indexer. Mirrors `Path.Index`.
    pub fn index(&self, i: usize) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Index(i));
        next
    }

    /// Append a `[key]` map indexer. Mirrors `Path.Key`.
    pub fn key(&self, k: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.segments.push(Segment::Key(k.into()));
        next
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for seg in &self.segments {
            match seg {
                Segment::Field(s) => {
                    if !first {
                        f.write_str(".")?;
                    }
                    f.write_str(s)?;
                }
                Segment::Index(i) => {
                    write!(f, "[{i}]")?;
                }
                Segment::Key(k) => {
                    write!(f, "[{k}]")?;
                }
            }
            first = false;
        }
        Ok(())
    }
}

/// Error type discriminator. Mirrors upstream `field.ErrorType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    NotFound,
    Required,
    Duplicate,
    Invalid,
    NotSupported,
    Forbidden,
    TooLong,
    TooMany,
    Internal,
    TypeInvalid,
}

impl ErrorType {
    /// Canonical error string upstream uses in `Error.ErrorBody()`.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorType::NotFound => "Not found",
            ErrorType::Required => "Required value",
            ErrorType::Duplicate => "Duplicate value",
            ErrorType::Invalid => "Invalid value",
            ErrorType::NotSupported => "Unsupported value",
            ErrorType::Forbidden => "Forbidden",
            ErrorType::TooLong => "Too long",
            ErrorType::TooMany => "Too many",
            ErrorType::Internal => "Internal error",
            ErrorType::TypeInvalid => "Invalid value",
        }
    }

    /// Upstream `Status.details.causes[].reason` string for this error type.
    ///
    /// Mirrors `apimachinery/pkg/api/errors/errors.go::NewInvalid` which maps
    /// every `field.ErrorType` to a `metav1.CauseType` string.
    pub fn cause_reason(self) -> &'static str {
        match self {
            ErrorType::NotFound => "FieldValueNotFound",
            ErrorType::Required => "FieldValueRequired",
            ErrorType::Duplicate => "FieldValueDuplicate",
            ErrorType::Invalid => "FieldValueInvalid",
            ErrorType::NotSupported => "FieldValueNotSupported",
            ErrorType::Forbidden => "FieldValueForbidden",
            ErrorType::TooLong => "FieldValueTooLong",
            ErrorType::TooMany => "FieldValueTooMany",
            // Upstream NewInvalid collapses Internal into FieldValueInvalid.
            ErrorType::Internal => "FieldValueInvalid",
            ErrorType::TypeInvalid => "FieldValueInvalid",
        }
    }
}

/// A `BadValue` payload that knows how to render itself the way upstream
/// `field.Error.ErrorBody()` does:
/// - strings become `"quoted"`
/// - integers and bools use the plain `%v` form
/// - anything else falls back to JSON.
#[derive(Debug, Clone, PartialEq)]
pub enum BadValue {
    String(String),
    I64(i64),
    Bool(bool),
    /// Renders as JSON via `serde_json::to_string`. Used for slice/struct
    /// values such as `[]string{"All", "False"}`.
    Json(serde_json::Value),
    /// Sentinel for "omit the value entirely". Mirrors upstream `omitValue`.
    Omit,
}

impl From<&str> for BadValue {
    fn from(s: &str) -> Self {
        BadValue::String(s.to_string())
    }
}
impl From<String> for BadValue {
    fn from(s: String) -> Self {
        BadValue::String(s)
    }
}
impl From<i32> for BadValue {
    fn from(n: i32) -> Self {
        BadValue::I64(n as i64)
    }
}
impl From<i64> for BadValue {
    fn from(n: i64) -> Self {
        BadValue::I64(n)
    }
}
impl From<bool> for BadValue {
    fn from(b: bool) -> Self {
        BadValue::Bool(b)
    }
}
impl From<Vec<String>> for BadValue {
    fn from(v: Vec<String>) -> Self {
        BadValue::Json(serde_json::Value::Array(
            v.into_iter().map(serde_json::Value::String).collect(),
        ))
    }
}
impl From<&[String]> for BadValue {
    fn from(v: &[String]) -> Self {
        BadValue::Json(serde_json::Value::Array(
            v.iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        ))
    }
}
impl From<serde_json::Value> for BadValue {
    fn from(v: serde_json::Value) -> Self {
        BadValue::Json(v)
    }
}

/// Structured validation error. Mirrors upstream `field.Error`.
#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub error_type: ErrorType,
    pub field: String,
    pub bad_value: BadValue,
    pub detail: String,
    pub origin: String,
}

impl Error {
    /// Construct a generic invalid-value error. Mirrors `field.Invalid`.
    pub fn invalid(path: &Path, value: impl Into<BadValue>, detail: impl Into<String>) -> Self {
        Self {
            error_type: ErrorType::Invalid,
            field: path.to_string(),
            bad_value: value.into(),
            detail: detail.into(),
            origin: String::new(),
        }
    }

    /// `field.Required` — the bad value is omitted.
    pub fn required(path: &Path, detail: impl Into<String>) -> Self {
        Self {
            error_type: ErrorType::Required,
            field: path.to_string(),
            bad_value: BadValue::Omit,
            detail: detail.into(),
            origin: String::new(),
        }
    }

    /// `field.Forbidden` — the bad value is omitted.
    pub fn forbidden(path: &Path, detail: impl Into<String>) -> Self {
        Self {
            error_type: ErrorType::Forbidden,
            field: path.to_string(),
            bad_value: BadValue::Omit,
            detail: detail.into(),
            origin: String::new(),
        }
    }

    /// `field.NotSupported` — value rendered, detail is the supported list.
    pub fn not_supported<S: AsRef<str>>(
        path: &Path,
        value: impl Into<BadValue>,
        valid_values: &[S],
    ) -> Self {
        let quoted: Vec<String> = valid_values
            .iter()
            .map(|s| format!("\"{}\"", s.as_ref()))
            .collect();
        let detail = format!("supported values: {}", quoted.join(", "));
        Self {
            error_type: ErrorType::NotSupported,
            field: path.to_string(),
            bad_value: value.into(),
            detail,
            origin: String::new(),
        }
    }

    /// `field.Duplicate` — value rendered, no detail.
    pub fn duplicate(path: &Path, value: impl Into<BadValue>) -> Self {
        Self {
            error_type: ErrorType::Duplicate,
            field: path.to_string(),
            bad_value: value.into(),
            detail: String::new(),
            origin: String::new(),
        }
    }

    /// `field.NotFound` — value rendered, no detail by default.
    pub fn not_found(path: &Path, value: impl Into<BadValue>) -> Self {
        Self {
            error_type: ErrorType::NotFound,
            field: path.to_string(),
            bad_value: value.into(),
            detail: String::new(),
            origin: String::new(),
        }
    }

    /// `field.TooLong` — bad value omitted, detail says the max length.
    pub fn too_long(path: &Path, max_length: usize) -> Self {
        Self {
            error_type: ErrorType::TooLong,
            field: path.to_string(),
            bad_value: BadValue::Omit,
            detail: format!("may not be more than {max_length} bytes"),
            origin: String::new(),
        }
    }

    /// `field.TooMany` — bad value omitted, detail says the max count.
    pub fn too_many(path: &Path, max_items: usize) -> Self {
        Self {
            error_type: ErrorType::TooMany,
            field: path.to_string(),
            bad_value: BadValue::Omit,
            detail: format!("must have at most {max_items} items"),
            origin: String::new(),
        }
    }

    /// Builder: attach an upstream-style origin tag (e.g. `format=k8s-label-key`).
    #[must_use]
    pub fn with_origin(mut self, origin: impl Into<String>) -> Self {
        self.origin = origin.into();
        self
    }

    /// Upstream `Error.ErrorBody()`: same string the test needles match against
    /// (without the leading `field: `).
    pub fn error_body(&self) -> String {
        let mut s = match self.error_type {
            ErrorType::Required
            | ErrorType::Forbidden
            | ErrorType::TooLong
            | ErrorType::Internal => self.error_type.as_str().to_string(),
            ErrorType::Invalid
            | ErrorType::TypeInvalid
            | ErrorType::NotSupported
            | ErrorType::NotFound
            | ErrorType::Duplicate
            | ErrorType::TooMany => match &self.bad_value {
                BadValue::Omit => self.error_type.as_str().to_string(),
                BadValue::String(v) => format!("{}: {:?}", self.error_type.as_str(), v),
                BadValue::I64(v) => format!("{}: {}", self.error_type.as_str(), v),
                BadValue::Bool(v) => format!("{}: {}", self.error_type.as_str(), v),
                BadValue::Json(v) => {
                    let rendered = serde_json::to_string(v).unwrap_or_else(|_| format!("{v:?}"));
                    format!("{}: {}", self.error_type.as_str(), rendered)
                }
            },
        };
        if !self.detail.is_empty() {
            s.push_str(": ");
            s.push_str(&self.detail);
        }
        s
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.error_body())
    }
}

impl std::error::Error for Error {}

/// Convenience alias mirroring upstream `field.ErrorList`. Validators
/// accumulate into a `Vec<Error>` rather than short-circuiting on the first
/// failure, so every input row is reported.
pub type ErrorList = Vec<Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_render_matches_upstream() {
        let p = Path::new("metadata").child("labels").key("app");
        assert_eq!(p.to_string(), "metadata.labels[app]");
        let p2 = Path::new("status")
            .child("conditions")
            .index(2)
            .child("type");
        assert_eq!(p2.to_string(), "status.conditions[2].type");
    }

    #[test]
    fn invalid_string_quoting() {
        let p = Path::new("status")
            .child("conditions")
            .index(0)
            .child("type");
        let e = Error::invalid(&p, ":invalid", "name part must");
        assert_eq!(
            e.to_string(),
            "status.conditions[0].type: Invalid value: \":invalid\": name part must"
        );
    }

    #[test]
    fn required_omits_value() {
        let p = Path::new("status")
            .child("conditions")
            .index(0)
            .child("lastTransitionTime");
        let e = Error::required(&p, "");
        assert_eq!(
            e.to_string(),
            "status.conditions[0].lastTransitionTime: Required value"
        );
    }

    #[test]
    fn duplicate_quotes_value() {
        let p = Path::new("status")
            .child("conditions")
            .index(2)
            .child("type");
        let e = Error::duplicate(&p, "First");
        assert_eq!(
            e.to_string(),
            "status.conditions[2].type: Duplicate value: \"First\""
        );
    }

    #[test]
    fn not_supported_renders_supported_list() {
        let p = Path::new("status")
            .child("conditions")
            .index(0)
            .child("status");
        let e = Error::not_supported(&p, "unknown", &["False", "True", "Unknown"]);
        assert_eq!(
            e.to_string(),
            "status.conditions[0].status: Unsupported value: \"unknown\": supported values: \"False\", \"True\", \"Unknown\""
        );
    }

    #[test]
    fn invalid_int_no_quotes() {
        let p = Path::new("status")
            .child("conditions")
            .index(0)
            .child("observedGeneration");
        let e = Error::invalid(&p, -1i64, "must be greater than or equal to zero");
        assert_eq!(
            e.to_string(),
            "status.conditions[0].observedGeneration: Invalid value: -1: must be greater than or equal to zero"
        );
    }
}
