//! Serde helpers shared across resource types.
//!
//! Kubernetes API clients sometimes serialize unset enum-typed fields as the
//! empty string instead of omitting them. Upstream Go decoders accept the
//! zero-value; strict Rust serde enums reject it as an unknown variant.
//! `empty_string_as_none` bridges that gap for `Option<T>` fields whose
//! underlying type is a string-tagged enum.

use serde::{Deserialize, Deserializer};

/// Deserialize an `Option<T>` where the JSON value may be `null`, missing,
/// or the empty string. Empty strings are coerced to `None` before falling
/// through to `T`'s own deserializer.
pub fn empty_string_as_none<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) if s.is_empty() => Ok(None),
        Some(v) => T::deserialize(v)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    enum Policy {
        Cluster,
        Local,
    }

    #[derive(Debug, Deserialize)]
    struct Holder {
        #[serde(default, deserialize_with = "empty_string_as_none")]
        policy: Option<Policy>,
    }

    #[test]
    fn empty_string_becomes_none() {
        let h: Holder = serde_json::from_str(r#"{"policy": ""}"#).unwrap();
        assert_eq!(h.policy, None);
    }

    #[test]
    fn missing_field_becomes_none() {
        let h: Holder = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(h.policy, None);
    }

    #[test]
    fn null_becomes_none() {
        let h: Holder = serde_json::from_str(r#"{"policy": null}"#).unwrap();
        assert_eq!(h.policy, None);
    }

    #[test]
    fn named_variant_passes_through() {
        let h: Holder = serde_json::from_str(r#"{"policy": "Local"}"#).unwrap();
        assert_eq!(h.policy, Some(Policy::Local));
    }

    #[test]
    fn bogus_variant_still_errors() {
        let r: Result<Holder, _> = serde_json::from_str(r#"{"policy": "Galactic"}"#);
        assert!(r.is_err());
    }
}
