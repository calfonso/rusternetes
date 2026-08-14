//! Serde helpers for the two timestamp formats in the Kubernetes API.
//!
//! `metav1.Time` marshals with Go's `time.RFC3339` layout, which has no
//! fractional-seconds component, so it is always whole seconds.
//! `metav1.MicroTime` marshals with microseconds. chrono's own `Serialize`
//! matches neither: it emits as many sub-second digits as the value carries, and
//! `Utc::now()` carries nanoseconds.
//!
//! Both formats accept plain RFC3339 on the way in, with or without a fractional
//! part, so objects already stored with sub-second precision keep deserializing.
//!
//! Apply them the way the resource types already do:
//!
//! ```ignore
//! #[serde(
//!     skip_serializing_if = "Option::is_none",
//!     default,
//!     serialize_with = "crate::time::k8s_time::serialize",
//!     deserialize_with = "crate::time::k8s_time::deserialize"
//! )]
//! pub last_transition_time: Option<DateTime<Utc>>,
//! ```
//!
//! `default` is required next to `deserialize_with` on an optional field:
//! without it serde stops treating a missing field as `None` and rejects the
//! object instead.

/// Kubernetes `Time` — RFC3339 without fractional seconds, e.g.
/// `2026-08-07T04:32:22Z`.
pub mod k8s_time {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(dt) => serialize_required(dt, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => parse(&s).map(Some),
            None => Ok(None),
        }
    }

    /// Serialize a required timestamp in `Time` format.
    pub fn serialize_required<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format(date))
    }

    /// Deserialize a required timestamp from `Time` format.
    pub fn deserialize_required<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        parse(&s)
    }

    /// Kubernetes Time format: WITHOUT fractional seconds (RFC3339 basic).
    pub(super) fn format(date: &DateTime<Utc>) -> String {
        date.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    }

    /// Accept RFC3339 with or without fractional seconds.
    pub(super) fn parse<E: serde::de::Error>(s: &str) -> Result<DateTime<Utc>, E> {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(dt.with_timezone(&Utc));
        }
        s.parse::<DateTime<Utc>>().map_err(serde::de::Error::custom)
    }
}

/// Kubernetes `Time` values keyed by name, as in
/// `PodDisruptionBudgetStatus.disruptedPods`.
pub mod k8s_time_map {
    use super::k8s_time;
    use chrono::{DateTime, Utc};
    use serde::{self, ser::SerializeMap, Deserialize, Deserializer, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S>(
        map: &Option<HashMap<String, DateTime<Utc>>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match map {
            Some(entries) => {
                let mut out = serializer.serialize_map(Some(entries.len()))?;
                for (name, time) in entries {
                    out.serialize_entry(name, &k8s_time::format(time))?;
                }
                out.end()
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<HashMap<String, DateTime<Utc>>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<HashMap<String, String>> = Option::deserialize(deserializer)?;
        match opt {
            Some(entries) => entries
                .into_iter()
                .map(|(name, s)| k8s_time::parse(&s).map(|time| (name, time)))
                .collect::<Result<HashMap<_, _>, _>>()
                .map(Some),
            None => Ok(None),
        }
    }
}

/// Kubernetes `MicroTime` — RFC3339 with microseconds, e.g.
/// `2026-08-07T04:32:22.089611Z`.
pub mod micro_time {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(dt) => serialize_required(dt, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => parse(&s).map(Some),
            None => Ok(None),
        }
    }

    /// Serialize a required timestamp in `MicroTime` format.
    pub fn serialize_required<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Always include microseconds for MicroTime — the K8s Events v1 client
        // uses time.Parse("2006-01-02T15:04:05.000000Z07:00"), which requires them.
        serializer.serialize_str(&date.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
    }

    /// Deserialize a required timestamp from `MicroTime` format.
    pub fn deserialize_required<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        parse(&s)
    }

    fn parse<E: serde::de::Error>(s: &str) -> Result<DateTime<Utc>, E> {
        // Try microseconds first, then plain RFC3339.
        if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.6fZ") {
            return Ok(dt.with_timezone(&Utc));
        }
        super::k8s_time::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serde::{Deserialize, Serialize};

    fn nanosecond_precision() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 7, 4, 32, 22)
            .unwrap()
            .with_nanosecond(89_611_138)
            .unwrap()
    }

    use chrono::Timelike;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Holder {
        #[serde(
            skip_serializing_if = "Option::is_none",
            default,
            serialize_with = "super::k8s_time::serialize",
            deserialize_with = "super::k8s_time::deserialize"
        )]
        time: Option<chrono::DateTime<Utc>>,
        #[serde(
            serialize_with = "super::k8s_time::serialize_required",
            deserialize_with = "super::k8s_time::deserialize_required"
        )]
        required: chrono::DateTime<Utc>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            default,
            serialize_with = "super::micro_time::serialize",
            deserialize_with = "super::micro_time::deserialize"
        )]
        micro: Option<chrono::DateTime<Utc>>,
    }

    fn holder() -> Holder {
        Holder {
            time: Some(nanosecond_precision()),
            required: nanosecond_precision(),
            micro: Some(nanosecond_precision()),
        }
    }

    #[test]
    fn test_time_is_truncated_to_whole_seconds() {
        let json = serde_json::to_value(holder()).unwrap();

        assert_eq!(json["time"], "2026-08-07T04:32:22Z");
        assert_eq!(json["required"], "2026-08-07T04:32:22Z");
    }

    #[test]
    fn test_micro_time_keeps_microseconds() {
        let json = serde_json::to_value(holder()).unwrap();

        assert_eq!(json["micro"], "2026-08-07T04:32:22.089611Z");
    }

    #[test]
    fn test_sub_second_input_is_still_accepted() {
        // Objects already stored with nanosecond precision must keep loading.
        let json = serde_json::json!({
            "time": "2026-08-07T04:32:22.089611138Z",
            "required": "2026-08-07T04:32:22.089611138Z",
            "micro": "2026-08-07T04:32:22.089611138Z",
        });

        let holder: Holder = serde_json::from_value(json).unwrap();

        assert_eq!(holder.time.unwrap(), nanosecond_precision());
        assert_eq!(holder.required, nanosecond_precision());
        assert_eq!(holder.micro.unwrap(), nanosecond_precision());
    }

    #[test]
    fn test_whole_second_input_round_trips() {
        let json = serde_json::json!({
            "time": "2026-08-07T04:32:22Z",
            "required": "2026-08-07T04:32:22Z",
            "micro": "2026-08-07T04:32:22Z",
        });

        let holder: Holder = serde_json::from_value(json.clone()).unwrap();
        let reserialized = serde_json::to_value(&holder).unwrap();

        assert_eq!(reserialized["time"], json["time"]);
        assert_eq!(reserialized["required"], json["required"]);
    }

    #[test]
    fn test_offset_timestamps_are_normalised_to_utc() {
        let json = serde_json::json!({
            "time": "2026-08-07T06:32:22+02:00",
            "required": "2026-08-07T06:32:22+02:00",
        });

        let holder: Holder = serde_json::from_value(json).unwrap();

        assert_eq!(
            serde_json::to_value(&holder).unwrap()["time"],
            "2026-08-07T04:32:22Z"
        );
    }

    #[test]
    fn test_absent_and_null_optional_timestamps() {
        // A missing optional field must stay None rather than failing to parse,
        // which is what `default` next to `deserialize_with` buys.
        let absent: Holder =
            serde_json::from_value(serde_json::json!({ "required": "2026-08-07T04:32:22Z" }))
                .unwrap();
        assert_eq!(absent.time, None);
        assert_eq!(absent.micro, None);

        let null: Holder = serde_json::from_value(serde_json::json!({
            "time": null,
            "required": "2026-08-07T04:32:22Z",
            "micro": null,
        }))
        .unwrap();
        assert_eq!(null.time, None);

        // And an absent optional stays out of the output entirely.
        let json = serde_json::to_value(&absent).unwrap();
        assert!(json.get("time").is_none());
    }
}
