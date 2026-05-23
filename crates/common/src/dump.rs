//! Payload-dump instrumentation for conformance debugging.
//!
//! When `RUSTERNETES_DUMP_PAYLOADS=1`, panics, 5xx responses, and JSON decode
//! failures emit a `tracing::error!` containing the offending request body
//! (with Secret data redacted). All entrypoints are no-ops when the env var
//! is unset.

use std::borrow::Cow;
use std::sync::OnceLock;

static DUMPS_ENABLED: OnceLock<bool> = OnceLock::new();

/// Returns true iff `RUSTERNETES_DUMP_PAYLOADS=1` was set when this process
/// started.
pub fn dumps_enabled() -> bool {
    *DUMPS_ENABLED
        .get_or_init(|| std::env::var("RUSTERNETES_DUMP_PAYLOADS").is_ok_and(|v| v == "1"))
}

/// Length of a base64-encoded payload's decoded bytes, or the encoded length
/// if decoding fails. Used for the redaction marker so the dump still hints
/// at the value's size.
fn decoded_len(b64: &str) -> usize {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map(|v| v.len())
        .unwrap_or_else(|_| b64.len())
}

fn redact_data_map(map: &mut serde_json::Map<String, serde_json::Value>) {
    for v in map.values_mut() {
        if let Some(s) = v.as_str() {
            let n = decoded_len(s);
            *v = serde_json::Value::String(format!("<redacted len={n}>"));
        }
    }
}

fn redact_string_data_map(map: &mut serde_json::Map<String, serde_json::Value>) {
    for v in map.values_mut() {
        if let Some(s) = v.as_str() {
            let n = s.len();
            *v = serde_json::Value::String(format!("<redacted len={n}>"));
        }
    }
}

fn redact_one(value: &mut serde_json::Value) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let kind = obj
        .get("kind")
        .and_then(|k| k.as_str())
        .unwrap_or("")
        .to_string();
    match kind.as_str() {
        "Secret" => {
            if let Some(serde_json::Value::Object(m)) = obj.get_mut("data") {
                redact_data_map(m);
            }
            if let Some(serde_json::Value::Object(m)) = obj.get_mut("stringData") {
                redact_string_data_map(m);
            }
        }
        "SecretList" => {
            if let Some(serde_json::Value::Array(items)) = obj.get_mut("items") {
                for item in items {
                    redact_one(item);
                }
            }
        }
        "AdmissionReview" => {
            if let Some(serde_json::Value::Object(req)) = obj.get_mut("request") {
                for key in ["object", "oldObject"] {
                    if let Some(v) = req.get_mut(key) {
                        redact_one(v);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Replace `data` / `stringData` values in any embedded `Secret` with
/// `"<redacted len=N>"`. Pass-through for non-JSON, non-Secret bodies.
pub fn redact_secret_like(bytes: &[u8]) -> Cow<'_, [u8]> {
    let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Cow::Borrowed(bytes);
    };
    let before = v.clone();
    redact_one(&mut v);
    if v == before {
        return Cow::Borrowed(bytes);
    }
    match serde_json::to_vec(&v) {
        Ok(out) => Cow::Owned(out),
        Err(_) => Cow::Borrowed(bytes),
    }
}

use std::cell::RefCell;

tokio::task_local! {
    pub static CURRENT_PAYLOAD: RefCell<Option<bytes::Bytes>>;
}

/// Run `fut` with `body` accessible via `CURRENT_PAYLOAD` for the duration
/// of the future (and any tasks it spawns that inherit the task-local).
pub async fn with_payload<F, T>(body: bytes::Bytes, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CURRENT_PAYLOAD.scope(RefCell::new(Some(body)), fut).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dumps_enabled_reads_env_once() {
        // Cannot reliably mutate process env across tests, so just assert
        // the function does not panic and returns a stable bool.
        let a = dumps_enabled();
        let b = dumps_enabled();
        assert_eq!(a, b);
    }

    #[test]
    fn redact_passthrough_for_plain_pod() {
        let input = br#"{"kind":"Pod","metadata":{"name":"p"}}"#;
        assert_eq!(&*redact_secret_like(input), input);
    }

    #[test]
    fn redact_replaces_secret_data_values() {
        let input = br#"{"kind":"Secret","data":{"token":"YWJjZA==","empty":""}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["data"]["token"], "<redacted len=4>");
        assert_eq!(v["data"]["empty"], "<redacted len=0>");
    }

    #[test]
    fn redact_replaces_secret_string_data_values() {
        let input = br#"{"kind":"Secret","stringData":{"pw":"hunter2"}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["stringData"]["pw"], "<redacted len=7>");
    }

    #[test]
    fn redact_walks_secret_list_items() {
        let input = br#"{"kind":"SecretList","items":[
        {"kind":"Secret","data":{"k":"YWI="}},
        {"kind":"Secret","stringData":{"k":"v"}}
    ]}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["items"][0]["data"]["k"], "<redacted len=2>");
        assert_eq!(v["items"][1]["stringData"]["k"], "<redacted len=1>");
    }

    #[test]
    fn redact_walks_admission_review_object() {
        let input = br#"{"kind":"AdmissionReview","request":{
        "object":{"kind":"Secret","data":{"k":"YWI="}},
        "oldObject":{"kind":"Secret","stringData":{"k":"x"}}
    }}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["request"]["object"]["data"]["k"], "<redacted len=2>");
        assert_eq!(
            v["request"]["oldObject"]["stringData"]["k"],
            "<redacted len=1>"
        );
    }

    #[test]
    fn redact_passthrough_for_malformed_json() {
        let input = b"not json at all";
        assert_eq!(&*redact_secret_like(input), input);
    }

    #[test]
    fn redact_leaves_configmap_alone() {
        let input = br#"{"kind":"ConfigMap","data":{"k":"v"}}"#;
        let out = redact_secret_like(input);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["data"]["k"], "v");
    }

    #[tokio::test]
    async fn with_payload_makes_bytes_visible_inside_scope() {
        let body = bytes::Bytes::from_static(b"hello");
        let seen = with_payload(body.clone(), async {
            CURRENT_PAYLOAD.with(|cell| cell.borrow().clone())
        })
        .await;
        assert_eq!(seen.as_deref(), Some(b"hello".as_ref()));
    }

    #[tokio::test]
    async fn current_payload_outside_scope_returns_err() {
        let res = CURRENT_PAYLOAD.try_with(|cell| cell.borrow().clone());
        assert!(res.is_err());
    }
}
