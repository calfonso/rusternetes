use crate::client::ApiClient;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;

/// List events in a namespace, optionally filtered by resource.
///
/// Equivalent to:
///   kubectl events
///   kubectl events --for pod/nginx
///   kubectl events -A
///   kubectl events --types=Warning
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    namespace: &str,
    all_namespaces: bool,
    for_object: Option<&str>,
    types: Option<&str>,
    watch: bool,
    no_headers: bool,
    output: Option<&str>,
) -> Result<()> {
    let ns_path = if all_namespaces {
        "/api/v1/events".to_string()
    } else {
        format!("/api/v1/namespaces/{}/events", namespace)
    };

    // Build field selector for --for filtering
    let path = if let Some(for_obj) = for_object {
        let (kind, obj_name) = parse_for_object(for_obj)?;
        let field_selector = format!(
            "involvedObject.kind={},involvedObject.name={}",
            kind, obj_name
        );
        format!("{}?fieldSelector={}", ns_path, urlencoding(&field_selector))
    } else {
        ns_path
    };

    // Parse type filters
    let type_filters: Vec<String> = types
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    // Validate type filters
    for t in &type_filters {
        if !t.eq_ignore_ascii_case("Normal") && !t.eq_ignore_ascii_case("Warning") {
            anyhow::bail!("valid --types are Normal or Warning");
        }
    }

    if watch {
        return execute_watch(client, &path, &type_filters, all_namespaces, no_headers).await;
    }

    let events: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list events: {}", e))?;

    let items = events
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();

    // Filter by type if specified
    let filtered: Vec<&Value> = items
        .iter()
        .filter(|event| {
            if type_filters.is_empty() {
                return true;
            }
            let event_type = event
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("Normal");
            type_filters
                .iter()
                .any(|f| f.eq_ignore_ascii_case(event_type))
        })
        .collect();

    if filtered.is_empty() {
        if all_namespaces {
            eprintln!("No events found.");
        } else {
            eprintln!("No events found in {} namespace.", namespace);
        }
        return Ok(());
    }

    // Sort events by lastTimestamp (or eventTime)
    let mut sorted: Vec<&Value> = filtered;
    sorted.sort_by(|a, b| {
        let time_a = get_event_time(a);
        let time_b = get_event_time(b);
        time_a.cmp(&time_b)
    });

    match output {
        Some("json") => {
            let output_list = serde_json::json!({
                "apiVersion": "v1",
                "kind": "EventList",
                "items": sorted,
            });
            println!("{}", serde_json::to_string_pretty(&output_list)?);
        }
        Some("yaml") => {
            let output_list = serde_json::json!({
                "apiVersion": "v1",
                "kind": "EventList",
                "items": sorted,
            });
            println!("{}", serde_yaml::to_string(&output_list)?);
        }
        _ => {
            // Table output
            if !no_headers {
                if all_namespaces {
                    println!(
                        "{:<12} {:<8} {:<6} {:<40} {:<20} {:<}",
                        "NAMESPACE", "LAST SEEN", "TYPE", "REASON", "OBJECT", "MESSAGE"
                    );
                } else {
                    println!(
                        "{:<8} {:<8} {:<6} {:<40} {:<20} {:<}",
                        "LAST SEEN", "TYPE", "COUNT", "REASON", "OBJECT", "MESSAGE"
                    );
                }
            }

            for event in &sorted {
                print_event_row(event, all_namespaces);
            }
        }
    }

    Ok(())
}

async fn execute_watch(
    client: &ApiClient,
    path: &str,
    type_filters: &[String],
    all_namespaces: bool,
    no_headers: bool,
) -> Result<()> {
    // First list existing events
    let events: Value = client
        .get(path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list events: {}", e))?;

    let items = events
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();

    if !no_headers {
        if all_namespaces {
            println!(
                "{:<12} {:<8} {:<6} {:<40} {:<20} {:<}",
                "NAMESPACE", "LAST SEEN", "TYPE", "REASON", "OBJECT", "MESSAGE"
            );
        } else {
            println!(
                "{:<8} {:<8} {:<6} {:<40} {:<20} {:<}",
                "LAST SEEN", "TYPE", "COUNT", "REASON", "OBJECT", "MESSAGE"
            );
        }
    }

    for event in &items {
        if !type_filters.is_empty() {
            let event_type = event
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("Normal");
            if !type_filters
                .iter()
                .any(|f| f.eq_ignore_ascii_case(event_type))
            {
                continue;
            }
        }
        print_event_row(event, all_namespaces);
    }

    // Get resource version for watch
    let resource_version = events
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|r| r.as_str())
        .unwrap_or("0");

    let watch_path = if path.contains('?') {
        format!("{}&watch=true&resourceVersion={}", path, resource_version)
    } else {
        format!("{}?watch=true&resourceVersion={}", path, resource_version)
    };

    // Stream watch events
    let text = client
        .get_text(&watch_path)
        .await
        .context("Failed to watch events")?;

    for line in text.lines() {
        if let Ok(watch_event) = serde_json::from_str::<Value>(line) {
            if let Some(event) = watch_event.get("object") {
                if !type_filters.is_empty() {
                    let event_type = event
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("Normal");
                    if !type_filters
                        .iter()
                        .any(|f| f.eq_ignore_ascii_case(event_type))
                    {
                        continue;
                    }
                }
                print_event_row(event, all_namespaces);
            }
        }
    }

    Ok(())
}

fn parse_for_object(for_obj: &str) -> Result<(String, &str)> {
    let (kind_str, name) = for_obj
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("--for must be in resource/name form (e.g., pod/nginx)"))?;

    // Capitalize and singularize the kind
    let kind = match kind_str.to_lowercase().as_str() {
        "pod" | "pods" | "po" => "Pod",
        "service" | "services" | "svc" => "Service",
        "deployment" | "deployments" | "deploy" => "Deployment",
        "replicaset" | "replicasets" | "rs" => "ReplicaSet",
        "statefulset" | "statefulsets" | "sts" => "StatefulSet",
        "daemonset" | "daemonsets" | "ds" => "DaemonSet",
        "job" | "jobs" => "Job",
        "cronjob" | "cronjobs" | "cj" => "CronJob",
        "node" | "nodes" | "no" => "Node",
        "namespace" | "namespaces" | "ns" => "Namespace",
        "configmap" | "configmaps" | "cm" => "ConfigMap",
        "secret" | "secrets" => "Secret",
        "persistentvolumeclaim" | "persistentvolumeclaims" | "pvc" => "PersistentVolumeClaim",
        "persistentvolume" | "persistentvolumes" | "pv" => "PersistentVolume",
        "ingress" | "ingresses" | "ing" => "Ingress",
        "horizontalpodautoscaler" | "horizontalpodautoscalers" | "hpa" => "HorizontalPodAutoscaler",
        other => {
            // Try to capitalize first letter
            let mut chars = other.chars();
            match chars.next() {
                None => return Err(anyhow::anyhow!("empty resource type")),
                Some(first) => {
                    let capitalized: String = first.to_uppercase().chain(chars).collect();
                    return Ok((capitalized, name));
                }
            }
        }
    };

    Ok((kind.to_string(), name))
}

fn get_event_time(event: &Value) -> String {
    // Prefer eventTime, then lastTimestamp, then firstTimestamp
    event
        .get("eventTime")
        .or_else(|| event.get("lastTimestamp"))
        .or_else(|| event.get("firstTimestamp"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string()
}

fn format_age(timestamp: &str) -> String {
    if timestamp.is_empty() {
        return "<unknown>".to_string();
    }

    let parsed = DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.with_timezone(&Utc))
        .ok();

    match parsed {
        Some(dt) => {
            let now = Utc::now();
            let duration = now.signed_duration_since(dt);
            let seconds = duration.num_seconds();
            if seconds < 0 {
                return "<future>".to_string();
            }
            if seconds < 60 {
                format!("{}s", seconds)
            } else if seconds < 3600 {
                format!("{}m", seconds / 60)
            } else if seconds < 86400 {
                format!("{}h", seconds / 3600)
            } else {
                format!("{}d", seconds / 86400)
            }
        }
        None => timestamp.to_string(),
    }
}

fn print_event_row(event: &Value, all_namespaces: bool) {
    let last_seen = get_event_time(event);
    let age = format_age(&last_seen);
    let event_type = event
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("Normal");
    let reason = event.get("reason").and_then(|r| r.as_str()).unwrap_or("");
    let message = event.get("message").and_then(|m| m.as_str()).unwrap_or("");
    let count = event.get("count").and_then(|c| c.as_i64()).unwrap_or(1);

    // Build object reference
    let involved = event.get("involvedObject");
    let obj_kind = involved
        .and_then(|o| o.get("kind"))
        .and_then(|k| k.as_str())
        .unwrap_or("");
    let obj_name = involved
        .and_then(|o| o.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("");
    let object = format!("{}/{}", obj_kind.to_lowercase(), obj_name);

    if all_namespaces {
        let ns = event
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        println!(
            "{:<12} {:<8} {:<6} {:<40} {:<20} {}",
            ns, age, event_type, reason, object, message
        );
    } else {
        println!(
            "{:<8} {:<8} {:<6} {:<40} {:<20} {}",
            age, event_type, count, reason, object, message
        );
    }
}

fn urlencoding(s: &str) -> String {
    s.replace('=', "%3D").replace(',', "%2C")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_for_object() {
        let (kind, name) = parse_for_object("pod/nginx").unwrap();
        assert_eq!(kind, "Pod");
        assert_eq!(name, "nginx");

        let (kind, name) = parse_for_object("deploy/myapp").unwrap();
        assert_eq!(kind, "Deployment");
        assert_eq!(name, "myapp");

        let (kind, name) = parse_for_object("node/worker-1").unwrap();
        assert_eq!(kind, "Node");
        assert_eq!(name, "worker-1");

        assert!(parse_for_object("justname").is_err());
    }

    #[test]
    fn test_parse_for_object_aliases() {
        // Test shorthand aliases
        let (kind, _) = parse_for_object("po/x").unwrap();
        assert_eq!(kind, "Pod");
        let (kind, _) = parse_for_object("svc/x").unwrap();
        assert_eq!(kind, "Service");
        let (kind, _) = parse_for_object("rs/x").unwrap();
        assert_eq!(kind, "ReplicaSet");
        let (kind, _) = parse_for_object("sts/x").unwrap();
        assert_eq!(kind, "StatefulSet");
        let (kind, _) = parse_for_object("ds/x").unwrap();
        assert_eq!(kind, "DaemonSet");
        let (kind, _) = parse_for_object("cm/x").unwrap();
        assert_eq!(kind, "ConfigMap");
        let (kind, _) = parse_for_object("pvc/x").unwrap();
        assert_eq!(kind, "PersistentVolumeClaim");
        let (kind, _) = parse_for_object("hpa/x").unwrap();
        assert_eq!(kind, "HorizontalPodAutoscaler");
    }

    #[test]
    fn test_parse_for_object_unknown_capitalizes() {
        let (kind, name) = parse_for_object("widget/foo").unwrap();
        assert_eq!(kind, "Widget");
        assert_eq!(name, "foo");
    }

    #[test]
    fn test_format_age() {
        assert_eq!(format_age(""), "<unknown>");
        let future = "2099-01-01T00:00:00Z";
        assert_eq!(format_age(future), "<future>");
    }

    #[test]
    fn test_urlencoding() {
        let encoded = urlencoding("involvedObject.kind=Pod,involvedObject.name=nginx");
        assert!(encoded.contains("%3D"));
        assert!(encoded.contains("%2C"));
    }

    #[test]
    fn test_event_query_url_construction() {
        // Test namespace-scoped path
        let namespace = "default";
        let ns_path = format!("/api/v1/namespaces/{}/events", namespace);
        assert_eq!(ns_path, "/api/v1/namespaces/default/events");

        // Test all-namespaces path
        let all_ns_path = "/api/v1/events".to_string();
        assert_eq!(all_ns_path, "/api/v1/events");

        // Test with --for filter
        let for_obj = "pod/nginx";
        let (kind, obj_name) = parse_for_object(for_obj).unwrap();
        let field_selector = format!(
            "involvedObject.kind={},involvedObject.name={}",
            kind, obj_name
        );
        let path = format!("{}?fieldSelector={}", ns_path, urlencoding(&field_selector));
        assert!(path.starts_with("/api/v1/namespaces/default/events?fieldSelector="));
        assert!(path.contains("involvedObject.kind"));
        assert!(path.contains("Pod"));
        assert!(path.contains("nginx"));
    }

    #[test]
    fn test_get_event_time_priority() {
        use serde_json::json;

        // eventTime takes priority
        let event = json!({
            "eventTime": "2024-01-01T00:00:00Z",
            "lastTimestamp": "2023-01-01T00:00:00Z",
            "firstTimestamp": "2022-01-01T00:00:00Z"
        });
        assert_eq!(get_event_time(&event), "2024-01-01T00:00:00Z");

        // Falls back to lastTimestamp
        let event = json!({
            "lastTimestamp": "2023-01-01T00:00:00Z",
            "firstTimestamp": "2022-01-01T00:00:00Z"
        });
        assert_eq!(get_event_time(&event), "2023-01-01T00:00:00Z");

        // Falls back to firstTimestamp
        let event = json!({"firstTimestamp": "2022-01-01T00:00:00Z"});
        assert_eq!(get_event_time(&event), "2022-01-01T00:00:00Z");

        // Returns empty string when nothing
        let event = json!({});
        assert_eq!(get_event_time(&event), "");
    }

    #[test]
    fn test_event_type_filtering() {
        use serde_json::json;

        let events = [
            json!({"type": "Normal", "reason": "Scheduled"}),
            json!({"type": "Warning", "reason": "BackOff"}),
            json!({"type": "Normal", "reason": "Pulled"}),
            json!({"type": "Warning", "reason": "Unhealthy"}),
        ];

        // Filter to Warning only
        let type_filters = ["Warning".to_string()];
        let filtered: Vec<&Value> = events
            .iter()
            .filter(|event| {
                let event_type = event
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("Normal");
                type_filters
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(event_type))
            })
            .collect();

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0]["reason"], "BackOff");
        assert_eq!(filtered[1]["reason"], "Unhealthy");
    }

    #[test]
    fn test_event_type_filter_case_insensitive() {
        use serde_json::json;

        let events = [
            json!({"type": "normal", "reason": "Started"}),
            json!({"type": "WARNING", "reason": "Failed"}),
        ];

        let type_filters = ["warning".to_string()];
        let filtered: Vec<&Value> = events
            .iter()
            .filter(|event| {
                let event_type = event
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("Normal");
                type_filters
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(event_type))
            })
            .collect();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["reason"], "Failed");
    }

    #[test]
    fn test_event_sorting_by_timestamp() {
        use serde_json::json;

        let events = [
            json!({"lastTimestamp": "2024-03-01T00:00:00Z", "reason": "third"}),
            json!({"lastTimestamp": "2024-01-01T00:00:00Z", "reason": "first"}),
            json!({"lastTimestamp": "2024-02-01T00:00:00Z", "reason": "second"}),
        ];

        let mut sorted: Vec<&Value> = events.iter().collect();
        sorted.sort_by(|a, b| {
            let time_a = get_event_time(a);
            let time_b = get_event_time(b);
            time_a.cmp(&time_b)
        });

        assert_eq!(sorted[0]["reason"], "first");
        assert_eq!(sorted[1]["reason"], "second");
        assert_eq!(sorted[2]["reason"], "third");
    }

    #[test]
    fn test_print_event_row_does_not_panic() {
        use serde_json::json;

        // Full event
        let event = json!({
            "type": "Normal",
            "reason": "Scheduled",
            "message": "Successfully assigned default/nginx to node-1",
            "count": 1,
            "lastTimestamp": "2024-01-01T00:00:00Z",
            "involvedObject": {
                "kind": "Pod",
                "name": "nginx"
            },
            "metadata": {
                "namespace": "default"
            }
        });
        // Should not panic for either namespace mode
        print_event_row(&event, false);
        print_event_row(&event, true);
    }

    #[test]
    fn test_print_event_row_minimal() {
        use serde_json::json;

        // Minimal event with missing fields
        let event = json!({});
        print_event_row(&event, false);
        print_event_row(&event, true);
    }

    #[test]
    fn test_format_age_recent_timestamps() {
        // Test with a timestamp from a few seconds ago
        let now = Utc::now();
        let recent = now - chrono::Duration::seconds(30);
        let ts = recent.to_rfc3339();
        let age = format_age(&ts);
        // Should be something like "30s" (could be 29s or 31s due to timing)
        assert!(age.ends_with('s'), "Expected seconds format, got: {}", age);

        // A few minutes ago
        let minutes_ago = now - chrono::Duration::minutes(5);
        let ts = minutes_ago.to_rfc3339();
        let age = format_age(&ts);
        assert!(age.ends_with('m'), "Expected minutes format, got: {}", age);

        // A few hours ago
        let hours_ago = now - chrono::Duration::hours(3);
        let ts = hours_ago.to_rfc3339();
        let age = format_age(&ts);
        assert!(age.ends_with('h'), "Expected hours format, got: {}", age);

        // Days ago
        let days_ago = now - chrono::Duration::days(7);
        let ts = days_ago.to_rfc3339();
        let age = format_age(&ts);
        assert!(age.ends_with('d'), "Expected days format, got: {}", age);
    }

    #[test]
    fn test_format_age_invalid_timestamp() {
        let age = format_age("not-a-timestamp");
        assert_eq!(age, "not-a-timestamp");
    }

    #[test]
    fn test_urlencoding_no_special_chars() {
        let encoded = urlencoding("simple.string");
        assert_eq!(encoded, "simple.string");
    }

    #[test]
    fn test_urlencoding_only_equals() {
        let encoded = urlencoding("key=value");
        assert_eq!(encoded, "key%3Dvalue");
    }

    #[test]
    fn test_urlencoding_only_commas() {
        let encoded = urlencoding("a,b,c");
        assert_eq!(encoded, "a%2Cb%2Cc");
    }

    #[test]
    fn test_get_event_time_with_null_values() {
        use serde_json::json;

        // When eventTime is null, get() returns Some(null), or_else is not called,
        // as_str() on null returns None, so result is ""
        let event = json!({"eventTime": null, "lastTimestamp": "2024-01-01T00:00:00Z"});
        assert_eq!(get_event_time(&event), "");

        // When eventTime key is absent, falls through to lastTimestamp
        let event = json!({"lastTimestamp": "2024-01-01T00:00:00Z"});
        assert_eq!(get_event_time(&event), "2024-01-01T00:00:00Z");
    }

    #[test]
    fn test_format_age_valid_rfc3339_variations() {
        // Test with timezone offset format
        let age = format_age("2020-01-01T00:00:00+00:00");
        assert!(age.ends_with('d'), "Expected days format, got: {}", age);
    }

    #[test]
    fn test_parse_for_object_empty_name() {
        // "pod/" gives an empty name
        let (kind, name) = parse_for_object("pod/").unwrap();
        assert_eq!(kind, "Pod");
        assert_eq!(name, "");
    }

    #[test]
    fn test_parse_for_object_name_with_slashes() {
        // Only splits on the first slash
        let result = parse_for_object("pod/my/complex/name");
        // split_once splits on first '/', so kind="pod", name="my/complex/name"
        assert!(result.is_ok());
        let (kind, name) = result.unwrap();
        assert_eq!(kind, "Pod");
        assert_eq!(name, "my/complex/name");
    }

    #[test]
    fn test_parse_for_object_more_types() {
        let (kind, name) = parse_for_object("cj/my-cron").unwrap();
        assert_eq!(kind, "CronJob");
        assert_eq!(name, "my-cron");

        let (kind, name) = parse_for_object("secret/my-secret").unwrap();
        assert_eq!(kind, "Secret");
        assert_eq!(name, "my-secret");

        let (kind, name) = parse_for_object("pv/my-volume").unwrap();
        assert_eq!(kind, "PersistentVolume");
        assert_eq!(name, "my-volume");

        let (kind, name) = parse_for_object("ing/my-ingress").unwrap();
        assert_eq!(kind, "Ingress");
        assert_eq!(name, "my-ingress");
    }
}
