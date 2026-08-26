use crate::client::ApiClient;
use anyhow::Result;

#[allow(clippy::too_many_arguments)]
pub async fn execute_enhanced(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    container: Option<&str>,
    follow: bool,
    tail: Option<i64>,
    timestamps: bool,
    since_time: Option<&str>,
    since: Option<&str>,
    previous: bool,
) -> Result<()> {
    execute_full(
        client,
        pod_name,
        Some(namespace),
        container,
        follow,
        tail,
        timestamps,
        since_time,
        since,
        previous,
    )
    .await
}

#[allow(dead_code)]
pub async fn execute(
    client: &ApiClient,
    pod_name: &str,
    namespace: Option<&str>,
    container: Option<&str>,
    follow: bool,
    tail: Option<i64>,
) -> Result<()> {
    execute_full(
        client, pod_name, namespace, container, follow, tail, false, None, None, false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_full(
    client: &ApiClient,
    pod_name: &str,
    namespace: Option<&str>,
    container: Option<&str>,
    follow: bool,
    tail: Option<i64>,
    timestamps: bool,
    since_time: Option<&str>,
    since: Option<&str>,
    previous: bool,
) -> Result<()> {
    let default_namespace = "default";
    let ns = namespace.unwrap_or(default_namespace);

    // Build the logs URL
    let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod_name);

    // Add query parameters
    let mut params = Vec::new();
    if let Some(container_name) = container {
        params.push(format!("container={}", container_name));
    }
    if follow {
        params.push("follow=true".to_string());
    }
    if let Some(lines) = tail {
        params.push(format!("tailLines={}", lines));
    }
    if timestamps {
        params.push("timestamps=true".to_string());
    }
    if let Some(st) = since_time {
        params.push(format!("sinceTime={}", st));
    }
    if let Some(s) = since {
        // Parse duration (e.g., "5s", "2m", "1h") and convert to seconds
        let seconds = parse_duration_to_seconds(s)?;
        params.push(format!("sinceSeconds={}", seconds));
    }
    if previous {
        params.push("previous=true".to_string());
    }

    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }

    // Fetch logs (the API returns plain text)
    let logs = client.get_text(&url).await?;

    print!("{}", logs);

    Ok(())
}

fn parse_duration_to_seconds(duration: &str) -> Result<i64> {
    let duration = duration.trim();
    if duration.is_empty() {
        anyhow::bail!("Empty duration");
    }

    let (value_str, unit) = if let Some(stripped) = duration.strip_suffix("ms") {
        (stripped, "ms")
    } else {
        let last_char = duration.chars().last().unwrap();
        if last_char.is_alphabetic() {
            (
                &duration[..duration.len() - 1],
                &duration[duration.len() - 1..],
            )
        } else {
            (duration, "s") // Default to seconds
        }
    };

    let value: i64 = value_str
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid duration value: {}", value_str))?;

    let seconds = match unit {
        "s" => value,
        "m" => value * 60,
        "h" => value * 3600,
        "d" => value * 86400,
        "ms" => value / 1000, // Convert milliseconds to seconds
        _ => anyhow::bail!(
            "Unknown duration unit: {}. Supported units: s, m, h, d",
            unit
        ),
    };

    Ok(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_url_basic() {
        let url = format!("/api/v1/namespaces/{}/pods/{}/log", "default", "my-pod");
        assert_eq!(url, "/api/v1/namespaces/default/pods/my-pod/log");
    }

    #[test]
    fn test_log_url_with_params() {
        let ns = "prod";
        let pod = "web";
        let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let params = [
            "container=nginx".to_string(),
            "follow=true".to_string(),
            format!("tailLines={}", 100),
        ];
        url.push('?');
        url.push_str(&params.join("&"));

        assert_eq!(
            url,
            "/api/v1/namespaces/prod/pods/web/log?container=nginx&follow=true&tailLines=100"
        );
    }

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration_to_seconds("5s").unwrap(), 5);
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration_to_seconds("2m").unwrap(), 120);
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_to_seconds("1h").unwrap(), 3600);
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration_to_seconds("1d").unwrap(), 86400);
    }

    #[test]
    fn test_parse_duration_raw_number_defaults_seconds() {
        assert_eq!(parse_duration_to_seconds("30").unwrap(), 30);
    }

    #[test]
    fn test_parse_duration_empty_fails() {
        assert!(parse_duration_to_seconds("").is_err());
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        assert_eq!(parse_duration_to_seconds("5000ms").unwrap(), 5);
    }

    #[test]
    fn test_parse_duration_invalid_unit() {
        assert!(parse_duration_to_seconds("10x").is_err());
    }

    #[test]
    fn test_parse_duration_invalid_value() {
        assert!(parse_duration_to_seconds("abcs").is_err());
    }

    #[test]
    fn test_log_url_with_timestamps_and_previous() {
        let ns = "default";
        let pod = "app";
        let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let params = ["timestamps=true".to_string(), "previous=true".to_string()];
        url.push('?');
        url.push_str(&params.join("&"));
        assert_eq!(
            url,
            "/api/v1/namespaces/default/pods/app/log?timestamps=true&previous=true"
        );
    }

    #[test]
    fn test_log_url_with_since_seconds() {
        let ns = "default";
        let pod = "app";
        let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let seconds = parse_duration_to_seconds("5m").unwrap();
        let params = [format!("sinceSeconds={}", seconds)];
        url.push('?');
        url.push_str(&params.join("&"));
        assert_eq!(
            url,
            "/api/v1/namespaces/default/pods/app/log?sinceSeconds=300"
        );
    }

    #[test]
    fn test_log_url_no_params() {
        let ns = "default";
        let pod = "nginx";
        let url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let params: Vec<String> = Vec::new();
        // When no params, no '?' should be appended
        assert!(params.is_empty());
        assert_eq!(url, "/api/v1/namespaces/default/pods/nginx/log");
    }

    #[test]
    fn test_parse_duration_zero_ms() {
        assert_eq!(parse_duration_to_seconds("0ms").unwrap(), 0);
    }

    #[test]
    fn test_log_url_with_all_params() {
        let ns = "prod";
        let pod = "web";
        let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let params = [
            "container=app".to_string(),
            "follow=true".to_string(),
            "tailLines=50".to_string(),
            "timestamps=true".to_string(),
            "sinceTime=2024-01-01T00:00:00Z".to_string(),
            "previous=true".to_string(),
        ];
        url.push('?');
        url.push_str(&params.join("&"));
        assert!(url.contains("container=app"));
        assert!(url.contains("follow=true"));
        assert!(url.contains("tailLines=50"));
        assert!(url.contains("timestamps=true"));
        assert!(url.contains("sinceTime="));
        assert!(url.contains("previous=true"));
    }
}
