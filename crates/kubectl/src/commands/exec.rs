use crate::websocket;
use anyhow::Result;
use rusternetes_client::http::ApiClient;

/// Execute a command in a container
pub async fn execute(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    container: Option<&str>,
    command: &[String],
    tty: bool,
    stdin: bool,
) -> Result<()> {
    // Build the exec URL path
    let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);

    // Add query parameters
    let mut query_params = vec![];

    for cmd in command {
        query_params.push(format!("command={}", urlencoding::encode(cmd)));
    }

    if let Some(cont) = container {
        query_params.push(format!("container={}", cont));
    }

    query_params.push("stdout=true".to_string());
    query_params.push("stderr=true".to_string());

    if stdin {
        query_params.push("stdin=true".to_string());
    }

    if tty {
        query_params.push("tty=true".to_string());
    }

    if !query_params.is_empty() {
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));
    }

    // Convert HTTP(S) URL to WebSocket URL
    let ws_url = client.get_ws_url(&url_path)?;

    // Execute with WebSocket streaming
    websocket::exec_stream(ws_url, stdin, tty).await
}

#[cfg(test)]
mod tests {
    use super::urlencoding;

    #[test]
    fn test_exec_url_construction_basic() {
        let namespace = "default";
        let pod_name = "my-pod";
        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);
        let mut query_params = vec![];
        query_params.push(format!("command={}", urlencoding::encode("ls")));
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert_eq!(
            url_path,
            "/api/v1/namespaces/default/pods/my-pod/exec?command=ls&stdout=true&stderr=true"
        );
    }

    #[test]
    fn test_exec_url_with_container_and_tty() {
        let namespace = "prod";
        let pod_name = "web";
        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);
        let mut query_params = vec![];
        query_params.push(format!("command={}", urlencoding::encode("/bin/sh")));
        query_params.push(format!("container={}", "main"));
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        query_params.push("stdin=true".to_string());
        query_params.push("tty=true".to_string());
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert!(url_path.contains("container=main"));
        assert!(url_path.contains("stdin=true"));
        assert!(url_path.contains("tty=true"));
        assert!(url_path.contains("command=%2Fbin%2Fsh"));
    }

    #[test]
    fn test_urlencoding_special_chars() {
        assert_eq!(urlencoding::encode("/bin/sh"), "%2Fbin%2Fsh");
        assert_eq!(urlencoding::encode("hello world"), "hello+world");
        assert_eq!(urlencoding::encode("simple"), "simple");
    }

    #[test]
    fn test_exec_url_multi_command_args() {
        let namespace = "default";
        let pod_name = "my-pod";
        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);
        let mut query_params = vec![];
        for cmd in &["sh", "-c", "echo hello"] {
            query_params.push(format!("command={}", urlencoding::encode(cmd)));
        }
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert!(url_path.contains("command=sh"));
        assert!(url_path.contains("command=-c"));
        assert!(url_path.contains("command=echo+hello"));
    }

    #[test]
    fn test_exec_url_no_stdin_no_tty() {
        let namespace = "default";
        let pod_name = "test-pod";
        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);
        let mut query_params = vec![];
        query_params.push(format!("command={}", urlencoding::encode("date")));
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        // stdin=false and tty=false means those params are not added
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert!(!url_path.contains("stdin=true"));
        assert!(!url_path.contains("tty=true"));
    }

    #[test]
    fn test_urlencoding_encode_preserves_alphanumeric() {
        let result = urlencoding::encode("abcABC123");
        assert_eq!(result, "abcABC123");
    }
}

mod urlencoding {
    pub fn encode(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
                ' ' => "+".to_string(),
                _ => format!("%{:02X}", c as u8),
            })
            .collect()
    }
}
