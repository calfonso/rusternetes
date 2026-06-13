use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use std::fs;
use std::path::Path;

/// Copy files between local filesystem and containers
pub async fn execute(
    client: &ApiClient,
    source: &str,
    destination: &str,
    namespace: &str,
    container: Option<&str>,
) -> Result<()> {
    // Parse source and destination to determine direction
    let (is_upload, pod_name, pod_path, local_path) = parse_copy_spec(source, destination)?;

    if is_upload {
        copy_to_pod(
            client,
            namespace,
            &pod_name,
            &pod_path,
            &local_path,
            container,
        )
        .await
    } else {
        copy_from_pod(
            client,
            namespace,
            &pod_name,
            &pod_path,
            &local_path,
            container,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_copy_spec_from_pod() {
        let (is_upload, pod, pod_path, local) =
            parse_copy_spec("my-pod:/var/log/app.log", "/tmp/app.log").unwrap();
        assert!(!is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/var/log/app.log");
        assert_eq!(local, "/tmp/app.log");
    }

    #[test]
    fn test_parse_copy_spec_to_pod() {
        let (is_upload, pod, pod_path, local) =
            parse_copy_spec("/tmp/config.yaml", "my-pod:/etc/config").unwrap();
        assert!(is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/etc/config");
        assert_eq!(local, "/tmp/config.yaml");
    }

    #[test]
    fn test_parse_copy_spec_both_local_fails() {
        let result = parse_copy_spec("/tmp/a", "/tmp/b");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_copy_spec_both_pod_fails() {
        let result = parse_copy_spec("pod1:/a", "pod2:/b");
        assert!(result.is_err());
    }

    // ===== Additional tests for untested functions =====

    #[test]
    fn test_parse_copy_spec_pod_with_namespace_colon() {
        // Pod name with path containing deeper directories
        let (is_upload, pod, pod_path, local) =
            parse_copy_spec("my-pod:/var/log/nested/deep/file.txt", "/tmp/out").unwrap();
        assert!(!is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/var/log/nested/deep/file.txt");
        assert_eq!(local, "/tmp/out");
    }

    #[test]
    fn test_parse_copy_spec_upload_relative_local() {
        let (is_upload, pod, pod_path, local) =
            parse_copy_spec("./local-file.txt", "my-pod:/tmp/dest").unwrap();
        assert!(is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/tmp/dest");
        assert_eq!(local, "./local-file.txt");
    }

    #[test]
    fn test_parse_copy_spec_root_path() {
        let (is_upload, pod, pod_path, local) = parse_copy_spec("my-pod:/", "/tmp/backup").unwrap();
        assert!(!is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/");
        assert_eq!(local, "/tmp/backup");
    }

    #[test]
    fn test_parse_copy_spec_empty_pod_path() {
        // Colon with empty path after it
        let (is_upload, pod, pod_path, _local) = parse_copy_spec("my-pod:", "/tmp/out").unwrap();
        assert!(!is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "");
    }

    #[test]
    fn test_create_tar_from_file_roundtrip() {
        use std::io::Write;
        // Create a temp file
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        {
            let mut f = fs::File::create(&file_path).unwrap();
            f.write_all(b"hello world").unwrap();
        }

        let tar_data = create_tar_from_file(file_path.to_str().unwrap()).unwrap();
        assert!(!tar_data.is_empty());

        // Verify tar contains the file
        let mut archive = tar::Archive::new(tar_data.as_slice());
        let entries: Vec<_> = archive.entries().unwrap().collect();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_create_tar_from_dir_roundtrip() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("subdir");
        fs::create_dir(&sub).unwrap();
        {
            let mut f = fs::File::create(sub.join("a.txt")).unwrap();
            f.write_all(b"aaa").unwrap();
        }
        {
            let mut f = fs::File::create(sub.join("b.txt")).unwrap();
            f.write_all(b"bbb").unwrap();
        }

        let tar_data = create_tar_from_dir(sub.to_str().unwrap()).unwrap();
        assert!(!tar_data.is_empty());

        // Verify tar contains both files
        let mut archive = tar::Archive::new(tar_data.as_slice());
        let entries: Vec<_> = archive.entries().unwrap().collect();
        // Should have at least the two files (may also have . directory entry)
        assert!(entries.len() >= 2);
    }

    #[test]
    fn test_extract_tar_to_local() {
        use std::io::Write;
        // Create a tar from a file
        let src_dir = tempfile::tempdir().unwrap();
        let file_path = src_dir.path().join("extract_me.txt");
        {
            let mut f = fs::File::create(&file_path).unwrap();
            f.write_all(b"extract this content").unwrap();
        }
        let tar_data = create_tar_from_file(file_path.to_str().unwrap()).unwrap();

        // Extract to a different directory
        let dest_dir = tempfile::tempdir().unwrap();
        extract_tar_to_local(dest_dir.path().to_str().unwrap(), &tar_data).unwrap();

        // Verify the extracted file exists
        let extracted = dest_dir.path().join("extract_me.txt");
        assert!(extracted.exists());
        let content = fs::read_to_string(&extracted).unwrap();
        assert_eq!(content, "extract this content");
    }

    #[test]
    fn test_urlencoding_basic() {
        assert_eq!(urlencoding::encode("tar"), "tar");
        assert_eq!(urlencoding::encode("-xf"), "-xf");
        assert_eq!(urlencoding::encode("-"), "-");
        assert_eq!(urlencoding::encode("hello world"), "hello+world");
    }

    #[test]
    fn test_urlencoding_special_chars() {
        assert_eq!(urlencoding::encode("/bin/sh"), "%2Fbin%2Fsh");
        assert_eq!(urlencoding::encode("a&b"), "a%26b");
        assert_eq!(urlencoding::encode(""), "");
        assert_eq!(urlencoding::encode("safe_name.txt"), "safe_name.txt");
        assert_eq!(urlencoding::encode("~tilde"), "~tilde");
    }

    #[test]
    fn test_create_tar_from_file_preserves_name() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("my-config.yaml");
        {
            let mut f = fs::File::create(&file_path).unwrap();
            f.write_all(b"key: value").unwrap();
        }

        let tar_data = create_tar_from_file(file_path.to_str().unwrap()).unwrap();
        let mut archive = tar::Archive::new(tar_data.as_slice());
        let entry = archive.entries().unwrap().next().unwrap().unwrap();
        let path = entry.path().unwrap();
        assert_eq!(path.to_str().unwrap(), "my-config.yaml");
    }

    // ===== 10 additional tests for untested functions =====

    fn make_test_client() -> ApiClient {
        ApiClient::new("http://127.0.0.1:1", true, None).unwrap()
    }

    #[tokio::test]
    async fn test_execute_upload_returns_err_for_nonexistent_local() {
        let client = make_test_client();
        let result = execute(
            &client,
            "/nonexistent/file.txt",
            "my-pod:/tmp/dest",
            "default",
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_download_returns_err_on_unreachable() {
        let client = make_test_client();
        let dest = tempfile::tempdir().unwrap();
        let result = execute(
            &client,
            "my-pod:/var/log/app.log",
            dest.path().to_str().unwrap(),
            "default",
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_copy_to_pod_nonexistent_local_path() {
        let client = make_test_client();
        let result = copy_to_pod(
            &client,
            "default",
            "my-pod",
            "/tmp",
            "/nonexistent/path/file.txt",
            None,
        )
        .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Local path not found"));
    }

    #[tokio::test]
    async fn test_copy_from_pod_returns_err_on_unreachable() {
        let client = make_test_client();
        let dest = tempfile::tempdir().unwrap();
        let result = copy_from_pod(
            &client,
            "default",
            "my-pod",
            "/var/log/app.log",
            dest.path().to_str().unwrap(),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_both_local_returns_err() {
        let client = make_test_client();
        let result = execute(&client, "/tmp/a", "/tmp/b", "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_both_pod_returns_err() {
        let client = make_test_client();
        let result = execute(&client, "pod1:/a", "pod2:/b", "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_with_container_returns_err_on_unreachable() {
        let client = make_test_client();
        let dest = tempfile::tempdir().unwrap();
        let result = execute(
            &client,
            "my-pod:/etc/config",
            dest.path().to_str().unwrap(),
            "default",
            Some("sidecar"),
        )
        .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_create_tar_from_file_nonexistent_returns_err() {
        let result = create_tar_from_file("/nonexistent/file.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_tar_from_dir_nonexistent_returns_err() {
        let result = create_tar_from_dir("/nonexistent/directory");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_tar_to_local_invalid_data_returns_err() {
        let dest = tempfile::tempdir().unwrap();
        let result = extract_tar_to_local(dest.path().to_str().unwrap(), b"not a tar");
        assert!(result.is_err());
    }
}

fn parse_copy_spec(source: &str, dest: &str) -> Result<(bool, String, String, String)> {
    // pod:path format
    if source.contains(':') && !dest.contains(':') {
        // Copy from pod to local
        let parts: Vec<&str> = source.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid source format: {}", source);
        }
        Ok((
            false,
            parts[0].to_string(),
            parts[1].to_string(),
            dest.to_string(),
        ))
    } else if dest.contains(':') && !source.contains(':') {
        // Copy from local to pod
        let parts: Vec<&str> = dest.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid destination format: {}", dest);
        }
        Ok((
            true,
            parts[0].to_string(),
            parts[1].to_string(),
            source.to_string(),
        ))
    } else {
        anyhow::bail!("One of source or destination must be in pod:path format");
    }
}

async fn copy_to_pod(
    client: &ApiClient,
    namespace: &str,
    pod_name: &str,
    pod_path: &str,
    local_path: &str,
    container: Option<&str>,
) -> Result<()> {
    // Check if local path exists
    let local_metadata =
        fs::metadata(local_path).context(format!("Local path not found: {}", local_path))?;

    // Create tar archive of local file/directory
    let tar_data = if local_metadata.is_dir() {
        create_tar_from_dir(local_path)?
    } else {
        create_tar_from_file(local_path)?
    };

    // Build exec command to extract tar in pod
    let extract_cmd = vec![
        "tar".to_string(),
        "-xf".to_string(),
        "-".to_string(),
        "-C".to_string(),
        pod_path.to_string(),
    ];

    // Execute tar extraction via exec API
    exec_with_stdin(
        client,
        namespace,
        pod_name,
        container,
        &extract_cmd,
        &tar_data,
    )
    .await?;

    println!("Copied {} to {}:{}", local_path, pod_name, pod_path);
    Ok(())
}

async fn copy_from_pod(
    client: &ApiClient,
    namespace: &str,
    pod_name: &str,
    pod_path: &str,
    local_path: &str,
    container: Option<&str>,
) -> Result<()> {
    // Build exec command to create tar in pod
    let tar_cmd = vec![
        "tar".to_string(),
        "-cf".to_string(),
        "-".to_string(),
        "-C".to_string(),
        "/".to_string(),
        pod_path.trim_start_matches('/').to_string(),
    ];

    // Execute tar creation and capture output
    let tar_data = exec_capture_output(client, namespace, pod_name, container, &tar_cmd).await?;

    // Extract tar to local filesystem
    extract_tar_to_local(local_path, &tar_data)?;

    println!("Copied {}:{} to {}", pod_name, pod_path, local_path);
    Ok(())
}

fn create_tar_from_file(path: &str) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut output);
        let file_path = Path::new(path);
        let file_name = file_path.file_name().context("Invalid file name")?;
        tar.append_path_with_name(path, file_name)?;
        tar.finish()?;
    }
    Ok(output)
}

fn create_tar_from_dir(path: &str) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut output);
        tar.append_dir_all(".", path)?;
        tar.finish()?;
    }
    Ok(output)
}

fn extract_tar_to_local(dest_path: &str, tar_data: &[u8]) -> Result<()> {
    let mut archive = tar::Archive::new(tar_data);
    archive.unpack(dest_path)?;
    Ok(())
}

async fn exec_with_stdin(
    client: &ApiClient,
    namespace: &str,
    pod_name: &str,
    container: Option<&str>,
    command: &[String],
    stdin_data: &[u8],
) -> Result<()> {
    use crate::websocket::{StreamChannel, StreamMessage};
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    // Build the exec URL with query parameters
    let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec?", namespace, pod_name);

    for cmd in command {
        url_path.push_str(&format!("command={}&", urlencoding::encode(cmd)));
    }

    if let Some(cont) = container {
        url_path.push_str(&format!("container={}&", cont));
    }

    url_path.push_str("stdin=true&stdout=true&stderr=true");

    // Get WebSocket URL
    let ws_url = client.get_ws_url(&url_path)?;
    let url = url::Url::parse(&ws_url)?;

    // Connect WebSocket
    let (ws_stream, _) = connect_async(url)
        .await
        .context("Failed to connect to exec WebSocket")?;

    let (mut write, mut read) = ws_stream.split();

    // Send stdin data
    let stdin_msg = StreamMessage::new(StreamChannel::Stdin, stdin_data.to_vec());
    write
        .send(Message::Binary(stdin_msg.encode()))
        .await
        .context("Failed to send stdin data")?;

    // Close stdin to signal end of input
    write.close().await.ok();

    // Read output (for error handling)
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if let Ok(stream_msg) = StreamMessage::decode(&data) {
                    match stream_msg.channel {
                        StreamChannel::Stdout
                            // Print any stdout for debugging
                            if !stream_msg.data.is_empty() => {
                                print!("{}", String::from_utf8_lossy(&stream_msg.data));
                            }
                        StreamChannel::Stderr
                            // Print stderr
                            if !stream_msg.data.is_empty() => {
                                eprint!("{}", String::from_utf8_lossy(&stream_msg.data));
                            }
                        StreamChannel::Error => {
                            return Err(anyhow::anyhow!(
                                "Exec error: {}",
                                String::from_utf8_lossy(&stream_msg.data)
                            ));
                        }
                        _ => {}
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => return Err(anyhow::anyhow!("WebSocket error: {}", e)),
            _ => {}
        }
    }

    Ok(())
}

async fn exec_capture_output(
    client: &ApiClient,
    namespace: &str,
    pod_name: &str,
    container: Option<&str>,
    command: &[String],
) -> Result<Vec<u8>> {
    use crate::websocket::{StreamChannel, StreamMessage};
    use futures::StreamExt;
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    // Build the exec URL with query parameters
    let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec?", namespace, pod_name);

    for cmd in command {
        url_path.push_str(&format!("command={}&", urlencoding::encode(cmd)));
    }

    if let Some(cont) = container {
        url_path.push_str(&format!("container={}&", cont));
    }

    url_path.push_str("stdout=true&stderr=true");

    // Get WebSocket URL
    let ws_url = client.get_ws_url(&url_path)?;
    let url = url::Url::parse(&ws_url)?;

    // Connect WebSocket
    let (ws_stream, _) = connect_async(url)
        .await
        .context("Failed to connect to exec WebSocket")?;

    let (_, mut read) = ws_stream.split();

    // Capture stdout data
    let mut output = Vec::new();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if let Ok(stream_msg) = StreamMessage::decode(&data) {
                    match stream_msg.channel {
                        StreamChannel::Stdout => {
                            // Append stdout data to output buffer
                            output.extend_from_slice(&stream_msg.data);
                        }
                        StreamChannel::Stderr => {
                            // Log stderr but don't include in output
                            eprintln!("{}", String::from_utf8_lossy(&stream_msg.data));
                        }
                        StreamChannel::Error => {
                            return Err(anyhow::anyhow!(
                                "Exec error: {}",
                                String::from_utf8_lossy(&stream_msg.data)
                            ));
                        }
                        _ => {}
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => return Err(anyhow::anyhow!("WebSocket error: {}", e)),
            _ => {}
        }
    }

    Ok(output)
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
