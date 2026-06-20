//! CRI-backed exec + log helpers shared by the pod exec/logs subresource
//! handlers (HTTP one-shot), the WebSocket exec path, and the SPDY exec path.
//!
//! The api-server used to talk to the container runtime directly over the
//! Docker API for exec and pod logs. The kubelet has since moved to a CRI v1
//! backend, so the api-server speaks the same Container Runtime Interface here:
//!
//! * exec runs via the CRI `ExecSync` RPC (one-shot: collect stdout/stderr +
//!   exit code), matching the kubelet's own `/exec` endpoint.
//! * pod logs are read from the CRI container **log file** (the path the
//!   runtime reports in `ContainerStatus.log_path`) and the CRI log line format
//!   is parsed into the raw message bytes the API returns.
//!
//! The CRI runtime endpoint comes from `CONTAINER_RUNTIME_ENDPOINT`
//! (default `unix:///run/containerd/containerd.sock`), the same env var the
//! kubelet uses.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rusternetes_common::resources::Pod;
use rusternetes_cri::{v1, CriClient};

/// CRI pod/container labels the kubelet stamps on every sandbox/container.
/// Kept in sync with `rusternetes_kubelet::cri_runtime::translate::labels`.
pub mod labels {
    pub const POD_NAME: &str = "io.kubernetes.pod.name";
    pub const POD_NAMESPACE: &str = "io.kubernetes.pod.namespace";
    pub const POD_UID: &str = "io.kubernetes.pod.uid";
    pub const CONTAINER_NAME: &str = "io.kubernetes.container.name";
}

/// The CRI runtime endpoint, from `CONTAINER_RUNTIME_ENDPOINT`
/// (default `unix:///run/containerd/containerd.sock`).
pub fn runtime_endpoint() -> String {
    std::env::var("CONTAINER_RUNTIME_ENDPOINT")
        .unwrap_or_else(|_| "unix:///run/containerd/containerd.sock".to_string())
}

/// Connect a fresh CRI client to the configured runtime endpoint.
pub async fn connect() -> anyhow::Result<CriClient> {
    let socket = runtime_endpoint();
    CriClient::connect(&socket)
        .await
        .map_err(|e| anyhow::anyhow!("failed to connect to CRI runtime at {socket}: {e}"))
}

/// Rewrite a CRI streaming URL's host:port to a network-reachable target.
///
/// containerd returns exec/attach/port-forward URLs whose host is its configured
/// `stream_server_address` — we bind `0.0.0.0:10010` (see
/// `deploy/containerd/config.toml`). `0.0.0.0` is not dialable from the
/// api-server container, so the streaming proxy rewrites the host to the
/// `containerd` compose service before connecting, preserving the runtime-issued
/// path + token query. Building block for the interactive streaming-exec proxy
/// (#1173 follow-up); the one-shot `ExecSync` path does not use it.
pub fn rewrite_stream_url(stream_url: &str, host: &str, port: u16) -> anyhow::Result<String> {
    let mut u = url::Url::parse(stream_url)
        .map_err(|e| anyhow::anyhow!("parsing CRI stream URL {stream_url:?}: {e}"))?;
    u.set_host(Some(host))
        .map_err(|e| anyhow::anyhow!("setting stream host {host:?}: {e}"))?;
    u.set_port(Some(port))
        .map_err(|()| anyhow::anyhow!("setting stream port {port}"))?;
    Ok(u.into())
}

/// A WebSocket stream to the CRI runtime's streaming server, carrying the
/// channel-framed `remotecommand` protocol (byte 0 = channel).
pub type CriStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Host:port the api-server dials to reach containerd's CRI streaming server.
///
/// containerd advertises exec/attach URLs with its bind address (`0.0.0.0` /
/// `[::]`), which is not routable from the api-server container, so the proxy
/// rewrites the host to this reachable target. Defaults to the `containerd`
/// compose service on the fixed stream port (`deploy/containerd/config.toml`);
/// override with `CONTAINERD_STREAM_HOST` / `CONTAINERD_STREAM_PORT`.
pub fn stream_target() -> (String, u16) {
    let host = std::env::var("CONTAINERD_STREAM_HOST").unwrap_or_else(|_| "containerd".to_string());
    let port = std::env::var("CONTAINERD_STREAM_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(10010);
    (host, port)
}

/// Open an interactive exec WebSocket to a container over CRI.
///
/// Calls the streaming `Exec` RPC, rewrites the runtime-advertised URL host to
/// the reachable [`stream_target`], and opens a WebSocket carrying the
/// `v5.channel.k8s.io` subprotocol. The returned stream speaks the same
/// channel-framed protocol the api-server exposes to kubectl, so the exec
/// handler proxies frames straight through. (Interactive streaming exec, #1256.)
pub async fn open_exec_stream(
    cri: &mut CriClient,
    container_id: &str,
    cmd: &[String],
    tty: bool,
    stdin: bool,
) -> anyhow::Result<CriStream> {
    // CRI forbids tty && stderr both set; with a TTY stdout+stderr are merged.
    let url = cri
        .exec(v1::ExecRequest {
            container_id: container_id.to_string(),
            cmd: cmd.to_vec(),
            tty,
            stdin,
            stdout: true,
            stderr: !tty,
        })
        .await
        .map_err(|e| anyhow::anyhow!("CRI Exec for {container_id}: {e}"))?;

    let (host, port) = stream_target();
    let rewritten = rewrite_stream_url(&url, &host, port)?;
    // tungstenite needs a ws:// scheme; the runtime hands back http(s)://.
    let ws_url = if let Some(rest) = rewritten.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = rewritten.strip_prefix("https://") {
        format!("wss://{rest}")
    } else {
        rewritten
    };

    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = ws_url
        .as_str()
        .into_client_request()
        .map_err(|e| anyhow::anyhow!("building WS request for {ws_url:?}: {e}"))?;
    // Offer the full channel-protocol ladder; the runtime's stream server picks
    // the highest it supports. Offering only v5 makes containerd's WebSocket
    // handshake reject with 403 when it negotiates an older revision.
    req.headers_mut().insert(
        "sec-websocket-protocol",
        tokio_tungstenite::tungstenite::http::HeaderValue::from_static(
            "v5.channel.k8s.io, v4.channel.k8s.io, v3.channel.k8s.io, v2.channel.k8s.io, channel.k8s.io",
        ),
    );
    let (stream, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(|e| anyhow::anyhow!("connecting to CRI stream {ws_url:?}: {e}"))?;
    Ok(stream)
}

/// Resolve the CRI container id for a pod's named container.
///
/// Filters `ListContainers` by the pod uid + container name labels the kubelet
/// stamps (`io.kubernetes.pod.uid` / `io.kubernetes.container.name`). If the pod
/// has no uid yet, falls back to namespace + name + container labels. Returns
/// the most-recently-created matching container's id, or `None` if no container
/// for that name exists on the runtime.
pub async fn resolve_container_id(
    cri: &mut CriClient,
    pod: &Pod,
    container_name: &str,
) -> anyhow::Result<Option<String>> {
    let mut label_selector = HashMap::new();
    label_selector.insert(
        labels::CONTAINER_NAME.to_string(),
        container_name.to_string(),
    );
    if !pod.metadata.uid.is_empty() {
        label_selector.insert(labels::POD_UID.to_string(), pod.metadata.uid.clone());
    } else {
        label_selector.insert(labels::POD_NAME.to_string(), pod.metadata.name.clone());
        if let Some(ns) = pod.metadata.namespace.as_deref() {
            label_selector.insert(labels::POD_NAMESPACE.to_string(), ns.to_string());
        }
    }
    let filter = v1::ContainerFilter {
        label_selector,
        ..Default::default()
    };
    let mut containers = cri.list_containers(Some(filter)).await?;
    // Prefer the most recently created container so a restarted container's
    // current instance wins over an older exited one.
    containers.sort_by_key(|c| std::cmp::Reverse(c.created_at));
    Ok(containers.into_iter().next().map(|c| c.id))
}

/// Run a command in a container one-shot via CRI `ExecSync`, returning
/// `(stdout, stderr, exit_code)`. `timeout_secs` is the ExecSync timeout
/// (0 = no timeout).
pub async fn exec_sync(
    cri: &mut CriClient,
    container_id: &str,
    command: &[String],
    timeout_secs: i64,
) -> anyhow::Result<(Vec<u8>, Vec<u8>, i32)> {
    let cmd: Vec<&str> = command.iter().map(String::as_str).collect();
    let resp = cri.exec_sync(container_id, &cmd, timeout_secs).await?;
    Ok((resp.stdout, resp.stderr, resp.exit_code))
}

/// Options for reading container logs, mirroring the K8s log subresource query.
#[derive(Debug, Default, Clone)]
pub struct LogReadOptions {
    /// Keep the leading RFC3339Nano timestamp on each emitted line.
    pub timestamps: bool,
    /// Only the last N lines.
    pub tail_lines: Option<i32>,
    /// Stop after this many bytes of emitted output.
    pub limit_bytes: Option<i64>,
    /// Only lines whose timestamp is at/after this absolute Unix epoch second.
    pub since_unix: Option<i64>,
}

/// Resolve the on-disk path of a container's CRI log file.
///
/// CRI runtimes report `ContainerStatus.log_path`. Per the CRI contract that
/// path is relative to the owning sandbox's `log_directory`; containerd usually
/// returns it already absolute, so:
///
/// * absolute `log_path` → used as-is.
/// * relative `log_path` → joined under the pod log directory, derived as
///   `<CONTAINER_LOG_ROOT>/<namespace>_<name>_<uid>` (`CONTAINER_LOG_ROOT`
///   defaults to `/var/log/pods`, matching the kubelet's `pod-logs` layout).
///
/// `None` if the runtime reports no log path.
pub async fn resolve_log_path(
    cri: &mut CriClient,
    pod: &Pod,
    container_id: &str,
) -> anyhow::Result<Option<PathBuf>> {
    let status = cri.container_status(container_id, false).await?;
    let Some(log_path) = status.status.map(|s| s.log_path) else {
        return Ok(None);
    };
    if log_path.is_empty() {
        return Ok(None);
    }
    let p = Path::new(&log_path);
    if p.is_absolute() {
        return Ok(Some(p.to_path_buf()));
    }
    // Relative — join under the pod's log directory.
    let log_root =
        std::env::var("CONTAINER_LOG_ROOT").unwrap_or_else(|_| "/var/log/pods".to_string());
    let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
    let pod_dir = format!("{ns}_{}_{}", pod.metadata.name, pod.metadata.uid);
    Ok(Some(Path::new(&log_root).join(pod_dir).join(p)))
}

/// One parsed CRI log line: the RFC3339Nano timestamp prefix (with trailing
/// space) and the message body (without the `<stream> <P|F> ` middle fields).
pub struct CriLogLine {
    /// `<timestamp> ` including the trailing space, or empty if unparseable.
    pub timestamp_prefix: String,
    /// Timestamp as Unix epoch seconds, if parseable (for `since` filtering).
    pub timestamp_unix: Option<i64>,
    /// The message bytes, with the trailing newline included.
    pub message: Vec<u8>,
}

/// Parse one raw CRI log line into `(timestamp_prefix, message)`.
///
/// CRI log format (per line):
///   `<RFC3339Nano-timestamp> <stdout|stderr> <P|F> <message>`
///
/// `P` = partial line (no newline yet), `F` = full line. We strip the first
/// three space-separated fields and keep the message. A trailing `\n` is
/// re-appended for full lines so consumers see line-delimited output.
pub fn parse_cri_log_line(line: &[u8]) -> CriLogLine {
    // Split off the timestamp (field 1).
    let s = String::from_utf8_lossy(line);
    let mut parts = s.splitn(4, ' ');
    let ts = parts.next().unwrap_or("");
    let _stream = parts.next();
    let tag = parts.next();
    let msg = parts.next();

    // Only treat as a CRI-formatted line if the tag field is P or F and we
    // actually parsed a timestamp. Otherwise emit the raw line unchanged.
    let is_cri =
        matches!(tag, Some("P") | Some("F")) && chrono::DateTime::parse_from_rfc3339(ts).is_ok();

    if !is_cri {
        return CriLogLine {
            timestamp_prefix: String::new(),
            timestamp_unix: chrono::DateTime::parse_from_rfc3339(ts)
                .ok()
                .map(|t| t.timestamp()),
            message: line.to_vec(),
        };
    }

    let timestamp_unix = chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|t| t.timestamp());
    let full = tag == Some("F");
    let mut message = msg.unwrap_or("").as_bytes().to_vec();
    if full {
        message.push(b'\n');
    }
    CriLogLine {
        timestamp_prefix: format!("{ts} "),
        timestamp_unix,
        message,
    }
}

/// Read and parse a container's CRI log file into the bytes the API returns.
///
/// Honors `timestamps`, `tail_lines`, `limit_bytes`, and `since_unix`. When
/// `timestamps` is set the RFC3339Nano prefix is preserved; otherwise only the
/// message body is emitted (the CRI `<stream> <P|F>` middle fields are always
/// stripped).
pub fn read_log_file(path: &Path, opts: &LogReadOptions) -> anyhow::Result<Vec<u8>> {
    let raw = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("reading container log {}: {e}", path.display()))?;

    // Split into lines (keep them so tail counts logical lines).
    let mut lines: Vec<&[u8]> = raw.split(|&b| b == b'\n').collect();
    // `split` yields a trailing empty slice when the file ends in '\n'; drop it.
    if lines.last().is_some_and(|l| l.is_empty()) {
        lines.pop();
    }

    if let Some(tail) = opts.tail_lines {
        if tail >= 0 {
            let tail = tail as usize;
            if lines.len() > tail {
                lines.drain(0..lines.len() - tail);
            }
        }
    }

    let mut out = Vec::new();
    let limit = opts.limit_bytes.map(|l| l.max(0) as usize);
    for line in lines {
        let parsed = parse_cri_log_line(line);
        if let Some(since) = opts.since_unix {
            if let Some(ts) = parsed.timestamp_unix {
                if ts < since {
                    continue;
                }
            }
        }
        if opts.timestamps && !parsed.timestamp_prefix.is_empty() {
            out.extend_from_slice(parsed.timestamp_prefix.as_bytes());
        }
        out.extend_from_slice(&parsed.message);
        if let Some(limit) = limit {
            if out.len() >= limit {
                out.truncate(limit);
                break;
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_stream_url_replaces_host_port_keeping_path_and_query() {
        let got = rewrite_stream_url(
            "http://0.0.0.0:10010/exec/abc123?command=ls&output=1",
            "containerd",
            10010,
        )
        .unwrap();
        assert_eq!(
            got,
            "http://containerd:10010/exec/abc123?command=ls&output=1"
        );
    }

    #[test]
    fn rewrite_stream_url_overrides_any_original_host() {
        let got =
            rewrite_stream_url("http://127.0.0.1:34567/attach/tok", "containerd", 10010).unwrap();
        assert_eq!(got, "http://containerd:10010/attach/tok");
    }

    #[test]
    fn rewrite_stream_url_rejects_garbage() {
        assert!(rewrite_stream_url("not a url", "containerd", 10010).is_err());
    }

    #[test]
    fn parses_full_line_strips_stream_and_tag() {
        let line = b"2024-01-01T00:00:00.000000000Z stdout F hello world";
        let parsed = parse_cri_log_line(line);
        assert_eq!(parsed.message, b"hello world\n");
        assert_eq!(parsed.timestamp_prefix, "2024-01-01T00:00:00.000000000Z ");
    }

    #[test]
    fn parses_partial_line_no_newline() {
        let line = b"2024-01-01T00:00:00.000000000Z stdout P partial";
        let parsed = parse_cri_log_line(line);
        assert_eq!(parsed.message, b"partial");
    }

    #[test]
    fn non_cri_line_passes_through() {
        let line = b"just a raw line";
        let parsed = parse_cri_log_line(line);
        assert_eq!(parsed.message, b"just a raw line");
        assert!(parsed.timestamp_prefix.is_empty());
    }

    #[test]
    fn read_log_file_strips_timestamps_by_default() {
        let dir = std::env::temp_dir().join(format!("cri-log-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.log");
        std::fs::write(
            &path,
            "2024-01-01T00:00:00.000000000Z stdout F line one\n\
             2024-01-01T00:00:01.000000000Z stderr F line two\n",
        )
        .unwrap();

        let out = read_log_file(&path, &LogReadOptions::default()).unwrap();
        assert_eq!(out, b"line one\nline two\n");

        let out_ts = read_log_file(
            &path,
            &LogReadOptions {
                timestamps: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            out_ts,
            b"2024-01-01T00:00:00.000000000Z line one\n\
              2024-01-01T00:00:01.000000000Z line two\n"
        );

        let out_tail = read_log_file(
            &path,
            &LogReadOptions {
                tail_lines: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(out_tail, b"line two\n");

        std::fs::remove_dir_all(&dir).ok();
    }
}
