//! HTTP handlers for `/exec` and `/attach` — upgrade-proxy to the containerd-rs
//! CRI streaming server (upstream-faithful kubelet streaming).
//!
//! Upstream reference: `pkg/kubelet/server/server.go` routes
//! `/exec/{podNamespace}/{podID}/{containerName}` (+ `/{uid}/` variant).
//! `getExec` calls CRI `Exec` (returns a URL on the runtime streaming server)
//! then `proxyStream` = upgrade-aware proxy to that URL.
//!
//! This module replicates the same flow: resolve the container id from the CRI
//! `ListContainers` labels, call CRI `Exec`/`Attach` to get the runtime stream
//! URL, `rewrite_stream_url` it to the node-local streaming server, then
//! `proxy_upgrade(url, req)`.

use axum::{
    body::Body,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use rusternetes_common::resources::Pod;
use rusternetes_common::types::ObjectMeta;
use tracing::{info, warn};

/// Query parameters for exec/attach requests, mirroring upstream
/// `remotecommandserver.Options`.
#[derive(Debug, Clone, Default)]
pub struct ExecParams {
    /// Command to execute (repeatable: `command=ls&command=-la`).
    pub command: Vec<String>,
    /// Connect stdin.
    pub stdin: bool,
    /// Stream stdout.
    pub stdout: bool,
    /// Stream stderr.
    pub stderr: bool,
    /// Allocate a pseudo-TTY.
    pub tty: bool,
}

impl ExecParams {
    /// Parse exec/attach query params from a raw query string.
    ///
    /// Supports repeated `command=` keys (upstream passes each argv element as a
    /// separate `command=` value). Boolean params (`stdin`, `stdout`, `stderr`,
    /// `tty`) accept `"true"` / `"1"` (case-insensitive); anything else is false.
    pub fn from_query(query: &str) -> Self {
        let mut command = Vec::new();
        let mut stdin = false;
        let mut stdout = false;
        let mut stderr = false;
        let mut tty = false;

        for pair in query.split('&') {
            let (key, value) = if let Some(pos) = pair.find('=') {
                (&pair[..pos], &pair[pos + 1..])
            } else {
                (pair, "")
            };
            // Percent-decode simple `+` and `%XX` sequences for the value.
            let value = decode_simple(value);
            match key {
                "command" if !value.is_empty() => {
                    command.push(value);
                }
                "stdin" => stdin = is_true(&value),
                "stdout" => stdout = is_true(&value),
                "stderr" => stderr = is_true(&value),
                "tty" => tty = is_true(&value),
                _ => {}
            }
        }
        ExecParams {
            command,
            stdin,
            stdout,
            stderr,
            tty,
        }
    }
}

fn is_true(s: &str) -> bool {
    let l = s.to_ascii_lowercase();
    l == "true" || l == "1"
}

/// Minimal percent-decode: replace `+` with space and `%XX` hex sequences.
fn decode_simple(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b as char);
                    i += 3;
                    continue;
                }
            }
            out.push('%');
            i += 1;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Query parameters for `/containerLogs` requests, mirroring upstream
/// `v1.PodLogOptions` fields forwarded by the api-server.
#[derive(Debug, Clone, Default)]
pub struct LogParams {
    /// Only return the last N lines of container output.
    pub tail_lines: Option<i32>,
    /// Stop after this many bytes.
    pub limit_bytes: Option<i64>,
    /// Stay attached and stream new output as it is produced.
    pub follow: bool,
    /// Preserve the RFC3339Nano timestamp prefix on each emitted line.
    pub timestamps: bool,
    /// Only lines at or after this RFC3339 timestamp (resolved to Unix epoch).
    pub since_time: Option<String>,
}

impl LogParams {
    /// Parse `/containerLogs` query params from a raw query string.
    ///
    /// Integer fields (`tailLines`, `limitBytes`) are parsed with
    /// `str::parse`; silently ignored on parse failure. Booleans accept
    /// `"true"` / `"1"` (case-insensitive).
    pub fn from_query(query: &str) -> Self {
        let mut tail_lines: Option<i32> = None;
        let mut limit_bytes: Option<i64> = None;
        let mut follow = false;
        let mut timestamps = false;
        let mut since_time: Option<String> = None;

        for pair in query.split('&') {
            let (key, value) = if let Some(pos) = pair.find('=') {
                (&pair[..pos], &pair[pos + 1..])
            } else {
                (pair, "")
            };
            let value = decode_simple(value);
            match key {
                "tailLines" => tail_lines = value.parse().ok(),
                "limitBytes" => limit_bytes = value.parse().ok(),
                "follow" => follow = is_true(&value),
                "timestamps" => timestamps = is_true(&value),
                "sinceTime" if !value.is_empty() => {
                    since_time = Some(value);
                }
                _ => {}
            }
        }
        LogParams {
            tail_lines,
            limit_bytes,
            follow,
            timestamps,
            since_time,
        }
    }
}

/// Map [`LogParams`] to [`rusternetes_cri::stream::LogReadOptions`].
fn log_read_options(params: &LogParams) -> rusternetes_cri::stream::LogReadOptions {
    let since_unix = params.since_time.as_deref().and_then(|s| {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| t.timestamp())
    });
    rusternetes_cri::stream::LogReadOptions {
        timestamps: params.timestamps,
        tail_lines: params.tail_lines,
        limit_bytes: params.limit_bytes,
        since_unix,
    }
}

/// Path parameters for the exec/attach routes (3-segment variant: no uid).
#[derive(serde::Deserialize, Debug)]
pub struct ExecPath3 {
    pub namespace: String,
    pub pod: String,
    pub container: String,
}

/// Path parameters for the exec/attach routes (4-segment variant: with uid).
#[derive(serde::Deserialize, Debug)]
pub struct ExecPath4 {
    pub namespace: String,
    pub pod: String,
    pub uid: String,
    pub container: String,
}

/// Build a minimal [`Pod`] value sufficient for [`resolve_container_id`].
fn pod_for_resolve(namespace: &str, pod_name: &str, uid: &str) -> Pod {
    Pod {
        type_meta: rusternetes_common::types::TypeMeta {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        metadata: ObjectMeta {
            name: pod_name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uid.to_string(),
            ..Default::default()
        },
        spec: None,
        status: None,
    }
}

/// Resolve the container id and build the upgrade target URI, then proxy.
async fn exec_proxy(
    namespace: &str,
    pod_name: &str,
    uid: &str,
    container: &str,
    params: &ExecParams,
    req: axum::extract::Request,
) -> Response {
    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("exec_proxy: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(namespace, pod_name, uid);
    let container_id =
        match rusternetes_cri::stream::resolve_container_id(&mut cri, &pod, container).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn!(
                    "exec_proxy: container not found: ns={namespace} pod={pod_name} \
                     container={container}"
                );
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "container {container:?} not found in pod {pod_name:?} \
                         (namespace {namespace:?})"
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("exec_proxy: resolve_container_id failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    // Call CRI Exec → get the runtime stream URL.
    let stream_url = match cri
        .exec(rusternetes_cri::v1::ExecRequest {
            container_id: container_id.clone(),
            cmd: params.command.clone(),
            tty: params.tty,
            stdin: params.stdin,
            stdout: true,
            stderr: !params.tty,
        })
        .await
    {
        Ok(url) => url,
        Err(e) => {
            warn!("exec_proxy: CRI Exec failed for {container_id}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Rewrite the stream URL to the node-local streaming server port.
    let (host, port) = rusternetes_cri::stream::stream_target();
    let rewritten = match rusternetes_cri::stream::rewrite_stream_url(&stream_url, &host, port) {
        Ok(u) => u,
        Err(e) => {
            warn!("exec_proxy: rewrite_stream_url failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    info!(
        "exec_proxy: ns={namespace} pod={pod_name} container={container} \
         container_id={container_id} target={rewritten}"
    );

    let target: http::Uri = match rewritten.parse() {
        Ok(u) => u,
        Err(e) => {
            warn!("exec_proxy: failed to parse rewritten URI {rewritten:?}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    rusternetes_streamproxy::proxy_upgrade(target, req).await
}

/// Resolve the container id and proxy an attach request to the CRI stream.
async fn attach_proxy(
    namespace: &str,
    pod_name: &str,
    uid: &str,
    container: &str,
    params: &ExecParams,
    req: axum::extract::Request,
) -> Response {
    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("attach_proxy: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(namespace, pod_name, uid);
    let container_id =
        match rusternetes_cri::stream::resolve_container_id(&mut cri, &pod, container).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn!(
                    "attach_proxy: container not found: ns={namespace} pod={pod_name} \
                     container={container}"
                );
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "container {container:?} not found in pod {pod_name:?} \
                         (namespace {namespace:?})"
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("attach_proxy: resolve_container_id failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    // Call CRI Attach → get the runtime stream URL.
    let stream_url = match cri
        .attach(rusternetes_cri::v1::AttachRequest {
            container_id: container_id.clone(),
            stdin: params.stdin,
            tty: params.tty,
            stdout: true,
            stderr: !params.tty,
        })
        .await
    {
        Ok(url) => url,
        Err(e) => {
            warn!("attach_proxy: CRI Attach failed for {container_id}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let (host, port) = rusternetes_cri::stream::stream_target();
    let rewritten = match rusternetes_cri::stream::rewrite_stream_url(&stream_url, &host, port) {
        Ok(u) => u,
        Err(e) => {
            warn!("attach_proxy: rewrite_stream_url failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    info!(
        "attach_proxy: ns={namespace} pod={pod_name} container={container} \
         container_id={container_id} target={rewritten}"
    );

    let target: http::Uri = match rewritten.parse() {
        Ok(u) => u,
        Err(e) => {
            warn!("attach_proxy: failed to parse rewritten URI {rewritten:?}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    rusternetes_streamproxy::proxy_upgrade(target, req).await
}

// ---------------------------------------------------------------------------
// Axum route handlers
// ---------------------------------------------------------------------------

/// `POST /exec/:namespace/:pod/:container`
pub async fn handle_exec(Path(p): Path<ExecPath3>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    exec_proxy(&p.namespace, &p.pod, "", &p.container, &params, req).await
}

/// `POST /exec/:namespace/:pod/:uid/:container`
pub async fn handle_exec_uid(Path(p): Path<ExecPath4>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    exec_proxy(&p.namespace, &p.pod, &p.uid, &p.container, &params, req).await
}

/// `POST /attach/:namespace/:pod/:container`
pub async fn handle_attach(Path(p): Path<ExecPath3>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    attach_proxy(&p.namespace, &p.pod, "", &p.container, &params, req).await
}

/// `POST /attach/:namespace/:pod/:uid/:container`
pub async fn handle_attach_uid(Path(p): Path<ExecPath4>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    attach_proxy(&p.namespace, &p.pod, &p.uid, &p.container, &params, req).await
}

/// `GET /containerLogs/:namespace/:pod/:container`
///
/// Reads the container's CRI logfile directly from the local node filesystem.
/// When `follow=true` the response body is a tailing stream that stays open
/// and pushes new lines as they are written; otherwise the current log
/// contents are returned in one shot.
pub async fn handle_container_logs(
    Path(p): Path<ExecPath3>,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = LogParams::from_query(&query);

    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("handle_container_logs: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(&p.namespace, &p.pod, "");
    let container_id =
        match rusternetes_cri::stream::resolve_container_id(&mut cri, &pod, &p.container).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn!(
                    "handle_container_logs: container not found: ns={} pod={} container={}",
                    p.namespace, p.pod, p.container
                );
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "container {:?} not found in pod {:?} (namespace {:?})",
                        p.container, p.pod, p.namespace
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("handle_container_logs: resolve_container_id failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    let log_path =
        match rusternetes_cri::stream::resolve_log_path(&mut cri, &pod, &container_id).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                warn!("handle_container_logs: no log_path for container_id={container_id}");
                return (
                    StatusCode::NOT_FOUND,
                    "container log file not available".to_string(),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("handle_container_logs: resolve_log_path failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    info!(
        "handle_container_logs: ns={} pod={} container={} container_id={} log_path={} follow={}",
        p.namespace,
        p.pod,
        p.container,
        container_id,
        log_path.display(),
        params.follow
    );

    if params.follow {
        // Tailing stream: emit existing backlog then poll for new bytes.
        let stream = follow_log_stream(log_path, params);
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(Body::from_stream(stream))
            .unwrap();
    }

    // Non-follow: read current log contents and return in one shot.
    let opts = log_read_options(&params);
    match rusternetes_cri::stream::read_log_file(&log_path, &opts) {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(Body::from(bytes))
            .unwrap(),
        Err(e) => {
            warn!("handle_container_logs: read_log_file failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// Build a tailing `Stream` over the container log file.
///
/// Emits the existing backlog first (honoring `tail_lines`/`since_time`),
/// then polls for new bytes every 500ms — identical to the api-server
/// `stream_container_logs` approach. The stream ends when the log file
/// disappears (container removed) or after a bounded wait if the file
/// never appears (container exited without writing logs).
fn follow_log_stream(
    log_path: std::path::PathBuf,
    params: LogParams,
) -> impl futures::Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>> {
    use std::io::{Read, Seek, SeekFrom};

    let opts = log_read_options(&params);
    let timestamps = opts.timestamps;
    let since_unix = opts.since_unix;

    async_stream::stream! {
        // Emit existing backlog; a missing file is non-fatal (container may
        // not have written yet — fall through to the tail loop).
        match rusternetes_cri::stream::read_log_file(&log_path, &opts) {
            Ok(bytes) if !bytes.is_empty() => {
                yield Ok(bytes::Bytes::from(bytes));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(
                    "follow_log_stream: backlog read failed (may not exist yet): {e}"
                );
            }
        }

        let mut offset: u64 = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
        let mut carry: Vec<u8> = Vec::new();
        let mut seen_file = std::fs::metadata(&log_path).is_ok();
        let mut waited_ticks: u32 = 0;
        const MAX_PRE_FILE_TICKS: u32 = 240; // 240 × 500ms = 120s

        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let len = match std::fs::metadata(&log_path) {
                Ok(m) => {
                    seen_file = true;
                    m.len()
                }
                Err(_) if !seen_file => {
                    waited_ticks += 1;
                    if waited_ticks >= MAX_PRE_FILE_TICKS {
                        break;
                    }
                    continue;
                }
                Err(_) => break, // file vanished — container removed
            };
            if len < offset {
                // Truncated / rotated — restart from the beginning.
                offset = 0;
                carry.clear();
            }
            if len == offset {
                continue;
            }
            let mut f = match std::fs::File::open(&log_path) {
                Ok(f) => f,
                Err(_) => break,
            };
            if f.seek(SeekFrom::Start(offset)).is_err() {
                break;
            }
            let mut buf = Vec::new();
            if f.read_to_end(&mut buf).is_err() {
                break;
            }
            offset = len;
            carry.extend_from_slice(&buf);

            // Only process complete lines; keep any partial tail.
            let last_nl = carry.iter().rposition(|&b| b == b'\n');
            let Some(idx) = last_nl else { continue };
            let complete: Vec<u8> = carry.drain(..=idx).collect();

            let mut out = Vec::new();
            for line in complete.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let parsed = rusternetes_cri::stream::parse_cri_log_line(line);
                if let Some(since) = since_unix {
                    if let Some(ts) = parsed.timestamp_unix {
                        if ts < since {
                            continue;
                        }
                    }
                }
                if timestamps && !parsed.timestamp_prefix.is_empty() {
                    out.extend_from_slice(parsed.timestamp_prefix.as_bytes());
                }
                out.extend_from_slice(&parsed.message);
            }
            if !out.is_empty() {
                yield Ok(bytes::Bytes::from(out));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_params_parse_tail_and_follow() {
        let p = LogParams::from_query("tailLines=10&follow=true&timestamps=false&limitBytes=2048");
        assert_eq!(p.tail_lines, Some(10));
        assert!(p.follow);
        assert_eq!(p.limit_bytes, Some(2048));
    }

    #[test]
    fn log_params_empty_query() {
        let p = LogParams::from_query("");
        assert_eq!(p.tail_lines, None);
        assert!(!p.follow);
        assert!(!p.timestamps);
        assert_eq!(p.limit_bytes, None);
        assert!(p.since_time.is_none());
    }

    #[test]
    fn log_params_timestamps_and_since_time() {
        let p = LogParams::from_query(
            "timestamps=true&sinceTime=2024-01-01T00%3A00%3A00Z&follow=false",
        );
        assert!(p.timestamps);
        assert!(!p.follow);
        assert!(p.since_time.is_some());
    }

    #[test]
    fn exec_params_parse_command_and_tty() {
        let q = "command=ls&command=-la&tty=true&stdin=false&stdout=true&stderr=true";
        let p = ExecParams::from_query(q);
        assert_eq!(p.command, vec!["ls", "-la"]);
        assert!(p.tty);
        assert!(!p.stdin && p.stdout && p.stderr);
    }

    #[test]
    fn exec_params_empty_query() {
        let p = ExecParams::from_query("");
        assert!(p.command.is_empty());
        assert!(!p.stdin && !p.stdout && !p.stderr && !p.tty);
    }

    #[test]
    fn exec_params_single_command() {
        let p = ExecParams::from_query("command=echo&stdin=1&stdout=1");
        assert_eq!(p.command, vec!["echo"]);
        assert!(p.stdin);
        assert!(p.stdout);
        assert!(!p.stderr);
    }

    #[test]
    fn exec_params_tty_suppresses_stderr() {
        // When tty=true, the caller sets stdin+stdout; stderr is irrelevant
        // (CRI forbids tty && stderr) but we parse what we get.
        let p = ExecParams::from_query("command=bash&tty=true&stdin=true&stdout=true&stderr=false");
        assert!(p.tty);
        assert!(p.stdin);
        assert!(p.stdout);
        assert!(!p.stderr);
    }
}
