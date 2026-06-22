//! Pod subresource handlers
//!
//! Implements pod subresources required for Kubernetes conformance:
//! - /logs - Get container logs
//! - /exec - Execute commands in containers (SPDY and WebSocket)
//! - /attach - Attach to running containers (SPDY and WebSocket)
//! - /portforward - Forward ports to pods (SPDY and WebSocket)

use crate::{middleware::AuthContext, spdy, spdy_handlers, state::ApiServerState, streaming};
use axum::{
    body::Body,
    extract::{ws::WebSocketUpgrade, Path, Query, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    Error, Result,
};
use rusternetes_storage::Storage;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, info};

/// Simple percent-decoding for URL query parameters
fn percent_decode_str(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().unwrap_or(b'0');
            let lo = chars.next().unwrap_or(b'0');
            let hex = [hi, lo];
            if let Ok(s) = std::str::from_utf8(&hex) {
                if let Ok(val) = u8::from_str_radix(s, 16) {
                    result.push(val as char);
                    continue;
                }
            }
            result.push('%');
            result.push(hi as char);
            result.push(lo as char);
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
}

#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    /// The container for which to stream logs
    #[serde(default)]
    pub container: Option<String>,
    /// Follow the log stream
    #[serde(default)]
    pub follow: bool,
    /// Return previous terminated container logs
    #[serde(default)]
    #[allow(dead_code)]
    pub previous: bool,
    /// Show timestamps
    #[serde(default)]
    pub timestamps: bool,
    /// If set, the number of lines from the end of the logs to show
    #[serde(rename = "tailLines")]
    pub tail_lines: Option<i32>,
    /// If set, the number of bytes to read from the server before terminating
    #[serde(rename = "limitBytes")]
    pub limit_bytes: Option<i64>,
    /// Relative time in seconds before the current time from which to show logs
    #[serde(rename = "sinceSeconds")]
    pub since_seconds: Option<i64>,
    /// RFC3339 timestamp from which to show logs
    #[serde(rename = "sinceTime")]
    pub since_time: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExecQuery {
    /// Container in which to execute the command
    pub container: Option<String>,
    /// Command to execute
    pub command: Vec<String>,
    /// Redirect stdin
    #[serde(default)]
    pub stdin: bool,
    /// Redirect stdout
    #[serde(default)]
    pub stdout: bool,
    /// Redirect stderr
    #[serde(default)]
    pub stderr: bool,
    /// Use TTY
    #[serde(default)]
    pub tty: bool,
}

#[derive(Debug, Deserialize)]
pub struct AttachQuery {
    /// Container to attach to
    pub container: Option<String>,
    /// Redirect stdin
    #[serde(default)]
    pub stdin: bool,
    /// Redirect stdout
    #[serde(default)]
    pub stdout: bool,
    /// Redirect stderr
    #[serde(default)]
    pub stderr: bool,
    /// Use TTY
    #[serde(default)]
    pub tty: bool,
}

#[derive(Debug, Deserialize)]
pub struct PortForwardQuery {
    /// Ports to forward
    pub ports: Option<String>,
}

/// GET /api/v1/namespaces/{namespace}/pods/{name}/log
/// Stream logs from a pod (supports both HTTP and WebSocket)
pub async fn get_logs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<LogsQuery>,
    ws: Option<WebSocketUpgrade>,
) -> Result<Response> {
    debug!("Getting logs for pod {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("log");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the pod to verify it exists and get container information
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Determine which container to get logs from
    let container_name = if let Some(ref container) = query.container {
        container.clone()
    } else {
        // If no container specified, use the first container
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Verify the container exists in the pod (check containers, init containers, ephemeral containers)
    let container_exists = pod
        .spec
        .as_ref()
        .map(|spec| {
            spec.containers.iter().any(|c| c.name == container_name)
                || spec
                    .init_containers
                    .as_ref()
                    .map(|ics| ics.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
                || spec
                    .ephemeral_containers
                    .as_ref()
                    .map(|ecs| ecs.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
        })
        .unwrap_or(false);

    if !container_exists {
        return Err(Error::NotFound(format!(
            "Container {} not found in pod {}/{}",
            container_name, namespace, name
        )));
    }

    // follow=true: stream incrementally for as long as the container produces
    // output. Without this, hydrophone (and `kubectl logs -f`) hit EOF on the
    // first read and reconnect in a tight ~1s loop (>2000 reconnects observed
    // during a single 17-min conformance run). The streaming path only kicks
    // in for the plain HTTP case; the WS branch below still uses the buffered
    // payload because the upstream pod-log WS conformance test reads once and
    // compares against a fixed byte slice.
    if query.follow && ws.is_none() {
        match stream_container_logs(&pod, &container_name, &query).await {
            Ok(stream) => {
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .body(Body::from_stream(stream))
                    .unwrap());
            }
            Err(e) => {
                info!(
                    "Failed to open follow log stream, falling back to buffered: {}",
                    e
                );
                // fall through to the buffered path below
            }
        }
    }

    // Get logs from the container runtime. Never fabricate output: a failure to
    // read the real container logs must surface as an error so clients (and the
    // e2e framework, which retries on error) do not receive synthetic content
    // masquerading as the container's stdout/stderr.
    let logs = get_container_logs(&pod, &container_name, &query)
        .await
        .map_err(|e| {
            tracing::warn!(
                "Failed to get container logs for {}/{}: {}",
                pod.metadata.name,
                container_name,
                e
            );
            Error::Internal(format!("unable to retrieve container logs: {e}"))
        })?;

    // If WebSocket upgrade requested, send logs as the upstream Kubernetes
    // log-subresource websocket subprotocols expect.
    //
    // Logs are a single read-only byte stream, so the canonical subprotocols
    // are `binary.k8s.io` (raw bytes — the exact bytes written to the stream,
    // no channel byte prefix) and `base64.binary.k8s.io` (base64-encoded raw).
    // The multi-stream `channel.k8s.io` / `v4.channel.k8s.io` /
    // `v5.channel.k8s.io` family is reserved for exec / attach, where
    // stdin/stdout/stderr/err/resize need to be multiplexed onto one socket.
    //
    // Upstream's `Pods should support retrieving logs from the container over
    // websockets` test (pods.go:583) negotiates ONLY `binary.k8s.io` and
    // asserts the accumulated bytes equal the raw log payload — a channel-1
    // prefix would corrupt the first byte and fail the assertion.
    //
    // K8s ref: pkg/registry/core/pod/rest/log.go (LogREST) +
    //          staging/.../wsstream/stream.go (binaryWebSocketProtocol).
    if let Some(ws) = ws {
        let logs_clone = logs.clone();
        Ok(ws
            .protocols(["binary.k8s.io", "base64.binary.k8s.io"])
            .on_upgrade(move |socket| async move {
                streaming::handle_ws_logs(socket, logs_clone).await;
            }))
    } else {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(Body::from(logs))
            .unwrap())
    }
}

/// Convert a [`LogsQuery`] into the [`LogReadOptions`](crate::cri_exec::LogReadOptions)
/// the CRI log reader uses, resolving `sinceSeconds`/`sinceTime` to an absolute
/// Unix-epoch bound.
fn log_read_options(query: &LogsQuery) -> crate::cri_exec::LogReadOptions {
    let mut opts = crate::cri_exec::LogReadOptions {
        timestamps: query.timestamps,
        tail_lines: query.tail_lines,
        limit_bytes: query.limit_bytes,
        since_unix: None,
    };

    // K8s sinceSeconds is a relative duration (seconds ago from now); resolve
    // it to an absolute Unix-epoch bound. sinceTime is already absolute.
    if let Some(since_seconds) = query.since_seconds {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        opts.since_unix = Some(now - since_seconds);
    }
    if let Some(ref since_time) = query.since_time {
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(since_time) {
            opts.since_unix = Some(parsed.timestamp());
        }
    }
    opts
}

/// Get real logs from the container runtime via CRI.
///
/// Resolves the named container to its CRI container id, reads the runtime's
/// container log file ([`resolve_log_path`](crate::cri_exec::resolve_log_path)),
/// and parses the CRI log line format into the raw bytes the API returns —
/// honoring `timestamps`, `tailLines`, `limitBytes`, `sinceSeconds`/`sinceTime`.
async fn get_container_logs(
    pod: &rusternetes_common::resources::Pod,
    container_name: &str,
    query: &LogsQuery,
) -> anyhow::Result<String> {
    let mut cri = crate::cri_exec::connect().await?;

    let container_id = crate::cri_exec::resolve_container_id(&mut cri, pod, container_name)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no container {} found for pod {}",
                container_name,
                pod.metadata.name
            )
        })?;

    let Some(log_path) = crate::cri_exec::resolve_log_path(&mut cri, pod, &container_id).await?
    else {
        tracing::debug!(
            "CRI reported no log_path for container {} of pod {}",
            container_name,
            pod.metadata.name
        );
        return Ok(String::new());
    };

    let opts = log_read_options(query);
    let bytes = crate::cri_exec::read_log_file(&log_path, &opts)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Streaming variant of [`get_container_logs`] used when the client requests
/// `follow=true`. Returns a byte stream that stays open and tails the CRI
/// container log file, polling for appended output, so incremental log lines
/// reach the client without the EOF-then-reconnect loop hydrophone fell into.
///
/// `limit_bytes` is not enforced on this path — the upstream conformance
/// tests that exercise follow don't set it, and the cost of tracking a
/// running byte count through a generic boxed stream isn't worth it.
async fn stream_container_logs(
    pod: &rusternetes_common::resources::Pod,
    container_name: &str,
    query: &LogsQuery,
) -> anyhow::Result<impl futures::Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>>>
{
    use std::io::{Read, Seek, SeekFrom};

    let mut cri = crate::cri_exec::connect().await?;
    let container_id = crate::cri_exec::resolve_container_id(&mut cri, pod, container_name)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no container {} found for pod {}",
                container_name,
                pod.metadata.name
            )
        })?;
    let log_path = crate::cri_exec::resolve_log_path(&mut cri, pod, &container_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "CRI reported no log_path for container {} of pod {}",
                container_name,
                pod.metadata.name
            )
        })?;

    let opts = log_read_options(query);
    let timestamps = opts.timestamps;
    let since_unix = opts.since_unix;

    // First emit the existing contents (honoring tail/since), then tail the
    // file for new lines. `async_stream` lets us poll the file from inside the
    // stream so the HTTP response body stays open until the container exits.
    let stream = async_stream::stream! {
        // Initial backlog read (tail/since honored by read_log_file). A read
        // error here is NOT fatal: the container may have only just started and
        // containerd may not have created the log file yet. Yielding an Err
        // would make hyper reset the HTTP/2 stream (RST_STREAM, INTERNAL_ERROR),
        // which clients like hydrophone treat as a hard failure rather than a
        // retryable empty read. Instead, emit nothing and fall through to the
        // tail loop, which waits for the file to appear.
        match crate::cri_exec::read_log_file(&log_path, &opts) {
            Ok(bytes) if !bytes.is_empty() => {
                yield Ok(bytes::Bytes::from(bytes));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(
                    "follow log backlog read failed for {} (container may not have written yet): {}",
                    log_path.display(),
                    e
                );
            }
        }

        // Seek to end of file and tail. Re-open on each tick so a rotated /
        // recreated log file is picked up.
        let mut offset: u64 = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
        let mut carry: Vec<u8> = Vec::new();
        // Whether the log file has ever existed. Before it appears, a missing
        // file means "container hasn't logged yet" (keep waiting); after it has
        // appeared, a missing file means the container/log was removed (stop).
        let mut seen_file = std::fs::metadata(&log_path).is_ok();
        // Bound the pre-first-appearance wait so a container that exits without
        // ever producing a log file ends the stream cleanly (EOF) instead of
        // tailing a path that will never exist.
        let mut waited_ticks: u32 = 0;
        const MAX_PRE_FILE_TICKS: u32 = 240; // 240 * 500ms = 120s
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let len = match std::fs::metadata(&log_path) {
                Ok(m) => { seen_file = true; m.len() }
                Err(_) if !seen_file => {
                    // Log file not created yet — keep waiting, up to the bound.
                    waited_ticks += 1;
                    if waited_ticks >= MAX_PRE_FILE_TICKS { break; }
                    continue;
                }
                Err(_) => break, // file existed then vanished — container removed; end stream
            };
            if len < offset {
                // File truncated/rotated — restart from the beginning.
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

            // Only parse up to the last complete line; keep any partial tail.
            let last_nl = carry.iter().rposition(|&b| b == b'\n');
            let Some(idx) = last_nl else { continue };
            let complete: Vec<u8> = carry.drain(..=idx).collect();

            let mut out = Vec::new();
            for line in complete.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let parsed = crate::cri_exec::parse_cri_log_line(line);
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
    };

    Ok(stream)
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/exec
/// Execute a command in a container (supports both SPDY and WebSocket)
pub async fn exec(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    ws: Option<WebSocketUpgrade>,
    req: Request,
) -> Result<Response> {
    // Parse query params manually because `command` can appear multiple times
    // (e.g., ?command=/bin/sh&command=-c&command=echo+hello) which serde's
    // query deserializer can't handle as Vec<String>.
    let raw_query = req.uri().query().unwrap_or("");
    let query = {
        let mut command = Vec::new();
        let mut container = None;
        let mut stdin = false;
        let mut stdout = false;
        let mut stderr = false;
        let mut tty = false;
        for pair in raw_query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                let value = percent_decode_str(value);
                match key {
                    "command" => command.push(value),
                    "container" => container = Some(value),
                    "stdin" => stdin = value == "true" || value == "1",
                    "stdout" => stdout = value == "true" || value == "1",
                    "stderr" => stderr = value == "true" || value == "1",
                    "tty" => tty = value == "true" || value == "1",
                    _ => {}
                }
            }
        }
        ExecQuery {
            container,
            command,
            stdin,
            stdout,
            stderr,
            tty,
        }
    };

    // Log request headers for debugging exec protocol
    let upgrade_header = req
        .headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");
    let connection_header = req
        .headers()
        .get("connection")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");
    let sec_ws_protocol = req
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");
    info!(
        "Exec {}/{}: cmd={:?} upgrade={} connection={} ws-protocol={}",
        namespace, name, query.command, upgrade_header, connection_header, sec_ws_protocol
    );

    // Save user info for webhook check before auth moves ownership
    let webhook_user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("exec");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Run admission webhooks for Connect operation (exec)
    // K8s passes PodExecOptions as the admission object so webhooks can
    // inspect what command is being run and what streams are requested.
    {
        use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "PodExecOptions".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods/exec".to_string(),
        };
        // Build PodExecOptions object matching K8s schema
        let exec_options = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PodExecOptions",
            "stdin": query.stdin,
            "stdout": query.stdout,
            "stderr": query.stderr,
            "tty": query.tty,
            "container": query.container.as_deref().unwrap_or(""),
            "command": query.command
        });
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &Operation::Connect,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                Some(exec_options),
                None,
                &webhook_user_info,
            )
            .await?
        {
            return Err(Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    // Get the pod
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Determine which container to exec into
    let container_name = if let Some(ref container) = query.container {
        container.clone()
    } else {
        // If no container specified, use the first container
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Verify the container exists in regular, init, or ephemeral containers
    let container_exists = pod
        .spec
        .as_ref()
        .map(|spec| {
            spec.containers.iter().any(|c| c.name == container_name)
                || spec
                    .init_containers
                    .as_ref()
                    .map(|ics| ics.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
                || spec
                    .ephemeral_containers
                    .as_ref()
                    .map(|ecs| ecs.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
        })
        .unwrap_or(false);

    if !container_exists {
        return Err(Error::NotFound(format!(
            "Container {} not found in pod {}/{}",
            container_name, namespace, name
        )));
    }

    // Handle WebSocket upgrade if requested
    if let Some(ws) = ws {
        info!("Upgrading exec to WebSocket for pod {}/{}", namespace, name);
        // Accept the v5.channel.k8s.io subprotocol for Kubernetes exec
        // Detect which protocol the client requested. v1 (channel.k8s.io) doesn't
        // use channel 3 for status; v4/v5 do. We need to tell the handler.
        let is_v1_protocol = sec_ws_protocol == "channel.k8s.io"
            || (!sec_ws_protocol.contains("v4.channel") && !sec_ws_protocol.contains("v5.channel"));
        return Ok(ws
            .protocols(["v5.channel.k8s.io", "v4.channel.k8s.io", "channel.k8s.io"])
            .on_upgrade(move |socket| {
                streaming::handle_exec_websocket_with_protocol(
                    socket,
                    pod,
                    container_name,
                    query.command,
                    query.stdin,
                    query.stdout,
                    query.stderr,
                    query.tty,
                    is_v1_protocol,
                )
            })
            .into_response());
    }

    // SPDY upgrade (older kubectl / client-go SPDY executor): bridge to the CRI
    // exec stream over the real SPDY/3.1 codec (#1264).
    if spdy::is_spdy_request(&req) {
        info!("Upgrading exec to SPDY for pod {}/{}", namespace, name);
        let response = spdy::create_spdy_upgrade_response()
            .map_err(|e| Error::Internal(format!("Failed to create SPDY upgrade response: {e}")))?;
        tokio::spawn(async move {
            match hyper::upgrade::on(req).await {
                Ok(upgraded) => {
                    let io = hyper_util::rt::TokioIo::new(upgraded);
                    crate::spdy3_handlers::handle_spdy3_exec(
                        io,
                        pod,
                        container_name,
                        query.command,
                        query.stdin,
                        query.stdout,
                        query.stderr,
                        query.tty,
                    )
                    .await;
                }
                Err(e) => tracing::error!("exec SPDY upgrade failed: {e}"),
            }
        });
        return Ok(response.into_response());
    }

    // Plain HTTP (no upgrade): one-shot exec, return collected output as the body.
    info!(
        "Direct exec for pod {}/{}: {:?}",
        namespace, name, query.command
    );

    // Run the command one-shot via CRI ExecSync and collect stdout/stderr +
    // exit code. (The SPDY/plain-HTTP path was never interactive — it always
    // collected output one-shot — so ExecSync is a faithful replacement.)
    let mut cri = crate::cri_exec::connect()
        .await
        .map_err(|e| Error::Internal(format!("CRI connect: {e}")))?;

    let container_id = crate::cri_exec::resolve_container_id(&mut cri, &pod, &container_name)
        .await
        .map_err(|e| Error::Internal(format!("CRI resolve container: {e}")))?
        .ok_or_else(|| {
            Error::NotFound(format!(
                "no running container {} found for pod {}/{}",
                container_name, namespace, name
            ))
        })?;

    let (stdout_data, stderr_data, exit_code) =
        crate::cri_exec::exec_sync(&mut cri, &container_id, &query.command, 60)
            .await
            .map_err(|e| Error::Internal(format!("CRI exec: {e}")))?;

    // Return as Kubernetes-compatible exec response
    // For SPDY clients: return 101 Switching Protocols with the output
    // This isn't proper SPDY but gives kubectl something to parse
    // Return exec output as plain HTTP response
    let mut output_str = String::from_utf8_lossy(&stdout_data).to_string();
    if !stderr_data.is_empty() {
        output_str.push_str(&String::from_utf8_lossy(&stderr_data));
    }
    Ok(Response::builder()
        .status(if exit_code == 0 {
            StatusCode::OK
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        })
        .header("Content-Type", "text/plain")
        .body(Body::from(output_str))
        .unwrap())
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/attach
/// Attach to a running container (supports both SPDY and WebSocket)
pub async fn attach(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<AttachQuery>,
    ws: Option<WebSocketUpgrade>,
    req: Request,
) -> Result<Response> {
    info!("Attaching to pod {}/{}", namespace, name);

    // Save user info for webhook check before auth moves ownership
    let webhook_user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("attach");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the pod
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Determine which container to attach to
    let container_name = if let Some(ref container) = query.container {
        container.clone()
    } else {
        // If no container specified, use the first container
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Verify the container exists in regular, init, or ephemeral containers
    let container_exists = pod
        .spec
        .as_ref()
        .map(|spec| {
            spec.containers.iter().any(|c| c.name == container_name)
                || spec
                    .init_containers
                    .as_ref()
                    .map(|ics| ics.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
                || spec
                    .ephemeral_containers
                    .as_ref()
                    .map(|ecs| ecs.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
        })
        .unwrap_or(false);

    if !container_exists {
        return Err(Error::NotFound(format!(
            "Container {} not found in pod {}/{}",
            container_name, namespace, name
        )));
    }

    // Run admission webhooks for Connect operation (attach)
    // K8s validates attach requests through admission webhooks the same as exec.
    // The GVR resource must include the subresource (pods/attach) so that webhook
    // rules matching "pods/attach" or "pods/*" are correctly triggered.
    // K8s passes PodAttachOptions as the admission object so webhooks can inspect
    // which container is being attached to and what streams are requested.
    {
        use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "PodAttachOptions".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods/attach".to_string(),
        };
        // Build PodAttachOptions object matching K8s schema
        let attach_options = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PodAttachOptions",
            "stdin": query.stdin,
            "stdout": query.stdout,
            "stderr": query.stderr,
            "tty": query.tty,
            "container": query.container.as_deref().unwrap_or("")
        });
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &Operation::Connect,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                Some(attach_options),
                None,
                &webhook_user_info,
            )
            .await?
        {
            return Err(Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    // Check if this is a SPDY upgrade request (kubectl uses SPDY)
    if spdy::is_spdy_request(&req) {
        info!(
            "Upgrading attach to SPDY for pod {}/{} (kubectl compatibility)",
            namespace, name
        );

        // Create SPDY upgrade response
        let response = spdy::create_spdy_upgrade_response().map_err(|e| {
            Error::Internal(format!("Failed to create SPDY upgrade response: {}", e))
        })?;

        // Spawn task to handle SPDY connection after upgrade
        tokio::spawn(async move {
            match spdy::upgrade_to_spdy(req).await {
                Ok(spdy_conn) => {
                    spdy_handlers::handle_spdy_attach(
                        spdy_conn,
                        pod,
                        container_name,
                        query.stdin,
                        query.stdout,
                        query.stderr,
                        query.tty,
                    )
                    .await;
                }
                Err(e) => {
                    tracing::error!("Failed to upgrade to SPDY: {}", e);
                }
            }
        });

        return Ok(response.into_response());
    }

    // Handle WebSocket upgrade if requested
    if let Some(ws) = ws {
        info!(
            "Upgrading attach to WebSocket for pod {}/{}",
            namespace, name
        );
        Ok(ws
            .on_upgrade(move |socket| {
                streaming::handle_attach_websocket(
                    socket,
                    pod,
                    container_name,
                    query.stdin,
                    query.stdout,
                    query.stderr,
                    query.tty,
                )
            })
            .into_response())
    } else {
        // No upgrade requested - return error
        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("Content-Type", "text/plain")
            .body(Body::from(
                "Attach requires protocol upgrade (SPDY or WebSocket). Use:\n\
                - kubectl (uses SPDY automatically)\n\
                - WebSocket protocol for custom clients\n",
            ))
            .unwrap())
    }
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/portforward
/// Forward ports to a pod (supports both SPDY and WebSocket)
pub async fn portforward(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<PortForwardQuery>,
    ws: Option<WebSocketUpgrade>,
    req: Request,
) -> Result<Response> {
    info!("Port forwarding to pod {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("portforward");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the pod
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Parse ports from query parameter
    let ports: Vec<u16> = if let Some(ref ports_str) = query.ports {
        ports_str
            .split(',')
            .filter_map(|p| p.trim().parse().ok())
            .collect()
    } else {
        vec![]
    };

    if ports.is_empty() {
        return Err(Error::InvalidResource(
            "No ports specified for port forwarding".to_string(),
        ));
    }

    // Check if this is a SPDY upgrade request (kubectl uses SPDY)
    if spdy::is_spdy_request(&req) {
        info!(
            "Upgrading port-forward to SPDY for pod {}/{}, ports: {:?} (kubectl compatibility)",
            namespace, name, ports
        );

        // Create SPDY upgrade response
        let response = spdy::create_spdy_upgrade_response().map_err(|e| {
            Error::Internal(format!("Failed to create SPDY upgrade response: {}", e))
        })?;

        // Spawn task to handle SPDY connection after upgrade
        tokio::spawn(async move {
            match spdy::upgrade_to_spdy(req).await {
                Ok(spdy_conn) => {
                    spdy_handlers::handle_spdy_portforward(spdy_conn, pod, ports).await;
                }
                Err(e) => {
                    tracing::error!("Failed to upgrade to SPDY: {}", e);
                }
            }
        });

        return Ok(response.into_response());
    }

    // Handle WebSocket upgrade if requested
    if let Some(ws) = ws {
        info!(
            "Upgrading port-forward to WebSocket for pod {}/{}, ports: {:?}",
            namespace, name, ports
        );
        Ok(ws
            .on_upgrade(move |socket| streaming::handle_portforward_websocket(socket, pod, ports))
            .into_response())
    } else {
        // No upgrade requested - return error
        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("Content-Type", "text/plain")
            .body(Body::from(
                "Port forward requires protocol upgrade (SPDY or WebSocket). Use:\n\
                - kubectl (uses SPDY automatically)\n\
                - WebSocket protocol for custom clients\n",
            ))
            .unwrap())
    }
}

/// POST /api/v1/namespaces/{namespace}/pods/{name}/binding
/// Bind a pod to a node
pub async fn create_binding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    body: String,
) -> Result<Response> {
    info!("Creating binding for pod {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("binding");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Parse binding request
    let binding: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::InvalidResource(format!("Invalid binding format: {}", e)))?;

    // target.kind, when set, must be "Node" (upstream ValidatePodBinding).
    if let Some(kind) = binding
        .get("target")
        .and_then(|t| t.get("kind"))
        .and_then(|k| k.as_str())
    {
        if !kind.is_empty() && kind != "Node" {
            return Err(Error::InvalidResource(format!(
                "Unsupported value: target.kind: \"{}\": supported values: \"Node\", \"<empty>\"",
                kind
            )));
        }
    }

    // Extract target node from binding
    let node_name = binding
        .get("target")
        .and_then(|t: &serde_json::Value| t.get("name"))
        .and_then(|n: &serde_json::Value| n.as_str())
        .ok_or_else(|| Error::InvalidResource("Missing target.name in binding".to_string()))?;

    // Update pod's spec.nodeName to bind it to the node
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let mut pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // K8s ref: pkg/registry/core/pod/storage/storage.go BindingREST.Create —
    // a Pod can only be bound once. A second POST /binding against an
    // already-bound Pod returns 409 Conflict
    // ("pod ... is already assigned to node ..."). Without this check the
    // handler silently overwrites spec.nodeName.
    if let Some(existing) = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .filter(|s| !s.is_empty())
    {
        return Err(Error::Conflict(format!(
            "pod {} is already assigned to node \"{}\"",
            name, existing
        )));
    }

    // Set the nodeName in spec
    if let Some(ref mut spec) = pod.spec {
        spec.node_name = Some(node_name.to_string());
    } else {
        return Err(Error::InvalidResource("Pod has no spec".to_string()));
    }

    // K8s ref: plugin/pkg/admission/podtopologylabels/admission.go (release-1.35).
    // The PodTopologyLabelsAdmission mutating plugin copies the canonical
    // `topology.kubernetes.io/zone` and `topology.kubernetes.io/region` labels
    // (and ONLY those two keys) from the bound Node onto the Pod when a
    // Binding is created. Node labels win over pod-level labels. Subdomains
    // of `topology.kubernetes.io` and other keys under that domain are not
    // copied.
    //
    // Gated by the `PodTopologyLabelsAdmission` feature gate (Beta + default
    // on at v1.35; flipped process-wide via `rusternetes_common::feature_gates`).
    // When the gate is off the plugin is a no-op — exactly the behaviour
    // upstream's `Plugin.Admit` exhibits when `p.enabled == false`.
    if rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::PodTopologyLabelsAdmission,
    ) {
        let node_key = rusternetes_storage::build_key("nodes", None::<&str>, node_name);
        if let Ok(node) = state
            .storage
            .get::<rusternetes_common::resources::Node>(&node_key)
            .await
        {
            if let Some(node_labels) = node.metadata.labels.as_ref() {
                const ZONE_KEY: &str = "topology.kubernetes.io/zone";
                const REGION_KEY: &str = "topology.kubernetes.io/region";
                for key in [ZONE_KEY, REGION_KEY] {
                    if let Some(value) = node_labels.get(key) {
                        let labels = pod.metadata.labels.get_or_insert_with(Default::default);
                        labels.insert(key.to_string(), value.clone());
                    }
                }
            }
        }
    }

    // Update the pod in the storage
    state.storage.update(&pod_key, &pod).await?;

    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header("Content-Type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Binding",
                "metadata": {
                    "name": name,
                    "namespace": namespace
                },
                "target": {
                    "kind": "Node",
                    "name": node_name
                }
            })
            .to_string(),
        ))
        .unwrap())
}

/// POST /api/v1/namespaces/{namespace}/pods/{name}/eviction
///
/// Evict a pod, mirroring upstream
/// `pkg/registry/core/pod/storage/eviction.go` at release-1.35:
///
///   1. Validate the body's `apiVersion` (accept `policy/v1` and
///      `policy/v1beta1`, reject other `policy/*` shapes with 400).
///   2. Honor `deleteOptions.preconditions.uid` — a UID mismatch returns
///      409 Conflict and the pod is **not** deleted.
///   3. `canIgnorePDB`: pods in a terminal phase (`Succeeded`, `Failed`,
///      `Pending`) or already carrying a `deletionTimestamp` bypass the
///      PDB check entirely. For these pods we proceed straight to the
///      condition-update + delete path.
///   4. For healthy (Running) pods, check matching PDB(s) and return 429
///      Too Many Requests with `Retry-After: 10` if the disruption budget
///      would be violated. The error message wording mirrors upstream's
///      `createTooManyRequestsError` byte-for-byte.
///   5. Append a `DisruptionTarget=True` condition to `pod.status.conditions`
///      via a storage update (skipped if `deleteOptions.dryRun == ["All"]`).
///   6. Delete the pod through `handle_delete_with_finalizers` so finalizers
///      hold the pod (deletion_timestamp set, pod still readable).
pub async fn create_eviction(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    body: String,
) -> Result<Response> {
    info!("Creating eviction for pod {}/{}", namespace, name);

    // ----------------------------------------------------------------- authz
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("eviction");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // ----------------------------------------------------------------- parse
    let eviction: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::InvalidResource(format!("Invalid eviction format: {}", e)))?;

    // Validate apiVersion. Upstream `AcceptsGroupVersion` accepts only
    // policy/v1 and policy/v1beta1. An empty/missing apiVersion is permitted
    // (treated as the canonical policy/v1 shape) since the body name+ns alone
    // is enough to identify the target.
    if let Some(av) = eviction.get("apiVersion").and_then(|v| v.as_str()) {
        if !av.is_empty() && av != "policy/v1" && av != "policy/v1beta1" {
            return Err(Error::InvalidResource(format!(
                "unsupported Eviction apiVersion {:?}; expected policy/v1 or policy/v1beta1",
                av
            )));
        }
    }

    // Parse deleteOptions (UID precondition, dryRun) from the body.
    let delete_opts = eviction.get("deleteOptions");
    let precond_uid = delete_opts
        .and_then(|o| o.get("preconditions"))
        .and_then(|p| p.get("uid"))
        .and_then(|u| u.as_str())
        .map(|s| s.to_string());
    let dry_run_all = delete_opts
        .and_then(|o| o.get("dryRun"))
        .and_then(|d| d.as_array())
        .map(|arr| arr.iter().any(|v| v.as_str() == Some("All")))
        .unwrap_or(false);
    let grace_period_seconds = delete_opts
        .and_then(|opts| opts.get("gracePeriodSeconds"))
        .and_then(|gp| gp.as_i64());

    // deleteOptions.gracePeriodSeconds must be non-negative (upstream
    // ValidateDeleteOptions).
    if let Some(g) = grace_period_seconds {
        if g < 0 {
            return Err(Error::Invalid(vec![
                rusternetes_common::validation::field::Error::invalid(
                    &rusternetes_common::validation::field::Path::new("deleteOptions")
                        .child("gracePeriodSeconds"),
                    g,
                    "must be greater than or equal to 0",
                ),
            ]));
        }
    }

    // ----------------------------------------------------------------- fetch
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // ----------------------------------------------------------- precondition
    // UID precondition check mirrors upstream's `addConditionAndDeletePod`
    // getLatestPod hook. Mismatch returns 409 Conflict.
    if let Some(ref want_uid) = precond_uid {
        if !want_uid.is_empty() && *want_uid != pod.metadata.uid {
            return Err(Error::Conflict(format!(
                "the UID in the precondition ({}) does not match the UID in record ({}). \
                 The object might have been deleted and then recreated",
                want_uid, pod.metadata.uid
            )));
        }
    }

    // ----------------------------------------------------------- canIgnorePDB
    // Upstream `canIgnorePDB`: terminal pods (Succeeded/Failed/Pending) and
    // pods already mid-deletion bypass the PDB check entirely.
    let bypass_pdb = pod_can_ignore_pdb(&pod);

    if !bypass_pdb {
        // Healthy pod — evaluate matching PDBs.
        let pdb_prefix =
            rusternetes_storage::build_prefix("poddisruptionbudgets", Some(&namespace));
        let pdbs: Vec<rusternetes_common::resources::PodDisruptionBudget> =
            state.storage.list(&pdb_prefix).await.unwrap_or_default();
        let pod_labels = pod.metadata.labels.clone().unwrap_or_default();

        for pdb in &pdbs {
            // Full label-selector match (matchLabels + matchExpressions).
            // Upstream eviction skips PDBs whose selector is empty or does not
            // match the pod (`selector.Empty() || !selector.Matches(...)`).
            if !pdb.spec.selector.matches_labels(&pod_labels) {
                continue;
            }

            // This PDB applies. Compute disruptions_allowed inline rather
            // than trusting the cached status — the PDB controller may not
            // have observed the latest pods yet (matches upstream's
            // observedGeneration<generation gate, which would also return
            // 429 in that case).
            let disruptions_allowed =
                compute_pdb_disruptions_allowed(state.storage.as_ref(), pdb, &namespace).await;

            if disruptions_allowed <= 0 {
                let current_healthy =
                    compute_pdb_healthy_count(state.storage.as_ref(), pdb, &namespace).await;
                let desired_healthy = compute_pdb_desired_healthy(pdb, current_healthy);
                // Upstream wording from `createTooManyRequestsError` +
                // `checkAndDecrement`'s detail message.
                let summary =
                    "Cannot evict pod as it would violate the pod's disruption budget.".to_string();
                let detail = format!(
                    "The disruption budget {} needs {} healthy pods and has {} currently",
                    pdb.metadata.name, desired_healthy, current_healthy
                );
                let status_body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "metadata": {},
                    "status": "Failure",
                    "message": summary,
                    "reason": "TooManyRequests",
                    "details": {
                        "causes": [{
                            "reason": "DisruptionBudget",
                            "message": detail
                        }],
                        "retryAfterSeconds": 10
                    },
                    "code": 429
                });
                return Ok(axum::response::Response::builder()
                    .status(axum::http::StatusCode::TOO_MANY_REQUESTS)
                    .header("Content-Type", "application/json")
                    .header("Retry-After", "10")
                    .body(axum::body::Body::from(
                        serde_json::to_string(&status_body).unwrap(),
                    ))
                    .unwrap()
                    .into_response());
            }

            info!(
                "Eviction for pod {}/{} passes PDB {} check (disruptions_allowed = {})",
                namespace, name, pdb.metadata.name, disruptions_allowed
            );

            // Bookkeep the disruption on the PDB status (skip on dryRun, as
            // upstream does in `checkAndDecrement`).
            if !dry_run_all {
                let mut updated_pdb = pdb.clone();
                let disrupted_pods = updated_pdb
                    .status
                    .get_or_insert(rusternetes_common::resources::PodDisruptionBudgetStatus {
                        current_healthy: 0,
                        desired_healthy: 0,
                        disruptions_allowed: 0,
                        expected_pods: 0,
                        observed_generation: None,
                        conditions: None,
                        disrupted_pods: None,
                    })
                    .disrupted_pods
                    .get_or_insert_with(std::collections::HashMap::new);
                disrupted_pods.insert(name.clone(), chrono::Utc::now());

                if let Some(ref mut status) = updated_pdb.status {
                    status.disruptions_allowed = disruptions_allowed - 1;
                }

                let pdb_key = rusternetes_storage::build_key(
                    "poddisruptionbudgets",
                    Some(&namespace),
                    &pdb.metadata.name,
                );
                let _ = state.storage.update(&pdb_key, &updated_pdb).await;
            }
        }
    }

    // -------------------------------------------------- DisruptionTarget cond
    // Skip both the condition update and the delete on dryRun (mirrors
    // upstream's `if !dryrun.IsDryRun(options.DryRun) { ... }` gate).
    if !dry_run_all {
        // The condition update is best-effort relative to the delete;
        // upstream surfaces errors, so we do too.
        append_disruption_target_condition(state.storage.as_ref(), &pod_key).await?;

        // Re-read the pod after the condition update so that finalizer
        // handling sees the freshest copy.
        let fresh_pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

        // Delete the pod through the finalizer-aware helper. If the pod
        // carries finalizers, this sets `deletionTimestamp` and writes it
        // back instead of removing it from storage.
        crate::handlers::finalizers::handle_delete_with_finalizers(
            state.storage.as_ref(),
            &pod_key,
            &fresh_pod,
        )
        .await?;
    }

    info!(
        "Evicted pod {}/{} (dryRun={}, gracePeriod={:?})",
        namespace, name, dry_run_all, grace_period_seconds
    );

    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header("Content-Type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "apiVersion": "policy/v1",
                "kind": "Eviction",
                "metadata": {
                    "name": name,
                    "namespace": namespace
                }
            })
            .to_string(),
        ))
        .unwrap())
}

/// Upstream `canIgnorePDB`: a pod is freely evictable (no PDB check needed)
/// if it is in a terminal phase (Succeeded, Failed), Pending, or already
/// carries a deletionTimestamp.
fn pod_can_ignore_pdb(pod: &rusternetes_common::resources::Pod) -> bool {
    if pod.metadata.deletion_timestamp.is_some() {
        return true;
    }
    matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(
            rusternetes_common::types::Phase::Succeeded
                | rusternetes_common::types::Phase::Failed
                | rusternetes_common::types::Phase::Pending
        )
    )
}

/// Append the `DisruptionTarget=True` condition (reason `EvictionByEvictionAPI`)
/// to `pod.status.conditions`. Mirrors upstream's `conditionAppender` in
/// `addConditionAndDeletePod`. No-op (Ok) if the condition is already
/// present with the same status/reason.
async fn append_disruption_target_condition<S: Storage>(storage: &S, pod_key: &str) -> Result<()> {
    let mut pod: rusternetes_common::resources::Pod = storage.get(pod_key).await?;
    let status = pod.status.get_or_insert_with(Default::default);
    let conditions = status.conditions.get_or_insert_with(Vec::new);

    let now = chrono::Utc::now();
    let new_cond = rusternetes_common::resources::PodCondition {
        condition_type: "DisruptionTarget".to_string(),
        status: "True".to_string(),
        reason: Some("EvictionByEvictionAPI".to_string()),
        message: Some("Eviction API: evicting".to_string()),
        last_probe_time: None,
        last_transition_time: Some(now),
        observed_generation: None,
    };

    if let Some(existing) = conditions
        .iter_mut()
        .find(|c| c.condition_type == "DisruptionTarget")
    {
        if existing.status == new_cond.status && existing.reason == new_cond.reason {
            // No change — skip the write to mirror upstream's
            // UpdatePodCondition which only bumps lastTransitionTime when
            // the status flips.
            return Ok(());
        }
        existing.status = new_cond.status;
        existing.reason = new_cond.reason;
        existing.message = new_cond.message;
        existing.last_transition_time = new_cond.last_transition_time;
    } else {
        conditions.push(new_cond);
    }

    storage.update(pod_key, &pod).await?;
    Ok(())
}

/// Check if a pod matches a PDB's label selector
fn pod_matches_pdb_selector(
    pod: &rusternetes_common::resources::Pod,
    selector: &rusternetes_common::types::LabelSelector,
) -> bool {
    let pod_labels = match &pod.metadata.labels {
        Some(labels) => labels,
        None => return false,
    };

    if let Some(ref match_labels) = selector.match_labels {
        for (key, value) in match_labels {
            if pod_labels.get(key) != Some(value) {
                return false;
            }
        }
    }

    true
}

/// Check if a pod is healthy (Running phase)
fn is_pod_healthy(pod: &rusternetes_common::resources::Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_ref())
        .map(|p| matches!(p, rusternetes_common::types::Phase::Running))
        .unwrap_or(false)
}

/// Compute the number of healthy pods matching a PDB's selector
async fn compute_pdb_healthy_count<S: Storage>(
    storage: &S,
    pdb: &rusternetes_common::resources::PodDisruptionBudget,
    namespace: &str,
) -> i32 {
    let pods_prefix = rusternetes_storage::build_prefix("pods", Some(namespace));
    let all_pods: Vec<rusternetes_common::resources::Pod> =
        storage.list(&pods_prefix).await.unwrap_or_default();

    let matching_pods: Vec<&rusternetes_common::resources::Pod> = all_pods
        .iter()
        .filter(|p| pod_matches_pdb_selector(p, &pdb.spec.selector))
        .collect();

    matching_pods.iter().filter(|p| is_pod_healthy(p)).count() as i32
}

/// Compute the desired healthy count from a PDB spec
fn compute_pdb_desired_healthy(
    pdb: &rusternetes_common::resources::PodDisruptionBudget,
    total_pods: i32,
) -> i32 {
    if let Some(ref min_available) = pdb.spec.min_available {
        match min_available {
            rusternetes_common::resources::IntOrString::Int(value) => *value,
            rusternetes_common::resources::IntOrString::String(s) => {
                if let Some(stripped) = s.strip_suffix('%') {
                    if let Ok(percentage) = stripped.parse::<f64>() {
                        ((total_pods as f64) * (percentage / 100.0)).ceil() as i32
                    } else {
                        total_pods
                    }
                } else {
                    total_pods
                }
            }
        }
    } else if let Some(ref max_unavailable) = pdb.spec.max_unavailable {
        let max_unavailable_count = match max_unavailable {
            rusternetes_common::resources::IntOrString::Int(value) => *value,
            rusternetes_common::resources::IntOrString::String(s) => {
                if let Some(stripped) = s.strip_suffix('%') {
                    if let Ok(percentage) = stripped.parse::<f64>() {
                        ((total_pods as f64) * (percentage / 100.0)).floor() as i32
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
        };
        total_pods - max_unavailable_count
    } else {
        // No min_available or max_unavailable - default to requiring all pods
        total_pods
    }
}

/// Compute disruptions_allowed for a PDB by counting matching healthy pods
async fn compute_pdb_disruptions_allowed<S: Storage>(
    storage: &S,
    pdb: &rusternetes_common::resources::PodDisruptionBudget,
    namespace: &str,
) -> i32 {
    let pods_prefix = rusternetes_storage::build_prefix("pods", Some(namespace));
    let all_pods: Vec<rusternetes_common::resources::Pod> =
        storage.list(&pods_prefix).await.unwrap_or_default();

    let matching_pods: Vec<&rusternetes_common::resources::Pod> = all_pods
        .iter()
        .filter(|p| pod_matches_pdb_selector(p, &pdb.spec.selector))
        .collect();

    let total_pods = matching_pods.len() as i32;
    let healthy_pods = matching_pods.iter().filter(|p| is_pod_healthy(p)).count() as i32;
    let desired_healthy = compute_pdb_desired_healthy(pdb, total_pods);

    healthy_pods - desired_healthy
}

/// Feature name kubelets advertise in `node.status.declaredFeatures` once they
/// support the CPU-resize path for Guaranteed-QoS pods (KEP-5328 — KEP-1287
/// integration). The api-server uses this constant when deciding whether to
/// admit a `/pods/{name}/resize` request that mutates a Guaranteed pod's CPU.
pub(crate) const GUARANTEED_QOS_POD_CPU_RESIZE: &str = "GuaranteedQoSPodCPUResize";

/// Returns `true` if `pod` is Guaranteed-QoS (all containers have `cpu` AND
/// `memory` in both `requests` and `limits`, with `requests == limits`).
///
/// Mirrors `pkg/apis/core/v1/helper/qos/qos.go::GetPodQOS` for the Guaranteed
/// branch. Keeping the helper local avoids dragging the full QoS classifier
/// into the resize path — the admission check only needs the Guaranteed/not
/// distinction.
fn is_guaranteed_qos(pod: &rusternetes_common::resources::Pod) -> bool {
    let Some(spec) = pod.spec.as_ref() else {
        return false;
    };
    if spec.containers.is_empty() {
        return false;
    }
    for c in &spec.containers {
        let Some(res) = c.resources.as_ref() else {
            return false;
        };
        let has_limits = res
            .limits
            .as_ref()
            .is_some_and(|l| l.contains_key("cpu") && l.contains_key("memory"));
        if !has_limits {
            return false;
        }
        // For Guaranteed, requests must either be absent (defaulted from
        // limits) or equal to limits. Upstream `qos.go` collapses these.
        if let Some(req) = res.requests.as_ref() {
            if res.limits.as_ref() != Some(req) {
                return false;
            }
        }
    }
    true
}

/// Returns `true` if the CPU resource request OR limit differs between any
/// matching container in `desired` vs `current`. The match is by container
/// name; missing-on-one-side counts as a change.
fn resize_changes_cpu(
    current: &rusternetes_common::resources::Pod,
    desired: &rusternetes_common::resources::Pod,
) -> bool {
    let (Some(cur_spec), Some(des_spec)) = (current.spec.as_ref(), desired.spec.as_ref()) else {
        return false;
    };
    for desired_c in &des_spec.containers {
        let Some(current_c) = cur_spec
            .containers
            .iter()
            .find(|c| c.name == desired_c.name)
        else {
            continue;
        };
        let cur_cpu_req = current_c
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .and_then(|m| m.get("cpu"));
        let des_cpu_req = desired_c
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .and_then(|m| m.get("cpu"));
        let cur_cpu_lim = current_c
            .resources
            .as_ref()
            .and_then(|r| r.limits.as_ref())
            .and_then(|m| m.get("cpu"));
        let des_cpu_lim = desired_c
            .resources
            .as_ref()
            .and_then(|r| r.limits.as_ref())
            .and_then(|m| m.get("cpu"));
        if cur_cpu_req != des_cpu_req || cur_cpu_lim != des_cpu_lim {
            return true;
        }
    }
    false
}

/// Node-declared-feature admission for `/pods/{name}/resize` (KEP-5328 +
/// KEP-1287). When both `NodeDeclaredFeatures` and `InPlacePodVerticalScaling`
/// are enabled, a CPU resize against a Guaranteed-QoS pod is admitted only if
/// the target node has advertised `GuaranteedQoSPodCPUResize` in
/// `status.declaredFeatures`.
///
/// Returns:
///   - `Ok(())` when the gate combination does not apply (either gate off, or
///     the change does not touch CPU, or the pod is not Guaranteed), when the
///     pod is not yet bound to a node, when the node does not exist (the
///     resize is allowed in that case — upstream defers to scheduler), or
///     when the node *has* declared the feature.
///   - `Err(Error::Forbidden(_))` when the target node exists, both gates are
///     on, the pod is Guaranteed, the resize touches CPU, and the node has
///     not declared `GuaranteedQoSPodCPUResize`.
///
/// Upstream: `plugin/pkg/admission/noderestriction/admission.go` consults
/// `nodeFeatures` when admitting `/pods/{name}/resize`; missing
/// `GuaranteedQoSPodCPUResize` produces:
///   "spec.containers[*].resources: Forbidden: node \"<n>\" has not declared
///    feature \"GuaranteedQoSPodCPUResize\""
async fn check_node_declared_features_for_resize<S: Storage>(
    storage: &S,
    current: &rusternetes_common::resources::Pod,
    desired: &rusternetes_common::resources::Pod,
) -> Result<()> {
    use rusternetes_common::feature_gates::{enabled, Feature};

    // Both gates must be on. Mirrors upstream's
    // `if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures)
    //   && utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling)`
    // in the resize strategy.
    if !enabled(Feature::NodeDeclaredFeatures) || !enabled(Feature::InPlacePodVerticalScaling) {
        return Ok(());
    }

    // The check only fires when the change actually touches CPU on a
    // Guaranteed pod — label-only resizes (or memory-only) sail through.
    if !is_guaranteed_qos(current) || !resize_changes_cpu(current, desired) {
        return Ok(());
    }

    // Pod must already be bound to a node — there is nothing to consult
    // otherwise. Upstream returns nil in that case.
    let Some(node_name) = current
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_ref())
        .filter(|n| !n.is_empty())
    else {
        return Ok(());
    };

    let node_key = rusternetes_storage::build_key("nodes", None, node_name);
    let node: rusternetes_common::resources::Node = match storage.get(&node_key).await {
        Ok(n) => n,
        // Missing node: upstream's NodeRestriction admission allows the
        // request through and lets the scheduler / node controller catch up.
        Err(Error::NotFound(_)) => return Ok(()),
        Err(e) => return Err(e),
    };

    let declares_resize = node
        .status
        .as_ref()
        .and_then(|s| s.declared_features.as_ref())
        .map(|features| features.iter().any(|f| f == GUARANTEED_QOS_POD_CPU_RESIZE))
        .unwrap_or(false);

    if declares_resize {
        Ok(())
    } else {
        Err(Error::Forbidden(format!(
            "spec.containers[*].resources: Forbidden: node \"{}\" has not declared feature \"{}\"",
            node_name, GUARANTEED_QOS_POD_CPU_RESIZE
        )))
    }
}

/// Maximum number of CAS retry attempts for `/pods/{name}/resize` (KEP-1287).
///
/// In-place pod resize is a hot path: kubelet + admission webhooks may bump the
/// stored resourceVersion between the client's GET and PUT, producing a Conflict.
/// Upstream `kubectl` and the e2e in `common/node/pod_resize.go` expect the API
/// server to absorb a small number of races by re-reading the latest object and
/// re-applying the spec-resource changes, mirroring the behaviour of the Go
/// apiserver's `RetryConflict` helper.
const POD_RESIZE_MAX_RETRIES: usize = 5;

/// Apply an in-place resize to a Pod with CAS-retry on conflict.
///
/// Reads the latest stored Pod, copies the requested `spec.containers[*].resources`
/// (and `spec.initContainers[*].resources`) from `desired` onto it, marks
/// `status.resize = "Proposed"` so the kubelet picks the change up (KEP-1287),
/// and writes it back. On `Error::Conflict` (storage-level resourceVersion mismatch),
/// we drop the stale RV and retry up to [`POD_RESIZE_MAX_RETRIES`] times.
///
/// Generic over `S: Storage` so the same code path is exercised by both the
/// production `StorageBackend` and the in-memory test harness.
pub async fn apply_pod_resize_with_retry<S: Storage>(
    storage: &S,
    namespace: &str,
    name: &str,
    desired: &rusternetes_common::resources::Pod,
) -> Result<rusternetes_common::resources::Pod> {
    let key = rusternetes_storage::build_key("pods", Some(namespace), name);

    let mut last_err: Option<Error> = None;
    for attempt in 0..POD_RESIZE_MAX_RETRIES {
        // Always re-read so the storage CAS compares against the freshest resourceVersion.
        let mut fresh: rusternetes_common::resources::Pod = storage.get(&key).await?;

        // Bump metadata.generation when the resize actually changes the pod
        // spec (KEP-1287 resize mutates spec.containers[*].resources). Upstream
        // does this generically in the registry's PrepareForUpdate; the
        // conformance "resize pod via the replace endpoint" test asserts the
        // generation goes 1 -> 2 after one resize (pod_resize.go:761). Compare
        // before/after the merge but before the status mutation, so a status-only
        // change never trips a bump.
        let before = serde_json::to_value(&fresh).unwrap_or(serde_json::Value::Null);
        merge_container_resources_from(&mut fresh, desired);
        let after = serde_json::to_value(&fresh).unwrap_or(serde_json::Value::Null);
        crate::handlers::lifecycle::maybe_increment_generation(
            &before,
            &after,
            &mut fresh.metadata,
        );
        fresh.status.get_or_insert_with(Default::default).resize = Some("Proposed".to_string());

        match storage.update(&key, &fresh).await {
            Ok(updated) => {
                if attempt > 0 {
                    info!(
                        "Pod resize succeeded for {}/{} after {} retries",
                        namespace, name, attempt
                    );
                }
                return Ok(updated);
            }
            Err(Error::Conflict(msg)) => {
                debug!(
                    "Pod resize conflict for {}/{} on attempt {}: {} — retrying",
                    namespace, name, attempt, msg
                );
                last_err = Some(Error::Conflict(msg));
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| {
        Error::Conflict(format!(
            "pod resize failed after {} retries for {}/{}",
            POD_RESIZE_MAX_RETRIES, namespace, name
        ))
    }))
}

/// Copy `spec.containers[*].resources` and `spec.initContainers[*].resources`
/// from `desired` onto `target`, matching by container name. Containers in
/// `target` that are not mentioned in `desired` are left untouched.
fn merge_container_resources_from(
    target: &mut rusternetes_common::resources::Pod,
    desired: &rusternetes_common::resources::Pod,
) {
    let (Some(target_spec), Some(desired_spec)) = (target.spec.as_mut(), desired.spec.as_ref())
    else {
        return;
    };

    for desired_c in &desired_spec.containers {
        if let Some(target_c) = target_spec
            .containers
            .iter_mut()
            .find(|c| c.name == desired_c.name)
        {
            target_c.resources = desired_c.resources.clone();
        }
    }

    if let (Some(target_inits), Some(desired_inits)) = (
        target_spec.init_containers.as_mut(),
        desired_spec.init_containers.as_ref(),
    ) {
        for desired_c in desired_inits {
            if let Some(target_c) = target_inits.iter_mut().find(|c| c.name == desired_c.name) {
                target_c.resources = desired_c.resources.clone();
            }
        }
    }
}

/// PUT /api/v1/namespaces/{namespace}/pods/{name}/resize
///
/// Subresource handler for in-place pod resize (KEP-1287). Wraps
/// [`apply_pod_resize_with_retry`] with auth + axum extraction.
///
/// Before applying the resize, this enforces delta-aware ResourceQuota
/// admission: the pod was already counted against the namespace's quota
/// `status.used` (its current `containers[*].resources` contribution is
/// the *baseline*), so we charge only the **difference** between the
/// requested resources and the old resources. An over-budget delta is
/// rejected with `403 Forbidden ("exceeded quota")`; an in-budget delta
/// passes through to `apply_pod_resize_with_retry`.
///
/// K8s ref:
///   - plugin/pkg/admission/resourcequota/controller.go (`checkRequest`
///     subtracts the old object's contribution before charging the new
///     one — the same logic powering the main PUT path's
///     `check_resource_quota_with_old`).
///   - pkg/registry/core/pod/strategy.go `ResizeStrategy` runs quota
///     admission on the `/resize` subresource specifically.
pub async fn resize_pod(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Result<axum::Json<rusternetes_common::resources::Pod>> {
    info!("Resizing pod {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("resize");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(Error::Forbidden(reason)),
    }

    let desired: rusternetes_common::resources::Pod = serde_json::from_slice(&body)
        .map_err(|e| Error::InvalidResource(format!("failed to decode resize body: {}", e)))?;

    // Delta-aware ResourceQuota admission. Read the currently-stored Pod
    // (the baseline already accounted in quota.status.used), build the
    // *would-be* post-resize Pod by overlaying the desired container
    // resources, and run quota admission with `old_pod = Some(&current)`
    // so the evaluator charges only `new − old` against `.spec.hard`.
    //
    // Upstream Go does this in `plugin/pkg/admission/resourcequota/
    // controller.go::checkRequest`, which is invoked for both main-PUT
    // and the `/resize` subresource via `ResizeStrategy`.
    let key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let current: rusternetes_common::resources::Pod = state.storage.get(&key).await?;
    let mut projected = current.clone();
    merge_container_resources_from(&mut projected, &desired);

    // KEP-5328 + KEP-1287: when both `NodeDeclaredFeatures` and
    // `InPlacePodVerticalScaling` are enabled, refuse a Guaranteed-QoS CPU
    // resize against a node that has not advertised
    // `GuaranteedQoSPodCPUResize`. Off-by-default; mirrors the upstream
    // resize strategy's call into noderestriction admission.
    check_node_declared_features_for_resize(state.storage.as_ref(), &current, &projected).await?;

    match crate::admission::check_resource_quota_with_old(
        &state.storage,
        &namespace,
        &projected,
        Some(&current),
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return Err(Error::Forbidden("exceeded quota".to_string()));
        }
        Err(e) => {
            // Match the main PUT path: log but do not block on internal
            // evaluator errors. This is the same posture as `update_pod`.
            tracing::warn!("Error checking ResourceQuota on pod resize: {}", e);
        }
    }

    let updated =
        apply_pod_resize_with_retry(state.storage.as_ref(), &namespace, &name, &desired).await?;
    Ok(axum::Json(updated))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        IntOrString, Pod, PodDisruptionBudget, PodDisruptionBudgetSpec,
    };
    use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    fn make_pod(
        name: &str,
        namespace: &str,
        labels: HashMap<String, String>,
        running: bool,
    ) -> Pod {
        let phase = if running { "Running" } else { "Pending" };
        let labels_json = serde_json::to_value(&labels).unwrap();
        let json = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": labels_json
            },
            "spec": {
                "containers": [{
                    "name": "test",
                    "image": "nginx"
                }]
            },
            "status": {
                "phase": phase
            }
        });
        serde_json::from_value(json).unwrap()
    }

    fn make_pdb(
        name: &str,
        namespace: &str,
        min_available: i32,
        match_labels: HashMap<String, String>,
    ) -> PodDisruptionBudget {
        PodDisruptionBudget {
            type_meta: TypeMeta {
                api_version: "policy/v1".to_string(),
                kind: "PodDisruptionBudget".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            spec: PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::Int(min_available)),
                max_unavailable: None,
                selector: LabelSelector {
                    match_labels: Some(match_labels),
                    match_expressions: None,
                },
                unhealthy_pod_eviction_policy: None,
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn test_pdb_blocks_eviction_then_allows_after_update() {
        let storage = Arc::new(MemoryStorage::new());
        let ns = "test-eviction-ns";

        let labels = HashMap::from([("app".to_string(), "web".to_string())]);

        // Create a single running pod matching the PDB
        let pod = make_pod("test-pod-1", ns, labels.clone(), true);
        let pod_key = rusternetes_storage::build_key("pods", Some(ns), "test-pod-1");
        storage.create(&pod_key, &pod).await.unwrap();

        // Create a PDB with minAvailable=1 (so with 1 healthy pod, disruptions_allowed=0)
        let pdb = make_pdb("test-pdb", ns, 1, labels.clone());
        let pdb_key = rusternetes_storage::build_key("poddisruptionbudgets", Some(ns), "test-pdb");
        storage.create(&pdb_key, &pdb).await.unwrap();

        // Compute disruptions_allowed - should be 0 (1 healthy - 1 desired = 0)
        let pdb_stored: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
        let disruptions = compute_pdb_disruptions_allowed(&*storage, &pdb_stored, ns).await;
        assert_eq!(
            disruptions, 0,
            "Should not allow any disruptions with minAvailable=1 and 1 pod"
        );

        // Verify that the desired_healthy calculation is correct
        let healthy = compute_pdb_healthy_count(&*storage, &pdb_stored, ns).await;
        assert_eq!(healthy, 1);
        let desired = compute_pdb_desired_healthy(&pdb_stored, 1);
        assert_eq!(desired, 1);

        // Now update PDB to minAvailable=0 (allowing eviction)
        let mut updated_pdb = pdb_stored.clone();
        updated_pdb.spec.min_available = Some(IntOrString::Int(0));
        storage.update(&pdb_key, &updated_pdb).await.unwrap();

        // Now disruptions should be allowed (1 healthy - 0 desired = 1)
        let pdb_updated: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
        let disruptions_after = compute_pdb_disruptions_allowed(&*storage, &pdb_updated, ns).await;
        assert_eq!(
            disruptions_after, 1,
            "Should allow 1 disruption after lowering minAvailable to 0"
        );
    }

    /// A pod resize via the `/resize` subresource MUST bump `metadata.generation`
    /// when it changes `spec.containers[*].resources`, and MUST leave it unchanged
    /// for a no-op (idempotent) resize. Upstream conformance
    /// `[sig-node] Pod InPlace Resize Container resize pod via the replace endpoint`
    /// asserts `updatedPod.Generation == 2` after one resize of a freshly-created
    /// (generation 1) pod (pod_resize.go:761).
    #[tokio::test]
    async fn test_resize_bumps_generation_on_spec_change() {
        let storage = Arc::new(MemoryStorage::new());
        let ns = "test-resize-gen";

        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "p1", "namespace": ns, "generation": 1 },
            "spec": { "containers": [{
                "name": "c1",
                "image": "nginx",
                "resources": { "requests": { "cpu": "1" }, "limits": { "cpu": "1" } }
            }]},
            "status": { "phase": "Running" }
        }))
        .unwrap();
        let key = rusternetes_storage::build_key("pods", Some(ns), "p1");
        storage.create(&key, &pod).await.unwrap();

        // Resize c1 from cpu=1 to cpu=2.
        let desired: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "p1", "namespace": ns },
            "spec": { "containers": [{
                "name": "c1",
                "image": "nginx",
                "resources": { "requests": { "cpu": "2" }, "limits": { "cpu": "2" } }
            }]}
        }))
        .unwrap();

        let updated = apply_pod_resize_with_retry(&*storage, ns, "p1", &desired)
            .await
            .unwrap();

        assert_eq!(
            updated.metadata.generation,
            Some(2),
            "resize that changes resources must bump generation 1 -> 2"
        );
        let cpu = updated.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap()
            .get("cpu")
            .unwrap();
        assert_eq!(cpu, "2", "resized cpu request must be applied");

        // Idempotent re-resize with the same desired resources must NOT bump again.
        let again = apply_pod_resize_with_retry(&*storage, ns, "p1", &desired)
            .await
            .unwrap();
        assert_eq!(
            again.metadata.generation,
            Some(2),
            "no-op resize must not bump generation"
        );
    }

    #[tokio::test]
    async fn test_pdb_allows_eviction_with_extra_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let ns = "test-eviction-ns2";

        let labels = HashMap::from([("app".to_string(), "web".to_string())]);

        // Create 3 running pods
        for i in 1..=3 {
            let name = format!("pod-{}", i);
            let pod = make_pod(&name, ns, labels.clone(), true);
            let key = rusternetes_storage::build_key("pods", Some(ns), &name);
            storage.create(&key, &pod).await.unwrap();
        }

        // PDB with minAvailable=2 and 3 healthy pods => disruptions_allowed=1
        let pdb = make_pdb("pdb-extra", ns, 2, labels.clone());
        let pdb_key = rusternetes_storage::build_key("poddisruptionbudgets", Some(ns), "pdb-extra");
        storage.create(&pdb_key, &pdb).await.unwrap();

        let pdb_stored: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
        let disruptions = compute_pdb_disruptions_allowed(&*storage, &pdb_stored, ns).await;
        assert_eq!(
            disruptions, 1,
            "Should allow 1 disruption with 3 healthy pods and minAvailable=2"
        );
    }

    #[tokio::test]
    async fn test_pdb_no_status_still_blocks() {
        // This tests the key bug fix: PDB with no status should still block evictions
        let storage = Arc::new(MemoryStorage::new());
        let ns = "test-no-status-ns";

        let labels = HashMap::from([("app".to_string(), "web".to_string())]);

        // Create 2 running pods
        for i in 1..=2 {
            let name = format!("pod-{}", i);
            let pod = make_pod(&name, ns, labels.clone(), true);
            let key = rusternetes_storage::build_key("pods", Some(ns), &name);
            storage.create(&key, &pod).await.unwrap();
        }

        // PDB with minAvailable=2 and NO status set (freshly created, controller hasn't reconciled)
        let pdb = make_pdb("pdb-no-status", ns, 2, labels.clone());
        assert!(pdb.status.is_none(), "PDB should have no status initially");

        let pdb_key =
            rusternetes_storage::build_key("poddisruptionbudgets", Some(ns), "pdb-no-status");
        storage.create(&pdb_key, &pdb).await.unwrap();

        let pdb_stored: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
        let disruptions = compute_pdb_disruptions_allowed(&*storage, &pdb_stored, ns).await;
        assert_eq!(
            disruptions, 0,
            "PDB with no status should still block when minAvailable equals pod count"
        );
    }

    #[test]
    fn test_pod_matches_pdb_selector_basic() {
        let labels = HashMap::from([("app".to_string(), "web".to_string())]);
        let pod = make_pod("p1", "default", labels, true);
        let selector = LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
            match_expressions: None,
        };
        assert!(pod_matches_pdb_selector(&pod, &selector));

        let wrong_selector = LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "api".to_string())])),
            match_expressions: None,
        };
        assert!(!pod_matches_pdb_selector(&pod, &wrong_selector));
    }

    #[test]
    fn test_is_pod_healthy_checks_phase() {
        let labels = HashMap::new();
        let running_pod = make_pod("p1", "default", labels.clone(), true);
        assert!(is_pod_healthy(&running_pod));

        let pending_pod = make_pod("p2", "default", labels, false);
        assert!(!is_pod_healthy(&pending_pod));
    }

    #[test]
    fn test_compute_desired_healthy_min_available() {
        let labels = HashMap::from([("app".to_string(), "web".to_string())]);
        let pdb = make_pdb("pdb", "default", 3, labels);
        assert_eq!(compute_pdb_desired_healthy(&pdb, 5), 3);
    }

    #[test]
    fn test_compute_desired_healthy_percentage() {
        let pdb = PodDisruptionBudget {
            type_meta: TypeMeta {
                api_version: "policy/v1".to_string(),
                kind: "PodDisruptionBudget".to_string(),
            },
            metadata: ObjectMeta::new("pdb").with_namespace("default"),
            spec: PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::String("50%".to_string())),
                max_unavailable: None,
                selector: LabelSelector {
                    match_labels: Some(HashMap::new()),
                    match_expressions: None,
                },
                unhealthy_pod_eviction_policy: None,
            },
            status: None,
        };
        // 50% of 10 = 5
        assert_eq!(compute_pdb_desired_healthy(&pdb, 10), 5);
    }
}
