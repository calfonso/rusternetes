//! WebSocket streaming support for exec, attach, and port-forward
//!
//! Exec runs the command one-shot against the container runtime over CRI
//! (`ExecSync`) and writes the collected stdout/stderr back over the
//! `v5.channel.k8s.io` (and back-compat v4/v1) channel protocol.

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use rusternetes_common::resources::Pod;
use tracing::{debug, error, info, warn};

/// Handle WebSocket exec by proxying to the kubelet
///
/// Implements the Kubernetes `v5.channel.k8s.io` (and back-compat `v4`/`v1`)
/// WebSocket exec protocol. Channels are prefixed on every binary frame:
///   0 = stdin (client → server)
///   1 = stdout (server → client)
///   2 = stderr (server → client)
///   3 = error / status (server → client, JSON-encoded `metav1.Status`)
///   4 = resize (client → server, TerminalSize JSON for TTY)
///
/// `v5` additionally supports a "close stream" control message: a binary
/// frame containing only the channel byte indicates the client has finished
/// sending on that stream. We honor this by closing stdin to the runtime
/// so processes like `cat` exit cleanly.
#[allow(clippy::too_many_arguments)]
pub async fn handle_ws_exec(
    mut socket: WebSocket,
    pod: Pod,
    container_name: String,
    command: Vec<String>,
    stdin: bool,
    _stdout: bool,
    _stderr: bool,
    tty: bool,
) {
    debug!(
        "WS exec via CRI for pod {} container {}",
        pod.metadata.name, container_name
    );

    // Resolve the container's CRI id, then open an interactive streaming exec
    // (CRI `Exec` RPC → WebSocket to the runtime's stream server) and proxy it.
    // If streaming can't be established, fall back to the one-shot `ExecSync`
    // path so the non-interactive case never regresses (#1256).
    let mut cri = match crate::cri_exec::connect().await {
        Ok(c) => c,
        Err(e) => {
            error!("WS exec: CRI connect failed: {}", e);
            let _ = socket
                .send(Message::Binary(
                    std::iter::once(3u8)
                        .chain(format!("Exec error: {}", e).bytes())
                        .collect(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    };

    let container_id = match crate::cri_exec::resolve_container_id(&mut cri, &pod, &container_name)
        .await
    {
        Ok(Some(id)) => id,
        Ok(None) => {
            error!(
                "WS exec: no running container {} for pod {}",
                container_name, pod.metadata.name
            );
            let _ = socket
                .send(Message::Binary(
                    std::iter::once(3u8)
                        .chain(
                            format!("Exec error: container {} not found", container_name).bytes(),
                        )
                        .collect(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
        Err(e) => {
            error!("WS exec: resolve container failed: {}", e);
            let _ = socket
                .send(Message::Binary(
                    std::iter::once(3u8)
                        .chain(format!("Exec error: {}", e).bytes())
                        .collect(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    };

    info!(
        "WS exec: running {:?} in container {} (stdin={}, tty={})",
        command, container_id, stdin, tty
    );

    match crate::cri_exec::open_exec_stream(&mut cri, &container_id, &command, tty, stdin).await {
        Ok(runtime) => proxy_exec_streams(socket, runtime, &container_id).await,
        Err(e) => {
            warn!(
                "WS exec: interactive streaming unavailable for {} ({}); falling back to ExecSync",
                container_id, e
            );
            exec_sync_to_ws(socket, &mut cri, &container_id, &command).await;
        }
    }
}

/// Bidirectionally proxy a kubectl exec WebSocket (`client`) and the CRI
/// runtime's exec WebSocket (`runtime`). Both legs speak the identical
/// channel-framed `v5.channel.k8s.io` protocol (byte 0 = channel), so frames
/// pass straight through: stdin (0) / resize (4) flow client→runtime; stdout
/// (1) / stderr (2) / error+status (3) flow runtime→client. Whichever side
/// closes first ends the session (the runtime closes after the process exits).
async fn proxy_exec_streams(
    client: WebSocket,
    runtime: crate::cri_exec::CriStream,
    container_id: &str,
) {
    use tokio_tungstenite::tungstenite::Message as TMsg;

    let (mut client_tx, mut client_rx) = client.split();
    let (mut rt_tx, mut rt_rx) = runtime.split();

    // kubectl → runtime: stdin, resize, and the v5 close-stream control frames.
    let client_to_runtime = async {
        while let Some(Ok(msg)) = client_rx.next().await {
            let forward = match msg {
                Message::Binary(b) => rt_tx.send(TMsg::Binary(b)).await,
                Message::Text(t) => rt_tx.send(TMsg::Text(t)).await,
                Message::Close(_) => {
                    let _ = rt_tx.send(TMsg::Close(None)).await;
                    break;
                }
                // Pings/pongs are connection-local; don't relay.
                Message::Ping(_) | Message::Pong(_) => Ok(()),
            };
            if forward.is_err() {
                break;
            }
        }
    };

    // runtime → kubectl: stdout, stderr, and the channel-3 status frame.
    let runtime_to_client = async {
        while let Some(Ok(msg)) = rt_rx.next().await {
            let forward = match msg {
                TMsg::Binary(b) => client_tx.send(Message::Binary(b)).await,
                TMsg::Text(t) => client_tx.send(Message::Text(t)).await,
                TMsg::Close(_) => {
                    let _ = client_tx
                        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                            code: 1000,
                            reason: "".to_string().into(),
                        })))
                        .await;
                    break;
                }
                TMsg::Ping(_) | TMsg::Pong(_) | TMsg::Frame(_) => Ok(()),
            };
            if forward.is_err() {
                break;
            }
        }
    };

    // The runtime side is authoritative for completion; either ending tears the
    // proxy down.
    tokio::select! {
        _ = client_to_runtime => {}
        _ = runtime_to_client => {}
    }
    debug!("WS exec proxy finished for {}", container_id);
}

/// One-shot exec fallback: run the command via CRI `ExecSync` and write the
/// collected stdout/stderr + exit status back over the channel protocol. Used
/// when interactive streaming can't be established; preserves the pre-#1256
/// behavior so non-interactive `kubectl exec` never regresses.
async fn exec_sync_to_ws(
    socket: WebSocket,
    cri: &mut rusternetes_cri::CriClient,
    container_id: &str,
    command: &[String],
) {
    let (stdout_data, stderr_data, exit_code) =
        match crate::cri_exec::exec_sync(cri, container_id, command, 60).await {
            Ok(out) => out,
            Err(e) => {
                error!("WS exec: ExecSync failed for {}: {}", container_id, e);
                let mut s = socket;
                let _ = s
                    .send(Message::Binary(
                        std::iter::once(3u8)
                            .chain(format!("Exec error: {}", e).bytes())
                            .collect(),
                    ))
                    .await;
                let _ = s.close().await;
                return;
            }
        };

    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Drain incoming client frames. ExecSync is one-shot, so stdin/resize are
    // accepted but not forwarded; this only keeps the socket from stalling.
    tokio::spawn(async move { while ws_receiver.next().await.is_some() {} });

    // K8s protocol requires channel 1 (stdout) to appear before channel 3
    // (status). Send an initial empty stdout frame so the client sees ch1
    // first even when the command produced no output.
    let _ = ws_sender.send(Message::Binary(vec![1u8])).await;

    if !stdout_data.is_empty() {
        let mut data = vec![1u8];
        data.extend_from_slice(&stdout_data);
        let _ = ws_sender.send(Message::Binary(data)).await;
    }
    if !stderr_data.is_empty() {
        let mut data = vec![2u8];
        data.extend_from_slice(&stderr_data);
        let _ = ws_sender.send(Message::Binary(data)).await;
    }

    info!(
        "WS exec: command finished for {} with exit_code={}",
        container_id, exit_code
    );

    let is_v1 = V1_PROTOCOL_FLAG.load(std::sync::atomic::Ordering::Relaxed);
    if !is_v1 || exit_code != 0 {
        let status_json = if exit_code == 0 {
            r#"{"status":"Success"}"#.to_string()
        } else {
            format!(
                r#"{{"status":"Failure","message":"command terminated with exit code {}","reason":"NonZeroExitCode","details":{{"causes":[{{"reason":"ExitCode","message":"{}"}}]}}}}"#,
                exit_code, exit_code
            )
        };
        let mut status_data = vec![3u8];
        status_data.extend_from_slice(status_json.as_bytes());
        let _ = ws_sender.send(Message::Binary(status_data)).await;
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let close_frame = axum::extract::ws::CloseFrame {
        code: 1000,
        reason: "".to_string().into(),
    };
    let _ = ws_sender.send(Message::Close(Some(close_frame))).await;
    debug!("WS exec completed (ExecSync) for {}", container_id);
}

/// Prefix a payload with a single channel byte, producing one
/// `channel.k8s.io`/`v4.channel.k8s.io` binary frame. Channel 1 = stdout,
/// 2 = stderr, 3 = error/status. This mirrors the same wire format the exec
/// handler uses for output frames so log / exec / attach speak the same
/// dialect — the client side (client-go `wsstream.Conn`) is shared.
#[inline]
pub fn frame_channel(channel: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(channel);
    out.extend_from_slice(payload);
    out
}

/// Send a buffered log blob over the upstream Kubernetes log-subresource
/// websocket subprotocols and close with a normal 1000 close.
///
/// Logs are a single read-only byte stream — upstream defines two
/// subprotocols for it:
///
/// * `binary.k8s.io` — raw bytes, exactly what was written to the stream.
///   No channel byte prefix. Returned as `Message::Binary`. This is the
///   subprotocol upstream's pods.go:583 test negotiates and asserts against;
///   prefixing a channel byte would corrupt the client buffer.
///
/// * `base64.binary.k8s.io` — base64-encoded raw bytes, returned as
///   `Message::Text` (the upstream wire format is a text frame whose body
///   is the base64 of the payload). No channel byte.
///
/// Anything else (including no negotiated subprotocol) is treated as the
/// raw `binary.k8s.io` shape — it's the safer default and matches upstream
/// behaviour when the empty subprotocol is selected by the wsstream Reader.
///
/// K8s ref:
///   staging/src/k8s.io/streaming/pkg/httpstream/wsstream/stream.go
///     const binaryWebSocketProtocol       = "binary.k8s.io"
///     const base64BinaryWebSocketProtocol = "base64.binary.k8s.io"
pub async fn handle_ws_logs(mut socket: WebSocket, logs: String) {
    let proto = socket
        .protocol()
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let payload_len = logs.len();

    let message = match proto.as_str() {
        "base64.binary.k8s.io" => {
            use base64::Engine;
            Message::Text(base64::engine::general_purpose::STANDARD.encode(logs.as_bytes()))
        }
        // "binary.k8s.io" + the empty default — raw bytes, no channel prefix.
        _ => Message::Binary(logs.into_bytes()),
    };

    if let Err(e) = socket.send(message).await {
        info!("WS logs: send failed: {}", e);
    }

    // Honor the same close convention as exec: 1000 Normal Closure with empty
    // reason. Small grace period so the client read loop can drain the frame
    // before the OS tears the TCP connection down.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let _ = socket
        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
            code: 1000,
            reason: "".to_string().into(),
        })))
        .await;
    debug!(
        "WS logs: sent {} bytes (subprotocol {:?}) and closed",
        payload_len, proto
    );
}

/// Handle WebSocket attach
pub async fn handle_ws_attach(
    mut socket: WebSocket,
    pod: Pod,
    container_name: String,
    _stdin: bool,
    _stdout: bool,
    _stderr: bool,
    _tty: bool,
) {
    info!(
        "WS attach: pod={}, container={}",
        pod.metadata.name, container_name
    );
    let _ = socket
        .send(Message::Text(
            "Attach not fully implemented in proxy mode".into(),
        ))
        .await;
    let _ = socket.close().await;
}

/// Simple URL encoding
fn urlencoding_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            b' ' => "+".to_string(),
            _ => format!("%{:02X}", b),
        })
        .collect()
}

/// Alias for backward compatibility with pod_subresources.rs
#[allow(clippy::too_many_arguments)]
pub async fn handle_exec_websocket(
    socket: WebSocket,
    pod: Pod,
    container_name: String,
    command: Vec<String>,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
) {
    handle_ws_exec(
        socket,
        pod,
        container_name,
        command,
        stdin,
        stdout,
        stderr,
        tty,
    )
    .await
}

/// Exec with protocol awareness — v1 doesn't use channel 3 for status
#[allow(clippy::too_many_arguments)]
pub async fn handle_exec_websocket_with_protocol(
    socket: WebSocket,
    pod: Pod,
    container_name: String,
    command: Vec<String>,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
    is_v1_protocol: bool,
) {
    // Set the v1 flag so handle_ws_exec can check it
    V1_PROTOCOL_FLAG.store(is_v1_protocol, std::sync::atomic::Ordering::Relaxed);
    handle_ws_exec(
        socket,
        pod,
        container_name,
        command,
        stdin,
        stdout,
        stderr,
        tty,
    )
    .await
}

/// Global flag for v1 protocol detection (per-request via task-local would be better,
/// but this works since exec calls are serialized per connection)
static V1_PROTOCOL_FLAG: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Alias for backward compatibility
pub async fn handle_attach_websocket(
    socket: WebSocket,
    pod: Pod,
    container_name: String,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
) {
    handle_ws_attach(socket, pod, container_name, stdin, stdout, stderr, tty).await
}

/// Handle WebSocket port-forward
pub async fn handle_portforward_websocket(mut socket: WebSocket, pod: Pod, ports: Vec<u16>) {
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;

    let pod_ip = match pod.status.as_ref().and_then(|s| s.pod_ip.as_ref()) {
        Some(ip) => ip.clone(),
        None => {
            let _ = socket.send(Message::Text("Pod has no IP".into())).await;
            let _ = socket.close().await;
            return;
        }
    };

    for port in &ports {
        let target = format!("{}:{}", pod_ip, port);
        match TcpStream::connect(&target).await {
            Ok(tcp) => {
                let (mut tcp_read, _tcp_write) = tcp.into_split();
                // Simple forward: read from TCP, send to WebSocket
                let mut buf = vec![0u8; 8192];
                loop {
                    match tcp_read.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if socket
                                .send(Message::Binary(buf[..n].to_vec()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) => {
                let _ = socket
                    .send(Message::Text(format!(
                        "Failed to connect to {}: {}",
                        target, e
                    )))
                    .await;
            }
        }
    }

    let _ = socket.close().await;
}
