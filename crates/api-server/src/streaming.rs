//! WebSocket streaming support for logs and port-forward.
//!
//! exec and attach are now upgrade-proxied to the pod's kubelet
//! via `rusternetes_streamproxy::proxy_upgrade` (Task 6).

use axum::extract::ws::{Message, WebSocket};
use rusternetes_common::resources::Pod;
use tracing::{debug, info};

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
