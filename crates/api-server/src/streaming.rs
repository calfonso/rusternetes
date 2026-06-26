//! WebSocket streaming support for logs and port-forward.
//!
//! exec and attach are now upgrade-proxied to the pod's kubelet
//! via `rusternetes_streamproxy::proxy_upgrade` (Task 6).

use axum::extract::ws::{Message, WebSocket};
use rusternetes_common::resources::Pod;

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
