//! WebSocket streaming support for exec, attach, and port-forward
//!
//! Proxies exec requests to the kubelet's HTTP endpoint,
//! keeping the API server runtime-agnostic.

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use rusternetes_common::resources::Pod;
use tracing::{debug, error, info};

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
    let container_id = format!("{}_{}", pod.metadata.name, container_name);

    debug!("WS exec direct Docker for container: {}", container_id);

    // Execute directly via Docker/Podman (API server has container socket mounted)
    use bollard::exec::{CreateExecOptions, StartExecResults};
    use bollard::Docker;

    // Use a shared Docker client to avoid connection issues from creating
    // a new client per exec call.
    static DOCKER_CLIENT: std::sync::OnceLock<Docker> = std::sync::OnceLock::new();
    let docker = DOCKER_CLIENT.get_or_init(|| {
        Docker::connect_with_local_defaults().expect("Failed to connect to container runtime")
    });
    info!(
        "WS exec: using container runtime client for {} (stdin={}, tty={})",
        container_id, stdin, tty
    );

    let exec_config = CreateExecOptions {
        cmd: Some(command.iter().map(|s| s.as_str()).collect()),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        attach_stdin: Some(stdin),
        tty: Some(tty),
        ..Default::default()
    };

    let exec = match docker.create_exec(&container_id, exec_config).await {
        Ok(e) => {
            info!("WS exec: created exec {} for {}", e.id, container_id);
            e
        }
        Err(e) => {
            error!("WS exec: create_exec failed for {}: {}", container_id, e);
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

    let output = match docker
        .start_exec(
            &exec.id,
            Some(bollard::exec::StartExecOptions {
                detach: false,
                ..Default::default()
            }),
        )
        .await
    {
        Ok(o) => o,
        Err(e) => {
            let _ = socket
                .send(Message::Binary(
                    std::iter::once(3u8)
                        .chain(format!("Start exec error: {}", e).bytes())
                        .collect(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    };

    // Split WebSocket into sender and receiver so we can read client messages
    // (stdin, close) concurrently with writing exec output.
    let (mut ws_sender, mut ws_receiver) = socket.split();

    let (mut output_stream, exec_input) = match output {
        StartExecResults::Attached { output, input } => (output, Some(input)),
        StartExecResults::Detached => {
            // No streams to attach — just send a Success status and close.
            let mut status_data = vec![3u8];
            status_data.extend_from_slice(br#"{"status":"Success"}"#);
            let _ = ws_sender.send(Message::Binary(status_data)).await;
            let _ = ws_sender
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1000,
                    reason: "".to_string().into(),
                })))
                .await;
            return;
        }
    };

    // Spawn a task to drain incoming WebSocket messages and forward stdin to
    // the exec process. Without this drain, client pings/close frames stall
    // the connection. v5 also defines a "close stream" message (just the
    // channel byte) which we honor by dropping the writer half for stdin.
    let client_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let client_closed2 = client_closed.clone();
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let mut exec_input = exec_input;
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(Message::Close(_)) | Err(_) => {
                    client_closed2.store(true, std::sync::atomic::Ordering::Relaxed);
                    if let Some(mut w) = exec_input.take() {
                        let _ = w.shutdown().await;
                    }
                    break;
                }
                Ok(Message::Binary(data)) if !data.is_empty() => {
                    let channel = data[0];
                    let payload = &data[1..];
                    // Channel 0 is stdin; channels 1-3 are server→client only,
                    // channel 4 (resize) is accepted but not acted on since
                    // bollard doesn't expose resize_exec here.
                    if channel == 0 {
                        if payload.is_empty() {
                            // v5 close-stream signal for stdin
                            if let Some(mut w) = exec_input.take() {
                                let _ = w.shutdown().await;
                            }
                        } else if let Some(w) = exec_input.as_mut() {
                            if w.write_all(payload).await.is_err() {
                                let _ = w.shutdown().await;
                                exec_input = None;
                            } else {
                                let _ = w.flush().await;
                            }
                        }
                    }
                }
                _ => {} // ignore text frames, pings, pongs
            }
        }
    });

    // Stream output to WebSocket using v5.channel.k8s.io protocol
    // Channel prefix: 0=stdin, 1=stdout, 2=stderr, 3=error
    // K8s protocol requires channel 1 (stdout) to appear before channel 3 (status).
    // Send an initial empty stdout frame so the client sees ch1 first, even if the
    // exec command produces no output or finishes before we read from the stream.
    let _ = ws_sender.send(Message::Binary(vec![1u8])).await;

    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(1), output_stream.next()).await {
            Ok(Some(Ok(msg))) => match msg {
                bollard::container::LogOutput::StdOut { message } => {
                    let mut data = vec![1u8]; // stdout channel
                    data.extend_from_slice(&message);
                    if ws_sender.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
                bollard::container::LogOutput::StdErr { message } => {
                    let mut data = vec![2u8]; // stderr channel
                    data.extend_from_slice(&message);
                    if ws_sender.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
                // Some runtimes (TTY mode) deliver everything as Console.
                // Treat console output as stdout for client compatibility.
                bollard::container::LogOutput::Console { message } => {
                    let mut data = vec![1u8];
                    data.extend_from_slice(&message);
                    if ws_sender.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
                _ => {}
            },
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {
                // 1s timeout hit — check if command finished
                if let Ok(info) = docker.inspect_exec(&exec.id).await {
                    if !info.running.unwrap_or(false) {
                        break;
                    }
                } else {
                    break;
                }
                // Also bail if client disconnected
                if client_closed.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }
        }
    }

    // Send exit code as status on error channel (channel 3).
    // Only send for v4/v5 protocols — v1 (channel.k8s.io) doesn't use
    // the status channel and clients fail if they see non-stdout data.
    // K8s ref: staging/src/k8s.io/client-go/tools/remotecommand/v4.go
    let exit_code = docker
        .inspect_exec(&exec.id)
        .await
        .ok()
        .and_then(|info| info.exit_code)
        .unwrap_or(0);
    info!(
        "WS exec: command finished for {} with exit_code={}",
        container_id, exit_code
    );

    let is_v1 = V1_PROTOCOL_FLAG.load(std::sync::atomic::Ordering::Relaxed);
    if !is_v1 || exit_code != 0 {
        // v4/v5: always send status. v1: only send for non-zero exit (error reporting).
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

    // Send proper close frame. The client (client-go) expects a 1000 close after
    // receiving status on channel 3. Wait briefly then close.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let close_frame = axum::extract::ws::CloseFrame {
        code: 1000,
        reason: "".to_string().into(),
    };
    let _ = ws_sender.send(Message::Close(Some(close_frame))).await;
    debug!("WS exec completed for {}", container_id);
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
