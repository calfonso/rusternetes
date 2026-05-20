/// Integration test for WebSocket exec close sequence.
///
/// Verifies that the server sends the status message on channel 3
/// and waits before closing, so the client doesn't get
/// "connection reset by peer" errors.
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
    Router,
};
use futures::StreamExt;
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;

/// Simulate the exec close sequence from streaming.rs
async fn handle_test_ws(mut socket: WebSocket) {
    // Send initial stdout frame (channel 1)
    let _ = socket.send(Message::Binary(vec![1u8])).await;

    // Send stdout data
    let mut stdout = vec![1u8];
    stdout.extend_from_slice(b"hello\n");
    let _ = socket.send(Message::Binary(stdout)).await;

    // Send empty stdout/stderr frames
    let _ = socket.send(Message::Binary(vec![1u8])).await;
    let _ = socket.send(Message::Binary(vec![2u8])).await;

    // Flush with ping
    let _ = socket.send(Message::Ping(vec![])).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Send status on channel 3
    let status_json = r#"{"status":"Success"}"#;
    let mut status_data = vec![3u8];
    status_data.extend_from_slice(status_json.as_bytes());
    let _ = socket.send(Message::Binary(status_data)).await;

    // THE FIX: delay before close to let client read channel 3
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Close
    let close_frame = axum::extract::ws::CloseFrame {
        code: 1000,
        reason: "Success".to_string().into(),
    };
    let _ = socket.send(Message::Close(Some(close_frame))).await;
}

async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_test_ws)
}

#[tokio::test]
async fn test_exec_websocket_client_receives_status_before_close() {
    // Start a test WebSocket server
    let app = Router::new().route("/exec", get(ws_handler));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Connect as WebSocket client
    let url = format!("ws://127.0.0.1:{}/exec", addr.port());
    let (mut ws_stream, _) = connect_async(&url).await.expect("Failed to connect");

    // Collect all messages until close
    let mut received_stdout = false;
    let mut received_status = false;
    let mut status_content = String::new();

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                if data.is_empty() {
                    continue;
                }
                let channel = data[0];
                match channel {
                    1 => received_stdout = true,
                    3 => {
                        received_status = true;
                        status_content = String::from_utf8_lossy(&data[1..]).to_string();
                    }
                    _ => {}
                }
            }
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                break;
            }
            Ok(tokio_tungstenite::tungstenite::Message::Ping(_)) => {
                // Respond to ping with pong (tungstenite does this automatically)
            }
            Err(_) => break,
            _ => {}
        }
    }

    assert!(received_stdout, "Client should receive stdout on channel 1");
    assert!(
        received_status,
        "Client should receive status on channel 3 BEFORE connection close"
    );
    assert!(
        status_content.contains("Success"),
        "Status should contain Success, got: {}",
        status_content
    );
}

#[tokio::test]
async fn test_exec_websocket_nonzero_exit_status() {
    async fn handle_fail_ws(mut socket: WebSocket) {
        let _ = socket.send(Message::Binary(vec![1u8])).await;

        // Send failure status on channel 3
        let status_json = r#"{"status":"Failure","message":"command terminated with exit code 1"}"#;
        let mut status_data = vec![3u8];
        status_data.extend_from_slice(status_json.as_bytes());
        let _ = socket.send(Message::Binary(status_data)).await;

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let close_frame = axum::extract::ws::CloseFrame {
            code: 1000,
            reason: "NonZeroExitCode".to_string().into(),
        };
        let _ = socket.send(Message::Close(Some(close_frame))).await;
    }

    async fn fail_handler(ws: WebSocketUpgrade) -> Response {
        ws.on_upgrade(handle_fail_ws)
    }

    let app = Router::new().route("/exec", get(fail_handler));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let url = format!("ws://127.0.0.1:{}/exec", addr.port());
    let (mut ws_stream, _) = connect_async(&url).await.expect("Failed to connect");

    let mut received_status = false;
    let mut status_content = String::new();

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Binary(data))
                if !data.is_empty() && data[0] == 3 =>
            {
                received_status = true;
                status_content = String::from_utf8_lossy(&data[1..]).to_string();
            }
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
            Err(_) => break,
            _ => {}
        }
    }

    assert!(
        received_status,
        "Client should receive failure status on channel 3"
    );
    assert!(
        status_content.contains("Failure"),
        "Status should contain Failure, got: {}",
        status_content
    );
    assert!(
        status_content.contains("exit code 1"),
        "Status should mention exit code"
    );
}

/// `streaming::frame_channel` must prefix exactly one channel byte to the
/// payload. This is the wire-format invariant the log-over-websocket path
/// (pods.go:583) and exec stdout/stderr paths share — the same encoder is
/// reused so a regression here breaks both consumers at once.
#[test]
fn frame_channel_prefixes_byte_to_payload() {
    let payload = b"hello world\n";

    let stdout = rusternetes_api_server::streaming::frame_channel(1, payload);
    assert_eq!(stdout[0], 1, "channel 1 byte (stdout) must come first");
    assert_eq!(&stdout[1..], payload, "rest of frame is the raw payload");
    assert_eq!(
        stdout.len(),
        payload.len() + 1,
        "exactly one byte prepended"
    );

    let stderr = rusternetes_api_server::streaming::frame_channel(2, payload);
    assert_eq!(stderr[0], 2, "channel 2 byte (stderr) routes the same way");

    // Empty payload: still produces a single-byte channel-only frame
    // (this is what `handle_ws_exec` sends as a "stdout opened" marker).
    let empty = rusternetes_api_server::streaming::frame_channel(1, &[]);
    assert_eq!(empty, vec![1u8], "empty payload → single channel byte");
}

// ---- handle_ws_logs subprotocol contract -----------------------------------
//
// Upstream Kubernetes treats the log subresource as a single byte stream and
// negotiates one of two subprotocols:
//
//   * `binary.k8s.io`        — raw bytes, no channel prefix.
//                              "The received messages are the exact bytes
//                              written to the stream."
//                              (k8s.io/streaming/pkg/httpstream/wsstream/stream.go)
//
//   * `base64.binary.k8s.io` — base64-encoded raw, sent as a Text frame
//                              whose body is the base64 of the payload.
//
// The upstream conformance test `Pods should support retrieving logs from the
// container over websockets` (pods.go:583) offers ONLY `binary.k8s.io` and
// asserts `buf.String() == "container is alive\n"` over the accumulated read
// bytes — a channel byte prefix would corrupt that assertion. The three
// tests below pin each branch of `handle_ws_logs` against the corresponding
// upstream subprotocol contract.

async fn drain_ws(
    mut ws_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> (Vec<Vec<u8>>, Vec<String>, bool) {
    use futures::StreamExt;
    let mut bins: Vec<Vec<u8>> = Vec::new();
    let mut texts: Vec<String> = Vec::new();
    let mut closed = false;
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Binary(d)) => bins.push(d),
            Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => texts.push(t),
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                closed = true;
                break;
            }
            Err(_) => break,
            _ => {}
        }
    }
    (bins, texts, closed)
}

async fn start_logs_server(logs: &'static str) -> u16 {
    async fn logs_handler(ws: WebSocketUpgrade, logs: String) -> Response {
        ws.protocols(["binary.k8s.io", "base64.binary.k8s.io"])
            .on_upgrade(move |socket| async move {
                rusternetes_api_server::streaming::handle_ws_logs(socket, logs).await;
            })
    }
    let app = Router::new().route("/log", get(move |ws| logs_handler(ws, logs.to_string())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    port
}

fn ws_request_with_protocol(
    url: &str,
    subprotocol: &str,
) -> tokio_tungstenite::tungstenite::http::Request<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        tokio_tungstenite::tungstenite::http::HeaderValue::from_str(subprotocol).unwrap(),
    );
    req
}

/// REGRESSION GUARD for upstream pods.go:583.
///
/// Client offers only `binary.k8s.io`; the server must reply with that
/// subprotocol and send the log buffer as raw bytes — NO channel byte
/// prefix — in `Message::Binary`. A channel-1 prefix here would corrupt
/// the first byte of the upstream test's accumulated buffer and fail
/// the `buf.String() == "container is alive\n"` assertion.
#[tokio::test]
async fn handle_ws_logs_with_binary_k8s_io_sends_raw_payload_no_channel_byte() {
    let port = start_logs_server("container is alive\n").await;
    let url = format!("ws://127.0.0.1:{port}/log");
    let req = ws_request_with_protocol(&url, "binary.k8s.io");
    let (ws_stream, resp) = connect_async(req).await.expect("connect");

    // Server must echo the negotiated subprotocol back.
    let negotiated = resp
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    assert_eq!(negotiated, "binary.k8s.io");

    let (bins, texts, closed) = drain_ws(ws_stream).await;
    assert!(closed, "server must close cleanly with 1000");
    assert!(
        texts.is_empty(),
        "binary.k8s.io: must NOT emit any Text frames"
    );
    assert!(
        !bins.is_empty(),
        "binary.k8s.io: must emit at least one Binary frame"
    );

    // Concatenate all binary frames (upstream's read loop accumulates).
    let accumulated: Vec<u8> = bins.into_iter().flatten().collect();
    assert_eq!(
        accumulated, b"container is alive\n",
        "binary.k8s.io: raw bytes, no channel byte prefix"
    );
}

/// `base64.binary.k8s.io`: server must send the base64-encoded log payload
/// as a `Message::Text` (per upstream wsstream Reader, the base64 subprotocol
/// uses text frames with base64 bodies). No channel byte either way.
#[tokio::test]
async fn handle_ws_logs_with_base64_binary_k8s_io_sends_text_base64_payload() {
    let port = start_logs_server("container is alive\n").await;
    let url = format!("ws://127.0.0.1:{port}/log");
    let req = ws_request_with_protocol(&url, "base64.binary.k8s.io");
    let (ws_stream, resp) = connect_async(req).await.expect("connect");

    let negotiated = resp
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    assert_eq!(negotiated, "base64.binary.k8s.io");

    let (bins, texts, closed) = drain_ws(ws_stream).await;
    assert!(closed, "server must close cleanly with 1000");
    assert!(
        bins.is_empty(),
        "base64.binary.k8s.io: must NOT emit any Binary frames"
    );
    assert_eq!(
        texts.len(),
        1,
        "base64.binary.k8s.io: exactly one Text frame with the base64 body"
    );

    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&texts[0])
        .expect("body must be valid base64");
    assert_eq!(decoded, b"container is alive\n");
}

/// When the client offers no subprotocol — or offers something the server
/// does not support — the WebSocket still upgrades without a negotiated
/// subprotocol. Per upstream wsstream's `NewDefaultReaderProtocols`, the
/// empty default `""` maps to `{Binary: true}`, i.e. behaves as raw binary.
/// `handle_ws_logs` falls into the same raw-bytes branch.
#[tokio::test]
async fn handle_ws_logs_with_no_negotiated_subprotocol_falls_back_to_raw_bytes() {
    let port = start_logs_server("plain stream\n").await;
    let url = format!("ws://127.0.0.1:{port}/log");
    // connect_async without a subprotocol header: client offers none.
    let (ws_stream, _resp) = connect_async(&url).await.expect("connect");

    let (bins, texts, closed) = drain_ws(ws_stream).await;
    assert!(closed);
    assert!(texts.is_empty(), "default: no Text frames");
    let accumulated: Vec<u8> = bins.into_iter().flatten().collect();
    assert_eq!(accumulated, b"plain stream\n", "default: raw bytes");
}
