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
            Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                if !data.is_empty() && data[0] == 3 {
                    received_status = true;
                    status_content = String::from_utf8_lossy(&data[1..]).to_string();
                }
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
