//! Socket-gated live verification of the CRI WebSocket attach proxy (#1256).
//!
//! Exercises [`cri_exec::open_attach_stream`] against a REAL containerd stream
//! server: attaches to a container's running stdio, writes a line on the stdin
//! channel (0), and asserts it is echoed back on the stdout channel (1).
//!
//! Requires a container whose pid 1 echoes stdin (e.g. `cat`) created with
//! `stdin: true`. Skips unless the runtime env is provided. Recipe:
//! ```text
//! # containerd from deploy/containerd/config.toml (stream 0.0.0.0:10010),
//! # CRI socket exposed to host, a `cat` container with stdin:true running:
//! #   crictl create config: {"command":["cat"],"stdin":true, ...}
//! RUSTERNETES_CRI_TEST_ENDPOINT=unix:///tmp/wsrun/containerd.sock \
//! RUSTERNETES_CRI_TEST_ATTACH_CONTAINER=<container-id> \
//! CONTAINERD_STREAM_HOST=127.0.0.1 CONTAINERD_STREAM_PORT=10010 \
//!   cargo test -p rusternetes-api-server --test cri_ws_attach_proxy_test -- --nocapture
//! ```

use futures::{SinkExt, StreamExt};
use rusternetes_cri::stream as cri_exec;
use rusternetes_cri::CriClient;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn open_attach_stream_echoes_stdin_on_stdout_from_live_containerd() {
    let (Ok(endpoint), Ok(container)) = (
        std::env::var("RUSTERNETES_CRI_TEST_ENDPOINT"),
        std::env::var("RUSTERNETES_CRI_TEST_ATTACH_CONTAINER"),
    ) else {
        eprintln!(
            "skipping: set RUSTERNETES_CRI_TEST_ENDPOINT + RUSTERNETES_CRI_TEST_ATTACH_CONTAINER \
             (a `cat`-with-stdin container) + CONTAINERD_STREAM_HOST/PORT to run this live check"
        );
        return;
    };

    let mut cri = CriClient::connect(&endpoint)
        .await
        .expect("connect to CRI runtime");

    // Attach to the container's stdio with stdin enabled.
    let mut ws = cri_exec::open_attach_stream(
        &mut cri, &container, /*tty*/ false, /*stdin*/ true,
    )
    .await
    .expect("open interactive attach stream to containerd");

    // Write a line on the stdin channel (byte 0 = stdin).
    let mut stdin_frame = vec![0u8];
    stdin_frame.extend_from_slice(b"PING_ATTACH\n");
    ws.send(Message::Binary(stdin_frame))
        .await
        .expect("send stdin frame");

    // Collect stdout (channel 1) until the echoed sentinel arrives (or timeout).
    let mut stdout = Vec::new();
    let deadline = tokio::time::Duration::from_secs(10);
    let _ = tokio::time::timeout(deadline, async {
        while let Some(frame) = ws.next().await {
            match frame.expect("ws frame") {
                Message::Binary(b) if b.first() == Some(&1) => {
                    stdout.extend_from_slice(&b[1..]);
                    if String::from_utf8_lossy(&stdout).contains("PING_ATTACH") {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    })
    .await;

    let out = String::from_utf8_lossy(&stdout);
    assert!(
        out.contains("PING_ATTACH"),
        "expected stdin echoed back on channel 1; got {out:?}"
    );
}
