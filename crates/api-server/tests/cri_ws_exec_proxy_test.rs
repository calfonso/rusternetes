//! Socket-gated live verification of the CRI WebSocket exec proxy (#1256).
//!
//! Exercises [`cri_exec::open_exec_stream`] against a REAL containerd stream
//! server — the api-server↔runtime leg of interactive `kubectl exec` that
//! cannot be unit-tested (single-use stream tokens mean the only way to verify
//! the WebSocket handshake is to drive it with our own client).
//!
//! Skips unless the runtime env is provided. Recipe:
//! ```text
//! # containerd built with deploy/containerd/config.toml (stream 0.0.0.0:10010),
//! # CRI socket exposed to the host and a running container present:
//! docker run -d --privileged --name wsx \
//!   -v wsx-cd:/var/lib/containerd -v /tmp/wsrun:/run/containerd \
//!   -v $PWD/deploy/containerd/config.toml:/etc/containerd/config.toml:ro \
//!   -p 10010:10010 rusternetes-containerd:latest
//! # ... import an image into the k8s.io ns, runp+create+start a `sleep` ctr ...
//! RUSTERNETES_CRI_TEST_ENDPOINT=unix:///tmp/wsrun/containerd.sock \
//! RUSTERNETES_CRI_TEST_CONTAINER=<container-id> \
//! CONTAINERD_STREAM_HOST=127.0.0.1 CONTAINERD_STREAM_PORT=10010 \
//!   cargo test -p rusternetes-api-server --test cri_ws_exec_proxy_test -- --nocapture
//! ```

use futures::StreamExt;
use rusternetes_cri::stream as cri_exec;
use rusternetes_cri::CriClient;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn open_exec_stream_streams_stdout_from_live_containerd() {
    let (Ok(endpoint), Ok(container)) = (
        std::env::var("RUSTERNETES_CRI_TEST_ENDPOINT"),
        std::env::var("RUSTERNETES_CRI_TEST_CONTAINER"),
    ) else {
        eprintln!(
            "skipping: set RUSTERNETES_CRI_TEST_ENDPOINT + RUSTERNETES_CRI_TEST_CONTAINER \
             (+ CONTAINERD_STREAM_HOST/PORT) to run this live check"
        );
        return;
    };

    let mut cri = CriClient::connect(&endpoint)
        .await
        .expect("connect to CRI runtime");

    // Non-interactive exec: `echo` a sentinel; assert it streams back on ch1.
    let mut ws = cri_exec::open_exec_stream(
        &mut cri,
        &container,
        &["echo".to_string(), "HELLO_WS_PROXY".to_string()],
        false, // tty
        false, // stdin
    )
    .await
    .expect("open interactive exec stream to containerd");

    let mut stdout = Vec::new();
    while let Some(frame) = ws.next().await {
        match frame.expect("ws frame") {
            // Channel-framed: byte 0 = channel (1 = stdout, 2 = stderr, 3 = status).
            Message::Binary(b) if b.first() == Some(&1) => stdout.extend_from_slice(&b[1..]),
            Message::Close(_) => break,
            _ => {}
        }
    }

    let out = String::from_utf8_lossy(&stdout);
    assert!(
        out.contains("HELLO_WS_PROXY"),
        "expected echoed sentinel on channel 1; got {out:?}"
    );
}
