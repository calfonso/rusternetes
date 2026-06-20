//! Socket-gated live verification of the SPDY/3.1 exec bridge (#1264).
//!
//! Drives `spdy3_handlers::bridge_exec_streams` with a REAL SPDY client (built
//! from the `spdy3` wire codec itself) over an in-memory duplex, while the
//! bridge talks to a live containerd CRI exec stream. Verifies the full path:
//! SPDY SYN_STREAM/SYN_REPLY/DATA (zlib-dictionary headers) ↔ CRI WS channels.
//!
//! Skips unless the runtime env is provided (same containerd setup as the WS
//! exec test):
//! ```text
//! RUSTERNETES_CRI_TEST_ENDPOINT=unix:///tmp/wsrun/containerd.sock \
//! RUSTERNETES_CRI_TEST_CONTAINER=<running-container-id> \
//! CONTAINERD_STREAM_HOST=127.0.0.1 CONTAINERD_STREAM_PORT=10010 \
//!   cargo test -p rusternetes-api-server --test spdy3_exec_bridge_test -- --nocapture
//! ```

use rusternetes_api_server::cri_exec;
use rusternetes_api_server::spdy3::{self, parse_frame, write_frame, Frame, NvCodec};
use rusternetes_api_server::spdy3_handlers::bridge_exec_streams;
use rusternetes_cri::CriClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spdy_exec_bridge_streams_stdout_from_live_containerd() {
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

    // In-memory connection: client half <-> server half (what the api-server
    // would get from the HTTP/1.1 SPDY upgrade).
    let (client, server_io) = tokio::io::duplex(1 << 16);

    // Server side: open the CRI exec stream (echo a sentinel) and run the bridge.
    tokio::spawn(async move {
        let mut cri = CriClient::connect(&endpoint).await.expect("connect CRI");
        let runtime = cri_exec::open_exec_stream(
            &mut cri,
            &container,
            &["echo".to_string(), "HELLO_SPDY".to_string()],
            false, // tty
            false, // stdin
        )
        .await
        .expect("open CRI exec stream");
        let server = spdy3::serve(server_io).await.expect("serve SPDY");
        bridge_exec_streams(server, runtime, false, true, true, false).await;
    });

    // Client side: speak the SPDY codec. Open error(1)/stdout(3)/stderr(5)
    // streams, then read DATA frames and collect channel-3-equivalent stdout.
    let (mut rd, mut wr) = tokio::io::split(client);
    let mut nv = NvCodec::new();

    for (id, st) in [(1u32, "error"), (3, "stdout"), (5, "stderr")] {
        let mut buf = Vec::new();
        write_frame(
            &mut buf,
            &Frame::SynStream {
                stream_id: id,
                flags: 0,
                headers: vec![("streamType".to_string(), st.to_string())],
            },
            &mut nv,
        )
        .unwrap();
        wr.write_all(&buf).await.unwrap();
    }
    wr.flush().await.unwrap();

    // Read + parse frames until the echoed sentinel arrives on the stdout
    // stream (id 3) or the server goes away.
    let mut acc: Vec<u8> = Vec::new();
    let mut stdout: Vec<u8> = Vec::new();
    let mut rbuf = [0u8; 4096];
    let got = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            // Drain any complete frames already buffered.
            while let Some((frame, consumed)) = parse_frame(&acc, &mut nv).expect("parse") {
                acc.drain(..consumed);
                match frame {
                    Frame::Data {
                        stream_id: 3,
                        payload,
                        ..
                    } => stdout.extend_from_slice(&payload),
                    Frame::GoAway { .. } => return,
                    _ => {}
                }
                if String::from_utf8_lossy(&stdout).contains("HELLO_SPDY") {
                    return;
                }
            }
            match rd.read(&mut rbuf).await {
                Ok(0) => return,
                Ok(n) => acc.extend_from_slice(&rbuf[..n]),
                Err(_) => return,
            }
        }
    })
    .await;
    assert!(got.is_ok(), "timed out waiting for SPDY stdout");

    let out = String::from_utf8_lossy(&stdout);
    assert!(
        out.contains("HELLO_SPDY"),
        "expected echoed sentinel on the SPDY stdout stream; got {out:?}"
    );
}
