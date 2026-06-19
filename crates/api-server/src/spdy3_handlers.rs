//! SPDY/3.1 exec handler on the real wire codec ([`crate::spdy3`]), bridged to
//! the CRI exec WebSocket stream (the same one the WS exec proxy uses, #1258).
//!
//! kubectl SPDY exec opens one stream per `streamType` (stdin/stdout/stderr/
//! error/resize). The CRI runtime side is a single channel-framed WebSocket
//! (byte 0 = channel: stdin 0 / stdout 1 / stderr 2 / error 3 / resize 4). This
//! bridges between the two: SPDY stdin → CRI channel 0; CRI channels 1/2/3 →
//! the SPDY stdout/stderr/error streams. (#1264; codec ported from
//! indyjonesnl/containerd-rs in #1263, bridge mirrors its `streaming.rs`.)

use crate::cri_exec::{self, CriStream};
use crate::spdy3::{self, SpdyServer, ST_ERROR, ST_RESIZE, ST_STDERR, ST_STDIN, ST_STDOUT};
use futures::{SinkExt, StreamExt};
use rusternetes_common::resources::Pod;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMsg;
use tracing::{debug, error, info};

/// Collect the inbound remotecommand streams the client opens up front; returns
/// once the mandatory set (`error` + whatever was requested) has arrived or a
/// short idle timeout elapses. Verbatim shape from containerd-rs
/// `streaming.rs::collect_rc_streams`. Resize is drained best-effort (the CRI
/// stream carries tty resize on channel 4; forwarding it is a follow-up).
async fn collect_rc_streams<W>(
    server: &mut SpdyServer<W>,
    want_stdin: bool,
    want_stdout: bool,
    want_stderr: bool,
) -> (
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<mpsc::UnboundedReceiver<Vec<u8>>>,
)
where
    W: AsyncWrite + Unpin,
{
    let (mut error_id, mut stdout_id, mut stderr_id, mut stdin_rx) = (None, None, None, None);
    loop {
        let have = error_id.is_some()
            && (!want_stdout || stdout_id.is_some())
            && (!want_stdin || stdin_rx.is_some())
            && (!want_stderr || stderr_id.is_some());
        if have {
            break;
        }
        match tokio::time::timeout(Duration::from_secs(10), server.accept()).await {
            Ok(Some(stream)) => match stream.stream_type() {
                Some(ST_ERROR) => error_id = Some(stream.id),
                Some(ST_STDOUT) => stdout_id = Some(stream.id),
                Some(ST_STDERR) => stderr_id = Some(stream.id),
                Some(ST_STDIN) => stdin_rx = Some(stream.data),
                Some(ST_RESIZE) => {
                    let mut d = stream.data;
                    tokio::spawn(async move { while d.recv().await.is_some() {} });
                }
                _ => {}
            },
            _ => break, // peer closed or idle
        }
    }
    (error_id, stdout_id, stderr_id, stdin_rx)
}

/// Bridge an accepted SPDY exec connection to a CRI exec WebSocket. Forwards
/// SPDY stdin → CRI channel 0 (with a v5 channel-0 close on EOF) and demuxes CRI
/// channels 1/2/3 → the SPDY stdout/stderr/error streams. Returns when the CRI
/// stream closes (process exit), then FINs the error stream and sends GOAWAY.
pub async fn bridge_exec_streams<W>(
    mut server: SpdyServer<W>,
    runtime: CriStream,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let writer = server.writer.clone();
    // With a TTY, stdout+stderr are merged on the CRI side, so there is no
    // stderr stream.
    let want_stderr = stderr && !tty;
    let (error_id, stdout_id, stderr_id, stdin_rx) =
        collect_rc_streams(&mut server, stdin, stdout, want_stderr).await;

    let (mut ws_tx, mut ws_rx) = runtime.split();

    // SPDY stdin → CRI channel 0; on EOF send an empty channel-0 frame (v5
    // close-stream) so processes like `cat` see EOF.
    if let Some(mut rx) = stdin_rx {
        tokio::spawn(async move {
            while let Some(chunk) = rx.recv().await {
                let mut frame = Vec::with_capacity(chunk.len() + 1);
                frame.push(0u8);
                frame.extend_from_slice(&chunk);
                if ws_tx.send(WsMsg::Binary(frame)).await.is_err() {
                    return;
                }
            }
            let _ = ws_tx.send(WsMsg::Binary(vec![0u8])).await;
        });
    }

    // CRI → SPDY: demux the channel byte to the matching SPDY stream.
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(WsMsg::Binary(b)) if !b.is_empty() => {
                let target = match b[0] {
                    1 => stdout_id,
                    2 => stderr_id,
                    3 => error_id,
                    _ => None,
                };
                if let Some(id) = target {
                    if writer.send_data(id, false, &b[1..]).await.is_err() {
                        break;
                    }
                }
            }
            Ok(WsMsg::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    // CRI closed (exec finished): the channel-3 status was already forwarded;
    // half-close the error stream and tear the connection down.
    if let Some(eid) = error_id {
        let _ = writer.send_data(eid, true, &[]).await;
    }
    let _ = writer.goaway(0).await;
    debug!("SPDY exec bridge finished");
}

/// Full SPDY exec handler: serve SPDY over the upgraded `io`, resolve the
/// container, open the CRI exec stream, and bridge.
#[allow(clippy::too_many_arguments)]
pub async fn handle_spdy3_exec<S>(
    io: S,
    pod: Pod,
    container_name: String,
    command: Vec<String>,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    info!(
        "SPDY exec: pod={}, container={}, cmd={:?} (stdin={}, tty={})",
        pod.metadata.name, container_name, command, stdin, tty
    );
    let server = match spdy3::serve(io).await {
        Ok(s) => s,
        Err(e) => {
            error!("SPDY exec: serve failed: {e}");
            return;
        }
    };
    let mut cri = match cri_exec::connect().await {
        Ok(c) => c,
        Err(e) => {
            error!("SPDY exec: CRI connect failed: {e}");
            let _ = server.writer.goaway(0).await;
            return;
        }
    };
    let container_id = match cri_exec::resolve_container_id(&mut cri, &pod, &container_name).await {
        Ok(Some(id)) => id,
        _ => {
            error!("SPDY exec: container {container_name} not found");
            let _ = server.writer.goaway(0).await;
            return;
        }
    };
    let runtime =
        match cri_exec::open_exec_stream(&mut cri, &container_id, &command, tty, stdin).await {
            Ok(r) => r,
            Err(e) => {
                error!("SPDY exec: open exec stream failed: {e}");
                let _ = server.writer.goaway(0).await;
                return;
            }
        };
    bridge_exec_streams(server, runtime, stdin, stdout, stderr, tty).await;
}
