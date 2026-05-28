//! Test helpers shared across the netstack crate's `#[cfg(test)]`
//! modules. Lives at the crate root so `runtime`, `manager`, and any
//! future test module can pull from one place instead of redefining
//! their own fake-TAP each time.

use crate::runtime::PodIo;
use async_trait::async_trait;
use std::io;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Channel-backed fake TAP. The runtime sees a normal [`PodIo`]; the
/// test drives bytes via [`FakeTapHandle::send_in`] and observes
/// outbound bytes via [`FakeTapHandle::recv_out`]. No kernel TAP, no
/// `CAP_NET_ADMIN`.
pub struct FakeTap {
    inbox: Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
    outbox: mpsc::UnboundedSender<Vec<u8>>,
}

/// Paired-with-[`FakeTap`] handle for the test side.
pub struct FakeTapHandle {
    pub send_in: mpsc::UnboundedSender<Vec<u8>>,
    pub recv_out: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl FakeTap {
    /// Construct a fake TAP + the matching test-side handle. Hand the
    /// `Arc<FakeTap>` to the runtime (or anything taking a `PodIo`);
    /// use the `FakeTapHandle` from the test body.
    pub fn pair() -> (Arc<FakeTap>, FakeTapHandle) {
        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let tap = Arc::new(FakeTap {
            inbox: Mutex::new(in_rx),
            outbox: out_tx,
        });
        let handle = FakeTapHandle {
            send_in: in_tx,
            recv_out: out_rx,
        };
        (tap, handle)
    }
}

#[async_trait]
impl PodIo for FakeTap {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut rx = self.inbox.lock().await;
        let pkt = rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::ConnectionAborted, "inbox closed"))?;
        let n = pkt.len().min(buf.len());
        buf[..n].copy_from_slice(&pkt[..n]);
        Ok(n)
    }
    async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.outbox
            .send(buf.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "outbox closed"))?;
        Ok(buf.len())
    }
}
