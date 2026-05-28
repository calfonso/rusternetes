//! TCP proxy — the bidirectional byte pump that connects a smoltcp
//! TCP socket (one end of a Service-VIP connection from a pod) to a
//! tokio `TcpStream` (the chosen backend).
//!
//! ### How the pieces fit
//!
//! 1. Pod opens a TCP connection to a Service VIP (e.g.,
//!    `10.96.0.1:443`).
//! 2. PodNet's smoltcp Interface routes the SYN to one of the
//!    listener-pool sockets bound to that VIP
//!    ([`crate::podnet::PodNet::bind_tcp_service`]). The socket
//!    transitions to `State::Established`.
//! 3. The runtime's poll task notices via
//!    [`crate::podnet::PodNet::accepted_tcp_connections`] and gets
//!    back a [`TcpAccept`].
//! 4. The runtime spawns [`proxy_tcp_connection`] per accept. The
//!    pump opens a tokio `TcpStream` to the picker-chosen backend
//!    and shuffles bytes until either side closes.
//! 5. When the pump exits, the smoltcp socket is left in `Closed`
//!    state. The next `accepted_tcp_connections` scan re-listens
//!    on the handle so the pool size stays stable.
//!
//! ### Why polling rather than waker integration
//!
//! smoltcp's TCP `Socket` exposes `register_recv_waker` /
//! `register_send_waker` for async integration. Wiring those to the
//! tokio task's waker would give us truly event-driven pumps —
//! zero wakeups when nothing's happening. We don't do that yet;
//! instead the pump wakes up on every `pump_wake` notification (one
//! per PodNet poll cycle) and re-checks its socket. The cost: every
//! active pump wakes up at poll cadence regardless of whether its
//! own socket has anything to do. Acceptable for the spike; the
//! waker-based optimization is a clean follow-up since this module
//! is the only smoltcp-async-integration call site.

use crate::podnet::{PodNet, TcpAccept};
use smoltcp::socket::tcp;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, trace, warn};

/// Maximum bytes we read from the smoltcp socket or the tokio
/// stream per iteration. Smaller than the smoltcp socket's 4 KB
/// ring buffer so we don't have to handle partial reads.
const PUMP_CHUNK: usize = 2048;

/// Pump bytes between an established smoltcp TCP socket and a
/// freshly-opened backend `TcpStream` until either side closes.
///
/// Spawned as a tokio task per [`TcpAccept`] by the runtime.
/// Returns cleanly on:
///   - Either side closing the connection (FIN, RST, peer drop).
///   - The global `cancel` notify firing (runtime shutdown).
///   - The backend `connect` failing (we close the smoltcp socket
///     so the pod sees a clean RST).
pub async fn proxy_tcp_connection(
    podnet: Arc<Mutex<PodNet>>,
    accept: TcpAccept,
    pump_wake: Arc<Notify>,
    cancel: Arc<Notify>,
) {
    debug!(?accept.local, ?accept.remote, ?accept.backend, "proxy: starting pump");

    // 1. Open the backend connection. If this fails the pod sees a
    //    cleanly-closed VIP connection (smoltcp socket aborted on
    //    drop).
    let mut backend = match TcpStream::connect(accept.backend).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                backend = %accept.backend,
                error = %e,
                "proxy: backend connect failed, closing pod-side socket"
            );
            close_smoltcp_socket(&podnet, accept.handle).await;
            return;
        }
    };

    let (mut backend_rx, mut backend_tx) = backend.split();
    let mut backend_to_pod = [0u8; PUMP_CHUNK];
    let mut backend_closed = false;
    let mut pod_closed = false;

    // Exit when EITHER side closes. We don't try to half-close —
    // any in-flight bytes in the other direction may be lost. Phase
    // 4 follow-up: implement proper TCP half-close so long-poll
    // backends survive a pod-side FIN.
    while !backend_closed && !pod_closed {
        // Drain pod-side: any bytes smoltcp has buffered → backend.
        loop {
            let chunk = {
                let mut net = podnet.lock().await;
                let sock: &mut tcp::Socket = net.get_socket_mut(accept.handle);
                if !sock.may_recv() {
                    pod_closed = true;
                    None
                } else if sock.can_recv() {
                    let mut buf = [0u8; PUMP_CHUNK];
                    match sock.recv_slice(&mut buf) {
                        Ok(0) => None,
                        Ok(n) => Some(buf[..n].to_vec()),
                        Err(e) => {
                            warn!(?e, "proxy: smoltcp recv_slice failed");
                            pod_closed = true;
                            None
                        }
                    }
                } else {
                    None
                }
            };
            let Some(bytes) = chunk else {
                break;
            };
            if backend_tx.write_all(&bytes).await.is_err() {
                backend_closed = true;
                break;
            }
            trace!(len = bytes.len(), "proxy: pod → backend");
        }

        // Drain backend-side: any bytes pending on TcpStream → smoltcp.
        // Use a short timeout so we can return to the wake-wait loop.
        let backend_read = tokio::time::timeout(
            tokio::time::Duration::from_millis(1),
            backend_rx.read(&mut backend_to_pod),
        )
        .await;
        match backend_read {
            Ok(Ok(0)) => {
                // EOF from backend.
                backend_closed = true;
                // Half-close the smoltcp side so the pod sees a FIN.
                let mut net = podnet.lock().await;
                let sock: &mut tcp::Socket = net.get_socket_mut(accept.handle);
                sock.close();
                drop(net);
                pump_wake.notify_one();
            }
            Ok(Ok(n)) => {
                let mut net = podnet.lock().await;
                let sock: &mut tcp::Socket = net.get_socket_mut(accept.handle);
                let mut written = 0;
                while written < n {
                    match sock.send_slice(&backend_to_pod[written..n]) {
                        Ok(0) => break, // socket buffer full; try again on next wake
                        Ok(w) => written += w,
                        Err(e) => {
                            warn!(?e, "proxy: smoltcp send_slice failed");
                            pod_closed = true;
                            break;
                        }
                    }
                }
                drop(net);
                pump_wake.notify_one();
                trace!(len = n, "proxy: backend → pod");
            }
            Ok(Err(e)) => {
                warn!(error = %e, "proxy: backend read failed");
                backend_closed = true;
            }
            Err(_) => {
                // Timeout — no backend bytes ready this tick.
            }
        }

        // Wait for the next pump cycle or the cancel signal.
        // Always check cancel — even if both sides are still alive,
        // a runtime shutdown should land within one wake cycle.
        tokio::select! {
            biased;
            _ = cancel.notified() => {
                debug!("proxy: cancel notified, closing");
                close_smoltcp_socket(&podnet, accept.handle).await;
                return;
            }
            _ = pump_wake.notified() => {}
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {}
        }
    }

    // Drain anything still buffered, then close the smoltcp socket
    // so the listener-pool scan can re-listen on the handle.
    close_smoltcp_socket(&podnet, accept.handle).await;
    pump_wake.notify_one();
    debug!(?accept.local, ?accept.remote, ?accept.backend, "proxy: pump finished");
}

async fn close_smoltcp_socket(podnet: &Arc<Mutex<PodNet>>, handle: smoltcp::iface::SocketHandle) {
    let mut net = podnet.lock().await;
    let sock: &mut tcp::Socket = net.get_socket_mut(handle);
    sock.close();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::RoundRobinPicker;
    use crate::podnet::{PodNet, PodNetConfig};
    use smoltcp::wire::{IpAddress, IpCidr, IpEndpoint};
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio::time::{timeout, Duration};

    fn default_config() -> PodNetConfig {
        PodNetConfig {
            host_ips: vec![IpCidr::new(IpAddress::v4(10, 96, 0, 1), 12)],
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn proxy_closes_pod_side_when_backend_connect_fails() {
        // Bind a Service VIP and synthesise an accepted connection
        // with a backend address that nobody's listening on. The
        // pump must close the smoltcp socket cleanly rather than
        // hanging.
        let mut net = PodNet::new(&default_config()).unwrap();
        let vip: SocketAddr = "10.96.0.1:443".parse().unwrap();
        let picker = Arc::new(RoundRobinPicker::new(vec![
            // 127.0.0.1:1 — port 1 is reserved + nobody listens. Connect fails fast.
            "127.0.0.1:1".parse().unwrap(),
        ]));
        net.bind_tcp_service(vip, picker, 4).unwrap();

        // Synthetic accept — we need a real SocketHandle, so grab
        // one from the just-bound pool. State is Listen, not
        // Established, but for this test we only care that the
        // pump's "backend connect failed" path runs and closes
        // the socket without panicking.
        let podnet = Arc::new(Mutex::new(net));
        let pump_wake = Arc::new(Notify::new());
        let cancel = Arc::new(Notify::new());

        // Pull out a pool handle to use in the synthetic accept.
        let handle = {
            let net = podnet.lock().await;
            // The pool entries are pub(crate)/private so reach in
            // via a fresh socket addition for the test instead.
            // Simpler: add our own tcp::Socket explicitly.
            drop(net);
            let mut net = podnet.lock().await;
            let rx = tcp::SocketBuffer::new(vec![0u8; 1024]);
            let tx = tcp::SocketBuffer::new(vec![0u8; 1024]);
            net.add_socket(tcp::Socket::new(rx, tx))
        };

        let accept = TcpAccept {
            handle,
            local: IpEndpoint::new(IpAddress::v4(10, 96, 0, 1), 443),
            remote: IpEndpoint::new(IpAddress::v4(10, 244, 0, 5), 33000),
            backend: "127.0.0.1:1".parse().unwrap(),
        };

        timeout(
            Duration::from_secs(5),
            proxy_tcp_connection(podnet.clone(), accept, pump_wake, cancel),
        )
        .await
        .expect("pump returns within 5s when backend connect fails");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn proxy_closes_pod_side_when_cancel_fires() {
        // Start the pump against a backend that DOES accept (so
        // connect succeeds and the pump enters its main loop).
        // Fire the cancel signal — pump must exit within a wake
        // cycle.
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_listener.local_addr().unwrap();
        // Accept-and-hold task — keeps the listener alive while
        // the pump connects.
        let _accept_task = tokio::spawn(async move {
            if let Ok((_stream, _)) = backend_listener.accept().await {
                // Hold the stream so the pump's connect succeeds.
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });

        let mut net = PodNet::new(&default_config()).unwrap();
        let rx = tcp::SocketBuffer::new(vec![0u8; 1024]);
        let tx = tcp::SocketBuffer::new(vec![0u8; 1024]);
        let handle = net.add_socket(tcp::Socket::new(rx, tx));
        let podnet = Arc::new(Mutex::new(net));
        let pump_wake = Arc::new(Notify::new());
        let cancel = Arc::new(Notify::new());

        let accept = TcpAccept {
            handle,
            local: IpEndpoint::new(IpAddress::v4(10, 96, 0, 1), 443),
            remote: IpEndpoint::new(IpAddress::v4(10, 244, 0, 5), 33000),
            backend: backend_addr,
        };

        let pump_handle = tokio::spawn(proxy_tcp_connection(
            podnet,
            accept,
            pump_wake,
            cancel.clone(),
        ));

        // Give the pump a moment to enter its main loop.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.notify_waiters();

        timeout(Duration::from_secs(2), pump_handle)
            .await
            .expect("pump exits within 2s after cancel")
            .expect("pump task joins cleanly");
    }
}
