//! End-to-end TCP integration test: pod → Service VIP → backend
//! round-trip through every layer of the netstack.
//!
//! ### Topology
//!
//! ```text
//!   Pod side (synthetic):                   Netstack side (real):
//!   ┌──────────────────────┐                ┌────────────────────────────────┐
//!   │ smoltcp Interface    │                │ PodTapRuntime                  │
//!   │  + TestDevice        │                │   ├ PodNet (MultiDevice)       │
//!   │  + tcp::Socket       │  ←─ shuttle ─→ │   └ bound TCP listener pool    │
//!   │      .connect(VIP)   │   (test task)  │      + proxy_tcp_connection    │
//!   └──────────────────────┘                │        task spawned on accept  │
//!         IP: 10.244.0.5                    └────────────────────────────────┘
//!                                                            │
//!                                                            ▼
//!                                              tokio TcpListener (echo backend)
//! ```
//!
//! The "shuttle" task drains the pod's smoltcp TX queue into the
//! netstack's `FakeTap.send_in` (becomes RX from the netstack's
//! POV), and pulls from `FakeTap.recv_out` back into the pod's RX
//! queue. Smoltcp on both sides handles the full TCP state machine
//! (SYN, SYN-ACK, ACK, data, FIN-ACK ...) so the test doesn't
//! hand-craft any packets.
//!
//! ### What this catches
//!
//! - Regressions in `proxy::proxy_tcp_connection` (the byte pump
//!   between smoltcp `tcp::Socket` and tokio `TcpStream`).
//! - Regressions in `PodNet::bind_tcp_service` /
//!   `accepted_tcp_connections` accept-detection.
//! - Regressions in `MultiDevice::forward_or_inject` /
//!   `take_egress` packet routing for VIP-bound TCP.
//! - Regressions in `PodTapRuntime::poll_loop` spawning pumps and
//!   recycling pool sockets between Established and Closed.
//!
//! A failure here means one of those layers broke its contract
//! with the others — the class of bug that unit tests of isolated
//! layers don't catch.

use async_trait::async_trait;
use rusternetes_netstack::manager::{Netstack, NetstackConfig, TapFactory};
use rusternetes_netstack::runtime::PodIo;
use rusternetes_netstack::wire::{IpAddress, IpCidr};
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::tcp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{HardwareAddress, IpEndpoint, Ipv4Address};
use std::collections::{HashMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, timeout, Duration};

// ────────────────────────────────────────────────────────────────────
// FakeTap — duplicated from the in-crate `test_helpers` module
// because that module is `#[cfg(test)] pub(crate)` and therefore
// invisible to integration tests in `tests/`. If a second
// integration test needs the same fake, lift it behind a
// `test-utils` cargo feature.
// ────────────────────────────────────────────────────────────────────

struct FakeTap {
    inbox: Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
    outbox: mpsc::UnboundedSender<Vec<u8>>,
}

struct FakeTapHandle {
    send_in: mpsc::UnboundedSender<Vec<u8>>,
    recv_out: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl FakeTap {
    fn pair() -> (Arc<FakeTap>, FakeTapHandle) {
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
    async fn recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut rx = self.inbox.lock().await;
        let pkt = rx.recv().await.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "inbox closed")
        })?;
        let n = pkt.len().min(buf.len());
        buf[..n].copy_from_slice(&pkt[..n]);
        Ok(n)
    }
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.outbox
            .send(buf.to_vec())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "outbox closed"))?;
        Ok(buf.len())
    }
}

struct FakeTapFactory {
    handles: Arc<StdMutex<HashMap<String, FakeTapHandle>>>,
}

impl FakeTapFactory {
    fn new() -> Self {
        Self {
            handles: Arc::new(StdMutex::new(HashMap::new())),
        }
    }
}

impl TapFactory for FakeTapFactory {
    type Io = FakeTap;
    fn create_tap(
        &self,
        tap_name: &str,
    ) -> Result<Arc<FakeTap>, rusternetes_netstack::iface::OpenTapError> {
        let (tap, handle) = FakeTap::pair();
        self.handles
            .lock()
            .expect("FakeTapFactory mutex poisoned")
            .insert(tap_name.to_string(), handle);
        Ok(tap)
    }
}

// ────────────────────────────────────────────────────────────────────
// TestDevice — minimal smoltcp `Device` for the pod-side stack.
// MultiDevice routes TX by destination IP into per-pod queues —
// wrong for a single-wire link. RingDevice in `iface.rs` is
// `pub(crate)`. Inline a small Device with one RX and one TX queue
// and let the test shuttle move bytes between them and the FakeTap.
// ────────────────────────────────────────────────────────────────────

const MTU: usize = 1500;

struct TestDevice {
    rx_queue: VecDeque<Vec<u8>>,
    tx_queue: VecDeque<Vec<u8>>,
}

impl TestDevice {
    fn new() -> Self {
        Self {
            rx_queue: VecDeque::with_capacity(16),
            tx_queue: VecDeque::with_capacity(16),
        }
    }
}

impl Device for TestDevice {
    type RxToken<'a> = TestRx;
    type TxToken<'a> = TestTx<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = MTU;
        caps
    }

    fn receive(&mut self, _ts: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let bytes = self.rx_queue.pop_front()?;
        Some((TestRx(bytes), TestTx(&mut self.tx_queue)))
    }

    fn transmit(&mut self, _ts: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(TestTx(&mut self.tx_queue))
    }
}

struct TestRx(Vec<u8>);
impl RxToken for TestRx {
    fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
        f(&self.0)
    }
}

struct TestTx<'a>(&'a mut VecDeque<Vec<u8>>);
impl<'a> TxToken for TestTx<'a> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut buf = vec![0u8; len];
        let r = f(&mut buf);
        self.0.push_back(buf);
        r
    }
}

// ────────────────────────────────────────────────────────────────────
// PodSide — synthetic pod-side smoltcp stack.
// ────────────────────────────────────────────────────────────────────

struct PodSide {
    iface: Interface,
    device: TestDevice,
    sockets: SocketSet<'static>,
    socket: smoltcp::iface::SocketHandle,
}

impl PodSide {
    /// Build a pod-side stack with `pod_ip/16` and a fresh TCP
    /// socket. Default route via `pod_ip | 1` (advisory; smoltcp
    /// transmits via the only device either way for Medium::Ip).
    fn new(pod_ip: Ipv4Addr) -> Self {
        let mut device = TestDevice::new();
        let config = Config::new(HardwareAddress::Ip);
        let mut iface = Interface::new(config, &mut device, SmolInstant::now());
        let o = pod_ip.octets();
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::v4(o[0], o[1], o[2], o[3]), 16))
                .unwrap();
        });
        let gw = Ipv4Address::new(o[0], o[1], o[2], o[3] | 1);
        iface.routes_mut().add_default_ipv4_route(gw).unwrap();

        let rx = tcp::SocketBuffer::new(vec![0u8; 4096]);
        let tx = tcp::SocketBuffer::new(vec![0u8; 4096]);
        let sock = tcp::Socket::new(rx, tx);
        let mut sockets = SocketSet::new(vec![]);
        let socket = sockets.add(sock);

        Self {
            iface,
            device,
            sockets,
            socket,
        }
    }

    /// Initiate a TCP connect from this pod to `remote`. Smoltcp
    /// queues the SYN; the actual on-the-wire send happens on the
    /// next `poll()`.
    fn connect(&mut self, remote: IpEndpoint, local_port: u16) {
        let Self {
            iface,
            sockets,
            socket,
            ..
        } = self;
        let cx = iface.context();
        let s: &mut tcp::Socket = sockets.get_mut(*socket);
        s.connect(cx, remote, local_port)
            .expect("smoltcp connect queues the SYN");
    }

    fn poll(&mut self) -> bool {
        let Self {
            iface,
            device,
            sockets,
            ..
        } = self;
        matches!(
            iface.poll(SmolInstant::now(), device, sockets),
            smoltcp::iface::PollResult::SocketStateChanged
        )
    }

    fn tcp(&mut self) -> &mut tcp::Socket<'static> {
        self.sockets.get_mut::<tcp::Socket>(self.socket)
    }
}

// ────────────────────────────────────────────────────────────────────
// Shuttle — pumps packets in both directions between the pod-side
// `TestDevice` queues and the netstack-side `FakeTap` channels.
// Runs until cancelled.
// ────────────────────────────────────────────────────────────────────

async fn run_shuttle(
    pod: Arc<StdMutex<PodSide>>,
    mut handle: FakeTapHandle,
    cancel: Arc<tokio::sync::Notify>,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.notified() => return,
            _ = sleep(Duration::from_millis(2)) => {}
        }
        // Pod → netstack: drain pod's tx_queue, send via FakeTap inbox.
        let pod_tx: Vec<Vec<u8>> = {
            let mut p = pod.lock().unwrap();
            std::mem::take(&mut p.device.tx_queue).into_iter().collect()
        };
        for pkt in pod_tx {
            if handle.send_in.send(pkt).is_err() {
                return;
            }
        }
        // Netstack → pod: pull queued packets from FakeTap outbox.
        while let Ok(pkt) = handle.recv_out.try_recv() {
            let mut p = pod.lock().unwrap();
            p.device.rx_queue.push_back(pkt);
        }
        // Poll pod's smoltcp Interface so it advances its own state.
        {
            let mut p = pod.lock().unwrap();
            let _ = p.poll();
        }
    }
}

// ────────────────────────────────────────────────────────────────────
// Backend — a real tokio TcpListener that echoes everything until
// the peer closes.
// ────────────────────────────────────────────────────────────────────

async fn spawn_echo_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind echo");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    });
    addr
}

// ────────────────────────────────────────────────────────────────────
// The test.
// ────────────────────────────────────────────────────────────────────

fn build_netstack(factory: FakeTapFactory) -> Arc<Netstack<FakeTapFactory>> {
    let cfg = NetstackConfig {
        pod_cidr_base: Ipv4Addr::new(10, 244, 0, 0),
        pod_cidr_prefix: 16,
        // /12 for routing + /32 for the VIP we terminate locally.
        host_ips: vec![
            IpCidr::new(IpAddress::v4(10, 96, 0, 1), 12),
            IpCidr::new(IpAddress::v4(10, 96, 0, 1), 32),
        ],
    };
    Arc::new(Netstack::new(cfg, factory).expect("Netstack::new"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pod_to_vip_tcp_round_trips_through_proxy_pump() {
    // 1. Echo backend.
    let backend_addr = spawn_echo_backend().await;

    // 2. Real netstack with VIP bound.
    let factory = FakeTapFactory::new();
    let factory_handles = factory.handles.clone();
    let netstack = build_netstack(factory);
    let vip: SocketAddr = "10.96.0.1:443".parse().unwrap();
    netstack
        .bind_tcp_service(vip, vec![backend_addr], 4)
        .await
        .expect("bind_tcp_service");

    // 3. Register the pod and grab the FakeTap handle.
    let pod_uid = "pod-e2e";
    let pod_ip = netstack.start_pod(pod_uid).await.expect("start_pod");
    let fake_tap_handle = {
        let mut g = factory_handles.lock().unwrap();
        // start_pod created exactly one TAP; grab its handle.
        let key = g
            .keys()
            .next()
            .expect("FakeTapFactory recorded a TAP")
            .clone();
        g.remove(&key).unwrap()
    };

    // 4. Synthetic pod side.
    let pod = Arc::new(StdMutex::new(PodSide::new(pod_ip)));

    // 5. Shuttle task — runs until we fire `cancel` at the end.
    let cancel = Arc::new(tokio::sync::Notify::new());
    let shuttle = tokio::spawn(run_shuttle(pod.clone(), fake_tap_handle, cancel.clone()));

    // 6. Initiate connect from the pod-side TCP socket. smoltcp
    //    queues the SYN; the shuttle picks it up on its next tick.
    {
        let mut p = pod.lock().unwrap();
        let remote = IpEndpoint::new(IpAddress::v4(10, 96, 0, 1), 443);
        p.connect(remote, 49152);
        // Force one poll so the SYN gets emitted now rather than
        // waiting for the shuttle's next 2 ms tick.
        let _ = p.poll();
    }

    // 7. Wait for the pod-side socket to reach Established.
    let establish_result = timeout(Duration::from_secs(3), async {
        loop {
            {
                let mut p = pod.lock().unwrap();
                if p.tcp().state() == tcp::State::Established {
                    return;
                }
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        establish_result.is_ok(),
        "pod-side TCP socket reached Established within 3s"
    );

    // 8. Send a payload from the pod.
    let payload = b"hello-netstack";
    {
        let mut p = pod.lock().unwrap();
        let s = p.tcp();
        let n = s.send_slice(payload).expect("send_slice");
        assert_eq!(n, payload.len(), "send_slice queued full payload");
    }
    // Poll so the data gets emitted.
    {
        let mut p = pod.lock().unwrap();
        let _ = p.poll();
    }

    // 9. Wait for the backend's echo to arrive at the pod-side socket.
    let recv_result = timeout(Duration::from_secs(3), async {
        let mut received = Vec::new();
        while received.len() < payload.len() {
            {
                let mut p = pod.lock().unwrap();
                let s = p.tcp();
                if s.can_recv() {
                    let mut buf = [0u8; 1024];
                    let n = s.recv_slice(&mut buf).unwrap_or(0);
                    received.extend_from_slice(&buf[..n]);
                }
            }
            sleep(Duration::from_millis(5)).await;
        }
        received
    })
    .await;
    let received = recv_result.expect("backend echo received within 3s");
    assert_eq!(
        received, payload,
        "round-tripped payload matches what we sent"
    );

    // 10. Clean teardown — close the pod-side socket and shut the
    //     netstack down.
    {
        let mut p = pod.lock().unwrap();
        p.tcp().close();
        let _ = p.poll();
    }
    cancel.notify_waiters();
    let _ = timeout(Duration::from_millis(500), shuttle).await;
    timeout(Duration::from_secs(2), netstack.stop_pod(pod_uid))
        .await
        .expect("stop_pod completes within 2s");
}
