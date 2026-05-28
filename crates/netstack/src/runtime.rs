//! `PodTapRuntime` — the tokio task layer that pumps real (or fake)
//! per-pod TAP handles into the [`PodNet`] data plane.
//!
//! ### Task layout
//!
//! ```text
//!                ┌───────────── PodTapRuntime ─────────────┐
//!                │                                          │
//!   pod-A TAP ──►│  pod_task(pod_a)  ─┐                     │
//!                │                    │                     │
//!   pod-B TAP ──►│  pod_task(pod_b)  ─┼─► Arc<Mutex<PodNet>>│
//!                │                    │                     │
//!   pod-N TAP ──►│  pod_task(pod_n)  ─┘    ▲                │
//!                │                         │                │
//!                │  poll_task ─────────────┘                │
//!                │                                          │
//!                └──────────────────────────────────────────┘
//! ```
//!
//! - **One `pod_task` per registered pod**. On every loop iteration it
//!   first drains the pod's egress queue (writes any pending bytes to
//!   the TAP), then `select!`s on (cancel, egress_wake, `io.recv()`).
//!   Inbound packets go through [`PodNet::forward_or_inject`]; if it
//!   reports a pod-to-pod fast-path forward, the dst pod's egress_wake
//!   is fired so its task drains immediately. Otherwise the global
//!   poll_wake is fired so smoltcp sees the packet on its next poll.
//! - **One `poll_task`**. Sleeps on (cancel, poll_wake, sleep(poll_delay)).
//!   On wake it drives `PodNet::poll`, then notifies any pod whose
//!   egress queue is now non-empty.
//!
//! ### Why two channels worth of signalling
//!
//! `poll_wake` and per-pod `egress_wake` are both `tokio::sync::Notify`.
//! `Notify::notify_one` stores one permit if no waiter is listening, so
//! we never lose a wake-up to a race between "signal fired" and "task
//! enters select". The cost: a pod task that's currently sending a
//! packet to its TAP can miss a wake-up batch (only one permit is
//! stored), so we re-check egress at the top of every loop iteration.
//!
//! ### Testing
//!
//! The runtime is generic over the [`PodIo`] trait so tests can
//! substitute a channel-backed fake (`FakeTap`) for `tokio_tun::Tun`.
//! That keeps the data-plane tests in this module hermetic — no
//! `CAP_NET_ADMIN` required, no real netlink, no flakiness from kernel
//! TAP timing.

use crate::podnet::PodNet;
use async_trait::async_trait;
use smoltcp::time::Instant;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

/// Abstraction over a per-pod TAP. `tokio_tun::Tun` is the production
/// impl; tests substitute a `FakeTap` backed by tokio channels.
///
/// Both methods take `&self` because the underlying `tokio_tun::Tun`
/// is internally synchronised — multiple references can be held by the
/// per-pod read and write paths without external locking.
#[async_trait]
pub trait PodIo: Send + Sync + 'static {
    async fn recv(&self, buf: &mut [u8]) -> std::io::Result<usize>;
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize>;
}

#[async_trait]
impl PodIo for tokio_tun::Tun {
    async fn recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        tokio_tun::Tun::recv(self, buf).await
    }
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        tokio_tun::Tun::send(self, buf).await
    }
}

/// Supervises per-pod TAP I/O tasks and one smoltcp poll task against
/// a single shared [`PodNet`].
///
/// Construct via [`PodTapRuntime::spawn`]. Add pods with
/// [`register_pod`](Self::register_pod) and remove them with
/// [`unregister_pod`](Self::unregister_pod). Stop everything with
/// [`shutdown`](Self::shutdown) — it `await`s every spawned task so
/// the runtime exits cleanly with no orphans.
pub struct PodTapRuntime {
    podnet: Arc<Mutex<PodNet>>,
    pods: Arc<Mutex<HashMap<Ipv4Addr, PodHandle>>>,
    poll_wake: Arc<Notify>,
    cancel: Arc<Notify>,
    poll_task: Option<JoinHandle<()>>,
}

struct PodHandle {
    egress_wake: Arc<Notify>,
    pod_cancel: Arc<Notify>,
    task: JoinHandle<()>,
}

impl PodTapRuntime {
    /// Spawn the runtime against an already-constructed [`PodNet`].
    /// The poll task starts immediately; per-pod tasks are spawned by
    /// [`register_pod`](Self::register_pod).
    pub fn spawn(podnet: PodNet) -> Self {
        let podnet = Arc::new(Mutex::new(podnet));
        let pods: Arc<Mutex<HashMap<Ipv4Addr, PodHandle>>> = Arc::new(Mutex::new(HashMap::new()));
        let poll_wake = Arc::new(Notify::new());
        let cancel = Arc::new(Notify::new());
        let poll_task = tokio::spawn(poll_loop(
            podnet.clone(),
            pods.clone(),
            poll_wake.clone(),
            cancel.clone(),
        ));
        debug!("PodTapRuntime: spawned");
        Self {
            podnet,
            pods,
            poll_wake,
            cancel,
            poll_task: Some(poll_task),
        }
    }

    /// Direct access to the underlying `PodNet`. The dispatcher uses
    /// this to bind smoltcp sockets, add/remove Service-VIP host IPs,
    /// etc. Hold the lock for as little time as possible — the per-pod
    /// and poll tasks contend for it.
    pub fn podnet(&self) -> Arc<Mutex<PodNet>> {
        self.podnet.clone()
    }

    /// Register a pod and spawn its TAP I/O task. Returns `true` if
    /// the pod was new, `false` if it was already registered (existing
    /// task is preserved).
    pub async fn register_pod<T: PodIo>(&self, pod_ip: Ipv4Addr, io: Arc<T>) -> bool {
        let new = {
            let mut p = self.podnet.lock().await;
            p.register_pod(pod_ip)
        };
        if !new {
            return false;
        }
        let egress_wake = Arc::new(Notify::new());
        let pod_cancel = Arc::new(Notify::new());
        let task = tokio::spawn(pod_task(PodTaskCtx {
            pod_ip,
            io: io as Arc<dyn PodIo>,
            podnet: self.podnet.clone(),
            poll_wake: self.poll_wake.clone(),
            pods: self.pods.clone(),
            egress_wake: egress_wake.clone(),
            pod_cancel: pod_cancel.clone(),
            global_cancel: self.cancel.clone(),
        }));
        let mut pods = self.pods.lock().await;
        pods.insert(
            pod_ip,
            PodHandle {
                egress_wake,
                pod_cancel,
                task,
            },
        );
        debug!(?pod_ip, "PodTapRuntime: pod registered");
        true
    }

    /// Stop a pod's I/O task and unregister it from the data plane.
    /// Awaits the task so by the time this returns the pod is fully
    /// detached and no late TX can race the unregister. Returns `true`
    /// if the pod was registered.
    pub async fn unregister_pod(&self, pod_ip: Ipv4Addr) -> bool {
        let handle = {
            let mut pods = self.pods.lock().await;
            pods.remove(&pod_ip)
        };
        let was_registered = handle.is_some();
        if let Some(h) = handle {
            h.pod_cancel.notify_one();
            let _ = h.task.await;
        }
        {
            let mut p = self.podnet.lock().await;
            p.unregister_pod(pod_ip);
        }
        if was_registered {
            debug!(?pod_ip, "PodTapRuntime: pod unregistered");
        }
        was_registered
    }

    /// Stop the poll task and every per-pod task, then drop the
    /// runtime. `await`s everything so the caller knows nothing is
    /// still running.
    pub async fn shutdown(mut self) {
        debug!("PodTapRuntime: shutdown initiated");
        // Fire the global cancel — waiters on `notify_waiters` get woken.
        self.cancel.notify_waiters();
        // Each pod task also listens on its own pod_cancel; fire both to
        // cover the (rare) case where the task arrived at its select
        // *after* notify_waiters fired and missed the global cancel.
        let handles: Vec<PodHandle> = {
            let mut pods = self.pods.lock().await;
            pods.drain().map(|(_, h)| h).collect()
        };
        for h in handles {
            h.pod_cancel.notify_one();
            let _ = h.task.await;
        }
        if let Some(pt) = self.poll_task.take() {
            // The poll task is parked on `cancel.notified()` — re-fire
            // just in case (notify_waiters above only wakes existing
            // waiters; if the poll task wasn't yet at the await point,
            // we'd hang).
            self.cancel.notify_waiters();
            let _ = pt.await;
        }
        debug!("PodTapRuntime: shutdown complete");
    }
}

/// Everything `pod_task` needs to run — bundled into one struct so
/// the function signature stays clippy-clean and so the runtime's
/// `register_pod` plumbing has one obvious thing to hand over.
struct PodTaskCtx {
    pod_ip: Ipv4Addr,
    io: Arc<dyn PodIo>,
    podnet: Arc<Mutex<PodNet>>,
    poll_wake: Arc<Notify>,
    pods: Arc<Mutex<HashMap<Ipv4Addr, PodHandle>>>,
    egress_wake: Arc<Notify>,
    pod_cancel: Arc<Notify>,
    global_cancel: Arc<Notify>,
}

async fn pod_task(ctx: PodTaskCtx) {
    // Same MTU as the rest of the netstack (`crate::multi::MTU`).
    let mut buf = vec![0u8; 1500];
    trace!(?ctx.pod_ip, "pod_task started");
    loop {
        // 1. Drain anything already queued for this pod's TAP.
        loop {
            let pkt = {
                let mut p = ctx.podnet.lock().await;
                p.take_egress(ctx.pod_ip)
            };
            match pkt {
                Some(p) => {
                    if let Err(e) = ctx.io.send(&p).await {
                        warn!(?ctx.pod_ip, error = %e, "TAP send failed; dropping packet");
                    }
                }
                None => break,
            }
        }

        // 2. Wait for the next event.
        tokio::select! {
            biased;
            _ = ctx.global_cancel.notified() => {
                trace!(?ctx.pod_ip, "pod_task: global cancel, exiting");
                return;
            }
            _ = ctx.pod_cancel.notified() => {
                trace!(?ctx.pod_ip, "pod_task: pod cancel, exiting");
                return;
            }
            _ = ctx.egress_wake.notified() => {
                // Loop back to step 1 to drain.
            }
            recv_res = ctx.io.recv(&mut buf) => {
                match recv_res {
                    Ok(n) => {
                        let packet = buf[..n].to_vec();
                        let dst_pod = {
                            let mut p = ctx.podnet.lock().await;
                            p.forward_or_inject(ctx.pod_ip, packet)
                        };
                        match dst_pod {
                            Some(dst) => {
                                // Pod-to-pod fast path — wake the dst's task.
                                let pods_g = ctx.pods.lock().await;
                                if let Some(handle) = pods_g.get(&dst) {
                                    handle.egress_wake.notify_one();
                                }
                            }
                            None => {
                                // smoltcp will dispatch on its next poll.
                                ctx.poll_wake.notify_one();
                            }
                        }
                    }
                    Err(e) => {
                        warn!(?ctx.pod_ip, error = %e,
                            "TAP recv failed; pod_task exiting (TAP is dead)");
                        return;
                    }
                }
            }
        }
    }
}

async fn poll_loop(
    podnet: Arc<Mutex<PodNet>>,
    pods: Arc<Mutex<HashMap<Ipv4Addr, PodHandle>>>,
    poll_wake: Arc<Notify>,
    cancel: Arc<Notify>,
) {
    trace!("poll_loop started");
    loop {
        let delay = {
            let mut p = podnet.lock().await;
            p.poll_delay(Instant::now())
                .unwrap_or(Duration::from_secs(1))
        };

        tokio::select! {
            biased;
            _ = cancel.notified() => {
                trace!("poll_loop: cancel, exiting");
                return;
            }
            _ = poll_wake.notified() => {}
            _ = tokio::time::sleep(delay) => {}
        }

        // Drive smoltcp once + find pods that got egress to wake up.
        let pods_to_wake: Vec<Ipv4Addr> = {
            let mut p = podnet.lock().await;
            p.poll(Instant::now());
            p.registered_pods()
                .filter(|ip| p.egress_len(*ip) > 0)
                .collect()
        };

        if !pods_to_wake.is_empty() {
            let pods_g = pods.lock().await;
            for ip in pods_to_wake {
                if let Some(handle) = pods_g.get(&ip) {
                    handle.egress_wake.notify_one();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::podnet::PodNetConfig;
    use smoltcp::socket::udp::{PacketBuffer, PacketMetadata, Socket as UdpSocket};
    use smoltcp::wire::{IpAddress, IpCidr, IpEndpoint};
    use std::io;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    /// Channel-backed fake TAP for runtime tests — no kernel TAP, no
    /// `CAP_NET_ADMIN`. The runtime sees a normal [`PodIo`]; the test
    /// drives bytes via [`FakeTapHandle`].
    struct FakeTap {
        inbox: Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
        outbox: mpsc::UnboundedSender<Vec<u8>>,
    }

    /// Test-side handle to drive a `FakeTap`: push bytes "from the
    /// pod" via [`send_in`](Self::send_in), pull bytes "to the pod"
    /// via [`recv_out`](Self::recv_out).
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

    fn default_config() -> PodNetConfig {
        PodNetConfig {
            host_ips: vec![IpCidr::new(IpAddress::v4(10, 96, 0, 1), 12)],
        }
    }

    /// Same UDP-over-IPv4 builder as in `podnet::tests`, copied here so
    /// each test module is self-contained.
    fn udp_ipv4_packet(
        src: [u8; 4],
        src_port: u16,
        dst: [u8; 4],
        dst_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        use smoltcp::wire::{IpProtocol, Ipv4Address, Ipv4Packet, Ipv4Repr, UdpPacket, UdpRepr};
        let src_addr = Ipv4Address::from_octets(src);
        let dst_addr = Ipv4Address::from_octets(dst);
        let udp_repr = UdpRepr { src_port, dst_port };
        let udp_len = udp_repr.header_len() + payload.len();
        let ip_repr = Ipv4Repr {
            src_addr,
            dst_addr,
            next_header: IpProtocol::Udp,
            payload_len: udp_len,
            hop_limit: 64,
        };
        let total = ip_repr.buffer_len() + udp_len;
        let mut buf = vec![0u8; total];
        let mut ip_packet = Ipv4Packet::new_unchecked(&mut buf);
        ip_repr.emit(&mut ip_packet, &Default::default());
        let mut udp_packet =
            UdpPacket::new_unchecked(&mut ip_packet.into_inner()[ip_repr.buffer_len()..]);
        udp_repr.emit(
            &mut udp_packet,
            &IpAddress::Ipv4(src_addr),
            &IpAddress::Ipv4(dst_addr),
            payload.len(),
            |p| p.copy_from_slice(payload),
            &Default::default(),
        );
        buf
    }

    fn fresh_udp_socket() -> UdpSocket<'static> {
        let rx = PacketBuffer::new(vec![PacketMetadata::EMPTY; 4], vec![0u8; 2048]);
        let tx = PacketBuffer::new(vec![PacketMetadata::EMPTY; 4], vec![0u8; 2048]);
        UdpSocket::new(rx, tx)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pod_to_pod_packet_flows_through_runtime_without_smoltcp() {
        // Two pods, two fake TAPs. Pod A sends a packet destined for
        // pod B; the runtime's pod_task for A picks it up, hands it to
        // PodNet, which fast-paths it to pod B's egress queue. The
        // pod_task for B then drains and writes to pod B's fake TAP.
        let podnet = PodNet::new(&default_config()).unwrap();
        let rt = PodTapRuntime::spawn(podnet);

        let pod_a: Ipv4Addr = Ipv4Addr::new(10, 244, 1, 5);
        let pod_b: Ipv4Addr = Ipv4Addr::new(10, 244, 2, 7);
        let (tap_a, mut hdl_a) = FakeTap::pair();
        let (tap_b, mut hdl_b) = FakeTap::pair();
        assert!(rt.register_pod(pod_a, tap_a).await);
        assert!(rt.register_pod(pod_b, tap_b).await);

        let pkt = udp_ipv4_packet([10, 244, 1, 5], 4000, [10, 244, 2, 7], 5000, b"hello-b");
        hdl_a.send_in.send(pkt.clone()).unwrap();

        let received = timeout(Duration::from_secs(2), hdl_b.recv_out.recv())
            .await
            .expect("pod B got a packet within 2s")
            .expect("outbox not closed");
        assert_eq!(
            received, pkt,
            "pod B's TAP received exactly what pod A sent"
        );

        // Pod A's fake TAP must NOT have received anything back —
        // we sent a unicast to pod B, not a loop.
        assert!(
            hdl_a.recv_out.try_recv().is_err(),
            "pod A's TAP must not echo"
        );

        rt.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn vip_bound_udp_query_and_reply_flow_through_runtime() {
        // End-to-end through every layer:
        //   pod A TAP → pod_task → PodNet → smoltcp Interface → UdpSocket
        //   UdpSocket → smoltcp Interface → MultiDevice → pod A egress → pod_task → pod A TAP
        let mut podnet = PodNet::new(&default_config()).unwrap();
        let kube_dns_port: u16 = 53;
        assert!(podnet.add_host_ip(IpCidr::new(IpAddress::v4(10, 96, 0, 10), 32)));
        let mut sock = fresh_udp_socket();
        sock.bind(IpEndpoint::new(IpAddress::v4(10, 96, 0, 10), kube_dns_port))
            .unwrap();
        let sock_handle = podnet.add_socket(sock);

        let rt = PodTapRuntime::spawn(podnet);
        let pod_a: Ipv4Addr = Ipv4Addr::new(10, 244, 1, 5);
        let (tap_a, mut hdl_a) = FakeTap::pair();
        assert!(rt.register_pod(pod_a, tap_a).await);

        // Pod A sends a UDP query to kube-dns.
        let query = b"qXYZ4242";
        let query_pkt = udp_ipv4_packet([10, 244, 1, 5], 33333, [10, 96, 0, 10], 53, query);
        hdl_a.send_in.send(query_pkt).unwrap();

        // Wait for the UDP socket to see the query; spin briefly because
        // the runtime is async and we don't know the exact poll cadence.
        let podnet_handle = rt.podnet();
        let recv_payload = timeout(Duration::from_secs(2), async {
            loop {
                {
                    let mut p = podnet_handle.lock().await;
                    let s: &mut UdpSocket = p.get_socket_mut(sock_handle);
                    if let Ok((bytes, meta)) = s.recv() {
                        let payload = bytes.to_vec();
                        // Echo a reply back to the originating endpoint.
                        let reply = b"rABCD8765";
                        s.send_slice(reply, meta.endpoint).expect("queue reply");
                        return payload;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("socket received the query within 2s");
        assert_eq!(recv_payload, query);

        // The reply has been queued in the smoltcp socket; the poll
        // task will emit it on its next tick. To minimise latency we
        // also poke the runtime by waking the poll task — in production
        // the periodic poll picks it up regardless. Here we just want
        // the test to land quickly.
        {
            let p = podnet_handle.lock().await;
            // poll_delay returning Some means smoltcp wants us to wake
            // up soonish — exactly the right signal.
            drop(p);
        }

        let reply = timeout(Duration::from_secs(2), hdl_a.recv_out.recv())
            .await
            .expect("pod A got the reply within 2s")
            .expect("outbox not closed");
        assert!(
            reply.ends_with(b"rABCD8765"),
            "reply payload reached pod A's TAP (got {} bytes ending in {:?})",
            reply.len(),
            &reply[reply.len() - 9..]
        );

        rt.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unregister_pod_stops_its_task_and_clears_data_plane() {
        let podnet = PodNet::new(&default_config()).unwrap();
        let rt = PodTapRuntime::spawn(podnet);
        let pod = Ipv4Addr::new(10, 244, 1, 5);
        let (tap, _hdl) = FakeTap::pair();
        assert!(rt.register_pod(pod, tap).await);

        assert!(
            rt.unregister_pod(pod).await,
            "unregister returns true for known pod"
        );
        assert!(
            !rt.unregister_pod(pod).await,
            "second unregister returns false"
        );

        // After unregister, the pod is gone from the data plane.
        let podnet_handle = rt.podnet();
        let p = podnet_handle.lock().await;
        assert_eq!(p.registered_pods().count(), 0);

        drop(p);
        rt.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_stops_all_tasks_cleanly() {
        let podnet = PodNet::new(&default_config()).unwrap();
        let rt = PodTapRuntime::spawn(podnet);
        let (tap1, _hdl1) = FakeTap::pair();
        let (tap2, _hdl2) = FakeTap::pair();
        rt.register_pod(Ipv4Addr::new(10, 244, 1, 5), tap1).await;
        rt.register_pod(Ipv4Addr::new(10, 244, 1, 6), tap2).await;

        // Shutdown completes within a reasonable budget; if a task
        // missed the cancel, we'd hang here forever.
        timeout(Duration::from_secs(2), rt.shutdown())
            .await
            .expect("shutdown completes within 2s");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_pod_returns_false_on_double_registration() {
        let podnet = PodNet::new(&default_config()).unwrap();
        let rt = PodTapRuntime::spawn(podnet);
        let pod = Ipv4Addr::new(10, 244, 1, 5);
        let (tap1, _hdl1) = FakeTap::pair();
        let (tap2, _hdl2) = FakeTap::pair();
        assert!(rt.register_pod(pod, tap1).await);
        assert!(!rt.register_pod(pod, tap2).await);
        rt.shutdown().await;
    }
}
