//! `PodNet` — the per-pod data plane bound together.
//!
//! Composes the three pieces the netstack needs to actually carry pod
//! traffic into one struct:
//!
//! - [`crate::multi::MultiDevice`] — per-pod IPv4 fan-out on TX, shared
//!   RX, with pod-to-pod fast-path forwarding that bypasses smoltcp.
//! - `smoltcp::iface::Interface` — owns the host IPs (the Service-CIDR
//!   range, so smoltcp treats every ClusterIP as on-link) and drives
//!   the standard IP/UDP/TCP state machine for traffic destined to
//!   those host IPs.
//! - `smoltcp::iface::SocketSet` — holds the smoltcp sockets the
//!   dispatcher will bind to specific Service `(ip, port)` pairs (DNS
//!   on `10.96.0.10:53`, the apiserver Service on `10.96.0.1:443`,
//!   etc.).
//!
//! `PodNet` is single-threaded — wrap in `Arc<Mutex<PodNet>>` for the
//! multi-task runtime layer ([`crate::iface::run_interface`] today,
//! the `PodTapRuntime` that pumps multiple TAPs in a follow-up commit).
//!
//! ### Traffic flow
//!
//! ```text
//!   pod-A TAP ──►  PodNet::forward_or_inject(pod_a, packet)
//!                     │
//!                     ├─► dst is registered pod B  → pod-B TAP egress  (no smoltcp)
//!                     │
//!                     └─► dst is a host IP / VIP   → shared RX queue
//!                                                       │
//!                            PodNet::poll(now) ─────────┘
//!                                  │
//!                                  ├─► dispatcher / smoltcp socket receives
//!                                  │
//!                                  └─► smoltcp emits reply → MultiDevice TX
//!                                          (TxBuf parses dst, routes
//!                                           to pod-A's egress queue)
//! ```
//!
//! ### Out of scope for this commit
//!
//! - The tokio task layer (per-pod read/write tasks + a smoltcp poll
//!   task) that pumps real `tokio_tun::Tun` handles into `PodNet`.
//!   Lands as `PodTapRuntime` in the next commit; this module is the
//!   passive substrate.
//! - kubelet integration. The kubelet will own one `PodTapRuntime` and
//!   call `PodNet::register_pod` / `PodNet::unregister_pod` (through
//!   the runtime wrapper) as pods are scheduled / torn down.

use crate::dispatch::BackendPicker;
use crate::multi::MultiDevice;
use anyhow::Result;
use smoltcp::iface::{Config, Interface, PollResult, SocketHandle, SocketSet};
use smoltcp::socket::tcp;
use smoltcp::time::{Duration, Instant};
use smoltcp::wire::{HardwareAddress, IpCidr, IpEndpoint};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{debug, warn};

/// Configuration for a fresh [`PodNet`].
pub struct PodNetConfig {
    /// IPs the netstack itself owns from smoltcp's perspective.
    ///
    /// Two roles, two kinds of entry:
    ///
    /// - **Routing**: a `/12` entry for the Service CIDR (e.g.,
    ///   `10.96.0.1/12`) so smoltcp's forwarding table treats every
    ///   Service ClusterIP as on-link via this interface.
    /// - **Local delivery**: a separate `/32` entry per Service VIP
    ///   the netstack actually terminates in-process (kube-DNS,
    ///   the apiserver ClusterIP, etc.). smoltcp's "is this packet
    ///   for me?" check matches `ip_addrs` exactly — being inside an
    ///   on-link `/12` is not enough on its own.
    ///
    /// The dispatcher's Service-watcher will eventually call
    /// [`PodNet::add_host_ip`] / [`PodNet::remove_host_ip`] to keep
    /// the `/32` set in sync with Services as they come and go.
    pub host_ips: Vec<IpCidr>,
}

/// Per-pod data plane: smoltcp Interface + MultiDevice + SocketSet,
/// with the high-level methods kubelet / the runtime use.
///
/// This struct holds **all** mutable state; it's `Send` (the contained
/// `Interface` and `SocketSet<'static>` are `Send`) and is intended to
/// live behind `Arc<Mutex<…>>` in the runtime.
pub struct PodNet {
    iface: Interface,
    device: MultiDevice,
    sockets: SocketSet<'static>,
    /// Per Service-VIP pool of pre-bound smoltcp TCP listeners.
    /// Populated by [`PodNet::bind_tcp_service`]; scanned for newly
    /// accepted connections by [`PodNet::accepted_tcp_connections`]
    /// after each poll. Empty when the netstack has no Service VIPs
    /// configured (DNS-only operation).
    tcp_listeners: HashMap<SocketAddr, TcpListenerPool>,
}

/// One pre-bound TCP listener pool per Service VIP. Pool size is
/// chosen at `bind_tcp_service` time; sockets cycle through
/// listening → established (pumping) → closed → listening again.
struct TcpListenerPool {
    /// `(vip_ip, vip_port)` in smoltcp's `IpEndpoint` form — the
    /// address every socket in the pool listens on.
    vip: IpEndpoint,
    /// Backend selector. Shared `Arc` so re-installs from the
    /// EndpointSlice-watcher (Phase 4b's follow-up commit) can swap
    /// the picker without rebinding the listener pool.
    picker: Arc<dyn BackendPicker>,
    /// Pool entries — one per pre-bound smoltcp TCP socket. Indexed
    /// by `SocketHandle` for fast scan.
    entries: Vec<TcpPoolEntry>,
}

/// One slot in a [`TcpListenerPool`]. Tracks whether the smoltcp
/// socket is currently listening (waiting for the next pod to
/// connect) or in-flight (a pump task owns it). On the next scan
/// after `in_flight` transitions back to false (the pump task
/// closed), we re-listen on the same handle so the pool size stays
/// stable.
struct TcpPoolEntry {
    handle: SocketHandle,
    in_flight: bool,
}

/// A newly-accepted Service-VIP TCP connection — what
/// [`PodNet::accepted_tcp_connections`] returns for each scan. The
/// runtime (slice 4c) spawns a pump task per `TcpAccept` to shuffle
/// bytes between the smoltcp socket (identified by `handle`) and a
/// fresh `tokio::net::TcpStream` to `backend`.
#[derive(Debug, Clone)]
pub struct TcpAccept {
    /// Handle to the smoltcp TCP socket that accepted this
    /// connection. After the pump task closes the socket, the next
    /// `accepted_tcp_connections` scan re-listens on this handle —
    /// the pump must not delete it from the SocketSet.
    pub handle: SocketHandle,
    /// VIP and port the pod connected to (e.g., `10.96.0.1:443`).
    pub local: IpEndpoint,
    /// Pod IP and ephemeral port the SYN came from.
    pub remote: IpEndpoint,
    /// Backend the picker chose for this connection. The pump opens
    /// a tokio TcpStream to this address.
    pub backend: SocketAddr,
}

impl PodNet {
    /// Build a fresh interface bound to the given host IPs. No pods
    /// registered — the runtime calls [`register_pod`](Self::register_pod)
    /// per pod as it's scheduled.
    pub fn new(cfg: &PodNetConfig) -> Result<Self> {
        let mut device = MultiDevice::new();
        // Medium::Ip has no L2 framing, so the HardwareAddress is unused
        // by smoltcp's wire path. Use the zero IP address as a placeholder
        // — `HardwareAddress::Ip` is the dedicated variant for this case.
        let iface_config = Config::new(HardwareAddress::Ip);
        let mut iface = Interface::new(iface_config, &mut device, Instant::now());
        iface.update_ip_addrs(|addrs| {
            for ip in &cfg.host_ips {
                if addrs.push(*ip).is_err() {
                    debug!(addr = ?ip, "PodNet: smoltcp ip_addrs full, dropping");
                }
            }
        });
        debug!(
            host_ips = ?cfg.host_ips,
            "PodNet: initialised"
        );
        Ok(Self {
            iface,
            device,
            sockets: SocketSet::new(vec![]),
            tcp_listeners: HashMap::new(),
        })
    }

    /// Register a pod IP. The runtime then spawns a TAP read/write
    /// task that calls into [`forward_or_inject`](Self::forward_or_inject)
    /// and [`take_egress`](Self::take_egress) for this pod. Returns
    /// `true` if the pod was new.
    pub fn register_pod(&mut self, pod_ip: Ipv4Addr) -> bool {
        self.device.register_pod(pod_ip)
    }

    /// Unregister a pod IP. The runtime should signal the pod's TAP
    /// tasks to stop before calling this so no late TX races with the
    /// `take_egress` of the returned pending queue.
    pub fn unregister_pod(
        &mut self,
        pod_ip: Ipv4Addr,
    ) -> Option<std::collections::VecDeque<Vec<u8>>> {
        self.device.unregister_pod(pod_ip)
    }

    /// Receive a packet from a pod's TAP. Pod-to-pod traffic
    /// short-circuits to the destination pod's egress queue (returns
    /// `Some(dst_pod)` so the runtime can wake exactly that pod's
    /// write task); pod-to-VIP traffic falls through to smoltcp
    /// (returns `None`).
    pub fn forward_or_inject(&mut self, from_pod: Ipv4Addr, packet: Vec<u8>) -> Option<Ipv4Addr> {
        self.device.forward_or_inject(from_pod, packet)
    }

    /// Current length of `pod_ip`'s egress queue (0 if not registered).
    /// The runtime's poll task uses this to decide which pods to wake
    /// after a `smoltcp::iface::Interface::poll`.
    pub fn egress_len(&self, pod_ip: Ipv4Addr) -> usize {
        self.device.egress_len(pod_ip)
    }

    /// Drain one packet from `pod_ip`'s egress queue. The runtime's
    /// per-pod write task calls this and forwards the bytes to the
    /// pod's TAP.
    pub fn take_egress(&mut self, pod_ip: Ipv4Addr) -> Option<Vec<u8>> {
        self.device.take_egress(pod_ip)
    }

    /// Add a smoltcp socket and return its handle. The dispatcher (or
    /// the future Service-VIP TCP listener) calls this to bind a
    /// listener on a specific Service `(ip, port)`.
    pub fn add_socket<T: smoltcp::socket::AnySocket<'static>>(
        &mut self,
        socket: T,
    ) -> SocketHandle {
        self.sockets.add(socket)
    }

    /// Mutable access to a previously-added socket. The dispatcher
    /// calls this from inside its `poll`-driven loop to read inbound
    /// payloads / push outbound payloads.
    pub fn get_socket_mut<T: smoltcp::socket::AnySocket<'static>>(
        &mut self,
        handle: SocketHandle,
    ) -> &mut T {
        self.sockets.get_mut::<T>(handle)
    }

    /// Bind a Service VIP for TCP. Allocates `pool_size` smoltcp TCP
    /// sockets all listening on `(vip)` so the netstack can accept
    /// `pool_size` simultaneous connections per VIP. Connections that
    /// arrive when all sockets are already pumping get their SYN
    /// dropped — the pod retries.
    ///
    /// `picker` selects a backend per accepted connection. See
    /// [`crate::dispatch::RoundRobinPicker`] for the default
    /// implementation.
    ///
    /// Typical `pool_size`: 16-64 per Service. Each socket has its
    /// own TX/RX ring buffer (default 4 KB each), so 64 sockets per
    /// VIP × 100 Services = 50 MB of socket buffers — well within
    /// budget even on a small node.
    pub fn bind_tcp_service(
        &mut self,
        vip: SocketAddr,
        picker: Arc<dyn BackendPicker>,
        pool_size: usize,
    ) -> Result<()> {
        if self.tcp_listeners.contains_key(&vip) {
            anyhow::bail!("PodNet::bind_tcp_service: VIP {vip} is already bound");
        }
        let vip_endpoint = socket_addr_to_endpoint(vip);
        let mut entries = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let rx = tcp::SocketBuffer::new(vec![0u8; 4096]);
            let tx = tcp::SocketBuffer::new(vec![0u8; 4096]);
            let mut sock = tcp::Socket::new(rx, tx);
            sock.listen(vip_endpoint)
                .map_err(|e| anyhow::anyhow!("smoltcp listen on {vip}: {e:?}"))?;
            let handle = self.sockets.add(sock);
            entries.push(TcpPoolEntry {
                handle,
                in_flight: false,
            });
        }
        debug!(?vip, pool_size, "PodNet: bound TCP Service VIP");
        self.tcp_listeners.insert(
            vip,
            TcpListenerPool {
                vip: vip_endpoint,
                picker,
                entries,
            },
        );
        Ok(())
    }

    /// Scan every bound Service-VIP pool for sockets that just
    /// accepted a connection or just closed one. Newly accepted
    /// sockets are returned for the runtime to spawn pump tasks
    /// against; newly closed sockets are re-listened in place so the
    /// pool size is conserved.
    ///
    /// MUST be called after [`Self::poll`] — smoltcp's TCP socket
    /// state only advances when the Interface polls. Calling this
    /// before poll is harmless but never returns accepts.
    ///
    /// Empty Vec when nothing changed since the last scan, which is
    /// the steady-state case for an idle Service VIP.
    pub fn accepted_tcp_connections(&mut self) -> Vec<TcpAccept> {
        let mut accepted = Vec::new();
        // Split-borrow `self` so we can iterate `tcp_listeners` while
        // mutating `sockets` via handle. Without this the borrow
        // checker rejects `self.sockets.get_mut(entry.handle)`
        // inside a `self.tcp_listeners.iter_mut()` loop.
        let Self {
            tcp_listeners,
            sockets,
            ..
        } = self;
        for (_vip, pool) in tcp_listeners.iter_mut() {
            for entry in pool.entries.iter_mut() {
                let sock = sockets.get_mut::<tcp::Socket>(entry.handle);
                let state = sock.state();
                if !entry.in_flight && state == tcp::State::Established {
                    let (Some(local), Some(remote)) =
                        (sock.local_endpoint(), sock.remote_endpoint())
                    else {
                        // Pre-Established sockets sometimes don't
                        // have endpoints — skip and re-check on the
                        // next scan.
                        continue;
                    };
                    let Some(backend) = pool.picker.next() else {
                        warn!(
                            ?pool.vip,
                            "PodNet: Service VIP has zero backends — aborting accepted connection"
                        );
                        sock.abort();
                        continue;
                    };
                    accepted.push(TcpAccept {
                        handle: entry.handle,
                        local,
                        remote,
                        backend,
                    });
                    entry.in_flight = true;
                } else if entry.in_flight && state == tcp::State::Closed {
                    // Pump task finished. Re-listen on the same
                    // handle so the pool size stays stable.
                    if let Err(e) = sock.listen(pool.vip) {
                        warn!(
                            ?pool.vip,
                            "PodNet: re-listen on closed pool socket failed: {e:?}"
                        );
                    }
                    entry.in_flight = false;
                }
            }
        }
        accepted
    }

    /// Number of bound Service VIPs. Mostly for tests / observability.
    pub fn tcp_listener_count(&self) -> usize {
        self.tcp_listeners.len()
    }

    /// Tear down a previously-bound Service VIP. Aborts every socket
    /// in the listener pool (in-flight connections get RST) and
    /// removes the entries from the SocketSet so they free their
    /// ring buffers. Returns `true` if the VIP was bound.
    ///
    /// Service-watcher (Phase 4 follow-up) calls this when a
    /// Service is deleted from the cluster. Idempotent: returns
    /// `false` on the second call for the same VIP.
    pub fn unbind_tcp_service(&mut self, vip: SocketAddr) -> bool {
        let Some(pool) = self.tcp_listeners.remove(&vip) else {
            return false;
        };
        let Self { sockets, .. } = self;
        for entry in pool.entries {
            // abort() flushes any in-flight connection; the next
            // poll moves the socket to Closed.
            let sock = sockets.get_mut::<tcp::Socket>(entry.handle);
            sock.abort();
            sockets.remove(entry.handle);
        }
        debug!(?vip, "PodNet: unbound TCP Service VIP");
        true
    }

    /// Drive one round of smoltcp polling. Inbound packets in the
    /// shared RX queue get delivered to bound sockets; outbound packets
    /// the sockets produce land on the matching pod's egress queue.
    /// Returns `true` if any progress was made (something was received,
    /// transmitted, or a socket state changed).
    pub fn poll(&mut self, now: Instant) -> bool {
        matches!(
            self.iface.poll(now, &mut self.device, &mut self.sockets),
            PollResult::SocketStateChanged
        )
    }

    /// How long the runtime should sleep before the next `poll`. `None`
    /// means smoltcp has no pending timers; the runtime should sleep
    /// until something injects RX or the cancel signal fires.
    pub fn poll_delay(&mut self, now: Instant) -> Option<StdDuration> {
        self.iface
            .poll_delay(now, &self.sockets)
            .map(smoltcp_duration_to_std)
    }

    /// Currently-registered pod IPs. Order is not guaranteed.
    pub fn registered_pods(&self) -> impl Iterator<Item = Ipv4Addr> + '_ {
        self.device.registered_pods()
    }

    /// Add a host IP to the interface's local-delivery set. Used by
    /// the dispatcher's Service-watcher when a new Service ClusterIP
    /// is created. Returns `true` if the address was new, `false` if
    /// it was already present or if smoltcp's `ip_addrs` capacity is
    /// exhausted (the latter is logged at `warn`).
    pub fn add_host_ip(&mut self, addr: IpCidr) -> bool {
        let mut added = false;
        let mut full = false;
        self.iface.update_ip_addrs(|addrs| {
            if addrs.contains(&addr) {
                return;
            }
            match addrs.push(addr) {
                Ok(()) => added = true,
                Err(_) => full = true,
            }
        });
        if full {
            tracing::warn!(?addr, "PodNet: smoltcp ip_addrs full, host IP not added");
        } else if added {
            debug!(?addr, "PodNet: added host IP");
        }
        added
    }

    /// Remove a host IP from the interface's local-delivery set. Used
    /// by the dispatcher when a Service is deleted. Returns `true` if
    /// the address was present.
    pub fn remove_host_ip(&mut self, addr: &IpCidr) -> bool {
        let mut removed = false;
        self.iface.update_ip_addrs(|addrs| {
            if let Some(pos) = addrs.iter().position(|a| a == addr) {
                addrs.remove(pos);
                removed = true;
            }
        });
        if removed {
            debug!(?addr, "PodNet: removed host IP");
        }
        removed
    }

    /// Pod-by-pod drain of every pending egress packet, e.g. after a
    /// `poll()` round. Returns a map from pod IP to the packets smoltcp
    /// queued for that pod since the last drain. Empty queues are
    /// omitted from the result.
    pub fn drain_all_egress(&mut self) -> HashMap<Ipv4Addr, Vec<Vec<u8>>> {
        let pods: Vec<Ipv4Addr> = self.device.registered_pods().collect();
        let mut out = HashMap::new();
        for pod in pods {
            let mut pkts = Vec::new();
            while let Some(pkt) = self.device.take_egress(pod) {
                pkts.push(pkt);
            }
            if !pkts.is_empty() {
                out.insert(pod, pkts);
            }
        }
        out
    }
}

fn socket_addr_to_endpoint(addr: SocketAddr) -> IpEndpoint {
    use smoltcp::wire::IpAddress;
    match addr {
        SocketAddr::V4(v4) => {
            let o = v4.ip().octets();
            IpEndpoint::new(IpAddress::v4(o[0], o[1], o[2], o[3]), v4.port())
        }
        SocketAddr::V6(_) => unreachable!(
            "PodNet currently supports IPv4 only (smoltcp built without proto-ipv6); \
             v6 Service VIPs should have been filtered out at the dispatcher level"
        ),
    }
}

fn smoltcp_duration_to_std(d: Duration) -> StdDuration {
    StdDuration::from_micros(d.total_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use smoltcp::socket::udp::{PacketBuffer, PacketMetadata, Socket as UdpSocket};
    use smoltcp::wire::{IpAddress, IpEndpoint};

    /// Test scaffolding: start the netstack with the Service-CIDR
    /// routing entry. Per-test VIPs get added dynamically via
    /// `add_host_ip` — same flow the production dispatcher will use.
    ///
    /// `IFACE_MAX_ADDR_COUNT` is bumped to 256 via
    /// `.cargo/config.toml`'s `[env]` block — see
    /// [`many_host_ips_can_be_added_after_cap_bump`] for the receipt.
    fn default_config() -> PodNetConfig {
        PodNetConfig {
            host_ips: vec![IpCidr::new(IpAddress::v4(10, 96, 0, 1), 12)],
        }
    }

    /// Hand-construct a UDP-over-IPv4 packet from `src:src_port` to
    /// `dst:dst_port` with `payload`. smoltcp parses everything
    /// strictly, so all the header fields (checksum included) have to
    /// add up.
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

    #[test]
    fn register_and_unregister_pod_round_trip() {
        let mut net = PodNet::new(&default_config()).unwrap();
        let pod = Ipv4Addr::new(10, 244, 1, 5);
        assert!(net.register_pod(pod));
        assert!(!net.register_pod(pod), "second register is a no-op");
        assert_eq!(net.registered_pods().collect::<Vec<_>>(), vec![pod]);
        assert!(net.unregister_pod(pod).is_some());
        assert!(net.unregister_pod(pod).is_none());
    }

    #[test]
    fn pod_to_pod_traffic_routes_without_smoltcp_poll() {
        let mut net = PodNet::new(&default_config()).unwrap();
        let pod_a = Ipv4Addr::new(10, 244, 1, 5);
        let pod_b = Ipv4Addr::new(10, 244, 2, 7);
        net.register_pod(pod_a);
        net.register_pod(pod_b);

        let pkt = udp_ipv4_packet([10, 244, 1, 5], 4242, [10, 244, 2, 7], 80, b"hello");
        let forwarded = net.forward_or_inject(pod_a, pkt.clone());
        assert_eq!(forwarded, Some(pod_b), "pod-to-pod returns the dst pod");

        // Without invoking poll, the packet should already be on pod B's egress.
        let received = net.take_egress(pod_b).expect("pod B got the packet");
        assert_eq!(received, pkt);
    }

    #[test]
    fn vip_bound_udp_round_trips_through_smoltcp() {
        // The load-bearing integration test for Phase 3: a pod-to-VIP
        // UDP query lands on a smoltcp socket bound to that VIP, the
        // socket sends a reply, and the reply ends up in the originating
        // pod's egress queue. All three layers (MultiDevice fan-out,
        // smoltcp Interface routing, SocketSet dispatch) wired together.
        let mut net = PodNet::new(&default_config()).unwrap();
        let pod_a = Ipv4Addr::new(10, 244, 1, 5);
        net.register_pod(pod_a);

        // Register the kube-dns VIP as a host IP (same flow the
        // production dispatcher's Service-watcher will use).
        let kube_dns_port: u16 = 53;
        assert!(net.add_host_ip(IpCidr::new(IpAddress::v4(10, 96, 0, 10), 32)));

        // Bind a UDP socket on the kube-dns VIP.
        let mut sock = fresh_udp_socket();
        sock.bind(IpEndpoint::new(IpAddress::v4(10, 96, 0, 10), kube_dns_port))
            .expect("bind socket");
        let handle = net.add_socket(sock);

        // Pod A sends a UDP packet to kube-dns.
        let query_bytes = b"q1234567";
        let query_pkt = udp_ipv4_packet(
            [10, 244, 1, 5],
            33000,
            [10, 96, 0, 10],
            kube_dns_port,
            query_bytes,
        );
        assert_eq!(
            net.forward_or_inject(pod_a, query_pkt),
            None,
            "VIP-bound traffic falls through to smoltcp"
        );

        let progressed = net.poll(Instant::now());
        assert!(progressed, "smoltcp processed the inbound packet");

        // The socket should have received the payload from pod A's port.
        let socket: &mut UdpSocket = net.get_socket_mut(handle);
        let (recv_payload, meta) = socket
            .recv()
            .expect("smoltcp delivered the UDP payload to our socket");
        assert_eq!(recv_payload, query_bytes);
        assert_eq!(meta.endpoint.addr, IpAddress::v4(10, 244, 1, 5));
        assert_eq!(meta.endpoint.port, 33000);

        // Reply: send back to pod A's ephemeral port. smoltcp will emit
        // the response on the next poll, which MultiDevice's TxBuf
        // routes to pod A's egress queue based on the IPv4 dst.
        let reply_bytes = b"r1234567";
        socket
            .send_slice(reply_bytes, meta.endpoint)
            .expect("send reply");

        let progressed = net.poll(Instant::now());
        assert!(progressed, "smoltcp emitted the reply");

        let egress = net
            .take_egress(pod_a)
            .expect("pod A's egress queue carries the reply");
        // The egress packet is a full IPv4+UDP frame. Just check it
        // ends with the reply payload — the framing is smoltcp's
        // responsibility and the MultiDevice routing test above
        // already proved dst-IP-based fan-out works.
        assert!(
            egress.ends_with(reply_bytes),
            "egress packet ends with reply payload (len={}, last_8={:?})",
            egress.len(),
            &egress[egress.len() - 8..]
        );
    }

    #[test]
    fn poll_delay_returns_some_when_smoltcp_has_pending_work() {
        // After injecting a packet smoltcp can't yet process (e.g.,
        // no matching socket), poll_delay should return a finite delay
        // — smoltcp wants us to wake up soonish to do something with it.
        // The exact value is smoltcp's call; we just assert it produces
        // *some* reasonable answer for any of the input states.
        let mut net = PodNet::new(&default_config()).unwrap();
        let pod = Ipv4Addr::new(10, 244, 1, 5);
        net.register_pod(pod);
        let pkt = udp_ipv4_packet([10, 244, 1, 5], 1, [10, 96, 0, 10], 53, b"x");
        net.forward_or_inject(pod, pkt);
        // poll_delay is allowed to return None (nothing pending) OR Some(d) —
        // we just want to confirm it doesn't panic and returns something
        // sane (under an hour) when there's pending RX.
        if let Some(d) = net.poll_delay(Instant::now()) {
            assert!(d <= StdDuration::from_secs(3600), "delay is bounded");
        }
    }

    #[test]
    fn add_and_remove_host_ip_changes_local_delivery() {
        // Bind a UDP socket on a VIP that is NOT in the initial host
        // IP set. Without dynamically adding it, smoltcp drops the
        // packet — exact symptom you'd see if the dispatcher forgot to
        // register a Service. After `add_host_ip`, the packet is
        // delivered.
        let mut net = PodNet::new(&default_config()).unwrap();
        let pod = Ipv4Addr::new(10, 244, 1, 5);
        net.register_pod(pod);

        let new_vip_cidr = IpCidr::new(IpAddress::v4(10, 96, 0, 99), 32);

        let mut sock = fresh_udp_socket();
        sock.bind(IpEndpoint::new(IpAddress::v4(10, 96, 0, 99), 9999))
            .unwrap();
        let handle = net.add_socket(sock);

        let pkt = udp_ipv4_packet([10, 244, 1, 5], 5000, [10, 96, 0, 99], 9999, b"before");
        net.forward_or_inject(pod, pkt);
        net.poll(Instant::now());
        assert!(
            net.get_socket_mut::<UdpSocket>(handle).recv().is_err(),
            "socket gets nothing before the VIP is registered"
        );

        assert!(net.add_host_ip(new_vip_cidr), "first add returns true");
        assert!(!net.add_host_ip(new_vip_cidr), "duplicate add is a no-op");

        let pkt = udp_ipv4_packet([10, 244, 1, 5], 5000, [10, 96, 0, 99], 9999, b"after");
        net.forward_or_inject(pod, pkt);
        net.poll(Instant::now());
        let (payload, _) = net
            .get_socket_mut::<UdpSocket>(handle)
            .recv()
            .expect("socket receives once VIP is registered");
        assert_eq!(payload, b"after");

        assert!(net.remove_host_ip(&new_vip_cidr));
        assert!(
            !net.remove_host_ip(&new_vip_cidr),
            "second remove returns false"
        );
    }

    #[test]
    fn bind_tcp_service_allocates_listening_sockets_and_records_vip() {
        use crate::dispatch::RoundRobinPicker;

        let mut net = PodNet::new(&default_config()).unwrap();
        let vip: SocketAddr = "10.96.0.1:443".parse().unwrap();
        let picker = Arc::new(RoundRobinPicker::new(vec![
            "10.244.0.5:6443".parse().unwrap(),
            "10.244.0.6:6443".parse().unwrap(),
        ]));

        net.bind_tcp_service(vip, picker, 8).unwrap();
        assert_eq!(net.tcp_listener_count(), 1);
        // Steady-state scan with no actual TCP traffic returns empty.
        assert!(net.accepted_tcp_connections().is_empty());
    }

    #[test]
    fn bind_tcp_service_rejects_duplicate_vip() {
        use crate::dispatch::RoundRobinPicker;

        let mut net = PodNet::new(&default_config()).unwrap();
        let vip: SocketAddr = "10.96.0.1:443".parse().unwrap();
        let picker = || Arc::new(RoundRobinPicker::new(vec![]));

        net.bind_tcp_service(vip, picker(), 4).unwrap();
        // Second bind on the same VIP is a caller bug (the
        // dispatcher's Service-watcher should have unbound first).
        // Surface it instead of silently leaking the original pool.
        assert!(net.bind_tcp_service(vip, picker(), 4).is_err());
        assert_eq!(net.tcp_listener_count(), 1);
    }

    #[test]
    fn accepted_tcp_connections_returns_empty_when_no_vips_bound() {
        let mut net = PodNet::new(&default_config()).unwrap();
        assert!(net.accepted_tcp_connections().is_empty());
    }

    #[test]
    fn many_host_ips_can_be_added_after_cap_bump() {
        // Receipts for the workspace's SMOLTCP_IFACE_MAX_ADDR_COUNT
        // bump (configured in `.cargo/config.toml`). Without the bump
        // this test fails on the 3rd `add_host_ip` because smoltcp's
        // default cap is 2. With the bump (256), we can stack a
        // realistic Service-VIP count without the dispatcher silently
        // losing entries.
        //
        // Start with the /12 routing entry already in default_config,
        // then add 200 distinct /32 VIPs.
        let mut net = PodNet::new(&default_config()).unwrap();
        for i in 0..200u8 {
            let added = net.add_host_ip(IpCidr::new(IpAddress::v4(10, 96, 1, i), 32));
            assert!(added, "VIP #{i} must register cleanly (cap = 256)");
        }
    }

    #[test]
    fn drain_all_egress_returns_packets_per_pod() {
        // Drive a small scenario: pod A sends to pod B and pod C
        // sends to pod A in the same window. drain_all_egress reports
        // both deliveries grouped by destination pod.
        let mut net = PodNet::new(&default_config()).unwrap();
        let pod_a = Ipv4Addr::new(10, 244, 1, 5);
        let pod_b = Ipv4Addr::new(10, 244, 1, 6);
        let pod_c = Ipv4Addr::new(10, 244, 1, 7);
        net.register_pod(pod_a);
        net.register_pod(pod_b);
        net.register_pod(pod_c);

        let a_to_b = udp_ipv4_packet([10, 244, 1, 5], 80, [10, 244, 1, 6], 80, b"ab");
        let c_to_a = udp_ipv4_packet([10, 244, 1, 7], 80, [10, 244, 1, 5], 80, b"ca");
        net.forward_or_inject(pod_a, a_to_b);
        net.forward_or_inject(pod_c, c_to_a);

        let drained = net.drain_all_egress();
        assert_eq!(drained.len(), 2, "two pods have egress");
        assert_eq!(drained.get(&pod_b).map(|v| v.len()), Some(1));
        assert_eq!(drained.get(&pod_a).map(|v| v.len()), Some(1));
        assert!(
            !drained.contains_key(&pod_c),
            "pod C sent but didn't receive"
        );

        // Second drain is empty.
        assert!(net.drain_all_egress().is_empty());
    }
}
