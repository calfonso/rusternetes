//! Per-pod TAP multiplexer — **Phase 3 primitive**.
//!
//! The Phase 2 spike used a single TAP and a single smoltcp `Device`
//! ([`crate::iface::RingDevice`]) — enough to prove the smoltcp +
//! `Dispatcher` byte path round-trips DNS without a kernel socket, but
//! useless for the cross-pod traffic that motivated the netstack in the
//! first place. Phase 3 generalises to N pods, each with its own TAP,
//! all sharing **one** smoltcp `Interface` so smoltcp can route between
//! them through its own forwarding table.
//!
//! ### Why one Interface, many TAPs
//!
//! smoltcp's [`Device`] trait represents one "wire". An `Interface`
//! owns exactly one `Device`. To route between pods inside smoltcp we
//! pretend each pod-TAP is just another link on the same wire by
//! pushing every pod's ingress packets into one shared RX queue (smoltcp
//! sees them as if they all came from the same network) and fanning out
//! each TX packet to the right pod-TAP's egress queue by parsing the
//! IPv4 destination address from the packet smoltcp wrote.
//!
//! This is the classic "router-on-a-stick" pattern adapted for smoltcp:
//!
//! ```text
//!         ┌──── tokio: read TAP-A ──► push to shared rx_queue
//!         │
//!   pod-A TAP                         ┌── smoltcp Interface ──┐
//!         │                           │                       │
//!         └──── tokio: write TAP-A ◄──┤  MultiDevice          │
//!                                     │   ├ rx_queue (shared) │
//!         ┌──── tokio: read TAP-B ──► │   └ egress[pod-A.ip]  │
//!         │                           │     egress[pod-B.ip]  │
//!   pod-B TAP                         │     egress[pod-N.ip]  │
//!         │                           │                       │
//!         └──── tokio: write TAP-B ◄──┘                       │
//!                                     └───────────────────────┘
//! ```
//!
//! ### What lives in this module
//!
//! - [`MultiDevice`] — the smoltcp `Device` impl with per-pod IPv4
//!   fan-out on TX and a shared RX queue. Pure data plane; no I/O.
//! - [`MultiDevice::register_pod`] / [`MultiDevice::unregister_pod`] —
//!   the pod lifecycle hook. Each pod gets an egress queue keyed by its
//!   IPv4 address. Unknown destinations are dropped and counted.
//! - [`MultiDevice::inject_rx`] / [`MultiDevice::take_egress`] — the
//!   pump points the runtime task uses to bridge tokio-async TAPs and
//!   smoltcp's poll loop. The same pair is what unit tests use to
//!   exercise routing without ever opening a TAP.
//!
//! ### Out of scope for this commit (Phase 3 follow-ups)
//!
//! - The actual N-tokio-tasks-per-N-TAPs runtime that drives smoltcp
//!   `Interface::poll()` and pumps every TAP's read/write halves into
//!   `MultiDevice`. Lands in the next commit once we can gate the
//!   example on `unshare -U -n`.
//! - kubelet integration that calls `register_pod` / `unregister_pod`
//!   as pods are scheduled / torn down.
//! - IPv6. The Phase 2 spike enabled only `smoltcp/proto-ipv4`; pod
//!   IPv6 plumbing is out of Phase 3 scope.
//! - Ethernet medium (the spike enables `smoltcp/medium-ip` only).
//!   Adding `medium-ethernet` later just means handling the 14-byte
//!   L2 offset in `parse_ipv4_dst`.

use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;
use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use tracing::{debug, trace};

/// Maximum IP packet size buffered between TAPs and smoltcp. Matches
/// [`crate::iface`]'s MTU; jumbo frames need a separate pod-network MTU
/// story we don't have yet.
const MTU: usize = 1500;

/// smoltcp `Device` that fans out TX by IPv4 destination across N
/// per-pod egress queues, and pulls RX from one shared queue every TAP
/// reader pushes into.
///
/// See the module-level docs for the data-plane diagram.
pub struct MultiDevice {
    /// Shared ingress — every per-TAP reader pushes packets here.
    rx_queue: VecDeque<Vec<u8>>,
    /// Per-pod egress queues. Keyed by the pod's IPv4 address (the
    /// destination address smoltcp will write into the IP header when
    /// it transmits a packet for that pod).
    egress: HashMap<Ipv4Addr, VecDeque<Vec<u8>>>,
    /// Counter of TX packets dropped because their destination IP is not
    /// registered. Surfaced via [`MultiDevice::drops_unknown_dst`] so
    /// the runtime / tests can assert on it without having to scrape
    /// logs.
    drops_unknown_dst: u64,
}

impl MultiDevice {
    /// Construct an empty device with no pods registered. The runtime
    /// (or the test) calls [`MultiDevice::register_pod`] for each pod
    /// before driving smoltcp.
    pub fn new() -> Self {
        Self {
            rx_queue: VecDeque::with_capacity(64),
            egress: HashMap::new(),
            drops_unknown_dst: 0,
        }
    }

    /// Add an egress queue for `pod_ip`. Returns `true` if this is a
    /// new registration, `false` if the IP was already known (the
    /// existing queue is preserved unchanged so in-flight TX is not
    /// dropped).
    pub fn register_pod(&mut self, pod_ip: Ipv4Addr) -> bool {
        let new = !self.egress.contains_key(&pod_ip);
        self.egress.entry(pod_ip).or_default();
        if new {
            debug!(?pod_ip, "MultiDevice: registered pod");
        }
        new
    }

    /// Remove the egress queue for `pod_ip`. Returns the drained queue
    /// (so the caller can decide whether to surface still-pending
    /// packets to the pod's TAP one last time, or drop them).
    pub fn unregister_pod(&mut self, pod_ip: Ipv4Addr) -> Option<VecDeque<Vec<u8>>> {
        let removed = self.egress.remove(&pod_ip);
        if removed.is_some() {
            debug!(?pod_ip, "MultiDevice: unregistered pod");
        }
        removed
    }

    /// Push a packet read from any pod's TAP into the shared RX queue.
    /// smoltcp will see it on its next [`Device::receive`] call.
    ///
    /// This is the "always-let-smoltcp-handle-it" path — useful in tests
    /// to drive smoltcp directly, and the right call for packets whose
    /// destination is the netstack's own host IPs (Service VIPs). For
    /// the multi-pod runtime use [`forward_or_inject`](Self::forward_or_inject)
    /// which short-circuits pod-to-pod traffic without going through
    /// smoltcp.
    pub fn inject_rx(&mut self, packet: Vec<u8>) {
        self.rx_queue.push_back(packet);
    }

    /// Receive a packet read from any pod's TAP and route it:
    ///
    /// - If the destination IPv4 address belongs to **another registered
    ///   pod**, push it straight onto that pod's egress queue — pod-to-pod
    ///   traffic never touches smoltcp, just one HashMap lookup.
    /// - Otherwise (destination is one of the netstack's own host IPs,
    ///   or unparseable), fall through to the shared RX queue so smoltcp
    ///   gets the packet on its next [`Device::receive`].
    ///
    /// `from_pod` is the address of the pod whose TAP this packet was
    /// read from — used to avoid the silly loopback case where a pod
    /// somehow sends a packet to itself and the runtime echoes it back.
    /// Returns `true` if the packet was forwarded directly to another
    /// pod (smoltcp will not see it).
    pub fn forward_or_inject(&mut self, from_pod: Ipv4Addr, packet: Vec<u8>) -> bool {
        if let Some(dst) = parse_ipv4_dst(&packet) {
            if dst != from_pod {
                if let Some(queue) = self.egress.get_mut(&dst) {
                    trace!(
                        ?from_pod,
                        ?dst,
                        len = packet.len(),
                        "MultiDevice: pod-to-pod fast-path forward (bypasses smoltcp)"
                    );
                    queue.push_back(packet);
                    return true;
                }
            }
        }
        self.rx_queue.push_back(packet);
        false
    }

    /// Drain one packet from `pod_ip`'s egress queue. Used by the
    /// per-TAP write task to forward smoltcp-emitted packets to the
    /// pod's TAP. Returns `None` if the pod has no pending egress (or
    /// is not registered).
    pub fn take_egress(&mut self, pod_ip: Ipv4Addr) -> Option<Vec<u8>> {
        self.egress.get_mut(&pod_ip)?.pop_front()
    }

    /// Count of TX packets dropped because no pod was registered for
    /// the destination IP smoltcp wrote.
    pub fn drops_unknown_dst(&self) -> u64 {
        self.drops_unknown_dst
    }

    /// Currently-registered pod IPs. Order is not guaranteed.
    pub fn registered_pods(&self) -> impl Iterator<Item = Ipv4Addr> + '_ {
        self.egress.keys().copied()
    }
}

impl Default for MultiDevice {
    fn default() -> Self {
        Self::new()
    }
}

impl Device for MultiDevice {
    type RxToken<'a> = MultiRx;
    type TxToken<'a> = MultiTx<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = MTU;
        caps
    }

    fn receive(&mut self, _ts: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let bytes = self.rx_queue.pop_front()?;
        trace!(
            len = bytes.len(),
            "MultiDevice: smoltcp consuming RX packet"
        );
        Some((MultiRx(bytes), MultiTx(self)))
    }

    fn transmit(&mut self, _ts: Instant) -> Option<Self::TxToken<'_>> {
        Some(MultiTx(self))
    }
}

/// One-shot RX token holding the bytes smoltcp is about to parse.
pub struct MultiRx(Vec<u8>);

impl RxToken for MultiRx {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.0)
    }
}

/// One-shot TX token that, on `consume`, parses the IPv4 destination
/// address smoltcp wrote and routes the frame to the matching pod's
/// egress queue. Unknown destinations bump [`MultiDevice::drops_unknown_dst`].
pub struct MultiTx<'a>(&'a mut MultiDevice);

impl<'a> TxToken for MultiTx<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buf = vec![0u8; len];
        let r = f(&mut buf);
        match parse_ipv4_dst(&buf) {
            Some(dst) => match self.0.egress.get_mut(&dst) {
                Some(q) => {
                    trace!(?dst, len, "MultiDevice: TX routed to pod");
                    q.push_back(buf);
                }
                None => {
                    self.0.drops_unknown_dst += 1;
                    trace!(
                        ?dst,
                        len,
                        "MultiDevice: TX dropped — destination IP not registered"
                    );
                }
            },
            None => {
                self.0.drops_unknown_dst += 1;
                trace!(len, "MultiDevice: TX dropped — cannot parse IPv4 dst");
            }
        }
        r
    }
}

/// Extract the IPv4 destination address from a packet smoltcp emitted
/// over a `Medium::Ip` device.
///
/// Returns `None` for too-short buffers, non-IPv4 packets, or anything
/// else we can't make sense of as an IPv4 frame. Callers MUST treat
/// `None` as "drop this packet" — the multiplexer has no other way to
/// pick an egress queue.
fn parse_ipv4_dst(buf: &[u8]) -> Option<Ipv4Addr> {
    // IPv4 header is at least 20 bytes; the first nibble of byte 0 must
    // be `4` (the version); the destination address is bytes 16..20.
    if buf.len() < 20 {
        return None;
    }
    if (buf[0] >> 4) != 4 {
        return None;
    }
    Some(Ipv4Addr::new(buf[16], buf[17], buf[18], buf[19]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal IPv4 packet (20-byte header, configurable payload).
    /// Header fields beyond src/dst are best-effort — smoltcp's TX
    /// path produces real packets, this helper just lets us drive
    /// `MultiDevice` directly in tests.
    fn ipv4_packet(src: [u8; 4], dst: [u8; 4], payload: &[u8]) -> Vec<u8> {
        let total_len = 20 + payload.len();
        let mut buf = Vec::with_capacity(total_len);
        buf.push(0x45); // version=4, IHL=5
        buf.push(0x00); // DSCP/ECN
        buf.extend_from_slice(&(total_len as u16).to_be_bytes()); // total length
        buf.extend_from_slice(&[0, 0]); // identification
        buf.extend_from_slice(&[0, 0]); // flags + frag offset
        buf.push(64); // TTL
        buf.push(17); // protocol (UDP)
        buf.extend_from_slice(&[0, 0]); // header checksum (not validated)
        buf.extend_from_slice(&src);
        buf.extend_from_slice(&dst);
        buf.extend_from_slice(payload);
        buf
    }

    fn transmit_via_smoltcp_path(dev: &mut MultiDevice, packet: &[u8]) {
        let token = dev
            .transmit(Instant::now())
            .expect("transmit returns token");
        token.consume(packet.len(), |out| out.copy_from_slice(packet));
    }

    #[test]
    fn register_pod_creates_egress_queue_and_returns_true_only_once() {
        let mut dev = MultiDevice::new();
        let ip = Ipv4Addr::new(10, 244, 1, 5);
        assert!(dev.register_pod(ip), "first registration is new");
        assert!(!dev.register_pod(ip), "second registration is a no-op");
        assert_eq!(dev.registered_pods().count(), 1);
    }

    #[test]
    fn unregister_pod_returns_pending_egress_and_removes_queue() {
        let mut dev = MultiDevice::new();
        let ip = Ipv4Addr::new(10, 244, 1, 5);
        dev.register_pod(ip);
        let pkt = ipv4_packet([10, 244, 1, 1], [10, 244, 1, 5], b"hi");
        transmit_via_smoltcp_path(&mut dev, &pkt);

        let drained = dev
            .unregister_pod(ip)
            .expect("unregister returns the pending queue");
        assert_eq!(drained.len(), 1, "the queued packet is handed back");
        assert!(
            dev.unregister_pod(ip).is_none(),
            "second unregister returns None"
        );
        assert_eq!(dev.registered_pods().count(), 0);
    }

    #[test]
    fn tx_routes_by_ipv4_dst_to_the_correct_pod_egress_queue() {
        let mut dev = MultiDevice::new();
        let pod_a = Ipv4Addr::new(10, 244, 1, 5);
        let pod_b = Ipv4Addr::new(10, 244, 2, 7);
        dev.register_pod(pod_a);
        dev.register_pod(pod_b);

        let pkt_to_a = ipv4_packet([10, 244, 1, 1], [10, 244, 1, 5], b"ping-a");
        let pkt_to_b = ipv4_packet([10, 244, 2, 1], [10, 244, 2, 7], b"ping-b");
        transmit_via_smoltcp_path(&mut dev, &pkt_to_a);
        transmit_via_smoltcp_path(&mut dev, &pkt_to_b);

        assert_eq!(
            dev.take_egress(pod_a).as_deref(),
            Some(&pkt_to_a[..]),
            "pod-A receives only its own packet"
        );
        assert_eq!(
            dev.take_egress(pod_b).as_deref(),
            Some(&pkt_to_b[..]),
            "pod-B receives only its own packet"
        );
        assert!(dev.take_egress(pod_a).is_none(), "no further pod-A traffic");
        assert!(dev.take_egress(pod_b).is_none(), "no further pod-B traffic");
        assert_eq!(dev.drops_unknown_dst(), 0);
    }

    #[test]
    fn tx_to_unregistered_dst_drops_and_increments_counter() {
        let mut dev = MultiDevice::new();
        let known = Ipv4Addr::new(10, 244, 1, 5);
        dev.register_pod(known);

        let pkt = ipv4_packet([10, 244, 1, 1], [10, 244, 99, 99], b"who?");
        transmit_via_smoltcp_path(&mut dev, &pkt);

        assert_eq!(dev.drops_unknown_dst(), 1);
        assert!(
            dev.take_egress(known).is_none(),
            "registered pod's queue is not touched"
        );
    }

    #[test]
    fn tx_with_unparseable_buffer_drops_and_increments_counter() {
        let mut dev = MultiDevice::new();
        // 19 bytes — one less than a minimal IPv4 header.
        let runt = vec![0u8; 19];
        transmit_via_smoltcp_path(&mut dev, &runt);
        assert_eq!(dev.drops_unknown_dst(), 1, "runt packets count as drops");

        // 20 bytes but version=6 (the version nibble is in the high
        // 4 bits of byte 0).
        let mut wrong_version = vec![0u8; 20];
        wrong_version[0] = 0x60;
        transmit_via_smoltcp_path(&mut dev, &wrong_version);
        assert_eq!(
            dev.drops_unknown_dst(),
            2,
            "non-IPv4 packets count as drops"
        );
    }

    #[test]
    fn rx_returns_injected_packets_in_fifo_order_then_none() {
        let mut dev = MultiDevice::new();
        dev.inject_rx(vec![1, 2, 3]);
        dev.inject_rx(vec![4, 5, 6]);

        let (rx1, _tx) = dev.receive(Instant::now()).expect("first packet");
        rx1.consume(|bytes| assert_eq!(bytes, &[1, 2, 3]));

        let (rx2, _tx) = dev.receive(Instant::now()).expect("second packet");
        rx2.consume(|bytes| assert_eq!(bytes, &[4, 5, 6]));

        assert!(
            dev.receive(Instant::now()).is_none(),
            "queue drained, third receive yields None"
        );
    }

    #[test]
    fn capabilities_report_ip_medium_and_spike_mtu() {
        let dev = MultiDevice::new();
        let caps = dev.capabilities();
        assert_eq!(caps.medium, Medium::Ip);
        assert_eq!(caps.max_transmission_unit, MTU);
    }

    #[test]
    fn forward_or_inject_short_circuits_pod_to_pod_traffic() {
        let mut dev = MultiDevice::new();
        let pod_a = Ipv4Addr::new(10, 244, 1, 5);
        let pod_b = Ipv4Addr::new(10, 244, 2, 7);
        dev.register_pod(pod_a);
        dev.register_pod(pod_b);

        let pkt = ipv4_packet([10, 244, 1, 5], [10, 244, 2, 7], b"pod-to-pod");
        let forwarded = dev.forward_or_inject(pod_a, pkt.clone());

        assert!(forwarded, "pod-to-pod traffic returns true");
        assert_eq!(
            dev.take_egress(pod_b).as_deref(),
            Some(&pkt[..]),
            "packet landed in destination pod's egress queue"
        );
        // The shared rx_queue must NOT have received this packet —
        // smoltcp should not see pod-to-pod traffic at all.
        assert!(
            dev.receive(Instant::now()).is_none(),
            "smoltcp's rx_queue stays empty for forwarded traffic"
        );
    }

    #[test]
    fn forward_or_inject_falls_through_to_rx_queue_for_service_vips() {
        let mut dev = MultiDevice::new();
        let pod_a = Ipv4Addr::new(10, 244, 1, 5);
        dev.register_pod(pod_a);

        // Destination 10.96.0.10 is a Service VIP — not a registered
        // pod IP — so this should land in the shared rx_queue for
        // smoltcp to dispatch.
        let pkt = ipv4_packet([10, 244, 1, 5], [10, 96, 0, 10], b"dns?");
        let forwarded = dev.forward_or_inject(pod_a, pkt.clone());

        assert!(!forwarded, "VIP-bound traffic was not forwarded directly");
        let (rx_token, _tx) = dev
            .receive(Instant::now())
            .expect("packet enqueued for smoltcp");
        rx_token.consume(|bytes| assert_eq!(bytes, &pkt[..]));
    }

    #[test]
    fn forward_or_inject_does_not_loop_packets_back_to_their_source_pod() {
        // If a misbehaving pod somehow sets src == dst == its own IP,
        // forwarding back to that pod's TAP would echo the packet and
        // create a loop. The from_pod guard prevents that — the packet
        // falls through to the shared rx_queue where smoltcp can drop
        // it normally.
        let mut dev = MultiDevice::new();
        let pod_a = Ipv4Addr::new(10, 244, 1, 5);
        dev.register_pod(pod_a);

        let pkt = ipv4_packet([10, 244, 1, 5], [10, 244, 1, 5], b"loop?");
        let forwarded = dev.forward_or_inject(pod_a, pkt.clone());

        assert!(!forwarded, "self-addressed packet must not be forwarded");
        assert!(
            dev.take_egress(pod_a).is_none(),
            "pod's own egress queue stays empty"
        );
    }

    #[test]
    fn forward_or_inject_falls_through_when_dst_pod_unregistered() {
        let mut dev = MultiDevice::new();
        let pod_a = Ipv4Addr::new(10, 244, 1, 5);
        dev.register_pod(pod_a);

        // Pod 10.244.3.99 isn't registered — packet must NOT be lost,
        // it falls through to smoltcp which will drop it via its own
        // routing (and may emit ICMP destination-unreachable, smoltcp's
        // call).
        let pkt = ipv4_packet([10, 244, 1, 5], [10, 244, 3, 99], b"orphan");
        let forwarded = dev.forward_or_inject(pod_a, pkt.clone());

        assert!(!forwarded);
        assert!(
            dev.receive(Instant::now()).is_some(),
            "smoltcp gets the orphan packet"
        );
    }
}
