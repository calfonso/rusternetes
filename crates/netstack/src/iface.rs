//! Bridge between an async TAP device ([`tokio_tun::Tun`]) and a
//! smoltcp `Interface`.
//!
//! ### Why this exists
//!
//! smoltcp ships with a synchronous `phy::TunTapInterface` (gated on the
//! `phy-tun-tap-interface` feature) that uses blocking `read(2)` / `write(2)`.
//! Embedding that in a tokio app would block the runtime. Instead we
//! own the TAP fd via `tokio_tun::Tun` (which returns Futures), pump
//! packets through a `RingDevice` that implements smoltcp's `Device`
//! trait, and drive `Interface::poll()` from a dedicated tokio task.
//!
//! ### Polling loop
//!
//! ```text
//!   ┌─ tokio task ──────────────────────────────────────────────────┐
//!   │                                                               │
//!   │  loop {                                                       │
//!   │    tokio::select! {                                           │
//!   │      pkt = tun.recv()  → push into device.rx_queue            │
//!   │      _ = sleep(poll_delay) → wake up to call iface.poll()     │
//!   │    }                                                          │
//!   │    iface.poll(now, &mut device, &mut sockets);                │
//!   │    while let Some(pkt) = device.tx_queue.pop_front() {        │
//!   │      tun.send(&pkt).await                                     │
//!   │    }                                                          │
//!   │    let poll_delay = iface.poll_delay(now, &sockets);          │
//!   │  }                                                            │
//!   │                                                               │
//!   └───────────────────────────────────────────────────────────────┘
//! ```
//!
//! ### Status
//!
//! Spike-level. Single TAP, single interface, single tokio task. Phase 3
//! will need a multiplexer that handles N TAPs (one per pod) sharing a
//! single smoltcp `Interface` — but the per-packet path is the same.

use anyhow::{Context, Result};
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;
use smoltcp::wire::HardwareAddress;
use std::collections::VecDeque;
use tokio::sync::Mutex;
use tracing::{debug, trace, warn};

/// Maximum IP packet size we'll buffer between the TAP and smoltcp.
/// 1500 (standard Ethernet MTU) is fine for the spike; jumbo frames
/// are out of scope until we have a real pod-network MTU story.
const MTU: usize = 1500;

/// In-process packet buffers between the tokio TAP I/O and smoltcp's
/// `Device::receive` / `transmit` polls.
///
/// The fields are deliberately public to the crate so the polling task
/// in [`run_interface`] can push/pop without going through the smoltcp
/// poll boundary on every packet. smoltcp itself only sees `RingDevice`
/// via its `Device` trait impl.
pub(crate) struct RingDevice {
    /// Packets read from the TAP, waiting for smoltcp to consume.
    pub(crate) rx_queue: VecDeque<Vec<u8>>,
    /// Packets smoltcp emitted, waiting for the TAP-write task.
    pub(crate) tx_queue: VecDeque<Vec<u8>>,
    medium: Medium,
    mtu: usize,
}

impl RingDevice {
    pub(crate) fn new(medium: Medium) -> Self {
        Self {
            rx_queue: VecDeque::with_capacity(64),
            tx_queue: VecDeque::with_capacity(64),
            medium,
            mtu: MTU,
        }
    }
}

impl Device for RingDevice {
    type RxToken<'a> = RxBuf;
    type TxToken<'a> = TxBuf<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = self.medium;
        caps.max_transmission_unit = self.mtu;
        caps
    }

    fn receive(&mut self, _ts: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let bytes = self.rx_queue.pop_front()?;
        trace!(len = bytes.len(), "smoltcp consuming RX packet");
        Some((RxBuf(bytes), TxBuf(&mut self.tx_queue)))
    }

    fn transmit(&mut self, _ts: Instant) -> Option<Self::TxToken<'_>> {
        Some(TxBuf(&mut self.tx_queue))
    }
}

/// One-shot RX token holding the bytes smoltcp is about to parse.
pub(crate) struct RxBuf(Vec<u8>);

impl RxToken for RxBuf {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.0)
    }
}

/// One-shot TX token that pushes the smoltcp-emitted frame onto the
/// shared TX queue for the TAP-write task to drain.
pub(crate) struct TxBuf<'a>(&'a mut VecDeque<Vec<u8>>);

impl<'a> TxToken for TxBuf<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buf = vec![0u8; len];
        let r = f(&mut buf);
        trace!(len, "smoltcp emitted TX packet");
        self.0.push_back(buf);
        r
    }
}

/// Configuration for a single TAP-backed smoltcp interface.
pub struct InterfaceConfig {
    /// TAP device name (must already exist, e.g. created via `ip tuntap add`).
    /// In the spike example we create it inside an unshare-d netns; in
    /// Phase 3 the kubelet creates one per pod.
    pub tap_name: String,
    /// Hardware address. For `Medium::Ip` (no L2 framing) this is
    /// unused; for `Medium::Ethernet` it MUST be unique on the L2
    /// segment.
    pub hw_addr: HardwareAddress,
    /// IP addresses to assign to this interface (the netstack's "host"
    /// addresses inside the netns). The spike uses
    /// `10.96.0.1/12` (Service CIDR base) so the in-process VIP routing
    /// table treats every Service IP as on-link.
    pub ip_addrs: Vec<smoltcp::wire::IpCidr>,
    /// L2 medium. `Medium::Ip` for a TUN-style device, `Medium::Ethernet`
    /// for TAP.
    pub medium: Medium,
}

/// Owned smoltcp interface + the in-process packet queues that connect
/// it to the TAP-IO task. The actual polling loop is in [`run_interface`];
/// this struct is what you hand to it.
pub struct NetIf {
    pub(crate) iface: Interface,
    pub(crate) device: RingDevice,
    pub(crate) sockets: SocketSet<'static>,
}

impl NetIf {
    /// Build a fresh `Interface` from `cfg`. No packets flow until
    /// [`run_interface`] is spawned against the same `NetIf` and a TAP
    /// handle.
    pub fn new(cfg: &InterfaceConfig) -> Result<Self> {
        let mut device = RingDevice::new(cfg.medium);
        let iface_config = Config::new(cfg.hw_addr);
        let mut iface = Interface::new(iface_config, &mut device, Instant::now());
        iface.update_ip_addrs(|addrs| {
            for ip in &cfg.ip_addrs {
                if addrs.push(*ip).is_err() {
                    warn!(addr = ?ip, "smoltcp ip_addrs full, dropping");
                }
            }
        });
        debug!(
            tap = %cfg.tap_name,
            medium = ?cfg.medium,
            mtu = MTU,
            addrs = ?cfg.ip_addrs,
            "smoltcp interface initialised"
        );
        Ok(Self {
            iface,
            device,
            sockets: SocketSet::new(vec![]),
        })
    }
}

/// Drive the polling loop until `cancel` is signalled (typically a
/// `tokio::sync::Notify` so callers can shut us down on SIGTERM).
///
/// ### Spike note
///
/// This is the simplest possible bridge — one packet per task wake-up.
/// A production version would batch reads and tune `poll_delay` more
/// carefully; for the spike we just need the round-trip to work end to
/// end.
///
/// `tun` is wrapped in a `Mutex` because both directions (RX from TAP,
/// TX to TAP) need exclusive access. For the spike this is fine; Phase
/// 4 will split it into separate read/write halves.
pub async fn run_interface(
    netif: std::sync::Arc<Mutex<NetIf>>,
    tun: std::sync::Arc<Mutex<tokio_tun::Tun>>,
    cancel: std::sync::Arc<tokio::sync::Notify>,
) -> Result<()> {
    use tokio::time::Duration;
    let mut buf = vec![0u8; MTU];
    loop {
        // Pick a poll deadline: the shorter of "smoltcp asked us to
        // wake up by then" and "we want to bound idle latency".
        let poll_delay = {
            let mut g = netif.lock().await;
            // Borrow split: hold sockets shorter than iface.
            let ts = Instant::now();
            let NetIf { iface, sockets, .. } = &mut *g;
            iface.poll_delay(ts, sockets).map(|d| Duration::from_micros(d.total_micros()))
        }
        .unwrap_or(Duration::from_millis(10));

        let recv_fut = async {
            let t = tun.lock().await;
            t.recv(&mut buf).await.context("TAP recv failed")
        };

        tokio::select! {
            _ = cancel.notified() => {
                debug!("netstack interface task: cancel notified, exiting");
                return Ok(());
            }
            res = recv_fut => {
                let n = res?;
                let mut g = netif.lock().await;
                g.device.rx_queue.push_back(buf[..n].to_vec());
            }
            _ = tokio::time::sleep(poll_delay) => {
                // fall through to poll
            }
        }

        // Poll smoltcp + drain anything it emitted.
        let to_send: Vec<Vec<u8>> = {
            let mut g = netif.lock().await;
            let ts = Instant::now();
            let NetIf { iface, device, sockets } = &mut *g;
            iface.poll(ts, device, sockets);
            std::mem::take(&mut device.tx_queue).into_iter().collect()
        };
        for pkt in to_send {
            let t = tun.lock().await;
            if let Err(e) = t.send(&pkt).await {
                warn!(error = %e, "TAP send failed; dropping packet");
            }
        }
    }
}
