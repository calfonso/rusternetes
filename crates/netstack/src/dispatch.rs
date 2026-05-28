//! VIP → handler dispatch table.
//!
//! Pod packets arriving at the netstack interface come in two flavours
//! that we want to short-circuit:
//!
//! - **DNS queries to the kube-dns Service ClusterIP** (default
//!   `10.96.0.10:53/udp`) — we hand the DNS bytes to a
//!   [`rusternetes_dns::zone::Zone`] in the same process and write the
//!   answer back to the smoltcp UDP socket. No kernel sockets, no
//!   CoreDNS pod, no round-trip.
//! - **Service ClusterIP TCP** (the rest of `10.96.0.0/12`) — pick a
//!   backend pod, open a tokio TCP connection to its real IP, and shuffle
//!   bytes. Phase 4 work, sketched here so the dispatch table grows in
//!   the right shape.
//!
//! ### Shape
//!
//! ```
//! use rusternetes_netstack::dispatch::{Dispatcher, Handler};
//! use std::net::SocketAddr;
//!
//! let mut d = Dispatcher::new();
//! // d.bind("10.96.0.10:53".parse().unwrap(), Handler::Dns(zone));
//! // d.bind("10.96.0.1:443".parse().unwrap(), Handler::Service { backends: vec![...] });
//! ```
//!
//! ### Status
//!
//! Spike-level. Only the `Dns` arm has a real implementation in this
//! commit. `Service` is a stub that returns a "not implemented" error so
//! the dispatch wiring can be exercised end to end before Phase 4.

use anyhow::Result;
use rusternetes_dns::server::SharedZone;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

/// Per-VIP handler — what the netstack does with a packet destined to
/// a specific (ip, port) pair.
pub enum Handler {
    /// Short-circuit a DNS query: parse with `hickory-proto`, look up
    /// in the shared zone, return the wire-format response.
    Dns(Arc<SharedZone>),

    /// Forward TCP traffic to one of the registered backend endpoints
    /// (selected by `picker`). The picker is `Arc<dyn BackendPicker>`
    /// so the policy is swappable per Service in the future (today the
    /// only impl is [`RoundRobinPicker`]).
    ///
    /// The byte pump that actually moves traffic between the smoltcp
    /// TCP socket and the chosen backend lands in slice 4c (#45);
    /// this commit lands only the picker plumbing on the dispatch
    /// table.
    Service { picker: Arc<dyn BackendPicker> },
}

/// Backend-selection policy for [`Handler::Service`].
///
/// Implementors return one of the currently-healthy backend
/// `SocketAddr`s on each call, or `None` if no backends are
/// registered (in which case the dispatcher should drop the SYN and
/// rely on the client to retry — same behaviour as a stock K8s
/// Service with zero ready endpoints).
///
/// Object-safe (no associated types, no generic methods) so the
/// Dispatcher can hold `Arc<dyn BackendPicker>` without going generic.
pub trait BackendPicker: Send + Sync + 'static {
    /// Pick a backend for one incoming connection. Implementations
    /// MUST be cheap — called on every TCP accept on every Service
    /// VIP.
    fn next(&self) -> Option<SocketAddr>;

    /// Currently-registered backend count. Mostly for observability
    /// (logs / metrics) — picker behaviour is encapsulated by
    /// [`Self::next`].
    fn len(&self) -> usize;

    /// Whether the picker has no backends. Default impl delegates to
    /// `len`; impls don't usually need to override.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Round-robin picker — the dispatcher's default policy. Rotates
/// through the registered backends with an `AtomicUsize` cursor so
/// concurrent dispatches don't repeatedly pick the same backend.
///
/// Backends are immutable once the picker is constructed — the
/// EndpointSlice-watcher (slice 4b's follow-up) rebuilds and
/// re-installs a fresh picker on every endpoint change rather than
/// mutating in place, so in-flight `next` calls always see a
/// consistent backend set.
pub struct RoundRobinPicker {
    backends: Vec<SocketAddr>,
    cursor: AtomicUsize,
}

impl RoundRobinPicker {
    /// Construct a picker over `backends`. An empty vec is legal
    /// (every `next` returns `None` — "no ready endpoints" semantics).
    pub fn new(backends: Vec<SocketAddr>) -> Self {
        Self {
            backends,
            cursor: AtomicUsize::new(0),
        }
    }
}

impl BackendPicker for RoundRobinPicker {
    fn next(&self) -> Option<SocketAddr> {
        if self.backends.is_empty() {
            return None;
        }
        // `fetch_add` is Relaxed because the only invariant we care
        // about is "every increment is observed by exactly one
        // caller" — not the order across cursors. The modulo by len
        // wraps cleanly.
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.backends.len();
        Some(self.backends[idx])
    }

    fn len(&self) -> usize {
        self.backends.len()
    }
}

/// The VIP → handler table. One per netstack instance; populated at
/// startup from the storage backend's Services + EndpointSlices (Phase
/// 4) or wired up by hand for the spike example.
pub struct Dispatcher {
    handlers: HashMap<SocketAddr, Handler>,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a handler for `(ip, port)`. Returns the previous
    /// handler at that VIP if any (the caller almost always wants
    /// `None` — a non-None return means two handlers competed for the
    /// same VIP, which is a bug in the caller's reconcile loop).
    pub fn bind(&mut self, vip: SocketAddr, handler: Handler) -> Option<Handler> {
        debug!(?vip, "dispatcher bind");
        self.handlers.insert(vip, handler)
    }

    /// Dispatch a UDP payload arriving at `dst`. Returns the response
    /// bytes to send back, or `None` if no handler matched.
    pub async fn dispatch_udp(&self, dst: SocketAddr, payload: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(handler) = self.handlers.get(&dst) else {
            return Ok(None);
        };
        match handler {
            Handler::Dns(zone) => {
                let resp = handle_dns_query(zone, payload).await?;
                Ok(Some(resp))
            }
            Handler::Service { .. } => {
                warn!(
                    ?dst,
                    "Service handler hit on UDP path (TCP-only in Phase 4 spike)"
                );
                Ok(None)
            }
        }
    }
}

/// Parse a DNS query, look it up in the in-memory zone, and serialise
/// the answer.
///
/// This bypasses hickory's socket layer entirely — the byte path is
/// `smoltcp UDP socket → here → smoltcp UDP socket` with one function
/// call's worth of overhead. The zone is the same `SharedZone` that
/// `rusternetes_dns::server::serve` would feed to a UDP listener;
/// [`rusternetes_dns::server::respond_bytes`] is the shared
/// bytes-in/bytes-out responder so both paths emit identical wire
/// responses for identical queries.
async fn handle_dns_query(zone: &Arc<SharedZone>, query: &[u8]) -> Result<Vec<u8>> {
    let snapshot = zone.load().await;
    rusternetes_dns::server::respond_bytes(&snapshot, query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dispatch_returns_none_for_unbound_vip() {
        let d = Dispatcher::new();
        let resp = d
            .dispatch_udp("10.96.0.10:53".parse().unwrap(), b"whatever")
            .await
            .unwrap();
        assert!(resp.is_none(), "no handler bound → no response");
    }

    #[tokio::test]
    async fn dns_dispatch_returns_nxdomain_with_query_id_preserved() {
        let mut d = Dispatcher::new();
        let zone = Arc::new(SharedZone::new(rusternetes_dns::zone::Zone::empty(
            rusternetes_dns::zone::CLUSTER_ZONE,
        )));
        d.bind("10.96.0.10:53".parse().unwrap(), Handler::Dns(zone));

        // Minimal DNS query: header (12 bytes) + empty question section.
        // Header layout: id(2) flags(2) qdcount(2) ancount(2) nscount(2) arcount(2)
        let query = vec![
            0xab, 0xcd, // id
            0x01, 0x00, // flags: RD=1
            0x00, 0x01, // qdcount=1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // an/ns/ar = 0
            // question: foo.svc.cluster.local. A IN
            3, b'f', b'o', b'o', 3, b's', b'v', b'c', 7, b'c', b'l', b'u', b's', b't', b'e', b'r',
            5, b'l', b'o', b'c', b'a', b'l', 0, // root
            0x00, 0x01, // type A
            0x00, 0x01, // class IN
        ];
        let resp = d
            .dispatch_udp("10.96.0.10:53".parse().unwrap(), &query)
            .await
            .unwrap()
            .expect("DNS handler should respond");
        assert_eq!(&resp[..2], &[0xab, 0xcd], "transaction ID preserved");
        assert_eq!(resp[2] & 0x80, 0x80, "QR bit set");
        assert_eq!(resp[3] & 0x0f, 0x03, "RCODE = NXDOMAIN (3)");
    }

    fn sock(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn round_robin_picker_with_no_backends_returns_none() {
        let p = RoundRobinPicker::new(vec![]);
        assert_eq!(p.next(), None);
        assert_eq!(p.next(), None, "still None on retry");
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn round_robin_picker_with_one_backend_always_returns_it() {
        let only = sock("10.244.0.5:443");
        let p = RoundRobinPicker::new(vec![only]);
        for _ in 0..10 {
            assert_eq!(p.next(), Some(only));
        }
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn round_robin_picker_rotates_through_backends_then_wraps() {
        let backends = vec![
            sock("10.244.0.5:443"),
            sock("10.244.0.6:443"),
            sock("10.244.0.7:443"),
        ];
        let p = RoundRobinPicker::new(backends.clone());

        // First lap: each backend in order.
        for expected in &backends {
            assert_eq!(p.next(), Some(*expected));
        }
        // Second lap: wraps and repeats.
        for expected in &backends {
            assert_eq!(p.next(), Some(*expected));
        }
    }

    #[test]
    fn round_robin_picker_is_safe_under_concurrent_dispatch() {
        // The picker lives behind `Arc<dyn BackendPicker>` and gets
        // called from every TCP accept across every worker thread.
        // Confirm the cursor never hands out the same index twice in
        // one rotation under contention.
        use std::collections::HashMap;
        use std::sync::Mutex as StdMutex;

        let backends: Vec<SocketAddr> = (0..16)
            .map(|i| sock(&format!("10.244.0.{}:443", i + 10)))
            .collect();
        let n = backends.len();
        let p: Arc<dyn BackendPicker> = Arc::new(RoundRobinPicker::new(backends));

        let counts: Arc<StdMutex<HashMap<SocketAddr, usize>>> =
            Arc::new(StdMutex::new(HashMap::new()));
        let rotations = 100;
        let mut handles = vec![];
        for _ in 0..4 {
            let p = p.clone();
            let counts = counts.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..(n * rotations / 4) {
                    let picked = p.next().expect("picker returns Some");
                    *counts.lock().unwrap().entry(picked).or_insert(0) += 1;
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let counts = counts.lock().unwrap();
        // Total dispatches must equal what we asked for — no losses.
        let total: usize = counts.values().sum();
        assert_eq!(total, n * rotations);
        // Distribution must be perfectly balanced — every backend
        // picked exactly `rotations` times (round-robin under
        // contention preserves the invariant).
        for (addr, count) in counts.iter() {
            assert_eq!(
                *count, rotations,
                "backend {addr} got {count} picks, expected {rotations}"
            );
        }
    }
}
