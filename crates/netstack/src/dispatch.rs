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
use std::sync::Arc;
use tracing::{debug, warn};

/// Per-VIP handler — what the netstack does with a packet destined to
/// a specific (ip, port) pair.
pub enum Handler {
    /// Short-circuit a DNS query: parse with `hickory-proto`, look up
    /// in the shared zone, return the wire-format response.
    Dns(Arc<SharedZone>),

    /// Forward TCP traffic to one of the listed backend endpoints
    /// (round-robin / random — Phase 4 will plumb the upstream selection
    /// policy). The variant is here so the dispatch wiring compiles
    /// against the final shape; the handler itself is a stub.
    Service { backends: Vec<SocketAddr> },
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
}
