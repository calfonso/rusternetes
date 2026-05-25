//! Hickory-server integration: wires the in-memory [`Zone`](crate::zone::Zone)
//! to a `hickory_server::RequestHandler` that answers UDP and TCP DNS
//! queries.
//!
//! The split between this module and [`crate::zone`] is deliberate: all
//! K8s-specific record logic stays in `zone.rs` (unit-tested without any
//! socket I/O), while this module is a thin adapter that:
//!
//! 1. Translates each incoming DNS query into a (name, type) pair.
//! 2. Calls [`Zone::lookup`](crate::zone::Zone::lookup) with the
//!    matching record-type filter.
//! 3. Builds the appropriate hickory `Record` from the returned
//!    [`DnsRecord`](crate::zone::DnsRecord) variants.
//! 4. Returns NXDOMAIN / NOERROR-empty / answer-with-records per the
//!    lookup outcome.

use crate::zone::{ip_to_arpa, DnsRecord, LookupOutcome, Zone, DEFAULT_TTL};
use hickory_proto::op::{Header, HeaderCounts, Metadata, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA, CNAME, PTR, SOA, SRV};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use hickory_server::net::runtime::Time;
use hickory_server::server::{Request, RequestHandler, ResponseHandler, ResponseInfo};
use hickory_server::zone_handler::MessageResponseBuilder;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// Reference-counted, atomically-swappable handle to the current zone.
///
/// The watcher rebuilds a fresh `Zone` on every change event and calls
/// [`SharedZone::store`] to install it; in-flight `lookup()` calls always
/// see a complete snapshot because we hand out `Arc<Zone>` clones rather
/// than holding the write lock during the query.
#[derive(Clone)]
pub struct SharedZone {
    inner: Arc<RwLock<Arc<Zone>>>,
}

impl SharedZone {
    pub fn new(initial: Zone) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(initial))),
        }
    }

    /// Atomically replace the zone with a new snapshot. Cheap — the old
    /// `Arc<Zone>` lives until the last reader drops its handle, after
    /// which the storage is reclaimed.
    pub async fn store(&self, zone: Zone) {
        let mut guard = self.inner.write().await;
        *guard = Arc::new(zone);
    }

    /// Load the current zone snapshot. Returns an owned `Arc<Zone>` so
    /// the caller releases the read lock immediately.
    pub async fn load(&self) -> Arc<Zone> {
        Arc::clone(&*self.inner.read().await)
    }
}

/// `RequestHandler` impl that answers DNS queries from the shared zone.
#[derive(Clone)]
pub struct DnsHandler {
    zone: SharedZone,
}

impl DnsHandler {
    pub fn new(zone: SharedZone) -> Self {
        Self { zone }
    }
}

#[async_trait::async_trait]
impl RequestHandler for DnsHandler {
    async fn handle_request<R: ResponseHandler, T: Time>(
        &self,
        request: &Request,
        mut response_handle: R,
    ) -> ResponseInfo {
        let zone = self.zone.load().await;
        let queries = request.queries.queries();
        let Some(query) = queries.first() else {
            // No query block — return FormErr.
            return reply_error(request, &mut response_handle, ResponseCode::FormErr).await;
        };

        let qname = query.name().to_string();
        let qtype = query.query_type();

        let outcome = lookup_for_type(&zone, &qname, qtype);
        debug!(
            "DNS query name={qname} type={qtype:?} -> {outcome}",
            outcome = match &outcome {
                LookupOutcome::Records(r) => format!("Records({})", r.len()),
                LookupOutcome::NoData => "NoData".to_string(),
                LookupOutcome::NxDomain => "NxDomain".to_string(),
            }
        );

        // Translate to wire records.
        let answers: Vec<Record> = match &outcome {
            LookupOutcome::Records(records) => records
                .iter()
                .filter_map(|r| to_wire_record(&qname, r))
                .collect(),
            _ => Vec::new(),
        };

        let response_code = match &outcome {
            LookupOutcome::Records(_) | LookupOutcome::NoData => ResponseCode::NoError,
            LookupOutcome::NxDomain => ResponseCode::NXDomain,
        };

        let mut metadata = Metadata::response_from_request(&request.metadata);
        metadata.response_code = response_code;
        metadata.authoritative = true;

        // SOA in authority section for empty/NXDOMAIN responses, per RFC 2308.
        let soa_record: Option<Record> = if answers.is_empty() {
            Some(build_soa(zone.suffix()))
        } else {
            None
        };
        let authorities: Vec<Record> = soa_record.into_iter().collect();

        let builder = MessageResponseBuilder::from_message_request(&*request);
        let response = builder.build(
            metadata,
            answers.iter(),
            authorities.iter(),
            Vec::<&Record>::new(),
            Vec::<&Record>::new(),
        );

        match response_handle.send_response(response).await {
            Ok(info) => info,
            Err(e) => {
                error!("failed to send DNS response: {e}");
                // Best-effort: synthesize a ResponseInfo from the metadata.
                ResponseInfo::from(Header {
                    metadata,
                    counts: HeaderCounts::default(),
                })
            }
        }
    }
}

async fn reply_error<R: ResponseHandler>(
    request: &Request,
    response_handle: &mut R,
    code: ResponseCode,
) -> ResponseInfo {
    let builder = MessageResponseBuilder::from_message_request(&*request);
    let response = builder.error_msg(&request.metadata, code);
    let metadata = {
        let mut m = Metadata::response_from_request(&request.metadata);
        m.response_code = code;
        m
    };
    match response_handle.send_response(response).await {
        Ok(info) => info,
        Err(_) => ResponseInfo::from(Header {
            metadata,
            counts: HeaderCounts::default(),
        }),
    }
}

/// Translate a hickory `RecordType` into a [`DnsRecord`] filter predicate.
///
/// For `ANY` and `PTR` queries we accept everything / only PTR records
/// respectively. SOA queries get a hand-built record below; we never
/// look up the SOA in the zone index.
fn lookup_for_type(zone: &Zone, name: &str, qtype: RecordType) -> LookupOutcome {
    match qtype {
        RecordType::A => zone.lookup(name, |r| matches!(r, DnsRecord::A(_))),
        RecordType::AAAA => zone.lookup(name, |r| matches!(r, DnsRecord::Aaaa(_))),
        RecordType::SRV => zone.lookup(name, |r| matches!(r, DnsRecord::Srv { .. })),
        RecordType::CNAME => zone.lookup(name, |r| matches!(r, DnsRecord::Cname(_))),
        RecordType::PTR => zone.lookup(name, |r| matches!(r, DnsRecord::Ptr(_))),
        RecordType::ANY => zone.lookup(name, |_| true),
        // For SOA / NS queries against the apex, we don't store the record
        // in the zone index — let it fall through to NoData so the SOA in
        // authority section is sent (per RFC 2308).
        _ => zone.lookup(name, |_| false),
    }
}

fn to_wire_record(qname: &str, record: &DnsRecord) -> Option<Record> {
    let name = Name::from_str(qname)
        .or_else(|_| Name::from_ascii(qname))
        .ok()?;
    let r = match record {
        DnsRecord::A(ip) => Record::from_rdata(name, DEFAULT_TTL, RData::A(A(*ip))),
        DnsRecord::Aaaa(ip) => Record::from_rdata(name, DEFAULT_TTL, RData::AAAA(AAAA(*ip))),
        DnsRecord::Cname(target) => {
            let t = Name::from_ascii(target.trim_end_matches('.')).ok()?;
            Record::from_rdata(name, DEFAULT_TTL, RData::CNAME(CNAME(t)))
        }
        DnsRecord::Ptr(target) => {
            let t = Name::from_ascii(target.trim_end_matches('.')).ok()?;
            Record::from_rdata(name, DEFAULT_TTL, RData::PTR(PTR(t)))
        }
        DnsRecord::Srv {
            priority,
            weight,
            port,
            target,
        } => {
            let t = Name::from_ascii(target.trim_end_matches('.')).ok()?;
            Record::from_rdata(
                name,
                DEFAULT_TTL,
                RData::SRV(SRV::new(*priority, *weight, *port, t)),
            )
        }
    };
    Some(r)
}

/// Hand-built SOA for the zone apex. The serial number is stable per
/// process restart — we don't yet bump it on watch events because we are
/// authoritative for an internal zone with no secondaries.
fn build_soa(zone_suffix: &str) -> Record {
    let bare = zone_suffix.trim_end_matches('.');
    let apex = Name::from_ascii(bare).unwrap_or_else(|_| Name::root());
    let mname = Name::from_ascii(format!("ns.dns.{}", bare)).unwrap_or_else(|_| apex.clone());
    let rname = Name::from_ascii(format!("hostmaster.{}", bare)).unwrap_or_else(|_| apex.clone());
    Record::from_rdata(
        apex,
        DEFAULT_TTL,
        RData::SOA(SOA::new(
            mname,
            rname,
            // serial: epoch-style placeholder
            1,
            // refresh, retry, expire (seconds)
            7200,
            1800,
            86400,
            // minimum (negative-cache TTL)
            DEFAULT_TTL,
        )),
    )
}

/// Bind UDP+TCP listeners on the given addresses and run the hickory
/// `Server` until SIGTERM/SIGINT.
pub async fn serve(
    zone: SharedZone,
    udp_bind: SocketAddr,
    tcp_bind: SocketAddr,
) -> anyhow::Result<()> {
    let handler = DnsHandler::new(zone);
    let mut server = hickory_server::server::Server::new(handler);

    let udp = UdpSocket::bind(udp_bind).await?;
    server.register_socket(udp);

    let tcp = TcpListener::bind(tcp_bind).await?;
    // 65535 bytes per-connection response buffer — RFC 1035's max DNS
    // payload over TCP, matches hickory's `hickory-dns` defaults.
    server.register_listener(tcp, std::time::Duration::from_secs(5), 65535);

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            warn!("Received SIGINT — shutting down");
        }
        _ = sigterm.recv() => {
            warn!("Received SIGTERM — shutting down");
        }
    }

    // Hickory `Server` provides a cancellation token; drop the handle to
    // tear down listeners.
    drop(server);
    Ok(())
}

// Silence the unused-import warning when ip_to_arpa isn't referenced
// here — re-exported for the watcher / future PTR-zone work.
#[allow(dead_code)]
fn _ip_to_arpa_used(ip: IpAddr) -> String {
    ip_to_arpa(ip)
}
