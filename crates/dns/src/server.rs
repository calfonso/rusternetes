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
use hickory_proto::op::{Header, HeaderCounts, Message, Metadata, ResponseCode};
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

        let builder = MessageResponseBuilder::from_message_request(request);
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
    let builder = MessageResponseBuilder::from_message_request(request);
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

/// Bytes-in / bytes-out DNS responder. The wire-protocol counterpart of
/// [`DnsHandler::handle_request`] — same lookup logic and response shape,
/// but without the `hickory_server::RequestHandler` /
/// `ResponseHandler` plumbing. Used by the in-process netstack
/// (`rusternetes-netstack`) to short-circuit DNS at the smoltcp UDP
/// layer so the kernel never sees a socket on `10.96.0.10:53`.
///
/// Returns well-formed DNS response bytes for every recognised outcome
/// (`NoError`-with-records, `NoError`-empty / `NoData`, `NXDOMAIN`,
/// `FormErr`-on-empty-question). The `Err` arm is reserved for inputs
/// that aren't valid wire-format DNS at all — callers above the netstack
/// should drop those silently.
pub fn respond_bytes(zone: &Zone, query: &[u8]) -> anyhow::Result<Vec<u8>> {
    let req = Message::from_vec(query).map_err(|e| anyhow::anyhow!("malformed DNS query: {e}"))?;

    let mut resp = Message::response(req.metadata.id, req.metadata.op_code);
    resp.metadata = Metadata::response_from_request(&req.metadata);
    resp.metadata.authoritative = true;

    let Some(q) = req.queries.first() else {
        resp.metadata.response_code = ResponseCode::FormErr;
        return Ok(resp.to_vec()?);
    };
    resp.add_query(q.clone());

    let qname = q.name().to_string();
    let outcome = lookup_for_type(zone, &qname, q.query_type());
    debug!(
        "DNS in-proc query name={qname} type={qtype:?} -> {outcome}",
        qtype = q.query_type(),
        outcome = match &outcome {
            LookupOutcome::Records(r) => format!("Records({})", r.len()),
            LookupOutcome::NoData => "NoData".to_string(),
            LookupOutcome::NxDomain => "NxDomain".to_string(),
        }
    );

    match &outcome {
        LookupOutcome::Records(records) => {
            resp.metadata.response_code = ResponseCode::NoError;
            for r in records {
                if let Some(rec) = to_wire_record(&qname, r) {
                    resp.add_answer(rec);
                }
            }
        }
        LookupOutcome::NoData => {
            resp.metadata.response_code = ResponseCode::NoError;
            resp.add_authority(build_soa(zone.suffix()));
        }
        LookupOutcome::NxDomain => {
            resp.metadata.response_code = ResponseCode::NXDomain;
            resp.add_authority(build_soa(zone.suffix()));
        }
    }

    Ok(resp.to_vec()?)
}

// Silence the unused-import warning when ip_to_arpa isn't referenced
// here — re-exported for the watcher / future PTR-zone work.
#[allow(dead_code)]
fn _ip_to_arpa_used(ip: IpAddr) -> String {
    ip_to_arpa(ip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zone::CLUSTER_ZONE;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{DNSClass, Name, RecordType};
    use rusternetes_common::resources::{Service, ServiceSpec, ServiceType};
    use std::str::FromStr;

    fn cluster_ip_svc(name: &str, ns: &str, ip: &str) -> Service {
        let mut s = Service::new(name, ServiceSpec::default());
        s.metadata.namespace = Some(ns.to_string());
        s.spec.cluster_ip = Some(ip.to_string());
        s.spec.service_type = Some(ServiceType::ClusterIP);
        s
    }

    fn build_query(id: u16, name: &str, qtype: RecordType) -> Vec<u8> {
        let mut msg = Message::new(id, MessageType::Query, OpCode::Query);
        msg.metadata.recursion_desired = true;
        let mut q = Query::query(Name::from_str(name).unwrap(), qtype);
        q.set_query_class(DNSClass::IN);
        msg.add_query(q);
        msg.to_vec().unwrap()
    }

    #[test]
    fn respond_bytes_returns_a_record_for_known_service() {
        let zone = Zone::build(
            CLUSTER_ZONE,
            &[cluster_ip_svc("kubernetes", "default", "10.96.0.1")],
            &[],
            &[],
        );

        let query = build_query(
            0x4242,
            "kubernetes.default.svc.cluster.local.",
            RecordType::A,
        );
        let resp_bytes = respond_bytes(&zone, &query).expect("well-formed query must succeed");

        let resp = Message::from_vec(&resp_bytes).expect("response must parse");
        assert_eq!(resp.metadata.id, 0x4242, "transaction id preserved");
        assert_eq!(resp.metadata.message_type, MessageType::Response);
        assert!(resp.metadata.authoritative, "AA flag set");
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        assert_eq!(resp.answers.len(), 1, "exactly one A answer");
        let answer = &resp.answers[0];
        assert_eq!(answer.record_type(), RecordType::A);
        match answer.data {
            RData::A(a) => assert_eq!(a.0, std::net::Ipv4Addr::new(10, 96, 0, 1)),
            ref other => panic!("expected RData::A, got {other:?}"),
        }
    }

    #[test]
    fn respond_bytes_returns_nxdomain_for_unknown_name() {
        let zone = Zone::empty(CLUSTER_ZONE);
        let query = build_query(
            0x1337,
            "no-such-service.default.svc.cluster.local.",
            RecordType::A,
        );

        let resp_bytes = respond_bytes(&zone, &query).unwrap();
        let resp = Message::from_vec(&resp_bytes).unwrap();
        assert_eq!(resp.metadata.id, 0x1337);
        assert_eq!(resp.metadata.response_code, ResponseCode::NXDomain);
        assert!(resp.answers.is_empty());
        assert_eq!(resp.authorities.len(), 1, "SOA in authority section");
        assert_eq!(resp.authorities[0].record_type(), RecordType::SOA);
    }

    #[test]
    fn respond_bytes_errors_on_malformed_input() {
        let zone = Zone::empty(CLUSTER_ZONE);
        // Not valid wire-format DNS at all (random bytes shorter than a header).
        let resp = respond_bytes(&zone, &[0xff; 3]);
        assert!(resp.is_err(), "garbage input must surface as Err");
    }
}
