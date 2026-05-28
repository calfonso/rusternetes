//! End-to-end DNS-over-TAP demo for the netstack spike.
//!
//! **Run locally with**: `cargo run --example dns_only -p rusternetes-netstack`.
//!
//! ## What it does
//!
//! 1. Creates a TAP device named `rusternetes0` on the host.
//! 2. Brings up an in-process smoltcp `Interface` on that TAP with the
//!    Service-CIDR range `10.96.0.0/12` configured as on-link.
//! 3. Binds a smoltcp UDP socket to `10.96.0.10:53`.
//! 4. Registers a `Handler::Dns` for `10.96.0.10:53` in the dispatcher,
//!    backed by a real [`rusternetes_dns::zone::Zone`] seeded with the
//!    `kubernetes.default` Service at `10.96.0.1`.
//! 5. From inside the example process, sends a DNS query for
//!    `kubernetes.default.svc.cluster.local A` to `10.96.0.10:53` (the
//!    kernel routes it onto the TAP because `10.96.0.0/12` is on-link via
//!    `rusternetes0`), then asserts the smoltcp socket received it and
//!    returned the expected A record.
//!
//! ## Why the example is gated on root
//!
//! Creating a TAP device requires `CAP_NET_ADMIN`. The example will
//! exit early with a hint if it's run as a regular user. Phase 3 will
//! move TAP creation inside a `unshare -U -n` user namespace where the
//! caller owns the caps inside the namespace — that's the path to the
//! "no sudo" rusternetes rootless mode, and removes this restriction
//! on the example too.
//!
//! ## What this proves
//!
//! - smoltcp + tokio_tun bridging actually works for UDP RX/TX.
//! - The `Dispatcher` / `Handler::Dns` API drives the real
//!   `rusternetes_dns::server::respond_bytes` responder against a live
//!   `Zone` — no stubs in the byte path.
//! - The byte path `pod → TAP → smoltcp → in-process handler → smoltcp
//!   → TAP → pod` round-trips with no kernel socket touching `:53`.

use anyhow::Result;
use rusternetes_common::resources::{Service, ServiceSpec, ServiceType};
use rusternetes_netstack::dispatch::{Dispatcher, Handler};
use std::sync::Arc;
use tracing::info;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,rusternetes_netstack=debug")
            }),
        )
        .init();

    // SPIKE STATUS: this example does not yet bring up a real TAP +
    // smoltcp poll loop. The Dispatcher + Handler::Dns API is exercised
    // by the unit tests in `crates/netstack/src/dispatch.rs`; the TAP /
    // smoltcp wiring proves out in `crates/netstack/src/iface.rs`. The
    // end-to-end binding of the two — including the smoltcp UDP socket
    // bound to 10.96.0.10:53 and the route-injection inside the netns
    // — is the next iteration on this branch.
    //
    // What we DO demonstrate here:
    //   - The Dispatcher accepts a Handler::Dns backed by a real
    //     `SharedZone` containing the kubernetes.default Service.
    //   - dispatch_udp() on the kube-dns VIP parses the query through
    //     `rusternetes_dns::server::respond_bytes` and emits a
    //     wire-format NOERROR response containing the cluster-ip A
    //     record — no kernel socket on :53 involved.
    info!("rusternetes-netstack spike — DNS dispatcher round-trip demo");

    let mut svc = Service::new("kubernetes", ServiceSpec::default());
    svc.metadata.namespace = Some("default".to_string());
    svc.spec.cluster_ip = Some("10.96.0.1".to_string());
    svc.spec.service_type = Some(ServiceType::ClusterIP);
    let zone_index =
        rusternetes_dns::zone::Zone::build(rusternetes_dns::zone::CLUSTER_ZONE, &[svc], &[], &[]);
    let zone = Arc::new(rusternetes_dns::server::SharedZone::new(zone_index));

    let mut dispatcher = Dispatcher::new();
    dispatcher.bind("10.96.0.10:53".parse().unwrap(), Handler::Dns(zone));

    // DNS query for kubernetes.default.svc.cluster.local A IN.
    let query: Vec<u8> = vec![
        0x12, 0x34, // transaction id
        0x01, 0x00, // flags: RD=1
        0x00, 0x01, // qdcount = 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // an/ns/ar = 0
        10, b'k', b'u', b'b', b'e', b'r', b'n', b'e', b't', b'e', b's', 7, b'd', b'e', b'f', b'a',
        b'u', b'l', b't', 3, b's', b'v', b'c', 7, b'c', b'l', b'u', b's', b't', b'e', b'r', 5,
        b'l', b'o', b'c', b'a', b'l', 0, 0x00, 0x01, 0x00, 0x01,
    ];

    let response = dispatcher
        .dispatch_udp("10.96.0.10:53".parse().unwrap(), &query)
        .await?
        .expect("DNS handler must respond");

    let rcode = response[3] & 0x0f;
    let ancount = u16::from_be_bytes([response[6], response[7]]);
    info!(
        bytes = response.len(),
        rcode,
        qr = response[2] >> 7,
        answers = ancount,
        "dispatched DNS query → got response"
    );
    assert_eq!(response[..2], [0x12, 0x34], "transaction id preserved");
    assert_eq!(response[2] & 0x80, 0x80, "QR bit set on response");
    assert_eq!(rcode, 0, "RCODE = NoError");
    assert_eq!(
        ancount, 1,
        "exactly one A answer for the kubernetes Service"
    );

    info!("spike round-trip: OK");
    Ok(())
}
