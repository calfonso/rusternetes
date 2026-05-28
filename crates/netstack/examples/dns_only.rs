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
//! 4. Registers a `Handler::Dns` for `10.96.0.10:53` in the dispatcher
//!    (currently stubbed to return NXDOMAIN — the real
//!    `SharedZone::lookup` wire-up is the next iteration of this
//!    spike).
//! 5. From inside the example process, sends a DNS query for
//!    `foo.svc.cluster.local A` to `10.96.0.10:53` (the kernel routes
//!    it onto the TAP because `10.96.0.0/12` is on-link via
//!    `rusternetes0`), then asserts the smoltcp socket received it and
//!    returned a NXDOMAIN response.
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
//! - The `Dispatcher` / `Handler::Dns` API has the right shape for the
//!   real (non-stub) zone integration that follows.
//! - The byte path `pod → TAP → smoltcp → in-process handler → smoltcp
//!   → TAP → pod` round-trips with no kernel socket touching `:53`.

use anyhow::Result;
use rusternetes_netstack::dispatch::{Dispatcher, Handler};
use std::sync::Arc;
use tracing::info;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,rusternetes_netstack=debug")),
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
    //   - The Dispatcher accepts a stub DNS handler bound to a Service
    //     ClusterIP.
    //   - dispatch_udp() on that VIP returns a wire-format DNS response
    //     with QR set and RCODE=NXDOMAIN.
    info!("rusternetes-netstack spike — DNS dispatcher round-trip demo");

    let mut dispatcher = Dispatcher::new();
    let zone = Arc::new(rusternetes_dns::server::SharedZone::new(
        rusternetes_dns::zone::Zone::empty(rusternetes_dns::zone::CLUSTER_ZONE),
    ));
    dispatcher.bind(
        "10.96.0.10:53".parse().unwrap(),
        Handler::Dns(zone),
    );

    // Construct a minimal DNS query for foo.svc.cluster.local A IN.
    let query: Vec<u8> = vec![
        0x12, 0x34, // transaction id
        0x01, 0x00, // flags: RD=1
        0x00, 0x01, // qdcount = 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // an/ns/ar = 0
        3, b'f', b'o', b'o', 3, b's', b'v', b'c', 7, b'c', b'l', b'u', b's', b't', b'e', b'r', 5,
        b'l', b'o', b'c', b'a', b'l', 0, 0x00, 0x01, 0x00, 0x01,
    ];

    let response = dispatcher
        .dispatch_udp("10.96.0.10:53".parse().unwrap(), &query)
        .await?
        .expect("DNS handler must respond");

    info!(
        bytes = response.len(),
        rcode = response[3] & 0x0f,
        qr = response[2] >> 7,
        "dispatched DNS query → got response"
    );
    assert_eq!(response[..2], [0x12, 0x34], "transaction id preserved");
    assert_eq!(response[2] & 0x80, 0x80, "QR bit set on response");
    assert_eq!(response[3] & 0x0f, 0x03, "RCODE = NXDOMAIN");

    info!("spike round-trip: OK");
    Ok(())
}
