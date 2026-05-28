//! Rusternetes user-space network stack — **Phase 2 spike**.
//!
//! Goal: own pod traffic in-process so that the apiserver, kubelet, and
//! the DNS / Service-VIP dispatch logic can short-circuit kernel sockets
//! entirely. This unblocks two long-standing pain points:
//!
//! 1. **Cross-node pod networking on the multi-container stack** — each
//!    kubelet container today has its own Docker bridge; pods on
//!    different "node" containers can't reach each other. A shared
//!    user-space netstack assigns pod IPs from a unified pool and
//!    routes between them in-process, regardless of which container the
//!    pod is hosted in.
//! 2. **Rootless single-binary mode** — pod traffic terminates inside
//!    the rusternetes process via TAP-per-pod + smoltcp, so the whole
//!    pod-network setup never asks the kernel for `CAP_NET_ADMIN` /
//!    iptables.
//!
//! ### Scope of this crate
//!
//! - [`alloc`] — `PodIpAllocator`: the unified IP pool kubelet draws
//!   from when starting a pod. Replaces the per-Docker-bridge IP
//!   discovery that currently makes cross-kubelet networking
//!   impossible.
//! - [`capabilities`] — startup preflight: verify
//!   `CAP_NET_ADMIN` is in the effective capability set so the process
//!   fails fast at boot with an actionable error instead of `EPERM` on
//!   the first TAP open.
//! - [`iface`] — Phase 2 scaffolding that bridges one tokio-async TAP
//!   device (`tokio_tun::Tun`) to smoltcp's `Device` trait. Single-TAP
//!   only; superseded by [`multi`] for the multi-pod runtime that
//!   follows. Also exposes [`iface::open_tap`], the cap-checked TAP
//!   constructor every netstack code path should go through.
//! - [`multi`] — Phase 3 primitive: a smoltcp `Device` that fans out
//!   TX by IPv4 destination across per-pod egress queues, with one
//!   shared RX queue every per-TAP reader pushes into. The shape kubelet
//!   wires against once pods land on the netstack.
//! - [`dispatch`] — VIP → handler routing table. A handler is a typed
//!   enum (`Dns(Arc<Zone>)`, `Service { backends }`) so each in-process
//!   shortcut path is a function call, not a socket round-trip.
//! - `examples/dns_only.rs` — the spike: exercises the `Dispatcher` +
//!   real `SharedZone` byte path (no TAP yet). The runtime that brings
//!   up TAPs and drives `multi::MultiDevice` lands as the next commit
//!   on this branch.
//!
//! ### Out of scope for the spike
//!
//! - TCP. The DNS path is UDP-only; Service-VIP TCP dispatch is Phase 4.
//! - The per-TAP I/O runtime that pumps tokio_tun read/write halves
//!   into [`multi::MultiDevice`] + drives `Interface::poll()`. Phase 3
//!   continuation, gated on the `unshare -U -n` example wiring.
//! - kubelet integration. The kubelet still launches pods via
//!   Bollard/Docker until the netstack runtime above is in place.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod alloc;
pub mod capabilities;
pub mod dispatch;
pub mod iface;
pub mod manager;
pub mod multi;
pub mod netns;
pub mod podnet;
pub mod proxy;
pub mod runtime;

/// Re-exports of the smoltcp wire-format types our public APIs hand
/// out (`IpCidr` lives on `PodNetConfig::host_ips` and
/// `NetstackConfig::host_ips`). Re-exposing them here lets binaries
/// like the all-in-one build a netstack config without taking a
/// direct dependency on smoltcp.
pub mod wire {
    pub use smoltcp::wire::{IpAddress, IpCidr};
}

#[cfg(test)]
pub(crate) mod test_helpers;
