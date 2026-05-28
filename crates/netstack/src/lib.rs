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
//! - [`iface`] — bridges a tokio-async TAP device (`tokio_tun::Tun`) to
//!   smoltcp's `Device` trait, drives `smoltcp::iface::Interface::poll()`
//!   from a tokio task.
//! - [`dispatch`] — VIP → handler routing table. A handler is a typed
//!   enum (`Dns(Arc<Zone>)`, `Service { backends }`) so each in-process
//!   shortcut path is a function call, not a socket round-trip.
//! - `examples/dns_only.rs` — the spike: brings up one TAP in a fresh
//!   netns, binds `10.96.0.10:53` inside smoltcp, dispatches DNS
//!   queries to a [`rusternetes_dns::zone::Zone`] in-memory, and
//!   responds — all without a single kernel socket touching :53.
//!
//! ### Out of scope for the spike
//!
//! - TCP. The DNS path is UDP-only; Service-VIP TCP dispatch is Phase 4.
//! - Per-pod TAPs and pod-process attachment (`unshare -U -n`,
//!   `SCM_RIGHTS` fd-passing). The spike uses ONE TAP in a synthetic
//!   netns the example sets up; Phase 3 generalises this per pod.
//! - kubelet integration. The kubelet still launches pods via
//!   Bollard/Docker until Phase 3 wires the rootless runtime.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod dispatch;
pub mod iface;
