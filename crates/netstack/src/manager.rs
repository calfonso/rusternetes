//! `Netstack` — the kubelet-facing facade that binds the three
//! netstack primitives ([`PodIpAllocator`], [`PodTapRuntime`], and a
//! pluggable [`TapFactory`]) into one struct.
//!
//! ### The hook for kubelet
//!
//! Kubelet's per-pod lifecycle becomes:
//!
//! ```text
//!   on pod scheduled:
//!     let ip = netstack.start_pod(pod_uid).await?;
//!     // ip is now allocated, a TAP is open, the runtime knows about it
//!     // configure pod's netns to use the TAP with `ip`
//!     // start container
//!     pod.status.podIP = ip;
//!
//!   on pod deleted:
//!     netstack.stop_pod(pod_uid).await;
//!     // ip released, TAP closed, runtime detached
//! ```
//!
//! The actual kubelet-side wiring (replacing the
//! "discover IP from Docker bridge" path in
//! `crates/kubelet/src/runtime.rs`) is the next commit on this
//! branch, gated behind a `--pod-network-mode=netstack` flag so the
//! switch is reversible during shakeout.
//!
//! ### Why a `TapFactory` trait
//!
//! In production, [`crate::iface::open_tap`] opens a real
//! `tokio_tun::Tun`. In tests we want a channel-backed [`FakeTap`]
//! that needs no `CAP_NET_ADMIN`. Both implement [`TapFactory`] and
//! [`Netstack`] is generic over the trait, so the same `start_pod` /
//! `stop_pod` code-path runs in tests and production.

use crate::alloc::{AllocError, PodIpAllocator};
use crate::iface::{open_tap, OpenTapError};
use crate::podnet::{PodNet, PodNetConfig};
use crate::runtime::{PodIo, PodTapRuntime};
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Failure modes for [`Netstack::start_pod`].
#[derive(Debug, Error)]
pub enum StartPodError {
    /// The pod IP allocator is exhausted (very large cluster vs.
    /// undersized pod CIDR). Operator should widen the pod CIDR.
    #[error(transparent)]
    AllocFailed(#[from] AllocError),

    /// TAP creation failed. Most commonly missing `CAP_NET_ADMIN` (a
    /// configuration error — see [`OpenTapError::MissingCapability`])
    /// or `EEXIST` if the TAP name is already taken by another
    /// process (`OpenTapError::Build`).
    #[error(transparent)]
    TapFailed(#[from] OpenTapError),

    /// The `pod_uid` is already registered with the netstack —
    /// callers must `stop_pod` the existing entry first. Returned
    /// instead of silently overwriting (which would leak the
    /// previous IP).
    #[error("pod_uid {pod_uid:?} is already registered with the netstack")]
    AlreadyRegistered { pod_uid: String },
}

/// Pluggable TAP constructor — abstraction over [`open_tap`] so
/// tests can substitute a channel-backed fake. Methods are sync
/// because `tokio_tun::TunBuilder::build()` is sync (a blocking
/// netlink ioctl), and the production impl doesn't need to await
/// anything either.
pub trait TapFactory: Send + Sync + 'static {
    /// The concrete `PodIo` impl this factory hands out. In production
    /// `tokio_tun::Tun`; in tests, a channel-backed fake.
    type Io: PodIo;

    /// Create a TAP named `tap_name` and return it ready to register
    /// with [`PodTapRuntime`]. The returned `Arc` should be the only
    /// strong reference outside the runtime — when the runtime's
    /// per-pod task drops its clone on unregister, the TAP is
    /// closed (and the kernel device removed, since
    /// [`open_tap`] does not set `persist`).
    fn create_tap(&self, tap_name: &str) -> Result<Arc<Self::Io>, OpenTapError>;
}

/// Production TAP factory — delegates to [`crate::iface::open_tap`],
/// which preflights `CAP_NET_ADMIN` and builds a real
/// `tokio_tun::Tun`.
#[derive(Debug, Default)]
pub struct ProductionTapFactory;

impl TapFactory for ProductionTapFactory {
    type Io = tokio_tun::Tun;

    fn create_tap(&self, tap_name: &str) -> Result<Arc<tokio_tun::Tun>, OpenTapError> {
        open_tap(tap_name).map(Arc::new)
    }
}

/// One entry per registered pod. We don't keep the `Arc<Io>` —
/// [`PodTapRuntime`] owns the only strong reference once
/// [`PodTapRuntime::register_pod`] returns, so dropping the runtime's
/// reference on `unregister_pod` closes the TAP.
struct PodEntry {
    ip: Ipv4Addr,
    tap_name: String,
}

/// Configuration for a fresh [`Netstack`].
pub struct NetstackConfig {
    /// Pod CIDR — every pod IP is allocated from here. Typical
    /// production value is `10.244.0.0/16`.
    pub pod_cidr_base: Ipv4Addr,
    pub pod_cidr_prefix: u8,
    /// Host IPs the netstack itself claims. See
    /// [`PodNetConfig::host_ips`] for the two-roles distinction
    /// (routing `/12` + per-VIP `/32`).
    pub host_ips: Vec<smoltcp::wire::IpCidr>,
}

/// The single struct kubelet talks to.
pub struct Netstack<F: TapFactory> {
    factory: F,
    allocator: Arc<PodIpAllocator>,
    runtime: PodTapRuntime,
    pods: Mutex<HashMap<String, PodEntry>>,
}

impl<F: TapFactory> Netstack<F> {
    /// Build the data plane (`PodNet` + `PodTapRuntime`) and the
    /// allocator, hand back the facade.
    pub fn new(cfg: NetstackConfig, factory: F) -> anyhow::Result<Self> {
        let allocator = Arc::new(PodIpAllocator::new(cfg.pod_cidr_base, cfg.pod_cidr_prefix)?);
        let podnet = PodNet::new(&PodNetConfig {
            host_ips: cfg.host_ips,
        })?;
        let runtime = PodTapRuntime::spawn(podnet);
        debug!(
            pod_cidr = ?cfg.pod_cidr_base,
            prefix = cfg.pod_cidr_prefix,
            capacity = allocator.capacity(),
            "Netstack: initialised"
        );
        Ok(Self {
            factory,
            allocator,
            runtime,
            pods: Mutex::new(HashMap::new()),
        })
    }

    /// Allocate an IP, create a TAP, register the pod with the
    /// runtime, and return the IP for kubelet to stamp onto
    /// `pod.status.podIP`. The runtime spawns the pod's TAP I/O
    /// task before this returns; from the moment of return,
    /// packets read from the pod's netns are routed by the data
    /// plane.
    ///
    /// Idempotency is the caller's responsibility — re-calling
    /// `start_pod` for the same `pod_uid` returns
    /// `AlreadyRegistered` instead of silently leaking the previous
    /// allocation.
    pub async fn start_pod(&self, pod_uid: &str) -> Result<Ipv4Addr, StartPodError> {
        {
            let pods = self.pods.lock().await;
            if pods.contains_key(pod_uid) {
                return Err(StartPodError::AlreadyRegistered {
                    pod_uid: pod_uid.to_string(),
                });
            }
        }

        let ip = self.allocator.allocate()?;
        let tap_name = tap_name_for(pod_uid);
        let tap = match self.factory.create_tap(&tap_name) {
            Ok(tap) => tap,
            Err(e) => {
                // Roll back the IP allocation so we don't leak.
                self.allocator.release(ip);
                return Err(e.into());
            }
        };
        // Registration with the runtime can't really fail except
        // on logic errors (double-register), which we already
        // guard above.
        let new = self.runtime.register_pod(ip, tap).await;
        debug_assert!(
            new,
            "Netstack::start_pod: PodTapRuntime reported pod already registered (impossible after the AlreadyRegistered check above)"
        );

        let mut pods = self.pods.lock().await;
        pods.insert(
            pod_uid.to_string(),
            PodEntry {
                ip,
                tap_name: tap_name.clone(),
            },
        );
        debug!(?pod_uid, ?ip, %tap_name, "Netstack: pod started");
        Ok(ip)
    }

    /// Reverse of [`start_pod`]: detach the pod from the runtime
    /// (closes the TAP as a side effect), release the IP. Returns
    /// `true` if the pod was registered, `false` if it was not
    /// (idempotent — kubelet can call on a pod it never started
    /// without harm, and it's a useful signal that something is
    /// wrong upstream).
    pub async fn stop_pod(&self, pod_uid: &str) -> bool {
        let entry = {
            let mut pods = self.pods.lock().await;
            pods.remove(pod_uid)
        };
        let Some(entry) = entry else {
            warn!(?pod_uid, "Netstack::stop_pod: unknown pod");
            return false;
        };
        // Stop the runtime task first so no late TX can race the
        // IP release.
        self.runtime.unregister_pod(entry.ip).await;
        let released = self.allocator.release(entry.ip);
        debug_assert!(
            released,
            "Netstack::stop_pod: allocator did not own the IP we registered — invariant violation"
        );
        debug!(?pod_uid, ip = ?entry.ip, tap_name = %entry.tap_name,
            "Netstack: pod stopped");
        true
    }

    /// Stop every spawned task and drop the netstack. Idempotent in
    /// the sense that calling it twice on a clone is fine; the
    /// `self`-by-value receiver makes accidental double-shutdown of
    /// the same instance impossible.
    pub async fn shutdown(self) {
        debug!("Netstack: shutdown initiated");
        self.runtime.shutdown().await;
        debug!("Netstack: shutdown complete");
    }

    /// Look up the allocated IP for a pod. Returns `None` if the
    /// pod was never started or was already stopped.
    pub async fn pod_ip(&self, pod_uid: &str) -> Option<Ipv4Addr> {
        self.pods.lock().await.get(pod_uid).map(|e| e.ip)
    }

    /// Count of currently-registered pods.
    pub async fn pod_count(&self) -> usize {
        self.pods.lock().await.len()
    }

    /// Direct access to the allocator (for introspection — e.g., the
    /// kubelet status loop may want to report capacity remaining).
    pub fn allocator(&self) -> &Arc<PodIpAllocator> {
        &self.allocator
    }
}

/// Derive a TAP name from a pod identifier. Linux TAP names are
/// bounded by `IFNAMSIZ - 1 == 15`. The identifier can be anything
/// the caller uses to key the pod — kubelet's `pod_name` strings
/// (e.g., `"default_coredns-7c4..."`) and short pod UIDs both work;
/// we hash to 44 bits of entropy, which gives ~10^6 unique pods per
/// node before collisions become non-trivial. The `rust` prefix
/// makes the TAPs greppable in `ip link show`.
fn tap_name_for(pod_id: &str) -> String {
    let mut h = DefaultHasher::new();
    pod_id.hash(&mut h);
    // 11 hex chars = 44 bits of name space (`rust` prefix + 11 chars = 15 = IFNAMSIZ-1).
    let hash = h.finish() & 0x0fff_ffff_ffff;
    format!("rust{hash:011x}")
}

/// Object-safe trait surface for the kubelet-facing methods of
/// [`Netstack`]. Kubelet holds an `Arc<dyn NetstackHandle>` so it
/// doesn't need to be generic over [`TapFactory`].
///
/// In production the implementor is `Netstack<ProductionTapFactory>`;
/// in tests it's `Netstack<FakeTapFactory>`. Both go through the same
/// trait at the call sites.
#[async_trait]
pub trait NetstackHandle: Send + Sync + 'static {
    /// See [`Netstack::start_pod`].
    async fn start_pod(&self, pod_id: &str) -> Result<Ipv4Addr, StartPodError>;
    /// See [`Netstack::stop_pod`].
    async fn stop_pod(&self, pod_id: &str) -> bool;
    /// See [`Netstack::pod_count`].
    async fn pod_count(&self) -> usize;
}

#[async_trait]
impl<F: TapFactory> NetstackHandle for Netstack<F> {
    async fn start_pod(&self, pod_id: &str) -> Result<Ipv4Addr, StartPodError> {
        Netstack::start_pod(self, pod_id).await
    }
    async fn stop_pod(&self, pod_id: &str) -> bool {
        Netstack::stop_pod(self, pod_id).await
    }
    async fn pod_count(&self) -> usize {
        Netstack::pod_count(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{FakeTap, FakeTapHandle};
    use smoltcp::wire::{IpAddress, IpCidr};
    use std::sync::Mutex as StdMutex;

    /// Test factory that hands out `FakeTap`s and remembers each
    /// `(tap_name, handle)` so tests can grab the handle by name
    /// after `start_pod`.
    struct FakeTapFactory {
        handles: Arc<StdMutex<HashMap<String, FakeTapHandle>>>,
    }

    impl FakeTapFactory {
        fn new() -> Self {
            Self {
                handles: Arc::new(StdMutex::new(HashMap::new())),
            }
        }
    }

    impl TapFactory for FakeTapFactory {
        type Io = FakeTap;
        fn create_tap(&self, tap_name: &str) -> Result<Arc<FakeTap>, OpenTapError> {
            let (tap, handle) = FakeTap::pair();
            self.handles
                .lock()
                .expect("FakeTapFactory mutex poisoned")
                .insert(tap_name.to_string(), handle);
            Ok(tap)
        }
    }

    fn default_config() -> NetstackConfig {
        NetstackConfig {
            pod_cidr_base: Ipv4Addr::new(10, 244, 0, 0),
            pod_cidr_prefix: 16,
            host_ips: vec![IpCidr::new(IpAddress::v4(10, 96, 0, 1), 12)],
        }
    }

    #[test]
    fn tap_name_for_is_within_ifnamsiz_limit() {
        let n = tap_name_for("0a1b2c3d-4e5f-6789-abcd-ef0123456789");
        assert_eq!(n.len(), 15, "TAP name {n:?} should fill IFNAMSIZ-1");
        assert!(n.starts_with("rust"), "TAP name {n:?} missing rust prefix");
        // Distinct pod IDs produce distinct names via hash entropy.
        let m = tap_name_for("1a1b2c3d-4e5f-6789-abcd-ef0123456789");
        assert_ne!(n, m);
        // Hash-based naming accepts kubelet's `pod_name` strings —
        // longer than 15 chars, containing '_', '-' — without
        // truncation collisions or length blowups.
        let kubelet_name = tap_name_for("default_coredns-7c4a8d9b6f-x2k9p");
        assert_eq!(kubelet_name.len(), 15);
        assert!(kubelet_name.starts_with("rust"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn netstack_handle_trait_object_supports_kubelet_call_sites() {
        // Receipts that `Arc<dyn NetstackHandle>` works — the exact
        // shape kubelet uses so it doesn't need to be generic over
        // `TapFactory` everywhere.
        let factory = FakeTapFactory::new();
        let ns: Arc<dyn NetstackHandle> =
            Arc::new(Netstack::new(default_config(), factory).unwrap());
        let _ip = ns.start_pod("default_pod-abc").await.unwrap();
        assert_eq!(ns.pod_count().await, 1);
        assert!(ns.stop_pod("default_pod-abc").await);
        assert_eq!(ns.pod_count().await, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_pod_allocates_ip_and_registers_with_runtime() {
        let factory = FakeTapFactory::new();
        let handles = factory.handles.clone();
        let ns = Netstack::new(default_config(), factory).unwrap();

        let ip = ns.start_pod("pod-abc").await.unwrap();
        // Allocation comes from the /16's first usable address.
        assert_eq!(ip, Ipv4Addr::new(10, 244, 0, 2));
        assert_eq!(ns.pod_count().await, 1);
        assert_eq!(ns.pod_ip("pod-abc").await, Some(ip));
        // A TAP handle is now registered.
        assert!(
            handles
                .lock()
                .unwrap()
                .contains_key(&tap_name_for("pod-abc")),
            "factory recorded the TAP creation"
        );

        ns.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_pod_rejects_duplicate_pod_uid_without_leaking_ip() {
        let factory = FakeTapFactory::new();
        let ns = Netstack::new(default_config(), factory).unwrap();

        let ip = ns.start_pod("pod-abc").await.unwrap();
        let before_count = ns.allocator().allocated_count();

        match ns.start_pod("pod-abc").await {
            Err(StartPodError::AlreadyRegistered { pod_uid }) => {
                assert_eq!(pod_uid, "pod-abc");
            }
            other => panic!("expected AlreadyRegistered, got {other:?}"),
        }
        assert_eq!(
            ns.allocator().allocated_count(),
            before_count,
            "no IP leaked by the rejected re-registration"
        );
        let _ = ip;
        ns.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stop_pod_releases_ip_and_unregisters() {
        let factory = FakeTapFactory::new();
        let ns = Netstack::new(default_config(), factory).unwrap();

        let ip = ns.start_pod("pod-abc").await.unwrap();
        assert!(ns.allocator().is_allocated(ip));
        assert_eq!(ns.pod_count().await, 1);

        assert!(ns.stop_pod("pod-abc").await);
        assert!(!ns.allocator().is_allocated(ip));
        assert_eq!(ns.pod_count().await, 0);

        // Idempotent re-stop returns false (signal of upstream bug,
        // not a hard error).
        assert!(!ns.stop_pod("pod-abc").await);
        // A new pod gets a fresh IP from the pool. The released IP
        // does NOT come back immediately — the allocator's next-hint
        // pointer advances past it on each allocation so a
        // recently-freed IP is given a churn window before being
        // re-handed-out (avoids the case where a stale in-flight
        // packet for the previous pod hits the new pod's TAP).
        let ip2 = ns.start_pod("pod-xyz").await.unwrap();
        assert_ne!(
            ip2, ip,
            "freshly-allocated IP is not the recently-released one"
        );
        assert_eq!(ns.allocator().allocated_count(), 1);
        ns.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn many_pods_get_unique_ips() {
        let factory = FakeTapFactory::new();
        let ns = Netstack::new(default_config(), factory).unwrap();

        let mut ips = Vec::new();
        for i in 0..50 {
            let ip = ns.start_pod(&format!("pod-{i:04}")).await.unwrap();
            ips.push(ip);
        }
        ips.sort();
        ips.dedup();
        assert_eq!(ips.len(), 50, "every pod got a distinct IP");
        assert_eq!(ns.pod_count().await, 50);
        assert_eq!(ns.allocator().allocated_count(), 50);
        ns.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_pod_returns_alloc_error_when_pool_exhausted() {
        // /30 = 4 addresses; 4 - 2 (net/gw) - 1 (broadcast) = 1 usable.
        let mut cfg = default_config();
        cfg.pod_cidr_prefix = 30;
        let factory = FakeTapFactory::new();
        let ns = Netstack::new(cfg, factory).unwrap();

        ns.start_pod("pod-1").await.unwrap();
        match ns.start_pod("pod-2").await {
            Err(StartPodError::AllocFailed(AllocError::Exhausted { capacity, .. })) => {
                assert_eq!(capacity, 1);
            }
            other => panic!("expected AllocFailed::Exhausted, got {other:?}"),
        }
        ns.shutdown().await;
    }
}
