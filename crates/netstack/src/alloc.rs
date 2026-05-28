//! Pod IP allocator — the unified pool kubelet will draw from once
//! pods land on the netstack instead of per-kubelet Docker bridges.
//!
//! ### What it replaces
//!
//! Today the kubelet starts a container on a Docker bridge, the bridge
//! assigns the container an IP, and the kubelet reads that IP back via
//! `runtime.get_pod_ip` to stamp into `pod.status.podIP`. Each kubelet
//! container gets a different Docker bridge, so pods on different
//! "nodes" can't address each other directly — the fundamental problem
//! the netstack exists to solve.
//!
//! The netstack flips this: allocate the pod IP **first** from a
//! single shared pool, open a TAP, configure the pod's netns to use
//! that IP, then start the container. Every pod gets a unique address
//! drawn from the same `/16` (or whatever) regardless of which kubelet
//! container hosts it.
//!
//! ### Allocation strategy
//!
//! Linear scan from a "next hint" pointer that wraps. Simple, no
//! external dependencies, plenty fast for pod-create rates (well under
//! 1 ms per allocation up to ~10k pods). When we eventually need
//! faster (e.g., bulk pod-create during cluster boot), a free-list
//! with O(1) pop / push is the easy upgrade — keep the API the same.
//!
//! ### Reserved addresses
//!
//! Within a `/16` like `10.244.0.0/16`:
//!
//! - `.0` (network address) — never allocated
//! - `.1` (gateway address, by convention) — never allocated
//! - `.255` (broadcast for sub-/24) — allocated freely; we're a flat /16,
//!   no smaller subnets, so the .255 in each /24 is just another host
//! - The all-bits-set broadcast (e.g., `10.244.255.255` for /16) — never
//!   allocated

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, trace};

/// Failure modes for [`PodIpAllocator::new`] and `allocate`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AllocError {
    #[error("invalid pod CIDR prefix length {0}: must be 1..=30 (need room for at least 2 usable hosts)")]
    InvalidPrefix(u8),

    #[error("no free IPs in pod CIDR (capacity {capacity}, allocated {allocated})")]
    Exhausted { capacity: usize, allocated: usize },
}

/// Thread-safe pool that hands out fresh IPv4 addresses from a CIDR
/// and reclaims them on release.
///
/// Wrap in `Arc` for sharing across kubelet's pod-create tasks. Every
/// method takes `&self` and uses interior locking, so no external
/// synchronisation is needed at call sites.
#[derive(Debug)]
pub struct PodIpAllocator {
    /// First usable address, encoded as u32 host order. Always `network + 2`
    /// (network address + gateway).
    range_start: u32,
    /// One past the last usable address. Always `broadcast` (which is
    /// reserved).
    range_end: u32,
    /// CIDR base (network address) as originally given — exposed via
    /// [`PodIpAllocator::cidr_base`] so callers can derive the
    /// gateway / netmask without re-parsing the operator's input.
    cidr_base: Ipv4Addr,
    cidr_prefix: u8,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    allocated: HashSet<Ipv4Addr>,
    /// Where the next `allocate` scan begins. Wraps around after
    /// `range_end`. Reduces the average-case scan cost when the pool
    /// is mostly empty.
    next_hint: u32,
}

impl PodIpAllocator {
    /// Construct an allocator for `cidr_base / prefix_len`. The first
    /// two addresses (network + gateway) and the broadcast are
    /// reserved; everything in between is allocatable.
    ///
    /// Prefix length must leave at least 2 usable hosts, so 1..=30
    /// are accepted (a /30 gives exactly 2 usable hosts after the
    /// network/gateway/broadcast reservations).
    pub fn new(cidr_base: Ipv4Addr, prefix_len: u8) -> Result<Self, AllocError> {
        if !(1..=30).contains(&prefix_len) {
            return Err(AllocError::InvalidPrefix(prefix_len));
        }
        // Mask the base address to the network address so callers
        // can pass any IP within the range and we still get the
        // canonical network.
        let mask: u32 = !0u32 << (32 - prefix_len);
        let network = u32::from(cidr_base) & mask;
        let broadcast = network | !mask;
        let range_start = network + 2;
        let range_end = broadcast;
        debug!(
            cidr_base = ?Ipv4Addr::from(network),
            prefix_len,
            first_usable = ?Ipv4Addr::from(range_start),
            last_usable = ?Ipv4Addr::from(range_end - 1),
            "PodIpAllocator: initialised"
        );
        Ok(Self {
            range_start,
            range_end,
            cidr_base: Ipv4Addr::from(network),
            cidr_prefix: prefix_len,
            inner: Mutex::new(Inner {
                allocated: HashSet::new(),
                next_hint: range_start,
            }),
        })
    }

    /// CIDR base (network address) the allocator covers. Always the
    /// canonical network form (caller-supplied `cidr_base` masked to
    /// the prefix). Stable across the allocator's lifetime.
    pub fn cidr_base(&self) -> Ipv4Addr {
        self.cidr_base
    }

    /// CIDR prefix length the allocator covers (e.g., 16 for a /16).
    pub fn cidr_prefix(&self) -> u8 {
        self.cidr_prefix
    }

    /// Allocate the next free address. Returns `Err(Exhausted)` only
    /// when the entire usable range is in use.
    pub fn allocate(&self) -> Result<Ipv4Addr, AllocError> {
        let mut g = self.inner.lock().expect("PodIpAllocator mutex poisoned");
        let capacity = (self.range_end - self.range_start) as usize;
        if g.allocated.len() >= capacity {
            return Err(AllocError::Exhausted {
                capacity,
                allocated: g.allocated.len(),
            });
        }
        // Scan from next_hint forward, wrapping to range_start.
        let start = g.next_hint;
        let mut cur = start;
        loop {
            let ip = Ipv4Addr::from(cur);
            if !g.allocated.contains(&ip) {
                g.allocated.insert(ip);
                g.next_hint = if cur + 1 >= self.range_end {
                    self.range_start
                } else {
                    cur + 1
                };
                trace!(?ip, "PodIpAllocator: allocated");
                return Ok(ip);
            }
            cur = if cur + 1 >= self.range_end {
                self.range_start
            } else {
                cur + 1
            };
            // Defensive: should never fire because the capacity check
            // above guards the "all in use" case, but guard the loop.
            if cur == start {
                return Err(AllocError::Exhausted {
                    capacity,
                    allocated: g.allocated.len(),
                });
            }
        }
    }

    /// Release a previously-allocated address. Returns `true` if the
    /// address was present (kubelet's normal path); `false` if it was
    /// not (a no-op, but worth surfacing because a kubelet that
    /// double-frees a pod IP is buggy).
    pub fn release(&self, ip: Ipv4Addr) -> bool {
        let mut g = self.inner.lock().expect("PodIpAllocator mutex poisoned");
        let removed = g.allocated.remove(&ip);
        if removed {
            trace!(?ip, "PodIpAllocator: released");
        }
        removed
    }

    /// True if `ip` is currently allocated.
    pub fn is_allocated(&self, ip: Ipv4Addr) -> bool {
        self.inner
            .lock()
            .expect("PodIpAllocator mutex poisoned")
            .allocated
            .contains(&ip)
    }

    /// Number of currently-allocated addresses.
    pub fn allocated_count(&self) -> usize {
        self.inner
            .lock()
            .expect("PodIpAllocator mutex poisoned")
            .allocated
            .len()
    }

    /// Total usable address count (capacity ceiling for `allocate`).
    pub fn capacity(&self) -> usize {
        (self.range_end - self.range_start) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_allocator() -> PodIpAllocator {
        // /29 = 8 addresses; 8 - 2 (net/gw) - 1 (broadcast) = 5 usable.
        PodIpAllocator::new(Ipv4Addr::new(10, 244, 0, 0), 29).unwrap()
    }

    #[test]
    fn new_rejects_too_narrow_prefix() {
        assert_eq!(
            PodIpAllocator::new(Ipv4Addr::new(10, 244, 0, 0), 31).unwrap_err(),
            AllocError::InvalidPrefix(31),
        );
        assert_eq!(
            PodIpAllocator::new(Ipv4Addr::new(10, 244, 0, 0), 32).unwrap_err(),
            AllocError::InvalidPrefix(32),
        );
        assert_eq!(
            PodIpAllocator::new(Ipv4Addr::new(10, 244, 0, 0), 0).unwrap_err(),
            AllocError::InvalidPrefix(0),
        );
    }

    #[test]
    fn capacity_matches_usable_host_count() {
        let alloc = small_allocator();
        assert_eq!(alloc.capacity(), 5);
        // /16 = 65,536; usable = 65,536 - 2 (net/gw) - 1 (broadcast) = 65,533
        let big = PodIpAllocator::new(Ipv4Addr::new(10, 244, 0, 0), 16).unwrap();
        assert_eq!(big.capacity(), 65_533);
    }

    #[test]
    fn first_allocation_starts_after_network_and_gateway() {
        // For 10.244.0.0/29: network=.0, gateway=.1, first usable=.2.
        let alloc = small_allocator();
        assert_eq!(alloc.allocate().unwrap(), Ipv4Addr::new(10, 244, 0, 2));
    }

    #[test]
    fn allocations_are_unique_and_sequential() {
        let alloc = small_allocator();
        let ips: Vec<_> = (0..5).map(|_| alloc.allocate().unwrap()).collect();
        assert_eq!(
            ips,
            vec![
                Ipv4Addr::new(10, 244, 0, 2),
                Ipv4Addr::new(10, 244, 0, 3),
                Ipv4Addr::new(10, 244, 0, 4),
                Ipv4Addr::new(10, 244, 0, 5),
                Ipv4Addr::new(10, 244, 0, 6),
            ]
        );
    }

    #[test]
    fn allocate_returns_exhausted_when_pool_is_full() {
        let alloc = small_allocator();
        for _ in 0..5 {
            alloc.allocate().unwrap();
        }
        match alloc.allocate() {
            Err(AllocError::Exhausted {
                capacity,
                allocated,
            }) => {
                assert_eq!(capacity, 5);
                assert_eq!(allocated, 5);
            }
            other => panic!("expected Exhausted, got {other:?}"),
        }
    }

    #[test]
    fn release_returns_true_for_known_ip_and_false_for_unknown() {
        let alloc = small_allocator();
        let ip = alloc.allocate().unwrap();
        assert!(alloc.release(ip));
        assert!(
            !alloc.release(ip),
            "double-release returns false (caller bug signal)"
        );
        // Never-allocated IP also returns false.
        assert!(!alloc.release(Ipv4Addr::new(10, 244, 0, 99)));
    }

    #[test]
    fn released_address_is_reused_after_pool_pressure() {
        // Fill the pool, release one in the middle, allocate again.
        // The released slot should come back (the scan wraps).
        let alloc = small_allocator();
        let _ip1 = alloc.allocate().unwrap();
        let ip2 = alloc.allocate().unwrap();
        let _ip3 = alloc.allocate().unwrap();
        let _ip4 = alloc.allocate().unwrap();
        let _ip5 = alloc.allocate().unwrap();

        alloc.release(ip2);
        assert_eq!(alloc.allocated_count(), 4);

        let reused = alloc.allocate().unwrap();
        assert_eq!(reused, ip2, "the freed address comes back on next allocate");
    }

    #[test]
    fn is_allocated_reports_current_state() {
        let alloc = small_allocator();
        let ip = alloc.allocate().unwrap();
        assert!(alloc.is_allocated(ip));
        assert!(!alloc.is_allocated(Ipv4Addr::new(10, 244, 0, 99)));
        alloc.release(ip);
        assert!(!alloc.is_allocated(ip));
    }

    #[test]
    fn cidr_base_is_normalised_to_network_address() {
        // Caller passes 10.244.0.42/29 — that's the same network as
        // 10.244.0.40/29 (.40 is the network, .41 the gateway). We
        // should normalise.
        let alloc = PodIpAllocator::new(Ipv4Addr::new(10, 244, 0, 42), 29).unwrap();
        assert_eq!(alloc.allocate().unwrap(), Ipv4Addr::new(10, 244, 0, 42));
    }

    #[test]
    fn allocator_is_safe_to_share_across_threads() {
        use std::sync::Arc;
        // /16 — plenty of room. Two threads each allocate 1000 IPs;
        // every one should be distinct and within the range.
        let alloc = Arc::new(PodIpAllocator::new(Ipv4Addr::new(10, 244, 0, 0), 16).unwrap());

        let a = alloc.clone();
        let t1 = std::thread::spawn(move || {
            (0..1000).map(|_| a.allocate().unwrap()).collect::<Vec<_>>()
        });
        let b = alloc.clone();
        let t2 = std::thread::spawn(move || {
            (0..1000).map(|_| b.allocate().unwrap()).collect::<Vec<_>>()
        });
        let mut all: Vec<_> = t1.join().unwrap();
        all.extend(t2.join().unwrap());
        all.sort();
        all.dedup();
        assert_eq!(
            all.len(),
            2000,
            "every allocation under contention was unique"
        );
        assert_eq!(alloc.allocated_count(), 2000);
    }
}
