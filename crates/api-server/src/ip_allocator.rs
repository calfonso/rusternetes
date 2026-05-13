use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::sync::{Mutex, MutexGuard};

/// Simple ClusterIP allocator for services
/// Allocates IPs from a predefined CIDR range
pub struct ClusterIPAllocator {
    /// CIDR range for cluster IPs (e.g., "10.96.0.0/12" like Kubernetes default)
    base_ip: Ipv4Addr,
    /// Number of IPs available in the range
    pool_size: u32,
    /// Currently allocated IPs
    allocated: Mutex<HashSet<String>>,
}

impl ClusterIPAllocator {
    /// Lock the allocated-IP set, recovering from poison rather than panicking.
    /// A panic in another thread shouldn't lock out all future ClusterIP allocation.
    fn lock_allocated(&self) -> MutexGuard<'_, HashSet<String>> {
        self.allocated.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("ip_allocator mutex poisoned, recovering");
            poisoned.into_inner()
        })
    }

    /// Create a new allocator with the default Kubernetes service CIDR
    /// 10.96.0.0/12 provides 1,048,576 IPs (10.96.0.0 - 10.111.255.255)
    pub fn new() -> Self {
        Self::with_cidr("10.96.0.0".to_string(), 12)
    }

    /// Create allocator with custom CIDR
    pub fn with_cidr(base_ip: String, prefix_len: u8) -> Self {
        let base = base_ip
            .parse::<Ipv4Addr>()
            .unwrap_or(Ipv4Addr::new(10, 96, 0, 0));

        // Calculate pool size based on prefix length
        // For /12, that's 32-12 = 20 bits = 2^20 = 1,048,576 IPs
        let pool_size = 2u32.pow((32 - prefix_len) as u32);

        Self {
            base_ip: base,
            pool_size,
            allocated: Mutex::new(HashSet::new()),
        }
    }

    /// Allocate a new ClusterIP
    /// Returns None if all IPs are allocated
    pub fn allocate(&self) -> Option<String> {
        let mut allocated = self.lock_allocated();

        // Try to find an available IP
        // Start from .1 (skip .0 which is network address)
        for i in 1..self.pool_size {
            let ip = self.offset_to_ip(i);
            let ip_str = ip.to_string();

            if !allocated.contains(&ip_str) {
                allocated.insert(ip_str.clone());
                return Some(ip_str);
            }
        }

        None
    }

    /// Allocate a specific IP if available
    pub fn allocate_specific(&self, ip: String) -> bool {
        let mut allocated = self.lock_allocated();

        if allocated.contains(&ip) {
            return false;
        }

        // Verify IP is in our range
        if !self.is_in_range(&ip) {
            return false;
        }

        allocated.insert(ip);
        true
    }

    /// Release an allocated IP back to the pool
    pub fn release(&self, ip: &str) {
        let mut allocated = self.lock_allocated();
        allocated.remove(ip);
    }

    /// Check if an IP is in the allocatable range
    fn is_in_range(&self, ip_str: &str) -> bool {
        let ip = match ip_str.parse::<Ipv4Addr>() {
            Ok(addr) => addr,
            Err(_) => return false,
        };

        let base_u32 = u32::from(self.base_ip);
        let ip_u32 = u32::from(ip);
        let max_u32 = base_u32 + self.pool_size - 1;

        ip_u32 >= base_u32 && ip_u32 <= max_u32
    }

    /// Convert offset to IP address
    fn offset_to_ip(&self, offset: u32) -> Ipv4Addr {
        let base_u32 = u32::from(self.base_ip);
        Ipv4Addr::from(base_u32 + offset)
    }

    /// Mark existing IPs as allocated (for recovery after restart)
    #[allow(dead_code)]
    pub fn mark_allocated(&self, ip: String) {
        if self.is_in_range(&ip) {
            let mut allocated = self.lock_allocated();
            allocated.insert(ip);
        }
    }

    /// Get statistics about IP allocation
    #[allow(dead_code)]
    pub fn stats(&self) -> (usize, u32) {
        let allocated = self.lock_allocated();
        (allocated.len(), self.pool_size)
    }
}

impl Default for ClusterIPAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_and_release() {
        let allocator = ClusterIPAllocator::new();

        // Allocate an IP
        let ip1 = allocator.allocate().unwrap();
        assert_eq!(ip1, "10.96.0.1"); // First IP should be .1

        // Allocate another
        let ip2 = allocator.allocate().unwrap();
        assert_eq!(ip2, "10.96.0.2");

        // Release first IP
        allocator.release(&ip1);

        // Next allocation should reuse the released IP
        let ip3 = allocator.allocate().unwrap();
        assert_eq!(ip3, "10.96.0.1");
    }

    #[test]
    fn test_allocate_specific() {
        let allocator = ClusterIPAllocator::new();

        // Allocate specific IP
        assert!(allocator.allocate_specific("10.96.1.100".to_string()));

        // Try to allocate same IP again
        assert!(!allocator.allocate_specific("10.96.1.100".to_string()));

        // Allocate IP outside range
        assert!(!allocator.allocate_specific("10.0.0.1".to_string()));
    }

    #[test]
    fn test_is_in_range() {
        let allocator = ClusterIPAllocator::new();

        // IPs in range
        assert!(allocator.is_in_range("10.96.0.0"));
        assert!(allocator.is_in_range("10.96.0.1"));
        assert!(allocator.is_in_range("10.100.0.0"));
        assert!(allocator.is_in_range("10.111.255.255"));

        // IPs out of range
        assert!(!allocator.is_in_range("10.95.255.255"));
        assert!(!allocator.is_in_range("10.112.0.0"));
        assert!(!allocator.is_in_range("192.168.1.1"));
    }

    #[test]
    fn test_custom_cidr() {
        let allocator = ClusterIPAllocator::with_cidr("192.168.0.0".to_string(), 24);

        let ip1 = allocator.allocate().unwrap();
        assert_eq!(ip1, "192.168.0.1");

        let ip2 = allocator.allocate().unwrap();
        assert_eq!(ip2, "192.168.0.2");

        // Check range
        assert!(allocator.is_in_range("192.168.0.255"));
        assert!(!allocator.is_in_range("192.168.1.0"));
    }

    #[test]
    fn test_stats() {
        let allocator = ClusterIPAllocator::new();

        let (used, total) = allocator.stats();
        assert_eq!(used, 0);
        assert_eq!(total, 1048576); // 2^20 for /12

        allocator.allocate();
        allocator.allocate();

        let (used, total) = allocator.stats();
        assert_eq!(used, 2);
        assert_eq!(total, 1048576);
    }
}
