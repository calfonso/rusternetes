//! Node IPAM: allocate a per-node pod CIDR out of the cluster CIDR.
//!
//! Mirrors upstream kube-controller-manager's `nodeipam` range allocator
//! (`pkg/controller/nodeipam/ipam/range_allocator.go`): a node that already has
//! a `spec.podCIDR` keeps it (upstream `occupyCIDRs`), a node without one is
//! assigned the lowest free subnet (`AllocateOrOccupyCIDR`). Upstream keeps an
//! in-memory `CidrSet` rebuilt from existing nodes at startup; we derive the
//! used-set from the live Node objects on every pass instead, so allocations
//! survive controller restarts without separate persistence.
//!
//! Gated by `--allocate-node-cidrs` (which upstream requires be paired with
//! `--cluster-cidr`); IPv4 single-stack only for now.

use std::collections::HashSet;

use ipnet::Ipv4Net;

/// Static configuration for pod-CIDR allocation, parsed from the
/// `--cluster-cidr` / `--node-cidr-mask-size` flags.
#[derive(Debug, Clone)]
pub struct NodeIpamConfig {
    /// The whole cluster pod network (e.g. `10.244.0.0/16`).
    pub cluster_cidr: Ipv4Net,
    /// Prefix length of each per-node subnet (e.g. `24`).
    pub node_mask: u8,
}

impl NodeIpamConfig {
    /// Parse `--cluster-cidr` and validate `--node-cidr-mask-size` against it.
    /// The node mask must be no shorter than the cluster prefix and at most 32.
    pub fn new(cluster_cidr: &str, node_mask: u8) -> Result<Self, String> {
        let cluster: Ipv4Net = cluster_cidr
            .parse()
            .map_err(|e| format!("invalid --cluster-cidr {cluster_cidr:?}: {e}"))?;
        let cluster = cluster.trunc();
        if node_mask > 32 {
            return Err(format!("--node-cidr-mask-size {node_mask} exceeds 32"));
        }
        if node_mask < cluster.prefix_len() {
            return Err(format!(
                "--node-cidr-mask-size {node_mask} is shorter than the cluster CIDR prefix /{}",
                cluster.prefix_len()
            ));
        }
        Ok(Self {
            cluster_cidr: cluster,
            node_mask,
        })
    }
}

/// The lowest `node_mask`-sized subnet of the cluster CIDR not already in
/// `used`. Returns `None` when the cluster CIDR is exhausted. `used` entries
/// should be network-aligned (see [`add_used`]).
pub fn next_free_pod_cidr(cfg: &NodeIpamConfig, used: &HashSet<Ipv4Net>) -> Option<Ipv4Net> {
    cfg.cluster_cidr
        .subnets(cfg.node_mask)
        .ok()?
        .find(|subnet| !used.contains(subnet))
}

/// Insert a node's existing pod-CIDR string into the used-set, normalised to
/// its network address so it compares equal to a freshly-allocated subnet.
/// Unparseable or non-IPv4 values are ignored.
pub fn add_used(cidr: &str, used: &mut HashSet<Ipv4Net>) {
    if let Ok(net) = cidr.parse::<Ipv4Net>() {
        used.insert(net.trunc());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn net(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    #[test]
    fn config_rejects_node_mask_shorter_than_cluster() {
        // /16 cluster with a /8 node mask is nonsense.
        assert!(NodeIpamConfig::new("10.244.0.0/16", 8).is_err());
        assert!(NodeIpamConfig::new("10.244.0.0/16", 33).is_err());
        assert!(NodeIpamConfig::new("not-a-cidr", 24).is_err());
        assert!(NodeIpamConfig::new("10.244.0.0/16", 24).is_ok());
    }

    #[test]
    fn allocates_lowest_subnet_when_none_used() {
        let cfg = NodeIpamConfig::new("10.244.0.0/16", 24).unwrap();
        let used = HashSet::new();
        assert_eq!(next_free_pod_cidr(&cfg, &used), Some(net("10.244.0.0/24")));
    }

    #[test]
    fn skips_used_subnets() {
        let cfg = NodeIpamConfig::new("10.244.0.0/16", 24).unwrap();
        let mut used = HashSet::new();
        add_used("10.244.0.0/24", &mut used);
        add_used("10.244.1.0/24", &mut used);
        assert_eq!(next_free_pod_cidr(&cfg, &used), Some(net("10.244.2.0/24")));
    }

    #[test]
    fn add_used_normalises_to_network_address() {
        // A non-network-aligned stored value still occupies its /24 subnet.
        let cfg = NodeIpamConfig::new("10.244.0.0/16", 24).unwrap();
        let mut used = HashSet::new();
        add_used("10.244.0.5/24", &mut used);
        assert!(used.contains(&net("10.244.0.0/24")));
        assert_eq!(next_free_pod_cidr(&cfg, &used), Some(net("10.244.1.0/24")));
    }

    #[test]
    fn returns_none_when_exhausted() {
        // /30 cluster split into /31s yields exactly two subnets.
        let cfg = NodeIpamConfig::new("10.0.0.0/30", 31).unwrap();
        let mut used = HashSet::new();
        add_used("10.0.0.0/31", &mut used);
        add_used("10.0.0.2/31", &mut used);
        assert_eq!(next_free_pod_cidr(&cfg, &used), None);
    }
}
