use anyhow::{Context, Result};
use std::process::Command;
use tracing::{debug, error, info, warn};

/// Detect the correct iptables command to use.
/// Docker Desktop uses nftables backend (`iptables`), while Podman uses `iptables-legacy`.
/// We must match the backend that the container runtime uses, otherwise DNAT rules
/// won't apply to container traffic.
fn detect_iptables_cmd() -> &'static str {
    // Try `iptables` first (nftables backend, used by Docker Desktop)
    if let Ok(output) = Command::new("/usr/sbin/iptables")
        .args(["-t", "nat", "-L", "PREROUTING", "-n"])
        .output()
    {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // If the DOCKER chain jump exists in iptables (nftables), Docker is using this backend
        if stdout.contains("DOCKER") {
            info!("Detected Docker nftables backend, using /usr/sbin/iptables");
            return "/usr/sbin/iptables";
        }
    }
    // Fall back to iptables-legacy (Podman, older systems)
    info!("Using /usr/sbin/iptables-legacy (Podman/legacy backend)");
    "/usr/sbin/iptables-legacy"
}

/// Detect the container bridge interface and its CIDR.
/// Works with both Docker (172.18.0.0/16 on br-xxx) and Podman (10.89.0.0/24 on podman1).
/// Returns (interface_name, cidr) if found.
fn detect_bridge_network() -> Option<(String, String)> {
    let output = Command::new("ip").args(["route", "show"]).output().ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Look for routes that match known container bridge patterns.
    // Docker: "172.18.0.0/16 dev br-abcdef proto kernel scope link src 172.18.0.1"
    // Podman: "10.89.0.0/24 dev podman1 proto kernel scope link src 10.89.0.1"
    // We look for routes on interfaces named podman*, br-*, docker*, cni*, or
    // any interface with a 10.x or 172.x private subnet and "proto kernel".
    for line in stdout.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        // Route format: CIDR dev IFACE [proto kernel] [scope link] [src IP]
        if parts.len() < 3 || parts[1] != "dev" {
            continue;
        }
        let cidr = parts[0];
        let iface = parts[2];

        // Skip loopback and default routes
        if iface == "lo" || cidr == "default" {
            continue;
        }

        // Match known container bridge interface names
        let is_bridge = iface.starts_with("podman")
            || iface.starts_with("br-")
            || iface.starts_with("docker")
            || iface.starts_with("cni");

        // Also match private subnets on kernel routes (proto kernel = directly connected)
        let is_private = cidr.starts_with("10.")
            || cidr.starts_with("172.16.")
            || cidr.starts_with("172.17.")
            || cidr.starts_with("172.18.")
            || cidr.starts_with("172.19.")
            || cidr.starts_with("172.2")
            || cidr.starts_with("172.3")
            || cidr.starts_with("192.168.");
        let is_kernel_route = line.contains("proto kernel");

        if is_bridge || (is_private && is_kernel_route && !iface.starts_with("en")) {
            info!(
                "Detected container bridge network: {} on interface {}",
                cidr, iface
            );
            return Some((iface.to_string(), cidr.to_string()));
        }
    }

    warn!("Could not detect container bridge network");
    None
}

/// IptablesManager handles iptables rule programming for service networking
pub struct IptablesManager {
    /// Chain names we create
    services_chain: String,
    nodeports_chain: String,
    /// The iptables command to use (detected at init)
    iptables_cmd: String,
    /// Track per-endpoint chains (KUBE-SEP-*) so we can clean them up on flush
    sep_chains: std::sync::Mutex<Vec<String>>,
    /// Whether the xt_recent kernel module is available
    recent_available: bool,
    /// Detected container bridge interface name (e.g., "podman1", "br-abcdef")
    bridge_iface: Option<String>,
    /// Detected container bridge CIDR (e.g., "10.89.0.0/24", "172.18.0.0/16")
    bridge_cidr: Option<String>,
}

impl Default for IptablesManager {
    fn default() -> Self {
        Self::new()
    }
}

impl IptablesManager {
    /// Construct an IptablesManager with all subprocess detection skipped.
    /// Useful for unit tests: build_nat_rules() can be exercised in isolation
    /// without invoking the iptables binary. `recent_available` controls the
    /// session-affinity codepath the test wants to exercise.
    #[cfg(test)]
    pub fn for_testing(recent_available: bool) -> Self {
        Self {
            services_chain: "RUSTERNETES-SERVICES".to_string(),
            nodeports_chain: "RUSTERNETES-NODEPORTS".to_string(),
            iptables_cmd: "/usr/sbin/iptables".to_string(),
            sep_chains: std::sync::Mutex::new(Vec::new()),
            recent_available,
            bridge_iface: None,
            bridge_cidr: None,
        }
    }

    pub fn new() -> Self {
        let iptables_cmd = detect_iptables_cmd().to_string();
        let bridge_network = detect_bridge_network();
        // Probe whether the xt_recent module is available by trying a dummy check rule.
        // If the module is missing, stderr contains "Couldn't load" or "No such file".
        // If the rule just doesn't exist, stderr contains "does a matching rule exist"
        // which means the module loaded fine.
        let recent_available = Command::new(&iptables_cmd)
            .args([
                "-t",
                "nat",
                "-C",
                "OUTPUT",
                "-m",
                "recent",
                "--name",
                "__probe__",
                "--rcheck",
                "-j",
                "RETURN",
            ])
            .output()
            .map(|o| {
                let stderr = String::from_utf8_lossy(&o.stderr);
                !stderr.contains("Couldn't load") && !stderr.contains("No such file")
            })
            .unwrap_or(false);
        if recent_available {
            info!("xt_recent module is available, session affinity will use it");
        } else {
            warn!(
                "xt_recent module is NOT available, session affinity will fall back to direct DNAT"
            );
        }
        let (bridge_iface, bridge_cidr) = match bridge_network {
            Some((iface, cidr)) => (Some(iface), Some(cidr)),
            None => (None, None),
        };
        Self {
            services_chain: "RUSTERNETES-SERVICES".to_string(),
            nodeports_chain: "RUSTERNETES-NODEPORTS".to_string(),
            iptables_cmd,
            sep_chains: std::sync::Mutex::new(Vec::new()),
            recent_available,
            bridge_iface,
            bridge_cidr,
        }
    }

    /// Initialize iptables chains and jump rules
    pub fn initialize(&self) -> Result<()> {
        info!("Initializing iptables chains for kube-proxy");

        // Best-effort flush of any RUSTERNETES-* chains left over from a
        // previous instance that was force-killed (SIGKILL / OOM) and never
        // ran its Drop. Without this the next run would stack duplicate jump
        // rules onto the host's PREROUTING/OUTPUT and the iptables state
        // diverges from what kube-proxy believes it controls. Errors are
        // ignored — on a clean first run the chains simply don't exist yet.
        let _ = self.cleanup();

        // Enable bridge-nf-call-iptables so that iptables rules apply to
        // bridge-forwarded traffic. Without this, pod-to-NodePort traffic
        // within the container bridge bypasses PREROUTING/OUTPUT DNAT rules.
        // K8s ref: k8s.io/kubernetes/pkg/proxy/iptables/proxier.go — ensureBridgeNFCallIPTables
        for sysctl in [
            "/proc/sys/net/bridge/bridge-nf-call-iptables",
            "/proc/sys/net/bridge/bridge-nf-call-ip6tables",
        ] {
            if std::path::Path::new(sysctl).exists() {
                if let Err(e) = std::fs::write(sysctl, "1") {
                    warn!(
                        "Failed to enable {}: {} (NodePort from pods may not work)",
                        sysctl, e
                    );
                } else {
                    info!("Enabled {}", sysctl);
                }
            } else {
                // Try loading br_netfilter module
                let _ = Command::new("modprobe").arg("br_netfilter").output();
                if std::path::Path::new(sysctl).exists() {
                    let _ = std::fs::write(sysctl, "1");
                    info!("Loaded br_netfilter and enabled {}", sysctl);
                } else {
                    debug!("{} not available (br_netfilter module not loaded)", sysctl);
                }
            }
        }

        // Create our custom chains if they don't exist
        self.ensure_chain("nat", &self.services_chain)?;
        self.ensure_chain("nat", &self.nodeports_chain)?;

        // Add jump rules from PREROUTING and OUTPUT to our chains
        self.ensure_jump_rule(
            "nat",
            "PREROUTING",
            &self.services_chain,
            "kubernetes service portals",
        )?;

        self.ensure_jump_rule(
            "nat",
            "OUTPUT",
            &self.services_chain,
            "kubernetes service portals",
        )?;

        // Add jump rules for NodePort services (both PREROUTING and OUTPUT)
        self.ensure_jump_rule(
            "nat",
            "PREROUTING",
            &self.nodeports_chain,
            "kubernetes service node ports",
        )?;

        self.ensure_jump_rule(
            "nat",
            "OUTPUT",
            &self.nodeports_chain,
            "kubernetes service node ports",
        )?;

        // Add MASQUERADE rule for hairpin NAT (container→ClusterIP→container on same bridge).
        // Without this, DNATed traffic within the Docker bridge doesn't have its source
        // rewritten, so the return path bypasses NAT and the connection fails.
        // Use the dynamically detected bridge CIDR (works with both Docker and Podman).
        if let Some(ref cidr) = self.bridge_cidr {
            let masq_check = Command::new(&self.iptables_cmd)
                .args([
                    "-t",
                    "nat",
                    "-C",
                    "POSTROUTING",
                    "-m",
                    "comment",
                    "--comment",
                    "rusternetes service hairpin masquerade",
                    "-s",
                    cidr,
                    "-d",
                    cidr,
                    "-j",
                    "MASQUERADE",
                ])
                .output();
            if masq_check.map_or(true, |o| !o.status.success()) {
                let output = Command::new(&self.iptables_cmd)
                    .args([
                        "-t",
                        "nat",
                        "-A",
                        "POSTROUTING",
                        "-m",
                        "comment",
                        "--comment",
                        "rusternetes service hairpin masquerade",
                        "-s",
                        cidr,
                        "-d",
                        cidr,
                        "-j",
                        "MASQUERADE",
                    ])
                    .output()
                    .context("Failed to add hairpin MASQUERADE rule")?;
                if output.status.success() {
                    info!(
                        "Added hairpin MASQUERADE rule for {} on container bridge",
                        cidr
                    );
                } else {
                    warn!(
                        "Failed to add hairpin MASQUERADE: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }
        } else {
            warn!("No bridge CIDR detected, skipping hairpin MASQUERADE rule");
        }

        // Add MASQUERADE for NodePort traffic from local sources.
        // When a process on the node itself connects to a NodePort, the source IP
        // is local. Without MASQUERADE the backend pod replies directly, bypassing
        // conntrack, and the connection breaks (asymmetric routing).
        let nodeport_masq_check = Command::new(&self.iptables_cmd)
            .args([
                "-t",
                "nat",
                "-C",
                "POSTROUTING",
                "-m",
                "comment",
                "--comment",
                "rusternetes nodeport masquerade",
                "-m",
                "addrtype",
                "--src-type",
                "LOCAL",
                "-j",
                "MASQUERADE",
            ])
            .output();
        if nodeport_masq_check.map_or(true, |o| !o.status.success()) {
            let output = Command::new(&self.iptables_cmd)
                .args([
                    "-t",
                    "nat",
                    "-A",
                    "POSTROUTING",
                    "-m",
                    "comment",
                    "--comment",
                    "rusternetes nodeport masquerade",
                    "-m",
                    "addrtype",
                    "--src-type",
                    "LOCAL",
                    "-j",
                    "MASQUERADE",
                ])
                .output()
                .context("Failed to add NodePort MASQUERADE rule")?;
            if output.status.success() {
                info!("Added MASQUERADE rule for NodePort traffic (local source)");
            } else {
                warn!(
                    "Failed to add NodePort MASQUERADE: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }

        // MASQUERADE for ClusterIP DNATed traffic. When a pod connects to a
        // service ClusterIP, the packet is DNATed to a backend pod. The
        // source needs to be rewritten so the reply comes back through the
        // node for connection tracking.
        //
        // CRITICAL: scope this match to packets whose ORIGINAL destination
        // was the ClusterIP CIDR (`--ctorigdst 10.96.0.0/12`). Without
        // `--ctorigdst`, the rule fires on ANY DNATed packet — including
        // Docker's own DNS DNAT (127.0.0.11 → upstream resolver), which
        // makes container DNS lookups fail and breaks the host's DNS
        // resolution path. Upstream kube-proxy avoids this by gating its
        // MASQUERADE on a packet mark (`KUBE-MARK-MASQ`); the `--ctorigdst`
        // filter accomplishes the same scope with fewer moving parts.
        let masq_comment = "rusternetes ClusterIP DNAT masquerade";
        let cluster_cidr = "10.96.0.0/12";
        let dnat_masq_check = Command::new(&self.iptables_cmd)
            .args([
                "-t",
                "nat",
                "-C",
                "POSTROUTING",
                "-m",
                "comment",
                "--comment",
                masq_comment,
                "-m",
                "conntrack",
                "--ctstate",
                "DNAT",
                "--ctorigdst",
                cluster_cidr,
                "-j",
                "MASQUERADE",
            ])
            .output();
        if dnat_masq_check.map_or(true, |o| !o.status.success()) {
            let output = Command::new(&self.iptables_cmd)
                .args([
                    "-t",
                    "nat",
                    "-A",
                    "POSTROUTING",
                    "-m",
                    "comment",
                    "--comment",
                    masq_comment,
                    "-m",
                    "conntrack",
                    "--ctstate",
                    "DNAT",
                    "--ctorigdst",
                    cluster_cidr,
                    "-j",
                    "MASQUERADE",
                ])
                .output()
                .context("Failed to add ClusterIP MASQUERADE rule")?;
            if output.status.success() {
                info!(
                    "Added MASQUERADE rule for DNAT'd traffic to {}",
                    cluster_cidr
                );
            } else {
                warn!(
                    "Failed to add ClusterIP MASQUERADE: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }

        // Remove the legacy over-broad rule from older versions. Best-effort
        // — if it isn't there, -D fails silently. Looped so older versions
        // that stacked duplicates get fully cleared.
        let legacy_comment = "rusternetes DNAT traffic masquerade";
        for _ in 0..16 {
            let r = Command::new(&self.iptables_cmd)
                .args([
                    "-t",
                    "nat",
                    "-D",
                    "POSTROUTING",
                    "-m",
                    "comment",
                    "--comment",
                    legacy_comment,
                    "-m",
                    "conntrack",
                    "--ctstate",
                    "DNAT",
                    "-j",
                    "MASQUERADE",
                ])
                .output();
            if r.map_or(true, |o| !o.status.success()) {
                break;
            }
            info!(
                "Removed legacy over-broad MASQUERADE rule (comment: {})",
                legacy_comment
            );
        }

        // Add FILTER table rules to accept forwarded traffic.
        // K8s kube-proxy creates a KUBE-FORWARD chain in the filter table.
        // Without these rules, forwarded packets (e.g., ClusterIP→Pod DNAT) may
        // be dropped by the default FORWARD policy.
        // See: pkg/proxy/iptables/proxier.go — KUBE-FORWARD chain
        {
            let forward_chain = "KUBE-FORWARD";
            self.ensure_chain("filter", forward_chain)?;
            self.ensure_jump_rule(
                "filter",
                "FORWARD",
                forward_chain,
                "kubernetes forwarding rules",
            )?;

            // Accept packets that have been DNATed (conntrack state DNAT)
            let dnat_accept_check = Command::new(&self.iptables_cmd)
                .args([
                    "-t",
                    "filter",
                    "-C",
                    forward_chain,
                    "-m",
                    "conntrack",
                    "--ctstate",
                    "DNAT",
                    "-m",
                    "comment",
                    "--comment",
                    "kubernetes forwarding DNAT conntrack",
                    "-j",
                    "ACCEPT",
                ])
                .output();
            if dnat_accept_check.map_or(true, |o| !o.status.success()) {
                let output = Command::new(&self.iptables_cmd)
                    .args([
                        "-t",
                        "filter",
                        "-A",
                        forward_chain,
                        "-m",
                        "conntrack",
                        "--ctstate",
                        "DNAT",
                        "-m",
                        "comment",
                        "--comment",
                        "kubernetes forwarding DNAT conntrack",
                        "-j",
                        "ACCEPT",
                    ])
                    .output()
                    .context("Failed to add KUBE-FORWARD DNAT accept rule")?;
                if output.status.success() {
                    info!("Added KUBE-FORWARD DNAT accept rule (filter table)");
                } else {
                    warn!(
                        "Failed to add KUBE-FORWARD DNAT accept: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }

            // Accept packets from/to the service CIDR
            for cidr in &["10.96.0.0/12"] {
                for flag in &["-s", "-d"] {
                    let check = Command::new(&self.iptables_cmd)
                        .args([
                            "-t",
                            "filter",
                            "-C",
                            forward_chain,
                            flag,
                            cidr,
                            "-m",
                            "comment",
                            "--comment",
                            "kubernetes forwarding service CIDR",
                            "-j",
                            "ACCEPT",
                        ])
                        .output();
                    if check.map_or(true, |o| !o.status.success()) {
                        let _ = Command::new(&self.iptables_cmd)
                            .args([
                                "-t",
                                "filter",
                                "-A",
                                forward_chain,
                                flag,
                                cidr,
                                "-m",
                                "comment",
                                "--comment",
                                "kubernetes forwarding service CIDR",
                                "-j",
                                "ACCEPT",
                            ])
                            .output();
                    }
                }
            }
            // Accept RELATED,ESTABLISHED connections (return traffic from endpoints).
            // Without this, response packets from endpoints are dropped.
            // K8s ref: proxier.go line 1460-1466
            let related_check = Command::new(&self.iptables_cmd)
                .args([
                    "-t",
                    "filter",
                    "-C",
                    forward_chain,
                    "-m",
                    "conntrack",
                    "--ctstate",
                    "RELATED,ESTABLISHED",
                    "-m",
                    "comment",
                    "--comment",
                    "kubernetes forwarding conntrack",
                    "-j",
                    "ACCEPT",
                ])
                .output();
            if related_check.map_or(true, |o| !o.status.success()) {
                let _ = Command::new(&self.iptables_cmd)
                    .args([
                        "-t",
                        "filter",
                        "-A",
                        forward_chain,
                        "-m",
                        "conntrack",
                        "--ctstate",
                        "RELATED,ESTABLISHED",
                        "-m",
                        "comment",
                        "--comment",
                        "kubernetes forwarding conntrack",
                        "-j",
                        "ACCEPT",
                    ])
                    .output();
            }

            // Add jump from filter OUTPUT to KUBE-FORWARD for local traffic.
            // K8s ref: proxier.go line 386 — filter OUTPUT → KUBE-SERVICES
            // Local pods connecting to ClusterIPs go through OUTPUT, not FORWARD.
            self.ensure_jump_rule(
                "filter",
                "OUTPUT",
                forward_chain,
                "kubernetes service portals",
            )?;

            info!("Ensured KUBE-FORWARD filter rules for service traffic");
        }

        // Ensure the service CIDR (10.96.0.0/12) is routable.
        // Without a route, packets to ClusterIPs may be dropped before reaching
        // iptables PREROUTING/DNAT. We add a route pointing to the container bridge
        // so the kernel accepts the packets and lets iptables DNAT them.
        // Note: PREROUTING DNAT happens before routing, so the route is mainly
        // needed for locally originated traffic (OUTPUT chain) and as a safety net.
        let route_check = Command::new("ip")
            .args(["route", "show", "10.96.0.0/12"])
            .output();
        if route_check.map_or(true, |o| o.stdout.is_empty()) {
            // Use the dynamically detected bridge interface
            if let Some(ref iface) = self.bridge_iface {
                let output = Command::new("ip")
                    .args(["route", "add", "10.96.0.0/12", "dev", iface])
                    .output();
                match output {
                    Ok(o) if o.status.success() => {
                        info!("Added route for service CIDR 10.96.0.0/12 via {}", iface);
                    }
                    Ok(o) => {
                        let stderr = String::from_utf8_lossy(&o.stderr);
                        if !stderr.contains("File exists") {
                            warn!("Failed to add service CIDR route: {}", stderr);
                        }
                    }
                    Err(e) => warn!("Failed to add service CIDR route: {}", e),
                }
            } else {
                warn!("No bridge interface detected, cannot add service CIDR route");
            }
        }

        // Create KUBE-FORWARD chain in the filter table and add rules.
        // K8s kube-proxy creates filter table rules that allow forwarded service traffic.
        // Without these, Docker's default FORWARD policy (DROP) blocks all DNATed traffic,
        // making services unreachable from other containers on the bridge network.
        // K8s ref: pkg/proxy/iptables/proxier.go:384,1452-1466
        self.ensure_chain("filter", "KUBE-FORWARD")?;
        self.ensure_jump_rule(
            "filter",
            "FORWARD",
            "KUBE-FORWARD",
            "kubernetes forwarding rules",
        )?;

        // Accept RELATED,ESTABLISHED traffic — return traffic for established connections.
        // This is critical: without it, response packets from the DNAT'd pod back to
        // the client are dropped by the FORWARD chain.
        let _ = Command::new(&self.iptables_cmd)
            .args([
                "-t",
                "filter",
                "-C",
                "KUBE-FORWARD",
                "-m",
                "conntrack",
                "--ctstate",
                "RELATED,ESTABLISHED",
                "-j",
                "ACCEPT",
            ])
            .output()
            .inspect(|o| {
                if !o.status.success() {
                    let _ = Command::new(&self.iptables_cmd)
                        .args([
                            "-t",
                            "filter",
                            "-A",
                            "KUBE-FORWARD",
                            "-m",
                            "comment",
                            "--comment",
                            "kubernetes forwarding conntrack rule",
                            "-m",
                            "conntrack",
                            "--ctstate",
                            "RELATED,ESTABLISHED",
                            "-j",
                            "ACCEPT",
                        ])
                        .output();
                }
            });

        // Accept all forwarded traffic on the Docker bridge — our services use
        // the Docker bridge network. Without this, NEW connections to DNATed
        // service IPs are blocked if the default FORWARD policy is DROP.
        let _ = Command::new(&self.iptables_cmd)
            .args([
                "-t",
                "filter",
                "-C",
                "KUBE-FORWARD",
                "-m",
                "comment",
                "--comment",
                "kubernetes forwarding rules",
                "-j",
                "ACCEPT",
            ])
            .output()
            .inspect(|o| {
                if !o.status.success() {
                    let _ = Command::new(&self.iptables_cmd)
                        .args([
                            "-t",
                            "filter",
                            "-A",
                            "KUBE-FORWARD",
                            "-m",
                            "comment",
                            "--comment",
                            "kubernetes forwarding rules",
                            "-j",
                            "ACCEPT",
                        ])
                        .output();
                }
            });

        // Also ensure the OUTPUT chain in the filter table accepts service traffic.
        // Traffic originating from the kube-proxy host (API server container) goes
        // through OUTPUT, not FORWARD.
        self.ensure_jump_rule(
            "filter",
            "OUTPUT",
            "KUBE-FORWARD",
            "kubernetes forwarding rules",
        )?;

        info!("Iptables chains initialized successfully");
        Ok(())
    }

    /// Ensure an iptables chain exists
    fn ensure_chain(&self, table: &str, chain: &str) -> Result<()> {
        // Try to create the chain, ignore error if it already exists
        let output = Command::new(&self.iptables_cmd)
            .args(["-t", table, "-N", chain])
            .output()
            .context("Failed to create iptables chain")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Chain already exists is not an error
            if !stderr.contains("Chain already exists") {
                warn!("Chain creation warning for {}: {}", chain, stderr);
            }
        }

        debug!("Ensured chain {} exists in table {}", chain, table);
        Ok(())
    }

    /// Ensure a jump rule exists
    fn ensure_jump_rule(
        &self,
        table: &str,
        from_chain: &str,
        to_chain: &str,
        comment: &str,
    ) -> Result<()> {
        // Check if jump rule already exists
        let check = Command::new(&self.iptables_cmd)
            .args([
                "-t",
                table,
                "-C",
                from_chain,
                "-j",
                to_chain,
                "-m",
                "comment",
                "--comment",
                comment,
            ])
            .output();

        match check {
            Ok(output) if output.status.success() => {
                debug!(
                    "Jump rule from {} to {} already exists",
                    from_chain, to_chain
                );
                return Ok(());
            }
            _ => {}
        }

        // Insert the jump rule at the top of the chain so our rules are
        // evaluated before any Docker/Podman rules that might RETURN early.
        let output = Command::new(&self.iptables_cmd)
            .args([
                "-t",
                table,
                "-I",
                from_chain,
                "1",
                "-j",
                to_chain,
                "-m",
                "comment",
                "--comment",
                comment,
            ])
            .output()
            .context("Failed to add iptables jump rule")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("Failed to add jump rule: {}", stderr);
            return Err(anyhow::anyhow!("Failed to add jump rule: {}", stderr));
        }

        debug!("Added jump rule from {} to {}", from_chain, to_chain);
        Ok(())
    }

    /// Flush all rules in our custom chains and clean up per-endpoint chains
    pub fn flush_rules(&self) -> Result<()> {
        debug!("Flushing kube-proxy iptables rules");

        self.flush_chain("nat", &self.services_chain)?;
        self.flush_chain("nat", &self.nodeports_chain)?;
        // KUBE-FORWARD must be flushed too — without this, cleanup()'s
        // delete_chain("filter", "KUBE-FORWARD") fails because the chain
        // still has rules.
        self.flush_chain("filter", "KUBE-FORWARD")?;

        // Clean up all per-endpoint (SEP) chains from previous sync.
        // These must be flushed and deleted, otherwise on resync:
        // - iptables -N fails (chain already exists)
        // - iptables -A appends duplicate rules to the existing chain
        // This causes stale/duplicate DNAT rules that break routing.
        let chains = {
            let mut guard = self.sep_chains.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        for chain in &chains {
            let _ = Command::new(&self.iptables_cmd)
                .args(["-t", "nat", "-F", chain.as_str()])
                .output();
            let _ = Command::new(&self.iptables_cmd)
                .args(["-t", "nat", "-X", chain.as_str()])
                .output();
            debug!("Cleaned up SEP chain {}", chain);
        }

        // Also clean up any leftover KUBE-SEP / KUBE-NP-SEP chains that might
        // exist from a previous run (e.g., after a crash or restart).
        if let Ok(output) = Command::new(&self.iptables_cmd)
            .args(["-t", "nat", "-L", "-n"])
            .output()
        {
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                if let Some(rest) = line
                    .strip_prefix("Chain KUBE-SEP-")
                    .or_else(|| line.strip_prefix("Chain KUBE-NP-SEP-"))
                {
                    // Extract chain name: "Chain KUBE-SEP-xxx (N references)"
                    let full_line = if line.starts_with("Chain KUBE-NP-SEP-") {
                        format!(
                            "KUBE-NP-SEP-{}",
                            rest.split_whitespace().next().unwrap_or("")
                        )
                    } else {
                        format!("KUBE-SEP-{}", rest.split_whitespace().next().unwrap_or(""))
                    };
                    let _ = Command::new(&self.iptables_cmd)
                        .args(["-t", "nat", "-F", full_line.as_str()])
                        .output();
                    let _ = Command::new(&self.iptables_cmd)
                        .args(["-t", "nat", "-X", full_line.as_str()])
                        .output();
                    debug!("Cleaned up leftover chain {}", full_line);
                }
            }
        }

        Ok(())
    }

    /// Flush all rules in a specific chain
    fn flush_chain(&self, table: &str, chain: &str) -> Result<()> {
        let output = Command::new(&self.iptables_cmd)
            .args(["-t", table, "-F", chain])
            .output()
            .context(format!("Failed to flush chain {}", chain))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("Failed to flush chain {}: {}", chain, stderr);
        } else {
            debug!("Flushed chain {}", chain);
        }

        Ok(())
    }

    /// Run an iptables command with error logging instead of silently discarding errors
    fn run_iptables_logged(&self, args: &[&str], description: &str) -> bool {
        match Command::new(&self.iptables_cmd).args(args).output() {
            Ok(output) if output.status.success() => {
                debug!("iptables {}: success", description);
                true
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // "Chain already exists" is expected for -N on existing chains
                if !stderr.contains("Chain already exists") {
                    warn!("iptables {} failed: {}", description, stderr.trim());
                }
                false
            }
            Err(e) => {
                error!("Failed to execute iptables for {}: {}", description, e);
                false
            }
        }
    }

    /// Track a SEP chain name so it can be cleaned up during flush
    fn track_sep_chain(&self, chain: &str) {
        let mut guard = self.sep_chains.lock().unwrap();
        guard.push(chain.to_string());
    }

    /// Add rules for a ClusterIP service.
    ///
    /// Session affinity implementation:
    /// - When `recent_available` is true, uses xt_recent module to track client IPs.
    ///   Each endpoint gets a per-endpoint chain (KUBE-SEP-*) with two separate rules:
    ///     1. `recent --set` to mark the source IP (always matches, does NOT terminate)
    ///     2. `DNAT` to redirect traffic (terminates)
    ///        The main chain has recent-check rules first (sticky routing), then
    ///        probability-based fallback rules for new connections.
    /// - When `recent_available` is false, falls back to direct DNAT rules without
    ///   session affinity (service is still reachable, just not sticky).
    ///
    /// Probability calculation uses the Kubernetes approach: rule i has probability
    /// 1/(N-i) so each endpoint gets an equal share of traffic.
    pub fn add_clusterip_rules(
        &self,
        service_ip: &str,
        port: u16,
        endpoints: &[(String, u16)],
        protocol: &str,
        session_affinity: bool,
        affinity_timeout: i32,
    ) -> Result<()> {
        if endpoints.is_empty() {
            debug!(
                "No endpoints for service {}:{}, skipping rules",
                service_ip, port
            );
            return Ok(());
        }

        debug!(
            "Adding ClusterIP rules for {}:{} ({}) with {} endpoints (affinity={})",
            service_ip,
            port,
            protocol,
            endpoints.len(),
            session_affinity
        );

        let proto = protocol.to_lowercase();
        let n = endpoints.len();

        if session_affinity && self.recent_available {
            // Session affinity with xt_recent module available.
            // Combine recent --set with DNAT in a single rule (K8s pattern) so
            // --set only fires when the rule actually matches (correct protocol
            // and port) rather than every packet that traverses the chain.
            // K8s ref: pkg/proxy/iptables/proxier.go writeServiceToEndpointRules.
            let timeout_str = affinity_timeout.to_string();
            for (idx, (endpoint_ip, endpoint_port)) in endpoints.iter().enumerate() {
                let sep_chain =
                    format!("KUBE-SEP-{}-{}-{}", service_ip.replace('.', ""), port, idx);
                let recent_name =
                    format!("AFFINITY-{}-{}-{}", service_ip.replace('.', ""), port, idx);
                let dnat_target = format!("{}:{}", endpoint_ip, endpoint_port);

                // Create the per-endpoint chain
                self.run_iptables_logged(
                    &["-t", "nat", "-N", &sep_chain],
                    &format!("create SEP chain {}", sep_chain),
                );
                self.track_sep_chain(&sep_chain);

                // Single SEP rule: recent --set + DNAT in one shot.
                self.run_iptables_logged(
                    &[
                        "-t",
                        "nat",
                        "-A",
                        &sep_chain,
                        "-p",
                        &proto,
                        "-m",
                        "recent",
                        "--name",
                        &recent_name,
                        "--set",
                        "-j",
                        "DNAT",
                        "--to-destination",
                        &dnat_target,
                    ],
                    &format!("SEP {} recent-set + DNAT -> {}", sep_chain, dnat_target),
                );

                // In the main services chain: check if source was recently seen
                // for this endpoint -> jump to its SEP chain (sticky routing)
                let port_str = port.to_string();
                self.run_iptables_logged(
                    &[
                        "-t",
                        "nat",
                        "-A",
                        &self.services_chain,
                        "-d",
                        service_ip,
                        "-p",
                        &proto,
                        "--dport",
                        &port_str,
                        "-m",
                        "recent",
                        "--name",
                        &recent_name,
                        "--rcheck",
                        "--seconds",
                        &timeout_str,
                        "--reap",
                        "-j",
                        &sep_chain,
                    ],
                    &format!("affinity recent check for {}", sep_chain),
                );
            }

            // Add probability-based fallback rules for new connections
            // (sources not yet tracked by recent module)
            let port_str = port.to_string();
            for (idx, _) in endpoints.iter().enumerate() {
                let is_last = idx == n - 1;
                let sep_chain =
                    format!("KUBE-SEP-{}-{}-{}", service_ip.replace('.', ""), port, idx);
                let mut args = vec![
                    "-t",
                    "nat",
                    "-A",
                    &self.services_chain,
                    "-d",
                    service_ip,
                    "-p",
                    &proto,
                    "--dport",
                    &port_str,
                ];
                let prob_str;
                if !is_last {
                    // Kubernetes probability: 1/(N-idx) for uniform distribution
                    let prob = 1.0 / (n - idx) as f64;
                    prob_str = format!("{:.10}", prob);
                    args.extend_from_slice(&[
                        "-m",
                        "statistic",
                        "--mode",
                        "random",
                        "--probability",
                        &prob_str,
                    ]);
                }
                args.extend_from_slice(&["-j", &sep_chain]);
                self.run_iptables_logged(
                    &args,
                    &format!("affinity probability fallback for {}", sep_chain),
                );
            }
        } else {
            // No session affinity, or single endpoint, or recent module unavailable.
            // Use direct DNAT rules with proper probability distribution.
            let port_str = port.to_string();
            for (idx, (endpoint_ip, endpoint_port)) in endpoints.iter().enumerate() {
                let is_last = idx == n - 1;
                let dnat_target = format!("{}:{}", endpoint_ip, endpoint_port);

                let mut args = vec![
                    "-t",
                    "nat",
                    "-A",
                    &self.services_chain,
                    "-d",
                    service_ip,
                    "-p",
                    &proto,
                    "--dport",
                    &port_str,
                ];
                let prob_str;
                if !is_last {
                    // Kubernetes probability: 1/(N-idx)
                    let prob = 1.0 / (n - idx) as f64;
                    prob_str = format!("{:.10}", prob);
                    args.extend_from_slice(&[
                        "-m",
                        "statistic",
                        "--mode",
                        "random",
                        "--probability",
                        &prob_str,
                    ]);
                }
                let comment = format!("rusternetes service {}:{}", service_ip, port);
                args.extend_from_slice(&[
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &dnat_target,
                    "-m",
                    "comment",
                    "--comment",
                    &comment,
                ]);
                let output = Command::new(&self.iptables_cmd)
                    .args(&args)
                    .output()
                    .context("Failed to add iptables DNAT rule")?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    error!("Failed to add DNAT rule for {}: {}", endpoint_ip, stderr);
                } else {
                    debug!("Added DNAT rule: {} -> {}", service_ip, dnat_target);
                }
            }
        }

        Ok(())
    }

    /// Add rules for a NodePort service.
    ///
    /// Supports session affinity using the same xt_recent approach as ClusterIP rules.
    /// Each endpoint gets a per-endpoint chain (KUBE-NP-SEP-*) with recent-set + DNAT.
    pub fn add_nodeport_rules(
        &self,
        node_port: u16,
        endpoints: &[(String, u16)],
        protocol: &str,
        session_affinity: bool,
        affinity_timeout: i32,
    ) -> Result<()> {
        if endpoints.is_empty() {
            debug!("No endpoints for NodePort {}, skipping rules", node_port);
            return Ok(());
        }

        debug!(
            "Adding NodePort rules for port {} ({}) with {} endpoints (affinity={})",
            node_port,
            protocol,
            endpoints.len(),
            session_affinity
        );

        let proto = protocol.to_lowercase();
        let n = endpoints.len();

        if session_affinity && self.recent_available {
            // Session affinity for NodePort with xt_recent module.
            // Combine --set + DNAT so --set only fires for matching packets.
            let timeout_str = affinity_timeout.to_string();
            for (idx, (endpoint_ip, endpoint_port)) in endpoints.iter().enumerate() {
                let sep_chain = format!("KUBE-NP-SEP-{}-{}", node_port, idx);
                let recent_name = format!("NP-AFFINITY-{}-{}", node_port, idx);
                let dnat_target = format!("{}:{}", endpoint_ip, endpoint_port);

                // Create per-endpoint chain
                self.run_iptables_logged(
                    &["-t", "nat", "-N", &sep_chain],
                    &format!("create NP SEP chain {}", sep_chain),
                );
                self.track_sep_chain(&sep_chain);

                // Single rule: recent --set + DNAT
                self.run_iptables_logged(
                    &[
                        "-t",
                        "nat",
                        "-A",
                        &sep_chain,
                        "-p",
                        &proto,
                        "-m",
                        "recent",
                        "--name",
                        &recent_name,
                        "--set",
                        "-j",
                        "DNAT",
                        "--to-destination",
                        &dnat_target,
                    ],
                    &format!("NP SEP {} recent-set + DNAT -> {}", sep_chain, dnat_target),
                );

                // Recent check in main nodeports chain
                let node_port_str = node_port.to_string();
                self.run_iptables_logged(
                    &[
                        "-t",
                        "nat",
                        "-A",
                        &self.nodeports_chain,
                        "-p",
                        &proto,
                        "--dport",
                        &node_port_str,
                        "-m",
                        "recent",
                        "--name",
                        &recent_name,
                        "--rcheck",
                        "--seconds",
                        &timeout_str,
                        "--reap",
                        "-j",
                        &sep_chain,
                    ],
                    &format!("NP affinity recent check for {}", sep_chain),
                );
            }

            // Probability-based fallback for new connections
            let node_port_str = node_port.to_string();
            for (idx, _) in endpoints.iter().enumerate() {
                let is_last = idx == n - 1;
                let sep_chain = format!("KUBE-NP-SEP-{}-{}", node_port, idx);
                let mut args = vec![
                    "-t",
                    "nat",
                    "-A",
                    &self.nodeports_chain,
                    "-p",
                    &proto,
                    "--dport",
                    &node_port_str,
                ];
                let prob_str;
                if !is_last {
                    let prob = 1.0 / (n - idx) as f64;
                    prob_str = format!("{:.10}", prob);
                    args.extend_from_slice(&[
                        "-m",
                        "statistic",
                        "--mode",
                        "random",
                        "--probability",
                        &prob_str,
                    ]);
                }
                args.extend_from_slice(&["-j", &sep_chain]);
                self.run_iptables_logged(
                    &args,
                    &format!("NP affinity probability fallback for {}", sep_chain),
                );
            }
        } else {
            // No session affinity or single endpoint — direct DNAT rules
            for (idx, (endpoint_ip, endpoint_port)) in endpoints.iter().enumerate() {
                let is_last = idx == n - 1;

                let node_port_str = node_port.to_string();
                let mut args = vec![
                    "-t",
                    "nat",
                    "-A",
                    &self.nodeports_chain,
                    "-p",
                    &proto,
                    "--dport",
                    &node_port_str,
                ];

                // Kubernetes probability: 1/(N-idx)
                let prob_str;
                if !is_last {
                    let prob = 1.0 / (n - idx) as f64;
                    prob_str = format!("{:.10}", prob);
                    args.extend_from_slice(&[
                        "-m",
                        "statistic",
                        "--mode",
                        "random",
                        "--probability",
                        &prob_str,
                    ]);
                }

                // Add DNAT to endpoint
                let dnat_target = format!("{}:{}", endpoint_ip, endpoint_port);
                let comment = format!("rusternetes nodeport {}", node_port);
                args.extend_from_slice(&[
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &dnat_target,
                    "-m",
                    "comment",
                    "--comment",
                    &comment,
                ]);

                let output = Command::new(&self.iptables_cmd)
                    .args(&args)
                    .output()
                    .context("Failed to add iptables DNAT rule for NodePort")?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    error!(
                        "Failed to add NodePort DNAT rule for {}: {}",
                        endpoint_ip, stderr
                    );
                } else {
                    debug!("Added NodePort DNAT rule: {} -> {}", node_port, dnat_target);
                }
            }
        }

        Ok(())
    }

    /// Clean up all kube-proxy iptables rules
    pub fn cleanup(&self) -> Result<()> {
        info!("Cleaning up kube-proxy iptables rules");

        // Flush our chains (also cleans up SEP chains)
        self.flush_rules()?;

        // Remove jump rules
        self.remove_jump_rule("nat", "PREROUTING", &self.services_chain)?;
        self.remove_jump_rule("nat", "OUTPUT", &self.services_chain)?;
        self.remove_jump_rule("nat", "PREROUTING", &self.nodeports_chain)?;
        self.remove_jump_rule("nat", "OUTPUT", &self.nodeports_chain)?;
        // KUBE-FORWARD lives in the filter table — initialize() jumps to it
        // from FORWARD and OUTPUT (see ensure_jump_rule call sites). Without
        // these two removals a force-killed kube-proxy leaves the jumps on
        // the host's filter table and the next run stacks duplicates.
        self.remove_jump_rule("filter", "FORWARD", "KUBE-FORWARD")?;
        self.remove_jump_rule("filter", "OUTPUT", "KUBE-FORWARD")?;

        // Remove the POSTROUTING MASQUERADE rules that initialize() added
        // directly (they aren't in our own chains, so flush_rules above
        // doesn't touch them). Looped to remove any duplicates left from
        // an older version that stacked them on each restart.
        let masq_comment = "rusternetes ClusterIP DNAT masquerade";
        let cluster_cidr = "10.96.0.0/12";
        for _ in 0..16 {
            let r = Command::new(&self.iptables_cmd)
                .args([
                    "-t",
                    "nat",
                    "-D",
                    "POSTROUTING",
                    "-m",
                    "comment",
                    "--comment",
                    masq_comment,
                    "-m",
                    "conntrack",
                    "--ctstate",
                    "DNAT",
                    "--ctorigdst",
                    cluster_cidr,
                    "-j",
                    "MASQUERADE",
                ])
                .output();
            if r.map_or(true, |o| !o.status.success()) {
                break;
            }
        }
        // Also clean up the legacy over-broad rule shape from older
        // installations, same loop pattern.
        let legacy_comment = "rusternetes DNAT traffic masquerade";
        for _ in 0..16 {
            let r = Command::new(&self.iptables_cmd)
                .args([
                    "-t",
                    "nat",
                    "-D",
                    "POSTROUTING",
                    "-m",
                    "comment",
                    "--comment",
                    legacy_comment,
                    "-m",
                    "conntrack",
                    "--ctstate",
                    "DNAT",
                    "-j",
                    "MASQUERADE",
                ])
                .output();
            if r.map_or(true, |o| !o.status.success()) {
                break;
            }
        }

        // Delete our chains
        self.delete_chain("nat", &self.services_chain)?;
        self.delete_chain("nat", &self.nodeports_chain)?;
        self.delete_chain("filter", "KUBE-FORWARD")?;

        info!("Kube-proxy iptables cleanup complete");
        Ok(())
    }

    fn remove_jump_rule(&self, table: &str, from_chain: &str, to_chain: &str) -> Result<()> {
        let output = Command::new(&self.iptables_cmd)
            .args(["-t", table, "-D", from_chain, "-j", to_chain])
            .output();

        match output {
            Ok(out) if out.status.success() => {
                debug!("Removed jump rule from {} to {}", from_chain, to_chain);
            }
            _ => {
                debug!(
                    "Jump rule from {} to {} may not exist",
                    from_chain, to_chain
                );
            }
        }

        Ok(())
    }

    fn delete_chain(&self, table: &str, chain: &str) -> Result<()> {
        let output = Command::new(&self.iptables_cmd)
            .args(["-t", table, "-X", chain])
            .output();

        match output {
            Ok(out) if out.status.success() => {
                debug!("Deleted chain {}", chain);
            }
            _ => {
                debug!("Chain {} may not exist", chain);
            }
        }

        Ok(())
    }

    /// Build all NAT rules for iptables-restore format.
    /// Returns the rules as a string ready for `iptables-restore --noflush`.
    /// K8s builds all rules in memory then applies atomically.
    /// See: pkg/proxy/iptables/proxier.go:1495
    #[allow(clippy::type_complexity)]
    pub async fn build_nat_rules(
        &self,
        services: &[rusternetes_common::resources::Service],
        endpointslice_map: &std::collections::HashMap<String, Vec<(String, Option<String>, u16)>>,
    ) -> String {
        let mut rules = String::new();

        // iptables-restore format: table header, chain definitions, rules, COMMIT
        rules.push_str("*nat\n");
        rules.push_str(&format!(":{} - [0:0]\n", self.services_chain));
        rules.push_str(&format!(":{} - [0:0]\n", self.nodeports_chain));

        // Build DNAT rules for each service
        for service in services {
            let cluster_ip = match &service.spec.cluster_ip {
                Some(ip) if !ip.is_empty() && ip != "None" => ip,
                _ => continue,
            };

            let namespace = service.metadata.namespace.as_deref().unwrap_or("default");

            for svc_port in &service.spec.ports {
                let port = svc_port.port;
                let proto = svc_port.protocol.as_deref().unwrap_or("TCP").to_lowercase();

                // Find endpoints for this service+port.
                // EndpointSlice ports are TARGET ports (container port), not service ports.
                // Match by: port name, target port number, or if service has only one port
                // take all endpoints.
                // K8s ref: pkg/proxy/iptables/proxier.go — servicePortEndpointChains
                let svc_key = format!("{}/{}", namespace, service.metadata.name);
                let target_port = svc_port.target_port.as_ref().and_then(|tp| match tp {
                    rusternetes_common::resources::IntOrString::Int(p) => Some(*p as u16),
                    rusternetes_common::resources::IntOrString::String(s) => s.parse::<u16>().ok(),
                });
                let endpoints: Vec<(String, u16)> = endpointslice_map
                    .get(&svc_key)
                    .map(|eps| {
                        eps.iter()
                            .filter(|(_, port_name, ep_port)| {
                                // Match by port name (most reliable)
                                if svc_port.name.is_some()
                                    && svc_port.name.as_deref() == port_name.as_deref()
                                {
                                    return true;
                                }
                                // Match by target port number
                                if let Some(tp) = target_port {
                                    if *ep_port == tp {
                                        return true;
                                    }
                                }
                                // Match by service port (for services where target_port == port)
                                if *ep_port == port {
                                    return true;
                                }
                                // If service has only one port, take all endpoints
                                service.spec.ports.len() == 1
                            })
                            .map(|(ip, _, ep_port)| (ip.clone(), *ep_port))
                            .collect()
                    })
                    .unwrap_or_default();

                if endpoints.is_empty() {
                    debug!(
                        "No endpoints matched for service {}/{}:{} (target_port={:?}, endpointslice_entries={})",
                        namespace,
                        service.metadata.name,
                        port,
                        target_port,
                        endpointslice_map.get(&svc_key).map(|e| e.len()).unwrap_or(0)
                    );
                    continue;
                }

                let session_affinity = service.spec.session_affinity.as_deref() == Some("ClientIP");
                let affinity_timeout = service
                    .spec
                    .session_affinity_config
                    .as_ref()
                    .and_then(|c| c.client_ip.as_ref())
                    .and_then(|c| c.timeout_seconds)
                    .unwrap_or(10800); // K8s default: 3 hours

                let n = endpoints.len();

                if session_affinity && self.recent_available {
                    // Session affinity with xt_recent: create per-endpoint chains.
                    // K8s pattern: writeServiceToEndpointRules (proxier.go:1541-1562).
                    // Combine recent --set with DNAT in one rule so --set only
                    // fires for packets that actually DNAT (correct protocol).
                    for (idx, (endpoint_ip, endpoint_port)) in endpoints.iter().enumerate() {
                        let sep_chain =
                            format!("KUBE-SEP-{}-{}-{}", cluster_ip.replace('.', ""), port, idx);
                        let recent_name =
                            format!("AFFINITY-{}-{}-{}", cluster_ip.replace('.', ""), port, idx);
                        let dnat_target = format!("{}:{}", endpoint_ip, endpoint_port);

                        // Define the per-endpoint chain
                        rules.push_str(&format!(":{} - [0:0]\n", sep_chain));

                        // SEP chain: single rule that sets the recent mark and DNATs.
                        rules.push_str(&format!(
                            "-A {} -p {} -m recent --name {} --set -j DNAT --to-destination {}\n",
                            sep_chain, proto, recent_name, dnat_target
                        ));

                        // Service chain: affinity check (--rcheck) jumps to SEP if recent
                        rules.push_str(&format!(
                            "-A {} -d {}/32 -p {} --dport {} -m recent --name {} --rcheck --seconds {} --reap -j {}\n",
                            self.services_chain, cluster_ip, proto, port,
                            recent_name, affinity_timeout, sep_chain
                        ));
                    }

                    // Fallback: probability-based load balancing for new connections.
                    // With a single endpoint the rule has no probability match
                    // and unconditionally jumps to that endpoint's SEP chain.
                    for (idx, (_endpoint_ip, _endpoint_port)) in endpoints.iter().enumerate() {
                        let is_last = idx == n - 1;
                        let sep_chain =
                            format!("KUBE-SEP-{}-{}-{}", cluster_ip.replace('.', ""), port, idx);

                        let mut rule = format!(
                            "-A {} -d {}/32 -p {} --dport {}",
                            self.services_chain, cluster_ip, proto, port
                        );
                        if !is_last {
                            let prob = 1.0 / (n - idx) as f64;
                            rule.push_str(&format!(
                                " -m statistic --mode random --probability {:.10}",
                                prob
                            ));
                        }
                        rule.push_str(&format!(" -j {}", sep_chain));
                        rules.push_str(&rule);
                        rules.push('\n');
                    }
                } else {
                    // No session affinity or single endpoint: direct DNAT
                    for (idx, (endpoint_ip, endpoint_port)) in endpoints.iter().enumerate() {
                        let is_last = idx == n - 1;
                        let dnat_target = format!("{}:{}", endpoint_ip, endpoint_port);
                        let comment = format!("rusternetes service {}:{}", cluster_ip, port);

                        let mut rule = format!(
                            "-A {} -d {}/32 -p {} --dport {}",
                            self.services_chain, cluster_ip, proto, port
                        );
                        if !is_last {
                            let prob = 1.0 / (n - idx) as f64;
                            rule.push_str(&format!(
                                " -m statistic --mode random --probability {:.10}",
                                prob
                            ));
                        }
                        rule.push_str(&format!(
                            " -j DNAT --to-destination {} -m comment --comment \"{}\"",
                            dnat_target, comment
                        ));
                        rules.push_str(&rule);
                        rules.push('\n');
                    }
                }
            }
        }

        // NodePort rules — add DNAT rules to RUSTERNETES-NODEPORTS chain.
        // Session affinity for NodePort reuses the same KUBE-SEP-* chains and
        // AFFINITY-* recent names created by the ClusterIP section above.
        // This matches K8s behavior where NodePort traffic goes through the same
        // KUBE-SVC chain and thus shares affinity state with ClusterIP traffic.
        // K8s ref: pkg/proxy/iptables/proxier.go — writeServiceToEndpointRules
        for service in services {
            let cluster_ip = match &service.spec.cluster_ip {
                Some(ip) if !ip.is_empty() && ip != "None" => ip,
                _ => continue,
            };

            let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
            for svc_port in &service.spec.ports {
                let node_port = match svc_port.node_port {
                    Some(np) if np > 0 => np,
                    _ => continue,
                };
                let proto = svc_port.protocol.as_deref().unwrap_or("TCP").to_lowercase();
                let port = svc_port.port;

                let svc_key = format!("{}/{}", namespace, service.metadata.name);
                let np_target_port = svc_port.target_port.as_ref().and_then(|tp| match tp {
                    rusternetes_common::resources::IntOrString::Int(p) => Some(*p as u16),
                    rusternetes_common::resources::IntOrString::String(s) => s.parse::<u16>().ok(),
                });
                let endpoints: Vec<(String, u16)> = endpointslice_map
                    .get(&svc_key)
                    .map(|eps| {
                        eps.iter()
                            .filter(|(_, port_name, ep_port)| {
                                if svc_port.name.is_some()
                                    && svc_port.name.as_deref() == port_name.as_deref()
                                {
                                    return true;
                                }
                                if let Some(tp) = np_target_port {
                                    if *ep_port == tp {
                                        return true;
                                    }
                                }
                                if *ep_port == svc_port.port {
                                    return true;
                                }
                                service.spec.ports.len() == 1
                            })
                            .map(|(ip, _, ep_port)| (ip.clone(), *ep_port))
                            .collect()
                    })
                    .unwrap_or_default();

                if endpoints.is_empty() {
                    continue;
                }

                let session_affinity = service.spec.session_affinity.as_deref() == Some("ClientIP");
                let affinity_timeout = service
                    .spec
                    .session_affinity_config
                    .as_ref()
                    .and_then(|c| c.client_ip.as_ref())
                    .and_then(|c| c.timeout_seconds)
                    .unwrap_or(10800);

                let n = endpoints.len();

                if session_affinity && self.recent_available {
                    // Session affinity for NodePort: reuse the same SEP chains and
                    // recent names as ClusterIP. The SEP chains (KUBE-SEP-*) were
                    // already defined in the ClusterIP section above and contain
                    // the --set + DNAT rules. We just need --rcheck rules and
                    // probability fallback rules in the nodeports chain.
                    let timeout_str = affinity_timeout.to_string();
                    for (idx, _) in endpoints.iter().enumerate() {
                        let sep_chain =
                            format!("KUBE-SEP-{}-{}-{}", cluster_ip.replace('.', ""), port, idx);
                        let recent_name =
                            format!("AFFINITY-{}-{}-{}", cluster_ip.replace('.', ""), port, idx);

                        // Affinity check: if client IP was recently seen, jump to SEP chain
                        rules.push_str(&format!(
                            "-A {} -p {} --dport {} -m recent --name {} --rcheck --seconds {} --reap -j {}\n",
                            self.nodeports_chain, proto, node_port,
                            recent_name, timeout_str, sep_chain
                        ));
                    }

                    // Probability-based fallback for new connections
                    for (idx, _) in endpoints.iter().enumerate() {
                        let is_last = idx == n - 1;
                        let sep_chain =
                            format!("KUBE-SEP-{}-{}-{}", cluster_ip.replace('.', ""), port, idx);

                        let mut rule = format!(
                            "-A {} -p {} --dport {}",
                            self.nodeports_chain, proto, node_port
                        );
                        if !is_last {
                            let prob = 1.0 / (n - idx) as f64;
                            rule.push_str(&format!(
                                " -m statistic --mode random --probability {:.10}",
                                prob
                            ));
                        }
                        rule.push_str(&format!(" -j {}", sep_chain));
                        rules.push_str(&rule);
                        rules.push('\n');
                    }
                } else {
                    // No session affinity or single endpoint: direct DNAT
                    for (idx, (endpoint_ip, endpoint_port)) in endpoints.iter().enumerate() {
                        let is_last = idx == n - 1;
                        let dnat_target = format!("{}:{}", endpoint_ip, endpoint_port);

                        let mut rule = format!(
                            "-A {} -p {} --dport {}",
                            self.nodeports_chain, proto, node_port
                        );
                        if !is_last {
                            let prob = 1.0 / (n - idx) as f64;
                            rule.push_str(&format!(
                                " -m statistic --mode random --probability {:.10}",
                                prob
                            ));
                        }
                        rule.push_str(&format!(" -j DNAT --to-destination {}", dnat_target));
                        rules.push_str(&rule);
                        rules.push('\n');
                    }
                }
            }
        }

        rules.push_str("COMMIT\n");
        rules
    }

    /// Atomically apply NAT rules using iptables-restore --noflush.
    /// This replaces our chain rules without any gap.
    pub fn apply_nat_rules_atomic(&self, rules: &str) -> Result<()> {
        use std::io::Write;

        debug!(
            "Applying {} bytes of NAT rules via iptables-restore",
            rules.len()
        );

        // DO NOT flush chains before iptables-restore.
        // iptables-restore --noflush with ":CHAIN - [0:0]" atomically
        // resets chain counters and replaces rules within the restore
        // transaction. Manual flush + restore creates a gap where
        // no rules exist (the original kube-proxy bug).
        //
        // Clean up old SEP chains AFTER restore (they're no longer referenced).
        let old_sep_chains = {
            let mut guard = self.sep_chains.lock().unwrap();
            std::mem::take(&mut *guard)
        };

        // Apply rules via iptables-restore --noflush
        // Use the matching restore command for the detected iptables backend
        let restore_cmd = if self.iptables_cmd.contains("legacy") {
            "iptables-legacy-restore"
        } else {
            "iptables-restore"
        };
        let mut child = Command::new(restore_cmd)
            .args(["--noflush"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn iptables-restore")?;

        if let Some(ref mut stdin) = child.stdin {
            stdin
                .write_all(rules.as_bytes())
                .context("Failed to write to iptables-restore stdin")?;
        }
        // Close stdin so iptables-restore processes the input
        drop(child.stdin.take());

        let output = child
            .wait_with_output()
            .context("Failed to wait for iptables-restore")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("iptables-restore failed: {}", stderr);
            return Err(anyhow::anyhow!("iptables-restore failed: {}", stderr));
        }

        debug!("iptables-restore applied successfully");

        // Clean up old SEP chains AFTER successful restore.
        // These chains are no longer referenced by the new rules.
        for chain in &old_sep_chains {
            let _ = Command::new(&self.iptables_cmd)
                .args(["-t", "nat", "-F", chain.as_str()])
                .output();
            let _ = Command::new(&self.iptables_cmd)
                .args(["-t", "nat", "-X", chain.as_str()])
                .output();
        }

        Ok(())
    }
}

impl Drop for IptablesManager {
    fn drop(&mut self) {
        // Best effort cleanup
        let _ = self.cleanup();
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_probability_calculation_uniform() {
        // Verify that the Kubernetes probability formula 1/(N-idx) produces
        // uniform distribution across all endpoints
        let n = 4;
        let mut remaining = 1.0_f64;
        let mut shares = Vec::new();
        for idx in 0..n {
            let is_last = idx == n - 1;
            if is_last {
                shares.push(remaining);
            } else {
                let prob = 1.0 / (n - idx) as f64;
                let share = remaining * prob;
                shares.push(share);
                remaining -= share;
            }
        }
        // Each endpoint should get ~0.25 (1/4)
        for share in &shares {
            assert!(
                (*share - 0.25).abs() < 1e-10,
                "Expected ~0.25, got {}",
                share
            );
        }
    }

    #[test]
    fn test_probability_calculation_two_endpoints() {
        let n = 2;
        let mut remaining = 1.0_f64;
        let mut shares = Vec::new();
        for idx in 0..n {
            let is_last = idx == n - 1;
            if is_last {
                shares.push(remaining);
            } else {
                let prob = 1.0 / (n - idx) as f64;
                let share = remaining * prob;
                shares.push(share);
                remaining -= share;
            }
        }
        assert!((shares[0] - 0.5).abs() < 1e-10);
        assert!((shares[1] - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_probability_calculation_three_endpoints() {
        let n = 3;
        let mut remaining = 1.0_f64;
        let mut shares = Vec::new();
        for idx in 0..n {
            let is_last = idx == n - 1;
            if is_last {
                shares.push(remaining);
            } else {
                let prob = 1.0 / (n - idx) as f64;
                let share = remaining * prob;
                shares.push(share);
                remaining -= share;
            }
        }
        let expected = 1.0 / 3.0;
        for (i, share) in shares.iter().enumerate() {
            assert!(
                (*share - expected).abs() < 1e-10,
                "Endpoint {}: expected ~{}, got {}",
                i,
                expected,
                share
            );
        }
    }

    #[test]
    fn test_probability_single_endpoint() {
        // With a single endpoint, no probability module is needed (is_last=true immediately)
        let n = 1;
        let idx = 0;
        let is_last = idx == n - 1;
        assert!(is_last, "Single endpoint should be the last");
    }

    #[test]
    fn test_sep_chain_tracking() {
        // Verify that SEP chains are tracked and can be retrieved
        let chains: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        {
            let mut guard = chains.lock().unwrap();
            guard.push("KUBE-SEP-10960001-80-0".to_string());
            guard.push("KUBE-SEP-10960001-80-1".to_string());
        }
        let taken = {
            let mut guard = chains.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0], "KUBE-SEP-10960001-80-0");
        assert_eq!(taken[1], "KUBE-SEP-10960001-80-1");

        // After take, the vec should be empty
        let guard = chains.lock().unwrap();
        assert!(guard.is_empty());
    }

    #[test]
    fn test_sep_chain_name_format() {
        // Verify chain name format for ClusterIP
        let service_ip = "10.96.0.1";
        let port: u16 = 80;
        let idx = 0;
        let chain = format!("KUBE-SEP-{}-{}-{}", service_ip.replace('.', ""), port, idx);
        assert_eq!(chain, "KUBE-SEP-109601-80-0");

        // Verify chain name format for NodePort
        let node_port: u16 = 30080;
        let np_chain = format!("KUBE-NP-SEP-{}-{}", node_port, idx);
        assert_eq!(np_chain, "KUBE-NP-SEP-30080-0");
    }

    use super::IptablesManager;
    use rusternetes_common::resources::service::{ClientIPConfig, SessionAffinityConfig};
    use rusternetes_common::resources::{Service, ServicePort, ServiceSpec, ServiceType};
    use rusternetes_common::types::ObjectMeta;
    use std::collections::HashMap;

    fn make_service(
        name: &str,
        namespace: &str,
        cluster_ip: &str,
        service_type: ServiceType,
        ports: Vec<ServicePort>,
        session_affinity: Option<&str>,
        timeout: Option<i32>,
    ) -> Service {
        let session_affinity_config = if session_affinity == Some("ClientIP") {
            Some(SessionAffinityConfig {
                client_ip: Some(ClientIPConfig {
                    timeout_seconds: timeout.or(Some(10800)),
                }),
            })
        } else {
            None
        };
        Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                ..ObjectMeta::default()
            },
            spec: ServiceSpec {
                cluster_ip: Some(cluster_ip.to_string()),
                service_type: Some(service_type),
                ports,
                session_affinity: session_affinity.map(String::from),
                session_affinity_config,
                ..ServiceSpec::default()
            },
            status: None,
        }
    }

    fn make_endpointslice_map(
        namespace: &str,
        service: &str,
        endpoints: &[(&str, Option<&str>, u16)],
    ) -> HashMap<String, Vec<(String, Option<String>, u16)>> {
        let mut m = HashMap::new();
        let key = format!("{}/{}", namespace, service);
        let v: Vec<(String, Option<String>, u16)> = endpoints
            .iter()
            .map(|(ip, name, port)| (ip.to_string(), name.map(String::from), *port))
            .collect();
        m.insert(key, v);
        m
    }

    #[tokio::test]
    async fn test_build_nat_rules_clusterip_session_affinity() {
        // ClusterIP + ClientIP affinity + 2 endpoints. Generated rules must
        // contain --rcheck rules in the services chain, per-endpoint SEP
        // chain definitions, and the combined --set+DNAT rule.
        let mgr = IptablesManager::for_testing(true);
        let service = make_service(
            "my-svc",
            "default",
            "10.96.0.5",
            ServiceType::ClusterIP,
            vec![ServicePort {
                name: None,
                port: 80,
                target_port: None,
                protocol: Some("TCP".to_string()),
                node_port: None,
                app_protocol: None,
            }],
            Some("ClientIP"),
            Some(7200),
        );
        let ep_map = make_endpointslice_map(
            "default",
            "my-svc",
            &[("10.0.0.1", None, 80), ("10.0.0.2", None, 80)],
        );

        let rules = mgr.build_nat_rules(&[service], &ep_map).await;

        assert!(
            rules.contains(":KUBE-SEP-109605-80-0 - [0:0]"),
            "missing SEP chain 0 declaration:\n{}",
            rules
        );
        assert!(
            rules.contains(":KUBE-SEP-109605-80-1 - [0:0]"),
            "missing SEP chain 1 declaration:\n{}",
            rules
        );

        // --rcheck rules in services chain with custom timeout
        assert!(
            rules.contains(
                "-A RUSTERNETES-SERVICES -d 10.96.0.5/32 -p tcp --dport 80 -m recent --name AFFINITY-109605-80-0 --rcheck --seconds 7200 --reap -j KUBE-SEP-109605-80-0"
            ),
            "missing --rcheck rule for endpoint 0:\n{}",
            rules
        );
        assert!(
            rules.contains("--name AFFINITY-109605-80-1 --rcheck --seconds 7200 --reap"),
            "missing --rcheck rule for endpoint 1:\n{}",
            rules
        );

        // Combined --set + DNAT inside SEP chain (K8s style)
        assert!(
            rules.contains(
                "-A KUBE-SEP-109605-80-0 -p tcp -m recent --name AFFINITY-109605-80-0 --set -j DNAT --to-destination 10.0.0.1:80"
            ),
            "missing combined set+DNAT for endpoint 0:\n{}",
            rules
        );
        assert!(
            rules.contains(
                "-A KUBE-SEP-109605-80-1 -p tcp -m recent --name AFFINITY-109605-80-1 --set -j DNAT --to-destination 10.0.0.2:80"
            ),
            "missing combined set+DNAT for endpoint 1:\n{}",
            rules
        );

        // Probability fallback for new connections: idx 0 gets 1/2, last has no prob.
        assert!(
            rules.contains("-m statistic --mode random --probability 0.5000000000"),
            "missing probability fallback rule:\n{}",
            rules
        );
    }

    #[tokio::test]
    async fn test_build_nat_rules_clusterip_affinity_single_endpoint() {
        // Single-endpoint ClientIP affinity must still produce a sticky path:
        // SEP chain + --rcheck rule. No probability match for the fallback.
        let mgr = IptablesManager::for_testing(true);
        let service = make_service(
            "solo",
            "default",
            "10.96.0.7",
            ServiceType::ClusterIP,
            vec![ServicePort {
                name: None,
                port: 8080,
                target_port: None,
                protocol: Some("TCP".to_string()),
                node_port: None,
                app_protocol: None,
            }],
            Some("ClientIP"),
            None,
        );
        let ep_map = make_endpointslice_map("default", "solo", &[("10.0.0.9", None, 8080)]);

        let rules = mgr.build_nat_rules(&[service], &ep_map).await;

        assert!(
            rules.contains(":KUBE-SEP-109607-8080-0 - [0:0]"),
            "single-endpoint SEP chain missing:\n{}",
            rules
        );
        assert!(
            rules.contains(
                "-A KUBE-SEP-109607-8080-0 -p tcp -m recent --name AFFINITY-109607-8080-0 --set -j DNAT --to-destination 10.0.0.9:8080"
            ),
            "single-endpoint set+DNAT missing:\n{}",
            rules
        );
        assert!(
            rules.contains("--rcheck --seconds 10800 --reap"),
            "default 10800 timeout missing:\n{}",
            rules
        );
        assert!(
            !rules.contains("-m statistic"),
            "single-endpoint must not produce probability match:\n{}",
            rules
        );
    }

    #[tokio::test]
    async fn test_build_nat_rules_nodeport_session_affinity() {
        // NodePort + ClientIP affinity must emit --rcheck rules in the
        // nodeports chain that reuse the per-service SEP chains.
        let mgr = IptablesManager::for_testing(true);
        let service = make_service(
            "np-svc",
            "default",
            "10.96.0.11",
            ServiceType::NodePort,
            vec![ServicePort {
                name: None,
                port: 80,
                target_port: None,
                protocol: Some("TCP".to_string()),
                node_port: Some(30080),
                app_protocol: None,
            }],
            Some("ClientIP"),
            Some(600),
        );
        let ep_map = make_endpointslice_map(
            "default",
            "np-svc",
            &[("10.0.0.3", None, 80), ("10.0.0.4", None, 80)],
        );

        let rules = mgr.build_nat_rules(&[service], &ep_map).await;

        assert!(
            rules.contains(
                "-A RUSTERNETES-NODEPORTS -p tcp --dport 30080 -m recent --name AFFINITY-1096011-80-0 --rcheck --seconds 600 --reap -j KUBE-SEP-1096011-80-0"
            ),
            "missing NodePort --rcheck rule for endpoint 0:\n{}",
            rules
        );
        assert!(
            rules.contains("--name AFFINITY-1096011-80-1 --rcheck --seconds 600 --reap"),
            "missing NodePort --rcheck rule for endpoint 1:\n{}",
            rules
        );
        assert!(
            rules.contains("-A RUSTERNETES-NODEPORTS -p tcp --dport 30080 -m statistic"),
            "missing NodePort probability fallback:\n{}",
            rules
        );
    }

    #[tokio::test]
    async fn test_build_nat_rules_affinity_off_no_recent() {
        // sessionAffinity=None must NOT emit any --rcheck / recent rules.
        let mgr = IptablesManager::for_testing(true);
        let service = make_service(
            "plain",
            "default",
            "10.96.0.13",
            ServiceType::ClusterIP,
            vec![ServicePort {
                name: None,
                port: 80,
                target_port: None,
                protocol: Some("TCP".to_string()),
                node_port: None,
                app_protocol: None,
            }],
            Some("None"),
            None,
        );
        let ep_map = make_endpointslice_map(
            "default",
            "plain",
            &[("10.0.0.5", None, 80), ("10.0.0.6", None, 80)],
        );

        let rules = mgr.build_nat_rules(&[service], &ep_map).await;
        assert!(
            !rules.contains("-m recent"),
            "no-affinity service should not emit recent rules:\n{}",
            rules
        );
        assert!(
            rules.contains("-j DNAT --to-destination 10.0.0.5:80"),
            "missing direct DNAT for endpoint 0:\n{}",
            rules
        );
        assert!(
            rules.contains("-j DNAT --to-destination 10.0.0.6:80"),
            "missing direct DNAT for endpoint 1:\n{}",
            rules
        );
    }

    #[tokio::test]
    async fn test_build_nat_rules_affinity_recent_unavailable_falls_back() {
        // Without xt_recent, ClientIP affinity must fall back to direct DNAT
        // so the service stays reachable (just not sticky).
        let mgr = IptablesManager::for_testing(false);
        let service = make_service(
            "no-recent",
            "default",
            "10.96.0.15",
            ServiceType::ClusterIP,
            vec![ServicePort {
                name: None,
                port: 80,
                target_port: None,
                protocol: Some("TCP".to_string()),
                node_port: None,
                app_protocol: None,
            }],
            Some("ClientIP"),
            None,
        );
        let ep_map = make_endpointslice_map("default", "no-recent", &[("10.0.0.7", None, 80)]);
        let rules = mgr.build_nat_rules(&[service], &ep_map).await;
        assert!(
            !rules.contains("-m recent"),
            "without xt_recent we must not emit recent rules:\n{}",
            rules
        );
        assert!(
            rules.contains("-j DNAT --to-destination 10.0.0.7:80"),
            "expected direct DNAT fallback:\n{}",
            rules
        );
    }
}
