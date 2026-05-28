//! Linux network-namespace operations the netstack performs against
//! pod-owned netns paths.
//!
//! Today kubelet's CNI runtime creates one netns per pod at
//! `/var/run/netns/cni-{pod_name}` (see
//! `crates/kubelet/src/runtime.rs:782`) and hands the path to the CNI
//! plugin, which configures veth + IP + routes inside it. This module
//! is the netstack equivalent of "what the CNI plugin does":
//!
//! - [`move_tap_to_netns`] (this commit, **slice A**) — move a TAP
//!   device created in the rusternetes netns into the pod's netns
//!   so packets read/written by the container land on it.
//! - `configure_pod_netns` (slice B, task #43) — assign the pod IP +
//!   default route + bring `lo` up inside the netns.
//! - Slice C (task #44) wires kubelet's `start_pod` to call both, in
//!   sequence, after the pause container is up.
//!
//! ### Why a separate module from [`crate::iface`]
//!
//! `iface` is about smoltcp ↔ TAP plumbing inside the rusternetes
//! process — pure userspace stack logic. This module is about Linux
//! kernel networking: netlink RPCs, namespace fd handling, `setns`
//! semantics. The two have nothing in common at the abstraction
//! layer; keeping them separate makes the cap requirements and the
//! "what's running in which namespace" reasoning clearer.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::debug;

/// Failure modes for the netns operations in this module.
///
/// Callers above the netstack (kubelet's pod-create path) typically
/// log + fail the pod create on any of these. Unlike `OpenTapError`,
/// none of these are operator-misconfiguration errors that warrant a
/// startup preflight — they're per-pod failures during steady-state
/// operation.
#[derive(Debug, Error)]
pub enum NetnsError {
    /// The netns path doesn't exist on the filesystem — kubelet
    /// either skipped `ip netns add`, the path was deleted out from
    /// under us, or we're being called with the wrong path.
    #[error("netns path {path:?} does not exist (kubelet did not `ip netns add` it?)")]
    PathMissing { path: PathBuf },

    /// Could not open the netns path as a file (to get an fd for
    /// `setns`/`IFLA_NET_NS_FD`). Usually a permissions issue.
    #[error("open netns {path:?}: {source}")]
    OpenNs {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Netlink RPC failed — could be the kernel rejecting our
    /// message, the connection being torn down, or rtnetlink itself
    /// reporting a protocol error.
    #[error("netlink op on TAP {tap_name:?}: {source}")]
    Netlink {
        tap_name: String,
        #[source]
        source: rtnetlink::Error,
    },

    /// The TAP we were asked to move doesn't exist in the calling
    /// process's netns. Most commonly because the move already
    /// succeeded (this method is not idempotent — re-calling it after
    /// success returns this error).
    #[error(
        "TAP {tap_name:?} not found in the calling netns (already moved, \
         or never created?)"
    )]
    TapNotFound { tap_name: String },
}

/// Move a TAP device named `tap_name` from the calling process's
/// netns into the netns at `netns_path` (typically
/// `/var/run/netns/cni-{pod_name}`).
///
/// The `tokio_tun::Tun` file descriptor the caller holds is
/// **unaffected** by this operation — TUN/TAP fds are detached from
/// the device's netns visibility, so the rusternetes process can keep
/// reading/writing packets while the device itself lives in (and is
/// only visible from inside) the pod's netns.
///
/// ### Requires
///
/// - `CAP_NET_ADMIN` in the calling process's netns (already verified
///   by [`crate::capabilities::require_cap_net_admin`] at startup).
/// - Write access to the target netns — for `/var/run/netns/*` paths
///   this is governed by the kernel's permission check on the netns
///   inode; the rusternetes container must run with sufficient
///   privilege (in practice: same `CAP_NET_ADMIN` covers it for
///   shared-host netns).
///
/// ### Not idempotent
///
/// Calling twice with the same TAP returns `TapNotFound` on the
/// second call — the TAP is gone from the source netns by then.
/// Kubelet should call once per pod-create and treat the result as
/// load-bearing.
pub async fn move_tap_to_netns(tap_name: &str, netns_path: &Path) -> Result<(), NetnsError> {
    use futures::TryStreamExt;

    if !netns_path.exists() {
        return Err(NetnsError::PathMissing {
            path: netns_path.to_path_buf(),
        });
    }

    // Open the netns as a file just to get an fd; the
    // `IFLA_NET_NS_FD` netlink attribute is what actually moves the
    // device.
    let netns_file = std::fs::File::open(netns_path).map_err(|e| NetnsError::OpenNs {
        path: netns_path.to_path_buf(),
        source: e,
    })?;

    let (connection, handle, _) = rtnetlink::new_connection().map_err(|e| NetnsError::OpenNs {
        path: PathBuf::from("/proc/net/netlink"),
        source: e,
    })?;
    tokio::spawn(connection);

    // Resolve TAP name → ifindex via RTM_GETLINK + IFLA_IFNAME filter.
    let mut links = handle
        .link()
        .get()
        .match_name(tap_name.to_string())
        .execute();
    let link = links
        .try_next()
        .await
        .map_err(|e| NetnsError::Netlink {
            tap_name: tap_name.to_string(),
            source: e,
        })?
        .ok_or_else(|| NetnsError::TapNotFound {
            tap_name: tap_name.to_string(),
        })?;
    let ifindex = link.header.index;

    // RTM_SETLINK with IFLA_NET_NS_FD — the actual move.
    handle
        .link()
        .set(ifindex)
        .setns_by_fd(netns_file.as_raw_fd())
        .execute()
        .await
        .map_err(|e| NetnsError::Netlink {
            tap_name: tap_name.to_string(),
            source: e,
        })?;

    debug!(
        ?tap_name,
        ?netns_path,
        ifindex,
        "netns: moved TAP into pod's network namespace"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn move_tap_to_netns_returns_path_missing_for_nonexistent_path() {
        // Doesn't need any caps — fails the path check before going
        // anywhere near netlink.
        let result = move_tap_to_netns(
            "nonexistent-tap",
            Path::new("/var/run/netns/this-does-not-exist"),
        )
        .await;
        match result {
            Err(NetnsError::PathMissing { path }) => {
                assert_eq!(path, PathBuf::from("/var/run/netns/this-does-not-exist"));
            }
            other => panic!("expected PathMissing, got {other:?}"),
        }
    }

    #[test]
    fn error_messages_name_the_failing_resource() {
        // Kubelet operators read these directly from logs — confirm
        // each variant names the thing that's wrong.
        let pm = NetnsError::PathMissing {
            path: PathBuf::from("/var/run/netns/cni-foo"),
        };
        assert!(pm.to_string().contains("/var/run/netns/cni-foo"));

        let tnf = NetnsError::TapNotFound {
            tap_name: "rust1234".to_string(),
        };
        assert!(tnf.to_string().contains("rust1234"));
        assert!(tnf.to_string().contains("already moved"));
    }
}
