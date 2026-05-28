//! Linux network-namespace operations the netstack performs against
//! pod-owned netns paths.
//!
//! Today kubelet's CNI runtime creates one netns per pod at
//! `/var/run/netns/cni-{pod_name}` (see
//! `crates/kubelet/src/runtime.rs:782`) and hands the path to the CNI
//! plugin, which configures veth + IP + routes inside it. This module
//! is the netstack equivalent of "what the CNI plugin does":
//!
//! - [`move_tap_to_netns`] (slice A) — move a TAP device created in
//!   the rusternetes netns into the pod's netns so packets
//!   read/written by the container land on it.
//! - [`configure_pod_netns`] (this commit, **slice B**) — assign the
//!   pod IP + default route + bring `lo` up inside the netns.
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

use std::net::Ipv4Addr;
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
    /// reporting a protocol error. Boxed because `rtnetlink::Error`
    /// is large and would inflate every `Result<_, NetnsError>`.
    #[error("netlink op on TAP {tap_name:?}: {source}")]
    Netlink {
        tap_name: String,
        #[source]
        source: Box<rtnetlink::Error>,
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

    /// `setns(2)` failed — the calling thread couldn't be moved into
    /// the target netns. Usually means we don't have `CAP_SYS_ADMIN`
    /// in the netns's user namespace (a deployment-config error).
    #[error("setns into {netns_path:?} failed: {source}")]
    Setns {
        netns_path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The worker thread that ran the in-netns netlink ops panicked
    /// or could not be joined. Should be impossible unless something
    /// inside the closure unwinded — surface explicitly so the kubelet
    /// doesn't get silent "configure succeeded" lies.
    #[error("netns worker thread did not produce a result: {detail}")]
    ThreadJoin { detail: String },
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
            source: Box::new(e),
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
            source: Box::new(e),
        })?;

    debug!(
        ?tap_name,
        ?netns_path,
        ifindex,
        "netns: moved TAP into pod's network namespace"
    );
    Ok(())
}

/// Configure a pod's network namespace: assign `pod_ip/prefix` to
/// `tap_name`, set a default route via `gateway`, and bring `lo` up.
/// These are the same operations a typical CNI plugin performs
/// after creating the pod's interface; this is the netstack
/// equivalent for the case where `tap_name` is the TAP that
/// [`move_tap_to_netns`] just placed into `netns_path`.
///
/// ### Why a dedicated OS thread
///
/// `setns(2)` mutates the calling thread's namespace association.
/// Tokio runtime threads multiplex many tasks, so calling `setns`
/// from inside an async task would corrupt every other task running
/// on the same worker thread (and on subsequent reuses of the
/// worker). We sidestep this by spawning a fresh `std::thread` per
/// `configure_pod_netns` call, doing the setns + netlink work, and
/// joining. The cost is one OS thread per pod-create — cheap, since
/// pod creates are already heavyweight events on the order of
/// hundreds of ms.
///
/// ### Idempotency
///
/// The netlink ops here ARE idempotent — adding an address or route
/// that already exists returns `EEXIST` from the kernel, which
/// rtnetlink surfaces as an `Error::NetlinkError`. We squash that
/// specific case to `Ok(())` so kubelet retries during pod create
/// don't need to special-case "already configured".
///
/// ### Requires
///
/// - `CAP_NET_ADMIN` in the target netns
/// - `CAP_SYS_ADMIN` in the netns's user namespace (for `setns`)
pub async fn configure_pod_netns(
    netns_path: &Path,
    tap_name: &str,
    pod_ip: Ipv4Addr,
    prefix: u8,
    gateway: Ipv4Addr,
) -> Result<(), NetnsError> {
    if !netns_path.exists() {
        return Err(NetnsError::PathMissing {
            path: netns_path.to_path_buf(),
        });
    }

    let netns_path_owned = netns_path.to_path_buf();
    let tap_name_owned = tap_name.to_string();

    // Run the setns + netlink work on a dedicated OS thread so the
    // namespace change can't leak across tokio worker threads.
    let join_result = tokio::task::spawn_blocking(move || -> Result<(), NetnsError> {
        let netns_file =
            std::fs::File::open(&netns_path_owned).map_err(|e| NetnsError::OpenNs {
                path: netns_path_owned.clone(),
                source: e,
            })?;

        // Move the calling thread into the pod's netns. From here on
        // every netlink RPC issued by this thread targets the pod's
        // kernel namespaces.
        nix::sched::setns(&netns_file, nix::sched::CloneFlags::CLONE_NEWNET).map_err(|e| {
            NetnsError::Setns {
                netns_path: netns_path_owned.clone(),
                source: std::io::Error::from_raw_os_error(e as i32),
            }
        })?;

        // Build a single-threaded tokio runtime inside this thread to
        // drive rtnetlink's async API. The runtime + connection live
        // and die with this thread, so no setns leakage is possible.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| NetnsError::ThreadJoin {
                detail: format!("build in-netns tokio runtime: {e}"),
            })?;

        rt.block_on(async move {
            use futures::TryStreamExt;
            use rtnetlink::Error as RtError;

            let (connection, handle, _) =
                rtnetlink::new_connection().map_err(|e| NetnsError::OpenNs {
                    path: PathBuf::from("/proc/net/netlink"),
                    source: e,
                })?;
            tokio::spawn(connection);

            // 1. Resolve TAP ifindex by name.
            let mut links = handle
                .link()
                .get()
                .match_name(tap_name_owned.clone())
                .execute();
            let link = links
                .try_next()
                .await
                .map_err(|e| NetnsError::Netlink {
                    tap_name: tap_name_owned.clone(),
                    source: Box::new(e),
                })?
                .ok_or_else(|| NetnsError::TapNotFound {
                    tap_name: tap_name_owned.clone(),
                })?;
            let tap_idx = link.header.index;

            // 2. ip addr add <pod_ip>/<prefix> dev <tap>
            //    Squash EEXIST → Ok so retries don't fail.
            match handle
                .address()
                .add(tap_idx, std::net::IpAddr::V4(pod_ip), prefix)
                .execute()
                .await
            {
                Ok(()) => {}
                Err(RtError::NetlinkError(e)) if e.raw_code() == -libc::EEXIST => {
                    debug!(
                        ?pod_ip,
                        prefix,
                        ?tap_name_owned,
                        "netns: pod IP already assigned (idempotent)"
                    );
                }
                Err(e) => {
                    return Err(NetnsError::Netlink {
                        tap_name: tap_name_owned.clone(),
                        source: Box::new(e),
                    });
                }
            }

            // 3. ip link set <tap> up — TunBuilder::up() sets it before
            //    the move, but the kernel sometimes drops the UP flag
            //    on netns transition. Re-asserting is cheap and idempotent.
            handle
                .link()
                .set(tap_idx)
                .up()
                .execute()
                .await
                .map_err(|e| NetnsError::Netlink {
                    tap_name: tap_name_owned.clone(),
                    source: Box::new(e),
                })?;

            // 4. ip route add default via <gateway> dev <tap>
            //    Squash EEXIST → Ok.
            match handle
                .route()
                .add()
                .v4()
                .gateway(gateway)
                .output_interface(tap_idx)
                .execute()
                .await
            {
                Ok(()) => {}
                Err(RtError::NetlinkError(e)) if e.raw_code() == -libc::EEXIST => {
                    debug!(
                        ?gateway,
                        ?tap_name_owned,
                        "netns: default route already present (idempotent)"
                    );
                }
                Err(e) => {
                    return Err(NetnsError::Netlink {
                        tap_name: tap_name_owned.clone(),
                        source: Box::new(e),
                    });
                }
            }

            // 5. ip link set lo up — pods expect 127.0.0.1 to work.
            let mut lo_links = handle.link().get().match_name("lo".to_string()).execute();
            if let Some(lo) = lo_links.try_next().await.map_err(|e| NetnsError::Netlink {
                tap_name: "lo".to_string(),
                source: Box::new(e),
            })? {
                handle
                    .link()
                    .set(lo.header.index)
                    .up()
                    .execute()
                    .await
                    .map_err(|e| NetnsError::Netlink {
                        tap_name: "lo".to_string(),
                        source: Box::new(e),
                    })?;
            }

            Ok(())
        })
    })
    .await;

    let inner = join_result.map_err(|e| NetnsError::ThreadJoin {
        detail: e.to_string(),
    })?;
    inner?;

    debug!(
        ?tap_name,
        ?netns_path,
        ?pod_ip,
        prefix,
        ?gateway,
        "netns: configured pod IP + default route"
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

        let setns = NetnsError::Setns {
            netns_path: PathBuf::from("/var/run/netns/cni-foo"),
            source: std::io::Error::from_raw_os_error(libc::EPERM),
        };
        assert!(setns.to_string().contains("/var/run/netns/cni-foo"));

        let tj = NetnsError::ThreadJoin {
            detail: "worker panicked".to_string(),
        };
        assert!(tj.to_string().contains("worker panicked"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn configure_pod_netns_returns_path_missing_for_nonexistent_path() {
        let result = configure_pod_netns(
            Path::new("/var/run/netns/this-does-not-exist"),
            "rust1234",
            Ipv4Addr::new(10, 244, 0, 2),
            16,
            Ipv4Addr::new(10, 96, 0, 1),
        )
        .await;
        match result {
            Err(NetnsError::PathMissing { path }) => {
                assert_eq!(path, PathBuf::from("/var/run/netns/this-does-not-exist"));
            }
            other => panic!("expected PathMissing, got {other:?}"),
        }
    }
}
