//! Startup capability preflight for the netstack process.
//!
//! The netstack opens TAP devices via `tokio_tun::Tun::new()` — that
//! syscall path requires `CAP_NET_ADMIN`. The intended deployment
//! shape is a Docker / Kubernetes container that runs as a non-root
//! user with **every** capability dropped except `NET_ADMIN`, added
//! back explicitly. If somebody forgets the `--cap-add` (or copies a
//! manifest from a different module's PodSecurityContext), the first
//! `Tun::new()` returns `EPERM` and the operator gets a cryptic
//! "Operation not permitted" deep inside the runtime path.
//!
//! [`require_cap_net_admin`] short-circuits that: call it at the
//! entry point of whatever process is going to own the netstack
//! runtime, before the first TAP open. On failure the caller gets a
//! typed error whose `Display` impl spells out exactly which
//! `--cap-add` / `securityContext` line to add for each common
//! runtime, so the fix is one config-tweak away from a misconfigured
//! deployment.
//!
//! ### Why the *effective* set, not bounding / permitted
//!
//! Linux capabilities live in five sets: ambient, bounding, effective,
//! permitted, inheritable. Container runtimes occasionally leave a
//! capability *permitted* / *bounded* without raising it into the
//! *effective* set — for instance, when a binary runs under a wrapper
//! that drops effective caps and expects the program to raise them
//! itself. The kernel checks the **effective** set for syscalls like
//! the TUN/TAP ioctl, so that's what we have to verify here.
//!
//! ### Rootless future
//!
//! When the spike's rootless-mode plan lands (each netstack instance
//! lives inside `unshare -U -n`), the netstack process gets
//! `CAP_NET_ADMIN` **inside the user namespace** while owning none on
//! the host. `caps::read(None, ...)` queries the calling thread's
//! current namespace, which is the right thing — but the *call site*
//! has to move into the namespaced child, not stay at the
//! pre-`unshare` `main()`. The function below has no hidden state; it
//! works correctly wherever you put it.

use caps::{CapSet, Capability};
use std::collections::HashSet;
use thiserror::Error;

/// Failure modes for the startup capability preflight.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CapabilityError {
    /// The process is not authorised to create TAP devices because
    /// `CAP_NET_ADMIN` is missing from its **effective** capability
    /// set. The `Display` impl includes runtime-specific remediation.
    #[error(
        "CAP_NET_ADMIN missing from the effective capability set — the netstack \
         cannot create TAP devices. Add it to the container's capability set:\n\
         \n\
         \tDocker:     --cap-add NET_ADMIN\n\
         \tPodman:     --cap-add net_admin\n\
         \tKubernetes: spec.containers[].securityContext.capabilities.add: [\"NET_ADMIN\"]\n\
         \tdocker-compose: cap_add: [\"NET_ADMIN\"]\n\
         \n\
         Run the netstack as a non-root user with all other capabilities dropped \
         (--cap-drop=ALL) for least-privilege; CAP_NET_ADMIN is the *only* one it needs."
    )]
    MissingCapNetAdmin,

    /// Querying the process capability set failed at the syscall level
    /// (rare — usually only on non-Linux platforms or extremely
    /// locked-down sandboxes). Bubbled up so callers can decide whether
    /// to treat it as fatal or fall through to a "best-effort" TAP open.
    #[error("failed to query process capability set: {0}")]
    QueryFailed(String),
}

/// Verify the calling thread has `CAP_NET_ADMIN` in its **effective**
/// capability set. Returns `Ok(())` if present.
///
/// Call this once at the entry point of the process / task that owns
/// the netstack runtime, before opening the first TAP. The error type's
/// `Display` impl gives the operator a copy-pasteable fix for every
/// common container runtime.
///
/// This is a single `capget(2)` syscall — feel free to call it at any
/// boot path, it's free. (Don't *re-* check inside hot loops; the
/// effective set can't change without an explicit `capset(2)` call from
/// the same thread, which the netstack doesn't make.)
pub fn require_cap_net_admin() -> Result<(), CapabilityError> {
    let effective = caps::read(None, CapSet::Effective)
        .map_err(|e| CapabilityError::QueryFailed(e.to_string()))?;
    check_cap_net_admin(&effective)
}

/// Pure check separated from the syscall so unit tests can drive every
/// branch without depending on the test runner's actual capability set.
fn check_cap_net_admin(effective: &HashSet<Capability>) -> Result<(), CapabilityError> {
    if effective.contains(&Capability::CAP_NET_ADMIN) {
        Ok(())
    } else {
        Err(CapabilityError::MissingCapNetAdmin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_passes_when_cap_net_admin_is_in_effective_set() {
        let mut effective = HashSet::new();
        effective.insert(Capability::CAP_NET_ADMIN);
        assert_eq!(check_cap_net_admin(&effective), Ok(()));
    }

    #[test]
    fn check_passes_when_extra_caps_are_present_alongside_net_admin() {
        let mut effective = HashSet::new();
        effective.insert(Capability::CAP_NET_ADMIN);
        effective.insert(Capability::CAP_NET_BIND_SERVICE);
        effective.insert(Capability::CAP_SYS_ADMIN);
        assert_eq!(check_cap_net_admin(&effective), Ok(()));
    }

    #[test]
    fn check_fails_on_empty_capability_set() {
        let effective = HashSet::new();
        assert_eq!(
            check_cap_net_admin(&effective),
            Err(CapabilityError::MissingCapNetAdmin)
        );
    }

    #[test]
    fn check_fails_when_only_unrelated_caps_are_present() {
        // The most common misconfiguration: someone gave the container
        // CAP_NET_BIND_SERVICE thinking it was the network cap, missed
        // NET_ADMIN. Make sure we don't accidentally pass on related caps.
        let mut effective = HashSet::new();
        effective.insert(Capability::CAP_NET_BIND_SERVICE);
        effective.insert(Capability::CAP_NET_RAW);
        assert_eq!(
            check_cap_net_admin(&effective),
            Err(CapabilityError::MissingCapNetAdmin)
        );
    }

    #[test]
    fn missing_cap_error_message_names_every_supported_runtime() {
        // The whole point of this check is a deployable error message.
        // If a future refactor drops one of these runtime hints, surface
        // it here rather than discovering it in production.
        let msg = CapabilityError::MissingCapNetAdmin.to_string();
        assert!(msg.contains("CAP_NET_ADMIN"), "names the missing cap");
        assert!(msg.contains("--cap-add NET_ADMIN"), "Docker hint");
        assert!(msg.contains("--cap-add net_admin"), "podman hint");
        assert!(msg.contains("securityContext"), "Kubernetes hint");
        assert!(msg.contains("cap_add"), "docker-compose hint");
        assert!(
            msg.contains("--cap-drop=ALL") || msg.contains("least-privilege"),
            "guidance to keep the rest of the cap set empty"
        );
    }

    #[test]
    fn require_cap_net_admin_returns_a_typed_result() {
        // Smoke test: the function compiles, runs, and returns *some*
        // Result. Whether it's Ok or Err depends on how the test runner
        // was invoked — both outcomes are valid. We assert only that we
        // got a definite Linux capability outcome (not the QueryFailed
        // arm, which would indicate a broken syscall binding on this
        // platform).
        let result = require_cap_net_admin();
        assert!(
            !matches!(result, Err(CapabilityError::QueryFailed(_))),
            "capget syscall must succeed on this platform; got {result:?}"
        );
    }
}
