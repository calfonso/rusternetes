//! Build/version info, populated by `build.rs`, for startup logging.
//!
//! Lets every binary log exactly which build it is — crate version, git commit
//! SHA (or `unknown` when built outside a checkout without an injected
//! `RUSTERNETES_GIT_SHA`), and build time. This is what tells you at a glance
//! whether a running cluster is on the commit you think it is.

/// Crate (workspace) semantic version, e.g. `0.1.0`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Short git commit SHA the binary was built from (suffixed `-dirty` for an
/// uncommitted working tree), or `unknown`.
pub const GIT_SHA: &str = env!("RUSTERNETES_BUILD_SHA");

/// UTC build time (RFC3339, or `epoch:<n>` / `unknown`).
pub const BUILD_TIME: &str = env!("RUSTERNETES_BUILD_TIME");

/// One-line version banner for startup logs, e.g.
/// `v0.1.0 (git a1b2c3d4e5f6, built 2026-06-16T14:00:00Z)`.
pub fn version_line() -> String {
    format!("v{VERSION} (git {GIT_SHA}, built {BUILD_TIME})")
}
