//! Shared test fixtures for the rusternetes workspace.
//!
//! This crate consolidates fixtures that test files across crates were
//! duplicating, and provides the substrate for porting upstream Kubernetes Go
//! unit tests into idiomatic Rust (see `docs/porting-upstream-tests.md`):
//!
//! - [`builders`] — fluent, JSON-backed builders for the common resource types
//!   (`pod()`, `service()`, `node()`, `endpoint_slice()`). JSON-backed because
//!   the resource structs deserialize but don't all derive `Default`, and JSON
//!   gives precise control over the (often deliberately invalid) inputs that
//!   validation tests need.
//! - [`harness`] (feature `apiserver-harness`) — an in-process api-server bound
//!   to `MemoryStorage`, driven via `tower::oneshot`, extracted from the
//!   pattern in `crates/api-server/tests/`.
//!
//! Future ports add Rust analogs of the Go fakes (`FakeIPTables`,
//! `FakeRuntimeService`) here behind their own features.

pub mod builders;

#[cfg(feature = "apiserver-harness")]
pub mod harness;

pub use builders::{
    endpoint_slice, node, node_with_resources, pod, service, PodBuilder, ResourceBuilder,
};
