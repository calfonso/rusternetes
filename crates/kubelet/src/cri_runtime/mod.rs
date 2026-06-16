//! CRI-backed kubelet runtime (in progress).
//!
//! This module is the replacement for the bollard-based [`crate::runtime`]: it
//! drives a CRI v1 runtime (containerd → Youki) via the `rusternetes-cri`
//! client instead of the Docker API. The migration lands incrementally —
//! [`translate`] (Pod → CRI config mapping) is the foundation; the
//! `ContainerRuntime`-equivalent lifecycle methods build on it in later steps.

pub mod runtime;
pub mod translate;

pub use runtime::{CriContainerRuntime, CriRuntimeError};
