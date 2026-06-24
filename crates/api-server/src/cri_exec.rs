//! CRI-backed exec + log helpers shared by the pod exec/logs subresource
//! handlers (HTTP one-shot), the WebSocket exec path, and the SPDY exec path.
//!
//! The api-server used to talk to the container runtime directly over the
//! Docker API for exec and pod logs. The kubelet has since moved to a CRI v1
//! backend, so the api-server speaks the same Container Runtime Interface here:
//!
//! * exec runs via the CRI `ExecSync` RPC (one-shot: collect stdout/stderr +
//!   exit code), matching the kubelet's own `/exec` endpoint.
//! * pod logs are read from the CRI container **log file** (the path the
//!   runtime reports in `ContainerStatus.log_path`) and the CRI log line format
//!   is parsed into the raw message bytes the API returns.
//!
//! The CRI runtime endpoint comes from `CONTAINER_RUNTIME_ENDPOINT`
//! (default `unix:///run/containerd/containerd.sock`), the same env var the
//! kubelet uses.
//!
//! # Note
//! The helpers below were moved to `rusternetes_cri::stream` so both the
//! api-server and the kubelet share one implementation. This module re-exports
//! them to keep existing call-sites in the api-server unchanged until Tasks 6–7
//! migrate them to the kubelet path.

#[allow(unused_imports)]
pub use rusternetes_cri::stream::{
    connect, exec_sync, labels, open_attach_stream, open_exec_stream, parse_cri_log_line,
    read_log_file, resolve_container_id, resolve_log_path, rewrite_stream_url, runtime_endpoint,
    stream_target, CriLogLine, CriStream, LogReadOptions,
};
