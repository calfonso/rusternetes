//! HTTP handlers for `/exec` and `/attach` — upgrade-proxy to the containerd-rs
//! CRI streaming server (upstream-faithful kubelet streaming).
//!
//! Upstream reference: `pkg/kubelet/server/server.go` routes
//! `/exec/{podNamespace}/{podID}/{containerName}` (+ `/{uid}/` variant).
//! `getExec` calls CRI `Exec` (returns a URL on the runtime streaming server)
//! then `proxyStream` = upgrade-aware proxy to that URL.
//!
//! This module replicates the same flow: resolve the container id from the CRI
//! `ListContainers` labels, call CRI `Exec`/`Attach` to get the runtime stream
//! URL, `rewrite_stream_url` it to the node-local streaming server, then
//! `proxy_upgrade(url, req)`.

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use rusternetes_common::resources::Pod;
use rusternetes_common::types::ObjectMeta;
use tracing::{info, warn};

/// Query parameters for exec/attach requests, mirroring upstream
/// `remotecommandserver.Options`.
#[derive(Debug, Clone, Default)]
pub struct ExecParams {
    /// Command to execute (repeatable: `command=ls&command=-la`).
    pub command: Vec<String>,
    /// Connect stdin.
    pub stdin: bool,
    /// Stream stdout.
    pub stdout: bool,
    /// Stream stderr.
    pub stderr: bool,
    /// Allocate a pseudo-TTY.
    pub tty: bool,
}

impl ExecParams {
    /// Parse exec/attach query params from a raw query string.
    ///
    /// Supports repeated `command=` keys (upstream passes each argv element as a
    /// separate `command=` value). Boolean params (`stdin`, `stdout`, `stderr`,
    /// `tty`) accept `"true"` / `"1"` (case-insensitive); anything else is false.
    pub fn from_query(query: &str) -> Self {
        let mut command = Vec::new();
        let mut stdin = false;
        let mut stdout = false;
        let mut stderr = false;
        let mut tty = false;

        for pair in query.split('&') {
            let (key, value) = if let Some(pos) = pair.find('=') {
                (&pair[..pos], &pair[pos + 1..])
            } else {
                (pair, "")
            };
            // Percent-decode simple `+` and `%XX` sequences for the value.
            let value = decode_simple(value);
            match key {
                "command" => {
                    if !value.is_empty() {
                        command.push(value);
                    }
                }
                "stdin" => stdin = is_true(&value),
                "stdout" => stdout = is_true(&value),
                "stderr" => stderr = is_true(&value),
                "tty" => tty = is_true(&value),
                _ => {}
            }
        }
        ExecParams {
            command,
            stdin,
            stdout,
            stderr,
            tty,
        }
    }
}

fn is_true(s: &str) -> bool {
    let l = s.to_ascii_lowercase();
    l == "true" || l == "1"
}

/// Minimal percent-decode: replace `+` with space and `%XX` hex sequences.
fn decode_simple(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b as char);
                    i += 3;
                    continue;
                }
            }
            out.push('%');
            i += 1;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Path parameters for the exec/attach routes (3-segment variant: no uid).
#[derive(serde::Deserialize, Debug)]
pub struct ExecPath3 {
    pub namespace: String,
    pub pod: String,
    pub container: String,
}

/// Path parameters for the exec/attach routes (4-segment variant: with uid).
#[derive(serde::Deserialize, Debug)]
pub struct ExecPath4 {
    pub namespace: String,
    pub pod: String,
    pub uid: String,
    pub container: String,
}

/// Build a minimal [`Pod`] value sufficient for [`resolve_container_id`].
fn pod_for_resolve(namespace: &str, pod_name: &str, uid: &str) -> Pod {
    Pod {
        type_meta: rusternetes_common::types::TypeMeta {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        metadata: ObjectMeta {
            name: pod_name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uid.to_string(),
            ..Default::default()
        },
        spec: None,
        status: None,
    }
}

/// Resolve the container id and build the upgrade target URI, then proxy.
async fn exec_proxy(
    namespace: &str,
    pod_name: &str,
    uid: &str,
    container: &str,
    params: &ExecParams,
    req: axum::extract::Request,
) -> Response {
    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("exec_proxy: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(namespace, pod_name, uid);
    let container_id =
        match rusternetes_cri::stream::resolve_container_id(&mut cri, &pod, container).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn!(
                    "exec_proxy: container not found: ns={namespace} pod={pod_name} \
                     container={container}"
                );
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "container {container:?} not found in pod {pod_name:?} \
                         (namespace {namespace:?})"
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("exec_proxy: resolve_container_id failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    // Call CRI Exec → get the runtime stream URL.
    let stream_url = match cri
        .exec(rusternetes_cri::v1::ExecRequest {
            container_id: container_id.clone(),
            cmd: params.command.clone(),
            tty: params.tty,
            stdin: params.stdin,
            stdout: true,
            stderr: !params.tty,
        })
        .await
    {
        Ok(url) => url,
        Err(e) => {
            warn!("exec_proxy: CRI Exec failed for {container_id}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Rewrite the stream URL to the node-local streaming server port.
    let (host, port) = rusternetes_cri::stream::stream_target();
    let rewritten = match rusternetes_cri::stream::rewrite_stream_url(&stream_url, &host, port) {
        Ok(u) => u,
        Err(e) => {
            warn!("exec_proxy: rewrite_stream_url failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    info!(
        "exec_proxy: ns={namespace} pod={pod_name} container={container} \
         container_id={container_id} target={rewritten}"
    );

    let target: http::Uri = match rewritten.parse() {
        Ok(u) => u,
        Err(e) => {
            warn!("exec_proxy: failed to parse rewritten URI {rewritten:?}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    rusternetes_streamproxy::proxy_upgrade(target, req).await
}

/// Resolve the container id and proxy an attach request to the CRI stream.
async fn attach_proxy(
    namespace: &str,
    pod_name: &str,
    uid: &str,
    container: &str,
    params: &ExecParams,
    req: axum::extract::Request,
) -> Response {
    let mut cri = match rusternetes_cri::stream::connect().await {
        Ok(c) => c,
        Err(e) => {
            warn!("attach_proxy: CRI connect failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let pod = pod_for_resolve(namespace, pod_name, uid);
    let container_id =
        match rusternetes_cri::stream::resolve_container_id(&mut cri, &pod, container).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn!(
                    "attach_proxy: container not found: ns={namespace} pod={pod_name} \
                     container={container}"
                );
                return (
                    StatusCode::NOT_FOUND,
                    format!(
                        "container {container:?} not found in pod {pod_name:?} \
                         (namespace {namespace:?})"
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                warn!("attach_proxy: resolve_container_id failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    // Call CRI Attach → get the runtime stream URL.
    let stream_url = match cri
        .attach(rusternetes_cri::v1::AttachRequest {
            container_id: container_id.clone(),
            stdin: params.stdin,
            tty: params.tty,
            stdout: true,
            stderr: !params.tty,
        })
        .await
    {
        Ok(url) => url,
        Err(e) => {
            warn!("attach_proxy: CRI Attach failed for {container_id}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let (host, port) = rusternetes_cri::stream::stream_target();
    let rewritten = match rusternetes_cri::stream::rewrite_stream_url(&stream_url, &host, port) {
        Ok(u) => u,
        Err(e) => {
            warn!("attach_proxy: rewrite_stream_url failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    info!(
        "attach_proxy: ns={namespace} pod={pod_name} container={container} \
         container_id={container_id} target={rewritten}"
    );

    let target: http::Uri = match rewritten.parse() {
        Ok(u) => u,
        Err(e) => {
            warn!("attach_proxy: failed to parse rewritten URI {rewritten:?}: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    rusternetes_streamproxy::proxy_upgrade(target, req).await
}

// ---------------------------------------------------------------------------
// Axum route handlers
// ---------------------------------------------------------------------------

/// `POST /exec/:namespace/:pod/:container`
pub async fn handle_exec(Path(p): Path<ExecPath3>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    exec_proxy(&p.namespace, &p.pod, "", &p.container, &params, req).await
}

/// `POST /exec/:namespace/:pod/:uid/:container`
pub async fn handle_exec_uid(Path(p): Path<ExecPath4>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    exec_proxy(&p.namespace, &p.pod, &p.uid, &p.container, &params, req).await
}

/// `POST /attach/:namespace/:pod/:container`
pub async fn handle_attach(Path(p): Path<ExecPath3>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    attach_proxy(&p.namespace, &p.pod, "", &p.container, &params, req).await
}

/// `POST /attach/:namespace/:pod/:uid/:container`
pub async fn handle_attach_uid(Path(p): Path<ExecPath4>, req: axum::extract::Request) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    let params = ExecParams::from_query(&query);
    attach_proxy(&p.namespace, &p.pod, &p.uid, &p.container, &params, req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_params_parse_command_and_tty() {
        let q = "command=ls&command=-la&tty=true&stdin=false&stdout=true&stderr=true";
        let p = ExecParams::from_query(q);
        assert_eq!(p.command, vec!["ls", "-la"]);
        assert!(p.tty);
        assert!(!p.stdin && p.stdout && p.stderr);
    }

    #[test]
    fn exec_params_empty_query() {
        let p = ExecParams::from_query("");
        assert!(p.command.is_empty());
        assert!(!p.stdin && !p.stdout && !p.stderr && !p.tty);
    }

    #[test]
    fn exec_params_single_command() {
        let p = ExecParams::from_query("command=echo&stdin=1&stdout=1");
        assert_eq!(p.command, vec!["echo"]);
        assert!(p.stdin);
        assert!(p.stdout);
        assert!(!p.stderr);
    }

    #[test]
    fn exec_params_tty_suppresses_stderr() {
        // When tty=true, the caller sets stdin+stdout; stderr is irrelevant
        // (CRI forbids tty && stderr) but we parse what we get.
        let p = ExecParams::from_query("command=bash&tty=true&stdin=true&stdout=true&stderr=false");
        assert!(p.tty);
        assert!(p.stdin);
        assert!(p.stdout);
        assert!(!p.stderr);
    }
}
