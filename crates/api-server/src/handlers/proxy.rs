/// HTTP proxy handlers for nodes, services, and pods
///
/// These handlers proxy HTTP requests to the target resource for debugging and monitoring.
///
/// Kubernetes proxy subresources:
/// - `/api/v1/nodes/{name}/proxy/{path}` - Proxy to node
/// - `/api/v1/namespaces/{ns}/services/{name}/proxy/{path}` - Proxy to service
/// - `/api/v1/namespaces/{ns}/pods/{name}/proxy/{path}` - Proxy to pod
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Body,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::Response,
    Extension,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    Result,
};
use rusternetes_storage::Storage;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Map an HTTP method to the Kubernetes authorization verb used for proxy
/// subresources.
///
/// K8s ref: staging/src/k8s.io/apiserver/pkg/endpoints/request/requestinfo.go
/// (RequestInfoFactory.NewRequestInfo). For proxy subresources the verb is
/// derived from the HTTP method just like any other resource verb, so that
/// RBAC rules can scope `get`/`create`/`update`/`patch`/`delete` separately
/// even though they all target `*/proxy`.
fn http_method_to_verb(method: &Method) -> &'static str {
    match *method {
        Method::POST => "create",
        Method::PUT => "update",
        Method::PATCH => "patch",
        Method::DELETE => "delete",
        // GET, HEAD, OPTIONS, and any other method default to "get".
        _ => "get",
    }
}

/// Extract the portion of `uri.path()` that follows the proxy marker for the
/// given resource. The returned slice preserves a trailing slash so that we
/// can forward `/foo/` and `/foo` distinctly to the backend (K8s does the
/// same). Returns an empty slice when no path component follows `proxy`.
///
/// `resource` is the resource segment in the URL, e.g. "pods" or "services".
fn extract_proxy_suffix<'a>(uri_path: &'a str, resource: &str) -> &'a str {
    // The expected layout is:
    //   /api/v1/namespaces/<ns>/<resource>/<name>/proxy[/...]
    // Locate the proxy marker and return whatever follows it (including any
    // trailing slash) so downstream code can preserve verbatim semantics.
    let marker = format!("/{}/", resource);
    if let Some(res_idx) = uri_path.find(&marker) {
        // Skip past "/<resource>/" then past the name segment.
        let after_resource = &uri_path[res_idx + marker.len()..];
        if let Some(name_end) = after_resource.find('/') {
            let after_name = &after_resource[name_end + 1..];
            // after_name should start with "proxy" — strip it.
            if let Some(rest) = after_name.strip_prefix("proxy") {
                // rest is either "" or "/..."
                return rest.strip_prefix('/').unwrap_or(rest);
            }
        }
    }
    ""
}

/// Parse a resource identifier in Kubernetes proxy format.
///
/// Matches the behavior of `k8s.io/apimachinery/pkg/util/net.SplitSchemeNamePort`:
/// - `"name"` -> (scheme="", name="name", port="")
/// - `"name:port"` -> (scheme="", name="name", port="port")
/// - `"scheme:name:port"` -> (scheme="scheme", name="name", port="port")
///
/// Valid schemes are "", "http", and "https".
fn split_scheme_name_port(id: &str) -> Option<(&str, &str, &str)> {
    let parts: Vec<&str> = id.splitn(4, ':').collect();
    match parts.len() {
        1 => {
            // "name"
            if parts[0].is_empty() {
                None
            } else {
                Some(("", parts[0], ""))
            }
        }
        2 => {
            // "name:port"
            if parts[0].is_empty() {
                None
            } else {
                Some(("", parts[0], parts[1]))
            }
        }
        3 => {
            // "scheme:name:port"
            let scheme = parts[0];
            if scheme != "http" && scheme != "https" {
                return None;
            }
            if parts[1].is_empty() {
                return None;
            }
            Some((scheme, parts[1], parts[2]))
        }
        _ => None,
    }
}

/// Proxy HTTP requests to a node
///
/// Proxies requests to the kubelet API on the specified node for debugging and metrics.
pub async fn proxy_node(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((node_name, _path)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    // Use the original URI to preserve trailing slashes and exact path bytes.
    let suffix = extract_proxy_suffix(original_uri.path(), "nodes");
    info!("Proxying request to node: {}, path: /{}", node_name, suffix);

    // Parse node id — Kubernetes proxy subresource format:
    //   "name", "name:port", or "scheme:name:port"
    // K8s ref: pkg/registry/core/node/strategy.go::ResourceLocation calls
    // `utilnet.SplitSchemeNamePort(id)` and uses `name` for the storage
    // lookup, `port` as the kubelet-endpoint override. The upstream e2e
    // framework constructs URLs of the form `<node>:10250` so the raw
    // path parameter from axum (`node_name`) is the *id*, not the name.
    let (scheme, actual_node_name, port_str) =
        split_scheme_name_port(&node_name).unwrap_or(("", node_name.as_str(), ""));

    // Rusternetes kubelet only serves plain HTTP today (see PR #188 note
    // below); reject `https` requests early rather than 502-ing on the
    // TLS handshake, matching the explicit-scheme contract upstream
    // sets in `SplitSchemeNamePort`.
    if scheme == "https" {
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "invalid node request {:?}: https scheme not supported by rusternetes kubelet yet",
            node_name
        )));
    }

    // Check authorization - requires permission to proxy to nodes.
    // Verb derives from the HTTP method (matches K8s RBAC semantics).
    // The RBAC name field carries the *node name*, not the raw id, so
    // policies that bind on `nodes/proxy` with a `resourceName` match.
    let verb = http_method_to_verb(&method);
    let attrs = RequestAttributes::new(auth_ctx.user, verb, "nodes/proxy")
        .with_api_group("")
        .with_name(actual_node_name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the node to find its address. Storage lookup uses the parsed
    // name; the raw `node_name` may carry a `:port` suffix the upstream
    // e2e framework appended.
    let node_key = rusternetes_storage::build_key("nodes", None, actual_node_name);
    let node: rusternetes_common::resources::Node = state.storage.get(&node_key).await?;

    // Resolve kubelet connection parameters (address, port, scheme) via the
    // shared helper so that exec/attach/logs handlers (Tasks 6/7) can reuse
    // the same logic without duplication.
    let port_override: Option<u16> = if port_str.is_empty() {
        None
    } else {
        port_str.parse::<u16>().ok()
    };
    let conn = super::node_conn::node_conn(&node, port_override)?;
    let target_url = format!("{}://{}:{}/{}", conn.scheme, conn.host, conn.port, suffix);

    // Forward the request to the kubelet, including the original query string.
    proxy_request(target_url, &original_uri, method, headers, body).await
}

/// Proxy HTTP requests to a service (root path — no sub-path)
pub async fn proxy_service_root(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, service_name)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    if let Some(redirect) = proxy_trailing_slash_redirect(&method, &original_uri) {
        return Ok(redirect);
    }
    proxy_service(
        State(state),
        Extension(auth_ctx),
        Path((namespace, service_name, String::new())),
        OriginalUri(original_uri),
        method,
        headers,
        body,
    )
    .await
}

/// Proxy HTTP requests to a service
///
/// Proxies requests to a service endpoint for debugging.
pub async fn proxy_service(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, service_name, _path)): Path<(String, String, String)>,
    OriginalUri(original_uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    // Use the original URI to preserve trailing slashes and exact path bytes.
    let suffix = extract_proxy_suffix(original_uri.path(), "services");
    info!(
        "Proxying request to service: {}/{}, path: /{}",
        namespace, service_name, suffix
    );

    // Parse service name — Kubernetes proxy subresource format:
    //   "name", "name:port", or "scheme:name:port"
    // K8s ref: k8s.io/apimachinery/pkg/util/net.SplitSchemeNamePort
    let (scheme, actual_service_name, port_id) =
        split_scheme_name_port(&service_name).unwrap_or(("", service_name.as_str(), ""));

    // Determine the URL scheme — default to http when unspecified
    let url_scheme = if scheme.is_empty() { "http" } else { scheme };

    // Check authorization. Verb derives from the HTTP method to match K8s RBAC.
    let verb = http_method_to_verb(&method);
    let attrs = RequestAttributes::new(auth_ctx.user, verb, "services/proxy")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(actual_service_name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the service to find its ClusterIP and port
    let service_key =
        rusternetes_storage::build_key("services", Some(&namespace), actual_service_name);
    let service: rusternetes_common::resources::Service = state.storage.get(&service_key).await?;

    // Resolve the service port:
    // 1. If port_id is a number, match by port number
    // 2. If port_id is a name, match by port name
    // 3. If port_id is empty, use the first port
    // K8s ref: pkg/registry/core/service/storage/storage.go ResourceLocation
    let port_id_as_num: Option<u16> = if port_id.is_empty() {
        None
    } else {
        port_id.parse::<u16>().ok()
    };

    let matched_service_port = if port_id.is_empty() {
        // No port specified — use first
        service.spec.ports.first()
    } else if let Some(num) = port_id_as_num {
        // Port specified as number — match by port number, fallback to first
        service
            .spec
            .ports
            .iter()
            .find(|p| p.port == num)
            .or_else(|| service.spec.ports.first())
    } else {
        // Port specified as name — match by name, fallback to first
        service
            .spec
            .ports
            .iter()
            .find(|p| p.name.as_deref() == Some(port_id))
            .or_else(|| service.spec.ports.first())
    };

    let service_port = matched_service_port.map(|p| p.port).unwrap_or(80);

    // Resolve target port (what the pod actually listens on)
    let target_port = matched_service_port
        .and_then(|sp| {
            sp.target_port.as_ref().map(|tp| match tp {
                rusternetes_common::resources::IntOrString::Int(p) => *p as u16,
                rusternetes_common::resources::IntOrString::String(s) => {
                    s.parse::<u16>().unwrap_or(service_port)
                }
            })
        })
        .unwrap_or(service_port);

    // Look up endpoint IPs — try EndpointSlices first, then fall back to Endpoints.
    // The API server container does NOT have kube-proxy's iptables rules,
    // so ClusterIP DNAT is not available. We MUST resolve to a pod IP.
    let mut endpoint_ip: Option<String> = None;
    // Resolved port from the endpoint itself (may differ from service targetPort
    // when using named ports). Falls back to target_port if endpoint has no port.
    let mut resolved_port: u16 = target_port;

    // Strategy 1: EndpointSlices
    let es_prefix = rusternetes_storage::build_prefix("endpointslices", Some(&namespace));
    let endpoint_slices: Vec<rusternetes_common::resources::EndpointSlice> =
        state.storage.list(&es_prefix).await.unwrap_or_default();
    for es in &endpoint_slices {
        let svc = es
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"));
        if svc.map(|s| s == actual_service_name).unwrap_or(false) {
            // Extract the endpoint port matching the requested service port.
            // EndpointSlice ports are at the slice level, not per-endpoint.
            // Match by port name first, then by port number.
            if let Some(ep_port) = if !port_id.is_empty() {
                // Match by name or number
                es.ports.iter().find(|p| {
                    p.name.as_deref() == Some(port_id)
                        || p.port.map(|n| n.to_string()) == Some(port_id.to_string())
                })
            } else {
                // No specific port requested — use first
                es.ports.first()
            } {
                if let Some(p) = ep_port.port {
                    resolved_port = p as u16;
                }
            } else if !es.ports.is_empty() {
                // Fallback: use first port from the EndpointSlice
                if let Some(p) = es.ports.first().and_then(|ep| ep.port) {
                    resolved_port = p as u16;
                }
            }

            for ep in &es.endpoints {
                if ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true) {
                    if let Some(addr) = ep.addresses.first() {
                        endpoint_ip = Some(addr.clone());
                        break;
                    }
                }
            }
            if endpoint_ip.is_some() {
                break;
            }
        }
    }

    // Strategy 2: Endpoints (legacy API) — fall back when no EndpointSlice is found.
    // Some tests create Endpoints directly, and the EndpointSlice mirroring controller
    // may not have run yet.
    if endpoint_ip.is_none() {
        let ep_key =
            rusternetes_storage::build_key("endpoints", Some(&namespace), actual_service_name);
        if let Ok(endpoints) = state
            .storage
            .get::<rusternetes_common::resources::Endpoints>(&ep_key)
            .await
        {
            for subset in &endpoints.subsets {
                // Also extract the port from the endpoint subset
                if let Some(ports) = &subset.ports {
                    if let Some(ep_port) = if !port_id.is_empty() {
                        ports.iter().find(|p| {
                            p.name.as_deref() == Some(port_id) || p.port.to_string() == port_id
                        })
                    } else {
                        ports.first()
                    } {
                        resolved_port = ep_port.port;
                    }
                }

                if let Some(addrs) = &subset.addresses {
                    if let Some(addr) = addrs.first() {
                        endpoint_ip = Some(addr.ip.clone());
                        break;
                    }
                }
            }
        }
    }

    // Build target URL — always prefer endpoint IP over ClusterIP.
    // Preserve the original path bytes (including any trailing slash) so
    // that proxy behaviour matches K8s verbatim.
    let target_url = if let Some(ep_ip) = &endpoint_ip {
        debug!(
            "Service proxy resolved endpoint: {}://{}:{} (service {}/{})",
            url_scheme, ep_ip, resolved_port, namespace, actual_service_name
        );
        format!("{}://{}:{}/{}", url_scheme, ep_ip, resolved_port, suffix)
    } else {
        // Last resort — use ClusterIP. This only works if kube-proxy rules
        // are somehow visible to the API server container (unlikely).
        let cluster_ip = service
            .spec
            .cluster_ip
            .clone()
            .unwrap_or_else(|| "127.0.0.1".to_string());
        warn!(
            "Service proxy: no endpoints found for {}/{}, falling back to ClusterIP {}",
            namespace, actual_service_name, cluster_ip
        );
        format!(
            "{}://{}:{}/{}",
            url_scheme, cluster_ip, service_port, suffix
        )
    };

    // Forward the request, preserving the original query string.
    proxy_request(target_url, &original_uri, method, headers, body).await
}

/// Proxy HTTP requests to a pod (root path, no trailing path component)
/// The pod/service proxy subresource redirects a bare `.../proxy` (no trailing
/// slash) to `.../proxy/` with HTTP 301, preserving the query string — matching
/// the upstream apiserver.
///
/// Only GET and HEAD are redirected. The e2e "Proxy ... valid responses" test
/// asserts GET/HEAD on a bare `.../proxy` return 301, but its verb sub-test
/// (DELETE/OPTIONS/PATCH/POST/PUT) uses a NON-redirect-following client
/// (`CheckRedirect: ErrUseLastResponse`) and asserts those return 200 directly.
/// So mutating methods must be proxied through, not redirected.
fn proxy_trailing_slash_redirect(method: &Method, uri: &Uri) -> Option<Response> {
    if !matches!(*method, Method::GET | Method::HEAD) {
        return None;
    }
    let path = uri.path();
    if !path.ends_with("/proxy") {
        return None;
    }
    let mut location = String::with_capacity(path.len() + 2);
    location.push_str(path);
    location.push('/');
    if let Some(q) = uri.query() {
        location.push('?');
        location.push_str(q);
    }
    Some(
        Response::builder()
            .status(StatusCode::MOVED_PERMANENTLY)
            .header(axum::http::header::LOCATION, location)
            .body(Body::empty())
            .unwrap(),
    )
}

pub async fn proxy_pod_root(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, pod_name)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    if let Some(redirect) = proxy_trailing_slash_redirect(&method, &original_uri) {
        return Ok(redirect);
    }
    proxy_pod(
        State(state),
        Extension(auth_ctx),
        Path((namespace, pod_name, String::new())),
        OriginalUri(original_uri),
        method,
        headers,
        body,
    )
    .await
}

/// Proxy HTTP requests to a pod
///
/// Proxies requests to a specific pod for debugging.
pub async fn proxy_pod(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, pod_name, _path)): Path<(String, String, String)>,
    OriginalUri(original_uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    // Use the original URI to preserve trailing slashes and exact path bytes.
    let suffix = extract_proxy_suffix(original_uri.path(), "pods");
    info!(
        "Proxying request to pod: {}/{}, path: /{}",
        namespace, pod_name, suffix
    );

    // Parse pod name — Kubernetes proxy subresource format:
    //   "name", "name:port", or "scheme:name:port"
    // K8s ref: k8s.io/apimachinery/pkg/util/net.SplitSchemeNamePort
    let (scheme, actual_pod_name, port_str) =
        split_scheme_name_port(&pod_name).unwrap_or(("", pod_name.as_str(), ""));

    // Port may be a number or a named container port. Numeric ports are
    // resolved as u16; otherwise we look up by container port name below.
    let explicit_port: Option<u16> = if port_str.is_empty() {
        None
    } else {
        port_str.parse::<u16>().ok()
    };

    // Determine the URL scheme — default to http when unspecified
    let url_scheme = if scheme.is_empty() { "http" } else { scheme };

    // Check authorization. Verb derives from the HTTP method to match K8s RBAC.
    let verb = http_method_to_verb(&method);
    let attrs = RequestAttributes::new(auth_ctx.user, verb, "pods/proxy")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(actual_pod_name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the pod to find its IP
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), actual_pod_name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Get pod IP from status.podIPs (preferred) or status.podIP
    let pod_ip = pod
        .status
        .as_ref()
        .and_then(|s| {
            s.pod_i_ps
                .as_ref()
                .and_then(|ips| ips.first().map(|p| p.ip.clone()))
                .or_else(|| s.pod_ip.clone())
        })
        .ok_or_else(|| {
            rusternetes_common::Error::NotFound(format!(
                "Pod {}/{} has no IP address yet",
                namespace, actual_pod_name
            ))
        })?;

    // Resolve the destination port:
    //   1. If a numeric port was given in the URL, use it as-is.
    //   2. If a non-numeric port string was given, treat it as a named
    //      container port and look it up across all containers.
    //   3. Otherwise, use the first containerPort on any container.
    //   4. Fall back to 80.
    // K8s ref: pkg/registry/core/pod/strategy.go ResourceLocation
    let port: u16 = if let Some(p) = explicit_port {
        p
    } else if !port_str.is_empty() {
        // Named port resolution.
        pod.spec
            .as_ref()
            .and_then(|spec| {
                spec.containers.iter().find_map(|c| {
                    c.ports.as_ref().and_then(|ports| {
                        ports
                            .iter()
                            .find(|cp| cp.name.as_deref() == Some(port_str))
                            .map(|cp| cp.container_port)
                    })
                })
            })
            .unwrap_or(80)
    } else {
        pod.spec
            .as_ref()
            .and_then(|spec| {
                spec.containers.iter().find_map(|c| {
                    c.ports
                        .as_ref()
                        .and_then(|ports| ports.first())
                        .map(|p| p.container_port)
                })
            })
            .unwrap_or(80)
    };

    debug!(
        "Pod proxy resolved: {}://{}:{} (pod {}/{})",
        url_scheme, pod_ip, port, namespace, actual_pod_name
    );

    // Build target URL — preserve the original path bytes (and trailing slash)
    let target_url = format!("{}://{}:{}/{}", url_scheme, pod_ip, port, suffix);

    // Forward the request with URL rewriting for HTML responses.
    // The proxy base path is what clients see; we use the raw `pod_name`
    // (which may include a port) verbatim so redirected URLs are still
    // resolvable via this API.
    let proxy_base = format!("/api/v1/namespaces/{}/pods/{}/proxy", namespace, pod_name);
    proxy_request_with_rewrite(
        target_url,
        &original_uri,
        method,
        headers,
        body,
        Some(proxy_base),
    )
    .await
}

/// Create a shared reqwest client for proxy requests.
/// Using a shared client enables connection pooling and avoids the overhead
/// of creating a new TLS context per request.
fn get_proxy_client() -> &'static reqwest::Client {
    use std::sync::OnceLock;
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .danger_accept_invalid_certs(true) // For kubelet self-signed certs
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(2))
            // Disable automatic redirect following — proxy should forward redirects
            // to the client as-is, not follow them internally.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("Failed to create proxy HTTP client")
    })
}

/// Helper function to proxy an HTTP request to a target URL.
///
/// K8s proxy endpoints return raw HTTP responses, NOT K8s Status JSON objects.
/// On connection errors, returns 502 Bad Gateway with a plain text error message.
/// On success, forwards the upstream response (status, headers, body) verbatim.
///
/// K8s ref: staging/src/k8s.io/apimachinery/pkg/util/proxy/upgradeaware.go
async fn proxy_request(
    target_url: String,
    original_uri: &Uri,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    proxy_request_with_rewrite(target_url, original_uri, method, headers, body, None).await
}

/// Proxy request with optional URL rewriting for HTML responses.
/// When `proxy_base_path` is Some, absolute URLs in HTML responses are
/// rewritten to include the API proxy path prefix — matching K8s behavior.
/// K8s ref: staging/src/k8s.io/apimachinery/pkg/util/proxy/upgradeaware.go
async fn proxy_request_with_rewrite(
    target_url: String,
    original_uri: &Uri,
    method: Method,
    headers: HeaderMap,
    body: Body,
    proxy_base_path: Option<String>,
) -> Result<Response> {
    // Forward the original query string verbatim. Re-encoding a HashMap
    // loses key ordering, duplicate keys, and exact encoding — all of which
    // proxy clients rely on. K8s forwards the raw query bytes through.
    let query_string = match original_uri.query() {
        Some(q) if !q.is_empty() => format!("?{}", q),
        _ => String::new(),
    };

    let full_url = format!("{}{}", target_url, query_string);

    info!("Forwarding {} request to {}", method, full_url);

    let client = get_proxy_client();

    // Convert axum body to bytes
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.map_err(|e| {
        rusternetes_common::Error::Internal(format!("Failed to read request body: {}", e))
    })?;

    // Collect forwarded headers (filter out hop-by-hop headers)
    let mut forward_headers: Vec<(String, String)> = Vec::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str();
        if !is_hop_by_hop_header(name_str) {
            if let Ok(val_str) = value.to_str() {
                forward_headers.push((name_str.to_string(), val_str.to_string()));
            }
        }
    }

    // Retry loop for transient connection errors.
    // Docker bridge networking can have brief connectivity gaps when containers
    // are starting or when the bridge is under load. Retrying with backoff
    // handles these transient failures.
    let max_retries = 3u32;
    let mut last_error = String::new();

    for attempt in 0..=max_retries {
        if attempt > 0 {
            // Exponential backoff: 100ms, 200ms, 400ms
            let delay = std::time::Duration::from_millis(100 * (1 << (attempt - 1)));
            tokio::time::sleep(delay).await;
            info!(
                "Proxy retry attempt {} for {} (last error: {})",
                attempt, full_url, last_error
            );
        }

        // Build the request
        let req_method: reqwest::Method = method.as_str().parse().unwrap_or(reqwest::Method::GET);
        let mut request = client
            .request(req_method, &full_url)
            .body(body_bytes.to_vec());

        // Forward headers
        for (name, value) in &forward_headers {
            request = request.header(name.as_str(), value.as_str());
        }

        // Execute the request
        match request.send().await {
            Ok(response) => {
                // Successfully connected — forward the response verbatim.
                let status = response.status();
                let mut axum_response = Response::builder().status(
                    StatusCode::from_u16(status.as_u16())
                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                );

                // Copy response headers, skipping Content-Length (recalculated from body).
                for (name, value) in response.headers().iter() {
                    let name_str = name.as_str();
                    if !is_hop_by_hop_header(name_str) {
                        // Always skip upstream Content-Length. reqwest may
                        // decompress the body (gzip/deflate), making the
                        // upstream length wrong. Let Axum set it from the
                        // actual body bytes.
                        // K8s ref: httputil.ReverseProxy copies Content-Length
                        // but Go's net/http handles the body streaming directly.
                        // We buffer the body, so we must recalculate.
                        if name_str == "content-length" {
                            continue;
                        }
                        axum_response = axum_response.header(name_str, value);
                    }
                }

                // Get response body
                let resp_bytes = match response.bytes().await {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("Failed to read proxy response body: {}", e);
                        // Return 502 with plain text error — NOT a K8s Status JSON.
                        // K8s proxy endpoints never wrap errors in Status objects.
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_GATEWAY)
                            .header("Content-Type", "text/plain")
                            .body(Body::from(format!(
                                "the backend attempted to proxy but the response body was unreadable: {}",
                                e
                            )))
                            .unwrap());
                    }
                };

                // Rewrite absolute URLs in HTML responses if proxy_base_path is set.
                // K8s rewrites href="/..." and src="/..." to include the proxy path prefix.
                let final_body = if let Some(ref base_path) = proxy_base_path {
                    let content_type = axum_response
                        .headers_ref()
                        .and_then(|h| h.get("content-type"))
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    if content_type.contains("text/html") {
                        let html = String::from_utf8_lossy(&resp_bytes);
                        let rewritten = html
                            .replace("href=\"/", &format!("href=\"{}/", base_path))
                            .replace("src=\"/", &format!("src=\"{}/", base_path));
                        Body::from(rewritten)
                    } else {
                        Body::from(resp_bytes.to_vec())
                    }
                } else {
                    Body::from(resp_bytes.to_vec())
                };

                // Build final response
                return axum_response.body(final_body).map_err(|e| {
                    rusternetes_common::Error::Internal(format!("Failed to build response: {}", e))
                });
            }
            Err(e) => {
                last_error = e.to_string();
                // Only retry on connection errors, not on timeouts or other errors
                let is_connect_error = e.is_connect();
                if !is_connect_error || attempt == max_retries {
                    warn!(
                        "Proxy request to {} failed after {} attempts: {}",
                        full_url,
                        attempt + 1,
                        e
                    );
                    // Return 502 Bad Gateway with plain text error — NOT a K8s Status JSON.
                    // K8s proxy endpoints return raw HTTP errors, not Status objects.
                    // The Go test client (DoRaw) expects either a successful raw response
                    // or an HTTP error it can retry on.
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header("Content-Type", "text/plain")
                        .body(Body::from(format!("error trying to reach backend: {}", e)))
                        .unwrap());
                }
                // Connection error — retry
            }
        }
    }

    // Should not reach here due to the loop logic, but just in case
    Ok(Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .header("Content-Type", "text/plain")
        .body(Body::from(format!(
            "error trying to reach backend: {}",
            last_error
        )))
        .unwrap())
}

/// Check if a header is a hop-by-hop header that should not be forwarded
fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "host"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hop_by_hop_headers() {
        assert!(is_hop_by_hop_header("Connection"));
        assert!(is_hop_by_hop_header("Keep-Alive"));
        assert!(is_hop_by_hop_header("transfer-encoding"));
        assert!(!is_hop_by_hop_header("Content-Type"));
        assert!(!is_hop_by_hop_header("Authorization"));
    }

    #[test]
    fn test_split_scheme_name_port_plain_name() {
        assert_eq!(split_scheme_name_port("myname"), Some(("", "myname", "")));
    }

    #[test]
    fn test_split_scheme_name_port_name_and_port() {
        assert_eq!(
            split_scheme_name_port("myname:8080"),
            Some(("", "myname", "8080"))
        );
    }

    #[test]
    fn test_split_scheme_name_port_scheme_name_port() {
        assert_eq!(
            split_scheme_name_port("http:myname:8080"),
            Some(("http", "myname", "8080"))
        );
        assert_eq!(
            split_scheme_name_port("https:myname:443"),
            Some(("https", "myname", "443"))
        );
    }

    #[test]
    fn test_split_scheme_name_port_invalid() {
        // Empty name
        assert_eq!(split_scheme_name_port(""), None);
        // Empty name with port
        assert_eq!(split_scheme_name_port(":8080"), None);
        // Invalid scheme
        assert_eq!(split_scheme_name_port("ftp:myname:21"), None);
        // Empty name with valid scheme
        assert_eq!(split_scheme_name_port("http::8080"), None);
    }

    #[test]
    fn test_http_method_to_verb_get_and_head() {
        // GET/HEAD/OPTIONS all map to "get" — matches K8s RequestInfoFactory.
        assert_eq!(http_method_to_verb(&Method::GET), "get");
        assert_eq!(http_method_to_verb(&Method::HEAD), "get");
        assert_eq!(http_method_to_verb(&Method::OPTIONS), "get");
    }

    #[test]
    fn test_http_method_to_verb_mutations() {
        assert_eq!(http_method_to_verb(&Method::POST), "create");
        assert_eq!(http_method_to_verb(&Method::PUT), "update");
        assert_eq!(http_method_to_verb(&Method::PATCH), "patch");
        assert_eq!(http_method_to_verb(&Method::DELETE), "delete");
    }

    #[test]
    fn test_extract_proxy_suffix_root_no_slash() {
        // /proxy with no trailing components — suffix is empty.
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy", "pods");
        assert_eq!(suffix, "");
    }

    #[test]
    fn test_extract_proxy_suffix_root_trailing_slash() {
        // /proxy/ — suffix is empty after the trailing slash strip.
        // (The forwarder appends `/{suffix}` so /proxy and /proxy/ both
        // produce a URL ending in `/` which is what backends expect.)
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy/", "pods");
        assert_eq!(suffix, "");
    }

    #[test]
    fn test_extract_proxy_suffix_path_no_trailing_slash() {
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy/foo/bar", "pods");
        assert_eq!(suffix, "foo/bar");
    }

    #[test]
    fn test_extract_proxy_suffix_preserves_trailing_slash() {
        // Critical for conformance: /foo/ must arrive at the backend as /foo/.
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy/foo/", "pods");
        assert_eq!(suffix, "foo/");
    }

    #[test]
    fn test_extract_proxy_suffix_services() {
        let suffix = extract_proxy_suffix(
            "/api/v1/namespaces/ns/services/svc:http/proxy/x/",
            "services",
        );
        assert_eq!(suffix, "x/");
    }

    #[test]
    fn test_extract_proxy_suffix_with_port_in_name() {
        // The pod/service name may include a port like `mypod:8080`.
        let suffix =
            extract_proxy_suffix("/api/v1/namespaces/ns/pods/mypod:8080/proxy/health", "pods");
        assert_eq!(suffix, "health");
    }

    #[test]
    fn test_extract_proxy_suffix_node() {
        let suffix = extract_proxy_suffix("/api/v1/nodes/node-1/proxy/stats/summary", "nodes");
        assert_eq!(suffix, "stats/summary");
    }

    /// Regression: node-proxy URLs that embed the kubelet port in the
    /// resource id must still surface the path suffix unchanged. The
    /// upstream e2e framework constructs URLs of the form
    /// `/api/v1/nodes/<name>:<port>/proxy/<path>`; `extract_proxy_suffix`
    /// is invoked with the verbatim path so the `:<port>` segment must
    /// not bleed into the returned suffix.
    /// K8s ref: test/e2e/framework/node/wait.go uses `node.Name + ":<port>"`.
    #[test]
    fn test_extract_proxy_suffix_node_name_with_port() {
        let suffix = extract_proxy_suffix("/api/v1/nodes/node-1:10250/proxy/pods", "nodes");
        assert_eq!(suffix, "pods");
    }

    /// Regression: when the port segment of a `<name>:<port>` resource id
    /// is non-numeric, `proxy_node` must fall back to the node-advertised
    /// kubelet port rather than crashing or proxying to port 0. We assert
    /// the parse step here so the fallback chain in `proxy_node` is
    /// driven by the same `port.parse::<u16>().ok()` contract used by
    /// `proxy_service` / `proxy_pod`.
    /// K8s ref: pkg/registry/core/node/strategy.go ResourceLocation —
    /// portReq carries the raw string; numeric validation is implicit
    /// in the downstream URL build.
    #[test]
    fn test_split_scheme_name_port_invalid_port_string() {
        let parsed = split_scheme_name_port("node-1:invalid-port");
        assert_eq!(parsed, Some(("", "node-1", "invalid-port")));
        // ...and the port-numeric guard the handler uses to gate the
        // override falls back to None.
        let override_port: Option<u16> = parsed.and_then(|(_, _, p)| p.parse::<u16>().ok());
        assert!(override_port.is_none());
    }

    #[test]
    fn test_extract_proxy_suffix_unrelated_path() {
        // No proxy segment — return empty.
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p", "pods");
        assert_eq!(suffix, "");
    }

    #[test]
    fn test_extract_proxy_suffix_encoded_chars_preserved() {
        // Path encoding such as %20 must pass through untouched.
        let suffix =
            extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy/with%20space", "pods");
        assert_eq!(suffix, "with%20space");
    }
}
