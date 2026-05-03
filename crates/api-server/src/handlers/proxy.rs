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
    extract::{Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
    Extension,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    Result,
};
use rusternetes_storage::Storage;
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, info, warn};

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
    Path((node_name, path)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Response> {
    info!("Proxying request to node: {}, path: {}", node_name, path);

    // Check authorization - requires permission to proxy to nodes
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "nodes/proxy")
        .with_api_group("")
        .with_name(&node_name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the node to find its address
    let node_key = rusternetes_storage::build_key("nodes", None, &node_name);
    let node: rusternetes_common::resources::Node = state.storage.get(&node_key).await?;

    // Extract node address (prefer InternalIP, fallback to ExternalIP)
    let node_address = node
        .status
        .as_ref()
        .and_then(|s| s.addresses.as_ref())
        .and_then(|addrs| {
            addrs
                .iter()
                .find(|a| a.address_type == "InternalIP")
                .or_else(|| addrs.iter().find(|a| a.address_type == "ExternalIP"))
        })
        .map(|a| a.address.clone())
        .ok_or_else(|| {
            rusternetes_common::Error::NotFound(format!("No address found for node {}", node_name))
        })?;

    // Build target URL (kubelet typically runs on port 10250)
    let kubelet_port = 10250;
    let path = path.trim_start_matches('/');
    let target_url = format!("https://{}:{}/{}", node_address, kubelet_port, path);

    // Forward the request to the kubelet
    proxy_request(target_url, method, headers, params, body).await
}

/// Proxy HTTP requests to a service (root path — no sub-path)
pub async fn proxy_service_root(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, service_name)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Response> {
    proxy_service(
        State(state),
        Extension(auth_ctx),
        Path((namespace, service_name, String::new())),
        method,
        headers,
        Query(params),
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
    Path((namespace, service_name, path)): Path<(String, String, String)>,
    method: Method,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Response> {
    info!(
        "Proxying request to service: {}/{}, path: {}",
        namespace, service_name, path
    );

    // Parse service name — Kubernetes proxy subresource format:
    //   "name", "name:port", or "scheme:name:port"
    // K8s ref: k8s.io/apimachinery/pkg/util/net.SplitSchemeNamePort
    let (scheme, actual_service_name, port_id) =
        split_scheme_name_port(&service_name).unwrap_or(("", service_name.as_str(), ""));

    // Determine the URL scheme — default to http when unspecified
    let url_scheme = if scheme.is_empty() { "http" } else { scheme };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "services/proxy")
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

    // Build target URL — always prefer endpoint IP over ClusterIP
    let path = path.trim_start_matches('/');
    let target_url = if let Some(ep_ip) = &endpoint_ip {
        debug!(
            "Service proxy resolved endpoint: {}://{}:{} (service {}/{})",
            url_scheme, ep_ip, resolved_port, namespace, actual_service_name
        );
        format!("{}://{}:{}/{}", url_scheme, ep_ip, resolved_port, path)
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
        format!("{}://{}:{}/{}", url_scheme, cluster_ip, service_port, path)
    };

    // Forward the request
    proxy_request(target_url, method, headers, params, body).await
}

/// Proxy HTTP requests to a pod (root path, no trailing path component)
pub async fn proxy_pod_root(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, pod_name)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Response> {
    proxy_pod(
        State(state),
        Extension(auth_ctx),
        Path((namespace, pod_name, String::new())),
        method,
        headers,
        Query(params),
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
    Path((namespace, pod_name, path)): Path<(String, String, String)>,
    method: Method,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Response> {
    info!(
        "Proxying request to pod: {}/{}, path: {}",
        namespace, pod_name, path
    );

    // Parse pod name — Kubernetes proxy subresource format:
    //   "name", "name:port", or "scheme:name:port"
    // K8s ref: k8s.io/apimachinery/pkg/util/net.SplitSchemeNamePort
    let (scheme, actual_pod_name, port_str) =
        split_scheme_name_port(&pod_name).unwrap_or(("", pod_name.as_str(), ""));

    let explicit_port: Option<u16> = if port_str.is_empty() {
        None
    } else {
        port_str.parse::<u16>().ok()
    };

    // Determine the URL scheme — default to http when unspecified
    let url_scheme = if scheme.is_empty() { "http" } else { scheme };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "pods/proxy")
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

    // Use explicit port from URL if provided, otherwise first container port
    // (scan all containers like K8s does), otherwise default to 80.
    // K8s ref: pkg/registry/core/pod/strategy.go ResourceLocation
    let port: u16 = if let Some(p) = explicit_port {
        p
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

    // Build target URL
    let path = path.trim_start_matches('/');
    let target_url = format!("{}://{}:{}/{}", url_scheme, pod_ip, port, path);

    // Forward the request with URL rewriting for HTML responses
    let proxy_base = format!("/api/v1/namespaces/{}/pods/{}/proxy", namespace, pod_name);
    proxy_request_with_rewrite(target_url, method, headers, params, body, Some(proxy_base)).await
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
    method: Method,
    headers: HeaderMap,
    params: HashMap<String, String>,
    body: Body,
) -> Result<Response> {
    proxy_request_with_rewrite(target_url, method, headers, params, body, None).await
}

/// Proxy request with optional URL rewriting for HTML responses.
/// When `proxy_base_path` is Some, absolute URLs in HTML responses are
/// rewritten to include the API proxy path prefix — matching K8s behavior.
/// K8s ref: staging/src/k8s.io/apimachinery/pkg/util/proxy/upgradeaware.go
async fn proxy_request_with_rewrite(
    target_url: String,
    method: Method,
    headers: HeaderMap,
    params: HashMap<String, String>,
    body: Body,
    proxy_base_path: Option<String>,
) -> Result<Response> {
    // Build query string from params
    let query_string = if !params.is_empty() {
        let pairs: Vec<String> = params.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
        format!("?{}", pairs.join("&"))
    } else {
        String::new()
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

                // Copy response headers.
                // Remove Content-Length if we might rewrite the body (HTML responses),
                // since rewriting changes the body length.
                let might_rewrite = proxy_base_path.is_some()
                    && response
                        .headers()
                        .get("content-type")
                        .and_then(|v| v.to_str().ok())
                        .map(|ct| ct.contains("text/html"))
                        .unwrap_or(false);
                for (name, value) in response.headers().iter() {
                    let name_str = name.as_str();
                    if !is_hop_by_hop_header(name_str) {
                        if might_rewrite && name_str == "content-length" {
                            continue; // Skip — body length will change
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
}
