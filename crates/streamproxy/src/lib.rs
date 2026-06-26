use axum::body::Body;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use http::{StatusCode, Uri};
use http_body_util::BodyExt;
use tracing::{debug, warn};

/// Returns true for the standard set of hop-by-hop headers that must NOT be
/// forwarded across a proxy (RFC 7230 §6.1). Mirrors apimachinery's
/// upgradeaware.go and `crates/api-server/src/handlers/proxy.rs`. `upgrade` is
/// included here; the upgrade path re-adds `Connection: Upgrade` + `Upgrade:
/// <proto>` explicitly after stripping.
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
    )
}

/// Collect the header names dynamically nominated as hop-by-hop by the
/// message's `Connection` header value (RFC 7230 §6.1). These tokens name
/// additional headers that apply only to a single transport-level connection
/// and must be removed before forwarding. Returns lowercase names.
fn connection_nominated_headers(headers: &http::HeaderMap) -> Vec<String> {
    let mut names = Vec::new();
    for value in headers.get_all(http::header::CONNECTION) {
        if let Ok(s) = value.to_str() {
            for token in s.split(',') {
                let token = token.trim();
                if !token.is_empty() {
                    names.push(token.to_ascii_lowercase());
                }
            }
        }
    }
    names
}

/// True if `name` must be stripped: either a standard hop-by-hop header or a
/// token dynamically nominated by the `Connection` header.
fn should_strip(name: &str, nominated: &[String]) -> bool {
    is_hop_by_hop(name) || nominated.iter().any(|n| n.eq_ignore_ascii_case(name))
}

/// Upgrade-aware reverse proxy: forwards `req` to `target`, and on a backend
/// 101 splices both upgraded byte streams. Mirrors apimachinery
/// proxy.NewUpgradeAwareHandler(upgradeRequired=true).
pub async fn proxy_upgrade(target: Uri, req: Request) -> Response {
    // 1. Take the client's OnUpgrade future before consuming the request.
    //    hyper 1.x places OnUpgrade as a request extension when it sees
    //    Connection: Upgrade on an HTTP/1.1 connection.
    let (mut parts, body) = req.into_parts();
    let client_on_upgrade = parts.extensions.remove::<hyper::upgrade::OnUpgrade>();

    // If hyper never set up an upgrade future for this request, we cannot
    // splice the client side after a backend 101. Returning 101 anyway would
    // leave the client connection hung. Fail fast with 502 instead.
    let Some(client_on_upgrade) = client_on_upgrade else {
        warn!("proxy_upgrade: client OnUpgrade not present — refusing to upgrade");
        return StatusCode::BAD_GATEWAY.into_response();
    };

    // Capture the protocol the client wants to upgrade to so we can echo it
    // back verbatim in our 101. Defaults to the conventional SPDY token.
    let upgrade_proto = parts
        .headers
        .get(http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // 2. Build the forwarded request to the backend.
    //    Preserve method, path (from target), and end-to-end headers, stripping
    //    hop-by-hop headers (standard set + Connection-nominated tokens). We
    //    then re-add Connection: Upgrade + the client's Upgrade header so the
    //    backend performs the upgrade handshake.
    let mut builder = http::Request::builder()
        .method(parts.method.clone())
        .uri(target.clone());

    let req_nominated = connection_nominated_headers(&parts.headers);
    {
        let fwd_headers = builder.headers_mut().unwrap();
        for (name, value) in &parts.headers {
            if !should_strip(name.as_str(), &req_nominated) {
                fwd_headers.append(name.clone(), value.clone());
            }
        }
        // Re-add the upgrade handshake headers explicitly.
        fwd_headers.insert(
            http::header::CONNECTION,
            http::HeaderValue::from_static("Upgrade"),
        );
        if let Some(proto) = &upgrade_proto {
            if let Ok(v) = http::HeaderValue::from_str(proto) {
                fwd_headers.insert(http::header::UPGRADE, v);
            }
        }
    }

    let backend_req = match builder.body(body) {
        Ok(r) => r,
        Err(e) => {
            warn!("proxy_upgrade: failed to build backend request: {e}");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    // 3. Send to backend.
    let connector = hyper_util::client::legacy::connect::HttpConnector::new();
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build(connector);

    let backend_resp = match client.request(backend_req).await {
        Ok(r) => r,
        Err(e) => {
            warn!("proxy_upgrade: backend request failed: {e}");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    if backend_resp.status() == StatusCode::SWITCHING_PROTOCOLS {
        // 4. Backend agreed to upgrade — splice both sides.
        //    hyper::upgrade::on takes &mut T, so we shadow as mutable.
        let mut backend_resp = backend_resp;
        let backend_on_upgrade = hyper::upgrade::on(&mut backend_resp);

        // Copy the backend's 101 headers, stripping hop-by-hop headers
        // (standard set + Connection-nominated tokens). We then re-add the
        // upgrade handshake headers explicitly so the client completes its end.
        let resp_nominated = connection_nominated_headers(backend_resp.headers());
        let mut resp_builder = Response::builder().status(StatusCode::SWITCHING_PROTOCOLS);
        for (name, value) in backend_resp.headers() {
            if !should_strip(name.as_str(), &resp_nominated) {
                resp_builder = resp_builder.header(name, value);
            }
        }
        resp_builder = resp_builder.header(http::header::CONNECTION, "Upgrade");
        if let Some(proto) = &upgrade_proto {
            resp_builder = resp_builder.header(http::header::UPGRADE, proto);
        }

        // Spawn the bidirectional splice task.
        tokio::spawn(async move {
            match tokio::try_join!(client_on_upgrade, backend_on_upgrade) {
                Ok((client_upgraded, backend_upgraded)) => {
                    let mut client_io = hyper_util::rt::TokioIo::new(client_upgraded);
                    let mut backend_io = hyper_util::rt::TokioIo::new(backend_upgraded);
                    if let Err(e) =
                        tokio::io::copy_bidirectional(&mut client_io, &mut backend_io).await
                    {
                        debug!("proxy_upgrade splice ended: {e}");
                    }
                }
                Err(e) => warn!("proxy_upgrade: upgrade handshake failed: {e}"),
            }
        });

        resp_builder
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    } else {
        // Non-upgrade response — stream it back, stripping hop-by-hop headers.
        let (resp_parts, resp_body) = backend_resp.into_parts();
        let resp_nominated = connection_nominated_headers(&resp_parts.headers);
        let mut resp_builder = Response::builder().status(resp_parts.status);
        for (name, value) in &resp_parts.headers {
            if !should_strip(name.as_str(), &resp_nominated) {
                resp_builder = resp_builder.header(name, value);
            }
        }
        resp_builder
            .body(Body::from_stream(resp_body.into_data_stream()))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

/// Non-upgrade streaming reverse proxy (body streamed, not buffered).
pub async fn proxy_stream(target: Uri, req: Request) -> Response {
    let (parts, body) = req.into_parts();

    let mut builder = http::Request::builder().method(parts.method).uri(target);

    let req_nominated = connection_nominated_headers(&parts.headers);
    {
        let fwd_headers = builder.headers_mut().unwrap();
        for (name, value) in &parts.headers {
            if !should_strip(name.as_str(), &req_nominated) {
                fwd_headers.append(name.clone(), value.clone());
            }
        }
    }

    let backend_req = match builder.body(body) {
        Ok(r) => r,
        Err(e) => {
            warn!("proxy_stream: failed to build backend request: {e}");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    let connector = hyper_util::client::legacy::connect::HttpConnector::new();
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build(connector);

    match client.request(backend_req).await {
        Ok(backend_resp) => {
            let (resp_parts, resp_body) = backend_resp.into_parts();
            let resp_nominated = connection_nominated_headers(&resp_parts.headers);
            let mut resp_builder = Response::builder().status(resp_parts.status);
            for (name, value) in &resp_parts.headers {
                if !should_strip(name.as_str(), &resp_nominated) {
                    resp_builder = resp_builder.header(name, value);
                }
            }
            resp_builder
                .body(Body::from_stream(resp_body.into_data_stream()))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Err(e) => {
            warn!("proxy_stream: backend request failed: {e}");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn proxy_upgrade_splices_bidirectional_after_101() {
        let backend = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = backend.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf).await.unwrap();
            s.write_all(
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n",
            )
            .await
            .unwrap();
            let mut line = [0u8; 5];
            s.read_exact(&mut line).await.unwrap();
            s.write_all(&line.to_ascii_uppercase()).await.unwrap();
        });

        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        let target: http::Uri = format!("http://{backend_addr}/x").parse().unwrap();
        tokio::spawn(async move {
            let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
                let t = target.clone();
                async move { proxy_upgrade(t, req).await }
            });
            axum::serve(proxy, app).await.unwrap();
        });

        let mut c = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
        c.write_all(b"GET /x HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n")
            .await
            .unwrap();
        let mut hdr = [0u8; 1024];
        let n = c.read(&mut hdr).await.unwrap();
        assert!(std::str::from_utf8(&hdr[..n]).unwrap().contains("101"));
        c.write_all(b"hello").await.unwrap();
        let mut out = [0u8; 5];
        c.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"HELLO");
    }

    // A backend that answers a plain GET with a normal 200 + body. Proves
    // proxy_stream forwards the request and streams the response body back.
    #[tokio::test]
    async fn proxy_stream_forwards_normal_get() {
        let backend = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = backend.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf).await.unwrap();
            s.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nhello, world!",
            )
            .await
            .unwrap();
        });

        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        let target: http::Uri = format!("http://{backend_addr}/data").parse().unwrap();
        tokio::spawn(async move {
            let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
                let t = target.clone();
                async move { proxy_stream(t, req).await }
            });
            axum::serve(proxy, app).await.unwrap();
        });

        let mut c = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
        c.write_all(b"GET /data HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        // Read the full proxied response.
        let mut resp = Vec::new();
        c.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8(resp).unwrap();
        assert!(text.contains("200"), "expected 200 status, got: {text}");
        assert!(
            text.contains("hello, world!"),
            "expected body forwarded, got: {text}"
        );
    }
}
