use axum::body::Body;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use http::{StatusCode, Uri};
use http_body_util::BodyExt;
use tracing::{debug, warn};

/// Returns true for hop-by-hop headers that must NOT be forwarded.
/// We handle Connection and Upgrade explicitly in the upgrade path.
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
    )
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

    // 2. Build the forwarded request to the backend.
    //    Preserve method, path (from target), and end-to-end headers.
    //    We explicitly keep Upgrade + Connection so the backend upgrades too.
    let mut builder = http::Request::builder()
        .method(parts.method.clone())
        .uri(target.clone());

    // Copy headers — filter hop-by-hop but keep Upgrade/Connection for the
    // upgrade handshake.
    {
        let fwd_headers = builder.headers_mut().unwrap();
        for (name, value) in &parts.headers {
            let n = name.as_str();
            // Explicitly keep Connection + Upgrade for the upgrade handshake;
            // forward everything else that is not hop-by-hop.
            if n.eq_ignore_ascii_case("connection")
                || n.eq_ignore_ascii_case("upgrade")
                || !is_hop_by_hop(n)
            {
                fwd_headers.insert(name.clone(), value.clone());
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

        // Copy upgrade-relevant headers from the backend 101 response.
        let mut resp_builder = Response::builder().status(StatusCode::SWITCHING_PROTOCOLS);
        for (name, value) in backend_resp.headers() {
            resp_builder = resp_builder.header(name, value);
        }

        // Spawn the bidirectional splice task.
        if let Some(client_up) = client_on_upgrade {
            tokio::spawn(async move {
                match tokio::try_join!(client_up, backend_on_upgrade) {
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
        } else {
            warn!("proxy_upgrade: client OnUpgrade not found — cannot splice");
        }

        resp_builder
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    } else {
        // Non-upgrade response — stream it back.
        let (resp_parts, resp_body) = backend_resp.into_parts();
        let mut resp_builder = Response::builder().status(resp_parts.status);
        for (name, value) in &resp_parts.headers {
            if !is_hop_by_hop(name.as_str()) {
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

    {
        let fwd_headers = builder.headers_mut().unwrap();
        for (name, value) in &parts.headers {
            if !is_hop_by_hop(name.as_str()) {
                fwd_headers.insert(name.clone(), value.clone());
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
            let mut resp_builder = Response::builder().status(resp_parts.status);
            for (name, value) in &resp_parts.headers {
                if !is_hop_by_hop(name.as_str()) {
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
}
