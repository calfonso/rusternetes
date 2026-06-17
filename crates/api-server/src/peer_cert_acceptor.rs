//! TLS acceptor that surfaces the verified client-certificate chain to handlers.
//!
//! `axum_server`'s stock `bind_rustls` serves the app via `into_make_service()`,
//! so even though the rustls layer verifies a client cert against
//! `--client-ca-file`, the verified peer cert never reaches a handler and CN/O
//! are never mapped to a user (#1129).
//!
//! [`PeerCertAcceptor`] wraps the stock [`RustlsAcceptor`]: after the TLS
//! handshake completes it reads the verified chain off the connection
//! ([`peer_certificates`]) and, via an [`axum::Extension`] layer applied to that
//! connection's service, injects it into every request's extensions as
//! [`PeerCertificates`]. The auth middleware then maps CN→user / O→groups.
//!
//! The chain is the one rustls already verified against the configured client
//! CA — this acceptor performs no verification of its own. When the client
//! presents no certificate (bearer-token clients), an empty chain is injected
//! and the middleware falls through to bearer-token / anonymous handling.
//!
//! [`peer_certificates`]: rustls::CommonState::peer_certificates

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use axum::Extension;
use axum_server::accept::Accept;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use rusternetes_middleware::PeerCertificates;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Layer;

/// A TLS acceptor that injects the verified client-cert chain as a
/// [`PeerCertificates`] request extension. Drop-in replacement for the acceptor
/// `bind_rustls` installs.
#[derive(Clone)]
pub struct PeerCertAcceptor {
    inner: RustlsAcceptor,
}

impl PeerCertAcceptor {
    /// Build from the same [`RustlsConfig`] you would pass to `bind_rustls`.
    pub fn new(config: RustlsConfig) -> Self {
        Self {
            inner: RustlsAcceptor::new(config),
        }
    }
}

impl<I, S> Accept<I, S> for PeerCertAcceptor
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Send + 'static,
{
    // Project the stream/service types through the wrapped acceptor + the
    // Extension layer so we never have to name `TlsStream` / `AddExtension`.
    type Stream = <RustlsAcceptor as Accept<I, S>>::Stream;
    type Service = <Extension<PeerCertificates> as Layer<S>>::Service;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            let (tls_stream, service) = inner.accept(stream, service).await?;

            // `get_ref().1` is the rustls ServerConnection post-handshake; the
            // chain it returns is already verified against the client CA. Copy
            // the DER out so the extension is `'static` and rustls-free.
            let chain: Vec<Vec<u8>> = tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .map(|certs| certs.iter().map(|c| c.as_ref().to_vec()).collect())
                .unwrap_or_default();

            let service = Extension(PeerCertificates(Arc::new(chain))).layer(service);
            Ok((tls_stream, service))
        })
    }
}
