use anyhow::{Context, Result};
use rcgen::{CertificateParams, DistinguishedName, DnType, Ia5String, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::str::FromStr;
use std::sync::Arc;

// Install the default crypto provider on module load
fn install_crypto_provider() {
    use rustls::crypto::CryptoProvider;
    let _ = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider());
}

// Call it immediately when the module is loaded
static CRYPTO_INIT: std::sync::Once = std::sync::Once::new();
fn ensure_crypto_provider() {
    CRYPTO_INIT.call_once(install_crypto_provider);
}

/// TLS certificate configuration
pub struct TlsConfig {
    pub cert: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    pub cert_pem: Option<String>, // PEM-encoded certificate for distribution to clients
}

impl TlsConfig {
    /// Generate a self-signed certificate for development/testing
    pub fn generate_self_signed(common_name: &str, subject_alt_names: Vec<String>) -> Result<Self> {
        ensure_crypto_provider();
        let mut params = CertificateParams::default();

        // Set certificate validity (10 years for development/testing)
        // Valid from 2024 to 2034
        params.not_before = rcgen::date_time_ymd(2024, 1, 1);
        params.not_after = rcgen::date_time_ymd(2034, 12, 31);

        // Mark this as a CA certificate so it can be trusted as a root CA
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

        // Set distinguished name
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, common_name);
        dn.push(DnType::OrganizationName, "Rusternetes");
        dn.push(DnType::CountryName, "US");
        params.distinguished_name = dn;

        // Add subject alternative names (SANs)
        for san in subject_alt_names {
            if san.parse::<std::net::IpAddr>().is_ok() {
                params
                    .subject_alt_names
                    .push(SanType::IpAddress(san.parse()?));
            } else {
                params
                    .subject_alt_names
                    .push(SanType::DnsName(Ia5String::from_str(&san)?));
            }
        }

        // Generate certificate
        let key_pair = rcgen::KeyPair::generate()?;
        let cert = params.self_signed(&key_pair)?;
        let cert_pem = cert.pem();
        let key_pem = key_pair.serialize_pem();

        // Parse to rustls types
        let cert_der = rustls_pemfile::certs(&mut cert_pem.as_bytes())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to parse certificate")?;

        let key_der = rustls_pemfile::private_key(&mut key_pem.as_bytes())
            .context("Failed to read private key")?
            .ok_or_else(|| anyhow::anyhow!("Failed to parse private key"))?;

        Ok(TlsConfig {
            cert: cert_der,
            key: key_der,
            cert_pem: Some(cert_pem),
        })
    }

    /// Load certificate and key from PEM files
    pub fn from_pem_files(cert_path: &str, key_path: &str) -> Result<Self> {
        ensure_crypto_provider();
        let cert_pem = std::fs::read(cert_path).context("Failed to read certificate file")?;
        let key_pem = std::fs::read(key_path).context("Failed to read key file")?;

        let cert_der = rustls_pemfile::certs(&mut cert_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to parse certificate")?;

        let key_der = rustls_pemfile::private_key(&mut key_pem.as_slice())
            .context("Failed to read private key")?
            .ok_or_else(|| anyhow::anyhow!("Failed to parse private key"))?;

        Ok(TlsConfig {
            cert: cert_der,
            key: key_der,
            cert_pem: String::from_utf8(cert_pem).ok(), // Try to convert to String, None if invalid UTF-8
        })
    }

    /// Create rustls server config
    pub fn into_server_config(self) -> Result<Arc<rustls::ServerConfig>> {
        ensure_crypto_provider();
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(self.cert, self.key)
            .context("Failed to create server config")?;

        // Enable HTTP/2 via ALPN negotiation.
        // K8s API server advertises h2 and http/1.1.
        // Go's client-go prefers HTTP/2 for multiplexed watch streams.
        // Without ALPN, client-go falls back to HTTP/1.1 which has
        // connection pooling issues that cause "context canceled" watch errors.
        // K8s ref: staging/src/k8s.io/apiserver/pkg/server/options/serving.go
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        Ok(Arc::new(config))
    }

    /// Create rustls server config with mutual TLS (mTLS)
    pub fn into_mtls_server_config(
        self,
        client_ca_cert_path: &str,
    ) -> Result<Arc<rustls::ServerConfig>> {
        // Load client CA certificate
        let client_ca_pem =
            std::fs::read(client_ca_cert_path).context("Failed to read client CA certificate")?;
        let client_ca_certs = rustls_pemfile::certs(&mut client_ca_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to parse client CA certificate")?;

        let mut client_auth_roots = rustls::RootCertStore::empty();
        client_auth_roots.add_parsable_certificates(client_ca_certs);

        // `allow_unauthenticated` makes the client certificate OPTIONAL, matching
        // upstream apiserver semantics: `--client-ca-file` *enables* x509
        // authentication but does not *require* a client cert (it requests one,
        // RequestClientCert). Clients that authenticate by bearer token (kubectl,
        // service accounts) connect without presenting a cert; clients that do
        // present one get it verified against the CA, and the verified chain is
        // surfaced to the auth middleware for CN→user / O→groups mapping (#1129).
        // A mandatory verifier (`.build()`) would instead reject every
        // bearer-token client at the TLS handshake.
        let client_cert_verifier =
            rustls::server::WebPkiClientVerifier::builder(Arc::new(client_auth_roots))
                .allow_unauthenticated()
                .build()
                .context("Failed to build client cert verifier")?;

        let mut config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(self.cert, self.key)
            .context("Failed to create mTLS server config")?;

        // Match into_server_config: advertise h2 + http/1.1 so client-go uses
        // HTTP/2 for watch multiplexing (see the note there).
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        Ok(Arc::new(config))
    }
}

/// TLS configuration for clients
pub struct TlsClientConfig {
    pub ca_cert: Vec<CertificateDer<'static>>,
    pub client_cert: Option<Vec<CertificateDer<'static>>>,
    pub client_key: Option<PrivateKeyDer<'static>>,
}

impl TlsClientConfig {
    /// Create client config with CA certificate (server verification only)
    pub fn new(ca_cert_path: &str) -> Result<Self> {
        let ca_pem = std::fs::read(ca_cert_path).context("Failed to read CA certificate")?;
        let ca_cert = rustls_pemfile::certs(&mut ca_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to parse CA certificate")?;

        Ok(TlsClientConfig {
            ca_cert,
            client_cert: None,
            client_key: None,
        })
    }

    /// Create client config with mTLS (client certificate authentication)
    pub fn new_with_client_cert(
        ca_cert_path: &str,
        client_cert_path: &str,
        client_key_path: &str,
    ) -> Result<Self> {
        let ca_pem = std::fs::read(ca_cert_path).context("Failed to read CA certificate")?;
        let ca_cert = rustls_pemfile::certs(&mut ca_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to parse CA certificate")?;

        let client_cert_pem =
            std::fs::read(client_cert_path).context("Failed to read client certificate")?;
        let client_cert = rustls_pemfile::certs(&mut client_cert_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to parse client certificate")?;

        let client_key_pem = std::fs::read(client_key_path).context("Failed to read client key")?;
        let client_key = rustls_pemfile::private_key(&mut client_key_pem.as_slice())
            .context("Failed to read client private key")?
            .ok_or_else(|| anyhow::anyhow!("Failed to parse client private key"))?;

        Ok(TlsClientConfig {
            ca_cert,
            client_cert: Some(client_cert),
            client_key: Some(client_key),
        })
    }

    /// Create rustls client config
    pub fn into_client_config(self) -> Result<Arc<rustls::ClientConfig>> {
        let mut root_cert_store = rustls::RootCertStore::empty();
        root_cert_store.add_parsable_certificates(self.ca_cert);

        let config = rustls::ClientConfig::builder().with_root_certificates(root_cert_store);

        let config = if let (Some(cert), Some(key)) = (self.client_cert, self.client_key) {
            config
                .with_client_auth_cert(cert, key)
                .context("Failed to create client config with client auth")?
        } else {
            config.with_no_client_auth()
        };

        Ok(Arc::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_self_signed_cert() {
        let tls_config = TlsConfig::generate_self_signed(
            "localhost",
            vec!["localhost".to_string(), "127.0.0.1".to_string()],
        )
        .expect("Failed to generate self-signed certificate");

        assert!(!tls_config.cert.is_empty());

        // Should be able to create server config
        let _server_config = tls_config
            .into_server_config()
            .expect("Failed to create server config");
    }

    #[test]
    fn test_cert_with_multiple_sans() {
        let tls_config = TlsConfig::generate_self_signed(
            "rusternetes-api",
            vec![
                "localhost".to_string(),
                "api.rusternetes.local".to_string(),
                "127.0.0.1".to_string(),
                "::1".to_string(),
            ],
        )
        .expect("Failed to generate certificate");

        assert!(!tls_config.cert.is_empty());
    }
}
