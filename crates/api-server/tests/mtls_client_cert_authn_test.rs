//! End-to-end mTLS x509 client-certificate authentication (#1186).
//!
//! Follow-up to #1129 / PR #1184, which had unit coverage for the subject
//! mapping (`UserInfo::from_cert_identity`, `user_from_client_cert_der`) but
//! left the runtime seam untested: a REAL TLS handshake where
//! [`PeerCertAcceptor`] injects the verified chain → `auth_middleware` reads the
//! `PeerCertificates` extension → the resulting `AuthContext`.
//!
//! This drives the production server path end to end:
//!   * `TlsConfig::into_mtls_server_config(--client-ca-file)` (client cert
//!     OPTIONAL via `allow_unauthenticated`)
//!   * served through `PeerCertAcceptor` (the acceptor `main.rs` installs when
//!     `--client-ca-file` is set), `skip_auth = false`
//!   * the identity is read back via `SelfSubjectReview`, which echoes the
//!     authenticated `AuthContext` user into `status.userInfo`.
//!
//! Three clients prove cert / anonymous / bearer-token all resolve correctly
//! and coexist over the same optional-cert listener.

use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use rusternetes_api_server::peer_cert_acceptor::PeerCertAcceptor;
use rusternetes_api_server::router::build_router;
use rusternetes_api_server::state::ApiServerState;
use rusternetes_common::auth::{ServiceAccountClaims, TokenManager};
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::tls::TlsConfig;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::{json, Value};

const SECRET: &[u8] = b"mtls-e2e-test-secret";
const SSR_PATH: &str = "/apis/authentication.k8s.io/v1/selfsubjectreviews";

/// A self-signed CA plus a `signed_by` leaf factory.
struct TestCa {
    cert: rcgen::Certificate,
    key: KeyPair,
}

/// A CA-signed leaf, kept as rcgen objects so callers can take DER (server
/// config) or PEM (reqwest identity) without a PEM round-trip.
struct Leaf {
    cert: rcgen::Certificate,
    key: KeyPair,
}

impl TestCa {
    fn new() -> Self {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "rusternetes-test-ca");
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Self { cert, key }
    }

    /// A leaf signed by this CA. `cn`/`orgs` populate the subject DN; `sans`
    /// populate SANs (for the server's `127.0.0.1`); `client_auth` selects the
    /// EKU so the rustls verifier on the far side accepts it.
    fn issue(&self, cn: &str, orgs: &[&str], sans: Vec<String>, client_auth: bool) -> Leaf {
        let mut params = CertificateParams::new(sans).unwrap();
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(DnType::CommonName, cn);
        for o in orgs {
            dn.push(DnType::OrganizationName, *o);
        }
        params.distinguished_name = dn;
        params.extended_key_usages = vec![if client_auth {
            ExtendedKeyUsagePurpose::ClientAuth
        } else {
            ExtendedKeyUsagePurpose::ServerAuth
        }];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
        Leaf { cert, key }
    }
}

/// Build the api-server state with real authn (`skip_auth = false`) and an
/// allow-all authorizer (this test isolates *authentication* — who you are —
/// not authorization).
fn build_state(
    backend: Arc<StorageBackend>,
    token_manager: Arc<TokenManager>,
) -> Arc<ApiServerState> {
    Arc::new(ApiServerState::new(
        backend,
        token_manager,
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        false, // skip_auth OFF — exercise auth_middleware
    ))
}

/// POST an (empty) SelfSubjectReview through `client` and return
/// `(status, status.userInfo)`.
async fn self_subject_review(
    client: &reqwest::Client,
    base: &str,
    bearer: Option<&str>,
) -> (u16, Value) {
    let mut req = client.post(format!("{base}{SSR_PATH}")).json(&json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "SelfSubjectReview"
    }));
    if let Some(token) = bearer {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await.expect("request");
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    let user_info = body
        .pointer("/status/userInfo")
        .cloned()
        .unwrap_or(Value::Null);
    (status, user_info)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_client_cert_resolves_user_anonymous_and_token() {
    // aws-lc-rs is the configured provider; install it for the server config
    // builder + the reqwest rustls client.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let ca = TestCa::new();
    let ca_pem = ca.cert.pem();

    // Server leaf valid for 127.0.0.1, and a client leaf CN=test-user/O=test-group.
    let server = ca.issue(
        "rusternetes-apiserver",
        &[],
        vec!["127.0.0.1".to_string()],
        false,
    );
    let client = ca.issue("test-user", &["test-group"], vec![], true);

    // Real mTLS server config via the production path. The CA must be a file
    // (the `--client-ca-file` seam); write it to a unique temp path.
    let ca_path = std::env::temp_dir().join(format!("rn-mtls-ca-{}.pem", uuid::Uuid::new_v4()));
    std::fs::write(&ca_path, ca_pem.as_bytes()).unwrap();

    let tls_config = TlsConfig {
        cert: vec![server.cert.der().clone()],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server.key.serialize_der())),
        cert_pem: None,
    };
    let server_config = tls_config
        .into_mtls_server_config(ca_path.to_str().unwrap())
        .expect("mtls server config");
    let _ = std::fs::remove_file(&ca_path);

    // Bring up the server on an ephemeral port, served via PeerCertAcceptor.
    let token_manager = Arc::new(TokenManager::new(SECRET));
    let backend = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
    // Upstream parity: auth_middleware only honors a SA token if the
    // ServiceAccount still exists (and its uid matches). Seed it so the
    // bearer-token leg authenticates.
    backend
        .create(
            &build_key("serviceaccounts", Some("kube-system"), "probe-sa"),
            &json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "probe-sa", "namespace": "kube-system", "uid": "uid-probe"}
            }),
        )
        .await
        .unwrap();
    let state = build_state(backend, token_manager.clone());
    let app = build_router(state, None);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let rustls_config = axum_server::tls_rustls::RustlsConfig::from_config(server_config);
    tokio::spawn(async move {
        axum_server::from_tcp(listener)
            .acceptor(PeerCertAcceptor::new(rustls_config))
            .serve(app.into_make_service())
            .await
            .unwrap();
    });
    // Let the accept loop come up.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let base = format!("https://127.0.0.1:{port}");
    let ca_root = reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap();

    // (2) No client cert + no token → anonymous.
    let anon_client = reqwest::Client::builder()
        .add_root_certificate(ca_root.clone())
        .use_rustls_tls()
        .build()
        .unwrap();
    let (status, ui) = self_subject_review(&anon_client, &base, None).await;
    assert_eq!(status, 200, "anonymous SSR status; userInfo={ui:?}");
    assert_eq!(
        ui.get("username").and_then(Value::as_str),
        Some("system:anonymous"),
        "no cert + no token must resolve as anonymous"
    );

    // (1) Client cert CN=test-user, O=test-group → authenticated as test-user.
    let identity_pem = format!("{}{}", client.cert.pem(), client.key.serialize_pem());
    let identity = reqwest::Identity::from_pem(identity_pem.as_bytes()).unwrap();
    let cert_client = reqwest::Client::builder()
        .add_root_certificate(ca_root.clone())
        .identity(identity)
        .use_rustls_tls()
        .build()
        .unwrap();
    let (status, ui) = self_subject_review(&cert_client, &base, None).await;
    assert_eq!(status, 200, "cert SSR status; userInfo={ui:?}");
    assert_eq!(
        ui.get("username").and_then(Value::as_str),
        Some("test-user"),
        "client-cert CN must map to username"
    );
    let groups: Vec<&str> = ui
        .get("groups")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    assert!(
        groups.contains(&"test-group"),
        "client-cert O must map to a group; got {groups:?}"
    );
    assert!(
        groups.contains(&"system:authenticated"),
        "a cert-authenticated user is system:authenticated; got {groups:?}"
    );

    // (3) Bearer token, no cert → authenticated by token (cert + token coexist).
    let claims = ServiceAccountClaims::new(
        "probe-sa".to_string(),
        "kube-system".to_string(),
        "uid-probe".to_string(),
        1,
    );
    let token = token_manager.generate_token(claims).unwrap();
    let token_client = reqwest::Client::builder()
        .add_root_certificate(ca_root.clone())
        .use_rustls_tls()
        .build()
        .unwrap();
    let (status, ui) = self_subject_review(&token_client, &base, Some(&token)).await;
    assert_eq!(status, 200, "token SSR status; userInfo={ui:?}");
    assert_eq!(
        ui.get("username").and_then(Value::as_str),
        Some("system:serviceaccount:kube-system:probe-sa"),
        "bearer token must authenticate by token even with no client cert"
    );
}
