use crate::state::ApiServerState;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthStatus {
    status: String,
    checks: Vec<ComponentHealth>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComponentHealth {
    name: String,
    status: String,
    message: Option<String>,
}

/// Health check endpoint - liveness probe
/// Matches upstream k8s `pkg/server/healthz/healthz.go`: 200 with body `ok`.
/// Clients (kubectl wait loops, hydrophone, probes) grep stdout for `ok`,
/// so an empty body silently fails the check.
pub async fn healthz() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok")
}

/// Readiness check endpoint - readiness probe
/// Returns OK if the server is ready to accept requests
/// Checks:
/// - Storage (etcd) connectivity
pub async fn readyz(State(state): State<Arc<ApiServerState>>) -> (StatusCode, Json<HealthStatus>) {
    let mut checks = Vec::new();
    let mut all_healthy = true;

    // Check storage connectivity
    match check_storage(&state).await {
        Ok(_) => {
            checks.push(ComponentHealth {
                name: "storage".to_string(),
                status: "ok".to_string(),
                message: None,
            });
        }
        Err(e) => {
            all_healthy = false;
            checks.push(ComponentHealth {
                name: "storage".to_string(),
                status: "failed".to_string(),
                message: Some(e),
            });
        }
    }

    let status = if all_healthy {
        HealthStatus {
            status: "ok".to_string(),
            checks,
        }
    } else {
        HealthStatus {
            status: "degraded".to_string(),
            checks,
        }
    };

    let status_code = if all_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(status))
}

/// Detailed health check endpoint
/// Returns detailed status of all components
pub async fn healthz_verbose(
    State(state): State<Arc<ApiServerState>>,
) -> (StatusCode, Json<HealthStatus>) {
    let mut checks = Vec::new();
    let mut all_healthy = true;

    // Check storage
    match check_storage(&state).await {
        Ok(_) => {
            checks.push(ComponentHealth {
                name: "storage".to_string(),
                status: "ok".to_string(),
                message: Some("etcd connection healthy".to_string()),
            });
        }
        Err(e) => {
            all_healthy = false;
            checks.push(ComponentHealth {
                name: "storage".to_string(),
                status: "failed".to_string(),
                message: Some(e),
            });
        }
    }

    // Add metrics status
    checks.push(ComponentHealth {
        name: "metrics".to_string(),
        status: "ok".to_string(),
        message: Some("metrics collection active".to_string()),
    });

    let status = if all_healthy {
        HealthStatus {
            status: "healthy".to_string(),
            checks,
        }
    } else {
        HealthStatus {
            status: "unhealthy".to_string(),
            checks,
        }
    };

    let status_code = if all_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(status))
}

/// Check storage connectivity by listing namespaces
async fn check_storage(state: &Arc<ApiServerState>) -> Result<(), String> {
    use rusternetes_common::resources::namespace::Namespace;
    use rusternetes_storage::Storage;

    // Try to list namespaces as a health check
    state
        .storage
        .list::<Namespace>("/registry/namespaces/")
        .await
        .map_err(|e| format!("Storage check failed: {}", e))?;

    Ok(())
}

/// Metrics endpoint - returns Prometheus metrics
pub async fn metrics(State(state): State<Arc<ApiServerState>>) -> String {
    state.metrics.gather()
}

/// CPU profiling endpoint - returns pprof CPU profile
/// This endpoint captures CPU profiling data for performance analysis
#[cfg(feature = "pprof")]
pub async fn pprof_profile() -> Result<Vec<u8>, (StatusCode, String)> {
    use pprof::ProfilerGuard;
    use std::time::Duration;

    // Create a profiler guard
    let guard = ProfilerGuard::new(100).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to start profiler: {}", e),
        )
    })?;

    // Profile for 30 seconds
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Build the report
    match guard.report().build() {
        Ok(report) => {
            let mut buffer = Vec::new();
            report.flamegraph(&mut buffer).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to generate flamegraph: {}", e),
                )
            })?;
            Ok(buffer)
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to build profile report: {}", e),
        )),
    }
}

/// Heap profiling endpoint - returns heap allocation profile
/// This endpoint provides information about memory allocations
#[cfg(feature = "pprof")]
pub async fn pprof_heap() -> Result<String, (StatusCode, String)> {
    // In Rust, we don't have runtime heap profiling like Go
    // This would require integration with jemalloc or another allocator
    // For now, return a stub response
    Ok("Heap profiling requires jemalloc or similar allocator integration".to_string())
}

/// Goroutine profiling endpoint (Rust equivalent: thread/task info)
/// This endpoint provides information about active threads and async tasks
pub async fn pprof_goroutine() -> Result<String, (StatusCode, String)> {
    // Collect thread information
    let thread_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    // Get Tokio runtime stats if available
    let runtime_info = format!(
        "Active threads: {}\nNote: Detailed async task profiling requires additional runtime instrumentation\n",
        thread_count
    );

    Ok(runtime_info)
}

/// Block profiling endpoint stub
/// Returns information about blocking operations
pub async fn pprof_block() -> Result<String, (StatusCode, String)> {
    Ok("Block profiling in Rust requires custom instrumentation\n\
        Consider using tokio-console for async task blocking analysis"
        .to_string())
}

/// Mutex profiling endpoint stub
/// Returns information about mutex contention
pub async fn pprof_mutex() -> Result<String, (StatusCode, String)> {
    Ok("Mutex profiling in Rust requires custom instrumentation\n\
        Consider using parking_lot with contention metrics"
        .to_string())
}

/// Symbol lookup endpoint
/// Returns symbol information for profiling
pub async fn pprof_symbol() -> String {
    "Symbol lookup not implemented\n\
     Rust binaries contain symbol information in debug builds"
        .to_string()
}

/// Trace endpoint - returns execution trace
/// This endpoint captures execution traces for analysis
pub async fn pprof_trace() -> Result<String, (StatusCode, String)> {
    Ok(
        "Execution tracing in Rust requires tokio-console or tracing subscriber integration"
            .to_string(),
    )
}

/// OpenID Connect discovery endpoint
/// Returns the OIDC provider metadata for service account issuer discovery
pub async fn openid_configuration() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "issuer": "https://kubernetes.default.svc.cluster.local",
        "jwks_uri": "https://kubernetes.default.svc.cluster.local/openid/v1/jwks",
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"]
    }))
}

/// JSON Web Key Set endpoint
/// Returns the public keys used to verify service account tokens.
/// K8s ref: pkg/serviceaccount/openidmetadata.go — publicJWKSFromKeys
pub async fn openid_jwks() -> Json<serde_json::Value> {
    // Try to load the RSA public key from standard paths
    let key_paths = [
        "/etc/kubernetes/pki/sa.pub",
        "/root/.rusternetes/keys/sa-signing-key.pub",
    ];

    for path in &key_paths {
        if let Ok(pem_data) = std::fs::read(path) {
            if let Some(jwk) = rsa_pem_to_jwk(&pem_data) {
                return Json(serde_json::json!({ "keys": [jwk] }));
            }
        }
    }

    // No RSA key available — return empty JWKS
    Json(serde_json::json!({ "keys": [] }))
}

/// Convert an RSA public key PEM to JWK format for OIDC discovery.
/// K8s uses the jose library; we extract n and e from the DER directly.
fn rsa_pem_to_jwk(pem_data: &[u8]) -> Option<serde_json::Value> {
    use base64::Engine;

    // Parse PEM to get DER bytes
    let pem_str = std::str::from_utf8(pem_data).ok()?;
    let der_b64: String = pem_str
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(&der_b64)
        .ok()?;

    // Parse SubjectPublicKeyInfo DER to extract RSA n and e
    // ASN.1 structure: SEQUENCE { SEQUENCE { OID, NULL }, BIT STRING { SEQUENCE { INTEGER n, INTEGER e } } }
    let (n, e) = extract_rsa_params_from_spki(&der)?;

    // Compute key ID as SHA-256 thumbprint of JWK canonical form
    use sha2::{Digest, Sha256};
    let n_b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&n);
    let e_b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&e);

    let canonical = format!(r#"{{"e":"{}","kty":"RSA","n":"{}"}}"#, e_b64url, n_b64url);
    let kid = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(canonical.as_bytes()));

    Some(serde_json::json!({
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": kid,
        "n": n_b64url,
        "e": e_b64url,
    }))
}

/// Extract RSA modulus (n) and exponent (e) from a SubjectPublicKeyInfo DER.
fn extract_rsa_params_from_spki(der: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    // Quick ASN.1 DER parser for RSA SPKI
    // Skip outer SEQUENCE, inner SEQUENCE (OID+NULL), BIT STRING header
    let mut pos = 0;

    // Outer SEQUENCE
    if pos >= der.len() || der[pos] != 0x30 {
        return None;
    }
    pos += 1;
    pos = skip_length(der, pos)?;

    // Inner SEQUENCE (algorithm identifier)
    if pos >= der.len() || der[pos] != 0x30 {
        return None;
    }
    pos += 1;
    let inner_len = read_length(der, pos)?;
    pos = skip_length(der, pos)?;
    pos += inner_len; // Skip algorithm identifier

    // BIT STRING
    if pos >= der.len() || der[pos] != 0x03 {
        return None;
    }
    pos += 1;
    pos = skip_length(der, pos)?;
    if pos >= der.len() {
        return None;
    }
    pos += 1; // Skip unused bits byte (should be 0)

    // Inner SEQUENCE containing n and e
    if pos >= der.len() || der[pos] != 0x30 {
        return None;
    }
    pos += 1;
    pos = skip_length(der, pos)?;

    // INTEGER n
    if pos >= der.len() || der[pos] != 0x02 {
        return None;
    }
    pos += 1;
    let n_len = read_length(der, pos)?;
    pos = skip_length(der, pos)?;
    let mut n = der[pos..pos + n_len].to_vec();
    // Strip leading zero (ASN.1 uses it for positive numbers)
    if !n.is_empty() && n[0] == 0 {
        n.remove(0);
    }
    pos += n_len;

    // INTEGER e
    if pos >= der.len() || der[pos] != 0x02 {
        return None;
    }
    pos += 1;
    let e_len = read_length(der, pos)?;
    pos = skip_length(der, pos)?;
    let e = der[pos..pos + e_len].to_vec();

    Some((n, e))
}

fn read_length(der: &[u8], pos: usize) -> Option<usize> {
    if pos >= der.len() {
        return None;
    }
    if der[pos] & 0x80 == 0 {
        Some(der[pos] as usize)
    } else {
        let num_bytes = (der[pos] & 0x7f) as usize;
        let mut len = 0usize;
        for i in 0..num_bytes {
            if pos + 1 + i >= der.len() {
                return None;
            }
            len = (len << 8) | der[pos + 1 + i] as usize;
        }
        Some(len)
    }
}

fn skip_length(der: &[u8], pos: usize) -> Option<usize> {
    if pos >= der.len() {
        return None;
    }
    if der[pos] & 0x80 == 0 {
        Some(pos + 1)
    } else {
        let num_bytes = (der[pos] & 0x7f) as usize;
        Some(pos + 1 + num_bytes)
    }
}
