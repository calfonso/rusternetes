//! A minimal X.509 certificate authority used by the CSR signing controller to
//! issue certificates for approved [`CertificateSigningRequest`]s.
//!
//! Mirrors the upstream kube-controller-manager signer
//! (`pkg/controller/certificates/signer` + `pkg/controller/certificates/authority`):
//! the signer parses the PKCS#10 request, builds an X.509 leaf whose Subject and
//! SANs come from the request, sets KeyUsage/ExtKeyUsage from the API
//! `spec.usages` (NOT the embedded CSR extensions — the API field is
//! authoritative upstream), and signs it with the cluster CA key, writing the
//! PEM into `status.certificate`.
//!
//! [`CertificateSigningRequest`]: rusternetes_common::resources::CertificateSigningRequest

use anyhow::{anyhow, Context, Result};
use chrono::Datelike;
use rcgen::{
    Certificate, CertificateParams, CertificateSigningRequestParams, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rusternetes_common::resources::KeyUsage;

/// Default issued-certificate lifetime when a CSR does not request one, matching
/// upstream's 1-year default (`pkg/controller/certificates/signer`).
const DEFAULT_DURATION_SECS: i64 = 365 * 24 * 60 * 60;

/// A cluster certificate authority capable of signing approved CSRs.
///
/// Built once from the cluster CA cert + key PEM. Cheap to share behind an
/// `Arc`; signing is a pure CPU operation with no shared mutable state.
pub struct CertificateAuthority {
    /// Issuer reconstructed from the CA cert params + key. Only its
    /// distinguished name, key-identifier method and signing key feed into
    /// `signed_by`, so leaves carry the real CA's issuer DN and are signed by
    /// the real CA key — i.e. they chain to the on-disk CA certificate.
    issuer: Certificate,
    ca_key: KeyPair,
    ca_cert_pem: String,
}

impl CertificateAuthority {
    /// Build a CA from the cluster CA certificate and private key, both PEM.
    pub fn from_pem(ca_cert_pem: &str, ca_key_pem: &str) -> Result<Self> {
        let ca_key = KeyPair::from_pem(ca_key_pem).context("parsing CA private key PEM")?;
        let params =
            CertificateParams::from_ca_cert_pem(ca_cert_pem).context("parsing CA certificate PEM")?;
        let issuer = params
            .self_signed(&ca_key)
            .context("reconstructing CA issuer certificate")?;
        Ok(Self {
            issuer,
            ca_key,
            ca_cert_pem: ca_cert_pem.to_string(),
        })
    }

    /// Sign an approved CSR, returning the issued leaf certificate as PEM.
    ///
    /// `request_pem` is the PKCS#10 `CERTIFICATE REQUEST` PEM (the CSR's
    /// `spec.request`). `usages` is the API `spec.usages` list, which is
    /// authoritative for the issued cert's KeyUsage/ExtKeyUsage. `expiration`
    /// is `spec.expirationSeconds`, defaulting to one year.
    pub fn sign(
        &self,
        request_pem: &str,
        usages: &[KeyUsage],
        expiration_seconds: Option<i32>,
    ) -> Result<String> {
        // Parse + verify the PKCS#10 request. `from_pem` also checks the CSR's
        // self-signature, so a tampered request is rejected here.
        let mut csr = CertificateSigningRequestParams::from_pem(request_pem)
            .map_err(|e| anyhow!("parsing certificate signing request: {e}"))?;

        // `spec.usages` is authoritative for the issued certificate's usages,
        // mirroring the upstream signer — override anything embedded in the
        // request's own extensions.
        let (key_usages, extended_key_usages) = map_usages(usages);
        csr.params.key_usages = key_usages;
        csr.params.extended_key_usages = extended_key_usages;

        // A signed leaf is never itself a CA.
        csr.params.is_ca = IsCa::ExplicitNoCa;

        // Validity window. `rcgen::date_time_ymd` gives day granularity (keeping
        // us off a direct `time` dependency); back-date `not_before` one day for
        // clock skew and round the lifetime up to at least one day so short
        // `expirationSeconds` requests still produce a currently-valid cert.
        let now = chrono::Utc::now();
        let secs = expiration_seconds
            .map(|s| i64::from(s).max(600))
            .unwrap_or(DEFAULT_DURATION_SECS);
        let not_before = now - chrono::Duration::days(1);
        let not_after = now + chrono::Duration::days((secs / 86_400).max(1));
        csr.params.not_before = rcgen::date_time_ymd(
            not_before.year(),
            not_before.month() as u8,
            not_before.day() as u8,
        );
        csr.params.not_after = rcgen::date_time_ymd(
            not_after.year(),
            not_after.month() as u8,
            not_after.day() as u8,
        );

        let cert = csr
            .signed_by(&self.issuer, &self.ca_key)
            .map_err(|e| anyhow!("signing certificate: {e}"))?;
        Ok(cert.pem())
    }

    /// The CA certificate PEM this authority signs with.
    pub fn ca_cert_pem(&self) -> &str {
        &self.ca_cert_pem
    }
}

/// Map the Kubernetes `spec.usages` strings to rcgen KeyUsage / ExtendedKeyUsage
/// purposes. Mirrors upstream `keyUsagesFromStrings`. IPsec and SGC usages have
/// no rcgen equivalent and are skipped (they are not used by the in-cluster
/// signers).
fn map_usages(usages: &[KeyUsage]) -> (Vec<KeyUsagePurpose>, Vec<ExtendedKeyUsagePurpose>) {
    use KeyUsage as K;
    let mut ku = Vec::new();
    let mut eku = Vec::new();
    for usage in usages {
        match usage {
            K::Signing | K::DigitalSignature => push_unique(&mut ku, KeyUsagePurpose::DigitalSignature),
            K::ContentCommitment => push_unique(&mut ku, KeyUsagePurpose::ContentCommitment),
            K::KeyEncipherment => push_unique(&mut ku, KeyUsagePurpose::KeyEncipherment),
            K::KeyAgreement => push_unique(&mut ku, KeyUsagePurpose::KeyAgreement),
            K::DataEncipherment => push_unique(&mut ku, KeyUsagePurpose::DataEncipherment),
            K::CertSign => push_unique(&mut ku, KeyUsagePurpose::KeyCertSign),
            K::CRLSign => push_unique(&mut ku, KeyUsagePurpose::CrlSign),
            K::EncipherOnly => push_unique(&mut ku, KeyUsagePurpose::EncipherOnly),
            K::DecipherOnly => push_unique(&mut ku, KeyUsagePurpose::DecipherOnly),
            K::ServerAuth => push_unique(&mut eku, ExtendedKeyUsagePurpose::ServerAuth),
            K::ClientAuth => push_unique(&mut eku, ExtendedKeyUsagePurpose::ClientAuth),
            K::CodeSigning => push_unique(&mut eku, ExtendedKeyUsagePurpose::CodeSigning),
            K::EmailProtection | K::SMIME => {
                push_unique(&mut eku, ExtendedKeyUsagePurpose::EmailProtection)
            }
            K::Timestamping => push_unique(&mut eku, ExtendedKeyUsagePurpose::TimeStamping),
            K::Any => push_unique(&mut eku, ExtendedKeyUsagePurpose::Any),
            // ipsec end system / tunnel / user, microsoft/netscape SGC: no rcgen
            // mapping, and unused by the in-cluster signers.
            _ => {}
        }
    }
    (ku, eku)
}

fn push_unique<T: PartialEq>(v: &mut Vec<T>, item: T) {
    if !v.contains(&item) {
        v.push(item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{BasicConstraints, CertificateParams, DnType, KeyPair};
    use x509_parser::prelude::*;

    /// Generate a self-signed CA, returning `(cert_pem, key_pem)`.
    fn make_ca() -> (String, String) {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "rusternetes-test-ca");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// Generate a PKCS#10 CSR PEM for the given subject common name.
    fn make_csr(common_name: &str) -> String {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        let key = KeyPair::generate().unwrap();
        params.serialize_request(&key).unwrap().pem().unwrap()
    }

    fn parse<'a>(pem: &'a ::pem::Pem) -> X509Certificate<'a> {
        X509Certificate::from_der(pem.contents()).unwrap().1
    }

    #[test]
    fn sign_issues_cert_that_chains_to_the_ca() {
        let (ca_cert_pem, ca_key_pem) = make_ca();
        let ca = CertificateAuthority::from_pem(&ca_cert_pem, &ca_key_pem).unwrap();
        let csr = make_csr("system:node:node-1");

        let issued = ca
            .sign(
                &csr,
                &[KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
                None,
            )
            .expect("signing an approved CSR must succeed");

        let issued_pem = ::pem::parse(&issued).expect("issued cert must be valid PEM");
        let ca_pem = ::pem::parse(&ca_cert_pem).unwrap();
        let leaf = parse(&issued_pem);
        let ca_x = parse(&ca_pem);

        // The leaf carries the requested subject.
        assert!(
            leaf.subject().to_string().contains("system:node:node-1"),
            "leaf subject must come from the CSR; got {}",
            leaf.subject()
        );
        // The leaf's issuer is the CA's subject — and its signature verifies
        // against the CA public key, i.e. it chains to the on-disk CA.
        assert_eq!(
            leaf.issuer().to_string(),
            ca_x.subject().to_string(),
            "leaf issuer DN must equal CA subject DN"
        );
        leaf.verify_signature(Some(ca_x.public_key()))
            .expect("issued leaf must be signed by the CA key");
    }

    #[test]
    fn sign_applies_usages_from_spec_not_csr() {
        let (ca_cert_pem, ca_key_pem) = make_ca();
        let ca = CertificateAuthority::from_pem(&ca_cert_pem, &ca_key_pem).unwrap();
        let csr = make_csr("serving.example.com");

        let issued = ca
            .sign(
                &csr,
                &[
                    KeyUsage::DigitalSignature,
                    KeyUsage::KeyEncipherment,
                    KeyUsage::ServerAuth,
                ],
                Some(3600),
            )
            .unwrap();

        let issued_pem = ::pem::parse(&issued).unwrap();
        let leaf = parse(&issued_pem);

        let eku = leaf
            .extended_key_usage()
            .unwrap()
            .expect("issued cert must carry extended key usage")
            .value;
        assert!(eku.server_auth, "server auth EKU must be set from spec.usages");
        assert!(!eku.client_auth, "client auth must NOT be set (not requested)");

        let ku = leaf
            .key_usage()
            .unwrap()
            .expect("issued cert must carry key usage")
            .value;
        assert!(ku.digital_signature(), "digital signature KU must be set");
        assert!(ku.key_encipherment(), "key encipherment KU must be set");
    }

    #[test]
    fn sign_rejects_a_malformed_request() {
        let (ca_cert_pem, ca_key_pem) = make_ca();
        let ca = CertificateAuthority::from_pem(&ca_cert_pem, &ca_key_pem).unwrap();
        let err = ca.sign("-----BEGIN CERTIFICATE REQUEST-----\nnot-a-csr\n-----END CERTIFICATE REQUEST-----\n", &[KeyUsage::ClientAuth], None);
        assert!(err.is_err(), "a malformed CSR must be rejected, not signed");
    }
}
