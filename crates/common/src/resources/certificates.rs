use crate::types::ObjectMeta;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// CertificateSigningRequest is used to request a certificate from a certificate authority
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateSigningRequest {
    #[serde(default = "default_api_version")]
    pub api_version: String,
    #[serde(default = "default_kind")]
    pub kind: String,
    pub metadata: ObjectMeta,
    /// CSR spec — optional to allow status-only patches/applies to deserialize
    #[serde(default)]
    pub spec: CertificateSigningRequestSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<CertificateSigningRequestStatus>,
}

fn default_api_version() -> String {
    "certificates.k8s.io/v1".to_string()
}

fn default_kind() -> String {
    "CertificateSigningRequest".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateSigningRequestSpec {
    /// Base64-encoded PKCS#10 CSR data
    #[serde(default)]
    pub request: String,

    /// signerName indicates the requested signer
    #[serde(default)]
    pub signer_name: String,

    /// expirationSeconds is the requested duration of validity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_seconds: Option<i32>,

    /// usages specifies a set of key usages requested in the issued certificate
    #[serde(default)]
    pub usages: Vec<KeyUsage>,

    /// username contains the name of the user that created the CertificateSigningRequest
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// uid contains the uid of the user that created the CertificateSigningRequest
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    /// groups contains group membership of the user that created the CertificateSigningRequest
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<String>>,

    /// extra contains extra attributes of the user that created the CertificateSigningRequest
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum KeyUsage {
    #[serde(rename = "signing")]
    Signing,
    #[serde(rename = "digital signature")]
    DigitalSignature,
    #[serde(rename = "content commitment")]
    ContentCommitment,
    #[serde(rename = "key encipherment")]
    KeyEncipherment,
    #[serde(rename = "key agreement")]
    KeyAgreement,
    #[serde(rename = "data encipherment")]
    DataEncipherment,
    #[serde(rename = "cert sign")]
    CertSign,
    #[serde(rename = "crl sign")]
    CRLSign,
    #[serde(rename = "encipher only")]
    EncipherOnly,
    #[serde(rename = "decipher only")]
    DecipherOnly,
    #[serde(rename = "any")]
    Any,
    #[serde(rename = "server auth")]
    ServerAuth,
    #[serde(rename = "client auth")]
    ClientAuth,
    #[serde(rename = "code signing")]
    CodeSigning,
    #[serde(rename = "email protection")]
    EmailProtection,
    #[serde(rename = "s/mime")]
    SMIME,
    #[serde(rename = "ipsec end system")]
    IPSECEndSystem,
    #[serde(rename = "ipsec tunnel")]
    IPSECTunnel,
    #[serde(rename = "ipsec user")]
    IPSECUser,
    #[serde(rename = "timestamping")]
    Timestamping,
    #[serde(rename = "ocsp signing")]
    OCSPSigning,
    #[serde(rename = "microsoft sgc")]
    MicrosoftSGC,
    #[serde(rename = "netscape sgc")]
    NetscapeSGC,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateSigningRequestStatus {
    /// conditions applied to the request
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<CertificateSigningRequestCondition>>,

    /// certificate is populated with an issued certificate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateSigningRequestCondition {
    /// type of the condition
    #[serde(rename = "type")]
    pub type_: String,

    /// status of the condition
    pub status: String,

    /// reason indicates a brief reason for the request state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// message contains a human readable message with details about the request state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// lastUpdateTime is the time of the last update to this condition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_update_time: Option<String>,

    /// lastTransitionTime is the time the condition last transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CertificateSigningRequestConditionType {
    Approved,
    Denied,
    Failed,
}
