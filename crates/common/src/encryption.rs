// Encryption at rest for Kubernetes secrets
//
// This module provides encryption of data stored in etcd, particularly for Secrets.
// It follows the Kubernetes encryption provider model.

use aes_gcm::{
    aead::{Aead, KeyInit, Nonce},
    Aes256Gcm, Key,
};
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Encryption provider type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderType {
    /// AES-GCM 256-bit encryption
    #[serde(rename = "aescbc")]
    AesGcm,
    /// AWS KMS integration
    KMS,
    /// No encryption (identity)
    Identity,
    /// Secret box (NaCl)
    Secretbox,
}

/// Encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub resources: Vec<ResourceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConfig {
    pub resources: Vec<String>,
    pub providers: Vec<ProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProviderConfig {
    AesGcm {
        #[serde(rename = "aescbc")]
        aescbc: AesGcmConfig,
    },
    KMS {
        kms: KMSConfig,
    },
    Identity {
        identity: IdentityConfig,
    },
    Secretbox {
        secretbox: SecretboxConfig,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AesGcmConfig {
    pub keys: Vec<KeyConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyConfig {
    pub name: String,
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSConfig {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "cachesize")]
    pub cache_size: Option<u32>,
    pub timeout: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretboxConfig {
    pub keys: Vec<KeyConfig>,
}

/// Encryption provider trait
pub trait EncryptionProvider: Send + Sync {
    /// Encrypt data
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>>;

    /// Decrypt data
    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>>;

    /// Provider name
    fn name(&self) -> &str;
}

/// AES-GCM encryption provider
pub struct AesGcmProvider {
    cipher: Aes256Gcm,
    key_name: String,
}

impl AesGcmProvider {
    pub fn new(key: &[u8], key_name: String) -> Result<Self> {
        if key.len() != 32 {
            return Err(anyhow!("AES-GCM key must be 32 bytes (256 bits)"));
        }

        let key_array: [u8; 32] = key.try_into().map_err(|_| anyhow!("Invalid key length"))?;
        let cipher = Aes256Gcm::new(&Key::<Aes256Gcm>::from(key_array));
        info!(
            "AES-GCM encryption provider initialized with key '{}'",
            key_name
        );

        Ok(Self { cipher, key_name })
    }

    pub fn from_base64(encoded_key: &str, key_name: String) -> Result<Self> {
        let key = general_purpose::STANDARD
            .decode(encoded_key)
            .map_err(|e| anyhow!("Failed to decode base64 key: {}", e))?;

        Self::new(&key, key_name)
    }

    pub fn generate_key() -> [u8; 32] {
        use aes_gcm::aead::Generate;
        let key = Key::<Aes256Gcm>::generate();
        key.into()
    }
}

impl EncryptionProvider for AesGcmProvider {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        // Generate random nonce
        let nonce_bytes = Self::generate_nonce();
        let nonce = &nonce_bytes.into();

        // Encrypt
        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| anyhow!("Encryption failed: {}", e))?;

        // Prepend nonce to ciphertext
        let mut result = nonce_bytes.to_vec();
        result.extend_from_slice(&ciphertext);

        debug!(
            "Encrypted {} bytes with key '{}'",
            plaintext.len(),
            self.key_name
        );
        Ok(result)
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < 12 {
            return Err(anyhow!("Ciphertext too short"));
        }

        // Extract nonce (first 12 bytes)
        let nonce_bytes: [u8; 12] = ciphertext[..12]
            .try_into()
            .map_err(|_| anyhow!("Invalid nonce length"))?;

        // Decrypt
        let plaintext = self
            .cipher
            .decrypt(&nonce_bytes.into(), &ciphertext[12..])
            .map_err(|e| anyhow!("Decryption failed: {}", e))?;

        debug!(
            "Decrypted {} bytes with key '{}'",
            plaintext.len(),
            self.key_name
        );
        Ok(plaintext)
    }

    fn name(&self) -> &str {
        &self.key_name
    }
}

impl AesGcmProvider {
    fn generate_nonce() -> [u8; 12] {
        use aes_gcm::aead::Generate;
        Nonce::<Aes256Gcm>::generate().into()
    }
}

/// Identity provider (no encryption)
pub struct IdentityProvider;

impl EncryptionProvider for IdentityProvider {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }

    fn name(&self) -> &str {
        "identity"
    }
}

/// Encryption transformer that handles encryption for specific resources
pub struct EncryptionTransformer {
    providers: HashMap<String, Arc<dyn EncryptionProvider>>,
    resource_providers: HashMap<String, String>, // resource -> provider name
}

impl EncryptionTransformer {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
            resource_providers: HashMap::new(),
        }
    }

    pub fn from_config(config: EncryptionConfig) -> Result<Self> {
        let mut transformer = Self::new();

        for resource_config in config.resources {
            for _resource in &resource_config.resources {
                // Use the first provider for each resource
                if let Some(provider_config) = resource_config.providers.first() {
                    let (provider_name, provider): (String, Arc<dyn EncryptionProvider>) =
                        match provider_config {
                            ProviderConfig::AesGcm { aescbc } => {
                                if let Some(key_config) = aescbc.keys.first() {
                                    let provider = Arc::new(AesGcmProvider::from_base64(
                                        &key_config.secret,
                                        key_config.name.clone(),
                                    )?);
                                    (key_config.name.clone(), provider)
                                } else {
                                    return Err(anyhow!("No keys configured for aescbc provider"));
                                }
                            }
                            ProviderConfig::Identity { .. } => {
                                ("identity".to_string(), Arc::new(IdentityProvider))
                            }
                            ProviderConfig::KMS { kms } => {
                                warn!("KMS provider not yet implemented, using identity");
                                (kms.name.clone(), Arc::new(IdentityProvider))
                            }
                            ProviderConfig::Secretbox { .. } => {
                                warn!("Secretbox provider not yet implemented, using identity");
                                ("secretbox".to_string(), Arc::new(IdentityProvider))
                            }
                        };

                    transformer.add_provider(provider_name.clone(), provider);
                    for res in &resource_config.resources {
                        transformer.set_resource_provider(res.clone(), provider_name.clone());
                    }
                }
            }
        }

        info!(
            "Encryption transformer initialized with {} providers",
            transformer.providers.len()
        );
        Ok(transformer)
    }

    pub fn add_provider(&mut self, name: String, provider: Arc<dyn EncryptionProvider>) {
        self.providers.insert(name, provider);
    }

    pub fn set_resource_provider(&mut self, resource: String, provider_name: String) {
        self.resource_providers.insert(resource, provider_name);
    }

    pub fn encrypt_for_resource(&self, resource: &str, data: &[u8]) -> Result<Vec<u8>> {
        if let Some(provider_name) = self.resource_providers.get(resource) {
            if let Some(provider) = self.providers.get(provider_name) {
                return provider.encrypt(data);
            }
        }

        // Default: no encryption
        Ok(data.to_vec())
    }

    pub fn decrypt_for_resource(&self, resource: &str, data: &[u8]) -> Result<Vec<u8>> {
        if let Some(provider_name) = self.resource_providers.get(resource) {
            if let Some(provider) = self.providers.get(provider_name) {
                return provider.decrypt(data);
            }
        }

        // Default: no encryption
        Ok(data.to_vec())
    }

    pub fn should_encrypt(&self, resource: &str) -> bool {
        self.resource_providers.contains_key(resource)
    }
}

impl Default for EncryptionTransformer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes_gcm_encrypt_decrypt() {
        let key = AesGcmProvider::generate_key();
        let provider = AesGcmProvider::new(&key, "test-key".to_string()).unwrap();

        let plaintext = b"Hello, World!";
        let ciphertext = provider.encrypt(plaintext).unwrap();

        assert_ne!(plaintext.to_vec(), ciphertext);

        let decrypted = provider.decrypt(&ciphertext).unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_aes_gcm_from_base64() {
        let key = AesGcmProvider::generate_key();
        let encoded = general_purpose::STANDARD.encode(key);

        let provider = AesGcmProvider::from_base64(&encoded, "test-key".to_string()).unwrap();

        let plaintext = b"Test data";
        let ciphertext = provider.encrypt(plaintext).unwrap();
        let decrypted = provider.decrypt(&ciphertext).unwrap();

        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_identity_provider() {
        let provider = IdentityProvider;

        let plaintext = b"Hello, World!";
        let ciphertext = provider.encrypt(plaintext).unwrap();

        assert_eq!(plaintext.to_vec(), ciphertext);

        let decrypted = provider.decrypt(&ciphertext).unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_encryption_transformer() {
        let mut transformer = EncryptionTransformer::new();

        let key = AesGcmProvider::generate_key();
        let provider = Arc::new(AesGcmProvider::new(&key, "test-key".to_string()).unwrap());

        transformer.add_provider("test-key".to_string(), provider);
        transformer.set_resource_provider("secrets".to_string(), "test-key".to_string());

        let plaintext = b"Secret data";

        // Encrypt
        let ciphertext = transformer
            .encrypt_for_resource("secrets", plaintext)
            .unwrap();
        assert_ne!(plaintext.to_vec(), ciphertext);

        // Decrypt
        let decrypted = transformer
            .decrypt_for_resource("secrets", &ciphertext)
            .unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);

        // Non-encrypted resource
        let other_data = b"Other data";
        let result = transformer
            .encrypt_for_resource("pods", other_data)
            .unwrap();
        assert_eq!(other_data.to_vec(), result);
    }

    #[test]
    fn test_should_encrypt() {
        let mut transformer = EncryptionTransformer::new();
        transformer.set_resource_provider("secrets".to_string(), "aes".to_string());

        assert!(transformer.should_encrypt("secrets"));
        assert!(!transformer.should_encrypt("pods"));
    }
}
