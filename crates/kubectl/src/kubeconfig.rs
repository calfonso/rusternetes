use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct KubeConfig {
    pub api_version: Option<String>,
    pub kind: Option<String>,
    pub current_context: String,
    pub contexts: Vec<ContextEntry>,
    pub clusters: Vec<ClusterEntry>,
    pub users: Vec<UserEntry>,
    #[serde(default)]
    pub preferences: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContextEntry {
    pub name: String,
    pub context: Context,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Context {
    pub cluster: String,
    pub user: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

fn default_namespace() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterEntry {
    pub name: String,
    pub cluster: Cluster,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Cluster {
    pub server: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate_authority: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate_authority_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insecure_skip_tls_verify: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserEntry {
    pub name: String,
    pub user: User,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct User {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_certificate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_certificate_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_provider: Option<AuthProvider>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthProvider {
    pub name: String,
    pub config: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecConfig {
    pub api_version: String,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

impl KubeConfig {
    /// Load kubeconfig from the default location or KUBECONFIG environment variable
    pub fn load_default() -> Result<Self> {
        let path = Self::default_path()?;
        Self::load_from_file(&path)
    }

    /// Get the default kubeconfig path
    pub fn default_path() -> Result<PathBuf> {
        // Check KUBECONFIG environment variable first
        if let Ok(kubeconfig_env) = std::env::var("KUBECONFIG") {
            return Ok(PathBuf::from(kubeconfig_env));
        }

        // Fall back to ~/.kube/config
        let home = std::env::var("HOME")
            .map_err(|_| anyhow::anyhow!("HOME environment variable not set"))?;
        Ok(PathBuf::from(home).join(".kube").join("config"))
    }

    /// Load kubeconfig from a specific file
    pub fn load_from_file(path: &PathBuf) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read kubeconfig from {:?}: {}", path, e))?;
        let config: KubeConfig = serde_yaml::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("Failed to parse kubeconfig from {:?}: {}", path, e))?;
        Ok(config)
    }

    /// Get the current context
    pub fn get_current_context(&self) -> Result<&Context> {
        self.contexts
            .iter()
            .find(|c| c.name == self.current_context)
            .map(|c| &c.context)
            .ok_or_else(|| anyhow::anyhow!("Current context '{}' not found", self.current_context))
    }

    /// Get a specific context by name
    #[allow(dead_code)]
    pub fn get_context(&self, name: &str) -> Result<&Context> {
        self.contexts
            .iter()
            .find(|c| c.name == name)
            .map(|c| &c.context)
            .ok_or_else(|| anyhow::anyhow!("Context '{}' not found", name))
    }

    /// Get the cluster for a given context
    pub fn get_cluster(&self, context: &Context) -> Result<&Cluster> {
        self.clusters
            .iter()
            .find(|c| c.name == context.cluster)
            .map(|c| &c.cluster)
            .ok_or_else(|| anyhow::anyhow!("Cluster '{}' not found", context.cluster))
    }

    /// Get the user for a given context
    pub fn get_user(&self, context: &Context) -> Result<&User> {
        self.users
            .iter()
            .find(|u| u.name == context.user)
            .map(|u| &u.user)
            .ok_or_else(|| anyhow::anyhow!("User '{}' not found", context.user))
    }

    /// Get the server URL for the current context
    pub fn get_server(&self) -> Result<String> {
        let context = self.get_current_context()?;
        let cluster = self.get_cluster(context)?;
        Ok(cluster.server.clone())
    }

    /// Get the namespace for the current context
    pub fn get_namespace(&self) -> Result<String> {
        let context = self.get_current_context()?;
        Ok(context.namespace.clone())
    }

    /// Check if TLS verification should be skipped
    pub fn should_skip_tls_verify(&self) -> Result<bool> {
        let context = self.get_current_context()?;
        let cluster = self.get_cluster(context)?;
        Ok(cluster.insecure_skip_tls_verify.unwrap_or(false))
    }

    /// Get the authentication token if available
    pub fn get_token(&self) -> Result<Option<String>> {
        let context = self.get_current_context()?;
        let user = self.get_user(context)?;
        Ok(user.token.clone())
    }

    /// Get client certificate data if available (base64 encoded)
    #[allow(dead_code)]
    pub fn get_client_cert_data(&self) -> Result<Option<String>> {
        let context = self.get_current_context()?;
        let user = self.get_user(context)?;
        Ok(user.client_certificate_data.clone())
    }

    /// Get client key data if available (base64 encoded)
    #[allow(dead_code)]
    pub fn get_client_key_data(&self) -> Result<Option<String>> {
        let context = self.get_current_context()?;
        let user = self.get_user(context)?;
        Ok(user.client_key_data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kubeconfig() {
        let yaml = r#"
apiVersion: v1
kind: Config
current-context: minikube
contexts:
- name: minikube
  context:
    cluster: minikube
    user: minikube
    namespace: default
clusters:
- name: minikube
  cluster:
    server: https://192.168.49.2:8443
    certificate-authority-data: LS0tLS1...
users:
- name: minikube
  user:
    client-certificate-data: LS0tLS1...
    client-key-data: LS0tLS1...
"#;

        let config: KubeConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.current_context, "minikube");
        assert_eq!(config.contexts.len(), 1);
        assert_eq!(config.clusters.len(), 1);
        assert_eq!(config.users.len(), 1);
    }
}
