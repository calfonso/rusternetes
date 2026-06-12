use crate::types::ConfigCommands;
use anyhow::Result;
use rusternetes_client::kubeconfig::{
    Cluster, ClusterEntry, Context, ContextEntry, KubeConfig, User, UserEntry,
};
use std::path::PathBuf;

/// Execute config commands for kubeconfig management
pub async fn execute(command: ConfigCommands, kubeconfig_path: Option<&str>) -> Result<()> {
    let config_path = if let Some(path) = kubeconfig_path {
        PathBuf::from(path)
    } else {
        KubeConfig::default_path()?
    };

    match command {
        ConfigCommands::CurrentContext {} => {
            let config = KubeConfig::load_from_file(&config_path)?;
            println!("{}", config.current_context);
        }
        ConfigCommands::View {
            minify: _,
            flatten: _,
        } => {
            let config = KubeConfig::load_from_file(&config_path)?;
            let yaml = serde_yaml::to_string(&config)?;
            println!("{}", yaml);
        }
        ConfigCommands::UseContext { name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            if !config.contexts.iter().any(|ctx| ctx.name == name) {
                anyhow::bail!("Context '{}' not found in kubeconfig", name);
            }
            config.current_context = name.clone();
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("Switched to context \"{}\"", name);
        }
        ConfigCommands::GetContexts { output: _ } => {
            let config = KubeConfig::load_from_file(&config_path)?;
            println!("CURRENT   NAME                CLUSTER             AUTHINFO");
            for ctx in &config.contexts {
                let current = if ctx.name == config.current_context {
                    "*"
                } else {
                    " "
                };
                println!(
                    "{}         {:<20} {:<20} {}",
                    current, ctx.name, ctx.context.cluster, ctx.context.user
                );
            }
        }
        ConfigCommands::GetClusters {} => {
            let config = KubeConfig::load_from_file(&config_path)?;
            println!("NAME");
            for cluster in &config.clusters {
                println!("{}", cluster.name);
            }
        }
        ConfigCommands::GetUsers {} => {
            let config = KubeConfig::load_from_file(&config_path)?;
            println!("NAME");
            for user in &config.users {
                println!("{}", user.name);
            }
        }
        ConfigCommands::Set { property, value } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let parts: Vec<&str> = property.split('.').collect();
            match parts.as_slice() {
                ["current-context"] => {
                    config.current_context = value.clone();
                    println!("Property \"current-context\" set");
                }
                ["contexts", ctx_name, "namespace"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.namespace = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.namespace\" set", ctx_name);
                }
                ["contexts", ctx_name, "cluster"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.cluster = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.cluster\" set", ctx_name);
                }
                ["contexts", ctx_name, "user"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.user = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.user\" set", ctx_name);
                }
                ["clusters", cluster_name, "server"] => {
                    let mut found = false;
                    for cluster in &mut config.clusters {
                        if cluster.name == *cluster_name {
                            cluster.cluster.server = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Cluster '{}' not found", cluster_name);
                    }
                    println!("Property \"clusters.{}.server\" set", cluster_name);
                }
                _ => {
                    anyhow::bail!("Unsupported property path: {}. Supported: current-context, contexts.<name>.namespace, contexts.<name>.cluster, contexts.<name>.user, clusters.<name>.server", property);
                }
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
        }
        ConfigCommands::Unset { property } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let parts: Vec<&str> = property.split('.').collect();
            match parts.as_slice() {
                ["current-context"] => {
                    config.current_context = String::new();
                    println!("Property \"current-context\" unset");
                }
                ["contexts", ctx_name, "namespace"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.namespace = String::new();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.namespace\" unset", ctx_name);
                }
                ["contexts", ctx_name, "cluster"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.cluster = String::new();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.cluster\" unset", ctx_name);
                }
                ["contexts", ctx_name, "user"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.user = String::new();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.user\" unset", ctx_name);
                }
                ["clusters", cluster_name, "server"] => {
                    let mut found = false;
                    for cluster in &mut config.clusters {
                        if cluster.name == *cluster_name {
                            cluster.cluster.server = String::new();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Cluster '{}' not found", cluster_name);
                    }
                    println!("Property \"clusters.{}.server\" unset", cluster_name);
                }
                _ => {
                    anyhow::bail!(
                        "Unsupported property path for unset: {}. Supported: current-context, contexts.<name>.namespace, contexts.<name>.cluster, contexts.<name>.user, clusters.<name>.server",
                        property
                    );
                }
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
        }
        ConfigCommands::DeleteCluster { name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let initial_len = config.clusters.len();
            config.clusters.retain(|c| c.name != name);
            if config.clusters.len() == initial_len {
                anyhow::bail!(
                    "cannot delete cluster \"{}\", not in {}",
                    name,
                    config_path.display()
                );
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!(
                "deleted cluster \"{}\" from {}",
                name,
                config_path.display()
            );
        }
        ConfigCommands::DeleteContext { name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let initial_len = config.contexts.len();
            config.contexts.retain(|c| c.name != name);
            if config.contexts.len() == initial_len {
                anyhow::bail!(
                    "cannot delete context \"{}\", not in {}",
                    name,
                    config_path.display()
                );
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!(
                "deleted context \"{}\" from {}",
                name,
                config_path.display()
            );
        }
        ConfigCommands::DeleteUser { name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let initial_len = config.users.len();
            config.users.retain(|u| u.name != name);
            if config.users.len() == initial_len {
                anyhow::bail!(
                    "cannot delete user \"{}\", not in {}",
                    name,
                    config_path.display()
                );
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("deleted user \"{}\" from {}", name, config_path.display());
        }
        ConfigCommands::RenameContext { old_name, new_name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            if config.contexts.iter().any(|c| c.name == new_name) {
                anyhow::bail!(
                    "cannot rename context \"{}\": context \"{}\" already exists",
                    old_name,
                    new_name
                );
            }
            let mut found = false;
            for ctx in &mut config.contexts {
                if ctx.name == old_name {
                    ctx.name = new_name.clone();
                    found = true;
                    break;
                }
            }
            if !found {
                anyhow::bail!("cannot rename context \"{}\": context not found", old_name);
            }
            if config.current_context == old_name {
                config.current_context = new_name.clone();
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("Context \"{}\" renamed to \"{}\".", old_name, new_name);
        }
        ConfigCommands::SetCluster {
            name,
            server,
            certificate_authority,
            certificate_authority_data,
            insecure_skip_tls_verify,
        } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let cluster_entry = config.clusters.iter_mut().find(|c| c.name == name);
            if let Some(entry) = cluster_entry {
                if let Some(s) = server {
                    entry.cluster.server = s;
                }
                if certificate_authority.is_some() {
                    entry.cluster.certificate_authority = certificate_authority;
                }
                if certificate_authority_data.is_some() {
                    entry.cluster.certificate_authority_data = certificate_authority_data;
                }
                if insecure_skip_tls_verify.is_some() {
                    entry.cluster.insecure_skip_tls_verify = insecure_skip_tls_verify;
                }
            } else {
                config.clusters.push(ClusterEntry {
                    name: name.clone(),
                    cluster: Cluster {
                        server: server.unwrap_or_default(),
                        certificate_authority,
                        certificate_authority_data,
                        insecure_skip_tls_verify,
                    },
                });
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("Cluster \"{}\" set.", name);
        }
        ConfigCommands::SetContext {
            name,
            cluster,
            user,
            namespace,
        } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let ctx_entry = config.contexts.iter_mut().find(|c| c.name == name);
            if let Some(entry) = ctx_entry {
                if let Some(c) = cluster {
                    entry.context.cluster = c;
                }
                if let Some(u) = user {
                    entry.context.user = u;
                }
                if let Some(ns) = namespace {
                    entry.context.namespace = ns;
                }
            } else {
                config.contexts.push(ContextEntry {
                    name: name.clone(),
                    context: Context {
                        cluster: cluster.unwrap_or_default(),
                        user: user.unwrap_or_default(),
                        namespace: namespace.unwrap_or_else(|| "default".to_string()),
                    },
                });
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("Context \"{}\" modified.", name);
        }
        ConfigCommands::SetCredentials {
            name,
            token,
            username,
            password,
            client_certificate,
            client_key,
            client_certificate_data,
            client_key_data,
        } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let user_entry = config.users.iter_mut().find(|u| u.name == name);
            if let Some(entry) = user_entry {
                if token.is_some() {
                    entry.user.token = token;
                }
                if username.is_some() {
                    entry.user.username = username;
                }
                if password.is_some() {
                    entry.user.password = password;
                }
                if client_certificate.is_some() {
                    entry.user.client_certificate = client_certificate;
                }
                if client_key.is_some() {
                    entry.user.client_key = client_key;
                }
                if client_certificate_data.is_some() {
                    entry.user.client_certificate_data = client_certificate_data;
                }
                if client_key_data.is_some() {
                    entry.user.client_key_data = client_key_data;
                }
            } else {
                config.users.push(UserEntry {
                    name: name.clone(),
                    user: User {
                        client_certificate,
                        client_certificate_data,
                        client_key,
                        client_key_data,
                        token,
                        username,
                        password,
                        auth_provider: None,
                        exec: None,
                    },
                });
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("User \"{}\" set.", name);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_kubeconfig() -> (NamedTempFile, PathBuf) {
        let yaml = r#"apiVersion: v1
kind: Config
current-context: test-ctx
contexts:
- name: test-ctx
  context:
    cluster: test-cluster
    user: test-user
    namespace: default
- name: other-ctx
  context:
    cluster: other-cluster
    user: other-user
    namespace: kube-system
clusters:
- name: test-cluster
  cluster:
    server: https://localhost:6443
- name: other-cluster
  cluster:
    server: https://other:6443
users:
- name: test-user
  user:
    token: test-token
- name: other-user
  user:
    token: other-token
"#;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        let path = f.path().to_path_buf();
        (f, path)
    }

    #[tokio::test]
    async fn test_config_current_context() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::CurrentContext {},
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_config_use_context_valid() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::UseContext {
                name: "other-ctx".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_ok());
        let updated = KubeConfig::load_from_file(&path).unwrap();
        assert_eq!(updated.current_context, "other-ctx");
    }

    #[tokio::test]
    async fn test_config_use_context_invalid() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::UseContext {
                name: "nonexistent".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_cluster() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::DeleteCluster {
                name: "other-cluster".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        assert_eq!(config.clusters.len(), 1);
        assert_eq!(config.clusters[0].name, "test-cluster");
    }

    #[tokio::test]
    async fn test_delete_cluster_not_found() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::DeleteCluster {
                name: "nonexistent".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_context() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::DeleteContext {
                name: "other-ctx".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        assert_eq!(config.contexts.len(), 1);
        assert_eq!(config.contexts[0].name, "test-ctx");
    }

    #[tokio::test]
    async fn test_delete_user() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::DeleteUser {
                name: "other-user".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        assert_eq!(config.users.len(), 1);
        assert_eq!(config.users[0].name, "test-user");
    }

    #[tokio::test]
    async fn test_get_users() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(ConfigCommands::GetUsers {}, Some(path.to_str().unwrap())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rename_context() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::RenameContext {
                old_name: "other-ctx".to_string(),
                new_name: "renamed-ctx".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        assert!(config.contexts.iter().any(|c| c.name == "renamed-ctx"));
        assert!(!config.contexts.iter().any(|c| c.name == "other-ctx"));
    }

    #[tokio::test]
    async fn test_rename_context_updates_current() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::RenameContext {
                old_name: "test-ctx".to_string(),
                new_name: "renamed-current".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        assert_eq!(config.current_context, "renamed-current");
    }

    #[tokio::test]
    async fn test_set_cluster_new() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetCluster {
                name: "new-cluster".to_string(),
                server: Some("https://new:6443".to_string()),
                certificate_authority: None,
                certificate_authority_data: None,
                insecure_skip_tls_verify: Some(true),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let cluster = config
            .clusters
            .iter()
            .find(|c| c.name == "new-cluster")
            .unwrap();
        assert_eq!(cluster.cluster.server, "https://new:6443");
        assert_eq!(cluster.cluster.insecure_skip_tls_verify, Some(true));
    }

    #[tokio::test]
    async fn test_set_context_new() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetContext {
                name: "new-ctx".to_string(),
                cluster: Some("test-cluster".to_string()),
                user: Some("test-user".to_string()),
                namespace: Some("custom-ns".to_string()),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "new-ctx")
            .unwrap();
        assert_eq!(ctx.context.cluster, "test-cluster");
        assert_eq!(ctx.context.namespace, "custom-ns");
    }

    #[tokio::test]
    async fn test_set_credentials_new() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetCredentials {
                name: "new-user".to_string(),
                token: Some("my-token".to_string()),
                username: None,
                password: None,
                client_certificate: None,
                client_key: None,
                client_certificate_data: None,
                client_key_data: None,
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let user = config.users.iter().find(|u| u.name == "new-user").unwrap();
        assert_eq!(user.user.token, Some("my-token".to_string()));
    }

    #[tokio::test]
    async fn test_unset_current_context() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Unset {
                property: "current-context".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        assert_eq!(config.current_context, "");
    }

    #[tokio::test]
    async fn test_config_view() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::View {
                minify: false,
                flatten: false,
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_contexts() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::GetContexts { output: None },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_clusters() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(ConfigCommands::GetClusters {}, Some(path.to_str().unwrap())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_current_context_property() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Set {
                property: "current-context".to_string(),
                value: "other-ctx".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        assert_eq!(config.current_context, "other-ctx");
    }

    #[tokio::test]
    async fn test_set_context_namespace() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Set {
                property: "contexts.test-ctx.namespace".to_string(),
                value: "my-ns".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "test-ctx")
            .unwrap();
        assert_eq!(ctx.context.namespace, "my-ns");
    }

    #[tokio::test]
    async fn test_set_context_cluster() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Set {
                property: "contexts.test-ctx.cluster".to_string(),
                value: "new-cluster".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "test-ctx")
            .unwrap();
        assert_eq!(ctx.context.cluster, "new-cluster");
    }

    #[tokio::test]
    async fn test_set_context_user() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Set {
                property: "contexts.test-ctx.user".to_string(),
                value: "new-user".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "test-ctx")
            .unwrap();
        assert_eq!(ctx.context.user, "new-user");
    }

    #[tokio::test]
    async fn test_set_cluster_server() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Set {
                property: "clusters.test-cluster.server".to_string(),
                value: "https://new-server:6443".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let cluster = config
            .clusters
            .iter()
            .find(|c| c.name == "test-cluster")
            .unwrap();
        assert_eq!(cluster.cluster.server, "https://new-server:6443");
    }

    #[tokio::test]
    async fn test_set_unsupported_property() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::Set {
                property: "unsupported.path".to_string(),
                value: "val".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_context_not_found() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::Set {
                property: "contexts.nonexistent.namespace".to_string(),
                value: "ns".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unset_context_namespace() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Unset {
                property: "contexts.test-ctx.namespace".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "test-ctx")
            .unwrap();
        assert_eq!(ctx.context.namespace, "");
    }

    #[tokio::test]
    async fn test_unset_context_cluster() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Unset {
                property: "contexts.test-ctx.cluster".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "test-ctx")
            .unwrap();
        assert_eq!(ctx.context.cluster, "");
    }

    #[tokio::test]
    async fn test_unset_context_user() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Unset {
                property: "contexts.test-ctx.user".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "test-ctx")
            .unwrap();
        assert_eq!(ctx.context.user, "");
    }

    #[tokio::test]
    async fn test_unset_cluster_server() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::Unset {
                property: "clusters.test-cluster.server".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let cluster = config
            .clusters
            .iter()
            .find(|c| c.name == "test-cluster")
            .unwrap();
        assert_eq!(cluster.cluster.server, "");
    }

    #[tokio::test]
    async fn test_unset_unsupported_property() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::Unset {
                property: "bad.path".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unset_context_not_found() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::Unset {
                property: "contexts.nonexistent.namespace".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unset_cluster_not_found() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::Unset {
                property: "clusters.nonexistent.server".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_context_not_found() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::DeleteContext {
                name: "nonexistent".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_user_not_found() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::DeleteUser {
                name: "nonexistent".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rename_context_duplicate() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::RenameContext {
                old_name: "test-ctx".to_string(),
                new_name: "other-ctx".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rename_context_not_found() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::RenameContext {
                old_name: "nonexistent".to_string(),
                new_name: "new-name".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_cluster_update_existing() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetCluster {
                name: "test-cluster".to_string(),
                server: Some("https://updated:9443".to_string()),
                certificate_authority: None,
                certificate_authority_data: None,
                insecure_skip_tls_verify: None,
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let cluster = config
            .clusters
            .iter()
            .find(|c| c.name == "test-cluster")
            .unwrap();
        assert_eq!(cluster.cluster.server, "https://updated:9443");
    }

    #[tokio::test]
    async fn test_set_context_update_existing() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetContext {
                name: "test-ctx".to_string(),
                cluster: None,
                user: None,
                namespace: Some("updated-ns".to_string()),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "test-ctx")
            .unwrap();
        assert_eq!(ctx.context.namespace, "updated-ns");
        // Original cluster should be preserved
        assert_eq!(ctx.context.cluster, "test-cluster");
    }

    #[tokio::test]
    async fn test_set_credentials_update_existing() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetCredentials {
                name: "test-user".to_string(),
                token: Some("new-token".to_string()),
                username: None,
                password: None,
                client_certificate: None,
                client_key: None,
                client_certificate_data: None,
                client_key_data: None,
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let user = config.users.iter().find(|u| u.name == "test-user").unwrap();
        assert_eq!(user.user.token, Some("new-token".to_string()));
    }

    #[tokio::test]
    async fn test_set_cluster_with_ca_and_insecure() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetCluster {
                name: "new-cluster-full".to_string(),
                server: Some("https://full:6443".to_string()),
                certificate_authority: Some("/path/to/ca.crt".to_string()),
                certificate_authority_data: Some("base64data".to_string()),
                insecure_skip_tls_verify: Some(true),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let cluster = config
            .clusters
            .iter()
            .find(|c| c.name == "new-cluster-full")
            .unwrap();
        assert_eq!(cluster.cluster.server, "https://full:6443");
        assert_eq!(
            cluster.cluster.certificate_authority,
            Some("/path/to/ca.crt".to_string())
        );
        assert_eq!(
            cluster.cluster.certificate_authority_data,
            Some("base64data".to_string())
        );
        assert_eq!(cluster.cluster.insecure_skip_tls_verify, Some(true));
    }

    #[tokio::test]
    async fn test_set_credentials_with_cert_and_key() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetCredentials {
                name: "cert-user".to_string(),
                token: None,
                username: Some("admin".to_string()),
                password: Some("secret".to_string()),
                client_certificate: Some("/path/cert.pem".to_string()),
                client_key: Some("/path/key.pem".to_string()),
                client_certificate_data: None,
                client_key_data: None,
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let user = config.users.iter().find(|u| u.name == "cert-user").unwrap();
        assert_eq!(user.user.username, Some("admin".to_string()));
        assert_eq!(user.user.password, Some("secret".to_string()));
        assert_eq!(
            user.user.client_certificate,
            Some("/path/cert.pem".to_string())
        );
        assert_eq!(user.user.client_key, Some("/path/key.pem".to_string()));
    }

    #[tokio::test]
    async fn test_set_cluster_server_not_found() {
        let (_f, path) = create_test_kubeconfig();
        let result = execute(
            ConfigCommands::Set {
                property: "clusters.nonexistent.server".to_string(),
                value: "https://x:6443".to_string(),
            },
            Some(path.to_str().unwrap()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_credentials_with_cert_data() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetCredentials {
                name: "data-user".to_string(),
                token: None,
                username: None,
                password: None,
                client_certificate: None,
                client_key: None,
                client_certificate_data: Some("Y2VydC1kYXRh".to_string()),
                client_key_data: Some("a2V5LWRhdGE=".to_string()),
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let user = config.users.iter().find(|u| u.name == "data-user").unwrap();
        assert_eq!(
            user.user.client_certificate_data,
            Some("Y2VydC1kYXRh".to_string())
        );
        assert_eq!(user.user.client_key_data, Some("a2V5LWRhdGE=".to_string()));
    }

    #[tokio::test]
    async fn test_set_context_defaults_namespace_to_default() {
        let (_f, path) = create_test_kubeconfig();
        execute(
            ConfigCommands::SetContext {
                name: "minimal-ctx".to_string(),
                cluster: None,
                user: None,
                namespace: None,
            },
            Some(path.to_str().unwrap()),
        )
        .await
        .unwrap();
        let config = KubeConfig::load_from_file(&path).unwrap();
        let ctx = config
            .contexts
            .iter()
            .find(|c| c.name == "minimal-ctx")
            .unwrap();
        assert_eq!(ctx.context.namespace, "default");
    }
}
