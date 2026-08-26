use crate::client::{ApiClient, GetError};
use crate::types::ApplyCommands;
use anyhow::{Context, Result};
use rusternetes_common::resources::{
    ClusterRole, ClusterRoleBinding, ConfigMap, CronJob, CustomResourceDefinition, DaemonSet,
    Deployment, Endpoints, Ingress, Job, LimitRange, Namespace, Node, PersistentVolume,
    PersistentVolumeClaim, Pod, PriorityClass, ResourceQuota, Role, RoleBinding, Secret, Service,
    ServiceAccount, StatefulSet, StorageClass, VolumeSnapshot, VolumeSnapshotClass,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use walkdir::WalkDir;

/// Options controlling how `kubectl apply` behaves.
#[derive(Debug, Clone)]
pub struct ApplyOptions {
    /// One or more file paths or directories.
    pub files: Vec<String>,
    /// Optional namespace override.
    pub namespace: Option<String>,
    /// Dry-run mode string (e.g. "client", "server", "none").
    pub dry_run: Option<String>,
    /// Use server-side apply.
    pub server_side: bool,
    /// Force apply (resolve conflicts).
    pub force: bool,
    /// Recurse into directories.
    pub recursive: bool,
    /// Field manager name for server-side apply.
    pub field_manager: String,
    /// Output format: json, yaml, or name.
    pub output: Option<String>,
    /// Validation directive: true/strict, false/ignore, warn.
    pub validate: Option<String>,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            files: Vec::new(),
            namespace: None,
            dry_run: None,
            server_side: false,
            force: false,
            recursive: false,
            field_manager: "kubectl-client-side-apply".to_string(),
            output: None,
            validate: None,
        }
    }
}

/// Legacy entry point — kept for backward compat.
#[allow(dead_code)]
pub async fn execute_enhanced(
    client: &ApiClient,
    file: &str,
    namespace: Option<&str>,
    dry_run: Option<&str>,
    server_side: bool,
    force: bool,
) -> Result<()> {
    let options = ApplyOptions {
        files: vec![file.to_string()],
        namespace: namespace.map(|s| s.to_string()),
        dry_run: dry_run.map(|s| s.to_string()),
        server_side,
        force,
        ..Default::default()
    };
    execute_with_options(client, &options).await
}

/// Main entry point that honours all ApplyOptions.
pub async fn execute_with_options(client: &ApiClient, options: &ApplyOptions) -> Result<()> {
    // Build the list of file paths to process.
    let file_paths = collect_files(&options.files, options.recursive)?;

    if file_paths.is_empty() {
        anyhow::bail!("no files to apply — specify at least one file with -f");
    }

    // Build query-parameter suffix.
    let query = build_query_string(options);

    for path in &file_paths {
        let contents = if path == "-" {
            let mut buffer = String::new();
            io::stdin()
                .read_to_string(&mut buffer)
                .context("Failed to read from stdin")?;
            buffer
        } else {
            fs::read_to_string(path).with_context(|| format!("Failed to read file: {}", path))?
        };

        // Support for multi-document YAML files
        for document in serde_yaml::Deserializer::from_str(&contents) {
            let value = serde_yaml::Value::deserialize(document)?;

            // Skip empty documents
            if value.is_null() {
                continue;
            }

            let result = apply_resource(client, &value, &query, options).await?;

            // Format output based on --output flag
            format_output(&result, options);
        }
    }

    Ok(())
}

/// Legacy entry point.
#[allow(dead_code)]
pub async fn execute(client: &ApiClient, file: &str) -> Result<()> {
    execute_enhanced(client, file, None, None, false, false).await
}

// ---------------------------------------------------------------------------
// File collection
// ---------------------------------------------------------------------------

/// Collect file paths from the supplied list, expanding directories.
pub fn collect_files(inputs: &[String], recursive: bool) -> Result<Vec<String>> {
    let mut result = Vec::new();

    for input in inputs {
        if input == "-" {
            result.push("-".to_string());
            continue;
        }

        let path = Path::new(input);
        if path.is_dir() {
            if recursive {
                for entry in WalkDir::new(path).sort_by_file_name() {
                    let entry = entry.with_context(|| {
                        format!("Failed to read directory entry under {}", input)
                    })?;
                    if entry.file_type().is_file() && is_manifest_file(entry.path()) {
                        result.push(entry.path().to_string_lossy().to_string());
                    }
                }
            } else {
                // Non-recursive: only direct children.
                let mut entries: Vec<_> = fs::read_dir(path)
                    .with_context(|| format!("Failed to read directory: {}", input))?
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.file_type().map(|ft| ft.is_file()).unwrap_or(false)
                            && is_manifest_file(&e.path())
                    })
                    .collect();
                entries.sort_by_key(|e| e.file_name());
                for entry in entries {
                    result.push(entry.path().to_string_lossy().to_string());
                }
            }
        } else if path.is_file() || input == "-" {
            result.push(input.to_string());
        } else {
            anyhow::bail!("path does not exist: {}", input);
        }
    }

    Ok(result)
}

/// Return true if the file extension indicates a Kubernetes manifest.
fn is_manifest_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("yaml") | Some("yml") | Some("json")
    )
}

// ---------------------------------------------------------------------------
// Query string building
// ---------------------------------------------------------------------------

fn build_query_string(options: &ApplyOptions) -> String {
    let mut params: Vec<String> = Vec::new();

    if let Some(ref dr) = options.dry_run {
        if dr == "server" || dr == "client" || dr == "none" {
            // For server-side dry run we pass the query param.
            if dr == "server" {
                params.push("dryRun=All".to_string());
            }
        }
    }

    if options.server_side {
        params.push(format!("fieldManager={}", options.field_manager));
        if options.force {
            params.push("force=true".to_string());
        }
    }

    if let Some(ref v) = options.validate {
        let directive = match v.as_str() {
            "true" | "strict" | "Strict" => "Strict",
            "warn" | "Warn" => "Warn",
            "false" | "ignore" | "Ignore" => "Ignore",
            other => other,
        };
        params.push(format!("fieldValidation={}", directive));
    }

    if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    }
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

/// Information returned from a single apply operation for output formatting.
#[derive(Debug, Clone)]
struct ApplyResult {
    kind: String,
    api_group: String,
    name: String,
    #[allow(dead_code)]
    namespace: Option<String>,
    action: ApplyAction,
    response: Value,
}

#[derive(Debug, Clone, PartialEq)]
enum ApplyAction {
    Created,
    Configured,
}

impl ApplyResult {
    /// Return the resource label like "deployment.apps/nginx".
    fn resource_label(&self) -> String {
        let kind_lower = self.kind.to_lowercase();
        if self.api_group.is_empty() {
            format!("{}/{}", kind_lower, self.name)
        } else {
            format!("{}.{}/{}", kind_lower, self.api_group, self.name)
        }
    }
}

fn format_output(result: &ApplyResult, options: &ApplyOptions) {
    match options.output.as_deref() {
        Some("json") => {
            if let Ok(pretty) = serde_json::to_string_pretty(&result.response) {
                println!("{}", pretty);
            }
        }
        Some("yaml") => {
            if let Ok(yaml) = serde_yaml::to_string(&result.response) {
                print!("{}", yaml);
            }
        }
        Some("name") => {
            println!("{}", result.resource_label());
        }
        _ => {
            // Default: standard kubectl output
            let action = match result.action {
                ApplyAction::Created => "created",
                ApplyAction::Configured => "configured",
            };
            println!("{} {}", result.resource_label(), action);
        }
    }
}

// ---------------------------------------------------------------------------
// Resource existence check
// ---------------------------------------------------------------------------

async fn resource_exists<T: DeserializeOwned>(client: &ApiClient, path: &str) -> Result<bool> {
    match client.get::<T>(path).await {
        Ok(_) => Ok(true),
        Err(GetError::NotFound) => Ok(false),
        Err(GetError::Other(e)) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// last-applied-configuration annotation
// ---------------------------------------------------------------------------

const LAST_APPLIED_ANNOTATION: &str = "kubectl.kubernetes.io/last-applied-configuration";

/// Inject the `last-applied-configuration` annotation into a JSON Value
/// representing the resource being applied.  The annotation stores the
/// *original* input config (before the annotation itself was added) serialised
/// as compact JSON, matching the behaviour of real kubectl.
fn set_last_applied_annotation(value: &mut Value) {
    // Build the annotation value from the *clean* config (without the
    // annotation itself) — this prevents recursive growth.
    let clean = strip_last_applied(value);
    let annotation_value = serde_json::to_string(&clean).unwrap_or_default();

    // Ensure metadata.annotations exists.
    let metadata = value.as_object_mut().and_then(|o| {
        o.entry("metadata")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .map(|m| m as *mut _)
    });

    if let Some(metadata) = metadata {
        let metadata: &mut serde_json::Map<String, Value> = unsafe { &mut *metadata };
        let annotations = metadata.entry("annotations").or_insert_with(|| json!({}));
        if let Some(ann) = annotations.as_object_mut() {
            ann.insert(
                LAST_APPLIED_ANNOTATION.to_string(),
                Value::String(annotation_value),
            );
        }
    }
}

/// Return a copy of the value with the last-applied annotation removed
/// (used to build the annotation value without recursion).
fn strip_last_applied(value: &Value) -> Value {
    let mut clean = value.clone();
    if let Some(metadata) = clean.get_mut("metadata") {
        if let Some(annotations) = metadata.get_mut("annotations") {
            if let Some(ann) = annotations.as_object_mut() {
                ann.remove(LAST_APPLIED_ANNOTATION);
                if ann.is_empty() {
                    // Remove empty annotations map entirely.
                    if let Some(m) = metadata.as_object_mut() {
                        m.remove("annotations");
                    }
                }
            }
        }
    }
    clean
}

// ---------------------------------------------------------------------------
// Core apply logic
// ---------------------------------------------------------------------------

async fn apply_resource(
    client: &ApiClient,
    value: &serde_yaml::Value,
    query: &str,
    options: &ApplyOptions,
) -> Result<ApplyResult> {
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field")?;

    let yaml_str = serde_yaml::to_string(value)?;

    match kind {
        "Pod" => {
            apply_namespaced::<Pod>(
                client, &yaml_str, kind, "", "pods", "/api/v1", query, options,
            )
            .await
        }
        "Service" => {
            apply_namespaced::<Service>(
                client, &yaml_str, kind, "", "services", "/api/v1", query, options,
            )
            .await
        }
        "Deployment" => {
            apply_namespaced::<Deployment>(
                client,
                &yaml_str,
                kind,
                "apps",
                "deployments",
                "/apis/apps/v1",
                query,
                options,
            )
            .await
        }
        "StatefulSet" => {
            apply_namespaced::<StatefulSet>(
                client,
                &yaml_str,
                kind,
                "apps",
                "statefulsets",
                "/apis/apps/v1",
                query,
                options,
            )
            .await
        }
        "DaemonSet" => {
            apply_namespaced::<DaemonSet>(
                client,
                &yaml_str,
                kind,
                "apps",
                "daemonsets",
                "/apis/apps/v1",
                query,
                options,
            )
            .await
        }
        "Job" => {
            apply_namespaced::<Job>(
                client,
                &yaml_str,
                kind,
                "batch",
                "jobs",
                "/apis/batch/v1",
                query,
                options,
            )
            .await
        }
        "CronJob" => {
            apply_namespaced::<CronJob>(
                client,
                &yaml_str,
                kind,
                "batch",
                "cronjobs",
                "/apis/batch/v1",
                query,
                options,
            )
            .await
        }
        "ConfigMap" => {
            apply_namespaced::<ConfigMap>(
                client,
                &yaml_str,
                kind,
                "",
                "configmaps",
                "/api/v1",
                query,
                options,
            )
            .await
        }
        "Secret" => {
            apply_namespaced::<Secret>(
                client, &yaml_str, kind, "", "secrets", "/api/v1", query, options,
            )
            .await
        }
        "ServiceAccount" => {
            apply_namespaced::<ServiceAccount>(
                client,
                &yaml_str,
                kind,
                "",
                "serviceaccounts",
                "/api/v1",
                query,
                options,
            )
            .await
        }
        "Endpoints" => {
            apply_namespaced::<Endpoints>(
                client,
                &yaml_str,
                kind,
                "",
                "endpoints",
                "/api/v1",
                query,
                options,
            )
            .await
        }
        "PersistentVolumeClaim" => {
            apply_namespaced::<PersistentVolumeClaim>(
                client,
                &yaml_str,
                kind,
                "",
                "persistentvolumeclaims",
                "/api/v1",
                query,
                options,
            )
            .await
        }
        "ResourceQuota" => {
            apply_namespaced::<ResourceQuota>(
                client,
                &yaml_str,
                kind,
                "",
                "resourcequotas",
                "/api/v1",
                query,
                options,
            )
            .await
        }
        "LimitRange" => {
            apply_namespaced::<LimitRange>(
                client,
                &yaml_str,
                kind,
                "",
                "limitranges",
                "/api/v1",
                query,
                options,
            )
            .await
        }
        "Role" => {
            apply_namespaced::<Role>(
                client,
                &yaml_str,
                kind,
                "rbac.authorization.k8s.io",
                "roles",
                "/apis/rbac.authorization.k8s.io/v1",
                query,
                options,
            )
            .await
        }
        "RoleBinding" => {
            apply_namespaced::<RoleBinding>(
                client,
                &yaml_str,
                kind,
                "rbac.authorization.k8s.io",
                "rolebindings",
                "/apis/rbac.authorization.k8s.io/v1",
                query,
                options,
            )
            .await
        }
        "Ingress" => {
            apply_namespaced::<Ingress>(
                client,
                &yaml_str,
                kind,
                "networking.k8s.io",
                "ingresses",
                "/apis/networking.k8s.io/v1",
                query,
                options,
            )
            .await
        }
        "VolumeSnapshot" => {
            apply_namespaced::<VolumeSnapshot>(
                client,
                &yaml_str,
                kind,
                "snapshot.storage.k8s.io",
                "volumesnapshots",
                "/apis/snapshot.storage.k8s.io/v1",
                query,
                options,
            )
            .await
        }
        // Cluster-scoped resources
        "Namespace" => {
            apply_cluster::<Namespace>(
                client,
                &yaml_str,
                kind,
                "",
                "namespaces",
                "/api/v1/namespaces",
                query,
                options,
            )
            .await
        }
        "Node" => {
            apply_cluster::<Node>(
                client,
                &yaml_str,
                kind,
                "",
                "nodes",
                "/api/v1/nodes",
                query,
                options,
            )
            .await
        }
        "PersistentVolume" => {
            apply_cluster::<PersistentVolume>(
                client,
                &yaml_str,
                kind,
                "",
                "persistentvolumes",
                "/api/v1/persistentvolumes",
                query,
                options,
            )
            .await
        }
        "ClusterRole" => {
            apply_cluster::<ClusterRole>(
                client,
                &yaml_str,
                kind,
                "rbac.authorization.k8s.io",
                "clusterroles",
                "/apis/rbac.authorization.k8s.io/v1/clusterroles",
                query,
                options,
            )
            .await
        }
        "ClusterRoleBinding" => {
            apply_cluster::<ClusterRoleBinding>(
                client,
                &yaml_str,
                kind,
                "rbac.authorization.k8s.io",
                "clusterrolebindings",
                "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
                query,
                options,
            )
            .await
        }
        "StorageClass" => {
            apply_cluster::<StorageClass>(
                client,
                &yaml_str,
                kind,
                "storage.k8s.io",
                "storageclasses",
                "/apis/storage.k8s.io/v1/storageclasses",
                query,
                options,
            )
            .await
        }
        "VolumeSnapshotClass" => {
            apply_cluster::<VolumeSnapshotClass>(
                client,
                &yaml_str,
                kind,
                "snapshot.storage.k8s.io",
                "volumesnapshotclasses",
                "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses",
                query,
                options,
            )
            .await
        }
        "PriorityClass" => {
            apply_cluster::<PriorityClass>(
                client,
                &yaml_str,
                kind,
                "scheduling.k8s.io",
                "priorityclasses",
                "/apis/scheduling.k8s.io/v1/priorityclasses",
                query,
                options,
            )
            .await
        }
        "CustomResourceDefinition" => {
            apply_cluster::<CustomResourceDefinition>(
                client,
                &yaml_str,
                kind,
                "apiextensions.k8s.io",
                "customresourcedefinitions",
                "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
                query,
                options,
            )
            .await
        }
        _ => anyhow::bail!("Unsupported resource kind: {}", kind),
    }
}

/// Helper trait to access metadata on any resource.
trait HasMetadata: serde::Serialize + DeserializeOwned {
    fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta;
    fn metadata(&self) -> &rusternetes_common::types::ObjectMeta;
}

// Implement for all supported types via a macro.
macro_rules! impl_has_metadata {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl HasMetadata for $ty {
                fn metadata_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
                    &mut self.metadata
                }
                fn metadata(&self) -> &rusternetes_common::types::ObjectMeta {
                    &self.metadata
                }
            }
        )+
    };
}

impl_has_metadata!(
    Pod,
    Service,
    Deployment,
    StatefulSet,
    DaemonSet,
    Job,
    CronJob,
    ConfigMap,
    Secret,
    ServiceAccount,
    Endpoints,
    PersistentVolumeClaim,
    ResourceQuota,
    LimitRange,
    Role,
    RoleBinding,
    Ingress,
    VolumeSnapshot,
    Namespace,
    Node,
    PersistentVolume,
    ClusterRole,
    ClusterRoleBinding,
    StorageClass,
    VolumeSnapshotClass,
    PriorityClass,
    CustomResourceDefinition,
);

/// Apply a namespaced resource.
#[allow(clippy::too_many_arguments)]
async fn apply_namespaced<T: HasMetadata>(
    client: &ApiClient,
    yaml_str: &str,
    kind: &str,
    api_group: &str,
    resource_plural: &str,
    api_base: &str,
    query: &str,
    options: &ApplyOptions,
) -> Result<ApplyResult> {
    let mut resource: T = serde_yaml::from_str(yaml_str)?;
    let ns = options
        .namespace
        .clone()
        .or_else(|| resource.metadata().namespace.clone())
        .unwrap_or_else(|| "default".to_string());
    let name = resource.metadata().name.clone();

    // Set the last-applied-configuration annotation.
    let mut json_val: Value = serde_json::to_value(&resource)?;
    set_last_applied_annotation(&mut json_val);
    // Re-deserialize with annotation applied.
    resource = serde_json::from_value(json_val)?;

    let item_path = format!(
        "{}/namespaces/{}/{}/{}",
        api_base, ns, resource_plural, name
    );
    let collection_path = format!("{}/namespaces/{}/{}", api_base, ns, resource_plural);

    let exists = resource_exists::<T>(client, &item_path).await?;

    let (action, response) = if exists {
        let result: Value = client
            .put(&format!("{}{}", item_path, query), &resource)
            .await?;
        (ApplyAction::Configured, result)
    } else {
        resource.metadata_mut().ensure_uid();
        resource.metadata_mut().ensure_creation_timestamp();
        let result: Value = client
            .post(&format!("{}{}", collection_path, query), &resource)
            .await?;
        (ApplyAction::Created, result)
    };

    Ok(ApplyResult {
        kind: kind.to_string(),
        api_group: api_group.to_string(),
        name,
        namespace: Some(ns),
        action,
        response,
    })
}

/// Apply a cluster-scoped resource.
#[allow(clippy::too_many_arguments)]
async fn apply_cluster<T: HasMetadata>(
    client: &ApiClient,
    yaml_str: &str,
    kind: &str,
    api_group: &str,
    _resource_plural: &str,
    collection_path: &str,
    query: &str,
    _options: &ApplyOptions,
) -> Result<ApplyResult> {
    let mut resource: T = serde_yaml::from_str(yaml_str)?;
    let name = resource.metadata().name.clone();

    // Set the last-applied-configuration annotation.
    let mut json_val: Value = serde_json::to_value(&resource)?;
    set_last_applied_annotation(&mut json_val);
    resource = serde_json::from_value(json_val)?;

    let item_path = format!("{}/{}", collection_path, name);

    let exists = resource_exists::<T>(client, &item_path).await?;

    let (action, response) = if exists {
        let result: Value = client
            .put(&format!("{}{}", item_path, query), &resource)
            .await?;
        (ApplyAction::Configured, result)
    } else {
        resource.metadata_mut().ensure_uid();
        resource.metadata_mut().ensure_creation_timestamp();
        let result: Value = client
            .post(&format!("{}{}", collection_path, query), &resource)
            .await?;
        (ApplyAction::Created, result)
    };

    Ok(ApplyResult {
        kind: kind.to_string(),
        api_group: api_group.to_string(),
        name,
        namespace: None,
        action,
        response,
    })
}

// ---------------------------------------------------------------------------
// Subcommand support (view-last-applied, set-last-applied, edit-last-applied)
// ---------------------------------------------------------------------------

/// Resolve an API path for a resource type and name.
fn resolve_api_path(resource_type: &str, name: &str, namespace: &str) -> Result<String> {
    let path = match resource_type {
        "pod" | "pods" | "po" => {
            format!("/api/v1/namespaces/{}/pods/{}", namespace, name)
        }
        "service" | "services" | "svc" => {
            format!("/api/v1/namespaces/{}/services/{}", namespace, name)
        }
        "deployment" | "deployments" | "deploy" => {
            format!(
                "/apis/apps/v1/namespaces/{}/deployments/{}",
                namespace, name
            )
        }
        "statefulset" | "statefulsets" | "sts" => {
            format!(
                "/apis/apps/v1/namespaces/{}/statefulsets/{}",
                namespace, name
            )
        }
        "daemonset" | "daemonsets" | "ds" => {
            format!("/apis/apps/v1/namespaces/{}/daemonsets/{}", namespace, name)
        }
        "configmap" | "configmaps" | "cm" => {
            format!("/api/v1/namespaces/{}/configmaps/{}", namespace, name)
        }
        "secret" | "secrets" => {
            format!("/api/v1/namespaces/{}/secrets/{}", namespace, name)
        }
        "serviceaccount" | "serviceaccounts" | "sa" => {
            format!("/api/v1/namespaces/{}/serviceaccounts/{}", namespace, name)
        }
        "job" | "jobs" => {
            format!("/apis/batch/v1/namespaces/{}/jobs/{}", namespace, name)
        }
        "cronjob" | "cronjobs" | "cj" => {
            format!("/apis/batch/v1/namespaces/{}/cronjobs/{}", namespace, name)
        }
        "role" | "roles" => {
            format!(
                "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/roles/{}",
                namespace, name
            )
        }
        "rolebinding" | "rolebindings" => {
            format!(
                "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/rolebindings/{}",
                namespace, name
            )
        }
        "clusterrole" | "clusterroles" => {
            format!("/apis/rbac.authorization.k8s.io/v1/clusterroles/{}", name)
        }
        "clusterrolebinding" | "clusterrolebindings" => {
            format!(
                "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{}",
                name
            )
        }
        "namespace" | "namespaces" | "ns" => {
            format!("/api/v1/namespaces/{}", name)
        }
        "node" | "nodes" => {
            format!("/api/v1/nodes/{}", name)
        }
        _ => anyhow::bail!(
            "Unsupported resource type for apply subcommands: {}",
            resource_type
        ),
    };
    Ok(path)
}

/// Execute apply subcommands (edit-last-applied, set-last-applied, view-last-applied)
pub async fn execute_subcommand(
    client: &ApiClient,
    command: ApplyCommands,
    default_namespace: &str,
) -> Result<()> {
    match command {
        ApplyCommands::ViewLastApplied {
            resource_type,
            name,
            namespace,
            output,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let path = resolve_api_path(&resource_type, &name, ns)?;

            let resource: Value = client
                .get(&path)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))
                .context("Failed to get resource")?;

            let annotation = resource
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(LAST_APPLIED_ANNOTATION))
                .and_then(|v| v.as_str());

            match annotation {
                Some(config) => {
                    let fmt = output.as_deref().unwrap_or("yaml");
                    match fmt {
                        "json" => {
                            // Parse and re-pretty-print
                            let parsed: Value = serde_json::from_str(config)
                                .unwrap_or_else(|_| Value::String(config.to_string()));
                            println!("{}", serde_json::to_string_pretty(&parsed)?);
                        }
                        _ => {
                            // Convert JSON annotation to YAML
                            let parsed: Value = serde_json::from_str(config)
                                .unwrap_or_else(|_| Value::String(config.to_string()));
                            println!("{}", serde_yaml::to_string(&parsed)?);
                        }
                    }
                }
                None => {
                    anyhow::bail!(
                        "no last-applied-configuration annotation found on {}/{}",
                        resource_type,
                        name
                    );
                }
            }
        }
        ApplyCommands::SetLastApplied {
            filename,
            create_annotation,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);

            // Read the file
            let contents = fs::read_to_string(&filename).context("Failed to read file")?;

            // Parse to get kind/name
            let value: serde_yaml::Value = serde_yaml::from_str(&contents)?;
            let kind = value
                .get("kind")
                .and_then(|k| k.as_str())
                .context("Missing 'kind' field")?;
            let name = value
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .context("Missing 'metadata.name' field")?;

            let resource_type = kind.to_lowercase();
            let path = resolve_api_path(&resource_type, name, ns)?;

            // Convert the YAML to JSON for the annotation value
            let json_value: Value = serde_yaml::from_str(&contents)?;
            let annotation_value = serde_json::to_string(&json_value)?;

            // Check if resource exists
            let exists: Result<Value, _> = client.get(&path).await;
            match exists {
                Ok(_) => {
                    // Patch the annotation
                    let patch = json!({
                        "metadata": {
                            "annotations": {
                                LAST_APPLIED_ANNOTATION: annotation_value,
                            }
                        }
                    });
                    let _: Value = client
                        .patch(&path, &patch, "application/strategic-merge-patch+json")
                        .await
                        .context("Failed to set last-applied-configuration")?;
                    println!("{}/{} last-applied-configuration set", resource_type, name);
                }
                Err(_) if create_annotation => {
                    anyhow::bail!(
                        "resource {}/{} not found; --create-annotation requires the resource to exist",
                        resource_type,
                        name
                    );
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("{}", e)).context("Failed to get resource");
                }
            }
        }
        ApplyCommands::EditLastApplied {
            resource_type,
            name,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let path = resolve_api_path(&resource_type, &name, ns)?;

            let resource: Value = client
                .get(&path)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))
                .context("Failed to get resource")?;

            let annotation = resource
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(LAST_APPLIED_ANNOTATION))
                .and_then(|v| v.as_str());

            let current_config = match annotation {
                Some(config) => config.to_string(),
                None => {
                    anyhow::bail!(
                        "no last-applied-configuration annotation found on {}/{}",
                        resource_type,
                        name
                    );
                }
            };

            // Write to temp file for editing
            let parsed: Value = serde_json::from_str(&current_config)?;
            let yaml_str = serde_yaml::to_string(&parsed)?;

            let tmp_dir = std::env::temp_dir();
            let tmp_path = tmp_dir.join(format!("{}-{}-last-applied.yaml", resource_type, name));
            fs::write(&tmp_path, &yaml_str)?;

            // Open editor
            let editor = std::env::var("KUBE_EDITOR")
                .or_else(|_| std::env::var("EDITOR"))
                .unwrap_or_else(|_| "vi".to_string());

            let status = std::process::Command::new(&editor)
                .arg(&tmp_path)
                .status()
                .context("Failed to open editor")?;

            if !status.success() {
                anyhow::bail!("Editor exited with non-zero status");
            }

            // Read back the edited file
            let edited = fs::read_to_string(&tmp_path)?;
            let edited_value: Value = serde_yaml::from_str(&edited)?;
            let new_annotation = serde_json::to_string(&edited_value)?;

            // Update the annotation
            let patch = json!({
                "metadata": {
                    "annotations": {
                        LAST_APPLIED_ANNOTATION: new_annotation,
                    }
                }
            });
            let _: Value = client
                .patch(&path, &patch, "application/strategic-merge-patch+json")
                .await
                .context("Failed to update last-applied-configuration")?;

            // Clean up temp file
            let _ = fs::remove_file(&tmp_path);

            println!(
                "{}/{} last-applied-configuration edited",
                resource_type, name
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // ---- Test 1: Multiple files ----
    #[test]
    fn test_collect_files_multiple() {
        let dir = TempDir::new().unwrap();
        let f1 = dir.path().join("a.yaml");
        let f2 = dir.path().join("b.yaml");
        fs::write(&f1, "kind: Pod").unwrap();
        fs::write(&f2, "kind: Service").unwrap();

        let inputs = vec![
            f1.to_string_lossy().to_string(),
            f2.to_string_lossy().to_string(),
        ];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files[0].ends_with("a.yaml"));
        assert!(files[1].ends_with("b.yaml"));
    }

    // ---- Test 2: Directory apply (non-recursive) ----
    #[test]
    fn test_collect_files_directory() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.yaml"), "kind: Pod").unwrap();
        fs::write(dir.path().join("b.yml"), "kind: Service").unwrap();
        fs::write(dir.path().join("c.json"), "{}").unwrap();
        fs::write(dir.path().join("d.txt"), "ignore").unwrap();

        let inputs = vec![dir.path().to_string_lossy().to_string()];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(
            files.len(),
            3,
            "should include .yaml, .yml, .json but not .txt"
        );
        // Verify .txt was excluded
        assert!(!files.iter().any(|f| f.ends_with(".txt")));
    }

    // ---- Test 3: Recursive directory ----
    #[test]
    fn test_collect_files_recursive() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("sub");
        fs::create_dir(&subdir).unwrap();
        fs::write(dir.path().join("top.yaml"), "kind: Pod").unwrap();
        fs::write(subdir.join("nested.yaml"), "kind: Service").unwrap();
        fs::write(subdir.join("skip.txt"), "not a manifest").unwrap();

        // Non-recursive should only find top-level files.
        let files_no_recurse =
            collect_files(&[dir.path().to_string_lossy().to_string()], false).unwrap();
        assert_eq!(files_no_recurse.len(), 1);

        // Recursive should find both.
        let files_recurse =
            collect_files(&[dir.path().to_string_lossy().to_string()], true).unwrap();
        assert_eq!(files_recurse.len(), 2);
    }

    // ---- Test 4: Field manager query string ----
    #[test]
    fn test_build_query_string_field_manager() {
        let options = ApplyOptions {
            server_side: true,
            field_manager: "my-manager".to_string(),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldManager=my-manager"), "qs = {}", qs);
    }

    #[test]
    fn test_build_query_string_field_manager_with_force() {
        let options = ApplyOptions {
            server_side: true,
            force: true,
            field_manager: "my-manager".to_string(),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldManager=my-manager"));
        assert!(qs.contains("force=true"));
    }

    // ---- Test 5: Output format label ----
    #[test]
    fn test_apply_result_resource_label_core() {
        let r = ApplyResult {
            kind: "Pod".to_string(),
            api_group: "".to_string(),
            name: "nginx".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "pod/nginx");
    }

    #[test]
    fn test_apply_result_resource_label_apps() {
        let r = ApplyResult {
            kind: "Deployment".to_string(),
            api_group: "apps".to_string(),
            name: "web".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Configured,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "deployment.apps/web");
    }

    // ---- Test 6: Validate flag query string ----
    #[test]
    fn test_build_query_string_validate_strict() {
        let options = ApplyOptions {
            validate: Some("strict".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Strict"), "qs = {}", qs);
    }

    #[test]
    fn test_build_query_string_validate_warn() {
        let options = ApplyOptions {
            validate: Some("warn".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Warn"));
    }

    #[test]
    fn test_build_query_string_validate_true() {
        let options = ApplyOptions {
            validate: Some("true".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Strict"));
    }

    #[test]
    fn test_build_query_string_validate_false() {
        let options = ApplyOptions {
            validate: Some("false".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Ignore"));
    }

    // ---- Test 7: last-applied-configuration annotation ----
    #[test]
    fn test_set_last_applied_annotation() {
        let mut val = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "test-cm"
            },
            "data": {
                "key": "value"
            }
        });

        set_last_applied_annotation(&mut val);

        let ann = val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION]
            .as_str()
            .expect("annotation should be set");
        // The annotation should be valid JSON.
        let parsed: Value = serde_json::from_str(ann).expect("annotation should be valid JSON");
        // It should contain the resource data.
        assert_eq!(parsed["kind"], "ConfigMap");
        assert_eq!(parsed["data"]["key"], "value");
        // The annotation value itself should NOT contain the annotation (no recursion).
        assert!(
            parsed
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(LAST_APPLIED_ANNOTATION))
                .is_none(),
            "annotation should not recursively contain itself"
        );
    }

    #[test]
    fn test_set_last_applied_annotation_idempotent() {
        let mut val = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "test-cm",
                "annotations": {
                    LAST_APPLIED_ANNOTATION: "old-value"
                }
            },
            "data": { "key": "value" }
        });

        set_last_applied_annotation(&mut val);

        let ann = val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION]
            .as_str()
            .unwrap();
        let parsed: Value = serde_json::from_str(ann).unwrap();
        // Should reflect the current config, not the old annotation value.
        assert_eq!(parsed["data"]["key"], "value");
    }

    // ---- Existing tests ----

    #[test]
    fn test_resolve_api_path_pods() {
        let path = resolve_api_path("pod", "nginx", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");
    }

    #[test]
    fn test_resolve_api_path_deployments() {
        let path = resolve_api_path("deployment", "web", "production").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/production/deployments/web");
    }

    #[test]
    fn test_resolve_api_path_clusterrole() {
        let path = resolve_api_path("clusterrole", "admin", "default").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin"
        );
    }

    #[test]
    fn test_resolve_api_path_unsupported() {
        let result = resolve_api_path("foobar", "test", "default");
        assert!(result.is_err());
    }

    #[test]
    fn test_last_applied_annotation_key() {
        assert_eq!(
            LAST_APPLIED_ANNOTATION,
            "kubectl.kubernetes.io/last-applied-configuration"
        );
    }

    // ---- Test: is_manifest_file ----
    #[test]
    fn test_is_manifest_file() {
        assert!(is_manifest_file(Path::new("foo.yaml")));
        assert!(is_manifest_file(Path::new("bar.yml")));
        assert!(is_manifest_file(Path::new("baz.json")));
        assert!(!is_manifest_file(Path::new("readme.txt")));
        assert!(!is_manifest_file(Path::new("Makefile")));
    }

    // ---- Test: default ApplyOptions ----
    #[test]
    fn test_apply_options_default() {
        let opts = ApplyOptions::default();
        assert_eq!(opts.field_manager, "kubectl-client-side-apply");
        assert!(!opts.server_side);
        assert!(!opts.recursive);
        assert!(opts.output.is_none());
        assert!(opts.validate.is_none());
    }

    // ---- Test: empty query string when no options ----
    #[test]
    fn test_build_query_string_empty() {
        let options = ApplyOptions::default();
        let qs = build_query_string(&options);
        assert_eq!(qs, "");
    }

    // ---- Test: stdin token ----
    #[test]
    fn test_collect_files_stdin() {
        let inputs = vec!["-".to_string()];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(files, vec!["-"]);
    }

    // ---- Test: nonexistent path ----
    #[test]
    fn test_collect_files_nonexistent() {
        let result = collect_files(&["/nonexistent/path/xyz".to_string()], false);
        assert!(result.is_err());
    }

    // ---- Test: dry run server query ----
    #[test]
    fn test_build_query_string_dry_run_server() {
        let options = ApplyOptions {
            dry_run: Some("server".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("dryRun=All"));
    }

    #[test]
    fn test_build_query_string_dry_run_client() {
        let options = ApplyOptions {
            dry_run: Some("client".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(!qs.contains("dryRun"));
    }

    #[test]
    fn test_build_query_string_dry_run_none() {
        let options = ApplyOptions {
            dry_run: Some("none".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(!qs.contains("dryRun"));
    }

    #[test]
    fn test_build_query_string_combined() {
        let options = ApplyOptions {
            server_side: true,
            force: true,
            field_manager: "test-mgr".to_string(),
            dry_run: Some("server".to_string()),
            validate: Some("strict".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.starts_with("?"));
        assert!(qs.contains("dryRun=All"));
        assert!(qs.contains("fieldManager=test-mgr"));
        assert!(qs.contains("force=true"));
        assert!(qs.contains("fieldValidation=Strict"));
    }

    #[test]
    fn test_build_query_string_validate_ignore() {
        let options = ApplyOptions {
            validate: Some("ignore".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Ignore"));
    }

    #[test]
    fn test_build_query_string_validate_warn_capitalized() {
        let options = ApplyOptions {
            validate: Some("Warn".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Warn"));
    }

    #[test]
    fn test_build_query_string_validate_strict_capitalized() {
        let options = ApplyOptions {
            validate: Some("Strict".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Strict"));
    }

    #[test]
    fn test_build_query_string_validate_unknown() {
        let options = ApplyOptions {
            validate: Some("custom".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=custom"));
    }

    #[test]
    fn test_strip_last_applied_removes_annotation() {
        let val = json!({
            "metadata": {
                "name": "test",
                "annotations": {
                    LAST_APPLIED_ANNOTATION: "old-config",
                    "other": "keep"
                }
            }
        });
        let clean = strip_last_applied(&val);
        let annotations = clean["metadata"]["annotations"].as_object().unwrap();
        assert!(!annotations.contains_key(LAST_APPLIED_ANNOTATION));
        assert_eq!(annotations["other"], "keep");
    }

    #[test]
    fn test_strip_last_applied_removes_empty_annotations() {
        let val = json!({
            "metadata": {
                "name": "test",
                "annotations": {
                    LAST_APPLIED_ANNOTATION: "old-config"
                }
            }
        });
        let clean = strip_last_applied(&val);
        assert!(clean["metadata"].get("annotations").is_none());
    }

    #[test]
    fn test_strip_last_applied_no_annotations() {
        let val = json!({
            "metadata": {
                "name": "test"
            }
        });
        let clean = strip_last_applied(&val);
        assert_eq!(clean["metadata"]["name"], "test");
    }

    #[test]
    fn test_format_output_default_created() {
        let result = ApplyResult {
            kind: "Pod".to_string(),
            api_group: "".to_string(),
            name: "nginx".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Created,
            response: json!({}),
        };
        let options = ApplyOptions::default();
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_default_configured() {
        let result = ApplyResult {
            kind: "Deployment".to_string(),
            api_group: "apps".to_string(),
            name: "web".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Configured,
            response: json!({}),
        };
        let options = ApplyOptions::default();
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_json() {
        let result = ApplyResult {
            kind: "Pod".to_string(),
            api_group: "".to_string(),
            name: "test".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Created,
            response: json!({"kind": "Pod", "metadata": {"name": "test"}}),
        };
        let options = ApplyOptions {
            output: Some("json".to_string()),
            ..Default::default()
        };
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_yaml() {
        let result = ApplyResult {
            kind: "Pod".to_string(),
            api_group: "".to_string(),
            name: "test".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Created,
            response: json!({"kind": "Pod"}),
        };
        let options = ApplyOptions {
            output: Some("yaml".to_string()),
            ..Default::default()
        };
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_name() {
        let result = ApplyResult {
            kind: "Service".to_string(),
            api_group: "".to_string(),
            name: "frontend".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Created,
            response: json!({}),
        };
        let options = ApplyOptions {
            output: Some("name".to_string()),
            ..Default::default()
        };
        format_output(&result, &options);
    }

    #[test]
    fn test_apply_action_eq() {
        assert_eq!(ApplyAction::Created, ApplyAction::Created);
        assert_eq!(ApplyAction::Configured, ApplyAction::Configured);
        assert_ne!(ApplyAction::Created, ApplyAction::Configured);
    }

    #[test]
    fn test_resource_label_rbac() {
        let r = ApplyResult {
            kind: "ClusterRole".to_string(),
            api_group: "rbac.authorization.k8s.io".to_string(),
            name: "admin".to_string(),
            namespace: None,
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(
            r.resource_label(),
            "clusterrole.rbac.authorization.k8s.io/admin"
        );
    }

    #[test]
    fn test_resource_label_namespace() {
        let r = ApplyResult {
            kind: "Namespace".to_string(),
            api_group: "".to_string(),
            name: "test-ns".to_string(),
            namespace: None,
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "namespace/test-ns");
    }

    #[test]
    fn test_resolve_api_path_services() {
        let path = resolve_api_path("svc", "my-svc", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/services/my-svc");
    }

    #[test]
    fn test_resolve_api_path_statefulsets() {
        let path = resolve_api_path("sts", "my-sts", "default").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/statefulsets/my-sts");
    }

    #[test]
    fn test_resolve_api_path_daemonsets() {
        let path = resolve_api_path("ds", "my-ds", "default").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/daemonsets/my-ds");
    }

    #[test]
    fn test_resolve_api_path_configmaps() {
        let path = resolve_api_path("cm", "my-cm", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/configmaps/my-cm");
    }

    #[test]
    fn test_resolve_api_path_secrets() {
        let path = resolve_api_path("secrets", "my-secret", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/secrets/my-secret");
    }

    #[test]
    fn test_resolve_api_path_serviceaccounts() {
        let path = resolve_api_path("sa", "my-sa", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/serviceaccounts/my-sa");
    }

    #[test]
    fn test_resolve_api_path_jobs() {
        let path = resolve_api_path("jobs", "my-job", "default").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/jobs/my-job");
    }

    #[test]
    fn test_resolve_api_path_cronjobs() {
        let path = resolve_api_path("cj", "my-cj", "default").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/cronjobs/my-cj");
    }

    #[test]
    fn test_resolve_api_path_roles() {
        let path = resolve_api_path("roles", "my-role", "default").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/my-role"
        );
    }

    #[test]
    fn test_resolve_api_path_rolebindings() {
        let path = resolve_api_path("rolebindings", "my-rb", "default").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings/my-rb"
        );
    }

    #[test]
    fn test_resolve_api_path_clusterrolebindings() {
        let path = resolve_api_path("clusterrolebindings", "my-crb", "default").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/my-crb"
        );
    }

    #[test]
    fn test_resolve_api_path_namespaces() {
        let path = resolve_api_path("ns", "test-ns", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/test-ns");
    }

    #[test]
    fn test_resolve_api_path_nodes() {
        let path = resolve_api_path("nodes", "node-1", "default").unwrap();
        assert_eq!(path, "/api/v1/nodes/node-1");
    }

    #[test]
    fn test_is_manifest_file_no_extension() {
        assert!(!is_manifest_file(Path::new("Dockerfile")));
    }

    #[test]
    fn test_is_manifest_file_hidden_yaml() {
        assert!(is_manifest_file(Path::new(".hidden.yaml")));
    }

    #[test]
    fn test_collect_files_empty_input() {
        let result = collect_files(&[], false).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_set_last_applied_annotation_creates_metadata() {
        let mut val = json!({
            "kind": "ConfigMap",
            "metadata": {}
        });
        set_last_applied_annotation(&mut val);
        assert!(val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION].is_string());
    }

    #[test]
    fn test_build_query_string_no_server_side() {
        let options = ApplyOptions {
            server_side: false,
            force: true,
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(!qs.contains("fieldManager"));
        assert!(!qs.contains("force"));
    }

    #[test]
    fn test_apply_options_custom() {
        let opts = ApplyOptions {
            files: vec!["test.yaml".to_string()],
            namespace: Some("prod".to_string()),
            dry_run: Some("server".to_string()),
            server_side: true,
            force: true,
            recursive: true,
            field_manager: "custom-manager".to_string(),
            output: Some("json".to_string()),
            validate: Some("strict".to_string()),
        };
        assert_eq!(opts.files.len(), 1);
        assert_eq!(opts.namespace, Some("prod".to_string()));
        assert!(opts.server_side);
        assert!(opts.force);
        assert!(opts.recursive);
        assert_eq!(opts.field_manager, "custom-manager");
    }

    // --- 21 additional tests below ---

    #[test]
    fn test_resolve_api_path_pod_alias_po() {
        let path = resolve_api_path("po", "my-pod", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/my-pod");
    }

    #[test]
    fn test_resolve_api_path_deploy_alias() {
        let path = resolve_api_path("deploy", "web", "default").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/deployments/web");
    }

    #[test]
    fn test_resolve_api_path_ingress() {
        // ingress is not supported in resolve_api_path
        let result = resolve_api_path("ingress", "my-ing", "default");
        assert!(result.is_err());
    }

    #[test]
    fn test_is_manifest_file_uppercase() {
        // The function only matches lowercase extensions
        assert!(!is_manifest_file(Path::new("FILE.YAML")));
    }

    #[test]
    fn test_is_manifest_file_json() {
        assert!(is_manifest_file(Path::new("config.json")));
    }

    #[test]
    fn test_is_manifest_file_yml() {
        assert!(is_manifest_file(Path::new("deploy.yml")));
    }

    #[test]
    fn test_strip_last_applied_preserves_other_fields() {
        let val = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test",
                "labels": {"app": "web"},
                "annotations": {
                    LAST_APPLIED_ANNOTATION: "old",
                    "other-ann": "keep-me"
                }
            },
            "spec": {"containers": []}
        });
        let clean = strip_last_applied(&val);
        assert_eq!(clean["apiVersion"], "v1");
        assert_eq!(clean["kind"], "Pod");
        assert_eq!(clean["metadata"]["labels"]["app"], "web");
        assert_eq!(clean["metadata"]["annotations"]["other-ann"], "keep-me");
    }

    #[test]
    fn test_strip_last_applied_no_metadata() {
        let val = json!({"kind": "Pod"});
        let clean = strip_last_applied(&val);
        assert_eq!(clean["kind"], "Pod");
    }

    #[test]
    fn test_set_last_applied_annotation_preserves_existing_annotations() {
        let mut val = json!({
            "kind": "ConfigMap",
            "metadata": {
                "name": "test",
                "annotations": {
                    "custom/annotation": "keep"
                }
            }
        });
        set_last_applied_annotation(&mut val);
        assert_eq!(val["metadata"]["annotations"]["custom/annotation"], "keep");
        assert!(val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION].is_string());
    }

    #[test]
    fn test_format_output_configured_action() {
        let result = ApplyResult {
            kind: "ConfigMap".to_string(),
            api_group: "".to_string(),
            name: "my-cm".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Configured,
            response: json!({}),
        };
        let options = ApplyOptions::default();
        // Should not panic
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_name_output() {
        let result = ApplyResult {
            kind: "Deployment".to_string(),
            api_group: "apps".to_string(),
            name: "web".to_string(),
            namespace: Some("prod".to_string()),
            action: ApplyAction::Created,
            response: json!({}),
        };
        let options = ApplyOptions {
            output: Some("name".to_string()),
            ..Default::default()
        };
        format_output(&result, &options);
    }

    #[test]
    fn test_resource_label_batch() {
        let r = ApplyResult {
            kind: "Job".to_string(),
            api_group: "batch".to_string(),
            name: "my-job".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "job.batch/my-job");
    }

    #[test]
    fn test_resource_label_core_service() {
        let r = ApplyResult {
            kind: "Service".to_string(),
            api_group: "".to_string(),
            name: "frontend".to_string(),
            namespace: Some("default".to_string()),
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "service/frontend");
    }

    #[test]
    fn test_apply_options_default_field_manager() {
        let opts = ApplyOptions::default();
        assert_eq!(opts.field_manager, "kubectl-client-side-apply");
    }

    #[test]
    fn test_apply_options_default_force_false() {
        let opts = ApplyOptions::default();
        assert!(!opts.force);
    }

    #[test]
    fn test_apply_options_default_dry_run_none() {
        let opts = ApplyOptions::default();
        assert!(opts.dry_run.is_none());
    }

    #[test]
    fn test_collect_files_single_file() {
        let dir = TempDir::new().unwrap();
        let f = dir.path().join("pod.yaml");
        fs::write(&f, "kind: Pod").unwrap();
        let inputs = vec![f.to_string_lossy().to_string()];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_collect_files_directory_excludes_subdirs_nonrecursive() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("subdir");
        fs::create_dir(&subdir).unwrap();
        fs::write(dir.path().join("top.yaml"), "kind: Pod").unwrap();
        fs::write(subdir.join("sub.yaml"), "kind: Service").unwrap();
        let inputs = vec![dir.path().to_string_lossy().to_string()];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_build_query_string_server_side_default_manager() {
        let options = ApplyOptions {
            server_side: true,
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldManager=kubectl-client-side-apply"));
    }

    #[test]
    fn test_resolve_api_path_secret_alias() {
        let path = resolve_api_path("secret", "my-secret", "ns1").unwrap();
        assert_eq!(path, "/api/v1/namespaces/ns1/secrets/my-secret");
    }

    #[test]
    fn test_has_metadata_trait_pod() {
        let mut pod = Pod {
            metadata: rusternetes_common::types::ObjectMeta {
                name: "test".to_string(),
                ..Default::default()
            },
            spec: None,
            status: None,
            type_meta: Default::default(),
        };
        assert_eq!(pod.metadata().name, "test");
        pod.metadata_mut().name = "changed".to_string();
        assert_eq!(pod.metadata().name, "changed");
    }
}
