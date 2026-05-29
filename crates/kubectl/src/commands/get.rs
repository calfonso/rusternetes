use crate::client::{ApiClient, GetError};
use anyhow::{Context, Result};
use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::resources::{
    CronJob, Deployment, Job, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod,
    Service,
};
use serde::Serialize;

// Helper to convert GetError to anyhow::Error (used in tests)
#[allow(dead_code)]
fn map_get_error(err: GetError) -> anyhow::Error {
    match err {
        GetError::NotFound => anyhow::anyhow!("Resource not found"),
        GetError::Other(e) => e,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OutputFormat {
    Table,
    Json,
    Yaml,
    Wide,
    Name,
    JsonPath(String),
}

impl OutputFormat {
    pub(crate) fn from_str(s: &str) -> Result<Self> {
        match s {
            "json" => Ok(Self::Json),
            "yaml" => Ok(Self::Yaml),
            "wide" => Ok(Self::Wide),
            "name" => Ok(Self::Name),
            _ if s.starts_with("jsonpath=") => Ok(Self::JsonPath(s[9..].to_string())),
            _ if s.starts_with("jsonpath-file=") => {
                let path = &s[14..];
                let expr = std::fs::read_to_string(path)
                    .with_context(|| format!("Failed to read jsonpath file: {}", path))?;
                Ok(Self::JsonPath(expr.trim().to_string()))
            }
            _ => anyhow::bail!(
                "Unknown output format: {}. Supported formats: json, yaml, wide, name, jsonpath=<expr>",
                s
            ),
        }
    }
}

pub(crate) fn format_output<T: Serialize>(resource: &T, format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(resource)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(resource)?);
        }
        OutputFormat::JsonPath(expr) => {
            let value = serde_json::to_value(resource)?;
            let result = evaluate_jsonpath(&value, expr)?;
            print!("{}", result);
        }
        OutputFormat::Name => {
            // Print kind/name format
            let value = serde_json::to_value(resource)?;
            let kind = value
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let name = value
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            println!("{}/{}", kind.to_lowercase(), name);
        }
        _ => {
            // For table and wide, just use JSON for now
            println!("{}", serde_json::to_string_pretty(resource)?);
        }
    }
    Ok(())
}

/// Evaluate a jsonpath expression against a JSON value.
/// Supports basic dotted paths, array indexing, and filter expressions.
fn evaluate_jsonpath(value: &serde_json::Value, expr: &str) -> Result<String> {
    // Strip leading { and trailing } if present (kubectl jsonpath format)
    let expr = expr.trim();
    let expr = expr.strip_prefix('{').unwrap_or(expr);
    let expr = expr.strip_suffix('}').unwrap_or(expr);

    // Handle multiple expressions joined by whitespace (e.g., {.foo}{"\n"}{.bar})
    // For simplicity, evaluate the full expr as a single path
    let result = resolve_path(value, expr)?;
    Ok(result)
}

fn resolve_path(value: &serde_json::Value, path: &str) -> Result<String> {
    // Handle string literals like "\n", "\t"
    if path.starts_with('"') && path.ends_with('"') {
        let s = &path[1..path.len() - 1];
        return Ok(s.replace("\\n", "\n").replace("\\t", "\t"));
    }

    // Strip leading dot
    let path = path.strip_prefix('.').unwrap_or(path);

    if path.is_empty() {
        return format_value(value);
    }

    // Split on first dot or bracket
    let mut current = value;
    let mut remaining = path;

    while !remaining.is_empty() {
        if remaining.starts_with('[') {
            // Array index or filter
            let end = remaining.find(']').unwrap_or(remaining.len());
            let idx_str = &remaining[1..end];
            remaining = remaining
                .get(end + 1..)
                .unwrap_or("")
                .trim_start_matches('.');

            if let Ok(idx) = idx_str.parse::<usize>() {
                match current.get(idx) {
                    Some(v) => current = v,
                    None => return Ok(String::new()),
                }
            } else if idx_str.starts_with('?') {
                // Filter expression like [?(@.type=="Ready")]
                // For now just return empty string for unsupported filters
                return Ok(String::new());
            } else {
                return Ok(String::new());
            }
        } else {
            // Field access
            let (key, rest) = match remaining.find(['.', '[']) {
                Some(i) => {
                    let (k, r) = remaining.split_at(i);
                    (k, r.trim_start_matches('.'))
                }
                None => (remaining, ""),
            };
            remaining = rest;
            match current.get(key) {
                Some(v) => current = v,
                None => return Ok(String::new()),
            }
        }
    }

    format_value(current)
}

fn format_value(value: &serde_json::Value) -> Result<String> {
    match value {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Null => Ok(String::new()),
        serde_json::Value::Bool(b) => Ok(b.to_string()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::Array(arr) => {
            let parts: Result<Vec<_>> = arr.iter().map(format_value).collect();
            Ok(parts?.join(" "))
        }
        serde_json::Value::Object(_) => Ok(serde_json::to_string(value)?),
    }
}

/// Parse resource_type for slash syntax (e.g. "pod/nginx") and return (type, optional_name)
fn parse_resource_slash(resource_type: &str) -> (&str, Option<&str>) {
    if let Some(idx) = resource_type.find('/') {
        let rtype = &resource_type[..idx];
        let rname = &resource_type[idx + 1..];
        if rname.is_empty() {
            (rtype, None)
        } else {
            (rtype, Some(rname))
        }
    } else {
        (resource_type, None)
    }
}

/// Resolve a JSONPath expression to a sortable value from a serde_json::Value
fn resolve_sort_key(value: &serde_json::Value, sort_expr: &str) -> String {
    let path = sort_expr
        .trim()
        .strip_prefix('{')
        .unwrap_or(sort_expr.trim());
    let path = path.strip_suffix('}').unwrap_or(path);
    let path = path.strip_prefix('.').unwrap_or(path);

    let mut current = value;
    for key in path.split('.') {
        if key.is_empty() {
            continue;
        }
        match current.get(key) {
            Some(v) => current = v,
            None => return String::new(),
        }
    }
    match current {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => format!("{:020}", n.as_f64().unwrap_or(0.0) as i64),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Sort a vector of serializable items by a JSONPath expression
fn sort_by_jsonpath<T: Serialize>(items: &mut [T], sort_expr: &str) {
    items.sort_by(|a, b| {
        let va = serde_json::to_value(a).unwrap_or(serde_json::Value::Null);
        let vb = serde_json::to_value(b).unwrap_or(serde_json::Value::Null);
        let ka = resolve_sort_key(&va, sort_expr);
        let kb = resolve_sort_key(&vb, sort_expr);
        ka.cmp(&kb)
    });
}

/// Format labels as a comma-separated string for --show-labels output
fn format_labels(labels: &Option<std::collections::HashMap<String, String>>) -> String {
    match labels {
        Some(l) if !l.is_empty() => {
            let mut pairs: Vec<_> = l.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
            pairs.sort();
            pairs.join(",")
        }
        _ => "<none>".to_string(),
    }
}

/// Watch resources by streaming newline-delimited JSON from the API
async fn watch_resources(client: &ApiClient, api_path: &str, query: &str) -> Result<()> {
    // If query already starts with ?, append &watch=true; else ?watch=true
    let watch_path = if query.is_empty() {
        format!("{}?watch=true", api_path)
    } else {
        format!("{}{}&watch=true", api_path, query)
    };

    let response = client.get_stream(&watch_path).await.map_err(|e| match e {
        GetError::NotFound => anyhow::anyhow!("Resource not found"),
        GetError::Other(e) => e,
    })?;

    let mut stream = response.bytes_stream();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error reading watch stream")?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        // Process complete lines
        while let Some(newline_pos) = buffer.find('\n') {
            let line = buffer[..newline_pos].trim().to_string();
            buffer = buffer[newline_pos + 1..].to_string();

            if line.is_empty() {
                continue;
            }

            // Parse watch event: {"type": "ADDED/MODIFIED/DELETED", "object": {...}}
            if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) {
                let event_type = event
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("UNKNOWN");
                let object = event.get("object").unwrap_or(&event);
                let kind = object
                    .get("kind")
                    .and_then(|k| k.as_str())
                    .unwrap_or("unknown");
                let name = object
                    .pointer("/metadata/name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown");
                let ns = object
                    .pointer("/metadata/namespace")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");

                let ns_prefix = if ns.is_empty() {
                    String::new()
                } else {
                    format!("{}/", ns)
                };
                println!(
                    "{:<10} {}/{}{}",
                    event_type,
                    kind.to_lowercase(),
                    ns_prefix,
                    name
                );
            }
        }
    }

    Ok(())
}

/// The curated set of kinds expanded by `get all`. Mirrors the legacy behavior
/// (pods, services, deployments, statefulsets, daemonsets, jobs, cronjobs).
/// Kinds the server doesn't serve are skipped at resolve time.
const ALL_RESOURCE_KINDS: &[&str] = &[
    "pods",
    "services",
    "deployments",
    "statefulsets",
    "daemonsets",
    "jobs",
    "cronjobs",
];

/// Enhanced execute with all new parameters — now backed by RestMapper.
#[allow(clippy::too_many_arguments)]
pub async fn execute_enhanced(
    client: &ApiClient,
    resource_type: &str,
    name: Option<&str>,
    namespace: Option<&str>,
    all_namespaces: bool,
    output: Option<&str>,
    no_headers: bool,
    selector: Option<&str>,
    field_selector: Option<&str>,
    watch: bool,
    show_labels: bool,
    sort_by: Option<&str>,
) -> Result<()> {
    // Build query string for label selector and field selector
    let mut query_params = Vec::new();
    if let Some(sel) = selector {
        query_params.push(format!("labelSelector={}", urlencoding::encode(sel)));
    }
    if let Some(fs) = field_selector {
        query_params.push(format!("fieldSelector={}", urlencoding::encode(fs)));
    }

    let query_string = if query_params.is_empty() {
        String::new()
    } else {
        format!("?{}", query_params.join("&"))
    };

    // Build the REST mapper once per invocation (one discovery round-trip).
    let mapper = crate::discovery::RestMapper::from_server(client).await?;

    // `get all` expands to a curated set of common kinds (mirrors the legacy
    // behavior). Each is resolved via the mapper; kinds the server doesn't
    // serve are silently skipped rather than erroring out.
    let is_all = resource_type.eq_ignore_ascii_case("all");

    // Support comma-separated resource types: "pods,services"
    let resource_types: Vec<&str> = if is_all {
        ALL_RESOURCE_KINDS.to_vec()
    } else {
        resource_type.split(',').collect()
    };

    // Tracks whether any kind under `get all` produced output (drives the
    // blank-line separators between non-empty kinds).
    let mut printed_any = false;

    for (i, rtype) in resource_types.iter().enumerate() {
        // Support type/name syntax: "pod/nginx"
        let (resolved_type, slash_name) = parse_resource_slash(rtype.trim());
        let effective_name = slash_name.or(if resource_types.len() == 1 {
            name
        } else {
            None
        });

        // Resolve the resource type via the discovery mapper. Under `get all`
        // an unservable kind is skipped instead of failing the whole command.
        let mapping = match mapper.resolve(resolved_type) {
            Some(m) => m,
            None if is_all => continue,
            None => {
                return Err(anyhow::anyhow!(
                    "error: the server doesn't have a resource type \"{}\"",
                    resolved_type
                ));
            }
        };

        if watch {
            // Build the watch collection path from the mapping. ops::list_path
            // applies the all-namespaces cluster-wide override (no namespace
            // segment), so `get pods -A --watch` hits `/api/v1/pods`.
            let base_path = crate::ops::list_path(mapping, namespace, all_namespaces);
            watch_resources(client, &base_path, &query_string).await?;
            return Ok(());
        }

        // For `get all`, suppress per-kind "No resources found" noise and only
        // separate kinds that actually produced output. The separator is
        // emitted inside execute_with_mapping right before it prints, so an
        // empty kind between two non-empty kinds never leaves a stray blank
        // line. For a normal single / comma-list invocation, keep the legacy
        // blank-line-before-subsequent behavior and the empty-collection
        // message.
        if is_all {
            // Ask the callee to lead with a blank line only if a prior kind
            // already printed; it will only honor this when it actually prints.
            let printed = execute_with_mapping(
                client,
                mapping,
                effective_name,
                namespace,
                all_namespaces,
                output,
                no_headers,
                &query_string,
                show_labels,
                sort_by,
                true,
                printed_any,
            )
            .await?;
            printed_any = printed_any || printed;
        } else {
            // Legacy comma-list behavior: a blank line before every kind after
            // the first, printed unconditionally here (not inside the callee).
            if i > 0 {
                println!();
            }
            execute_with_mapping(
                client,
                mapping,
                effective_name,
                namespace,
                all_namespaces,
                output,
                no_headers,
                &query_string,
                show_labels,
                sort_by,
                false,
                false,
            )
            .await?;
        }
    }

    Ok(())
}

mod urlencoding {
    pub fn encode(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' | '=' | ',' | '!' => {
                    c.to_string()
                }
                ' ' => "+".to_string(),
                _ => format!("%{:02X}", c as u8),
            })
            .collect()
    }
}

/// Inner dispatcher: fetch and render a single resolved resource kind.
///
/// For the well-known kinds that have typed table printers (Pod, Service,
/// Deployment, Node, Namespace, Job, CronJob, PersistentVolume,
/// PersistentVolumeClaim) the fetched JSON is deserialized into the concrete
/// type so the existing column-aware printers are preserved. For every other
/// kind — and for all non-table output formats — the raw `serde_json::Value`
/// is passed directly to `format_output`.
///
/// When `is_all` is true (invoked via `get all`), empty collections are
/// silently skipped (no "No resources found" message). `leading_separator`
/// requests a blank line before this kind's output; it is honored only when
/// the kind actually prints, so an empty kind never leaves a stray blank line.
/// Returns whether any output was produced.
#[allow(clippy::too_many_arguments)]
async fn execute_with_mapping(
    client: &ApiClient,
    mapping: &crate::discovery::ResourceMapping,
    name: Option<&str>,
    namespace: Option<&str>,
    all_namespaces: bool,
    output: Option<&str>,
    no_headers: bool,
    query: &str,
    show_labels: bool,
    sort_by: Option<&str>,
    is_all: bool,
    leading_separator: bool,
) -> Result<bool> {
    use crate::ops::{get_value, list_value};

    let format = output
        .map(OutputFormat::from_str)
        .transpose()?
        .unwrap_or(OutputFormat::Table);

    if let Some(n) = name {
        // Single-resource GET
        let value = get_value(client, mapping, namespace, n)
            .await
            .map_err(|e| {
                // Mirror kubectl's "Error from server (NotFound)" message when the
                // underlying error string contains "not found".
                let s = e.to_string();
                if s.to_lowercase().contains("not found") {
                    anyhow::anyhow!(
                        "Error from server (NotFound): {} \"{}\" not found",
                        mapping.kind.to_lowercase() + "s",
                        n
                    )
                } else {
                    e
                }
            })?;
        // selectors are not forwarded to single-item by-name GETs (they don't
        // apply).
        if leading_separator {
            println!();
        }
        format_output(&value, &format)?;
    } else {
        // Collection GET — all path logic (including the all-namespaces
        // cluster-wide override) lives in ops::list_value / ops::list_path.
        let mut items = list_value(client, mapping, namespace, all_namespaces, query).await?;

        if items.is_empty() {
            // Under `get all` an empty kind is skipped silently.
            if is_all {
                return Ok(false);
            }
            if all_namespaces {
                eprintln!("No resources found.");
            } else {
                eprintln!(
                    "No resources found in {} namespace.",
                    namespace.unwrap_or("default")
                );
            }
            return Ok(false);
        }

        if let Some(sort_expr) = sort_by {
            sort_by_jsonpath(&mut items, sort_expr);
        }

        // Emit the inter-kind separator only now that we know this kind prints.
        if leading_separator {
            println!();
        }

        // For well-known kinds with typed printers, attempt to deserialize and
        // use the column-aware printer. Fall back to generic Value output if
        // deserialization fails or the kind isn't recognized.
        match (mapping.kind.as_str(), &format) {
            ("Pod", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Pod> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_pods(&typed, no_headers, show_labels);
            }
            ("Service", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Service> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_services(&typed, no_headers, show_labels);
            }
            ("Deployment", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Deployment> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_deployments(&typed, no_headers, show_labels);
            }
            ("Node", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Node> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_nodes(&typed, no_headers, show_labels);
            }
            ("Namespace", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Namespace> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_namespaces(&typed, no_headers, show_labels);
            }
            ("Job", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Job> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_jobs(&typed, no_headers, show_labels);
            }
            ("CronJob", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<CronJob> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_cronjobs(&typed, no_headers, show_labels);
            }
            ("PersistentVolume", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<PersistentVolume> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_pvs(&typed, no_headers, show_labels);
            }
            ("PersistentVolumeClaim", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<PersistentVolumeClaim> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_pvcs(&typed, no_headers, show_labels);
            }
            _ => {
                // Generic Value output: works for all formats and any kind.
                format_output(&items, &format)?;
            }
        }
    }

    Ok(true)
}

/// Kept for callers that invoke `execute` directly (e.g., describe-fallback).
#[allow(dead_code)]
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: Option<&str>,
    namespace: Option<&str>,
    output: Option<&str>,
    no_headers: bool,
) -> Result<()> {
    // Route everything through the mapper-based path.
    execute_enhanced(
        client,
        resource_type,
        name,
        namespace,
        false,
        output,
        no_headers,
        None,
        None,
        false,
        false,
        None,
    )
    .await
}

fn print_pods(pods: &[Pod], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<15} {:<15} LABELS", "NAME", "STATUS", "NODE");
        } else {
            println!("{:<30} {:<15} {:<15}", "NAME", "STATUS", "NODE");
        }
    }
    for pod in pods {
        let status = pod
            .status
            .as_ref()
            .map(|s| format!("{:?}", s.phase))
            .unwrap_or_else(|| "Unknown".to_string());
        let node = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .map(|n| n.as_str())
            .unwrap_or("<none>");
        if show_labels {
            println!(
                "{:<30} {:<15} {:<15} {}",
                pod.metadata.name,
                status,
                node,
                format_labels(&pod.metadata.labels)
            );
        } else {
            println!("{:<30} {:<15} {:<15}", pod.metadata.name, status, node);
        }
    }
}

fn print_services(services: &[Service], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<20} {:<10} LABELS", "NAME", "CLUSTER-IP", "PORTS");
        } else {
            println!("{:<30} {:<20} {:<10}", "NAME", "CLUSTER-IP", "PORTS");
        }
    }
    for service in services {
        let cluster_ip = service.spec.cluster_ip.as_deref().unwrap_or("<none>");
        let ports = service
            .spec
            .ports
            .iter()
            .map(|p| p.port.to_string())
            .collect::<Vec<_>>()
            .join(",");
        if show_labels {
            println!(
                "{:<30} {:<20} {:<10} {}",
                service.metadata.name,
                cluster_ip,
                ports,
                format_labels(&service.metadata.labels)
            );
        } else {
            println!(
                "{:<30} {:<20} {:<10}",
                service.metadata.name, cluster_ip, ports
            );
        }
    }
}

fn format_duration(duration: chrono::Duration) -> String {
    let days = duration.num_days();
    if days > 0 {
        return format!("{}d", days);
    }
    let hours = duration.num_hours();
    if hours > 0 {
        return format!("{}h", hours);
    }
    let minutes = duration.num_minutes();
    if minutes > 0 {
        return format!("{}m", minutes);
    }
    let seconds = duration.num_seconds();
    format!("{}s", seconds)
}

fn print_deployments(deployments: &[Deployment], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!(
                "{:<30} {:<15} {:<15} {:<15} {:<10} LABELS",
                "NAME", "READY", "UP-TO-DATE", "AVAILABLE", "AGE"
            );
        } else {
            println!(
                "{:<30} {:<15} {:<15} {:<15} {:<10}",
                "NAME", "READY", "UP-TO-DATE", "AVAILABLE", "AGE"
            );
        }
    }
    for deployment in deployments {
        let desired = deployment.spec.replicas.unwrap_or(1);
        let ready = deployment
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0);
        let updated = deployment
            .status
            .as_ref()
            .and_then(|s| s.updated_replicas)
            .unwrap_or(0);
        let available = deployment
            .status
            .as_ref()
            .and_then(|s| s.available_replicas)
            .unwrap_or(0);
        let age = deployment
            .metadata
            .creation_timestamp
            .map(|ts| format_duration(Utc::now().signed_duration_since(ts)))
            .unwrap_or_else(|| "<unknown>".to_string());
        if show_labels {
            println!(
                "{:<30} {:<15} {:<15} {:<15} {:<10} {}",
                deployment.metadata.name,
                format!("{}/{}", ready, desired),
                updated,
                available,
                age,
                format_labels(&deployment.metadata.labels)
            );
        } else {
            println!(
                "{:<30} {:<15} {:<15} {:<15} {:<10}",
                deployment.metadata.name,
                format!("{}/{}", ready, desired),
                updated,
                available,
                age
            );
        }
    }
}

fn print_nodes(nodes: &[Node], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<15} LABELS", "NAME", "STATUS");
        } else {
            println!("{:<30} {:<15}", "NAME", "STATUS");
        }
    }
    for node in nodes {
        let status = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|c| c.iter().find(|cond| cond.condition_type == "Ready"))
            .map(|c| c.status.as_str())
            .unwrap_or("Unknown");
        if show_labels {
            println!(
                "{:<30} {:<15} {}",
                node.metadata.name,
                status,
                format_labels(&node.metadata.labels)
            );
        } else {
            println!("{:<30} {:<15}", node.metadata.name, status);
        }
    }
}

fn print_namespaces(namespaces: &[Namespace], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<15} LABELS", "NAME", "STATUS");
        } else {
            println!("{:<30} {:<15}", "NAME", "STATUS");
        }
    }
    for namespace in namespaces {
        let status = namespace
            .status
            .as_ref()
            .map(|s| format!("{:?}", s.phase))
            .unwrap_or_else(|| "Unknown".to_string());
        if show_labels {
            println!(
                "{:<30} {:<15} {}",
                namespace.metadata.name,
                status,
                format_labels(&namespace.metadata.labels)
            );
        } else {
            println!("{:<30} {:<15}", namespace.metadata.name, status);
        }
    }
}

fn print_pvs(pvs: &[PersistentVolume], no_headers: bool, _show_labels: bool) {
    if !no_headers {
        println!(
            "{:<30} {:<15} {:<20} {:<15}",
            "NAME", "CAPACITY", "ACCESS MODES", "STATUS"
        );
    }
    for pv in pvs {
        let capacity = pv
            .spec
            .capacity
            .get("storage")
            .map(|s| s.as_str())
            .unwrap_or("<none>");
        let access_modes = pv
            .spec
            .access_modes
            .iter()
            .map(|m| format!("{:?}", m))
            .collect::<Vec<_>>()
            .join(",");
        let status = pv
            .status
            .as_ref()
            .map(|s| format!("{:?}", s.phase))
            .unwrap_or_else(|| "Unknown".to_string());
        println!(
            "{:<30} {:<15} {:<20} {:<15}",
            pv.metadata.name, capacity, access_modes, status
        );
    }
}

fn print_pvcs(pvcs: &[PersistentVolumeClaim], no_headers: bool, _show_labels: bool) {
    if !no_headers {
        println!(
            "{:<30} {:<15} {:<20} {:<20} {:<15}",
            "NAME", "STATUS", "VOLUME", "CAPACITY", "ACCESS MODES"
        );
    }
    for pvc in pvcs {
        let status = pvc
            .status
            .as_ref()
            .map(|s| format!("{:?}", s.phase))
            .unwrap_or_else(|| "Unknown".to_string());
        let volume = pvc.spec.volume_name.as_deref().unwrap_or("<none>");
        let capacity = pvc
            .status
            .as_ref()
            .and_then(|s| s.capacity.as_ref())
            .and_then(|c| c.get("storage"))
            .map(|s| s.as_str())
            .unwrap_or("<none>");
        let access_modes = pvc
            .spec
            .access_modes
            .iter()
            .map(|m| format!("{:?}", m))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{:<30} {:<15} {:<20} {:<20} {:<15}",
            pvc.metadata.name, status, volume, capacity, access_modes
        );
    }
}

fn print_jobs(jobs: &[Job], no_headers: bool, _show_labels: bool) {
    if !no_headers {
        println!("{:<30} {:<15} {:<10}", "NAME", "COMPLETIONS", "AGE");
    }
    for job in jobs {
        let completions = job.spec.completions.unwrap_or(1);
        let succeeded = job.status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);
        let age = job
            .metadata
            .creation_timestamp
            .map(|ts| format_duration(Utc::now().signed_duration_since(ts)))
            .unwrap_or_else(|| "<unknown>".to_string());
        println!(
            "{:<30} {:<15} {:<10}",
            job.metadata.name,
            format!("{}/{}", succeeded, completions),
            age
        );
    }
}

fn print_cronjobs(cronjobs: &[CronJob], no_headers: bool, _show_labels: bool) {
    if !no_headers {
        println!(
            "{:<30} {:<20} {:<10} {:<20} {:<10}",
            "NAME", "SCHEDULE", "SUSPEND", "ACTIVE", "LAST SCHEDULE"
        );
    }
    for cronjob in cronjobs {
        let schedule = &cronjob.spec.schedule;
        let suspend = cronjob.spec.suspend.unwrap_or(false);
        let active = cronjob.status.as_ref().map(|s| s.active.len()).unwrap_or(0);
        let last_schedule = cronjob
            .status
            .as_ref()
            .and_then(|s| s.last_schedule_time)
            .map(|ts| format_duration(Utc::now().signed_duration_since(ts)))
            .unwrap_or_else(|| "<none>".to_string());
        println!(
            "{:<30} {:<20} {:<10} {:<20} {:<10}",
            cronjob.metadata.name, schedule, suspend, active, last_schedule
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_parse_resource_slash_with_name() {
        let (rtype, name) = parse_resource_slash("pod/nginx");
        assert_eq!(rtype, "pod");
        assert_eq!(name, Some("nginx"));
    }

    #[test]
    fn test_parse_resource_slash_without_name() {
        let (rtype, name) = parse_resource_slash("pods");
        assert_eq!(rtype, "pods");
        assert_eq!(name, None);
    }

    #[test]
    fn test_parse_resource_slash_empty_name() {
        let (rtype, name) = parse_resource_slash("pod/");
        assert_eq!(rtype, "pod");
        assert_eq!(name, None);
    }

    #[test]
    fn test_parse_resource_slash_service() {
        let (rtype, name) = parse_resource_slash("service/frontend");
        assert_eq!(rtype, "service");
        assert_eq!(name, Some("frontend"));
    }

    #[test]
    fn test_comma_separated_resource_types() {
        let input = "pods,services,deployments";
        let types: Vec<&str> = input.split(',').collect();
        assert_eq!(types, vec!["pods", "services", "deployments"]);
    }

    #[test]
    fn test_comma_separated_single_type() {
        let input = "pods";
        let types: Vec<&str> = input.split(',').collect();
        assert_eq!(types, vec!["pods"]);
    }

    #[test]
    fn test_sort_by_jsonpath_metadata_name() {
        use rusternetes_common::types::ObjectMeta;

        let mut pods = vec![
            Pod {
                metadata: ObjectMeta {
                    name: "charlie".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
            Pod {
                metadata: ObjectMeta {
                    name: "alpha".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
            Pod {
                metadata: ObjectMeta {
                    name: "bravo".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
        ];

        sort_by_jsonpath(&mut pods, ".metadata.name");

        assert_eq!(pods[0].metadata.name, "alpha");
        assert_eq!(pods[1].metadata.name, "bravo");
        assert_eq!(pods[2].metadata.name, "charlie");
    }

    #[test]
    fn test_sort_by_jsonpath_with_braces() {
        use rusternetes_common::types::ObjectMeta;

        let mut pods = vec![
            Pod {
                metadata: ObjectMeta {
                    name: "zulu".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
            Pod {
                metadata: ObjectMeta {
                    name: "alpha".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
        ];

        sort_by_jsonpath(&mut pods, "{.metadata.name}");

        assert_eq!(pods[0].metadata.name, "alpha");
        assert_eq!(pods[1].metadata.name, "zulu");
    }

    #[test]
    fn test_resolve_sort_key_nested() {
        let value = serde_json::json!({
            "metadata": {
                "name": "test-pod",
                "namespace": "default"
            },
            "status": {
                "phase": "Running"
            }
        });

        assert_eq!(resolve_sort_key(&value, ".metadata.name"), "test-pod");
        assert_eq!(resolve_sort_key(&value, ".status.phase"), "Running");
        assert_eq!(resolve_sort_key(&value, ".metadata.namespace"), "default");
        assert_eq!(resolve_sort_key(&value, ".nonexistent.field"), "");
    }

    #[test]
    fn test_format_labels_with_labels() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("env".to_string(), "prod".to_string());
        let result = format_labels(&Some(labels));
        // Labels are sorted alphabetically
        assert_eq!(result, "app=nginx,env=prod");
    }

    #[test]
    fn test_format_labels_empty() {
        let result = format_labels(&None);
        assert_eq!(result, "<none>");

        let result = format_labels(&Some(HashMap::new()));
        assert_eq!(result, "<none>");
    }

    #[test]
    fn test_format_labels_single() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());
        let result = format_labels(&Some(labels));
        assert_eq!(result, "app=web");
    }

    #[test]
    fn test_mapper_build_path_pods() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let pod_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Pod".into(),
            plural: "pods".into(),
            singular: "pod".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(
            build_path(&pod_mapping, Some("default"), None),
            "/api/v1/namespaces/default/pods"
        );
        assert_eq!(
            build_path(&pod_mapping, Some("kube-system"), None),
            "/api/v1/namespaces/kube-system/pods"
        );
    }

    #[test]
    fn test_mapper_build_path_deployments() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let dep_mapping = ResourceMapping {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
            plural: "deployments".into(),
            singular: "deployment".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(
            build_path(&dep_mapping, Some("default"), None),
            "/apis/apps/v1/namespaces/default/deployments"
        );
    }

    #[test]
    fn test_mapper_build_path_nodes() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let node_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Node".into(),
            plural: "nodes".into(),
            singular: "node".into(),
            namespaced: false,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(build_path(&node_mapping, None, None), "/api/v1/nodes");
    }

    #[test]
    fn test_mapper_build_path_namespaces() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let ns_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Namespace".into(),
            plural: "namespaces".into(),
            singular: "namespace".into(),
            namespaced: false,
            verbs: vec![],
            short_names: vec!["ns".into()],
        };
        assert_eq!(build_path(&ns_mapping, None, None), "/api/v1/namespaces");
    }

    #[test]
    fn test_no_headers_suppresses_header() {
        // Verify print_pods with no_headers=true doesn't print header
        // Just verify the function signature accepts no_headers=true
        // (actual stdout capture would need more infrastructure)
        let pods: Vec<Pod> = vec![];
        // This should not panic
        print_pods(&pods, true, false);
        print_pods(&pods, false, false);
    }

    #[test]
    fn test_show_labels_with_pods() {
        // Verify print_pods with show_labels=true doesn't panic
        let pods: Vec<Pod> = vec![];
        print_pods(&pods, false, true);
        print_pods(&pods, true, true);
    }

    #[test]
    fn test_map_get_error_not_found() {
        let err = map_get_error(GetError::NotFound);
        assert_eq!(err.to_string(), "Resource not found");
    }

    #[test]
    fn test_map_get_error_other() {
        let err = map_get_error(GetError::Other(anyhow::anyhow!("connection refused")));
        assert_eq!(err.to_string(), "connection refused");
    }

    #[test]
    fn test_output_format_json() {
        let fmt = OutputFormat::from_str("json").unwrap();
        assert_eq!(fmt, OutputFormat::Json);
    }

    #[test]
    fn test_output_format_yaml() {
        let fmt = OutputFormat::from_str("yaml").unwrap();
        assert_eq!(fmt, OutputFormat::Yaml);
    }

    #[test]
    fn test_output_format_wide() {
        let fmt = OutputFormat::from_str("wide").unwrap();
        assert_eq!(fmt, OutputFormat::Wide);
    }

    #[test]
    fn test_output_format_name() {
        let fmt = OutputFormat::from_str("name").unwrap();
        assert_eq!(fmt, OutputFormat::Name);
    }

    #[test]
    fn test_output_format_jsonpath() {
        let fmt = OutputFormat::from_str("jsonpath={.metadata.name}").unwrap();
        assert_eq!(fmt, OutputFormat::JsonPath("{.metadata.name}".to_string()));
    }

    #[test]
    fn test_output_format_unknown() {
        let result = OutputFormat::from_str("csv");
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_jsonpath_simple_field() {
        let val = serde_json::json!({"metadata": {"name": "test"}});
        let result = evaluate_jsonpath(&val, "{.metadata.name}").unwrap();
        assert_eq!(result, "test");
    }

    #[test]
    fn test_evaluate_jsonpath_no_braces() {
        let val = serde_json::json!({"metadata": {"name": "test"}});
        let result = evaluate_jsonpath(&val, ".metadata.name").unwrap();
        assert_eq!(result, "test");
    }

    #[test]
    fn test_evaluate_jsonpath_missing_field() {
        let val = serde_json::json!({"metadata": {"name": "test"}});
        let result = evaluate_jsonpath(&val, "{.metadata.labels}").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_resolve_path_string_literal() {
        let val = serde_json::json!({});
        let result = resolve_path(&val, "\"hello\\nworld\"").unwrap();
        assert_eq!(result, "hello\nworld");
    }

    #[test]
    fn test_resolve_path_empty() {
        let val = serde_json::json!("root_value");
        let result = resolve_path(&val, ".").unwrap();
        assert_eq!(result, "root_value");
    }

    #[test]
    fn test_resolve_path_array_index() {
        let val = serde_json::json!({"items": ["a", "b", "c"]});
        let result = resolve_path(&val, ".items[1]").unwrap();
        assert_eq!(result, "b");
    }

    #[test]
    fn test_resolve_path_array_index_out_of_bounds() {
        let val = serde_json::json!({"items": ["a"]});
        let result = resolve_path(&val, ".items[5]").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_format_value_string() {
        let val = serde_json::Value::String("hello".to_string());
        assert_eq!(format_value(&val).unwrap(), "hello");
    }

    #[test]
    fn test_format_value_null() {
        let val = serde_json::Value::Null;
        assert_eq!(format_value(&val).unwrap(), "");
    }

    #[test]
    fn test_format_value_bool() {
        let val = serde_json::Value::Bool(true);
        assert_eq!(format_value(&val).unwrap(), "true");
    }

    #[test]
    fn test_format_value_number() {
        let val = serde_json::json!(42);
        assert_eq!(format_value(&val).unwrap(), "42");
    }

    #[test]
    fn test_format_value_array() {
        let val = serde_json::json!(["a", "b", "c"]);
        assert_eq!(format_value(&val).unwrap(), "a b c");
    }

    #[test]
    fn test_format_value_object() {
        let val = serde_json::json!({"key": "val"});
        let result = format_value(&val).unwrap();
        assert!(result.contains("key"));
        assert!(result.contains("val"));
    }

    #[test]
    fn test_urlencoding_encode_passthrough() {
        assert_eq!(urlencoding::encode("app=nginx"), "app=nginx");
    }

    #[test]
    fn test_urlencoding_encode_spaces() {
        assert_eq!(urlencoding::encode("hello world"), "hello+world");
    }

    #[test]
    fn test_urlencoding_encode_special_chars() {
        let encoded = urlencoding::encode("key=val&other");
        assert!(encoded.contains("%26"));
    }

    #[test]
    fn test_format_duration_days() {
        let d = chrono::Duration::days(5);
        assert_eq!(format_duration(d), "5d");
    }

    #[test]
    fn test_format_duration_hours() {
        let d = chrono::Duration::hours(3);
        assert_eq!(format_duration(d), "3h");
    }

    #[test]
    fn test_format_duration_minutes() {
        let d = chrono::Duration::minutes(45);
        assert_eq!(format_duration(d), "45m");
    }

    #[test]
    fn test_format_duration_seconds() {
        let d = chrono::Duration::seconds(30);
        assert_eq!(format_duration(d), "30s");
    }

    #[test]
    fn test_format_duration_zero() {
        let d = chrono::Duration::seconds(0);
        assert_eq!(format_duration(d), "0s");
    }

    #[test]
    fn test_print_services_no_panic() {
        let services: Vec<Service> = vec![];
        print_services(&services, false, false);
        print_services(&services, true, false);
        print_services(&services, false, true);
    }

    #[test]
    fn test_print_deployments_no_panic() {
        let deployments: Vec<Deployment> = vec![];
        print_deployments(&deployments, false, false);
        print_deployments(&deployments, true, false);
        print_deployments(&deployments, false, true);
    }

    #[test]
    fn test_print_nodes_no_panic() {
        let nodes: Vec<Node> = vec![];
        print_nodes(&nodes, false, false);
        print_nodes(&nodes, true, false);
        print_nodes(&nodes, false, true);
    }

    #[test]
    fn test_print_namespaces_no_panic() {
        let namespaces: Vec<Namespace> = vec![];
        print_namespaces(&namespaces, false, false);
        print_namespaces(&namespaces, true, false);
        print_namespaces(&namespaces, false, true);
    }

    #[test]
    fn test_print_pvs_no_panic() {
        let pvs: Vec<PersistentVolume> = vec![];
        print_pvs(&pvs, false, false);
        print_pvs(&pvs, true, false);
    }

    #[test]
    fn test_print_pvcs_no_panic() {
        let pvcs: Vec<PersistentVolumeClaim> = vec![];
        print_pvcs(&pvcs, false, false);
        print_pvcs(&pvcs, true, false);
    }

    #[test]
    fn test_print_jobs_no_panic() {
        let jobs: Vec<Job> = vec![];
        print_jobs(&jobs, false, false);
        print_jobs(&jobs, true, false);
    }

    #[test]
    fn test_print_cronjobs_no_panic() {
        let cronjobs: Vec<CronJob> = vec![];
        print_cronjobs(&cronjobs, false, false);
        print_cronjobs(&cronjobs, true, false);
    }

    #[test]
    fn test_mapper_build_path_services() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let svc_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Service".into(),
            plural: "services".into(),
            singular: "service".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["svc".into()],
        };
        assert_eq!(
            build_path(&svc_mapping, Some("default"), None),
            "/api/v1/namespaces/default/services"
        );
        assert_eq!(
            build_path(&svc_mapping, Some("test-ns"), None),
            "/api/v1/namespaces/test-ns/services"
        );
    }

    #[test]
    fn test_mapper_build_path_configmaps() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let cm_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "ConfigMap".into(),
            plural: "configmaps".into(),
            singular: "configmap".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["cm".into()],
        };
        assert_eq!(
            build_path(&cm_mapping, Some("default"), None),
            "/api/v1/namespaces/default/configmaps"
        );
    }

    #[test]
    fn test_mapper_build_path_jobs() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let job_mapping = ResourceMapping {
            group: "batch".into(),
            version: "v1".into(),
            kind: "Job".into(),
            plural: "jobs".into(),
            singular: "job".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(
            build_path(&job_mapping, Some("default"), None),
            "/apis/batch/v1/namespaces/default/jobs"
        );
    }

    #[test]
    fn test_mapper_build_path_cronjobs() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let cj_mapping = ResourceMapping {
            group: "batch".into(),
            version: "v1".into(),
            kind: "CronJob".into(),
            plural: "cronjobs".into(),
            singular: "cronjob".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["cj".into()],
        };
        assert_eq!(
            build_path(&cj_mapping, Some("default"), None),
            "/apis/batch/v1/namespaces/default/cronjobs"
        );
    }

    #[test]
    fn test_mapper_build_path_statefulsets() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let sts_mapping = ResourceMapping {
            group: "apps".into(),
            version: "v1".into(),
            kind: "StatefulSet".into(),
            plural: "statefulsets".into(),
            singular: "statefulset".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["sts".into()],
        };
        assert_eq!(
            build_path(&sts_mapping, Some("default"), None),
            "/apis/apps/v1/namespaces/default/statefulsets"
        );
    }

    #[test]
    fn test_mapper_build_path_daemonsets() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let ds_mapping = ResourceMapping {
            group: "apps".into(),
            version: "v1".into(),
            kind: "DaemonSet".into(),
            plural: "daemonsets".into(),
            singular: "daemonset".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["ds".into()],
        };
        assert_eq!(
            build_path(&ds_mapping, Some("default"), None),
            "/apis/apps/v1/namespaces/default/daemonsets"
        );
    }

    #[test]
    fn test_resolve_sort_key_number() {
        let val = serde_json::json!({"spec": {"replicas": 3}});
        let key = resolve_sort_key(&val, ".spec.replicas");
        assert!(key.contains("3"));
    }

    #[test]
    fn test_resolve_sort_key_bool() {
        let val = serde_json::json!({"spec": {"active": true}});
        let key = resolve_sort_key(&val, ".spec.active");
        assert_eq!(key, "true");
    }

    #[test]
    fn test_resolve_sort_key_null() {
        let val = serde_json::json!({"spec": {"field": null}});
        let key = resolve_sort_key(&val, ".spec.field");
        assert_eq!(key, "");
    }

    #[test]
    fn test_evaluate_jsonpath_filter_returns_empty() {
        let val = serde_json::json!({"items": [{"type": "Ready"}]});
        let result = evaluate_jsonpath(&val, "{.items[?(@.type==\"Ready\")]}").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_resolve_path_tab_literal() {
        let val = serde_json::json!({});
        let result = resolve_path(&val, "\"col1\\tcol2\"").unwrap();
        assert_eq!(result, "col1\tcol2");
    }

    // --- 26 additional tests below ---

    #[test]
    fn test_format_output_json_mode() {
        let resource = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
        let result = format_output(&resource, &OutputFormat::Json);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_yaml_mode() {
        let resource = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
        let result = format_output(&resource, &OutputFormat::Yaml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_name_mode() {
        let resource = serde_json::json!({"kind": "Pod", "metadata": {"name": "nginx"}});
        let result = format_output(&resource, &OutputFormat::Name);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_jsonpath_mode() {
        let resource = serde_json::json!({"kind": "Pod", "metadata": {"name": "test-pod"}});
        let result = format_output(
            &resource,
            &OutputFormat::JsonPath("{.metadata.name}".to_string()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_table_mode() {
        let resource = serde_json::json!({"kind": "Pod"});
        let result = format_output(&resource, &OutputFormat::Table);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_wide_mode() {
        let resource = serde_json::json!({"kind": "Pod"});
        let result = format_output(&resource, &OutputFormat::Wide);
        assert!(result.is_ok());
    }

    #[test]
    fn test_resolve_path_nested_array() {
        let val = serde_json::json!({"spec": {"containers": [{"name": "c1"}, {"name": "c2"}]}});
        let result = resolve_path(&val, ".spec.containers[0].name").unwrap();
        assert_eq!(result, "c1");
    }

    #[test]
    fn test_resolve_path_missing_field() {
        let val = serde_json::json!({"metadata": {"name": "test"}});
        let result = resolve_path(&val, ".spec.nodeName").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_evaluate_jsonpath_nested_object() {
        let val = serde_json::json!({"status": {"phase": "Running"}});
        let result = evaluate_jsonpath(&val, "{.status.phase}").unwrap();
        assert_eq!(result, "Running");
    }

    #[test]
    fn test_evaluate_jsonpath_array() {
        let val = serde_json::json!({"items": ["a", "b", "c"]});
        let result = evaluate_jsonpath(&val, "{.items[2]}").unwrap();
        assert_eq!(result, "c");
    }

    #[test]
    fn test_resolve_sort_key_object() {
        let val = serde_json::json!({"metadata": {"labels": {"app": "nginx"}}});
        let key = resolve_sort_key(&val, ".metadata.labels");
        // Object should be serialized as JSON string
        assert!(key.contains("app"));
    }

    #[test]
    fn test_sort_by_jsonpath_empty_vec() {
        let mut items: Vec<serde_json::Value> = vec![];
        sort_by_jsonpath(&mut items, ".metadata.name");
        assert!(items.is_empty());
    }

    #[test]
    fn test_sort_by_jsonpath_single_item() {
        let mut items = vec![serde_json::json!({"metadata": {"name": "only"}})];
        sort_by_jsonpath(&mut items, ".metadata.name");
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn test_mapper_build_path_serviceaccounts() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let sa_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "ServiceAccount".into(),
            plural: "serviceaccounts".into(),
            singular: "serviceaccount".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["sa".into()],
        };
        assert_eq!(
            build_path(&sa_mapping, Some("default"), None),
            "/api/v1/namespaces/default/serviceaccounts"
        );
    }

    #[test]
    fn test_mapper_build_path_persistentvolumes() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let pv_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "PersistentVolume".into(),
            plural: "persistentvolumes".into(),
            singular: "persistentvolume".into(),
            namespaced: false,
            verbs: vec![],
            short_names: vec!["pv".into()],
        };
        // Cluster-scoped: no namespace segment
        assert_eq!(
            build_path(&pv_mapping, None, None),
            "/api/v1/persistentvolumes"
        );
    }

    #[test]
    fn test_mapper_build_path_ingresses() {
        use crate::discovery::ResourceMapping;
        use crate::ops::build_path;
        let ing_mapping = ResourceMapping {
            group: "networking.k8s.io".into(),
            version: "v1".into(),
            kind: "Ingress".into(),
            plural: "ingresses".into(),
            singular: "ingress".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["ing".into()],
        };
        assert_eq!(
            build_path(&ing_mapping, Some("default"), None),
            "/apis/networking.k8s.io/v1/namespaces/default/ingresses"
        );
    }

    #[test]
    fn test_urlencoding_encode_hash() {
        let encoded = urlencoding::encode("key#value");
        assert!(encoded.contains("%23"));
    }

    #[test]
    fn test_urlencoding_encode_slash() {
        let encoded = urlencoding::encode("path/to");
        assert!(encoded.contains("%2F"));
    }

    #[test]
    fn test_format_duration_large_days() {
        let d = chrono::Duration::days(365);
        assert_eq!(format_duration(d), "365d");
    }

    #[test]
    fn test_format_duration_negative() {
        let d = chrono::Duration::seconds(-10);
        // Should still produce some output without panic
        let result = format_duration(d);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_parse_resource_slash_multiple_slashes() {
        let (rtype, name) = parse_resource_slash("pod/my/pod");
        assert_eq!(rtype, "pod");
        assert_eq!(name, Some("my/pod"));
    }

    #[test]
    fn test_format_labels_many_labels() {
        let mut labels = HashMap::new();
        labels.insert("z".to_string(), "last".to_string());
        labels.insert("a".to_string(), "first".to_string());
        labels.insert("m".to_string(), "middle".to_string());
        let result = format_labels(&Some(labels));
        assert!(result.starts_with("a=first"));
        assert!(result.ends_with("z=last"));
    }

    #[test]
    fn test_output_format_jsonpath_file_nonexistent() {
        let result = OutputFormat::from_str("jsonpath-file=/nonexistent/path");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_path_filter_expression() {
        let val = serde_json::json!({"items": [{"type": "Ready"}, {"type": "NotReady"}]});
        let result = resolve_path(&val, ".items[?(@.type==\"Ready\")]").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_resolve_sort_key_empty_path() {
        let val = serde_json::json!({"name": "test"});
        let key = resolve_sort_key(&val, "");
        // empty path with no keys should serialize the whole value
        assert!(!key.is_empty());
    }

    #[test]
    fn test_format_value_nested_array() {
        let val = serde_json::json!([1, 2, 3]);
        let result = format_value(&val).unwrap();
        assert_eq!(result, "1 2 3");
    }
}
