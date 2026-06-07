/// Table output format support for kubectl get commands
///
/// This module implements the Table output format that kubectl uses to display
/// resources in a human-readable table format.
use rusternetes_common::types::ObjectMeta;
use serde::{Deserialize, Serialize};

/// Table is the response format for kubectl get requests with Accept: application/json;as=Table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    /// APIVersion defines the versioned schema
    pub api_version: String,

    /// Kind is always "Table"
    pub kind: String,

    /// Standard list metadata
    pub metadata: TableMetadata,

    /// Column definitions for the table
    pub column_definitions: Vec<ColumnDefinition>,

    /// Rows of data
    pub rows: Vec<TableRow>,
}

/// Metadata for the table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMetadata {
    /// Resource version for the list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,

    /// Continue token for pagination
    #[serde(skip_serializing_if = "Option::is_none", rename = "continue")]
    pub continue_token: Option<String>,

    /// Remaining items count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_item_count: Option<i64>,
}

/// Column definition in the table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnDefinition {
    /// Name of the column
    pub name: String,

    /// Type of the column (e.g., "string", "integer", "date")
    #[serde(rename = "type")]
    pub column_type: String,

    /// Format hint (e.g., "name", "date-time")
    pub format: String,

    /// Description of the column
    pub description: String,

    /// Priority determines visibility (0 = always shown)
    pub priority: i32,
}

/// Single row in the table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableRow {
    /// Cells contain the actual data
    pub cells: Vec<serde_json::Value>,

    /// Object contains the full resource (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<serde_json::Value>,
}

impl Table {
    /// Create a new Table
    pub fn new() -> Self {
        Self {
            api_version: "meta.k8s.io/v1".to_string(),
            kind: "Table".to_string(),
            metadata: TableMetadata {
                resource_version: None,
                continue_token: None,
                remaining_item_count: None,
            },
            column_definitions: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Add a column definition
    pub fn add_column(
        mut self,
        name: &str,
        column_type: &str,
        format: &str,
        description: &str,
        priority: i32,
    ) -> Self {
        self.column_definitions.push(ColumnDefinition {
            name: name.to_string(),
            column_type: column_type.to_string(),
            format: format.to_string(),
            description: description.to_string(),
            priority,
        });
        self
    }

    /// Add a row of data
    pub fn add_row(
        mut self,
        cells: Vec<serde_json::Value>,
        object: Option<serde_json::Value>,
    ) -> Self {
        self.rows.push(TableRow { cells, object });
        self
    }

    /// Set metadata
    pub fn with_metadata(
        mut self,
        resource_version: Option<String>,
        continue_token: Option<String>,
        remaining: Option<i64>,
    ) -> Self {
        self.metadata.resource_version = resource_version;
        self.metadata.continue_token = continue_token;
        self.metadata.remaining_item_count = remaining;
        self
    }
}

impl Default for Table {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to create a table for Pods.
///
/// Delegates column and cell definitions to [`printer_columns`] /
/// [`printer_row_cells`] so the LIST path here stays byte-for-byte consistent
/// with the single-resource GET path served by the response middleware,
/// including the `-o wide` columns (IP, NODE, ...).
pub fn pods_table<T>(pods: Vec<T>, resource_version: Option<String>) -> Table
where
    T: Serialize,
{
    let objects: Vec<serde_json::Value> = pods
        .iter()
        .map(|p| serde_json::to_value(p).unwrap_or(serde_json::Value::Null))
        .collect();
    kinded_table("Pod", objects, resource_version)
}

/// Build a Table for a known kind from already-serialized objects, using the
/// canonical [`printer_columns`] / [`printer_row_cells`]. Falls back to a
/// minimal NAME/AGE table for kinds without a rich printer.
pub fn kinded_table(
    kind: &str,
    objects: Vec<serde_json::Value>,
    resource_version: Option<String>,
) -> Table {
    let mut table = Table::new();
    if let Some(columns) = printer_columns(kind) {
        table.column_definitions = columns;
    } else {
        table = table
            .add_column("NAME", "string", "name", "Name of the resource", 0)
            .add_column("AGE", "string", "", "Age of the resource", 0);
    }

    for obj in objects {
        let cells = printer_row_cells(kind, &obj).unwrap_or_else(|| {
            let name = obj
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            vec![
                serde_json::Value::String(name),
                serde_json::Value::String(json_age(&obj)),
            ]
        });
        table = table.add_row(cells, Some(obj));
    }

    table.with_metadata(resource_version, None, None)
}

/// Helper function to create a table for generic resources with just NAME and AGE
pub fn generic_table<T>(
    resources: Vec<T>,
    resource_version: Option<String>,
    resource_kind: &str,
) -> Table
where
    T: Serialize + HasMetadata,
{
    let mut table = Table::new()
        .add_column(
            "NAME",
            "string",
            "name",
            &format!(
                "Name must be unique within a namespace for {}",
                resource_kind
            ),
            0,
        )
        .add_column(
            "AGE",
            "string",
            "",
            &format!("Age of the {}", resource_kind),
            0,
        );

    for resource in resources {
        let metadata = resource.metadata();
        let name = metadata.name.clone();
        let age = format_age(metadata);

        let cells = vec![
            serde_json::Value::String(name),
            serde_json::Value::String(age),
        ];
        let object = serde_json::to_value(&resource).ok();
        table = table.add_row(cells, object);
    }

    table.with_metadata(resource_version, None, None)
}

/// Trait for extracting metadata from resources
pub trait HasMetadata {
    fn metadata(&self) -> &ObjectMeta;
}

/// Humanize a creation timestamp into kubectl's compact age string
/// (e.g. `5d`, `3h`, `12m`, `42s`). `None` renders as `<unknown>`.
fn humanize_age(creation: Option<chrono::DateTime<chrono::Utc>>) -> String {
    match creation {
        Some(creation_time) => {
            let duration = chrono::Utc::now().signed_duration_since(creation_time);
            if duration.num_days() > 0 {
                format!("{}d", duration.num_days())
            } else if duration.num_hours() > 0 {
                format!("{}h", duration.num_hours())
            } else if duration.num_minutes() > 0 {
                format!("{}m", duration.num_minutes())
            } else {
                format!("{}s", duration.num_seconds().max(0))
            }
        }
        None => "<unknown>".to_string(),
    }
}

/// Format age from typed metadata.
fn format_age(metadata: &ObjectMeta) -> String {
    humanize_age(metadata.creation_timestamp)
}

/// Format age from a JSON object's `metadata.creationTimestamp`.
fn json_age(obj: &serde_json::Value) -> String {
    let creation = obj
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));
    humanize_age(creation)
}

/// Strip a trailing `List` so `PodList`/`NodeList` map to `Pod`/`Node`.
fn normalize_kind(kind: &str) -> &str {
    kind.strip_suffix("List").unwrap_or(kind)
}

fn col(
    name: &str,
    column_type: &str,
    format: &str,
    description: &str,
    priority: i32,
) -> ColumnDefinition {
    ColumnDefinition {
        name: name.to_string(),
        column_type: column_type.to_string(),
        format: format.to_string(),
        description: description.to_string(),
        priority,
    }
}

/// Canonical kubectl printer columns for a resource kind, or `None` when the
/// kind has no rich printer (callers fall back to the generic NAME/AGE table).
///
/// This is the single source of truth shared by the resource LIST handlers
/// (e.g. [`pods_table`]) and the middleware's on-demand Table conversion, so
/// `kubectl get` and `kubectl get -o wide` agree on every code path. Columns
/// with `priority: 1` are the wide columns kubectl only shows under `-o wide`.
pub fn printer_columns(kind: &str) -> Option<Vec<ColumnDefinition>> {
    let cols = match normalize_kind(kind) {
        "Pod" => vec![
            col(
                "NAME",
                "string",
                "name",
                "Name must be unique within a namespace",
                0,
            ),
            col(
                "READY",
                "string",
                "",
                "The aggregate readiness state of this pod for accepting traffic",
                0,
            ),
            col(
                "STATUS",
                "string",
                "",
                "The aggregate state of the containers in this pod",
                0,
            ),
            col(
                "RESTARTS",
                "integer",
                "",
                "The number of times the containers in this pod have been restarted",
                0,
            ),
            col("AGE", "string", "", "Age of the pod", 0),
            col("IP", "string", "", "The IP address assigned to the pod", 1),
            col("NODE", "string", "", "The node this pod is running on", 1),
            col(
                "NOMINATED NODE",
                "string",
                "",
                "The node nominated for preemption to schedule this pod",
                1,
            ),
            col(
                "READINESS GATES",
                "string",
                "",
                "The readiness gate status for the pod",
                1,
            ),
        ],
        "Node" => vec![
            col("NAME", "string", "name", "Name of the node", 0),
            col(
                "STATUS",
                "string",
                "",
                "The readiness status of the node",
                0,
            ),
            col("ROLES", "string", "", "The roles assigned to the node", 0),
            col("AGE", "string", "", "Age of the node", 0),
            col(
                "VERSION",
                "string",
                "",
                "The kubelet version running on the node",
                0,
            ),
            col(
                "INTERNAL-IP",
                "string",
                "",
                "The internal IP address of the node",
                1,
            ),
            col(
                "EXTERNAL-IP",
                "string",
                "",
                "The external IP address of the node",
                1,
            ),
            col("OS-IMAGE", "string", "", "The OS image of the node", 1),
            col(
                "KERNEL-VERSION",
                "string",
                "",
                "The kernel version of the node",
                1,
            ),
            col(
                "CONTAINER-RUNTIME",
                "string",
                "",
                "The container runtime of the node",
                1,
            ),
        ],
        "Namespace" => vec![
            col("NAME", "string", "name", "Name of the namespace", 0),
            col(
                "STATUS",
                "string",
                "",
                "The lifecycle phase of the namespace",
                0,
            ),
            col("AGE", "string", "", "Age of the namespace", 0),
        ],
        _ => return None,
    };
    Some(cols)
}

/// Roles for a node, derived from `node-role.kubernetes.io/<role>` (and the
/// legacy `kubernetes.io/role`) labels, comma-joined and sorted. `<none>`
/// when the node carries no role labels.
fn node_roles(obj: &serde_json::Value) -> String {
    let mut roles: Vec<String> = Vec::new();
    if let Some(labels) = obj
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|v| v.as_object())
    {
        for (key, value) in labels {
            if let Some(role) = key.strip_prefix("node-role.kubernetes.io/") {
                if !role.is_empty() {
                    roles.push(role.to_string());
                }
            } else if key == "kubernetes.io/role" {
                if let Some(role) = value.as_str() {
                    if !role.is_empty() {
                        roles.push(role.to_string());
                    }
                }
            }
        }
    }
    if roles.is_empty() {
        "<none>".to_string()
    } else {
        roles.sort();
        roles.join(",")
    }
}

/// Row cells for one serialized object of `kind`, matching the order of
/// [`printer_columns`] for that kind. Returns `None` for kinds without a rich
/// printer. Defensive against missing fields so partially-populated objects
/// (e.g. a freshly created pod with no status) still render.
pub fn printer_row_cells(kind: &str, obj: &serde_json::Value) -> Option<Vec<serde_json::Value>> {
    use serde_json::Value;

    let name = obj
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let age = json_age(obj);

    let str_at = |obj: &Value, path: &[&str]| -> Option<String> {
        let mut cur = obj;
        for key in path {
            cur = cur.get(key)?;
        }
        cur.as_str().map(|s| s.to_string())
    };

    let cells = match normalize_kind(kind) {
        "Pod" => {
            let status = obj.get("status");
            let phase = status
                .and_then(|s| s.get("phase"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let container_statuses = status
                .and_then(|s| s.get("containerStatuses"))
                .and_then(|v| v.as_array());
            let ready = container_statuses
                .map(|cs| {
                    cs.iter()
                        .filter(|c| c.get("ready").and_then(|v| v.as_bool()).unwrap_or(false))
                        .count()
                })
                .unwrap_or(0);
            let total = container_statuses.map(|cs| cs.len()).unwrap_or(0);
            let restarts: i64 = container_statuses
                .map(|cs| {
                    cs.iter()
                        .map(|c| c.get("restartCount").and_then(|v| v.as_i64()).unwrap_or(0))
                        .sum()
                })
                .unwrap_or(0);
            let ip = str_at(obj, &["status", "podIP"]).unwrap_or_else(|| "<none>".to_string());
            let node = str_at(obj, &["spec", "nodeName"]).unwrap_or_else(|| "<none>".to_string());
            let nominated = str_at(obj, &["status", "nominatedNodeName"])
                .unwrap_or_else(|| "<none>".to_string());
            vec![
                Value::String(name),
                Value::String(format!("{}/{}", ready, total)),
                Value::String(phase.to_string()),
                Value::Number(restarts.into()),
                Value::String(age),
                Value::String(ip),
                Value::String(node),
                Value::String(nominated),
                Value::String("<none>".to_string()),
            ]
        }
        "Node" => {
            let status = obj.get("status");
            let ready = status
                .and_then(|s| s.get("conditions"))
                .and_then(|v| v.as_array())
                .map(|conds| {
                    conds.iter().any(|c| {
                        c.get("type").and_then(|v| v.as_str()) == Some("Ready")
                            && c.get("status").and_then(|v| v.as_str()) == Some("True")
                    })
                })
                .unwrap_or(false);
            let mut node_status = if ready {
                "Ready".to_string()
            } else {
                "NotReady".to_string()
            };
            if obj
                .get("spec")
                .and_then(|s| s.get("unschedulable"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                node_status.push_str(",SchedulingDisabled");
            }
            let roles = node_roles(obj);
            let version = str_at(obj, &["status", "nodeInfo", "kubeletVersion"])
                .unwrap_or_else(|| "<unknown>".to_string());
            let addr_of = |kind: &str| -> String {
                status
                    .and_then(|s| s.get("addresses"))
                    .and_then(|v| v.as_array())
                    .and_then(|addrs| {
                        addrs
                            .iter()
                            .find(|a| a.get("type").and_then(|v| v.as_str()) == Some(kind))
                            .and_then(|a| a.get("address"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_else(|| "<none>".to_string())
            };
            let os_image = str_at(obj, &["status", "nodeInfo", "osImage"])
                .unwrap_or_else(|| "<unknown>".to_string());
            let kernel = str_at(obj, &["status", "nodeInfo", "kernelVersion"])
                .unwrap_or_else(|| "<unknown>".to_string());
            let runtime = str_at(obj, &["status", "nodeInfo", "containerRuntimeVersion"])
                .unwrap_or_else(|| "<unknown>".to_string());
            vec![
                Value::String(name),
                Value::String(node_status),
                Value::String(roles),
                Value::String(age),
                Value::String(version),
                Value::String(addr_of("InternalIP")),
                Value::String(addr_of("ExternalIP")),
                Value::String(os_image),
                Value::String(kernel),
                Value::String(runtime),
            ]
        }
        "Namespace" => {
            let phase = str_at(obj, &["status", "phase"]).unwrap_or_default();
            vec![
                Value::String(name),
                Value::String(phase),
                Value::String(age),
            ]
        }
        _ => return None,
    };
    Some(cells)
}

/// Check if the request wants table format
pub fn wants_table(accept_header: Option<&str>) -> bool {
    if let Some(accept) = accept_header {
        accept.contains("as=Table") || accept.contains("application/json;as=Table")
    } else {
        false
    }
}

// Trait implementations for common resource types

impl HasMetadata for rusternetes_common::resources::Pod {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Deployment {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Service {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicationController {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicaSet {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_table_creation() {
        let table = Table::new()
            .add_column("NAME", "string", "name", "Resource name", 0)
            .add_column("AGE", "string", "", "Resource age", 0);

        assert_eq!(table.kind, "Table");
        assert_eq!(table.api_version, "meta.k8s.io/v1");
        assert_eq!(table.column_definitions.len(), 2);
    }

    #[test]
    fn test_wants_table() {
        assert!(wants_table(Some("application/json;as=Table")));
        assert!(wants_table(Some("application/json;as=Table;v=v1")));
        assert!(!wants_table(Some("application/json")));
        assert!(!wants_table(None));
    }

    #[test]
    fn test_normalize_kind_strips_list() {
        assert_eq!(normalize_kind("PodList"), "Pod");
        assert_eq!(normalize_kind("Pod"), "Pod");
        assert_eq!(normalize_kind("NodeList"), "Node");
    }

    #[test]
    fn pod_columns_include_wide_ip_and_node() {
        let cols = printer_columns("Pod").expect("pods have a printer");
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "NAME",
                "READY",
                "STATUS",
                "RESTARTS",
                "AGE",
                "IP",
                "NODE",
                "NOMINATED NODE",
                "READINESS GATES"
            ]
        );
        // IP and NODE must be wide (priority 1) so plain `get` hides them
        // but `-o wide` reveals them.
        let by_name = |n: &str| cols.iter().find(|c| c.name == n).unwrap();
        assert_eq!(by_name("STATUS").priority, 0);
        assert_eq!(by_name("IP").priority, 1);
        assert_eq!(by_name("NODE").priority, 1);
        // PodList maps to the same columns.
        assert_eq!(printer_columns("PodList").unwrap().len(), cols.len());
    }

    #[test]
    fn pod_row_cells_extract_ip_and_node() {
        let pod = json!({
            "metadata": {"name": "smoke", "creationTimestamp": "2026-06-01T00:00:00Z"},
            "spec": {"nodeName": "node-1"},
            "status": {
                "phase": "Running",
                "podIP": "172.18.0.9",
                "containerStatuses": [
                    {"ready": true, "restartCount": 2},
                    {"ready": false, "restartCount": 1}
                ]
            }
        });
        let cells = printer_row_cells("Pod", &pod).expect("pod cells");
        assert_eq!(cells[0], json!("smoke")); // NAME
        assert_eq!(cells[1], json!("1/2")); // READY
        assert_eq!(cells[2], json!("Running")); // STATUS
        assert_eq!(cells[3], json!(3)); // RESTARTS (2+1)
        assert_eq!(cells[5], json!("172.18.0.9")); // IP (wide)
        assert_eq!(cells[6], json!("node-1")); // NODE (wide)
        assert_eq!(cells[7], json!("<none>")); // NOMINATED NODE
    }

    #[test]
    fn pod_row_cells_tolerate_missing_status() {
        let pod = json!({"metadata": {"name": "fresh"}, "spec": {}});
        let cells = printer_row_cells("Pod", &pod).expect("pod cells");
        assert_eq!(cells[0], json!("fresh"));
        assert_eq!(cells[1], json!("0/0")); // READY
        assert_eq!(cells[3], json!(0)); // RESTARTS
        assert_eq!(cells[5], json!("<none>")); // IP
        assert_eq!(cells[6], json!("<none>")); // NODE
    }

    #[test]
    fn node_row_cells_status_roles_and_wide() {
        let node = json!({
            "metadata": {
                "name": "node-1",
                "creationTimestamp": "2026-06-01T00:00:00Z",
                "labels": {
                    "node-role.kubernetes.io/control-plane": "",
                    "kubernetes.io/os": "linux"
                }
            },
            "spec": {"unschedulable": true},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "addresses": [
                    {"type": "InternalIP", "address": "172.18.0.8"},
                    {"type": "Hostname", "address": "node-1"}
                ],
                "nodeInfo": {
                    "kubeletVersion": "v1.35.0-rusternetes",
                    "osImage": "Rusternetes OS",
                    "kernelVersion": "6.1.0",
                    "containerRuntimeVersion": "containerd://1.7.0"
                }
            }
        });
        let cells = printer_row_cells("Node", &node).expect("node cells");
        assert_eq!(cells[0], json!("node-1")); // NAME
        assert_eq!(cells[1], json!("Ready,SchedulingDisabled")); // STATUS
        assert_eq!(cells[2], json!("control-plane")); // ROLES
        assert_eq!(cells[4], json!("v1.35.0-rusternetes")); // VERSION
        assert_eq!(cells[5], json!("172.18.0.8")); // INTERNAL-IP (wide)
        assert_eq!(cells[6], json!("<none>")); // EXTERNAL-IP (wide)
        assert_eq!(cells[7], json!("Rusternetes OS")); // OS-IMAGE (wide)
    }

    #[test]
    fn node_not_ready_when_no_ready_condition() {
        let node = json!({
            "metadata": {"name": "n", "labels": {}},
            "status": {"conditions": [{"type": "MemoryPressure", "status": "False"}]}
        });
        let cells = printer_row_cells("Node", &node).unwrap();
        assert_eq!(cells[1], json!("NotReady"));
        assert_eq!(cells[2], json!("<none>")); // ROLES
    }

    #[test]
    fn namespace_row_cells_show_phase() {
        let ns = json!({
            "metadata": {"name": "default", "creationTimestamp": "2026-06-01T00:00:00Z"},
            "status": {"phase": "Active"}
        });
        let cells = printer_row_cells("Namespace", &ns).expect("ns cells");
        assert_eq!(cells[0], json!("default"));
        assert_eq!(cells[1], json!("Active"));
        let cols = printer_columns("Namespace").unwrap();
        assert_eq!(cells.len(), cols.len());
    }

    #[test]
    fn unknown_kind_has_no_printer() {
        assert!(printer_columns("ConfigMap").is_none());
        assert!(printer_row_cells("ConfigMap", &json!({})).is_none());
    }

    #[test]
    fn pods_table_matches_printer_columns() {
        use rusternetes_common::resources::Pod;
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "smoke", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}], "nodeName": "node-1"},
            "status": {"phase": "Running", "podIP": "10.0.0.5"}
        }))
        .expect("valid pod");
        let table = pods_table(vec![pod], Some("42".to_string()));
        let expected: Vec<String> = printer_columns("Pod")
            .unwrap()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let got: Vec<String> = table
            .column_definitions
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(got, expected);
        assert_eq!(table.rows.len(), 1);
        // Cell count must match column count.
        assert_eq!(table.rows[0].cells.len(), table.column_definitions.len());
    }
}
