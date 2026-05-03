use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

/// Version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub major: String,
    pub minor: String,
    #[serde(rename = "gitVersion")]
    pub git_version: String,
    #[serde(rename = "gitCommit")]
    pub git_commit: String,
    #[serde(rename = "gitTreeState")]
    pub git_tree_state: String,
    #[serde(rename = "buildDate")]
    pub build_date: String,
    #[serde(rename = "goVersion")]
    pub go_version: String,
    pub compiler: String,
    pub platform: String,
}

/// APIVersions lists the versions that are available, to allow clients to discover
/// the API at /api
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct APIVersions {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub versions: Vec<String>,
    #[serde(rename = "serverAddressByClientCIDRs")]
    pub server_address_by_client_cidrs: Vec<ServerAddressByClientCIDR>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerAddressByClientCIDR {
    #[serde(rename = "clientCIDR")]
    pub client_cidr: String,
    #[serde(rename = "serverAddress")]
    pub server_address: String,
}

/// APIGroupList is a list of APIGroup, to allow clients to discover the API at /apis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct APIGroupList {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub groups: Vec<APIGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct APIGroup {
    pub name: String,
    pub versions: Vec<GroupVersionForDiscovery>,
    #[serde(rename = "preferredVersion")]
    pub preferred_version: GroupVersionForDiscovery,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupVersionForDiscovery {
    #[serde(rename = "groupVersion")]
    pub group_version: String,
    pub version: String,
}

/// Check if the Accept header SPECIFICALLY requests aggregated discovery format.
/// Only return aggregated format if the client explicitly prefers it
/// (i.e., the aggregated type appears BEFORE plain application/json).
/// Some clients (like sonobuoy) send both types in Accept but expect the standard format.
/// Returns the aggregated discovery version to use based on Accept header,
/// or None if the client doesn't want aggregated discovery.
fn aggregated_discovery_version(headers: &HeaderMap) -> Option<&'static str> {
    let accept = headers.get("accept").and_then(|v| v.to_str().ok())?;
    if !accept.contains("apidiscovery.k8s.io") {
        return None;
    }

    // Determine which version the client prefers
    let wants_v2 = accept.contains("v=v2;as=APIGroupDiscoveryList")
        || accept.contains("v=v2;as=APIGroupDiscoveryList;profile=nopeer");
    let wants_v2beta1 = accept.contains("v=v2beta1;as=APIGroupDiscoveryList");

    // Check q-values
    let mut agg_q: f32 = 1.0;
    let mut plain_q: f32 = -1.0;

    for part in accept.split(',') {
        let part = part.trim();
        let q = part
            .split(";q=")
            .nth(1)
            .and_then(|q| q.trim().parse::<f32>().ok())
            .unwrap_or(1.0);

        if part.contains("apidiscovery.k8s.io") {
            agg_q = q;
        } else if part.starts_with("application/json") && !part.contains("apidiscovery") {
            plain_q = q;
        }
    }

    if plain_q >= 0.0 && agg_q < plain_q {
        return None; // Plain JSON explicitly preferred
    }

    // Return the version the client asked for
    if wants_v2 {
        Some("v2")
    } else if wants_v2beta1 {
        Some("v2beta1")
    } else {
        Some("v2") // Default to v2
    }
}

#[allow(dead_code)]
fn wants_aggregated_discovery(headers: &HeaderMap) -> bool {
    aggregated_discovery_version(headers).is_some()
}

/// GET /api
/// Returns the list of API versions available at /api/v1
pub async fn get_core_api(headers: HeaderMap) -> Response {
    if let Some(disc_version) = aggregated_discovery_version(&headers) {
        // Return aggregated discovery format for /api (core API)
        let core_resources = get_aggregated_resources_for_group("", "v1");
        let api_version = format!("apidiscovery.k8s.io/{}", disc_version);
        let discovery = serde_json::json!({
            "kind": "APIGroupDiscoveryList",
            "apiVersion": api_version,
            "metadata": {},
            "items": [
                {
                    "metadata": {
                        "name": ""
                    },
                    "versions": [
                        {
                            "version": "v1",
                            "resources": core_resources,
                            "freshness": "Current"
                        }
                    ]
                }
            ]
        });
        let ct = format!(
            "application/json;g=apidiscovery.k8s.io;v={};as=APIGroupDiscoveryList",
            disc_version
        );
        let json_bytes = serde_json::to_vec(&discovery).unwrap_or_default();
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, ct)
            .body(Body::from(json_bytes))
            .unwrap();
    }

    let api_versions = APIVersions {
        kind: "APIVersions".to_string(),
        api_version: "v1".to_string(),
        versions: vec!["v1".to_string()],
        server_address_by_client_cidrs: vec![ServerAddressByClientCIDR {
            client_cidr: "0.0.0.0/0".to_string(),
            server_address: "".to_string(),
        }],
    };

    let json_bytes = serde_json::to_vec(&api_versions).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, json_bytes.len().to_string())
        .body(Body::from(json_bytes))
        .unwrap()
}

/// GET /apis
/// Returns the list of API groups available
pub async fn get_api_groups(
    state: Option<axum::extract::State<std::sync::Arc<crate::state::ApiServerState>>>,
    headers: HeaderMap,
) -> Response {
    if let Some(disc_version) = aggregated_discovery_version(&headers) {
        // Return aggregated discovery format for /apis with inline resources
        // Build groups, handling autoscaling specially (v1 + v2)
        let mut groups: Vec<serde_json::Value> = Vec::new();
        let mut seen_groups: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (name, version) in get_api_group_names() {
            if seen_groups.contains(name) {
                continue;
            }
            seen_groups.insert(name.to_string());
            let versions = if name == "autoscaling" {
                vec![
                    serde_json::json!({
                        "version": "v2",
                        "resources": get_aggregated_resources_for_group("autoscaling", "v2"),
                        "freshness": "Current"
                    }),
                    serde_json::json!({
                        "version": "v1",
                        "resources": get_aggregated_resources_for_group("autoscaling", "v1"),
                        "freshness": "Current"
                    }),
                ]
            } else {
                vec![serde_json::json!({
                    "version": version,
                    "resources": get_aggregated_resources_for_group(name, version),
                    "freshness": "Current"
                })]
            };
            groups.push(serde_json::json!({
                "metadata": { "name": name },
                "versions": versions
            }));
        }

        // Dynamically add CRD groups from storage.
        // Multiple CRDs can share the same group (e.g., different test CRDs under
        // "test.example.com"). Each CRD is a separate resource within the group.
        // K8s aggregated discovery returns one group entry with ALL resources nested
        // under their respective versions.
        if let Some(axum::extract::State(ref st)) = state {
            use rusternetes_storage::Storage;
            let crd_prefix = rusternetes_storage::build_prefix("customresourcedefinitions", None);
            if let Ok(crds) = st.storage.list::<serde_json::Value>(&crd_prefix).await {
                // Collect resources per group+version, then emit one group entry per group.
                // Key: (group, version) → Vec<resource_entry>
                let mut group_version_resources: std::collections::HashMap<
                    (String, String),
                    Vec<serde_json::Value>,
                > = std::collections::HashMap::new();
                let all_verbs = vec![
                    "create",
                    "delete",
                    "deletecollection",
                    "get",
                    "list",
                    "patch",
                    "update",
                    "watch",
                ];
                for crd in &crds {
                    let group = crd.pointer("/spec/group").and_then(|v| v.as_str());
                    let names = crd.pointer("/spec/names");
                    let versions_arr = crd.pointer("/spec/versions").and_then(|v| v.as_array());
                    if let (Some(group), Some(names), Some(versions_arr)) =
                        (group, names, versions_arr)
                    {
                        let plural = names.get("plural").and_then(|v| v.as_str()).unwrap_or("");
                        let singular = names.get("singular").and_then(|v| v.as_str()).unwrap_or("");
                        let kind = names.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                        let scope = crd
                            .pointer("/spec/scope")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Namespaced");
                        for ver in versions_arr {
                            let served =
                                ver.get("served").and_then(|v| v.as_bool()).unwrap_or(false);
                            if !served {
                                continue;
                            }
                            if let Some(ver_name) = ver.get("name").and_then(|v| v.as_str()) {
                                let mut subresources = Vec::new();
                                if ver.pointer("/subresources/status").is_some()
                                    || crd.pointer("/spec/subresources/status").is_some()
                                {
                                    subresources.push(serde_json::json!({
                                        "subresource": "status",
                                        "responseKind": { "group": group, "version": ver_name, "kind": kind },
                                        "verbs": ["get", "patch", "update"]
                                    }));
                                }
                                if ver.pointer("/subresources/scale").is_some()
                                    || crd.pointer("/spec/subresources/scale").is_some()
                                {
                                    subresources.push(serde_json::json!({
                                        "subresource": "scale",
                                        "responseKind": { "group": "autoscaling", "version": "v1", "kind": "Scale" },
                                        "verbs": ["get", "patch", "update"]
                                    }));
                                }
                                let mut resource_entry = serde_json::json!({
                                    "resource": plural,
                                    "responseKind": { "group": group, "version": ver_name, "kind": kind },
                                    "scope": scope, "singularResource": singular, "verbs": all_verbs
                                });
                                if !subresources.is_empty() {
                                    resource_entry.as_object_mut().unwrap().insert(
                                        "subresources".to_string(),
                                        serde_json::json!(subresources),
                                    );
                                }
                                group_version_resources
                                    .entry((group.to_string(), ver_name.to_string()))
                                    .or_default()
                                    .push(resource_entry);
                            }
                        }
                    }
                }
                // Build one group entry per unique group, with all versions and resources
                let mut crd_groups: std::collections::HashMap<String, Vec<serde_json::Value>> =
                    std::collections::HashMap::new();
                for ((group, version), resources) in &group_version_resources {
                    crd_groups
                        .entry(group.clone())
                        .or_default()
                        .push(serde_json::json!({
                            "version": version,
                            "resources": resources,
                            "freshness": "Current"
                        }));
                }
                for (group, versions) in crd_groups {
                    if !seen_groups.contains(&group) {
                        seen_groups.insert(group.clone());
                        groups.push(
                            serde_json::json!({ "metadata": { "name": group }, "versions": versions }),
                        );
                    }
                }
            }
        }

        let discovery = serde_json::json!({
            "kind": "APIGroupDiscoveryList",
            "apiVersion": format!("apidiscovery.k8s.io/{}", disc_version),
            "metadata": {},
            "items": groups
        });
        let ct = format!(
            "application/json;g=apidiscovery.k8s.io;v={};as=APIGroupDiscoveryList",
            disc_version
        );
        let json_bytes = serde_json::to_vec(&discovery).unwrap_or_default();
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, ct)
            .body(Body::from(json_bytes))
            .unwrap();
    }

    let mut groups = vec![
        // apps API group
        APIGroup {
            name: "apps".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "apps/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "apps/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // batch API group
        APIGroup {
            name: "batch".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "batch/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "batch/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // networking.k8s.io API group
        APIGroup {
            name: "networking.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "networking.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "networking.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // rbac.authorization.k8s.io API group
        APIGroup {
            name: "rbac.authorization.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "rbac.authorization.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "rbac.authorization.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // storage.k8s.io API group
        APIGroup {
            name: "storage.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "storage.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "storage.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // snapshot.storage.k8s.io API group
        APIGroup {
            name: "snapshot.storage.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "snapshot.storage.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "snapshot.storage.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // scheduling.k8s.io API group
        APIGroup {
            name: "scheduling.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "scheduling.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "scheduling.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // apiextensions.k8s.io API group
        APIGroup {
            name: "apiextensions.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "apiextensions.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "apiextensions.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // admissionregistration.k8s.io API group
        APIGroup {
            name: "admissionregistration.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "admissionregistration.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "admissionregistration.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // coordination.k8s.io API group
        APIGroup {
            name: "coordination.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "coordination.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "coordination.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // flowcontrol.apiserver.k8s.io API group
        APIGroup {
            name: "flowcontrol.apiserver.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "flowcontrol.apiserver.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "flowcontrol.apiserver.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // certificates.k8s.io API group
        APIGroup {
            name: "certificates.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "certificates.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "certificates.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // discovery.k8s.io API group
        APIGroup {
            name: "discovery.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "discovery.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "discovery.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // node.k8s.io API group
        APIGroup {
            name: "node.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "node.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "node.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // authentication.k8s.io API group
        APIGroup {
            name: "authentication.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "authentication.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "authentication.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // authorization.k8s.io API group
        APIGroup {
            name: "authorization.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "authorization.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "authorization.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // autoscaling API group
        APIGroup {
            name: "autoscaling".to_string(),
            versions: vec![
                GroupVersionForDiscovery {
                    group_version: "autoscaling/v1".to_string(),
                    version: "v1".to_string(),
                },
                GroupVersionForDiscovery {
                    group_version: "autoscaling/v2".to_string(),
                    version: "v2".to_string(),
                },
            ],
            preferred_version: GroupVersionForDiscovery {
                group_version: "autoscaling/v2".to_string(),
                version: "v2".to_string(),
            },
        },
        // policy API group
        APIGroup {
            name: "policy".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "policy/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "policy/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // metrics.k8s.io API group
        APIGroup {
            name: "metrics.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "metrics.k8s.io/v1beta1".to_string(),
                version: "v1beta1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "metrics.k8s.io/v1beta1".to_string(),
                version: "v1beta1".to_string(),
            },
        },
        // custom.metrics.k8s.io API group
        APIGroup {
            name: "custom.metrics.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "custom.metrics.k8s.io/v1beta2".to_string(),
                version: "v1beta2".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "custom.metrics.k8s.io/v1beta2".to_string(),
                version: "v1beta2".to_string(),
            },
        },
        // resource.k8s.io API group
        APIGroup {
            name: "resource.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "resource.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "resource.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // events.k8s.io API group
        APIGroup {
            name: "events.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "events.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "events.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        // apiregistration.k8s.io API group
        APIGroup {
            name: "apiregistration.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "apiregistration.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "apiregistration.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
    ];

    // Dynamically add CRD groups to the non-aggregated discovery response.
    // kubectl uses this to find resources by GVK. Without CRD groups here,
    // kubectl create/explain fails with "no matches for kind".
    // K8s ref: apiextensions-apiserver registers CRD API groups dynamically.
    if let Some(axum::extract::State(ref st)) = state {
        use rusternetes_storage::Storage;
        let crd_prefix = rusternetes_storage::build_prefix("customresourcedefinitions", None);
        if let Ok(crds) = st.storage.list::<serde_json::Value>(&crd_prefix).await {
            let mut seen_groups: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for crd in &crds {
                let group = crd.pointer("/spec/group").and_then(|v| v.as_str());
                let versions_arr = crd.pointer("/spec/versions").and_then(|v| v.as_array());
                if let (Some(group), Some(versions_arr)) = (group, versions_arr) {
                    if seen_groups.contains(group) {
                        continue;
                    }
                    seen_groups.insert(group.to_string());
                    let mut gv_versions = Vec::new();
                    let mut preferred = None;
                    for ver in versions_arr {
                        let ver_name = ver.get("name").and_then(|v| v.as_str()).unwrap_or("v1");
                        let served = ver.get("served").and_then(|v| v.as_bool()).unwrap_or(false);
                        if served {
                            let gv = GroupVersionForDiscovery {
                                group_version: format!("{}/{}", group, ver_name),
                                version: ver_name.to_string(),
                            };
                            if preferred.is_none() {
                                preferred = Some(gv.clone());
                            }
                            gv_versions.push(gv);
                        }
                    }
                    if !gv_versions.is_empty() {
                        groups.push(APIGroup {
                            name: group.to_string(),
                            versions: gv_versions,
                            preferred_version: preferred.unwrap(),
                        });
                    }
                }
            }
        }
    }

    let api_group_list = APIGroupList {
        kind: "APIGroupList".to_string(),
        api_version: "v1".to_string(),
        groups,
    };

    (StatusCode::OK, Json(api_group_list)).into_response()
}

/// Helper to get all API group names and their preferred versions
fn get_api_group_names() -> Vec<(&'static str, &'static str)> {
    vec![
        ("apps", "v1"),
        ("batch", "v1"),
        ("networking.k8s.io", "v1"),
        ("rbac.authorization.k8s.io", "v1"),
        ("storage.k8s.io", "v1"),
        ("snapshot.storage.k8s.io", "v1"),
        ("scheduling.k8s.io", "v1"),
        ("apiextensions.k8s.io", "v1"),
        ("admissionregistration.k8s.io", "v1"),
        ("coordination.k8s.io", "v1"),
        ("flowcontrol.apiserver.k8s.io", "v1"),
        ("certificates.k8s.io", "v1"),
        ("discovery.k8s.io", "v1"),
        ("node.k8s.io", "v1"),
        ("authentication.k8s.io", "v1"),
        ("authorization.k8s.io", "v1"),
        ("autoscaling", "v2"),
        ("policy", "v1"),
        ("metrics.k8s.io", "v1beta1"),
        ("custom.metrics.k8s.io", "v1beta2"),
        ("resource.k8s.io", "v1"),
        ("events.k8s.io", "v1"),
        ("apiregistration.k8s.io", "v1"),
    ]
}

/// Build aggregated discovery resource entries for a given API group.
/// Returns a list of resource objects in the apidiscovery.k8s.io/v2 format.
/// In v2, subresources are nested inside their parent resource's "subresources" array,
/// NOT listed as separate top-level entries with slashes in the name.
fn get_aggregated_resources_for_group(group: &str, version: &str) -> Vec<serde_json::Value> {
    // Helper to build a subresource entry (nested under parent)
    let sub = |name: &str, kind: &str, verbs: &[&str]| -> serde_json::Value {
        serde_json::json!({
            "subresource": name,
            "responseKind": {
                "group": group,
                "version": version,
                "kind": kind
            },
            "verbs": verbs
        })
    };

    // Helper to build a top-level resource entry with optional subresources and short names
    let res_with_short = |name: &str,
                          singular: &str,
                          kind: &str,
                          namespaced: bool,
                          verbs: &[&str],
                          subresources: Vec<serde_json::Value>,
                          short_names: Option<Vec<&str>>| {
        let scope = if namespaced { "Namespaced" } else { "Cluster" };
        let mut entry = serde_json::json!({
            "resource": name,
            "responseKind": {
                "group": group,
                "version": version,
                "kind": kind
            },
            "scope": scope,
            "singularResource": singular,
            "verbs": verbs
        });
        if !subresources.is_empty() {
            entry
                .as_object_mut()
                .unwrap()
                .insert("subresources".to_string(), serde_json::json!(subresources));
        }
        if let Some(sn) = short_names {
            entry
                .as_object_mut()
                .unwrap()
                .insert("shortNames".to_string(), serde_json::json!(sn));
        }
        entry
    };

    // Wrapper that defaults to no short names (most resources)
    let res = |name: &str,
               singular: &str,
               kind: &str,
               namespaced: bool,
               verbs: &[&str],
               subresources: Vec<serde_json::Value>| {
        res_with_short(name, singular, kind, namespaced, verbs, subresources, None)
    };

    let all_verbs: &[&str] = &[
        "create",
        "delete",
        "deletecollection",
        "get",
        "list",
        "patch",
        "update",
        "watch",
    ];
    let status_verbs: &[&str] = &["get", "patch", "update"];

    match group {
        "" => vec![
            res_with_short(
                "namespaces",
                "namespace",
                "Namespace",
                false,
                all_verbs,
                vec![
                    sub("status", "Namespace", status_verbs),
                    sub("finalize", "Namespace", &["update"]),
                ],
                Some(vec!["ns"]),
            ),
            res_with_short(
                "pods",
                "pod",
                "Pod",
                true,
                all_verbs,
                vec![
                    sub("status", "Pod", status_verbs),
                    sub("log", "Pod", &["get"]),
                    sub("exec", "PodExecOptions", &["get", "create"]),
                    sub("attach", "PodAttachOptions", &["get", "create"]),
                    sub("portforward", "PodPortForwardOptions", &["get", "create"]),
                    sub("binding", "Binding", &["create"]),
                    sub("eviction", "Eviction", &["create"]),
                    sub("ephemeralcontainers", "Pod", &["get", "patch", "update"]),
                    sub("proxy", "Pod", &["get", "put", "post", "delete", "patch"]),
                ],
                Some(vec!["po"]),
            ),
            res_with_short(
                "services",
                "service",
                "Service",
                true,
                all_verbs,
                vec![
                    sub("status", "Service", status_verbs),
                    sub(
                        "proxy",
                        "Service",
                        &["get", "put", "post", "delete", "patch"],
                    ),
                ],
                Some(vec!["svc"]),
            ),
            res_with_short(
                "endpoints",
                "endpoints",
                "Endpoints",
                true,
                all_verbs,
                vec![],
                Some(vec!["ep"]),
            ),
            res_with_short(
                "nodes",
                "node",
                "Node",
                false,
                all_verbs,
                vec![
                    sub("status", "Node", status_verbs),
                    sub("proxy", "Node", &["get", "put", "post", "delete", "patch"]),
                ],
                Some(vec!["no"]),
            ),
            res_with_short(
                "configmaps",
                "configmap",
                "ConfigMap",
                true,
                all_verbs,
                vec![],
                Some(vec!["cm"]),
            ),
            res("secrets", "secret", "Secret", true, all_verbs, vec![]),
            res_with_short(
                "serviceaccounts",
                "serviceaccount",
                "ServiceAccount",
                true,
                all_verbs,
                vec![sub("token", "TokenRequest", &["create"])],
                Some(vec!["sa"]),
            ),
            res_with_short(
                "persistentvolumes",
                "persistentvolume",
                "PersistentVolume",
                false,
                all_verbs,
                vec![sub("status", "PersistentVolume", status_verbs)],
                Some(vec!["pv"]),
            ),
            res_with_short(
                "persistentvolumeclaims",
                "persistentvolumeclaim",
                "PersistentVolumeClaim",
                true,
                all_verbs,
                vec![sub("status", "PersistentVolumeClaim", status_verbs)],
                Some(vec!["pvc"]),
            ),
            res_with_short(
                "events",
                "event",
                "Event",
                true,
                all_verbs,
                vec![],
                Some(vec!["ev"]),
            ),
            res_with_short(
                "resourcequotas",
                "resourcequota",
                "ResourceQuota",
                true,
                all_verbs,
                vec![sub("status", "ResourceQuota", status_verbs)],
                Some(vec!["quota"]),
            ),
            res_with_short(
                "limitranges",
                "limitrange",
                "LimitRange",
                true,
                all_verbs,
                vec![],
                Some(vec!["limits"]),
            ),
            res_with_short(
                "replicationcontrollers",
                "replicationcontroller",
                "ReplicationController",
                true,
                all_verbs,
                vec![
                    sub("status", "ReplicationController", status_verbs),
                    sub("scale", "Scale", status_verbs),
                ],
                Some(vec!["rc"]),
            ),
            res(
                "componentstatuses",
                "componentstatus",
                "ComponentStatus",
                false,
                &["get", "list"],
                vec![],
            ),
            res(
                "podtemplates",
                "podtemplate",
                "PodTemplate",
                true,
                all_verbs,
                vec![],
            ),
        ],
        "admissionregistration.k8s.io" => vec![
            res(
                "validatingwebhookconfigurations",
                "validatingwebhookconfiguration",
                "ValidatingWebhookConfiguration",
                false,
                all_verbs,
                vec![],
            ),
            res(
                "mutatingwebhookconfigurations",
                "mutatingwebhookconfiguration",
                "MutatingWebhookConfiguration",
                false,
                all_verbs,
                vec![],
            ),
            res(
                "validatingadmissionpolicies",
                "validatingadmissionpolicy",
                "ValidatingAdmissionPolicy",
                false,
                all_verbs,
                vec![sub("status", "ValidatingAdmissionPolicy", status_verbs)],
            ),
            res(
                "validatingadmissionpolicybindings",
                "validatingadmissionpolicybinding",
                "ValidatingAdmissionPolicyBinding",
                false,
                all_verbs,
                vec![],
            ),
        ],
        "apps" => vec![
            res(
                "deployments",
                "deployment",
                "Deployment",
                true,
                all_verbs,
                vec![
                    sub("status", "Deployment", status_verbs),
                    sub("scale", "Scale", status_verbs),
                ],
            ),
            res(
                "replicasets",
                "replicaset",
                "ReplicaSet",
                true,
                all_verbs,
                vec![
                    sub("status", "ReplicaSet", status_verbs),
                    sub("scale", "Scale", status_verbs),
                ],
            ),
            res(
                "daemonsets",
                "daemonset",
                "DaemonSet",
                true,
                all_verbs,
                vec![sub("status", "DaemonSet", status_verbs)],
            ),
            res(
                "statefulsets",
                "statefulset",
                "StatefulSet",
                true,
                all_verbs,
                vec![
                    sub("status", "StatefulSet", status_verbs),
                    sub("scale", "Scale", status_verbs),
                ],
            ),
            res(
                "controllerrevisions",
                "controllerrevision",
                "ControllerRevision",
                true,
                all_verbs,
                vec![],
            ),
        ],
        "batch" => vec![
            res(
                "jobs",
                "job",
                "Job",
                true,
                all_verbs,
                vec![sub("status", "Job", status_verbs)],
            ),
            res(
                "cronjobs",
                "cronjob",
                "CronJob",
                true,
                all_verbs,
                vec![sub("status", "CronJob", status_verbs)],
            ),
        ],
        "networking.k8s.io" => vec![
            res(
                "networkpolicies",
                "networkpolicy",
                "NetworkPolicy",
                true,
                all_verbs,
                vec![],
            ),
            res(
                "ingresses",
                "ingress",
                "Ingress",
                true,
                all_verbs,
                vec![sub("status", "Ingress", status_verbs)],
            ),
            res(
                "ingressclasses",
                "ingressclass",
                "IngressClass",
                false,
                all_verbs,
                vec![],
            ),
        ],
        "rbac.authorization.k8s.io" => vec![
            res("roles", "role", "Role", true, all_verbs, vec![]),
            res(
                "rolebindings",
                "rolebinding",
                "RoleBinding",
                true,
                all_verbs,
                vec![],
            ),
            res(
                "clusterroles",
                "clusterrole",
                "ClusterRole",
                false,
                all_verbs,
                vec![],
            ),
            res(
                "clusterrolebindings",
                "clusterrolebinding",
                "ClusterRoleBinding",
                false,
                all_verbs,
                vec![],
            ),
        ],
        "storage.k8s.io" => vec![
            res(
                "storageclasses",
                "storageclass",
                "StorageClass",
                false,
                all_verbs,
                vec![],
            ),
            res(
                "volumeattachments",
                "volumeattachment",
                "VolumeAttachment",
                false,
                all_verbs,
                vec![sub("status", "VolumeAttachment", status_verbs)],
            ),
            res("csinodes", "csinode", "CSINode", false, all_verbs, vec![]),
            res(
                "csidrivers",
                "csidriver",
                "CSIDriver",
                false,
                all_verbs,
                vec![],
            ),
            res(
                "csistoragecapacities",
                "csistoragecapacity",
                "CSIStorageCapacity",
                true,
                all_verbs,
                vec![],
            ),
        ],
        "scheduling.k8s.io" => vec![res(
            "priorityclasses",
            "priorityclass",
            "PriorityClass",
            false,
            all_verbs,
            vec![],
        )],
        "apiextensions.k8s.io" => vec![res(
            "customresourcedefinitions",
            "customresourcedefinition",
            "CustomResourceDefinition",
            false,
            all_verbs,
            vec![sub("status", "CustomResourceDefinition", status_verbs)],
        )],
        "coordination.k8s.io" => vec![res("leases", "lease", "Lease", true, all_verbs, vec![])],
        "certificates.k8s.io" => vec![res(
            "certificatesigningrequests",
            "certificatesigningrequest",
            "CertificateSigningRequest",
            false,
            all_verbs,
            vec![
                sub("status", "CertificateSigningRequest", status_verbs),
                sub(
                    "approval",
                    "CertificateSigningRequest",
                    &["get", "patch", "update"],
                ),
            ],
        )],
        "discovery.k8s.io" => vec![res(
            "endpointslices",
            "endpointslice",
            "EndpointSlice",
            true,
            all_verbs,
            vec![],
        )],
        "node.k8s.io" => vec![res(
            "runtimeclasses",
            "runtimeclass",
            "RuntimeClass",
            false,
            all_verbs,
            vec![],
        )],
        "authentication.k8s.io" => vec![res(
            "tokenreviews",
            "tokenreview",
            "TokenReview",
            false,
            &["create"],
            vec![],
        )],
        "authorization.k8s.io" => vec![
            res(
                "subjectaccessreviews",
                "subjectaccessreview",
                "SubjectAccessReview",
                false,
                &["create"],
                vec![],
            ),
            res(
                "localsubjectaccessreviews",
                "localsubjectaccessreview",
                "LocalSubjectAccessReview",
                true,
                &["create"],
                vec![],
            ),
            res(
                "selfsubjectaccessreviews",
                "selfsubjectaccessreview",
                "SelfSubjectAccessReview",
                false,
                &["create"],
                vec![],
            ),
            res(
                "selfsubjectrulesreviews",
                "selfsubjectrulesreview",
                "SelfSubjectRulesReview",
                false,
                &["create"],
                vec![],
            ),
        ],
        "autoscaling" => vec![res(
            "horizontalpodautoscalers",
            "horizontalpodautoscaler",
            "HorizontalPodAutoscaler",
            true,
            all_verbs,
            vec![sub("status", "HorizontalPodAutoscaler", status_verbs)],
        )],
        "policy" => vec![res(
            "poddisruptionbudgets",
            "poddisruptionbudget",
            "PodDisruptionBudget",
            true,
            all_verbs,
            vec![sub("status", "PodDisruptionBudget", status_verbs)],
        )],
        "flowcontrol.apiserver.k8s.io" => vec![
            res(
                "flowschemas",
                "flowschema",
                "FlowSchema",
                false,
                all_verbs,
                vec![sub("status", "FlowSchema", status_verbs)],
            ),
            res(
                "prioritylevelconfigurations",
                "prioritylevelconfiguration",
                "PriorityLevelConfiguration",
                false,
                all_verbs,
                vec![sub("status", "PriorityLevelConfiguration", status_verbs)],
            ),
        ],
        "events.k8s.io" => vec![res("events", "event", "Event", true, all_verbs, vec![])],
        "snapshot.storage.k8s.io" => vec![
            res(
                "volumesnapshots",
                "volumesnapshot",
                "VolumeSnapshot",
                true,
                all_verbs,
                vec![],
            ),
            res(
                "volumesnapshotclasses",
                "volumesnapshotclass",
                "VolumeSnapshotClass",
                false,
                all_verbs,
                vec![],
            ),
            res(
                "volumesnapshotcontents",
                "volumesnapshotcontent",
                "VolumeSnapshotContent",
                false,
                all_verbs,
                vec![],
            ),
        ],
        "metrics.k8s.io" => vec![
            res(
                "nodes",
                "node",
                "NodeMetrics",
                false,
                &["get", "list"],
                vec![],
            ),
            res("pods", "pod", "PodMetrics", true, &["get", "list"], vec![]),
        ],
        "custom.metrics.k8s.io" => vec![],
        "resource.k8s.io" => vec![
            res(
                "resourceclaims",
                "resourceclaim",
                "ResourceClaim",
                true,
                all_verbs,
                vec![sub("status", "ResourceClaim", status_verbs)],
            ),
            res(
                "resourceclaimtemplates",
                "resourceclaimtemplate",
                "ResourceClaimTemplate",
                true,
                all_verbs,
                vec![],
            ),
            res(
                "resourceslices",
                "resourceslice",
                "ResourceSlice",
                false,
                all_verbs,
                vec![],
            ),
            res(
                "deviceclasses",
                "deviceclass",
                "DeviceClass",
                false,
                all_verbs,
                vec![],
            ),
        ],
        "apiregistration.k8s.io" => vec![res(
            "apiservices",
            "apiservice",
            "APIService",
            false,
            all_verbs,
            vec![sub("status", "APIService", status_verbs)],
        )],
        _ => vec![],
    }
}

/// GET /apis/{group}/
/// Returns the APIGroup resource for a specific API group
pub async fn get_api_group(
    axum::extract::Path(group): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    // Find the group in our known groups
    let groups = get_api_group_names();
    let found = groups.iter().find(|(name, _)| *name == group.as_str());

    if let Some((name, version)) = found {
        // autoscaling has both v1 and v2
        let versions = if *name == "autoscaling" {
            vec![
                GroupVersionForDiscovery {
                    group_version: format!("{}/v2", name),
                    version: "v2".to_string(),
                },
                GroupVersionForDiscovery {
                    group_version: format!("{}/v1", name),
                    version: "v1".to_string(),
                },
            ]
        } else {
            vec![GroupVersionForDiscovery {
                group_version: format!("{}/{}", name, version),
                version: version.to_string(),
            }]
        };
        let preferred = versions[0].clone();
        let api_group = APIGroup {
            name: name.to_string(),
            versions,
            preferred_version: preferred,
        };
        let response = serde_json::json!({
            "kind": "APIGroup",
            "apiVersion": "v1",
            "name": api_group.name,
            "versions": api_group.versions.iter().map(|v| serde_json::json!({
                "groupVersion": v.group_version,
                "version": v.version,
            })).collect::<Vec<_>>(),
            "preferredVersion": {
                "groupVersion": api_group.preferred_version.group_version,
                "version": api_group.preferred_version.version,
            }
        });
        (StatusCode::OK, axum::Json(response)).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "message": format!("the server could not find the requested resource"),
                "reason": "NotFound",
                "code": 404
            })),
        )
            .into_response()
    }
}

/// GET /version
/// Returns the version information for the server
pub async fn get_version() -> (StatusCode, Json<VersionInfo>) {
    let version_info = VersionInfo {
        major: "1".to_string(),
        minor: "35".to_string(),
        git_version: "v1.35.0".to_string(),
        git_commit: "rusternetes-v0.1.0".to_string(),
        git_tree_state: "clean".to_string(),
        build_date: "2026-03-10T00:00:00Z".to_string(),
        go_version: "rust1.83".to_string(),
        compiler: "rustc".to_string(),
        platform: "linux/amd64".to_string(),
    };

    (StatusCode::OK, Json(version_info))
}

/// APIResourceList is the list of APIResources available at a particular group/version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct APIResourceList {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    #[serde(rename = "groupVersion")]
    pub group_version: String,
    pub resources: Vec<APIResource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct APIResource {
    pub name: String,
    #[serde(rename = "singularName")]
    pub singular_name: String,
    pub namespaced: bool,
    pub kind: String,
    pub verbs: Vec<String>,
    #[serde(rename = "shortNames", skip_serializing_if = "Option::is_none")]
    pub short_names: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub categories: Option<Vec<String>>,
    #[serde(rename = "storageVersionHash", skip_serializing_if = "Option::is_none")]
    pub storage_version_hash: Option<String>,
}

/// GET /api/v1
/// Returns the list of resources available in the core v1 API
pub async fn get_core_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "namespaces".to_string(),
            singular_name: "namespace".to_string(),
            namespaced: false,
            kind: "Namespace".to_string(),
            verbs: [
                "create", "delete", "get", "list", "patch", "update", "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["ns".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "namespaces/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "Namespace".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "pods".to_string(),
            singular_name: "pod".to_string(),
            namespaced: true,
            kind: "Pod".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["po".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "pods/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Pod".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "pods/log".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Pod".to_string(),
            verbs: ["get"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "pods/exec".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "PodExecOptions".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "pods/attach".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "PodAttachOptions".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "pods/portforward".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "PodPortForwardOptions".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "pods/binding".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Binding".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "pods/eviction".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Eviction".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "services".to_string(),
            singular_name: "service".to_string(),
            namespaced: true,
            kind: "Service".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["svc".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "services/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Service".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "nodes".to_string(),
            singular_name: "node".to_string(),
            namespaced: false,
            kind: "Node".to_string(),
            verbs: [
                "create", "delete", "get", "list", "patch", "update", "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["no".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "nodes/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "Node".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "configmaps".to_string(),
            singular_name: "configmap".to_string(),
            namespaced: true,
            kind: "ConfigMap".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["cm".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "secrets".to_string(),
            singular_name: "secret".to_string(),
            namespaced: true,
            kind: "Secret".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "serviceaccounts".to_string(),
            singular_name: "serviceaccount".to_string(),
            namespaced: true,
            kind: "ServiceAccount".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["sa".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "persistentvolumes".to_string(),
            singular_name: "persistentvolume".to_string(),
            namespaced: false,
            kind: "PersistentVolume".to_string(),
            verbs: [
                "create", "delete", "get", "list", "patch", "update", "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["pv".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "persistentvolumeclaims".to_string(),
            singular_name: "persistentvolumeclaim".to_string(),
            namespaced: true,
            kind: "PersistentVolumeClaim".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["pvc".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "endpoints".to_string(),
            singular_name: "endpoints".to_string(),
            namespaced: true,
            kind: "Endpoints".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["ep".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "events".to_string(),
            singular_name: "event".to_string(),
            namespaced: true,
            kind: "Event".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["ev".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "resourcequotas".to_string(),
            singular_name: "resourcequota".to_string(),
            namespaced: true,
            kind: "ResourceQuota".to_string(),
            verbs: [
                "create", "delete", "get", "list", "patch", "update", "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["quota".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "limitranges".to_string(),
            singular_name: "limitrange".to_string(),
            namespaced: true,
            kind: "LimitRange".to_string(),
            verbs: [
                "create", "delete", "get", "list", "patch", "update", "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["limits".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "replicationcontrollers".to_string(),
            singular_name: "replicationcontroller".to_string(),
            namespaced: true,
            kind: "ReplicationController".to_string(),
            verbs: [
                "create", "delete", "get", "list", "patch", "update", "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["rc".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "replicationcontrollers/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "ReplicationController".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "replicationcontrollers/scale".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Scale".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "podtemplates".to_string(),
            singular_name: "podtemplate".to_string(),
            namespaced: true,
            kind: "PodTemplate".to_string(),
            verbs: [
                "create", "delete", "get", "list", "patch", "update", "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "componentstatuses".to_string(),
            singular_name: "componentstatus".to_string(),
            namespaced: false,
            kind: "ComponentStatus".to_string(),
            verbs: ["get", "list"].iter().map(|s| s.to_string()).collect(),
            short_names: Some(vec!["cs".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/apps/v1
/// Returns the list of resources available in the apps/v1 API
pub async fn get_apps_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "deployments".to_string(),
            singular_name: "deployment".to_string(),
            namespaced: true,
            kind: "Deployment".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["deploy".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "deployments/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Deployment".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "deployments/scale".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Scale".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "replicasets".to_string(),
            singular_name: "replicaset".to_string(),
            namespaced: true,
            kind: "ReplicaSet".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["rs".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "replicasets/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "ReplicaSet".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "replicasets/scale".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Scale".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "statefulsets".to_string(),
            singular_name: "statefulset".to_string(),
            namespaced: true,
            kind: "StatefulSet".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["sts".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "statefulsets/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "StatefulSet".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "statefulsets/scale".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Scale".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "daemonsets".to_string(),
            singular_name: "daemonset".to_string(),
            namespaced: true,
            kind: "DaemonSet".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["ds".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "daemonsets/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "DaemonSet".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "controllerrevisions".to_string(),
            singular_name: "controllerrevision".to_string(),
            namespaced: true,
            kind: "ControllerRevision".to_string(),
            verbs: [
                "create", "delete", "get", "list", "patch", "update", "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "apps/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/batch/v1
/// Returns the list of resources available in the batch/v1 API
pub async fn get_batch_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "jobs".to_string(),
            singular_name: "job".to_string(),
            namespaced: true,
            kind: "Job".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "jobs/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Job".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "cronjobs".to_string(),
            singular_name: "cronjob".to_string(),
            namespaced: true,
            kind: "CronJob".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["cj".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "cronjobs/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "CronJob".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "batch/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/networking.k8s.io/v1
/// Returns the list of resources available in the networking.k8s.io/v1 API
pub async fn get_networking_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "ingresses".to_string(),
            singular_name: "ingress".to_string(),
            namespaced: true,
            kind: "Ingress".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["ing".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "ingresses/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "Ingress".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "ingressclasses".to_string(),
            singular_name: "ingressclass".to_string(),
            namespaced: false,
            kind: "IngressClass".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "networkpolicies".to_string(),
            singular_name: "networkpolicy".to_string(),
            namespaced: true,
            kind: "NetworkPolicy".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["netpol".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "servicecidrs".to_string(),
            singular_name: "servicecidr".to_string(),
            namespaced: false,
            kind: "ServiceCIDR".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "ipaddresses".to_string(),
            singular_name: "ipaddress".to_string(),
            namespaced: false,
            kind: "IPAddress".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "networking.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/rbac.authorization.k8s.io/v1
/// Returns the list of resources available in the rbac.authorization.k8s.io/v1 API
pub async fn get_rbac_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "roles".to_string(),
            singular_name: "role".to_string(),
            namespaced: true,
            kind: "Role".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "rolebindings".to_string(),
            singular_name: "rolebinding".to_string(),
            namespaced: true,
            kind: "RoleBinding".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "clusterroles".to_string(),
            singular_name: "clusterrole".to_string(),
            namespaced: false,
            kind: "ClusterRole".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "clusterrolebindings".to_string(),
            singular_name: "clusterrolebinding".to_string(),
            namespaced: false,
            kind: "ClusterRoleBinding".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "rbac.authorization.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/storage.k8s.io/v1
/// Returns the list of resources available in the storage.k8s.io/v1 API
pub async fn get_storage_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "storageclasses".to_string(),
            singular_name: "storageclass".to_string(),
            namespaced: false,
            kind: "StorageClass".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["sc".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "csidrivers".to_string(),
            singular_name: "csidriver".to_string(),
            namespaced: false,
            kind: "CSIDriver".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "csinodes".to_string(),
            singular_name: "csinode".to_string(),
            namespaced: false,
            kind: "CSINode".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "csistoragecapacities".to_string(),
            singular_name: "csistoragecapacity".to_string(),
            namespaced: true,
            kind: "CSIStorageCapacity".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "volumeattachments".to_string(),
            singular_name: "volumeattachment".to_string(),
            namespaced: false,
            kind: "VolumeAttachment".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "volumeattachments/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "VolumeAttachment".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "volumeattributesclasses".to_string(),
            singular_name: "volumeattributesclass".to_string(),
            namespaced: false,
            kind: "VolumeAttributesClass".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "storage.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/scheduling.k8s.io/v1
/// Returns the list of resources available in the scheduling.k8s.io/v1 API
pub async fn get_scheduling_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![APIResource {
        name: "priorityclasses".to_string(),
        singular_name: "priorityclass".to_string(),
        namespaced: false,
        kind: "PriorityClass".to_string(),
        verbs: [
            "create",
            "delete",
            "deletecollection",
            "get",
            "list",
            "patch",
            "update",
            "watch",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
        short_names: Some(vec!["pc".to_string()]),
        categories: None,
        storage_version_hash: None,
    }];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "scheduling.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/apiextensions.k8s.io/v1
/// Returns the list of resources available in the apiextensions.k8s.io/v1 API
pub async fn get_apiextensions_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "customresourcedefinitions".to_string(),
            singular_name: "customresourcedefinition".to_string(),
            namespaced: false,
            kind: "CustomResourceDefinition".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["crd".to_string(), "crds".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "customresourcedefinitions/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "CustomResourceDefinition".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "apiextensions.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/admissionregistration.k8s.io/v1
/// Returns the list of resources available in the admissionregistration.k8s.io/v1 API
pub async fn get_admissionregistration_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "validatingwebhookconfigurations".to_string(),
            singular_name: "validatingwebhookconfiguration".to_string(),
            namespaced: false,
            kind: "ValidatingWebhookConfiguration".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "mutatingwebhookconfigurations".to_string(),
            singular_name: "mutatingwebhookconfiguration".to_string(),
            namespaced: false,
            kind: "MutatingWebhookConfiguration".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "validatingadmissionpolicies".to_string(),
            singular_name: "validatingadmissionpolicy".to_string(),
            namespaced: false,
            kind: "ValidatingAdmissionPolicy".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "validatingadmissionpolicies/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "ValidatingAdmissionPolicy".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "validatingadmissionpolicybindings".to_string(),
            singular_name: "validatingadmissionpolicybinding".to_string(),
            namespaced: false,
            kind: "ValidatingAdmissionPolicyBinding".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "admissionregistration.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/coordination.k8s.io/v1
/// Returns the list of resources available in the coordination.k8s.io/v1 API
pub async fn get_coordination_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![APIResource {
        name: "leases".to_string(),
        singular_name: "lease".to_string(),
        namespaced: true,
        kind: "Lease".to_string(),
        verbs: [
            "create",
            "delete",
            "deletecollection",
            "get",
            "list",
            "patch",
            "update",
            "watch",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
        short_names: None,
        categories: None,
        storage_version_hash: None,
    }];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "coordination.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/flowcontrol.apiserver.k8s.io/v1
/// Returns the list of resources available in the flowcontrol.apiserver.k8s.io/v1 API
pub async fn get_flowcontrol_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "flowschemas".to_string(),
            singular_name: "flowschema".to_string(),
            namespaced: false,
            kind: "FlowSchema".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "flowschemas/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "FlowSchema".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "prioritylevelconfigurations".to_string(),
            singular_name: "prioritylevelconfiguration".to_string(),
            namespaced: false,
            kind: "PriorityLevelConfiguration".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "prioritylevelconfigurations/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "PriorityLevelConfiguration".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "flowcontrol.apiserver.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/certificates.k8s.io/v1
/// Returns the list of resources available in the certificates.k8s.io/v1 API
pub async fn get_certificates_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "certificatesigningrequests".to_string(),
            singular_name: "certificatesigningrequest".to_string(),
            namespaced: false,
            kind: "CertificateSigningRequest".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["csr".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "certificatesigningrequests/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "CertificateSigningRequest".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "certificatesigningrequests/approval".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "CertificateSigningRequest".to_string(),
            verbs: ["patch", "update"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "certificates.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/snapshot.storage.k8s.io/v1
/// Returns the list of resources available in the snapshot.storage.k8s.io/v1 API
pub async fn get_snapshot_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "volumesnapshotclasses".to_string(),
            singular_name: "volumesnapshotclass".to_string(),
            namespaced: false,
            kind: "VolumeSnapshotClass".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "volumesnapshots".to_string(),
            singular_name: "volumesnapshot".to_string(),
            namespaced: true,
            kind: "VolumeSnapshot".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "volumesnapshots/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "VolumeSnapshot".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "volumesnapshotcontents".to_string(),
            singular_name: "volumesnapshotcontent".to_string(),
            namespaced: false,
            kind: "VolumeSnapshotContent".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "volumesnapshotcontents/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "VolumeSnapshotContent".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "snapshot.storage.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/discovery.k8s.io/v1
/// Returns the list of resources available in the discovery.k8s.io/v1 API
pub async fn get_discovery_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![APIResource {
        name: "endpointslices".to_string(),
        singular_name: "endpointslice".to_string(),
        namespaced: true,
        kind: "EndpointSlice".to_string(),
        verbs: [
            "create", "delete", "get", "list", "patch", "update", "watch",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
        short_names: None,
        categories: None,
        storage_version_hash: None,
    }];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "discovery.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/node.k8s.io/v1
/// Returns the list of resources available in the node.k8s.io/v1 API
pub async fn get_node_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![APIResource {
        name: "runtimeclasses".to_string(),
        singular_name: "runtimeclass".to_string(),
        namespaced: false,
        kind: "RuntimeClass".to_string(),
        verbs: [
            "create",
            "delete",
            "deletecollection",
            "get",
            "list",
            "patch",
            "update",
            "watch",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
        short_names: None,
        categories: None,
        storage_version_hash: None,
    }];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "node.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/authentication.k8s.io/v1
/// Returns the list of resources available in the authentication.k8s.io/v1 API
pub async fn get_authentication_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "tokenreviews".to_string(),
            singular_name: "tokenreview".to_string(),
            namespaced: false,
            kind: "TokenReview".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "selfsubjectreviews".to_string(),
            singular_name: "selfsubjectreview".to_string(),
            namespaced: false,
            kind: "SelfSubjectReview".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "authentication.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/authorization.k8s.io/v1
/// Returns the list of resources available in the authorization.k8s.io/v1 API
pub async fn get_authorization_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "subjectaccessreviews".to_string(),
            singular_name: "subjectaccessreview".to_string(),
            namespaced: false,
            kind: "SubjectAccessReview".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "selfsubjectaccessreviews".to_string(),
            singular_name: "selfsubjectaccessreview".to_string(),
            namespaced: false,
            kind: "SelfSubjectAccessReview".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "localsubjectaccessreviews".to_string(),
            singular_name: "localsubjectaccessreview".to_string(),
            namespaced: true,
            kind: "LocalSubjectAccessReview".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "selfsubjectrulesreviews".to_string(),
            singular_name: "selfsubjectrulesreview".to_string(),
            namespaced: false,
            kind: "SelfSubjectRulesReview".to_string(),
            verbs: ["create"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "authorization.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/metrics.k8s.io/v1beta1
/// Returns the list of resources available in the metrics.k8s.io/v1beta1 API
pub async fn get_metrics_v1beta1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "nodes".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "NodeMetrics".to_string(),
            verbs: ["get", "list"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "pods".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "PodMetrics".to_string(),
            verbs: ["get", "list"].iter().map(|s| s.to_string()).collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "metrics.k8s.io/v1beta1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/custom.metrics.k8s.io/v1beta2
/// Returns the list of resources available in the custom.metrics.k8s.io/v1beta2 API
pub async fn get_custom_metrics_v1beta2_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![APIResource {
        name: "*".to_string(),
        singular_name: "".to_string(),
        namespaced: true,
        kind: "MetricValueList".to_string(),
        verbs: ["get"].iter().map(|s| s.to_string()).collect(),
        short_names: None,
        categories: None,
        storage_version_hash: None,
    }];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1beta2".to_string(),
        group_version: "custom.metrics.k8s.io/v1beta2".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/resource.k8s.io/v1
/// Returns the list of resources available in the resource.k8s.io/v1 API
pub async fn get_resource_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "resourceclaims".to_string(),
            singular_name: "resourceclaim".to_string(),
            namespaced: true,
            kind: "ResourceClaim".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "resourceclaims/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "ResourceClaim".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "resourceclaimtemplates".to_string(),
            singular_name: "resourceclaimtemplate".to_string(),
            namespaced: true,
            kind: "ResourceClaimTemplate".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "deviceclasses".to_string(),
            singular_name: "deviceclass".to_string(),
            namespaced: false,
            kind: "DeviceClass".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "resourceslices".to_string(),
            singular_name: "resourceslice".to_string(),
            namespaced: false,
            kind: "ResourceSlice".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "resource.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/autoscaling/v1
/// Returns the list of resources available in the autoscaling/v1 API
pub async fn get_autoscaling_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "horizontalpodautoscalers".to_string(),
            singular_name: "horizontalpodautoscaler".to_string(),
            namespaced: true,
            kind: "HorizontalPodAutoscaler".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["hpa".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "horizontalpodautoscalers/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "HorizontalPodAutoscaler".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "autoscaling/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/autoscaling/v2
/// Returns the list of resources available in the autoscaling/v2 API
pub async fn get_autoscaling_v2_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "horizontalpodautoscalers".to_string(),
            singular_name: "horizontalpodautoscaler".to_string(),
            namespaced: true,
            kind: "HorizontalPodAutoscaler".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["hpa".to_string()]),
            categories: Some(vec!["all".to_string()]),
            storage_version_hash: None,
        },
        APIResource {
            name: "horizontalpodautoscalers/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "HorizontalPodAutoscaler".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "autoscaling/v2".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/policy/v1
/// Returns the list of resources available in the policy/v1 API
pub async fn get_policy_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "poddisruptionbudgets".to_string(),
            singular_name: "poddisruptionbudget".to_string(),
            namespaced: true,
            kind: "PodDisruptionBudget".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: Some(vec!["pdb".to_string()]),
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "poddisruptionbudgets/status".to_string(),
            singular_name: "".to_string(),
            namespaced: true,
            kind: "PodDisruptionBudget".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "policy/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/events.k8s.io/v1
/// Returns the list of resources available in the events.k8s.io/v1 API group
pub async fn get_events_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![APIResource {
        name: "events".to_string(),
        singular_name: "event".to_string(),
        namespaced: true,
        kind: "Event".to_string(),
        verbs: [
            "create",
            "delete",
            "deletecollection",
            "get",
            "list",
            "patch",
            "update",
            "watch",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
        short_names: Some(vec!["ev".to_string()]),
        categories: None,
        storage_version_hash: None,
    }];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "events.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

/// GET /apis/apiregistration.k8s.io/v1
/// Returns the list of resources available in the apiregistration.k8s.io/v1 API group
pub async fn get_apiregistration_v1_resources() -> (StatusCode, Json<APIResourceList>) {
    let resources = vec![
        APIResource {
            name: "apiservices".to_string(),
            singular_name: "apiservice".to_string(),
            namespaced: false,
            kind: "APIService".to_string(),
            verbs: [
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
        APIResource {
            name: "apiservices/status".to_string(),
            singular_name: "".to_string(),
            namespaced: false,
            kind: "APIService".to_string(),
            verbs: ["get", "patch", "update"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            short_names: None,
            categories: None,
            storage_version_hash: None,
        },
    ];

    let resource_list = APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "apiregistration.k8s.io/v1".to_string(),
        resources,
    };

    (StatusCode::OK, Json(resource_list))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_wants_aggregated_discovery_with_explicit_accept() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
            ),
        );
        assert!(wants_aggregated_discovery(&headers));
    }

    #[test]
    fn test_wants_aggregated_discovery_k8s_client_default() {
        // K8s discovery client sends aggregated types first, then plain JSON — all q=1.0
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList,application/json",
            ),
        );
        assert!(wants_aggregated_discovery(&headers));
    }

    #[test]
    fn test_wants_aggregated_discovery_both_equal_q() {
        // When both aggregated and plain have equal q-values, return aggregated
        // (matches real K8s API server behavior)
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/json, application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
            ),
        );
        assert!(wants_aggregated_discovery(&headers));
    }

    #[test]
    fn test_wants_aggregated_discovery_with_higher_q_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;q=0.9, application/json;q=0.7",
            ),
        );
        assert!(wants_aggregated_discovery(&headers));
    }

    #[test]
    fn test_wants_aggregated_discovery_plain_json_preferred() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/json, application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;q=0.5",
            ),
        );
        assert!(!wants_aggregated_discovery(&headers));
    }

    #[test]
    fn test_wants_aggregated_discovery_no_apidiscovery() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("application/json"));
        assert!(!wants_aggregated_discovery(&headers));
    }

    #[test]
    fn test_aggregated_core_api_response_format() {
        let resources = get_aggregated_resources_for_group("", "v1");

        // Verify pods resource exists and has nested subresources
        let pods = resources
            .iter()
            .find(|r| r.get("resource").and_then(|v| v.as_str()) == Some("pods"))
            .expect("pods resource should exist");

        // pods should NOT have a slash in resource name
        assert_eq!(pods["resource"], "pods");
        assert_eq!(pods["scope"], "Namespaced");
        assert_eq!(pods["singularResource"], "pod");
        assert_eq!(pods["responseKind"]["kind"], "Pod");
        assert_eq!(pods["responseKind"]["group"], "");
        assert_eq!(pods["responseKind"]["version"], "v1");

        // pods should have subresources array
        let subresources = pods["subresources"]
            .as_array()
            .expect("pods should have subresources array");

        // Verify status subresource
        let status = subresources
            .iter()
            .find(|s| s.get("subresource").and_then(|v| v.as_str()) == Some("status"))
            .expect("pods should have status subresource");
        assert_eq!(status["responseKind"]["kind"], "Pod");
        assert!(status["verbs"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("get")));

        // Verify log subresource
        let log = subresources
            .iter()
            .find(|s| s.get("subresource").and_then(|v| v.as_str()) == Some("log"))
            .expect("pods should have log subresource");
        assert_eq!(log["verbs"], serde_json::json!(["get"]));

        // Verify exec subresource
        let exec = subresources
            .iter()
            .find(|s| s.get("subresource").and_then(|v| v.as_str()) == Some("exec"))
            .expect("pods should have exec subresource");
        assert_eq!(exec["responseKind"]["kind"], "PodExecOptions");

        // Verify eviction subresource
        let eviction = subresources
            .iter()
            .find(|s| s.get("subresource").and_then(|v| v.as_str()) == Some("eviction"))
            .expect("pods should have eviction subresource");
        assert_eq!(eviction["responseKind"]["kind"], "Eviction");

        // Verify NO flat entries with slashes exist
        for resource in &resources {
            let name = resource["resource"].as_str().unwrap_or("");
            assert!(
                !name.contains('/'),
                "Resource name '{}' should not contain slash in v2 format - subresources must be nested",
                name
            );
        }
    }

    #[test]
    fn test_aggregated_apps_group_response_format() {
        let resources = get_aggregated_resources_for_group("apps", "v1");

        // Verify deployments resource has nested subresources
        let deployments = resources
            .iter()
            .find(|r| r.get("resource").and_then(|v| v.as_str()) == Some("deployments"))
            .expect("deployments resource should exist");

        let subresources = deployments["subresources"]
            .as_array()
            .expect("deployments should have subresources");

        // Should have status and scale
        let status = subresources
            .iter()
            .find(|s| s.get("subresource").and_then(|v| v.as_str()) == Some("status"))
            .expect("deployments should have status subresource");
        assert_eq!(status["responseKind"]["kind"], "Deployment");

        let scale = subresources
            .iter()
            .find(|s| s.get("subresource").and_then(|v| v.as_str()) == Some("scale"))
            .expect("deployments should have scale subresource");
        assert_eq!(scale["responseKind"]["kind"], "Scale");

        // No flat entries with slashes
        for resource in &resources {
            let name = resource["resource"].as_str().unwrap_or("");
            assert!(
                !name.contains('/'),
                "Resource name '{}' should not contain slash in v2 format",
                name
            );
        }
    }

    #[test]
    fn test_aggregated_serviceaccounts_has_token_subresource() {
        let resources = get_aggregated_resources_for_group("", "v1");
        let sa = resources
            .iter()
            .find(|r| r.get("resource").and_then(|v| v.as_str()) == Some("serviceaccounts"))
            .expect("serviceaccounts resource should exist");

        let subresources = sa["subresources"]
            .as_array()
            .expect("serviceaccounts should have subresources");

        let token = subresources
            .iter()
            .find(|s| s.get("subresource").and_then(|v| v.as_str()) == Some("token"))
            .expect("serviceaccounts should have token subresource");
        assert_eq!(token["responseKind"]["kind"], "TokenRequest");
        assert_eq!(token["verbs"], serde_json::json!(["create"]));
    }

    #[test]
    fn test_aggregated_namespaces_has_finalize_subresource() {
        let resources = get_aggregated_resources_for_group("", "v1");
        let ns = resources
            .iter()
            .find(|r| r.get("resource").and_then(|v| v.as_str()) == Some("namespaces"))
            .expect("namespaces resource should exist");

        let subresources = ns["subresources"]
            .as_array()
            .expect("namespaces should have subresources");

        subresources
            .iter()
            .find(|s| s.get("subresource").and_then(|v| v.as_str()) == Some("finalize"))
            .expect("namespaces should have finalize subresource");
    }

    #[test]
    fn test_all_api_groups_have_entries() {
        // Verify every group in get_api_group_names returns non-empty (or at least valid) resources
        for (group, version) in get_api_group_names() {
            let resources = get_aggregated_resources_for_group(group, version);
            if group != "custom.metrics.k8s.io" {
                assert!(
                    !resources.is_empty(),
                    "Group '{}' version '{}' should have resources",
                    group,
                    version
                );
            }
            // Verify no flat subresources (no slashes in resource names)
            for resource in &resources {
                let name = resource["resource"].as_str().unwrap_or("");
                assert!(
                    !name.contains('/'),
                    "Group '{}' resource '{}' should not contain slash in v2 format",
                    group,
                    name
                );
            }
        }
    }

    #[tokio::test]
    async fn test_get_core_api_aggregated_response() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
            ),
        );

        let response = get_core_api(headers).await;
        assert_eq!(response.status(), StatusCode::OK);

        // Verify Content-Type header
        let content_type = response
            .headers()
            .get("content-type")
            .expect("should have content-type header")
            .to_str()
            .unwrap();
        assert_eq!(
            content_type,
            "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
        );

        // Parse body
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let discovery: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(discovery["kind"], "APIGroupDiscoveryList");
        assert_eq!(discovery["apiVersion"], "apidiscovery.k8s.io/v2");
        assert!(discovery["items"].is_array());

        let items = discovery["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "core API should have exactly 1 group item");

        // Core group has empty name
        assert_eq!(items[0]["metadata"]["name"], "");
        let versions = items[0]["versions"].as_array().unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0]["version"], "v1");

        // Verify resources have proper v2 structure (no slashes)
        let resources = versions[0]["resources"].as_array().unwrap();
        for resource in resources {
            let name = resource["resource"].as_str().unwrap_or("");
            assert!(
                !name.contains('/'),
                "Core API resource '{}' should not have slash (v2 nests subresources)",
                name
            );
        }
    }

    #[tokio::test]
    async fn test_aggregated_discovery_v2_format_and_subresources() {
        // Verify the /api endpoint returns v2 format with correct apiVersion
        // and that subresources are nested inside parent resources (not flat).
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
            ),
        );

        let response = get_core_api(headers).await;
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let discovery: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Must have v2 apiVersion
        assert_eq!(
            discovery["apiVersion"], "apidiscovery.k8s.io/v2",
            "aggregated discovery must use apidiscovery.k8s.io/v2"
        );

        // Navigate to core v1 resources
        let resources = discovery["items"][0]["versions"][0]["resources"]
            .as_array()
            .expect("resources array should exist");

        // Find pods and verify it has nested subresources
        let pods = resources
            .iter()
            .find(|r| r.get("resource").and_then(|v| v.as_str()) == Some("pods"))
            .expect("pods resource must exist");

        let subresources = pods["subresources"]
            .as_array()
            .expect("pods must have nested subresources in v2 format");

        // Verify at least status and log subresources exist
        let sub_names: Vec<&str> = subresources
            .iter()
            .filter_map(|s| s.get("subresource").and_then(|v| v.as_str()))
            .collect();
        assert!(
            sub_names.contains(&"status"),
            "pods should have status subresource"
        );
        assert!(
            sub_names.contains(&"log"),
            "pods should have log subresource"
        );
    }
}
