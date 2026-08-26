use crate::client::ApiClient;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tabled::{settings::Style, Table, Tabled};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGroupList {
    groups: Vec<ApiGroup>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGroup {
    name: String,
    #[allow(dead_code)]
    versions: Vec<GroupVersionForDiscovery>,
    preferred_version: GroupVersionForDiscovery,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupVersionForDiscovery {
    group_version: String,
    #[allow(dead_code)]
    version: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiResourceList {
    group_version: String,
    resources: Vec<ApiResource>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ApiResource {
    name: String,
    #[serde(default)]
    #[allow(dead_code)]
    singular_name: String,
    namespaced: bool,
    kind: String,
    #[serde(default)]
    short_names: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    verbs: Vec<String>,
}

#[derive(Tabled, Serialize)]
struct ApiResourceRow {
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "SHORTNAMES")]
    short_names: String,
    #[tabled(rename = "APIVERSION")]
    api_version: String,
    #[tabled(rename = "NAMESPACED")]
    namespaced: String,
    #[tabled(rename = "KIND")]
    kind: String,
}

/// Display available API resources
pub async fn execute(
    client: &ApiClient,
    namespaced: Option<bool>,
    api_group: Option<&str>,
    no_headers: bool,
    output: Option<&str>,
) -> Result<()> {
    let mut resources = Vec::new();

    // Get core API resources (v1)
    let core_resources: ApiResourceList = client.get("/api/v1").await.map_err(|e| match e {
        crate::client::GetError::NotFound => anyhow::anyhow!("Core API not found"),
        crate::client::GetError::Other(e) => e,
    })?;

    for resource in core_resources.resources {
        // Skip subresources (contain /)
        if !resource.name.contains('/') {
            resources.push((resource, core_resources.group_version.clone()));
        }
    }

    // Get API groups
    let api_groups: ApiGroupList = client.get("/apis").await.map_err(|e| match e {
        crate::client::GetError::NotFound => anyhow::anyhow!("API groups not found"),
        crate::client::GetError::Other(e) => e,
    })?;

    for group in api_groups.groups {
        // Filter by API group if specified
        if let Some(filter_group) = api_group {
            if group.name != filter_group {
                continue;
            }
        }

        // Use preferred version
        let group_version = &group.preferred_version.group_version;
        let path = format!("/apis/{}", group_version);

        if let Ok(resource_list) = client.get::<ApiResourceList>(&path).await {
            for resource in resource_list.resources {
                // Skip subresources
                if !resource.name.contains('/') {
                    resources.push((resource, resource_list.group_version.clone()));
                }
            }
        }
    }

    // Filter by namespaced if specified
    if let Some(ns) = namespaced {
        resources.retain(|(r, _)| r.namespaced == ns);
    }

    // Sort by name
    resources.sort_by(|a, b| a.0.name.cmp(&b.0.name));

    // Build table rows
    let rows: Vec<ApiResourceRow> = resources
        .into_iter()
        .map(|(resource, api_version)| ApiResourceRow {
            name: resource.name,
            short_names: resource.short_names.join(","),
            api_version,
            namespaced: resource.namespaced.to_string(),
            kind: resource.kind,
        })
        .collect();

    if rows.is_empty() {
        println!("No resources found");
        return Ok(());
    }

    match output {
        Some("json") => {
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        Some("yaml") => {
            println!("{}", serde_yaml::to_string(&rows)?);
        }
        _ => {
            let mut table = Table::new(&rows);
            table.with(Style::blank());
            if no_headers {
                // Remove headers - tabled doesn't have a simple way to do this,
                // so we'll just print without table
                for row in rows {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        row.name, row.short_names, row.api_version, row.namespaced, row.kind
                    );
                }
            } else {
                println!("{}", table);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_resource_deserialization() {
        let json = r#"{
            "name": "pods",
            "singularName": "pod",
            "namespaced": true,
            "kind": "Pod",
            "shortNames": ["po"],
            "verbs": ["get", "list", "create", "delete"]
        }"#;
        let resource: ApiResource = serde_json::from_str(json).unwrap();
        assert_eq!(resource.name, "pods");
        assert_eq!(resource.kind, "Pod");
        assert!(resource.namespaced);
        assert_eq!(resource.short_names, vec!["po"]);
    }

    #[test]
    fn test_api_resource_row_construction() {
        let row = ApiResourceRow {
            name: "deployments".to_string(),
            short_names: "deploy".to_string(),
            api_version: "apps/v1".to_string(),
            namespaced: "true".to_string(),
            kind: "Deployment".to_string(),
        };
        assert_eq!(row.name, "deployments");
        assert_eq!(row.api_version, "apps/v1");
    }

    #[test]
    fn test_api_resource_list_deserialization() {
        let json = r#"{
            "groupVersion": "v1",
            "resources": [
                {"name": "pods", "namespaced": true, "kind": "Pod", "verbs": ["get"]},
                {"name": "pods/log", "namespaced": true, "kind": "Pod", "verbs": ["get"]}
            ]
        }"#;
        let list: ApiResourceList = serde_json::from_str(json).unwrap();
        assert_eq!(list.group_version, "v1");
        assert_eq!(list.resources.len(), 2);
        // Subresources (containing /) should be filtered in the execute function
        assert!(list.resources[1].name.contains('/'));
    }

    #[test]
    fn test_api_resource_subresource_filtering() {
        let resources = [
            ("pods", false),
            ("pods/log", true),
            ("pods/status", true),
            ("deployments", false),
            ("deployments/scale", true),
        ];
        let filtered: Vec<_> = resources
            .iter()
            .filter(|(name, _)| !name.contains('/'))
            .collect();
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].0, "pods");
        assert_eq!(filtered[1].0, "deployments");
    }
}
