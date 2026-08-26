use crate::client::ApiClient;
use anyhow::Result;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGroupList {
    groups: Vec<ApiGroup>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGroup {
    #[allow(dead_code)]
    name: String,
    versions: Vec<GroupVersionForDiscovery>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupVersionForDiscovery {
    group_version: String,
}

/// Display supported API versions
pub async fn execute(client: &ApiClient) -> Result<()> {
    let mut versions = Vec::new();

    // Add core API version
    versions.push("v1".to_string());

    // Get API groups
    let api_groups: ApiGroupList = client.get("/apis").await.map_err(|e| match e {
        crate::client::GetError::NotFound => anyhow::anyhow!("API groups not found"),
        crate::client::GetError::Other(e) => e,
    })?;

    for group in api_groups.groups {
        for version in group.versions {
            versions.push(version.group_version);
        }
    }

    // Sort versions
    versions.sort();

    // Print each version
    for version in versions {
        println!("{}", version);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_group_list_deserialization() {
        let json = r#"{
            "groups": [
                {
                    "name": "apps",
                    "versions": [
                        {"groupVersion": "apps/v1"}
                    ]
                },
                {
                    "name": "batch",
                    "versions": [
                        {"groupVersion": "batch/v1"},
                        {"groupVersion": "batch/v1beta1"}
                    ]
                }
            ]
        }"#;
        let group_list: ApiGroupList = serde_json::from_str(json).unwrap();
        assert_eq!(group_list.groups.len(), 2);
        assert_eq!(group_list.groups[0].name, "apps");
        assert_eq!(group_list.groups[1].versions.len(), 2);
        assert_eq!(group_list.groups[1].versions[0].group_version, "batch/v1");
    }

    #[test]
    fn test_versions_sorting() {
        let mut versions = vec![
            "batch/v1".to_string(),
            "apps/v1".to_string(),
            "v1".to_string(),
        ];
        versions.sort();
        assert_eq!(versions, vec!["apps/v1", "batch/v1", "v1"]);
    }

    #[test]
    fn test_api_group_list_empty_groups() {
        let json = r#"{"groups": []}"#;
        let group_list: ApiGroupList = serde_json::from_str(json).unwrap();
        assert!(group_list.groups.is_empty());
    }
}
