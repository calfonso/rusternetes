#![allow(dead_code)] // consumed by later tasks in this branch
use anyhow::Result;
use serde_json::Value;

/// A resolved mapping from a resource (by any of its names) to the
/// information every kubectl verb needs to build an API path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceMapping {
    pub group: String,   // "" for the core group
    pub version: String, // preferred version, e.g. "v1"
    pub kind: String,    // "Deployment"
    pub plural: String,  // "deployments"
    pub singular: String,
    pub namespaced: bool,
    pub verbs: Vec<String>,
    pub short_names: Vec<String>,
}

/// Parse an aggregated-discovery (`APIGroupDiscoveryList`) JSON document into a
/// flat list of resource mappings. Subresources (those whose `resource` name
/// contains '/') are skipped — they are not independently addressable kinds.
pub fn parse_aggregated_discovery(doc: &Value) -> Result<Vec<ResourceMapping>> {
    let mut out = Vec::new();
    let Some(items) = doc.get("items").and_then(|i| i.as_array()) else {
        return Ok(out);
    };
    for group in items {
        let group_name = group
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let Some(versions) = group.get("versions").and_then(|v| v.as_array()) else {
            continue;
        };
        for ver in versions {
            let version = ver.get("version").and_then(|v| v.as_str()).unwrap_or("");
            let Some(resources) = ver.get("resources").and_then(|r| r.as_array()) else {
                continue;
            };
            for r in resources {
                let plural = r.get("resource").and_then(|v| v.as_str()).unwrap_or("");
                if plural.is_empty() || plural.contains('/') {
                    continue; // skip subresources / malformed
                }
                let kind = r
                    .pointer("/responseKind/kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if kind.is_empty() {
                    continue; // skip entries with no kind (garbage mapping / empty API path)
                }
                let namespaced = r.get("scope").and_then(|v| v.as_str()) == Some("Namespaced");
                let singular = r
                    .get("singularResource")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .unwrap_or_else(|| kind.to_lowercase());
                let short_names = r
                    .get("shortNames")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| s.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                let verbs = r
                    .get("verbs")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| s.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                out.push(ResourceMapping {
                    group: group_name.clone(),
                    version: version.to_string(),
                    kind,
                    plural: plural.to_string(),
                    singular,
                    namespaced,
                    verbs,
                    short_names,
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Value {
        let raw = include_str!("../tests/fixtures/aggregated-discovery.json");
        serde_json::from_str(raw).expect("fixture parses")
    }

    #[test]
    fn parses_core_and_grouped_kinds() {
        let mappings = parse_aggregated_discovery(&fixture()).unwrap();
        let dep = mappings
            .iter()
            .find(|m| m.kind == "Deployment")
            .expect("Deployment present");
        assert_eq!(dep.group, "apps");
        assert_eq!(dep.version, "v1");
        assert_eq!(dep.plural, "deployments");
        assert!(dep.namespaced);

        let ns = mappings
            .iter()
            .find(|m| m.kind == "Namespace")
            .expect("Namespace present");
        assert_eq!(ns.group, ""); // core
        assert!(!ns.namespaced);
        assert_eq!(ns.short_names, vec!["ns"]); // short names round-trip
    }

    #[test]
    fn skips_subresources() {
        let mappings = parse_aggregated_discovery(&fixture()).unwrap();
        assert!(mappings.iter().all(|m| !m.plural.contains('/')));
    }

    #[test]
    fn skips_slash_resource_entries() {
        let doc = serde_json::json!({
            "items": [{
                "metadata": {"name": ""},
                "versions": [{"version": "v1", "resources": [
                    {"resource": "pods", "responseKind": {"kind": "Pod"}, "scope": "Namespaced", "singularResource": "pod"},
                    {"resource": "pods/status", "responseKind": {"kind": "Pod"}, "scope": "Namespaced", "singularResource": "pod"}
                ]}]
            }]
        });
        let mappings = parse_aggregated_discovery(&doc).unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].plural, "pods");
    }
}
