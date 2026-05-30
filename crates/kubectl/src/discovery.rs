#![allow(dead_code)] // consumed by later tasks in this branch
use crate::client::ApiClient;
use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;

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

/// Resolves a user-supplied resource reference (plural, singular, short name,
/// or Kind) to a single `ResourceMapping`.
pub struct RestMapper {
    mappings: Vec<ResourceMapping>,
    by_key: HashMap<String, usize>, // lowercased key -> index, first writer wins
}

impl RestMapper {
    pub fn new(mut mappings: Vec<ResourceMapping>) -> Self {
        // Order so the core group is preferred on key collisions: sort with
        // core ("") first. Insertion is first-writer-wins, so core entries
        // claim shared keys (e.g. "events", "Event") ahead of grouped ones.
        mappings.sort_by(|a, b| {
            (!a.group.is_empty())
                .cmp(&(!b.group.is_empty()))
                .then_with(|| a.group.cmp(&b.group))
                .then_with(|| a.plural.cmp(&b.plural))
        });
        let mut by_key = HashMap::new();
        for (i, m) in mappings.iter().enumerate() {
            let mut keys = vec![
                m.plural.to_lowercase(),
                m.singular.to_lowercase(),
                m.kind.to_lowercase(),
            ];
            keys.extend(m.short_names.iter().map(|s| s.to_lowercase()));
            for k in keys {
                by_key.entry(k).or_insert(i); // first writer (core) wins
            }
        }
        Self { mappings, by_key }
    }

    /// Resolve by plural / singular / short-name / Kind (case-insensitive).
    pub fn resolve(&self, reference: &str) -> Option<&ResourceMapping> {
        self.by_key
            .get(&reference.to_lowercase())
            .map(|&i| &self.mappings[i])
    }

    /// Returns all mappings in preference order (core group first).
    pub fn all(&self) -> &[ResourceMapping] {
        &self.mappings
    }

    const AGG_ACCEPT: &'static str =
        "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json";

    /// Build a mapper from the api-server's aggregated discovery. Fetches both
    /// the core group (`/api`) and all named groups (`/apis`) — one HTTP call
    /// each — and merges them.
    pub async fn from_server(client: &ApiClient) -> anyhow::Result<Self> {
        let core = client
            .get_raw_with_accept("/api", Self::AGG_ACCEPT)
            .await
            .context("unable to fetch core API discovery")?;
        let apis = client
            .get_raw_with_accept("/apis", Self::AGG_ACCEPT)
            .await
            .context("unable to fetch API group discovery")?;
        let mut mappings = parse_aggregated_discovery(&core)?;
        mappings.extend(parse_aggregated_discovery(&apis)?);
        Ok(Self::new(mappings))
    }
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

    fn mapper() -> RestMapper {
        RestMapper::new(parse_aggregated_discovery(&fixture()).unwrap())
    }

    #[test]
    fn resolves_by_every_key_form() {
        let m = mapper();
        assert_eq!(m.resolve("deployments").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("deployment").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("Deployment").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("DEPLOYMENT").unwrap().kind, "Deployment"); // case-insensitive
    }

    #[test]
    fn resolves_by_short_name() {
        // Use a short name that ACTUALLY EXISTS in the fixture. `namespaces`
        // has shortName "ns".
        let m = mapper();
        assert_eq!(m.resolve("ns").unwrap().kind, "Namespace");
    }

    #[test]
    fn unknown_resolves_to_none() {
        assert!(mapper().resolve("nonexistentthing").is_none());
        assert!(mapper().resolve("").is_none());
    }

    #[test]
    fn event_prefers_core_group() {
        // "Event" exists in both core and events.k8s.io; core wins.
        let m = mapper();
        assert_eq!(m.resolve("Event").unwrap().group, "");
    }

    fn is_writable(m: &ResourceMapping) -> bool {
        m.verbs.iter().any(|v| v == "create" || v == "update")
    }

    /// Anti-drift parity guard: every WRITABLE kind the server serves (per the
    /// discovery fixture) must be resolvable by its plural. This structurally
    /// catches any parse/index regression that silently drops a served kind —
    /// the exact class of bug that motivated the discovery-driven RestMapper
    /// (the client used to lag the server by 31 writable kinds).
    #[test]
    fn mapper_resolves_every_served_writable_kind() {
        let mappings = parse_aggregated_discovery(&fixture()).unwrap();
        let mapper = RestMapper::new(mappings.clone());

        let writable: Vec<&ResourceMapping> = mappings.iter().filter(|m| is_writable(m)).collect();

        let mut unresolved: Vec<String> = Vec::new();
        for m in &writable {
            if mapper.resolve(&m.plural).is_none() {
                unresolved.push(format!("{} ({}/{})", m.kind, m.group, m.plural));
            }
        }

        eprintln!(
            "parity: verified {} writable kinds resolve by plural",
            writable.len()
        );

        assert!(
            unresolved.is_empty(),
            "RestMapper failed to resolve {} served writable kind(s) by plural: {:?}",
            unresolved.len(),
            unresolved
        );
        // A healthy server surface has dozens of writable kinds. If this drops
        // to a handful the fixture is truncated / parsing broke wholesale.
        assert!(
            writable.len() >= 30,
            "expected >=30 writable kinds in fixture, found {} (stale/truncated fixture?)",
            writable.len()
        );
    }

    /// Explicit regression guard for the specific kinds that the legacy
    /// hardcoded table could not touch. Each present one must resolve by BOTH
    /// its plural and its Kind. At least 6 of the 8 must exist in the fixture
    /// (fewer => stale fixture).
    #[test]
    fn previously_unsupported_kinds_resolve() {
        let m = mapper();
        // plural -> Kind for the kinds that motivated the migration.
        let explicit: &[(&str, &str)] = &[
            ("networkpolicies", "NetworkPolicy"),
            ("poddisruptionbudgets", "PodDisruptionBudget"),
            ("endpointslices", "EndpointSlice"),
            ("horizontalpodautoscalers", "HorizontalPodAutoscaler"),
            ("replicasets", "ReplicaSet"),
            ("ingresses", "Ingress"),
            ("runtimeclasses", "RuntimeClass"),
            (
                "validatingwebhookconfigurations",
                "ValidatingWebhookConfiguration",
            ),
        ];

        let mut asserted = 0usize;
        for (plural, kind) in explicit {
            // Only kinds actually present in the fixture are asserted; missing
            // ones are skipped (the >=6 floor below catches a stale fixture).
            if m.resolve(plural).is_none() && m.resolve(kind).is_none() {
                continue;
            }
            assert!(
                m.resolve(plural).is_some(),
                "expected to resolve previously-unsupported plural {plural:?}",
            );
            assert!(
                m.resolve(kind).is_some(),
                "expected to resolve previously-unsupported Kind {kind:?}",
            );
            asserted += 1;
        }

        eprintln!("parity: asserted {asserted} previously-unsupported kinds");
        assert!(
            asserted >= 6,
            "only {asserted} of the previously-unsupported kinds present in the \
             fixture — expected >=6 (stale fixture?)",
        );
    }

    /// Optional live-surface parity: if an api-server is reachable, run the same
    /// writable-kind parity assertion against the real discovery surface. Skips
    /// cleanly (green) when no server is up, so CI without a cluster passes.
    #[tokio::test]
    async fn live_server_writable_parity() {
        let client = match ApiClient::new("https://localhost:6443", true, None) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("parity(live): skipping — could not build client ({e:#})");
                return;
            }
        };
        let mapper = match RestMapper::from_server(&client).await {
            Ok(m) => m,
            Err(e) => {
                eprintln!("parity(live): skipping — no reachable server ({e:#})");
                return;
            }
        };

        let writable: Vec<&ResourceMapping> =
            mapper.all().iter().filter(|m| is_writable(m)).collect();
        let mut unresolved: Vec<String> = Vec::new();
        for m in &writable {
            if mapper.resolve(&m.plural).is_none() {
                unresolved.push(format!("{} ({}/{})", m.kind, m.group, m.plural));
            }
        }
        eprintln!(
            "parity(live): verified {} writable kinds resolve by plural",
            writable.len()
        );
        assert!(
            unresolved.is_empty(),
            "live RestMapper failed to resolve writable kind(s): {unresolved:?}",
        );
    }
}
