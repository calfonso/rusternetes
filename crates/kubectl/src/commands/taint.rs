use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// A parsed taint specification.
#[derive(Debug, Clone, PartialEq)]
struct TaintSpec {
    key: String,
    value: Option<String>,
    effect: Option<String>,
    remove: bool,
}

/// Parse a taint string into a TaintSpec.
///
/// Formats:
///   key=value:Effect     -> add taint
///   key:Effect           -> add taint (no value)
///   key=value:Effect-    -> remove taint
///   key:Effect-          -> remove taint with specific effect
///   key-                 -> remove all taints with key
fn parse_taint(taint_str: &str) -> Result<TaintSpec> {
    let remove = taint_str.ends_with('-');
    let s = if remove {
        &taint_str[..taint_str.len() - 1]
    } else {
        taint_str
    };

    // Try to split on ':'
    if let Some((key_value, effect)) = s.rsplit_once(':') {
        // Validate effect
        let valid_effects = ["NoSchedule", "PreferNoSchedule", "NoExecute"];
        if !valid_effects.contains(&effect) {
            anyhow::bail!(
                "Invalid taint effect '{}'. Must be one of: NoSchedule, PreferNoSchedule, NoExecute",
                effect
            );
        }

        // Split key=value
        if let Some((key, value)) = key_value.split_once('=') {
            Ok(TaintSpec {
                key: key.to_string(),
                value: Some(value.to_string()),
                effect: Some(effect.to_string()),
                remove,
            })
        } else {
            Ok(TaintSpec {
                key: key_value.to_string(),
                value: None,
                effect: Some(effect.to_string()),
                remove,
            })
        }
    } else if remove {
        // Just "key-" — remove all taints with this key
        if let Some((key, value)) = s.split_once('=') {
            Ok(TaintSpec {
                key: key.to_string(),
                value: Some(value.to_string()),
                effect: None,
                remove: true,
            })
        } else {
            Ok(TaintSpec {
                key: s.to_string(),
                value: None,
                effect: None,
                remove: true,
            })
        }
    } else {
        anyhow::bail!(
            "Invalid taint format: '{}'. Expected key=value:Effect, key:Effect, or key-",
            taint_str
        );
    }
}

/// Add or remove taints on a node.
///
/// Usage: `kubectl taint nodes node1 key=value:NoSchedule`
pub async fn execute(
    client: &ApiClient,
    node_name: &str,
    taints: &[String],
    overwrite: bool,
) -> Result<()> {
    // Parse taint specifications
    let mut taint_specs: Vec<TaintSpec> = Vec::new();
    for t in taints {
        taint_specs.push(parse_taint(t)?);
    }

    // Get the current node
    let path = format!("/api/v1/nodes/{}", node_name);
    let node: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get node")?;

    // Get current taints
    let mut current_taints: Vec<Value> = node
        .get("spec")
        .and_then(|s| s.get("taints"))
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();

    for spec in &taint_specs {
        if spec.remove {
            // Remove taints matching key (and optionally effect)
            let before_len = current_taints.len();
            current_taints.retain(|t| {
                let key = t.get("key").and_then(|k| k.as_str()).unwrap_or("");
                let effect = t.get("effect").and_then(|e| e.as_str());

                if key != spec.key {
                    return true;
                }
                if let Some(ref spec_effect) = spec.effect {
                    // Only remove if effect matches too
                    if effect != Some(spec_effect.as_str()) {
                        return true;
                    }
                }
                false
            });

            if current_taints.len() == before_len {
                eprintln!(
                    "warning: taint \"{}\" not found on node \"{}\"",
                    spec.key, node_name
                );
            }
        } else {
            // Check for duplicate taint
            let existing = current_taints.iter().position(|t| {
                let key = t.get("key").and_then(|k| k.as_str()).unwrap_or("");
                let effect = t.get("effect").and_then(|e| e.as_str());
                key == spec.key && effect == spec.effect.as_deref()
            });

            if let Some(idx) = existing {
                if overwrite {
                    current_taints.remove(idx);
                } else {
                    anyhow::bail!(
                        "Node '{}' already has a taint with key '{}' and effect '{}'. Use --overwrite to update",
                        node_name,
                        spec.key,
                        spec.effect.as_deref().unwrap_or(""),
                    );
                }
            }

            // Add the taint
            let mut taint = json!({
                "key": spec.key,
                "effect": spec.effect,
            });
            if let Some(ref value) = spec.value {
                taint["value"] = json!(value);
            }
            current_taints.push(taint);
        }
    }

    // Patch the node with updated taints
    let patch_body = json!({
        "spec": {
            "taints": if current_taints.is_empty() { Value::Null } else { json!(current_taints) },
        }
    });

    let _result: Value = client
        .patch(&path, &patch_body, "application/merge-patch+json")
        .await
        .context("Failed to update node taints")?;

    // Print results
    for spec in &taint_specs {
        if spec.remove {
            println!("node/{} untainted", node_name);
        } else {
            println!("node/{} tainted", node_name);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_taint_with_value_and_effect() {
        let spec = parse_taint("dedicated=special-user:NoSchedule").unwrap();
        assert_eq!(spec.key, "dedicated");
        assert_eq!(spec.value, Some("special-user".to_string()));
        assert_eq!(spec.effect, Some("NoSchedule".to_string()));
        assert!(!spec.remove);
    }

    #[test]
    fn test_parse_taint_no_value() {
        let spec = parse_taint("bar:NoSchedule").unwrap();
        assert_eq!(spec.key, "bar");
        assert_eq!(spec.value, None);
        assert_eq!(spec.effect, Some("NoSchedule".to_string()));
        assert!(!spec.remove);
    }

    #[test]
    fn test_parse_taint_remove_with_effect() {
        let spec = parse_taint("dedicated:NoSchedule-").unwrap();
        assert_eq!(spec.key, "dedicated");
        assert_eq!(spec.value, None);
        assert_eq!(spec.effect, Some("NoSchedule".to_string()));
        assert!(spec.remove);
    }

    #[test]
    fn test_parse_taint_remove_key_only() {
        let spec = parse_taint("dedicated-").unwrap();
        assert_eq!(spec.key, "dedicated");
        assert_eq!(spec.value, None);
        assert_eq!(spec.effect, None);
        assert!(spec.remove);
    }

    #[test]
    fn test_parse_taint_invalid_effect() {
        assert!(parse_taint("key=value:BadEffect").is_err());
    }

    #[test]
    fn test_parse_taint_remove_with_value_and_effect() {
        let spec = parse_taint("key=value:NoExecute-").unwrap();
        assert_eq!(spec.key, "key");
        assert_eq!(spec.value, Some("value".to_string()));
        assert_eq!(spec.effect, Some("NoExecute".to_string()));
        assert!(spec.remove);
    }

    #[test]
    fn test_parse_taint_prefer_no_schedule() {
        let spec = parse_taint("key=foo:PreferNoSchedule").unwrap();
        assert_eq!(spec.effect, Some("PreferNoSchedule".to_string()));
    }

    #[test]
    fn test_parse_taint_no_effect_no_dash_fails() {
        // "key=value" without effect and without dash should fail
        let result = parse_taint("key=value");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_taint_remove_with_value_no_effect() {
        let spec = parse_taint("key=value-").unwrap();
        assert_eq!(spec.key, "key");
        assert_eq!(spec.value, Some("value".to_string()));
        assert_eq!(spec.effect, None);
        assert!(spec.remove);
    }

    #[test]
    fn test_taint_spec_equality() {
        let a = TaintSpec {
            key: "node-role".to_string(),
            value: Some("master".to_string()),
            effect: Some("NoSchedule".to_string()),
            remove: false,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
