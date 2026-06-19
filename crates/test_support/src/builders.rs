//! JSON-backed resource builders.
//!
//! Each builder accumulates a `serde_json::Value` and yields either the raw
//! JSON (`json()`) or a typed resource (`build::<T>()`). This mirrors how the
//! existing tests seed resources with `json!(...)`, while giving porting code
//! precise control over malformed inputs.

use rusternetes_common::resources::{Node, NodeStatus};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::collections::HashMap;

/// Typed `Node` with `cpu`/`memory` set on both `status.capacity` and
/// `status.allocatable` — the shape scheduler fit/preemption tests need.
///
/// Lifted from the `make_node(name, cpu, memory) -> Node` helper that was
/// copy-pasted byte-for-byte across `crates/scheduler/tests/*`. Returns a typed
/// `Node` (not JSON) so it drops straight in for those `make_node` call sites.
pub fn node_with_resources(name: &str, cpu: &str, memory: &str) -> Node {
    let mut quantities = HashMap::new();
    quantities.insert("cpu".to_string(), cpu.to_string());
    quantities.insert("memory".to_string(), memory.to_string());
    let mut node = Node::new(name);
    node.status = Some(NodeStatus {
        capacity: Some(quantities.clone()),
        allocatable: Some(quantities),
        ..Default::default()
    });
    node
}

/// Deep-merge `src` into `dst` (objects merged recursively, scalars/arrays
/// overwritten). Used by the builders' `merge` setters.
fn deep_merge(dst: &mut Value, src: Value) {
    match (dst, src) {
        (Value::Object(d), Value::Object(s)) => {
            for (k, v) in s {
                deep_merge(d.entry(k).or_insert(Value::Null), v);
            }
        }
        (d, s) => *d = s,
    }
}

/// A generic resource builder over a JSON document. Use the constructors
/// ([`service`], [`node`], [`endpoint_slice`]) or [`ResourceBuilder::new`].
#[derive(Debug, Clone)]
pub struct ResourceBuilder {
    v: Value,
}

impl ResourceBuilder {
    /// Seed `apiVersion`, `kind`, and `metadata.name`.
    pub fn new(api_version: &str, kind: &str, name: &str) -> Self {
        Self {
            v: json!({
                "apiVersion": api_version,
                "kind": kind,
                "metadata": { "name": name },
            }),
        }
    }

    /// Set `metadata.namespace`.
    #[must_use]
    pub fn namespace(mut self, ns: &str) -> Self {
        self.v["metadata"]["namespace"] = json!(ns);
        self
    }

    /// Set `metadata.uid`.
    #[must_use]
    pub fn uid(mut self, uid: &str) -> Self {
        self.v["metadata"]["uid"] = json!(uid);
        self
    }

    /// Add a `metadata.labels` entry.
    #[must_use]
    pub fn label(mut self, key: &str, value: &str) -> Self {
        let meta = self.v["metadata"].as_object_mut().expect("metadata object");
        meta.entry("labels")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("labels object")
            .insert(key.to_string(), json!(value));
        self
    }

    /// Deep-merge an arbitrary fragment into the document root, e.g.
    /// `merge(json!({"spec": {"clusterIP": "10.0.0.1"}}))`.
    #[must_use]
    pub fn merge(mut self, fragment: Value) -> Self {
        deep_merge(&mut self.v, fragment);
        self
    }

    /// The accumulated JSON document.
    pub fn json(&self) -> Value {
        self.v.clone()
    }

    /// Deserialize into a typed resource. Panics with a clear message on
    /// failure so a malformed fixture is obvious in the test output.
    pub fn build<T: DeserializeOwned>(&self) -> T {
        serde_json::from_value(self.v.clone()).unwrap_or_else(|e| {
            panic!(
                "test_support: failed to build resource from JSON: {e}\n{}",
                self.v
            )
        })
    }
}

/// `v1` Service builder.
pub fn service(name: &str) -> ResourceBuilder {
    ResourceBuilder::new("v1", "Service", name)
}

/// `v1` Node builder.
pub fn node(name: &str) -> ResourceBuilder {
    ResourceBuilder::new("v1", "Node", name)
}

/// `discovery.k8s.io/v1` EndpointSlice builder.
pub fn endpoint_slice(name: &str) -> ResourceBuilder {
    ResourceBuilder::new("discovery.k8s.io/v1", "EndpointSlice", name)
}

/// `v1` Pod builder with first-class container helpers — the common shape for
/// validation tests.
#[derive(Debug, Clone)]
pub struct PodBuilder {
    v: Value,
}

/// Start a Pod with no containers (`spec.containers: []`). Add containers with
/// [`PodBuilder::container`].
pub fn pod(name: &str) -> PodBuilder {
    PodBuilder {
        v: json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": name },
            "spec": { "containers": [] },
        }),
    }
}

impl PodBuilder {
    /// Set `metadata.namespace`.
    #[must_use]
    pub fn namespace(mut self, ns: &str) -> Self {
        self.v["metadata"]["namespace"] = json!(ns);
        self
    }

    /// Add a `metadata.labels` entry.
    #[must_use]
    pub fn label(mut self, key: &str, value: &str) -> Self {
        let meta = self.v["metadata"].as_object_mut().expect("metadata object");
        meta.entry("labels")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("labels object")
            .insert(key.to_string(), json!(value));
        self
    }

    /// Append a minimal container `{name, image}`.
    #[must_use]
    pub fn container(self, name: &str, image: &str) -> Self {
        self.container_value(json!({ "name": name, "image": image }))
    }

    /// Append an arbitrary container JSON value (for cases needing extra fields
    /// or deliberately invalid containers).
    #[must_use]
    pub fn container_value(mut self, c: Value) -> Self {
        self.v["spec"]["containers"]
            .as_array_mut()
            .expect("spec.containers array")
            .push(c);
        self
    }

    /// Append an init container.
    #[must_use]
    pub fn init_container(mut self, name: &str, image: &str) -> Self {
        self.v["spec"]
            .as_object_mut()
            .expect("spec object")
            .entry("initContainers")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .expect("spec.initContainers array")
            .push(json!({ "name": name, "image": image }));
        self
    }

    /// Set `spec.restartPolicy`.
    #[must_use]
    pub fn restart_policy(mut self, policy: &str) -> Self {
        self.v["spec"]["restartPolicy"] = json!(policy);
        self
    }

    /// Set an arbitrary `spec.<key>`.
    #[must_use]
    pub fn spec_field(mut self, key: &str, value: Value) -> Self {
        self.v["spec"]
            .as_object_mut()
            .expect("spec object")
            .insert(key.to_string(), value);
        self
    }

    /// Deep-merge a fragment into the document root.
    #[must_use]
    pub fn merge(mut self, fragment: Value) -> Self {
        deep_merge(&mut self.v, fragment);
        self
    }

    /// The accumulated JSON document.
    pub fn json(&self) -> Value {
        self.v.clone()
    }

    /// Deserialize into a typed resource (usually `Pod`).
    pub fn build<T: DeserializeOwned>(&self) -> T {
        serde_json::from_value(self.v.clone()).unwrap_or_else(|e| {
            panic!(
                "test_support: failed to build pod from JSON: {e}\n{}",
                self.v
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_builder_shapes_spec() {
        let v = pod("p")
            .namespace("ns")
            .label("app", "x")
            .container("c", "img")
            .restart_policy("Never")
            .json();
        assert_eq!(v["metadata"]["name"], "p");
        assert_eq!(v["metadata"]["namespace"], "ns");
        assert_eq!(v["metadata"]["labels"]["app"], "x");
        assert_eq!(v["spec"]["containers"][0]["name"], "c");
        assert_eq!(v["spec"]["restartPolicy"], "Never");
    }

    #[test]
    fn resource_builder_merges() {
        let v = service("s")
            .namespace("ns")
            .merge(json!({"spec": {"clusterIP": "10.0.0.1", "ports": [{"port": 80}]}}))
            .json();
        assert_eq!(v["spec"]["clusterIP"], "10.0.0.1");
        assert_eq!(v["spec"]["ports"][0]["port"], 80);
    }
}
