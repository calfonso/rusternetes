//! Every timestamp a resource emits must match the API type it stands for:
//! `metav1.Time` is whole seconds, `metav1.MicroTime` is microseconds.
//!
//! Each case feeds a resource nanosecond-precision input — what a client or an
//! already-stored object may carry — and checks what comes back out. That covers
//! both halves of the contract, and walks real resource types rather than the
//! serde helpers in isolation, so a field that never had a helper attached is
//! caught here.

use serde_json::{json, Value};

const NANOS: &str = "2026-08-07T04:32:22.089611138Z";
const SECONDS: &str = "2026-08-07T04:32:22Z";
const MICROS: &str = "2026-08-07T04:32:22.089611Z";

/// Collect `(json path, value)` for every string that looks like a timestamp.
fn timestamps(value: &Value, path: &str, found: &mut Vec<(String, String)>) {
    match value {
        Value::String(s) => {
            // 2026-08-07T04:32:22... — enough to spot one without a regex crate.
            let looks_like_time = s.len() >= 20
                && s.as_bytes().get(4) == Some(&b'-')
                && s.as_bytes().get(10) == Some(&b'T')
                && s.ends_with('Z');
            if looks_like_time {
                found.push((path.to_string(), s.clone()));
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                timestamps(item, &format!("{path}[{i}]"), found);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                let child = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                timestamps(item, &child, found);
            }
        }
        _ => {}
    }
}

/// Round-trip `input` through `T` and return every timestamp it emits.
fn round_trip<T>(input: Value) -> Vec<(String, String)>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let resource: T = serde_json::from_value(input).expect("nanosecond input must deserialize");
    let json = serde_json::to_value(&resource).unwrap();
    let mut found = Vec::new();
    timestamps(&json, "", &mut found);
    found
}

/// Assert the named paths are present and that every timestamp is whole seconds.
fn assert_whole_seconds(found: &[(String, String)], expected: &[&str]) {
    for path in expected {
        assert!(
            found.iter().any(|(p, _)| p == path),
            "expected a timestamp at {path}; found {found:?}"
        );
    }
    for (path, value) in found {
        assert_eq!(
            value, SECONDS,
            "{path} should be whole seconds (metav1.Time)"
        );
    }
}

fn pod_template() -> Value {
    json!({
        "metadata": { "labels": { "app": "x" } },
        "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
    })
}

#[test]
fn object_meta_timestamps_are_whole_seconds() {
    use rusternetes_common::resources::Pod;

    let found = round_trip::<Pod>(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod",
            "namespace": "default",
            "creationTimestamp": NANOS,
            "deletionTimestamp": NANOS,
        },
        "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
    }));

    assert_whole_seconds(
        &found,
        &["metadata.creationTimestamp", "metadata.deletionTimestamp"],
    );
}

#[test]
fn node_condition_timestamps_are_whole_seconds() {
    use rusternetes_common::resources::node::Node;

    let found = round_trip::<Node>(json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": { "name": "node-1", "creationTimestamp": NANOS },
        "status": {
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "lastHeartbeatTime": NANOS,
                "lastTransitionTime": NANOS,
            }]
        }
    }));

    assert_whole_seconds(
        &found,
        &[
            "status.conditions[0].lastHeartbeatTime",
            "status.conditions[0].lastTransitionTime",
        ],
    );
}

#[test]
fn job_status_timestamps_are_whole_seconds() {
    use rusternetes_common::resources::workloads::Job;

    let found = round_trip::<Job>(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": { "name": "job", "namespace": "default" },
        "spec": { "template": pod_template() },
        "status": {
            "startTime": NANOS,
            "completionTime": NANOS,
            "conditions": [{
                "type": "Complete",
                "status": "True",
                "lastTransitionTime": NANOS,
            }]
        }
    }));

    assert_whole_seconds(
        &found,
        &[
            "status.startTime",
            "status.completionTime",
            "status.conditions[0].lastTransitionTime",
        ],
    );
}

#[test]
fn deployment_condition_timestamps_are_whole_seconds() {
    use rusternetes_common::resources::Deployment;

    let found = round_trip::<Deployment>(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "deploy", "namespace": "default" },
        "spec": {
            "selector": { "matchLabels": { "app": "x" } },
            "template": pod_template()
        },
        "status": {
            "conditions": [{
                "type": "Available",
                "status": "True",
                "lastUpdateTime": NANOS,
                "lastTransitionTime": NANOS,
            }]
        }
    }));

    assert_whole_seconds(
        &found,
        &[
            "status.conditions[0].lastUpdateTime",
            "status.conditions[0].lastTransitionTime",
        ],
    );
}

#[test]
fn pdb_disrupted_pods_timestamps_are_whole_seconds() {
    use rusternetes_common::resources::PodDisruptionBudget;

    let found = round_trip::<PodDisruptionBudget>(json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": { "name": "pdb", "namespace": "default" },
        "spec": { "selector": { "matchLabels": { "app": "x" } } },
        "status": {
            "currentHealthy": 1,
            "desiredHealthy": 1,
            "disruptionsAllowed": 0,
            "expectedPods": 1,
            "disruptedPods": { "pod-a": NANOS }
        }
    }));

    assert_whole_seconds(&found, &["status.disruptedPods.pod-a"]);
}

#[test]
fn event_keeps_micro_time_where_upstream_does() {
    use rusternetes_common::resources::event::Event;

    let found = round_trip::<Event>(json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": { "name": "event", "namespace": "default" },
        "involvedObject": { "kind": "Pod", "name": "pod", "namespace": "default" },
        "reason": "Started",
        "message": "message",
        "type": "Normal",
        "firstTimestamp": NANOS,
        "lastTimestamp": NANOS,
        "eventTime": NANOS,
    }));

    for (path, value) in &found {
        match path.as_str() {
            // metav1.MicroTime
            "eventTime" => assert_eq!(value, MICROS, "{path} should keep microseconds"),
            // metav1.Time
            _ => assert_eq!(value, SECONDS, "{path} should be whole seconds"),
        }
    }

    for path in ["firstTimestamp", "lastTimestamp", "eventTime"] {
        assert!(
            found.iter().any(|(p, _)| p == path),
            "{path} missing from {found:?}"
        );
    }
}

#[test]
fn lease_keeps_micro_time() {
    use rusternetes_common::resources::coordination::Lease;

    let found = round_trip::<Lease>(json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": { "name": "lease", "namespace": "kube-system" },
        "spec": { "acquireTime": NANOS, "renewTime": NANOS }
    }));

    for (path, value) in &found {
        if path.starts_with("spec.") {
            assert_eq!(value, MICROS, "{path} should keep microseconds");
        }
    }
    assert!(found.iter().any(|(p, _)| p == "spec.renewTime"));
}
