use rusternetes_common::resources::CSIStorageCapacity;
use rusternetes_common::validation::csistoragecapacity::validate_csi_storage_capacity_update;
use serde_json::json;

fn csc(storage_class: &str, node_topology: serde_json::Value) -> CSIStorageCapacity {
    let mut v = json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "CSIStorageCapacity",
        "metadata": {"name": "csc-1", "namespace": "default"},
        "storageClassName": storage_class
    });
    if !node_topology.is_null() {
        v["nodeTopology"] = node_topology;
    }
    serde_json::from_value(v).unwrap()
}

#[test]
fn unchanged_passes() {
    let old = csc("fast", json!({"matchLabels": {"zone": "a"}}));
    let new = csc("fast", json!({"matchLabels": {"zone": "a"}}));
    assert!(validate_csi_storage_capacity_update(&new, &old).is_empty());
}

#[test]
fn storage_class_name_immutable() {
    let old = csc("fast", json!(null));
    let new = csc("slow", json!(null));
    let errs = validate_csi_storage_capacity_update(&new, &old);
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("storageClassName")),
        "{errs:?}"
    );
}

#[test]
fn node_topology_immutable() {
    let old = csc("fast", json!({"matchLabels": {"zone": "a"}}));
    let new = csc("fast", json!({"matchLabels": {"zone": "b"}}));
    let errs = validate_csi_storage_capacity_update(&new, &old);
    assert!(
        errs.iter().any(|e| e.to_string().contains("nodeTopology")),
        "{errs:?}"
    );
}

#[test]
fn node_topology_added_is_immutable() {
    let old = csc("fast", json!(null));
    let new = csc("fast", json!({"matchLabels": {"zone": "a"}}));
    assert_eq!(validate_csi_storage_capacity_update(&new, &old).len(), 1);
}
