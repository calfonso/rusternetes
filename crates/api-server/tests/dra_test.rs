//! Integration tests for Dynamic Resource Allocation (DRA) API handlers
//!
//! Tests cover resource.k8s.io/v1 API group including:
//! - ResourceClaim (namespace-scoped with status subresource)
//! - ResourceClaimTemplate (namespace-scoped)
//! - DeviceClass (cluster-scoped)
//! - ResourceSlice (cluster-scoped)

use rusternetes_common::resources::{
    AllocationResult, CELDeviceSelector, Device, DeviceAllocationMode, DeviceAllocationResult,
    DeviceClaim, DeviceClass, DeviceClassConfiguration, DeviceClassSpec, DeviceRequest,
    DeviceSelector, ExactDeviceRequest, ResourceClaim, ResourceClaimSpec, ResourceClaimStatus,
    ResourceClaimTemplate, ResourceClaimTemplateSpec, ResourcePool, ResourceSlice,
    ResourceSliceSpec,
};

#[test]
fn test_resourceclaim_serialization() {
    let claim = ResourceClaim {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceClaim".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("test-claim".to_string()),
            namespace: Some("default".to_string()),
            uid: Some("test-uid-123".to_string()),
            creation_timestamp: Some(chrono::Utc::now()),
            ..Default::default()
        }),
        spec: ResourceClaimSpec {
            devices: DeviceClaim {
                requests: vec![DeviceRequest {
                    name: "gpu-req".to_string(),
                    exactly: Some(ExactDeviceRequest {
                        device_class_name: "gpu.example.com".to_string(),
                        selectors: vec![],
                        allocation_mode: Some(DeviceAllocationMode::ExactCount),
                        count: Some(1),
                        admin_access: None,
                        tolerations: vec![],
                        capacity: None,
                    }),
                    first_available: vec![],
                }],
                constraints: vec![],
                config: vec![],
            },
        },
        status: None,
    };

    // Test serialization
    let json = serde_json::to_string(&claim).expect("Failed to serialize ResourceClaim");
    assert!(json.contains("test-claim"));
    assert!(json.contains("gpu.example.com"));

    // Test deserialization
    let deserialized: ResourceClaim =
        serde_json::from_str(&json).expect("Failed to deserialize ResourceClaim");
    assert_eq!(
        deserialized
            .metadata
            .as_ref()
            .unwrap()
            .name
            .as_ref()
            .unwrap(),
        "test-claim"
    );
    assert_eq!(deserialized.spec.devices.requests.len(), 1);
    assert_eq!(deserialized.spec.devices.requests[0].name, "gpu-req");
}

#[test]
fn test_resourceclaim_with_allocation_modes() {
    let claim = ResourceClaim {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceClaim".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("multi-gpu-claim".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: ResourceClaimSpec {
            devices: DeviceClaim {
                requests: vec![DeviceRequest {
                    name: "exact-count-req".to_string(),
                    exactly: Some(ExactDeviceRequest {
                        device_class_name: "accelerator.example.com".to_string(),
                        selectors: vec![],
                        allocation_mode: Some(DeviceAllocationMode::ExactCount),
                        count: Some(4),
                        admin_access: Some(false),
                        tolerations: vec![],
                        capacity: None,
                    }),
                    first_available: vec![],
                }],
                constraints: vec![],
                config: vec![],
            },
        },
        status: None,
    };

    let json = serde_json::to_string(&claim).expect("Failed to serialize");
    assert!(json.contains("multi-gpu-claim"));
    assert!(json.contains("accelerator.example.com"));

    let deserialized: ResourceClaim = serde_json::from_str(&json).expect("Failed to deserialize");
    let exact_request = deserialized.spec.devices.requests[0]
        .exactly
        .as_ref()
        .unwrap();
    assert_eq!(exact_request.count.unwrap(), 4);
    assert_eq!(
        exact_request.allocation_mode.as_ref().unwrap(),
        &DeviceAllocationMode::ExactCount
    );
}

#[test]
fn test_resourceclaim_status() {
    let claim = ResourceClaim {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceClaim".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("test-claim-status".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: ResourceClaimSpec {
            devices: DeviceClaim::default(),
        },
        status: Some(ResourceClaimStatus {
            allocation: Some(AllocationResult {
                devices: DeviceAllocationResult {
                    results: vec![
                        rusternetes_common::resources::dra::DeviceRequestAllocationResult {
                            request: "gpu-req".to_string(),
                            driver: "gpu-driver.example.com".to_string(),
                            pool: "gpu-pool-1".to_string(),
                            device: "gpu-0".to_string(),
                        },
                    ],
                    config: vec![],
                },
                node_selector: None,
            }),
            devices: vec![],
            reserved_for: vec![],
            deallocation_requested: None,
        }),
    };

    let json = serde_json::to_string(&claim).expect("Failed to serialize");
    assert!(json.contains("gpu-driver.example.com"));

    let deserialized: ResourceClaim = serde_json::from_str(&json).expect("Failed to deserialize");
    assert!(deserialized.status.is_some());
    let status = deserialized.status.unwrap();
    assert!(status.allocation.is_some());
    let allocation = status.allocation.unwrap();
    assert_eq!(allocation.devices.results.len(), 1);
    assert_eq!(
        allocation.devices.results[0].driver,
        "gpu-driver.example.com"
    );
}

#[test]
fn test_resourceclaimtemplate_serialization() {
    let template = ResourceClaimTemplate {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceClaimTemplate".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("test-template".to_string()),
            namespace: Some("default".to_string()),
            uid: Some("template-uid-456".to_string()),
            ..Default::default()
        }),
        spec: ResourceClaimTemplateSpec {
            metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
                labels: Some(
                    vec![
                        ("app".to_string(), "ml-workload".to_string()),
                        ("tier".to_string(), "gpu".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            }),
            spec: ResourceClaimSpec {
                devices: DeviceClaim {
                    requests: vec![DeviceRequest {
                        name: "ml-gpu-req".to_string(),
                        exactly: Some(ExactDeviceRequest {
                            device_class_name: "ml-gpu.example.com".to_string(),
                            selectors: vec![],
                            allocation_mode: Some(DeviceAllocationMode::All),
                            count: None,
                            admin_access: None,
                            tolerations: vec![],
                            capacity: None,
                        }),
                        first_available: vec![],
                    }],
                    constraints: vec![],
                    config: vec![],
                },
            },
        },
    };

    // Test serialization
    let json = serde_json::to_string(&template).expect("Failed to serialize ResourceClaimTemplate");
    assert!(json.contains("test-template"));
    assert!(json.contains("ml-gpu.example.com"));
    assert!(json.contains("ml-workload"));

    // Test deserialization
    let deserialized: ResourceClaimTemplate =
        serde_json::from_str(&json).expect("Failed to deserialize ResourceClaimTemplate");
    assert_eq!(
        deserialized
            .metadata
            .as_ref()
            .unwrap()
            .name
            .as_ref()
            .unwrap(),
        "test-template"
    );
    assert_eq!(
        deserialized.spec.spec.devices.requests[0].name,
        "ml-gpu-req"
    );

    let labels = deserialized
        .spec
        .metadata
        .as_ref()
        .unwrap()
        .labels
        .as_ref()
        .unwrap();
    assert_eq!(labels.get("app").unwrap(), "ml-workload");
    assert_eq!(labels.get("tier").unwrap(), "gpu");
}

#[test]
fn test_deviceclass_serialization() {
    let device_class = DeviceClass {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "DeviceClass".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("nvidia-gpu-a100".to_string()),
            uid: Some("deviceclass-uid-789".to_string()),
            ..Default::default()
        }),
        spec: DeviceClassSpec {
            selectors: vec![
                DeviceSelector {
                    cel: Some(CELDeviceSelector {
                        expression: "device.driver == \"nvidia.com/gpu\"".to_string(),
                    }),
                },
                DeviceSelector {
                    cel: Some(CELDeviceSelector {
                        expression: "device.attributes[\"model\"].string == \"A100\"".to_string(),
                    }),
                },
            ],
            config: vec![],
            suitable_nodes: None,
        },
    };

    // Test serialization
    let json = serde_json::to_string(&device_class).expect("Failed to serialize DeviceClass");
    assert!(json.contains("nvidia-gpu-a100"));
    assert!(json.contains("nvidia.com/gpu"));
    assert!(json.contains("A100"));

    // Test deserialization
    let deserialized: DeviceClass =
        serde_json::from_str(&json).expect("Failed to deserialize DeviceClass");
    assert_eq!(
        deserialized
            .metadata
            .as_ref()
            .unwrap()
            .name
            .as_ref()
            .unwrap(),
        "nvidia-gpu-a100"
    );
    assert_eq!(deserialized.spec.selectors.len(), 2);

    let first_selector = &deserialized.spec.selectors[0];
    assert!(first_selector.cel.is_some());
    assert!(first_selector
        .cel
        .as_ref()
        .unwrap()
        .expression
        .contains("nvidia.com/gpu"));
}

#[test]
fn test_deviceclass_with_suitable_nodes() {
    let device_class = DeviceClass {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "DeviceClass".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("high-memory-gpu".to_string()),
            ..Default::default()
        }),
        spec: DeviceClassSpec {
            selectors: vec![DeviceSelector {
                cel: Some(CELDeviceSelector {
                    expression: "device.attributes[\"memory\"].int >= 80000000000".to_string(),
                }),
            }],
            config: vec![],
            suitable_nodes: Some(rusternetes_common::resources::NodeSelector {
                node_selector_terms: vec![rusternetes_common::resources::NodeSelectorTerm {
                    match_expressions: Some(vec![
                        rusternetes_common::resources::NodeSelectorRequirement {
                            key: "gpu".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec!["true".to_string()]),
                        },
                    ]),
                    match_fields: None,
                }],
            }),
        },
    };

    let json = serde_json::to_string(&device_class).expect("Failed to serialize");
    assert!(json.contains("high-memory-gpu"));
    assert!(json.contains("80000000000"));

    let deserialized: DeviceClass = serde_json::from_str(&json).expect("Failed to deserialize");
    assert!(deserialized.spec.suitable_nodes.is_some());
    let suitable_nodes = deserialized.spec.suitable_nodes.unwrap();
    assert_eq!(suitable_nodes.node_selector_terms.len(), 1);
}

#[test]
fn test_resourceslice_serialization() {
    let resource_slice = ResourceSlice {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceSlice".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("node-1-gpu-resources".to_string()),
            uid: Some("slice-uid-101".to_string()),
            ..Default::default()
        }),
        spec: ResourceSliceSpec {
            driver: "gpu-driver.example.com".to_string(),
            pool: ResourcePool {
                name: "gpu-pool-1".to_string(),
                resource_slice_count: 3,
                generation: 1,
            },
            node_name: Some("node-1".to_string()),
            node_selector: None,
            all_nodes: None,
            devices: vec![Device {
                name: "gpu-0".to_string(),
                attributes: Some(
                    vec![
                        (
                            "model".to_string(),
                            rusternetes_common::resources::dra::DeviceAttribute {
                                int_value: None,
                                bool_value: None,
                                string_value: Some("A100".to_string()),
                                version_value: None,
                            },
                        ),
                        (
                            "memory".to_string(),
                            rusternetes_common::resources::dra::DeviceAttribute {
                                int_value: Some(85899345920), // 80GB in bytes
                                bool_value: None,
                                string_value: None,
                                version_value: None,
                            },
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
                capacity: Some(
                    vec![(
                        "nvidia.com/gpu".to_string(),
                        rusternetes_common::resources::dra::DeviceCapacity {
                            value: "1".to_string(),
                            request_policy: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                ),
                consumes_counters: vec![],
                node_name: None,
                node_selector: None,
                all_nodes: None,
                taints: vec![],
                binds_to_node: None,
                binding_conditions: vec![],
                binding_failure_conditions: vec![],
                allow_multiple_allocations: None,
            }],
            per_device_node_selection: None,
            shared_counters: vec![],
        },
    };

    // Test serialization
    let json = serde_json::to_string(&resource_slice).expect("Failed to serialize ResourceSlice");
    assert!(json.contains("node-1-gpu-resources"));
    assert!(json.contains("gpu-driver.example.com"));
    assert!(json.contains("gpu-pool-1"));
    assert!(json.contains("A100"));

    // Test deserialization
    let deserialized: ResourceSlice =
        serde_json::from_str(&json).expect("Failed to deserialize ResourceSlice");
    assert_eq!(
        deserialized
            .metadata
            .as_ref()
            .unwrap()
            .name
            .as_ref()
            .unwrap(),
        "node-1-gpu-resources"
    );
    assert_eq!(deserialized.spec.driver, "gpu-driver.example.com");
    assert_eq!(deserialized.spec.node_name.as_ref().unwrap(), "node-1");

    assert_eq!(deserialized.spec.devices.len(), 1);
    assert_eq!(deserialized.spec.devices[0].name, "gpu-0");

    let attributes = deserialized.spec.devices[0].attributes.as_ref().unwrap();
    assert!(attributes.contains_key("model"));
    assert_eq!(
        attributes
            .get("model")
            .unwrap()
            .string_value
            .as_ref()
            .unwrap(),
        "A100"
    );
}

#[test]
fn test_resourceslice_pool_info() {
    let resource_slice = ResourceSlice {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceSlice".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("pool-test-slice".to_string()),
            ..Default::default()
        }),
        spec: ResourceSliceSpec {
            driver: "pool-driver.example.com".to_string(),
            pool: ResourcePool {
                name: "production-pool".to_string(),
                resource_slice_count: 10,
                generation: 5,
            },
            node_name: Some("node-123".to_string()),
            node_selector: None,
            all_nodes: None,
            devices: vec![],
            per_device_node_selection: None,
            shared_counters: vec![],
        },
    };

    let json = serde_json::to_string(&resource_slice).expect("Failed to serialize");
    assert!(json.contains("production-pool"));
    assert!(json.contains("10"));
    assert!(json.contains("\"generation\":5"));

    let deserialized: ResourceSlice = serde_json::from_str(&json).expect("Failed to deserialize");
    assert_eq!(deserialized.spec.pool.name, "production-pool");
    assert_eq!(deserialized.spec.pool.resource_slice_count, 10);
    assert_eq!(deserialized.spec.pool.generation, 5);
}

#[test]
fn test_deviceclass_empty_selectors() {
    let device_class = DeviceClass {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "DeviceClass".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("no-selector-class".to_string()),
            ..Default::default()
        }),
        spec: DeviceClassSpec {
            selectors: vec![],
            config: vec![],
            suitable_nodes: None,
        },
    };

    let json = serde_json::to_string(&device_class).expect("Failed to serialize");
    let deserialized: DeviceClass = serde_json::from_str(&json).expect("Failed to deserialize");
    assert_eq!(deserialized.spec.selectors.len(), 0);
}

#[test]
fn test_deviceclass_with_config() {
    let device_class = DeviceClass {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "DeviceClass".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("configured-class".to_string()),
            ..Default::default()
        }),
        spec: DeviceClassSpec {
            selectors: vec![],
            config: vec![DeviceClassConfiguration {
                opaque: Some(
                    rusternetes_common::resources::dra::OpaqueDeviceConfiguration {
                        driver: "example.com/gpu".to_string(),
                        parameters: serde_json::json!({
                            "mode": "performance",
                            "power_limit": 250
                        }),
                    },
                ),
            }],
            suitable_nodes: None,
        },
    };

    let json = serde_json::to_string(&device_class).expect("Failed to serialize");
    assert!(json.contains("example.com/gpu"));
    assert!(json.contains("performance"));

    let deserialized: DeviceClass = serde_json::from_str(&json).expect("Failed to deserialize");
    assert_eq!(deserialized.spec.config.len(), 1);
    let config = &deserialized.spec.config[0];
    assert!(config.opaque.is_some());
    assert_eq!(config.opaque.as_ref().unwrap().driver, "example.com/gpu");
}

#[test]
fn test_resourceclaim_all_allocation_mode() {
    let claim = ResourceClaim {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceClaim".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("all-mode-claim".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: ResourceClaimSpec {
            devices: DeviceClaim {
                requests: vec![DeviceRequest {
                    name: "all-gpus-req".to_string(),
                    exactly: Some(ExactDeviceRequest {
                        device_class_name: "gpu-class".to_string(),
                        selectors: vec![],
                        allocation_mode: Some(DeviceAllocationMode::All),
                        count: None, // count is not used with All mode
                        admin_access: None,
                        tolerations: vec![],
                        capacity: None,
                    }),
                    first_available: vec![],
                }],
                constraints: vec![],
                config: vec![],
            },
        },
        status: None,
    };

    let json = serde_json::to_string(&claim).expect("Serialization failed");
    assert!(json.contains("all-gpus-req"));

    let deserialized: ResourceClaim = serde_json::from_str(&json).expect("Deserialization failed");
    let request = &deserialized.spec.devices.requests[0];
    assert_eq!(
        request
            .exactly
            .as_ref()
            .unwrap()
            .allocation_mode
            .as_ref()
            .unwrap(),
        &DeviceAllocationMode::All
    );
}

#[test]
fn test_resourceslice_with_all_nodes() {
    let resource_slice = ResourceSlice {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceSlice".to_string(),
        metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
            name: Some("all-nodes-slice".to_string()),
            ..Default::default()
        }),
        spec: ResourceSliceSpec {
            driver: "cluster-wide-driver.example.com".to_string(),
            pool: ResourcePool {
                name: "cluster-pool".to_string(),
                resource_slice_count: 1,
                generation: 1,
            },
            node_name: None,
            node_selector: None,
            all_nodes: Some(true),
            devices: vec![],
            per_device_node_selection: None,
            shared_counters: vec![],
        },
    };

    let json = serde_json::to_string(&resource_slice).expect("Failed to serialize");
    assert!(json.contains("cluster-wide-driver.example.com"));

    let deserialized: ResourceSlice = serde_json::from_str(&json).expect("Failed to deserialize");
    assert!(deserialized.spec.all_nodes.unwrap());
    assert!(deserialized.spec.node_name.is_none());
}
