//! Upstream Kubernetes contract: every `*List` API response MUST serialize
//! `items: []` (an empty JSON array) when the collection is empty — never
//! `items: null`, never an absent `items` field.
//!
//! This contract is enforced upstream by the generic list types in
//! `k8s.io/apimachinery/pkg/apis/meta/v1` (e.g. `List.Items []runtime.Object`)
//! and by the protobuf wire format which serializes empty repeated fields as
//! zero entries. Clients (client-go, kubectl, controllers) all index into
//! `.items` unconditionally; a `null` would surface as
//! `cannot range over <nil>` / `json: cannot unmarshal null into Go struct
//! field`. See for example client-go's `cache.ListAll` and kubectl's
//! `cmd/get/get.go::printer` which both iterate `obj.Items` directly.
//!
//! Our generic [`List<T>`] uses a plain `Vec<T>`, which serde-json encodes as
//! `[]` for an empty vector and never as `null` — so the assertion below
//! mainly *pins* that property, preventing a future refactor to
//! `Option<Vec<T>>` or `#[serde(skip_serializing_if = "Vec::is_empty")]` from
//! silently breaking every client.
//!
//! Custom list types (e.g. [`EventList`], DRA list types,
//! [`MetricValueList`]) get the same coverage. If a future audit finds a list
//! type whose serialization emits `null` or omits `items`, mark the test
//! `#[ignore]` with a link to the tracking issue.

use rusternetes_common::resources::{
    Binding, CertificateSigningRequest, ClusterRole, ClusterRoleBinding, ComponentStatus,
    ConfigMap, ControllerRevision, CronJob, CustomResourceDefinition, DaemonSet, Deployment,
    DeviceClass, DeviceClassList, Endpoints, Event, EventList, FlowSchema, HorizontalPodAutoscaler,
    IPAddress, Ingress, IngressClass, Job, Lease, LimitRange, MetricValue, MetricValueList,
    Namespace, NetworkPolicy, Node, PersistentVolume, PersistentVolumeClaim, Pod,
    PodDisruptionBudget, PriorityClass, PriorityLevelConfiguration, ReplicaSet,
    ReplicationController, ResourceClaim, ResourceClaimList, ResourceClaimTemplate,
    ResourceClaimTemplateList, ResourceQuota, ResourceSlice, ResourceSliceList, Role, RoleBinding,
    RuntimeClass, Secret, Service, ServiceAccount, StatefulSet, StorageClass,
    ValidatingAdmissionPolicy, VolumeAttachment, VolumeSnapshot, VolumeSnapshotClass,
};
use rusternetes_common::types::{List, ListMeta};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Pure-serde invariant for the generic `List<T>` wrapper.
// ---------------------------------------------------------------------------

/// Core assertion: encode an empty list of `T`, parse the result back as a
/// `serde_json::Value`, and pin the shape of `items` (present, array, empty).
/// Then decode the bytes back into the typed wrapper and check item count.
///
/// This is the function the per-type tests call — keep it tiny and total.
fn assert_empty_list_invariant<T>(kind: &str, api_version: &str)
where
    T: Serialize + DeserializeOwned,
{
    let list: List<T> = List::new(kind, api_version, Vec::<T>::new());

    let encoded = serde_json::to_string(&list)
        .unwrap_or_else(|e| panic!("encoding empty List<{kind}> failed: {e}"));

    let value: Value = serde_json::from_str(&encoded)
        .unwrap_or_else(|e| panic!("re-parsing empty List<{kind}> as Value failed: {e}"));

    let obj = value
        .as_object()
        .unwrap_or_else(|| panic!("List<{kind}> did not serialize as a JSON object: {encoded}"));

    let items = obj.get("items").unwrap_or_else(|| {
        panic!("List<{kind}> serialization is missing the `items` key (must be `[]`, not absent): {encoded}")
    });

    assert!(
        items.is_array(),
        "List<{kind}>.items must serialize as a JSON array, got {items:?} (full payload: {encoded})",
    );
    assert!(
        items.as_array().unwrap().is_empty(),
        "List<{kind}>.items expected []; got {items:?}",
    );
    assert!(
        !items.is_null(),
        "List<{kind}>.items must NEVER be `null` when empty (full payload: {encoded})",
    );

    // Round-trip: decoded list keeps the contract (items.len() == 0).
    let decoded: List<T> = serde_json::from_str(&encoded)
        .unwrap_or_else(|e| panic!("decoding round-tripped List<{kind}> failed: {e}"));
    assert_eq!(
        decoded.items.len(),
        0,
        "round-tripped List<{kind}> should have items.len() == 0",
    );
}

/// Generate one `#[test]` per `(ListKind, ApiVersion, ItemType)` triple.
///
/// Each invocation expands to a function named `empty_<snake>_serializes_as_empty_array`.
macro_rules! empty_list_test {
    ($name:ident, $kind:expr, $api_version:expr, $item:ty) => {
        #[test]
        fn $name() {
            assert_empty_list_invariant::<$item>($kind, $api_version);
        }
    };
}

// Core/v1 list types (the upstream "Kubernetes core" group).
empty_list_test!(empty_pod_list, "PodList", "v1", Pod);
empty_list_test!(empty_service_list, "ServiceList", "v1", Service);
empty_list_test!(empty_namespace_list, "NamespaceList", "v1", Namespace);
empty_list_test!(empty_node_list, "NodeList", "v1", Node);
empty_list_test!(empty_configmap_list, "ConfigMapList", "v1", ConfigMap);
empty_list_test!(empty_secret_list, "SecretList", "v1", Secret);
empty_list_test!(empty_endpoints_list, "EndpointsList", "v1", Endpoints);
empty_list_test!(empty_event_core_list, "EventList", "v1", Event);
empty_list_test!(empty_limitrange_list, "LimitRangeList", "v1", LimitRange);
empty_list_test!(
    empty_resourcequota_list,
    "ResourceQuotaList",
    "v1",
    ResourceQuota
);
empty_list_test!(
    empty_serviceaccount_list,
    "ServiceAccountList",
    "v1",
    ServiceAccount
);
empty_list_test!(
    empty_persistentvolume_list,
    "PersistentVolumeList",
    "v1",
    PersistentVolume
);
empty_list_test!(
    empty_persistentvolumeclaim_list,
    "PersistentVolumeClaimList",
    "v1",
    PersistentVolumeClaim
);
empty_list_test!(
    empty_replicationcontroller_list,
    "ReplicationControllerList",
    "v1",
    ReplicationController
);
empty_list_test!(
    empty_componentstatus_list,
    "ComponentStatusList",
    "v1",
    ComponentStatus
);
empty_list_test!(empty_binding_list, "BindingList", "v1", Binding);

// apps/v1.
empty_list_test!(
    empty_deployment_list,
    "DeploymentList",
    "apps/v1",
    Deployment
);
empty_list_test!(
    empty_replicaset_list,
    "ReplicaSetList",
    "apps/v1",
    ReplicaSet
);
empty_list_test!(empty_daemonset_list, "DaemonSetList", "apps/v1", DaemonSet);
empty_list_test!(
    empty_statefulset_list,
    "StatefulSetList",
    "apps/v1",
    StatefulSet
);
empty_list_test!(
    empty_controllerrevision_list,
    "ControllerRevisionList",
    "apps/v1",
    ControllerRevision
);

// batch/v1.
empty_list_test!(empty_job_list, "JobList", "batch/v1", Job);
empty_list_test!(empty_cronjob_list, "CronJobList", "batch/v1", CronJob);

// networking.k8s.io/v1.
empty_list_test!(
    empty_ingress_list,
    "IngressList",
    "networking.k8s.io/v1",
    Ingress
);
empty_list_test!(
    empty_ingressclass_list,
    "IngressClassList",
    "networking.k8s.io/v1",
    IngressClass
);
empty_list_test!(
    empty_networkpolicy_list,
    "NetworkPolicyList",
    "networking.k8s.io/v1",
    NetworkPolicy
);
empty_list_test!(
    empty_ipaddress_list,
    "IPAddressList",
    "networking.k8s.io/v1",
    IPAddress
);

// rbac.authorization.k8s.io/v1.
empty_list_test!(
    empty_role_list,
    "RoleList",
    "rbac.authorization.k8s.io/v1",
    Role
);
empty_list_test!(
    empty_rolebinding_list,
    "RoleBindingList",
    "rbac.authorization.k8s.io/v1",
    RoleBinding
);
empty_list_test!(
    empty_clusterrole_list,
    "ClusterRoleList",
    "rbac.authorization.k8s.io/v1",
    ClusterRole
);
empty_list_test!(
    empty_clusterrolebinding_list,
    "ClusterRoleBindingList",
    "rbac.authorization.k8s.io/v1",
    ClusterRoleBinding
);

// autoscaling/v2.
empty_list_test!(
    empty_horizontalpodautoscaler_list,
    "HorizontalPodAutoscalerList",
    "autoscaling/v2",
    HorizontalPodAutoscaler
);

// policy/v1.
empty_list_test!(
    empty_poddisruptionbudget_list,
    "PodDisruptionBudgetList",
    "policy/v1",
    PodDisruptionBudget
);

// scheduling.k8s.io/v1.
empty_list_test!(
    empty_priorityclass_list,
    "PriorityClassList",
    "scheduling.k8s.io/v1",
    PriorityClass
);

// node.k8s.io/v1.
empty_list_test!(
    empty_runtimeclass_list,
    "RuntimeClassList",
    "node.k8s.io/v1",
    RuntimeClass
);

// coordination.k8s.io/v1.
empty_list_test!(
    empty_lease_list,
    "LeaseList",
    "coordination.k8s.io/v1",
    Lease
);

// flowcontrol.apiserver.k8s.io/v1.
empty_list_test!(
    empty_flowschema_list,
    "FlowSchemaList",
    "flowcontrol.apiserver.k8s.io/v1",
    FlowSchema
);
empty_list_test!(
    empty_prioritylevelconfiguration_list,
    "PriorityLevelConfigurationList",
    "flowcontrol.apiserver.k8s.io/v1",
    PriorityLevelConfiguration
);

// certificates.k8s.io/v1.
empty_list_test!(
    empty_csr_list,
    "CertificateSigningRequestList",
    "certificates.k8s.io/v1",
    CertificateSigningRequest
);

// storage.k8s.io/v1.
empty_list_test!(
    empty_storageclass_list,
    "StorageClassList",
    "storage.k8s.io/v1",
    StorageClass
);
empty_list_test!(
    empty_volumeattachment_list,
    "VolumeAttachmentList",
    "storage.k8s.io/v1",
    VolumeAttachment
);

// snapshot.storage.k8s.io/v1.
empty_list_test!(
    empty_volumesnapshot_list,
    "VolumeSnapshotList",
    "snapshot.storage.k8s.io/v1",
    VolumeSnapshot
);
empty_list_test!(
    empty_volumesnapshotclass_list,
    "VolumeSnapshotClassList",
    "snapshot.storage.k8s.io/v1",
    VolumeSnapshotClass
);

// apiextensions.k8s.io/v1.
empty_list_test!(
    empty_crd_list,
    "CustomResourceDefinitionList",
    "apiextensions.k8s.io/v1",
    CustomResourceDefinition
);

// admissionregistration.k8s.io/v1.
empty_list_test!(
    empty_validatingadmissionpolicy_list,
    "ValidatingAdmissionPolicyList",
    "admissionregistration.k8s.io/v1",
    ValidatingAdmissionPolicy
);

// ---------------------------------------------------------------------------
// Custom list types that don't go through `List<T>`.
//
// These hand-rolled wrappers must satisfy the same invariant. If any of them
// is found to emit `null` or omit `items`, mark it `#[ignore = "..."]`.
// ---------------------------------------------------------------------------

/// Encode `value`, re-parse as `Value`, and assert `items` is a present empty
/// JSON array.
fn assert_value_items_is_empty_array<T>(label: &str, value: &T)
where
    T: Serialize,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|e| panic!("encoding empty {label} failed: {e}"));
    let parsed: Value = serde_json::from_str(&encoded)
        .unwrap_or_else(|e| panic!("re-parsing empty {label} as Value failed: {e}"));
    let obj = parsed
        .as_object()
        .unwrap_or_else(|| panic!("{label} did not serialize as a JSON object: {encoded}"));
    let items = obj
        .get("items")
        .unwrap_or_else(|| panic!("{label} missing `items` key (must be `[]`): {encoded}"));
    assert!(
        items.is_array(),
        "{label}.items must serialize as a JSON array, got {items:?} (payload: {encoded})",
    );
    assert!(
        items.as_array().unwrap().is_empty(),
        "{label}.items expected []; got {items:?}",
    );
    assert!(
        !items.is_null(),
        "{label}.items must NEVER be `null` when empty (payload: {encoded})",
    );
}

#[test]
fn empty_event_list_struct() {
    let list = EventList::default();
    assert_value_items_is_empty_array("EventList", &list);

    // Round-trip back into the typed wrapper.
    let encoded = serde_json::to_string(&list).unwrap();
    let decoded: EventList = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.items.len(), 0);
}

#[test]
fn empty_resource_claim_list_struct() {
    let list = ResourceClaimList {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceClaimList".to_string(),
        metadata: None,
        items: Vec::<ResourceClaim>::new(),
    };
    assert_value_items_is_empty_array("ResourceClaimList", &list);

    let encoded = serde_json::to_string(&list).unwrap();
    let decoded: ResourceClaimList = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.items.len(), 0);
}

#[test]
fn empty_resource_claim_template_list_struct() {
    let list = ResourceClaimTemplateList {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceClaimTemplateList".to_string(),
        metadata: None,
        items: Vec::<ResourceClaimTemplate>::new(),
    };
    assert_value_items_is_empty_array("ResourceClaimTemplateList", &list);

    let encoded = serde_json::to_string(&list).unwrap();
    let decoded: ResourceClaimTemplateList = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.items.len(), 0);
}

#[test]
fn empty_device_class_list_struct() {
    let list = DeviceClassList {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "DeviceClassList".to_string(),
        metadata: None,
        items: Vec::<DeviceClass>::new(),
    };
    assert_value_items_is_empty_array("DeviceClassList", &list);

    let encoded = serde_json::to_string(&list).unwrap();
    let decoded: DeviceClassList = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.items.len(), 0);
}

#[test]
fn empty_resource_slice_list_struct() {
    let list = ResourceSliceList {
        api_version: "resource.k8s.io/v1".to_string(),
        kind: "ResourceSliceList".to_string(),
        metadata: None,
        items: Vec::<ResourceSlice>::new(),
    };
    assert_value_items_is_empty_array("ResourceSliceList", &list);

    let encoded = serde_json::to_string(&list).unwrap();
    let decoded: ResourceSliceList = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.items.len(), 0);
}

#[test]
fn empty_metric_value_list_struct() {
    let list = MetricValueList {
        api_version: "custom.metrics.k8s.io/v1beta1".to_string(),
        kind: "MetricValueList".to_string(),
        metadata: rusternetes_common::resources::ListMetadata { self_link: None },
        items: Vec::<MetricValue>::new(),
    };
    assert_value_items_is_empty_array("MetricValueList", &list);

    let encoded = serde_json::to_string(&list).unwrap();
    let decoded: MetricValueList = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.items.len(), 0);
}

// ---------------------------------------------------------------------------
// Sanity check: a `List<T>` *with* items must still serialize as a JSON array
// containing those items (we want to be sure the invariant is "always array",
// not "always empty array").
// ---------------------------------------------------------------------------

#[test]
fn populated_pod_list_serializes_as_non_empty_array() {
    let pod = Pod::new(
        "demo",
        rusternetes_common::resources::PodSpec {
            containers: vec![],
            ..Default::default()
        },
    );
    let list: List<Pod> = List::new("PodList", "v1", vec![pod]);
    let encoded = serde_json::to_string(&list).expect("encode populated PodList");
    let value: Value = serde_json::from_str(&encoded).expect("parse populated PodList");
    let items = value
        .get("items")
        .expect("populated PodList must have items key")
        .as_array()
        .expect("populated PodList.items must be array");
    assert_eq!(items.len(), 1, "populated PodList must keep the one item");
}

// ---------------------------------------------------------------------------
// Bonus: explicit `[]` literal stays `[]` after a Value -> List<T> -> Value
// round-trip. Mirrors the wire shape an api-server consumer would see.
// ---------------------------------------------------------------------------

#[test]
fn explicit_empty_array_json_roundtrips_through_list() {
    let raw = r#"{
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "1"},
        "items": []
    }"#;
    let decoded: List<Pod> = serde_json::from_str(raw).expect("decode literal items: []");
    assert!(decoded.items.is_empty());

    let re_encoded = serde_json::to_string(&decoded).unwrap();
    let value: Value = serde_json::from_str(&re_encoded).unwrap();
    let items = value
        .get("items")
        .expect("items key present after roundtrip");
    assert!(items.is_array());
    assert!(items.as_array().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Negative-control: `items: null` must NOT decode into a usable list.
//
// Pin the current behaviour: serde rejects `null` for a `Vec<T>` field. If
// this ever flips to "decode `null` as empty vec" we want to know — that's
// the failure mode the upstream Kubernetes contract is designed to prevent.
// ---------------------------------------------------------------------------

#[test]
fn null_items_is_rejected_by_decoder() {
    let raw = r#"{
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "1"},
        "items": null
    }"#;
    let result: Result<List<Pod>, _> = serde_json::from_str(raw);
    assert!(
        result.is_err(),
        "List<Pod> must reject items: null (got {result:?}); the on-the-wire shape must be an array",
    );
}

// ---------------------------------------------------------------------------
// Smoke: the ListMeta default produces a non-empty resourceVersion. Several
// clients rely on this — bare resourceVersion `""` or `"0"` causes client-go's
// reflector to bail with "initial RV '0' is not supported". The default is
// `"0"` here, but [`List::new`] bumps it to `"1"` via
// `set_resource_version_from_items`.
// ---------------------------------------------------------------------------

#[test]
fn empty_list_resource_version_is_at_least_one() {
    let list: List<Pod> = List::new("PodList", "v1", vec![]);
    let rv = list.metadata.resource_version.as_deref().unwrap_or("");
    assert!(
        rv == "1" || rv.parse::<i64>().is_ok_and(|n| n >= 1),
        "empty list must publish a usable resourceVersion, got {rv:?}",
    );
    // Belt-and-braces: ListMeta default is "0", but List::new lifts it to "1".
    assert_ne!(
        rv, "0",
        "List::new must upgrade default resourceVersion from `0` to `1`",
    );
}

/// Cheap compile-time guard that the imported types still implement what we
/// rely on. Doesn't actually execute serde, just ensures these paths are not
/// removed. Keeps the import block honest after refactors.
#[allow(dead_code)]
fn _typecheck_unused() {
    fn assert_serializable<T: Serialize + DeserializeOwned>() {}
    assert_serializable::<ListMeta>();
}
