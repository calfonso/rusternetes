# Protobuf schema coverage for Kubernetes conformance

This checklist enumerates every protobuf message a conformant Kubernetes API server (release-1.35) must decode, and flags which are registered today in the `ProtoRegistry` in `crates/api-server/src/protobuf.rs` versus which still need an entry. Client-go (used by `kubectl`, controller-runtime, hydrophone, and every controller) defaults to `Content-Type: application/vnd.kubernetes.protobuf` for writes, so any kind without a registered top-level schema is rejected with `No schema found for kind 'X'`, and any kind with a top-level schema but a missing nested-message schema decodes that nested field as `{}` and then trips the JSON-conversion step with errors like `missing field 'path'`. PR #134 fixed exactly that leaf-type case for `ProjectedVolumeSource` -> `ServiceAccountTokenProjection` -> `KeyToPath`, and the punch list below is what remains.

Scope: the 14 API groups picked for the core conformance surface, plus the shared `apimachinery/pkg/apis/meta/v1` types referenced from them.

## Summary

| API group | Registered / Total | % |
| --- | --- | --- |
| core/v1 | 209 / 209 | 100% |
| apps/v1 | 25 / 25 | 100% |
| batch/v1 | 15 / 15 | 100% |
| rbac/v1 | 8 / 8 | 100% |
| networking/v1 | 30 / 30 | 100% |
| apiextensions/v1 | 23 / 23 | 100% |
| admissionregistration/v1 | 23 / 23 | 100% |
| coordination/v1 | 2 / 2 | 100% |
| policy/v1 | 4 / 4 | 100% |
| discovery/v1 | 7 / 7 | 100% |
| autoscaling/v2 | 23 / 23 | 100% |
| scheduling/v1 | 1 / 1 | 100% |
| storage/v1 | 15 / 15 | 100% |
| apiregistration/v1 | 5 / 5 | 100% |
| apimachinery/meta/v1 | 15 / 15 | 100% |
| **Total** | **405 / 405** | **100%** |

## core/v1

### Kinds

- [x] `Binding`
- [x] `ComponentStatus`
- [x] `ConfigMap`
- [x] `Endpoints`
- [x] `Event`
- [x] `LimitRange`
- [x] `Namespace`
- [x] `Node`
- [x] `PersistentVolume`
- [x] `PersistentVolumeClaim`
- [x] `PersistentVolumeClaimTemplate`
- [x] `Pod`
- [x] `PodStatusResult`
- [x] `PodTemplate`
- [x] `PodTemplateSpec`
- [x] `RangeAllocation`
- [x] `ReplicationController`
- [x] `ResourceQuota`
- [x] `Secret`
- [x] `Service`
- [x] `ServiceAccount`

### Nested messages

- [x] `AWSElasticBlockStoreVolumeSource`
- [x] `Affinity`
- [x] `AppArmorProfile`
- [x] `AttachedVolume`
- [x] `AzureDiskVolumeSource`
- [x] `AzureFilePersistentVolumeSource`
- [x] `AzureFileVolumeSource`
- [x] `CSIPersistentVolumeSource`
- [x] `CSIVolumeSource`
- [x] `Capabilities`
- [x] `CephFSPersistentVolumeSource`
- [x] `CephFSVolumeSource`
- [x] `CinderPersistentVolumeSource`
- [x] `CinderVolumeSource`
- [x] `ClientIPConfig`
- [x] `ClusterTrustBundleProjection`
- [x] `ComponentCondition`
- [x] `ConfigMapEnvSource`
- [x] `ConfigMapKeySelector`
- [x] `ConfigMapNodeConfigSource`
- [x] `ConfigMapProjection`
- [x] `ConfigMapVolumeSource`
- [x] `Container`
- [x] `ContainerExtendedResourceRequest`
- [x] `ContainerImage`
- [x] `ContainerPort`
- [x] `ContainerResizePolicy`
- [x] `ContainerRestartRule`
- [x] `ContainerRestartRuleOnExitCodes`
- [x] `ContainerState`
- [x] `ContainerStateRunning`
- [x] `ContainerStateTerminated`
- [x] `ContainerStateWaiting`
- [x] `ContainerStatus`
- [x] `ContainerUser`
- [x] `DaemonEndpoint`
- [x] `DownwardAPIProjection`
- [x] `DownwardAPIVolumeFile`
- [x] `DownwardAPIVolumeSource`
- [x] `EmptyDirVolumeSource`
- [x] `EndpointAddress`
- [x] `EndpointPort`
- [x] `EndpointSubset`
- [x] `EnvFromSource`
- [x] `EnvVar`
- [x] `EnvVarSource`
- [x] `EphemeralContainer`
- [x] `EphemeralContainerCommon`
- [x] `EphemeralVolumeSource`
- [x] `EventSeries`
- [x] `EventSource`
- [x] `ExecAction`
- [x] `FCVolumeSource`
- [x] `FileKeySelector`
- [x] `FlexPersistentVolumeSource`
- [x] `FlexVolumeSource`
- [x] `FlockerVolumeSource`
- [x] `GCEPersistentDiskVolumeSource`
- [x] `GRPCAction`
- [x] `GitRepoVolumeSource`
- [x] `GlusterfsPersistentVolumeSource`
- [x] `GlusterfsVolumeSource`
- [x] `HTTPGetAction`
- [x] `HTTPHeader`
- [x] `HostAlias`
- [x] `HostIP`
- [x] `HostPathVolumeSource`
- [x] `ISCSIPersistentVolumeSource`
- [x] `ISCSIVolumeSource`
- [x] `ImageVolumeSource`
- [x] `KeyToPath`
- [x] `Lifecycle`
- [x] `LifecycleHandler`
- [x] `LimitRangeItem`
- [x] `LimitRangeSpec`
- [x] `LinuxContainerUser`
- [x] `LoadBalancerIngress`
- [x] `LoadBalancerStatus`
- [x] `LocalObjectReference`
- [x] `LocalVolumeSource`
- [x] `ModifyVolumeStatus`
- [x] `NFSVolumeSource`
- [x] `NamespaceCondition`
- [x] `NamespaceSpec`
- [x] `NamespaceStatus`
- [x] `NodeAddress`
- [x] `NodeAffinity`
- [x] `NodeCondition`
- [x] `NodeConfigSource`
- [x] `NodeConfigStatus`
- [x] `NodeDaemonEndpoints`
- [x] `NodeFeatures`
- [x] `NodeRuntimeHandler`
- [x] `NodeRuntimeHandlerFeatures`
- [x] `NodeSelector`
- [x] `NodeSelectorRequirement`
- [x] `NodeSelectorTerm`
- [x] `NodeSpec`
- [x] `NodeStatus`
- [x] `NodeSwapStatus`
- [x] `NodeSystemInfo`
- [x] `ObjectFieldSelector`
- [x] `ObjectReference`
- [x] `PersistentVolumeClaimCondition`
- [x] `PersistentVolumeClaimSpec`
- [x] `PersistentVolumeClaimStatus`
- [x] `PersistentVolumeClaimVolumeSource`
- [x] `PersistentVolumeSource`
- [x] `PersistentVolumeSpec`
- [x] `PersistentVolumeStatus`
- [x] `PhotonPersistentDiskVolumeSource`
- [x] `PodAffinity`
- [x] `PodAffinityTerm`
- [x] `PodAntiAffinity`
- [x] `PodCertificateProjection`
- [x] `PodCondition`
- [x] `PodDNSConfig`
- [x] `PodDNSConfigOption`
- [x] `PodExtendedResourceClaimStatus`
- [x] `PodIP`
- [x] `PodOS`
- [x] `PodReadinessGate`
- [x] `PodResourceClaim`
- [x] `PodResourceClaimStatus`
- [x] `PodSchedulingGate`
- [x] `PodSecurityContext`
- [x] `PodSpec`
- [x] `PodStatus`
- [x] `PortStatus`
- [x] `PortworxVolumeSource`
- [x] `PreferredSchedulingTerm`
- [x] `Probe`
- [x] `ProbeHandler`
- [x] `ProjectedVolumeSource`
- [x] `QuobyteVolumeSource`
- [x] `RBDPersistentVolumeSource`
- [x] `RBDVolumeSource`
- [x] `ReplicationControllerCondition`
- [x] `ReplicationControllerSpec`
- [x] `ReplicationControllerStatus`
- [x] `ResourceClaim`
- [x] `ResourceFieldSelector`
- [x] `ResourceHealth`
- [x] `ResourceQuotaSpec`
- [x] `ResourceQuotaStatus`
- [x] `ResourceRequirements`
- [x] `ResourceStatus`
- [x] `SELinuxOptions`
- [x] `ScaleIOPersistentVolumeSource`
- [x] `ScaleIOVolumeSource`
- [x] `ScopeSelector`
- [x] `ScopedResourceSelectorRequirement`
- [x] `SeccompProfile`
- [x] `SecretEnvSource`
- [x] `SecretKeySelector`
- [x] `SecretProjection`
- [x] `SecretReference`
- [x] `SecretVolumeSource`
- [x] `SecurityContext`
- [x] `ServiceAccountTokenProjection`
- [x] `ServicePort`
- [x] `ServiceSpec`
- [x] `ServiceStatus`
- [x] `SessionAffinityConfig`
- [x] `SleepAction`
- [x] `StorageOSPersistentVolumeSource`
- [x] `StorageOSVolumeSource`
- [x] `Sysctl`
- [x] `TCPSocketAction`
- [x] `Taint`
- [x] `Toleration`
- [x] `TopologySelectorLabelRequirement`
- [x] `TopologySelectorTerm`
- [x] `TopologySpreadConstraint`
- [x] `TypedLocalObjectReference`
- [x] `TypedObjectReference`
- [x] `Volume`
- [x] `VolumeDevice`
- [x] `VolumeMount`
- [x] `VolumeMountStatus`
- [x] `VolumeNodeAffinity`
- [x] `VolumeProjection`
- [x] `VolumeResourceRequirements`
- [x] `VolumeSource`
- [x] `VsphereVirtualDiskVolumeSource`
- [x] `WeightedPodAffinityTerm`
- [x] `WindowsSecurityContextOptions`
- [x] `WorkloadReference`

## apps/v1

### Kinds

- [x] `ControllerRevision`
- [x] `DaemonSet`
- [x] `Deployment`
- [x] `ReplicaSet`
- [x] `StatefulSet`

### Nested messages

- [x] `DaemonSetCondition`
- [x] `DaemonSetSpec`
- [x] `DaemonSetStatus`
- [x] `DaemonSetUpdateStrategy`
- [x] `DeploymentCondition`
- [x] `DeploymentSpec`
- [x] `DeploymentStatus`
- [x] `DeploymentStrategy`
- [x] `ReplicaSetCondition`
- [x] `ReplicaSetSpec`
- [x] `ReplicaSetStatus`
- [x] `RollingUpdateDaemonSet`
- [x] `RollingUpdateDeployment`
- [x] `RollingUpdateStatefulSetStrategy`
- [x] `StatefulSetCondition`
- [x] `StatefulSetOrdinals`
- [x] `StatefulSetPersistentVolumeClaimRetentionPolicy`
- [x] `StatefulSetSpec`
- [x] `StatefulSetStatus`
- [x] `StatefulSetUpdateStrategy`

## batch/v1

### Kinds

- [x] `CronJob`
- [x] `Job`
- [x] `JobTemplateSpec`

### Nested messages

- [x] `CronJobSpec`
- [x] `CronJobStatus`
- [x] `JobCondition`
- [x] `JobSpec`
- [x] `JobStatus`
- [x] `PodFailurePolicy`
- [x] `PodFailurePolicyOnExitCodesRequirement`
- [x] `PodFailurePolicyOnPodConditionsPattern`
- [x] `PodFailurePolicyRule`
- [x] `SuccessPolicy`
- [x] `SuccessPolicyRule`
- [x] `UncountedTerminatedPods`

## rbac/v1

### Kinds

- [x] `ClusterRole`
- [x] `ClusterRoleBinding`
- [x] `Role`
- [x] `RoleBinding`

### Nested messages

- [x] `AggregationRule`
- [x] `PolicyRule`
- [x] `RoleRef`
- [x] `Subject`

## networking/v1

### Kinds

- [x] `IPAddress`
- [x] `Ingress`
- [x] `IngressClass`
- [x] `NetworkPolicy`
- [x] `ServiceCIDR`

### Nested messages

- [x] `HTTPIngressPath`
- [x] `HTTPIngressRuleValue`
- [x] `IPAddressSpec`
- [x] `IPBlock`
- [x] `IngressBackend`
- [x] `IngressClassParametersReference`
- [x] `IngressClassSpec`
- [x] `IngressLoadBalancerIngress`
- [x] `IngressLoadBalancerStatus`
- [x] `IngressPortStatus`
- [x] `IngressRule`
- [x] `IngressRuleValue`
- [x] `IngressServiceBackend`
- [x] `IngressSpec`
- [x] `IngressStatus`
- [x] `IngressTLS`
- [x] `NetworkPolicyEgressRule`
- [x] `NetworkPolicyIngressRule`
- [x] `NetworkPolicyPeer`
- [x] `NetworkPolicyPort`
- [x] `NetworkPolicySpec`
- [x] `ParentReference`
- [x] `ServiceBackendPort`
- [x] `ServiceCIDRSpec`
- [x] `ServiceCIDRStatus`

## apiextensions/v1

### Kinds

- [x] `CustomResourceDefinition`

### Nested messages

- [x] `CustomResourceColumnDefinition`
- [x] `CustomResourceConversion`
- [x] `CustomResourceDefinitionCondition`
- [x] `CustomResourceDefinitionNames`
- [x] `CustomResourceDefinitionSpec`
- [x] `CustomResourceDefinitionStatus`
- [x] `CustomResourceDefinitionVersion`
- [x] `CustomResourceSubresourceScale`
- [x] `CustomResourceSubresourceStatus`
- [x] `CustomResourceSubresources`
- [x] `CustomResourceValidation`
- [x] `ExternalDocumentation`
- [x] `JSON`
- [x] `JSONSchemaProps`
- [x] `JSONSchemaPropsOrArray`
- [x] `JSONSchemaPropsOrBool`
- [x] `JSONSchemaPropsOrStringArray`
- [x] `SelectableField`
- [x] `ServiceReference`
- [x] `ValidationRule`
- [x] `WebhookClientConfig`
- [x] `WebhookConversion`

## admissionregistration/v1

### Kinds

- [x] `MutatingWebhookConfiguration`
- [x] `ValidatingAdmissionPolicy`
- [x] `ValidatingAdmissionPolicyBinding`
- [x] `ValidatingWebhookConfiguration`

### Nested messages

- [x] `AuditAnnotation`
- [x] `ExpressionWarning`
- [x] `MatchCondition`
- [x] `MatchResources`
- [x] `MutatingWebhook`
- [x] `NamedRuleWithOperations`
- [x] `ParamKind`
- [x] `ParamRef`
- [x] `Rule`
- [x] `RuleWithOperations`
- [x] `ServiceReference`
- [x] `TypeChecking`
- [x] `ValidatingAdmissionPolicyBindingSpec`
- [x] `ValidatingAdmissionPolicySpec`
- [x] `ValidatingAdmissionPolicyStatus`
- [x] `ValidatingWebhook`
- [x] `Validation`
- [x] `Variable`
- [x] `WebhookClientConfig`

## coordination/v1

### Kinds

- [x] `Lease`

### Nested messages

- [x] `LeaseSpec`

## policy/v1

### Kinds

- [x] `Eviction`
- [x] `PodDisruptionBudget`

### Nested messages

- [x] `PodDisruptionBudgetSpec`
- [x] `PodDisruptionBudgetStatus`

## discovery/v1

### Kinds

- [x] `EndpointSlice`

### Nested messages

- [x] `Endpoint`
- [x] `EndpointConditions`
- [x] `EndpointHints`
- [x] `EndpointPort`
- [x] `ForNode`
- [x] `ForZone`

## autoscaling/v2

### Kinds

- [x] `HorizontalPodAutoscaler`

### Nested messages

- [x] `ContainerResourceMetricSource`
- [x] `ContainerResourceMetricStatus`
- [x] `CrossVersionObjectReference`
- [x] `ExternalMetricSource`
- [x] `ExternalMetricStatus`
- [x] `HPAScalingPolicy`
- [x] `HPAScalingRules`
- [x] `HorizontalPodAutoscalerBehavior`
- [x] `HorizontalPodAutoscalerCondition`
- [x] `HorizontalPodAutoscalerSpec`
- [x] `HorizontalPodAutoscalerStatus`
- [x] `MetricIdentifier`
- [x] `MetricSpec`
- [x] `MetricStatus`
- [x] `MetricTarget`
- [x] `MetricValueStatus`
- [x] `ObjectMetricSource`
- [x] `ObjectMetricStatus`
- [x] `PodsMetricSource`
- [x] `PodsMetricStatus`
- [x] `ResourceMetricSource`
- [x] `ResourceMetricStatus`

## scheduling/v1

### Kinds

- [x] `PriorityClass`

### Nested messages

_(none referenced from this group's kinds)_

## storage/v1

### Kinds

- [x] `CSIDriver`
- [x] `CSINode`
- [x] `CSIStorageCapacity`
- [x] `StorageClass`
- [x] `VolumeAttachment`
- [x] `VolumeAttributesClass`

### Nested messages

- [x] `CSIDriverSpec`
- [x] `CSINodeDriver`
- [x] `CSINodeSpec`
- [x] `TokenRequest`
- [x] `VolumeAttachmentSource`
- [x] `VolumeAttachmentSpec`
- [x] `VolumeAttachmentStatus`
- [x] `VolumeError`
- [x] `VolumeNodeResources`

## apiregistration/v1

### Kinds

- [x] `APIService`

### Nested messages

- [x] `APIServiceCondition`
- [x] `APIServiceSpec`
- [x] `APIServiceStatus`
- [x] `ServiceReference`

## apimachinery/meta/v1

Shared types referenced from one or more kinds in the 14 API groups above. `TypeMeta` (embedded inline in the protobuf `Unknown` envelope) and `Patch` (PATCH request body) are listed even though no field declaration references them directly.

- [x] `Condition`
- [x] `DeleteOptions`
- [x] `FieldsV1`
- [x] `LabelSelector`
- [x] `LabelSelectorRequirement`
- [x] `ListMeta`
- [x] `ManagedFieldsEntry`
- [x] `MicroTime`
- [x] `ObjectMeta`
- [x] `OwnerReference`
- [x] `Patch`
- [x] `Preconditions`
- [x] `Status`
- [x] `Time`
- [x] `TypeMeta`

## Notes

- List wrappers (`PodList`, `ServiceList`, ...) are intentionally NOT tracked unless already registered; list endpoints return JSON, not protobuf. None are registered today.
- "Registered" means there is a `schemas.insert("Name".into(), ...)` entry in `crates/api-server/src/protobuf.rs`. The entry's field-number completeness is not audited here — that is a follow-up.
- Some proto messages are flattened by the Go generator (e.g. `Volume.volumeSource = 2` inlines every `*VolumeSource` field number directly into `Volume`'s wire format). The current decoder mirrors that by hand-rolling the per-source field numbers in the `Volume` schema rather than registering a separate `VolumeSource` schema. Either approach is wire-correct, but the leaf type (`HostPathVolumeSource`, `ProjectedVolumeSource`, ...) MUST still be registered for the nested message to decode. See PR #134.
- Counts in the summary table are derived by transitively walking every field reference from each group's kinds (excluding `*List` wrappers). A nested message is attributed to the API group whose `.proto` file defines it, regardless of which group's kind reaches it — so e.g. `PodSpec` is counted under `core/v1` even though `apps/v1.Deployment` is what pulls it in.
- External value types referenced from these protos but not tracked here: `apimachinery/pkg/api/resource.Quantity`, `apimachinery/pkg/util/intstr.IntOrString`, `apimachinery/pkg/runtime.RawExtension`. These are leaf scalars from the decoder's perspective (decoded by `FieldType::IntOrString` / `FieldType::JsonRaw` / similar) and do not need their own `schemas.insert(...)` entry.
