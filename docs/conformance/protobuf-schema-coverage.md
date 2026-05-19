# Protobuf schema coverage for Kubernetes conformance

This checklist enumerates every protobuf message a conformant Kubernetes API server (release-1.35) must decode, and flags which are registered today in the `ProtoRegistry` in `crates/api-server/src/protobuf.rs` versus which still need an entry. Client-go (used by `kubectl`, controller-runtime, hydrophone, and every controller) defaults to `Content-Type: application/vnd.kubernetes.protobuf` for writes, so any kind without a registered top-level schema is rejected with `No schema found for kind 'X'`, and any kind with a top-level schema but a missing nested-message schema decodes that nested field as `{}` and then trips the JSON-conversion step with errors like `missing field 'path'`. PR #134 fixed exactly that leaf-type case for `ProjectedVolumeSource` -> `ServiceAccountTokenProjection` -> `KeyToPath`, and the punch list below is what remains.

Scope: the 14 API groups picked for the core conformance surface, plus the shared `apimachinery/pkg/apis/meta/v1` types referenced from them.

## Summary

| API group | Registered / Total | % |
| --- | --- | --- |
| core/v1 | 67 / 209 | 32% |
| apps/v1 | 24 / 25 | 96% |
| batch/v1 | 5 / 15 | 33% |
| rbac/v1 | 0 / 8 | 0% |
| networking/v1 | 0 / 30 | 0% |
| apiextensions/v1 | 18 / 23 | 78% |
| admissionregistration/v1 | 0 / 23 | 0% |
| coordination/v1 | 0 / 2 | 0% |
| policy/v1 | 0 / 4 | 0% |
| discovery/v1 | 7 / 7 | 100% |
| autoscaling/v2 | 0 / 23 | 0% |
| scheduling/v1 | 1 / 1 | 100% |
| storage/v1 | 0 / 15 | 0% |
| apiregistration/v1 | 0 / 5 | 0% |
| apimachinery/meta/v1 | 8 / 15 | 53% |
| **Total** | **130 / 405** | **32%** |

## core/v1

### Kinds

- [ ] `Binding`
- [ ] `ComponentStatus`
- [x] `ConfigMap`
- [x] `Endpoints`
- [ ] `Event`
- [ ] `LimitRange`
- [x] `Namespace`
- [x] `Node`
- [ ] `PersistentVolume`
- [x] `PersistentVolumeClaim`
- [ ] `PersistentVolumeClaimTemplate`
- [x] `Pod`
- [ ] `PodStatusResult`
- [ ] `PodTemplate`
- [x] `PodTemplateSpec`
- [ ] `RangeAllocation`
- [x] `ReplicationController`
- [ ] `ResourceQuota`
- [x] `Secret`
- [x] `Service`
- [x] `ServiceAccount`

### Nested messages

- [ ] `AWSElasticBlockStoreVolumeSource`
- [x] `Affinity`
- [x] `AppArmorProfile`
- [ ] `AttachedVolume`
- [ ] `AzureDiskVolumeSource`
- [ ] `AzureFilePersistentVolumeSource`
- [ ] `AzureFileVolumeSource`
- [ ] `CSIPersistentVolumeSource`
- [ ] `CSIVolumeSource`
- [x] `Capabilities`
- [ ] `CephFSPersistentVolumeSource`
- [ ] `CephFSVolumeSource`
- [ ] `CinderPersistentVolumeSource`
- [ ] `CinderVolumeSource`
- [ ] `ClientIPConfig`
- [ ] `ClusterTrustBundleProjection`
- [ ] `ComponentCondition`
- [ ] `ConfigMapEnvSource`
- [x] `ConfigMapKeySelector`
- [ ] `ConfigMapNodeConfigSource`
- [ ] `ConfigMapProjection`
- [ ] `ConfigMapVolumeSource`
- [x] `Container`
- [ ] `ContainerExtendedResourceRequest`
- [ ] `ContainerImage`
- [x] `ContainerPort`
- [ ] `ContainerResizePolicy`
- [ ] `ContainerRestartRule`
- [ ] `ContainerRestartRuleOnExitCodes`
- [ ] `ContainerState`
- [ ] `ContainerStateRunning`
- [ ] `ContainerStateTerminated`
- [ ] `ContainerStateWaiting`
- [ ] `ContainerStatus`
- [ ] `ContainerUser`
- [ ] `DaemonEndpoint`
- [ ] `DownwardAPIProjection`
- [ ] `DownwardAPIVolumeFile`
- [ ] `DownwardAPIVolumeSource`
- [ ] `EmptyDirVolumeSource`
- [ ] `EndpointAddress`
- [ ] `EndpointPort`
- [x] `EndpointSubset`
- [ ] `EnvFromSource`
- [x] `EnvVar`
- [x] `EnvVarSource`
- [ ] `EphemeralContainer`
- [ ] `EphemeralContainerCommon`
- [ ] `EphemeralVolumeSource`
- [ ] `EventSeries`
- [ ] `EventSource`
- [x] `ExecAction`
- [ ] `FCVolumeSource`
- [ ] `FileKeySelector`
- [ ] `FlexPersistentVolumeSource`
- [ ] `FlexVolumeSource`
- [ ] `FlockerVolumeSource`
- [ ] `GCEPersistentDiskVolumeSource`
- [x] `GRPCAction`
- [ ] `GitRepoVolumeSource`
- [ ] `GlusterfsPersistentVolumeSource`
- [ ] `GlusterfsVolumeSource`
- [x] `HTTPGetAction`
- [x] `HTTPHeader`
- [ ] `HostAlias`
- [ ] `HostIP`
- [ ] `HostPathVolumeSource`
- [ ] `ISCSIPersistentVolumeSource`
- [ ] `ISCSIVolumeSource`
- [ ] `ImageVolumeSource`
- [ ] `KeyToPath`
- [x] `Lifecycle`
- [x] `LifecycleHandler`
- [ ] `LimitRangeItem`
- [ ] `LimitRangeSpec`
- [ ] `LinuxContainerUser`
- [ ] `LoadBalancerIngress`
- [ ] `LoadBalancerStatus`
- [x] `LocalObjectReference`
- [ ] `LocalVolumeSource`
- [ ] `ModifyVolumeStatus`
- [ ] `NFSVolumeSource`
- [x] `NamespaceCondition`
- [x] `NamespaceSpec`
- [x] `NamespaceStatus`
- [ ] `NodeAddress`
- [x] `NodeAffinity`
- [ ] `NodeCondition`
- [ ] `NodeConfigSource`
- [ ] `NodeConfigStatus`
- [ ] `NodeDaemonEndpoints`
- [ ] `NodeFeatures`
- [ ] `NodeRuntimeHandler`
- [ ] `NodeRuntimeHandlerFeatures`
- [ ] `NodeSelector`
- [ ] `NodeSelectorRequirement`
- [ ] `NodeSelectorTerm`
- [x] `NodeSpec`
- [x] `NodeStatus`
- [ ] `NodeSwapStatus`
- [ ] `NodeSystemInfo`
- [x] `ObjectFieldSelector`
- [x] `ObjectReference`
- [ ] `PersistentVolumeClaimCondition`
- [x] `PersistentVolumeClaimSpec`
- [x] `PersistentVolumeClaimStatus`
- [ ] `PersistentVolumeClaimVolumeSource`
- [ ] `PersistentVolumeSource`
- [ ] `PersistentVolumeSpec`
- [ ] `PersistentVolumeStatus`
- [ ] `PhotonPersistentDiskVolumeSource`
- [x] `PodAffinity`
- [ ] `PodAffinityTerm`
- [x] `PodAntiAffinity`
- [ ] `PodCertificateProjection`
- [ ] `PodCondition`
- [x] `PodDNSConfig`
- [x] `PodDNSConfigOption`
- [ ] `PodExtendedResourceClaimStatus`
- [ ] `PodIP`
- [ ] `PodOS`
- [ ] `PodReadinessGate`
- [ ] `PodResourceClaim`
- [ ] `PodResourceClaimStatus`
- [ ] `PodSchedulingGate`
- [x] `PodSecurityContext`
- [x] `PodSpec`
- [x] `PodStatus`
- [ ] `PortStatus`
- [ ] `PortworxVolumeSource`
- [ ] `PreferredSchedulingTerm`
- [x] `Probe`
- [x] `ProbeHandler`
- [ ] `ProjectedVolumeSource`
- [ ] `QuobyteVolumeSource`
- [ ] `RBDPersistentVolumeSource`
- [ ] `RBDVolumeSource`
- [ ] `ReplicationControllerCondition`
- [x] `ReplicationControllerSpec`
- [x] `ReplicationControllerStatus`
- [ ] `ResourceClaim`
- [x] `ResourceFieldSelector`
- [ ] `ResourceHealth`
- [ ] `ResourceQuotaSpec`
- [ ] `ResourceQuotaStatus`
- [x] `ResourceRequirements`
- [ ] `ResourceStatus`
- [x] `SELinuxOptions`
- [ ] `ScaleIOPersistentVolumeSource`
- [ ] `ScaleIOVolumeSource`
- [ ] `ScopeSelector`
- [ ] `ScopedResourceSelectorRequirement`
- [x] `SeccompProfile`
- [ ] `SecretEnvSource`
- [x] `SecretKeySelector`
- [ ] `SecretProjection`
- [ ] `SecretReference`
- [ ] `SecretVolumeSource`
- [x] `SecurityContext`
- [ ] `ServiceAccountTokenProjection`
- [x] `ServicePort`
- [x] `ServiceSpec`
- [x] `ServiceStatus`
- [x] `SessionAffinityConfig`
- [x] `SleepAction`
- [ ] `StorageOSPersistentVolumeSource`
- [ ] `StorageOSVolumeSource`
- [ ] `Sysctl`
- [x] `TCPSocketAction`
- [ ] `Taint`
- [x] `Toleration`
- [ ] `TopologySelectorLabelRequirement`
- [ ] `TopologySelectorTerm`
- [x] `TopologySpreadConstraint`
- [x] `TypedLocalObjectReference`
- [x] `TypedObjectReference`
- [x] `Volume`
- [ ] `VolumeDevice`
- [x] `VolumeMount`
- [ ] `VolumeMountStatus`
- [ ] `VolumeNodeAffinity`
- [ ] `VolumeProjection`
- [x] `VolumeResourceRequirements`
- [ ] `VolumeSource`
- [ ] `VsphereVirtualDiskVolumeSource`
- [ ] `WeightedPodAffinityTerm`
- [ ] `WindowsSecurityContextOptions`
- [ ] `WorkloadReference`

## apps/v1

### Kinds

- [ ] `ControllerRevision`
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

- [ ] `CronJob`
- [x] `Job`
- [ ] `JobTemplateSpec`

### Nested messages

- [ ] `CronJobSpec`
- [ ] `CronJobStatus`
- [ ] `JobCondition`
- [x] `JobSpec`
- [x] `JobStatus`
- [x] `PodFailurePolicy`
- [ ] `PodFailurePolicyOnExitCodesRequirement`
- [ ] `PodFailurePolicyOnPodConditionsPattern`
- [ ] `PodFailurePolicyRule`
- [x] `SuccessPolicy`
- [ ] `SuccessPolicyRule`
- [ ] `UncountedTerminatedPods`

## rbac/v1

### Kinds

- [ ] `ClusterRole`
- [ ] `ClusterRoleBinding`
- [ ] `Role`
- [ ] `RoleBinding`

### Nested messages

- [ ] `AggregationRule`
- [ ] `PolicyRule`
- [ ] `RoleRef`
- [ ] `Subject`

## networking/v1

### Kinds

- [ ] `IPAddress`
- [ ] `Ingress`
- [ ] `IngressClass`
- [ ] `NetworkPolicy`
- [ ] `ServiceCIDR`

### Nested messages

- [ ] `HTTPIngressPath`
- [ ] `HTTPIngressRuleValue`
- [ ] `IPAddressSpec`
- [ ] `IPBlock`
- [ ] `IngressBackend`
- [ ] `IngressClassParametersReference`
- [ ] `IngressClassSpec`
- [ ] `IngressLoadBalancerIngress`
- [ ] `IngressLoadBalancerStatus`
- [ ] `IngressPortStatus`
- [ ] `IngressRule`
- [ ] `IngressRuleValue`
- [ ] `IngressServiceBackend`
- [ ] `IngressSpec`
- [ ] `IngressStatus`
- [ ] `IngressTLS`
- [ ] `NetworkPolicyEgressRule`
- [ ] `NetworkPolicyIngressRule`
- [ ] `NetworkPolicyPeer`
- [ ] `NetworkPolicyPort`
- [ ] `NetworkPolicySpec`
- [ ] `ParentReference`
- [ ] `ServiceBackendPort`
- [ ] `ServiceCIDRSpec`
- [ ] `ServiceCIDRStatus`

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
- [ ] `ExternalDocumentation`
- [ ] `JSON`
- [x] `JSONSchemaProps`
- [x] `JSONSchemaPropsOrArray`
- [x] `JSONSchemaPropsOrBool`
- [ ] `JSONSchemaPropsOrStringArray`
- [x] `SelectableField`
- [ ] `ServiceReference`
- [x] `ValidationRule`
- [ ] `WebhookClientConfig`
- [x] `WebhookConversion`

## admissionregistration/v1

### Kinds

- [ ] `MutatingWebhookConfiguration`
- [ ] `ValidatingAdmissionPolicy`
- [ ] `ValidatingAdmissionPolicyBinding`
- [ ] `ValidatingWebhookConfiguration`

### Nested messages

- [ ] `AuditAnnotation`
- [ ] `ExpressionWarning`
- [ ] `MatchCondition`
- [ ] `MatchResources`
- [ ] `MutatingWebhook`
- [ ] `NamedRuleWithOperations`
- [ ] `ParamKind`
- [ ] `ParamRef`
- [ ] `Rule`
- [ ] `RuleWithOperations`
- [ ] `ServiceReference`
- [ ] `TypeChecking`
- [ ] `ValidatingAdmissionPolicyBindingSpec`
- [ ] `ValidatingAdmissionPolicySpec`
- [ ] `ValidatingAdmissionPolicyStatus`
- [ ] `ValidatingWebhook`
- [ ] `Validation`
- [ ] `Variable`
- [ ] `WebhookClientConfig`

## coordination/v1

### Kinds

- [ ] `Lease`

### Nested messages

- [ ] `LeaseSpec`

## policy/v1

### Kinds

- [ ] `Eviction`
- [ ] `PodDisruptionBudget`

### Nested messages

- [ ] `PodDisruptionBudgetSpec`
- [ ] `PodDisruptionBudgetStatus`

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

- [ ] `HorizontalPodAutoscaler`

### Nested messages

- [ ] `ContainerResourceMetricSource`
- [ ] `ContainerResourceMetricStatus`
- [ ] `CrossVersionObjectReference`
- [ ] `ExternalMetricSource`
- [ ] `ExternalMetricStatus`
- [ ] `HPAScalingPolicy`
- [ ] `HPAScalingRules`
- [ ] `HorizontalPodAutoscalerBehavior`
- [ ] `HorizontalPodAutoscalerCondition`
- [ ] `HorizontalPodAutoscalerSpec`
- [ ] `HorizontalPodAutoscalerStatus`
- [ ] `MetricIdentifier`
- [ ] `MetricSpec`
- [ ] `MetricStatus`
- [ ] `MetricTarget`
- [ ] `MetricValueStatus`
- [ ] `ObjectMetricSource`
- [ ] `ObjectMetricStatus`
- [ ] `PodsMetricSource`
- [ ] `PodsMetricStatus`
- [ ] `ResourceMetricSource`
- [ ] `ResourceMetricStatus`

## scheduling/v1

### Kinds

- [x] `PriorityClass`

### Nested messages

_(none referenced from this group's kinds)_

## storage/v1

### Kinds

- [ ] `CSIDriver`
- [ ] `CSINode`
- [ ] `CSIStorageCapacity`
- [ ] `StorageClass`
- [ ] `VolumeAttachment`
- [ ] `VolumeAttributesClass`

### Nested messages

- [ ] `CSIDriverSpec`
- [ ] `CSINodeDriver`
- [ ] `CSINodeSpec`
- [ ] `TokenRequest`
- [ ] `VolumeAttachmentSource`
- [ ] `VolumeAttachmentSpec`
- [ ] `VolumeAttachmentStatus`
- [ ] `VolumeError`
- [ ] `VolumeNodeResources`

## apiregistration/v1

### Kinds

- [ ] `APIService`

### Nested messages

- [ ] `APIServiceCondition`
- [ ] `APIServiceSpec`
- [ ] `APIServiceStatus`
- [ ] `ServiceReference`

## apimachinery/meta/v1

Shared types referenced from one or more kinds in the 14 API groups above. `TypeMeta` (embedded inline in the protobuf `Unknown` envelope) and `Patch` (PATCH request body) are listed even though no field declaration references them directly.

- [ ] `Condition`
- [x] `DeleteOptions`
- [ ] `FieldsV1`
- [x] `LabelSelector`
- [x] `LabelSelectorRequirement`
- [ ] `ListMeta`
- [x] `ManagedFieldsEntry`
- [ ] `MicroTime`
- [x] `ObjectMeta`
- [x] `OwnerReference`
- [ ] `Patch`
- [x] `Preconditions`
- [ ] `Status`
- [x] `Time`
- [ ] `TypeMeta`

## Notes

- List wrappers (`PodList`, `ServiceList`, ...) are intentionally NOT tracked unless already registered; list endpoints return JSON, not protobuf. None are registered today.
- "Registered" means there is a `schemas.insert("Name".into(), ...)` entry in `crates/api-server/src/protobuf.rs`. The entry's field-number completeness is not audited here — that is a follow-up.
- Some proto messages are flattened by the Go generator (e.g. `Volume.volumeSource = 2` inlines every `*VolumeSource` field number directly into `Volume`'s wire format). The current decoder mirrors that by hand-rolling the per-source field numbers in the `Volume` schema rather than registering a separate `VolumeSource` schema. Either approach is wire-correct, but the leaf type (`HostPathVolumeSource`, `ProjectedVolumeSource`, ...) MUST still be registered for the nested message to decode. See PR #134.
- Counts in the summary table are derived by transitively walking every field reference from each group's kinds (excluding `*List` wrappers). A nested message is attributed to the API group whose `.proto` file defines it, regardless of which group's kind reaches it — so e.g. `PodSpec` is counted under `core/v1` even though `apps/v1.Deployment` is what pulls it in.
- External value types referenced from these protos but not tracked here: `apimachinery/pkg/api/resource.Quantity`, `apimachinery/pkg/util/intstr.IntOrString`, `apimachinery/pkg/runtime.RawExtension`. These are leaf scalars from the decoder's perspective (decoded by `FieldType::IntOrString` / `FieldType::JsonRaw` / similar) and do not need their own `schemas.insert(...)` entry.
