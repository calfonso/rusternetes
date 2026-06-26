use crate::resources::policy::IntOrString;
use crate::types::{ObjectMeta, Phase, ResourceRequirements, TypeMeta};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Deserialize an Option<ContainerState> that tolerates empty objects `{}`.
/// Go's Kubernetes client serializes nil ContainerState as `{}` instead of `null`,
/// which would fail to parse as the tagged enum. This treats `{}` as `None`.
fn deserialize_container_state_option<'de, D>(
    deserializer: D,
) -> Result<Option<ContainerState>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Object(ref map)) if map.is_empty() => Ok(None),
        Some(v) => serde_json::from_value(v)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// Macro to create a skip_serializing_if function for Option<T> where T has all optional fields.
/// This prevents serializing empty structs as {} when all fields are None.
macro_rules! skip_if_empty {
    ($fn_name:ident, $type:ty, $($field:ident),+) => {
        #[allow(dead_code)]
        fn $fn_name(value: &Option<$type>) -> bool {
            match value {
                None => true,
                Some(v) => {
                    $(v.$field.is_none())&&+
                }
            }
        }
    };
}

/// Pod is the smallest deployable unit in Kubernetes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pod {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    /// ObjectMeta is `+optional` upstream; a body that omits `metadata`
    /// (e.g. some PreStop e2e payloads) must still decode and let validation
    /// report the missing name, rather than failing with a raw serde
    /// `missing field 'metadata'`.
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<PodSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PodStatus>,
}

impl Pod {
    pub fn new(name: impl Into<String>, spec: PodSpec) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: Some(spec),
            status: None,
        }
    }
}

/// PodSpec describes the desired state of a pod
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodSpec {
    pub containers: Vec<Container>,

    /// Init containers run before app containers and must complete successfully
    /// Sidecar containers are init containers with restartPolicy: Always
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init_containers: Option<Vec<Container>>,

    /// Ephemeral containers are temporary containers for debugging
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral_containers: Option<Vec<EphemeralContainer>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>, // Always, OnFailure, Never

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,

    /// Deprecated: Use serviceAccountName instead
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub subdomain: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_network: Option<bool>,

    #[serde(rename = "hostPID", skip_serializing_if = "Option::is_none")]
    pub host_pid: Option<bool>,

    #[serde(rename = "hostIPC", skip_serializing_if = "Option::is_none")]
    pub host_ipc: Option<bool>,

    /// Affinity rules for pod scheduling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Affinity>,

    /// Tolerations for node taints
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tolerations: Option<Vec<Toleration>>,

    /// Priority value - higher priority pods are scheduled first
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,

    /// Priority class name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority_class_name: Option<String>,

    /// AutomountServiceAccountToken indicates whether a service account token should be automatically mounted
    #[serde(skip_serializing_if = "Option::is_none")]
    pub automount_service_account_token: Option<bool>,

    /// TopologySpreadConstraints describes how a group of pods ought to spread across topology domains
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topology_spread_constraints: Option<Vec<TopologySpreadConstraint>>,

    /// Overhead represents the resource overhead associated with running a pod
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub overhead: Option<HashMap<String, String>>,

    /// SchedulerName is the name of the scheduler to be used to schedule this pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduler_name: Option<String>,

    /// ResourceClaims defines which ResourceClaims must be allocated and reserved before the Pod is allowed to start
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claims: Option<Vec<PodResourceClaim>>,

    /// ActiveDeadlineSeconds is the max duration in seconds before the pod may be terminated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deadline_seconds: Option<i64>,

    /// DNSPolicy determines DNS policy for the pod (ClusterFirst, ClusterFirstWithHostNet, Default, None)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_policy: Option<String>,

    /// DNSConfig defines DNS parameters for the pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_config: Option<PodDNSConfig>,

    /// SecurityContext holds pod-level security attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_context: Option<PodSecurityContext>,

    /// ImagePullSecrets is a list of references to secrets for pulling images
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_secrets: Option<Vec<LocalObjectReference>>,

    /// ShareProcessNamespace indicates whether a single process namespace should be shared between all containers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share_process_namespace: Option<bool>,

    /// ReadinessGates specifies additional conditions to be evaluated for pod readiness
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_gates: Option<Vec<PodReadinessGate>>,

    /// RuntimeClassName refers to a RuntimeClass object in the node.k8s.io group
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_class_name: Option<String>,

    /// EnableServiceLinks indicates whether information about services should be injected into pod's environment variables
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_service_links: Option<bool>,

    /// PreemptionPolicy is the policy for preempting pods with lower priority (Never, PreemptLowerPriority)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preemption_policy: Option<String>,

    /// HostUsers controls whether host user namespace is used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_users: Option<bool>,

    /// SetHostnameAsFQDN determines if the pod's hostname will be configured as the pod's FQDN
    #[serde(rename = "setHostnameAsFQDN", skip_serializing_if = "Option::is_none")]
    pub set_hostname_as_fqdn: Option<bool>,

    /// HostnameOverride specifies an explicit override for the pod's hostname (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname_override: Option<String>,

    /// WorkloadRef is a reference to the workload this pod belongs to (k8s 1.35, alpha)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_ref: Option<WorkloadReference>,

    /// TerminationGracePeriodSeconds is the grace period before forcible pod termination
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_grace_period_seconds: Option<i64>,

    /// HostAliases is a list of hosts and IPs that will be injected into /etc/hosts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_aliases: Option<Vec<HostAlias>>,

    /// OS is the target operating system for this pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<PodOS>,

    /// SchedulingGates is a list of conditions that must be satisfied before the pod may be scheduled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduling_gates: Option<Vec<PodSchedulingGate>>,

    /// Resources is the total amount of CPU and Memory resources required by all containers in the pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

/// PodResourceClaim references a ResourceClaim that must be allocated for the pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodResourceClaim {
    /// Name uniquely identifies this resource claim inside the pod
    pub name: String,

    /// ResourceClaimName is the name of a ResourceClaim object in the same namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claim_name: Option<String>,

    /// ResourceClaimTemplateName is the name of a ResourceClaimTemplate object in the same namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claim_template_name: Option<String>,
}

/// HostAlias holds the mapping between IP and hostnames that will be injected into /etc/hosts
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostAlias {
    #[serde(default)]
    pub ip: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostnames: Option<Vec<String>>,
}

/// PodOS identifies the OS for a pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodOS {
    pub name: String,
}

/// PodSchedulingGate is associated to a Pod to guard its scheduling
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodSchedulingGate {
    pub name: String,
}

/// PodDNSConfig defines DNS parameters for the pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDNSConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nameservers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub searches: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<PodDNSConfigOption>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDNSConfigOption {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// PodSecurityContext holds pod-level security attributes
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodSecurityContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_user: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_group: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_non_root: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_group: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_group_change_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplemental_groups: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sysctls: Option<Vec<Sysctl>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seccomp_profile: Option<SeccompProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_armor_profile: Option<AppArmorProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub se_linux_options: Option<SELinuxOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub windows_options: Option<WindowsSecurityContextOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub se_linux_change_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplemental_groups_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sysctl {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppArmorProfile {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub localhost_profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SELinuxOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsSecurityContextOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gmsa_credential_spec_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gmsa_credential_spec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_user_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_process: Option<bool>,
}

/// LocalObjectReference is a reference to an object in the same namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalObjectReference {
    pub name: String,
}

/// PodReadinessGate specifies an additional condition to evaluate for pod readiness
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodReadinessGate {
    pub condition_type: String,
}

/// WorkloadReference references the workload a pod belongs to (k8s 1.35, alpha)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadReference {
    pub name: String,
    pub pod_group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_group_replica_key: Option<String>,
}

/// ContainerRestartRule describes how a container exit maps to a restart
/// decision (k8s 1.35).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRestartRule {
    /// Action taken when the rule matches. Currently only "Restart".
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_codes: Option<ContainerRestartRuleOnExitCodes>,
}

/// ContainerRestartRuleOnExitCodes matches a container exit by exit code.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRestartRuleOnExitCodes {
    /// Operator relating the container exit code(s) to `values` (In / NotIn).
    pub operator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<i32>>,
}

/// PodExtendedResourceClaimStatus is the status of the ResourceClaim generated
/// for a pod's extended resource requests (k8s 1.35).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodExtendedResourceClaimStatus {
    /// RequestMappings map each container extended-resource request to a
    /// DeviceRequest in the generated ResourceClaim.
    pub request_mappings: Vec<ContainerExtendedResourceRequest>,
    /// ResourceClaimName is the name of the generated ResourceClaim.
    pub resource_claim_name: String,
}

/// ContainerExtendedResourceRequest maps a container's extended-resource
/// request to a generated DeviceRequest (k8s 1.35).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerExtendedResourceRequest {
    pub container_name: String,
    pub request_name: String,
    pub resource_name: String,
}

/// PodCertificateProjection projects a short-lived X.509 certificate issued for
/// the pod into the volume (k8s 1.35).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodCertificateProjection {
    /// SignerName is the name of the signer that signs the certificate.
    pub signer_name: String,
    /// KeyType is the type of keypair the kubelet generates (e.g. RSA3072, ED25519).
    pub key_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_expiration_seconds: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_bundle_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate_chain_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_annotations: Option<std::collections::HashMap<String, String>>,
}

/// EphemeralContainer is a temporary container added to a running pod for debugging
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EphemeralContainer {
    pub name: String,
    pub image: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,

    /// TargetContainerName is the name of the container to attach to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_container_name: Option<String>,

    /// Stdin enables redirecting stdin to the ephemeral container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin: Option<bool>,

    /// StdinOnce closes stdin after the first attach
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin_once: Option<bool>,

    /// TTY allocates a pseudo-TTY for the ephemeral container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,

    /// ResizePolicy is a list of resource resize policies for containers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resize_policy: Option<Vec<ContainerResizePolicy>>,

    /// RestartPolicy defines the restart behavior of this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,

    /// Resources are the compute resource requirements for this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<crate::types::ResourceRequirements>,

    /// TerminationMessagePath is the file path for the container's termination message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_message_path: Option<String>,

    /// TerminationMessagePolicy indicates how the termination message should be populated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_message_policy: Option<String>,

    /// Ports exposed by the ephemeral container (rarely used; kept for parity)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<ContainerPort>>,

    /// EnvFrom sources to populate environment variables
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_from: Option<Vec<EnvFromSource>>,

    /// VolumeDevices is the list of block devices to be used by the container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_devices: Option<Vec<VolumeDevice>>,

    /// Lifecycle is not allowed for ephemeral containers, but present for parity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,

    /// LivenessProbe is not allowed for ephemeral containers, but present for parity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness_probe: Option<Probe>,

    /// ReadinessProbe is not allowed for ephemeral containers, but present for parity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<Probe>,

    /// StartupProbe is not allowed for ephemeral containers, but present for parity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup_probe: Option<Probe>,

    /// RestartPolicyRules are evaluated in order to determine restart behaviour (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy_rules: Option<Vec<ContainerRestartRule>>,
}

/// TopologySpreadConstraint specifies how to spread pods across topology domains
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologySpreadConstraint {
    /// MaxSkew describes the degree to which pods may be unevenly distributed
    pub max_skew: i32,

    /// TopologyKey is the key of node labels
    pub topology_key: String,

    /// WhenUnsatisfiable indicates how to deal with a pod if it doesn't satisfy the spread constraint
    /// Possible values: DoNotSchedule, ScheduleAnyway
    pub when_unsatisfiable: String,

    /// LabelSelector is used to find matching pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<crate::types::LabelSelector>,

    /// MinDomains indicates a minimum number of eligible domains
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_domains: Option<i32>,

    /// NodeAffinityPolicy indicates how we will treat Pod's nodeAffinity/nodeSelector
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_affinity_policy: Option<String>,

    /// NodeTaintsPolicy indicates how we will treat node taints
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_taints_policy: Option<String>,

    /// MatchLabelKeys is a set of pod label keys to select pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_label_keys: Option<Vec<String>>,
}

/// Container represents a single container in a pod
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Container {
    pub name: String,
    pub image: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<ContainerPort>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_from: Option<Vec<EnvFromSource>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_devices: Option<Vec<VolumeDevice>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>, // Always, Never, IfNotPresent

    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness_probe: Option<Probe>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<Probe>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup_probe: Option<Probe>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,

    /// RestartPolicy for the container. Only applies to init containers.
    /// Possible values: Always (sidecar container that runs alongside main containers)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,

    /// RestartPolicyRules are evaluated in order to determine restart behaviour (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy_rules: Option<Vec<ContainerRestartRule>>,

    /// ResizePolicy is the list of container resource resize policies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resize_policy: Option<Vec<ContainerResizePolicy>>,

    /// Lifecycle describes actions that the management system should take in response to container lifecycle events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_message_path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_message_policy: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin_once: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,
}

/// Lifecycle describes actions that management system should take in response to container lifecycle events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lifecycle {
    /// PostStart is called immediately after a container is created
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_start: Option<LifecycleHandler>,

    /// PreStop is called immediately before a container is terminated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_stop: Option<LifecycleHandler>,

    /// StopSignal defines the stop signal to send to the container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,
}

/// LifecycleHandler defines a specific action that should be taken in a lifecycle hook
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleHandler {
    /// Exec specifies the action to take
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecAction>,

    /// HTTPGet specifies the http request to perform
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_get: Option<HTTPGetAction>,

    /// TCPSocket specifies an action involving a TCP port
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_socket: Option<TCPSocketAction>,

    /// Sleep represents the duration that the container should sleep before being terminated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep: Option<SleepAction>,
}

/// SleepAction describes a "sleep" action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SleepAction {
    /// Seconds is the number of seconds to sleep
    pub seconds: i64,
}

/// ContainerResizePolicy represents resource resize policy for the container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResizePolicy {
    /// Name of the resource to which this resource resize policy applies (cpu, memory)
    pub resource_name: String,
    /// Restart policy to apply when the specified resource is resized (NotRequired or RestartContainer)
    pub restart_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub privileged: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_user: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_group: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_non_root: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only_root_filesystem: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_privilege_escalation: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub proc_mount: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub seccomp_profile: Option<SeccompProfile>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub se_linux_options: Option<SELinuxOptions>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_armor_profile: Option<AppArmorProfile>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub windows_options: Option<WindowsSecurityContextOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeccompProfile {
    pub r#type: String, // RuntimeDefault, Unconfined, Localhost

    #[serde(skip_serializing_if = "Option::is_none")]
    pub localhost_profile: Option<String>,
}

fn default_protocol() -> String {
    "TCP".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerPort {
    pub container_port: u16,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(default = "default_protocol")]
    pub protocol: String, // TCP, UDP, SCTP

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_port: Option<u16>,

    /// What host IP to bind the external port to.
    #[serde(skip_serializing_if = "Option::is_none", rename = "hostIP")]
    pub host_ip: Option<String>,
}

// Generate skip functions for structs with all-optional fields
// NOTE: Do NOT use skip_if_empty for structs with mutually exclusive fields:
// - EnvVarSource: only one of (config_map_key_ref, secret_key_ref, field_ref, resource_field_ref) should be set
// - ClaimSource: only one of (resource_claim_name, resource_claim_template_name) should be set
// Using skip_if_empty would incorrectly skip serialization when only one field is set.
skip_if_empty!(
    skip_empty_security_context,
    SecurityContext,
    privileged,
    run_as_user,
    run_as_non_root,
    allow_privilege_escalation,
    capabilities,
    seccomp_profile
);
skip_if_empty!(skip_empty_capabilities, Capabilities, add, drop);
skip_if_empty!(
    skip_empty_empty_dir_volume_source,
    EmptyDirVolumeSource,
    medium
);
skip_if_empty!(
    skip_empty_config_map_volume_source,
    ConfigMapVolumeSource,
    name,
    items,
    default_mode,
    optional
);
skip_if_empty!(
    skip_empty_secret_volume_source,
    SecretVolumeSource,
    secret_name,
    items,
    default_mode,
    optional
);
skip_if_empty!(
    skip_empty_downward_api_volume_source,
    DownwardAPIVolumeSource,
    items,
    default_mode
);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvVar {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_from: Option<EnvVarSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvVarSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map_key_ref: Option<ConfigMapKeySelector>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_key_ref: Option<SecretKeySelector>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_ref: Option<ObjectFieldSelector>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_field_ref: Option<ResourceFieldSelector>,

    /// Selects a key from a file volume source
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_key_ref: Option<FileKeySelector>,
}

/// FileKeySelector selects a key from a file volume source
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileKeySelector {
    /// The name of the volume containing the file
    pub volume_name: String,
    /// The relative path of the file to map the key to
    pub path: String,
    /// The key to select
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Specify whether the volume or its key must be defined
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// EnvFromSource represents the source of a set of ConfigMaps/Secrets to populate env vars
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvFromSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map_ref: Option<ConfigMapEnvSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretEnvSource>,
}

/// ConfigMapEnvSource selects a ConfigMap to populate the env vars with
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapEnvSource {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// SecretEnvSource selects a Secret to populate the env vars with
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretEnvSource {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// VolumeDevice describes a mapping of a raw block device within a container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeDevice {
    pub name: String,
    pub device_path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapKeySelector {
    pub name: String,
    pub key: String,

    /// Specify whether the ConfigMap or its key must be defined
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeySelector {
    pub name: String,
    pub key: String,

    /// Specify whether the Secret or its key must be defined
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path_expr: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mount_propagation: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive_read_only: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub empty_dir: Option<EmptyDirVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_path: Option<HostPathVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistent_volume_claim: Option<PersistentVolumeClaimVolumeSource>,

    /// Downward API volume source
    #[serde(
        rename = "downwardAPI",
        alias = "downwardApi",
        skip_serializing_if = "Option::is_none"
    )]
    pub downward_api: Option<DownwardAPIVolumeSource>,

    /// CSI (Container Storage Interface) ephemeral inline volume
    #[serde(skip_serializing_if = "Option::is_none")]
    pub csi: Option<crate::resources::csi::CSIVolumeSource>,

    /// Generic ephemeral volume with volumeClaimTemplate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<EphemeralVolumeSource>,

    /// NFS represents an NFS mount on the host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nfs: Option<crate::resources::volume::NFSVolumeSource>,

    /// ISCSI represents an ISCSI volume attached and mounted on the host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iscsi: Option<crate::resources::volume::ISCSIVolumeSource>,

    /// Projected defines a projected volume that maps several existing volume sources into one
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projected: Option<ProjectedVolumeSource>,

    /// Image represents an OCI object (container image or artifact) pulled and mounted on the host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<ImageVolumeSource>,
}

/// ProjectedVolumeSource represents a projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<VolumeProjection>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
}

/// VolumeProjection is a single volume source for a projected volume
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_token: Option<ServiceAccountTokenProjection>,
    #[serde(
        rename = "downwardAPI",
        alias = "downwardApi",
        skip_serializing_if = "Option::is_none"
    )]
    pub downward_api: Option<DownwardAPIProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_trust_bundle: Option<ClusterTrustBundleProjection>,
    /// PodCertificate projects a freshly-issued X.509 certificate (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_certificate: Option<PodCertificateProjection>,
}

/// SecretProjection adapts a Secret into a projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<KeyToPath>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// ConfigMapProjection adapts a ConfigMap into a projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<KeyToPath>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// ServiceAccountTokenProjection represents a projected service account token volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountTokenProjection {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_seconds: Option<i64>,
}

/// DownwardAPIProjection represents Downward API info for the projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownwardAPIProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<DownwardAPIVolumeFile>>,
}

/// ClusterTrustBundleProjection projects a ClusterTrustBundle into a projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterTrustBundleProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signer_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<crate::types::LabelSelector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
    pub path: String,
}

/// ImageVolumeSource represents an OCI object (container image or artifact)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmptyDirVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub medium: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_limit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostPathVolumeSource {
    pub path: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<KeyToPath>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none", alias = "secret_name")]
    pub secret_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<KeyToPath>>,

    #[serde(skip_serializing_if = "Option::is_none", alias = "default_mode")]
    pub default_mode: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyToPath {
    pub key: String,
    pub path: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaimVolumeSource {
    pub claim_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// DownwardAPIVolumeSource represents a downward API volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownwardAPIVolumeSource {
    /// Items is a list of downward API volume file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<DownwardAPIVolumeFile>>,

    /// Optional: mode bits to use on created files by default
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
}

/// DownwardAPIVolumeFile represents information to create the file containing the pod field
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownwardAPIVolumeFile {
    /// Required: Path is the relative path name of the file to be created
    pub path: String,

    /// Required: Selects a field of the pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_ref: Option<ObjectFieldSelector>,

    /// Selects a resource of the container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_field_ref: Option<ResourceFieldSelector>,

    /// Optional: mode bits to use on this file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<i32>,
}

/// ObjectFieldSelector selects an API object field
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectFieldSelector {
    /// Path of the field to select in the specified API version
    pub field_path: String,

    /// Version of the schema the FieldPath is written in terms of, defaults to "v1"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

/// ResourceFieldSelector selects a resource of the container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceFieldSelector {
    /// Container name: required for volumes, optional for env vars
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,

    /// Required: resource to select
    pub resource: String,

    /// Specifies the output format of the exposed resources, defaults to "1"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub divisor: Option<String>,
}

/// EphemeralVolumeSource represents an ephemeral volume with a volume claim template
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EphemeralVolumeSource {
    /// Will be used to create a stand-alone PVC to provision the volume
    pub volume_claim_template: Option<PersistentVolumeClaimTemplate>,
}

/// PersistentVolumeClaimTemplate is used to produce PVC objects
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaimTemplate {
    /// May contain labels and annotations that will be copied into the PVC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    /// The specification for the PersistentVolumeClaim
    pub spec: crate::resources::volume::PersistentVolumeClaimSpec,
}

/// HostIP represents a single host IP address
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostIP {
    #[serde(default)]
    pub ip: String,
}

/// PodIP represents a single pod IP address
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodIP {
    #[serde(default)]
    pub ip: String,
}

/// PodCondition represents a condition of a pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodCondition {
    /// Type of pod condition (e.g. Ready, ContainersReady, Initialized, PodScheduled)
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition (True, False, or Unknown)
    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Last time the condition was probed.
    /// Mirrors upstream `PodCondition.LastProbeTime` (k8s.io/api/core/v1).
    /// Optional in the API and currently not populated by the kubelet, but
    /// strict decoding requires the field to be recognised when clients
    /// echo it back on Pod updates (e.g. e2e conformance test fixtures).
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize",
        default
    )]
    pub last_probe_time: Option<chrono::DateTime<chrono::Utc>>,

    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize",
        default
    )]
    pub last_transition_time: Option<chrono::DateTime<chrono::Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// PodStatus represents the current state of a pod
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(rename = "hostIP", skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,

    /// All host IPs for dual-stack
    #[serde(rename = "hostIPs", skip_serializing_if = "Option::is_none")]
    pub host_i_ps: Option<Vec<HostIP>>,

    #[serde(rename = "podIP", skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<String>,

    /// All pod IPs for dual-stack
    #[serde(rename = "podIPs", skip_serializing_if = "Option::is_none")]
    pub pod_i_ps: Option<Vec<PodIP>>,

    /// Node nominated for preemption to make room for this pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominated_node_name: Option<String>,

    /// QOS class for the pod (Guaranteed, Burstable, or BestEffort)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos_class: Option<String>,

    /// Time at which the pod was acknowledged by the kubelet
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize",
        default
    )]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Pod-level conditions (Ready, ContainersReady, Initialized, PodScheduled)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<PodCondition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_statuses: Option<Vec<ContainerStatus>>,

    /// Status of init containers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init_container_statuses: Option<Vec<ContainerStatus>>,

    /// Status of ephemeral containers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral_container_statuses: Option<Vec<ContainerStatus>>,

    /// Status of resource resize for the pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resize: Option<String>,

    /// Total compute resources actually allocated to the pod (pod-level resize)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated_resources: Option<HashMap<String, String>>,

    /// Compute resources the kubelet enforces for the pod (pod-level resize)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<crate::types::ResourceRequirements>,

    /// Status of resource claims
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claim_statuses: Option<Vec<PodResourceClaimStatus>>,

    /// ObservedGeneration represents the .metadata.generation that the status was set based upon
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// ExtendedResourceClaimStatus is the status of the extended resource claim
    /// generated for this pod (k8s 1.35, DRA)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_resource_claim_status: Option<PodExtendedResourceClaimStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStatus {
    pub name: String,
    pub ready: bool,
    pub restart_count: u32,

    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_container_state_option"
    )]
    pub state: Option<ContainerState>,

    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_container_state_option"
    )]
    pub last_state: Option<ContainerState>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    #[serde(rename = "imageID", skip_serializing_if = "Option::is_none")]
    pub image_id: Option<String>,

    #[serde(rename = "containerID", skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,

    /// Whether the container has passed its startup probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started: Option<bool>,

    /// Resources allocated to this container by the node
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub allocated_resources: Option<HashMap<String, String>>,

    /// Detailed status of allocated resources for this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated_resources_status: Option<Vec<ResourceStatus>>,

    /// Resources represents the compute resource requests/limits of this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// User represents the user identity for this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<ContainerUser>,

    /// VolumeMounts reports status of volume mounts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMountStatus>>,

    /// StopSignal reports the effective stop signal for this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,
}

/// ResourceStatus represents the status of an individual resource allocation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStatus {
    /// Name of the resource (e.g., "cpu", "memory", or extended resource)
    pub name: String,

    /// List of individual resources tracked for this allocation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<ResourceHealth>>,
}

/// ResourceHealth represents the health of an individual resource
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceHealth {
    /// Unique identifier of the resource (e.g., device ID)
    #[serde(rename = "resourceID")]
    pub resource_id: String,

    /// Health status of the resource (Healthy, Unhealthy, Unknown)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ContainerState {
    Waiting {
        reason: Option<String>,
        message: Option<String>,
    },
    Running {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::types::k8s_time_str::serialize",
            deserialize_with = "crate::types::k8s_time_str::deserialize"
        )]
        started_at: Option<String>,
    },
    Terminated {
        exit_code: i32,
        signal: Option<i32>,
        reason: Option<String>,
        message: Option<String>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::types::k8s_time_str::serialize",
            deserialize_with = "crate::types::k8s_time_str::deserialize"
        )]
        started_at: Option<String>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::types::k8s_time_str::serialize",
            deserialize_with = "crate::types::k8s_time_str::deserialize"
        )]
        finished_at: Option<String>,
        container_id: Option<String>,
    },
}

/// Affinity is a group of affinity scheduling rules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Affinity {
    /// Node affinity scheduling rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<NodeAffinity>,

    /// Pod affinity scheduling rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_affinity: Option<PodAffinity>,

    /// Pod anti-affinity scheduling rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_anti_affinity: Option<PodAntiAffinity>,
}

/// Node affinity is a group of node affinity scheduling rules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeAffinity {
    /// Hard node affinity requirements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_during_scheduling_ignored_during_execution: Option<NodeSelector>,

    /// Soft node affinity preferences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_during_scheduling_ignored_during_execution: Option<Vec<PreferredSchedulingTerm>>,
}

/// A node selector represents the union of the results of one or more label queries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelector {
    /// A list of node selector terms (ORed together)
    pub node_selector_terms: Vec<NodeSelectorTerm>,
}

/// A node selector term is associated with the corresponding weight
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelectorTerm {
    /// A list of node selector requirements by node's labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<NodeSelectorRequirement>>,

    /// A list of node selector requirements by node's fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_fields: Option<Vec<NodeSelectorRequirement>>,
}

/// A node selector requirement is a selector that contains values, a key, and an operator
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelectorRequirement {
    /// The label key
    pub key: String,

    /// Operator: In, NotIn, Exists, DoesNotExist, Gt, Lt
    pub operator: String,

    /// An array of string values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// An empty preferred scheduling term matches all objects with implicit weight 0
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PreferredSchedulingTerm {
    /// Weight associated with matching the corresponding nodeSelectorTerm, in the range 1-100
    pub weight: i32,

    /// A node selector term, associated with the corresponding weight
    pub preference: NodeSelectorTerm,
}

/// Pod affinity is a group of inter pod affinity scheduling rules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodAffinity {
    /// Hard pod affinity requirements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_during_scheduling_ignored_during_execution: Option<Vec<PodAffinityTerm>>,

    /// Soft pod affinity preferences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_during_scheduling_ignored_during_execution: Option<Vec<WeightedPodAffinityTerm>>,
}

/// Pod anti-affinity is a group of inter pod anti affinity scheduling rules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodAntiAffinity {
    /// Hard pod anti-affinity requirements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_during_scheduling_ignored_during_execution: Option<Vec<PodAffinityTerm>>,

    /// Soft pod anti-affinity preferences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_during_scheduling_ignored_during_execution: Option<Vec<WeightedPodAffinityTerm>>,
}

/// Defines a set of pods that should be co-located
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodAffinityTerm {
    /// A label selector over a set of resources
    pub label_selector: crate::types::LabelSelector,

    /// Namespaces specifies which namespaces the labelSelector applies to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<Vec<String>>,

    /// Topology key for pod placement
    pub topology_key: String,

    /// A label query over the set of namespaces that the term applies to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<crate::types::LabelSelector>,

    /// Keys of pod labels merged into the labelSelector to select against
    /// pods that have the same values for these keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_label_keys: Option<Vec<String>>,

    /// Keys of pod labels merged into the labelSelector to select against
    /// pods that have different values for these keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mismatch_label_keys: Option<Vec<String>>,
}

/// The weights of all the matched WeightedPodAffinityTerm fields are added per-node to find the most preferred node(s)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WeightedPodAffinityTerm {
    /// Weight associated with matching the corresponding podAffinityTerm, in the range 1-100
    pub weight: i32,

    /// Required pod affinity term
    pub pod_affinity_term: PodAffinityTerm,
}

/// The pod this Toleration is attached to tolerates any taint that matches the triple using the matching operator
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    /// Key is the taint key that the toleration applies to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// Operator represents a key's relationship to the value: Equal or Exists
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,

    /// Value is the taint value the toleration matches to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// Effect indicates the taint effect to match: NoSchedule, PreferNoSchedule, NoExecute
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,

    /// TolerationSeconds represents the period of time the toleration tolerates the taint
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

/// Probe describes a health check to be performed against a container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Probe {
    /// HTTP GET probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_get: Option<HTTPGetAction>,

    /// TCP socket probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_socket: Option<TCPSocketAction>,

    /// Exec command probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecAction>,

    /// Number of seconds after the container has started before probes are initiated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_delay_seconds: Option<i32>,

    /// Number of seconds after which the probe times out
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    /// How often (in seconds) to perform the probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_seconds: Option<i32>,

    /// Minimum consecutive successes for the probe to be considered successful
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success_threshold: Option<i32>,

    /// Minimum consecutive failures for the probe to be considered failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_threshold: Option<i32>,

    /// GRPC probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc: Option<GRPCAction>,

    /// Optional duration in seconds the pod needs to terminate gracefully upon probe failure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_grace_period_seconds: Option<i64>,
}

/// GRPCAction describes an action based on GRPC health check
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GRPCAction {
    /// Port number of the gRPC service.
    /// K8s API: IntOrString — int port or named port from container.ports[].name.
    pub port: IntOrString,

    /// Service is the name of the service to place in the gRPC HealthCheckRequest
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
}

/// Resolve a probe port (which K8s spec'd as IntOrString) against a Container's
/// declared ports. Integer values pass through; string values are looked up by
/// `containerPort.name`. Returns None if a named port has no matching entry —
/// callers should treat that as a probe failure.
pub fn resolve_probe_port(port: &IntOrString, container: &Container) -> Option<u16> {
    match port {
        IntOrString::Int(n) => {
            if *n < 0 || *n > u16::MAX as i32 {
                None
            } else {
                Some(*n as u16)
            }
        }
        IntOrString::String(name) => container
            .ports
            .as_ref()
            .and_then(|ports| {
                ports
                    .iter()
                    .find(|p| p.name.as_deref() == Some(name.as_str()))
                    .map(|p| p.container_port)
            })
            .or_else(|| name.parse::<u16>().ok()),
    }
}

/// HTTPGetAction describes an action based on HTTP Get requests
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HTTPGetAction {
    /// Path to access on the HTTP server
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Port to access on the container.
    /// K8s API: IntOrString — int port or named port from container.ports[].name.
    pub port: IntOrString,

    /// Host name to connect to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    /// Scheme to use for connecting (HTTP or HTTPS)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,

    /// Custom headers to set in the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_headers: Option<Vec<HTTPHeader>>,
}

/// HTTPHeader describes a custom header to be used in HTTP probes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HTTPHeader {
    pub name: String,
    pub value: String,
}

/// TCPSocketAction describes an action based on opening a socket
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TCPSocketAction {
    /// Port to connect to on the container.
    /// K8s API: IntOrString — int port or named port from container.ports[].name.
    pub port: IntOrString,

    /// Host name to connect to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

/// ExecAction describes a command-based action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecAction {
    /// Command to execute
    pub command: Vec<String>,
}

/// ContainerUser represents user identity information for a container
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContainerUser {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linux: Option<LinuxContainerUser>,
}

/// LinuxContainerUser represents Linux-specific user identity
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LinuxContainerUser {
    pub uid: i64,
    pub gid: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplemental_groups: Option<Vec<i64>>,
}

/// VolumeMountStatus shows status of a VolumeMount
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMountStatus {
    pub name: String,
    pub mount_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive_read_only: Option<String>,
}

/// PodResourceClaimStatus describes the status of a resource claim in the pod
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodResourceClaimStatus {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claim_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pod condition timestamps MUST serialize at whole-second precision, like
    /// upstream `metav1.Time.MarshalJSON`. Sub-second precision diverges from
    /// the protobuf/typed-client representation and breaks the
    /// `[sig-node] ... resize pod via the replace endpoint` conformance test,
    /// whose `Semantic.DeepEqual` compares the `/resize` (JSON) read against the
    /// main (typed) read of the same pod (#477).
    #[test]
    fn test_pod_condition_timestamp_serializes_at_second_precision() {
        let cond = PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(
                "2026-06-05T07:21:39.569909935Z"
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap(),
            ),
            observed_generation: None,
        };
        let json = serde_json::to_string(&cond).unwrap();
        assert!(
            json.contains("\"lastTransitionTime\":\"2026-06-05T07:21:39Z\""),
            "condition timestamp must serialize at second precision: {json}"
        );
        assert!(
            !json.contains(".569909935"),
            "must not emit sub-second precision: {json}"
        );
    }

    #[test]
    fn test_pod_with_pvc_volume_serialization() {
        let json = r#"{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "name": "test-pod",
    "namespace": "default"
  },
  "spec": {
    "containers": [
      {
        "name": "test-container",
        "image": "nginx:latest",
        "volumeMounts": [
          {
            "name": "test-volume",
            "mountPath": "/data"
          }
        ]
      }
    ],
    "volumes": [
      {
        "name": "test-volume",
        "persistentVolumeClaim": {
          "claimName": "test-pvc"
        }
      }
    ]
  }
}"#;

        // Test deserialization
        let pod: Pod = serde_json::from_str(json).expect("Failed to deserialize Pod");

        assert_eq!(pod.metadata.name, "test-pod");
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.containers.len(), 1);
        assert_eq!(spec.containers[0].name, "test-container");

        // Check volumes
        assert!(spec.volumes.is_some(), "volumes should be Some");
        let volumes = spec.volumes.as_ref().unwrap();
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].name, "test-volume");
        assert!(
            volumes[0].persistent_volume_claim.is_some(),
            "persistent_volume_claim should be Some"
        );
        assert_eq!(
            volumes[0]
                .persistent_volume_claim
                .as_ref()
                .unwrap()
                .claim_name,
            "test-pvc"
        );

        // Check volume mounts
        assert!(
            spec.containers[0].volume_mounts.is_some(),
            "volume_mounts should be Some"
        );
        let mounts = spec.containers[0].volume_mounts.as_ref().unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].name, "test-volume");
        assert_eq!(mounts[0].mount_path, "/data");

        // Test serialization
        let serialized = serde_json::to_string_pretty(&pod).expect("Failed to serialize Pod");

        // Verify round-trip
        let pod2: Pod =
            serde_json::from_str(&serialized).expect("Failed to deserialize serialized Pod");
        let spec2 = pod2.spec.as_ref().unwrap();
        assert!(spec2.volumes.is_some());
        assert_eq!(spec2.volumes.as_ref().unwrap()[0].name, "test-volume");
    }

    #[test]
    fn test_pod_with_init_containers() {
        let spec = PodSpec {
            init_containers: Some(vec![
                Container {
                    name: "init-myservice".to_string(),
                    image: "busybox:1.28".to_string(),
                    command: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "echo initializing".to_string(),
                    ]),
                    args: None,
                    working_dir: None,
                    ports: None,
                    env: None,
                    resources: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                    resize_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                },
                Container {
                    name: "init-mydb".to_string(),
                    image: "busybox:1.28".to_string(),
                    command: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "echo waiting for db".to_string(),
                    ]),
                    args: None,
                    working_dir: None,
                    ports: None,
                    env: None,
                    resources: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                    resize_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                },
            ]),
            containers: vec![Container {
                name: "app".to_string(),
                image: "nginx:latest".to_string(),
                command: None,
                args: None,
                working_dir: None,
                ports: Some(vec![ContainerPort {
                    container_port: 80,
                    name: Some("http".to_string()),
                    protocol: "TCP".to_string(),
                    host_port: None,
                    host_ip: None,
                }]),
                env: None,
                resources: None,
                volume_mounts: None,
                image_pull_policy: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                security_context: None,
                restart_policy: None,
                resize_policy: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            ephemeral_containers: None,
            volumes: None,
            restart_policy: Some("Always".to_string()),
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority: None,
            priority_class_name: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        };

        let pod = Pod::new("myapp-pod", spec);

        assert_eq!(pod.metadata.name, "myapp-pod");
        let pod_spec = pod.spec.as_ref().unwrap();

        // Verify init containers
        assert!(pod_spec.init_containers.is_some());
        let init_containers = pod_spec.init_containers.as_ref().unwrap();
        assert_eq!(init_containers.len(), 2);
        assert_eq!(init_containers[0].name, "init-myservice");
        assert_eq!(init_containers[1].name, "init-mydb");

        // Verify main containers
        assert_eq!(pod_spec.containers.len(), 1);
        assert_eq!(pod_spec.containers[0].name, "app");
    }

    #[test]
    fn test_pod_with_init_containers_serialization() {
        let json = r#"{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "name": "myapp-pod",
    "namespace": "default"
  },
  "spec": {
    "initContainers": [
      {
        "name": "init-myservice",
        "image": "busybox:1.28",
        "command": ["sh", "-c", "until nslookup myservice; do echo waiting for myservice; sleep 2; done;"]
      },
      {
        "name": "init-mydb",
        "image": "busybox:1.28",
        "command": ["sh", "-c", "until nslookup mydb; do echo waiting for mydb; sleep 2; done;"]
      }
    ],
    "containers": [
      {
        "name": "myapp-container",
        "image": "nginx:latest",
        "ports": [
          {
            "containerPort": 80
          }
        ]
      }
    ]
  }
}"#;

        let pod: Pod = serde_json::from_str(json).unwrap();
        assert_eq!(pod.metadata.name, "myapp-pod");

        let spec = pod.spec.as_ref().unwrap();
        assert!(spec.init_containers.is_some());

        let init_containers = spec.init_containers.as_ref().unwrap();
        assert_eq!(init_containers.len(), 2);
        assert_eq!(init_containers[0].name, "init-myservice");
        assert_eq!(init_containers[0].image, "busybox:1.28");
        assert_eq!(init_containers[1].name, "init-mydb");

        assert_eq!(spec.containers.len(), 1);
        assert_eq!(spec.containers[0].name, "myapp-container");

        // Test round-trip serialization
        let serialized = serde_json::to_string(&pod).unwrap();
        let pod2: Pod = serde_json::from_str(&serialized).unwrap();
        assert!(pod2.spec.as_ref().unwrap().init_containers.is_some());
        assert_eq!(
            pod2.spec
                .as_ref()
                .unwrap()
                .init_containers
                .as_ref()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn test_pod_status_with_init_container_statuses() {
        let status = PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("192.168.1.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.0.0.1".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: Some(vec![ContainerStatus {
                name: "app".to_string(),
                ready: true,
                restart_count: 0,
                state: Some(ContainerState::Running {
                    started_at: Some("2024-01-01T00:00:00Z".to_string()),
                }),
                last_state: None,
                image: Some("nginx:latest".to_string()),
                image_id: None,
                container_id: Some("containerd://abc123".to_string()),
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            }]),
            init_container_statuses: Some(vec![
                ContainerStatus {
                    name: "init-myservice".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: Some(ContainerState::Terminated {
                        exit_code: 0,
                        reason: Some("Completed".to_string()),
                        signal: None,
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                    }),
                    last_state: None,
                    image: Some("busybox:1.28".to_string()),
                    image_id: None,
                    container_id: Some("containerd://def456".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                },
                ContainerStatus {
                    name: "init-mydb".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: Some(ContainerState::Terminated {
                        exit_code: 0,
                        reason: Some("Completed".to_string()),
                        signal: None,
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                    }),
                    last_state: None,
                    image: Some("busybox:1.28".to_string()),
                    image_id: None,
                    container_id: Some("containerd://ghi789".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                },
            ]),
            ephemeral_container_statuses: None,
            conditions: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        };

        assert_eq!(status.phase, Some(Phase::Running));
        assert!(status.init_container_statuses.is_some());

        let init_statuses = status.init_container_statuses.as_ref().unwrap();
        assert_eq!(init_statuses.len(), 2);
        assert_eq!(init_statuses[0].name, "init-myservice");
        assert_eq!(init_statuses[1].name, "init-mydb");

        // Verify both init containers completed successfully
        for init_status in init_statuses {
            if let Some(ContainerState::Terminated { exit_code, .. }) = &init_status.state {
                assert_eq!(*exit_code, 0);
            }
        }
    }

    #[test]
    fn test_env_var_with_field_ref_serialization() {
        // Test deserialization of env var with fieldRef
        let json = r#"{"name":"NODE_NAME","valueFrom":{"fieldRef":{"fieldPath":"spec.nodeName"}}}"#;
        let env_var: EnvVar = serde_json::from_str(json).expect("Failed to deserialize EnvVar");

        assert_eq!(env_var.name, "NODE_NAME");
        assert!(env_var.value.is_none());
        assert!(env_var.value_from.is_some());

        let value_from = env_var.value_from.as_ref().unwrap();
        assert!(value_from.field_ref.is_some());
        assert_eq!(
            value_from.field_ref.as_ref().unwrap().field_path,
            "spec.nodeName"
        );

        // Test serialization - should preserve the fieldRef
        let serialized = serde_json::to_string(&env_var).expect("Failed to serialize EnvVar");
        println!("Serialized: {}", serialized);

        // Verify valueFrom is in the serialized output
        assert!(serialized.contains("valueFrom"));
        assert!(serialized.contains("fieldRef"));
        assert!(serialized.contains("spec.nodeName"));

        // Test round-trip
        let env_var2: EnvVar =
            serde_json::from_str(&serialized).expect("Failed to deserialize serialized EnvVar");
        assert!(env_var2.value_from.is_some());
        assert!(env_var2.value_from.as_ref().unwrap().field_ref.is_some());
    }

    #[test]
    fn test_env_var_with_field_ref_via_value() {
        // Test the round-trip through serde_json::Value (what the API server does)
        let json = r#"{"name":"NODE_NAME","valueFrom":{"fieldRef":{"fieldPath":"spec.nodeName"}}}"#;
        let env_var: EnvVar = serde_json::from_str(json).expect("Failed to deserialize EnvVar");

        // Convert to Value and back (simulating webhook flow)
        let value = serde_json::to_value(&env_var).expect("Failed to convert to Value");
        println!(
            "As Value: {}",
            serde_json::to_string_pretty(&value).unwrap()
        );

        let env_var2: EnvVar = serde_json::from_value(value).expect("Failed to convert from Value");

        // Verify fieldRef is still there after round-trip through Value
        assert!(
            env_var2.value_from.is_some(),
            "valueFrom should be Some after Value round-trip"
        );
        let value_from = env_var2.value_from.as_ref().unwrap();
        assert!(
            value_from.field_ref.is_some(),
            "fieldRef should be Some after Value round-trip"
        );
        assert_eq!(
            value_from.field_ref.as_ref().unwrap().field_path,
            "spec.nodeName"
        );
    }

    #[test]
    fn test_pod_spec_clone_serialization() {
        use super::*;

        // Create a PodSpec with env vars that have valueFrom.fieldRef
        let original_spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "busybox".to_string(),
                env: Some(vec![EnvVar {
                    name: "NODE_NAME".to_string(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        field_ref: Some(ObjectFieldSelector {
                            field_path: "spec.nodeName".to_string(),
                            api_version: None,
                        }),
                        config_map_key_ref: None,
                        secret_key_ref: None,
                        resource_field_ref: None,
                        file_key_ref: None,
                    }),
                }]),
                command: None,
                args: None,
                working_dir: None,
                ports: None,
                resources: None,
                volume_mounts: None,
                image_pull_policy: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                security_context: None,
                restart_policy: None,
                resize_policy: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            init_containers: None,
            ephemeral_containers: None,
            volumes: None,
            restart_policy: None,
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority: None,
            priority_class_name: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        };

        // Clone it (like DaemonSet controller does)
        let cloned_spec = original_spec.clone();

        // Serialize and deserialize (like etcd storage does)
        let json = serde_json::to_string(&cloned_spec).unwrap();
        println!("Serialized JSON:\n{}", json);

        let deserialized: PodSpec = serde_json::from_str(&json).unwrap();

        // Check if valueFrom.fieldRef is preserved
        let env_var = &deserialized.containers[0].env.as_ref().unwrap()[0];
        assert_eq!(env_var.name, "NODE_NAME");
        assert!(env_var.value_from.is_some(), "value_from should be Some");
        assert!(
            env_var.value_from.as_ref().unwrap().field_ref.is_some(),
            "field_ref should be Some"
        );
        assert_eq!(
            env_var
                .value_from
                .as_ref()
                .unwrap()
                .field_ref
                .as_ref()
                .unwrap()
                .field_path,
            "spec.nodeName"
        );
    }

    #[test]
    fn test_pod_resource_claim_serialization() {
        // Test that PodResourceClaim with resourceClaimName serializes correctly
        let resource_claim = PodResourceClaim {
            name: "test-claim".to_string(),
            resource_claim_name: Some("my-resource-claim".to_string()),
            resource_claim_template_name: None,
        };

        let json =
            serde_json::to_string(&resource_claim).expect("Failed to serialize PodResourceClaim");
        println!("Serialized PodResourceClaim: {}", json);

        assert!(
            json.contains("resourceClaimName"),
            "Should contain resourceClaimName field"
        );
        assert!(
            json.contains("my-resource-claim"),
            "Should contain the claim name"
        );

        // Test round-trip
        let deserialized: PodResourceClaim =
            serde_json::from_str(&json).expect("Failed to deserialize PodResourceClaim");
        assert_eq!(deserialized.name, "test-claim");
        assert_eq!(
            deserialized.resource_claim_name,
            Some("my-resource-claim".to_string())
        );
        assert_eq!(deserialized.resource_claim_template_name, None);

        // Test with resourceClaimTemplateName instead
        let resource_claim2 = PodResourceClaim {
            name: "template-claim".to_string(),
            resource_claim_name: None,
            resource_claim_template_name: Some("my-template".to_string()),
        };

        let json2 = serde_json::to_string(&resource_claim2)
            .expect("Failed to serialize PodResourceClaim with template");
        assert!(
            json2.contains("resourceClaimTemplateName"),
            "Should contain resourceClaimTemplateName field"
        );
        assert!(
            json2.contains("my-template"),
            "Should contain the template name"
        );
    }

    #[test]
    fn test_downward_api_volume_roundtrip() {
        let json = r#"{
            "name": "podinfo",
            "downwardAPI": {
                "items": [
                    {
                        "path": "labels",
                        "fieldRef": {
                            "fieldPath": "metadata.labels"
                        },
                        "mode": 256
                    }
                ]
            }
        }"#;

        let vol: Volume = serde_json::from_str(json).expect("Failed to deserialize Volume");
        assert_eq!(vol.name, "podinfo");
        assert!(
            vol.downward_api.is_some(),
            "downward_api should be Some but was None"
        );
        let da = vol.downward_api.as_ref().unwrap();
        assert!(da.items.is_some(), "items should be Some");
        let items = da.items.as_ref().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].path, "labels");
        assert_eq!(items[0].mode, Some(256));

        // Verify serialization preserves the field name as "downwardAPI"
        let serialized = serde_json::to_value(&vol).expect("Failed to serialize");
        assert!(
            serialized.get("downwardAPI").is_some(),
            "Serialized JSON should have 'downwardAPI' key, got: {}",
            serde_json::to_string_pretty(&serialized).unwrap()
        );

        // Verify round-trip through serde_json::Value
        let vol2: Volume = serde_json::from_value(serialized).expect("Failed round-trip");
        assert!(
            vol2.downward_api.is_some(),
            "downward_api should survive round-trip"
        );
    }

    #[test]
    fn test_pod_resize_status_serialization() {
        // Pod with resize="Proposed" should serialize/deserialize correctly
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "resize-pod", "namespace": "default" },
            "spec": {
                "containers": [{
                    "name": "app",
                    "image": "nginx:latest",
                    "resources": {
                        "requests": { "cpu": "100m", "memory": "128Mi" },
                        "limits": { "cpu": "200m", "memory": "256Mi" }
                    },
                    "resizePolicy": [
                        { "resourceName": "cpu", "restartPolicy": "NotRequired" },
                        { "resourceName": "memory", "restartPolicy": "RestartContainer" }
                    ]
                }]
            },
            "status": {
                "phase": "Running",
                "resize": "Proposed",
                "containerStatuses": [{
                    "name": "app",
                    "ready": true,
                    "restartCount": 0,
                    "state": { "running": { "startedAt": "2024-01-01T00:00:00Z" } },
                    "allocatedResources": {
                        "cpu": "100m",
                        "memory": "128Mi"
                    },
                    "resources": {
                        "requests": { "cpu": "100m", "memory": "128Mi" },
                        "limits": { "cpu": "200m", "memory": "256Mi" }
                    }
                }]
            }
        }"#;

        let pod: Pod =
            serde_json::from_str(json).expect("Failed to deserialize pod with resize fields");

        // Verify resize status field
        let status = pod.status.as_ref().unwrap();
        assert_eq!(status.resize.as_deref(), Some("Proposed"));

        // Verify allocatedResources in container status
        let cs = &status.container_statuses.as_ref().unwrap()[0];
        let alloc = cs.allocated_resources.as_ref().unwrap();
        assert_eq!(alloc.get("cpu"), Some(&"100m".to_string()));
        assert_eq!(alloc.get("memory"), Some(&"128Mi".to_string()));

        // Verify resources in container status
        let res = cs.resources.as_ref().unwrap();
        assert_eq!(
            res.requests.as_ref().unwrap().get("cpu"),
            Some(&"100m".to_string())
        );

        // Verify resizePolicy in spec container
        let spec = pod.spec.as_ref().unwrap();
        let resize_policy = spec.containers[0].resize_policy.as_ref().unwrap();
        assert_eq!(resize_policy.len(), 2);
        assert_eq!(resize_policy[0].resource_name, "cpu");
        assert_eq!(resize_policy[0].restart_policy, "NotRequired");
        assert_eq!(resize_policy[1].resource_name, "memory");
        assert_eq!(resize_policy[1].restart_policy, "RestartContainer");
    }

    #[test]
    fn test_pod_resize_status_roundtrip() {
        use std::collections::HashMap;

        let mut alloc = HashMap::new();
        alloc.insert("cpu".to_string(), "250m".to_string());
        alloc.insert("memory".to_string(), "512Mi".to_string());

        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("resize-roundtrip").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "nginx".to_string(),
                    resize_policy: Some(vec![ContainerResizePolicy {
                        resource_name: "cpu".to_string(),
                        restart_policy: "NotRequired".to_string(),
                    }]),
                    resources: Some(crate::types::ResourceRequirements {
                        requests: Some({
                            let mut m = HashMap::new();
                            m.insert("cpu".to_string(), "500m".to_string());
                            m
                        }),
                        limits: Some({
                            let mut m = HashMap::new();
                            m.insert("cpu".to_string(), "1".to_string());
                            m
                        }),
                        claims: None,
                    }),
                    command: None,
                    args: None,
                    working_dir: None,
                    ports: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                }],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: None,
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                message: None,
                reason: None,
                host_ip: Some("10.0.0.1".to_string()),
                host_i_ps: None,
                pod_ip: Some("10.244.0.5".to_string()),
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: Some(vec![ContainerStatus {
                    name: "app".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: Some(ContainerState::Running {
                        started_at: Some("2024-01-01T00:00:00Z".to_string()),
                    }),
                    last_state: None,
                    image: Some("nginx".to_string()),
                    image_id: None,
                    container_id: Some("docker://abc123".to_string()),
                    started: Some(true),
                    allocated_resources: Some(alloc.clone()),
                    allocated_resources_status: None,
                    resources: Some(crate::types::ResourceRequirements {
                        requests: Some({
                            let mut m = HashMap::new();
                            m.insert("cpu".to_string(), "250m".to_string());
                            m
                        }),
                        limits: None,
                        claims: None,
                    }),
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: Some("InProgress".to_string()),
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        };

        // Serialize and deserialize
        let json_str = serde_json::to_string(&pod).unwrap();
        let deserialized: Pod = serde_json::from_str(&json_str).unwrap();

        // Check resize status survived round-trip
        let status = deserialized.status.as_ref().unwrap();
        assert_eq!(status.resize.as_deref(), Some("InProgress"));

        // Check allocatedResources survived round-trip
        let cs = &status.container_statuses.as_ref().unwrap()[0];
        let ar = cs.allocated_resources.as_ref().unwrap();
        assert_eq!(ar.get("cpu"), Some(&"250m".to_string()));
        assert_eq!(ar.get("memory"), Some(&"512Mi".to_string()));

        // Check resizePolicy survived round-trip
        let spec = deserialized.spec.as_ref().unwrap();
        let rp = spec.containers[0].resize_policy.as_ref().unwrap();
        assert_eq!(rp[0].resource_name, "cpu");
        assert_eq!(rp[0].restart_policy, "NotRequired");

        // Check JSON field names are camelCase
        let json_val = serde_json::to_value(&pod).unwrap();
        let status_val = &json_val["status"];
        assert!(status_val.get("resize").is_some());
        let cs_val = &status_val["containerStatuses"][0];
        assert!(cs_val.get("allocatedResources").is_some());

        let spec_val = &json_val["spec"]["containers"][0];
        assert!(spec_val.get("resizePolicy").is_some());
    }

    #[test]
    fn test_pod_resize_empty_string_means_complete() {
        // When resize is empty string, it means resize is complete
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "resize-done", "namespace": "default" },
            "spec": {
                "containers": [{ "name": "app", "image": "nginx" }]
            },
            "status": {
                "phase": "Running",
                "resize": ""
            }
        }"#;

        let pod: Pod = serde_json::from_str(json).unwrap();
        let status = pod.status.as_ref().unwrap();
        assert_eq!(status.resize.as_deref(), Some(""));
    }

    #[test]
    fn test_pod_resize_omitted_when_none() {
        // When resize is None, it should not appear in serialized JSON
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("no-resize").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "nginx".to_string(),
                    command: None,
                    args: None,
                    working_dir: None,
                    ports: None,
                    resources: None,
                    volume_mounts: None,
                    image_pull_policy: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    security_context: None,
                    restart_policy: None,
                    resize_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                }],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                restart_policy: None,
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        };

        let json = serde_json::to_value(&pod).unwrap();
        let status_val = &json["status"];
        // resize should be omitted when None (skip_serializing_if)
        assert!(
            status_val.get("resize").is_none(),
            "resize should be omitted from JSON when None"
        );
    }

    #[test]
    fn probe_port_accepts_named_string() {
        // K8s spec: probe.httpGet.port is IntOrString — must accept a named
        // port that refers to container.ports[].name (e.g. "http").
        // Previously typed as i32, which rejected named-port YAML at parse
        // time and broke any pod using named-port probes.
        let json = r#"{"path":"/health","port":"http","scheme":"HTTP"}"#;
        let parsed: HTTPGetAction = serde_json::from_str(json).expect("named port must parse");
        assert_eq!(parsed.port, IntOrString::String("http".to_string()));

        let json2 = r#"{"port":8080}"#;
        let parsed2: HTTPGetAction = serde_json::from_str(json2).expect("int port must parse");
        assert_eq!(parsed2.port, IntOrString::Int(8080));
    }

    #[test]
    fn resolve_probe_port_int_passes_through() {
        let c = Container::default();
        assert_eq!(resolve_probe_port(&IntOrString::Int(80), &c), Some(80));
        assert_eq!(resolve_probe_port(&IntOrString::Int(-1), &c), None);
        assert_eq!(
            resolve_probe_port(&IntOrString::Int(70_000), &c),
            None,
            "ports outside u16 range must not resolve"
        );
    }

    #[test]
    fn resolve_probe_port_named_via_container_ports() {
        let mut c = Container {
            name: "web".to_string(),
            ..Default::default()
        };
        c.ports = Some(vec![
            ContainerPort {
                container_port: 8080,
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                host_port: None,
                host_ip: None,
            },
            ContainerPort {
                container_port: 8443,
                name: Some("https".to_string()),
                protocol: "TCP".to_string(),
                host_port: None,
                host_ip: None,
            },
        ]);
        assert_eq!(
            resolve_probe_port(&IntOrString::String("http".to_string()), &c),
            Some(8080)
        );
        assert_eq!(
            resolve_probe_port(&IntOrString::String("https".to_string()), &c),
            Some(8443)
        );
        // Unknown named port: falls back to parsing the string as a number,
        // returning None when neither lookup nor parse succeeds.
        assert_eq!(
            resolve_probe_port(&IntOrString::String("missing".to_string()), &c),
            None
        );
        // A numeric string is parsed as a fallback (matches K8s relaxed behavior).
        assert_eq!(
            resolve_probe_port(&IntOrString::String("9000".to_string()), &c),
            Some(9000)
        );
    }

    /// Regression test: upstream `PodCondition.LastProbeTime` must round-trip
    /// through serde so strict-decoding Pod-status updates don't reject the
    /// field with `unknown field "status.conditions[N].lastProbeTime"`.
    #[test]
    fn pod_condition_round_trips_last_probe_time() {
        let json = r#"{
            "type": "Ready",
            "status": "True",
            "lastProbeTime": "2026-05-27T10:15:30Z",
            "lastTransitionTime": "2026-05-27T10:15:35Z"
        }"#;

        let cond: PodCondition =
            serde_json::from_str(json).expect("PodCondition must accept lastProbeTime");
        assert_eq!(cond.condition_type, "Ready");
        assert_eq!(cond.status, "True");
        assert!(
            cond.last_probe_time.is_some(),
            "lastProbeTime must deserialize into Some"
        );
        assert!(cond.last_transition_time.is_some());

        let serialized =
            serde_json::to_value(&cond).expect("PodCondition must serialize back to JSON");
        assert_eq!(
            serialized.get("lastProbeTime").and_then(|v| v.as_str()),
            Some("2026-05-27T10:15:30Z"),
            "lastProbeTime must round-trip with the upstream camelCase name"
        );

        // Absent lastProbeTime must remain absent on the wire (skip_serializing_if).
        let minimal: PodCondition = serde_json::from_str(
            r#"{"type":"Ready","status":"True","lastTransitionTime":"2026-05-27T10:15:35Z"}"#,
        )
        .expect("PodCondition without lastProbeTime must still parse");
        assert!(minimal.last_probe_time.is_none());
        let serialized = serde_json::to_value(&minimal).unwrap();
        assert!(
            serialized.get("lastProbeTime").is_none(),
            "absent lastProbeTime must not be serialized"
        );
    }
}
