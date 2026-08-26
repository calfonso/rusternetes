use clap::Subcommand;

#[derive(Subcommand)]
pub enum CreateCommands {
    /// Create a ClusterRole
    #[command(name = "clusterrole")]
    ClusterRole {
        /// Name of the ClusterRole
        name: String,
        /// Verbs that apply to the resources (e.g., get,list,watch)
        #[arg(long, value_delimiter = ',')]
        verb: Vec<String>,
        /// Resources the rule applies to (e.g., pods, deployments.apps)
        #[arg(long, value_delimiter = ',')]
        resource: Vec<String>,
        /// Resource names to include in the rule
        #[arg(long)]
        resource_name: Vec<String>,
        /// Non-resource URLs to include in the rule
        #[arg(long)]
        non_resource_url: Vec<String>,
        /// Aggregation rule label selectors (key=value)
        #[arg(long)]
        aggregation_rule: Vec<String>,
    },
    /// Create a ClusterRoleBinding
    #[command(name = "clusterrolebinding")]
    ClusterRoleBinding {
        /// Name of the ClusterRoleBinding
        name: String,
        /// ClusterRole to reference
        #[arg(long)]
        clusterrole: String,
        /// Users to bind
        #[arg(long)]
        user: Vec<String>,
        /// Groups to bind
        #[arg(long)]
        group: Vec<String>,
        /// Service accounts to bind (namespace:name)
        #[arg(long)]
        serviceaccount: Vec<String>,
    },
    /// Create a ConfigMap
    #[command(name = "configmap", alias = "cm")]
    ConfigMap {
        /// Name of the ConfigMap
        name: String,
        /// Literal key=value pairs
        #[arg(long)]
        from_literal: Vec<String>,
        /// Files to read (key=filepath or filepath)
        #[arg(long)]
        from_file: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a CronJob
    #[command(name = "cronjob", alias = "cj")]
    CronJob {
        /// Name of the CronJob
        name: String,
        /// Image name to run
        #[arg(long)]
        image: String,
        /// Cron schedule (e.g., "*/1 * * * *")
        #[arg(long)]
        schedule: String,
        /// Restart policy (OnFailure, Never)
        #[arg(long, default_value = "OnFailure")]
        restart: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Command to run
        #[arg(last = true)]
        command: Vec<String>,
    },
    /// Create an Ingress
    #[command(name = "ingress", alias = "ing")]
    Ingress {
        /// Name of the Ingress
        name: String,
        /// Ingress class
        #[arg(long = "class")]
        ingress_class: Option<String>,
        /// Rules in format host/path=service:port[,tls[=secret]]
        #[arg(long)]
        rule: Vec<String>,
        /// Default backend (service:port)
        #[arg(long)]
        default_backend: Option<String>,
        /// Annotations (key=value)
        #[arg(long)]
        annotation: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a Job
    #[command(name = "job")]
    Job {
        /// Name of the Job
        name: String,
        /// Image name to run
        #[arg(long)]
        image: Option<String>,
        /// Create from a CronJob (cronjob/name)
        #[arg(long)]
        from: Option<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Command to run
        #[arg(last = true)]
        command: Vec<String>,
    },
    /// Create a PodDisruptionBudget
    #[command(name = "poddisruptionbudget", alias = "pdb")]
    Pdb {
        /// Name of the PDB
        name: String,
        /// Label selector
        #[arg(long)]
        selector: String,
        /// Minimum available pods (number or percentage)
        #[arg(long)]
        min_available: Option<String>,
        /// Maximum unavailable pods (number or percentage)
        #[arg(long)]
        max_unavailable: Option<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a Role
    #[command(name = "role")]
    Role {
        /// Name of the Role
        name: String,
        /// Verbs that apply to the resources
        #[arg(long, value_delimiter = ',')]
        verb: Vec<String>,
        /// Resources the rule applies to
        #[arg(long, value_delimiter = ',')]
        resource: Vec<String>,
        /// Resource names to include
        #[arg(long)]
        resource_name: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a RoleBinding
    #[command(name = "rolebinding")]
    RoleBinding {
        /// Name of the RoleBinding
        name: String,
        /// ClusterRole to reference
        #[arg(long)]
        clusterrole: Option<String>,
        /// Role to reference
        #[arg(long)]
        role: Option<String>,
        /// Users to bind
        #[arg(long)]
        user: Vec<String>,
        /// Groups to bind
        #[arg(long)]
        group: Vec<String>,
        /// Service accounts to bind (namespace:name)
        #[arg(long)]
        serviceaccount: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a Secret
    #[command(name = "secret")]
    Secret {
        #[command(subcommand)]
        subcommand: SecretCommands,
    },
    /// Create a Namespace
    #[command(name = "namespace", alias = "ns")]
    Namespace {
        /// Name of the Namespace
        name: String,
    },
    /// Create a Deployment
    #[command(name = "deployment")]
    Deployment {
        /// Name of the Deployment
        name: String,
        /// Container image
        #[arg(long)]
        image: String,
        /// Number of replicas
        #[arg(long, short = 'r', default_value = "1")]
        replicas: i32,
        /// Container port to expose
        #[arg(long)]
        port: Option<i32>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a Service
    #[command(name = "service", alias = "svc")]
    Service {
        #[command(subcommand)]
        subcommand: ServiceCommands,
    },
    /// Create a PriorityClass
    #[command(name = "priorityclass", alias = "pc")]
    PriorityClass {
        /// Name of the PriorityClass
        name: String,
        /// Priority value
        #[arg(long)]
        value: i32,
        /// Whether this is the global default
        #[arg(long)]
        global_default: bool,
        /// Preemption policy (PreemptLowerPriority or Never)
        #[arg(long, default_value = "PreemptLowerPriority")]
        preemption_policy: String,
        /// Description of the PriorityClass
        #[arg(long)]
        description: Option<String>,
    },
    /// Create a ResourceQuota
    #[command(name = "quota", alias = "resourcequota")]
    Quota {
        /// Name of the ResourceQuota
        name: String,
        /// Hard resource limits (e.g., pods=10,services=5)
        #[arg(long)]
        hard: Option<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a ServiceAccount
    #[command(name = "serviceaccount", alias = "sa")]
    ServiceAccount {
        /// Name of the ServiceAccount
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Request a service account token
    #[command(name = "token")]
    Token {
        /// Name of the ServiceAccount
        name: String,
        /// Audiences for the token
        #[arg(long)]
        audience: Vec<String>,
        /// Token lifetime (e.g., 10m, 1h)
        #[arg(long)]
        duration: Option<String>,
        /// Kind of object to bind the token to (Pod, Secret, Node)
        #[arg(long)]
        bound_object_kind: Option<String>,
        /// Name of object to bind the token to
        #[arg(long)]
        bound_object_name: Option<String>,
        /// UID of object to bind the token to
        #[arg(long)]
        bound_object_uid: Option<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum SecretCommands {
    /// Create a generic secret
    #[command(name = "generic")]
    Generic {
        /// Name of the Secret
        name: String,
        /// Literal key=value pairs
        #[arg(long)]
        from_literal: Vec<String>,
        /// Files to read (key=filepath or filepath)
        #[arg(long)]
        from_file: Vec<String>,
        /// Secret type
        #[arg(long = "type")]
        secret_type: Option<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a docker-registry secret
    #[command(name = "docker-registry")]
    DockerRegistry {
        /// Name of the Secret
        name: String,
        /// Docker registry server
        #[arg(long, default_value = "https://index.docker.io/v1/")]
        docker_server: String,
        /// Docker registry username
        #[arg(long)]
        docker_username: Option<String>,
        /// Docker registry password
        #[arg(long)]
        docker_password: Option<String>,
        /// Docker registry email
        #[arg(long)]
        docker_email: Option<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a TLS secret
    #[command(name = "tls")]
    Tls {
        /// Name of the Secret
        name: String,
        /// Path to PEM encoded public key certificate
        #[arg(long)]
        cert: String,
        /// Path to private key
        #[arg(long)]
        key: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ServiceCommands {
    /// Create a ClusterIP service
    #[command(name = "clusterip")]
    ClusterIP {
        /// Name of the Service
        name: String,
        /// Port mappings (port:targetPort)
        #[arg(long)]
        tcp: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a NodePort service
    #[command(name = "nodeport")]
    NodePort {
        /// Name of the Service
        name: String,
        /// Port mappings (port:targetPort)
        #[arg(long)]
        tcp: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create a LoadBalancer service
    #[command(name = "loadbalancer")]
    LoadBalancer {
        /// Name of the Service
        name: String,
        /// Port mappings (port:targetPort)
        #[arg(long)]
        tcp: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Create an ExternalName service
    #[command(name = "externalname")]
    ExternalName {
        /// Name of the Service
        name: String,
        /// External name (DNS name)
        #[arg(long)]
        external_name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum KubercCommands {
    /// Set kuberc configuration values
    Set {
        /// Property path to set
        property: Option<String>,
        /// Value to set
        value: Option<String>,
    },
    /// View the current kuberc configuration
    View {},
}

#[derive(Subcommand)]
pub enum CertificateCommands {
    /// Approve a certificate signing request
    Approve {
        /// CSR names to approve
        csr_names: Vec<String>,
        /// Update the CSR even if it is already approved
        #[arg(long)]
        force: bool,
    },
    /// Deny a certificate signing request
    Deny {
        /// CSR names to deny
        csr_names: Vec<String>,
        /// Update the CSR even if it is already denied
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
pub enum RolloutCommands {
    /// Show rollout status
    Status {
        /// Resource type
        resource_type: String,
        /// Resource name
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// View rollout history
    History {
        /// Resource type
        resource_type: String,
        /// Resource name
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Show details of a specific revision
        #[arg(long)]
        revision: Option<i32>,
    },
    /// Rollback to a previous revision
    Undo {
        /// Resource type
        resource_type: String,
        /// Resource name
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Revision to rollback to
        #[arg(long)]
        to_revision: Option<i32>,
    },
    /// Restart a resource
    Restart {
        /// Resource type
        resource_type: String,
        /// Resource name
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Pause a resource rollout
    Pause {
        /// Resource type
        resource_type: String,
        /// Resource name
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Resume a paused resource
    Resume {
        /// Resource type
        resource_type: String,
        /// Resource name
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum TopCommands {
    /// Display resource usage of nodes
    Node {
        /// Node name (optional)
        name: Option<String>,
        /// Label selector
        #[arg(short = 'l', long)]
        selector: Option<String>,
    },
    /// Display resource usage of pods
    Pod {
        /// Pod name (optional)
        name: Option<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// List pods in all namespaces
        #[arg(short = 'A', long)]
        all_namespaces: bool,
        /// Label selector
        #[arg(short = 'l', long)]
        selector: Option<String>,
        /// Show containers
        #[arg(long)]
        containers: bool,
    },
}

#[derive(Subcommand)]
pub enum AuthCommands {
    /// Check if an action is allowed
    CanI {
        /// Verb (e.g., get, list, create, delete)
        verb: String,
        /// Resource (e.g., pods, deployments)
        resource: String,
        /// Resource name
        name: Option<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Check all namespaces
        #[arg(short = 'A', long)]
        all_namespaces: bool,
    },
    /// Experimental: Check who you are
    Whoami {
        /// Output format (json, yaml)
        #[arg(short = 'o', long)]
        output: Option<String>,
    },
    /// Reconciles rules for RBAC Role, RoleBinding, ClusterRole, and ClusterRoleBinding objects
    Reconcile {
        /// Path to YAML file containing RBAC resources
        #[arg(short = 'f', long)]
        filename: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Remove extra permissions added to the role
        #[arg(long)]
        remove_extra_permissions: bool,
        /// Remove extra subjects added to the binding
        #[arg(long)]
        remove_extra_subjects: bool,
    },
}

#[derive(Subcommand)]
pub enum SetCommands {
    /// Update container images in a resource
    Image {
        /// Resource (TYPE/NAME, e.g., deployment/nginx)
        resource: String,
        /// Container=image pairs (e.g., nginx=nginx:1.9.1)
        container_images: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Update environment variables on a resource
    Env {
        /// Resource (TYPE/NAME, e.g., deployment/registry)
        resource: String,
        /// Environment variables (KEY=VALUE or KEY-)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        env_vars: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Container name
        #[arg(short = 'c', long)]
        container: Option<String>,
        /// List environment variables
        #[arg(long)]
        list: bool,
    },
    /// Update resource requests/limits on a resource
    Resources {
        /// Resource (TYPE/NAME, e.g., deployment/nginx)
        resource: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Container name
        #[arg(short = 'c', long)]
        container: Option<String>,
        /// Resource limits (cpu=200m,memory=512Mi)
        #[arg(long)]
        limits: Option<String>,
        /// Resource requests (cpu=100m,memory=256Mi)
        #[arg(long)]
        requests: Option<String>,
    },
    /// Set the selector on a resource
    Selector {
        /// Resource (TYPE/NAME, e.g., service/myapp)
        resource: String,
        /// Selector expressions (key=value)
        expressions: Vec<String>,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Use --all to select all resources
        #[arg(long)]
        all: bool,
        /// Resource container name (for pods)
        #[arg(long)]
        resource_version: Option<String>,
    },
    /// Update ServiceAccountName of a resource
    #[command(name = "serviceaccount")]
    ServiceAccount {
        /// Resource (TYPE/NAME, e.g., deployment/nginx)
        resource: String,
        /// ServiceAccount name to set
        service_account_name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Update User, Group, or ServiceAccount in a RoleBinding/ClusterRoleBinding
    Subject {
        /// Resource (TYPE/NAME, e.g., clusterrolebinding/mycrb)
        resource: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// ServiceAccount to add (namespace:name)
        #[arg(long)]
        serviceaccount: Option<String>,
        /// User to add
        #[arg(long)]
        user: Option<String>,
        /// Group to add
        #[arg(long)]
        group: Option<String>,
    },
}

#[derive(Subcommand)]
#[allow(clippy::enum_variant_names)]
pub enum ApplyCommands {
    /// Edit the latest last-applied-configuration annotation of a resource
    EditLastApplied {
        /// Resource type
        resource_type: String,
        /// Resource name
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Set the last-applied-configuration annotation on a resource to match the contents of a file
    SetLastApplied {
        /// Path to YAML file
        #[arg(short = 'f', long)]
        filename: String,
        /// Create the annotation if it does not already exist
        #[arg(long)]
        create_annotation: bool,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// View the latest last-applied-configuration annotation of a resource
    ViewLastApplied {
        /// Resource type
        resource_type: String,
        /// Resource name
        name: String,
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
        /// Output format (json, yaml)
        #[arg(short = 'o', long)]
        output: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum PluginCommands {
    /// List all available plugin files on a user's PATH
    List {},
}

#[derive(Subcommand)]
pub enum ConfigCommands {
    /// Display current context
    CurrentContext {},
    /// Display merged kubeconfig
    View {
        /// Minify output
        #[arg(long)]
        minify: bool,
        /// Flatten output
        #[arg(long)]
        flatten: bool,
    },
    /// Set a context
    UseContext {
        /// Context name
        name: String,
    },
    /// Get contexts
    GetContexts {
        /// Output format
        #[arg(short = 'o', long)]
        output: Option<String>,
    },
    /// Get clusters
    GetClusters {},
    /// Get users defined in kubeconfig
    GetUsers {},
    /// Set a property in kubeconfig
    Set {
        /// Property path
        property: String,
        /// Value
        value: String,
    },
    /// Unset a property in kubeconfig
    Unset {
        /// Property path
        property: String,
    },
    /// Delete a cluster from kubeconfig
    DeleteCluster {
        /// Cluster name to delete
        name: String,
    },
    /// Delete a context from kubeconfig
    DeleteContext {
        /// Context name to delete
        name: String,
    },
    /// Delete a user from kubeconfig
    DeleteUser {
        /// User name to delete
        name: String,
    },
    /// Rename a context in kubeconfig
    RenameContext {
        /// Current context name
        old_name: String,
        /// New context name
        new_name: String,
    },
    /// Set cluster info in kubeconfig
    SetCluster {
        /// Cluster name
        name: String,
        /// Server URL
        #[arg(long)]
        server: Option<String>,
        /// Path to certificate authority file
        #[arg(long)]
        certificate_authority: Option<String>,
        /// Base64-encoded certificate authority data
        #[arg(long)]
        certificate_authority_data: Option<String>,
        /// Skip TLS verification
        #[arg(long)]
        insecure_skip_tls_verify: Option<bool>,
    },
    /// Set context info in kubeconfig
    SetContext {
        /// Context name
        name: String,
        /// Cluster name
        #[arg(long)]
        cluster: Option<String>,
        /// User name
        #[arg(long)]
        user: Option<String>,
        /// Default namespace
        #[arg(long)]
        namespace: Option<String>,
    },
    /// Set user credentials in kubeconfig
    SetCredentials {
        /// User name
        name: String,
        /// Bearer token
        #[arg(long)]
        token: Option<String>,
        /// Username for basic auth
        #[arg(long)]
        username: Option<String>,
        /// Password for basic auth
        #[arg(long)]
        password: Option<String>,
        /// Path to client certificate file
        #[arg(long)]
        client_certificate: Option<String>,
        /// Path to client key file
        #[arg(long)]
        client_key: Option<String>,
        /// Base64-encoded client certificate data
        #[arg(long)]
        client_certificate_data: Option<String>,
        /// Base64-encoded client key data
        #[arg(long)]
        client_key_data: Option<String>,
    },
}
