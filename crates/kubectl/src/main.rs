mod client;
mod commands;
mod kubeconfig;
mod types;
mod websocket;

use anyhow::Result;
use clap::{Parser, Subcommand};
use client::ApiClient;
use kubeconfig::KubeConfig;
use types::{
    ApplyCommands, AuthCommands, CertificateCommands, ConfigCommands, CreateCommands,
    KubercCommands, PluginCommands, RolloutCommands, SetCommands, TopCommands,
};

#[derive(Parser)]
#[command(name = "kubectl")]
#[command(about = "Rusternetes kubectl - Command line tool for Rusternetes")]
struct Cli {
    /// Path to kubeconfig file
    #[arg(long, global = true)]
    kubeconfig: Option<String>,

    /// Context to use from kubeconfig
    #[arg(long, global = true)]
    context: Option<String>,

    /// API server address (overrides kubeconfig)
    #[arg(long, global = true)]
    server: Option<String>,

    /// Skip TLS certificate verification (insecure)
    #[arg(long, global = true)]
    insecure_skip_tls_verify: bool,

    /// Bearer token for authentication (overrides kubeconfig)
    #[arg(long, global = true)]
    token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Get resources
    Get {
        /// Resource type (pod, service, deployment, node, namespace)
        resource_type: String,

        /// Resource name (optional)
        name: Option<String>,

        /// Namespace (for namespaced resources)
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// List resources in all namespaces
        #[arg(short = 'A', long)]
        all_namespaces: bool,

        /// Output format (json, yaml, wide, name, custom-columns, jsonpath, go-template)
        #[arg(short = 'o', long)]
        output: Option<String>,

        /// Don't print headers (for table output)
        #[arg(long)]
        no_headers: bool,

        /// Label selector to filter on
        #[arg(short = 'l', long)]
        selector: Option<String>,

        /// Field selector to filter on
        #[arg(long)]
        field_selector: Option<String>,

        /// Watch for changes
        #[arg(short = 'w', long)]
        watch: bool,

        /// Show resource details in additional columns (wider output)
        #[arg(long)]
        show_labels: bool,

        /// Sort output by JSONPath expression (e.g. .metadata.name, .status.startTime)
        #[arg(long)]
        sort_by: Option<String>,
    },

    /// Create a resource from a file or using subcommands
    Create {
        /// Path to YAML file
        #[arg(short = 'f', long)]
        file: Option<String>,

        /// Namespace for the resource
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Subcommand for specific resource creation
        #[command(subcommand)]
        subcommand: Option<CreateCommands>,

        /// Inline resource type and name (e.g., kubectl create namespace foo)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Delete a resource
    Delete {
        /// Resource type (pod, service, deployment, node, namespace)
        resource_type: Option<String>,

        /// Resource name
        name: Option<String>,

        /// Delete from file
        #[arg(short = 'f', long)]
        file: Option<String>,

        /// Namespace (for namespaced resources)
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Label selector to filter resources to delete
        #[arg(short = 'l', long)]
        selector: Option<String>,

        /// Delete all resources of the specified type
        #[arg(long)]
        all: bool,

        /// Force deletion (grace-period=0 + cascade=Background)
        #[arg(long)]
        force: bool,

        /// Grace period in seconds before forceful termination
        #[arg(long)]
        grace_period: Option<i64>,

        /// Cascade strategy: background, foreground, or orphan
        #[arg(long, default_value = "background")]
        cascade: String,

        /// Server-side dry run (no changes persisted). Use --dry-run=server
        #[arg(long)]
        dry_run: Option<String>,

        /// Wait for resources to be fully deleted
        #[arg(long)]
        wait: bool,

        /// Output format (only 'name' is supported: prints resource/name)
        #[arg(short = 'o', long)]
        output: Option<String>,
    },

    /// Apply a configuration to a resource
    Apply {
        /// Path to YAML/JSON file(s) or directory. Can be specified multiple times.
        #[arg(short = 'f', long = "filename", num_args = 1..)]
        file: Vec<String>,

        /// Namespace for the resource
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Perform a dry run
        #[arg(long)]
        dry_run: Option<String>,

        /// Use server-side apply
        #[arg(long)]
        server_side: bool,

        /// Force apply (for conflicts)
        #[arg(long)]
        force: bool,

        /// Process the directory used in -f recursively
        #[arg(short = 'R', long)]
        recursive: bool,

        /// Name of the manager used to track field ownership (server-side apply)
        #[arg(long, default_value = "kubectl-client-side-apply")]
        field_manager: String,

        /// Output format: json, yaml, or name
        #[arg(short = 'o', long)]
        output: Option<String>,

        /// Validation mode: true, false, strict, warn, ignore
        #[arg(long)]
        validate: Option<String>,

        /// Apply subcommands
        #[command(subcommand)]
        subcommand: Option<ApplyCommands>,
    },

    /// Replace a resource by file name or stdin
    Replace {
        /// Path to YAML/JSON file (use - for stdin)
        #[arg(short = 'f', long)]
        file: String,

        /// Namespace for the resource
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },

    /// Run a particular image on the cluster
    Run {
        /// Name for the pod
        name: String,

        /// Container image to run
        #[arg(long)]
        image: String,

        /// Container port to expose
        #[arg(long)]
        port: Option<u16>,

        /// Environment variables (KEY=VALUE)
        #[arg(long = "env")]
        env: Vec<String>,

        /// Labels to apply (key=value,key=value)
        #[arg(short = 'l', long)]
        labels: Option<String>,

        /// Restart policy (Always, OnFailure, Never)
        #[arg(long, default_value = "Always")]
        restart: String,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// If true, use extra arguments as command instead of args
        #[arg(long)]
        command: bool,

        /// Perform a dry run (client or server)
        #[arg(long)]
        dry_run: Option<String>,

        /// Extra arguments (command or args for the container)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        extra_args: Vec<String>,
    },

    /// Describe a resource
    Describe {
        /// Resource type (pod, service, deployment, node, namespace)
        resource_type: String,

        /// Resource name
        name: Option<String>,

        /// Namespace (for namespaced resources)
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Label selector to filter resources
        #[arg(short = 'l', long)]
        selector: Option<String>,

        /// List resources in all namespaces
        #[arg(short = 'A', long)]
        all_namespaces: bool,
    },

    /// Get logs from a pod
    Logs {
        /// Pod name
        pod_name: String,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Container name (optional, for multi-container pods)
        #[arg(short = 'c', long)]
        container: Option<String>,

        /// Follow the logs
        #[arg(short = 'f', long)]
        follow: bool,

        /// Number of lines to show from the end of the logs
        #[arg(long)]
        tail: Option<i64>,

        /// Show timestamps
        #[arg(long)]
        timestamps: bool,

        /// Show logs since a specific time (RFC3339)
        #[arg(long)]
        since_time: Option<String>,

        /// Show logs since a duration ago (e.g., 5s, 2m, 1h)
        #[arg(long)]
        since: Option<String>,

        /// Show logs for previous container instance
        #[arg(short = 'p', long)]
        previous: bool,
    },

    /// Execute a command in a container
    Exec {
        /// Pod name
        pod_name: String,

        /// Container name (optional, for multi-container pods)
        #[arg(short = 'c', long)]
        container: Option<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Allocate a TTY
        #[arg(short = 't', long)]
        tty: bool,

        /// Pass stdin to the container
        #[arg(short = 'i', long)]
        stdin: bool,

        /// Command to execute
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
        command: Vec<String>,
    },

    /// Forward one or more local ports to a pod
    PortForward {
        /// Pod name
        pod_name: String,

        /// Port mappings (e.g., 8080:80 or 8080)
        ports: Vec<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Address to bind to
        #[arg(long, default_value = "localhost")]
        address: String,
    },

    /// Copy files to/from containers
    Cp {
        /// Source (pod:path or local path)
        source: String,

        /// Destination (pod:path or local path)
        destination: String,

        /// Container name
        #[arg(short = 'c', long)]
        container: Option<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },

    /// Edit a resource
    Edit {
        /// Resource type
        resource_type: String,

        /// Resource name
        name: String,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Output format (json or yaml)
        #[arg(short = 'o', long, default_value = "yaml")]
        output: String,
    },

    /// Patch a resource
    Patch {
        /// Resource type
        resource_type: String,

        /// Resource name
        name: String,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Patch content
        #[arg(short = 'p', long)]
        patch: Option<String>,

        /// Patch file
        #[arg(long)]
        patch_file: Option<String>,

        /// Patch type (json, merge, strategic)
        #[arg(long, default_value = "strategic")]
        patch_type: String,
    },

    /// Scale a resource
    Scale {
        /// Resource type
        resource_type: String,

        /// Resource name
        name: String,

        /// Number of replicas
        #[arg(long)]
        replicas: i32,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },

    /// Expose a resource as a new Kubernetes service
    Expose {
        /// Resource type (pod, service, replicationcontroller, deployment, replicaset)
        resource_type: String,

        /// Resource name
        resource_name: String,

        /// The port that the service should serve on
        #[arg(long)]
        port: Option<i32>,

        /// Name or number for the port on the container that the service should direct traffic to
        #[arg(long)]
        target_port: Option<i32>,

        /// The network protocol for the service (TCP, UDP, SCTP)
        #[arg(long, default_value = "TCP")]
        protocol: String,

        /// The name for the newly created service
        #[arg(long)]
        name: Option<String>,

        /// Type for this service: ClusterIP, NodePort, LoadBalancer, or ExternalName
        #[arg(long, name = "type")]
        service_type: Option<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },

    /// Attach to a running container
    Attach {
        /// Pod name
        pod_name: String,

        /// Container name (optional, for multi-container pods)
        #[arg(short = 'c', long)]
        container: Option<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Allocate a TTY
        #[arg(short = 't', long)]
        tty: bool,

        /// Pass stdin to the container
        #[arg(short = 'i', long)]
        stdin: bool,
    },

    /// Set specific features on objects
    Set {
        #[command(subcommand)]
        command: SetCommands,
    },

    /// Update the taints on one or more nodes
    Taint {
        /// Resource type (must be "nodes" or "node")
        resource_type: String,

        /// Node name
        node_name: String,

        /// Taint specifications (key=value:Effect or key:Effect- or key-)
        taints: Vec<String>,

        /// Overwrite existing taints with same key and effect
        #[arg(long)]
        overwrite: bool,
    },

    /// Drain a node in preparation for maintenance
    Drain {
        /// Node name
        node_name: String,

        /// Continue even if there are pods not managed by a controller
        #[arg(long)]
        force: bool,

        /// Ignore DaemonSet-managed pods
        #[arg(long)]
        ignore_daemonsets: bool,

        /// Continue even if there are pods using emptyDir
        #[arg(long)]
        delete_emptydir_data: bool,

        /// Grace period in seconds for pod termination
        #[arg(long)]
        grace_period: Option<i64>,

        /// Timeout in seconds to wait for drain to complete
        #[arg(long)]
        timeout: Option<u64>,
    },

    /// Mark node as unschedulable
    Cordon {
        /// Node name
        node_name: String,
    },

    /// Mark node as schedulable
    Uncordon {
        /// Node name
        node_name: String,
    },

    /// Rollout management commands
    Rollout {
        #[command(subcommand)]
        command: RolloutCommands,
    },

    /// Display resource usage (CPU/memory)
    Top {
        #[command(subcommand)]
        command: TopCommands,
    },

    /// Update labels on a resource
    Label {
        /// Resource type
        resource_type: String,

        /// Resource name
        name: String,

        /// Labels to set (key=value) or remove (key-)
        labels: Vec<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Overwrite existing labels
        #[arg(long)]
        overwrite: bool,
    },

    /// Update annotations on a resource
    Annotate {
        /// Resource type
        resource_type: String,

        /// Resource name
        name: String,

        /// Annotations to set (key=value) or remove (key-)
        annotations: Vec<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Overwrite existing annotations
        #[arg(long)]
        overwrite: bool,
    },

    /// Explain resource documentation
    Explain {
        /// Resource type (e.g., pod, pod.spec, pod.spec.containers)
        resource: String,

        /// API version to use
        #[arg(long)]
        api_version: Option<String>,

        /// Show recursive schema
        #[arg(long)]
        recursive: bool,
    },

    /// Wait for a specific condition on resources
    Wait {
        /// Resource type and name (e.g., pod/mypod)
        resource: Vec<String>,

        /// Condition to wait for
        #[arg(long)]
        for_condition: Option<String>,

        /// Wait for deletion
        #[arg(long)]
        for_delete: bool,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Timeout duration
        #[arg(long, default_value = "30s")]
        timeout: String,

        /// Label selector
        #[arg(short = 'l', long)]
        selector: Option<String>,
    },

    /// Show difference between current and applied configuration
    Diff {
        /// Path to YAML file
        #[arg(short = 'f', long)]
        file: String,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },

    /// Authentication commands
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },

    /// Display available API resources
    ApiResources {
        /// Show namespaced resources
        #[arg(long)]
        namespaced: Option<bool>,

        /// API group to filter
        #[arg(long)]
        api_group: Option<String>,

        /// Show only resource names
        #[arg(long)]
        no_headers: bool,

        /// Output format
        #[arg(short = 'o', long)]
        output: Option<String>,
    },

    /// Print supported API versions
    ApiVersions {},

    /// Manage kubeconfig files
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },

    /// Display cluster information
    ClusterInfo {
        /// Dump detailed cluster information
        #[arg(long)]
        dump: bool,
    },

    /// Run a proxy to the Kubernetes API server
    Proxy {
        /// The port on which to run the proxy (0 = random available port)
        #[arg(short = 'p', long, default_value = "8001")]
        port: u16,

        /// The IP address on which to serve
        #[arg(long, default_value = "127.0.0.1")]
        address: String,
    },

    /// Auto-scale a deployment, replica set, stateful set, or replication controller
    Autoscale {
        /// Resource type (deployment, replicaset, statefulset, replicationcontroller)
        resource_type: String,

        /// Resource name
        name: String,

        /// The upper limit for the number of pods that can be set by the autoscaler (required)
        #[arg(long)]
        max: i32,

        /// The lower limit for the number of pods that can be set by the autoscaler
        #[arg(long)]
        min: Option<i32>,

        /// The target average CPU utilization (represented as a percent of requested CPU)
        #[arg(long)]
        cpu_percent: Option<i32>,

        /// The name for the newly created HPA (defaults to resource name)
        #[arg(long = "name")]
        hpa_name: Option<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },

    /// Create debugging sessions for troubleshooting workloads and nodes
    Debug {
        /// Target to debug (pod name or type/name, e.g., pod/nginx or node/mynode)
        target: String,

        /// Container image to use for debug container
        #[arg(long)]
        image: String,

        /// Container name to use for debug container
        #[arg(short = 'c', long)]
        container: Option<String>,

        /// Keep stdin open on the container
        #[arg(short = 'i', long)]
        stdin: bool,

        /// Allocate a TTY for the debugging container
        #[arg(short = 't', long)]
        tty: bool,

        /// When using an ephemeral container, target processes in this container name
        #[arg(long)]
        target_container: Option<String>,

        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// Command to run in the debug container
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },

    /// List events
    Events {
        /// Namespace
        #[arg(short = 'n', long)]
        namespace: Option<String>,

        /// List events in all namespaces
        #[arg(short = 'A', long)]
        all_namespaces: bool,

        /// Filter events to only those pertaining to the specified resource (e.g., pod/nginx)
        #[arg(long = "for")]
        for_object: Option<String>,

        /// Output only events of given types (Normal, Warning)
        #[arg(long)]
        types: Option<String>,

        /// Watch for new events after listing
        #[arg(short = 'w', long)]
        watch: bool,

        /// Don't print headers
        #[arg(long)]
        no_headers: bool,

        /// Output format (json, yaml)
        #[arg(short = 'o', long)]
        output: Option<String>,
    },

    /// Modify certificate resources (approve/deny CSRs)
    Certificate {
        #[command(subcommand)]
        command: CertificateCommands,
    },

    /// Output shell completion code for the specified shell
    Completion {
        /// Shell type (bash, zsh, fish, powershell)
        shell: String,
    },

    /// Print help for a command
    Help {
        /// Command to get help for
        command: Option<String>,
    },

    /// Print the list of flags inherited by all commands
    Options {},

    /// Build a kustomization target from a directory or URL
    Kustomize {
        /// Path to a directory containing a kustomization.yaml file, or a URL
        #[arg(default_value = ".")]
        dir: String,
    },

    /// Provides utilities for interacting with plugins
    Plugin {
        #[command(subcommand)]
        command: PluginCommands,
    },

    /// KubeRC configuration (alpha)
    Kuberc {
        #[command(subcommand)]
        subcommand: Option<KubercCommands>,
    },

    /// Display Kubernetes version
    Version {
        /// Show client version only
        #[arg(long)]
        client: bool,

        /// Output format (json, yaml)
        #[arg(short = 'o', long)]
        output: Option<String>,
    },
}

// Rollout, Top, Auth, and Config commands are now in types.rs

/*
#[derive(Subcommand)]
enum RolloutCommands {
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
enum TopCommands {
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
enum AuthCommands {
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
}

#[derive(Subcommand)]
enum ConfigCommands {
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
}
*/

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load kubeconfig or use CLI flags
    let (server, skip_tls, token, default_namespace) = if let Some(server_url) = cli.server {
        // CLI flags override kubeconfig
        (
            server_url,
            cli.insecure_skip_tls_verify,
            cli.token,
            "default".to_string(),
        )
    } else {
        // Try to load from kubeconfig
        let config = if let Some(path) = &cli.kubeconfig {
            KubeConfig::load_from_file(&std::path::PathBuf::from(path))?
        } else {
            KubeConfig::load_default().unwrap_or_else(|_| {
                eprintln!("Warning: Could not load kubeconfig, using defaults");
                // Return a minimal default config
                KubeConfig {
                    api_version: Some("v1".to_string()),
                    kind: Some("Config".to_string()),
                    current_context: "default".to_string(),
                    contexts: vec![],
                    clusters: vec![],
                    users: vec![],
                    preferences: std::collections::HashMap::new(),
                }
            })
        };

        let server = config
            .get_server()
            .unwrap_or_else(|_| "https://localhost:6443".to_string());
        let skip_tls = config
            .should_skip_tls_verify()
            .unwrap_or(cli.insecure_skip_tls_verify);
        let token = cli.token.or_else(|| config.get_token().ok().flatten());
        let namespace = config
            .get_namespace()
            .unwrap_or_else(|_| "default".to_string());

        (server, skip_tls, token, namespace)
    };

    let client = ApiClient::new(&server, skip_tls, token)?;

    match cli.command {
        Commands::Get {
            resource_type,
            name,
            namespace,
            all_namespaces,
            output,
            no_headers,
            selector,
            field_selector,
            watch,
            show_labels,
            sort_by,
        } => {
            let ns = if all_namespaces {
                None
            } else {
                Some(namespace.as_deref().unwrap_or(&default_namespace))
            };
            commands::get::execute_enhanced(
                &client,
                &resource_type,
                name.as_deref(),
                ns,
                all_namespaces,
                output.as_deref(),
                no_headers,
                selector.as_deref(),
                field_selector.as_deref(),
                watch,
                show_labels,
                sort_by.as_deref(),
            )
            .await?;
        }
        Commands::Create {
            file,
            namespace,
            subcommand,
            args,
        } => {
            if let Some(file_path) = file {
                commands::create::execute(&client, &file_path).await?;
            } else if let Some(ref sub) = subcommand {
                commands::create::execute_subcommand(
                    &client,
                    sub,
                    namespace.as_deref().unwrap_or(&default_namespace),
                )
                .await?;
            } else if !args.is_empty() {
                commands::create::execute_inline(
                    &client,
                    &args,
                    namespace.as_deref().unwrap_or(&default_namespace),
                )
                .await?;
            } else {
                anyhow::bail!(
                    "Either --file, a subcommand, or resource arguments must be provided"
                );
            }
        }
        Commands::Delete {
            resource_type,
            name,
            file,
            namespace,
            selector,
            all,
            force,
            grace_period,
            cascade,
            dry_run,
            wait,
            output,
        } => {
            let mut opts = commands::delete::DeleteOptions {
                grace_period,
                force,
                cascade: commands::delete::CascadeStrategy::from_str_value(&cascade)?,
                delete_all: all,
                dry_run: dry_run.as_deref() == Some("server"),
                wait,
                output,
            };
            opts.resolve();

            if let Some(file_path) = file {
                commands::delete::execute_from_file(&client, &file_path).await?;
            } else if all {
                if let Some(rt) = resource_type {
                    commands::delete::execute_delete_all(
                        &client,
                        &rt,
                        namespace.as_deref().unwrap_or(&default_namespace),
                        &opts,
                    )
                    .await?;
                } else {
                    anyhow::bail!("Must provide resource type when using --all");
                }
            } else if let (Some(rt), Some(n)) = (resource_type.clone(), name) {
                commands::delete::execute_enhanced(
                    &client,
                    &rt,
                    &n,
                    namespace.as_deref().unwrap_or(&default_namespace),
                    &opts,
                )
                .await?;
            } else if let (Some(rt), Some(sel)) = (resource_type, selector) {
                commands::delete::execute_with_selector(
                    &client,
                    &rt,
                    &sel,
                    namespace.as_deref().unwrap_or(&default_namespace),
                    &opts,
                )
                .await?;
            } else {
                anyhow::bail!("Must provide either resource type/name, file, or selector");
            }
        }
        Commands::Apply {
            file,
            namespace,
            dry_run,
            server_side,
            force,
            recursive,
            field_manager,
            output,
            validate,
            subcommand,
        } => {
            if let Some(sub) = subcommand {
                commands::apply::execute_subcommand(
                    &client,
                    sub,
                    namespace.as_deref().unwrap_or(&default_namespace),
                )
                .await?;
            } else if !file.is_empty() {
                let options = commands::apply::ApplyOptions {
                    files: file,
                    namespace: namespace.clone(),
                    dry_run: dry_run.clone(),
                    server_side,
                    force,
                    recursive,
                    field_manager: field_manager.clone(),
                    output: output.clone(),
                    validate: validate.clone(),
                };
                commands::apply::execute_with_options(&client, &options).await?;
            } else {
                anyhow::bail!("Either --filename/-f or a subcommand must be provided");
            }
        }
        Commands::Replace {
            file,
            namespace: _namespace,
        } => {
            commands::replace::execute(&client, &file).await?;
        }
        Commands::Run {
            name,
            image,
            port,
            env,
            labels,
            restart,
            namespace,
            command,
            dry_run,
            extra_args,
        } => {
            // Split extra_args into command vs args based on --command flag
            let (cmd_args, container_args) = if command {
                (extra_args.as_slice(), [].as_slice())
            } else {
                ([].as_slice(), extra_args.as_slice())
            };
            commands::run::execute(
                &client,
                &name,
                namespace.as_deref().unwrap_or(&default_namespace),
                &image,
                port,
                &env,
                labels.as_deref(),
                &restart,
                cmd_args,
                container_args,
                dry_run.as_deref(),
            )
            .await?;
        }
        Commands::Describe {
            resource_type,
            name,
            namespace,
            selector,
            all_namespaces,
        } => {
            commands::describe::execute_enhanced(
                &client,
                &resource_type,
                name.as_deref(),
                namespace.as_deref().unwrap_or(&default_namespace),
                selector.as_deref(),
                all_namespaces,
            )
            .await?;
        }
        Commands::Logs {
            pod_name,
            namespace,
            container,
            follow,
            tail,
            timestamps,
            since_time,
            since,
            previous,
        } => {
            commands::logs::execute_enhanced(
                &client,
                &pod_name,
                namespace.as_deref().unwrap_or(&default_namespace),
                container.as_deref(),
                follow,
                tail,
                timestamps,
                since_time.as_deref(),
                since.as_deref(),
                previous,
            )
            .await?;
        }
        Commands::Exec {
            pod_name,
            container,
            namespace,
            tty,
            stdin,
            command,
        } => {
            commands::exec::execute(
                &client,
                &pod_name,
                namespace.as_deref().unwrap_or(&default_namespace),
                container.as_deref(),
                &command,
                tty,
                stdin,
            )
            .await?;
        }
        Commands::PortForward {
            pod_name,
            ports,
            namespace,
            address,
        } => {
            commands::port_forward::execute(
                &client,
                &pod_name,
                namespace.as_deref().unwrap_or(&default_namespace),
                &ports,
                &address,
            )
            .await?;
        }
        Commands::Cp {
            source,
            destination,
            container,
            namespace,
        } => {
            commands::cp::execute(
                &client,
                &source,
                &destination,
                namespace.as_deref().unwrap_or(&default_namespace),
                container.as_deref(),
            )
            .await?;
        }
        Commands::Edit {
            resource_type,
            name,
            namespace,
            output,
        } => {
            commands::edit::execute(
                &client,
                &resource_type,
                &name,
                namespace.as_deref().unwrap_or(&default_namespace),
                &output,
            )
            .await?;
        }
        Commands::Patch {
            resource_type,
            name,
            namespace,
            patch,
            patch_file,
            patch_type,
        } => {
            commands::patch::execute(
                &client,
                &resource_type,
                &name,
                namespace.as_deref().unwrap_or(&default_namespace),
                patch.as_deref(),
                patch_file.as_deref(),
                &patch_type,
            )
            .await?;
        }
        Commands::Scale {
            resource_type,
            name,
            replicas,
            namespace,
        } => {
            commands::scale::execute(
                &client,
                &resource_type,
                &name,
                namespace.as_deref().unwrap_or(&default_namespace),
                replicas,
            )
            .await?;
        }
        Commands::Expose {
            resource_type,
            resource_name,
            port,
            target_port,
            protocol,
            name,
            service_type,
            namespace,
        } => {
            commands::expose::execute(
                &client,
                &resource_type,
                &resource_name,
                namespace.as_deref().unwrap_or(&default_namespace),
                port,
                target_port,
                &protocol,
                name.as_deref(),
                service_type.as_deref(),
            )
            .await?;
        }
        Commands::Attach {
            pod_name,
            container,
            namespace,
            tty,
            stdin,
        } => {
            commands::attach::execute(
                &client,
                &pod_name,
                namespace.as_deref().unwrap_or(&default_namespace),
                container.as_deref(),
                tty,
                stdin,
            )
            .await?;
        }
        Commands::Set { command } => {
            commands::set::execute_set(&client, command, &default_namespace).await?;
        }
        Commands::Taint {
            resource_type,
            node_name,
            taints,
            overwrite,
        } => {
            if resource_type != "nodes" && resource_type != "node" {
                anyhow::bail!("Taint only supports nodes, got: {}", resource_type);
            }
            commands::taint::execute(&client, &node_name, &taints, overwrite).await?;
        }
        Commands::Drain {
            node_name,
            force,
            ignore_daemonsets,
            delete_emptydir_data,
            grace_period,
            timeout,
        } => {
            commands::drain::execute_drain(
                &client,
                &node_name,
                force,
                ignore_daemonsets,
                delete_emptydir_data,
                grace_period,
                timeout,
            )
            .await?;
        }
        Commands::Cordon { node_name } => {
            commands::drain::execute_cordon(&client, &node_name).await?;
        }
        Commands::Uncordon { node_name } => {
            commands::drain::execute_uncordon(&client, &node_name).await?;
        }
        Commands::Rollout { command } => {
            commands::rollout::execute(&client, command, &default_namespace).await?;
        }
        Commands::Top { command } => {
            commands::top::execute(&client, command, &default_namespace).await?;
        }
        Commands::Label {
            resource_type,
            name,
            labels,
            namespace,
            overwrite,
        } => {
            commands::label::execute(
                &client,
                &resource_type,
                &name,
                namespace.as_deref().unwrap_or(&default_namespace),
                &labels,
                overwrite,
            )
            .await?;
        }
        Commands::Annotate {
            resource_type,
            name,
            annotations,
            namespace,
            overwrite,
        } => {
            commands::annotate::execute(
                &client,
                &resource_type,
                &name,
                namespace.as_deref().unwrap_or(&default_namespace),
                &annotations,
                overwrite,
            )
            .await?;
        }
        Commands::Explain {
            resource,
            api_version,
            recursive,
        } => {
            commands::explain::execute(&resource, api_version.as_deref(), recursive).await?;
        }
        Commands::Wait {
            resource,
            for_condition,
            for_delete,
            namespace,
            timeout,
            selector,
        } => {
            commands::wait::execute(
                &client,
                &resource,
                for_condition.as_deref(),
                for_delete,
                namespace.as_deref().unwrap_or(&default_namespace),
                &timeout,
                selector.as_deref(),
            )
            .await?;
        }
        Commands::Diff { file, namespace } => {
            commands::diff::execute(
                &client,
                &file,
                namespace.as_deref().unwrap_or(&default_namespace),
            )
            .await?;
        }
        Commands::Auth { command } => {
            commands::auth::execute(&client, command, &default_namespace).await?;
        }
        Commands::ApiResources {
            namespaced,
            api_group,
            no_headers,
            output,
        } => {
            commands::api_resources::execute(
                &client,
                namespaced,
                api_group.as_deref(),
                no_headers,
                output.as_deref(),
            )
            .await?;
        }
        Commands::ApiVersions {} => {
            commands::api_versions::execute(&client).await?;
        }
        Commands::Config { command } => {
            commands::config::execute(command, cli.kubeconfig.as_deref()).await?;
        }
        Commands::ClusterInfo { dump } => {
            commands::cluster_info::execute(&client, dump).await?;
        }
        Commands::Proxy { port, address } => {
            let proxy_config = commands::proxy::ProxyConfig {
                address,
                port,
                api_server: server.clone(),
                token: client.get_token().cloned(),
                skip_tls_verify: skip_tls,
            };
            commands::proxy::execute(proxy_config).await?;
        }
        Commands::Autoscale {
            resource_type,
            name,
            max,
            min,
            cpu_percent,
            hpa_name,
            namespace,
        } => {
            commands::autoscale::execute(
                &client,
                &resource_type,
                &name,
                namespace.as_deref().unwrap_or(&default_namespace),
                min,
                max,
                cpu_percent,
                hpa_name.as_deref(),
            )
            .await?;
        }
        Commands::Debug {
            target,
            image,
            container,
            stdin,
            tty,
            target_container,
            namespace,
            command,
        } => {
            commands::debug::execute(
                &client,
                &target,
                namespace.as_deref().unwrap_or(&default_namespace),
                &image,
                container.as_deref(),
                stdin,
                tty,
                target_container.as_deref(),
                &command,
            )
            .await?;
        }
        Commands::Events {
            namespace,
            all_namespaces,
            for_object,
            types,
            watch,
            no_headers,
            output,
        } => {
            let ns = namespace.as_deref().unwrap_or(&default_namespace);
            commands::events::execute(
                &client,
                ns,
                all_namespaces,
                for_object.as_deref(),
                types.as_deref(),
                watch,
                no_headers,
                output.as_deref(),
            )
            .await?;
        }
        Commands::Certificate { command } => match command {
            CertificateCommands::Approve { csr_names, force } => {
                commands::certificate::execute(&client, "approve", &csr_names, force).await?;
            }
            CertificateCommands::Deny { csr_names, force } => {
                commands::certificate::execute(&client, "deny", &csr_names, force).await?;
            }
        },
        Commands::Completion { shell } => {
            commands::completion::execute(&shell)?;
        }
        Commands::Help { command } => {
            commands::help::execute::<Cli>(command.as_deref())?;
        }
        Commands::Options {} => {
            commands::options::execute()?;
        }
        Commands::Kustomize { dir } => {
            commands::kustomize::execute(&dir)?;
        }
        Commands::Plugin { command } => match command {
            PluginCommands::List {} => {
                commands::plugin::execute_list()?;
            }
        },
        Commands::Kuberc { subcommand } => {
            commands::kuberc::execute(subcommand.as_ref())?;
        }
        Commands::Version {
            client: client_only,
            output,
        } => {
            commands::version::execute(&client, client_only, output.as_deref()).await?;
        }
    }

    Ok(())
}
