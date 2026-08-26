use anyhow::{Context, Result};
use rusternetes_common::resources::{EndpointSlice, Endpoints};
use rusternetes_storage::Storage;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
use tracing::{info, warn};

/// Get the API server's IP address from the network interface
/// This discovers the container's IP on the Docker/Podman network
fn get_api_server_ip() -> Result<String> {
    // Try to get IP from network interfaces
    // Look for non-loopback IPv4 addresses
    let interfaces = match get_if_addrs::get_if_addrs() {
        Ok(addrs) => addrs,
        Err(e) => {
            warn!("Failed to get network interfaces: {}", e);
            return Err(anyhow::anyhow!("Failed to get network interfaces: {}", e));
        }
    };

    // Find the first non-loopback IPv4 address
    for iface in interfaces {
        if !iface.is_loopback() {
            if let get_if_addrs::IfAddr::V4(addr) = iface.addr {
                let ip = addr.ip.to_string();
                info!(
                    "Discovered API server IP: {} (interface: {})",
                    ip, iface.name
                );
                return Ok(ip);
            }
        }
    }

    Err(anyhow::anyhow!("No non-loopback IPv4 address found"))
}

/// Bootstrap the kubernetes Service and Endpoints in the default namespace
/// This ensures the "kubernetes" service always points to this API server
pub async fn bootstrap_kubernetes_service(
    storage: Arc<StorageBackend>,
    api_server_port: u16,
) -> Result<()> {
    info!("Bootstrapping kubernetes Service and Endpoints");

    // Get the API server's IP address
    let api_server_ip = get_api_server_ip().context("Failed to discover API server IP address")?;

    info!(
        "API server IP: {}, Port: {}",
        api_server_ip, api_server_port
    );

    // Check if the kubernetes Endpoints already exist
    let endpoints_key = "/registry/endpoints/default/kubernetes";

    match storage.get::<Endpoints>(endpoints_key).await {
        Ok(mut endpoints) => {
            // Update existing endpoints
            info!("Updating existing kubernetes Endpoints");
            // endpoints.subsets is Vec<EndpointSubset>
            if let Some(subset) = endpoints.subsets.first_mut() {
                // subset.addresses is Option<Vec<EndpointAddress>>
                if let Some(addresses) = &mut subset.addresses {
                    if let Some(addr) = addresses.first_mut() {
                        if addr.ip != api_server_ip {
                            info!(
                                "Updating API server IP from {} to {}",
                                addr.ip, api_server_ip
                            );
                            addr.ip = api_server_ip.clone();
                            storage
                                .update(endpoints_key, &endpoints)
                                .await
                                .context("Failed to update kubernetes Endpoints")?;
                        } else {
                            info!("API server IP already correct: {}", api_server_ip);
                        }
                    }
                }
            }
        }
        Err(_) => {
            // Create new Endpoints if they don't exist
            info!("kubernetes Endpoints not found - creating new Endpoints");
            use rusternetes_common::resources::{EndpointAddress, EndpointPort, EndpointSubset};
            use rusternetes_common::types::{ObjectMeta, TypeMeta};

            let mut metadata = ObjectMeta::new("kubernetes");
            metadata.namespace = Some("default".to_string());

            let endpoints = Endpoints {
                type_meta: TypeMeta {
                    kind: "Endpoints".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata,
                subsets: vec![EndpointSubset {
                    addresses: Some(vec![EndpointAddress {
                        ip: api_server_ip.clone(),
                        hostname: None,
                        node_name: None,
                        target_ref: None,
                    }]),
                    not_ready_addresses: None,
                    ports: Some(vec![EndpointPort {
                        name: Some("https".to_string()),
                        port: api_server_port,
                        protocol: Some("TCP".to_string()),
                        app_protocol: None,
                    }]),
                }],
            };

            storage
                .create(endpoints_key, &endpoints)
                .await
                .context("Failed to create kubernetes Endpoints")?;
            info!("Created kubernetes Endpoints with IP: {}", api_server_ip);
        }
    }

    // Also create/update an EndpointSlice for the kubernetes service.
    // The conformance test "should have Endpoints and EndpointSlices pointing to API Server"
    // (apiserver.go) expects both Endpoints AND EndpointSlices to exist for the "kubernetes" service.
    // The mirroring controller would handle this eventually, but bootstrapping it directly
    // ensures it's available immediately.
    let es_key = "/registry/endpointslices/default/kubernetes";
    match storage.get::<EndpointSlice>(es_key).await {
        Ok(mut es) => {
            // Update existing EndpointSlice
            let needs_update = es
                .endpoints
                .first()
                .map(|ep| ep.addresses.first().map(|a| a.as_str()) != Some(api_server_ip.as_str()))
                .unwrap_or(true);
            if needs_update {
                es.endpoints = vec![rusternetes_common::resources::endpointslice::Endpoint {
                    addresses: vec![api_server_ip.clone()],
                    conditions: Some(
                        rusternetes_common::resources::endpointslice::EndpointConditions {
                            ready: Some(true),
                            serving: Some(true),
                            terminating: Some(false),
                        },
                    ),
                    hostname: None,
                    target_ref: None,
                    node_name: None,
                    zone: None,
                    hints: None,
                    deprecated_topology: None,
                }];
                storage
                    .update(es_key, &es)
                    .await
                    .context("Failed to update kubernetes EndpointSlice")?;
                info!(
                    "Updated kubernetes EndpointSlice with IP: {}",
                    api_server_ip
                );
            }
        }
        Err(_) => {
            use rusternetes_common::resources::endpointslice::{
                Endpoint, EndpointConditions, EndpointPort,
            };
            use rusternetes_common::types::ObjectMeta;

            let mut metadata = ObjectMeta::new("kubernetes");
            metadata.namespace = Some("default".to_string());
            let mut labels = std::collections::HashMap::new();
            labels.insert(
                "kubernetes.io/service-name".to_string(),
                "kubernetes".to_string(),
            );
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-mirroring-controller.k8s.io".to_string(),
            );
            metadata.labels = Some(labels);
            metadata.ensure_uid();
            metadata.ensure_creation_timestamp();

            let es = EndpointSlice {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "EndpointSlice".to_string(),
                    api_version: "discovery.k8s.io/v1".to_string(),
                },
                metadata,
                address_type: "IPv4".to_string(),
                endpoints: vec![Endpoint {
                    addresses: vec![api_server_ip.clone()],
                    conditions: Some(EndpointConditions {
                        ready: Some(true),
                        serving: Some(true),
                        terminating: Some(false),
                    }),
                    hostname: None,
                    target_ref: None,
                    node_name: None,
                    zone: None,
                    hints: None,
                    deprecated_topology: None,
                }],
                ports: vec![EndpointPort {
                    name: Some("https".to_string()),
                    port: Some(api_server_port as i32),
                    protocol: Some("TCP".to_string()),
                    app_protocol: None,
                }],
            };

            storage
                .create(es_key, &es)
                .await
                .context("Failed to create kubernetes EndpointSlice")?;
            info!(
                "Created kubernetes EndpointSlice with IP: {}",
                api_server_ip
            );
        }
    }

    Ok(())
}
