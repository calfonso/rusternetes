/// Resolve the kubelet connection parameters from a `Node` resource.
///
/// Extracted from `proxy.rs` so that Tasks 6 and 7 (exec/attach/logs routing)
/// can reuse the same InternalIP→ExternalIP fallback, advertised-port lookup,
/// and scheme selection without duplicating the logic.
use rusternetes_common::resources::Node;

/// Resolved kubelet connection parameters for a node.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeConn {
    /// The hostname or IP address to connect to (InternalIP preferred, ExternalIP fallback).
    pub host: String,
    /// The TCP port of the kubelet API.
    pub port: u16,
    /// URL scheme. Always `"http"` because the rusternetes kubelet serves plain
    /// HTTP today (no TLS layer on the metrics listener).
    pub scheme: &'static str,
}

/// Resolve kubelet connection parameters from a node's status.
///
/// Port resolution order (mirrors `pkg/registry/core/node/strategy.go::ResourceLocation`):
/// 1. `port_override` — explicit port from the URL id (`<name>:<port>`).
/// 2. `status.daemonEndpoints.kubeletEndpoint.port` — advertised by the kubelet.
/// 3. 10250 — the Kubernetes default.
///
/// Returns `Err(rusternetes_common::Error::NotFound)` when the node has no
/// usable address.
pub fn node_conn(
    node: &Node,
    port_override: Option<u16>,
) -> Result<NodeConn, rusternetes_common::Error> {
    let host = node
        .status
        .as_ref()
        .and_then(|s| s.addresses.as_ref())
        .and_then(|addrs| {
            addrs
                .iter()
                .find(|a| a.address_type == "InternalIP")
                .or_else(|| addrs.iter().find(|a| a.address_type == "ExternalIP"))
        })
        .map(|a| a.address.clone())
        .ok_or_else(|| {
            rusternetes_common::Error::NotFound(format!(
                "No address found for node {}",
                node.metadata.name,
            ))
        })?;

    let port = port_override.unwrap_or_else(|| {
        node.status
            .as_ref()
            .and_then(|s| s.daemon_endpoints.as_ref())
            .and_then(|d| d.kubelet_endpoint.as_ref())
            .map(|e| e.port as u16)
            .unwrap_or(10250)
    });

    Ok(NodeConn {
        host,
        port,
        scheme: "http",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        DaemonEndpoint, Node, NodeAddress, NodeDaemonEndpoints, NodeStatus,
    };

    fn make_node(addresses: Vec<NodeAddress>, kubelet_port: Option<i32>) -> Node {
        let mut node = Node::new("test-node");
        node.status = Some(NodeStatus {
            addresses: Some(addresses),
            daemon_endpoints: kubelet_port.map(|p| NodeDaemonEndpoints {
                kubelet_endpoint: Some(DaemonEndpoint { port: p }),
            }),
            ..Default::default()
        });
        node
    }

    #[test]
    fn prefers_internal_ip_and_uses_advertised_port() {
        let node = make_node(
            vec![
                NodeAddress {
                    address_type: "ExternalIP".to_string(),
                    address: "1.2.3.4".to_string(),
                },
                NodeAddress {
                    address_type: "InternalIP".to_string(),
                    address: "192.168.1.10".to_string(),
                },
            ],
            Some(10255),
        );

        let conn = node_conn(&node, None).expect("should resolve");
        assert_eq!(conn.host, "192.168.1.10");
        assert_eq!(conn.port, 10255);
        assert_eq!(conn.scheme, "http");
    }

    #[test]
    fn falls_back_to_external_ip_when_no_internal() {
        let node = make_node(
            vec![NodeAddress {
                address_type: "ExternalIP".to_string(),
                address: "5.6.7.8".to_string(),
            }],
            None,
        );

        let conn = node_conn(&node, None).expect("should resolve");
        assert_eq!(conn.host, "5.6.7.8");
        assert_eq!(conn.port, 10250); // default
    }

    #[test]
    fn port_override_wins_over_advertised_port() {
        let node = make_node(
            vec![NodeAddress {
                address_type: "InternalIP".to_string(),
                address: "10.0.0.1".to_string(),
            }],
            Some(10255),
        );

        let conn = node_conn(&node, Some(9090)).expect("should resolve");
        assert_eq!(conn.port, 9090);
    }

    #[test]
    fn no_address_returns_not_found() {
        let node = make_node(vec![], None);
        let err = node_conn(&node, None).unwrap_err();
        assert!(
            matches!(err, rusternetes_common::Error::NotFound(_)),
            "expected NotFound, got: {err:?}"
        );
    }

    #[test]
    fn no_status_returns_not_found() {
        let node = Node::new("bare-node");
        let err = node_conn(&node, None).unwrap_err();
        assert!(matches!(err, rusternetes_common::Error::NotFound(_)));
    }

    #[test]
    fn scheme_is_always_http() {
        let node = make_node(
            vec![NodeAddress {
                address_type: "InternalIP".to_string(),
                address: "10.0.0.2".to_string(),
            }],
            None,
        );
        let conn = node_conn(&node, None).expect("should resolve");
        assert_eq!(conn.scheme, "http");
    }
}
