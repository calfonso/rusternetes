use crate::client::ApiClient;
use crate::websocket;
use anyhow::Result;

/// Forward one or more local ports to a pod
pub async fn execute(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    ports: &[String],
    address: &str,
) -> Result<()> {
    if ports.is_empty() {
        anyhow::bail!("At least one port must be specified");
    }

    // Parse port mapping (format: "local:remote" or "port")
    let (local_port, remote_port) = parse_port_mapping(&ports[0])?;

    // Build WebSocket URL
    let url_path = format!(
        "/api/v1/namespaces/{}/pods/{}/portforward?ports={}",
        namespace, pod_name, remote_port
    );

    let ws_url = client.get_ws_url(&url_path)?;

    // Start port forwarding
    websocket::port_forward_stream(ws_url, local_port, remote_port, address).await
}

fn parse_port_mapping(port_spec: &str) -> Result<(u16, u16)> {
    if let Some((local, remote)) = port_spec.split_once(':') {
        let local_port = local.parse::<u16>()?;
        let remote_port = remote.parse::<u16>()?;
        Ok((local_port, remote_port))
    } else {
        // If only one port specified, use same for both local and remote
        let port = port_spec.parse::<u16>()?;
        Ok((port, port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_port_mapping_local_remote() {
        let (local, remote) = parse_port_mapping("8080:80").unwrap();
        assert_eq!(local, 8080);
        assert_eq!(remote, 80);
    }

    #[test]
    fn test_parse_port_mapping_same_port() {
        let (local, remote) = parse_port_mapping("3000").unwrap();
        assert_eq!(local, 3000);
        assert_eq!(remote, 3000);
    }

    #[test]
    fn test_parse_port_mapping_invalid() {
        let result = parse_port_mapping("abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_port_mapping_invalid_local() {
        let result = parse_port_mapping("abc:80");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_port_mapping_invalid_remote() {
        let result = parse_port_mapping("80:abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_port_mapping_high_port() {
        let (local, remote) = parse_port_mapping("65535:443").unwrap();
        assert_eq!(local, 65535);
        assert_eq!(remote, 443);
    }

    #[test]
    fn test_parse_port_mapping_overflow_fails() {
        // Port number exceeding u16 max should fail
        let result = parse_port_mapping("70000");
        assert!(result.is_err());
    }
}
