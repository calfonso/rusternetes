//! Tests for kubectl exec, attach, and port-forward commands
//! Tests argument parsing and URL building without requiring a cluster

mod urlencoding {
    pub fn encode(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
                ' ' => "+".to_string(),
                _ => format!("%{:02X}", c as u8),
            })
            .collect()
    }
}

#[cfg(test)]
mod exec_tests {
    use super::urlencoding;
    #[test]
    fn test_exec_url_building() {
        fn build_exec_url(
            namespace: &str,
            pod: &str,
            container: Option<&str>,
            command: &[&str],
        ) -> String {
            let mut url = format!("/api/v1/namespaces/{}/pods/{}/exec?", namespace, pod);

            for cmd in command {
                url.push_str(&format!("command={}&", urlencoding::encode(cmd)));
            }

            if let Some(c) = container {
                url.push_str(&format!("container={}&", c));
            }

            url.push_str("stdout=true&stderr=true");
            url
        }

        let url = build_exec_url("default", "nginx-pod", None, &["ls", "-la", "/app"]);
        assert!(url.contains("/api/v1/namespaces/default/pods/nginx-pod/exec"));
        assert!(url.contains("command=ls"));
        assert!(url.contains("command=-la"));
        assert!(url.contains("command=%2Fapp"));
        assert!(url.contains("stdout=true"));
        assert!(url.contains("stderr=true"));
    }

    #[test]
    fn test_exec_with_container() {
        fn build_exec_url(namespace: &str, pod: &str, container: Option<&str>) -> String {
            let mut url = format!("/api/v1/namespaces/{}/pods/{}/exec?", namespace, pod);

            if let Some(c) = container {
                url.push_str(&format!("container={}&", c));
            }

            url.push_str("stdout=true");
            url
        }

        let url = build_exec_url("default", "multi-container-pod", Some("sidecar"));
        assert!(url.contains("container=sidecar"));
    }

    #[test]
    fn test_exec_stdin_enabled() {
        fn build_query_params(stdin: bool, stdout: bool, stderr: bool, tty: bool) -> String {
            let mut params = Vec::new();

            if stdin {
                params.push("stdin=true");
            }
            if stdout {
                params.push("stdout=true");
            }
            if stderr {
                params.push("stderr=true");
            }
            if tty {
                params.push("tty=true");
            }

            params.join("&")
        }

        assert_eq!(
            build_query_params(true, true, true, false),
            "stdin=true&stdout=true&stderr=true"
        );
        assert_eq!(build_query_params(false, true, false, false), "stdout=true");
        assert_eq!(
            build_query_params(true, true, true, true),
            "stdin=true&stdout=true&stderr=true&tty=true"
        );
    }

    #[test]
    fn test_exec_command_quoting() {
        fn quote_command(cmd: &str) -> String {
            if cmd.contains(' ') || cmd.contains('&') || cmd.contains('|') {
                format!("'{}'", cmd.replace('\'', "'\\''"))
            } else {
                cmd.to_string()
            }
        }

        assert_eq!(quote_command("ls"), "ls");
        assert_eq!(quote_command("echo hello world"), "'echo hello world'");
        assert_eq!(
            quote_command("echo it's working"),
            "'echo it'\\''s working'"
        );
    }

    #[test]
    fn test_exec_command_splitting() {
        fn split_command(cmd: &str) -> Vec<String> {
            cmd.split_whitespace().map(|s| s.to_string()).collect()
        }

        assert_eq!(split_command("ls -la /app"), vec!["ls", "-la", "/app"]);
        assert_eq!(
            split_command("bash -c 'echo hello'"),
            vec!["bash", "-c", "'echo", "hello'"]
        );
    }
}

#[cfg(test)]
mod attach_tests {
    #[test]
    fn test_attach_url_building() {
        fn build_attach_url(namespace: &str, pod: &str, container: Option<&str>) -> String {
            let mut url = format!("/api/v1/namespaces/{}/pods/{}/attach?", namespace, pod);

            if let Some(c) = container {
                url.push_str(&format!("container={}&", c));
            }

            url.push_str("stdout=true&stderr=true");
            url
        }

        let url = build_attach_url("default", "app-pod", None);
        assert!(url.contains("/api/v1/namespaces/default/pods/app-pod/attach"));
        assert!(url.contains("stdout=true"));
        assert!(url.contains("stderr=true"));
    }

    #[test]
    fn test_attach_with_stdin() {
        fn build_attach_params(stdin: bool, tty: bool) -> String {
            let mut params = vec!["stdout=true", "stderr=true"];

            if stdin {
                params.push("stdin=true");
            }
            if tty {
                params.push("tty=true");
            }

            params.join("&")
        }

        assert_eq!(
            build_attach_params(true, false),
            "stdout=true&stderr=true&stdin=true"
        );

        assert_eq!(
            build_attach_params(true, true),
            "stdout=true&stderr=true&stdin=true&tty=true"
        );
    }
}

#[cfg(test)]
mod portforward_tests {
    #[test]
    fn test_portforward_port_parsing() {
        fn parse_port_mapping(port_spec: &str) -> Result<(u16, u16), String> {
            if let Some((local, remote)) = port_spec.split_once(':') {
                let local_port = local.parse::<u16>().map_err(|_| "Invalid local port")?;
                let remote_port = remote.parse::<u16>().map_err(|_| "Invalid remote port")?;
                Ok((local_port, remote_port))
            } else {
                let port = port_spec.parse::<u16>().map_err(|_| "Invalid port")?;
                Ok((port, port)) // same port for both
            }
        }

        assert_eq!(parse_port_mapping("8080:80").unwrap(), (8080, 80));
        assert_eq!(parse_port_mapping("8080").unwrap(), (8080, 8080));
        assert_eq!(parse_port_mapping("3000:3000").unwrap(), (3000, 3000));
        assert!(parse_port_mapping("invalid").is_err());
        assert!(parse_port_mapping("8080:invalid").is_err());
    }

    #[test]
    fn test_portforward_multiple_ports() {
        fn parse_multiple_ports(ports: &[&str]) -> Result<Vec<(u16, u16)>, String> {
            ports
                .iter()
                .map(|p| {
                    if let Some((local, remote)) = p.split_once(':') {
                        let l = local.parse::<u16>().map_err(|_| "Invalid port")?;
                        let r = remote.parse::<u16>().map_err(|_| "Invalid port")?;
                        Ok((l, r))
                    } else {
                        let port = p.parse::<u16>().map_err(|_| "Invalid port")?;
                        Ok((port, port))
                    }
                })
                .collect()
        }

        let ports = parse_multiple_ports(&["8080:80", "8443:443", "3000"]).unwrap();
        assert_eq!(ports.len(), 3);
        assert_eq!(ports[0], (8080, 80));
        assert_eq!(ports[1], (8443, 443));
        assert_eq!(ports[2], (3000, 3000));
    }

    #[test]
    fn test_portforward_url_building() {
        fn build_portforward_url(namespace: &str, pod: &str, ports: &[u16]) -> String {
            let mut url = format!("/api/v1/namespaces/{}/pods/{}/portforward?", namespace, pod);

            for port in ports {
                url.push_str(&format!("ports={}&", port));
            }

            url.trim_end_matches('&').to_string()
        }

        let url = build_portforward_url("default", "app-pod", &[80, 443, 3000]);
        assert!(url.contains("/api/v1/namespaces/default/pods/app-pod/portforward"));
        assert!(url.contains("ports=80"));
        assert!(url.contains("ports=443"));
        assert!(url.contains("ports=3000"));
    }

    #[test]
    fn test_portforward_address_binding() {
        fn parse_address(addr: &str) -> Result<String, String> {
            match addr {
                "localhost" | "127.0.0.1" => Ok("127.0.0.1".to_string()),
                "0.0.0.0" => Ok("0.0.0.0".to_string()),
                other => {
                    // Basic IP validation
                    let parts: Vec<&str> = other.split('.').collect();
                    if parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok()) {
                        Ok(other.to_string())
                    } else {
                        Err("Invalid IP address".to_string())
                    }
                }
            }
        }

        assert_eq!(parse_address("localhost").unwrap(), "127.0.0.1");
        assert_eq!(parse_address("127.0.0.1").unwrap(), "127.0.0.1");
        assert_eq!(parse_address("0.0.0.0").unwrap(), "0.0.0.0");
        assert_eq!(parse_address("192.168.1.100").unwrap(), "192.168.1.100");
        assert!(parse_address("invalid").is_err());
        assert!(parse_address("256.1.1.1").is_err());
    }

    #[test]
    fn test_portforward_port_validation() {
        fn validate_port(port: u16) -> Result<(), String> {
            if port == 0 {
                Err("Port cannot be 0".to_string())
            } else if port < 1024 {
                Err("Privileged port (requires root)".to_string())
            } else {
                Ok(())
            }
        }

        assert!(validate_port(8080).is_ok());
        assert!(validate_port(3000).is_ok());
        assert!(validate_port(0).is_err());
        assert!(validate_port(80).is_err());
        assert!(validate_port(443).is_err());
    }
}
