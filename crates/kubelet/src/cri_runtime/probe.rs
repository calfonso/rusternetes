//! Single-attempt container probing for the CRI runtime.
//!
//! The kubelet owns the probe *state machine* (initial delay, period, success/
//! failure thresholds) — that is runtime-agnostic. What is runtime-specific is
//! executing one probe attempt against a container, which is what
//! [`super::CriContainerRuntime::probe_container`] does: exec probes run via the
//! CRI `ExecSync`, while http/tcp probes are dialed from the node against the
//! pod IP. This module holds the pure helpers (port resolution).

use rusternetes_common::resources::pod::Container;
use rusternetes_common::resources::policy::IntOrString;

/// Resolve a probe port to a numeric container port. Integer ports pass through;
/// named ports are looked up in the container's declared `ports`. Returns `None`
/// if a named port is not declared.
pub fn resolve_port(container: &Container, port: &IntOrString) -> Option<i32> {
    match port {
        IntOrString::Int(n) => Some(*n),
        IntOrString::String(name) => container.ports.as_ref().and_then(|ports| {
            ports
                .iter()
                .find(|p| p.name.as_deref() == Some(name.as_str()))
                .map(|p| i32::from(p.container_port))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::pod::ContainerPort;

    fn container_with_named_port() -> Container {
        Container {
            name: "web".to_string(),
            image: "nginx".to_string(),
            ports: Some(vec![ContainerPort {
                container_port: 8080,
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                host_port: None,
                host_ip: None,
            }]),
            ..Default::default()
        }
    }

    #[test]
    fn int_port_passes_through() {
        let c = container_with_named_port();
        assert_eq!(resolve_port(&c, &IntOrString::Int(443)), Some(443));
    }

    #[test]
    fn named_port_resolves_from_spec() {
        let c = container_with_named_port();
        assert_eq!(
            resolve_port(&c, &IntOrString::String("http".to_string())),
            Some(8080)
        );
    }

    #[test]
    fn unknown_named_port_is_none() {
        let c = container_with_named_port();
        assert_eq!(
            resolve_port(&c, &IntOrString::String("grpc".to_string())),
            None
        );
    }
}
