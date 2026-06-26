//! SPDY handlers for pod port-forward.
//!
//! exec and attach are now upgrade-proxied to the pod's kubelet
//! via `rusternetes_streamproxy::proxy_upgrade` (Task 6).

use crate::spdy::{SpdyChannel, SpdyConnection};
use rusternetes_common::resources::Pod;
use std::sync::Arc;
use tracing::{error, info};

/// Handle SPDY port-forward connection
pub async fn handle_spdy_portforward(spdy: SpdyConnection, pod: Pod, ports: Vec<u16>) {
    tracing::debug!(
        "SPDY port-forward: pod={}, ports={:?}",
        pod.metadata.name,
        ports
    );

    // Get pod IP from status
    let pod_ip = match &pod.status {
        Some(status) => match &status.pod_ip {
            Some(ip) => ip.clone(),
            None => {
                let _ = spdy.write_error("Pod has no IP address assigned").await;
                let _ = spdy.close().await;
                return;
            }
        },
        None => {
            let _ = spdy.write_error("Pod has no status").await;
            let _ = spdy.close().await;
            return;
        }
    };

    let spdy = Arc::new(spdy);

    for port in ports {
        let pod_ip = pod_ip.clone();
        let spdy_clone = Arc::clone(&spdy);

        tokio::spawn(async move {
            match setup_port_forward(spdy_clone, &pod_ip, port).await {
                Ok(_) => info!("Port-forward for port {} completed", port),
                Err(e) => error!("Port-forward for port {} failed: {}", port, e),
            }
        });
    }

    // Keep connection alive
    loop {
        match spdy.read_frame().await {
            Ok(None) => break,
            Ok(Some(_)) => {}
            Err(_) => break,
        }
    }

    let _ = spdy.close().await;
}

/// Set up TCP proxy for a single port
async fn setup_port_forward(
    spdy: Arc<SpdyConnection>,
    pod_ip: &str,
    port: u16,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let target = format!("{}:{}", pod_ip, port);
    info!("Setting up port-forward to {}", target);

    let tcp = TcpStream::connect(&target)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", target, e))?;

    let (mut tcp_read, mut tcp_write) = tcp.into_split();

    // SPDY → TCP
    let spdy_to_tcp = Arc::clone(&spdy);
    tokio::spawn(async move {
        loop {
            match spdy_to_tcp.read_frame().await {
                Ok(Some(frame)) if frame.channel == SpdyChannel::Stdin => {
                    if let Err(e) = tcp_write.write_all(&frame.data).await {
                        tracing::error!(task = "spdy_to_tcp", error = %e, "TCP write failed");
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!(task = "spdy_to_tcp", error = %e, "SPDY read_frame failed");
                    break;
                }
                _ => {}
            }
        }
    });

    // TCP → SPDY
    let mut buf = vec![0u8; 8192];
    loop {
        match tcp_read.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                if spdy
                    .write_channel(SpdyChannel::Stdout, buf[..n].to_vec())
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    Ok(())
}
