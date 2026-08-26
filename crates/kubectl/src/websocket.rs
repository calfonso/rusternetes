/// WebSocket streaming support for kubectl
///
/// Implements the Kubernetes streaming protocol for exec, attach, and port-forward
use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

/// Kubernetes streaming protocol channels
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamChannel {
    Stdin = 0,
    Stdout = 1,
    Stderr = 2,
    Error = 3,
    Resize = 4,
}

impl StreamChannel {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Stdin),
            1 => Some(Self::Stdout),
            2 => Some(Self::Stderr),
            3 => Some(Self::Error),
            4 => Some(Self::Resize),
            _ => None,
        }
    }
}

/// A message in the Kubernetes streaming protocol
#[derive(Debug, Clone)]
pub struct StreamMessage {
    pub channel: StreamChannel,
    pub data: Bytes,
}

impl StreamMessage {
    pub fn new(channel: StreamChannel, data: impl Into<Bytes>) -> Self {
        Self {
            channel,
            data: data.into(),
        }
    }

    pub fn stdin(data: impl Into<Bytes>) -> Self {
        Self::new(StreamChannel::Stdin, data)
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(anyhow!("Empty message"));
        }

        let channel = StreamChannel::from_u8(data[0])
            .ok_or_else(|| anyhow!("Invalid channel: {}", data[0]))?;

        let payload = if data.len() > 1 {
            Bytes::copy_from_slice(&data[1..])
        } else {
            Bytes::new()
        };

        Ok(Self {
            channel,
            data: payload,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + self.data.len());
        buf.push(self.channel as u8);
        buf.extend_from_slice(&self.data);
        buf
    }
}

/// Execute a command in a pod with WebSocket streaming
pub async fn exec_stream(ws_url: String, stdin_enabled: bool, _tty_enabled: bool) -> Result<()> {
    // Parse URL
    let url = Url::parse(&ws_url)?;

    // Connect WebSocket
    let (ws_stream, _) = connect_async(url)
        .await
        .map_err(|e| anyhow!("Failed to connect WebSocket: {}", e))?;

    let (mut write, mut read) = ws_stream.split();

    // Spawn task to read from WebSocket and write to stdout/stderr
    let read_task = tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if let Ok(stream_msg) = StreamMessage::decode(&data) {
                        match stream_msg.channel {
                            StreamChannel::Stdout => {
                                let mut stdout = tokio::io::stdout();
                                if stdout.write_all(&stream_msg.data).await.is_err() {
                                    break;
                                }
                                let _ = stdout.flush().await;
                            }
                            StreamChannel::Stderr => {
                                let mut stderr = tokio::io::stderr();
                                if stderr.write_all(&stream_msg.data).await.is_err() {
                                    break;
                                }
                                let _ = stderr.flush().await;
                            }
                            StreamChannel::Error => {
                                eprintln!("Error: {}", String::from_utf8_lossy(&stream_msg.data));
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    eprintln!("websocket read task error: {e}");
                    break;
                }
                _ => {}
            }
        }
    });

    // If stdin is enabled, read from stdin and write to WebSocket
    if stdin_enabled {
        let write_task = tokio::spawn(async move {
            let mut stdin = tokio::io::stdin();
            let mut buf = vec![0u8; 8192];

            loop {
                match stdin.read(&mut buf).await {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        // Copy data to avoid borrow issues
                        let data = buf[..n].to_vec();
                        let msg = StreamMessage::stdin(data);
                        let encoded = msg.encode();

                        if write.send(Message::Binary(encoded)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("websocket stdin read error: {e}");
                        break;
                    }
                }
            }

            let _ = write.close().await;
        });

        // Wait for either task to complete
        tokio::select! {
            _ = read_task => {}
            _ = write_task => {}
        }
    } else {
        // Just wait for reading to complete
        let _ = read_task.await;
    }

    Ok(())
}

/// Port-forward frame for TCP tunneling
pub struct PortForwardFrame {
    pub port: u16,
    pub stream_type: u8, // 0 = data, 1 = error
    pub data: Bytes,
}

impl PortForwardFrame {
    pub fn new(port: u16, stream_type: u8, data: impl Into<Bytes>) -> Self {
        Self {
            port,
            stream_type,
            data: data.into(),
        }
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 3 {
            return Err(anyhow!("Port-forward frame too short"));
        }

        let port = u16::from_be_bytes([data[0], data[1]]);
        let stream_type = data[2];
        let payload = if data.len() > 3 {
            Bytes::copy_from_slice(&data[3..])
        } else {
            Bytes::new()
        };

        Ok(Self {
            port,
            stream_type,
            data: payload,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(3 + self.data.len());
        buf.push((self.port >> 8) as u8);
        buf.push((self.port & 0xff) as u8);
        buf.push(self.stream_type);
        buf.extend_from_slice(&self.data);
        buf
    }
}

/// Forward a local port to a pod port
pub async fn port_forward_stream(
    ws_url: String,
    local_port: u16,
    remote_port: u16,
    bind_address: &str,
) -> Result<()> {
    use tokio::net::TcpListener;

    // Parse WebSocket URL
    let url = Url::parse(&ws_url)?;

    // Bind local TCP listener
    let listener = TcpListener::bind(format!("{}:{}", bind_address, local_port)).await?;

    println!(
        "Forwarding from {}:{} -> pod port {}",
        bind_address, local_port, remote_port
    );

    loop {
        // Accept connection
        let (tcp_stream, addr) = listener.accept().await?;
        println!("Connection from {}", addr);

        // Clone URL for this connection
        let ws_url = url.clone();

        // Spawn handler for this connection
        tokio::spawn(async move {
            if let Err(e) = handle_port_forward_connection(tcp_stream, ws_url, remote_port).await {
                eprintln!("Port-forward error: {}", e);
            }
        });
    }
}

async fn handle_port_forward_connection(
    tcp_stream: tokio::net::TcpStream,
    ws_url: Url,
    remote_port: u16,
) -> Result<()> {
    // Connect WebSocket
    let (ws_stream, _) = connect_async(ws_url).await?;
    let (mut ws_write, mut ws_read) = ws_stream.split();

    // Split TCP stream
    let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();

    // Spawn task to forward TCP -> WebSocket
    let tcp_to_ws = tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    // Copy data to avoid borrow issues
                    let data = buf[..n].to_vec();
                    let frame = PortForwardFrame::new(remote_port, 0, data);
                    if ws_write
                        .send(Message::Binary(frame.encode()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("port-forward TCP read error: {e}");
                    break;
                }
            }
        }
        let _ = ws_write.close().await;
    });

    // Spawn task to forward WebSocket -> TCP
    let ws_to_tcp = tokio::spawn(async move {
        while let Some(msg) = ws_read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if let Ok(frame) = PortForwardFrame::decode(&data) {
                        if frame.stream_type == 0 && !frame.data.is_empty() {
                            if tcp_write.write_all(&frame.data).await.is_err() {
                                break;
                            }
                        } else if frame.stream_type == 1 {
                            eprintln!(
                                "Port-forward error: {}",
                                String::from_utf8_lossy(&frame.data)
                            );
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    eprintln!("port-forward WebSocket read error: {e}");
                    break;
                }
                _ => {}
            }
        }
    });

    // Wait for either direction to close
    tokio::select! {
        _ = tcp_to_ws => {}
        _ = ws_to_tcp => {}
    }

    Ok(())
}
