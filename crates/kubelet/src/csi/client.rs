//! gRPC channels to plugins over unix sockets.

use anyhow::{Context, Result};
use std::path::Path;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint, Uri};

/// Connect to a gRPC server on a unix socket. The channel reconnects to the
/// same path on its own if the plugin restarts.
pub async fn connect_unix(path: &Path) -> Result<Channel> {
    let socket = path.to_path_buf();
    // The URI is required by tonic but unused: the connector picks the socket.
    Endpoint::try_from("http://[::]:50051")?
        .connect_timeout(Duration::from_secs(5))
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    tokio::net::UnixStream::connect(socket).await?,
                ))
            }
        }))
        .await
        .with_context(|| format!("connecting to {}", path.display()))
}
