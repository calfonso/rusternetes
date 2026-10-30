//! Test-only fake CSI node plugin with a registration socket, served over
//! unix sockets like a real driver plus `node-driver-registrar`. It records
//! every call and behaves like a well-mannered driver: publish creates the
//! target, unpublish removes it, and repeats are fine.

use super::proto::csi::v1 as csipb;
use super::proto::pluginregistration as regpb;
use csipb::node_server::{Node, NodeServer};
use regpb::registration_server::{Registration, RegistrationServer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(crate) struct FakeOptions {
    pub name: String,
    pub plugin_type: String,
    pub versions: Vec<String>,
    pub stage_unstage: bool,
    /// Fail these ops (e.g. `"NodePublishVolume"`) with this code.
    pub fail: HashMap<&'static str, tonic::Code>,
}

impl Default for FakeOptions {
    fn default() -> Self {
        Self {
            name: "fake.csi.rusternetes.io".to_string(),
            plugin_type: "CSIPlugin".to_string(),
            versions: vec!["1.0.0".to_string()],
            stage_unstage: true,
            fail: HashMap::new(),
        }
    }
}

#[derive(Default)]
pub(crate) struct Recorded {
    pub calls: Vec<String>,
    pub publish: Vec<csipb::NodePublishVolumeRequest>,
    pub stage: Vec<csipb::NodeStageVolumeRequest>,
}

#[derive(Clone)]
struct State {
    opts: Arc<Mutex<FakeOptions>>,
    rec: Arc<Mutex<Recorded>>,
    endpoint: PathBuf,
}

impl State {
    #[allow(clippy::result_large_err)] // tonic::Status is the gRPC error type
    fn record(&self, op: &'static str, detail: String) -> Result<(), Status> {
        self.rec
            .lock()
            .unwrap()
            .calls
            .push(format!("{} {}", op, detail).trim_end().to_string());
        match self.opts.lock().unwrap().fail.get(op) {
            Some(code) => Err(Status::new(*code, format!("fake {} failure", op))),
            None => Ok(()),
        }
    }
}

pub(crate) struct FakePlugin {
    pub reg_socket: PathBuf,
    state: State,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl FakePlugin {
    /// Serve the driver on `<plugin_dir>/csi.sock` and its registration
    /// socket on `<registry_dir>/<name>-reg.sock`.
    pub async fn start(registry_dir: &Path, plugin_dir: &Path, opts: FakeOptions) -> Self {
        std::fs::create_dir_all(registry_dir).unwrap();
        std::fs::create_dir_all(plugin_dir).unwrap();
        let endpoint = plugin_dir.join("csi.sock");
        let reg_socket = registry_dir.join(format!("{}-reg.sock", opts.name));
        let state = State {
            opts: Arc::new(Mutex::new(opts)),
            rec: Arc::new(Mutex::new(Recorded::default())),
            endpoint: endpoint.clone(),
        };
        let node = UnixListener::bind(&endpoint).unwrap();
        let reg = UnixListener::bind(&reg_socket).unwrap();
        let s1 = state.clone();
        let s2 = state.clone();
        let tasks = vec![
            tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(NodeServer::new(FakeNode(s1)))
                    .serve_with_incoming(UnixListenerStream::new(node))
                    .await;
            }),
            tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(RegistrationServer::new(FakeRegistration(s2)))
                    .serve_with_incoming(UnixListenerStream::new(reg))
                    .await;
            }),
        ];
        Self {
            reg_socket,
            state,
            tasks,
        }
    }

    pub fn calls(&self) -> Vec<String> {
        self.state.rec.lock().unwrap().calls.clone()
    }

    pub fn clear(&self) {
        self.state.rec.lock().unwrap().calls.clear();
    }

    pub fn recorded<T>(&self, f: impl FnOnce(&Recorded) -> T) -> T {
        f(&self.state.rec.lock().unwrap())
    }

    pub fn set_fail(&self, op: &'static str, code: Option<tonic::Code>) {
        let mut opts = self.state.opts.lock().unwrap();
        match code {
            Some(c) => opts.fail.insert(op, c),
            None => opts.fail.remove(op),
        };
    }

    /// Stop serving and remove the registration socket, as when the
    /// driver pod goes away.
    pub fn stop(&mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
        let _ = std::fs::remove_file(&self.reg_socket);
        let _ = std::fs::remove_file(&self.state.endpoint);
    }
}

impl Drop for FakePlugin {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

struct FakeRegistration(State);

#[tonic::async_trait]
impl Registration for FakeRegistration {
    async fn get_info(
        &self,
        _: Request<regpb::InfoRequest>,
    ) -> Result<Response<regpb::PluginInfo>, Status> {
        self.0.record("GetInfo", String::new())?;
        let opts = self.0.opts.lock().unwrap().clone();
        Ok(Response::new(regpb::PluginInfo {
            r#type: opts.plugin_type,
            name: opts.name,
            endpoint: self.0.endpoint.to_string_lossy().into_owned(),
            supported_versions: opts.versions,
        }))
    }

    async fn notify_registration_status(
        &self,
        req: Request<regpb::RegistrationStatus>,
    ) -> Result<Response<regpb::RegistrationStatusResponse>, Status> {
        let r = req.into_inner();
        self.0.record(
            "NotifyRegistrationStatus",
            format!("{} {}", r.plugin_registered, r.error),
        )?;
        Ok(Response::new(regpb::RegistrationStatusResponse {}))
    }
}

struct FakeNode(State);

#[tonic::async_trait]
impl Node for FakeNode {
    async fn node_stage_volume(
        &self,
        req: Request<csipb::NodeStageVolumeRequest>,
    ) -> Result<Response<csipb::NodeStageVolumeResponse>, Status> {
        let r = req.into_inner();
        self.0.record(
            "NodeStageVolume",
            format!("{} {}", r.volume_id, r.staging_target_path),
        )?;
        self.0.rec.lock().unwrap().stage.push(r);
        Ok(Response::new(csipb::NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        req: Request<csipb::NodeUnstageVolumeRequest>,
    ) -> Result<Response<csipb::NodeUnstageVolumeResponse>, Status> {
        let r = req.into_inner();
        self.0.record(
            "NodeUnstageVolume",
            format!("{} {}", r.volume_id, r.staging_target_path),
        )?;
        Ok(Response::new(csipb::NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        req: Request<csipb::NodePublishVolumeRequest>,
    ) -> Result<Response<csipb::NodePublishVolumeResponse>, Status> {
        let r = req.into_inner();
        self.0.record(
            "NodePublishVolume",
            format!("{} {}", r.volume_id, r.target_path),
        )?;
        std::fs::create_dir_all(&r.target_path).map_err(|e| Status::internal(e.to_string()))?;
        self.0.rec.lock().unwrap().publish.push(r);
        Ok(Response::new(csipb::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        req: Request<csipb::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<csipb::NodeUnpublishVolumeResponse>, Status> {
        let r = req.into_inner();
        self.0.record(
            "NodeUnpublishVolume",
            format!("{} {}", r.volume_id, r.target_path),
        )?;
        let _ = std::fs::remove_dir(&r.target_path);
        Ok(Response::new(csipb::NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        _: Request<csipb::NodeGetVolumeStatsRequest>,
    ) -> Result<Response<csipb::NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn node_get_volume_health(
        &self,
        _: Request<csipb::NodeGetVolumeHealthRequest>,
    ) -> Result<Response<csipb::NodeGetVolumeHealthResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn node_get_storage_health(
        &self,
        _: Request<csipb::NodeGetStorageHealthRequest>,
    ) -> Result<Response<csipb::NodeGetStorageHealthResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn node_expand_volume(
        &self,
        _: Request<csipb::NodeExpandVolumeRequest>,
    ) -> Result<Response<csipb::NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn node_get_capabilities(
        &self,
        _: Request<csipb::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<csipb::NodeGetCapabilitiesResponse>, Status> {
        self.0.record("NodeGetCapabilities", String::new())?;
        use csipb::node_service_capability::{rpc::Type, Rpc, Type as CapType};
        let capabilities = if self.0.opts.lock().unwrap().stage_unstage {
            vec![csipb::NodeServiceCapability {
                r#type: Some(CapType::Rpc(Rpc {
                    r#type: Type::StageUnstageVolume as i32,
                })),
            }]
        } else {
            Vec::new()
        };
        Ok(Response::new(csipb::NodeGetCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn node_get_info(
        &self,
        _: Request<csipb::NodeGetInfoRequest>,
    ) -> Result<Response<csipb::NodeGetInfoResponse>, Status> {
        self.0.record("NodeGetInfo", String::new())?;
        Ok(Response::new(csipb::NodeGetInfoResponse {
            node_id: "fake-node-id".to_string(),
            max_volumes_per_node: 8,
            accessible_topology: Some(csipb::Topology {
                segments: [("topology.fake/zone".to_string(), "z1".to_string())].into(),
            }),
        }))
    }
}
