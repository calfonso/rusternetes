//! CSI tests against the fake node plugin.

use super::fake::{FakeOptions, FakePlugin};
use super::proto::csi::v1::volume_capability::access_mode::Mode;
use super::*;
use rusternetes_common::resources::csi::CSINode;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};

struct Env {
    root: tempfile::TempDir,
    volumes: tempfile::TempDir,
    manager: CsiManager,
}

impl Env {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let manager = CsiManager::new(root.path());
        Self {
            root,
            volumes: tempfile::tempdir().unwrap(),
            manager,
        }
    }

    async fn plugin(&self, opts: FakeOptions) -> FakePlugin {
        let plugin_dir = self.root.path().join("plugins").join(&opts.name);
        FakePlugin::start(&self.manager.plugins_registry_dir(), &plugin_dir, opts).await
    }

    /// Start a plugin and register it with one watcher scan.
    async fn registered(&self, opts: FakeOptions) -> FakePlugin {
        let plugin = self.plugin(opts).await;
        self.manager.plugin_watcher().scan().await;
        assert_eq!(self.manager.registry().names().len(), 1);
        plugin.clear();
        plugin
    }

    fn request(&self, pod: &str, handle: &str) -> MountRequest {
        MountRequest {
            driver: FakeOptions::default().name,
            volume_handle: handle.to_string(),
            volume_dir: self.volumes.path().join(pod).join("data"),
            read_only: false,
            access_type: AccessType::Mount {
                fs_type: "ext4".to_string(),
                mount_flags: vec!["noatime".to_string()],
            },
            access_mode: Mode::SingleNodeWriter,
            volume_context: [("k".to_string(), "v".to_string())].into(),
            publish_context: HashMap::new(),
            stage_secrets: [("stage".to_string(), "s".to_string())].into(),
            publish_secrets: [("publish".to_string(), "p".to_string())].into(),
            ephemeral: false,
        }
    }

    fn pod_dir(&self, pod: &str) -> PathBuf {
        self.volumes.path().join(pod)
    }
}

fn ops(calls: &[String]) -> Vec<&str> {
    calls.iter().map(|c| c.split(' ').next().unwrap()).collect()
}

#[tokio::test]
async fn registration_registers_driver_and_updates_csinode() {
    let env = Env::new();
    let storage = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
    let mut plugin = env.plugin(FakeOptions::default()).await;
    let mut watcher = env
        .manager
        .plugin_watcher()
        .with_csinode(storage.clone(), "node-1".to_string());

    watcher.scan().await;

    let calls = plugin.calls();
    assert_eq!(
        ops(&calls),
        vec![
            "GetInfo",
            "NodeGetInfo",
            "NodeGetCapabilities",
            "NotifyRegistrationStatus"
        ]
    );
    assert_eq!(calls[3], "NotifyRegistrationStatus true");
    let driver = env
        .manager
        .registry()
        .get("fake.csi.rusternetes.io")
        .unwrap();
    assert_eq!(driver.node_id, "fake-node-id");
    assert!(driver.stage_unstage);

    let key = build_key("csinodes", None, "node-1");
    let csinode: CSINode = storage.get(&key).await.unwrap();
    assert_eq!(csinode.spec.drivers.len(), 1);
    let d = &csinode.spec.drivers[0];
    assert_eq!(d.name, "fake.csi.rusternetes.io");
    assert_eq!(d.node_id, "fake-node-id");
    assert_eq!(
        d.topology_keys.as_deref(),
        Some(&["topology.fake/zone".to_string()][..])
    );
    assert_eq!(d.allocatable.as_ref().unwrap().count, Some(8));

    // A second scan does not re-register.
    plugin.clear();
    watcher.scan().await;
    assert!(plugin.calls().is_empty());

    // The socket goes away: the driver is deregistered and leaves CSINode.
    plugin.stop();
    watcher.scan().await;
    assert!(env.manager.registry().names().is_empty());
    let csinode: CSINode = storage.get(&key).await.unwrap();
    assert!(csinode.spec.drivers.is_empty());
}

#[tokio::test]
async fn registration_rejects_unsupported_plugins() {
    for opts in [
        FakeOptions {
            plugin_type: "DevicePlugin".to_string(),
            ..Default::default()
        },
        FakeOptions {
            versions: vec!["0.3.0".to_string()],
            ..Default::default()
        },
    ] {
        let env = Env::new();
        let plugin = env.plugin(opts).await;
        env.manager.plugin_watcher().scan().await;
        assert!(env.manager.registry().names().is_empty());
        let calls = plugin.calls();
        assert_eq!(ops(&calls), vec!["GetInfo", "NotifyRegistrationStatus"]);
        assert!(calls[1].starts_with("NotifyRegistrationStatus false "));
    }
}

#[tokio::test]
async fn stage_publish_unpublish_unstage_in_order() {
    let env = Env::new();
    let plugin = env.registered(FakeOptions::default()).await;
    let req = env.request("pod-a", "vol-1");

    let target = env.manager.mount_volume(&req).await.unwrap();
    assert_eq!(target, req.volume_dir.join("mount"));
    assert!(target.is_dir());
    let staging = env.manager.staging_path(&req.driver, "vol-1");
    assert!(staging.is_dir());

    plugin.recorded(|r| {
        let stage = &r.stage[0];
        assert_eq!(stage.staging_target_path, staging.to_string_lossy());
        assert_eq!(stage.secrets.get("stage").map(String::as_str), Some("s"));
        assert_eq!(stage.volume_context.get("k").map(String::as_str), Some("v"));
        let publish = &r.publish[0];
        assert_eq!(publish.staging_target_path, staging.to_string_lossy());
        assert_eq!(publish.target_path, target.to_string_lossy());
        assert_eq!(
            publish.secrets.get("publish").map(String::as_str),
            Some("p")
        );
        let cap = publish.volume_capability.as_ref().unwrap();
        assert_eq!(
            cap.access_mode.as_ref().unwrap().mode,
            Mode::SingleNodeWriter as i32
        );
        match cap.access_type.as_ref().unwrap() {
            proto::csi::v1::volume_capability::AccessType::Mount(m) => {
                assert_eq!(m.fs_type, "ext4");
                assert_eq!(m.mount_flags, vec!["noatime".to_string()]);
            }
            other => panic!("unexpected access type {other:?}"),
        }
    });

    env.manager
        .unmount_pod_volumes(&env.pod_dir("pod-a"))
        .await
        .unwrap();
    assert_eq!(
        ops(&plugin.calls()),
        vec![
            "NodeStageVolume",
            "NodePublishVolume",
            "NodeUnpublishVolume",
            "NodeUnstageVolume"
        ]
    );
    assert!(!req.volume_dir.join(VOL_DATA_FILE).exists());
    assert!(!staging.exists());
}

#[tokio::test]
async fn no_staging_without_capability_or_for_ephemeral() {
    let env = Env::new();
    let plugin = env
        .registered(FakeOptions {
            stage_unstage: false,
            ..Default::default()
        })
        .await;
    let req = env.request("pod-a", "vol-1");
    env.manager.mount_volume(&req).await.unwrap();
    env.manager
        .unmount_pod_volumes(&env.pod_dir("pod-a"))
        .await
        .unwrap();
    assert_eq!(
        ops(&plugin.calls()),
        vec!["NodePublishVolume", "NodeUnpublishVolume"]
    );

    let env = Env::new();
    let plugin = env.registered(FakeOptions::default()).await;
    let mut req = env.request("pod-b", "csi-eph");
    req.ephemeral = true;
    env.manager.mount_volume(&req).await.unwrap();
    env.manager
        .unmount_pod_volumes(&env.pod_dir("pod-b"))
        .await
        .unwrap();
    assert_eq!(
        ops(&plugin.calls()),
        vec!["NodePublishVolume", "NodeUnpublishVolume"]
    );
    plugin.recorded(|r| assert_eq!(r.publish[0].staging_target_path, ""));
}

#[tokio::test]
async fn block_volumes_publish_to_a_block_target() {
    let env = Env::new();
    let plugin = env.registered(FakeOptions::default()).await;
    let mut req = env.request("pod-a", "vol-1");
    req.access_type = AccessType::Block;
    let target = env.manager.mount_volume(&req).await.unwrap();
    assert_eq!(target, req.volume_dir.join("block"));
    plugin.recorded(|r| {
        let cap = r.publish[0].volume_capability.as_ref().unwrap();
        assert!(matches!(
            cap.access_type,
            Some(proto::csi::v1::volume_capability::AccessType::Block(_))
        ));
    });
    assert!(read_vol_data(&req.volume_dir).unwrap().unwrap().block);
}

#[tokio::test]
async fn retries_are_idempotent_and_shared_staging_is_kept() {
    let env = Env::new();
    let plugin = env.registered(FakeOptions::default()).await;

    // A retried mount (e.g. after a start_pod timeout) repeats both calls.
    let a = env.request("pod-a", "vol-1");
    env.manager.mount_volume(&a).await.unwrap();
    env.manager.mount_volume(&a).await.unwrap();
    // A second pod uses the same volume.
    let b = env.request("pod-b", "vol-1");
    env.manager.mount_volume(&b).await.unwrap();
    plugin.clear();

    // The first pod goes: unpublish only, staging is still used by pod-b.
    env.manager
        .unmount_pod_volumes(&env.pod_dir("pod-a"))
        .await
        .unwrap();
    assert_eq!(ops(&plugin.calls()), vec!["NodeUnpublishVolume"]);
    // Teardown again: nothing recorded any more, nothing to do.
    env.manager
        .unmount_pod_volumes(&env.pod_dir("pod-a"))
        .await
        .unwrap();
    assert_eq!(ops(&plugin.calls()), vec!["NodeUnpublishVolume"]);

    // The last pod goes: unpublish and unstage.
    plugin.clear();
    env.manager
        .unmount_pod_volumes(&env.pod_dir("pod-b"))
        .await
        .unwrap();
    assert_eq!(
        ops(&plugin.calls()),
        vec!["NodeUnpublishVolume", "NodeUnstageVolume"]
    );
}

#[tokio::test]
async fn driver_errors_propagate_and_teardown_still_cleans_up() {
    let env = Env::new();
    let plugin = env.registered(FakeOptions::default()).await;
    plugin.set_fail("NodePublishVolume", Some(tonic::Code::ResourceExhausted));
    let req = env.request("pod-a", "vol-1");

    let err = env.manager.mount_volume(&req).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("NodePublishVolume"), "{msg}");
    assert!(msg.contains("ResourceExhausted"), "{msg}");
    // The volume was staged, so it is recorded for teardown.
    assert!(req.volume_dir.join(VOL_DATA_FILE).exists());

    // Unpublish failing keeps the record (the mount may still be live).
    plugin.set_fail("NodeUnpublishVolume", Some(tonic::Code::Internal));
    assert!(env
        .manager
        .unmount_pod_volumes(&env.pod_dir("pod-a"))
        .await
        .is_err());
    assert!(req.volume_dir.join(VOL_DATA_FILE).exists());

    plugin.set_fail("NodeUnpublishVolume", None);
    plugin.clear();
    env.manager
        .unmount_pod_volumes(&env.pod_dir("pod-a"))
        .await
        .unwrap();
    assert_eq!(
        ops(&plugin.calls()),
        vec!["NodeUnpublishVolume", "NodeUnstageVolume"]
    );
}

#[tokio::test]
async fn unknown_driver_is_an_error() {
    let env = Env::new();
    let err = env
        .manager
        .mount_volume(&env.request("pod-a", "vol-1"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not registered"), "{err}");
    // Nothing was recorded, so teardown has nothing to do.
    assert!(!env
        .pod_dir("pod-a")
        .join("data")
        .join(VOL_DATA_FILE)
        .exists());
}
