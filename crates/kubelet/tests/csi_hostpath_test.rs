//! Optional test against the real csi-driver-host-path plugin and the
//! node-driver-registrar sidecar. Ignored by default; run with
//! `RUSTERNETES_CSI_IT_ROOT=<dir>` pointing at a kubelet root where
//! `hostpathplugin` serves `plugins/hostpath.csi.k8s.io/csi.sock` and the
//! registrar has put its socket into `plugins_registry/`, with mount
//! propagation so that the driver's mounts are visible here:
//!
//! `cargo test -p rusternetes-kubelet --test csi_hostpath_test -- --ignored`

use rusternetes_kubelet::csi::proto::csi::v1 as csipb;
use rusternetes_kubelet::csi::{client::connect_unix, AccessType, CsiManager, MountRequest};
use std::path::PathBuf;
use std::time::Duration;

const DRIVER: &str = "hostpath.csi.k8s.io";

fn is_mounted(path: &std::path::Path) -> bool {
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    let p = path.to_string_lossy();
    mounts
        .lines()
        .any(|l| l.split(' ').nth(4) == Some(p.as_ref()))
}

#[tokio::test]
#[ignore]
async fn hostpath_driver_end_to_end() {
    let Ok(root) = std::env::var("RUSTERNETES_CSI_IT_ROOT") else {
        eprintln!("RUSTERNETES_CSI_IT_ROOT not set, skipping");
        return;
    };
    let root = PathBuf::from(root);
    let manager = CsiManager::new(&root);

    // Registration through node-driver-registrar.
    let mut watcher = manager.plugin_watcher();
    for _ in 0..60 {
        watcher.scan().await;
        if manager.registry().get(DRIVER).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let driver = manager
        .registry()
        .get(DRIVER)
        .expect("hostpath driver registered");
    assert!(driver.stage_unstage);

    // Provision a volume through the driver's controller service.
    let channel = connect_unix(&driver.endpoint).await.unwrap();
    let mut controller = csipb::controller_client::ControllerClient::new(channel);
    let mount_cap = csipb::VolumeCapability {
        access_mode: Some(csipb::volume_capability::AccessMode {
            mode: csipb::volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
        access_type: Some(csipb::volume_capability::AccessType::Mount(
            csipb::volume_capability::MountVolume::default(),
        )),
    };
    let created = controller
        .create_volume(csipb::CreateVolumeRequest {
            name: "rusternetes-it".to_string(),
            capacity_range: Some(csipb::CapacityRange {
                required_bytes: 1 << 20,
                limit_bytes: 0,
            }),
            volume_capabilities: vec![mount_cap],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .volume
        .unwrap();

    let volume_dir = root.join("volumes/it-pod/data");
    let req = MountRequest {
        driver: DRIVER.to_string(),
        volume_handle: created.volume_id.clone(),
        volume_dir: volume_dir.clone(),
        read_only: false,
        access_type: AccessType::Mount {
            fs_type: String::new(),
            mount_flags: Vec::new(),
        },
        access_mode: csipb::volume_capability::access_mode::Mode::SingleNodeWriter,
        volume_context: created.volume_context.clone(),
        publish_context: Default::default(),
        stage_secrets: Default::default(),
        publish_secrets: Default::default(),
        ephemeral: false,
    };
    let target = manager.mount_volume(&req).await.unwrap();
    // A retry is harmless.
    assert_eq!(manager.mount_volume(&req).await.unwrap(), target);
    assert!(is_mounted(&target), "{} not mounted", target.display());
    std::fs::write(target.join("hello"), b"world").unwrap();

    manager
        .unmount_pod_volumes(&root.join("volumes/it-pod"))
        .await
        .unwrap();
    assert!(!is_mounted(&target));

    // The data lives on in the volume: publish again and read it back.
    let target = manager.mount_volume(&req).await.unwrap();
    assert_eq!(std::fs::read(target.join("hello")).unwrap(), b"world");
    manager
        .unmount_pod_volumes(&root.join("volumes/it-pod"))
        .await
        .unwrap();

    controller
        .delete_volume(csipb::DeleteVolumeRequest {
            volume_id: created.volume_id,
            secrets: Default::default(),
        })
        .await
        .unwrap();
}
