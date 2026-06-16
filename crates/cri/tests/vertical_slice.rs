//! End-to-end vertical slice for the CRI client: pull an image, run a pod
//! sandbox, create + start a container, and confirm it reaches RUNNING.
//!
//! This is the sub-project 2 milestone — one pod running entirely through CRI,
//! with no bollard/Docker API involved. It is **socket-gated**: it does nothing
//! unless `RUSTERNETES_CRI_SOCKET` points at a real CRI runtime (e.g.
//! `/run/containerd/containerd.sock`), so `cargo test` stays green on machines
//! without one. Optionally set `RUSTERNETES_CRI_RUNTIME_HANDLER` to select a
//! runtime class (e.g. the Youki handler); it defaults to the runtime default.
//!
//! Run it against a live runtime with:
//!
//! ```bash
//! RUSTERNETES_CRI_SOCKET=/run/containerd/containerd.sock \
//!   cargo test -p rusternetes-cri --test vertical_slice -- --nocapture
//! ```

use std::time::Duration;

use rusternetes_cri::{v1, CriClient};

const IMAGE: &str = "docker.io/library/busybox:latest";

fn socket() -> Option<String> {
    std::env::var("RUSTERNETES_CRI_SOCKET").ok()
}

fn runtime_handler() -> String {
    std::env::var("RUSTERNETES_CRI_RUNTIME_HANDLER").unwrap_or_default()
}

/// Minimal sandbox config for a single test pod. containerd requires
/// `log_directory` to exist, so the caller passes a real temp dir.
fn sandbox_config(log_dir: &str) -> v1::PodSandboxConfig {
    v1::PodSandboxConfig {
        metadata: Some(v1::PodSandboxMetadata {
            name: "cri-slice-pod".to_string(),
            uid: "cri-slice-uid-0001".to_string(),
            namespace: "default".to_string(),
            attempt: 0,
        }),
        log_directory: log_dir.to_string(),
        linux: Some(v1::LinuxPodSandboxConfig {
            security_context: Some(v1::LinuxSandboxSecurityContext {
                // Host network namespace: pod-network (POD) would require the
                // runtime to invoke CNI, which is a separate concern handled in
                // the networking sub-project. NODE skips CNI so this slice
                // exercises only the pull/sandbox/container path.
                namespace_options: Some(v1::NamespaceOption {
                    network: v1::NamespaceMode::Node as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn container_config() -> v1::ContainerConfig {
    v1::ContainerConfig {
        metadata: Some(v1::ContainerMetadata {
            name: "sleeper".to_string(),
            attempt: 0,
        }),
        image: Some(v1::ImageSpec {
            image: IMAGE.to_string(),
            ..Default::default()
        }),
        command: vec!["/bin/sh".to_string()],
        args: vec!["-c".to_string(), "sleep 3600".to_string()],
        log_path: "sleeper.log".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn pod_runs_via_cri() {
    let Some(socket) = socket() else {
        eprintln!("RUSTERNETES_CRI_SOCKET unset; skipping CRI vertical-slice e2e");
        return;
    };
    let handler = runtime_handler();

    let log_dir = std::env::temp_dir().join("rusternetes-cri-slice");
    std::fs::create_dir_all(&log_dir).expect("create log dir");
    let log_dir = log_dir.to_string_lossy().to_string();

    let mut cri = CriClient::connect(&socket).await.expect("connect CRI");

    // Sanity: runtime speaks CRI v1.
    let v = cri.version().await.expect("Version");
    eprintln!("runtime: {} {}", v.runtime_name, v.runtime_version);

    // 1. Pull the image.
    let image_ref = cri
        .pull_image(IMAGE, None, None)
        .await
        .expect("PullImage busybox");
    eprintln!("pulled: {image_ref}");

    // 2. Run the pod sandbox.
    let sb_config = sandbox_config(&log_dir);
    let sandbox_id = cri
        .run_pod_sandbox(sb_config.clone(), &handler)
        .await
        .expect("RunPodSandbox");
    eprintln!("sandbox: {sandbox_id}");

    // 3 + 4. Create and start the container — with sandbox cleanup on the way out.
    let result = async {
        let container_id = cri
            .create_container(&sandbox_id, container_config(), sb_config.clone())
            .await
            .expect("CreateContainer");
        eprintln!("container: {container_id}");

        cri.start_container(&container_id)
            .await
            .expect("StartContainer");

        // 5. Poll until RUNNING (or give up).
        let running = v1::ContainerState::ContainerRunning as i32;
        let mut state = -1;
        for _ in 0..50 {
            let status = cri
                .container_status(&container_id, false)
                .await
                .expect("ContainerStatus")
                .status
                .expect("status present");
            state = status.state;
            if state == running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            state, running,
            "container did not reach RUNNING (final state code {state})"
        );
        eprintln!("container RUNNING — vertical slice OK");

        let _ = cri.stop_container(&container_id, 5).await;
        let _ = cri.remove_container(&container_id).await;
    }
    .await;

    // Cleanup sandbox regardless of outcome.
    let _ = cri.stop_pod_sandbox(&sandbox_id).await;
    let _ = cri.remove_pod_sandbox(&sandbox_id).await;

    // Re-surface any panic from the inner block.
    let () = result;
}
