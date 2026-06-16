//! End-to-end proof that the Pod → CRI translation produces configs a real
//! runtime accepts: build a rusternetes `Pod`, translate it, and run it through
//! containerd via the `rusternetes-cri` client until the container is RUNNING.
//!
//! Socket-gated like the crates/cri slice — does nothing unless
//! `RUSTERNETES_CRI_SOCKET` is set. Optionally set `RUSTERNETES_CRI_RUNTIME_HANDLER`
//! (e.g. `youki`).
//!
//! ```bash
//! RUSTERNETES_CRI_SOCKET=/tmp/cri-verify/containerd.sock \
//! RUSTERNETES_CRI_RUNTIME_HANDLER=youki \
//!   cargo test -p rusternetes-kubelet --test cri_translate_e2e -- --nocapture
//! ```

use std::collections::HashMap;

use rusternetes_common::resources::pod::{Container, Pod, PodSpec};
use rusternetes_cri::CriClient;
use rusternetes_kubelet::cri_runtime::{translate, CriContainerRuntime};

const IMAGE: &str = "docker.io/library/busybox:latest";

/// Build a single-container test pod. `name` is distinct per test so the two
/// (parallel) e2e tests don't collide on the runtime's sandbox-name reservation.
fn test_pod(name: &str) -> Pod {
    let container = Container {
        name: "sleeper".to_string(),
        image: IMAGE.to_string(),
        command: Some(vec!["/bin/sh".to_string()]),
        args: Some(vec!["-c".to_string(), "sleep 3600".to_string()]),
        env: Some(vec![rusternetes_common::resources::pod::EnvVar {
            name: "GREETING".to_string(),
            value: Some("from-translation".to_string()),
            value_from: None,
        }]),
        ..Default::default()
    };
    let mut pod = Pod::new(
        name,
        PodSpec {
            containers: vec![container],
            // Host network so the runtime skips CNI (not configured in the test rig).
            host_network: Some(true),
            ..Default::default()
        },
    );
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.uid = format!("{name}-uid");
    pod
}

#[tokio::test]
async fn translated_pod_runs_on_containerd() {
    let Ok(socket) = std::env::var("RUSTERNETES_CRI_SOCKET") else {
        eprintln!("RUSTERNETES_CRI_SOCKET unset; skipping translation e2e");
        return;
    };
    let handler = std::env::var("RUSTERNETES_CRI_RUNTIME_HANDLER").unwrap_or_default();

    let log_dir = std::env::temp_dir().join("rusternetes-cri-translate");
    std::fs::create_dir_all(&log_dir).expect("log dir");
    let log_dir = log_dir.to_string_lossy().to_string();

    let pod = test_pod("translate-e2e");
    let container = &pod.spec.as_ref().unwrap().containers[0];

    // The whole point: configs come from the translation layer, not hand-built.
    let sandbox_cfg = translate::sandbox_config(&pod, &log_dir);
    let container_cfg = translate::container_config(&pod, container, IMAGE, &HashMap::new());

    assert_eq!(sandbox_cfg.metadata.as_ref().unwrap().name, "translate-e2e");

    let mut cri = CriClient::connect(&socket).await.expect("connect CRI");
    cri.pull_image(IMAGE, None, None).await.expect("PullImage");

    let sandbox_id = cri
        .run_pod_sandbox(sandbox_cfg.clone(), &handler)
        .await
        .expect("RunPodSandbox from translated config");

    let result = async {
        let container_id = cri
            .create_container(&sandbox_id, container_cfg, sandbox_cfg.clone())
            .await
            .expect("CreateContainer from translated config");
        cri.start_container(&container_id)
            .await
            .expect("StartContainer");

        let running = rusternetes_cri::v1::ContainerState::ContainerRunning as i32;
        let mut state = -1;
        for _ in 0..50 {
            state = cri
                .container_status(&container_id, false)
                .await
                .expect("ContainerStatus")
                .status
                .expect("status")
                .state;
            if state == running {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(state, running, "translated pod did not reach RUNNING");

        // Confirm the translated env var made it into the container.
        let exec = cri
            .exec_sync(&container_id, &["/bin/sh", "-c", "echo $GREETING"], 5)
            .await
            .expect("ExecSync");
        assert_eq!(
            String::from_utf8_lossy(&exec.stdout).trim(),
            "from-translation",
            "translated env var not present in container"
        );
        eprintln!("translated pod RUNNING with env propagated — integration OK");

        let _ = cri.stop_container(&container_id, 5).await;
        let _ = cri.remove_container(&container_id).await;
    }
    .await;

    let _ = cri.stop_pod_sandbox(&sandbox_id).await;
    let _ = cri.remove_pod_sandbox(&sandbox_id).await;

    let () = result;
}

/// Drive the full `CriContainerRuntime` lifecycle type (not the raw client):
/// start_pod -> is_pod_running -> list_running_pods -> stop_and_remove_pod.
#[tokio::test]
async fn cri_container_runtime_lifecycle() {
    let Ok(socket) = std::env::var("RUSTERNETES_CRI_SOCKET") else {
        eprintln!("RUSTERNETES_CRI_SOCKET unset; skipping runtime lifecycle e2e");
        return;
    };
    let handler = std::env::var("RUSTERNETES_CRI_RUNTIME_HANDLER").unwrap_or_default();

    let log_root = std::env::temp_dir().join("rusternetes-cri-runtime");
    let runtime = CriContainerRuntime::connect(&socket, handler, log_root.to_string_lossy())
        .await
        .expect("connect runtime");

    let pod = test_pod("runtime-e2e");
    let pod_name = pod.metadata.name.clone();

    // Clean any leftover from a previous run, then bring the pod up.
    let _ = runtime.stop_and_remove_pod(&pod_name).await;
    runtime.start_pod(&pod).await.expect("start_pod");

    // Poll until the runtime reports the pod running.
    let mut running = false;
    for _ in 0..50 {
        if runtime.is_pod_running(&pod).await.expect("is_pod_running") {
            running = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(running, "CriContainerRuntime did not report pod running");

    let pods = runtime
        .list_running_pods()
        .await
        .expect("list_running_pods");
    assert!(
        pods.contains(&pod_name),
        "started pod missing from list_running_pods: {pods:?}"
    );
    // Container status maps to Running/ready for the live container.
    let statuses = runtime
        .get_container_statuses(&pod)
        .await
        .expect("get_container_statuses");
    assert_eq!(statuses.len(), 1, "expected one container status");
    let st = &statuses[0];
    assert_eq!(st.name, "sleeper");
    assert!(st.ready, "container not ready");
    assert!(
        matches!(
            st.state,
            Some(rusternetes_common::resources::pod::ContainerState::Running { .. })
        ),
        "expected Running state, got {:?}",
        st.state
    );
    // Introspection helpers used by the kubelet reconcile loop.
    assert!(
        runtime
            .is_container_running("sleeper")
            .await
            .expect("is_container_running"),
        "is_container_running(sleeper) should be true"
    );
    assert!(
        runtime
            .list_all_pods()
            .await
            .expect("list_all_pods")
            .contains(&pod_name),
        "pod missing from list_all_pods"
    );
    // Host-network pod: IP may be the node IP or empty depending on runtime;
    // just assert the call succeeds and log what it returns.
    let ip = runtime.get_pod_ip(&pod_name).await.expect("get_pod_ip");
    eprintln!("CriContainerRuntime introspection OK (pod_ip={ip:?})");

    // Graceful teardown path.
    runtime.stop_pod_for(&pod, 5).await.expect("stop_pod_for");

    // Sandbox gone -> no longer running.
    assert!(
        !runtime.is_pod_running(&pod).await.expect("is_pod_running"),
        "pod still running after stop_pod_for"
    );
    eprintln!("CriContainerRuntime teardown OK");
}
