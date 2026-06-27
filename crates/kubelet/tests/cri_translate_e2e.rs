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
use rusternetes_kubelet::volumes::VolumeManager;

const IMAGE: &str = "docker.io/library/busybox:latest";

/// An exec liveness probe running a single command (Probe has no Default).
fn exec_probe(cmd: &str) -> rusternetes_common::resources::pod::Probe {
    rusternetes_common::resources::pod::Probe {
        http_get: None,
        tcp_socket: None,
        exec: Some(rusternetes_common::resources::pod::ExecAction {
            command: vec![cmd.to_string()],
        }),
        initial_delay_seconds: None,
        timeout_seconds: Some(2),
        period_seconds: None,
        success_threshold: None,
        failure_threshold: None,
        grpc: None,
        termination_grace_period_seconds: None,
    }
}

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
    let container_cfg = translate::container_config(
        &pod,
        container,
        IMAGE,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
    );

    assert_eq!(sandbox_cfg.metadata.as_ref().unwrap().name, "translate-e2e");

    let mut cri = CriClient::connect(&socket).await.expect("connect CRI");
    cri.pull_image(IMAGE, None, None).await.expect("PullImage");

    // Remove any leftover sandbox for this pod from an interrupted prior run so
    // the runtime's sandbox-name reservation doesn't block RunPodSandbox.
    if let Ok(existing) = cri
        .list_pod_sandbox(Some(rusternetes_cri::v1::PodSandboxFilter {
            label_selector: std::collections::HashMap::from([(
                "io.kubernetes.pod.uid".to_string(),
                "translate-e2e-uid".to_string(),
            )]),
            ..Default::default()
        }))
        .await
    {
        for sb in existing {
            let _ = cri.stop_pod_sandbox(&sb.id).await;
            let _ = cri.remove_pod_sandbox(&sb.id).await;
        }
    }

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
            .is_container_running(&pod.metadata.uid, "sleeper")
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

    // Per-pod metrics include the running container.
    let metrics = runtime
        .collect_pod_metrics(std::slice::from_ref(&pod_name))
        .await;
    let pod_metrics = metrics.get(&pod_name).expect("metrics for pod");
    assert!(
        pod_metrics.iter().any(|(name, _, _)| name == "sleeper"),
        "sleeper missing from pod metrics: {pod_metrics:?}"
    );

    // In-place resource update is accepted by the runtime.
    runtime
        .update_container_resources(
            "sleeper",
            Some(100_000),
            Some(50_000),
            None,
            Some(128 * 1024 * 1024),
        )
        .await
        .expect("update_container_resources");
    eprintln!("CriContainerRuntime metrics + resource update OK");

    // Existence / termination / age introspection.
    assert!(
        runtime.container_exists(&pod.metadata.uid, "sleeper").await,
        "container_exists(sleeper) should be true"
    );
    assert!(
        !runtime.has_terminated_containers(&pod).await,
        "running pod should have no terminated containers"
    );
    let (node_cpu, node_mem) = runtime
        .collect_node_metrics(std::slice::from_ref(&pod_name))
        .await;
    eprintln!("node metrics: cpu={node_cpu} mem={node_mem}");
    let age = runtime.get_container_age(&pod_name).await.expect("age");
    assert!(age > std::time::Duration::ZERO, "sandbox age should be > 0");
    eprintln!("CriContainerRuntime existence/age introspection OK");

    // Exec probe: `true` succeeds, `false` fails — run via CRI ExecSync.
    let app_container = &pod.spec.as_ref().unwrap().containers[0];
    assert!(
        runtime
            .probe_container(&pod, app_container, &exec_probe("/bin/true"))
            .await
            .expect("probe ok"),
        "exec /bin/true probe should succeed"
    );
    assert!(
        !runtime
            .probe_container(&pod, app_container, &exec_probe("/bin/false"))
            .await
            .expect("probe fail"),
        "exec /bin/false probe should fail"
    );
    eprintln!("CriContainerRuntime exec probe OK");

    // garbage_collect_containers is intentionally NOT exercised here: it removes
    // every sandbox not in the keep-set, which would race with the sibling
    // parallel tests' sandboxes. It reuses the already-verified list_pod_sandbox
    // + remove_pod_sandbox primitives.

    // Graceful teardown path.
    runtime.stop_pod_for(&pod, 5).await.expect("stop_pod_for");

    // Sandbox gone -> no longer running.
    assert!(
        !runtime.is_pod_running(&pod).await.expect("is_pod_running"),
        "pod still running after stop_pod_for"
    );
    eprintln!("CriContainerRuntime teardown OK");
}

/// An emptyDir volume is provisioned on the host and mounted into the
/// container: writing then reading a file under the mount path works.
#[tokio::test]
async fn emptydir_volume_provisioned_and_mounted() {
    let Ok(socket) = std::env::var("RUSTERNETES_CRI_SOCKET") else {
        eprintln!("RUSTERNETES_CRI_SOCKET unset; skipping volume e2e");
        return;
    };
    let handler = std::env::var("RUSTERNETES_CRI_RUNTIME_HANDLER").unwrap_or_default();

    let vol_base = std::env::temp_dir().join("rusternetes-cri-volbase");
    std::fs::create_dir_all(&vol_base).expect("vol base");
    let vm = VolumeManager::new(
        vol_base.to_string_lossy().to_string(),
        None, // emptyDir needs no storage
        rusternetes_common::auth::TokenManager::new(b"test-secret"),
    );

    let log_root = std::env::temp_dir().join("rusternetes-cri-vol");
    let runtime = CriContainerRuntime::connect(&socket, handler, log_root.to_string_lossy())
        .await
        .expect("connect")
        .with_volumes(vm);

    // Container that mounts an emptyDir at /scratch.
    let mut app = Container {
        name: "sleeper".to_string(),
        image: IMAGE.to_string(),
        command: Some(vec!["/bin/sh".to_string()]),
        args: Some(vec!["-c".to_string(), "sleep 3600".to_string()]),
        ..Default::default()
    };
    app.volume_mounts = Some(vec![serde_json::from_value(
        serde_json::json!({"name": "scratch", "mountPath": "/scratch"}),
    )
    .unwrap()]);

    let mut pod = Pod::new(
        "vol-e2e",
        PodSpec {
            containers: vec![app],
            volumes: Some(vec![serde_json::from_value(
                serde_json::json!({"name": "scratch", "emptyDir": {}}),
            )
            .unwrap()]),
            host_network: Some(true),
            ..Default::default()
        },
    );
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.uid = "vol-e2e-uid".to_string();
    let pod_name = pod.metadata.name.clone();

    let _ = runtime.stop_and_remove_pod(&pod_name).await;
    runtime
        .start_pod(&pod)
        .await
        .expect("start_pod with volume");

    // Wait until running, then write+read a file in the mounted emptyDir.
    for _ in 0..50 {
        if runtime.is_pod_running(&pod).await.unwrap_or(false) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let container = &pod.spec.as_ref().unwrap().containers[0];
    let probe = exec_probe("/bin/true");
    // (probe just confirms exec path; the real check is the write/read below.)
    let _ = runtime.probe_container(&pod, container, &probe).await;

    let mut cri = CriClient::connect(&socket).await.expect("cri");
    let filter = rusternetes_cri::v1::ContainerFilter {
        label_selector: std::collections::HashMap::from([(
            "io.kubernetes.pod.uid".to_string(),
            "vol-e2e-uid".to_string(),
        )]),
        ..Default::default()
    };
    let cid = cri
        .list_containers(Some(filter))
        .await
        .expect("list")
        .into_iter()
        .next()
        .expect("container exists")
        .id;
    let out = cri
        .exec_sync(
            &cid,
            &[
                "/bin/sh",
                "-c",
                "echo persisted > /scratch/f && cat /scratch/f",
            ],
            5,
        )
        .await
        .expect("exec in volume");
    assert_eq!(
        out.exit_code,
        0,
        "stderr: {:?}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "persisted");
    eprintln!("emptyDir volume mount OK");

    runtime
        .stop_and_remove_pod(&pod_name)
        .await
        .expect("teardown");
}

/// A failing liveness probe (failureThreshold=1) makes check_liveness ask for a
/// restart; a passing one does not.
#[tokio::test]
async fn liveness_probe_drives_check_liveness() {
    let Ok(socket) = std::env::var("RUSTERNETES_CRI_SOCKET") else {
        eprintln!("RUSTERNETES_CRI_SOCKET unset; skipping liveness e2e");
        return;
    };
    let handler = std::env::var("RUSTERNETES_CRI_RUNTIME_HANDLER").unwrap_or_default();
    let log_root = std::env::temp_dir().join("rusternetes-cri-live");
    let runtime = CriContainerRuntime::connect(&socket, handler, log_root.to_string_lossy())
        .await
        .expect("connect");

    let liveness = |cmd: &str| rusternetes_common::resources::pod::Probe {
        http_get: None,
        tcp_socket: None,
        exec: Some(rusternetes_common::resources::pod::ExecAction {
            command: vec![cmd.to_string()],
        }),
        initial_delay_seconds: None,
        timeout_seconds: Some(2),
        period_seconds: None,
        success_threshold: None,
        failure_threshold: Some(1), // single failure trips a restart
        grpc: None,
        termination_grace_period_seconds: Some(7),
    };

    let mut app = Container {
        name: "sleeper".to_string(),
        image: IMAGE.to_string(),
        command: Some(vec!["/bin/sh".to_string()]),
        args: Some(vec!["-c".to_string(), "sleep 3600".to_string()]),
        ..Default::default()
    };
    app.liveness_probe = Some(liveness("/bin/false"));

    let mut pod = Pod::new(
        "live-e2e",
        PodSpec {
            containers: vec![app],
            host_network: Some(true),
            ..Default::default()
        },
    );
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.uid = "live-e2e-uid".to_string();
    let pod_name = pod.metadata.name.clone();

    let _ = runtime.stop_and_remove_pod(&pod_name).await;
    runtime.start_pod(&pod).await.expect("start_pod");
    for _ in 0..50 {
        if runtime.is_pod_running(&pod).await.unwrap_or(false) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Failing liveness probe -> restart requested with the probe's grace (7).
    let grace = runtime.check_liveness(&pod).await.expect("check_liveness");
    assert_eq!(
        grace,
        Some(7),
        "failing liveness should request restart w/ grace 7"
    );

    // Swap to a passing probe -> no restart. Clear prior threshold state first.
    runtime.clear_probe_states_for_pod(&pod_name);
    pod.spec.as_mut().unwrap().containers[0].liveness_probe = Some(liveness("/bin/true"));
    let grace_ok = runtime
        .check_liveness(&pod)
        .await
        .expect("check_liveness ok");
    assert_eq!(
        grace_ok, None,
        "passing liveness should not request restart"
    );
    eprintln!("check_liveness OK (fail->Some(7), pass->None)");

    runtime
        .stop_and_remove_pod(&pod_name)
        .await
        .expect("teardown");
}

/// Init container runs to completion before the app container starts.
#[tokio::test]
async fn init_container_runs_before_app() {
    let Ok(socket) = std::env::var("RUSTERNETES_CRI_SOCKET") else {
        eprintln!("RUSTERNETES_CRI_SOCKET unset; skipping init-container e2e");
        return;
    };
    let handler = std::env::var("RUSTERNETES_CRI_RUNTIME_HANDLER").unwrap_or_default();

    let log_root = std::env::temp_dir().join("rusternetes-cri-init");
    let runtime = CriContainerRuntime::connect(&socket, handler, log_root.to_string_lossy())
        .await
        .expect("connect runtime");

    let init = Container {
        name: "setup".to_string(),
        image: IMAGE.to_string(),
        command: Some(vec!["/bin/sh".to_string()]),
        args: Some(vec!["-c".to_string(), "true".to_string()]),
        ..Default::default()
    };
    let app = Container {
        name: "sleeper".to_string(),
        image: IMAGE.to_string(),
        command: Some(vec!["/bin/sh".to_string()]),
        args: Some(vec!["-c".to_string(), "sleep 3600".to_string()]),
        ..Default::default()
    };
    let mut pod = Pod::new(
        "init-e2e",
        PodSpec {
            containers: vec![app],
            init_containers: Some(vec![init]),
            host_network: Some(true),
            ..Default::default()
        },
    );
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.uid = "init-e2e-uid".to_string();
    let pod_name = pod.metadata.name.clone();

    let _ = runtime.stop_and_remove_pod(&pod_name).await;
    // start_pod blocks on the init container completing successfully.
    runtime.start_pod(&pod).await.expect("start_pod with init");

    // Init container terminated with exit 0.
    let init_statuses = runtime
        .get_init_container_statuses(&pod)
        .await
        .expect("pod has init containers");
    assert_eq!(init_statuses.len(), 1);
    match &init_statuses[0].state {
        Some(rusternetes_common::resources::pod::ContainerState::Terminated {
            exit_code, ..
        }) => {
            assert_eq!(*exit_code, 0, "init container should exit 0");
        }
        other => panic!("expected init Terminated, got {other:?}"),
    }

    // App container is running.
    assert!(
        runtime.is_pod_running(&pod).await.expect("is_pod_running"),
        "app container should be running after init completed"
    );

    // compute_init_container_actions reports init complete (no next index).
    let (all_done, next, _retry) = runtime.compute_init_container_actions(&pod).await;
    assert!(all_done, "init should be reported done");
    assert!(next.is_none(), "no further init container to start");
    // No ephemeral containers on this pod.
    assert!(runtime
        .get_ephemeral_container_statuses(&pod)
        .await
        .is_none());
    eprintln!("init-container ordering + actions OK");

    runtime
        .stop_and_remove_pod(&pod_name)
        .await
        .expect("teardown");
}
