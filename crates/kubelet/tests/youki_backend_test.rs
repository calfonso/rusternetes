//! Runs real containers through the youki backend.
//!
//! Needs root, cgroup v2 with the cpu/memory/pids controllers delegated,
//! overlayfs and network access to Docker Hub, so it is ignored by default:
//!
//! ```text
//! RUSTERNETES_YOUKI_IT=1 cargo test -p rusternetes-kubelet --features youki \
//!     --test youki_backend_test -- --ignored --test-threads=1
//! ```
#![cfg(all(feature = "youki", target_os = "linux"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use rusternetes_kubelet::backend::youki::{YoukiBackend, YoukiConfig};
use rusternetes_kubelet::backend::*;

fn backend() -> Option<YoukiBackend> {
    if std::env::var("RUSTERNETES_YOUKI_IT").as_deref() != Ok("1") {
        eprintln!("skipped: set RUSTERNETES_YOUKI_IT=1 (root + cgroup v2 required)");
        return None;
    }
    let base = PathBuf::from(
        std::env::var("RUSTERNETES_YOUKI_IT_DIR").unwrap_or_else(|_| "/var/lib/rk-youki-it".into()),
    );
    Some(
        YoukiBackend::new(YoukiConfig {
            root: base.join("root"),
            state_root: base.join("state"),
            shim: PathBuf::from(env!("CARGO_BIN_EXE_rusternetes-youki-shim")),
        })
        .unwrap(),
    )
}

fn sh(script: &str, network_mode: &str) -> ContainerConfig {
    ContainerConfig {
        image: Some("busybox:latest".into()),
        cmd: Some(vec!["sh".into(), "-c".into(), script.into()]),
        hostname: (network_mode == "none").then(|| "it-box".to_string()),
        host_config: Some(HostConfig {
            network_mode: Some(network_mode.into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn wait_exited(b: &YoukiBackend, name: &str) -> ContainerStateInfo {
    for _ in 0..100 {
        let st = b.inspect_container(name).await.unwrap().state.unwrap();
        if st.running != Some(true) && st.status == Some(ContainerStateStatus::Exited) {
            return st;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{name} did not exit");
}

async fn exec(b: &YoukiBackend, name: &str, cmd: &[&str]) -> (String, i64) {
    let id = b
        .create_exec(
            name,
            CreateExecOptions {
                cmd: Some(cmd.iter().map(|s| s.to_string()).collect()),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut out = String::new();
    if let StartExecResults::Attached { mut output } = b.start_exec(&id).await.unwrap() {
        while let Some(chunk) = output.next().await {
            out.push_str(&chunk.unwrap().to_string());
        }
    }
    let code = b.inspect_exec(&id).await.unwrap().exit_code.unwrap();
    (out, code)
}

#[tokio::test]
#[ignore = "needs root, cgroup v2 and registry access; set RUSTERNETES_YOUKI_IT=1"]
async fn youki_runs_a_pod_like_pair_of_containers() {
    let Some(b) = backend() else { return };
    for n in ["it_box", "it_side", "it_exit"] {
        let _ = b
            .remove_container(n, RemoveContainerOptions { force: true })
            .await;
    }

    // Pull.
    let mut errors = Vec::new();
    let mut pull = b.pull_image("busybox:latest");
    while let Some(p) = pull.next().await {
        if let Some(e) = p.unwrap().error {
            errors.push(e);
        }
    }
    drop(pull);
    assert!(errors.is_empty(), "{errors:?}");
    let image = b.inspect_image("docker.io/library/busybox").await.unwrap();
    assert!(image.id.unwrap().starts_with("sha256:"));

    // A long-running container with its own (empty) network namespace.
    let id = b
        .create_container(
            "it_box",
            sh(
                "echo hello; echo oops >&2; echo done > /tmp/msg; sleep 300",
                "none",
            ),
        )
        .await
        .unwrap();
    let st = b.inspect_container("it_box").await.unwrap().state.unwrap();
    assert_eq!(st.status, Some(ContainerStateStatus::Created));
    b.start_container("it_box").await.unwrap();
    let inspect = b.inspect_container(&id).await.unwrap();
    assert_eq!(inspect.state.as_ref().unwrap().running, Some(true));

    // Exec.
    let (out, code) = exec(&b, "it_box", &["sh", "-c", "echo from-exec; exit 3"]).await;
    assert_eq!((out.trim(), code), ("from-exec", 3));
    let (out, code) = exec(&b, "it_box", &["cat", "/proc/sys/kernel/hostname"]).await;
    assert_eq!((out.trim(), code), ("it-box", 0));

    // A second container joining its namespaces, like an app joining a pod.
    b.create_container(
        "it_side",
        ContainerConfig {
            host_config: Some(HostConfig {
                network_mode: Some("container:it_box".into()),
                ipc_mode: Some("container:it_box".into()),
                uts_mode: Some("container:it_box".into()),
                ..Default::default()
            }),
            ..sh("sleep 300", "container:it_box")
        },
    )
    .await
    .unwrap();
    b.start_container("it_side").await.unwrap();
    let (hostname, code) = exec(&b, "it_side", &["cat", "/proc/sys/kernel/hostname"]).await;
    assert_eq!((hostname.trim(), code), ("it-box", 0));

    let (_, code) = exec(&b, "it_box", &["/no/such/binary"]).await;
    assert_ne!(code, 0);

    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut logs = String::new();
    let mut s = b.logs(
        "it_box",
        LogsOptions {
            stdout: true,
            stderr: true,
            tail: "all".into(),
        },
    );
    while let Some(l) = s.next().await {
        logs.push_str(&l.unwrap().to_string());
    }
    drop(s);
    assert!(logs.contains("hello") && logs.contains("oops"), "{logs}");

    let mut tar_bytes = Vec::new();
    let mut s = b.download_from_container("it_box", "/tmp/msg");
    while let Some(c) = s.next().await {
        tar_bytes.extend(c.unwrap());
    }
    drop(s);
    let mut archive = tar::Archive::new(&tar_bytes[..]);
    let mut entry = archive.entries().unwrap().next().unwrap().unwrap();
    let mut content = String::new();
    std::io::Read::read_to_string(&mut entry, &mut content).unwrap();
    assert_eq!(content, "done\n");

    // List, stats.
    let running = b
        .list_containers(ListContainersOptions {
            all: false,
            filters: HashMap::from([("name".to_string(), vec!["^/it_".to_string()])]),
        })
        .await
        .unwrap();
    assert_eq!(running.len(), 2, "{running:?}");
    let mut stats = b.stats(
        "it_box",
        StatsOptions {
            stream: false,
            one_shot: true,
        },
    );
    let sample = stats.next().await.unwrap().unwrap();
    drop(stats);
    assert!(sample.memory_usage.unwrap_or(0) > 0, "{sample:?}");

    // Exit codes.
    b.create_container("it_exit", sh("exit 7", "none"))
        .await
        .unwrap();
    b.start_container("it_exit").await.unwrap();
    assert_eq!(wait_exited(&b, "it_exit").await.exit_code, Some(7));

    // Stop: sh as PID 1 ignores SIGTERM, so this ends in SIGKILL.
    b.stop_container("it_side", StopContainerOptions { t: 1 })
        .await
        .unwrap();
    b.stop_container("it_box", StopContainerOptions { t: 1 })
        .await
        .unwrap();
    let st = wait_exited(&b, "it_box").await;
    assert_eq!(st.exit_code, Some(137));
    assert!(st.finished_at.is_some());

    let all = b
        .list_containers(ListContainersOptions {
            all: true,
            filters: HashMap::from([("status".to_string(), vec!["exited".to_string()])]),
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    for n in ["it_side", "it_box", "it_exit"] {
        b.remove_container(n, RemoveContainerOptions { force: false })
            .await
            .unwrap();
        assert!(b.inspect_container(n).await.is_err());
    }
}
