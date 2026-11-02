//! `rusternetes-youki-shim`: a small single-threaded process per container
//! (and per exec) that calls libcontainer and stays the parent of the
//! container's init process.
//!
//! libcontainer clones processes, which is not safe from the kubelet's
//! multi-threaded tokio runtime. The shim also outlives kubelet restarts and
//! is the one that reaps the container, so exit codes are never lost.
//!
//! ```text
//! rusternetes-youki-shim create <state-root> <id> <bundle> <log> <exit-file>
//! rusternetes-youki-shim exec   <state-root> <id> <args.json> <out> <err> <exit-file>
//! ```
//!
//! `create` prints `ok <pid>` (or `err <message>`) on stdout once the
//! container exists, then waits for it and writes the exit file.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};

use libcontainer::container::builder::ContainerBuilder;
use libcontainer::syscall::syscall::SyscallType;
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

/// Written by the shim when the process it watches exits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitRecord {
    pub exit_code: i64,
    /// RFC 3339.
    pub finished_at: String,
    #[serde(default)]
    pub error: Option<String>,
}

pub fn write_exit(path: &Path, exit_code: i64, error: Option<String>) {
    let rec = ExitRecord {
        exit_code,
        finished_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        error,
    };
    let _ = super::image::write_json(path, &rec);
}

fn wait_exit_code(pid: Pid) -> i64 {
    loop {
        match waitpid(pid, None) {
            Ok(WaitStatus::Exited(_, code)) => return code as i64,
            Ok(WaitStatus::Signaled(_, sig, _)) => return 128 + sig as i64,
            Ok(_) => continue,
            Err(nix::errno::Errno::EINTR) => continue,
            Err(_) => return 255,
        }
    }
}

fn devnull() -> std::io::Result<OwnedFd> {
    Ok(File::open("/dev/null")?.into())
}

fn append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// Entry point of the shim binary; returns the process exit code.
pub fn main(args: Vec<String>) -> i32 {
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    match a.as_slice() {
        [_, "create", state, id, bundle, log, exit] => create(
            state.into(),
            id,
            bundle.into(),
            Path::new(log),
            Path::new(exit),
        ),
        [_, "exec", state, id, args, out, err, exit] => exec(
            state.into(),
            id,
            Path::new(args),
            Path::new(out),
            Path::new(err),
            Path::new(exit),
        ),
        _ => {
            eprintln!("usage: rusternetes-youki-shim create|exec ...");
            2
        }
    }
}

fn create(state: PathBuf, id: &str, bundle: PathBuf, log: &Path, exit: &Path) -> i32 {
    // Own session: signals to the kubelet's process group do not reach us.
    let _ = nix::unistd::setsid();
    let built = (|| -> anyhow::Result<i32> {
        let log = append(log)?;
        let container = ContainerBuilder::new(id.to_string(), SyscallType::default())
            .with_root_path(state)?
            .with_stdin(devnull()?)
            .with_stdout(log.try_clone()?)
            .with_stderr(log)
            .as_init(bundle)
            .with_systemd(false)
            .with_detach(true)
            .build()?;
        container
            .pid()
            .map(|p| p.as_raw())
            .ok_or_else(|| anyhow::anyhow!("container has no init pid"))
    })();
    let mut stdout = std::io::stdout();
    let pid = match built {
        Ok(pid) => {
            let _ = writeln!(stdout, "ok {}", pid);
            let _ = stdout.flush();
            pid
        }
        Err(e) => {
            let _ = writeln!(stdout, "err {:#}", e);
            return 1;
        }
    };
    let code = wait_exit_code(Pid::from_raw(pid));
    write_exit(exit, code, None);
    0
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecArgs {
    pub cmd: Vec<String>,
}

fn exec(state: PathBuf, id: &str, args: &Path, out: &Path, err: &Path, exit: &Path) -> i32 {
    let result = (|| -> anyhow::Result<i64> {
        let args: ExecArgs = serde_json::from_slice(&std::fs::read(args)?)?;
        let pid = ContainerBuilder::new(id.to_string(), SyscallType::default())
            .with_root_path(state)?
            .with_stdin(devnull()?)
            .with_stdout(append(out)?)
            .with_stderr(append(err)?)
            .as_tenant()
            .with_container_args(args.cmd)
            .with_detach(true)
            .build()?;
        Ok(wait_exit_code(pid))
    })();
    match result {
        Ok(code) => {
            write_exit(exit, code, None);
            code as i32
        }
        Err(e) => {
            let msg = format!("{:#}", e);
            if let Ok(mut f) = append(err) {
                let _ = writeln!(f, "{}", msg);
            }
            // Docker: 127 for a missing command, 126 for one that cannot run.
            let code = if msg.contains("not found") || msg.contains("No such file") {
                127
            } else {
                126
            };
            write_exit(exit, code, Some(msg));
            code as i32
        }
    }
}
