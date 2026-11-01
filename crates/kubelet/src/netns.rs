//! Named network namespaces under `/var/run/netns`, the way `ip netns`
//! makes them, without iproute2 on the node.
//!
//! `add` creates an empty file, unshares a new network namespace on a
//! short-lived thread and bind-mounts that thread's namespace onto the file,
//! so the namespace outlives the thread. `del` unmounts and removes it. CNI
//! plugins and the container backend open the path as before.

use std::fs::{self, OpenOptions};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

/// Where named network namespaces live (iproute2's `NETNS_RUN_DIR`).
pub const NETNS_RUN_DIR: &str = "/var/run/netns";

/// The path of the named namespace.
pub fn path(name: &str) -> PathBuf {
    Path::new(NETNS_RUN_DIR).join(name)
}

fn cstr(p: &Path) -> io::Result<std::ffi::CString> {
    std::ffi::CString::new(p.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path with a NUL byte"))
}

fn check(rc: libc::c_int) -> io::Result<()> {
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Make the run directory a shared mount point, as iproute2 does, so a
/// namespace mounted later is seen by mount namespaces created after it.
fn share_run_dir() -> io::Result<()> {
    fs::create_dir_all(NETNS_RUN_DIR)?;
    let dir = cstr(Path::new(NETNS_RUN_DIR))?;
    let flags = libc::MS_SHARED | libc::MS_REC;
    // SAFETY: every pointer is a NUL-terminated string that outlives the
    // call, or null where mount(2) allows it (a propagation change).
    let shared = unsafe {
        libc::mount(
            c"none".as_ptr(),
            dir.as_ptr(),
            std::ptr::null(),
            flags,
            std::ptr::null(),
        )
    };
    if shared == 0 {
        return Ok(());
    }
    // Not a mount point yet: bind it onto itself, then share it.
    // SAFETY: as above.
    check(unsafe {
        libc::mount(
            dir.as_ptr(),
            dir.as_ptr(),
            c"none".as_ptr(),
            libc::MS_BIND | libc::MS_REC,
            std::ptr::null(),
        )
    })?;
    // SAFETY: as above.
    check(unsafe {
        libc::mount(
            c"none".as_ptr(),
            dir.as_ptr(),
            std::ptr::null(),
            flags,
            std::ptr::null(),
        )
    })
}

/// Create the named network namespace. An existing one is an
/// [`io::ErrorKind::AlreadyExists`] error.
pub fn add(name: &str) -> io::Result<()> {
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("bad netns name {name:?}"),
        ));
    }
    share_run_dir()?;
    let target = path(name);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target)?;
    let t = target.clone();
    let made = std::thread::spawn(move || -> io::Result<()> {
        // SAFETY: unshare(2) only changes this thread's namespaces.
        check(unsafe { libc::unshare(libc::CLONE_NEWNET) })?;
        let src = cstr(Path::new("/proc/thread-self/ns/net"))?;
        let dst = cstr(&t)?;
        // SAFETY: NUL-terminated strings that outlive the call; a bind takes
        // no filesystem type or data.
        check(unsafe {
            libc::mount(
                src.as_ptr(),
                dst.as_ptr(),
                c"none".as_ptr(),
                libc::MS_BIND,
                std::ptr::null(),
            )
        })
    })
    .join()
    .unwrap_or_else(|_| Err(io::Error::other("netns thread panicked")));
    if made.is_err() {
        let _ = fs::remove_file(&target);
    }
    made
}

/// Remove the named network namespace (unmount, then delete the file).
pub fn del(name: &str) -> io::Result<()> {
    let target = path(name);
    let c = cstr(&target)?;
    // SAFETY: a NUL-terminated path that outlives the call.
    let _ = unsafe { libc::umount2(c.as_ptr(), libc::MNT_DETACH) };
    fs::remove_file(&target)
}
