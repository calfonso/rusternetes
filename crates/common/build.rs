// Capture the git commit SHA and build time so every binary can log which
// build it is — `.git` is excluded from the Docker build context
// (.dockerignore), so container builds inject the SHA via the
// `RUSTERNETES_GIT_SHA` env/ARG; local `cargo build` reads it from the checkout.
use std::process::Command;

fn main() {
    // Re-run when HEAD moves (local builds) or the injected SHA changes (Docker).
    println!("cargo:rerun-if-env-changed=RUSTERNETES_GIT_SHA");
    // Ask git for the real HEAD/refs locations rather than hardcoding
    // `../../.git/...`: in a linked worktree `.git` is a *file* pointing
    // elsewhere, so the hardcoded path misses and the stamped SHA goes stale
    // on every rebuild until something unrelated forces a recompile.
    for rel in ["HEAD", "refs"] {
        if let Some(p) = git_path(rel) {
            if std::path::Path::new(&p).exists() {
                println!("cargo:rerun-if-changed={p}");
            }
        }
    }

    let sha = std::env::var("RUSTERNETES_GIT_SHA")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .or_else(git_sha)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=RUSTERNETES_BUILD_SHA={sha}");

    println!("cargo:rustc-env=RUSTERNETES_BUILD_TIME={}", build_time());
}

/// Resolve a path inside the git dir (e.g. `HEAD`, `refs`) in a way that works
/// from both a normal checkout and a linked worktree. Returns git's own
/// `rev-parse --git-path` answer, which already accounts for the worktree
/// indirection.
fn git_path(rel: &str) -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--git-path", rel])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let p = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if p.is_empty() {
        None
    } else {
        Some(p)
    }
}

fn git_sha() -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let mut sha = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if sha.is_empty() {
        return None;
    }
    // Mark an uncommitted working tree so a dev build isn't mistaken for the
    // exact commit.
    if let Ok(st) = Command::new("git").args(["status", "--porcelain"]).output() {
        if !st.stdout.is_empty() {
            sha.push_str("-dirty");
        }
    }
    Some(sha)
}

fn build_time() -> String {
    // Prefer a reproducible source date (set by some CI), else wall-clock UTC.
    if let Ok(epoch) = std::env::var("SOURCE_DATE_EPOCH") {
        if !epoch.trim().is_empty() {
            return format!("epoch:{}", epoch.trim());
        }
    }
    Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}
