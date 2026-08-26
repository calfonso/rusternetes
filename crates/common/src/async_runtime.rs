//! Tokio runtime configuration shared by the Rusternetes binaries.

/// Stack size to reserve for tokio worker threads, in bytes.
///
/// Tokio defaults to 2 MiB per worker thread, which is not enough for the pod
/// lifecycle path in an unoptimised build. `Kubelet::sync_pod` is a single
/// `async fn` with over 130 `.await` points, and every awaited sub-future is
/// stored inline in the generated state machine, so a debug build spills
/// ~1.6 MiB of that state onto the stack in one frame — with
/// `ensure_pod_worker` adding ~0.4 MiB beneath it. Together they clear 2 MiB
/// and the first pod sync hits the guard page, aborting the whole process.
///
/// 8 MiB is the value `compose.sqlite.yml` already exports as `RUST_MIN_STACK`
/// for its kubelet services, and it matches the Linux default for the main
/// thread (`ulimit -s`). Thread stacks are mapped lazily, so the larger size
/// costs address space per worker thread rather than resident memory.
pub const DEFAULT_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;

// Frame sizes measured under gdb on a debug build: sync_pod took 1,626,304
// bytes of stack, with ensure_pod_worker adding 438,656 beneath it. The default
// must stay clear of that, so lowering it fails the build.
const _: () = assert!(DEFAULT_WORKER_STACK_SIZE > 1_626_304 + 438_656);

/// Stack size to give tokio worker threads.
///
/// [`DEFAULT_WORKER_STACK_SIZE`], unless `RUST_MIN_STACK` asks for more.
/// Passing an explicit size to `thread_stack_size` makes the standard library
/// stop consulting `RUST_MIN_STACK`, so it is honoured here to keep the
/// environment variable working as an escape hatch for anyone who needs a
/// deeper stack than the default provides.
pub fn worker_stack_size() -> usize {
    resolve_worker_stack_size(std::env::var("RUST_MIN_STACK").ok().as_deref())
}

/// Testable core of [`worker_stack_size`].
///
/// Takes the `RUST_MIN_STACK` value as an argument instead of reading the
/// environment, so tests do not have to mutate process-global state. The value
/// is parsed the way the standard library parses it: anything that is not a
/// plain integer is ignored.
fn resolve_worker_stack_size(rust_min_stack: Option<&str>) -> usize {
    let requested = rust_min_stack
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);

    requested.max(DEFAULT_WORKER_STACK_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_eight_mib() {
        assert_eq!(resolve_worker_stack_size(None), 8 * 1024 * 1024);
    }

    #[test]
    fn a_larger_rust_min_stack_wins() {
        assert_eq!(
            resolve_worker_stack_size(Some("16777216")),
            16 * 1024 * 1024
        );
    }

    #[test]
    fn a_smaller_rust_min_stack_does_not_shrink_the_stack() {
        // 2 MiB is the size that overflows, so it must never be selected.
        assert_eq!(
            resolve_worker_stack_size(Some("2097152")),
            DEFAULT_WORKER_STACK_SIZE
        );
    }

    #[test]
    fn an_unparsable_rust_min_stack_is_ignored() {
        for value in ["", "8MB", "-1", "8 388 608", "many"] {
            assert_eq!(
                resolve_worker_stack_size(Some(value)),
                DEFAULT_WORKER_STACK_SIZE,
                "RUST_MIN_STACK={value}"
            );
        }
    }
}
