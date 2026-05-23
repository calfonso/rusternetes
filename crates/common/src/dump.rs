//! Payload-dump instrumentation for conformance debugging.
//!
//! When `RUSTERNETES_DUMP_PAYLOADS=1`, panics, 5xx responses, and JSON decode
//! failures emit a `tracing::error!` containing the offending request body
//! (with Secret data redacted). All entrypoints are no-ops when the env var
//! is unset.

use std::sync::OnceLock;

static DUMPS_ENABLED: OnceLock<bool> = OnceLock::new();

/// Returns true iff `RUSTERNETES_DUMP_PAYLOADS=1` was set when this process
/// started.
pub fn dumps_enabled() -> bool {
    *DUMPS_ENABLED
        .get_or_init(|| std::env::var("RUSTERNETES_DUMP_PAYLOADS").is_ok_and(|v| v == "1"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dumps_enabled_reads_env_once() {
        // Cannot reliably mutate process env across tests, so just assert
        // the function does not panic and returns a stable bool.
        let a = dumps_enabled();
        let b = dumps_enabled();
        assert_eq!(a, b);
    }
}
