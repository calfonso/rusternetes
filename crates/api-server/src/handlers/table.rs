/// Table output format support for kubectl get commands.
///
/// All types and functions are defined in `rusternetes-middleware` and
/// re-exported here so existing call sites (`crate::handlers::table::…`)
/// continue to work without modification.
pub use rusternetes_middleware::table::*;
