//! HTTP response handling with content negotiation.
//!
//! All types and functions are defined in `rusternetes-middleware` and
//! re-exported here so existing call sites (`crate::response::…`) continue to
//! work without modification.
pub use rusternetes_middleware::response::*;
