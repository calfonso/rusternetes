pub mod admission;
pub mod audit;
pub mod auth;
pub mod authz;
pub mod cel;
pub mod cloud_provider;
pub mod deletion;
pub mod encryption;
pub mod error;
pub mod field_selector;
pub mod label_selector;
pub mod leader_election;
pub mod observability;
pub mod pagination;
pub mod protobuf;
pub mod quantity;
pub mod resources;
pub mod schema_validation;
pub mod server_side_apply;
pub mod tls;
pub mod tracing;
pub mod types;
pub mod validation;

pub use cel::{CELContext, CELEvaluator};
pub use error::{Error, Result};
pub use pagination::{paginate, PaginationError, PaginationParams};
pub use types::{List, ListMeta, Status, StatusCause, StatusDetails};

/// Deserialize null as the default value for a type.
/// Use with `#[serde(deserialize_with = "crate::deserialize_null_default")]`
pub fn deserialize_null_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + serde::Deserialize<'de>,
{
    Ok(<Option<T> as serde::Deserialize>::deserialize(deserializer)?.unwrap_or_default())
}
