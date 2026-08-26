//! API pagination support for Kubernetes-compliant list operations
//!
//! Implements limit/continue pagination as per Kubernetes API conventions:
//! - limit: Maximum number of items to return
//! - continue: Continuation token from a previous response
//! - remainingItemCount: Number of items remaining after this page

use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Pagination parameters from query string
#[derive(Debug, Clone, Deserialize)]
pub struct PaginationParams {
    /// Maximum number of items to return per page
    #[serde(default)]
    pub limit: Option<i64>,

    /// Continue token from previous response
    #[serde(default, rename = "continue")]
    pub continue_token: Option<String>,
}

/// Internal continuation token structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContinuationToken {
    /// Starting index for next page
    start: usize,
    /// Resource version for consistency
    resource_version: String,
    /// Optional filter parameters to ensure consistency
    filters: HashMap<String, String>,
    /// Unique nonce to ensure tokens differ across requests
    #[serde(default)]
    nonce: u64,
    /// Total item count at time of token creation (for staleness detection)
    #[serde(default)]
    total_at_creation: usize,
    /// Unix timestamp when the token was created (for expiry detection)
    #[serde(default)]
    created_at: u64,
}

impl ContinuationToken {
    /// Encode the token to a base64 string
    fn encode(&self) -> Result<String, serde_json::Error> {
        let json = serde_json::to_string(self)?;
        Ok(general_purpose::STANDARD.encode(json.as_bytes()))
    }

    /// Decode a token from base64 string
    fn decode(token: &str) -> Result<Self, String> {
        let bytes = general_purpose::STANDARD
            .decode(token)
            .map_err(|e| format!("Invalid continue token: {}", e))?;

        let json = String::from_utf8(bytes)
            .map_err(|e| format!("Invalid continue token encoding: {}", e))?;

        serde_json::from_str(&json).map_err(|e| format!("Invalid continue token format: {}", e))
    }
}

/// Result of paginating a list of items
#[derive(Debug)]
pub struct PaginatedResult<T> {
    /// Items for this page
    pub items: Vec<T>,
    /// Continue token for next page (None if this is the last page)
    pub continue_token: Option<String>,
    /// Number of remaining items after this page
    pub remaining_item_count: Option<i64>,
    /// The resource version to use in the response (consistent across pages)
    pub resource_version: String,
}

/// Error returned when pagination fails
#[derive(Debug)]
pub struct PaginationError {
    pub message: String,
    /// A fresh continue token the client can use to restart the list
    pub fresh_continue_token: Option<String>,
}

/// Paginate a list of items according to Kubernetes API conventions
pub fn paginate<T>(
    mut items: Vec<T>,
    params: PaginationParams,
    resource_version: &str,
) -> Result<PaginatedResult<T>, PaginationError> {
    // Parse continue token if provided
    // Use the token's resource_version for consistency across pages
    let (start, effective_rv) = if let Some(token) = &params.continue_token {
        let cont = ContinuationToken::decode(token).map_err(|e| PaginationError {
            message: e,
            fresh_continue_token: None,
        })?;

        let is_stale =
            // Check if token expired (etcd compaction, 5 min).
            // K8s uses resourceVersion consistency, not item count comparison.
            // Item count can change between pages (controllers creating resources)
            // without invalidating the pagination — items are sorted by key.
            cont.created_at > 0 && {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                now.saturating_sub(cont.created_at) > 300
            };

        if is_stale {
            // Generate a fresh continue token starting from the same offset
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let fresh = ContinuationToken {
                start: cont.start.min(items.len()),
                total_at_creation: items.len(),
                resource_version: resource_version.to_string(),
                filters: HashMap::new(),
                nonce,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            };
            let fresh_token = fresh.encode().ok();
            return Err(PaginationError {
                message: "410 Gone: the resource version in the continue token is too old; the client must restart the list without a continue token".to_string(),
                fresh_continue_token: fresh_token,
            });
        }

        // Use the token's resource_version for consistent pagination
        (cont.start, cont.resource_version.clone())
    } else {
        (0, resource_version.to_string())
    };

    // If no limit is specified, return all items
    let limit = match params.limit {
        Some(l) if l > 0 => l as usize,
        Some(0) => {
            // limit=0 is a special case that returns only metadata
            // For now, treat it as returning an empty list
            return Ok(PaginatedResult {
                items: vec![],
                continue_token: None,
                remaining_item_count: Some(items.len() as i64),
                resource_version: effective_rv,
            });
        }
        _ => {
            // No limit, return all items from start
            let result_items = items.drain(start..).collect();
            return Ok(PaginatedResult {
                items: result_items,
                continue_token: None,
                remaining_item_count: None,
                resource_version: effective_rv,
            });
        }
    };

    let total = items.len();

    // Check if start is beyond the list
    if start >= total {
        return Ok(PaginatedResult {
            items: vec![],
            continue_token: None,
            remaining_item_count: Some(0),
            resource_version: effective_rv,
        });
    }

    // Calculate end index
    let end = (start + limit).min(total);

    // Extract the page of items
    let page_items: Vec<T> = items.drain(start..end).collect();

    // Check if there are more items
    let (continue_token, remaining_count) = if end < total {
        // Include a nonce so tokens are always unique, even for the same offset
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let next_token = ContinuationToken {
            start: end,
            total_at_creation: total,
            resource_version: effective_rv.clone(),
            filters: HashMap::new(),
            nonce,
            created_at,
        };

        let token = next_token.encode().map_err(|e| PaginationError {
            message: format!("Failed to encode continue token: {}", e),
            fresh_continue_token: None,
        })?;

        (Some(token), Some((total - end) as i64))
    } else {
        // Last page: no continue token, no remainingItemCount
        // Kubernetes expects remainingItemCount to be nil when continue is empty
        (None, None)
    };

    Ok(PaginatedResult {
        items: page_items,
        continue_token,
        remaining_item_count: remaining_count,
        resource_version: effective_rv,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_pagination() {
        let items = vec![1, 2, 3, 4, 5];
        let params = PaginationParams {
            limit: None,
            continue_token: None,
        };

        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items.len(), 5);
        assert_eq!(result.continue_token, None);
        assert_eq!(result.remaining_item_count, None);
    }

    #[test]
    fn test_first_page() {
        let items = vec![1, 2, 3, 4, 5];
        let params = PaginationParams {
            limit: Some(2),
            continue_token: None,
        };

        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items, vec![1, 2]);
        assert!(result.continue_token.is_some());
        assert_eq!(result.remaining_item_count, Some(3));
    }

    #[test]
    fn test_second_page() {
        let items = vec![1, 2, 3, 4, 5];

        // Get first page
        let params = PaginationParams {
            limit: Some(2),
            continue_token: None,
        };
        let first_result = paginate(items.clone(), params, "v1").unwrap();

        // Get second page using continue token
        let params = PaginationParams {
            limit: Some(2),
            continue_token: first_result.continue_token,
        };
        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items, vec![3, 4]);
        assert!(result.continue_token.is_some());
        assert_eq!(result.remaining_item_count, Some(1));
    }

    #[test]
    fn test_last_page() {
        let items = vec![1, 2, 3, 4, 5];

        // Simulate getting to the last page
        let token = ContinuationToken {
            start: 4,
            resource_version: "v1".to_string(),
            filters: HashMap::new(),
            nonce: 0,
            total_at_creation: 0,
            created_at: 0,
        }
        .encode()
        .unwrap();

        let params = PaginationParams {
            limit: Some(2),
            continue_token: Some(token),
        };
        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items, vec![5]);
        assert_eq!(result.continue_token, None);
        // Kubernetes convention: remainingItemCount is nil when continue is empty
        assert_eq!(result.remaining_item_count, None);
    }

    #[test]
    fn test_limit_zero() {
        let items = vec![1, 2, 3, 4, 5];
        let params = PaginationParams {
            limit: Some(0),
            continue_token: None,
        };

        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items.len(), 0);
        assert_eq!(result.continue_token, None);
        assert_eq!(result.remaining_item_count, Some(5));
    }

    #[test]
    fn test_resource_version_mismatch_is_tolerated() {
        let items = vec![1, 2, 3, 4, 5];

        let token = ContinuationToken {
            start: 2,
            resource_version: "v1".to_string(),
            filters: HashMap::new(),
            nonce: 0,
            total_at_creation: 0,
            created_at: 0,
        }
        .encode()
        .unwrap();

        let params = PaginationParams {
            limit: Some(2),
            continue_token: Some(token),
        };

        // Resource version change should be tolerated (Kubernetes compacts versions)
        let result = paginate(items, params, "v2");

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.items, vec![3, 4]);
    }
}
