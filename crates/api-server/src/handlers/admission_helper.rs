//! Shared helpers for invoking admission webhooks from core-resource handlers.
//!
//! Core (typed) resource handlers — ConfigMap, Pod, Secret, etc. — each invoke
//! the [`crate::admission_webhook::AdmissionWebhookManager`] directly on
//! create/update. DELETE admission was historically only wired for custom
//! resources. This module centralizes the DELETE-time validating-webhook call so
//! every core resource gets the same behavior with one tested code path.
//!
//! K8s semantics modeled here:
//! - On DELETE, only **validating** webhooks run; mutating webhooks are never
//!   invoked for a delete. The AdmissionReview carries `object = nil` and
//!   `oldObject = <resource being deleted>`, so the webhook inspects `oldObject`.
//!   K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/validating/dispatcher.go
//! - A denial is surfaced as `403 Forbidden` with the upstream-compatible
//!   "admission webhook denied the request" prefix.

use crate::state::ApiServerState;
use rusternetes_common::auth::UserInfo as AuthUserInfo;
use rusternetes_common::{admission, Result};
use serde::Serialize;
use std::sync::Arc;

/// Run validating admission webhooks for a DELETE of a core resource.
///
/// `group`/`version` are the GVR coordinates (e.g. `""`/`"v1"` for ConfigMap),
/// `kind` the resource Kind (`"ConfigMap"`), `resource` the plural lowercase
/// resource name (`"configmaps"`). `namespace` is `None` for cluster-scoped
/// resources. `old_object` is the resource currently in storage that is about to
/// be deleted; it is serialized into the AdmissionReview's `oldObject`.
///
/// Returns `Err(Forbidden)` if any matching validating webhook denies the
/// request (or fails closed under `failurePolicy: Fail`). Returns `Ok(())` when
/// no webhook matches or all matching webhooks allow.
#[allow(clippy::too_many_arguments)]
pub async fn run_delete_validating_webhooks<T: Serialize>(
    state: &Arc<ApiServerState>,
    group: &str,
    version: &str,
    kind: &str,
    resource: &str,
    namespace: Option<&str>,
    name: &str,
    old_object: &T,
    user: &AuthUserInfo,
    is_dry_run: bool,
) -> Result<()> {
    let gvk = admission::GroupVersionKind {
        group: group.to_string(),
        version: version.to_string(),
        kind: kind.to_string(),
    };
    let gvr = admission::GroupVersionResource {
        group: group.to_string(),
        version: version.to_string(),
        resource: resource.to_string(),
    };
    let user_info = admission::UserInfo {
        username: user.username.clone(),
        uid: user.uid.clone(),
        groups: user.groups.clone(),
    };

    // DELETE AdmissionReview: object is nil, oldObject is the resource being deleted.
    let old_value = serde_json::to_value(old_object).ok();

    if let admission::AdmissionResponse::Deny(reason) = state
        .webhook_manager
        .run_validating_webhooks_with_dryrun(
            &admission::Operation::Delete,
            &gvk,
            &gvr,
            namespace,
            name,
            None,
            old_value,
            &user_info,
            is_dry_run,
        )
        .await?
    {
        return Err(rusternetes_common::Error::Forbidden(format!(
            "admission webhook denied the request: {}",
            reason
        )));
    }

    Ok(())
}
