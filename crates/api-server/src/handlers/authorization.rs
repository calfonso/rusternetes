use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, State},
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    auth::UserInfo,
    authz::{Decision, RequestAttributes},
    resources::{
        LocalSubjectAccessReview, SelfSubjectAccessReview, SelfSubjectRulesReview,
        SubjectAccessReview, SubjectAccessReviewStatus, SubjectRulesReviewStatus,
    },
    Result,
};
use std::sync::Arc;
use tracing::info;

/// Create a SubjectAccessReview (authorization.k8s.io/v1)
/// SubjectAccessReview checks whether or not a user or group can perform an action.
pub async fn create_subject_access_review(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    DumpingJson(mut sar): DumpingJson<SubjectAccessReview>,
) -> Result<Json<SubjectAccessReview>> {
    info!("Creating subject access review");

    // Check authorization - creating a SubjectAccessReview requires impersonation privileges
    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "create", "subjectaccessreviews")
        .with_api_group("authorization.k8s.io");

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Build the authorization request from the spec
    // Exactly one of resourceAttributes / nonResourceAttributes may be set
    // (upstream ValidateSubjectAccessReviewSpec rejects both being present).
    if sar.spec.resource_attributes.is_some() && sar.spec.non_resource_attributes.is_some() {
        return Err(rusternetes_common::Error::Invalid(vec![
            rusternetes_common::validation::field::Error::invalid(
                &rusternetes_common::validation::field::Path::new("spec")
                    .child("nonResourceAttributes"),
                "<set>".to_string(),
                "cannot be specified in combination with resourceAttributes",
            ),
        ]));
    }

    let check_attrs = if let Some(ref resource_attrs) = sar.spec.resource_attributes {
        let mut attrs = RequestAttributes::new(
            UserInfo {
                username: sar.spec.user.clone().unwrap_or_default(),
                uid: sar.spec.uid.clone().unwrap_or_default(),
                groups: sar.spec.groups.clone().unwrap_or_default(),
                extra: sar.spec.extra.clone().unwrap_or_default(),
            },
            resource_attrs.verb.as_deref().unwrap_or(""),
            resource_attrs.resource.as_deref().unwrap_or(""),
        );

        if let Some(ref group) = resource_attrs.group {
            attrs = attrs.with_api_group(group);
        }
        if let Some(ref namespace) = resource_attrs.namespace {
            attrs = attrs.with_namespace(namespace);
        }
        if let Some(ref name) = resource_attrs.name {
            attrs = attrs.with_name(name);
        }

        attrs
    } else if let Some(ref non_resource_attrs) = sar.spec.non_resource_attributes {
        RequestAttributes::new_non_resource(
            UserInfo {
                username: sar.spec.user.clone().unwrap_or_default(),
                uid: sar.spec.uid.clone().unwrap_or_default(),
                groups: sar.spec.groups.clone().unwrap_or_default(),
                extra: sar.spec.extra.clone().unwrap_or_default(),
            },
            non_resource_attrs.verb.as_deref().unwrap_or(""),
            non_resource_attrs.path.as_deref().unwrap_or(""),
        )
    } else {
        return Err(rusternetes_common::Error::Internal(
            "Either resourceAttributes or nonResourceAttributes must be specified".to_string(),
        ));
    };

    // Perform the authorization check
    let decision = state.authorizer.authorize(&check_attrs).await?;

    sar.status = Some(SubjectAccessReviewStatus {
        allowed: matches!(decision, Decision::Allow),
        denied: Some(matches!(decision, Decision::Deny(_))),
        evaluation_error: None,
        reason: match decision {
            Decision::Allow => Some("Allowed by RBAC".to_string()),
            Decision::Deny(reason) => Some(reason),
        },
    });

    Ok(Json(sar))
}

/// Create a SelfSubjectAccessReview (authorization.k8s.io/v1)
/// SelfSubjectAccessReview checks whether the current user can perform an action.
pub async fn create_self_subject_access_review(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    DumpingJson(mut ssar): DumpingJson<SelfSubjectAccessReview>,
) -> Result<Json<SelfSubjectAccessReview>> {
    info!(
        "Creating self subject access review for user: {}",
        auth_ctx.user.username
    );

    // SelfSubjectAccessReview does not support Table/PartialObjectMetadata format.
    // Return 406 Not Acceptable when client requests it.
    if let Some(accept) = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
    {
        if accept.contains("as=Table") || accept.contains("as=PartialObjectMetadata") {
            return Err(rusternetes_common::Error::NotAcceptable(
                "the resource does not support the requested content type".to_string(),
            ));
        }
    }

    // Creating a SelfSubjectAccessReview is always allowed
    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "create", "selfsubjectaccessreviews")
        .with_api_group("authorization.k8s.io");

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Build the authorization request from the spec using the current user
    // Exactly one of resourceAttributes / nonResourceAttributes may be set
    // (upstream ValidateSubjectAccessReviewSpec rejects both being present).
    if ssar.spec.resource_attributes.is_some() && ssar.spec.non_resource_attributes.is_some() {
        return Err(rusternetes_common::Error::Invalid(vec![
            rusternetes_common::validation::field::Error::invalid(
                &rusternetes_common::validation::field::Path::new("spec")
                    .child("nonResourceAttributes"),
                "<set>".to_string(),
                "cannot be specified in combination with resourceAttributes",
            ),
        ]));
    }

    let check_attrs = if let Some(ref resource_attrs) = ssar.spec.resource_attributes {
        let mut attrs = RequestAttributes::new(
            auth_ctx.user.clone(),
            resource_attrs.verb.as_deref().unwrap_or(""),
            resource_attrs.resource.as_deref().unwrap_or(""),
        );

        if let Some(ref group) = resource_attrs.group {
            attrs = attrs.with_api_group(group);
        }
        if let Some(ref namespace) = resource_attrs.namespace {
            attrs = attrs.with_namespace(namespace);
        }
        if let Some(ref name) = resource_attrs.name {
            attrs = attrs.with_name(name);
        }

        attrs
    } else if let Some(ref non_resource_attrs) = ssar.spec.non_resource_attributes {
        RequestAttributes::new_non_resource(
            auth_ctx.user.clone(),
            non_resource_attrs.verb.as_deref().unwrap_or(""),
            non_resource_attrs.path.as_deref().unwrap_or(""),
        )
    } else {
        return Err(rusternetes_common::Error::Internal(
            "Either resourceAttributes or nonResourceAttributes must be specified".to_string(),
        ));
    };

    // Perform the authorization check
    let decision = state.authorizer.authorize(&check_attrs).await?;

    ssar.status = Some(SubjectAccessReviewStatus {
        allowed: matches!(decision, Decision::Allow),
        denied: Some(matches!(decision, Decision::Deny(_))),
        evaluation_error: None,
        reason: match decision {
            Decision::Allow => Some("Allowed by RBAC".to_string()),
            Decision::Deny(reason) => Some(reason),
        },
    });

    Ok(Json(ssar))
}

/// Create a LocalSubjectAccessReview (authorization.k8s.io/v1)
/// LocalSubjectAccessReview checks whether or not a user or group can perform an action in a given namespace.
pub async fn create_local_subject_access_review(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    DumpingJson(mut lsar): DumpingJson<LocalSubjectAccessReview>,
) -> Result<Json<LocalSubjectAccessReview>> {
    info!(
        "Creating local subject access review in namespace: {}",
        namespace
    );

    // Check authorization
    let attrs =
        RequestAttributes::new(auth_ctx.user.clone(), "create", "localsubjectaccessreviews")
            .with_api_group("authorization.k8s.io")
            .with_namespace(&namespace);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Build the authorization request from the spec
    // LocalSubjectAccessReview is namespace-scoped
    // Exactly one of resourceAttributes / nonResourceAttributes may be set
    // (upstream ValidateSubjectAccessReviewSpec rejects both being present).
    if lsar.spec.resource_attributes.is_some() && lsar.spec.non_resource_attributes.is_some() {
        return Err(rusternetes_common::Error::Invalid(vec![
            rusternetes_common::validation::field::Error::invalid(
                &rusternetes_common::validation::field::Path::new("spec")
                    .child("nonResourceAttributes"),
                "<set>".to_string(),
                "cannot be specified in combination with resourceAttributes",
            ),
        ]));
    }

    let check_attrs = if let Some(ref resource_attrs) = lsar.spec.resource_attributes {
        let mut attrs = RequestAttributes::new(
            UserInfo {
                username: lsar.spec.user.clone().unwrap_or_default(),
                uid: lsar.spec.uid.clone().unwrap_or_default(),
                groups: lsar.spec.groups.clone().unwrap_or_default(),
                extra: lsar.spec.extra.clone().unwrap_or_default(),
            },
            resource_attrs.verb.as_deref().unwrap_or(""),
            resource_attrs.resource.as_deref().unwrap_or(""),
        );

        if let Some(ref group) = resource_attrs.group {
            attrs = attrs.with_api_group(group);
        }
        // Force the namespace to be the one specified in the URL
        attrs = attrs.with_namespace(&namespace);
        if let Some(ref name) = resource_attrs.name {
            attrs = attrs.with_name(name);
        }

        attrs
    } else {
        return Err(rusternetes_common::Error::Internal(
            "resourceAttributes must be specified for LocalSubjectAccessReview".to_string(),
        ));
    };

    // Perform the authorization check
    let decision = state.authorizer.authorize(&check_attrs).await?;

    lsar.status = Some(SubjectAccessReviewStatus {
        allowed: matches!(decision, Decision::Allow),
        denied: Some(matches!(decision, Decision::Deny(_))),
        evaluation_error: None,
        reason: match decision {
            Decision::Allow => Some("Allowed by RBAC".to_string()),
            Decision::Deny(reason) => Some(reason),
        },
    });

    Ok(Json(lsar))
}

/// Create a SelfSubjectRulesReview (authorization.k8s.io/v1)
/// SelfSubjectRulesReview enumerates the set of actions the current user can perform within a namespace.
pub async fn create_self_subject_rules_review(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    DumpingJson(mut ssrr): DumpingJson<SelfSubjectRulesReview>,
) -> Result<Json<SelfSubjectRulesReview>> {
    info!(
        "Creating self subject rules review for user: {} in namespace: {}",
        auth_ctx.user.username, ssrr.spec.namespace
    );

    // Creating a SelfSubjectRulesReview is always allowed
    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "create", "selfsubjectrulesreviews")
        .with_api_group("authorization.k8s.io");

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Get all rules that apply to the current user in the specified namespace
    let (resource_rules, non_resource_rules) = state
        .authorizer
        .get_user_rules(&auth_ctx.user, &ssrr.spec.namespace)
        .await?;

    ssrr.status = Some(SubjectRulesReviewStatus {
        resource_rules,
        non_resource_rules,
        incomplete: false,
        evaluation_error: None,
    });

    Ok(Json(ssrr))
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_subject_access_review_allowed() {
        // This test would verify that a user with permissions gets allowed=true
    }

    #[tokio::test]
    async fn test_subject_access_review_denied() {
        // This test would verify that a user without permissions gets allowed=false
    }

    #[tokio::test]
    async fn test_self_subject_access_review() {
        // This test would verify that the current user can check their own permissions
    }

    #[tokio::test]
    async fn test_self_subject_rules_review() {
        // This test would verify that the current user can enumerate their permissions
    }
}
