use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding},
    types::LabelSelector,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Test whether `labels` satisfies a `types::LabelSelector` (matchLabels +
/// matchExpressions). Mirrors upstream `apimachinery/pkg/apis/meta/v1`
/// `LabelSelectorAsSelector` semantics used by the ClusterRole aggregation
/// controller. An empty selector (no matchLabels, no matchExpressions) matches
/// nothing here — upstream's aggregation controller skips empty selectors
/// rather than selecting everything.
fn label_selector_matches(selector: &LabelSelector, labels: &HashMap<String, String>) -> bool {
    let has_match_labels = selector
        .match_labels
        .as_ref()
        .is_some_and(|m| !m.is_empty());
    let has_match_exprs = selector
        .match_expressions
        .as_ref()
        .is_some_and(|e| !e.is_empty());
    if !has_match_labels && !has_match_exprs {
        return false;
    }

    if let Some(match_labels) = &selector.match_labels {
        for (k, v) in match_labels {
            if labels.get(k) != Some(v) {
                return false;
            }
        }
    }

    if let Some(exprs) = &selector.match_expressions {
        for req in exprs {
            let present = labels.contains_key(&req.key);
            let matched = match req.operator.as_str() {
                "In" => req
                    .values
                    .as_ref()
                    .is_some_and(|vals| labels.get(&req.key).is_some_and(|v| vals.contains(v))),
                "NotIn" => !req
                    .values
                    .as_ref()
                    .is_some_and(|vals| labels.get(&req.key).is_some_and(|v| vals.contains(v))),
                "Exists" => present,
                "DoesNotExist" => !present,
                _ => false,
            };
            if !matched {
                return false;
            }
        }
    }

    true
}

/// Whether `set` contains `ele`.
fn has(set: &[String], ele: &str) -> bool {
    set.iter().any(|s| s == ele)
}

/// Whether every element of `contains` is present in `set`.
fn has_all(set: &[String], contains: &[String]) -> bool {
    contains.iter().all(|ele| set.contains(ele))
}

/// Whether `owner` fully covers `servant` for the resource axis. Mirrors
/// upstream `resourceCoversAll`: an owner `*` covers everything, otherwise the
/// owner must list every requested resource explicitly.
fn resource_covers_all(owner: &[String], servant: &[String]) -> bool {
    has(owner, "*") || has_all(owner, servant)
}

/// Whether owner rule `owner` covers the (already broken-down) servant rule.
/// Mirrors upstream `ruleCovers` in
/// `component-helpers/auth/rbac/validation/policy_comparator.go` (release-1.35),
/// restricted to the resource axes RBAC RoleBindings care about.
fn rule_covers(owner: &PolicyRule, servant: &PolicyRule) -> bool {
    let owner_verbs = &owner.verbs;
    let verb_matches = has(owner_verbs, "*") || has_all(owner_verbs, &servant.verbs);

    let owner_groups = owner.api_groups.clone().unwrap_or_default();
    let servant_groups = servant.api_groups.clone().unwrap_or_default();
    let group_matches = has(&owner_groups, "*") || has_all(&owner_groups, &servant_groups);

    let owner_resources = owner.resources.clone().unwrap_or_default();
    let servant_resources = servant.resources.clone().unwrap_or_default();
    let resource_matches = resource_covers_all(&owner_resources, &servant_resources);

    let owner_names = owner.resource_names.clone().unwrap_or_default();
    let servant_names = servant.resource_names.clone().unwrap_or_default();
    let resource_name_matches = if servant_names.is_empty() {
        owner_names.is_empty()
    } else {
        owner_names.is_empty() || has_all(&owner_names, &servant_names)
    };

    verb_matches && group_matches && resource_matches && resource_name_matches
}

/// Break a PolicyRule down into atomic (verb x group x resource x resourceName)
/// tuples so that coverage can be decided against a single owner rule per
/// subrule. Mirrors upstream `BreakdownRule`. Only the resource axes are
/// modelled (non-resource URLs are not bound by RoleBindings).
fn breakdown_rule(rule: &PolicyRule) -> Vec<PolicyRule> {
    let mut out = Vec::new();
    let groups = rule.api_groups.clone().unwrap_or_default();
    let resources = rule.resources.clone().unwrap_or_default();
    let names = rule.resource_names.clone().unwrap_or_default();
    for verb in &rule.verbs {
        for group in &groups {
            for resource in &resources {
                if names.is_empty() {
                    out.push(PolicyRule {
                        verbs: vec![verb.clone()],
                        api_groups: Some(vec![group.clone()]),
                        resources: Some(vec![resource.clone()]),
                        resource_names: None,
                        non_resource_urls: None,
                    });
                } else {
                    for name in &names {
                        out.push(PolicyRule {
                            verbs: vec![verb.clone()],
                            api_groups: Some(vec![group.clone()]),
                            resources: Some(vec![resource.clone()]),
                            resource_names: Some(vec![name.clone()]),
                            non_resource_urls: None,
                        });
                    }
                }
            }
        }
    }
    out
}

/// Whether `owner_rules` fully cover every rule in `servant_rules`. Mirrors
/// upstream `Covers`: each servant rule is broken into atomic subrules and each
/// subrule must be covered by at least one owner rule.
fn rules_cover(owner_rules: &[PolicyRule], servant_rules: &[PolicyRule]) -> bool {
    for servant in servant_rules {
        for subrule in breakdown_rule(servant) {
            if !owner_rules.iter().any(|owner| rule_covers(owner, &subrule)) {
                return false;
            }
        }
    }
    true
}

/// Convert resolved `ResourceRule`s (from `Authorizer::get_user_rules`) into
/// `PolicyRule`s so they can be fed to the coverage comparator.
fn resource_rules_to_policy_rules(
    rules: &[rusternetes_common::resources::ResourceRule],
) -> Vec<PolicyRule> {
    rules
        .iter()
        .map(|r| PolicyRule {
            verbs: r.verbs.clone(),
            api_groups: r.api_groups.clone(),
            resources: r.resources.clone(),
            resource_names: r.resource_names.clone(),
            non_resource_urls: None,
        })
        .collect()
}

/// Materialise the `rules` of a ClusterRole carrying an `aggregationRule` by
/// unioning the rules of every ClusterRole whose labels match any of the
/// `clusterRoleSelectors`. Mirrors upstream
/// `pkg/controller/clusterroleaggregation` + the `clusterrole/policybased`
/// storage layer (release-1.35): the aggregated `rules` field is recomputed
/// server-side from the matching child ClusterRoles. The parent's own name is
/// excluded to avoid self-aggregation loops.
///
/// No-op when `aggregation_rule` is `None`.
async fn materialise_aggregated_rules<S: Storage>(storage: &Arc<S>, clusterrole: &mut ClusterRole) {
    let Some(aggregation_rule) = clusterrole.aggregation_rule.clone() else {
        return;
    };
    let Some(selectors) = aggregation_rule.cluster_role_selectors else {
        clusterrole.rules = Vec::new();
        return;
    };

    let prefix = build_prefix("clusterroles", None);
    let candidates = match storage.list::<ClusterRole>(&prefix).await {
        Ok(list) => list,
        Err(e) => {
            tracing::warn!(
                "Failed to list ClusterRoles for aggregation of {}: {}",
                clusterrole.metadata.name,
                e
            );
            return;
        }
    };

    let mut aggregated: Vec<PolicyRule> = Vec::new();
    for candidate in &candidates {
        // Skip the parent itself and any other aggregating ClusterRole — upstream
        // only collects rules from leaf (non-aggregating) ClusterRoles.
        if candidate.metadata.name == clusterrole.metadata.name {
            continue;
        }
        if candidate.aggregation_rule.is_some() {
            continue;
        }
        let labels = candidate.metadata.labels.clone().unwrap_or_default();
        let matches = selectors
            .iter()
            .any(|selector| label_selector_matches(selector, &labels));
        if !matches {
            continue;
        }
        for rule in &candidate.rules {
            if !aggregated.contains(rule) {
                aggregated.push(rule.clone());
            }
        }
    }

    clusterrole.rules = aggregated;
}

/// Resolve the PolicyRules granted by a binding's `roleRef`. A `Role` is looked
/// up in `binding_namespace`; a `ClusterRole` is looked up cluster-wide.
/// Returns an empty rule set if the referenced role does not exist (upstream's
/// `GetRoleReferenceRules` returns the rules it can resolve; a missing role
/// grants nothing, so an empty set is trivially covered).
async fn resolve_role_ref_rules(
    state: &Arc<ApiServerState>,
    role_ref: &rusternetes_common::resources::RoleRef,
    binding_namespace: &str,
) -> Vec<PolicyRule> {
    match role_ref.kind.as_str() {
        "ClusterRole" => {
            let key = build_key("clusterroles", None, &role_ref.name);
            match state.storage.get::<ClusterRole>(&key).await {
                Ok(cr) => cr.rules,
                Err(_) => Vec::new(),
            }
        }
        "Role" => {
            let key = build_key("roles", Some(binding_namespace), &role_ref.name);
            match state.storage.get::<Role>(&key).await {
                Ok(role) => role.rules,
                Err(_) => Vec::new(),
            }
        }
        _ => Vec::new(),
    }
}

/// Enforce the privilege-escalation prevention rule on (Cluster)RoleBinding
/// create. Mirrors upstream `pkg/registry/rbac/rolebinding/policybased`:
///
///   1. If the caller already holds every PolicyRule granted by the referenced
///      role (superset / `Covers`), the binding is allowed.
///   2. Otherwise the caller must hold the synthetic `escalate` verb on the
///      referenced `roles`/`clusterroles` resource.
///   3. If neither holds, return `403 Forbidden`.
///
/// `binding_namespace` is the namespace the binding lives in (empty for a
/// ClusterRoleBinding); it scopes both the caller's resolved rules and the
/// `escalate` authorization check.
async fn confirm_no_escalation(
    state: &Arc<ApiServerState>,
    user: &rusternetes_common::auth::UserInfo,
    role_ref: &rusternetes_common::resources::RoleRef,
    binding_namespace: &str,
    binding_name: &str,
) -> Result<()> {
    let granted_rules = resolve_role_ref_rules(state, role_ref, binding_namespace).await;
    if granted_rules.is_empty() {
        return Ok(());
    }

    // (1) Superset check: does the caller already hold every granted rule?
    let (caller_resource_rules, _caller_non_resource_rules) = state
        .authorizer
        .get_user_rules(user, binding_namespace)
        .await?;
    let caller_rules = resource_rules_to_policy_rules(&caller_resource_rules);
    if rules_cover(&caller_rules, &granted_rules) {
        return Ok(());
    }

    // (2) `escalate` verb check on the referenced role resource.
    let escalate_resource = match role_ref.kind.as_str() {
        "ClusterRole" => "clusterroles",
        _ => "roles",
    };
    let mut escalate_attrs = RequestAttributes::new(user.clone(), "escalate", escalate_resource)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&role_ref.name);
    if !binding_namespace.is_empty() {
        escalate_attrs = escalate_attrs.with_namespace(binding_namespace);
    }
    if let Decision::Allow = state.authorizer.authorize(&escalate_attrs).await? {
        return Ok(());
    }

    // (3) Neither holds — reject, mirroring upstream's Forbidden message shape.
    Err(rusternetes_common::Error::Forbidden(format!(
        "user \"{}\" (groups={:?}) is attempting to grant RBAC permissions not currently held \
         and is not authorized to escalate {} \"{}\"",
        user.username, user.groups, escalate_resource, binding_name
    )))
}

// Role handlers
pub async fn create_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut role): DumpingJson<Role>,
) -> Result<(StatusCode, Json<Role>)> {
    info!("Creating role: {}/{}", namespace, role.metadata.name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &role.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::PathSegment,
    )?;

    // Validate policy rules (upstream rbac ValidateRole).
    let errs = rusternetes_common::validation::rbac::validate_role(&role);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    role.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    role.metadata.ensure_uid();
    role.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Role validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(role)));
    }

    let key = build_key("roles", Some(&namespace), &role.metadata.name);
    match state.storage.create(&key, &role).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(rusternetes_common::Error::AlreadyExists(_)) => {
            Err(rusternetes_common::Error::AlreadyExists(format!(
                "roles \"{}\" already exists",
                role.metadata.name
            )))
        }
        Err(e) => Err(e),
    }
}

pub async fn get_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Role>> {
    debug!("Getting role: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("roles", Some(&namespace), &name);
    let role = state.storage.get(&key).await?;

    Ok(Json(role))
}

pub async fn update_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut role): DumpingJson<Role>,
) -> Result<Json<Role>> {
    info!("Updating role: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    role.metadata.name = name.clone();
    role.metadata.namespace = Some(namespace.clone());

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Role validated successfully (not updated)");
        return Ok(Json(role));
    }

    let key = build_key("roles", Some(&namespace), &name);
    let updated = state.storage.update(&key, &role).await?;

    Ok(Json(updated))
}

pub async fn delete_role(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Role>> {
    info!("Deleting role: {}/{}", namespace, name);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("roles", Some(&namespace), &name);

    // Get the resource for finalizer handling
    let role: Role = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating admission webhooks for DELETE (object=nil, oldObject=role).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "rbac.authorization.k8s.io",
        "v1",
        "Role",
        "roles",
        Some(&namespace),
        &name,
        &role,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    if is_dry_run {
        info!("Dry-run: Role validated successfully (not deleted)");
        return Ok(Json(role));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &role)
            .await?;

    if deleted_immediately {
        Ok(Json(role))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Role = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<Role>(
            state,
            auth_ctx,
            namespace,
            "roles",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing roles in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("roles", Some(&namespace));
    let mut roles = state.storage.list::<Role>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut roles, &params)?;

    let mut list = List::new("RoleList", "rbac.authorization.k8s.io/v1", roles);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all roles across all namespaces
pub async fn list_all_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<Role>(
            state,
            auth_ctx,
            "roles",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all roles");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "roles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("roles", None);
    let mut roles = state.storage.list::<Role>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut roles, &params)?;

    let mut list = List::new("RoleList", "rbac.authorization.k8s.io/v1", roles);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// RoleBinding handlers
pub async fn create_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut rolebinding): DumpingJson<RoleBinding>,
) -> Result<(StatusCode, Json<RoleBinding>)> {
    info!(
        "Creating rolebinding: {}/{}",
        namespace, rolebinding.metadata.name
    );

    // Check authorization
    let user = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Privilege-escalation prevention: the caller must already hold the rules
    // granted by the referenced role, or hold the `escalate` verb on it.
    confirm_no_escalation(
        &state,
        &user,
        &rolebinding.role_ref,
        &namespace,
        &rolebinding.metadata.name,
    )
    .await?;

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &rolebinding.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::PathSegment,
    )?;

    // Default roleRef/subject apiGroups before validating (upstream rbac
    // SetDefaults_RoleBinding): an omitted roleRef.apiGroup is the RBAC group.
    rusternetes_common::validation::rbac::default_role_binding(&mut rolebinding);

    // Validate roleRef + subjects (upstream rbac ValidateRoleBinding).
    let errs = rusternetes_common::validation::rbac::validate_role_binding(&rolebinding);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    rolebinding.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    rolebinding.metadata.ensure_uid();
    rolebinding.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: RoleBinding validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(rolebinding)));
    }

    let key = build_key("rolebindings", Some(&namespace), &rolebinding.metadata.name);
    match state.storage.create(&key, &rolebinding).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(rusternetes_common::Error::AlreadyExists(_)) => {
            Err(rusternetes_common::Error::AlreadyExists(format!(
                "rolebindings \"{}\" already exists",
                rolebinding.metadata.name
            )))
        }
        Err(e) => Err(e),
    }
}

pub async fn get_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<RoleBinding>> {
    debug!("Getting rolebinding: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("rolebindings", Some(&namespace), &name);
    let rolebinding = state.storage.get(&key).await?;

    Ok(Json(rolebinding))
}

pub async fn update_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut rolebinding): DumpingJson<RoleBinding>,
) -> Result<Json<RoleBinding>> {
    info!("Updating rolebinding: {}/{}", namespace, name);

    // Check authorization
    let user = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    rolebinding.metadata.name = name.clone();
    rolebinding.metadata.namespace = Some(namespace.clone());

    let key = build_key("rolebindings", Some(&namespace), &name);

    // Full update validation (upstream ValidateRoleBindingUpdate): re-run the
    // create checks on the new object and forbid changing roleRef.
    if let Ok(existing) = state.storage.get::<RoleBinding>(&key).await {
        let errs = rusternetes_common::validation::rbac::validate_role_binding_update(
            &rolebinding,
            &existing,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Privilege-escalation prevention on update: upstream's `rolebinding/policybased`
    // runs the same ConfirmNoEscalation check on UPDATE as on CREATE, since the
    // bound role's rules can be granted to new subjects.
    confirm_no_escalation(&state, &user, &rolebinding.role_ref, &namespace, &name).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: RoleBinding validated successfully (not updated)");
        return Ok(Json(rolebinding));
    }

    let updated = state.storage.update(&key, &rolebinding).await?;

    Ok(Json(updated))
}

pub async fn delete_rolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<RoleBinding>> {
    info!("Deleting rolebinding: {}/{}", namespace, name);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("rolebindings", Some(&namespace), &name);

    // Get the resource for finalizer handling
    let rolebinding: RoleBinding = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating admission webhooks for DELETE (object=nil, oldObject=rolebinding).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "rbac.authorization.k8s.io",
        "v1",
        "RoleBinding",
        "rolebindings",
        Some(&namespace),
        &name,
        &rolebinding,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    if is_dry_run {
        info!("Dry-run: RoleBinding validated successfully (not deleted)");
        return Ok(Json(rolebinding));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &rolebinding,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(rolebinding))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: RoleBinding = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<RoleBinding>(
            state,
            auth_ctx,
            namespace,
            "rolebindings",
            "rbac.authorization.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing rolebindings in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("rolebindings", Some(&namespace));
    let mut rolebindings = state.storage.list::<RoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rolebindings, &params)?;

    let mut list = List::new(
        "RoleBindingList",
        "rbac.authorization.k8s.io/v1",
        rolebindings,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all rolebindings across all namespaces
pub async fn list_all_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Honor `?watch=true` on the all-namespaces collection (informer/Lens path).
    // Mirrors list_all_roles: the all-NS watch is just the no-namespace prefix,
    // so it routes through watch_cluster_scoped.
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<RoleBinding>(
            state,
            auth_ctx,
            "rolebindings",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    debug!("Listing all rolebindings");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "rolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("rolebindings", None);
    let mut rolebindings = state.storage.list::<RoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rolebindings, &params)?;

    let mut list = List::new(
        "RoleBindingList",
        "rbac.authorization.k8s.io/v1",
        rolebindings,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// ClusterRole handlers
pub async fn create_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut clusterrole): DumpingJson<ClusterRole>,
) -> Result<(StatusCode, Json<ClusterRole>)> {
    info!("Creating clusterrole: {}", clusterrole.metadata.name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &clusterrole.metadata,
        None,
        crate::handlers::validation::NameKind::PathSegment,
    )?;

    // Validate policy rules + aggregationRule (upstream rbac ValidateClusterRole).
    let errs = rusternetes_common::validation::rbac::validate_cluster_role(&clusterrole);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Enrich metadata with system fields
    clusterrole.metadata.ensure_uid();
    clusterrole.metadata.ensure_creation_timestamp();

    // Materialise aggregationRule.clusterRoleSelectors into `rules` (upstream
    // `pkg/controller/clusterroleaggregation` + `clusterrole/policybased`).
    materialise_aggregated_rules(&state.storage, &mut clusterrole).await;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRole validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(clusterrole)));
    }

    let key = build_key("clusterroles", None, &clusterrole.metadata.name);

    match state.storage.create(&key, &clusterrole).await {
        Ok(created) => {
            info!(
                "ClusterRole created successfully: {}",
                clusterrole.metadata.name
            );
            Ok((StatusCode::CREATED, Json(created)))
        }
        Err(e) => {
            tracing::warn!(
                "Failed to create ClusterRole {}: {}",
                clusterrole.metadata.name,
                e
            );
            Err(e)
        }
    }
}

pub async fn get_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ClusterRole>> {
    debug!("Getting clusterrole: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("clusterroles", None, &name);
    let clusterrole = state.storage.get(&key).await?;

    Ok(Json(clusterrole))
}

pub async fn update_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut clusterrole): DumpingJson<ClusterRole>,
) -> Result<Json<ClusterRole>> {
    info!("Updating clusterrole: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    clusterrole.metadata.name = name.clone();

    // Recompute aggregated rules on update (upstream re-aggregates whenever the
    // parent or any matching child ClusterRole changes).
    materialise_aggregated_rules(&state.storage, &mut clusterrole).await;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRole validated successfully (not updated)");
        return Ok(Json(clusterrole));
    }

    let key = build_key("clusterroles", None, &name);
    let updated = state.storage.update(&key, &clusterrole).await?;

    Ok(Json(updated))
}

pub async fn delete_clusterrole(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ClusterRole>> {
    info!("Deleting clusterrole: {}", name);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("clusterroles", None, &name);

    // Get the resource for finalizer handling
    let clusterrole: ClusterRole = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating admission webhooks for DELETE (object=nil, oldObject=clusterrole).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "rbac.authorization.k8s.io",
        "v1",
        "ClusterRole",
        "clusterroles",
        None,
        &name,
        &clusterrole,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    if is_dry_run {
        info!("Dry-run: ClusterRole validated successfully (not deleted)");
        return Ok(Json(clusterrole));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &clusterrole,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(clusterrole))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ClusterRole = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_clusterroles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing clusterroles");

    // Honor `?watch=true` on the collection endpoint (what Lens / client-go
    // informers use). Without this the legacy `/watch/clusterroles` subpath is
    // the only way to stream, so list-then-watch clients never see updates.
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ClusterRole>(
            state,
            auth_ctx,
            "clusterroles",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("clusterroles", None);
    let mut clusterroles = state.storage.list::<ClusterRole>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterroles, &params)?;

    let mut list = List::new(
        "ClusterRoleList",
        "rbac.authorization.k8s.io/v1",
        clusterroles,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// ClusterRoleBinding handlers
pub async fn create_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut clusterrolebinding): DumpingJson<ClusterRoleBinding>,
) -> Result<(StatusCode, Json<ClusterRoleBinding>)> {
    info!(
        "Creating clusterrolebinding: {}",
        clusterrolebinding.metadata.name
    );

    // Check authorization
    let user = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Privilege-escalation prevention: the caller must already hold the rules
    // granted by the referenced ClusterRole, or hold the `escalate` verb on it.
    // Cluster-scoped, so the binding namespace is empty.
    confirm_no_escalation(
        &state,
        &user,
        &clusterrolebinding.role_ref,
        "",
        &clusterrolebinding.metadata.name,
    )
    .await?;

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &clusterrolebinding.metadata,
        None,
        crate::handlers::validation::NameKind::PathSegment,
    )?;

    // Default roleRef/subject apiGroups before validating (upstream rbac
    // SetDefaults_ClusterRoleBinding): an omitted roleRef.apiGroup is the RBAC
    // group.
    rusternetes_common::validation::rbac::default_cluster_role_binding(&mut clusterrolebinding);

    // Validate roleRef + subjects (upstream rbac ValidateClusterRoleBinding).
    let errs =
        rusternetes_common::validation::rbac::validate_cluster_role_binding(&clusterrolebinding);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Enrich metadata with system fields
    clusterrolebinding.metadata.ensure_uid();
    clusterrolebinding.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRoleBinding validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(clusterrolebinding)));
    }

    let key = build_key(
        "clusterrolebindings",
        None,
        &clusterrolebinding.metadata.name,
    );

    match state.storage.create(&key, &clusterrolebinding).await {
        Ok(created) => {
            info!(
                "ClusterRoleBinding created successfully: {}",
                clusterrolebinding.metadata.name
            );
            Ok((StatusCode::CREATED, Json(created)))
        }
        Err(e) => {
            tracing::warn!(
                "Failed to create ClusterRoleBinding {}: {}",
                clusterrolebinding.metadata.name,
                e
            );
            Err(e)
        }
    }
}

pub async fn get_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<ClusterRoleBinding>> {
    debug!("Getting clusterrolebinding: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("clusterrolebindings", None, &name);
    let clusterrolebinding = state.storage.get(&key).await?;

    Ok(Json(clusterrolebinding))
}

pub async fn update_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut clusterrolebinding): DumpingJson<ClusterRoleBinding>,
) -> Result<Json<ClusterRoleBinding>> {
    info!("Updating clusterrolebinding: {}", name);

    // Check authorization
    let user = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    clusterrolebinding.metadata.name = name.clone();

    let key = build_key("clusterrolebindings", None, &name);

    // Full update validation (upstream ValidateClusterRoleBindingUpdate): re-run
    // the create checks on the new object and forbid changing roleRef.
    if let Ok(existing) = state.storage.get::<ClusterRoleBinding>(&key).await {
        let errs = rusternetes_common::validation::rbac::validate_cluster_role_binding_update(
            &clusterrolebinding,
            &existing,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Privilege-escalation prevention on update: same ConfirmNoEscalation check
    // as create, mirroring upstream's `clusterrolebinding/policybased`.
    confirm_no_escalation(&state, &user, &clusterrolebinding.role_ref, "", &name).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRoleBinding validated successfully (not updated)");
        return Ok(Json(clusterrolebinding));
    }

    let updated = state.storage.update(&key, &clusterrolebinding).await?;

    Ok(Json(updated))
}

pub async fn delete_clusterrolebinding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ClusterRoleBinding>> {
    info!("Deleting clusterrolebinding: {}", name);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("clusterrolebindings", None, &name);

    // Get the resource for finalizer handling
    let clusterrolebinding: ClusterRoleBinding = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating admission webhooks for DELETE (object=nil, oldObject=clusterrolebinding).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "rbac.authorization.k8s.io",
        "v1",
        "ClusterRoleBinding",
        "clusterrolebindings",
        None,
        &name,
        &clusterrolebinding,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    if is_dry_run {
        info!("Dry-run: ClusterRoleBinding validated successfully (not deleted)");
        return Ok(Json(clusterrolebinding));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &clusterrolebinding,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(clusterrolebinding))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ClusterRoleBinding = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_clusterrolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing clusterrolebindings");

    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<ClusterRoleBinding>(
            state,
            auth_ctx,
            "clusterrolebindings",
            "rbac.authorization.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("clusterrolebindings", None);
    let mut clusterrolebindings = state.storage.list::<ClusterRoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterrolebindings, &params)?;

    let mut list = List::new(
        "ClusterRoleBindingList",
        "rbac.authorization.k8s.io/v1",
        clusterrolebindings,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// DeleteCollection handlers for RBAC resources
pub async fn deletecollection_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection roles in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "roles")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Role collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all roles in the namespace
    let prefix = build_prefix("roles", Some(&namespace));
    let mut roles = state.storage.list::<Role>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut roles, &params)?;

    // Delete each matching role
    let mut deleted_count = 0;
    for role in roles {
        let key = build_key("roles", Some(&namespace), &role.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "rbac.authorization.k8s.io",
            "v1",
            "Role",
            "roles",
            Some(&namespace),
            &role.metadata.name,
            &role,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &role,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} roles deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection rolebindings in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "rolebindings")
        .with_namespace(&namespace)
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: RoleBinding collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all rolebindings in the namespace
    let prefix = build_prefix("rolebindings", Some(&namespace));
    let mut rolebindings = state.storage.list::<RoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rolebindings, &params)?;

    // Delete each matching rolebinding
    let mut deleted_count = 0;
    for rolebinding in rolebindings {
        let key = build_key("rolebindings", Some(&namespace), &rolebinding.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "rbac.authorization.k8s.io",
            "v1",
            "RoleBinding",
            "rolebindings",
            Some(&namespace),
            &rolebinding.metadata.name,
            &rolebinding,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &rolebinding,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} rolebindings deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_clusterroles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection clusterroles with params: {:?}", params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "clusterroles")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRole collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all clusterroles
    let prefix = build_prefix("clusterroles", None);
    let mut clusterroles = state.storage.list::<ClusterRole>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterroles, &params)?;

    // Delete each matching clusterrole
    let mut deleted_count = 0;
    for clusterrole in clusterroles {
        let key = build_key("clusterroles", None, &clusterrole.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "rbac.authorization.k8s.io",
            "v1",
            "ClusterRole",
            "clusterroles",
            None,
            &clusterrole.metadata.name,
            &clusterrole,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &clusterrole,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} clusterroles deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_clusterrolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection clusterrolebindings with params: {:?}",
        params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "clusterrolebindings")
        .with_api_group("rbac.authorization.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ClusterRoleBinding collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all clusterrolebindings
    let prefix = build_prefix("clusterrolebindings", None);
    let mut clusterrolebindings = state.storage.list::<ClusterRoleBinding>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut clusterrolebindings, &params)?;

    // Delete each matching clusterrolebinding
    let mut deleted_count = 0;
    for clusterrolebinding in clusterrolebindings {
        let key = build_key(
            "clusterrolebindings",
            None,
            &clusterrolebinding.metadata.name,
        );

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "rbac.authorization.k8s.io",
            "v1",
            "ClusterRoleBinding",
            "clusterrolebindings",
            None,
            &clusterrolebinding.metadata.name,
            &clusterrolebinding,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &clusterrolebinding,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} clusterrolebindings deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

// Use macros to create PATCH handlers for RBAC resources
crate::patch_handler_namespaced!(patch_role, Role, "roles", "rbac.authorization.k8s.io");
crate::patch_handler_namespaced!(
    patch_rolebinding,
    RoleBinding,
    "rolebindings",
    "rbac.authorization.k8s.io"
);
crate::patch_handler_cluster!(
    patch_clusterrole,
    ClusterRole,
    "clusterroles",
    "rbac.authorization.k8s.io"
);
crate::patch_handler_cluster!(
    patch_clusterrolebinding,
    ClusterRoleBinding,
    "clusterrolebindings",
    "rbac.authorization.k8s.io"
);
