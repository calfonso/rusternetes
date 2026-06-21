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
    resources::Job,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut job): DumpingJson<Job>,
) -> Result<(StatusCode, Json<Job>)> {
    info!("Creating job: {}/{}", namespace, job.metadata.name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "jobs")
        .with_namespace(&namespace)
        .with_api_group("batch");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &job.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    job.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    job.metadata.ensure_uid();
    job.metadata.ensure_creation_timestamp();

    // Apply K8s defaults (SetDefaults_Job + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_job_defaults(&mut job);

    // Auto-generate selector if not set (like real K8s).
    // When manualSelector is not true, K8s generates a selector from the controller-uid label
    // and adds the controller-uid label to the pod template.
    if !job.spec.manual_selector.unwrap_or(false) && job.spec.selector.is_none() {
        let uid = job.metadata.uid.clone();
        let mut match_labels = std::collections::HashMap::new();
        match_labels.insert("controller-uid".to_string(), uid.clone());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });

        // Also ensure the pod template has the controller-uid label
        let template_labels = job
            .spec
            .template
            .metadata
            .get_or_insert_with(Default::default)
            .labels
            .get_or_insert_with(Default::default);
        template_labels.insert("controller-uid".to_string(), uid.clone());
        // Also add the standard job-name label
        template_labels.insert("job-name".to_string(), job.metadata.name.clone());
    }

    // Validate the (now defaulted + selector-generated) Job spec, mirroring
    // upstream batch ValidateJob ordering. Rejects bad completionMode/Indexed
    // combinations, negative counts, invalid restartPolicy, selector mismatch.
    let errs = rusternetes_common::validation::job::validate_job(&job);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Job {}/{} validated successfully (not created)",
            namespace, job.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(job)));
    }

    let key = build_key("jobs", Some(&namespace), &job.metadata.name);
    let created = state.storage.create(&key, &job).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Job>> {
    debug!("Getting job: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "jobs")
        .with_namespace(&namespace)
        .with_api_group("batch")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("jobs", Some(&namespace), &name);
    let job = state.storage.get(&key).await?;

    Ok(Json(job))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut job): DumpingJson<Job>,
) -> Result<Json<Job>> {
    info!("Updating job: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "jobs")
        .with_namespace(&namespace)
        .with_api_group("batch")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    job.metadata.name = name.clone();
    job.metadata.namespace = Some(namespace.clone());

    // Apply K8s defaults (SetDefaults_Job + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_job_defaults(&mut job);

    let key = build_key("jobs", Some(&namespace), &name);

    // ValidateJobUpdate immutability fence — mirrors upstream
    // pkg/registry/batch/job/strategy.go ValidateUpdate. Only
    // `spec.parallelism`, `spec.activeDeadlineSeconds`, `spec.suspend`, and
    // `spec.ttlSecondsAfterFinished` are mutable. Mutating any of:
    //   * spec.completions
    //   * spec.completionMode
    //   * spec.selector
    //   * spec.template
    // is rejected with Invalid.
    if let Ok(mut existing) = state.storage.get::<Job>(&key).await {
        // Mirror upstream: defaulting runs on every request, and the
        // immutability comparison is between post-default specs. Without
        // symmetric defaulting, partial UPDATE bodies + objects seeded via
        // direct storage writes (tests, raw etcd writes) trip the fence on
        // server-defaulted fields that the client never echoed.
        crate::handlers::defaults::apply_job_defaults(&mut existing);

        if existing.spec.completions != job.spec.completions {
            return Err(rusternetes_common::Error::InvalidResource(
                "Job.spec.completions: Invalid value: field is immutable".to_string(),
            ));
        }
        if existing.spec.completion_mode != job.spec.completion_mode {
            return Err(rusternetes_common::Error::InvalidResource(
                "Job.spec.completionMode: Invalid value: field is immutable".to_string(),
            ));
        }
        if existing.spec.selector != job.spec.selector {
            return Err(rusternetes_common::Error::InvalidResource(
                "Job.spec.selector: Invalid value: field is immutable".to_string(),
            ));
        }
        // PodTemplateSpec does not derive PartialEq; compare via canonical
        // JSON like upstream's apiequality.Semantic.DeepEqual.
        let old_template = serde_json::to_value(&existing.spec.template).unwrap_or_default();
        let new_template = serde_json::to_value(&job.spec.template).unwrap_or_default();
        if old_template != new_template {
            return Err(rusternetes_common::Error::InvalidResource(
                "Job.spec.template: Invalid value: field is immutable".to_string(),
            ));
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Job {:}/{:} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(job));
    }
    let updated = state.storage.update(&key, &job).await?;

    Ok(Json(updated))
}

pub async fn delete_job(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Job>> {
    info!("Deleting job: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "jobs")
        .with_namespace(&namespace)
        .with_api_group("batch")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("jobs", Some(&namespace), &name);

    // Get the resource to check if it exists
    let job: Job = state.storage.get(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=job).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "batch",
        "v1",
        "Job",
        "jobs",
        Some(&namespace),
        &name,
        &job,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Job {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(job));
    }

    let propagation_policy = params.get("propagationPolicy").map(|s| s.as_str());
    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers_and_propagation(
            &*state.storage,
            &key,
            &job,
            propagation_policy,
        )
        .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Job = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(job))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        let watch_params = crate::handlers::watch::WatchParams {
            resource_version: crate::handlers::watch::normalize_resource_version(
                params.get("resourceVersion").cloned(),
            ),
            timeout_seconds: params
                .get("timeoutSeconds")
                .and_then(|v| v.parse::<u64>().ok()),
            label_selector: params.get("labelSelector").cloned(),
            field_selector: params.get("fieldSelector").cloned(),
            watch: Some(true),
            allow_watch_bookmarks: params
                .get("allowWatchBookmarks")
                .and_then(|v| v.parse::<bool>().ok()),
            send_initial_events: params
                .get("sendInitialEvents")
                .and_then(|v| v.parse::<bool>().ok()),
        };
        return crate::handlers::watch::watch_namespaced::<Job>(
            state,
            auth_ctx,
            namespace,
            "jobs",
            "batch",
            watch_params,
        )
        .await;
    }

    debug!("Listing jobs in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "jobs")
        .with_namespace(&namespace)
        .with_api_group("batch");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("jobs", Some(&namespace));
    let mut jobs: Vec<Job> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut jobs, &params)?;

    let mut list = List::new("JobList", "batch/v1", jobs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all jobs across all namespaces
pub async fn list_all_jobs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        let watch_params = crate::handlers::watch::WatchParams {
            resource_version: crate::handlers::watch::normalize_resource_version(
                params.get("resourceVersion").cloned(),
            ),
            timeout_seconds: params
                .get("timeoutSeconds")
                .and_then(|v| v.parse::<u64>().ok()),
            label_selector: params.get("labelSelector").cloned(),
            field_selector: params.get("fieldSelector").cloned(),
            watch: Some(true),
            allow_watch_bookmarks: params
                .get("allowWatchBookmarks")
                .and_then(|v| v.parse::<bool>().ok()),
            send_initial_events: params
                .get("sendInitialEvents")
                .and_then(|v| v.parse::<bool>().ok()),
        };
        return crate::handlers::watch::watch_cluster_scoped::<Job>(
            state,
            auth_ctx,
            "jobs",
            "batch",
            watch_params,
        )
        .await;
    }

    debug!("Listing all jobs");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "jobs").with_api_group("batch");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("jobs", None);
    let mut jobs = state.storage.list::<Job>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut jobs, &params)?;

    let mut list = List::new("JobList", "batch/v1", jobs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, Job, "jobs", "batch");

pub async fn deletecollection_jobs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection jobs in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "jobs")
        .with_namespace(&namespace)
        .with_api_group("batch");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Job collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all jobs in the namespace
    let prefix = build_prefix("jobs", Some(&namespace));
    let mut items = state.storage.list::<Job>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("jobs", Some(&namespace), &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "batch",
            "v1",
            "Job",
            "jobs",
            Some(&namespace),
            &item.metadata.name,
            &item,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &item,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!("DeleteCollection completed: {} jobs deleted", deleted_count);
    Ok(StatusCode::OK)
}
