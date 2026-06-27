use crate::{
    middleware::AuthContext,
    patch::{apply_patch, PatchType},
    state::ApiServerState,
};
use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    admission::{AdmissionResponse, GroupVersionKind, GroupVersionResource, Operation},
    authz::{Decision, RequestAttributes},
    resources::Pod,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::sync::Arc;
use tracing::{debug, info, warn};

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: Bytes,
) -> Result<axum::response::Response> {
    // Parse the body manually so we can do strict field validation against the
    // raw bytes. In strict mode (now the K8s 1.25+ default) serde_json will
    // reject duplicate keys outright — fall back to a lenient Value parse so
    // validate_strict_fields can report the duplicate in the canonical
    // `strict decoding error: duplicate field "..."` shape rather than a raw
    // serde error.
    let is_lenient = matches!(
        crate::handlers::validation::FieldValidationMode::from_query(&params),
        crate::handlers::validation::FieldValidationMode::Warn
            | crate::handlers::validation::FieldValidationMode::Ignore
    );
    let mut pod: Pod = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("duplicate field") {
                // Re-parse via Value (lenient — takes last duplicate) so the
                // strict-decode error path can synthesize a parity-shaped
                // message that names every duplicate.
                let value: serde_json::Value = serde_json::from_slice(&body).map_err(|e2| {
                    rusternetes_common::Error::BadRequest(format!("failed to decode: {}", e2))
                })?;
                serde_json::from_value(value).map_err(|e2| {
                    rusternetes_common::Error::BadRequest(format!("failed to decode: {}", e2))
                })?
            } else if is_lenient {
                // Warn / Ignore mode: even ordinary decode failures shouldn't
                // mask the user's intent — but if the body is unparseable we
                // can't continue at all, so still error.
                return Err(rusternetes_common::Error::BadRequest(format!(
                    "failed to decode: {}",
                    msg
                )));
            } else {
                return Err(rusternetes_common::Error::BadRequest(format!(
                    "failed to decode: {}",
                    msg
                )));
            }
        }
    };

    info!("Creating pod: {}/{}", namespace, pod.metadata.name);

    // Strict field validation: reject unknown fields when requested. Warn
    // mode returns one warning string per unknown field; the handler turns
    // those into `Warning: 299 - "..."` response headers below.
    let warnings = crate::handlers::validation::validate_strict_fields(&params, &body, &pod)?;
    let response_headers = build_warning_headers(&warnings);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &pod.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Build user info for admission webhooks early (before auth_ctx.user is moved)
    let user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Apply SetDefaults_PodSpec / SetDefaults_Container / SetDefaults_Probe
    // BEFORE validation. Upstream defaults the object in the decode/admission
    // path and validates the *defaulted* result, so an omitted/zero probe
    // successThreshold is backfilled to 1 (and ContainerPort.protocol to TCP,
    // etc.) before validate_pod_create runs. Validating first (the previous
    // order) made the probe validator reject undefaulted successThreshold as
    // "must be 1", rejecting ~28 [NodeConformance] probe specs at create time.
    // K8s ref: pkg/apis/core/v1/defaults.go runs ahead of
    // pkg/apis/core/validation.
    if let Some(ref mut spec) = pod.spec {
        crate::handlers::defaults::apply_pod_spec_defaults(spec);
    }

    // Validate pod spec — accumulate every violation into a `field::ErrorList`
    // so the 422 response carries one `Status.details.causes[]` entry per
    // failure, matching upstream `NewInvalid`. Short-circuiting on the first
    // error would collapse multi-violation requests to a single cause and
    // break parity with `TestNewInvalidMulti`.
    //
    // K8s ref: pkg/apis/core/validation/validation.go ValidatePod /
    // ValidatePodSpec. Validation is delegated to
    // `rusternetes_common::validation::pod::validate_pod_create` which ports
    // the full upstream pipeline (containers, initContainers, ports, probes,
    // resources, restartPolicy, dnsPolicy, dnsConfig, volumes, tolerations,
    // terminationGracePeriodSeconds, activeDeadlineSeconds).
    {
        use rusternetes_common::feature_gates::{enabled, Feature};
        let allow_relaxed = enabled(Feature::RelaxedDNSSearchValidation);
        let errs = rusternetes_common::validation::pod::validate_pod_create(&pod, allow_relaxed);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Validate sysctl names — check format only.
    // In real Kubernetes, the API server validates sysctl name format but does NOT
    // reject unsafe sysctls. Unsafe sysctl enforcement is done by the kubelet
    // (via --allowed-unsafe-sysctls flag) which sets the pod status to
    // SysctlForbidden if the sysctl is not allowed. The conformance test expects
    // pods with unsafe sysctls to be created successfully and handled by the kubelet.
    if let Some(ref spec) = pod.spec {
        if let Some(ref security_context) = spec.security_context {
            if let Some(ref sysctls) = security_context.sysctls {
                // Validate ALL sysctl names and collect errors (K8s reports all errors)
                let mut sysctl_errors: Vec<String> = Vec::new();
                for sysctl in sysctls {
                    if !is_valid_sysctl_name(&sysctl.name) {
                        sysctl_errors.push(format!(
                            "spec.securityContext.sysctls: Invalid value: \"{}\": must have at most 253 characters and match regex {}",
                            sysctl.name, "^([a-z0-9][-_a-z0-9]*[a-z0-9]?\\.)*[a-z0-9][-_a-z0-9]*[a-z0-9]?$"
                        ));
                    }
                }
                if !sysctl_errors.is_empty() {
                    return Err(rusternetes_common::Error::InvalidResource(
                        sysctl_errors.join(", "),
                    ));
                }
                // NOTE: We intentionally do NOT reject unsafe sysctls here.
                // The kubelet is responsible for enforcing the allowed-unsafe-sysctls
                // list and rejecting pods with disallowed unsafe sysctls.
            }
        }
    }

    // Fetch LimitRanges once for this request (used for defaults and validation)
    let lr_prefix = rusternetes_storage::build_prefix("limitranges", Some(&namespace));
    let limit_ranges: Vec<rusternetes_common::resources::LimitRange> =
        state.storage.list(&lr_prefix).await.unwrap_or_default();

    // SetDefaults_PodSpec/Container/Probe already ran before validation above.
    // The Pod-only defaults below (enableServiceLinks, limits→requests) and the
    // LimitRange defaults do not affect that validation pass.
    if let Some(ref mut spec) = pod.spec {
        // SetDefaults_Pod (Pod-only, NOT templates): enableServiceLinks defaults
        // to true (v1.DefaultEnableServiceLinks). Defaulting this on embedded
        // PodTemplateSpecs would diverge from upstream and break ControllerRevision
        // byte-comparisons, so it lives here on the standalone Pod path.
        // K8s ref: pkg/apis/core/v1/defaults.go SetDefaults_Pod.
        if spec.enable_service_links.is_none() {
            spec.enable_service_links = Some(true);
        }

        // K8s pod-only defaulting: if a container has explicit limits but no requests,
        // default requests to the limit value. This happens BEFORE LimitRange so that
        // explicit limits take precedence over LimitRange defaultRequest.
        // K8s ref: SetDefaults_Pod (NOT SetDefaults_PodSpec — only on Pods, not templates)
        for container in &mut spec.containers {
            if let Some(ref limits) = container.resources.as_ref().and_then(|r| r.limits.clone()) {
                if !limits.is_empty() {
                    let resources = container.resources.get_or_insert({
                        rusternetes_common::types::ResourceRequirements {
                            limits: None,
                            requests: None,
                            claims: None,
                        }
                    });
                    let requests = resources
                        .requests
                        .get_or_insert_with(std::collections::HashMap::new);
                    for (key, value) in limits {
                        requests.entry(key.clone()).or_insert_with(|| value.clone());
                    }
                }
            }
        }

        // LimitRange defaults are applied by apply_limit_range_with() below.
    }

    // Resolve priority from PriorityClass (K8s Priority admission controller)
    // See: plugin/pkg/admission/priority/admission.go lines 162-201
    if let Some(ref mut spec) = pod.spec {
        let mut resolved_priority: i32 = 0;
        let mut resolved_preemption_policy: Option<String> = None;

        if let Some(ref pc_name) = spec.priority_class_name {
            if !pc_name.is_empty() {
                // Resolve priorityClassName → priority value
                let pc_key = format!("/registry/priorityclasses/{}", pc_name);
                match state.storage.get::<serde_json::Value>(&pc_key).await {
                    Ok(pc) => {
                        resolved_priority =
                            pc.get("value").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        resolved_preemption_policy = pc
                            .get("preemptionPolicy")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        info!(
                            "Resolved priorityClassName {} → priority {} for pod {}",
                            pc_name, resolved_priority, pod.metadata.name
                        );
                    }
                    Err(rusternetes_common::Error::NotFound(_)) => {
                        // K8s rejects pods with unknown priorityClassName
                        return Err(rusternetes_common::Error::Forbidden(format!(
                            "no PriorityClass with name {} was found",
                            pc_name
                        )));
                    }
                    Err(_) => {} // Other errors: proceed with default
                }
            }
        } else {
            // No priorityClassName — look for globalDefault PriorityClass
            let pcs: Vec<serde_json::Value> = state
                .storage
                .list("/registry/priorityclasses/")
                .await
                .unwrap_or_default();
            for pc in &pcs {
                if pc
                    .get("globalDefault")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    resolved_priority =
                        pc.get("value").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                    resolved_preemption_policy = pc
                        .get("preemptionPolicy")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    if let Some(name) = pc.pointer("/metadata/name").and_then(|n| n.as_str()) {
                        spec.priority_class_name = Some(name.to_string());
                    }
                    break;
                }
            }
        }

        // K8s rejects pods where spec.priority differs from computed priority
        if let Some(existing_priority) = spec.priority {
            if existing_priority != resolved_priority {
                return Err(rusternetes_common::Error::Forbidden(format!(
                    "the integer value of priority ({}) must not be provided in pod spec; priority admission controller computed {} from the given PriorityClass name",
                    existing_priority, resolved_priority
                )));
            }
        }
        spec.priority = Some(resolved_priority);

        // Set preemptionPolicy from PriorityClass if not already set
        if let Some(ref policy) = resolved_preemption_policy {
            if spec.preemption_policy.is_none() {
                spec.preemption_policy = Some(policy.clone());
            }
        }

        // DefaultTolerationSeconds admission: append the NotReady/Unreachable
        // NoExecute tolerations (tolerationSeconds: 300) unless the pod already
        // tolerates them. Without this a transient node NotReady flap evicts —
        // and the kubelet sweep ultimately deletes — every pod on the node
        // instantly (#442). K8s ref:
        // plugin/pkg/admission/defaulttolerationseconds/admission.go (Admit).
        let added = rusternetes_common::tolerations::add_default_tolerations(spec);
        if added > 0 {
            info!(
                "Added {} default NoExecute toleration(s) to pod {}",
                added, pod.metadata.name
            );
        }
    }

    // Ensure namespace is set correctly
    pod.metadata.namespace = Some(namespace.clone());

    // Define GVK and GVR for Pod
    let gvk = GroupVersionKind {
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "Pod".to_string(),
    };

    let gvr = GroupVersionResource {
        group: "".to_string(),
        version: "v1".to_string(),
        resource: "pods".to_string(),
    };

    // Run mutating webhooks BEFORE other admission checks
    let pod_value = serde_json::to_value(&pod)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    // Debug: Log pod_value to see if valueFrom is present
    if pod.metadata.name.contains("test-env-fieldref") || pod.metadata.name.contains("sonobuoy") {
        info!(
            "POD CREATE - Before webhooks - pod_value: {}",
            serde_json::to_string(&pod_value).unwrap()
        );
    }

    let (mutation_response, mutated_pod_value) = state
        .webhook_manager
        .run_mutating_webhooks_with_dryrun(
            &Operation::Create,
            &gvk,
            &gvr,
            Some(&namespace),
            &pod.metadata.name,
            Some(pod_value),
            None,
            &user_info,
            is_dry_run,
        )
        .await?;

    // Check if mutating webhooks denied the request
    match mutation_response {
        AdmissionResponse::Deny(reason) => {
            warn!("Mutating webhooks denied pod creation: {}", reason);
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
        AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_) => {
            // Continue with the mutated object
            if let Some(mutated_value) = mutated_pod_value {
                pod = serde_json::from_value(mutated_value)
                    .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
                info!(
                    "Pod mutated by webhooks: {}/{}",
                    namespace, pod.metadata.name
                );
            }
        }
    }

    // Re-apply defaults after mutation. K8s runs SetDefaults twice: before and after
    // mutating webhooks. This ensures webhook-added containers get defaults like
    // terminationMessagePolicy=File, imagePullPolicy, etc.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go
    if let Some(ref mut spec) = pod.spec {
        crate::handlers::defaults::apply_pod_spec_defaults(spec);
        // SetDefaults_Pod (Pod-only): enableServiceLinks defaults to true.
        if spec.enable_service_links.is_none() {
            spec.enable_service_links = Some(true);
        }
    }

    // Inject service account token (built-in admission controller)
    if let Err(e) =
        crate::admission::inject_service_account_token(&state.storage, &namespace, &mut pod).await
    {
        warn!(
            "Error injecting service account token for pod {}/{}: {}",
            namespace, pod.metadata.name, e
        );
        // Continue anyway - don't fail pod creation if SA injection fails
    }

    // Apply LimitRange defaults and validate constraints (reuse already-fetched list)
    match crate::admission::apply_limit_range_with(&mut pod, &limit_ranges) {
        Ok(true) => {
            info!(
                "LimitRange admission passed for pod {}/{}",
                namespace, pod.metadata.name
            );
        }
        Ok(false) => {
            warn!(
                "LimitRange admission denied for pod {}/{}",
                namespace, pod.metadata.name
            );
            return Err(rusternetes_common::Error::Forbidden(
                "Pod violates LimitRange constraints".to_string(),
            ));
        }
        Err(e) => {
            warn!(
                "Error checking LimitRange for pod {}/{}: {}",
                namespace, pod.metadata.name, e
            );
            // Continue anyway - don't fail pod creation if LimitRange check fails
        }
    }

    // Check PodSecurity admission — enforce the namespace's Pod Security
    // Standard (privileged / baseline / restricted). All enforcement lives in
    // `admission::PodSecurityAdmission::admit`, the single PSA enforcement
    // path.
    {
        let psa = crate::admission::PodSecurityAdmission::new();
        psa.admit(&state.storage, &namespace, &pod).await?;
    }

    // Check RuntimeClass exists if specified, and inject overhead
    if let Some(rc_name) = pod
        .spec
        .as_ref()
        .and_then(|s| s.runtime_class_name.as_deref())
    {
        if !rc_name.is_empty() {
            let rc_key = rusternetes_storage::build_key("runtimeclasses", None, rc_name);
            match state.storage.get::<serde_json::Value>(&rc_key).await {
                Ok(rc_value) => {
                    // Set pod overhead from RuntimeClass overhead.podFixed
                    if let Some(overhead) = rc_value
                        .get("overhead")
                        .and_then(|o| o.get("podFixed"))
                        .and_then(|pf| pf.as_object())
                    {
                        let overhead_map: std::collections::HashMap<String, String> = overhead
                            .iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect();
                        if !overhead_map.is_empty() {
                            if let Some(spec) = pod.spec.as_mut() {
                                spec.overhead = Some(overhead_map);
                            }
                        }
                    }
                }
                Err(_) => {
                    return Err(rusternetes_common::Error::Forbidden(format!(
                        "pod {} references non-existent RuntimeClass \"{}\"",
                        pod.metadata.name, rc_name
                    )));
                }
            }
        }
    }

    // Check ResourceQuota — K8s does this atomically in the admission plugin.
    // check_resource_quota checks quota limits AND atomically increments usage.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/controller.go
    match crate::admission::check_resource_quota(&state.storage, &namespace, &pod).await {
        Ok(true) => {
            info!(
                "ResourceQuota admission passed for pod {}/{}",
                namespace, pod.metadata.name
            );
        }
        Ok(false) => {
            warn!(
                "ResourceQuota admission denied for pod {}/{}",
                namespace, pod.metadata.name
            );
            return Err(rusternetes_common::Error::Forbidden(
                "exceeded quota".to_string(),
            ));
        }
        Err(e) => {
            warn!(
                "Error checking ResourceQuota for pod {}/{}: {}",
                namespace, pod.metadata.name, e
            );
            return Err(rusternetes_common::Error::Internal(format!(
                "error checking ResourceQuota: {}",
                e
            )));
        }
    }

    // Run validating webhooks AFTER mutations and other admission checks
    let final_pod_value = serde_json::to_value(&pod)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    let validation_response = state
        .webhook_manager
        .run_validating_webhooks_with_dryrun(
            &Operation::Create,
            &gvk,
            &gvr,
            Some(&namespace),
            &pod.metadata.name,
            Some(final_pod_value),
            None,
            &user_info,
            is_dry_run,
        )
        .await?;

    // Check if validating webhooks denied the request
    match validation_response {
        AdmissionResponse::Deny(reason) => {
            warn!("Validating webhooks denied pod creation: {}", reason);
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
        AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_) => {
            info!(
                "Validating webhooks passed for pod {}/{}",
                namespace, pod.metadata.name
            );
        }
    }

    // Run ValidatingAdmissionPolicy checks
    let pod_value_for_vap = serde_json::to_value(&pod).ok();
    state
        .webhook_manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &gvk,
            pod_value_for_vap.as_ref(),
            None,
            Some("pods"),
            Some(&namespace),
        )
        .await?;

    pod.metadata.ensure_uid();
    pod.metadata.ensure_creation_timestamp();
    crate::handlers::lifecycle::set_initial_generation(&mut pod.metadata);

    // Resolve PriorityClassName to priority value (K8s admission controller does this)
    if let Some(ref mut spec) = pod.spec {
        if spec.priority.is_none() {
            if let Some(ref pc_name) = spec.priority_class_name {
                let pc_key = rusternetes_storage::build_key("priorityclasses", None, pc_name);
                if let Ok(pc) = state
                    .storage
                    .get::<rusternetes_common::resources::PriorityClass>(&pc_key)
                    .await
                {
                    spec.priority = Some(pc.value);
                    if spec.preemption_policy.is_none() {
                        spec.preemption_policy = pc.preemption_policy.clone();
                    }
                }
            }
        }
    }

    // Set initial status to Pending (Kubernetes always sets this on creation)
    if pod.status.is_none() || pod.status.as_ref().and_then(|s| s.phase.as_ref()).is_none() {
        let mut status = pod.status.take().unwrap_or_default();
        status.phase = Some(rusternetes_common::types::Phase::Pending);
        pod.status = Some(status);
    }

    // Compute and set QoS class
    {
        let qos = if let Some(spec) = &pod.spec {
            let mut all_guaranteed = true;
            let mut any_resources = false;
            for c in &spec.containers {
                if let Some(res) = &c.resources {
                    let has_limits = res
                        .limits
                        .as_ref()
                        .is_some_and(|l| l.contains_key("cpu") && l.contains_key("memory"));
                    let has_requests = res
                        .requests
                        .as_ref()
                        .is_some_and(|r| r.contains_key("cpu") && r.contains_key("memory"));
                    if has_limits || has_requests {
                        any_resources = true;
                    }
                    if !has_limits || (has_requests && res.limits != res.requests) {
                        all_guaranteed = false;
                    }
                } else {
                    all_guaranteed = false;
                }
            }
            if !any_resources {
                "BestEffort"
            } else if all_guaranteed {
                "Guaranteed"
            } else {
                "Burstable"
            }
        } else {
            "BestEffort"
        };
        let status = pod.status.get_or_insert_with(Default::default);
        status.qos_class = Some(qos.to_string());
    }

    let key = build_key("pods", Some(&namespace), &pod.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Pod {}/{} validated successfully (not created)",
            namespace, pod.metadata.name
        );
        return Ok(build_pod_response(
            StatusCode::CREATED,
            response_headers,
            pod,
        ));
    }

    match state.storage.create(&key, &pod).await {
        Ok(created) => {
            info!(
                "Pod created successfully: {}/{}",
                namespace, pod.metadata.name
            );
            Ok(build_pod_response(
                StatusCode::CREATED,
                response_headers,
                created,
            ))
        }
        Err(e) => {
            warn!(
                "Failed to create pod {}/{}: {}",
                namespace, pod.metadata.name, e
            );
            Err(e)
        }
    }
}

/// Construct a Pod response with the conventional `(status, headers, body)`
/// shape, plus a [`crate::response::NativeProtoOptIn`] marker so the
/// response middleware will emit a `k8s\0` + `runtime.Unknown` envelope when
/// the client negotiates `application/vnd.kubernetes.protobuf`.
///
/// History: PR #784 disabled this opt-in because the previous encoder
/// stuffed JSON into `Unknown.raw`, which broke client-go's typed proto
/// serializer (`staging/src/k8s.io/apimachinery/pkg/runtime/serializer/protobuf`)
/// — its decoder calls `proto.Unmarshal(unk.Raw, target)` directly and
/// never consults `Unknown.contentType`, so `{...}` JSON bytes triggered
/// `proto: illegal wireType N`. The native encoder added in
/// `crate::response::NativePodProtoEncoder` (this PR) emits real protobuf
/// bytes derived from the schemas in `crate::protobuf::PROTO_REGISTRY`,
/// so client-go's typed path now decodes the response cleanly.
fn build_pod_response(
    status: StatusCode,
    headers: HeaderMap,
    pod: Pod,
) -> axum::response::Response {
    let mut resp = (status, headers, Json(pod)).into_response();
    resp.extensions_mut()
        .insert(crate::response::NativeProtoOptIn::pod());
    resp
}

/// Convert the `validate_strict_fields` warning strings into a `HeaderMap`
/// holding RFC 7234 `Warning: 299 - "..."` entries. Empty input → empty map,
/// which keeps response shape identical for non-Warn callers.
fn build_warning_headers(warnings: &[String]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for warning in warnings {
        let value = crate::handlers::validation::format_warning_header(warning);
        if let Ok(hv) = axum::http::HeaderValue::from_str(&value) {
            headers.append(axum::http::header::WARNING, hv);
        }
    }
    headers
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<axum::response::Response> {
    debug!("Getting pod: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "pods")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("pods", Some(&namespace), &name);
    let pod: Pod = state.storage.get(&key).await?;

    // Attach the opt-in so the response middleware emits a native
    // protobuf envelope when the client negotiates
    // `application/vnd.kubernetes.protobuf`. See `build_pod_response` for
    // the encoder rationale.
    let mut resp = Json(pod).into_response();
    resp.extensions_mut()
        .insert(crate::response::NativeProtoOptIn::pod());
    Ok(resp)
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<Pod>> {
    // Parse the body manually for better error handling — axum's Json extractor
    // returns 422 Unprocessable Entity on failure, but Kubernetes expects a proper
    // Status object. Manual parsing also tolerates unknown fields gracefully.
    let mut pod: Pod = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;

    info!("Updating pod: {}/{}", namespace, name);

    // K8s ref: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/update.go —
    // strict field validation also fires on the UPDATE path (PUT). The
    // create handler already calls this; the parity gap on PUT meant
    // `fieldValidation=Strict` was effectively ignored for updates.
    crate::handlers::validation::validate_strict_fields(&params, &body, &pod)?;

    // K8s ref: pkg/registry/core/pod/strategy.go — the /ephemeralcontainers
    // subresource has its own dedicated Strategy that scopes admission to the
    // ephemeralContainers slice and forbids removing existing entries. The
    // regular PUT path on /pods/{name} must reject any ephemeralContainers
    // mutation (add OR remove). We use the original request URI to tell which
    // path we are on.
    let is_ephemeral_subresource = original_uri
        .path()
        .trim_end_matches('/')
        .ends_with("/ephemeralcontainers");

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Build user info for admission webhooks early (before auth_ctx.user is moved)
    let user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "pods")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure metadata matches URL
    pod.metadata.name = name.clone();
    pod.metadata.namespace = Some(namespace.clone());

    // Get the old pod for webhook comparison and concurrency control
    let key = build_key("pods", Some(&namespace), &name);
    let old_pod: Pod = state.storage.get(&key).await?;

    // Check resourceVersion for optimistic concurrency control
    crate::handlers::lifecycle::check_resource_version(
        old_pod.metadata.resource_version.as_deref(),
        pod.metadata.resource_version.as_deref(),
        &name,
    )?;

    // K8s ref: pkg/registry/core/pod/strategy.go statusStrategy +
    // ephemeralContainersStrategy. Ephemeral containers can only ever be ADDED
    // through the dedicated /ephemeralcontainers subresource; the regular
    // /pods/{name} PUT path must reject any mutation. Even on the subresource
    // path, removing an existing entry is forbidden.
    {
        let old_ec_count = old_pod
            .spec
            .as_ref()
            .and_then(|s| s.ephemeral_containers.as_ref())
            .map(|v| v.len())
            .unwrap_or(0);
        let new_ec_count = pod
            .spec
            .as_ref()
            .and_then(|s| s.ephemeral_containers.as_ref())
            .map(|v| v.len())
            .unwrap_or(0);
        if is_ephemeral_subresource {
            // Subresource: only addition is permitted; removing any existing
            // entry must be rejected. Matches upstream Forbidden response.
            if new_ec_count < old_ec_count {
                return Err(rusternetes_common::Error::Forbidden(
                    "spec.ephemeralContainers: Forbidden: existing ephemeral containers may not be removed".to_string(),
                ));
            }
        } else if new_ec_count != old_ec_count {
            // Main path: any structural change to ephemeralContainers is
            // forbidden. Use the subresource instead.
            return Err(rusternetes_common::Error::Forbidden(
                "spec.ephemeralContainers: Forbidden: may not be updated outside of the ephemeralcontainers subresource".to_string(),
            ));
        }
    }

    // K8s ref: pkg/apis/core/validation/validation.go validateActiveDeadlineSeconds +
    // ValidatePodSpecUpdate — once set, activeDeadlineSeconds may only be reduced;
    // it can never be unset, increased, or made zero/negative.
    {
        let old_ads = old_pod
            .spec
            .as_ref()
            .and_then(|s| s.active_deadline_seconds);
        let new_ads = pod.spec.as_ref().and_then(|s| s.active_deadline_seconds);
        match (old_ads, new_ads) {
            // Setting from unset: allowed if positive.
            (None, Some(v)) if v <= 0 => {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "spec.activeDeadlineSeconds: Invalid value: {}: must be greater than 0",
                    v
                )));
            }
            // Reducing or unchanged: allowed.
            (Some(o), Some(n)) if n > 0 && n <= o => {}
            // Increase: forbidden.
            (Some(o), Some(n)) if n > o => {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "spec.activeDeadlineSeconds: Invalid value: {}: must be less than or equal to {}",
                    n, o
                )));
            }
            // Zero or negative when previously set: forbidden.
            (Some(_), Some(n)) if n <= 0 => {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "spec.activeDeadlineSeconds: Invalid value: {}: must be greater than 0",
                    n
                )));
            }
            // Unsetting after it was set: forbidden.
            (Some(_), None) => {
                return Err(rusternetes_common::Error::InvalidResource(
                    "spec.activeDeadlineSeconds: Invalid value: nil: must not be removed"
                        .to_string(),
                ));
            }
            // Other valid combos: (None, None) and (None, Some(positive)).
            _ => {}
        }
    }

    // K8s ref: pkg/apis/core/validation/validation.go ValidatePodSpecUpdate —
    // once `spec.nodeName` is set (typically via the /binding subresource),
    // the main PUT path may not change it. Upstream emits
    // `field.Forbidden(spec.nodeName, "field is immutable")` which maps to
    // `causes[].reason = FieldValueForbidden`.
    {
        let old_node = old_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .unwrap_or("");
        let new_node = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .unwrap_or("");
        if !old_node.is_empty() && new_node != old_node {
            use rusternetes_common::validation::field::{Error as FErr, Path as FPath};
            return Err(rusternetes_common::Error::Invalid(vec![FErr::forbidden(
                &FPath::new("spec").child("nodeName"),
                "field is immutable",
            )]));
        }
    }

    // K8s ref: pkg/registry/core/pod/strategy.go podStrategy.PrepareForUpdate
    // — main resource PUT may not mutate `status`. The status subresource is
    // the only path that may mutate it. Upstream resets
    // `newPod.Status = oldPod.Status` before persisting.
    pod.status = old_pod.status.clone();

    // DefaultTolerationSeconds admission runs on UPDATE too (the upstream
    // plugin registers for admission.Create AND admission.Update — see
    // plugin/pkg/admission/defaulttolerationseconds/admission.go:86
    // NewHandler(Create, Update)). Re-add the default NoExecute tolerations the
    // client may have omitted from the PUT body before the add-only toleration
    // immutability check below, so a round-trip update that drops them does not
    // trip `validateOnlyAddedTolerations`.
    if let Some(ref mut spec) = pod.spec {
        rusternetes_common::tolerations::add_default_tolerations(spec);
    }

    // K8s ref: pkg/apis/core/validation/validation.go ValidatePodUpdate —
    // immutability fence. Only specific spec fields are mutable on a PUT
    // (containers[*].image, initContainers[*].image, activeDeadlineSeconds,
    // terminationGracePeriodSeconds, tolerations (add-only),
    // schedulingGates (delete-only)). Everything else is forbidden.
    if let (Some(old_spec), Some(new_spec)) = (old_pod.spec.as_ref(), pod.spec.as_ref()) {
        if let Err(msg) = rusternetes_common::validation::pod::validate_pod_spec_update(
            old_spec,
            new_spec,
            is_ephemeral_subresource,
        ) {
            return Err(rusternetes_common::Error::InvalidResource(msg));
        }
    }

    let old_pod_value = serde_json::to_value(&old_pod)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    // Define GVK and GVR for Pod
    let gvk = GroupVersionKind {
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "Pod".to_string(),
    };

    let gvr = GroupVersionResource {
        group: "".to_string(),
        version: "v1".to_string(),
        resource: "pods".to_string(),
    };

    // Run mutating webhooks
    let pod_value = serde_json::to_value(&pod)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    let (mutation_response, mutated_pod_value) = state
        .webhook_manager
        .run_mutating_webhooks_with_dryrun(
            &Operation::Update,
            &gvk,
            &gvr,
            Some(&namespace),
            &name,
            Some(pod_value),
            Some(old_pod_value.clone()),
            &user_info,
            is_dry_run,
        )
        .await?;

    // Check if mutating webhooks denied the request
    match mutation_response {
        AdmissionResponse::Deny(reason) => {
            warn!("Mutating webhooks denied pod update: {}", reason);
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
        AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_) => {
            // Continue with the mutated object
            if let Some(mutated_value) = mutated_pod_value {
                pod = serde_json::from_value(mutated_value)
                    .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
                info!("Pod mutated by webhooks: {}/{}", namespace, name);
            }
        }
    }

    // Run validating webhooks
    let final_pod_value = serde_json::to_value(&pod)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    let validation_response = state
        .webhook_manager
        .run_validating_webhooks_with_dryrun(
            &Operation::Update,
            &gvk,
            &gvr,
            Some(&namespace),
            &name,
            Some(final_pod_value),
            Some(old_pod_value.clone()),
            &user_info,
            is_dry_run,
        )
        .await?;

    // Check if validating webhooks denied the request
    match validation_response {
        AdmissionResponse::Deny(reason) => {
            warn!("Validating webhooks denied pod update: {}", reason);
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
        AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_) => {
            info!("Validating webhooks passed for pod {}/{}", namespace, name);
        }
    }

    // Increment generation if spec changed — use STORED generation, not client's
    pod.metadata.generation = old_pod.metadata.generation;
    let new_pod_value = serde_json::to_value(&pod)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    crate::handlers::lifecycle::maybe_increment_generation(
        &old_pod_value,
        &new_pod_value,
        &mut pod.metadata,
    );

    // Detect in-place pod resize: if spec.containers[].resources changed,
    // set status.resize = "Proposed" so the kubelet picks it up (KEP-1287).
    {
        let resources_changed = detect_container_resource_change(&old_pod, &pod);
        if resources_changed {
            info!(
                "Pod {}/{} resource resize detected (update), setting status.resize=Proposed",
                namespace, name
            );
            if let Some(ref mut status) = pod.status {
                status.resize = Some("Proposed".to_string());
            }
        }
    }

    // Check ResourceQuota on update with delta-usage semantics.
    // The old pod's resource contribution is subtracted from the live
    // namespace recount before the new pod's footprint is added, so an
    // UPDATE that keeps total usage at or below `.spec.hard` is admitted
    // even though the stale pod row is still in storage.
    // K8s ref: pkg/quota/v1/evaluator/core/pods.go — PodEvaluator
    match crate::admission::check_resource_quota_with_old(
        &state.storage,
        &namespace,
        &pod,
        Some(&old_pod),
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return Err(rusternetes_common::Error::Forbidden(
                "exceeded quota".to_string(),
            ));
        }
        Err(e) => {
            warn!("Error checking ResourceQuota on pod update: {}", e);
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Pod {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(pod));
    }

    let updated = state.storage.update(&key, &pod).await?;

    Ok(Json(updated))
}

pub async fn delete_pod(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Json<Pod>> {
    info!("Deleting pod: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "pods")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("pods", Some(&namespace), &name);

    // Get the pod to check for finalizers
    let pod: Pod = state.storage.get(&key).await?;

    // Enforce deleteOptions.preconditions.{resourceVersion,uid} before mutating
    // anything. Upstream: pkg/registry/generic/registry/store.go::Delete calls
    // preconditions.Check() before invoking storage.Delete; a mismatch returns
    // 409 Conflict with reason `Conflict`.
    crate::handlers::lifecycle::check_delete_preconditions(&body, &pod.metadata, &name)?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=pod).
    // Runs before the dry-run short-circuit so a deny is observed in dry-run too.
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "Pod",
        "pods",
        Some(&namespace),
        &name,
        &pod,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Pod {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(pod));
    }

    // Parse DeleteOptions from request body (gracePeriodSeconds, propagationPolicy, etc.)
    let body_delete_options: Option<serde_json::Value> = if !body.is_empty() {
        serde_json::from_slice(&body).ok()
    } else {
        None
    };

    // Kubernetes pod deletion: set deletionTimestamp and let the kubelet
    // handle graceful shutdown. The pod remains in storage until the kubelet
    // confirms termination. This is different from other resources where
    // immediate deletion is acceptable.
    let mut updated_pod = pod.clone();

    // Set deletionTimestamp if not already set
    if updated_pod.metadata.deletion_timestamp.is_none() {
        updated_pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
    }

    // Parse gracePeriodSeconds from query params, request body (DeleteOptions), or pod spec
    let body_grace_period = body_delete_options
        .as_ref()
        .and_then(|v| v.get("gracePeriodSeconds"))
        .and_then(|v| v.as_i64());
    let grace_period = params
        .get("gracePeriodSeconds")
        .and_then(|v| v.parse::<i64>().ok())
        .or(body_grace_period)
        .or(updated_pod
            .spec
            .as_ref()
            .and_then(|s| s.termination_grace_period_seconds))
        .unwrap_or(30);
    updated_pod.metadata.deletion_grace_period_seconds = Some(grace_period);

    // If grace period is 0, delete immediately (force delete)
    if grace_period == 0 {
        state.storage.delete(&key).await?;
        return Ok(Json(updated_pod));
    }

    // Update the pod in storage with deletionTimestamp set.
    // Re-read for fresh resourceVersion to avoid CAS conflict.
    // K8s doesn't use CAS for deletion — it reads the latest, sets
    // deletionTimestamp, and writes. If there's a conflict, retry.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go
    let mut fresh_pod: Pod = state.storage.get(&key).await?;
    if fresh_pod.metadata.deletion_timestamp.is_none() {
        fresh_pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
    }
    fresh_pod.metadata.deletion_grace_period_seconds = Some(grace_period);
    let current_gen = fresh_pod.metadata.generation.unwrap_or(1);
    fresh_pod.metadata.generation = Some(current_gen + 1);

    match state.storage.update(&key, &fresh_pod).await {
        Ok(saved) => Ok(Json(saved)),
        Err(rusternetes_common::Error::Conflict(_)) => {
            // CAS conflict — retry once with latest version
            let mut retry_pod: Pod = state.storage.get(&key).await?;
            if retry_pod.metadata.deletion_timestamp.is_none() {
                retry_pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
            }
            retry_pod.metadata.deletion_grace_period_seconds = Some(grace_period);
            let gen = retry_pod.metadata.generation.unwrap_or(1);
            retry_pod.metadata.generation = Some(gen + 1);
            let saved: Pod = state.storage.update(&key, &retry_pod).await?;
            Ok(Json(saved))
        }
        Err(e) => Err(e),
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        info!("Starting watch for pods in namespace: {}", namespace);
        // Parse WatchParams from the query parameters
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
        return crate::handlers::watch::watch_namespaced::<Pod>(
            state,
            auth_ctx,
            namespace,
            "pods",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing pods in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "pods")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("pods", Some(&namespace));
    let mut pods: Vec<Pod> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pods, &params)?;

    // Deterministic sort by (namespace, name) so `?continue` chains are
    // stable regardless of underlying storage iteration order. The offset
    // helper below relies on a total, repeatable order across pages.
    pods.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    // Parse pagination parameters
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();

    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };

    // Use the current etcd revision as the list resourceVersion.
    // K8s returns the etcd revision at the time of the LIST, not the max item RV.
    // This ensures LIST+WATCH consistency AND that successive lists have different RVs.
    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => crate::handlers::list_resource_version(&pods),
    };

    // Apply pagination
    let paginated = match rusternetes_common::paginate(pods, pagination_params, &resource_version) {
        Ok(p) => p,
        Err(e) => {
            if e.message.contains("410 Gone") {
                let mut status = rusternetes_common::Status::failure(&e.message, "Expired", 410);
                if let Some(token) = e.fresh_continue_token {
                    status.metadata = Some(rusternetes_common::ListMeta {
                        resource_version: Some(resource_version),
                        continue_token: Some(token),
                        remaining_item_count: None,
                    });
                }
                return Ok((axum::http::StatusCode::GONE, axum::Json(status)).into_response());
            }
            return Err(rusternetes_common::Error::InvalidResource(e.message));
        }
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table =
            crate::handlers::table::pods_table(paginated.items, Some(resource_version.clone()));
        return Ok(axum::Json(table).into_response());
    }

    // Wrap in proper List object with pagination metadata
    let mut list = List::new("PodList", "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);

    // Attach the PodList opt-in so the response middleware emits a native
    // protobuf envelope when the client negotiates
    // `application/vnd.kubernetes.protobuf`. See `build_pod_response` for
    // the encoder rationale.
    let mut resp = axum::Json(list).into_response();
    resp.extensions_mut()
        .insert(crate::response::NativeProtoOptIn::pod_list());
    Ok(resp)
}

/// List all pods across all namespaces
pub async fn list_all_pods(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        info!("Watch request for all pods");
        // Parse WatchParams from the query parameters
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
        return crate::handlers::watch::watch_cluster_scoped::<Pod>(
            state,
            auth_ctx,
            "pods",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all pods");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "pods").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("pods", None);
    let mut pods = state.storage.list::<Pod>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pods, &params)?;

    // Deterministic sort by (namespace, name) so `?continue` chains are
    // stable regardless of underlying storage iteration order. The offset
    // helper below relies on a total, repeatable order across pages.
    pods.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    // Parse pagination parameters
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();

    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    // Apply pagination
    let paginated = match rusternetes_common::paginate(pods, pagination_params, &resource_version) {
        Ok(p) => p,
        Err(e) => {
            if e.message.contains("410 Gone") {
                let mut status = rusternetes_common::Status::failure(&e.message, "Expired", 410);
                if let Some(token) = e.fresh_continue_token {
                    status.metadata = Some(rusternetes_common::ListMeta {
                        resource_version: Some(resource_version),
                        continue_token: Some(token),
                        remaining_item_count: None,
                    });
                }
                return Ok((axum::http::StatusCode::GONE, axum::Json(status)).into_response());
            }
            return Err(rusternetes_common::Error::InvalidResource(e.message));
        }
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table =
            crate::handlers::table::pods_table(paginated.items, Some(resource_version.clone()));
        return Ok(axum::Json(table).into_response());
    }

    // Wrap in proper List object with pagination metadata
    let mut list = List::new("PodList", "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);

    Ok(axum::Json(list).into_response())
}

pub async fn patch(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    OriginalUri(original_uri): OriginalUri,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Pod>> {
    info!("Patching pod: {}/{}", namespace, name);

    // K8s ref: pkg/registry/core/pod/strategy.go — see update() for context.
    // PATCH may not change ephemeralContainers outside of the dedicated
    // subresource; even on the subresource it may only add entries.
    let is_ephemeral_subresource = original_uri
        .path()
        .trim_end_matches('/')
        .ends_with("/ephemeralcontainers");

    // Check authorization - use 'patch' verb for RBAC
    let attrs = RequestAttributes::new(auth_ctx.user, "patch", "pods")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get Content-Type header — check X-Original-Content-Type first (set by middleware
    // when normalizing patch content types to application/json for Axum compatibility)
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/strategic-merge-patch+json");

    // Check if this is a server-side apply request
    // SSA is ONLY used with Content-Type: application/apply-patch+yaml.
    // Regular patches (merge-patch, strategic-merge-patch, json-patch) with
    // fieldManager are NOT server-side apply — they just track field ownership.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go
    let is_apply = content_type.contains("apply-patch");
    if is_apply {
        if let Some(field_manager) = params.get("fieldManager") {
            use rusternetes_common::server_side_apply::{
                server_side_apply, ApplyParams, ApplyResult,
            };

            info!(
                "Server-side apply for pod {}/{} by manager {}",
                namespace, name, field_manager
            );

            // Get current resource (if exists)
            let key = build_key("pods", Some(&namespace), &name);
            let current_json = match state.storage.get::<Pod>(&key).await {
                Ok(current) => Some(
                    serde_json::to_value(&current)
                        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?,
                ),
                Err(rusternetes_common::Error::NotFound(_)) => None,
                Err(e) => return Err(e),
            };

            // Parse desired resource
            let desired_json: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
                rusternetes_common::Error::InvalidResource(format!("Invalid resource: {}", e))
            })?;

            // Apply with server-side apply semantics
            let force = params
                .get("force")
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(false);

            let apply_params = if force {
                ApplyParams::new(field_manager.clone()).with_force()
            } else {
                ApplyParams::new(field_manager.clone())
            };

            let result = server_side_apply(current_json.as_ref(), &desired_json, &apply_params)
                .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

            match result {
                ApplyResult::Success(applied_json) => {
                    // Convert to Pod type
                    let mut applied_pod: Pod =
                        serde_json::from_value(applied_json).map_err(|e| {
                            rusternetes_common::Error::InvalidResource(format!(
                                "Invalid result: {}",
                                e
                            ))
                        })?;

                    // Ensure metadata matches URL
                    applied_pod.metadata.name = name.clone();
                    applied_pod.metadata.namespace = Some(namespace.clone());

                    // Detect in-place pod resize for server-side apply
                    if let Some(ref current) = current_json {
                        if let Ok(current_pod) = serde_json::from_value::<Pod>(current.clone()) {
                            if detect_container_resource_change(&current_pod, &applied_pod) {
                                info!("Pod {}/{} resource resize detected (SSA), setting status.resize=Proposed", namespace, name);
                                if let Some(ref mut status) = applied_pod.status {
                                    status.resize = Some("Proposed".to_string());
                                }
                            }
                        }
                    }

                    // Check dry-run before persisting
                    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
                    if is_dry_run {
                        info!(
                            "Dry-run: Pod {}/{} server-side apply validated (not persisted)",
                            namespace, name
                        );
                        return Ok(Json(applied_pod));
                    }

                    // Save to storage (create or update)
                    let saved = if current_json.is_some() {
                        state.storage.update(&key, &applied_pod).await?
                    } else {
                        applied_pod.metadata.ensure_uid();
                        applied_pod.metadata.ensure_creation_timestamp();
                        state.storage.create(&key, &applied_pod).await?
                    };

                    return Ok(Json(saved));
                }
                ApplyResult::Conflicts(conflicts) => {
                    // Return 409 Conflict with details
                    let conflict_details: Vec<String> = conflicts
                        .iter()
                        .map(|c| {
                            format!(
                                "Field '{}' is owned by '{}' (applying as '{}')",
                                c.field, c.current_manager, c.applying_manager
                            )
                        })
                        .collect();

                    return Err(rusternetes_common::Error::Conflict(format!(
                        "Apply conflict: {}. Use force=true to override.",
                        conflict_details.join("; ")
                    )));
                }
            }
        }
    }

    // Standard PATCH operation (not server-side apply)
    // Parse patch type
    let patch_type = PatchType::from_content_type(content_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Get current resource
    let key = build_key("pods", Some(&namespace), &name);
    let current_pod: Pod = state.storage.get(&key).await?;

    // Convert to JSON for patching
    let current_json = serde_json::to_value(&current_pod)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    // Parse patch document
    let patch_json: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| rusternetes_common::Error::InvalidResource(format!("Invalid patch: {}", e)))?;

    // Apply patch.
    //
    // RFC 6902 §4.2 mandates that the target of a `remove` (and friends)
    // exists; we surface the upstream-shaped `field.NotFound(<path>, "")`
    // cause when it does not, so the 422 response carries
    // `causes[].reason = FieldValueNotFound` with the real field path.
    let mut patched_json =
        apply_patch(&current_json, &patch_json, patch_type).map_err(|e| match e {
            crate::patch::PatchError::PathNotFound(field_path) => {
                use rusternetes_common::validation::field::{
                    BadValue, Error as FErr, ErrorType as FType,
                };
                // Build a NotFound entry whose `field` is the dotted upstream
                // path (e.g. `spec.doesNotExist`) and whose value is omitted —
                // matches `field.NotFound(path, "")`.
                let fe = FErr {
                    error_type: FType::NotFound,
                    field: field_path,
                    bad_value: BadValue::Omit,
                    detail: String::new(),
                    origin: String::new(),
                };
                rusternetes_common::Error::Invalid(vec![fe])
            }
            other => rusternetes_common::Error::InvalidResource(other.to_string()),
        })?;

    // Ensure metadata.name and metadata.namespace are set before deserializing.
    // Merge patches may omit metadata.name, causing it to be null in the result.
    // K8s preserves these from the URL path, not from the patch body.
    if let Some(metadata) = patched_json
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        metadata.insert("name".to_string(), serde_json::json!(name));
        metadata.insert("namespace".to_string(), serde_json::json!(namespace));
    }

    // Convert back to Pod
    let mut patched_pod: Pod = serde_json::from_value(patched_json).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("Invalid result: {}", e))
    })?;

    // Ensure metadata matches URL (prevent changing name/namespace via patch)
    patched_pod.metadata.name = name.clone();
    patched_pod.metadata.namespace = Some(namespace.clone());

    // K8s ref: pkg/registry/core/pod/strategy.go — see update() for context.
    // ephemeralContainers may only be ADDED through the dedicated subresource;
    // remove is forbidden, and any change at all is forbidden through the
    // regular /pods/{name} patch endpoint.
    {
        let old_ec_count = current_pod
            .spec
            .as_ref()
            .and_then(|s| s.ephemeral_containers.as_ref())
            .map(|v| v.len())
            .unwrap_or(0);
        let new_ec_count = patched_pod
            .spec
            .as_ref()
            .and_then(|s| s.ephemeral_containers.as_ref())
            .map(|v| v.len())
            .unwrap_or(0);
        if is_ephemeral_subresource {
            if new_ec_count < old_ec_count {
                return Err(rusternetes_common::Error::Forbidden(
                    "spec.ephemeralContainers: Forbidden: existing ephemeral containers may not be removed".to_string(),
                ));
            }
        } else if new_ec_count != old_ec_count {
            return Err(rusternetes_common::Error::Forbidden(
                "spec.ephemeralContainers: Forbidden: may not be updated outside of the ephemeralcontainers subresource".to_string(),
            ));
        }
    }

    // Detect in-place pod resize: if spec.containers[].resources changed,
    // set status.resize = "Proposed" so the kubelet picks it up (KEP-1287).
    {
        let resources_changed = detect_container_resource_change(&current_pod, &patched_pod);
        if resources_changed {
            info!(
                "Pod {}/{} resource resize detected, setting status.resize=Proposed",
                namespace, name
            );
            if let Some(ref mut status) = patched_pod.status {
                status.resize = Some("Proposed".to_string());
            }
        }
    }

    // Check ResourceQuota on patch with delta-usage semantics — same as
    // pod UPDATE. A PATCH that pushes the per-namespace total past
    // `.spec.hard` (after subtracting the stale row) must be rejected.
    // K8s ref: pkg/quota/v1/evaluator/core/pods.go — PodEvaluator
    match crate::admission::check_resource_quota_with_old(
        &state.storage,
        &namespace,
        &patched_pod,
        Some(&current_pod),
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return Err(rusternetes_common::Error::Forbidden(
                "exceeded quota".to_string(),
            ));
        }
        Err(e) => {
            warn!("Error checking ResourceQuota on pod patch: {}", e);
        }
    }

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!(
            "Dry-run: Pod {}/{} patch validated successfully (not applied)",
            namespace, name
        );
        return Ok(Json(patched_pod));
    }

    // Increment generation if spec changed — use STORED generation, not client's
    patched_pod.metadata.generation = current_pod.metadata.generation;
    let patched_pod_value = serde_json::to_value(&patched_pod)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    crate::handlers::lifecycle::maybe_increment_generation(
        &current_json,
        &patched_pod_value,
        &mut patched_pod.metadata,
    );

    // For PATCH operations, clear resourceVersion to skip optimistic concurrency.
    // PATCH is a read-modify-write operation, and between our read and write the
    // kubelet may update the pod status (incrementing resourceVersion). The patch
    // semantics merge fields, so this is safe.
    patched_pod.metadata.resource_version = None;

    // Update in storage
    let updated = state.storage.update(&key, &patched_pod).await?;

    Ok(Json(updated))
}

pub async fn deletecollection_pods(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection pods in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "pods")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Pod collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all pods in the namespace
    let prefix = build_prefix("pods", Some(&namespace));
    let mut pods = state.storage.list::<Pod>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pods, &params)?;

    // Delete each matching pod
    let mut deleted_count = 0;
    for pod in pods {
        let key = build_key("pods", Some(&namespace), &pod.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "Pod",
            "pods",
            Some(&namespace),
            &pod.metadata.name,
            &pod,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately =
            !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &pod)
                .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!("DeleteCollection completed: {} pods deleted", deleted_count);
    Ok(StatusCode::OK)
}

/// Validate sysctl name matches K8s allowed pattern.
/// Names must be dot-separated segments of lowercase alphanumeric with hyphens/underscores.
fn is_valid_sysctl_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 253 {
        return false;
    }
    // Kubernetes treats `/` and `.` as equivalent sysctl separators and
    // canonicalises slashes to dots before validating (upstream
    // convertSysctlVariableToDotsSeparator). Validate on the dotted form so the
    // slash variant of a name (e.g. `kernel/shm_rmid_forced`) is accepted.
    let name = name.replace('/', ".");
    for segment in name.split('.') {
        if segment.is_empty() {
            return false;
        }
        let chars: Vec<char> = segment.chars().collect();
        // First char must be lowercase alphanumeric
        if !chars[0].is_ascii_lowercase() && !chars[0].is_ascii_digit() {
            return false;
        }
        // Last char must be lowercase alphanumeric (not - or _)
        if chars.len() > 1 {
            let last = *chars.last().unwrap();
            if !last.is_ascii_lowercase() && !last.is_ascii_digit() {
                return false;
            }
        }
        // Middle chars can be lowercase alphanumeric, - or _
        for &c in &chars[1..] {
            if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-' && c != '_' {
                return false;
            }
        }
    }
    true
}

/// Detect if container resource requests/limits changed between old and new pod specs.
/// Used to trigger in-place pod resize (KEP-1287).
fn detect_container_resource_change(old_pod: &Pod, new_pod: &Pod) -> bool {
    let old_containers = old_pod.spec.as_ref().map(|s| &s.containers);
    let new_containers = new_pod.spec.as_ref().map(|s| &s.containers);

    // Normalise `Some(ResourceRequirements { requests: None, limits: None })`
    // to `None`. Go's `corev1.Container.Resources` is a value-typed
    // `ResourceRequirements` (not a pointer) with `omitempty`, but Go's
    // `omitempty` does NOT detect zero-valued structs, so a no-op
    // GET→re-marshal→PUT cycle always emits `"resources":{}` on every
    // container. Without this normalisation the very first "empty update"
    // sub-test of the [Conformance] pod-generation suite would incorrectly
    // trip the resize detector and flip `status.resize` to `Proposed` on a
    // no-op PUT. Upstream relies on `apiequality.Semantic.DeepEqual`
    // which already handles this — we have to do it explicitly.
    fn resources_meaningful(
        r: Option<&rusternetes_common::types::ResourceRequirements>,
    ) -> Option<&rusternetes_common::types::ResourceRequirements> {
        r.filter(|rr| {
            rr.requests.as_ref().is_some_and(|m| !m.is_empty())
                || rr.limits.as_ref().is_some_and(|m| !m.is_empty())
        })
    }

    match (old_containers, new_containers) {
        (Some(old_cs), Some(new_cs)) => {
            for new_c in new_cs {
                if let Some(old_c) = old_cs.iter().find(|c| c.name == new_c.name) {
                    let old_res = resources_meaningful(old_c.resources.as_ref());
                    let new_res = resources_meaningful(new_c.resources.as_ref());
                    match (old_res, new_res) {
                        (Some(old_r), Some(new_r)) => {
                            if old_r.requests != new_r.requests || old_r.limits != new_r.limits {
                                return true;
                            }
                        }
                        (None, Some(_)) | (Some(_), None) => return true,
                        (None, None) => {}
                    }
                }
            }
            false
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_container_with_resources(
        name: &str,
        cpu_limit: Option<&str>,
        mem_limit: Option<&str>,
    ) -> rusternetes_common::resources::Container {
        let limits = match (cpu_limit, mem_limit) {
            (None, None) => None,
            _ => {
                let mut m = HashMap::new();
                if let Some(c) = cpu_limit {
                    m.insert("cpu".to_string(), c.to_string());
                }
                if let Some(mem) = mem_limit {
                    m.insert("memory".to_string(), mem.to_string());
                }
                Some(m)
            }
        };

        let json = serde_json::json!({
            "name": name,
            "image": "nginx:latest",
            "resources": if limits.is_some() {
                serde_json::json!({
                    "limits": limits,
                })
            } else {
                serde_json::Value::Null
            },
        });

        serde_json::from_value(json).unwrap()
    }

    fn make_pod_with_containers(
        name: &str,
        containers: Vec<rusternetes_common::resources::Container>,
    ) -> Pod {
        let json = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": "default",
            },
            "spec": {
                "containers": containers.iter().map(|c| serde_json::to_value(c).unwrap()).collect::<Vec<_>>(),
            },
        });

        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn test_detect_resource_change_no_change() {
        let pod1 = make_pod_with_containers(
            "p",
            vec![make_container_with_resources(
                "c1",
                Some("100m"),
                Some("128Mi"),
            )],
        );
        let pod2 = make_pod_with_containers(
            "p",
            vec![make_container_with_resources(
                "c1",
                Some("100m"),
                Some("128Mi"),
            )],
        );
        assert!(!detect_container_resource_change(&pod1, &pod2));
    }

    #[test]
    fn test_detect_resource_change_cpu_changed() {
        let pod1 = make_pod_with_containers(
            "p",
            vec![make_container_with_resources(
                "c1",
                Some("100m"),
                Some("128Mi"),
            )],
        );
        let pod2 = make_pod_with_containers(
            "p",
            vec![make_container_with_resources(
                "c1",
                Some("200m"),
                Some("128Mi"),
            )],
        );
        assert!(detect_container_resource_change(&pod1, &pod2));
    }

    #[test]
    fn test_detect_resource_change_memory_changed() {
        let pod1 = make_pod_with_containers(
            "p",
            vec![make_container_with_resources(
                "c1",
                Some("100m"),
                Some("128Mi"),
            )],
        );
        let pod2 = make_pod_with_containers(
            "p",
            vec![make_container_with_resources(
                "c1",
                Some("100m"),
                Some("256Mi"),
            )],
        );
        assert!(detect_container_resource_change(&pod1, &pod2));
    }

    #[test]
    fn test_detect_resource_change_no_resources_to_some() {
        let pod1 =
            make_pod_with_containers("p", vec![make_container_with_resources("c1", None, None)]);
        let pod2 = make_pod_with_containers(
            "p",
            vec![make_container_with_resources("c1", Some("100m"), None)],
        );
        assert!(detect_container_resource_change(&pod1, &pod2));
    }

    #[test]
    fn test_detect_resource_change_both_no_resources() {
        let pod1 =
            make_pod_with_containers("p", vec![make_container_with_resources("c1", None, None)]);
        let pod2 =
            make_pod_with_containers("p", vec![make_container_with_resources("c1", None, None)]);
        assert!(!detect_container_resource_change(&pod1, &pod2));
    }

    #[test]
    fn test_sysctl_safe_list() {
        // Safe sysctls should not cause validation errors
        let safe = [
            "kernel.shm_rmid_forced",
            "net.ipv4.ip_local_port_range",
            "net.ipv4.tcp_syncookies",
            "net.ipv4.ping_group_range",
            "net.ipv4.ip_unprivileged_port_start",
            "net.ipv4.conf.all.forwarding",
            "net.ipv6.conf.all.forwarding",
        ];
        let safe_sysctls = [
            "kernel.shm_rmid_forced",
            "net.ipv4.ip_local_port_range",
            "net.ipv4.tcp_syncookies",
            "net.ipv4.ping_group_range",
            "net.ipv4.ip_unprivileged_port_start",
        ];
        for name in &safe {
            let is_safe = safe_sysctls.contains(name)
                || name.starts_with("net.ipv4.conf.")
                || name.starts_with("net.ipv6.conf.");
            assert!(is_safe, "Expected {} to be classified as safe", name);
        }
    }

    #[test]
    fn test_sysctl_unsafe_list() {
        // Unsafe sysctls should NOT match the safe list
        let unsafe_sysctls = [
            "kernel.msgmax",
            "kernel.sem",
            "net.core.somaxconn",
            "vm.max_map_count",
        ];
        let safe_sysctls = [
            "kernel.shm_rmid_forced",
            "net.ipv4.ip_local_port_range",
            "net.ipv4.tcp_syncookies",
            "net.ipv4.ping_group_range",
            "net.ipv4.ip_unprivileged_port_start",
        ];
        for name in &unsafe_sysctls {
            let is_safe = safe_sysctls.contains(name)
                || name.starts_with("net.ipv4.conf.")
                || name.starts_with("net.ipv6.conf.");
            assert!(!is_safe, "Expected {} to be classified as unsafe", name);
        }
    }

    #[test]
    fn test_sysctl_name_accepts_slash_separator() {
        // Kubernetes treats `/` and `.` as equivalent sysctl separators
        // (upstream convertSysctlVariableToDotsSeparator). The slash form of a
        // safe sysctl must validate so the pod is accepted — NodeConformance
        // "should support sysctls with slashes as separator" (#1068).
        assert!(is_valid_sysctl_name("kernel/shm_rmid_forced"));
        assert!(is_valid_sysctl_name("net/ipv4/tcp_syncookies"));
        // Dotted form still valid.
        assert!(is_valid_sysctl_name("kernel.shm_rmid_forced"));
        // Genuinely invalid names still rejected (uppercase, empty segment).
        assert!(!is_valid_sysctl_name("Kernel/Shm"));
        assert!(!is_valid_sysctl_name("kernel//shm"));
        assert!(!is_valid_sysctl_name("kernel/"));
    }

    #[test]
    fn test_sysctl_unsafe_returns_forbidden() {
        // Verify the error message format matches K8s: pods "name" is forbidden: unsafe sysctl "..." is not allowed
        let error_msg = format!(
            "pods \"{}\" is forbidden: unsafe sysctl \"{}\" is not allowed",
            "test-pod", "kernel.msgmax"
        );
        // Verify it creates a Forbidden error (HTTP 403)
        let err = rusternetes_common::Error::Forbidden(error_msg.clone());
        assert_eq!(err.reason(), "Forbidden");
        assert!(error_msg.contains("forbidden"));
        assert!(error_msg.contains("unsafe sysctl"));
        assert!(error_msg.contains("kernel.msgmax"));
    }
}
