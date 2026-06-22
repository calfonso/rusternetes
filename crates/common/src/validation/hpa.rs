//! HorizontalPodAutoscaler validation — port of upstream Kubernetes
//! `pkg/apis/autoscaling/validation/validation.go::ValidateHorizontalPodAutoscalerSpec`
//! (release-1.35).
//!
//! Covers `minReplicas`/`maxReplicas` bounds, `scaleTargetRef`
//! (`ValidateCrossVersionObjectReference` + `ValidateAPIVersion`), the per-metric
//! source validation (`validateMetrics`/`validateMetricSpec` and each
//! per-source validator), `validateMetricTarget`, `validateMetricIdentifier`,
//! the scale-to-zero guard, and the `behavior` scaling-rules validation.

use crate::quantity::Quantity;
use crate::resources::autoscaling::{
    ContainerResourceMetricSource, CrossVersionObjectReference, ExternalMetricSource,
    HPAScalingPolicy, HPAScalingRules, HorizontalPodAutoscaler, HorizontalPodAutoscalerBehavior,
    HorizontalPodAutoscalerSpec, MetricIdentifier, MetricSpec, MetricTarget, ObjectMetricSource,
    PodsMetricSource, ResourceMetricSource,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_label;
use crate::validation::objectmeta::name_is_path_segment;

/// Largest allowed scaling-policy period, in seconds. Mirrors upstream
/// `autoscaling/validation.MaxPeriodSeconds`.
const MAX_PERIOD_SECONDS: i32 = 1800;
/// Largest allowed stabilization window, in seconds. Mirrors upstream
/// `autoscaling/validation.MaxStabilizationWindowSeconds`.
const MAX_STABILIZATION_WINDOW_SECONDS: i32 = 3600;

/// Valid metric `type` enum values (`validMetricSourceTypes`).
const VALID_METRIC_SOURCE_TYPES: [&str; 5] = [
    "ContainerResource",
    "External",
    "Object",
    "Pods",
    "Resource",
];
/// Valid `selectPolicy` enum values (`validSelectPolicyTypes`), sorted as
/// upstream's `sets.String.List()` returns them.
const VALID_SELECT_POLICY_TYPES: [&str; 3] = ["Disabled", "Max", "Min"];
/// Valid scaling-policy `type` enum values (`validPolicyTypes`), sorted.
const VALID_POLICY_TYPES: [&str; 2] = ["Percent", "Pods"];

/// Options controlling `CrossVersionObjectReference` validation. Mirrors
/// upstream `CrossVersionObjectReferenceValidationOptions`.
#[derive(Clone, Copy, Default)]
pub struct CrossVersionObjectReferenceValidationOptions {
    /// Allow an empty API group (apiVersion without a group, e.g. core `v1`).
    pub allow_empty_api_group: bool,
    /// Skip apiVersion validation entirely.
    pub allow_invalid_api_version: bool,
}

/// Options for `HorizontalPodAutoscalerSpec` validation. Mirrors upstream
/// `HorizontalPodAutoscalerSpecValidationOptions`.
#[derive(Clone, Copy)]
pub struct HorizontalPodAutoscalerSpecValidationOptions {
    /// The minimum allowed value for `minReplicas`.
    pub min_replicas_lower_bound: i32,
    pub scale_target_ref_validation_options: CrossVersionObjectReferenceValidationOptions,
    pub object_metrics_validation_options: CrossVersionObjectReferenceValidationOptions,
}

impl HorizontalPodAutoscalerSpecValidationOptions {
    /// Defaults for a *create* (no old object), mirroring
    /// `validationOptionsForHorizontalPodAutoscaler(newHPA, nil)` with the
    /// `HPAScaleToZero` feature gate off: lower bound 1, scaleTargetRef must
    /// carry an API group (except `ReplicationController`), object metrics may
    /// omit the group.
    fn for_create(spec: &HorizontalPodAutoscalerSpec) -> Self {
        let mut scale_target_ref = CrossVersionObjectReferenceValidationOptions {
            allow_empty_api_group: false,
            allow_invalid_api_version: false,
        };
        // Upstream: allow empty apiVersion for the only scalable core/v1 type.
        if spec.scale_target_ref.kind == "ReplicationController" {
            scale_target_ref.allow_empty_api_group = true;
        }
        Self {
            min_replicas_lower_bound: 1,
            scale_target_ref_validation_options: scale_target_ref,
            object_metrics_validation_options: CrossVersionObjectReferenceValidationOptions {
                allow_empty_api_group: true,
                allow_invalid_api_version: false,
            },
        }
    }
}

/// Upstream `schema.ParseGroupVersion`: returns `(group, version)` or an error
/// string for a malformed value (more than one `/`). Empty / `"/"` parse to an
/// empty `GroupVersion` with no error.
fn parse_group_version(gv: &str) -> Result<(String, String), String> {
    if gv.is_empty() || gv == "/" {
        return Ok((String::new(), String::new()));
    }
    match gv.matches('/').count() {
        0 => Ok((String::new(), gv.to_string())),
        1 => {
            let (g, v) = gv.split_once('/').expect("one slash present");
            Ok((g.to_string(), v.to_string()))
        }
        _ => Err(format!("unexpected GroupVersion string: {gv}")),
    }
}

/// Upstream `ValidateAPIVersion`: returns an error message if the apiVersion is
/// unparseable, or (without `allow_empty_api_group`) names no API group.
fn validate_api_version(
    api_version: &str,
    opts: CrossVersionObjectReferenceValidationOptions,
) -> Option<String> {
    if opts.allow_invalid_api_version {
        return None;
    }
    match parse_group_version(api_version) {
        Err(e) => Some(e),
        Ok((group, _version)) => {
            if !opts.allow_empty_api_group && group.is_empty() {
                Some("apiVersion must specify API group".to_string())
            } else {
                None
            }
        }
    }
}

/// Upstream `ValidateCrossVersionObjectReference`.
fn validate_cross_version_object_reference(
    reference: &CrossVersionObjectReference,
    fld_path: &Path,
    opts: CrossVersionObjectReferenceValidationOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if reference.kind.is_empty() {
        errs.push(Error::required(&fld_path.child("kind"), ""));
    } else {
        for msg in name_is_path_segment(&reference.kind, false) {
            errs.push(Error::invalid(
                &fld_path.child("kind"),
                reference.kind.clone(),
                msg,
            ));
        }
    }
    if reference.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    } else {
        for msg in name_is_path_segment(&reference.name, false) {
            errs.push(Error::invalid(
                &fld_path.child("name"),
                reference.name.clone(),
                msg,
            ));
        }
    }
    let api_version = reference.api_version.clone().unwrap_or_default();
    if let Some(msg) = validate_api_version(&api_version, opts) {
        errs.push(Error::invalid(
            &fld_path.child("apiVersion"),
            api_version,
            msg,
        ));
    }
    errs
}

/// Validate a `HorizontalPodAutoscalerSpec`. Mirrors upstream
/// `validateHorizontalPodAutoscalerSpec` with create-time options.
pub fn validate_hpa_spec(spec: &HorizontalPodAutoscalerSpec, fld_path: &Path) -> ErrorList {
    let opts = HorizontalPodAutoscalerSpecValidationOptions::for_create(spec);
    validate_hpa_spec_with_opts(spec, fld_path, &opts)
}

fn validate_hpa_spec_with_opts(
    spec: &HorizontalPodAutoscalerSpec,
    fld_path: &Path,
    opts: &HorizontalPodAutoscalerSpecValidationOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(min) = spec.min_replicas {
        if min < opts.min_replicas_lower_bound {
            errs.push(Error::invalid(
                &fld_path.child("minReplicas"),
                min,
                format!(
                    "must be greater than or equal to {}",
                    opts.min_replicas_lower_bound
                ),
            ));
        }
    }
    if spec.max_replicas < 1 {
        errs.push(Error::invalid(
            &fld_path.child("maxReplicas"),
            spec.max_replicas,
            "must be greater than 0",
        ));
    }
    if let Some(min) = spec.min_replicas {
        if spec.max_replicas < min {
            errs.push(Error::invalid(
                &fld_path.child("maxReplicas"),
                spec.max_replicas,
                "must be greater than or equal to `minReplicas`",
            ));
        }
    }

    errs.extend(validate_cross_version_object_reference(
        &spec.scale_target_ref,
        &fld_path.child("scaleTargetRef"),
        opts.scale_target_ref_validation_options,
    ));
    errs.extend(validate_metrics(
        spec.metrics.as_deref().unwrap_or(&[]),
        &fld_path.child("metrics"),
        spec.min_replicas,
        opts.object_metrics_validation_options,
    ));
    errs.extend(validate_behavior(
        spec.behavior.as_ref(),
        &fld_path.child("behavior"),
    ));

    errs
}

/// Upstream `validateMetrics`: validate each metric spec, then enforce the
/// scale-to-zero guard (`minReplicas == 0` requires an Object/External metric).
fn validate_metrics(
    metrics: &[MetricSpec],
    fld_path: &Path,
    min_replicas: Option<i32>,
    opts: CrossVersionObjectReferenceValidationOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut has_object_metrics = false;
    let mut has_external_metrics = false;

    for (i, metric) in metrics.iter().enumerate() {
        let idx_path = fld_path.index(i);
        errs.extend(validate_metric_spec(metric, &idx_path, opts));
        if metric.metric_type == "Object" {
            has_object_metrics = true;
        }
        if metric.metric_type == "External" {
            has_external_metrics = true;
        }
    }

    if min_replicas == Some(0) && !has_object_metrics && !has_external_metrics {
        errs.push(Error::forbidden(
            fld_path,
            "must specify at least one Object or External metric to support scaling to zero replicas",
        ));
    }

    errs
}

/// Upstream `validateMetricSpec`: enforce a valid `type` enum, the
/// exactly-one-source rule (the populated source must match `type`), and
/// delegate to the matching per-source validator.
fn validate_metric_spec(
    spec: &MetricSpec,
    fld_path: &Path,
    opts: CrossVersionObjectReferenceValidationOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if spec.metric_type.is_empty() {
        errs.push(Error::required(
            &fld_path.child("type"),
            "must specify a metric source type",
        ));
    }

    if !VALID_METRIC_SOURCE_TYPES.contains(&spec.metric_type.as_str()) {
        errs.push(Error::not_supported(
            &fld_path.child("type"),
            spec.metric_type.clone(),
            &VALID_METRIC_SOURCE_TYPES,
        ));
    }

    // Track which source structs are populated (upstream `typesPresent`), and
    // validate each populated source only when it is the first one seen.
    let mut types_present: Vec<&str> = Vec::new();
    if let Some(object) = &spec.object {
        types_present.push("object");
        if types_present.len() == 1 {
            errs.extend(validate_object_source(
                object,
                &fld_path.child("object"),
                opts,
            ));
        }
    }
    if let Some(external) = &spec.external {
        types_present.push("external");
        if types_present.len() == 1 {
            errs.extend(validate_external_source(
                external,
                &fld_path.child("external"),
            ));
        }
    }
    if let Some(pods) = &spec.pods {
        types_present.push("pods");
        if types_present.len() == 1 {
            errs.extend(validate_pods_source(pods, &fld_path.child("pods")));
        }
    }
    if let Some(resource) = &spec.resource {
        types_present.push("resource");
        if types_present.len() == 1 {
            errs.extend(validate_resource_source(
                resource,
                &fld_path.child("resource"),
            ));
        }
    }
    if let Some(container_resource) = &spec.container_resource {
        types_present.push("containerResource");
        if types_present.len() == 1 {
            errs.extend(validate_container_resource_source(
                container_resource,
                &fld_path.child("containerResource"),
            ));
        }
    }

    // The source struct matching `type` must be populated.
    let expected_field = match spec.metric_type.as_str() {
        "Object" => {
            if spec.object.is_none() {
                errs.push(Error::required(
                    &fld_path.child("object"),
                    "must populate information for the given metric source",
                ));
            }
            Some("object")
        }
        "Pods" => {
            if spec.pods.is_none() {
                errs.push(Error::required(
                    &fld_path.child("pods"),
                    "must populate information for the given metric source",
                ));
            }
            Some("pods")
        }
        "Resource" => {
            if spec.resource.is_none() {
                errs.push(Error::required(
                    &fld_path.child("resource"),
                    "must populate information for the given metric source",
                ));
            }
            Some("resource")
        }
        "External" => {
            if spec.external.is_none() {
                errs.push(Error::required(
                    &fld_path.child("external"),
                    "must populate information for the given metric source",
                ));
            }
            Some("external")
        }
        "ContainerResource" => {
            if spec.container_resource.is_none() {
                errs.push(Error::required(
                    &fld_path.child("containerResource"),
                    "must populate information for the given metric source",
                ));
            }
            Some("containerResource")
        }
        _ => {
            errs.push(Error::not_supported(
                &fld_path.child("type"),
                spec.metric_type.clone(),
                &VALID_METRIC_SOURCE_TYPES,
            ));
            None
        }
    };

    // Exactly one source may be populated; any extra (beyond the one matching
    // `type`) is forbidden.
    if types_present.len() != 1 {
        for typ in types_present.iter().filter(|&&t| Some(t) != expected_field) {
            errs.push(Error::forbidden(
                &fld_path.child(*typ),
                "must populate the given metric source only",
            ));
        }
    }

    errs
}

/// Upstream `validateObjectSource`.
fn validate_object_source(
    src: &ObjectMetricSource,
    fld_path: &Path,
    opts: CrossVersionObjectReferenceValidationOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    errs.extend(validate_cross_version_object_reference(
        &src.described_object,
        &fld_path.child("describedObject"),
        opts,
    ));
    errs.extend(validate_metric_identifier(
        &src.metric,
        &fld_path.child("metric"),
    ));
    errs.extend(validate_metric_target(
        &src.target,
        &fld_path.child("target"),
    ));

    if src.target.value.is_none() && src.target.average_value.is_none() {
        errs.push(Error::required(
            &fld_path.child("target").child("averageValue"),
            "must set either a target value or averageValue",
        ));
    }
    errs
}

/// Upstream `validateExternalSource`.
fn validate_external_source(src: &ExternalMetricSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    errs.extend(validate_metric_identifier(
        &src.metric,
        &fld_path.child("metric"),
    ));
    errs.extend(validate_metric_target(
        &src.target,
        &fld_path.child("target"),
    ));

    if src.target.value.is_none() && src.target.average_value.is_none() {
        errs.push(Error::required(
            &fld_path.child("target").child("averageValue"),
            "must set either a target value for metric or a per-pod target",
        ));
    }
    if src.target.value.is_some() && src.target.average_value.is_some() {
        errs.push(Error::forbidden(
            &fld_path.child("target").child("value"),
            "may not set both a target value for metric and a per-pod target",
        ));
    }
    errs
}

/// Upstream `validatePodsSource`.
fn validate_pods_source(src: &PodsMetricSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    errs.extend(validate_metric_identifier(
        &src.metric,
        &fld_path.child("metric"),
    ));
    errs.extend(validate_metric_target(
        &src.target,
        &fld_path.child("target"),
    ));

    if src.target.average_value.is_none() {
        errs.push(Error::required(
            &fld_path.child("target").child("averageValue"),
            "must specify a positive target averageValue",
        ));
    }
    errs
}

/// Upstream `validateContainerResourceSource`.
fn validate_container_resource_source(
    src: &ContainerResourceMetricSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if src.name.is_empty() {
        errs.push(Error::required(
            &fld_path.child("name"),
            "must specify a resource name",
        ));
    }
    if src.container.is_empty() {
        errs.push(Error::required(
            &fld_path.child("container"),
            "must specify a container",
        ));
    } else {
        for msg in is_dns1123_label(&src.container) {
            errs.push(Error::invalid(
                &fld_path.child("container"),
                src.container.clone(),
                msg,
            ));
        }
    }
    errs.extend(validate_metric_target(
        &src.target,
        &fld_path.child("target"),
    ));

    if src.target.average_utilization.is_none() && src.target.average_value.is_none() {
        errs.push(Error::required(
            &fld_path.child("target").child("averageUtilization"),
            "must set either a target raw value or a target utilization",
        ));
    }
    if src.target.average_utilization.is_some() && src.target.average_value.is_some() {
        errs.push(Error::forbidden(
            &fld_path.child("target").child("averageValue"),
            "may not set both a target raw value and a target utilization",
        ));
    }
    errs
}

/// Upstream `validateResourceSource`.
fn validate_resource_source(src: &ResourceMetricSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if src.name.is_empty() {
        errs.push(Error::required(
            &fld_path.child("name"),
            "must specify a resource name",
        ));
    }
    errs.extend(validate_metric_target(
        &src.target,
        &fld_path.child("target"),
    ));

    if src.target.average_utilization.is_none() && src.target.average_value.is_none() {
        errs.push(Error::required(
            &fld_path.child("target").child("averageUtilization"),
            "must set either a target raw value or a target utilization",
        ));
    }
    if src.target.average_utilization.is_some() && src.target.average_value.is_some() {
        errs.push(Error::forbidden(
            &fld_path.child("target").child("averageValue"),
            "may not set both a target raw value and a target utilization",
        ));
    }
    errs
}

/// Returns true when a quantity string parses to a strictly positive value
/// (upstream `Quantity.Sign() == 1`). Unparseable strings are treated as
/// non-positive here; in practice apiserver decoding already rejects them.
fn quantity_is_positive(value: &str) -> bool {
    let zero = Quantity::parse("0").expect("zero parses");
    match Quantity::parse(value) {
        Ok(q) => q.cmp_value(&zero) == std::cmp::Ordering::Greater,
        Err(_) => false,
    }
}

/// Upstream `validateMetricTarget`.
fn validate_metric_target(mt: &MetricTarget, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if mt.target_type.is_empty() {
        errs.push(Error::required(
            &fld_path.child("type"),
            "must specify a metric target type",
        ));
    }

    if mt.target_type != "Utilization"
        && mt.target_type != "Value"
        && mt.target_type != "AverageValue"
    {
        errs.push(Error::invalid(
            &fld_path.child("type"),
            mt.target_type.clone(),
            "must be either Utilization, Value, or AverageValue",
        ));
    }

    if let Some(value) = &mt.value {
        if !quantity_is_positive(value) {
            errs.push(Error::invalid(
                &fld_path.child("value"),
                value.clone(),
                "must be positive",
            ));
        }
    }

    if let Some(average_value) = &mt.average_value {
        if !quantity_is_positive(average_value) {
            errs.push(Error::invalid(
                &fld_path.child("averageValue"),
                average_value.clone(),
                "must be positive",
            ));
        }
    }

    if let Some(average_utilization) = mt.average_utilization {
        if average_utilization < 1 {
            errs.push(Error::invalid(
                &fld_path.child("averageUtilization"),
                average_utilization,
                "must be greater than 0",
            ));
        }
    }

    errs
}

/// Upstream `validateMetricIdentifier`.
fn validate_metric_identifier(id: &MetricIdentifier, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if id.name.is_empty() {
        errs.push(Error::required(
            &fld_path.child("name"),
            "must specify a metric name",
        ));
    } else {
        for msg in name_is_path_segment(&id.name, false) {
            errs.push(Error::invalid(
                &fld_path.child("name"),
                id.name.clone(),
                msg,
            ));
        }
    }
    errs
}

/// Upstream `validateBehavior`.
fn validate_behavior(
    behavior: Option<&HorizontalPodAutoscalerBehavior>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(behavior) = behavior {
        errs.extend(validate_scaling_rules(
            behavior.scale_up.as_ref(),
            &fld_path.child("scaleUp"),
        ));
        errs.extend(validate_scaling_rules(
            behavior.scale_down.as_ref(),
            &fld_path.child("scaleDown"),
        ));
    }
    errs
}

/// Upstream `validateScalingRules`.
fn validate_scaling_rules(rules: Option<&HPAScalingRules>, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(rules) = rules else {
        return errs;
    };

    if let Some(window) = rules.stabilization_window_seconds {
        if window < 0 {
            errs.push(Error::invalid(
                &fld_path.child("stabilizationWindowSeconds"),
                window,
                "must be greater than or equal to zero",
            ));
        }
        if window > MAX_STABILIZATION_WINDOW_SECONDS {
            errs.push(Error::invalid(
                &fld_path.child("stabilizationWindowSeconds"),
                window,
                format!("must be less than or equal to {MAX_STABILIZATION_WINDOW_SECONDS}"),
            ));
        }
    }

    if let Some(select_policy) = &rules.select_policy {
        if !VALID_SELECT_POLICY_TYPES.contains(&select_policy.as_str()) {
            errs.push(Error::not_supported(
                &fld_path.child("selectPolicy"),
                select_policy.clone(),
                &VALID_SELECT_POLICY_TYPES,
            ));
        }
    }

    let policies_path = fld_path.child("policies");
    let policies = rules.policies.as_deref().unwrap_or(&[]);
    if policies.is_empty() {
        errs.push(Error::required(
            &policies_path,
            "must specify at least one Policy",
        ));
    }
    for (i, policy) in policies.iter().enumerate() {
        errs.extend(validate_scaling_policy(policy, &policies_path.index(i)));
    }

    if let Some(tolerance) = &rules.tolerance {
        errs.extend(validate_nonnegative_quantity(
            tolerance,
            &fld_path.child("tolerance"),
        ));
    }

    errs
}

/// Upstream `apivalidation.ValidateNonnegativeQuantity`: the quantity must
/// parse and be `>= 0`.
fn validate_nonnegative_quantity(value: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let zero = Quantity::parse("0").expect("zero parses");
    match Quantity::parse(value) {
        Ok(q) if q.cmp_value(&zero) == std::cmp::Ordering::Less => {
            errs.push(Error::invalid(
                fld_path,
                value.to_string(),
                "must be greater than or equal to 0",
            ));
        }
        Ok(_) => {}
        Err(_) => {
            errs.push(Error::invalid(
                fld_path,
                value.to_string(),
                "must be greater than or equal to 0",
            ));
        }
    }
    errs
}

/// Upstream `validateScalingPolicy`.
fn validate_scaling_policy(policy: &HPAScalingPolicy, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if policy.policy_type != "Pods" && policy.policy_type != "Percent" {
        errs.push(Error::not_supported(
            &fld_path.child("type"),
            policy.policy_type.clone(),
            &VALID_POLICY_TYPES,
        ));
    }
    if policy.value <= 0 {
        errs.push(Error::invalid(
            &fld_path.child("value"),
            policy.value,
            "must be greater than zero",
        ));
    }
    if policy.period_seconds <= 0 {
        errs.push(Error::invalid(
            &fld_path.child("periodSeconds"),
            policy.period_seconds,
            "must be greater than zero",
        ));
    }
    if policy.period_seconds > MAX_PERIOD_SECONDS {
        errs.push(Error::invalid(
            &fld_path.child("periodSeconds"),
            policy.period_seconds,
            format!("must be less than or equal to {MAX_PERIOD_SECONDS}"),
        ));
    }
    errs
}

/// Validate a new `HorizontalPodAutoscaler`. Mirrors upstream
/// `ValidateHorizontalPodAutoscaler`.
pub fn validate_horizontal_pod_autoscaler(hpa: &HorizontalPodAutoscaler) -> ErrorList {
    validate_hpa_spec(&hpa.spec, &Path::new("spec"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::autoscaling::HorizontalPodAutoscalerBehavior;
    use crate::validation::field::ErrorType;
    use serde_json::json;

    fn hpa_spec(spec: serde_json::Value) -> HorizontalPodAutoscalerSpec {
        serde_json::from_value(spec).expect("spec deserializes")
    }

    fn errs_for(spec: serde_json::Value) -> ErrorList {
        validate_hpa_spec(&hpa_spec(spec), &Path::new("spec"))
    }

    fn has(errs: &ErrorList, field: &str, ty: ErrorType) -> bool {
        errs.iter().any(|e| e.field == field && e.error_type == ty)
    }

    fn target() -> serde_json::Value {
        json!({"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"})
    }

    #[test]
    fn valid_resource_metric_passes() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "minReplicas": 1,
            "maxReplicas": 10,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}}
            }]
        }));
        assert!(errs.is_empty(), "unexpected: {errs:?}");
    }

    #[test]
    fn metric_type_required() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{"type": ""}]
        }));
        assert!(
            has(&errs, "spec.metrics[0].type", ErrorType::Required),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_type_invalid_enum_not_supported() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{"type": "Bogus"}]
        }));
        assert!(
            has(&errs, "spec.metrics[0].type", ErrorType::NotSupported),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_source_must_match_type() {
        // type=Pods but only `resource` populated. Upstream: typesPresent={resource}
        // has len 1, so the forbidden loop is skipped — only required(pods) fires.
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Pods",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}}
            }]
        }));
        assert!(
            has(&errs, "spec.metrics[0].pods", ErrorType::Required),
            "got: {errs:?}"
        );
        assert!(
            !has(&errs, "spec.metrics[0].resource", ErrorType::Forbidden),
            "single source present must not be forbidden; got: {errs:?}"
        );
    }

    #[test]
    fn two_sources_forbidden() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}},
                "pods": {"metric": {"name": "qps"}, "target": {"type": "AverageValue", "averageValue": "1"}}
            }]
        }));
        // resource matches type → pods is the extra one and is forbidden.
        assert!(
            has(&errs, "spec.metrics[0].pods", ErrorType::Forbidden),
            "got: {errs:?}"
        );
    }

    #[test]
    fn resource_source_requires_value_or_utilization() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization"}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].resource.target.averageUtilization",
                ErrorType::Required
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_target_type_enum_checked() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Pods",
                "pods": {"metric": {"name": "qps"}, "target": {"type": "Bogus", "averageValue": "1"}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].pods.target.type",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_target_value_must_be_positive() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Object",
                "object": {
                    "describedObject": {"kind": "Service", "name": "svc", "apiVersion": "v1"},
                    "metric": {"name": "requests"},
                    "target": {"type": "Value", "value": "0"}
                }
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].object.target.value",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_target_average_utilization_must_be_at_least_one() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 0}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].resource.target.averageUtilization",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_identifier_name_required() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Pods",
                "pods": {"metric": {"name": ""}, "target": {"type": "AverageValue", "averageValue": "1"}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].pods.metric.name",
                ErrorType::Required
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn metric_identifier_name_path_segment() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5,
            "metrics": [{
                "type": "Pods",
                "pods": {"metric": {"name": "bad/name"}, "target": {"type": "AverageValue", "averageValue": "1"}}
            }]
        }));
        assert!(
            has(
                &errs,
                "spec.metrics[0].pods.metric.name",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn scale_target_ref_kind_path_segment() {
        let errs = errs_for(json!({
            "scaleTargetRef": {"kind": "bad/kind", "name": "web", "apiVersion": "apps/v1"},
            "maxReplicas": 5
        }));
        assert!(
            has(&errs, "spec.scaleTargetRef.kind", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn scale_target_ref_requires_api_group() {
        // apiVersion "v1" → empty group; non-RC kind → must specify API group.
        let errs = errs_for(json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "v1"},
            "maxReplicas": 5
        }));
        assert!(
            has(&errs, "spec.scaleTargetRef.apiVersion", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn replication_controller_allows_empty_api_group() {
        let errs = errs_for(json!({
            "scaleTargetRef": {"kind": "ReplicationController", "name": "web", "apiVersion": "v1"},
            "maxReplicas": 5
        }));
        assert!(
            !has(&errs, "spec.scaleTargetRef.apiVersion", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn malformed_api_version_rejected() {
        let errs = errs_for(json!({
            "scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "a/b/c"},
            "maxReplicas": 5
        }));
        assert!(
            has(&errs, "spec.scaleTargetRef.apiVersion", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn scale_to_zero_requires_object_or_external_metric() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "minReplicas": 0,
            "maxReplicas": 5,
            "metrics": [{
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}}
            }]
        }));
        assert!(
            has(&errs, "spec.metrics", ErrorType::Forbidden),
            "got: {errs:?}"
        );
    }

    #[test]
    fn scale_to_zero_allowed_with_external_metric() {
        let errs = errs_for(json!({
            "scaleTargetRef": target(),
            "minReplicas": 0,
            "maxReplicas": 5,
            "metrics": [{
                "type": "External",
                "external": {"metric": {"name": "queue"}, "target": {"type": "Value", "value": "10"}}
            }]
        }));
        assert!(
            !has(&errs, "spec.metrics", ErrorType::Forbidden),
            "got: {errs:?}"
        );
        // minReplicas==0 with lower bound 1 still triggers the bound error.
        assert!(
            has(&errs, "spec.minReplicas", ErrorType::Invalid),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_select_policy_enum_checked() {
        let mut spec = hpa_spec(json!({
            "scaleTargetRef": target(),
            "maxReplicas": 5
        }));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_up: Some(
                serde_json::from_value(json!({
                    "selectPolicy": "Bogus",
                    "policies": [{"type": "Pods", "value": 1, "periodSeconds": 60}]
                }))
                .unwrap(),
            ),
            scale_down: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(
                &errs,
                "spec.behavior.scaleUp.selectPolicy",
                ErrorType::NotSupported
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_requires_at_least_one_policy() {
        let mut spec = hpa_spec(json!({"scaleTargetRef": target(), "maxReplicas": 5}));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_up: Some(serde_json::from_value(json!({"policies": []})).unwrap()),
            scale_down: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(&errs, "spec.behavior.scaleUp.policies", ErrorType::Required),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_policy_value_and_period_checked() {
        let mut spec = hpa_spec(json!({"scaleTargetRef": target(), "maxReplicas": 5}));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_down: Some(
                serde_json::from_value(json!({
                    "policies": [{"type": "Percent", "value": 0, "periodSeconds": 0}]
                }))
                .unwrap(),
            ),
            scale_up: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(
                &errs,
                "spec.behavior.scaleDown.policies[0].value",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
        assert!(
            has(
                &errs,
                "spec.behavior.scaleDown.policies[0].periodSeconds",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_policy_period_upper_bound() {
        let mut spec = hpa_spec(json!({"scaleTargetRef": target(), "maxReplicas": 5}));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_up: Some(
                serde_json::from_value(json!({
                    "policies": [{"type": "Pods", "value": 4, "periodSeconds": 1801}]
                }))
                .unwrap(),
            ),
            scale_down: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(
                &errs,
                "spec.behavior.scaleUp.policies[0].periodSeconds",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }

    #[test]
    fn behavior_stabilization_window_bounds() {
        let mut spec = hpa_spec(json!({"scaleTargetRef": target(), "maxReplicas": 5}));
        spec.behavior = Some(HorizontalPodAutoscalerBehavior {
            scale_up: Some(
                serde_json::from_value(json!({
                    "stabilizationWindowSeconds": 3601,
                    "policies": [{"type": "Pods", "value": 4, "periodSeconds": 60}]
                }))
                .unwrap(),
            ),
            scale_down: None,
        });
        let errs = validate_hpa_spec(&spec, &Path::new("spec"));
        assert!(
            has(
                &errs,
                "spec.behavior.scaleUp.stabilizationWindowSeconds",
                ErrorType::Invalid
            ),
            "got: {errs:?}"
        );
    }
}
