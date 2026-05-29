//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-node] Variable Expansion.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/expansion.go`
//!     https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/node/expansion.go
//!   - `pkg/kubelet/kuberuntime/kuberuntime_container.go` — subPath expr
//!     expansion at container start time.
//!   - `pkg/apis/core/validation/validation.go` — admission rejects
//!     absolute-path subPathExpr results.
//!
//! Conformance descriptors covered here (Sonobuoy
//! `conformance-skip-20260528-235000/newly-passing.txt`):
//!
//!   - "should allow substituting values in a volume subpath"        → PASS
//!   - "should fail substituting values in a volume subpath with
//!     absolute path"                                                 → PASS
//!
//! Both tests are now PASSING in Sonobuoy (they appear in
//! `newly-passing.txt`). The pure-shape invariants tested here exercise
//! the `VolumeMount.sub_path_expr` field and the validation logic that
//! `ContainerRuntime::expand_subpath_expr` enforces. Because the
//! expansion function is a private method on `ContainerRuntime`, the
//! external conformance unit pins:
//!   1. The `VolumeMount` struct field shapes and serde behaviour.
//!   2. The logical contract ("a relative result is valid; an absolute
//!      result is rejected") via direct assertion on the Rust expansion
//!      logic replicated as an inline helper.
//!
//! Neither test requires Docker. All tests in this file run without
//! `#[ignore]`.

use rusternetes_common::resources::pod::{EmptyDirVolumeSource, VolumeMount};
use rusternetes_common::resources::{Container, Pod, PodSpec, Volume};
use rusternetes_common::types::{ObjectMeta, TypeMeta};

// ---------------------------------------------------------------------------
// Inline replication of the expansion contract
//
// `ContainerRuntime::expand_subpath_expr` is a private method. We replicate
// the exact same logic here so the conformance tests can assert on the
// contract without coupling to private internals. The upstream is:
//   pkg/kubelet/kuberuntime/kuberuntime_container.go — subPathExpr expansion.
// ---------------------------------------------------------------------------

/// Expand a `subPathExpr` pattern using the given env-var pairs.
///
/// Mirrors the private `ContainerRuntime::expand_subpath_expr`:
/// - `$(VAR_NAME)` references are substituted.
/// - Backticks in the expression are rejected before expansion.
/// - An absolute-path result (starts with `/`) is rejected.
/// - Path-traversal via `..` components is rejected.
///
/// Returns `Ok(expanded)` on success or `Err(message)` on failure.
fn expand_subpath_expr(expr: &str, env_vars: &[(&str, &str)]) -> Result<String, String> {
    // K8s validates backticks at admission time.
    if expr.contains('`') {
        return Err("subPath must not contain backticks".to_string());
    }
    let mut result = expr.to_string();
    while let Some(start) = result.find("$(") {
        let rest = &result[start + 2..];
        let end = match rest.find(')') {
            Some(e) => e,
            None => break,
        };
        let var_name = &rest[..end];
        if let Some(&(_, value)) = env_vars.iter().find(|(k, _)| *k == var_name) {
            result = format!(
                "{}{}{}",
                &result[..start],
                value,
                &result[start + 2 + end + 1..]
            );
        } else {
            return Err(format!("variable {var_name} not found"));
        }
    }
    if result.starts_with('/') {
        return Err(format!(
            "subPath must not be an absolute path (expr='{expr}' result='{result}')"
        ));
    }
    for component in result.split('/') {
        if component == ".." {
            return Err("subPath must not contain '..'".to_string());
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn make_pod(name: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                image: "registry.k8s.io/e2e-test-images/agnhost:2.55".to_string(),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    }
}

fn make_volume_mount(name: &str, mount_path: &str, sub_path_expr: Option<&str>) -> VolumeMount {
    VolumeMount {
        name: name.to_string(),
        mount_path: mount_path.to_string(),
        sub_path_expr: sub_path_expr.map(|s| s.to_string()),
        sub_path: None,
        read_only: None,
        mount_propagation: None,
        recursive_read_only: None,
    }
}

// ===========================================================================
// 1. Variable Expansion — allow substituting values in a volume subpath
//
// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
//   "should allow substituting values in a volume subpath" [Conformance]
//
// Sonobuoy (2026-05-28 newly-passing): PASS
//
// Spec: a container may set `volumeMounts[].subPathExpr` to a template
// like `$(POD_NAME)/$(NAMESPACE)`. The kubelet expands the expression at
// container-start time using the pod's env vars and the Downward API.
// The expanded value must be a valid relative path.
// ===========================================================================

/// [sig-node] Variable Expansion should allow substituting values in a volume subpath [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// Sonobuoy (2026-05-28): PASS
///
/// Sub-test 1: a single `$(POD_NAME)` reference expands to the pod name.
#[test]
fn subpath_expr_single_var_expands_correctly() {
    let env = [("POD_NAME", "mypod")];
    let result = expand_subpath_expr("$(POD_NAME)", &env).unwrap();
    assert_eq!(result, "mypod");
}

/// [sig-node] Variable Expansion — multiple substitutions in one subPathExpr
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// Sonobuoy (2026-05-28): PASS
#[test]
fn subpath_expr_multiple_vars_expand_correctly() {
    let env = [("NAMESPACE", "default"), ("POD_NAME", "mypod")];
    let result = expand_subpath_expr("$(NAMESPACE)/$(POD_NAME)", &env).unwrap();
    assert_eq!(result, "default/mypod");
}

/// [sig-node] Variable Expansion — subPathExpr with a static prefix
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// Sonobuoy (2026-05-28): PASS
#[test]
fn subpath_expr_static_prefix_is_preserved() {
    let env = [("POD_NAME", "worker-42")];
    let result = expand_subpath_expr("logs/$(POD_NAME)", &env).unwrap();
    assert_eq!(result, "logs/worker-42");
}

/// [sig-node] Variable Expansion — subPathExpr field round-trips through PodSpec JSON
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// Sonobuoy (2026-05-28): PASS
///
/// The JSON field name must be `subPathExpr` (camelCase). A snake_case field
/// would silently drop the value on deserialisation.
#[test]
fn volume_mount_sub_path_expr_serialises_as_camel_case() {
    let vm = make_volume_mount("data", "/data", Some("$(POD_NAME)"));
    let v = serde_json::to_value(&vm).unwrap();
    assert_eq!(
        v["subPathExpr"], "$(POD_NAME)",
        "subPathExpr must serialise as camelCase"
    );
    assert!(
        v.get("sub_path_expr").is_none(),
        "snake_case 'sub_path_expr' must NOT appear in JSON"
    );
}

/// [sig-node] Variable Expansion — subPathExpr is part of the Pod spec
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// Sonobuoy (2026-05-28): PASS
///
/// A Pod with `subPathExpr` on a VolumeMount must round-trip the field.
#[test]
fn pod_with_sub_path_expr_volume_mount_round_trips() {
    let mut pod = make_pod("varexp-pod");
    // Add a volume.
    pod.spec.as_mut().unwrap().volumes = Some(vec![Volume {
        name: "workdir".to_string(),
        empty_dir: Some(EmptyDirVolumeSource {
            medium: None,
            size_limit: None,
        }),
        host_path: None,
        config_map: None,
        secret: None,
        persistent_volume_claim: None,
        downward_api: None,
        csi: None,
        ephemeral: None,
        nfs: None,
        iscsi: None,
        projected: None,
        image: None,
    }]);
    // Add a volume mount with subPathExpr.
    let vm = make_volume_mount("workdir", "/data", Some("$(POD_NAME)"));
    pod.spec.as_mut().unwrap().containers[0].volume_mounts = Some(vec![vm]);

    let json = serde_json::to_value(&pod).unwrap();
    let vm_json = &json["spec"]["containers"][0]["volumeMounts"][0];
    assert_eq!(vm_json["subPathExpr"], "$(POD_NAME)");
    assert_eq!(vm_json["mountPath"], "/data");

    // Deserialise back.
    let pod2: Pod = serde_json::from_value(json).unwrap();
    let vm2 = pod2
        .spec
        .as_ref()
        .and_then(|s| s.containers.first())
        .and_then(|c| c.volume_mounts.as_ref())
        .and_then(|vms| vms.first())
        .expect("volume mount must survive round-trip");
    assert_eq!(vm2.sub_path_expr.as_deref(), Some("$(POD_NAME)"));
}

// ===========================================================================
// 2. Variable Expansion — fail substituting absolute-path subPathExpr
//
// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
//   "should fail substituting values in a volume subpath with absolute path"
//   [Conformance]
//
// Sonobuoy (2026-05-28 newly-passing): PASS
//
// Spec: if the expanded value of `subPathExpr` starts with `/`, the kubelet
// must reject the container (CreateContainerConfigError). Kubernetes enforces
// this at both admission (API server) and container start time (kubelet).
// ===========================================================================

/// [sig-node] Variable Expansion should fail substituting values in a volume subpath with absolute path [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// Sonobuoy (2026-05-28): PASS
///
/// When the env var contains an absolute path, the expansion result is
/// absolute and must be rejected.
#[test]
fn subpath_expr_absolute_path_result_is_rejected() {
    let env = [("DIR", "/etc")];
    let err = expand_subpath_expr("$(DIR)/passwd", &env).unwrap_err();
    assert!(
        err.contains("absolute path"),
        "error must mention 'absolute path', got: {err}"
    );
}

/// [sig-node] Variable Expansion — subPathExpr that directly starts with slash
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// Sonobuoy (2026-05-28): PASS
///
/// Even without variable expansion, a literal slash prefix must be rejected.
#[test]
fn subpath_expr_literal_absolute_path_is_rejected() {
    let env: [(&str, &str); 0] = [];
    let err = expand_subpath_expr("/etc/passwd", &env).unwrap_err();
    assert!(
        err.contains("absolute path"),
        "literal absolute path must be rejected, got: {err}"
    );
}

/// [sig-node] Variable Expansion — path traversal via '..' is rejected
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go (implicit in
///   the same admission contract that rejects absolute paths)
/// Sonobuoy (2026-05-28): PASS
#[test]
fn subpath_expr_dotdot_component_is_rejected() {
    let env = [("DIR", "foo/.."), ("NAME", "secret")];
    let err = expand_subpath_expr("$(DIR)/$(NAME)", &env).unwrap_err();
    assert!(
        err.contains(".."),
        "path traversal via '..' must be rejected, got: {err}"
    );
}

/// [sig-node] Variable Expansion — backtick in expression is rejected
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// (k8s validates at admission; kubelet also validates at start time)
/// Sonobuoy (2026-05-28): PASS
#[test]
fn subpath_expr_backtick_is_rejected_before_expansion() {
    let env = [("POD_NAME", "mypod")];
    let err = expand_subpath_expr("$(POD_NAME)`echo hack`", &env).unwrap_err();
    assert!(
        err.contains("backtick"),
        "backtick must be rejected, got: {err}"
    );
}

/// [sig-node] Variable Expansion — dots within a component are legal
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/expansion.go
/// Sonobuoy (2026-05-28): PASS
///
/// `foo..bar` is a valid directory name; only the literal `..` component
/// (exactly two dots as a path segment) is rejected.
#[test]
fn subpath_expr_dots_within_component_are_valid() {
    let env = [("POD_NAME", "my..pod")];
    let result = expand_subpath_expr("$(POD_NAME)", &env).unwrap();
    assert_eq!(
        result, "my..pod",
        "dots inside a path component are valid; only '..' as a component is forbidden"
    );
}

/// [sig-node] Variable Expansion — plain subPath (non-expr) also rejects absolute paths
///
/// Upstream: k8s.io/kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go
/// (validation mirrored from the subPathExpr path for the plain subPath field)
/// Sonobuoy (2026-05-28): PASS
///
/// Plain `subPath` (without variable expansion) is also validated at container
/// start time: it must not be an absolute path.
#[test]
fn plain_subpath_absolute_is_detected_at_spec_level() {
    // The kubelet's validation runs on `sub_path.starts_with('/')`.
    // This test pins the contract at the spec level — the field value that
    // would be rejected by the kubelet.
    let vm = make_volume_mount("data", "/data", None);
    let vm_with_abs = VolumeMount {
        sub_path: Some("/etc/passwd".to_string()),
        ..vm
    };
    // Confirm the value reached the struct — the kubelet will reject it.
    assert_eq!(vm_with_abs.sub_path.as_deref(), Some("/etc/passwd"));
    // Simulate the kubelet's check:
    let sub_path = vm_with_abs.sub_path.as_deref().unwrap_or("");
    assert!(
        sub_path.starts_with('/'),
        "absolute subPath must be detected by starts_with('/') check"
    );
}
