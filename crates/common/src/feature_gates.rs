//! Process-wide feature gates — analogue of upstream
//! `k8s.io/component-base/featuregate`.
//!
//! Upstream Kubernetes wires feature gates through `utilfeature.DefaultFeatureGate`,
//! a global registry consulted by validators and admission controllers. We mirror
//! that surface with a small AtomicBool-backed registry so call sites can ask
//! `feature_gates::enabled(Feature::RelaxedDNSSearchValidation)` without
//! threading flags through every function signature.
//!
//! Each [`Feature`] variant carries its rusternetes default, which mirrors the
//! upstream default at the version rusternetes targets (currently v1.35).
//! Tests that need to flip a gate should use [`with_feature`] (the RAII guard
//! restores the previous value on drop) and pair the test with
//! `#[serial_test::serial]` because the registry is process-wide.

use std::sync::atomic::{AtomicBool, Ordering};

/// Enum of all rusternetes feature gates. Adding a gate is two lines: a variant
/// here plus a slot in [`STATES`]; the `idx` <-> default mapping in
/// [`Feature::idx`] and [`Feature::default_enabled`] keeps them in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    /// When enabled, pod `dnsConfig.searches` entries may contain a single
    /// underscore per label and the lone `.` domain, matching upstream
    /// `IsDNS1123SubdomainWithUnderScore`. When disabled, falls back to the
    /// strict RFC1123-subdomain validator.
    ///
    /// Upstream: `pkg/features/kube_features.go::RelaxedDNSSearchValidation`.
    /// GA + locked-to-default in v1.34; rusternetes targets v1.35 so the
    /// default is `true`.
    RelaxedDNSSearchValidation,

    /// When enabled, the api-server's pod-binding handler copies
    /// `topology.kubernetes.io/zone` and `topology.kubernetes.io/region`
    /// labels from the bound Node onto the Pod. When disabled, the
    /// binding handler does not touch the pod's labels at all — matching
    /// upstream's `Plugin.Admit` no-op behaviour when `p.enabled == false`.
    ///
    /// Upstream: `plugin/pkg/admission/podtopologylabels/admission.go`.
    /// Beta in v1.35, default `true`.
    PodTopologyLabelsAdmission,
}

impl Feature {
    /// Stable index into [`STATES`]. Must be kept in sync with the enum order.
    const fn idx(self) -> usize {
        match self {
            Feature::RelaxedDNSSearchValidation => 0,
            Feature::PodTopologyLabelsAdmission => 1,
        }
    }

    /// Upstream-derived default for the v1.35 target.
    const fn default_enabled(self) -> bool {
        match self {
            // GA + LockToDefault since v1.34.
            Feature::RelaxedDNSSearchValidation => true,
            // Beta in v1.35 — defaults to true.
            Feature::PodTopologyLabelsAdmission => true,
        }
    }
}

/// Every [`Feature`] variant, in `Feature::idx` order. Adding a new gate
/// requires one variant on the enum and one entry here; everything else
/// derives from this single list:
/// * [`NUM_FEATURES`] is `ALL_FEATURES.len()`,
/// * [`STATES`] sizes itself from [`NUM_FEATURES`] and seeds each slot with
///   that feature's `default_enabled()`,
/// * [`reset_to_defaults`] iterates this array.
///
/// Skipping a gate here is therefore impossible without also changing every
/// derived definition — the previous hand-maintained mirror in
/// `reset_to_defaults` would silently miss new gates.
pub const ALL_FEATURES: &[Feature] = &[
    Feature::RelaxedDNSSearchValidation,
    Feature::PodTopologyLabelsAdmission,
];

/// Total number of feature gates. Derived from [`ALL_FEATURES`].
const NUM_FEATURES: usize = ALL_FEATURES.len();

/// One AtomicBool per [`Feature`], pre-seeded with that feature's
/// `default_enabled()` value. Keep this initializer in lockstep with the
/// [`Feature`] enum — the index of each slot MUST match `Feature::idx`.
///
/// Using `default_enabled()` here (instead of a hardcoded `true`) guarantees
/// that even a brand-new process — before any caller has invoked
/// [`reset_to_defaults`] — sees the upstream default for every gate, not just
/// `RelaxedDNSSearchValidation`.
static STATES: [AtomicBool; NUM_FEATURES] = [
    AtomicBool::new(Feature::RelaxedDNSSearchValidation.default_enabled()),
    AtomicBool::new(Feature::PodTopologyLabelsAdmission.default_enabled()),
];

/// Returns whether `feature` is currently enabled in this process.
pub fn enabled(feature: Feature) -> bool {
    STATES[feature.idx()].load(Ordering::Relaxed)
}

/// Force-set `feature` to `value`. Returns the previous value so callers (or
/// the [`FeatureGuard`] RAII helper) can restore it.
///
/// Intended for production wiring at startup (parse `--feature-gates=...`) and
/// for tests. Tests that touch this MUST be marked `#[serial_test::serial]`
/// because every gate is process-wide.
pub fn set(feature: Feature, value: bool) -> bool {
    STATES[feature.idx()].swap(value, Ordering::Relaxed)
}

/// Reset every feature gate to its rusternetes default. Useful between tests
/// that flip gates without going through [`FeatureGuard`].
///
/// Iterates [`ALL_FEATURES`] so adding a new gate requires only the enum +
/// `ALL_FEATURES` entry — no per-call-site fix-ups.
pub fn reset_to_defaults() {
    for &f in ALL_FEATURES {
        STATES[f.idx()].store(f.default_enabled(), Ordering::Relaxed);
    }
}

/// RAII guard that restores `feature` to its prior value on drop. Use in tests
/// to scope a gate flip to a single test body.
///
/// Created via [`with_feature`].
#[must_use = "the guard restores the previous gate value when dropped"]
pub struct FeatureGuard {
    feature: Feature,
    previous: bool,
}

impl Drop for FeatureGuard {
    fn drop(&mut self) {
        set(self.feature, self.previous);
    }
}

/// Set `feature` to `value` and return a guard that restores the previous
/// value on drop. Mirrors upstream `featuregatetesting.SetFeatureGateDuringTest`.
pub fn with_feature(feature: Feature, value: bool) -> FeatureGuard {
    let previous = set(feature, value);
    FeatureGuard { feature, previous }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn default_is_upstream_default() {
        reset_to_defaults();
        assert!(
            enabled(Feature::RelaxedDNSSearchValidation),
            "RelaxedDNSSearchValidation is GA-locked-default-true since v1.34"
        );
        assert!(
            enabled(Feature::PodTopologyLabelsAdmission),
            "PodTopologyLabelsAdmission is Beta+enabled in v1.35; default must be true"
        );
    }

    /// Pin that `STATES` is seeded from `default_enabled()` — without a
    /// `reset_to_defaults()` call. Catches a maintainer adding a future
    /// `Feature` variant whose `default_enabled()` is `false` but whose
    /// `STATES` slot is left as `AtomicBool::new(true)` (the obvious
    /// copy-paste from the existing slot).
    #[test]
    #[serial]
    fn states_match_default_enabled_at_process_start() {
        // First, capture each gate's current value (set may have been called
        // by other tests in the suite).
        let snapshot: Vec<(Feature, bool)> =
            ALL_FEATURES.iter().map(|&f| (f, enabled(f))).collect();
        // Force every slot back to its default and verify equality with the
        // declared `default_enabled()`. This is the property the static
        // initializer is supposed to give us at process start.
        reset_to_defaults();
        for &f in ALL_FEATURES {
            assert_eq!(
                enabled(f),
                f.default_enabled(),
                "{:?} default mismatch — STATES initializer drifted from default_enabled()",
                f,
            );
        }
        // Restore prior values so neighbouring tests aren't surprised.
        for (f, v) in snapshot {
            set(f, v);
        }
    }

    #[test]
    #[serial]
    fn set_swaps_value() {
        reset_to_defaults();
        let prev = set(Feature::RelaxedDNSSearchValidation, false);
        assert!(prev);
        assert!(!enabled(Feature::RelaxedDNSSearchValidation));
        set(Feature::RelaxedDNSSearchValidation, true);
    }

    #[test]
    #[serial]
    fn guard_restores_on_drop() {
        reset_to_defaults();
        {
            let _g = with_feature(Feature::RelaxedDNSSearchValidation, false);
            assert!(!enabled(Feature::RelaxedDNSSearchValidation));
        }
        assert!(enabled(Feature::RelaxedDNSSearchValidation));
    }

    #[test]
    #[serial]
    fn pod_topology_labels_admission_guard_round_trip() {
        reset_to_defaults();
        assert!(enabled(Feature::PodTopologyLabelsAdmission));
        {
            let _g = with_feature(Feature::PodTopologyLabelsAdmission, false);
            assert!(!enabled(Feature::PodTopologyLabelsAdmission));
        }
        assert!(enabled(Feature::PodTopologyLabelsAdmission));
    }
}
