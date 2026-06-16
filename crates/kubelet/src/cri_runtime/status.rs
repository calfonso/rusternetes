//! Map CRI container status onto rusternetes [`ContainerStatus`].
//!
//! Pure translation in the other direction from [`super::translate`]: the
//! runtime reports a `runtime.v1::ContainerStatus`, and the kubelet's pod-status
//! machinery wants a rusternetes `ContainerStatus`. Readiness/started here are
//! derived purely from the runtime state; the kubelet overlays probe results on
//! top.

use rusternetes_common::resources::pod::{ContainerState, ContainerStatus};
use rusternetes_cri::v1;

/// Convert a unix-nanoseconds timestamp into an RFC3339 string, or `None` for a
/// zero/absent timestamp.
fn nanos_to_rfc3339(nanos: i64) -> Option<String> {
    if nanos == 0 {
        return None;
    }
    let secs = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, sub).map(|dt| dt.to_rfc3339())
}

fn empty_to_none(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Map the CRI runtime state into a rusternetes [`ContainerState`].
fn map_state(cri: &v1::ContainerStatus) -> ContainerState {
    match v1::ContainerState::try_from(cri.state).unwrap_or(v1::ContainerState::ContainerUnknown) {
        v1::ContainerState::ContainerCreated => ContainerState::Waiting {
            reason: Some("ContainerCreating".to_string()),
            message: empty_to_none(&cri.message),
        },
        v1::ContainerState::ContainerRunning => ContainerState::Running {
            started_at: nanos_to_rfc3339(cri.started_at),
        },
        v1::ContainerState::ContainerExited => ContainerState::Terminated {
            exit_code: cri.exit_code,
            signal: None,
            reason: empty_to_none(&cri.reason),
            message: empty_to_none(&cri.message),
            started_at: nanos_to_rfc3339(cri.started_at),
            finished_at: nanos_to_rfc3339(cri.finished_at),
            container_id: empty_to_none(&cri.id),
        },
        v1::ContainerState::ContainerUnknown => ContainerState::Waiting {
            reason: Some("Unknown".to_string()),
            message: empty_to_none(&cri.message),
        },
    }
}

/// Translate a CRI [`ContainerStatus`](v1::ContainerStatus) into a rusternetes
/// one. `ready`/`started` reflect the runtime RUNNING state only; the kubelet
/// applies probe results afterwards.
pub fn map_container_status(cri: &v1::ContainerStatus) -> ContainerStatus {
    let running = cri.state == v1::ContainerState::ContainerRunning as i32;
    let name = cri
        .metadata
        .as_ref()
        .map(|m| m.name.clone())
        .unwrap_or_default();
    let restart_count = cri.metadata.as_ref().map(|m| m.attempt).unwrap_or(0);

    ContainerStatus {
        name,
        ready: running,
        restart_count,
        state: Some(map_state(cri)),
        last_state: None,
        image: cri.image.as_ref().map(|i| i.image.clone()),
        image_id: empty_to_none(&cri.image_ref),
        container_id: empty_to_none(&cri.id),
        started: Some(running),
        allocated_resources: None,
        allocated_resources_status: None,
        resources: None,
        user: None,
        volume_mounts: None,
        stop_signal: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cri_status(state: v1::ContainerState) -> v1::ContainerStatus {
        v1::ContainerStatus {
            id: "ctr-abc".to_string(),
            metadata: Some(v1::ContainerMetadata {
                name: "app".to_string(),
                attempt: 2,
            }),
            state: state as i32,
            exit_code: 0,
            ..Default::default()
        }
    }

    #[test]
    fn running_maps_to_ready_running() {
        let s = map_container_status(&cri_status(v1::ContainerState::ContainerRunning));
        assert_eq!(s.name, "app");
        assert!(s.ready);
        assert_eq!(s.restart_count, 2);
        assert_eq!(s.container_id.as_deref(), Some("ctr-abc"));
        assert!(matches!(s.state, Some(ContainerState::Running { .. })));
    }

    #[test]
    fn created_maps_to_waiting_creating() {
        let s = map_container_status(&cri_status(v1::ContainerState::ContainerCreated));
        assert!(!s.ready);
        match s.state {
            Some(ContainerState::Waiting { reason, .. }) => {
                assert_eq!(reason.as_deref(), Some("ContainerCreating"));
            }
            other => panic!("expected Waiting, got {other:?}"),
        }
    }

    #[test]
    fn exited_maps_to_terminated_with_exit_code() {
        let mut cri = cri_status(v1::ContainerState::ContainerExited);
        cri.exit_code = 137;
        cri.finished_at = 1_700_000_000_000_000_000;
        let s = map_container_status(&cri);
        assert!(!s.ready);
        match s.state {
            Some(ContainerState::Terminated {
                exit_code,
                finished_at,
                ..
            }) => {
                assert_eq!(exit_code, 137);
                assert!(finished_at.is_some(), "finished_at should be set");
            }
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    #[test]
    fn zero_timestamp_is_none() {
        assert_eq!(nanos_to_rfc3339(0), None);
        assert!(nanos_to_rfc3339(1_700_000_000_000_000_000).is_some());
    }
}
