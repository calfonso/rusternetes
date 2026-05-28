// JSON roundtrip tests for batch/v1, autoscaling/v1, autoscaling/v2, and
// policy/v1 resources.
//
// These tests mirror the roundtrip serialization layer that upstream
// kube-apiserver runs against every external API type (k8s.io/api/<group>/v1).
// For each fixture we assert:
//
//   1. decode succeeds:   T::deserialize(fixture)
//   2. re-encode succeeds: serde_json::to_string(&decoded)
//   3. re-decode succeeds: T::deserialize(re_encoded)
//   4. roundtrip stable:   serde_json::Value(decoded) == Value(re_decoded)
//
// We compare via `serde_json::Value` because the resource structs do not
// implement `PartialEq` (mirroring upstream Go, where equality is checked at
// the wire level rather than struct level).
//
// The fixtures intentionally cover both the autoscaling/v1 (simple CPU-target)
// HPA shape and the autoscaling/v2 (multi-metric + behavior) HPA shape, which
// share the same Rust struct in this codebase but have very different JSON.

use rusternetes_common::resources::{
    CronJob, HorizontalPodAutoscaler, IntOrString, Job, PodDisruptionBudget,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Runs the four-step roundtrip check for a single fixture and returns the
/// decoded value so callers can perform additional field-level assertions.
fn assert_roundtrip<T>(fixture: &str) -> T
where
    T: DeserializeOwned + Serialize,
{
    let decoded: T = serde_json::from_str(fixture)
        .unwrap_or_else(|e| panic!("step 1 (decode) failed: {e}\nfixture: {fixture}"));

    let re_encoded =
        serde_json::to_string(&decoded).unwrap_or_else(|e| panic!("step 2 (encode) failed: {e}"));

    let re_decoded: T = serde_json::from_str(&re_encoded)
        .unwrap_or_else(|e| panic!("step 3 (re-decode) failed: {e}\nre-encoded: {re_encoded}"));

    let lhs: serde_json::Value =
        serde_json::to_value(&decoded).expect("decoded -> Value must succeed");
    let rhs: serde_json::Value =
        serde_json::to_value(&re_decoded).expect("re_decoded -> Value must succeed");
    assert_eq!(
        lhs, rhs,
        "step 4 (roundtrip stability) failed\nlhs: {lhs}\nrhs: {rhs}"
    );

    decoded
}

// ---------------------------------------------------------------------------
// batch/v1 — Job
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_job_minimal_spec() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "Job",
      "metadata": {"name": "pi", "namespace": "default"},
      "spec": {
        "template": {
          "spec": {
            "containers": [{"name": "pi", "image": "perl:5.34"}],
            "restartPolicy": "Never"
          }
        }
      }
    }"#;
    let job: Job = assert_roundtrip(fixture);
    assert_eq!(job.metadata.name, "pi");
    assert_eq!(job.type_meta.api_version, "batch/v1");
    assert_eq!(job.spec.template.spec.containers.len(), 1);
}

#[test]
fn roundtrip_job_with_parallelism_and_completions() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "Job",
      "metadata": {"name": "parallel", "namespace": "batch"},
      "spec": {
        "parallelism": 4,
        "completions": 8,
        "backoffLimit": 6,
        "activeDeadlineSeconds": 300,
        "template": {
          "spec": {
            "containers": [{"name": "worker", "image": "busybox:1.36"}],
            "restartPolicy": "OnFailure"
          }
        }
      }
    }"#;
    let job: Job = assert_roundtrip(fixture);
    assert_eq!(job.spec.parallelism, Some(4));
    assert_eq!(job.spec.completions, Some(8));
    assert_eq!(job.spec.backoff_limit, Some(6));
    assert_eq!(job.spec.active_deadline_seconds, Some(300));
}

#[test]
fn roundtrip_job_indexed_completion_mode() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "Job",
      "metadata": {"name": "indexed", "namespace": "default"},
      "spec": {
        "completions": 10,
        "parallelism": 5,
        "completionMode": "Indexed",
        "backoffLimitPerIndex": 2,
        "maxFailedIndexes": 3,
        "template": {
          "spec": {
            "containers": [{"name": "shard", "image": "alpine:3.20"}],
            "restartPolicy": "Never"
          }
        }
      }
    }"#;
    let job: Job = assert_roundtrip(fixture);
    assert_eq!(job.spec.completion_mode.as_deref(), Some("Indexed"));
    assert_eq!(job.spec.backoff_limit_per_index, Some(2));
    assert_eq!(job.spec.max_failed_indexes, Some(3));
}

#[test]
fn roundtrip_job_with_selector_and_manual_selector() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "Job",
      "metadata": {"name": "manual-sel", "namespace": "default"},
      "spec": {
        "manualSelector": true,
        "selector": {
          "matchLabels": {"controller-uid": "abc-123"}
        },
        "template": {
          "metadata": {"labels": {"controller-uid": "abc-123"}},
          "spec": {
            "containers": [{"name": "x", "image": "busybox"}],
            "restartPolicy": "Never"
          }
        }
      }
    }"#;
    let job: Job = assert_roundtrip(fixture);
    assert_eq!(job.spec.manual_selector, Some(true));
    assert!(job.spec.selector.is_some());
}

#[test]
fn roundtrip_job_suspended_with_ttl() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "Job",
      "metadata": {"name": "suspended", "namespace": "default"},
      "spec": {
        "suspend": true,
        "ttlSecondsAfterFinished": 600,
        "podReplacementPolicy": "Failed",
        "template": {
          "spec": {
            "containers": [{"name": "x", "image": "busybox"}],
            "restartPolicy": "Never"
          }
        }
      }
    }"#;
    let job: Job = assert_roundtrip(fixture);
    assert_eq!(job.spec.suspend, Some(true));
    assert_eq!(job.spec.ttl_seconds_after_finished, Some(600));
    assert_eq!(job.spec.pod_replacement_policy.as_deref(), Some("Failed"));
}

#[test]
fn roundtrip_job_with_status() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "Job",
      "metadata": {"name": "running", "namespace": "default"},
      "spec": {
        "template": {
          "spec": {
            "containers": [{"name": "x", "image": "busybox"}],
            "restartPolicy": "Never"
          }
        }
      },
      "status": {
        "active": 2,
        "succeeded": 3,
        "failed": 1,
        "ready": 2,
        "terminating": 0,
        "startTime": "2024-01-01T00:00:00Z",
        "conditions": [
          {"type": "Complete", "status": "False"}
        ]
      }
    }"#;
    let job: Job = assert_roundtrip(fixture);
    let status = job.status.expect("status present");
    assert_eq!(status.active, Some(2));
    assert_eq!(status.succeeded, Some(3));
    assert_eq!(status.failed, Some(1));
}

// ---------------------------------------------------------------------------
// batch/v1 — CronJob
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_cronjob_minimal() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "CronJob",
      "metadata": {"name": "hello", "namespace": "default"},
      "spec": {
        "schedule": "* * * * *",
        "jobTemplate": {
          "spec": {
            "template": {
              "spec": {
                "containers": [{"name": "hello", "image": "busybox"}],
                "restartPolicy": "OnFailure"
              }
            }
          }
        }
      }
    }"#;
    let cj: CronJob = assert_roundtrip(fixture);
    assert_eq!(cj.spec.schedule, "* * * * *");
    assert_eq!(cj.type_meta.api_version, "batch/v1");
}

#[test]
fn roundtrip_cronjob_with_history_and_concurrency() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "CronJob",
      "metadata": {"name": "every-hour", "namespace": "ops"},
      "spec": {
        "schedule": "0 * * * *",
        "concurrencyPolicy": "Forbid",
        "suspend": false,
        "successfulJobsHistoryLimit": 3,
        "failedJobsHistoryLimit": 1,
        "startingDeadlineSeconds": 200,
        "jobTemplate": {
          "spec": {
            "template": {
              "spec": {
                "containers": [{"name": "task", "image": "alpine"}],
                "restartPolicy": "OnFailure"
              }
            }
          }
        }
      }
    }"#;
    let cj: CronJob = assert_roundtrip(fixture);
    assert_eq!(cj.spec.concurrency_policy.as_deref(), Some("Forbid"));
    assert_eq!(cj.spec.successful_jobs_history_limit, Some(3));
    assert_eq!(cj.spec.failed_jobs_history_limit, Some(1));
    assert_eq!(cj.spec.starting_deadline_seconds, Some(200));
}

#[test]
fn roundtrip_cronjob_with_timezone() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "CronJob",
      "metadata": {"name": "ny-tz", "namespace": "default"},
      "spec": {
        "schedule": "0 9 * * 1-5",
        "timeZone": "America/New_York",
        "concurrencyPolicy": "Replace",
        "jobTemplate": {
          "spec": {
            "template": {
              "spec": {
                "containers": [{"name": "x", "image": "busybox"}],
                "restartPolicy": "Never"
              }
            }
          }
        }
      }
    }"#;
    let cj: CronJob = assert_roundtrip(fixture);
    assert_eq!(cj.spec.time_zone.as_deref(), Some("America/New_York"));
    assert_eq!(cj.spec.concurrency_policy.as_deref(), Some("Replace"));
}

#[test]
fn roundtrip_cronjob_suspended() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "CronJob",
      "metadata": {"name": "paused", "namespace": "default"},
      "spec": {
        "schedule": "@daily",
        "suspend": true,
        "jobTemplate": {
          "spec": {
            "template": {
              "spec": {
                "containers": [{"name": "x", "image": "busybox"}],
                "restartPolicy": "Never"
              }
            }
          }
        }
      }
    }"#;
    let cj: CronJob = assert_roundtrip(fixture);
    assert_eq!(cj.spec.suspend, Some(true));
}

#[test]
fn roundtrip_cronjob_with_status_active_and_times() {
    let fixture = r#"{
      "apiVersion": "batch/v1",
      "kind": "CronJob",
      "metadata": {"name": "with-status", "namespace": "default"},
      "spec": {
        "schedule": "*/5 * * * *",
        "jobTemplate": {
          "spec": {
            "template": {
              "spec": {
                "containers": [{"name": "x", "image": "busybox"}],
                "restartPolicy": "Never"
              }
            }
          }
        }
      },
      "status": {
        "active": [
          {"kind": "Job", "name": "with-status-12345", "namespace": "default", "uid": "deadbeef"}
        ],
        "lastScheduleTime": "2024-06-01T12:00:00Z",
        "lastSuccessfulTime": "2024-06-01T11:55:00Z"
      }
    }"#;
    let cj: CronJob = assert_roundtrip(fixture);
    let status = cj.status.expect("status present");
    assert_eq!(status.active.len(), 1);
    assert!(status.last_schedule_time.is_some());
    assert!(status.last_successful_time.is_some());
}

// ---------------------------------------------------------------------------
// autoscaling/v1 — HorizontalPodAutoscaler (simple CPU target)
//
// One struct backs both autoscaling/v1 and autoscaling/v2 in this codebase, so
// the v1-specific `targetCPUUtilizationPercentage` field is silently dropped
// by serde. These fixtures verify that the fields the struct *does* model
// (scaleTargetRef + min/max + apiVersion) round-trip stably from a v1 payload.
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_hpa_v1_simple_cpu_target() {
    let fixture = r#"{
      "apiVersion": "autoscaling/v1",
      "kind": "HorizontalPodAutoscaler",
      "metadata": {"name": "frontend", "namespace": "default"},
      "spec": {
        "scaleTargetRef": {
          "kind": "Deployment",
          "name": "frontend",
          "apiVersion": "apps/v1"
        },
        "minReplicas": 1,
        "maxReplicas": 10
      }
    }"#;
    let hpa: HorizontalPodAutoscaler = assert_roundtrip(fixture);
    assert_eq!(hpa.type_meta.api_version, "autoscaling/v1");
    assert_eq!(hpa.spec.scale_target_ref.kind, "Deployment");
    assert_eq!(hpa.spec.min_replicas, Some(1));
    assert_eq!(hpa.spec.max_replicas, 10);
}

#[test]
fn roundtrip_hpa_v1_no_min_replicas() {
    let fixture = r#"{
      "apiVersion": "autoscaling/v1",
      "kind": "HorizontalPodAutoscaler",
      "metadata": {"name": "no-min", "namespace": "default"},
      "spec": {
        "scaleTargetRef": {"kind": "Deployment", "name": "no-min"},
        "maxReplicas": 5
      }
    }"#;
    let hpa: HorizontalPodAutoscaler = assert_roundtrip(fixture);
    assert_eq!(hpa.spec.min_replicas, None);
    assert_eq!(hpa.spec.max_replicas, 5);
    assert!(hpa.spec.scale_target_ref.api_version.is_none());
}

// ---------------------------------------------------------------------------
// autoscaling/v2 — HorizontalPodAutoscaler (multi-metric + behavior)
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_hpa_v2_resource_metric_cpu_utilization() {
    let fixture = r#"{
      "apiVersion": "autoscaling/v2",
      "kind": "HorizontalPodAutoscaler",
      "metadata": {"name": "cpu-hpa", "namespace": "default"},
      "spec": {
        "scaleTargetRef": {
          "kind": "Deployment",
          "name": "web",
          "apiVersion": "apps/v1"
        },
        "minReplicas": 2,
        "maxReplicas": 20,
        "metrics": [
          {
            "type": "Resource",
            "resource": {
              "name": "cpu",
              "target": {"type": "Utilization", "averageUtilization": 70}
            }
          }
        ]
      }
    }"#;
    let hpa: HorizontalPodAutoscaler = assert_roundtrip(fixture);
    assert_eq!(hpa.type_meta.api_version, "autoscaling/v2");
    let metrics = hpa.spec.metrics.expect("metrics present");
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].metric_type, "Resource");
}

#[test]
fn roundtrip_hpa_v2_multi_metric_pods_and_external() {
    let fixture = r#"{
      "apiVersion": "autoscaling/v2",
      "kind": "HorizontalPodAutoscaler",
      "metadata": {"name": "multi", "namespace": "default"},
      "spec": {
        "scaleTargetRef": {"kind": "Deployment", "name": "multi", "apiVersion": "apps/v1"},
        "minReplicas": 1,
        "maxReplicas": 50,
        "metrics": [
          {
            "type": "Pods",
            "pods": {
              "metric": {"name": "packets-per-second"},
              "target": {"type": "AverageValue", "averageValue": "1k"}
            }
          },
          {
            "type": "External",
            "external": {
              "metric": {
                "name": "queue_messages_ready",
                "selector": {"matchLabels": {"queue": "worker_tasks"}}
              },
              "target": {"type": "AverageValue", "averageValue": "30"}
            }
          }
        ]
      }
    }"#;
    let hpa: HorizontalPodAutoscaler = assert_roundtrip(fixture);
    let metrics = hpa.spec.metrics.expect("metrics present");
    assert_eq!(metrics.len(), 2);
    assert_eq!(metrics[0].metric_type, "Pods");
    assert_eq!(metrics[1].metric_type, "External");
}

#[test]
fn roundtrip_hpa_v2_object_metric() {
    let fixture = r#"{
      "apiVersion": "autoscaling/v2",
      "kind": "HorizontalPodAutoscaler",
      "metadata": {"name": "object", "namespace": "default"},
      "spec": {
        "scaleTargetRef": {"kind": "Deployment", "name": "ingress", "apiVersion": "apps/v1"},
        "maxReplicas": 10,
        "metrics": [
          {
            "type": "Object",
            "object": {
              "describedObject": {
                "kind": "Ingress",
                "name": "main-route",
                "apiVersion": "networking.k8s.io/v1"
              },
              "metric": {"name": "requests-per-second"},
              "target": {"type": "Value", "value": "10k"}
            }
          }
        ]
      }
    }"#;
    let hpa: HorizontalPodAutoscaler = assert_roundtrip(fixture);
    let metrics = hpa.spec.metrics.expect("metrics present");
    assert_eq!(metrics[0].metric_type, "Object");
    let object = metrics[0].object.as_ref().expect("object metric present");
    assert_eq!(object.described_object.kind, "Ingress");
}

#[test]
fn roundtrip_hpa_v2_container_resource_metric() {
    let fixture = r#"{
      "apiVersion": "autoscaling/v2",
      "kind": "HorizontalPodAutoscaler",
      "metadata": {"name": "container", "namespace": "default"},
      "spec": {
        "scaleTargetRef": {"kind": "Deployment", "name": "container", "apiVersion": "apps/v1"},
        "minReplicas": 1,
        "maxReplicas": 5,
        "metrics": [
          {
            "type": "ContainerResource",
            "containerResource": {
              "name": "memory",
              "container": "app",
              "target": {"type": "Utilization", "averageUtilization": 80}
            }
          }
        ]
      }
    }"#;
    let hpa: HorizontalPodAutoscaler = assert_roundtrip(fixture);
    let metrics = hpa.spec.metrics.expect("metrics present");
    assert_eq!(metrics[0].metric_type, "ContainerResource");
    let cr = metrics[0]
        .container_resource
        .as_ref()
        .expect("containerResource present");
    assert_eq!(cr.container, "app");
    assert_eq!(cr.name, "memory");
}

#[test]
fn roundtrip_hpa_v2_with_behavior() {
    let fixture = r#"{
      "apiVersion": "autoscaling/v2",
      "kind": "HorizontalPodAutoscaler",
      "metadata": {"name": "behavior", "namespace": "default"},
      "spec": {
        "scaleTargetRef": {"kind": "Deployment", "name": "behavior", "apiVersion": "apps/v1"},
        "minReplicas": 1,
        "maxReplicas": 100,
        "metrics": [
          {
            "type": "Resource",
            "resource": {
              "name": "cpu",
              "target": {"type": "Utilization", "averageUtilization": 50}
            }
          }
        ],
        "behavior": {
          "scaleDown": {
            "stabilizationWindowSeconds": 300,
            "selectPolicy": "Min",
            "policies": [
              {"type": "Pods", "value": 4, "periodSeconds": 60},
              {"type": "Percent", "value": 10, "periodSeconds": 60}
            ]
          },
          "scaleUp": {
            "stabilizationWindowSeconds": 0,
            "selectPolicy": "Max",
            "policies": [
              {"type": "Percent", "value": 100, "periodSeconds": 15}
            ]
          }
        }
      }
    }"#;
    let hpa: HorizontalPodAutoscaler = assert_roundtrip(fixture);
    let behavior = hpa.spec.behavior.expect("behavior present");
    let scale_down = behavior.scale_down.expect("scaleDown present");
    assert_eq!(scale_down.stabilization_window_seconds, Some(300));
    assert_eq!(scale_down.select_policy.as_deref(), Some("Min"));
    let policies = scale_down.policies.expect("policies present");
    assert_eq!(policies.len(), 2);
    let scale_up = behavior.scale_up.expect("scaleUp present");
    assert_eq!(scale_up.select_policy.as_deref(), Some("Max"));
}

#[test]
fn roundtrip_hpa_v2_with_status_and_conditions() {
    let fixture = r#"{
      "apiVersion": "autoscaling/v2",
      "kind": "HorizontalPodAutoscaler",
      "metadata": {"name": "status", "namespace": "default"},
      "spec": {
        "scaleTargetRef": {"kind": "Deployment", "name": "status", "apiVersion": "apps/v1"},
        "minReplicas": 1,
        "maxReplicas": 10,
        "metrics": [
          {
            "type": "Resource",
            "resource": {
              "name": "cpu",
              "target": {"type": "Utilization", "averageUtilization": 70}
            }
          }
        ]
      },
      "status": {
        "observedGeneration": 1,
        "lastScaleTime": "2024-06-01T00:00:00Z",
        "currentReplicas": 3,
        "desiredReplicas": 5,
        "currentMetrics": [
          {
            "type": "Resource",
            "resource": {
              "name": "cpu",
              "current": {"averageUtilization": 65, "averageValue": "650m"}
            }
          }
        ],
        "conditions": [
          {
            "type": "AbleToScale",
            "status": "True",
            "lastTransitionTime": "2024-06-01T00:00:00Z",
            "reason": "ReadyForNewScale",
            "message": "recommended size matches current size"
          }
        ]
      }
    }"#;
    let hpa: HorizontalPodAutoscaler = assert_roundtrip(fixture);
    let status = hpa.status.expect("status present");
    assert_eq!(status.current_replicas, 3);
    assert_eq!(status.desired_replicas, 5);
    assert_eq!(status.observed_generation, Some(1));
    let conditions = status.conditions.expect("conditions present");
    assert_eq!(conditions.len(), 1);
    assert_eq!(conditions[0].condition_type, "AbleToScale");
}

// ---------------------------------------------------------------------------
// policy/v1 — PodDisruptionBudget
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_pdb_min_available_int() {
    let fixture = r#"{
      "apiVersion": "policy/v1",
      "kind": "PodDisruptionBudget",
      "metadata": {"name": "web-pdb", "namespace": "default"},
      "spec": {
        "minAvailable": 2,
        "selector": {"matchLabels": {"app": "web"}}
      }
    }"#;
    let pdb: PodDisruptionBudget = assert_roundtrip(fixture);
    assert_eq!(pdb.type_meta.api_version, "policy/v1");
    match pdb.spec.min_available {
        Some(IntOrString::Int(n)) => assert_eq!(n, 2),
        other => panic!("expected Int(2), got {:?}", other),
    }
}

#[test]
fn roundtrip_pdb_max_unavailable_percent() {
    let fixture = r#"{
      "apiVersion": "policy/v1",
      "kind": "PodDisruptionBudget",
      "metadata": {"name": "db-pdb", "namespace": "production"},
      "spec": {
        "maxUnavailable": "25%",
        "selector": {"matchLabels": {"app": "database"}}
      }
    }"#;
    let pdb: PodDisruptionBudget = assert_roundtrip(fixture);
    match pdb.spec.max_unavailable {
        Some(IntOrString::String(ref s)) => assert_eq!(s, "25%"),
        ref other => panic!("expected String(\"25%\"), got {:?}", other),
    }
}

#[test]
fn roundtrip_pdb_with_match_expressions() {
    let fixture = r#"{
      "apiVersion": "policy/v1",
      "kind": "PodDisruptionBudget",
      "metadata": {"name": "expr-pdb", "namespace": "default"},
      "spec": {
        "minAvailable": 1,
        "selector": {
          "matchExpressions": [
            {"key": "tier", "operator": "In", "values": ["frontend", "backend"]}
          ]
        }
      }
    }"#;
    let pdb: PodDisruptionBudget = assert_roundtrip(fixture);
    let exprs = pdb
        .spec
        .selector
        .match_expressions
        .expect("matchExpressions present");
    assert_eq!(exprs.len(), 1);
    assert_eq!(exprs[0].key, "tier");
    assert_eq!(exprs[0].operator, "In");
}

#[test]
fn roundtrip_pdb_unhealthy_pod_eviction_policy() {
    let fixture = r#"{
      "apiVersion": "policy/v1",
      "kind": "PodDisruptionBudget",
      "metadata": {"name": "evict", "namespace": "default"},
      "spec": {
        "minAvailable": 1,
        "selector": {"matchLabels": {"app": "x"}},
        "unhealthyPodEvictionPolicy": "AlwaysAllow"
      }
    }"#;
    let pdb: PodDisruptionBudget = assert_roundtrip(fixture);
    assert_eq!(
        pdb.spec.unhealthy_pod_eviction_policy.as_deref(),
        Some("AlwaysAllow"),
    );
}

#[test]
fn roundtrip_pdb_with_status() {
    let fixture = r#"{
      "apiVersion": "policy/v1",
      "kind": "PodDisruptionBudget",
      "metadata": {"name": "status-pdb", "namespace": "default"},
      "spec": {
        "minAvailable": 2,
        "selector": {"matchLabels": {"app": "x"}}
      },
      "status": {
        "currentHealthy": 3,
        "desiredHealthy": 2,
        "disruptionsAllowed": 1,
        "expectedPods": 3,
        "observedGeneration": 7
      }
    }"#;
    let pdb: PodDisruptionBudget = assert_roundtrip(fixture);
    let status = pdb.status.expect("status present");
    assert_eq!(status.current_healthy, 3);
    assert_eq!(status.desired_healthy, 2);
    assert_eq!(status.disruptions_allowed, 1);
    assert_eq!(status.expected_pods, 3);
    assert_eq!(status.observed_generation, Some(7));
}

#[test]
fn roundtrip_pdb_zero_max_unavailable_int() {
    // maxUnavailable: 0 is a valid (and meaningful) PDB shape — block any voluntary disruption.
    let fixture = r#"{
      "apiVersion": "policy/v1",
      "kind": "PodDisruptionBudget",
      "metadata": {"name": "zero", "namespace": "default"},
      "spec": {
        "maxUnavailable": 0,
        "selector": {"matchLabels": {"app": "critical"}}
      }
    }"#;
    let pdb: PodDisruptionBudget = assert_roundtrip(fixture);
    match pdb.spec.max_unavailable {
        Some(IntOrString::Int(n)) => assert_eq!(n, 0),
        other => panic!("expected Int(0), got {:?}", other),
    }
}
