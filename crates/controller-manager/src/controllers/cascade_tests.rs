/// Cascading deletion tests for all controllers.
///
/// These tests verify that:
/// 1. Controllers set ownerReferences on resources they create
/// 2. The garbage collector detects and deletes orphaned resources
/// 3. Controllers skip reconciliation when being deleted
/// 4. The full cascade chain works end-to-end (owner deleted → GC removes dependents)
#[cfg(test)]
mod tests {
    use crate::controllers::{
        daemonset::DaemonSetController, garbage_collector::GarbageCollector, job::JobController,
        statefulset::StatefulSetController,
    };
    use rusternetes_common::resources::workloads::{
        DaemonSet, DaemonSetSpec, Job, JobSpec, ReplicaSet, ReplicaSetSpec, StatefulSet,
        StatefulSetSpec,
    };
    use rusternetes_common::resources::{
        Endpoints, Node, NodeStatus, Pod, PodSpec, PodStatus, PodTemplateSpec,
    };
    use rusternetes_common::types::{LabelSelector, ObjectMeta, OwnerReference, TypeMeta};
    use rusternetes_storage::{memory::MemoryStorage, Storage};
    use std::sync::Arc;

    // ── helpers ───────────────────────────────────────────────────────────────

    fn make_storage() -> Arc<MemoryStorage> {
        Arc::new(MemoryStorage::new())
    }

    fn minimal_pod_template() -> PodTemplateSpec {
        PodTemplateSpec {
            metadata: Some(ObjectMeta::new("pod-template")),
            spec: PodSpec {
                containers: vec![],
                init_containers: None,
                ephemeral_containers: None,
                volumes: None,
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                restart_policy: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
            },
        }
    }

    #[allow(dead_code)]
    fn minimal_pod_status() -> PodStatus {
        PodStatus {
            phase: None,
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
        }
    }

    fn make_node(name: &str) -> Node {
        Node {
            type_meta: TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: None,
            status: Some(NodeStatus {
                conditions: None,
                addresses: None,
                capacity: None,
                allocatable: None,
                node_info: None,
                images: None,
                volumes_in_use: None,
                volumes_attached: None,
                daemon_endpoints: None,
                config: None,
                features: None,
                runtime_handlers: None,
            }),
        }
    }

    fn make_daemonset(name: &str, namespace: &str, uid: &str) -> DaemonSet {
        let mut ds = DaemonSet::new(
            name,
            namespace,
            DaemonSetSpec {
                selector: LabelSelector {
                    match_labels: None,
                    match_expressions: None,
                },
                template: minimal_pod_template(),
                update_strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
            },
        );
        ds.metadata.uid = uid.to_string();
        ds
    }

    fn make_statefulset(name: &str, namespace: &str, uid: &str, replicas: i32) -> StatefulSet {
        let mut sts = StatefulSet::new(
            name,
            namespace,
            StatefulSetSpec {
                replicas: Some(replicas),
                selector: LabelSelector {
                    match_labels: None,
                    match_expressions: None,
                },
                service_name: "headless".to_string(),
                template: minimal_pod_template(),
                update_strategy: None,
                pod_management_policy: Some("Parallel".to_string()),
                min_ready_seconds: None,
                revision_history_limit: None,
                volume_claim_templates: None,
                persistent_volume_claim_retention_policy: None,
                ordinals: None,
            },
        );
        sts.metadata.uid = uid.to_string();
        sts
    }

    fn make_job(name: &str, namespace: &str, uid: &str) -> Job {
        let mut job = Job::new(
            name,
            namespace,
            JobSpec {
                template: minimal_pod_template(),
                completions: Some(1),
                parallelism: Some(1),
                backoff_limit: Some(3),
                active_deadline_seconds: None,
                selector: None,
                manual_selector: None,
                suspend: None,
                ttl_seconds_after_finished: None,
                completion_mode: None,
                backoff_limit_per_index: None,
                max_failed_indexes: None,
                pod_failure_policy: None,
                pod_replacement_policy: None,
                success_policy: None,
                managed_by: None,
            },
        );
        job.metadata.uid = uid.to_string();
        job
    }

    fn make_replicaset(name: &str, namespace: &str, uid: &str) -> ReplicaSet {
        let mut m = ObjectMeta::new(name);
        m.namespace = Some(namespace.to_string());
        m.uid = uid.to_string();
        ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: m,
            spec: ReplicaSetSpec {
                replicas: 1,
                selector: LabelSelector {
                    match_labels: None,
                    match_expressions: None,
                },
                template: minimal_pod_template(),
                min_ready_seconds: None,
            },
            status: None,
        }
    }

    fn make_orphan_pod(name: &str, namespace: &str, owner_uid: &str, owner_kind: &str) -> Pod {
        let mut m = ObjectMeta::new(name);
        m.namespace = Some(namespace.to_string());
        m.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: owner_kind.to_string(),
            name: "gone-owner".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: m,
            spec: None,
            status: None,
        }
    }

    async fn get_pods(storage: &Arc<MemoryStorage>, namespace: &str) -> Vec<Pod> {
        storage
            .list(&format!("/registry/pods/{}/", namespace))
            .await
            .unwrap()
    }

    // ── DaemonSet tests ───────────────────────────────────────────────────────

    /// DaemonSet reconcile creates pods with ownerReferences pointing to the DS.
    #[tokio::test]
    async fn test_daemonset_pods_have_owner_references() {
        let storage = make_storage();
        let controller = DaemonSetController::new(storage.clone());

        storage
            .create("/registry/nodes/node-1", &make_node("node-1"))
            .await
            .unwrap();

        let ds = make_daemonset("my-ds", "default", "ds-uid-001");
        storage
            .create("/registry/daemonsets/default/my-ds", &ds)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods = get_pods(&storage, "default").await;
        assert_eq!(pods.len(), 1, "expected one pod per node");

        let owner_refs = pods[0]
            .metadata
            .owner_references
            .as_ref()
            .expect("pod must have ownerReferences");
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "DaemonSet");
        assert_eq!(owner_refs[0].name, "my-ds");
        assert_eq!(owner_refs[0].uid, ds.metadata.uid);
        assert_eq!(owner_refs[0].controller, Some(true));
        assert_eq!(owner_refs[0].block_owner_deletion, Some(true));
    }

    /// DaemonSet reconcile skips pod creation when the DS has a deletionTimestamp.
    #[tokio::test]
    async fn test_daemonset_skips_reconcile_when_being_deleted() {
        let storage = make_storage();
        let controller = DaemonSetController::new(storage.clone());

        storage
            .create("/registry/nodes/node-1", &make_node("node-1"))
            .await
            .unwrap();

        let mut ds = make_daemonset("my-ds", "default", "ds-uid-002");
        ds.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .create("/registry/daemonsets/default/my-ds", &ds)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "must not create pods for a deleting DaemonSet"
        );
    }

    /// GC deletes DaemonSet pods after the DaemonSet owner is removed from storage.
    #[tokio::test]
    async fn test_daemonset_cascade_delete_via_gc() {
        let storage = make_storage();
        let ds_controller = DaemonSetController::new(storage.clone());
        let gc = GarbageCollector::new(storage.clone());

        storage
            .create("/registry/nodes/node-1", &make_node("node-1"))
            .await
            .unwrap();

        let ds = make_daemonset("my-ds", "default", "ds-uid-003");
        storage
            .create("/registry/daemonsets/default/my-ds", &ds)
            .await
            .unwrap();

        ds_controller.reconcile_all().await.unwrap();
        assert_eq!(get_pods(&storage, "default").await.len(), 1);

        // Delete the DaemonSet
        storage
            .delete("/registry/daemonsets/default/my-ds")
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "pod should be GC'd after DaemonSet is deleted"
        );
    }

    /// DaemonSet creates one pod per node, each pod owned by the DS.
    #[tokio::test]
    async fn test_daemonset_one_pod_per_node_with_owner_refs() {
        let storage = make_storage();
        let controller = DaemonSetController::new(storage.clone());

        for i in 1..=3 {
            storage
                .create(
                    &format!("/registry/nodes/node-{}", i),
                    &make_node(&format!("node-{}", i)),
                )
                .await
                .unwrap();
        }

        let ds = make_daemonset("fleet-ds", "default", "ds-uid-004");
        storage
            .create("/registry/daemonsets/default/fleet-ds", &ds)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods = get_pods(&storage, "default").await;
        assert_eq!(pods.len(), 3, "one pod per node");

        for pod in &pods {
            let refs = pod
                .metadata
                .owner_references
                .as_ref()
                .expect("every pod must have ownerReferences");
            assert!(
                refs.iter().any(|r| r.uid == ds.metadata.uid),
                "pod must reference the DaemonSet uid"
            );
        }
    }

    /// All three DaemonSet pods are GC'd when the DS is deleted (multi-node).
    #[tokio::test]
    async fn test_daemonset_cascade_multi_node() {
        let storage = make_storage();
        let ds_controller = DaemonSetController::new(storage.clone());
        let gc = GarbageCollector::new(storage.clone());

        for i in 1..=3 {
            storage
                .create(
                    &format!("/registry/nodes/node-{}", i),
                    &make_node(&format!("node-{}", i)),
                )
                .await
                .unwrap();
        }

        let ds = make_daemonset("fleet-ds", "default", "ds-uid-005");
        storage
            .create("/registry/daemonsets/default/fleet-ds", &ds)
            .await
            .unwrap();

        ds_controller.reconcile_all().await.unwrap();
        assert_eq!(get_pods(&storage, "default").await.len(), 3);

        storage
            .delete("/registry/daemonsets/default/fleet-ds")
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "all 3 DS pods must be GC'd"
        );
    }

    // ── StatefulSet tests ─────────────────────────────────────────────────────

    /// StatefulSet reconcile creates pods with ownerReferences pointing to the STS.
    #[tokio::test]
    async fn test_statefulset_pods_have_owner_references() {
        let storage = make_storage();
        let controller = StatefulSetController::new(storage.clone());

        let sts = make_statefulset("my-sts", "default", "sts-uid-001", 2);
        storage
            .create("/registry/statefulsets/default/my-sts", &sts)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods = get_pods(&storage, "default").await;
        assert_eq!(pods.len(), 2);

        for pod in &pods {
            let refs = pod
                .metadata
                .owner_references
                .as_ref()
                .expect("pod must have ownerReferences");
            assert_eq!(refs[0].kind, "StatefulSet");
            assert_eq!(refs[0].uid, sts.metadata.uid);
            assert_eq!(refs[0].controller, Some(true));
        }
    }

    /// StatefulSet skips reconcile when being deleted.
    #[tokio::test]
    async fn test_statefulset_skips_reconcile_when_being_deleted() {
        let storage = make_storage();
        let controller = StatefulSetController::new(storage.clone());

        let mut sts = make_statefulset("my-sts", "default", "sts-uid-002", 3);
        sts.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .create("/registry/statefulsets/default/my-sts", &sts)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "must not create pods for a deleting StatefulSet"
        );
    }

    /// GC deletes StatefulSet pods after the StatefulSet owner is removed.
    #[tokio::test]
    async fn test_statefulset_cascade_delete_via_gc() {
        let storage = make_storage();
        let sts_controller = StatefulSetController::new(storage.clone());
        let gc = GarbageCollector::new(storage.clone());

        let sts = make_statefulset("my-sts", "default", "sts-uid-003", 2);
        storage
            .create("/registry/statefulsets/default/my-sts", &sts)
            .await
            .unwrap();

        sts_controller.reconcile_all().await.unwrap();
        assert_eq!(get_pods(&storage, "default").await.len(), 2);

        storage
            .delete("/registry/statefulsets/default/my-sts")
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "all STS pods should be GC'd after StatefulSet deletion"
        );
    }

    // ── Job tests ─────────────────────────────────────────────────────────────

    /// Job reconcile creates pods with ownerReferences pointing to the Job.
    #[tokio::test]
    async fn test_job_pods_have_owner_references() {
        let storage = make_storage();
        let controller = JobController::new(storage.clone());

        let job = make_job("my-job", "default", "job-uid-001");
        storage
            .create("/registry/jobs/default/my-job", &job)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods = get_pods(&storage, "default").await;
        assert_eq!(pods.len(), 1, "job should create one pod");

        let refs = pods[0]
            .metadata
            .owner_references
            .as_ref()
            .expect("pod must have ownerReferences");
        assert_eq!(refs[0].kind, "Job");
        assert_eq!(refs[0].uid, job.metadata.uid);
        assert_eq!(refs[0].controller, Some(true));
        assert_eq!(refs[0].block_owner_deletion, Some(true));
    }

    /// Job skips reconcile when being deleted.
    #[tokio::test]
    async fn test_job_skips_reconcile_when_being_deleted() {
        let storage = make_storage();
        let controller = JobController::new(storage.clone());

        let mut job = make_job("my-job", "default", "job-uid-002");
        job.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .create("/registry/jobs/default/my-job", &job)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "must not create pods for a deleting Job"
        );
    }

    /// GC deletes Job pods after the Job owner is removed from storage.
    #[tokio::test]
    async fn test_job_cascade_delete_via_gc() {
        let storage = make_storage();
        let job_controller = JobController::new(storage.clone());
        let gc = GarbageCollector::new(storage.clone());

        let job = make_job("my-job", "default", "job-uid-003");
        storage
            .create("/registry/jobs/default/my-job", &job)
            .await
            .unwrap();

        job_controller.reconcile_all().await.unwrap();
        assert_eq!(get_pods(&storage, "default").await.len(), 1);

        storage
            .delete("/registry/jobs/default/my-job")
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "job pod should be GC'd after Job deletion"
        );
    }

    // ── CronJob → Job cascade ─────────────────────────────────────────────────

    /// Jobs created with a CronJob ownerReference are cleaned up when the CronJob is deleted.
    #[tokio::test]
    async fn test_cronjob_cascade_delete_jobs_via_gc() {
        let storage = make_storage();
        let gc = GarbageCollector::new(storage.clone());

        // Manually store a Job with a CronJob ownerReference (simulating what
        // CronJobController creates after the fix).
        let mut job = make_job("my-cj-job", "default", "job-uid-cj-001");
        job.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "batch/v1".to_string(),
            kind: "CronJob".to_string(),
            name: "my-cj".to_string(),
            uid: "cj-uid-001".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        storage
            .create("/registry/jobs/default/my-cj-job", &job)
            .await
            .unwrap();

        // CronJob does NOT exist in storage → Job is orphaned
        gc.scan_and_collect().await.unwrap();

        let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
        assert_eq!(
            jobs.len(),
            0,
            "Job should be GC'd when its CronJob owner is gone"
        );
    }

    // ── Endpoints → Service cascade ───────────────────────────────────────────

    /// Endpoints with a Service ownerReference are GC'd when the Service is deleted.
    #[tokio::test]
    async fn test_endpoints_cascade_delete_via_gc() {
        let storage = make_storage();
        let gc = GarbageCollector::new(storage.clone());

        let endpoints = Endpoints {
            type_meta: TypeMeta {
                kind: "Endpoints".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "my-svc".to_string(),
                namespace: Some("default".to_string()),
                uid: String::new(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: Some(vec![OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "Service".to_string(),
                    name: "my-svc".to_string(),
                    uid: "svc-uid-001".to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: None,
                annotations: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            subsets: vec![],
        };
        storage
            .create("/registry/endpoints/default/my-svc", &endpoints)
            .await
            .unwrap();

        // Service does NOT exist → Endpoints are orphaned
        gc.scan_and_collect().await.unwrap();

        let remaining: Vec<Endpoints> = storage.list("/registry/endpoints/default/").await.unwrap();
        assert_eq!(
            remaining.len(),
            0,
            "Endpoints should be GC'd when Service owner is gone"
        );
    }

    // ── GC orphan detection ───────────────────────────────────────────────────

    /// A pod with a missing owner UID is detected and deleted by GC.
    #[tokio::test]
    async fn test_gc_detects_orphan_with_missing_owner() {
        let storage = make_storage();
        let gc = GarbageCollector::new(storage.clone());

        let pod = make_orphan_pod("orphan-pod", "default", "uid-does-not-exist", "ReplicaSet");
        storage
            .create("/registry/pods/default/orphan-pod", &pod)
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "orphaned pod must be deleted by GC"
        );
    }

    /// A pod whose owner still exists is NOT deleted by GC.
    #[tokio::test]
    async fn test_gc_does_not_delete_pod_with_live_owner() {
        let storage = make_storage();
        let gc = GarbageCollector::new(storage.clone());

        let rs = make_replicaset("live-rs", "default", "live-rs-uid");
        storage
            .create("/registry/replicasets/default/live-rs", &rs)
            .await
            .unwrap();

        // Pod owned by the live RS
        let mut m = ObjectMeta::new("live-pod");
        m.namespace = Some("default".to_string());
        m.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: "live-rs".to_string(),
            uid: "live-rs-uid".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: m,
            spec: None,
            status: None,
        };
        storage
            .create("/registry/pods/default/live-pod", &pod)
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            1,
            "pod with living owner must NOT be GC'd"
        );
    }

    /// Resources without ownerReferences (cluster-level, standalone pods) are never GC'd.
    #[tokio::test]
    async fn test_gc_does_not_delete_unowned_resources() {
        let storage = make_storage();
        let gc = GarbageCollector::new(storage.clone());

        // Pod with no owner references (user-created standalone pod)
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new("standalone-pod");
                m.namespace = Some("default".to_string());
                m
            },
            spec: None,
            status: None,
        };
        storage
            .create("/registry/pods/default/standalone-pod", &pod)
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            1,
            "standalone pod (no ownerRef) must never be GC'd"
        );
    }

    // ── Multi-level cascade ───────────────────────────────────────────────────

    /// Deployment → ReplicaSet → Pods: two GC passes clean everything up.
    #[tokio::test]
    async fn test_gc_multi_level_cascade() {
        let storage = make_storage();
        let gc = GarbageCollector::new(storage.clone());

        let rs_uid = "rs-uid-001";

        // ReplicaSet owned by a (now-deleted) Deployment
        let mut rs = make_replicaset("my-rs", "default", rs_uid);
        rs.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
            name: "my-deploy".to_string(),
            uid: "deploy-uid-001".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        storage
            .create("/registry/replicasets/default/my-rs", &rs)
            .await
            .unwrap();

        // Pods owned by the ReplicaSet
        for i in 0..2 {
            let pod = make_orphan_pod(&format!("pod-{}", i), "default", rs_uid, "ReplicaSet");
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        // Pass 1: Deployment gone → RS is orphaned → deleted
        gc.scan_and_collect().await.unwrap();

        let rs_list: Vec<ReplicaSet> = storage
            .list("/registry/replicasets/default/")
            .await
            .unwrap();
        assert_eq!(rs_list.len(), 0, "RS should be GC'd (Deployment gone)");

        // Pass 2: RS is now gone → pods are orphaned → deleted
        gc.scan_and_collect().await.unwrap();

        assert_eq!(
            get_pods(&storage, "default").await.len(),
            0,
            "pods should be GC'd on second pass (RS gone)"
        );
    }

    // ── ownerReference field correctness ──────────────────────────────────────

    /// All required ownerReference fields are set by DaemonSetController.
    #[tokio::test]
    async fn test_daemonset_owner_ref_fields_complete() {
        let storage = make_storage();
        let controller = DaemonSetController::new(storage.clone());

        storage
            .create("/registry/nodes/node-1", &make_node("node-1"))
            .await
            .unwrap();

        let ds = make_daemonset("checker-ds", "default", "ds-check-uid");
        storage
            .create("/registry/daemonsets/default/checker-ds", &ds)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods = get_pods(&storage, "default").await;
        let r = &pods[0].metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(r.api_version, "apps/v1");
        assert_eq!(r.kind, "DaemonSet");
        assert_eq!(r.name, "checker-ds");
        assert_eq!(r.uid, "ds-check-uid");
        assert_eq!(r.controller, Some(true));
        assert_eq!(r.block_owner_deletion, Some(true));
    }

    /// All required ownerReference fields are set by JobController.
    #[tokio::test]
    async fn test_job_owner_ref_fields_complete() {
        let storage = make_storage();
        let controller = JobController::new(storage.clone());

        let job = make_job("checker-job", "default", "job-check-uid");
        storage
            .create("/registry/jobs/default/checker-job", &job)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods = get_pods(&storage, "default").await;
        let r = &pods[0].metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(r.api_version, "batch/v1");
        assert_eq!(r.kind, "Job");
        assert_eq!(r.name, "checker-job");
        assert_eq!(r.uid, "job-check-uid");
        assert_eq!(r.controller, Some(true));
        assert_eq!(r.block_owner_deletion, Some(true));
    }

    /// All required ownerReference fields are set by StatefulSetController.
    #[tokio::test]
    async fn test_statefulset_owner_ref_fields_complete() {
        let storage = make_storage();
        let controller = StatefulSetController::new(storage.clone());

        let sts = make_statefulset("checker-sts", "default", "sts-check-uid", 1);
        storage
            .create("/registry/statefulsets/default/checker-sts", &sts)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods = get_pods(&storage, "default").await;
        let r = &pods[0].metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(r.api_version, "apps/v1");
        assert_eq!(r.kind, "StatefulSet");
        assert_eq!(r.name, "checker-sts");
        assert_eq!(r.uid, "sts-check-uid");
        assert_eq!(r.controller, Some(true));
        assert_eq!(r.block_owner_deletion, Some(true));
    }
}
