# Per-test conformance results

A row per official Kubernetes `[Conformance]` test, with one **dated, backend- and OS-tagged column per run**. Add a new column for each run rather than overwriting — this preserves history at the per-test level. The round-summary table lives in [`../CONFORMANCE.md`](../CONFORMANCE.md); the per-area triage matrices (with Rust test fn + notes) live alongside this file (`apps-deployment-replicaset.md`, etc.).

> Keys are the **verbatim ginkgo test names** from the run's JUnit, so they are stable and unambiguous. Each run column is identified by its **date, OS, storage backend, conformance image, and the rusternetes git commit it was measured on** (the commit doubles as the build version). Columns are **not comparable across backends** — etcd and SQLite/rhino exercise different code paths.

**Legend:** `PASS` / `FAIL` as reported by ginkgo; `—` = not exercised in that run (e.g. excluded by the run's focus/skip or a `[Feature:…]` gate).

## Runs

| Column | Date | OS | Backend | Image | Commit (version) | Pass | Fail | Not run |
|--------|------|----|---------|-------|------------------|-----:|-----:|--------:|
| Hydrophone 31 May '26 | 2026-05-31 | — | SQLite / rhino | `conformance:v1.35.0` | `e9c9f507` | 342 | 99 | — |
| Hydrophone 2 Jun '26 | 2026-06-02 | Zorin OS 18 | SQLite / rhino | `conformance:v1.35.0` | `e1758455` | 373 | 64 | 4 |

Run of 2026-06-02 measured in two legs on the same cluster: the known-green regression gate (`scripts/conformance-canary-run.sh`, 0 regressions) plus the full `[Conformance]` suite with the known-green set skipped (62 newly-green specs). Stack: `compose.sqlite.yml` + `compose.dind.yml`, rhino submodule.

## Results

| Ginkgo test [Conformance] | Hydrophone 31 May '26 | Hydrophone 2 Jun '26 |
|---|:---:|:---:|
| [sig-api-machinery] API priority and fairness should support FlowSchema API operations [Conformance] | FAIL | PASS |
| [sig-api-machinery] API priority and fairness should support PriorityLevelConfiguration API operations [Conformance] | PASS | PASS |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] listing mutating webhooks should work [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] listing validating webhooks should work [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] patching/updating a mutating webhook should work [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] patching/updating a validating webhook should work [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should be able to create and update mutating webhook configurations with match conditions [Conformance] | FAIL | PASS |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should be able to create and update validating webhook configurations with match conditions [Conformance] | FAIL | PASS |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should be able to deny attaching pod [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should be able to deny custom resource creation, update and deletion [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should be able to deny pod and configmap creation [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should deny crd creation [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should honor timeout [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should include webhook resources in discovery documents [Conformance] | FAIL | PASS |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should mutate configmap [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should mutate custom resource [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should mutate custom resource with different stored version [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should mutate custom resource with pruning [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should mutate everything except 'skip-me' configmaps [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should mutate pod and apply defaults after mutation [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should not be able to mutate or prevent deletion of webhook configuration objects [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should reject mutating webhook configurations with invalid match conditions [Conformance] | FAIL | PASS |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should reject validating webhook configurations with invalid match conditions [Conformance] | FAIL | PASS |
| [sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin] should unconditionally reject operations on fail closed webhook [Conformance] | FAIL | FAIL |
| [sig-api-machinery] AggregatedDiscovery should support aggregated discovery interface [Conformance] | PASS | PASS |
| [sig-api-machinery] AggregatedDiscovery should support aggregated discovery interface for CRDs [Conformance] | FAIL | PASS |
| [sig-api-machinery] AggregatedDiscovery should support raw aggregated discovery endpoint Accept headers [Conformance] | PASS | PASS |
| [sig-api-machinery] AggregatedDiscovery should support raw aggregated discovery request for CRDs [Conformance] | FAIL | PASS |
| [sig-api-machinery] Aggregator Should be able to support the 1.17 Sample API Server using the current Aggregator [LinuxOnly] [Conformance] | FAIL | FAIL |
| [sig-api-machinery] CustomResourceConversionWebhook [Privileged:ClusterAdmin] should be able to convert a non homogeneous list of CRs [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourceConversionWebhook [Privileged:ClusterAdmin] should be able to convert from CR v1 to CR v2 [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourceDefinition Watch [Privileged:ClusterAdmin] CustomResourceDefinition Watch watch on custom resource definition objects [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourceDefinition resources [Privileged:ClusterAdmin] Simple CustomResourceDefinition creating/deleting custom resource definition objects works [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourceDefinition resources [Privileged:ClusterAdmin] Simple CustomResourceDefinition getting/updating/patching custom resource definition status sub-resource works [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourceDefinition resources [Privileged:ClusterAdmin] Simple CustomResourceDefinition listing custom resource definition objects works [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourceDefinition resources [Privileged:ClusterAdmin] custom resource defaulting for requests and from storage works [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourceDefinition resources [Privileged:ClusterAdmin] should include custom resource definition resources in discovery documents [Conformance] | PASS | PASS |
| [sig-api-machinery] CustomResourceFieldSelectors [Privileged:ClusterAdmin] CustomResourceFieldSelectors MUST list and watch custom resources matching the field selector [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] removes definition from spec when one version gets changed to not be served [Conformance] | FAIL | FAIL |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] updates the published spec when one version gets renamed [Conformance] | FAIL | FAIL |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] works for CRD preserving unknown fields at the schema root [Conformance] | FAIL | FAIL |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] works for CRD preserving unknown fields in an embedded object [Conformance] | FAIL | FAIL |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] works for CRD with validation schema [Conformance] | FAIL | FAIL |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] works for CRD without validation schema [Conformance] | FAIL | PASS |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] works for multiple CRDs of different groups [Conformance] | FAIL | FAIL |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] works for multiple CRDs of same group and version but different kinds [Conformance] | FAIL | FAIL |
| [sig-api-machinery] CustomResourcePublishOpenAPI [Privileged:ClusterAdmin] works for multiple CRDs of same group but different versions [Conformance] | FAIL | FAIL |
| [sig-api-machinery] Discovery should locate the groupVersion and a resource within each APIGroup [Conformance] | PASS | PASS |
| [sig-api-machinery] Discovery should validate PreferredVersion for each APIGroup [Conformance] | PASS | PASS |
| [sig-api-machinery] FieldValidation should create/apply a CR with unknown fields for CRD with no validation schema [Conformance] | FAIL | PASS |
| [sig-api-machinery] FieldValidation should create/apply a valid CR for CRD with validation schema [Conformance] | FAIL | FAIL |
| [sig-api-machinery] FieldValidation should create/apply an invalid CR with extra properties for CRD with validation schema [Conformance] | FAIL | FAIL |
| [sig-api-machinery] FieldValidation should detect duplicates in a CR when preserving unknown fields [Conformance] | FAIL | FAIL |
| [sig-api-machinery] FieldValidation should detect unknown and duplicate fields of a typed object [Conformance] | PASS | PASS |
| [sig-api-machinery] FieldValidation should detect unknown metadata fields in both the root and embedded object of a CR [Conformance] | FAIL | FAIL |
| [sig-api-machinery] FieldValidation should detect unknown metadata fields of a typed object [Conformance] | PASS | PASS |
| [sig-api-machinery] Garbage collector should delete RS created by deployment when not orphaning [Conformance] | PASS | PASS |
| [sig-api-machinery] Garbage collector should delete pods created by rc when not orphaning [Conformance] | FAIL | PASS |
| [sig-api-machinery] Garbage collector should keep the rc around until all its pods are deleted if the deleteOptions says so [Serial] [Conformance] | FAIL | PASS |
| [sig-api-machinery] Garbage collector should not be blocked by dependency circle [Conformance] | PASS | PASS |
| [sig-api-machinery] Garbage collector should not delete dependents that have both valid owner and owner that's waiting for dependents to be deleted [Serial] [Conformance] | FAIL | PASS |
| [sig-api-machinery] Garbage collector should orphan RS created by deployment when deleteOptions.PropagationPolicy is Orphan [Conformance] | PASS | PASS |
| [sig-api-machinery] Garbage collector should orphan pods created by rc if delete options say so [Serial] [Conformance] | FAIL | PASS |
| [sig-api-machinery] Namespaces [Serial] should apply a finalizer to a Namespace [Conformance] | PASS | PASS |
| [sig-api-machinery] Namespaces [Serial] should apply an update to a Namespace [Conformance] | PASS | PASS |
| [sig-api-machinery] Namespaces [Serial] should apply changes to a namespace status [Conformance] | FAIL | FAIL |
| [sig-api-machinery] Namespaces [Serial] should ensure that all pods are removed when a namespace is deleted [Conformance] | PASS | PASS |
| [sig-api-machinery] Namespaces [Serial] should ensure that all services are removed when a namespace is deleted [Conformance] | PASS | PASS |
| [sig-api-machinery] Namespaces [Serial] should patch a Namespace [Conformance] | PASS | PASS |
| [sig-api-machinery] OrderedNamespaceDeletion namespace deletion should delete pod first [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should apply changes to a resourcequota status [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should be able to update and delete ResourceQuota. [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should create a ResourceQuota and capture the life of a configMap. [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should create a ResourceQuota and capture the life of a pod. [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should create a ResourceQuota and capture the life of a replica set. [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should create a ResourceQuota and capture the life of a replication controller. [Conformance] | FAIL | PASS |
| [sig-api-machinery] ResourceQuota should create a ResourceQuota and capture the life of a secret. [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should create a ResourceQuota and capture the life of a service. [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should create a ResourceQuota and ensure its status is promptly calculated. [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should manage the lifecycle of a ResourceQuota [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should verify ResourceQuota with best effort scope. [Conformance] | PASS | PASS |
| [sig-api-machinery] ResourceQuota should verify ResourceQuota with terminating scopes. [Conformance] | PASS | PASS |
| [sig-api-machinery] Servers with support for API chunking should return chunks of results for list calls [Conformance] | PASS | PASS |
| [sig-api-machinery] Servers with support for API chunking should support continue listing from the last key if the original version has been compacted away, though the list is inconsistent [Slow] [Conformance] | FAIL | FAIL |
| [sig-api-machinery] Servers with support for Table transformation should return a 406 for a backend which does not implement metadata [Conformance] | FAIL | PASS |
| [sig-api-machinery] ValidatingAdmissionPolicy [Privileged:ClusterAdmin] should allow expressions to refer variables. [Conformance] | PASS | PASS |
| [sig-api-machinery] ValidatingAdmissionPolicy [Privileged:ClusterAdmin] should support ValidatingAdmissionPolicy API operations [Conformance] | PASS | PASS |
| [sig-api-machinery] ValidatingAdmissionPolicy [Privileged:ClusterAdmin] should support ValidatingAdmissionPolicyBinding API operations [Conformance] | PASS | PASS |
| [sig-api-machinery] ValidatingAdmissionPolicy [Privileged:ClusterAdmin] should validate against a Deployment [Conformance] | PASS | PASS |
| [sig-api-machinery] Watchers should be able to restart watching from the last resource version observed by the previous watch [Conformance] | PASS | PASS |
| [sig-api-machinery] Watchers should be able to start watching from a specific resource version [Conformance] | PASS | PASS |
| [sig-api-machinery] Watchers should observe add, update, and delete watch notifications on configmaps [Conformance] | PASS | PASS |
| [sig-api-machinery] Watchers should observe an object deletion if it stops meeting the requirements of the selector [Conformance] | PASS | PASS |
| [sig-api-machinery] Watchers should receive events on concurrent watches in same order [Conformance] | PASS | PASS |
| [sig-api-machinery] server version should find the server version [Conformance] | PASS | PASS |
| [sig-apps] ControllerRevision [Serial] should manage the lifecycle of a ControllerRevision [Conformance] | FAIL | FAIL |
| [sig-apps] CronJob should not schedule jobs when suspended [Slow] [Conformance] | PASS | PASS |
| [sig-apps] CronJob should not schedule new jobs when ForbidConcurrent [Slow] [Conformance] | PASS | PASS |
| [sig-apps] CronJob should replace jobs when ReplaceConcurrent [Conformance] | PASS | PASS |
| [sig-apps] CronJob should schedule multiple jobs concurrently [Conformance] | PASS | PASS |
| [sig-apps] CronJob should support CronJob API operations [Conformance] | PASS | PASS |
| [sig-apps] Daemon set [Serial] should list and delete a collection of DaemonSets [Conformance] | PASS | PASS |
| [sig-apps] Daemon set [Serial] should retry creating failed daemon pods [Conformance] | PASS | PASS |
| [sig-apps] Daemon set [Serial] should rollback without unnecessary restarts [Conformance] | PASS | PASS |
| [sig-apps] Daemon set [Serial] should run and stop complex daemon [Conformance] | PASS | PASS |
| [sig-apps] Daemon set [Serial] should run and stop simple daemon [Conformance] | PASS | PASS |
| [sig-apps] Daemon set [Serial] should update pod when spec was updated and update strategy is RollingUpdate [Conformance] | PASS | PASS |
| [sig-apps] Daemon set [Serial] should verify changes to a daemon set status [Conformance] | PASS | PASS |
| [sig-apps] Deployment Deployment should have a working scale subresource [Conformance] | PASS | PASS |
| [sig-apps] Deployment RecreateDeployment should delete old pods and create new ones [Conformance] | PASS | PASS |
| [sig-apps] Deployment RollingUpdateDeployment should delete old pods and create new ones [Conformance] | PASS | PASS |
| [sig-apps] Deployment deployment should delete old replica sets [Conformance] | PASS | PASS |
| [sig-apps] Deployment deployment should support proportional scaling [Conformance] | PASS | PASS |
| [sig-apps] Deployment deployment should support rollover [Conformance] | FAIL | FAIL |
| [sig-apps] Deployment should run the lifecycle of a Deployment [Conformance] | PASS | PASS |
| [sig-apps] Deployment should validate Deployment Status endpoints [Conformance] | PASS | PASS |
| [sig-apps] DisruptionController Listing PodDisruptionBudgets for all namespaces should list and delete a collection of PodDisruptionBudgets [Conformance] | PASS | PASS |
| [sig-apps] DisruptionController should block an eviction until the PDB is updated to allow it [Conformance] | PASS | PASS |
| [sig-apps] DisruptionController should create a PodDisruptionBudget [Conformance] | PASS | PASS |
| [sig-apps] DisruptionController should observe PodDisruptionBudget status updated [Conformance] | PASS | PASS |
| [sig-apps] DisruptionController should update/patch PodDisruptionBudget status [Conformance] | FAIL | PASS |
| [sig-apps] Job should adopt matching orphans and release non-matching pods [Conformance] | PASS | PASS |
| [sig-apps] Job should allow to use a pod failure policy to ignore failure matching on DisruptionTarget condition [Conformance] | PASS | PASS |
| [sig-apps] Job should allow to use the pod failure policy on exit code to fail the job early [Conformance] | PASS | PASS |
| [sig-apps] Job should apply changes to a job status [Conformance] | FAIL | FAIL |
| [sig-apps] Job should create pods for an Indexed job with completion indexes and specified hostname [Conformance] | PASS | PASS |
| [sig-apps] Job should delete a job [Conformance] | PASS | PASS |
| [sig-apps] Job should execute all indexes despite some failing when using backoffLimitPerIndex [Conformance] | PASS | PASS |
| [sig-apps] Job should manage the lifecycle of a job [Conformance] | PASS | PASS |
| [sig-apps] Job should mark indexes as failed when the FailIndex action is matched in podFailurePolicy [Conformance] | PASS | PASS |
| [sig-apps] Job should run a job to completion when tasks sometimes fail and are locally restarted [Conformance] | PASS | PASS |
| [sig-apps] Job should terminate job execution when the number of failed indexes exceeds maxFailedIndexes [Conformance] | PASS | PASS |
| [sig-apps] Job with successPolicy should succeeded when all indexes succeeded [Conformance] | PASS | PASS |
| [sig-apps] Job with successPolicy succeededCount rule should succeeded even when some indexes remain pending [Conformance] | FAIL | FAIL |
| [sig-apps] Job with successPolicy succeededIndexes rule should succeeded even when some indexes remain pending [Conformance] | PASS | PASS |
| [sig-apps] ReplicaSet Replace and Patch tests [Conformance] | PASS | PASS |
| [sig-apps] ReplicaSet Replicaset should have a working scale subresource [Conformance] | PASS | PASS |
| [sig-apps] ReplicaSet should adopt matching pods on creation and release no longer matching pods [Conformance] | PASS | PASS |
| [sig-apps] ReplicaSet should list and delete a collection of ReplicaSets [Conformance] | PASS | PASS |
| [sig-apps] ReplicaSet should serve a basic image on each replica with a public image [Conformance] | PASS | PASS |
| [sig-apps] ReplicaSet should validate Replicaset Status endpoints [Conformance] | PASS | PASS |
| [sig-apps] ReplicationController should adopt matching pods on creation [Conformance] | FAIL | PASS |
| [sig-apps] ReplicationController should get and update a ReplicationController scale [Conformance] | FAIL | PASS |
| [sig-apps] ReplicationController should release no longer matching pods [Conformance] | FAIL | PASS |
| [sig-apps] ReplicationController should serve a basic image on each replica with a public image [Conformance] | FAIL | PASS |
| [sig-apps] ReplicationController should surface a failure condition on a common issue like exceeded quota [Conformance] | FAIL | PASS |
| [sig-apps] ReplicationController should test the lifecycle of a ReplicationController [Conformance] | FAIL | PASS |
| [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic] Burst scaling should run to completion even with unhealthy pods [Slow] [Conformance] | PASS | PASS |
| [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic] Scaling should happen in predictable order and halt if any stateful pod is unhealthy [Slow] [Conformance] | PASS | PASS |
| [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic] Should recreate evicted statefulset [Conformance] | PASS | PASS |
| [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic] should have a working scale subresource [Conformance] | PASS | PASS |
| [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic] should list, patch and delete a collection of StatefulSets [Conformance] | PASS | PASS |
| [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic] should perform canary updates and phased rolling updates of template modifications [Conformance] | PASS | PASS |
| [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic] should perform rolling updates and roll backs of template modifications [Conformance] | FAIL | FAIL |
| [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic] should validate Statefulset Status endpoints [Conformance] | PASS | PASS |
| [sig-architecture] Conformance Tests should have at least two untainted nodes [Conformance] | PASS | PASS |
| [sig-auth] Certificates API [Privileged:ClusterAdmin] should support CSR API operations [Conformance] | FAIL | FAIL |
| [sig-auth] ServiceAccounts ServiceAccountIssuerDiscovery should support OIDC discovery of service account issuer [Conformance] | PASS | PASS |
| [sig-auth] ServiceAccounts should allow opting out of API token automount [Conformance] | PASS | PASS |
| [sig-auth] ServiceAccounts should create a serviceAccountToken and ensure a successful TokenReview [Conformance] | PASS | PASS |
| [sig-auth] ServiceAccounts should guarantee kube-root-ca.crt exist in any namespace [Conformance] | PASS | PASS |
| [sig-auth] ServiceAccounts should mount an API token into pods [Conformance] | PASS | PASS |
| [sig-auth] ServiceAccounts should mount projected service account token [Conformance] | PASS | PASS |
| [sig-auth] ServiceAccounts should run through the lifecycle of a ServiceAccount [Conformance] | PASS | PASS |
| [sig-auth] ServiceAccounts should update a ServiceAccount [Conformance] | PASS | PASS |
| [sig-auth] SubjectReview should support SubjectReview API operations [Conformance] | FAIL | FAIL |
| [sig-cli] Kubectl client Guestbook application should create and stop a working application [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl api-versions should check if v1 is in available api versions [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl cluster-info should check if Kubernetes control plane services is included in cluster-info [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl describe should check if kubectl describe prints relevant information for rc and pods [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl diff should check if kubectl diff finds a difference for Deployments [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl expose should create services for rc [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl label should update the label on a resource [Conformance] | FAIL | PASS |
| [sig-cli] Kubectl client Kubectl patch should add annotations for pods in rc [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl replace should update a single-container pod's image [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl run pod should create a pod from an image when restart is Never [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl server-side dry-run should check if kubectl can dry-run update Pods [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Kubectl version should check is all data is printed [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Proxy server should support --unix-socket=/path [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Proxy server should support proxy with --port 0 [Conformance] | PASS | PASS |
| [sig-cli] Kubectl client Update Demo should create and stop a replication controller [Conformance] | FAIL | PASS |
| [sig-cli] Kubectl client Update Demo should scale a replication controller [Conformance] | FAIL | PASS |
| [sig-cli] Kubectl logs logs should be able to retrieve and filter logs [Conformance] | PASS | PASS |
| [sig-instrumentation] Events API should delete a collection of events [Conformance] | PASS | PASS |
| [sig-instrumentation] Events API should ensure that an event can be fetched, patched, deleted, and listed [Conformance] | PASS | PASS |
| [sig-instrumentation] Events should delete a collection of events [Conformance] | PASS | PASS |
| [sig-instrumentation] Events should manage the lifecycle of an event [Conformance] | PASS | PASS |
| [sig-network] API Server should have Endpoints and EndpointSlices pointing to API Server [Conformance] | PASS | PASS |
| [sig-network] API Server should provide secure master service [Conformance] | PASS | PASS |
| [sig-network] DNS should provide /etc/hosts entries for the cluster [Conformance] | PASS | PASS |
| [sig-network] DNS should provide DNS for ExternalName services [Conformance] | FAIL | PASS |
| [sig-network] DNS should provide DNS for pods for Hostname [Conformance] | PASS | PASS |
| [sig-network] DNS should provide DNS for pods for Subdomain [Conformance] | PASS | PASS |
| [sig-network] DNS should provide DNS for services [Conformance] | PASS | PASS |
| [sig-network] DNS should provide DNS for the cluster [Conformance] | PASS | PASS |
| [sig-network] DNS should resolve DNS of partial qualified names for services [LinuxOnly] [Conformance] | PASS | PASS |
| [sig-network] DNS should support configurable pod DNS nameservers [Conformance] | PASS | PASS |
| [sig-network] EndpointSlice should create Endpoints and EndpointSlices for Pods matching a Service [Conformance] | PASS | PASS |
| [sig-network] EndpointSlice should create and delete EndpointSlices for a Service with a selector that matches no pods [Conformance] | PASS | PASS |
| [sig-network] EndpointSlice should support a Service with multiple endpoint IPs specified in multiple EndpointSlices [Conformance] | PASS | PASS |
| [sig-network] EndpointSlice should support a Service with multiple ports specified in multiple EndpointSlices [Conformance] | PASS | PASS |
| [sig-network] EndpointSlice should support creating EndpointSlice API operations [Conformance] | PASS | PASS |
| [sig-network] EndpointSliceMirroring should mirror a custom Endpoints resource through create update and delete [Conformance] | FAIL | FAIL |
| [sig-network] Endpoints should test the lifecycle of an Endpoint [Conformance] | PASS | PASS |
| [sig-network] EndpointsController should create Endpoints for Pods matching a Service [Conformance] | PASS | PASS |
| [sig-network] EndpointsController should create and delete Endpoints for a Service with a selector that matches no pods [Conformance] | PASS | PASS |
| [sig-network] HostPort validates that there is no conflict between pods with same hostPort but different hostIP and protocol [LinuxOnly] [Conformance] | FAIL | FAIL |
| [sig-network] Ingress API should support creating Ingress API operations [Conformance] | PASS | PASS |
| [sig-network] IngressClass API should support creating IngressClass API operations [Conformance] | PASS | PASS |
| [sig-network] Networking Granular Checks: Pods should function for intra-pod communication: http [NodeConformance] [Conformance] | FAIL | FAIL |
| [sig-network] Networking Granular Checks: Pods should function for intra-pod communication: udp [NodeConformance] [Conformance] | FAIL | FAIL |
| [sig-network] Networking Granular Checks: Pods should function for node-pod communication: http [LinuxOnly] [NodeConformance] [Conformance] | FAIL | FAIL |
| [sig-network] Networking Granular Checks: Pods should function for node-pod communication: udp [LinuxOnly] [NodeConformance] [Conformance] | FAIL | FAIL |
| [sig-network] Proxy version v1 A set of valid responses are returned for both pod and service Proxy [Conformance] | FAIL | FAIL |
| [sig-network] Proxy version v1 A set of valid responses are returned for both pod and service ProxyWithPath [Conformance] | PASS | PASS |
| [sig-network] Proxy version v1 should proxy through a service and a pod [Conformance] | PASS | PASS |
| [sig-network] Service endpoints latency should not be very high [Conformance] | PASS | PASS |
| [sig-network] ServiceCIDR and IPAddress API should support IPAddress API operations [Conformance] | PASS | PASS |
| [sig-network] ServiceCIDR and IPAddress API should support ServiceCIDR API operations [Conformance] | PASS | PASS |
| [sig-network] Services should be able to change the type from ClusterIP to ExternalName [Conformance] | PASS | PASS |
| [sig-network] Services should be able to change the type from ExternalName to ClusterIP [Conformance] | PASS | PASS |
| [sig-network] Services should be able to change the type from ExternalName to NodePort [Conformance] | FAIL | FAIL |
| [sig-network] Services should be able to change the type from NodePort to ExternalName [Conformance] | PASS | PASS |
| [sig-network] Services should be able to create a functioning NodePort service [Conformance] | FAIL | FAIL |
| [sig-network] Services should be able to switch session affinity for NodePort service [LinuxOnly] [Conformance] | FAIL | FAIL |
| [sig-network] Services should be able to switch session affinity for service with type clusterIP [LinuxOnly] [Conformance] | PASS | PASS |
| [sig-network] Services should complete a service status lifecycle [Conformance] | FAIL | FAIL |
| [sig-network] Services should delete a collection of services [Conformance] | PASS | PASS |
| [sig-network] Services should find a service from listing all namespaces [Conformance] | PASS | PASS |
| [sig-network] Services should have session affinity work for NodePort service [LinuxOnly] [Conformance] | FAIL | FAIL |
| [sig-network] Services should have session affinity work for service with type clusterIP [LinuxOnly] [Conformance] | PASS | PASS |
| [sig-network] Services should serve a basic endpoint from pods [Conformance] | PASS | PASS |
| [sig-network] Services should serve endpoints on same port and different protocols [Conformance] | PASS | PASS |
| [sig-network] Services should serve multiport endpoints from pods [Conformance] | PASS | PASS |
| [sig-node] ConfigMap should be consumable as environment variable names with various prefixes [Conformance] | PASS | PASS |
| [sig-node] ConfigMap should be consumable via environment variable [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] ConfigMap should be consumable via the environment [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] ConfigMap should fail to create ConfigMap with empty key [Conformance] | PASS | PASS |
| [sig-node] ConfigMap should run through a ConfigMap lifecycle [Conformance] | PASS | PASS |
| [sig-node] ConfigMap should update ConfigMap successfully [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Container Lifecycle Hook when create a pod with lifecycle hook should execute poststart exec hook properly [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Container Lifecycle Hook when create a pod with lifecycle hook should execute poststart http hook properly [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Container Lifecycle Hook when create a pod with lifecycle hook should execute prestop exec hook properly [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Container Lifecycle Hook when create a pod with lifecycle hook should execute prestop http hook properly [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Container Runtime blackbox test on terminated container should report termination message as empty when pod succeeds and TerminationMessagePolicy FallbackToLogsOnError is set [NodeConformance] [Conformance] | PASS | — |
| [sig-node] Container Runtime blackbox test on terminated container should report termination message from file when pod succeeds and TerminationMessagePolicy FallbackToLogsOnError is set [NodeConformance] [Conformance] | PASS | — |
| [sig-node] Container Runtime blackbox test on terminated container should report termination message from log output if TerminationMessagePolicy FallbackToLogsOnError is set [NodeConformance] [Conformance] | PASS | — |
| [sig-node] Container Runtime blackbox test on terminated container should report termination message if TerminationMessagePath is set as non-root user and at a non-default path [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Container Runtime blackbox test when starting a container that exits should run with the expected status [NodeConformance] [Conformance] | FAIL | FAIL |
| [sig-node] Containers should be able to override the image's default arguments (container cmd) [NodeConformance] [Conformance] | PASS | — |
| [sig-node] Containers should be able to override the image's default command (container entrypoint) [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Containers should be able to override the image's default command and arguments [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Containers should use the image defaults if command and args are blank [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Downward API should provide container's limits.cpu/memory and requests.cpu/memory as env vars [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Downward API should provide default limits.cpu/memory from node allocatable [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Downward API should provide host IP as an env var [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Downward API should provide hostIPs as an env var [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Downward API should provide pod UID as env vars [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Downward API should provide pod name, namespace and IP address as env vars [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Ephemeral Containers [NodeConformance] should update the ephemeral containers in an existing pod [Conformance] | PASS | PASS |
| [sig-node] Ephemeral Containers [NodeConformance] will start an ephemeral container in an existing pod [Conformance] | PASS | PASS |
| [sig-node] InitContainer [NodeConformance] should invoke init containers on a RestartAlways pod [Conformance] | PASS | PASS |
| [sig-node] InitContainer [NodeConformance] should invoke init containers on a RestartNever pod [Conformance] | PASS | PASS |
| [sig-node] InitContainer [NodeConformance] should not start app containers and fail the pod if init containers fail on a RestartNever pod [Conformance] | PASS | PASS |
| [sig-node] InitContainer [NodeConformance] should not start app containers if init containers fail on a RestartAlways pod [Conformance] | FAIL | FAIL |
| [sig-node] Kubelet when scheduling a busybox command in a pod should print the output to logs [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Kubelet when scheduling a busybox command that always fails in a pod should be possible to delete [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Kubelet when scheduling a busybox command that always fails in a pod should have an terminated reason [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Kubelet when scheduling a read only busybox container should not write to root filesystem [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Kubelet when scheduling an agnhost Pod with hostAliases should write entries to /etc/hosts [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] KubeletManagedEtcHosts should test kubelet managed /etc/hosts file [NodeConformance] [Conformance] | FAIL | FAIL |
| [sig-node] Lease lease API should be available [Conformance] | PASS | PASS |
| [sig-node] NoExecuteTaintManager Multiple Pods [Serial] evicts pods with minTolerationSeconds [Disruptive] [Conformance] | PASS | PASS |
| [sig-node] NoExecuteTaintManager Single Pod [Serial] removing taint cancels eviction [Disruptive] [Conformance] | PASS | PASS |
| [sig-node] Node Lifecycle should run through the lifecycle of a node [Conformance] | PASS | PASS |
| [sig-node] Pod InPlace Resize Container burstable pods - extended 6 containers - various operations performed (including adding limits and requests) [MinimumKubeletVersion:1.34] [Conformance] | PASS | PASS |
| [sig-node] Pod InPlace Resize Container burstable pods - extended resize with equivalents [MinimumKubeletVersion:1.34] [Conformance] | PASS | PASS |
| [sig-node] Pod InPlace Resize Container guaranteed pods with multiple containers 3 containers - increase cpu & mem on c1, c2, decrease cpu & mem on c3 - net increase [MinimumKubeletVersion:1.34] [Conformance] | PASS | PASS |
| [sig-node] Pod InPlace Resize Container guaranteed pods with multiple containers 3 containers - increase cpu & mem on c1, decrease cpu & mem on c2, c3 - net decrease [MinimumKubeletVersion:1.34] [Conformance] | PASS | PASS |
| [sig-node] Pod InPlace Resize Container guaranteed pods with multiple containers 3 containers - increase: CPU (c1,c3), memory (c2, c3) ; decrease: CPU (c2) [MinimumKubeletVersion:1.34] [Conformance] | PASS | PASS |
| [sig-node] Pod InPlace Resize Container resize pod via the replace endpoint [MinimumKubeletVersion:1.34] [Conformance] | FAIL | FAIL |
| [sig-node] PodTemplates should delete a collection of pod templates [Conformance] | PASS | PASS |
| [sig-node] PodTemplates should replace a pod template [Conformance] | PASS | PASS |
| [sig-node] PodTemplates should run the lifecycle of PodTemplates [Conformance] | PASS | PASS |
| [sig-node] Pods Extended (pod generation) Pod Generation custom-set generation on new pods and graceful delete [Conformance] | PASS | PASS |
| [sig-node] Pods Extended (pod generation) Pod Generation issue 500 podspec updates and verify generation and observedGeneration eventually converge [MinimumKubeletVersion:1.34] [Conformance] | PASS | PASS |
| [sig-node] Pods Extended (pod generation) Pod Generation pod generation should start at 1 and increment per update [MinimumKubeletVersion:1.34] [Conformance] | PASS | PASS |
| [sig-node] Pods Extended Pods Set QOS Class should be set on Pods with matching resource requests and limits for memory and cpu [Conformance] | PASS | PASS |
| [sig-node] Pods should allow activeDeadlineSeconds to be updated [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Pods should be submitted and removed [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Pods should be updated [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Pods should contain environment variables for services [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Pods should delete a collection of pods [Conformance] | PASS | PASS |
| [sig-node] Pods should get a host IP [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Pods should patch a pod status [Conformance] | PASS | PASS |
| [sig-node] Pods should run through the lifecycle of Pods and PodStatus [Conformance] | PASS | PASS |
| [sig-node] Pods should support remote command execution over websockets [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Pods should support retrieving logs from the container over websockets [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] PreStop should call prestop when killing a pod [Conformance] | FAIL | FAIL |
| [sig-node] Probing container should *not* be restarted with a /healthz http liveness probe [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container should *not* be restarted with a GRPC liveness probe [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container should *not* be restarted with a exec "cat /tmp/health" liveness probe [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container should *not* be restarted with a tcp:8080 liveness probe [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container should be restarted with a /healthz http liveness probe [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container should be restarted with a GRPC liveness probe [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container should be restarted with a exec "cat /tmp/health" liveness probe [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container should have monotonically increasing restart count [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container with readiness probe should not be ready before initial delay and never restart [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Probing container with readiness probe that fails should never be ready and never restart [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] RuntimeClass should reject a Pod requesting a deleted RuntimeClass [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] RuntimeClass should reject a Pod requesting a non-existent RuntimeClass [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] RuntimeClass should schedule a Pod requesting a RuntimeClass and initialize its Overhead [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] RuntimeClass should schedule a Pod requesting a RuntimeClass without PodOverhead [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] RuntimeClass should support RuntimeClasses API operations [Conformance] | PASS | PASS |
| [sig-node] Secrets should be consumable as environment variable names variable names with various prefixes [Conformance] | PASS | PASS |
| [sig-node] Secrets should be consumable from pods in env vars [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Secrets should be consumable via the environment [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Secrets should fail to create secret due to empty secret key [Conformance] | PASS | PASS |
| [sig-node] Secrets should patch a secret [Conformance] | PASS | PASS |
| [sig-node] Security Context When creating a container with runAsUser should run the container with uid 65534 [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Security Context When creating a pod with privileged should run the container as unprivileged when false [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Security Context When creating a pod with readOnlyRootFilesystem should run the container with writable rootfs when readOnlyRootFilesystem=false [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Security Context should support container.SecurityContext.RunAsUser And container.SecurityContext.RunAsGroup [LinuxOnly] [Conformance] | PASS | PASS |
| [sig-node] Security Context should support pod.Spec.SecurityContext.RunAsUser And pod.Spec.SecurityContext.RunAsGroup [LinuxOnly] [Conformance] | PASS | PASS |
| [sig-node] Security Context when creating containers with AllowPrivilegeEscalation should not allow privilege escalation when false [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Sysctls [LinuxOnly] [NodeConformance] should reject invalid sysctls [Conformance] | PASS | PASS |
| [sig-node] Sysctls [LinuxOnly] [NodeConformance] should support sysctls [Environment:NotInUserNS] [Conformance] | PASS | PASS |
| [sig-node] Variable Expansion should allow composing env vars into new env vars [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Variable Expansion should allow substituting values in a container's args [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Variable Expansion should allow substituting values in a container's command [NodeConformance] [Conformance] | PASS | PASS |
| [sig-node] Variable Expansion should allow substituting values in a volume subpath [Conformance] | PASS | PASS |
| [sig-node] Variable Expansion should fail substituting values in a volume subpath with absolute path [Conformance] | PASS | PASS |
| [sig-node] Variable Expansion should fail substituting values in a volume subpath with backticks [Conformance] | PASS | PASS |
| [sig-node] Variable Expansion should succeed in writing subpaths in container [Conformance] | PASS | PASS |
| [sig-node] Variable Expansion should verify that a failing subpath expansion can be modified during the lifecycle of a container [Slow] [Conformance] | PASS | PASS |
| [sig-node] [DRA] CRUD Tests resource.k8s.io/v1 DeviceClass [Conformance] | PASS | PASS |
| [sig-node] [DRA] CRUD Tests resource.k8s.io/v1 ResourceClaim [Conformance] | PASS | PASS |
| [sig-node] [DRA] CRUD Tests resource.k8s.io/v1 ResourceClaimTemplate [Conformance] | PASS | PASS |
| [sig-node] [DRA] CRUD Tests resource.k8s.io/v1 ResourceSlice [Conformance] | PASS | PASS |
| [sig-scheduling] LimitRange should create a LimitRange with defaults and ensure pod has those defaults applied. [Conformance] | PASS | PASS |
| [sig-scheduling] LimitRange should list, patch and delete a LimitRange by collection [Conformance] | PASS | PASS |
| [sig-scheduling] SchedulerPredicates [Serial] validates resource limits of pods that are allowed to run [Conformance] | FAIL | FAIL |
| [sig-scheduling] SchedulerPredicates [Serial] validates that NodeSelector is respected if matching [Conformance] | PASS | PASS |
| [sig-scheduling] SchedulerPredicates [Serial] validates that NodeSelector is respected if not matching [Conformance] | FAIL | FAIL |
| [sig-scheduling] SchedulerPredicates [Serial] validates that there exists conflict between pods with same hostPort and protocol but one using 0.0.0.0 hostIP [Conformance] | PASS | PASS |
| [sig-scheduling] SchedulerPreemption [Serial] PreemptionExecutionPath runs ReplicaSets to verify preemption running path [Conformance] | FAIL | FAIL |
| [sig-scheduling] SchedulerPreemption [Serial] PriorityClass endpoints verify PriorityClass endpoints can be operated with different HTTP methods [Conformance] | PASS | PASS |
| [sig-scheduling] SchedulerPreemption [Serial] validates basic preemption works [Conformance] | FAIL | FAIL |
| [sig-scheduling] SchedulerPreemption [Serial] validates lower priority pod preemption by critical pod [Conformance] | FAIL | FAIL |
| [sig-scheduling] SchedulerPreemption [Serial] validates pod disruption condition is added to the preempted pod [Conformance] | FAIL | FAIL |
| [sig-storage] CSIInlineVolumes should run through the lifecycle of a CSIDriver [Conformance] | PASS | PASS |
| [sig-storage] CSIInlineVolumes should support CSIVolumeSource in Pod API [Conformance] | PASS | PASS |
| [sig-storage] CSINodes CSI Conformance should run through the lifecycle of a csinode [Conformance] | PASS | PASS |
| [sig-storage] CSIStorageCapacity should support CSIStorageCapacities API operations [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap binary data should be reflected in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap optional updates should be reflected in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap should be consumable from pods in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap should be consumable from pods in volume as non-root [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap should be consumable from pods in volume with defaultMode set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap should be consumable from pods in volume with mappings [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap should be consumable from pods in volume with mappings and Item mode set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap should be consumable from pods in volume with mappings as non-root [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap should be consumable in multiple volumes in the same pod [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap should be immutable if `immutable` field is set [Conformance] | PASS | PASS |
| [sig-storage] ConfigMap updates should be reflected in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should provide container's cpu limit [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should provide container's cpu request [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should provide container's memory limit [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should provide container's memory request [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should provide node allocatable (cpu) as default cpu limit if the limit is not set [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should provide node allocatable (memory) as default memory limit if the limit is not set [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should provide podname only [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should set DefaultMode on files [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should set mode on item file [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should update annotations on modification [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Downward API volume should update labels on modification [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes pod should support shared volumes between containers [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (non-root,0644,default) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (non-root,0644,tmpfs) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (non-root,0666,default) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (non-root,0666,tmpfs) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (non-root,0777,default) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (non-root,0777,tmpfs) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (root,0644,default) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (root,0644,tmpfs) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (root,0666,default) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (root,0666,tmpfs) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (root,0777,default) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes should support (root,0777,tmpfs) [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes volume on default medium should have the correct mode [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir volumes volume on tmpfs should have the correct mode [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] EmptyDir wrapper volumes should not cause race condition when used for configmaps [Serial] [Conformance] | FAIL | PASS |
| [sig-storage] EmptyDir wrapper volumes should not conflict [Conformance] | PASS | PASS |
| [sig-storage] PersistentVolumes CSI Conformance should apply changes to a pv/pvc status [Conformance] | FAIL | FAIL |
| [sig-storage] PersistentVolumes CSI Conformance should run through the lifecycle of a PV and a PVC [Conformance] | PASS | FAIL |
| [sig-storage] Projected combined should project all components that make up the projection API [Projection] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap optional updates should be reflected in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap should be consumable from pods in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap should be consumable from pods in volume as non-root [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap should be consumable from pods in volume with defaultMode set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap should be consumable from pods in volume with mappings [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap should be consumable from pods in volume with mappings and Item mode set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap should be consumable from pods in volume with mappings as non-root [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap should be consumable in multiple volumes in the same pod [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected configMap updates should be reflected in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should provide container's cpu limit [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should provide container's cpu request [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should provide container's memory limit [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should provide container's memory request [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should provide node allocatable (cpu) as default cpu limit if the limit is not set [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should provide node allocatable (memory) as default memory limit if the limit is not set [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should provide podname only [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should set DefaultMode on files [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should set mode on item file [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should update annotations on modification [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected downwardAPI should update labels on modification [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected secret optional updates should be reflected in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected secret should be consumable from pods in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected secret should be consumable from pods in volume as non-root with defaultMode and fsGroup set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected secret should be consumable from pods in volume with defaultMode set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected secret should be consumable from pods in volume with mappings [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected secret should be consumable from pods in volume with mappings and Item Mode set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Projected secret should be consumable in multiple volumes in a pod [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets optional updates should be reflected in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets should be able to mount in a volume regardless of a different secret existing with same name in different namespace [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets should be consumable from pods in volume [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets should be consumable from pods in volume as non-root with defaultMode and fsGroup set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets should be consumable from pods in volume with defaultMode set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets should be consumable from pods in volume with mappings [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets should be consumable from pods in volume with mappings and Item Mode set [LinuxOnly] [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets should be consumable in multiple volumes in a pod [NodeConformance] [Conformance] | PASS | PASS |
| [sig-storage] Secrets should be immutable if `immutable` field is set [Conformance] | PASS | PASS |
| [sig-storage] StorageClasses CSI Conformance should run through the lifecycle of a StorageClass [Conformance] | PASS | PASS |
| [sig-storage] Subpath Atomic writer volumes should support subpaths with configmap pod [Conformance] | PASS | PASS |
| [sig-storage] Subpath Atomic writer volumes should support subpaths with configmap pod with mountPath of existing file [Conformance] | PASS | PASS |
| [sig-storage] Subpath Atomic writer volumes should support subpaths with downward pod [Conformance] | PASS | PASS |
| [sig-storage] Subpath Atomic writer volumes should support subpaths with projected pod [Conformance] | PASS | PASS |
| [sig-storage] Subpath Atomic writer volumes should support subpaths with secret pod [Conformance] | PASS | PASS |
| [sig-storage] VolumeAttachment Conformance should apply changes to a volumeattachment status [Conformance] | PASS | PASS |
| [sig-storage] VolumeAttachment Conformance should run through the lifecycle of a VolumeAttachment [Conformance] | PASS | PASS |
| [sig-storage] VolumeAttributesClass [FeatureGate:VolumeAttributesClass] should run through the lifecycle of a VolumeAttributesClass [Conformance] | PASS | PASS |

