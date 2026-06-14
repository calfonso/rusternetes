//! Scheduler API-mode contract tests.
//!
//! ## Classification of the original 22 `self.storage.<op>` sites
//!
//! Every storage call in `scheduler.rs` was replaced by a `self.data.<method>`
//! dispatch through `DataPlane` (`src/data_plane.rs`). In API mode reads come
//! from the pods/nodes informer stores, the pod watch becomes the pods
//! reflector's `subscribe()` channel, binds POST the binding subresource, other
//! writes PUT the pod (status/spec) and events POST. No raw storage call
//! survives in API mode. Mapping (scheduler.rs line in fork/main → replacement):
//!
//! | site (orig line) | original call                         | DataPlane replacement                |
//! |------------------|---------------------------------------|--------------------------------------|
//! |  84  | `storage.get(pod_key)` (retry helper)            | `data.get_pod(ns, name)`             |
//! |  93  | `storage.update(pod_key, pod)` (retry helper)    | `data.update_pod(ns, name, pod)`     |
//! | 125  | `storage.watch(/registry/pods)` (run loop)       | pods reflector `subscribe()` channel |
//! | 185  | `storage.list(pods)` (enqueue_all)               | `data.list_pods()`                   |
//! | 223  | `storage.get(pod_key)` (try_schedule_pod)        | `data.get_pod(ns, name)`             |
//! | 255  | `storage.list(nodes)` (try_schedule_pod)         | `data.list_nodes()`                  |
//! | 259  | `storage.list(pods)` (try_schedule_pod)          | `data.list_pods()`                   |
//! | 349  | `storage.list(pods)` (schedule_pending_pods)     | `data.list_pods()`                   |
//! | 399  | `storage.list(nodes)` (schedule_pending_pods)    | `data.list_nodes()`                  |
//! | 476  | `storage.list(pods)` (re-read after bind)        | `data.list_pods()`                   |
//! | 516  | `storage.list(pods)` (re-read after preempt)     | `data.list_pods()`                   |
//! | 518  | `storage.list(nodes)` (fresh nodes)              | `data.list_nodes()`                  |
//! | 524  | `storage.get::<Pod>(pod_key)` (fresh pod)        | `data.get_pod(ns, name)`             |
//! | 581  | `storage.list(pods)` (re-read tail)              | `data.list_pods()`                   |
//! | 939  | `storage.update(key, pod)` (bind_pod_to_node)    | `data.bind(ns, pod, node)` (binding) |
//! | 947  | `storage.get(key)` (bind conflict re-read)       | inside `data.bind` Storage retry     |
//! | 971  | `storage.update(key, fresh_pod)` (bind retry)    | inside `data.bind` Storage retry     |
//! | 1072 | `storage.list(pods)` (evict_pod scan)            | `data.list_pods()`                   |
//! | 1111 | `storage.update(key, pod)` (evict_pod write)     | `data.update_pod(ns, name, pod)`     |
//! | 1127 | `storage.list(priorityclasses)`                  | `data.list_priority_classes()`       |
//! | 1341 | `storage.get(claim_key)` (DRA ResourceClaim)     | `data.get_resource_claim(ns, name)`  |
//! | 1443 | `storage.list(resourceslices)` (DRA)             | `data.list_resource_slices()`        |
//!
//! Events (`emit_pod_event`): storage mode routes through the unified
//! `EventRecorder` (correlator); API mode POSTs via `ClientEventRecorder`
//! through `data.emit_event_api`.

use rusternetes_scheduler::data_plane::binding_body;

#[test]
fn binding_body_matches_apiserver_contract() {
    // create_binding (api-server pod_subresources.rs) requires target.name;
    // this is the wire contract test.
    let b = binding_body("web-1", "node-2");
    assert_eq!(b["target"]["name"], "node-2");
    assert_eq!(b["metadata"]["name"], "web-1");
    assert_eq!(b["apiVersion"], "v1");
    assert_eq!(b["kind"], "Binding");
    assert_eq!(b["target"]["kind"], "Node");
}
