# Webhook Integration - Implementation Complete


> **Tip:** You can manage related resources through the [web console](CONSOLE_USER_GUIDE.md).
## Summary

The admission webhook infrastructure has been successfully integrated into the Rusternetes API server request pipeline. Webhooks are now called during resource creation and update operations.

## What Was Implemented

### 1. Webhook Manager in ApiServerState ✓

The `AdmissionWebhookManager` has been added to `ApiServerState`:

**File:** `crates/api-server/src/state.rs`

```rust
pub struct ApiServerState {
    pub storage: Arc<EtcdStorage>,
    pub token_manager: Arc<TokenManager>,
    pub authorizer: Arc<dyn Authorizer>,
    pub metrics: Arc<MetricsRegistry>,
    pub skip_auth: bool,
    pub ip_allocator: Arc<ClusterIPAllocator>,
    pub webhook_manager: Arc<AdmissionWebhookManager<EtcdStorage>>,  // NEW
}
```

The webhook manager is automatically initialized when the API server starts.

### 2. Mutating Webhooks Integration ✓

Mutating webhooks are now called **before** resource creation/update in the Pod handler:

**File:** `crates/api-server/src/handlers/pod.rs`

**Flow:**
1. Authorization check
2. **→ Run mutating webhooks** (new)
3. Apply mutations from webhook responses
4. LimitRange admission
5. ResourceQuota admission
6. **→ Run validating webhooks** (new)
7. Persist to storage

### 3. Validating Webhooks Integration ✓

Validating webhooks are called **after** mutations but **before** persistence:

```rust
// Run validating webhooks AFTER mutations and other admission checks
let validation_response = state
    .webhook_manager
    .run_validating_webhooks(
        &Operation::Create,
        &gvk,
        &gvr,
        Some(&namespace),
        &pod.metadata.name,
        Some(final_pod_value),
        None,
        &user_info,
    )
    .await?;
```

### 4. Webhook Response Handling ✓

Both webhook types properly handle:
- **Allow**: Request proceeds
- **Deny**: Returns 403 Forbidden with custom message
- **Patches**: JSON patches are applied to the resource

Example denial handling:
```rust
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
        }
    }
}
```

## Request Flow

Here's the complete admission flow for Pod creation:

```
Client Request (POST /api/v1/namespaces/{ns}/pods)
    ↓
Authentication (JWT token validation)
    ↓
Authorization (RBAC check)
    ↓
=== MUTATING WEBHOOKS ===
    ↓
Load MutatingWebhookConfigurations from etcd
    ↓
For each matching webhook:
  - Call webhook with AdmissionReview request
  - Handle failure policy (Fail/Ignore)
  - Apply JSON patches to object
  - Check for denials
    ↓
=== BUILT-IN ADMISSION ===
    ↓
LimitRange admission (set defaults, validate limits)
    ↓
ResourceQuota admission (check quota)
    ↓
=== VALIDATING WEBHOOKS ===
    ↓
Load ValidatingWebhookConfigurations from etcd
    ↓
For each matching webhook:
  - Call webhook with AdmissionReview request
  - Handle failure policy (Fail/Ignore)
  - Check for denials
    ↓
=== PERSISTENCE ===
    ↓
Set UID and creation timestamp
    ↓
Persist to etcd
    ↓
Return response to client
```

## Files Modified

1. **`crates/api-server/src/state.rs`**
   - Added `webhook_manager` field to `ApiServerState`
   - Initialize webhook manager with storage

2. **`crates/api-server/src/main.rs`**
   - Added `mod admission_webhook;` declaration

3. **`crates/api-server/src/handlers/pod.rs`**
   - Integrated webhook calls in `create()` function
   - Integrated webhook calls in `update()` function
   - Convert between `auth::UserInfo` and `admission::UserInfo`

## How It Works

### Webhook Matching

The webhook manager uses the configuration rules to determine which webhooks apply:

```rust
fn webhook_matches(
    &self,
    rules: &[RuleWithOperations],
    operation: &Operation,
    gvk: &GroupVersionKind,
    gvr: &GroupVersionResource,
    namespace: Option<&str>,
) -> bool
```

Checks:
- Operation type (CREATE, UPDATE, DELETE, etc.)
- API group (e.g., "", "apps", "batch")
- API version (e.g., "v1")
- Resource type (e.g., "pods", "deployments")
- Scope (Namespaced vs Cluster)

### Failure Handling

Webhooks can specify a `failurePolicy`:

- **`Fail`** (default): Request is rejected if webhook fails
- **`Ignore`**: Request continues even if webhook fails

Example from code:
```rust
match failure_policy {
    FailurePolicy::Ignore => {
        warn!("Webhook {} failed but FailurePolicy is Ignore: {}", webhook.name, e);
        Ok(AdmissionReviewResponse::allow(request.uid.clone()))
    }
    FailurePolicy::Fail => {
        error!("Webhook {} failed with FailurePolicy Fail: {}", webhook.name, e);
        Err(e)
    }
}
```

### JSON Patch Application

Mutating webhooks can return JSON patches that are automatically applied:

```rust
// Apply patches to object
if let Some(ref mut obj) = object {
    for patch in &patches {
        apply_json_patch(obj, patch)?;
    }
}
```

Supported operations:
- `add`: Add a field
- `remove`: Remove a field
- `replace`: Replace a field value

## Testing

### 1. Apply Example Webhook Configurations

```bash
# Apply validating webhook
kubectl apply -f examples/admission-webhooks/validating-webhook.yaml

# Apply mutating webhook
kubectl apply -f examples/admission-webhooks/mutating-webhook.yaml
```

### 2. Create a Pod to Trigger Webhooks

```bash
kubectl create -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  namespace: default
spec:
  containers:
  - name: nginx
    image: nginx:latest
EOF
```

### 3. Check Logs

The API server will log webhook activity:

```
INFO Running mutating webhook example-webhook for Pod/test-pod
INFO Pod mutated by webhooks: default/test-pod
INFO Running validating webhook security-policy for Pod/test-pod
INFO Validating webhooks passed for pod default/test-pod
```

## Next Steps

To apply webhooks to other resources (Deployments, Services, etc.):

1. Follow the same pattern used in `pod.rs`
2. Call `run_mutating_webhooks()` before admission checks
3. Call `run_validating_webhooks()` after mutations
4. Handle responses (allow/deny/patch)

Example template:
```rust
// Convert user info
let user_info = rusternetes_common::admission::UserInfo {
    username: auth_ctx.user.username.clone(),
    uid: auth_ctx.user.uid.clone(),
    groups: auth_ctx.user.groups.clone(),
};

// Define GVK/GVR for your resource
let gvk = GroupVersionKind {
    group: "apps".to_string(),
    version: "v1".to_string(),
    kind: "Deployment".to_string(),
};

// Run mutating webhooks
let (mutation_response, mutated_value) = state
    .webhook_manager
    .run_mutating_webhooks(/* ... */)
    .await?;

// Handle response and apply mutations
// ...

// Run validating webhooks
let validation_response = state
    .webhook_manager
    .run_validating_webhooks(/* ... */)
    .await?;

// Handle validation response
```

## Architecture

```
┌─────────────────────────────────────────┐
│         API Server Request              │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│      AdmissionWebhookManager            │
│  (manages webhook configurations)       │
└─────────────────┬───────────────────────┘
                  │
      ┌───────────┴───────────┐
      │                       │
      ▼                       ▼
┌─────────────┐      ┌─────────────────┐
│  Mutating   │      │   Validating    │
│  Webhooks   │      │    Webhooks     │
└─────┬───────┘      └────────┬────────┘
      │                       │
      ▼                       ▼
┌──────────────────────────────────────┐
│    AdmissionWebhookClient            │
│  (HTTP client for calling webhooks)  │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│      External Webhook Server         │
│  (validates/mutates AdmissionReview) │
└──────────────────────────────────────┘
```

## References

- Implementation: `crates/api-server/src/admission_webhook.rs`
- Integration: `crates/api-server/src/handlers/pod.rs`
- Examples: `examples/admission-webhooks/`
- Documentation: `examples/admission-webhooks/README.md`
