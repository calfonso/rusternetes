# Security Features in Rūsternetes

**Last Updated:** March 10, 2026

This document covers admission controllers, pod security standards, secrets encryption at rest, and audit logging.

**For authentication and authorization** (JWT tokens, RBAC, mTLS client certificates), see **[AUTHENTICATION.md](AUTHENTICATION.md)**.

## Table of Contents

- [Admission Controllers](#admission-controllers)
- [Pod Security Standards](#pod-security-standards)
- [Secrets Encryption at Rest](#secrets-encryption-at-rest)
- [Audit Logging](#audit-logging)
- [Configuration](#configuration)
- [Examples](#examples)
- [Best Practices](#best-practices)

## Admission Controllers

Admission controllers intercept API requests before they are persisted to etcd. They can validate requests (reject invalid requests) or mutate requests (modify requests before storage).

### Architecture

```
API Request
    ↓
Authentication
    ↓
Authorization (RBAC)
    ↓
Admission Controllers  ← You are here
    ↓
Validation
    ↓
Storage (etcd)
```

### Built-in Admission Controllers

Rusternetes includes several built-in admission controllers that are automatically enabled:

#### 1. NamespaceLifecycle

**Purpose**: Prevents creating resources in non-existent or terminating namespaces.

**Behavior**:
- Rejects requests to create resources in namespaces that don't exist
- Allows creating resources in system namespaces (kube-system, kube-public, default)

#### 2. ResourceQuota (Framework)

**Purpose**: Enforces resource consumption limits per namespace.

**Status**: Framework implemented, needs controller implementation.

**Future capabilities**:
- Enforce CPU/memory limits per namespace
- Enforce object count limits (pods, services, etc.)
- Prevent namespace from exceeding quota

#### 3. LimitRanger (Framework)

**Purpose**: Enforces min/max resource limits and provides defaults.

**Status**: Framework implemented, needs controller implementation.

**Future capabilities**:
- Set default requests/limits for containers
- Enforce min/max resource constraints
- Validate resource requests are within limits

#### 4. PodSecurityStandards

**Purpose**: Enforces Pod Security Standards at namespace level.

**See**: [Pod Security Standards](#pod-security-standards) section below.

### Custom Admission Controllers

You can implement custom admission controllers by implementing the `AdmissionController` trait:

```rust
use rusternetes_common::admission::*;
use async_trait::async_trait;

struct MyAdmissionController;

#[async_trait]
impl AdmissionController for MyAdmissionController {
    fn name(&self) -> &str {
        "MyAdmissionController"
    }

    async fn admit(&self, request: &AdmissionRequest) -> AdmissionResponse {
        // Validate the request
        if should_deny(request) {
            return AdmissionResponse::Deny("reason".to_string());
        }

        // Optionally mutate the request
        let patches = vec![
            PatchOperation {
                op: PatchOp::Add,
                path: "/metadata/labels/admitted".to_string(),
                value: Some(serde_json::json!("true")),
                from: None,
            }
        ];

        AdmissionResponse::AllowWithPatch(patches)
    }

    fn supports_operation(&self, operation: &Operation) -> bool {
        matches!(operation, Operation::Create | Operation::Update)
    }
}
```

### Admission Chain

Multiple admission controllers run in sequence via an `AdmissionChain`:

```rust
use rusternetes_common::admission::*;

let chain = AdmissionChain::new()
    .with_built_in_controllers()  // Adds all built-in controllers
    .with_controller(Arc::new(MyAdmissionController));

// Run all admission controllers
let response = chain.admit(&request).await;

if !response.is_allowed() {
    // Request denied
    println!("Denied: {}", response.deny_reason().unwrap());
}
```

## Pod Security Standards

Pod Security Standards define three levels of security restrictions for pods:

### Security Levels

#### 1. Privileged

**Use case**: Trusted workloads, system components

**Restrictions**: None (allows everything)

**When to use**:
- System pods (kube-system namespace)
- Monitoring agents
- Network plugins
- Storage drivers

#### 2. Baseline (Default)

**Use case**: General workloads that need some privileges

**Restrictions**:
- ❌ hostNetwork, hostPID, hostIPC
- ❌ Privileged containers
- ❌ Dangerous Linux capabilities (only allows safe baseline capabilities)

**Allowed capabilities** (baseline):
- AUDIT_WRITE
- CHOWN
- DAC_OVERRIDE
- FOWNER
- FSETID
- KILL
- MKNOD
- NET_BIND_SERVICE
- SETFCAP
- SETGID
- SETPCAP
- SETUID
- SYS_CHROOT

**When to use**:
- Web applications
- Databases
- Most application workloads

#### 3. Restricted

**Use case**: Security-critical workloads

**Restrictions**: All baseline restrictions, plus:
- ❌ Must set runAsNonRoot=true for all containers
- ❌ Must set allowPrivilegeEscalation=false
- ❌ Must drop ALL capabilities
- ❌ Must define seccomp profile
- ❌ Cannot use hostPath volumes

**When to use**:
- Financial applications
- PCI-DSS compliance workloads
- Multi-tenant environments
- Public-facing APIs

### Namespace-Level Enforcement

Pod Security Standards are enforced at the namespace level using labels:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: my-namespace
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

### Example: Restricted Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: secure-pod
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    fsGroup: 2000
    seccompProfile:
      type: RuntimeDefault

  containers:
  - name: app
    image: nginx:1.25-alpine
    securityContext:
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      capabilities:
        drop:
        - ALL
      seccompProfile:
        type: RuntimeDefault
```

### Violation Messages

When a pod violates a security standard, you get detailed error messages:

```
Pod violates restricted security standard:
- Container 'app' must set runAsNonRoot=true
- Container 'app' must set allowPrivilegeEscalation=false
- Container 'app' must drop ALL capabilities
- Container 'app' must define seccomp profile
```

## Secrets Encryption at Rest

Secrets (and other resources) can be encrypted at rest in etcd using multiple encryption providers.

### Encryption Providers

#### 1. AES-GCM (Recommended)

**Algorithm**: AES-256-GCM (Galois/Counter Mode)

**Features**:
- 256-bit encryption
- Authenticated encryption (AEAD)
- Unique nonce per encryption
- Production-ready

**Key generation**:
```bash
# Generate a 256-bit key
openssl rand -base64 32
```

**Example configuration**:
```yaml
kind: EncryptionConfig
apiVersion: v1
resources:
  - resources:
    - secrets
    providers:
    - aescbc:
        keys:
        - name: key1
          secret: <base64-encoded-key>
```

#### 2. Identity

**Purpose**: No encryption (passthrough)

**Use case**: Testing, migration

**Example**:
```yaml
kind: EncryptionConfig
apiVersion: v1
resources:
  - resources:
    - secrets
    providers:
    - identity: {}
```

#### 3. KMS (Framework)

**Status**: Framework implemented, AWS KMS stub available

**Future capabilities**:
- Integration with AWS KMS
- Automatic key rotation
- Envelope encryption
- Audit trail of key usage

### Encryption Configuration

Create an encryption configuration file:

```yaml
# encryption-config.yaml
kind: EncryptionConfig
apiVersion: v1
resources:
  - resources:
    - secrets
    providers:
    # Try key1 first
    - aescbc:
        keys:
        - name: key1
          secret: K7XSxhxZZ7Y5QZ0ckZjW8qY4b2X+J5GvP9N2bXxYqT8=
    # Then try key2 (for rotation)
    - aescbc:
        keys:
        - name: key2
          secret: dGVzdGtleWRhdGF0ZXN0a2V5ZGF0YXRlc3RrZXlkYQ==
    # Finally, try identity (for migration from unencrypted)
    - identity: {}

  - resources:
    - configmaps
    providers:
    - identity: {}  # Don't encrypt ConfigMaps
```

### Key Rotation

To rotate encryption keys:

1. Add new key to providers list (at the top)
2. Restart API server
3. Read and write all secrets to re-encrypt with new key:
   ```bash
   kubectl get secrets --all-namespaces -o json | \
     kubectl replace -f -
   ```
4. Remove old key from configuration

### Using Encryption in Code

```rust
use rusternetes_common::encryption::*;

// Load encryption config
let config_yaml = std::fs::read_to_string("encryption-config.yaml")?;
let config: EncryptionConfig = serde_yaml::from_str(&config_yaml)?;

// Create transformer
let transformer = EncryptionTransformer::from_config(config)?;

// Encrypt a secret
let plaintext = b"my-secret-data";
let ciphertext = transformer.encrypt_for_resource("secrets", plaintext)?;

// Decrypt a secret
let decrypted = transformer.decrypt_for_resource("secrets", &ciphertext)?;
assert_eq!(plaintext, decrypted.as_slice());
```

## Audit Logging

Audit logging tracks all API requests for security, compliance, and debugging.

### Audit Levels

#### 1. None

No audit logging.

#### 2. Metadata

Log request metadata only (no request/response bodies).

**Includes**:
- Request URI
- HTTP verb
- User information
- Resource being accessed
- Response status code

**Example use case**: Compliance logging without PII

#### 3. Request

Log metadata + request body (no response body).

**Includes**: Everything in Metadata, plus:
- Request body (JSON)

**Example use case**: Track what users are trying to create/update

#### 4. RequestResponse

Log everything (metadata + request body + response body).

**Includes**: Everything in Request, plus:
- Response body (JSON)

**Example use case**: Full audit trail for forensics

### Audit Stages

Each request goes through multiple stages:

1. **RequestReceived**: Request received by API server
2. **ResponseStarted**: Response headers sent
3. **ResponseComplete**: Response fully sent
4. **Panic**: Handler panicked during request processing

### Audit Event Format

Audit events follow Kubernetes audit.k8s.io/v1 format:

```json
{
  "apiVersion": "audit.k8s.io/v1",
  "kind": "Event",
  "level": "Metadata",
  "auditID": "550e8400-e29b-41d4-a716-446655440000",
  "stage": "ResponseComplete",
  "requestURI": "/api/v1/namespaces/default/pods",
  "verb": "create",
  "user": {
    "username": "alice",
    "uid": "alice-uid",
    "groups": ["system:authenticated"]
  },
  "objectRef": {
    "resource": "pods",
    "namespace": "default",
    "name": "my-pod",
    "apiVersion": "v1"
  },
  "responseStatus": {
    "code": 201
  },
  "requestReceivedTimestamp": "2026-03-10T10:00:00Z",
  "stageTimestamp": "2026-03-10T10:00:00.123Z"
}
```

### Configuration

Create an audit policy:

```rust
use rusternetes_common::audit::*;

let policy = AuditPolicy {
    level: AuditLevel::Metadata,
    log_requests: true,
    log_responses: true,
    log_metadata: true,
};
```

Create an audit backend:

```rust
// File backend
let backend = Arc::new(
    FileAuditBackend::new("/var/log/kubernetes/audit.log".to_string()).await?
);

// Create logger
let logger = AuditLogger::new(backend, policy);
```

Log requests and responses:

```rust
// Log request
let audit_id = logger.log_request(
    "/api/v1/pods".to_string(),
    "GET".to_string(),
    user_info,
    None,
).await;

// ... process request ...

// Log response
logger.log_response(
    audit_id,
    "/api/v1/pods".to_string(),
    "GET".to_string(),
    user_info,
    None,
    200,
    None,
).await;
```

### Audit Log Files

Audit logs are written as JSON Lines (one JSON object per line):

```bash
# View audit logs
tail -f /var/log/kubernetes/audit.log

# Parse with jq
cat /var/log/kubernetes/audit.log | jq '.user.username'

# Find all denied requests
cat /var/log/kubernetes/audit.log | jq 'select(.responseStatus.code >= 400)'

# Find all actions by user
cat /var/log/kubernetes/audit.log | jq 'select(.user.username == "alice")'
```

## Configuration

### API Server Configuration

Add security flags to the API server:

```bash
./rusternetes-api-server \
  --bind-address 0.0.0.0:6443 \
  --etcd-servers http://localhost:2379 \
  --tls \
  --tls-self-signed \
  --enable-admission-plugins "NamespaceLifecycle,PodSecurityStandards" \
  --encryption-provider-config /etc/kubernetes/encryption-config.yaml \
  --audit-log-path /var/log/kubernetes/audit.log \
  --audit-policy-file /etc/kubernetes/audit-policy.yaml
```

## Examples

### Example 1: Enforcing Restricted Security in a Namespace

```yaml
# Create namespace with restricted security
apiVersion: v1
kind: Namespace
metadata:
  name: production
  labels:
    pod-security.kubernetes.io/enforce: restricted

---

# This pod will be ALLOWED
apiVersion: v1
kind: Pod
metadata:
  name: secure-app
  namespace: production
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    seccompProfile:
      type: RuntimeDefault
  containers:
  - name: app
    image: nginx:1.25-alpine
    securityContext:
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      capabilities:
        drop:
        - ALL
      seccompProfile:
        type: RuntimeDefault

---

# This pod will be DENIED (privileged)
apiVersion: v1
kind: Pod
metadata:
  name: insecure-app
  namespace: production
spec:
  containers:
  - name: app
    image: nginx
    securityContext:
      privileged: true  # VIOLATION!
```

### Example 2: Encrypting Secrets

```yaml
# encryption-config.yaml
kind: EncryptionConfig
apiVersion: v1
resources:
  - resources:
    - secrets
    providers:
    - aescbc:
        keys:
        - name: key1
          secret: K7XSxhxZZ7Y5QZ0ckZjW8qY4b2X+J5GvP9N2bXxYqT8=

---

# Create a secret (will be encrypted in etcd)
apiVersion: v1
kind: Secret
metadata:
  name: my-secret
type: Opaque
data:
  password: cGFzc3dvcmQ=  # base64("password")
```

### Example 3: Audit Logging for Compliance

```rust
// Initialize audit logger
let backend = Arc::new(
    FileAuditBackend::new("/var/log/kubernetes/audit.log".to_string()).await?
);

let policy = AuditPolicy {
    level: AuditLevel::Metadata,  // No PII in logs
    log_requests: true,
    log_responses: true,
    log_metadata: true,
};

let logger = AuditLogger::new(backend, policy);

// All API requests are now logged
```

## Best Practices

### Admission Controllers

1. **Enable built-in controllers**: Always enable NamespaceLifecycle and PodSecurityStandards
2. **Order matters**: Controllers run sequentially; put mutation before validation
3. **Error messages**: Provide clear, actionable error messages in denials
4. **Performance**: Keep admission logic fast (< 100ms per controller)

### Pod Security Standards

1. **Start with baseline**: Use baseline for most namespaces
2. **Restricted for sensitive data**: Use restricted for namespaces with sensitive workloads
3. **Privileged sparingly**: Only use privileged for system namespaces
4. **Test before enforcing**: Use `warn` mode before `enforce` mode
5. **Document exceptions**: If you need privileged pods, document why

### Secrets Encryption

1. **Always encrypt secrets**: Use AES-GCM for production
2. **Rotate keys regularly**: Rotate encryption keys every 90 days
3. **Store keys securely**: Never commit encryption keys to git
4. **Use multiple keys**: Keep old keys during rotation period
5. **Monitor access**: Audit who accesses encryption keys

### Audit Logging

1. **Start with Metadata**: Avoid logging sensitive data
2. **Rotate logs**: Use logrotate to prevent disk fill
3. **Centralize logs**: Send audit logs to SIEM (Splunk, Elasticsearch)
4. **Alert on anomalies**: Monitor for unusual patterns
5. **Retention policy**: Keep audit logs for compliance period (90 days, 1 year, etc.)

## Security Checklist

Before going to production:

- [ ] Enable TLS on API server
- [ ] Enable RBAC authorization
- [ ] Enable admission controllers (at minimum: NamespaceLifecycle, PodSecurityStandards)
- [ ] Configure Pod Security Standards (baseline or restricted)
- [ ] Enable secrets encryption at rest (AES-GCM)
- [ ] Enable audit logging (at least Metadata level)
- [ ] Rotate encryption keys regularly
- [ ] Review and update RBAC policies
- [ ] Monitor audit logs for anomalies
- [ ] Test disaster recovery procedures

## Troubleshooting

### Admission Controllers

**Problem**: Pods are being rejected with unclear errors

**Solution**:
- Check admission controller logs: `podman logs rusternetes-api-server | grep admission`
- Verify namespace labels for Pod Security Standards
- Test pod manifest against security level manually

**Problem**: Custom admission controller not being called

**Solution**:
- Verify controller is added to AdmissionChain
- Check controller's `supports_operation()` method
- Ensure controller is not panicking (check logs)

### Secrets Encryption

**Problem**: Cannot read secrets after enabling encryption

**Solution**:
- Verify encryption config is valid YAML
- Check that key is base64-encoded
- Ensure API server has read access to encryption config file
- Check API server logs for encryption errors

**Problem**: Key rotation not working

**Solution**:
- Ensure new key is first in providers list
- Restart API server after config change
- Re-write all secrets to re-encrypt with new key
- Remove old key only after all secrets are re-encrypted

### Audit Logging

**Problem**: Audit log file not being created

**Solution**:
- Check file path is writable by API server
- Verify parent directory exists
- Check disk space
- Review API server logs for permission errors

**Problem**: Audit logs too large

**Solution**:
- Reduce audit level (RequestResponse → Request → Metadata)
- Implement log rotation (logrotate)
- Filter out high-volume endpoints
- Send logs to external system instead of files

## Related Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture
- [QUICKSTART.md](QUICKSTART.md) - Cluster deployment guide
- [AUTHENTICATION.md](AUTHENTICATION.md) - Authentication, RBAC, and authorization
- [TLS_GUIDE.md](TLS_GUIDE.md) - TLS configuration

## Further Reading

- [Kubernetes Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
- [Kubernetes Admission Controllers](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/)
- [Encrypting Secret Data at Rest](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)
- [Kubernetes Auditing](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/)
