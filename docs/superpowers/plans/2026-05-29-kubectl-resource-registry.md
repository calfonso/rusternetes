# kubectl discovery-driven resource registry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the per-command hardcoded `match kind { … }` tables in the rusternetes `kubectl` with a single discovery-driven RESTMapper that resolves any kind the api-server serves, operating on `serde_json::Value`.

**Architecture:** A new `discovery` module fetches the api-server's aggregated discovery (`APIGroupDiscoveryList` v2) once per run and builds a `RestMapper` (kind/plural/singular/short-name → `ResourceMapping{group,version,plural,namespaced,…}`). A new `ops` module performs generic `Value`-based apply/get/delete against a `ResourceMapping`. Each command resolves through the mapper instead of its own table.

**Tech Stack:** Rust, `reqwest` (via existing `ApiClient`), `serde_json`, `serde_yaml`, `anyhow`, `tokio`, design spec at `docs/superpowers/specs/2026-05-29-kubectl-resource-registry-design.md`.

**Reference — aggregated discovery JSON shape** (verified against live api-server 2026-05-29; same for `/api` and `/apis`):

```json
{
  "kind": "APIGroupDiscoveryList",
  "items": [
    {
      "metadata": { "name": "apps" },        // "" for core group
      "versions": [
        {
          "version": "v1",
          "resources": [
            {
              "resource": "deployments",                       // plural
              "responseKind": { "group": "apps", "kind": "Deployment", "version": "v1" },
              "scope": "Namespaced",                            // or "Cluster"
              "singularResource": "deployment",
              "shortNames": ["deploy"],                         // may be null/absent
              "verbs": ["create","delete","get","list","patch","update","watch"]
            }
          ]
        }
      ]
    }
  ]
}
```

---

## File Structure

- **Create** `crates/kubectl/src/discovery.rs` — `ResourceMapping`, `RestMapper`, discovery parsing + fetch. One responsibility: kind→resource resolution.
- **Create** `crates/kubectl/src/ops.rs` — generic `Value`-based resource operations (`apply_value`, `get_value`, `list_value`, `delete_value`) and `build_path`. One responsibility: turn a `ResourceMapping` + verb into an HTTP call.
- **Create** `crates/kubectl/tests/fixtures/aggregated-discovery.json` — recorded discovery for fast unit tests.
- **Modify** `crates/kubectl/src/client.rs` — add `get_raw_with_accept` (GET with a custom `Accept` header, returning the parsed JSON `Value`).
- **Modify** `crates/kubectl/src/main.rs` — register `mod discovery; mod ops;`.
- **Modify** `crates/kubectl/src/commands/apply.rs`, `get.rs`, `delete.rs`, `diff.rs`, `create.rs`, `set.rs` — delete private kind tables, resolve via `RestMapper`.
- **Modify** `scripts/bootstrap-cluster.sh` — replace the EndpointSlice curl workaround with `kubectl apply`.

Tasks 1–4 build the foundation (no behaviour change). Tasks 5–10 migrate one command each. Task 11 adds the parity test. Task 12 cleans up bootstrap + docs.

---

## Task 1: `ResourceMapping` + discovery parsing (pure function)

**Files:**
- Create: `crates/kubectl/src/discovery.rs`
- Create: `crates/kubectl/tests/fixtures/aggregated-discovery.json`
- Modify: `crates/kubectl/src/main.rs` (add `mod discovery;`)

- [ ] **Step 1: Record the discovery fixture**

Run (api-server must be up — `docker compose -f compose.yml -f compose.dind.yml up -d`):

```bash
ACC='Accept: application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json'
mkdir -p crates/kubectl/tests/fixtures
curl -sk -H "$ACC" https://localhost:6443/apis > crates/kubectl/tests/fixtures/aggregated-discovery.json
# sanity: must say APIGroupDiscoveryList
head -c 120 crates/kubectl/tests/fixtures/aggregated-discovery.json
```

Expected: file begins with `{"kind":"APIGroupDiscoveryList"` (or similar).

- [ ] **Step 2: Write the failing test**

Create `crates/kubectl/src/discovery.rs`:

```rust
use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

/// A resolved mapping from a resource (by any of its names) to the
/// information every kubectl verb needs to build an API path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceMapping {
    pub group: String,   // "" for the core group
    pub version: String, // preferred version, e.g. "v1"
    pub kind: String,    // "Deployment"
    pub plural: String,  // "deployments"
    pub singular: String,
    pub namespaced: bool,
    pub verbs: Vec<String>,
    pub short_names: Vec<String>,
}

/// Parse an aggregated-discovery (`APIGroupDiscoveryList`) JSON document into a
/// flat list of resource mappings. Subresources (those whose `resource` name
/// contains '/') are skipped — they are not independently addressable kinds.
pub fn parse_aggregated_discovery(doc: &Value) -> Result<Vec<ResourceMapping>> {
    let mut out = Vec::new();
    let items = doc.get("items").and_then(|i| i.as_array());
    let Some(items) = items else { return Ok(out) };
    for group in items {
        let group_name = group
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let Some(versions) = group.get("versions").and_then(|v| v.as_array()) else {
            continue;
        };
        for ver in versions {
            let version = ver.get("version").and_then(|v| v.as_str()).unwrap_or("");
            let Some(resources) = ver.get("resources").and_then(|r| r.as_array()) else {
                continue;
            };
            for r in resources {
                let plural = r.get("resource").and_then(|v| v.as_str()).unwrap_or("");
                if plural.is_empty() || plural.contains('/') {
                    continue; // skip subresources / malformed
                }
                let kind = r
                    .pointer("/responseKind/kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let namespaced = r.get("scope").and_then(|v| v.as_str()) == Some("Namespaced");
                let singular = r
                    .get("singularResource")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&kind.to_lowercase().as_str().to_string())
                    .to_string();
                let short_names = r
                    .get("shortNames")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                let verbs = r
                    .get("verbs")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                out.push(ResourceMapping {
                    group: group_name.clone(),
                    version: version.to_string(),
                    kind,
                    plural: plural.to_string(),
                    singular,
                    namespaced,
                    verbs,
                    short_names,
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Value {
        let raw = include_str!("../tests/fixtures/aggregated-discovery.json");
        serde_json::from_str(raw).expect("fixture parses")
    }

    #[test]
    fn parses_core_and_grouped_kinds() {
        let mappings = parse_aggregated_discovery(&fixture()).unwrap();
        let dep = mappings
            .iter()
            .find(|m| m.kind == "Deployment")
            .expect("Deployment present");
        assert_eq!(dep.group, "apps");
        assert_eq!(dep.version, "v1");
        assert_eq!(dep.plural, "deployments");
        assert!(dep.namespaced);

        let ns = mappings
            .iter()
            .find(|m| m.kind == "Namespace")
            .expect("Namespace present");
        assert_eq!(ns.group, ""); // core
        assert!(!ns.namespaced);
    }

    #[test]
    fn skips_subresources() {
        let mappings = parse_aggregated_discovery(&fixture()).unwrap();
        assert!(mappings.iter().all(|m| !m.plural.contains('/')));
    }
}
```

Add to `crates/kubectl/src/main.rs` near the other `mod` lines:

```rust
mod discovery;
```

- [ ] **Step 3: Run the test to verify it fails, then passes**

Note: the test body and implementation are added together (the function is small and the test is the spec). The `unwrap_or(&kind.to_lowercase()…)` line in Step 2 will not compile as written — fix it now:

Replace the `singular` block with:

```rust
                let singular = r
                    .get("singularResource")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .unwrap_or_else(|| kind.to_lowercase());
```

Run: `cargo test -p rusternetes-kubectl discovery::tests -- --nocapture`
Expected: PASS (2 tests).

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/kubectl/src/discovery.rs crates/kubectl/src/main.rs crates/kubectl/tests/fixtures/aggregated-discovery.json
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "feat(kubectl): parse aggregated discovery into ResourceMapping"
```

---

## Task 2: `RestMapper` lookup + ambiguity rule

**Files:**
- Modify: `crates/kubectl/src/discovery.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/kubectl/src/discovery.rs` (above the `#[cfg(test)]` block):

```rust
/// Resolves a user-supplied resource reference (plural, singular, short name,
/// or Kind) to a single `ResourceMapping`.
pub struct RestMapper {
    mappings: Vec<ResourceMapping>,
    by_key: HashMap<String, usize>, // lowercased key -> index, first writer wins
}

impl RestMapper {
    pub fn new(mappings: Vec<ResourceMapping>) -> Self {
        // Order so the core group is preferred on key collisions: stable sort
        // with core ("") first. Insertion is first-writer-wins, so core entries
        // claim shared keys (e.g. "events", "Event") ahead of grouped ones.
        let mut mappings = mappings;
        mappings.sort_by(|a, b| {
            (a.group != "")
                .cmp(&(b.group != ""))
                .then(a.group.cmp(&b.group))
                .then(a.plural.cmp(&b.plural))
        });
        let mut by_key = HashMap::new();
        for (i, m) in mappings.iter().enumerate() {
            let mut keys = vec![
                m.plural.to_lowercase(),
                m.singular.to_lowercase(),
                m.kind.to_lowercase(),
            ];
            keys.extend(m.short_names.iter().map(|s| s.to_lowercase()));
            for k in keys {
                by_key.entry(k).or_insert(i); // first writer (core) wins
            }
        }
        Self { mappings, by_key }
    }

    /// Resolve by plural / singular / short-name / Kind (case-insensitive).
    pub fn resolve(&self, reference: &str) -> Option<&ResourceMapping> {
        self.by_key
            .get(&reference.to_lowercase())
            .map(|&i| &self.mappings[i])
    }

    pub fn all(&self) -> &[ResourceMapping] {
        &self.mappings
    }
}
```

Add tests inside the existing `#[cfg(test)] mod tests`:

```rust
    fn mapper() -> RestMapper {
        RestMapper::new(parse_aggregated_discovery(&fixture()).unwrap())
    }

    #[test]
    fn resolves_by_every_key_form() {
        let m = mapper();
        assert_eq!(m.resolve("deployments").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("deployment").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("Deployment").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("deploy").unwrap().kind, "Deployment"); // short name
        assert_eq!(m.resolve("DEPLOY").unwrap().kind, "Deployment"); // case-insensitive
    }

    #[test]
    fn unknown_resolves_to_none() {
        assert!(mapper().resolve("nonexistentthing").is_none());
    }

    #[test]
    fn event_prefers_core_group() {
        // "Event" exists in both core and events.k8s.io; core wins.
        let m = mapper();
        assert_eq!(m.resolve("Event").unwrap().group, "");
    }
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p rusternetes-kubectl discovery::tests -- --nocapture`
Expected: PASS (all tests). If `deploy` short name is absent in your fixture, replace that assertion with a short name present in the fixture (check with `grep -o '"shortNames":\[[^]]*\]' crates/kubectl/tests/fixtures/aggregated-discovery.json | sort -u`).

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/kubectl/src/discovery.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "feat(kubectl): RestMapper resolve with core-group preference"
```

---

## Task 3: `ApiClient::get_raw_with_accept` + `RestMapper::from_server`

**Files:**
- Modify: `crates/kubectl/src/client.rs`
- Modify: `crates/kubectl/src/discovery.rs`

- [ ] **Step 1: Add the client method**

In `crates/kubectl/src/client.rs`, inside `impl ApiClient` (after `get_text`, around line 305), add:

```rust
    /// GET a path with a custom `Accept` header, returning the parsed JSON.
    /// Used for aggregated API discovery (`APIGroupDiscoveryList`).
    pub async fn get_raw_with_accept(
        &self,
        path: &str,
        accept: &str,
    ) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.get_base_url(), path);
        let mut req = self.client.get(&url).header("Accept", accept);
        if let Some(token) = self.get_token() {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            anyhow::bail!("discovery request to {url} failed: {status}: {text}");
        }
        Ok(serde_json::from_str(&text)?)
    }
```

Note: confirm the field name of the inner `reqwest::Client` on `ApiClient` (read `crates/kubectl/src/client.rs:17-60`). If it is not `self.client`, adjust. If `ApiClient` already exposes a builder/helper for authenticated requests, prefer reusing it over re-reading the token.

- [ ] **Step 2: Add `RestMapper::from_server`**

In `crates/kubectl/src/discovery.rs`, add to `impl RestMapper`:

```rust
    const AGG_ACCEPT: &'static str =
        "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json";

    /// Build a mapper from the api-server's aggregated discovery. Fetches both
    /// the core group (`/api`) and all named groups (`/apis`) — one HTTP call
    /// each — and merges them.
    pub async fn from_server(client: &crate::client::ApiClient) -> anyhow::Result<Self> {
        use anyhow::Context;
        let core = client
            .get_raw_with_accept("/api", Self::AGG_ACCEPT)
            .await
            .context("unable to fetch core API discovery")?;
        let apis = client
            .get_raw_with_accept("/apis", Self::AGG_ACCEPT)
            .await
            .context("unable to fetch API group discovery")?;
        let mut mappings = parse_aggregated_discovery(&core)?;
        mappings.extend(parse_aggregated_discovery(&apis)?);
        Ok(Self::new(mappings))
    }
```

- [ ] **Step 3: Verify it compiles and unit tests still pass**

Run: `cargo test -p rusternetes-kubectl discovery::tests`
Expected: PASS. (No new unit test here — `from_server` needs a live server and is exercised by the parity test in Task 11.)

Run: `cargo clippy -p rusternetes-kubectl --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/kubectl/src/client.rs crates/kubectl/src/discovery.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "feat(kubectl): fetch aggregated discovery to build RestMapper"
```

---

## Task 4: `ops` module — `build_path` + generic Value operations

**Files:**
- Create: `crates/kubectl/src/ops.rs`
- Modify: `crates/kubectl/src/main.rs` (add `mod ops;`)

- [ ] **Step 1: Write the failing test**

Create `crates/kubectl/src/ops.rs`:

```rust
use crate::client::ApiClient;
use crate::discovery::ResourceMapping;
use anyhow::{Context, Result};
use serde_json::Value;

/// Build the API path for a resource. When `name` is `None` the collection
/// path is returned; otherwise the item path.
pub fn build_path(m: &ResourceMapping, namespace: Option<&str>, name: Option<&str>) -> String {
    let base = if m.group.is_empty() {
        format!("/api/{}", m.version)
    } else {
        format!("/apis/{}/{}", m.group, m.version)
    };
    let mut path = if m.namespaced {
        let ns = namespace.unwrap_or("default");
        format!("{base}/namespaces/{ns}/{}", m.plural)
    } else {
        format!("{base}/{}", m.plural)
    };
    if let Some(n) = name {
        path.push('/');
        path.push_str(n);
    }
    path
}

/// Read `metadata.name` from a resource Value.
pub fn value_name(v: &Value) -> Option<String> {
    v.pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .map(String::from)
}

/// Read `metadata.namespace` from a resource Value.
pub fn value_namespace(v: &Value) -> Option<String> {
    v.pointer("/metadata/namespace")
        .and_then(|n| n.as_str())
        .map(String::from)
}

/// Apply a resource Value (create-or-replace). Returns ("created"|"configured", response).
pub async fn apply_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    body: &Value,
) -> Result<(&'static str, Value)> {
    let name = value_name(body).context("resource is missing metadata.name")?;
    let ns = m
        .namespaced
        .then(|| namespace.map(String::from).or_else(|| value_namespace(body)))
        .flatten();
    let item = build_path(m, ns.as_deref(), Some(&name));
    let collection = build_path(m, ns.as_deref(), None);
    if client.resource_exists(&item).await? {
        let resp: Value = client.put(&item, body).await?;
        Ok(("configured", resp))
    } else {
        let resp: Value = client.post(&collection, body).await?;
        Ok(("created", resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::ResourceMapping;

    fn m(group: &str, plural: &str, namespaced: bool) -> ResourceMapping {
        ResourceMapping {
            group: group.into(),
            version: "v1".into(),
            kind: "X".into(),
            plural: plural.into(),
            singular: "x".into(),
            namespaced,
            verbs: vec![],
            short_names: vec![],
        }
    }

    #[test]
    fn core_namespaced_paths() {
        let pod = m("", "pods", true);
        assert_eq!(
            build_path(&pod, Some("kube-system"), Some("dns")),
            "/api/v1/namespaces/kube-system/pods/dns"
        );
        assert_eq!(
            build_path(&pod, None, None),
            "/api/v1/namespaces/default/pods"
        );
    }

    #[test]
    fn grouped_cluster_path() {
        let crb = m("rbac.authorization.k8s.io", "clusterrolebindings", false);
        assert_eq!(
            build_path(&crb, Some("ignored"), Some("admin")),
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/admin"
        );
    }
}
```

Add to `crates/kubectl/src/main.rs`:

```rust
mod ops;
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p rusternetes-kubectl ops::tests`
Expected: PASS (2 tests).

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/kubectl/src/ops.rs crates/kubectl/src/main.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "feat(kubectl): generic Value-based ops + path builder"
```

---

## Task 5: Migrate `apply` to the RestMapper

**Files:**
- Modify: `crates/kubectl/src/commands/apply.rs`

This is the largest migration. The plan: keep the public entry points and `ApplyResult`/`ApplyOptions` types and the `set_last_applied_annotation`/`strip_last_applied` helpers (they already operate on `Value`). Replace `apply_resource`'s `match kind { … }` body, and delete the per-type `apply_namespaced`/`apply_cluster` generics, the `HasMetadata` trait + macro, and the now-unused `Pod`/`Service`/… imports.

- [ ] **Step 1: Rewrite `apply_resource`**

Read the current `apply_resource` (`crates/kubectl/src/commands/apply.rs:356`) and `ApplyOptions`/`ApplyResult` definitions first. Replace the function with:

```rust
async fn apply_resource(
    client: &ApiClient,
    value: &serde_yaml::Value,
    query: &str,
    options: &ApplyOptions,
) -> Result<ApplyResult> {
    use crate::discovery::RestMapper;

    // Convert the YAML document to JSON once.
    let mut json: serde_json::Value = serde_json::to_value(value)?;
    let kind = json
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field")?
        .to_string();

    let mapper = RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(&kind).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{kind}\"")
    })?;

    // last-applied annotation (helper already takes &mut Value).
    set_last_applied_annotation(&mut json);

    // Ensure uid + creationTimestamp on create (matches prior behaviour). The
    // api-server overwrites these, but kubectl set them historically.
    ensure_metadata_defaults(&mut json);

    let ns = options.namespace.clone();
    let (action, response) =
        crate::ops::apply_value(client, mapping, ns.as_deref(), &json).await?;

    Ok(ApplyResult {
        kind,
        api_group: mapping.group.clone(),
        name: crate::ops::value_name(&json).unwrap_or_default(),
        namespace: mapping
            .namespaced
            .then(|| ns.or_else(|| crate::ops::value_namespace(&json)))
            .flatten(),
        action: match action {
            "created" => ApplyAction::Created,
            _ => ApplyAction::Configured,
        },
        response,
    })
}

/// Set metadata.uid and metadata.creationTimestamp if absent (cheap, no type).
fn ensure_metadata_defaults(value: &mut serde_json::Value) {
    if let Some(meta) = value
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.entry("uid")
            .or_insert_with(|| serde_json::Value::String(uuid::Uuid::new_v4().to_string()));
    }
}
```

Note on `uuid`: check whether `kubectl`'s `Cargo.toml` already depends on `uuid` (the old code called `ensure_uid()` from `common`). If `common` exposes a helper that takes `&mut serde_json::Value`, prefer it. Otherwise add `uuid = { workspace = true, features = ["v4"] }` to `crates/kubectl/Cargo.toml` if not present. Simplest alternative: drop `ensure_metadata_defaults` entirely if the api-server assigns the uid (verify by applying without it — see Step 3).

- [ ] **Step 2: Delete the dead code**

Remove from `apply.rs`:
- the entire `match kind { … }` arms (now gone with the rewrite),
- `async fn apply_namespaced<T>` and `async fn apply_cluster<T>`,
- the `trait HasMetadata` + the `impl HasMetadata for $ty` macro and its invocations,
- the local `resource_exists::<T>` helper (now `client.resource_exists` is used),
- now-unused `use` imports of concrete resource types (`Pod`, `Service`, …). Let the compiler tell you which: `cargo build -p rusternetes-kubectl` and delete each `unused import` it flags.

- [ ] **Step 3: Verify against a live cluster (the EndpointSlice that broke bootstrap)**

Ensure cluster is up + bootstrapped. Run:

```bash
cargo build -p rusternetes-kubectl
cat <<'EOF' | ./target/debug/kubectl --insecure-skip-tls-verify --server https://localhost:6443 apply -f -
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: plan-test-slice
  namespace: kube-system
  labels:
    kubernetes.io/service-name: kube-dns
addressType: IPv4
ports:
  - name: dns
    port: 53
    protocol: UDP
endpoints:
  - addresses: ["172.20.0.4"]
    conditions: { ready: true }
EOF
```

Expected: `endpointslice.discovery.k8s.io/plan-test-slice created` (or the project's apply success wording) — NOT "Unsupported resource kind". Re-run to confirm it prints `configured`. Then clean up:

```bash
curl -sk -X DELETE https://localhost:6443/apis/discovery.k8s.io/v1/namespaces/kube-system/endpointslices/plan-test-slice -o /dev/null -w "%{http_code}\n"
```

Also re-apply a previously-working kind to confirm no regression:

```bash
echo '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"plan-test-cm","namespace":"default"},"data":{"a":"b"}}' \
 | ./target/debug/kubectl --insecure-skip-tls-verify --server https://localhost:6443 apply -f -
```

Expected: `configmap/plan-test-cm created`. Clean up with `kubectl delete configmap plan-test-cm`.

- [ ] **Step 4: Run existing apply unit tests**

Run: `cargo test -p rusternetes-kubectl apply`
Expected: PASS. Some existing tests may assert against the deleted typed path — update those that test dispatch to instead test `apply_resource` behaviour or `RestMapper`/`ops`. Do NOT delete coverage; port it.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p rusternetes-kubectl --all-targets -- -D warnings
git add crates/kubectl/src/commands/apply.rs crates/kubectl/Cargo.toml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "refactor(kubectl): drive apply via RestMapper, drop per-kind tables"
```

---

## Task 6: Migrate `get`

**Files:**
- Modify: `crates/kubectl/src/commands/get.rs`

`get` currently maps a lowercase resource string to a path (`get.rs:388+`). Replace that resolver with `RestMapper`, preserving output formatting (`format_output`, table/wide/json/yaml) and list-vs-single behaviour.

- [ ] **Step 1: Add `list_value`/`get_value` to `ops.rs`**

Append to `crates/kubectl/src/ops.rs` (above `#[cfg(test)]`):

```rust
/// GET a single resource as Value.
pub async fn get_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    name: &str,
) -> Result<Value> {
    let ns = m.namespaced.then(|| namespace.unwrap_or("default")).flatten().map(str::to_string);
    let path = build_path(m, ns.as_deref(), Some(name));
    client
        .get::<Value>(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// GET a resource collection; returns the `.items` array as Values.
pub async fn list_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
) -> Result<Vec<Value>> {
    let ns = m.namespaced.then(|| namespace.unwrap_or("default")).flatten().map(str::to_string);
    let path = build_path(m, ns.as_deref(), None);
    let list: Value = client.get::<Value>(&path).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(list
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default())
}
```

Fix the `.then(...).flatten()` shape: `m.namespaced.then(|| namespace.unwrap_or("default").to_string())` is simpler — adjust both functions to:

```rust
    let ns = if m.namespaced {
        Some(namespace.unwrap_or("default").to_string())
    } else {
        None
    };
```

- [ ] **Step 2: Replace the resolver in `get.rs`**

Find the function that maps the resource arg to a path (around `get.rs:388`). Replace its body so it resolves via the mapper. Concretely, where `get` currently does `match resource_arg { "pod" => …, … }`, build the mapper once and call `ops::list_value` / `ops::get_value`:

```rust
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(resource_arg).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{resource_arg}\"")
    })?;

    let items = match name {
        Some(n) => vec![crate::ops::get_value(client, mapping, namespace.as_deref(), n).await?],
        None => crate::ops::list_value(client, mapping, namespace.as_deref()).await?,
    };
    // feed `items` (Vec<serde_json::Value>) into the existing output formatter.
```

Adapt to `get.rs`'s actual variable names (`resource_arg`, `name`, `namespace`) — read the function signature first. Keep `format_output` and the table renderer; they already accept serializable values, and `serde_json::Value` is `Serialize`.

- [ ] **Step 3: Verify against live cluster**

```bash
cargo build -p rusternetes-kubectl
K="./target/debug/kubectl --insecure-skip-tls-verify --server https://localhost:6443"
$K get pods -n kube-system
$K get networkpolicies -A          # previously unsupported kind
$K get poddisruptionbudgets -A     # previously unsupported kind
$K get ns
```

Expected: each prints results (or "No resources found"), none print "Unsupported resource kind".

- [ ] **Step 4: Run tests + commit**

Run: `cargo test -p rusternetes-kubectl get && cargo clippy -p rusternetes-kubectl --all-targets -- -D warnings`
Expected: PASS, no warnings.

```bash
cargo fmt --all
git add crates/kubectl/src/commands/get.rs crates/kubectl/src/ops.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "refactor(kubectl): drive get via RestMapper"
```

---

## Task 7: Migrate `delete`

**Files:**
- Modify: `crates/kubectl/src/commands/delete.rs`

- [ ] **Step 1: Add `delete_value` to `ops.rs`**

Append to `crates/kubectl/src/ops.rs`:

```rust
/// DELETE a single resource by name.
pub async fn delete_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    name: &str,
) -> Result<()> {
    let ns = if m.namespaced {
        Some(namespace.unwrap_or("default").to_string())
    } else {
        None
    };
    let path = build_path(m, ns.as_deref(), Some(name));
    client.delete(&path).await
}
```

- [ ] **Step 2: Replace the `match kind` in `delete.rs`**

Read `delete.rs:264` (the `_ => bail!` arm) and the surrounding match. Replace the whole match with:

```rust
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(&kind).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{kind}\"")
    })?;
    crate::ops::delete_value(client, mapping, namespace.as_deref(), &name).await?;
    println!("{}/{} deleted", mapping.singular, name);
```

Match `delete.rs`'s actual binding names (`kind`, `name`, `namespace`).

- [ ] **Step 3: Verify + test + commit**

```bash
cargo build -p rusternetes-kubectl
K="./target/debug/kubectl --insecure-skip-tls-verify --server https://localhost:6443"
echo '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"del-test","namespace":"default"}}' | $K apply -f -
$K delete configmap del-test
```

Expected: `configmap/del-test deleted`.

Run: `cargo test -p rusternetes-kubectl delete && cargo clippy -p rusternetes-kubectl --all-targets -- -D warnings`

```bash
cargo fmt --all
git add crates/kubectl/src/commands/delete.rs crates/kubectl/src/ops.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "refactor(kubectl): drive delete via RestMapper"
```

---

## Task 8: Migrate `diff`

**Files:**
- Modify: `crates/kubectl/src/commands/diff.rs`

`diff` GETs the live object and compares to the file. Reuse `ops::get_value` + the existing diff renderer.

- [ ] **Step 1: Replace the `match kind` in `diff.rs`**

Read `diff.rs:170` (the `_ => bail!` arm) and surrounding match. Replace with:

```rust
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(&kind).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{kind}\"")
    })?;
    let name = crate::ops::value_name(&desired).context("resource missing metadata.name")?;
    let ns = mapping
        .namespaced
        .then(|| namespace.clone().or_else(|| crate::ops::value_namespace(&desired)))
        .flatten();
    let live = crate::ops::get_value(client, mapping, ns.as_deref(), &name).await.ok();
    // feed `live` (Option<Value>) + `desired` (Value) into the existing diff printer.
```

Adapt to `diff.rs`'s variable names (`desired` is whatever holds the parsed file as JSON `Value`; convert from YAML if needed with `serde_json::to_value`).

- [ ] **Step 2: Verify + test + commit**

```bash
cargo build -p rusternetes-kubectl
K="./target/debug/kubectl --insecure-skip-tls-verify --server https://localhost:6443"
echo '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"diff-test","namespace":"default"},"data":{"a":"1"}}' | $K apply -f -
echo '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"diff-test","namespace":"default"},"data":{"a":"2"}}' | $K diff -f -
$K delete configmap diff-test
```

Expected: diff shows `a: "1"` → `a: "2"`.

Run: `cargo test -p rusternetes-kubectl diff && cargo clippy -p rusternetes-kubectl --all-targets -- -D warnings`

```bash
cargo fmt --all
git add crates/kubectl/src/commands/diff.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "refactor(kubectl): drive diff via RestMapper"
```

---

## Task 9: Migrate `create`

**Files:**
- Modify: `crates/kubectl/src/commands/create.rs`

`create` is like `apply` but errors if the object already exists (POST only). Reuse the mapper + a POST.

- [ ] **Step 1: Replace the `match kind` in `create.rs`**

Read `create.rs:1550` (the `_ => bail!` arm) and surrounding match. For the file-driven `create -f` path, replace with:

```rust
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(&kind).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{kind}\"")
    })?;
    let ns = if mapping.namespaced {
        Some(namespace.clone().or_else(|| crate::ops::value_namespace(&json)).unwrap_or_else(|| "default".into()))
    } else {
        None
    };
    let collection = crate::ops::build_path(mapping, ns.as_deref(), None);
    let resp: serde_json::Value = client.post(&collection, &json).await?;
    let name = crate::ops::value_name(&resp).unwrap_or_default();
    println!("{}/{} created", mapping.singular, name);
```

Note: `create` also has imperative subcommands (`create namespace foo`, `create secret …`) that build objects programmatically — leave those untouched; only replace the generic kind-dispatch used by `create -f`. Adapt variable names (`json` = the parsed document as `Value`).

- [ ] **Step 2: Verify + test + commit**

```bash
cargo build -p rusternetes-kubectl
K="./target/debug/kubectl --insecure-skip-tls-verify --server https://localhost:6443"
echo '{"apiVersion":"policy/v1","kind":"PodDisruptionBudget","metadata":{"name":"create-test","namespace":"default"},"spec":{"minAvailable":1,"selector":{"matchLabels":{"app":"x"}}}}' | $K create -f -
$K delete poddisruptionbudget create-test
```

Expected: `poddisruptionbudget/create-test created` (previously unsupported kind).

Run: `cargo test -p rusternetes-kubectl create && cargo clippy -p rusternetes-kubectl --all-targets -- -D warnings`

```bash
cargo fmt --all
git add crates/kubectl/src/commands/create.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "refactor(kubectl): drive create -f via RestMapper"
```

---

## Task 10: Migrate `set` (if it dispatches by kind)

**Files:**
- Modify: `crates/kubectl/src/commands/set.rs`

`set.rs` was the only file flagged with a central-resolver grep hit. Read it first: if it already uses a shared resolver, wire it to `RestMapper`; if it only operates on a fixed set of workload kinds (`set image deployment/...`), it may not need migration.

- [ ] **Step 1: Inspect**

Run: `grep -nE "match |=> |resolve|/apis|/api/v1" crates/kubectl/src/commands/set.rs | head -30`

- [ ] **Step 2: If it has a kind table, replace it** with the same `RestMapper::from_server` + `ops::get_value`/`put` pattern used above (GET object, mutate the field, PUT back via `build_path`). If it does not, write a one-line note in the commit that `set` needs no change and skip to Task 11.

- [ ] **Step 3: Verify + test + commit**

Run: `cargo test -p rusternetes-kubectl set && cargo clippy -p rusternetes-kubectl --all-targets -- -D warnings`

```bash
cargo fmt --all
git add crates/kubectl/src/commands/set.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "refactor(kubectl): drive set via RestMapper"   # or: "docs: note set needs no kind-table migration"
```

---

## Task 11: Parity / anti-drift test

**Files:**
- Create: `crates/kubectl/tests/discovery_parity.rs`

This is the test that answers the original question: *does the client cover every server kind?* It runs only when a cluster is reachable.

- [ ] **Step 1: Write the test**

Create `crates/kubectl/tests/discovery_parity.rs`:

```rust
//! Parity check: the RestMapper must resolve every writable kind the
//! api-server serves. Skips automatically when no cluster is reachable, so it
//! is safe in CI without a live server; run it against a bootstrapped cluster
//! (or under the conformance harness) to catch client/server drift.

use rusternetes_kubectl::client::ApiClient;
use rusternetes_kubectl::discovery::RestMapper;

const SERVER: &str = "https://localhost:6443";

// Virtual / read-only kinds that are not independently appliable; absence is
// expected, not drift.
const VIRTUAL: &[&str] = &[
    "ComponentStatus", "Event", "LocalSubjectAccessReview", "MetricValueList",
    "NodeMetrics", "PodMetrics", "SelfSubjectAccessReview", "SelfSubjectReview",
    "SelfSubjectRulesReview", "SubjectAccessReview", "TokenReview", "Binding",
];

#[tokio::test]
async fn mapper_resolves_every_writable_served_kind() {
    let client = match ApiClient::insecure(SERVER) {
        Ok(c) => c,
        Err(_) => {
            eprintln!("no client; skipping parity test");
            return;
        }
    };
    let mapper = match RestMapper::from_server(&client).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("api-server unreachable ({e}); skipping parity test");
            return;
        }
    };

    let mut missing = Vec::new();
    for m in mapper.all() {
        if VIRTUAL.contains(&m.kind.as_str()) {
            continue;
        }
        // A kind is "writable" if discovery lists create/update verbs.
        let writable = m.verbs.iter().any(|v| v == "create" || v == "update");
        if !writable {
            continue;
        }
        // It must be resolvable by its own plural and Kind.
        if mapper.resolve(&m.plural).is_none() || mapper.resolve(&m.kind).is_none() {
            missing.push(format!("{} ({})", m.kind, m.plural));
        }
    }
    assert!(
        missing.is_empty(),
        "RestMapper failed to resolve served writable kinds: {missing:?}"
    );
}
```

Note: this test requires `ApiClient` and `discovery`/`RestMapper` to be reachable from an integration test — i.e. exported from the crate's library target. Check `crates/kubectl/src/main.rs` / `lib.rs`: if `kubectl` is a binary-only crate, either (a) add a thin `lib.rs` re-exporting `pub mod client; pub mod discovery; pub mod ops;`, or (b) move this test into a `#[cfg(test)]` module inside `discovery.rs` and call `from_server` there. Prefer (a) — verify a `[lib]` target exists or add one. Also confirm `ApiClient` has an `insecure(server)` constructor; if not, use the same constructor the commands use (read `ApiClient::new` at `client.rs:41`) and build the test client the same way `main.rs` does for `--insecure-skip-tls-verify --server`.

- [ ] **Step 2: Run against the live cluster**

Run: `cargo test -p rusternetes-kubectl --test discovery_parity -- --nocapture`
Expected: PASS (with cluster up). With cluster down: PASS (prints "skipping").

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add crates/kubectl/tests/discovery_parity.rs crates/kubectl/src/lib.rs crates/kubectl/src/main.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "test(kubectl): parity check mapper resolves every served writable kind"
```

---

## Task 12: Remove the bootstrap EndpointSlice workaround + docs

**Files:**
- Modify: `scripts/bootstrap-cluster.sh`

- [ ] **Step 1: Replace the curl/limitation with `kubectl apply`**

The EndpointSlice apply at `scripts/bootstrap-cluster.sh:277` already uses `$KUBECTL $KUBECTL_FLAGS apply -f -` — it failed only because the client rejected the kind. With Task 5 done, this now works as-is. Verify there is no separate curl fallback or comment claiming EndpointSlice is unsupported; if a comment exists, update it to note the client supports it via discovery. Search:

```bash
grep -n "Unsupported\|curl.*endpointslice\|cannot apply EndpointSlice\|kubectl clone" scripts/bootstrap-cluster.sh
```

Remove/adjust any stale comment found.

- [ ] **Step 2: End-to-end verify**

Bring the cluster up fresh and run the real bootstrap (no manual curl):

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
docker compose -f compose.yml -f compose.dind.yml up -d
bash scripts/bootstrap-cluster.sh
```

Expected: completes through "Swapping CoreDNS Pod" and the EndpointSlice apply with NO "Unsupported resource kind" error.

- [ ] **Step 3: Commit**

```bash
git add scripts/bootstrap-cluster.sh
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
  git commit -m "chore: bootstrap applies EndpointSlice via kubectl (discovery-backed)"
```

---

## Final verification (before PR)

- [ ] `cargo fmt --all -- --check` exits 0
- [ ] `cargo clippy -p rusternetes-kubectl --all-targets --all-features -- -D warnings` clean
- [ ] `cargo test -p rusternetes-kubectl` passes
- [ ] Parity test passes against a live bootstrapped cluster
- [ ] `git log fork/main..HEAD --format="%h %an <%ae> | %cn <%ce>"` — every line `Indy Jones <indyjonesnl@gmail.com>`
- [ ] Open PR against `indyjonesnl/rusternetes` base `main`
