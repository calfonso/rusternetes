//! Schema parity test: the protobuf [`ProtoRegistry`] in `src/protobuf.rs`
//! against the upstream Kubernetes `generated.proto` snapshots bundled under
//! `proto/upstream/v1.35/`.
//!
//! Motivation
//! ----------
//! PR #690 fixed a bug where `PodStatus` was registered with an empty
//! `fields` map, which caused the native-protobuf decoder to silently drop
//! every field on `UpdateStatus` requests. That bug class is invisible to
//! JSON-only tests. This file is the regression net: for every message in
//! the registry it verifies `(field_number → name + type)` against the
//! upstream `.proto` schema.
//!
//! A second `#[ignore]`d test prints the upstream messages that the registry
//! does not yet cover — a coverage dashboard for future work.
//!
//! Parser
//! ------
//! Uses [`protox_parse::parse`] (the same engine `protox` uses internally).
//! Three quirks worth knowing:
//!
//! 1. **Map fields are nested types.** `map<K, V>` becomes a synthetic
//!    `<OuterMessage>.<FieldName>Entry` message with `map_entry = true`.
//!    We collapse those back into a logical [`LogicalType::Map`] view so
//!    the registry's `StringMap` / `MessageMap` / `QuantityMap` variants
//!    can match.
//! 2. **`type_name` trumps `r#type()`.** When `type_name` is non-empty the
//!    parser leaves `r#type()` as the proto3 default of `TYPE_DOUBLE`, since
//!    type resolution is not performed. We dispatch on `type_name` whenever
//!    it is set.
//! 3. **Fully-qualified names.** `IntOrString` shows up as
//!    `.k8s.io.apimachinery.pkg.util.intstr.IntOrString`. We split on `.`
//!    and use the last segment for lookup.

use prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};
use rusternetes_api_server::protobuf::{FieldType, ProtoRegistry};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

// -------- proto bundle ---------------------------------------------------

const UPSTREAM_DIR: &str = "proto/upstream/v1.35";

const PROTO_FILES: &[&str] = &[
    "k8s.io/api/core/v1/generated.proto",
    "k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto",
    "k8s.io/api/apps/v1/generated.proto",
    "k8s.io/api/batch/v1/generated.proto",
    "k8s.io/api/networking/v1/generated.proto",
    "k8s.io/api/policy/v1/generated.proto",
    "k8s.io/api/rbac/v1/generated.proto",
    "k8s.io/api/storage/v1/generated.proto",
    "k8s.io/api/autoscaling/v1/generated.proto",
    "k8s.io/api/autoscaling/v2/generated.proto",
    "k8s.io/api/discovery/v1/generated.proto",
    "k8s.io/api/admissionregistration/v1/generated.proto",
    "k8s.io/api/coordination/v1/generated.proto",
    "k8s.io/api/scheduling/v1/generated.proto",
    "k8s.io/api/events/v1/generated.proto",
    "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/generated.proto",
    "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/generated.proto",
    "k8s.io/apimachinery/pkg/runtime/generated.proto",
    "k8s.io/apimachinery/pkg/api/resource/generated.proto",
    "k8s.io/apimachinery/pkg/runtime/schema/generated.proto",
    "k8s.io/apimachinery/pkg/util/intstr/generated.proto",
];

/// Map a bundled proto file's directory path to the registry's qualified
/// key prefix (i.e. the same `apiVersion` form rusternetes uses for
/// group-qualified schema lookups). Returned tuple is `(prefix, file_path)`.
///
/// `prefix` is `""` for core/v1 (registry uses bare simple names for core
/// types — they win the unqualified `iter_schemas` slot). For every other
/// group/version the qualified prefix is the apiVersion that the K8s
/// client and rusternetes' router both speak (e.g. `events.k8s.io/v1`).
///
/// Without this mapping the parity test silently misses any schema whose
/// registry key is `<group>/<version>.<MessageName>` — `events.k8s.io/v1.Event`
/// is the canonical example (see the off-by-one regression in fields 12-15
/// fixed in the same PR that added this helper).
fn qualified_prefix_for(rel_path: &str) -> &'static str {
    match rel_path {
        "k8s.io/api/core/v1/generated.proto" => "",
        "k8s.io/api/apps/v1/generated.proto" => "apps/v1",
        "k8s.io/api/batch/v1/generated.proto" => "batch/v1",
        "k8s.io/api/networking/v1/generated.proto" => "networking.k8s.io/v1",
        "k8s.io/api/policy/v1/generated.proto" => "policy/v1",
        "k8s.io/api/rbac/v1/generated.proto" => "rbac.authorization.k8s.io/v1",
        "k8s.io/api/storage/v1/generated.proto" => "storage.k8s.io/v1",
        "k8s.io/api/autoscaling/v1/generated.proto" => "autoscaling/v1",
        "k8s.io/api/autoscaling/v2/generated.proto" => "autoscaling/v2",
        "k8s.io/api/discovery/v1/generated.proto" => "discovery.k8s.io/v1",
        "k8s.io/api/admissionregistration/v1/generated.proto" => "admissionregistration.k8s.io/v1",
        "k8s.io/api/coordination/v1/generated.proto" => "coordination.k8s.io/v1",
        "k8s.io/api/scheduling/v1/generated.proto" => "scheduling.k8s.io/v1",
        "k8s.io/api/events/v1/generated.proto" => "events.k8s.io/v1",
        "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/generated.proto" => {
            "apiextensions.k8s.io/v1"
        }
        "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/generated.proto" => {
            "apiregistration.k8s.io/v1"
        }
        // apimachinery shared packages — their messages (Time, MicroTime,
        // ObjectMeta, ListMeta, IntOrString, Quantity, RawExtension, …) are
        // registered under bare simple names. No qualified-key entries
        // expected, so prefix is empty.
        _ => "",
    }
}

fn upstream_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(UPSTREAM_DIR)
}

fn read_proto(rel: &str) -> String {
    let path = upstream_root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e))
}

fn parse_all_files() -> Vec<FileDescriptorProto> {
    PROTO_FILES
        .iter()
        .map(|rel| {
            let src = read_proto(rel);
            protox_parse::parse(rel, &src)
                .unwrap_or_else(|e| panic!("failed to parse {}: {}", rel, e))
        })
        .collect()
}

/// Build a per-file qualified index keyed by the registry's qualified key
/// format (e.g. `events.k8s.io/v1.Event`). Messages from files with no
/// qualified prefix (core/v1, apimachinery) are NOT included — they're
/// already covered by the bare-name `UpstreamIndex`.
fn build_qualified_index(files: &[FileDescriptorProto]) -> UpstreamIndex {
    let map_entries = collect_map_entries(files);
    let mut index: UpstreamIndex = BTreeMap::new();
    for file in files {
        let prefix = qualified_prefix_for(file.name());
        if prefix.is_empty() {
            continue;
        }
        for msg in &file.message_type {
            collect_qualified(msg, prefix, &map_entries, &mut index);
        }
    }
    index
}

fn collect_qualified(
    msg: &DescriptorProto,
    prefix: &str,
    map_entries: &MapEntryNames,
    out: &mut UpstreamIndex,
) {
    if !map_entries.contains(msg.name()) {
        let mut fields_by_number: BTreeMap<u32, UpstreamField> = BTreeMap::new();
        for f in &msg.field {
            let logical = build_logical(f, map_entries);
            // NB: Map collapsing isn't repeated here — qualified lookups
            // currently only target leaf resource messages where map<>
            // semantics aren't on the critical path. If a qualified-key
            // schema starts using a map field, lift the collapse logic
            // out of build_upstream_index() into a shared helper.
            fields_by_number.insert(
                f.number() as u32,
                UpstreamField {
                    name: f.name().to_string(),
                    logical,
                },
            );
        }
        let key = format!("{}.{}", prefix, msg.name());
        out.insert(key, fields_by_number);
    }
    for nested in &msg.nested_type {
        collect_qualified(nested, prefix, map_entries, out);
    }
}

// -------- upstream view --------------------------------------------------

/// Field's effective shape in the upstream schema.
///
/// Built from a [`FieldDescriptorProto`] by:
///   - resolving `type_name` to the simple (last-segment) message name when
///     it is set, otherwise mapping the scalar `r#type()` enum;
///   - collapsing a `Repeated(MapEntry)` field into a logical `Map(K, V)`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LogicalType {
    Scalar(Scalar),
    Message(String),
    /// Map field, value is the value type (key is always string in K8s usage).
    Map(Box<LogicalType>),
    Repeated(Box<LogicalType>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Scalar {
    String,
    Int,
    Bool,
    Bytes,
    Float,
}

#[derive(Debug, Clone)]
struct UpstreamField {
    name: String,
    logical: LogicalType,
}

/// Indexed view: message simple name -> (field number -> upstream field).
type UpstreamIndex = BTreeMap<String, BTreeMap<u32, UpstreamField>>;

/// Set of synthetic map-entry message names (one per `map<>` field). We skip
/// these when listing "messages we don't register yet" — they are an
/// implementation detail of the proto encoding.
type MapEntryNames = BTreeSet<String>;

/// Strip the package prefix off a fully-qualified `type_name`, e.g.
/// `.k8s.io.apimachinery.pkg.util.intstr.IntOrString` → `IntOrString`.
fn simple_name(type_name: &str) -> &str {
    let trimmed = type_name.trim_start_matches('.');
    trimmed.rsplit('.').next().unwrap_or(trimmed)
}

fn scalar_from_proto_type(t: ProtoType) -> Option<Scalar> {
    use ProtoType::*;
    Some(match t {
        String => Scalar::String,
        Bool => Scalar::Bool,
        Bytes => Scalar::Bytes,
        Int32 | Int64 | Uint32 | Uint64 | Sint32 | Sint64 | Fixed32 | Fixed64 | Sfixed32
        | Sfixed64 => Scalar::Int,
        Float | Double => Scalar::Float,
        Enum => Scalar::Int,
        Group | Message => return None,
    })
}

/// Build the logical type for a single field. `parent_msg_simple` is the
/// simple name of the enclosing message — needed to disambiguate the
/// synthetic `<Parent>.<Field>Entry` map types from regular messages.
fn build_logical(field: &FieldDescriptorProto, map_entries: &MapEntryNames) -> LogicalType {
    let repeated = field.label() == Label::Repeated;

    let base = if !field.type_name().is_empty() {
        // `type_name` is the resolved type reference. Per the protox-parse
        // quirk noted above, ignore r#type() here.
        let name = simple_name(field.type_name()).to_string();
        if map_entries.contains(&name) {
            // Synthetic map entry — caller turns this into a Map.
            LogicalType::Message(name)
        } else {
            LogicalType::Message(name)
        }
    } else {
        match scalar_from_proto_type(field.r#type()) {
            Some(s) => LogicalType::Scalar(s),
            None => LogicalType::Message("unknown".into()),
        }
    };

    if repeated {
        LogicalType::Repeated(Box::new(base))
    } else {
        base
    }
}

/// Compute the set of synthetic map-entry message names across all bundled
/// proto files. Walks every nested type and returns the simple names that
/// have `options.map_entry == true`.
fn collect_map_entries(files: &[FileDescriptorProto]) -> MapEntryNames {
    let mut out = BTreeSet::new();
    for file in files {
        for msg in &file.message_type {
            collect_map_entries_in(msg, &mut out);
        }
    }
    out
}

fn collect_map_entries_in(msg: &DescriptorProto, out: &mut MapEntryNames) {
    if let Some(opts) = &msg.options {
        if opts.map_entry == Some(true) {
            out.insert(msg.name().to_string());
        }
    }
    for nested in &msg.nested_type {
        collect_map_entries_in(nested, out);
    }
}

/// For a given map-entry message descriptor, return the value-side
/// logical type.
fn map_value_type(entry: &DescriptorProto, map_entries: &MapEntryNames) -> LogicalType {
    // Map entry messages always have exactly two fields: key (1) and
    // value (2). Pull the value type.
    for f in &entry.field {
        if f.number() == 2 {
            return build_logical(f, map_entries);
        }
    }
    LogicalType::Message("unknown".into())
}

fn build_upstream_index(files: &[FileDescriptorProto]) -> (UpstreamIndex, MapEntryNames) {
    let map_entries = collect_map_entries(files);
    let mut index: UpstreamIndex = BTreeMap::new();

    // Collect every top-level + nested message keyed by simple name. We use
    // this as a fallback when a `type_name` reference appears unqualified.
    //
    // Synthetic map-entry messages (`<Parent>.<Field>Entry`) collide on
    // simple name across distinct parents (e.g. `Secret.DataEntry` and
    // `ConfigMap.DataEntry` both flatten to `DataEntry`). Hashing them by
    // simple name silently picked one entry's value type for the other,
    // which used to be invisible — every `data` field happened to be
    // `map<string, string>` — but now that `Secret.data` is genuinely
    // `map<string, bytes>` the collision flips `ConfigMap.data`'s reported
    // value type to bytes. Resolve map entries from the parent message's
    // own nested types so each map keeps its true value side.
    // The bare-name view is for the registry's UNqualified keys. Those
    // keys win the namespace for collisions across groups — see the
    // doc-comment on `qualified_prefix_for`. Messages from a file with a
    // non-empty qualified prefix (events.k8s.io, networking.k8s.io, …) are
    // intentionally omitted: they would otherwise overwrite the core/v1
    // entry under `HashMap` last-writer-wins and silently flip the parity
    // assertion to the wrong upstream shape (regression first surfaced
    // 2026-05-27 when events/v1/generated.proto was added to PROTO_FILES
    // and `Event` started reporting all-fields-mismatched against
    // core/v1's registry entry).
    let mut by_simple_name: HashMap<String, DescriptorProto> = HashMap::new();
    for file in files {
        if !qualified_prefix_for(file.name()).is_empty() {
            continue;
        }
        for msg in &file.message_type {
            walk_collect(msg, &mut by_simple_name);
        }
    }

    for msg in by_simple_name.values() {
        if map_entries.contains(msg.name()) {
            continue;
        }
        // Per-parent index of nested map entries, used to disambiguate
        // colliding simple names across messages.
        let local_map_entries: HashMap<String, &DescriptorProto> = msg
            .nested_type
            .iter()
            .filter(|n| map_entries.contains(n.name()))
            .map(|n| (n.name().to_string(), n))
            .collect();

        let mut fields_by_number: BTreeMap<u32, UpstreamField> = BTreeMap::new();
        for f in &msg.field {
            let logical = build_logical(f, &map_entries);
            // Collapse a Repeated(Message<MapEntry>) into a Map. Prefer the
            // parent's nested map-entry over the global by-simple-name view.
            let logical = match &logical {
                LogicalType::Repeated(inner) => match inner.as_ref() {
                    LogicalType::Message(name) if map_entries.contains(name) => {
                        let entry = local_map_entries
                            .get(name)
                            .copied()
                            .or_else(|| by_simple_name.get(name));
                        let value_ty = entry
                            .map(|e| map_value_type(e, &map_entries))
                            .unwrap_or(LogicalType::Message("unknown".into()));
                        LogicalType::Map(Box::new(value_ty))
                    }
                    _ => logical.clone(),
                },
                _ => logical.clone(),
            };
            fields_by_number.insert(
                f.number() as u32,
                UpstreamField {
                    name: f.name().to_string(),
                    logical,
                },
            );
        }
        index.insert(msg.name().to_string(), fields_by_number);
    }

    (index, map_entries)
}

fn walk_collect(msg: &DescriptorProto, out: &mut HashMap<String, DescriptorProto>) {
    out.insert(msg.name().to_string(), msg.clone());
    for nested in &msg.nested_type {
        walk_collect(nested, out);
    }
}

// -------- type comparison ------------------------------------------------

/// Compare our [`FieldType`] against an upstream [`LogicalType`]. Returns
/// `None` on match, or `Some(reason)` on mismatch.
fn compare_types(ours: &FieldType, theirs: &LogicalType) -> Option<String> {
    match (ours, theirs) {
        // Scalars
        (FieldType::String, LogicalType::Scalar(Scalar::String)) => None,
        (FieldType::Int, LogicalType::Scalar(Scalar::Int)) => None,
        (FieldType::Double, LogicalType::Scalar(Scalar::Float)) => None,
        (FieldType::Bool, LogicalType::Scalar(Scalar::Bool)) => None,
        (FieldType::Bytes, LogicalType::Scalar(Scalar::Bytes)) => None,

        // K8s-specific aliases
        (FieldType::Quantity, LogicalType::Message(name)) if name == "Quantity" => None,
        (FieldType::IntOrString, LogicalType::Message(name)) if name == "IntOrString" => None,
        // JSON is a runtime/extension wrapper around RawExtension or its
        // own message. Accept either.
        (FieldType::JsonRaw, LogicalType::Message(name))
            if matches!(name.as_str(), "JSON" | "RawExtension" | "JSONSchemaProps") =>
        {
            None
        }

        // Messages — names should match (after stripping any nested prefix)
        (FieldType::Message(ours_name), LogicalType::Message(their_name))
        | (FieldType::InlineMessage(ours_name), LogicalType::Message(their_name)) => {
            if ours_name == their_name {
                None
            } else {
                Some(format!(
                    "message name mismatch (ours={}, theirs={})",
                    ours_name, their_name
                ))
            }
        }

        // Maps
        (FieldType::StringMap, LogicalType::Map(value)) => match value.as_ref() {
            LogicalType::Scalar(Scalar::String) => None,
            other => Some(format!(
                "expected map<string,string>, upstream value={:?}",
                other
            )),
        },
        (FieldType::BytesMap, LogicalType::Map(value)) => match value.as_ref() {
            LogicalType::Scalar(Scalar::Bytes) => None,
            other => Some(format!(
                "expected map<string,bytes>, upstream value={:?}",
                other
            )),
        },
        (FieldType::QuantityMap, LogicalType::Map(value)) => match value.as_ref() {
            LogicalType::Message(name) if name == "Quantity" => None,
            other => Some(format!(
                "expected map<string,Quantity>, upstream value={:?}",
                other
            )),
        },
        (FieldType::MessageMap(expected), LogicalType::Map(value)) => match value.as_ref() {
            LogicalType::Message(name) if name == expected => None,
            other => Some(format!(
                "expected map<string,{}>, upstream value={:?}",
                expected, other
            )),
        },

        // Repeated
        (FieldType::Repeated(inner_ours), LogicalType::Repeated(inner_theirs)) => {
            compare_types(inner_ours, inner_theirs)
                .map(|m| format!("repeated element mismatch: {}", m))
        }

        // Catch-alls — registry uses Repeated but upstream is a Map (or
        // vice versa). Treat the directions separately so the message is
        // useful.
        (FieldType::Repeated(_), LogicalType::Map(_)) => {
            Some("ours=Repeated, upstream=Map (registry needs StringMap/MessageMap)".into())
        }
        (
            FieldType::StringMap
            | FieldType::BytesMap
            | FieldType::MessageMap(_)
            | FieldType::QuantityMap,
            _,
        ) => Some(format!(
            "ours=Map, upstream={:?} (registry expected a map field)",
            theirs
        )),

        // Anything else — print both sides.
        (ours, theirs) => Some(format!(
            "type shape differs (ours={:?}, theirs={:?})",
            ours, theirs
        )),
    }
}

// -------- registry view --------------------------------------------------

/// Messages registered in our [`ProtoRegistry`] that intentionally have no
/// 1:1 upstream counterpart by name. Skip the comparison for these.
///
/// Most of these come from registry-side renames (we use the inner type's
/// simple name where K8s uses a wrapper) or registry-side splits of
/// `oneof` blocks (Volume, EnvVarSource, ProbeHandler etc.).
const REGISTRY_SKIP: &[&str] = &[
    // Registry uses bare "Time" / "MicroTime" for the apimachinery types;
    // they map cleanly so they ARE checked. Listed here would be names
    // that genuinely do not exist upstream.
];

/// Messages where a specific field number is intentionally not in the
/// upstream schema and should be tolerated. Keyed by message name.
///
/// Most entries below are *JSON-name vs Go-field-name* differences:
/// the registry tracks the wire/JSON name (the one that has to
/// round-trip through every client), while upstream's `protoc-gen-go`
/// strips `$` prefixes, kebab-case dashes, and PascalCases the first
/// letter to derive a Go field name. Both names describe the same
/// field number — only their string differs.
fn intentional_field_skip(msg: &str, field_number: u32) -> bool {
    match (msg, field_number) {
        // `TypeMeta` exists in two upstream packages with different
        // field layouts:
        //   apimachinery/pkg/apis/meta/v1.TypeMeta — kind=1, apiVersion=2;
        //     embedded in every API kind body.
        //   apimachinery/pkg/runtime.TypeMeta    — apiVersion=1, kind=2;
        //     part of the `Unknown` envelope.
        // The registry intentionally tracks the meta/v1 form (see
        // src/protobuf.rs and `test_meta_v1_schemas_present`).
        // Upstream's by-simple-name index collapses both into one
        // entry, so suppress the field-name false positives at #1 and
        // #2 here. The Unknown envelope decoder handles its own
        // TypeMeta out-of-band, so the skip is safe.
        ("TypeMeta", 1 | 2) => true,

        // JSONSchemaProps — `$schema`, `$ref`, and `x-kubernetes-*`
        // are the actual JSON field names in CRD JSON-Schema
        // documents. protoc-gen-go strips the `$` and dash-camel-cases
        // to derive `schema`, `ref`, `xKubernetesPreserveUnknownFields`,
        // etc. Wire numbers match; only the Go-side string differs.
        ("JSONSchemaProps", 2 | 3) => true,
        ("JSONSchemaProps", 38..=44) => true,

        // JSONSchemaPropsOrArray — `jsonSchemas` (JSON name) becomes
        // `jSONSchemas` (Go field name) under protoc-gen-go's
        // initialism convention.
        ("JSONSchemaPropsOrArray", 2) => true,

        // Webhook configurations — `webhooks` (JSON name) becomes
        // `Webhooks` (PascalCase Go field name).
        ("MutatingWebhookConfiguration", 2) => true,
        ("ValidatingWebhookConfiguration", 2) => true,

        // CEL ValidatingAdmissionPolicy types — Go-side PascalCase
        // variants of the JSON names.
        ("Validation", 1) => true,
        ("Variable", 1 | 2) => true,
        _ => false,
    }
}

// -------- the tests ------------------------------------------------------

#[test]
fn upstream_protos_parse_cleanly() {
    let files = parse_all_files();
    assert_eq!(
        files.len(),
        PROTO_FILES.len(),
        "expected to parse {} proto files, got {}",
        PROTO_FILES.len(),
        files.len()
    );
    // Sanity print of the first message in core/v1 so a regression in the
    // parser surfaces here, not deep in the parity assertion.
    let core = &files[0];
    let first_msg = core
        .message_type
        .first()
        .expect("core/v1 should contain at least one message");
    eprintln!(
        "parsed {}; first message: {}",
        core.name(),
        first_msg.name()
    );
}

/// Strict parity check — fails the moment any field number/name/type
/// in our `ProtoRegistry` disagrees with the upstream `.proto` schema.
/// The first invocation surfaced ~75 pre-existing mismatches that were
/// then closed across a sequence of `fix(api-server): protobuf
/// registry parity for ...` PRs. The remaining JSON-name vs Go-name
/// false positives (CRD JSON-Schema fields, CEL Validation /
/// Variable, webhook configs) are whitelisted in
/// [`intentional_field_skip`].
///
/// Runs as part of `cargo test` so any future drift — a new resource
/// landing with wrong field numbers, or a forgotten skip-entry —
/// goes red immediately.
#[test]
fn registry_parity_with_upstream() {
    let files = parse_all_files();
    let (upstream, _map_entries) = build_upstream_index(&files);
    let qualified_upstream = build_qualified_index(&files);
    let registry = ProtoRegistry::new();

    let mut mismatches: Vec<String> = Vec::new();
    let mut unmatched_messages: Vec<String> = Vec::new();

    for (msg_name, schema) in registry.iter_schemas() {
        if REGISTRY_SKIP.contains(&msg_name) {
            continue;
        }
        // Qualified registry keys (e.g. `events.k8s.io/v1.Event`) look up
        // in the per-group index built from the matching `generated.proto`.
        // Bare keys (`Event`, `Pod`, …) use the legacy by-simple-name view
        // — for shapes that exist in multiple groups (core's `Event` vs
        // events.k8s.io's `Event`), the bare registry key represents the
        // core/v1 shape and matches the corresponding upstream entry.
        let upstream_fields = if msg_name.contains('/') && msg_name.contains('.') {
            qualified_upstream.get(msg_name)
        } else {
            upstream.get(msg_name)
        };
        let Some(upstream_fields) = upstream_fields else {
            // The registry contains messages that upstream does not — these
            // are usually registry-side helper shapes (e.g. our split of
            // VolumeSource into a flat struct). Surface as a single line,
            // not a per-field flood.
            unmatched_messages.push(msg_name.to_string());
            continue;
        };

        for (field_number, (our_name, our_type)) in &schema.fields {
            if intentional_field_skip(msg_name, *field_number) {
                continue;
            }
            let Some(up) = upstream_fields.get(field_number) else {
                mismatches.push(format!(
                    "{}#{}: registry has '{}' but upstream has no field at that number",
                    msg_name, field_number, our_name
                ));
                continue;
            };
            if up.name != *our_name {
                mismatches.push(format!(
                    "{}#{}: name mismatch (ours='{}', upstream='{}')",
                    msg_name, field_number, our_name, up.name
                ));
            }
            if let Some(reason) = compare_types(our_type, &up.logical) {
                mismatches.push(format!(
                    "{}#{} ('{}'): {}",
                    msg_name, field_number, our_name, reason
                ));
            }
        }
    }

    // Sort so reruns produce a stable diff.
    mismatches.sort();
    unmatched_messages.sort();

    // Surface counters even on pass, so CI logs are useful.
    eprintln!(
        "registry parity: {} schemas, {} unmatched message names, {} field mismatches",
        registry.schema_count(),
        unmatched_messages.len(),
        mismatches.len()
    );
    if !unmatched_messages.is_empty() {
        eprintln!("\nRegistry messages with no upstream counterpart by name:");
        for m in &unmatched_messages {
            eprintln!("  - {}", m);
        }
    }

    if !mismatches.is_empty() {
        panic!(
            "{} protobuf schema mismatches vs upstream:\n{}",
            mismatches.len(),
            mismatches
                .iter()
                .map(|s| format!("  - {}", s))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

/// Fields a registered message intentionally does NOT carry on the wire,
/// even though the upstream `.proto` declares them at that number. Keep this
/// list tiny and well-justified — each entry is a place where protobuf
/// round-trip is knowingly lossy.
fn completeness_skip(msg: &str, field_number: u32) -> bool {
    match (msg, field_number) {
        // selfLink: deprecated in k8s 1.16, removed from server responses;
        // our ObjectMeta intentionally omits it (mirrors the JSON parity
        // test's KNOWN_GAPS). Not modelled on either codec.
        ("ObjectMeta", 4) => true,

        // Legacy / removed in-tree volume plugins. Our Volume struct does not
        // implement these provider-specific sources (same KNOWN_GAPS set the
        // JSON parity test allowlists), so the registry has no field for them.
        //   3 gcePersistentDisk, 4 awsElasticBlockStore, 5 gitRepo,
        //   9 glusterfs, 11 rbd, 12 flexVolume, 13 cinder, 14 cephfs,
        //   15 flocker, 17 fc, 18 azureFile, 20 vsphereVolume, 21 quobyte,
        //   22 azureDisk, 23 photonPersistentDisk, 24 portworxVolume,
        //   25 scaleIO, 27 storageos.
        ("VolumeSource", 3 | 4 | 5 | 9 | 11 | 12 | 13 | 14 | 15 | 17 | 18) => true,
        ("VolumeSource", 20 | 21 | 22 | 23 | 24 | 25 | 27) => true,

        _ => false,
    }
}

/// Field-level completeness gate (the inverse of [`registry_parity_with_upstream`]).
///
/// `registry_parity_with_upstream` walks the *registry* and checks every
/// registered field against upstream — so a message that is registered but
/// *missing* some upstream fields slips through silently. That is the exact
/// bug class that drops data on the native-protobuf wire: the field is absent
/// from the schema, so the decoder never reads it (PR #690 was the message-
/// level version; this is the field-level version).
///
/// For every message present in BOTH the registry and the bundled upstream
/// snapshot, assert the registry carries an entry for every upstream field
/// number (modulo [`intentional_field_skip`] name-divergence cases and the
/// explicit [`completeness_skip`] allowlist).
#[test]
fn registry_covers_every_upstream_field() {
    let files = parse_all_files();
    let (upstream, _map_entries) = build_upstream_index(&files);
    let qualified_upstream = build_qualified_index(&files);
    let registry = ProtoRegistry::new();

    let mut missing: Vec<String> = Vec::new();

    for (msg_name, schema) in registry.iter_schemas() {
        if REGISTRY_SKIP.contains(&msg_name) {
            continue;
        }
        let upstream_fields = if msg_name.contains('/') && msg_name.contains('.') {
            qualified_upstream.get(msg_name)
        } else {
            upstream.get(msg_name)
        };
        // Registry messages with no upstream counterpart are reported by
        // registry_parity_with_upstream; nothing to complete here.
        let Some(upstream_fields) = upstream_fields else {
            continue;
        };
        for (field_number, up) in upstream_fields {
            if intentional_field_skip(msg_name, *field_number)
                || completeness_skip(msg_name, *field_number)
            {
                continue;
            }
            if !schema.fields.contains_key(field_number) {
                missing.push(format!(
                    "{}#{} '{}' ({:?}) present upstream but MISSING from registry",
                    msg_name, field_number, up.name, up.logical
                ));
            }
        }
    }

    missing.sort();
    eprintln!(
        "registry completeness: {} upstream field(s) unmodelled",
        missing.len()
    );
    if !missing.is_empty() {
        panic!(
            "{} upstream field(s) missing from the registry (native-protobuf would drop them):\n{}",
            missing.len(),
            missing
                .iter()
                .map(|s| format!("  - {}", s))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

/// Coverage check: every upstream message in our bundled snapshots has a
/// counterpart in the registry. Originally a `#[ignore]`d dashboard that
/// listed the missing entries; now enforced as a hard parity gate.
///
/// If this fails, the panic body lists every upstream message that has no
/// registry entry, grouped by source `.proto` file. Recover by either:
///   - adding the missing entry to a `register_*` helper in `src/protobuf.rs`
///     (the by-far-most-common case — even no-op shapes like List wrappers
///     belong in the registry), or
///   - if a message has no rusternetes counterpart (a deprecated upstream
///     type, internal-only payload), wire it into a name-skip set here and
///     leave a comment explaining why.
#[test]
fn upstream_messages_we_dont_register_yet() {
    let files = parse_all_files();
    let (upstream, _) = build_upstream_index(&files);
    let registry = ProtoRegistry::new();

    let registered: BTreeSet<String> = registry
        .iter_schemas()
        .map(|(name, _)| name.to_string())
        .collect();

    // Group missing messages by the file they came from.
    let mut by_file: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for file in &files {
        let path = file.name().to_string();
        collect_message_names(&file.message_type, &mut |name| {
            if !registered.contains(name) && upstream.contains_key(name) {
                by_file
                    .entry(path.clone())
                    .or_default()
                    .insert(name.to_string());
            }
        });
    }

    if by_file.is_empty() {
        eprintln!(
            "all {} upstream messages are registered in ProtoRegistry",
            upstream.len()
        );
        return;
    }

    let mut report = String::from("upstream messages NOT registered in ProtoRegistry:\n");
    let mut total = 0;
    for (file, messages) in &by_file {
        report.push_str(&format!("\n[{}] ({} missing)\n", file, messages.len()));
        for m in messages {
            report.push_str(&format!("  - {}\n", m));
            total += 1;
        }
    }
    panic!("{}\n{} message(s) missing from the registry", report, total);
}

fn collect_message_names<F: FnMut(&str)>(messages: &[DescriptorProto], visit: &mut F) {
    for msg in messages {
        visit(msg.name());
        collect_message_names(&msg.nested_type, visit);
    }
}

// -------- unit tests for the helpers -------------------------------------

#[test]
fn map_entry_collapses_to_logical_map() {
    // Build a tiny synthetic file with a map field and check the upstream
    // index turns it into a Map. The exact name is what `protox-parse`
    // produces: <field-name-titlecased>Entry.
    let src = r#"
        syntax = "proto3";
        package test;
        message HasMap {
          map<string, string> labels = 1;
          map<string, int64> counts = 2;
        }
    "#;
    let file = protox_parse::parse("synthetic.proto", src).expect("parse");
    let (index, _) = build_upstream_index(std::slice::from_ref(&file));
    let has_map = index.get("HasMap").expect("HasMap not indexed");
    let labels = has_map.get(&1).expect("field 1");
    assert!(
        matches!(
            &labels.logical,
            LogicalType::Map(v) if matches!(v.as_ref(), LogicalType::Scalar(Scalar::String))
        ),
        "expected Map<_, String>, got {:?}",
        labels.logical
    );
    let counts = has_map.get(&2).expect("field 2");
    assert!(
        matches!(
            &counts.logical,
            LogicalType::Map(v) if matches!(v.as_ref(), LogicalType::Scalar(Scalar::Int))
        ),
        "expected Map<_, Int>, got {:?}",
        counts.logical
    );
}

#[test]
fn simple_name_strips_package() {
    assert_eq!(
        simple_name(".k8s.io.apimachinery.pkg.util.intstr.IntOrString"),
        "IntOrString"
    );
    assert_eq!(simple_name("IntOrString"), "IntOrString");
    assert_eq!(simple_name(".Quantity"), "Quantity");
}

// Keep this assertion small so the binary stays healthy even if the proto
// snapshots get moved.
#[test]
fn upstream_dir_exists() {
    let root = upstream_root();
    assert!(
        Path::new(&root).is_dir(),
        "upstream proto dir missing: {}",
        root.display()
    );
}
