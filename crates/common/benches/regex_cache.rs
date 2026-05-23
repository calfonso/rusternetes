// Criterion benchmark for the regex cache used by CRD schema pattern validation.
//
// The cache (see `schema_validation::compile_regex_cached`) memoizes compiled
// `regex::Regex` instances keyed by pattern string. CRD admission can hit the
// same pattern repeatedly on every API call; recompiling each time shows up in
// profiles for cluster-wide reconciles. This bench compares the cached path
// (via `SchemaValidator::validate`) against re-compiling `Regex::new(pattern)`
// per call (the pre-cache baseline).
//
// Run:
//   cargo bench -p rusternetes-common --bench regex_cache
//   cargo bench -p rusternetes-common --bench regex_cache -- --quick

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rusternetes_common::resources::crd::JSONSchemaProps;
use rusternetes_common::schema_validation::SchemaValidator;

// A pattern with character classes + quantifiers + anchors so compile work is
// non-trivial. Same shape as common K8s patterns (DNS label, semver,
// label-value).
const PATTERN: &str = r"^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$";
const INPUT: &str = "subdomain.example.com";

fn bench_regex_cache(c: &mut Criterion) {
    let schema = JSONSchemaProps {
        type_: Some("string".to_string()),
        pattern: Some(PATTERN.to_string()),
        ..Default::default()
    };
    let value = serde_json::Value::String(INPUT.to_string());

    // Warm the cache once so the cached group measures hit-path only.
    SchemaValidator::validate(&schema, &value).expect("warm-up validate");

    let mut group = c.benchmark_group("regex_cache");

    // Cached path: every iteration goes through `compile_regex_cached` via
    // the public `validate` entry point. Mirrors real CRD admission.
    group.bench_function("cached_validate", |b| {
        b.iter(|| {
            SchemaValidator::validate(black_box(&schema), black_box(&value))
                .expect("cached validate");
        });
    });

    // Uncached baseline: what the code did before the cache existed —
    // `Regex::new` + `is_match` per call.
    group.bench_function("uncached_regex_new", |b| {
        b.iter(|| {
            let re = regex::Regex::new(black_box(PATTERN)).expect("compile pattern");
            assert!(re.is_match(black_box(INPUT)));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_regex_cache);
criterion_main!(benches);
