use rusternetes_kubelet::static_pods::{
    is_mirror_pod, load_static_pods, make_mirror_pod, normalize_static_pod, parse_manifest,
    pod_config_hash, CONFIG_HASH_ANNOTATION, CONFIG_MIRROR_ANNOTATION, CONFIG_SOURCE_ANNOTATION,
};

const YAML_POD: &str = r#"
apiVersion: v1
kind: Pod
metadata:
  name: kube-scheduler
  namespace: kube-system
spec:
  containers:
  - name: scheduler
    image: ghcr.io/indyjonesnl/rusternetes-scheduler:latest
"#;

#[test]
fn parses_yaml_pod_manifest() {
    let pod = parse_manifest(YAML_POD.as_bytes(), "kube-scheduler.yaml").unwrap();
    assert_eq!(pod.metadata.name, "kube-scheduler");
}

#[test]
fn parses_json_pod_manifest() {
    let json = r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[{"name":"c","image":"i"}]}}"#;
    let pod = parse_manifest(json.as_bytes(), "p.json").unwrap();
    assert_eq!(pod.metadata.name, "p");
}

#[test]
fn rejects_non_pod_kind() {
    let yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n";
    assert!(parse_manifest(yaml.as_bytes(), "x.yaml").is_err());
}

#[test]
fn rejects_missing_name() {
    let yaml = "apiVersion: v1\nkind: Pod\nmetadata: {}\nspec:\n  containers: []\n";
    assert!(parse_manifest(yaml.as_bytes(), "x.yaml").is_err());
}

#[test]
fn normalize_applies_node_suffix_namespace_annotations_and_uid() {
    let pod = parse_manifest(YAML_POD.as_bytes(), "kube-scheduler.yaml").unwrap();
    let pod = normalize_static_pod(pod, "node-1").unwrap();
    // upstream: pkg/kubelet/config/common.go generatePodName → "<name>-<node>"
    assert_eq!(pod.metadata.name, "kube-scheduler-node-1");
    assert_eq!(pod.metadata.namespace.as_deref(), Some("kube-system"));
    assert_eq!(
        pod.spec.as_ref().unwrap().node_name.as_deref(),
        Some("node-1")
    );
    let ann = pod.metadata.annotations.as_ref().unwrap();
    assert_eq!(
        ann.get(CONFIG_SOURCE_ANNOTATION).map(String::as_str),
        Some("file")
    );
    assert!(ann.contains_key(CONFIG_HASH_ANNOTATION));
    assert!(!pod.metadata.uid.is_empty());
}

/// The committed kube-scheduler static pod manifest must parse against the real
/// `Pod` schema (args, tolerations, volumeMounts, hostPath) and normalize to the
/// mirror name the metrics-grabber matches (`kube-scheduler-node-1`). The
/// manifest carries an `@CERTS_PATH@` placeholder that bootstrap-cluster.sh
/// templates; we substitute a dummy value here so the YAML is well-formed.
#[test]
fn committed_kube_scheduler_manifest_parses_and_suffixes_to_node_1() {
    let manifest_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../manifests/control-plane/kube-scheduler.yaml"
    );
    let raw = std::fs::read_to_string(manifest_path)
        .expect("committed kube-scheduler static pod manifest must exist");
    let templated = raw.replace("@CERTS_PATH@", "/host/.rusternetes/certs");
    let pod = parse_manifest(templated.as_bytes(), "kube-scheduler.yaml")
        .expect("manifest must parse against the Pod schema");
    let pod = normalize_static_pod(pod, "node-1").unwrap();
    // metrics-grabber regex is kube-scheduler-.* — node-1 suffix produces it.
    assert_eq!(pod.metadata.name, "kube-scheduler-node-1");
    assert_eq!(pod.metadata.namespace.as_deref(), Some("kube-system"));
    let spec = pod.spec.as_ref().unwrap();
    // Tolerates the control-plane NoSchedule taint so it runs on tainted node-1.
    let tol = spec.tolerations.as_ref().expect("tolerations present");
    assert!(tol
        .iter()
        .any(|t| t.key.as_deref() == Some("node-role.kubernetes.io/control-plane")));
    // The certs hostPath resolved from the placeholder.
    let vol = spec.volumes.as_ref().unwrap();
    let certs = vol
        .iter()
        .find(|v| v.name == "certs")
        .expect("certs volume");
    assert_eq!(
        certs.host_path.as_ref().unwrap().path,
        "/host/.rusternetes/certs"
    );
}

#[test]
fn normalize_defaults_namespace_to_default() {
    let yaml =
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nspec:\n  containers:\n  - name: c\n    image: i\n";
    let pod = parse_manifest(yaml.as_bytes(), "p.yaml").unwrap();
    let pod = normalize_static_pod(pod, "node-1").unwrap();
    assert_eq!(pod.metadata.namespace.as_deref(), Some("default"));
}

#[test]
fn hash_is_deterministic_and_spec_sensitive() {
    let a = normalize_static_pod(
        parse_manifest(YAML_POD.as_bytes(), "a.yaml").unwrap(),
        "node-1",
    )
    .unwrap();
    let b = normalize_static_pod(
        parse_manifest(YAML_POD.as_bytes(), "a.yaml").unwrap(),
        "node-1",
    )
    .unwrap();
    assert_eq!(pod_config_hash(&a), pod_config_hash(&b));
    assert_eq!(a.metadata.uid, b.metadata.uid); // stable UID across restarts

    let changed = YAML_POD.replace(":latest", ":v2");
    let c = normalize_static_pod(
        parse_manifest(changed.as_bytes(), "a.yaml").unwrap(),
        "node-1",
    )
    .unwrap();
    assert_ne!(pod_config_hash(&a), pod_config_hash(&c));
}

#[test]
fn mirror_pod_carries_mirror_annotation_and_is_detected() {
    let pod = normalize_static_pod(
        parse_manifest(YAML_POD.as_bytes(), "a.yaml").unwrap(),
        "node-1",
    )
    .unwrap();
    let mirror = make_mirror_pod(&pod);
    let ann = mirror.metadata.annotations.as_ref().unwrap();
    assert_eq!(
        ann.get(CONFIG_MIRROR_ANNOTATION),
        ann.get(CONFIG_HASH_ANNOTATION)
    );
    assert!(is_mirror_pod(&mirror));
    assert!(!is_mirror_pod(&pod));
}

#[test]
fn load_static_pods_reads_dir_skips_invalid_sorted() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("b.yaml"),
        YAML_POD.replace("kube-scheduler", "bbb"),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("a.yaml"),
        YAML_POD.replace("kube-scheduler", "aaa"),
    )
    .unwrap();
    std::fs::write(dir.path().join("broken.yaml"), "not: [valid").unwrap();
    std::fs::write(dir.path().join("ignore.txt"), "x").unwrap();
    let pods = load_static_pods(dir.path(), "node-1");
    let names: Vec<_> = pods.iter().map(|p| p.metadata.name.as_str()).collect();
    assert_eq!(names, vec!["aaa-node-1", "bbb-node-1"]);
}

use rusternetes_kubelet::static_pods::reconcile_mirror_pods;
use rusternetes_storage::{build_key, MemoryStorage, Storage};

fn static_pod(name: &str, image: &str) -> rusternetes_common::resources::Pod {
    let yaml = YAML_POD
        .replace("kube-scheduler", name)
        .replace(":latest", image);
    normalize_static_pod(parse_manifest(yaml.as_bytes(), "t.yaml").unwrap(), "node-1").unwrap()
}

#[tokio::test]
async fn creates_missing_mirror() {
    let storage = MemoryStorage::new();
    let desired = vec![static_pod("sch", ":v1")];
    reconcile_mirror_pods(&storage, "node-1", &desired)
        .await
        .unwrap();
    let key = build_key("pods", Some("kube-system"), "sch-node-1");
    let mirror: rusternetes_common::resources::Pod = storage.get(&key).await.unwrap();
    assert!(is_mirror_pod(&mirror));
}

#[tokio::test]
async fn recreates_mirror_on_hash_change() {
    let storage = MemoryStorage::new();
    let v1 = vec![static_pod("sch", ":v1")];
    reconcile_mirror_pods(&storage, "node-1", &v1)
        .await
        .unwrap();
    let v2 = vec![static_pod("sch", ":v2")];
    reconcile_mirror_pods(&storage, "node-1", &v2)
        .await
        .unwrap();
    let key = build_key("pods", Some("kube-system"), "sch-node-1");
    let mirror: rusternetes_common::resources::Pod = storage.get(&key).await.unwrap();
    let ann = mirror.metadata.annotations.as_ref().unwrap();
    assert_eq!(
        ann.get(CONFIG_MIRROR_ANNOTATION),
        v2[0]
            .metadata
            .annotations
            .as_ref()
            .unwrap()
            .get(CONFIG_HASH_ANNOTATION)
    );
}

#[tokio::test]
async fn unchanged_manifest_does_not_clobber_mirror_status() {
    use rusternetes_common::types::Phase;
    let storage = MemoryStorage::new();
    let desired = vec![static_pod("sch", ":v1")];
    reconcile_mirror_pods(&storage, "node-1", &desired)
        .await
        .unwrap();
    let key = build_key("pods", Some("kube-system"), "sch-node-1");
    // simulate kubelet status sync writing phase=Running onto the mirror
    let mut mirror: rusternetes_common::resources::Pod = storage.get(&key).await.unwrap();
    let mut status = mirror.status.clone().unwrap_or_default();
    status.phase = Some(Phase::Running);
    mirror.status = Some(status);
    storage.update(&key, &mirror).await.unwrap();

    reconcile_mirror_pods(&storage, "node-1", &desired)
        .await
        .unwrap();
    let mirror: rusternetes_common::resources::Pod = storage.get(&key).await.unwrap();
    assert_eq!(
        mirror.status.as_ref().and_then(|s| s.phase.clone()),
        Some(Phase::Running)
    );
}

#[tokio::test]
async fn deletes_stale_mirror_when_manifest_removed() {
    let storage = MemoryStorage::new();
    let desired = vec![static_pod("sch", ":v1")];
    reconcile_mirror_pods(&storage, "node-1", &desired)
        .await
        .unwrap();
    reconcile_mirror_pods(&storage, "node-1", &[])
        .await
        .unwrap();
    let key = build_key("pods", Some("kube-system"), "sch-node-1");
    let got: Result<rusternetes_common::resources::Pod, _> = storage.get(&key).await;
    assert!(got.is_err());
}

#[tokio::test]
async fn does_not_touch_other_nodes_or_regular_pods() {
    let storage = MemoryStorage::new();
    // a regular (non-mirror) pod on this node must never be deleted
    let yaml = YAML_POD.replace("kube-scheduler", "regular");
    let mut regular = parse_manifest(yaml.as_bytes(), "r.yaml").unwrap();
    regular.spec.as_mut().unwrap().node_name = Some("node-1".to_string());
    let rkey = build_key("pods", Some("kube-system"), "regular");
    storage.create(&rkey, &regular).await.unwrap();

    reconcile_mirror_pods(&storage, "node-1", &[])
        .await
        .unwrap();
    assert!(storage
        .get::<rusternetes_common::resources::Pod>(&rkey)
        .await
        .is_ok());
}

use rusternetes_kubelet::static_pods::merge_node_pods;

#[test]
fn merge_prefers_file_version_and_drops_mirror_duplicates() {
    let file_pod = static_pod("sch", ":v1"); // name sch-node-1, node-1
    let mirror = make_mirror_pod(&file_pod); // same name — storage copy
    let yaml = YAML_POD.replace("kube-scheduler", "regular");
    let mut regular = parse_manifest(yaml.as_bytes(), "r.yaml").unwrap();
    regular.spec.as_mut().unwrap().node_name = Some("node-1".to_string());

    let merged = merge_node_pods(vec![mirror, regular], vec![file_pod.clone()], "node-1");
    let names: Vec<_> = merged.iter().map(|p| p.metadata.name.as_str()).collect();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"sch-node-1"));
    assert!(names.contains(&"regular"));
    // the sch-node-1 entry must be the file version (no mirror annotation)
    let sch = merged
        .iter()
        .find(|p| p.metadata.name == "sch-node-1")
        .unwrap();
    assert!(!is_mirror_pod(sch));
}

#[test]
fn merge_filters_other_nodes() {
    let mut other = static_pod("sch", ":v1");
    other.spec.as_mut().unwrap().node_name = Some("node-2".to_string());
    let merged = merge_node_pods(vec![other], vec![], "node-1");
    assert!(merged.is_empty());
}
