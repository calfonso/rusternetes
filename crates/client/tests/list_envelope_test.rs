use rusternetes_client::http::KubernetesList;

#[test]
fn list_envelope_captures_resource_version() {
    let json = r#"{"apiVersion":"v1","kind":"PodList",
        "metadata":{"resourceVersion":"42"},
        "items":[{"metadata":{"name":"p"}}]}"#;
    let list: KubernetesList<serde_json::Value> = serde_json::from_str(json).unwrap();
    assert_eq!(
        list.metadata.as_ref().unwrap().resource_version.as_deref(),
        Some("42")
    );
    assert_eq!(list.items.len(), 1);
}

#[test]
fn list_envelope_tolerates_missing_metadata() {
    let json = r#"{"apiVersion":"v1","kind":"PodList","items":[]}"#;
    let list: KubernetesList<serde_json::Value> = serde_json::from_str(json).unwrap();
    assert!(list.metadata.is_none());
}
