use rusternetes_client::events::build_event;

#[test]
fn build_event_shapes_a_v1_event_for_post() {
    let ev = build_event(
        "default",                              // namespace
        "FailedScheduling",                     // reason
        "0/2 nodes available",                  // message
        "Warning",                              // type
        ("Pod", "default", "web-1", "uid-123"), // involvedObject (kind, ns, name, uid)
        "default-scheduler",                    // reporting component / source
    );
    assert_eq!(ev["involvedObject"]["name"], "web-1");
    assert_eq!(ev["reason"], "FailedScheduling");
    assert_eq!(ev["metadata"]["namespace"], "default");
    assert!(ev["metadata"]["name"]
        .as_str()
        .unwrap()
        .starts_with("web-1."));
}

#[test]
fn build_event_name_is_stable_object_reason_uid() {
    // Mirrors storage's Event::generate_name: {name}.{reason_lc}.{uid_prefix(8)}.
    // Two emissions for the same (object, reason) must collapse onto one name
    // so the api-server deduplicates the recurrence.
    let a = build_event(
        "ns",
        "Scheduled",
        "first",
        "Normal",
        ("Pod", "ns", "web", "abcdef0123456789"),
        "default-scheduler",
    );
    let b = build_event(
        "ns",
        "Scheduled",
        "second",
        "Normal",
        ("Pod", "ns", "web", "abcdef0123456789"),
        "default-scheduler",
    );
    assert_eq!(a["metadata"]["name"], b["metadata"]["name"]);
    assert_eq!(a["metadata"]["name"], "web.scheduled.abcdef01");
}

#[test]
fn build_event_carries_source_component_and_count() {
    let ev = build_event(
        "default",
        "Scheduled",
        "Successfully assigned default/web to node-2",
        "Normal",
        ("Pod", "default", "web", "uid-xyz"),
        "default-scheduler",
    );
    assert_eq!(ev["source"]["component"], "default-scheduler");
    assert_eq!(ev["count"], 1);
    assert_eq!(ev["type"], "Normal");
    assert_eq!(ev["involvedObject"]["uid"], "uid-xyz");
}
