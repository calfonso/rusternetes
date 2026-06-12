use rusternetes_client::reflector::{ListWatch, Reflector, StoreEvent};
use rusternetes_client::watch::WatchEvent;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
struct Obj {
    name: String,
    v: u64,
}

/// One scripted watch session: (events, final observed resourceVersion).
type WatchBatch = (Vec<WatchEvent<Obj>>, Option<String>);

struct MockLw {
    // each watch() call pops the next scripted (events, final_rv) batch
    batches: Mutex<Vec<WatchBatch>>,
    list_result: (Vec<Obj>, String),
    watch_calls: Mutex<Vec<Option<String>>>, // recorded resourceVersions
}

#[async_trait::async_trait]
impl ListWatch<Obj> for MockLw {
    async fn list(&self) -> anyhow::Result<(Vec<Obj>, String)> {
        Ok(self.list_result.clone())
    }
    async fn watch(
        &self,
        rv: Option<String>,
    ) -> anyhow::Result<(Vec<WatchEvent<Obj>>, Option<String>)> {
        self.watch_calls.lock().unwrap().push(rv);
        Ok(self.batches.lock().unwrap().remove(0))
    }
}

fn key(o: &Obj) -> String {
    o.name.clone()
}

#[tokio::test]
async fn initial_list_populates_store_and_watch_resumes_from_list_rv() {
    let lw = Arc::new(MockLw {
        batches: Mutex::new(vec![(vec![], None)]),
        list_result: (
            vec![Obj {
                name: "a".into(),
                v: 1,
            }],
            "10".into(),
        ),
        watch_calls: Mutex::new(vec![]),
    });
    let r = Reflector::new(lw.clone(), key);
    r.sync_once().await.unwrap(); // one list + one watch batch
    assert_eq!(r.store().get("a").unwrap().v, 1);
    // watch must have been started from the list's resourceVersion
    assert_eq!(
        lw.watch_calls.lock().unwrap().as_slice(),
        &[Some("10".to_string())]
    );
}

#[tokio::test]
async fn watch_events_mutate_store_and_emit() {
    let lw = Arc::new(MockLw {
        batches: Mutex::new(vec![(
            vec![
                WatchEvent::Added(Obj {
                    name: "b".into(),
                    v: 1,
                }),
                WatchEvent::Modified(Obj {
                    name: "b".into(),
                    v: 2,
                }),
                WatchEvent::Deleted(Obj {
                    name: "b".into(),
                    v: 2,
                }),
            ],
            None,
        )]),
        list_result: (vec![], "1".into()),
        watch_calls: Mutex::new(vec![]),
    });
    let r = Reflector::new(lw, key);
    let mut events = r.subscribe();
    r.sync_once().await.unwrap();
    assert!(r.store().get("b").is_none()); // added, modified, then deleted
    assert!(matches!(events.try_recv().unwrap(), StoreEvent::Added(_)));
    assert!(matches!(
        events.try_recv().unwrap(),
        StoreEvent::Modified(_)
    ));
    assert!(matches!(events.try_recv().unwrap(), StoreEvent::Deleted(_)));
}

#[tokio::test]
async fn bookmark_advances_rv_without_store_change() {
    let lw = Arc::new(MockLw {
        batches: Mutex::new(vec![
            // first watch session: only a bookmark; final observed rv is 20
            (
                vec![WatchEvent::Bookmark(Obj {
                    name: "ignored".into(),
                    v: 0,
                })],
                Some("20".to_string()),
            ),
            // second watch session: nothing
            (vec![], None),
        ]),
        list_result: (
            vec![Obj {
                name: "a".into(),
                v: 1,
            }],
            "10".into(),
        ),
        watch_calls: Mutex::new(vec![]),
    });
    let r = Reflector::new(lw.clone(), key);
    let mut events = r.subscribe();
    r.sync_once().await.unwrap();
    r.sync_once().await.unwrap();
    // second watch resumed from the bookmark-advanced rv
    assert_eq!(
        lw.watch_calls.lock().unwrap().as_slice(),
        &[Some("10".to_string()), Some("20".to_string())]
    );
    // bookmark neither mutates the store nor emits a StoreEvent
    assert_eq!(r.store().get("a").unwrap().v, 1);
    assert!(r.store().get("ignored").is_none());
    assert!(events.try_recv().is_err());
}
