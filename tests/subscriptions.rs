//! The subscription pipeline: store change tap → SubscriptionHub → consumer,
//! including delivery of observations that arrive via anti-entropy sync and
//! the per-subscription max_age freshness guard.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use interstellar_db::interstellar::interstellar_sync_client::InterstellarSyncClient;
use interstellar_db::interstellar::interstellar_sync_server::InterstellarSyncServer;
use interstellar_db::{sync_once, SpatioTemporalStore, SubscriptionHub, SyncService};
use tokio_stream::wrappers::TcpListenerStream;

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "interstellar-subs-{tag}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(dir: &TempDir) -> Arc<SpatioTemporalStore> {
    Arc::new(SpatioTemporalStore::open(&dir.0, 1000.0, 3600).unwrap())
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// Receive with a timeout so a missing event fails the test instead of
/// hanging it.
async fn expect_event(
    rx: &mut tokio::sync::mpsc::Receiver<interstellar_db::ObservationEvent>,
) -> interstellar_db::ObservationEvent {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for a subscription event")
        .expect("hub dropped the subscription")
}

#[tokio::test]
async fn watch_fires_on_store_and_batch_but_not_noop_ingest() {
    let dir = TempDir::new("tap");
    let store = open(&dir);
    let mut tap = store.watch();

    let key = store.store(1.0, 2.0, 3.0, 100, 7, b"first").await.unwrap();
    let records: &[(f64, f64, f64, u64, u64, &[u8])] =
        &[(10.0, 20.0, 30.0, 200, 0, b"batch-a"), (-10.0, -20.0, -30.0, 300, 0, b"batch-b")];
    store.store_batch(records).await.unwrap();

    let e1 = tap.recv().await.unwrap();
    assert_eq!(e1.key, key);
    assert_eq!(e1.timestamp_secs, 100);
    assert_eq!(e1.timestamp_nanos, 7);
    assert_eq!(*e1.payload, b"first".to_vec());
    assert_eq!(tap.recv().await.unwrap().timestamp_secs, 200);
    assert_eq!(tap.recv().await.unwrap().timestamp_secs, 300);

    // Effective ingest fires; duplicate ingest does not.
    let foreign_key = store.store(5.0, 5.0, 5.0, 400, 0, b"win").await.unwrap();
    let _ = tap.recv().await.unwrap(); // consume the store event
    assert!(!store.ingest(foreign_key, b"win").await.unwrap(), "duplicate is a no-op");
    store.store(6.0, 6.0, 6.0, 500, 0, b"tail").await.unwrap();
    let next = tap.recv().await.unwrap();
    assert_eq!(
        next.timestamp_secs, 500,
        "the no-op ingest must not have published an event before the tail write"
    );
}

#[tokio::test]
async fn hub_filters_by_region_and_max_age() {
    let dir = TempDir::new("hub");
    let store = open(&dir);
    let hub = Arc::new(SubscriptionHub::new());
    tokio::spawn(Arc::clone(&hub).run(store.watch()));

    let mut near = hub.attach(0.0, 0.0, 0.0, 50.0, 0, 16);
    let mut far = hub.attach(800.0, 800.0, 800.0, 10.0, 0, 16);
    let mut fresh_only = hub.attach(0.0, 0.0, 0.0, 50.0, 60, 16);

    let now = now_secs();
    // In the near region, fresh.
    store.store(1.0, 1.0, 1.0, now, 0, b"fresh-near").await.unwrap();
    // In the near region, but hours old — a sync replay stand-in.
    store.store(2.0, 2.0, 2.0, now - 7200, 0, b"stale-near").await.unwrap();

    // Unfiltered near sub sees both.
    assert_eq!(*expect_event(&mut near).await.payload, b"fresh-near".to_vec());
    assert_eq!(*expect_event(&mut near).await.payload, b"stale-near".to_vec());
    // Age-guarded sub sees only the fresh one; verify by sending a marker.
    assert_eq!(*expect_event(&mut fresh_only).await.payload, b"fresh-near".to_vec());
    store.store(3.0, 3.0, 3.0, now, 0, b"marker").await.unwrap();
    assert_eq!(
        *expect_event(&mut fresh_only).await.payload,
        b"marker".to_vec(),
        "the stale observation must have been skipped for the age-guarded sub"
    );
    // The far sub saw nothing: its next event is the marker-region miss — so
    // nothing at all. Give the dispatcher a beat, then confirm it's empty.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(far.try_recv().is_err(), "far region must receive no events");
}

#[tokio::test]
async fn sync_ingest_fires_subscribers_through_the_hub() {
    let (da, db) = (TempDir::new("sync-a"), TempDir::new("sync-b"));
    let a = open(&da);
    let b = open(&db);

    // A subscriber on node A watches a region near the origin.
    let hub = Arc::new(SubscriptionHub::new());
    tokio::spawn(Arc::clone(&hub).run(a.watch()));
    let mut sub = hub.attach(0.0, 0.0, 0.0, 100.0, 0, 16);

    // Node B (remote) takes a write inside that region.
    let now = now_secs();
    b.store(10.0, -10.0, 5.0, now, 0, b"remote-observation").await.unwrap();

    // Serve B's sync service; A pulls from it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InterstellarSyncServer::new(SyncService::new(b)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut client = InterstellarSyncClient::connect(format!("http://{addr}")).await.unwrap();
    let stats = sync_once(&a, &mut client).await.unwrap();
    assert_eq!(stats.rows_applied, 1);

    // The subscriber hears about the synced observation.
    let event = expect_event(&mut sub).await;
    assert_eq!(*event.payload, b"remote-observation".to_vec());
    assert_eq!(event.timestamp_secs, now);
    srv.abort();
}
