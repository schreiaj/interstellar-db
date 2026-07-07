//! End-to-end mesh sync over real gRPC: two stores behind tonic servers on
//! ephemeral ports converge by pulling from each other with `sync_once`.

use std::sync::Arc;

use interstellar_db::interstellar::interstellar_sync_client::InterstellarSyncClient;
use interstellar_db::interstellar::interstellar_sync_server::InterstellarSyncServer;
use interstellar_db::{sync_once, SpatioTemporalStore, SyncService};
use tokio_stream::wrappers::TcpListenerStream;

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "interstellar-grpc-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
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

/// Serve one store's sync service on an ephemeral port; returns its URI and
/// the server task handle (aborted on test exit).
async fn serve(store: Arc<SpatioTemporalStore>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InterstellarSyncServer::new(SyncService::new(store)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn two_grpc_nodes_converge_by_pulling_from_each_other() {
    let (da, db) = (TempDir::new("a"), TempDir::new("b"));
    let a = Arc::new(SpatioTemporalStore::open(&da.0, 1000.0, 3600).unwrap());
    let b = Arc::new(SpatioTemporalStore::open(&db.0, 1000.0, 3600).unwrap());

    // Shared history plus divergent tails on both sides, across two epochs.
    for i in 0..30u64 {
        let (x, y, z) = (i as f64 * 17.0 - 250.0, i as f64 * -9.0 + 100.0, 40.0);
        a.store(x, y, z, 100 + i, 0, b"shared").await.unwrap();
        b.store(x, y, z, 100 + i, 0, b"shared").await.unwrap();
    }
    for i in 0..12u64 {
        a.store(400.0 + i as f64, 10.0, -20.0, 300 + i, 0, format!("a{i}").as_bytes())
            .await
            .unwrap();
        b.store(-450.0, 60.0 + i as f64, 33.0, 4200 + i, 0, format!("b{i}").as_bytes())
            .await
            .unwrap();
    }
    // Same key, conflicting payloads.
    a.store(7.0, 8.0, 9.0, 777, 0, b"conflict-a").await.unwrap();
    b.store(7.0, 8.0, 9.0, 777, 0, b"conflict-b").await.unwrap();

    let (uri_a, srv_a) = serve(Arc::clone(&a)).await;
    let (uri_b, srv_b) = serve(Arc::clone(&b)).await;
    let mut client_of_b = InterstellarSyncClient::connect(uri_b).await.unwrap();
    let mut client_of_a = InterstellarSyncClient::connect(uri_a).await.unwrap();

    // Round 1: each side pulls what the other has.
    let a_stats = sync_once(&a, &mut client_of_b).await.unwrap();
    let b_stats = sync_once(&b, &mut client_of_a).await.unwrap();
    assert!(a_stats.rows_applied > 0, "A must have pulled B's tail");
    assert!(b_stats.rows_applied > 0, "B must have pulled A's tail");

    // Round 2: settles the conflict loser's side; then nothing left to pull.
    sync_once(&a, &mut client_of_b).await.unwrap();
    sync_once(&b, &mut client_of_a).await.unwrap();
    let a_final = sync_once(&a, &mut client_of_b).await.unwrap();
    let b_final = sync_once(&b, &mut client_of_a).await.unwrap();
    assert_eq!(a_final.rows_applied, 0, "A is converged");
    assert_eq!(b_final.rows_applied, 0, "B is converged");

    assert_eq!(
        a.merkle_forest().epoch_roots(),
        b.merkle_forest().epoch_roots(),
        "identical forests after mutual pulls"
    );
    // Both sides answer queries over the union.
    for s in [&a, &b] {
        assert_eq!(s.query_window(406.0, 10.0, -20.0, 30.0, 300, 320).unwrap().len(), 12);
        assert_eq!(s.query_window(-450.0, 66.0, 33.0, 30.0, 4200, 4220).unwrap().len(), 12);
    }

    srv_a.abort();
    srv_b.abort();
}

#[tokio::test]
async fn pull_from_empty_peer_is_a_cheap_no_op() {
    let (da, db) = (TempDir::new("ne-a"), TempDir::new("ne-b"));
    let a = Arc::new(SpatioTemporalStore::open(&da.0, 1000.0, 3600).unwrap());
    let empty = Arc::new(SpatioTemporalStore::open(&db.0, 1000.0, 3600).unwrap());
    a.store(1.0, 2.0, 3.0, 100, 0, b"x").await.unwrap();

    let (uri, srv) = serve(Arc::clone(&empty)).await;
    let mut client = InterstellarSyncClient::connect(uri).await.unwrap();

    // The peer has nothing we lack: no leaves fetched, no rows moved.
    let stats = sync_once(&a, &mut client).await.unwrap();
    assert_eq!(stats.leaves_fetched, 0);
    assert_eq!(stats.rows_received, 0);
    srv.abort();
}
