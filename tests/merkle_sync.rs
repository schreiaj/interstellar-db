//! Two-store anti-entropy convergence: peers with divergent contents exchange
//! only Merkle-diffed key ranges and end up identical.

use interstellar_db::SpatioTemporalStore;

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "interstellar-merkle-{tag}-{}-{}",
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

fn open(dir: &TempDir) -> SpatioTemporalStore {
    SpatioTemporalStore::open(&dir.0, 1000.0, 3600).unwrap()
}

/// One full bidirectional anti-entropy round between two peers.
async fn sync(a: &SpatioTemporalStore, b: &SpatioTemporalStore) {
    let plan = a.sync_plan(&b.merkle_forest());
    for (start, end) in &plan {
        for (key, payload) in a.scan_range(start, end).unwrap() {
            b.ingest(key, &payload).await.unwrap();
        }
        for (key, payload) in b.scan_range(start, end).unwrap() {
            a.ingest(key, &payload).await.unwrap();
        }
    }
}

fn assert_converged(a: &SpatioTemporalStore, b: &SpatioTemporalStore) {
    let fa = a.merkle_forest();
    let fb = b.merkle_forest();
    assert_eq!(fa.epoch_roots(), fb.epoch_roots(), "epoch roots must match");
    assert!(a.sync_plan(&fb).is_empty(), "no divergent ranges may remain");
    assert!(b.sync_plan(&fa).is_empty());
}

#[tokio::test]
async fn divergent_stores_converge_after_one_round() {
    let (da, db) = (TempDir::new("a"), TempDir::new("b"));
    let a = open(&da);
    let b = open(&db);

    // Shared observations, written to both.
    for i in 0..40u64 {
        let (x, y, z) = (i as f64 * 23.0 - 500.0, i as f64 * -11.0 + 200.0, 5.0);
        let payload = format!("shared-{i}");
        a.store(x, y, z, 100 + i, 0, payload.as_bytes()).await.unwrap();
        b.store(x, y, z, 100 + i, 0, payload.as_bytes()).await.unwrap();
    }
    // Disjoint observations, spanning two epochs.
    for i in 0..15u64 {
        a.store(300.0 + i as f64, -40.0, 12.0, 200 + i, 0, format!("a-only-{i}").as_bytes())
            .await
            .unwrap();
        b.store(-700.0, 90.0 + i as f64, -3.0, 4000 + i, 0, format!("b-only-{i}").as_bytes())
            .await
            .unwrap();
    }

    // Sanity: they disagree before syncing.
    assert!(!a.sync_plan(&b.merkle_forest()).is_empty());

    sync(&a, &b).await;
    assert_converged(&a, &b);

    // Both sides now answer queries over the union.
    for store in [&a, &b] {
        let hits = store.query_window(300.0, -40.0, 12.0, 60.0, 200, 220).unwrap();
        assert!(hits.len() >= 15, "a-only rows visible on both peers");
        let hits = store.query_window(-700.0, 97.0, -3.0, 60.0, 4000, 4020).unwrap();
        assert!(hits.len() >= 15, "b-only rows visible on both peers");
    }
}

#[tokio::test]
async fn same_key_payload_conflict_resolves_deterministically() {
    let (da, db) = (TempDir::new("ca"), TempDir::new("cb"));
    let a = open(&da);
    let b = open(&db);

    // Identical 4-D point → identical key, different payloads.
    let ka = a.store(1.0, 2.0, 3.0, 500, 0, b"version-from-a").await.unwrap();
    let kb = b.store(1.0, 2.0, 3.0, 500, 0, b"version-from-b").await.unwrap();
    assert_eq!(ka, kb, "same point must produce the same key on both peers");

    sync(&a, &b).await;
    assert_converged(&a, &b);

    let pa = &a.scan_range(&ka, &ka).unwrap()[0].1;
    let pb = &b.scan_range(&kb, &kb).unwrap()[0].1;
    assert_eq!(pa, pb, "both peers must settle on the same winning payload");
}

#[tokio::test]
async fn ingest_is_idempotent_and_over_shipping_is_safe() {
    let (da, db) = (TempDir::new("ia"), TempDir::new("ib"));
    let a = open(&da);
    let b = open(&db);

    let key = a.store(10.0, 20.0, 30.0, 750, 123, b"row").await.unwrap();
    let root_before = a.merkle_forest().epoch_roots();

    // Re-delivering a row the peer already has must change nothing.
    assert!(b.ingest(key, b"row").await.unwrap());
    assert!(!b.ingest(key, b"row").await.unwrap(), "duplicate delivery is a no-op");
    assert!(!a.ingest(key, b"row").await.unwrap(), "self re-ingest is a no-op");

    assert_eq!(a.merkle_forest().epoch_roots(), root_before);
    assert_converged(&a, &b);
}

#[tokio::test]
async fn forest_survives_reopen() {
    let dir = TempDir::new("reopen");
    let roots_before = {
        let s = open(&dir);
        for i in 0..25u64 {
            s.store(i as f64 * 31.0 - 400.0, 50.0, -60.0, 900 + i * 400, 0, b"persisted")
                .await
                .unwrap();
        }
        let roots = s.merkle_forest().epoch_roots();
        s.close().await.unwrap();
        roots
    };

    let reopened = open(&dir);
    assert_eq!(
        reopened.merkle_forest().epoch_roots(),
        roots_before,
        "rebuilt forest must match the pre-close forest exactly"
    );
}
