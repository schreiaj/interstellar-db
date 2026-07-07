//! Epoch sealing: settled epochs persist compact Merkle seal records so a
//! reopen rebuilds the forest without scanning their data — and late writes
//! into a sealed epoch safely invalidate the seal.

use interstellar_db::SpatioTemporalStore;

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "interstellar-seal-{tag}-{}-{}",
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

const EPOCH: u64 = 3600;
const GRACE: u64 = 3600;

fn open(dir: &TempDir) -> SpatioTemporalStore {
    SpatioTemporalStore::open(&dir.0, 1000.0, EPOCH as u32).unwrap()
}

/// Populate epochs 0, 1, 2 with distinct observations.
async fn populate(s: &SpatioTemporalStore) {
    for epoch in 0..3u64 {
        for i in 0..10u64 {
            s.store(
                epoch as f64 * 100.0 + i as f64 * 7.0 - 400.0,
                -200.0 + i as f64 * 31.0,
                50.0,
                epoch * EPOCH + 100 + i,
                0,
                format!("e{epoch}-i{i}").as_bytes(),
            )
            .await
            .unwrap();
        }
    }
}

/// A `now` that makes epochs 0 and 1 settled but leaves epoch 2 live:
/// epoch 1 ends at 7200, so now − grace must reach 7200.
const NOW_SETTLING_0_AND_1: u64 = 2 * EPOCH + GRACE;

#[tokio::test]
async fn seal_persists_forest_across_reopen() {
    let dir = TempDir::new("reopen");
    let roots_before = {
        let s = open(&dir);
        populate(&s).await;
        let sealed = s.seal_settled_epochs(NOW_SETTLING_0_AND_1, GRACE).await.unwrap();
        assert_eq!(sealed, vec![0, 1], "exactly the settled epochs get sealed");
        assert_eq!(s.sealed_epochs(), vec![0, 1]);
        let roots = s.merkle_forest().epoch_roots();
        s.close().await.unwrap();
        roots
    };

    let reopened = open(&dir);
    assert_eq!(reopened.sealed_epochs(), vec![0, 1], "seal records survive reopen");
    assert_eq!(
        reopened.merkle_forest().epoch_roots(),
        roots_before,
        "forest rebuilt from seal records + live-epoch scan must match exactly"
    );
    // Sealed epochs remain fully queryable and descendable.
    let hits = reopened.query_window(-400.0, -200.0, 50.0, 500.0, 0, 3 * EPOCH).unwrap();
    assert_eq!(hits.len(), 30);
}

#[tokio::test]
async fn sealing_is_idempotent_and_respects_cutoff() {
    let dir = TempDir::new("cutoff");
    let s = open(&dir);
    populate(&s).await;

    // Nothing settled yet: now − grace = 0.
    assert!(s.seal_settled_epochs(GRACE, GRACE).await.unwrap().is_empty());

    let first = s.seal_settled_epochs(NOW_SETTLING_0_AND_1, GRACE).await.unwrap();
    assert_eq!(first, vec![0, 1]);
    // Second call: nothing new to seal.
    let second = s.seal_settled_epochs(NOW_SETTLING_0_AND_1, GRACE).await.unwrap();
    assert!(second.is_empty());
    // Time passes; epoch 2 settles too.
    let third = s.seal_settled_epochs(3 * EPOCH + GRACE, GRACE).await.unwrap();
    assert_eq!(third, vec![2]);
}

#[tokio::test]
async fn late_write_unseals_and_reopen_stays_correct() {
    let dir = TempDir::new("late");
    let roots_after_late_write = {
        let s = open(&dir);
        populate(&s).await;
        s.seal_settled_epochs(NOW_SETTLING_0_AND_1, GRACE).await.unwrap();
        assert_eq!(s.sealed_epochs(), vec![0, 1]);

        // A DTN-style late arrival lands in sealed epoch 0.
        s.store(333.0, -333.0, 12.0, 500, 0, b"late-arrival").await.unwrap();
        assert_eq!(s.sealed_epochs(), vec![1], "the written epoch must unseal");

        let roots = s.merkle_forest().epoch_roots();
        s.close().await.unwrap();
        roots
    };

    // Reopen: epoch 0 has no seal record now, so it is rescanned — and the
    // rescan must include the late arrival.
    let reopened = open(&dir);
    assert_eq!(reopened.sealed_epochs(), vec![1]);
    assert_eq!(reopened.merkle_forest().epoch_roots(), roots_after_late_write);

    // And the epoch can be resealed afterwards.
    let resealed = reopened.seal_settled_epochs(NOW_SETTLING_0_AND_1, GRACE).await.unwrap();
    assert_eq!(resealed, vec![0]);
    reopened.close().await.unwrap();

    let reopened_again = open(&dir);
    assert_eq!(reopened_again.sealed_epochs(), vec![0, 1]);
    assert_eq!(reopened_again.merkle_forest().epoch_roots(), roots_after_late_write);
}

#[tokio::test]
async fn sync_into_sealed_epoch_converges() {
    let (da, db) = (TempDir::new("sync-a"), TempDir::new("sync-b"));
    let a = open(&da);
    let b = open(&db);

    populate(&a).await;
    populate(&b).await;
    a.seal_settled_epochs(NOW_SETTLING_0_AND_1, GRACE).await.unwrap();

    // B holds an observation in an epoch A has sealed.
    b.store(-11.0, 22.0, -33.0, 150, 0, b"only-b-epoch0").await.unwrap();

    // One anti-entropy round.
    let plan = a.sync_plan(&b.merkle_forest());
    assert!(!plan.is_empty());
    for (start, end) in &plan {
        for (key, payload) in b.scan_range(start, end).unwrap() {
            a.ingest(key, &payload).await.unwrap();
        }
        for (key, payload) in a.scan_range(start, end).unwrap() {
            b.ingest(key, &payload).await.unwrap();
        }
    }

    assert_eq!(
        a.merkle_forest().epoch_roots(),
        b.merkle_forest().epoch_roots(),
        "peers converge even when the diff lands in a sealed epoch"
    );
    assert_eq!(a.sealed_epochs(), vec![1], "ingest into sealed epoch 0 unsealed it");
}
