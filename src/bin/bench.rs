//! Performance benchmark for insert, read, and subscription fan-out at
//! four record scales: 10k, 100k, 1M, 10M.
//!
//! Run: `cargo run --release --bin bench`
//!
//! Three operations are measured:
//!   insert   — bulk-load via store_batch (1 000 records/txn), then single-commit
//!              latency on a fresh sample so both throughput and per-op overhead
//!              are visible.
//!   read     — query_window at three radii after the bulk load.  Hit count and
//!              wall-time over 5 repeated queries are reported so you can see how
//!              result-set size affects latency.
//!   subscribe— the extra cost added to each single-commit insert when the
//!              server fan-out loop is active.  Measured as overhead % over
//!              a no-subscription baseline using the same 10k-record sample
//!              at every scale.

use std::time::{Duration, Instant};

use interstellar_db::{HybridSpatioTemporalIndexer, SpatioTemporalStore};
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Simple deterministic LCG — avoids adding the `rand` crate
// ---------------------------------------------------------------------------

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self { Self(seed ^ 0xdeadbeef_cafebabe) }

    fn next(&mut self) -> u64 {
        self.0 = self.0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn f64_in(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (self.next() as f64 / u64::MAX as f64) * (hi - lo)
    }

    fn u64_in(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

// ---------------------------------------------------------------------------
// Record type
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Rec { x: f64, y: f64, z: f64, secs: u64 }

fn gen_records(n: usize, seed: u64) -> Vec<Rec> {
    let mut rng = Lcg::new(seed);
    (0..n)
        .map(|_| Rec {
            x: rng.f64_in(-950.0, 950.0),
            y: rng.f64_in(-950.0, 950.0),
            z: rng.f64_in(-950.0, 950.0),
            secs: rng.u64_in(0, 86_400),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 1_000;
const QUERY_RUNS: usize = 5;
// Timestamp offsets so sample inserts don't collide with bulk load keys.
const SINGLE_TS_OFFSET: u64 = 100_000;
const SUB_BASELINE_OFFSET: u64 = 200_000;
const SUB_ACTIVE_OFFSET: u64 = 300_000;

/// Average duration over `runs` calls to `query_window` with custom time window.
async fn bench_query_window(
    store: &SpatioTemporalStore,
    radius: f64,
    start_secs: u64,
    end_secs: u64,
) -> (usize, Duration) {
    let mut total = Duration::ZERO;
    let mut hits = 0;
    for _ in 0..QUERY_RUNS {
        let t = Instant::now();
        let obs = store.query_window(0.0, 0.0, 0.0, radius, start_secs, end_secs).unwrap();
        total += t.elapsed();
        hits = obs.len();
    }
    (hits, total / QUERY_RUNS as u32)
}

/// Average duration over `runs` calls to `query_window`.
async fn bench_query(
    store: &SpatioTemporalStore,
    radius: f64,
) -> (usize, Duration) {
    bench_query_window(store, radius, 0, 86_400).await
}

/// (overhead_fraction, n_matches)
/// overhead_fraction = (with_sub_elapsed - baseline_elapsed) / baseline_elapsed
async fn bench_subscribe(store: &SpatioTemporalStore, n_sample: usize) -> (f64, usize) {
    let records = gen_records(n_sample, 77);
    let r2 = 200.0_f64.powi(2);

    // Baseline: plain single-commit inserts.
    let t0 = Instant::now();
    for (i, r) in records.iter().enumerate() {
        store
            .store(r.x, r.y, r.z, r.secs + SUB_BASELINE_OFFSET, i as u64, b"1")
            .await
            .unwrap();
    }
    let baseline = t0.elapsed();

    // With subscription: drain notifications in background to avoid back-pressure.
    let (tx, mut rx) = mpsc::channel::<()>(n_sample);
    tokio::spawn(async move {
        while rx.recv().await.is_some() {}
    });

    let mut n_match = 0usize;
    let t0 = Instant::now();
    for (i, r) in records.iter().enumerate() {
        store
            .store(r.x, r.y, r.z, r.secs + SUB_ACTIVE_OFFSET, i as u64, b"1")
            .await
            .unwrap();
        // Mirror the server's fan-out loop cost.
        let (dx, dy, dz) = (r.x, r.y, r.z);
        if dx * dx + dy * dy + dz * dz <= r2 {
            let _ = tx.try_send(());
            n_match += 1;
        }
    }
    let with_sub = t0.elapsed();

    let overhead = (with_sub.as_secs_f64() - baseline.as_secs_f64()) / baseline.as_secs_f64();
    (overhead.max(0.0), n_match)
}

// ---------------------------------------------------------------------------
// Per-scale runner
// ---------------------------------------------------------------------------

fn fmt_count(n: usize) -> String {
    if n >= 1_000_000 { format!("{:.1}M", n as f64 / 1_000_000.0) }
    else if n >= 1_000 { format!("{:.0}k", n as f64 / 1_000.0) }
    else { n.to_string() }
}

async fn run_scale(n: usize) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = format!("/tmp/interstellar-bench-{n}-{ts}");
    let _ = std::fs::remove_dir_all(&db_path);

    let store =
        SpatioTemporalStore::open(&db_path, 1000.0, 3_600).expect("open store");

    // ── Bulk load ──────────────────────────────────────────────────────────
    let records = gen_records(n, 42);
    let t0 = Instant::now();
    for chunk in records.chunks(BATCH_SIZE) {
        let batch: Vec<_> = chunk.iter().map(|r| (r.x, r.y, r.z, r.secs, 0u64, b"1" as &[u8])).collect();
        store.store_batch(&batch).await.expect("store_batch");
    }
    let bulk_elapsed = t0.elapsed();
    let bulk_rps = n as f64 / bulk_elapsed.as_secs_f64();

    // ── Single-commit latency (1 000-record sample) ─────────────────────
    let n_single = 1_000;
    let sample = gen_records(n_single, 99);
    let t0 = Instant::now();
    for (i, r) in sample.iter().enumerate() {
        store
            .store(r.x, r.y, r.z, r.secs + SINGLE_TS_OFFSET, i as u64, b"1")
            .await
            .expect("store");
    }
    let single_elapsed = t0.elapsed();
    let single_us = single_elapsed.as_micros() as f64 / n_single as f64;

    // ── Queries ────────────────────────────────────────────────────────────
    let (hits_50, q50)   = bench_query(&store, 50.0).await;
    let (hits_200, q200) = bench_query(&store, 200.0).await;
    let (hits_500, q500) = bench_query(&store, 500.0).await;

    // ── Subscribe overhead ─────────────────────────────────────────────────
    let n_sub_sample = 10_000;
    let (overhead, n_match) = bench_subscribe(&store, n_sub_sample).await;

    // ── Print section ──────────────────────────────────────────────────────
    let sep = "─".repeat(72);
    println!("\nScale: {}", fmt_count(n));
    println!("{sep}");
    println!(
        "  Insert (batch {BATCH_SIZE}/txn) : {:>10.0} rec/s  ({:.2}s total)",
        bulk_rps, bulk_elapsed.as_secs_f64()
    );
    println!(
        "  Insert (single commit, {n_single} sample) : {:>7.1} µs/rec",
        single_us
    );
    println!();
    println!("  Queries (avg of {QUERY_RUNS} runs, full time window [0, 86400 s]):");
    println!(
        "    radius =  50  →  {:>7} hits   {:>8.2} ms",
        hits_50,  q50.as_secs_f64()  * 1_000.0
    );
    println!(
        "    radius = 200  →  {:>7} hits   {:>8.2} ms",
        hits_200, q200.as_secs_f64() * 1_000.0
    );
    println!(
        "    radius = 500  →  {:>7} hits   {:>8.2} ms",
        hits_500, q500.as_secs_f64() * 1_000.0
    );
    println!();
    println!(
        "  Subscribe overhead ({n_sub_sample} single-commit inserts, 1 sub @ r=200):"
    );
    println!(
        "    {n_match} / {n_sub_sample} inserts matched   overhead: {:.1}%",
        overhead * 100.0
    );

    drop(store);
    let _ = std::fs::remove_dir_all(&db_path);
}

// ---------------------------------------------------------------------------
// Spatial-config comparison
//
// calculate_optimal_depth always targets ≤ 2 Morton cells per axis (8 total).
// What changes with a larger domain is the *resolution* of those cells:
//
//   max_range=1000,  r=500  → depth=3,  32 cells/axis → each cell=1000u wide
//   max_range=50000, r=1000 → depth=15, 32 cells/axis → each cell=3125u wide
//
// At 10M records: first case scans ~10M rows per query; second scans ~2440.
// ---------------------------------------------------------------------------

fn cell_width(max_range: f64, depth: usize) -> f64 {
    (2.0 * max_range) / (2.0_f64.powi((depth / 3) as i32))
}

async fn run_spatial_config(label: &str, max_range: f64, radius: f64, n: usize) {
    let depth = HybridSpatioTemporalIndexer::new(max_range, 3600).calculate_optimal_depth(radius);
    let cells_per_axis = 2_usize.pow((depth / 3) as u32);
    let total_cells = cells_per_axis.pow(3);
    let cell_w = cell_width(max_range, depth);
    let records_per_cell = n as f64 / total_cells as f64;

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = format!("/tmp/interstellar-spatial-{ts}");
    let _ = std::fs::remove_dir_all(&db_path);
    let store = SpatioTemporalStore::open(&db_path, max_range, 3_600).unwrap();

    // Records distributed across the full space.
    let scale = max_range * 0.95;
    let mut rng = Lcg::new(42);
    let records: Vec<Rec> = (0..n)
        .map(|_| Rec {
            x: rng.f64_in(-scale, scale),
            y: rng.f64_in(-scale, scale),
            z: rng.f64_in(-scale, scale),
            secs: rng.u64_in(0, 86_400),
        })
        .collect();

    for chunk in records.chunks(BATCH_SIZE) {
        let batch: Vec<_> = chunk.iter().map(|r| (r.x, r.y, r.z, r.secs, 0u64, b"1" as &[u8])).collect();
        store.store_batch(&batch).await.unwrap();
    }

    let (hits, elapsed) = bench_query(&store, radius).await;
    let scan_pct = hits as f64 / n as f64 * 100.0;

    println!(
        "  {label:<30}  depth={depth:>2}  cell_w={cell_w:>8.0}  \
         recs/cell≈{records_per_cell:>8.0}  hits={hits:>8}  ({scan_pct:.3}%)  {:.1}ms",
        elapsed.as_secs_f64() * 1_000.0
    );

    drop(store);
    let _ = std::fs::remove_dir_all(&db_path);
}

// ---------------------------------------------------------------------------
// Temporal scaling helpers
// ---------------------------------------------------------------------------

/// Insert `n_total` records distributed uniformly across `n_epochs` epochs,
/// each epoch spanning `epoch_secs` seconds.  Returns the store path so the
/// caller can clean up.
async fn build_temporal_store(
    n_total: usize,
    n_epochs: usize,
    epoch_secs: u64,
    max_range: f64,
    label: &str,
) -> (SpatioTemporalStore, String) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = format!("/tmp/interstellar-temporal-{label}-{ts}");
    let _ = std::fs::remove_dir_all(&db_path);

    let store = SpatioTemporalStore::open(&db_path, max_range, epoch_secs as u32).unwrap();
    let span = (n_epochs as u64) * epoch_secs;
    let scale = max_range * 0.95;
    let mut rng = Lcg::new(42);

    let batch: Vec<_> = (0..n_total)
        .map(|_| {
            (
                rng.f64_in(-scale, scale),
                rng.f64_in(-scale, scale),
                rng.f64_in(-scale, scale),
                rng.u64_in(0, span),  // spread uniformly across all n_epochs
                0u64,
                b"1" as &[u8],
            )
        })
        .collect();

    for chunk in batch.chunks(BATCH_SIZE) {
        store.store_batch(chunk).await.unwrap();
    }

    (store, db_path)
}

/// Part 1: 10M records spread across N epochs; always query epoch 0.
/// Shows that current-epoch query cost ∝ recs/epoch, not total records.
async fn run_temporal_part1() {
    const N: usize = 10_000_000;
    const EPOCH_SECS: u64 = 3_600;
    const MAX_RANGE: f64 = 1_000.0;
    const RADIUS: f64 = 200.0;

    for &n_epochs in &[1usize, 10, 100, 1_000] {
        let recs_per_epoch = N / n_epochs;
        let (store, db_path) =
            build_temporal_store(N, n_epochs, EPOCH_SECS, MAX_RANGE, &n_epochs.to_string()).await;

        // Query epoch 0 only.
        // end_secs = epoch_secs - 1 so we stay strictly inside epoch 0.
        let (hits, elapsed) = bench_query_window(&store, RADIUS, 0, EPOCH_SECS - 1).await;

        // Records in epoch 0 ≈ recs_per_epoch; the scan touches at most a few
        // spatial cells worth of those, not the records in epochs 1..N-1.
        println!(
            "  {:>10}  {:>12}  {:>12}  {:>8}  {:>7.1}ms",
            n_epochs,
            recs_per_epoch,
            format!("≤{}", recs_per_epoch),
            hits,
            elapsed.as_secs_f64() * 1_000.0
        );

        drop(store);
        let _ = std::fs::remove_dir_all(&db_path);
    }
}

/// Part 2: 10M records across 1000 epochs (~10k/epoch); grow the query window.
/// Shows cost grows linearly with epochs queried, not with total history.
async fn run_temporal_part2() {
    const N: usize = 10_000_000;
    const N_EPOCHS: usize = 1_000;
    const EPOCH_SECS: u64 = 3_600;
    const MAX_RANGE: f64 = 1_000.0;
    const RADIUS: f64 = 200.0;

    let (store, db_path) =
        build_temporal_store(N, N_EPOCHS, EPOCH_SECS, MAX_RANGE, "part2").await;

    for &window_epochs in &[1usize, 2, 5, 10, 50, 100] {
        let end_secs = (window_epochs as u64) * EPOCH_SECS - 1;
        let approx_scanned = (N / N_EPOCHS) * window_epochs;

        let (hits, elapsed) = bench_query_window(&store, RADIUS, 0, end_secs).await;

        println!(
            "  {:>14}  {:>12}  {:>8}  {:>7.1}ms",
            window_epochs,
            format!("≈{}", approx_scanned),
            hits,
            elapsed.as_secs_f64() * 1_000.0
        );
    }

    drop(store);
    let _ = std::fs::remove_dir_all(&db_path);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    println!("═══════════════════════════════════════════════════════════════════════");
    println!(" interstellar-db benchmark");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!(" Indexer: max_range=1000, epoch_duration=3600 s");
    println!(" Spatial distribution: uniform in [-950, 950]³");
    println!(" Time distribution:    uniform in [0, 86400 s]  (24 epochs)");
    println!();
    println!(" Queries are centred on (0,0,0); hit counts scale with sphere volume.");
    println!(" Subscribe overhead = (single-insert-with-sub − baseline) / baseline.");
    println!("═══════════════════════════════════════════════════════════════════════");

    for &n in &[10_000_usize, 100_000, 1_000_000, 10_000_000] {
        run_scale(n).await;
    }

    // ── Spatial-config comparison ─────────────────────────────────────────────
    println!("\n═══════════════════════════════════════════════════════════════════════");
    println!(" Spatial-config comparison  (10M records, query at (0,0,0))");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!(" The Morton depth adapts to r/max_range.  A larger domain gives finer");
    println!(" cells for the same absolute radius → far fewer rows reach the exact");
    println!(" sphere filter.  Rows scanned ≈ 8 cells × records_per_cell.");
    println!();
    println!(
        "  {:<30}  {:>8}  {:>12}  {:>14}  {:>8}  {:>8}  {:>6}",
        "config (max_range, r)", "depth", "cell_w", "recs/cell", "hits", "(%)", "time"
    );
    println!("  {}", "─".repeat(92));

    let n = 10_000_000;
    run_spatial_config("max=  1 000, r=   50  ( 5%)", 1_000.0,    50.0,  n).await;
    run_spatial_config("max=  1 000, r=  500  (50%)", 1_000.0,   500.0,  n).await;
    run_spatial_config("max= 50 000, r=  500  ( 1%)", 50_000.0,  500.0,  n).await;
    run_spatial_config("max= 50 000, r= 1 000 ( 2%)", 50_000.0, 1_000.0, n).await;
    run_spatial_config("max= 50 000, r= 5 000 (10%)", 50_000.0, 5_000.0, n).await;
    run_spatial_config("max= 50 000, r=25 000 (50%)", 50_000.0, 25_000.0, n).await;

    // ── Temporal-scaling section ──────────────────────────────────────────────
    println!("\n═══════════════════════════════════════════════════════════════════════");
    println!(" Temporal scaling  (10M records, max_range=1000, r=200)");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!(" Part 1 — epoch partitioning hides history from current queries.");
    println!(" All 10M records exist; they are spread across 1 / 10 / 100 / 1000");
    println!(" epochs.  The query always targets epoch 0 only.");
    println!();
    println!("  {:>10}  {:>12}  {:>12}  {:>8}  {:>8}",
             "n_epochs", "recs/epoch", "recs scanned", "hits", "time");
    println!("  {}", "─".repeat(58));
    run_temporal_part1().await;

    println!();
    println!(" Part 2 — window cost scales with epochs queried, not total history.");
    println!(" 10M records, 1000 epochs (~10k recs/epoch).  Query window grows.");
    println!();
    println!("  {:>14}  {:>12}  {:>8}  {:>8}",
             "window (epochs)", "recs scanned", "hits", "time");
    println!("  {}", "─".repeat(46));
    run_temporal_part2().await;

    println!("\n═══════════════════════════════════════════════════════════════════════");
}
