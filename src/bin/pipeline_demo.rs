/// Mock Kalman filter pipeline demonstrating two query modes via
/// [`SpatioTemporalStore`]:
///
///  1. Single-epoch scan  — real-time update for a fresh track.
///  2. Time-window scan   — reconstruct a stale track from historical data,
///                          then extrapolate its position forward.
use interstellar_db::{Observation, SpatioTemporalStore};

// ---------------------------------------------------------------------------
// Kalman-style linear extrapolation (domain logic, not storage logic)
// ---------------------------------------------------------------------------

/// `obs` must be sorted chronologically.
fn extrapolate_position(obs: &[Observation], to_secs: u64) -> Option<(f64, f64, f64)> {
    if obs.len() < 2 {
        return None;
    }
    let last = &obs[obs.len() - 1];
    let prev = &obs[obs.len() - 2];
    let dt_history = (last.timestamp_secs - prev.timestamp_secs) as f64;
    if dt_history == 0.0 {
        return None;
    }
    let dt_predict = (to_secs as i64 - last.timestamp_secs as i64) as f64;
    let vx = (last.x - prev.x) / dt_history;
    let vy = (last.y - prev.y) / dt_history;
    let vz = (last.z - prev.z) / dt_history;
    Some((
        last.x + vx * dt_predict,
        last.y + vy * dt_predict,
        last.z + vz * dt_predict,
    ))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = format!("/tmp/interstellar-demo-{ts}");

    let store = SpatioTemporalStore::open(&db_path, 1000.0, 3600)
        .expect("failed to open store");

    // Simulates a track active 2 h ago that went silent at t = 4800 s.
    let historical: &[(f64, f64, f64, u64, u64)] = &[
        // epoch 0 — track moving from (0,0,0) toward (30,10,5)
        ( 0.0,  0.0,  0.0,   0, 0),
        ( 5.0,  1.5,  0.8,  60, 0),
        (10.0,  3.0,  1.6, 120, 0),
        (15.0,  4.5,  2.5, 180, 0),
        (20.0,  6.0,  3.3, 240, 0),
        (25.0,  7.5,  4.2, 300, 0),
        (30.0,  9.0,  4.9, 360, 0),
        // epoch 1 — updates become sparse
        (35.0, 10.5,  5.0, 3700, 0),
        (40.0, 12.0,  5.1, 4200, 0),
        // last known position — long silence after this
        (45.0, 13.5,  5.2, 4800, 0),
        // Out-of-range decoy — different region entirely
        (900.0, 900.0, 900.0, 4800, 0),
    ];

    for &(x, y, z, secs, nanos) in historical {
        store.store(x, y, z, secs, nanos, b"1").await.expect("store");
    }

    let now_secs: u64 = 10_800; // 3 h elapsed; track went silent at t = 4800 s

    // ── Mode 1: fresh epoch scan around last known position ──────────────────
    // Scope the window to the single epoch that contains `now_secs`.
    const EPOCH_SECS: u64 = 3600;
    let epoch_start = (now_secs / EPOCH_SECS) * EPOCH_SECS;
    let epoch_end = epoch_start + EPOCH_SECS - 1;
    println!("=== Mode 1: single-epoch update (t = {now_secs} s) ===");
    let fresh = store
        .query_window(45.0, 13.5, 5.2, 20.0, epoch_start, epoch_end)
        .expect("query epoch");
    println!("  {} observation(s) in current epoch", fresh.len());
    if fresh.is_empty() {
        println!("  → no new data; escalating to stale-track prediction\n");
    }

    // ── Mode 2: look back over full history to reconstruct trajectory ────────
    println!("=== Mode 2: stale-track prediction (lookback = {now_secs} s) ===");
    let obs = store
        .query_window(45.0, 13.5, 5.2, 80.0, 0, now_secs)
        .expect("query window");

    println!("\n  Chronological track replay ({} obs):", obs.len());
    println!("  {:<5}  {:>8}  {:>8}  {:>8}  {:>10}", "t(s)", "x", "y", "z", "nanos");
    println!("  {}", "-".repeat(50));
    for o in &obs {
        println!(
            "  {:<5}  {:>8.2}  {:>8.2}  {:>8.2}  {:>10}",
            o.timestamp_secs, o.x, o.y, o.z, o.timestamp_nanos
        );
    }

    // ── Extrapolate forward ──────────────────────────────────────────────────
    println!();
    match extrapolate_position(&obs, now_secs) {
        Some((px, py, pz)) => {
            println!("  Predicted pose at t = {now_secs} s:");
            println!("    x = {px:.2}  y = {py:.2}  z = {pz:.2}");
            println!("  (velocity estimated from last two observations)");
        }
        None => println!("  Not enough observations to extrapolate."),
    }

    // ── Sanity: out-of-range decoy must not appear ───────────────────────────
    let has_decoy = obs.iter().any(|o| (o.x - 900.0).abs() < 1.0);
    println!("\n  Out-of-region decoy excluded: {}", !has_decoy);

    std::fs::remove_dir_all(&db_path).ok();
}
