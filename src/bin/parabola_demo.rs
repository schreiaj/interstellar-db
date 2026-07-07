//! Parabolic-arc demo — stores an entity's trajectory via gRPC, queries
//! the detection zone per-frame, and subscribes to async push notifications.
//!
//! Run: `cargo run --bin parabola_demo`
//! (builds and starts the server binary automatically)

use std::time::Duration;

use prost::Message as _;
use prost_types::Any;
use tokio::sync::mpsc;

use interstellar_db::interstellar::{
    interstellar_db_client::InterstellarDbClient, EntityState, QueryResponse, QueryWindowRequest,
    StoreRequest, SubscribeRequest,
};

// ---------------------------------------------------------------------------
// Simulation parameters
// ---------------------------------------------------------------------------

const FRAMES: usize = 60;   // one frame per simulated second
const MAX_X: f64   = 500.0; // trajectory spans x ∈ [−500, 500]
const PEAK_Y: f64  = 400.0; // parabola peaks at this height at the midpoint

// Detection zone: a sphere centred just below the peak.
const ZONE_CX: f64 = 0.0;
const ZONE_CY: f64 = 390.0;
const ZONE_CZ: f64 = 0.0;
const ZONE_R:  f64 = 60.0;

fn position(frame: usize) -> (f64, f64, f64) {
    let t = frame as f64 / (FRAMES - 1) as f64;
    let x = -MAX_X + 2.0 * MAX_X * t;
    let y = PEAK_Y * 4.0 * t * (1.0 - t);
    (x, y, 0.0)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Find and start the server ─────────────────────────────────────────
    let exe = std::env::current_exe()?;
    let server_bin = exe.parent().unwrap().join("server");
    if !server_bin.exists() {
        eprintln!(
            "server binary not found at {server_bin:?}\n\
             Run `cargo build --bin server` first."
        );
        std::process::exit(1);
    }

    let db_path = "/tmp/interstellar-parabola-demo";
    let port = "50099";
    let _ = std::fs::remove_dir_all(db_path);
    let _ = std::process::Command::new("sh")
        .args(["-c", &format!("lsof -ti :{port} | xargs kill -9 2>/dev/null; true")])
        .status();

    let mut server_proc = std::process::Command::new(&server_bin)
        .env("DB_PATH", db_path)
        .env("PORT", port)
        .stderr(std::process::Stdio::null())
        .spawn()?;

    // ── 2. Connect — retry until server is ready ─────────────────────────────
    let endpoint = format!("http://[::1]:{port}");
    let client = {
        let mut last_err = None;
        let mut connected = None;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            match InterstellarDbClient::connect(endpoint.clone()).await {
                Ok(c) => { connected = Some(c); break; }
                Err(e) => { last_err = Some(e); }
            }
        }
        connected.ok_or_else(|| last_err.unwrap())?
    };

    // ── 3. Subscribe to the detection zone ───────────────────────────────────
    // Open the subscription before the simulation loop so no events are missed.
    let (notify_tx, mut notify_rx) = mpsc::channel::<QueryResponse>(256);
    {
        let mut sub_client = client.clone();
        tokio::spawn(async move {
            let Ok(response) = sub_client.subscribe(SubscribeRequest {
                center_x: ZONE_CX, center_y: ZONE_CY, center_z: ZONE_CZ, radius: ZONE_R,
                max_age_secs: 0,
            }).await else { return };

            let mut stream = response.into_inner();
            while let Ok(Some(resp)) = stream.message().await {
                if notify_tx.send(resp).await.is_err() {
                    break;
                }
            }
        });
    }

    // ── 4. Print header ──────────────────────────────────────────────────────
    let mut client = client; // reclaim after the clone above
    println!();
    println!("InterstellarDB — parabolic arc demo");
    println!("Trajectory : x ∈ [−{MAX_X}, {MAX_X}], y = parabola (peak {PEAK_Y}), z = 0");
    println!("Detection  : center=({ZONE_CX}, {ZONE_CY}, {ZONE_CZ})  radius={ZONE_R}");
    println!();
    println!(" frame       x        y      z   zone?");
    println!("─────────────────────────────────────────");

    let mut zone_count = 0usize;
    let mut first_in: Option<usize> = None;
    let mut last_in: Option<usize> = None;

    // ── 5. Simulate ──────────────────────────────────────────────────────────
    for frame in 0..FRAMES {
        let (x, y, z) = position(frame);
        let t_secs = frame as u64;

        let mut buf = Vec::new();
        EntityState { entity_id: "obj_1".to_string() }.encode(&mut buf)?;
        let payload = Any {
            type_url: "type.googleapis.com/interstellar.EntityState".to_string(),
            value: buf,
        };

        // Store fires the subscription fan-out server-side.
        client.store(StoreRequest {
            x, y, z,
            timestamp_secs: t_secs, timestamp_nanos: 0,
            payload: Some(payload),
        }).await?;

        // Synchronous per-frame query to drive the zone? column.
        let mut stream = client.query_window(QueryWindowRequest {
            center_x: ZONE_CX, center_y: ZONE_CY, center_z: ZONE_CZ,
            radius: ZONE_R,
            start_secs: t_secs, end_secs: t_secs,
        }).await?.into_inner();

        let mut in_zone = false;
        while stream.message().await?.is_some() {
            in_zone = true;
        }

        if in_zone {
            zone_count += 1;
            if first_in.is_none() { first_in = Some(frame); }
            last_in = Some(frame);
        }

        let marker = if in_zone { "● IN ZONE" } else { "·" };
        println!(" {:>5}  {:>7.1}  {:>7.1}  {:>4.1}   {}", frame, x, y, z, marker);
    }

    // ── 6. Query summary ─────────────────────────────────────────────────────
    println!("─────────────────────────────────────────");
    match (first_in, last_in) {
        (Some(f), Some(l)) => println!("\nQuery — in zone: {} frame(s), frames {}–{}", zone_count, f, l),
        _ => println!("\nQuery — entity never entered detection zone"),
    }

    // ── 7. Subscription summary ───────────────────────────────────────────────
    // Give any in-flight notifications a moment to arrive, then drain the channel.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut notifications: Vec<QueryResponse> = Vec::new();
    while let Ok(n) = notify_rx.try_recv() {
        notifications.push(n);
    }

    println!();
    println!("Subscription notifications received: {}", notifications.len());
    if !notifications.is_empty() {
        println!("  {:<6}  {:>8}  {:>8}  {:>6}", "t(s)", "x", "y", "z");
        println!("  {}", "─".repeat(34));
        for n in &notifications {
            println!("  {:<6}  {:>8.1}  {:>8.1}  {:>6.1}", n.timestamp_secs, n.x, n.y, n.z);
        }
    }

    // ── 8. Cleanup ───────────────────────────────────────────────────────────
    server_proc.kill().ok();
    server_proc.wait().ok();
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
